"""策略版本生命周期服务测试。

覆盖：草稿/复核/批准链、并发发布、撤回、定时切换、回滚、
异常重启恢复、运行快照固定、信号与订单 provenance、历史不可变。
"""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pytest

from app.config import get_engine
from app.entities.strategy import (
    Base as StrategyBase,
    PublishAction,
    PublishEventStatus,
    RunStatus,
    VersionStatus,
)
from app.services.strategy_lifecycle_service import (
    StrategyLifecycleException,
    StrategyLifecycleService,
    canonical_content_hash,
)


@pytest.fixture(autouse=True)
def setup_database():
    engine = get_engine()
    StrategyBase.metadata.drop_all(bind=engine)
    StrategyBase.metadata.create_all(bind=engine)
    yield
    StrategyBase.metadata.drop_all(bind=engine)


class FakeClock:
    """可控时钟，模拟定时切换与异常重启的时间推进。"""

    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float):
        self.now += timedelta(seconds=seconds)


@pytest.fixture
def clock():
    return FakeClock(datetime(2026, 10, 7, 9, 0, 0))


@pytest.fixture
def service(clock):
    return StrategyLifecycleService(clock=clock)


def _params(v):
    return {"ma_period": v, "threshold": 0.5}


def _risk(v):
    return {"max_single_order_amount": v, "stop_loss_ratio": 0.08}


def _approved_version(service, key="chan-a", params_v=10, reviewers=("alice", "bob")):
    """创建草稿并走完整批准链，返回版本 dict。"""
    v = service.create_draft(key, _params(params_v), _risk(100000), created_by="quant")
    service.submit_for_review(v["id"], list(reviewers))
    for r in reviewers:
        service.record_decision(v["id"], r, "approved")
    return service.get_version(v["id"])


# ---------------------------------------------------------------------------
# 草稿与版本号
# ---------------------------------------------------------------------------

class TestDraft:

    def test_version_numbers_monotonic_and_hash_stable(self, service):
        v1 = service.create_draft("k", _params(1), _risk(1))
        v2 = service.create_draft("k", _params(2), _risk(1))
        v3 = service.create_draft("other", _params(1), _risk(1))
        assert [v1["version"], v2["version"]] == [1, 2]
        assert v3["version"] == 1
        assert v1["content_hash"] == canonical_content_hash(_params(1), _risk(1))
        assert v1["content_hash"] != v2["content_hash"]

    def test_only_draft_editable(self, service):
        v = service.create_draft("k", _params(1), _risk(1))
        updated = service.update_draft(v["id"], params=_params(2))
        assert updated["params"] == _params(2)
        assert updated["content_hash"] == canonical_content_hash(_params(2), _risk(1))

        service.submit_for_review(v["id"], ["alice"])
        with pytest.raises(StrategyLifecycleException) as exc:
            service.update_draft(v["id"], params=_params(3))
        assert exc.value.status_code == 409

    def test_params_must_be_objects(self, service):
        with pytest.raises(StrategyLifecycleException):
            service.create_draft("k", [], _risk(1))
        with pytest.raises(StrategyLifecycleException):
            service.create_draft("k", _params(1), [])


# ---------------------------------------------------------------------------
# 复核与批准链
# ---------------------------------------------------------------------------

class TestApprovalChain:

    def test_ordered_chain_must_complete_before_publish(self, service):
        v = service.create_draft("k", _params(1), _risk(1))
        service.submit_for_review(v["id"], ["alice", "bob"])

        # 草稿已进入复核，不能重复提交
        with pytest.raises(StrategyLifecycleException):
            service.submit_for_review(v["id"], ["alice"])

        # bob 不能越级
        with pytest.raises(StrategyLifecycleException) as exc:
            service.record_decision(v["id"], "bob", "approved")
        assert exc.value.status_code == 409
        approvals = service.list_approvals(v["id"])
        assert [a["reviewer"] for a in approvals] == ["alice", "bob"]
        assert all(a["decision"] == "pending" for a in approvals)

        # 未完成批准链不能发布
        service.record_decision(v["id"], "alice", "approved", comment="ok")
        with pytest.raises(StrategyLifecycleException) as exc:
            service.publish(v["id"])
        assert exc.value.status_code == 409

        service.record_decision(v["id"], "bob", "approved")
        assert service.get_version(v["id"])["status"] == VersionStatus.APPROVED
        event = service.publish(v["id"], actor="ops")
        assert event["action"] == PublishAction.PUBLISH

    def test_rejection_is_terminal(self, service):
        v = service.create_draft("k", _params(1), _risk(1))
        service.submit_for_review(v["id"], ["alice", "bob"])
        service.record_decision(v["id"], "alice", "rejected", comment="参数有问题")
        assert service.get_version(v["id"])["status"] == VersionStatus.REJECTED
        with pytest.raises(StrategyLifecycleException):
            service.publish(v["id"])
        with pytest.raises(StrategyLifecycleException):
            service.record_decision(v["id"], "bob", "approved")

    def test_duplicate_reviewer_rejected(self, service):
        v = service.create_draft("k", _params(1), _risk(1))
        with pytest.raises(StrategyLifecycleException):
            service.submit_for_review(v["id"], ["alice", "alice"])


# ---------------------------------------------------------------------------
# 发布 / 撤回 / 回滚 / 定时
# ---------------------------------------------------------------------------

class TestPublishTimeline:

    def test_full_publish_withdraw_rollback(self, service, clock):
        v1 = _approved_version(service, params_v=10)
        e1 = service.publish(v1["id"])
        active = service.get_active_version("chan-a")
        assert active["version"] == 1
        assert e1["status"] == PublishEventStatus.ACTIVE

        # 新版本走完整批准链后发布，v1 变 superseded
        v2 = _approved_version(service, params_v=20)
        service.publish(v2["id"])
        assert service.get_version(v1["id"])["status"] == VersionStatus.SUPERSEDED
        assert service.get_version(v2["id"])["status"] == VersionStatus.PUBLISHED
        assert service.get_active_version("chan-a")["version"] == 2

        # 回滚到 v1
        rb = service.rollback("chan-a", reason="v2 异常")
        assert rb["action"] == PublishAction.ROLLBACK
        assert rb["version_id"] == v1["id"]
        assert service.get_active_version("chan-a")["version"] == 1
        assert service.get_version(v2["id"])["status"] == VersionStatus.SUPERSEDED
        assert service.get_version(v1["id"])["status"] == VersionStatus.PUBLISHED

        # 撤回后无线上版本，不能再回滚
        service.withdraw("chan-a")
        assert service.get_active_version("chan-a") is None
        with pytest.raises(StrategyLifecycleException):
            service.rollback("chan-a")

        timeline = service.list_timeline("chan-a")
        assert [e["action"] for e in timeline] == [
            PublishAction.PUBLISH,
            PublishAction.PUBLISH,
            PublishAction.ROLLBACK,
            PublishAction.WITHDRAW,
        ]

    def test_duplicate_publish_same_version_rejected(self, service):
        v = _approved_version(service)
        service.publish(v["id"])
        with pytest.raises(StrategyLifecycleException) as exc:
            service.publish(v["id"])
        assert exc.value.status_code == 409

    def test_no_active_strategy_withdraw_rejected(self, service):
        with pytest.raises(StrategyLifecycleException):
            service.withdraw("never-published")

    def test_backdated_event_rejected(self, service, clock):
        v = _approved_version(service)
        service.publish(v["id"])
        past = clock.now - timedelta(minutes=1)
        with pytest.raises(StrategyLifecycleException) as exc:
            service.publish(v["id"], effective_at=past)
        assert exc.value.status_code == 422

    def test_withdraw_marks_version_and_republish_preserves_approvals(self, service):
        v1 = _approved_version(service, params_v=10)
        service.publish(v1["id"])
        service.withdraw("chan-a")
        assert service.get_version(v1["id"])["status"] == VersionStatus.WITHDRAWN

        # 撤回的版本审批链仍然完整，可以重新发布，无需再次复核
        event = service.publish(v1["id"], reason="问题已排查")
        assert event["status"] == PublishEventStatus.ACTIVE
        assert service.get_version(v1["id"])["status"] == VersionStatus.PUBLISHED
        assert all(a["decision"] == "approved" for a in service.list_approvals(v1["id"]))

    def test_scheduled_switch_deterministic(self, service, clock):
        v1 = _approved_version(service, params_v=10)
        service.publish(v1["id"])

        v2 = _approved_version(service, params_v=20)
        switch_at = clock.now + timedelta(minutes=10)
        event = service.publish(v2["id"], effective_at=switch_at)
        assert event["status"] == PublishEventStatus.SCHEDULED
        assert service.list_scheduled("chan-a")[0]["id"] == event["id"]

        # 切换前线上仍是 v1，即使推进了一点时间
        clock.advance(60)
        assert service.get_active_version("chan-a")["version"] == 1
        assert service.process_due() == []

        # 到点后激活，v2 上线；重复 process_due 结果为空（幂等）
        clock.advance(600)
        activated = service.process_due()
        assert len(activated) == 1 and activated[0]["version_id"] == v2["id"]
        assert service.get_active_version("chan-a")["version"] == 2
        assert service.process_due() == []

    def test_cancel_scheduled_event(self, service, clock):
        v1 = _approved_version(service)
        service.publish(v1["id"])
        v2 = _approved_version(service, params_v=20)
        event = service.publish(v2["id"], effective_at=clock.now + timedelta(minutes=10))

        cancelled = service.cancel_scheduled(event["id"])
        assert cancelled["status"] == PublishEventStatus.CANCELLED
        clock.advance(1200)
        assert service.process_due() == []
        assert service.get_active_version("chan-a")["version"] == 1

    def test_cancel_after_effective_time_rejected(self, service, clock):
        v1 = _approved_version(service)
        service.publish(v1["id"])
        v2 = _approved_version(service, params_v=20)
        event = service.publish(v2["id"], effective_at=clock.now + timedelta(minutes=1))
        clock.advance(120)
        with pytest.raises(StrategyLifecycleException):
            service.cancel_scheduled(event["id"])

    def test_scheduled_withdraw_then_publish_chain(self, service, clock):
        v1 = _approved_version(service)
        service.publish(v1["id"])
        # 定时撤回，之后再发布新版本；顺序按 (effective_at, id) 折叠
        service.withdraw("chan-a", effective_at=clock.now + timedelta(minutes=5))
        v2 = _approved_version(service, params_v=20)
        service.publish(v2["id"], effective_at=clock.now + timedelta(minutes=10))

        clock.advance(300)
        service.process_due()
        assert service.get_active_version("chan-a") is None
        clock.advance(300)
        service.process_due()
        assert service.get_active_version("chan-a")["version"] == 2

    def test_history_query_is_stable_after_later_publishes(self, service):
        v1 = _approved_version(service, params_v=10)
        t1 = datetime(2026, 10, 7, 9, 0, 0)
        service.publish(v1["id"], effective_at=t1)
        v2 = _approved_version(service, params_v=20)
        service.publish(v2["id"], effective_at=t1 + timedelta(hours=1))
        v3 = _approved_version(service, params_v=30)
        service.publish(v3["id"], effective_at=t1 + timedelta(hours=2))

        assert service.get_active_version("chan-a", at=t1 + timedelta(minutes=30))["version"] == 1
        assert service.get_active_version("chan-a", at=t1 + timedelta(minutes=70))["version"] == 2
        assert service.get_active_version("chan-a", at=t1 + timedelta(hours=3))["version"] == 3


# ---------------------------------------------------------------------------
# 并发确定性
# ---------------------------------------------------------------------------

class TestConcurrency:

    def test_concurrent_draft_creation_unique_versions(self, service):
        def create(i):
            service.create_draft("k", _params(i), _risk(1))

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(create, range(20)))

        versions = service.list_versions("k")
        assert len(versions) == 20
        assert sorted(v["version"] for v in versions) == list(range(1, 21))
        assert len({v["content_hash"] for v in versions}) == 20

    def test_concurrent_publish_only_one_active(self, service):
        v = _approved_version(service)

        def publish(_):
            try:
                return service.publish(v["id"])
            except StrategyLifecycleException:
                return None

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(publish, range(10)))

        succeeded = [r for r in results if r is not None]
        assert len(succeeded) == 1
        timeline = service.list_timeline("chan-a")
        active = [e for e in timeline if e["status"] == PublishEventStatus.ACTIVE]
        assert len(active) == 1

    def test_concurrent_decisions_keep_chain_order(self, service):
        v = service.create_draft("k", _params(1), _risk(1))
        service.submit_for_review(v["id"], ["alice", "bob", "carol"])

        errors = []

        def decide(reviewer):
            try:
                service.record_decision(v["id"], reviewer, "approved")
            except StrategyLifecycleException as exc:
                errors.append(exc)

        # 三个人同时尝试：无论到达顺序如何，已批准节点必须恰好构成批准链前缀，
        # 不会出现 bob 已批而 alice 未批的越级状态。
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(decide, ["alice", "bob", "carol"]))

        approvals = service.list_approvals(v["id"])
        decisions = [a["decision"] for a in approvals]
        approved_count = decisions.count("approved")
        assert decisions == ["approved"] * approved_count + ["pending"] * (3 - approved_count)
        assert approved_count >= 1
        assert len(errors) == 3 - approved_count


# ---------------------------------------------------------------------------
# 运行快照
# ---------------------------------------------------------------------------

class TestRunSnapshot:

    def test_start_requires_published_version(self, service):
        v = service.create_draft("k", _params(1), _risk(1))
        service.submit_for_review(v["id"], ["alice"])
        service.record_decision(v["id"], "alice", "approved")
        with pytest.raises(StrategyLifecycleException) as exc:
            service.start_run("k")
        assert exc.value.status_code == 409

        service.publish(v["id"])
        snap = service.start_run("k", run_id="RUN-1")
        assert snap["version_number"] == 1
        assert snap["content_hash"] == v["content_hash"]
        assert snap["status"] == RunStatus.RUNNING
        assert [a["decision"] for a in snap["approval_basis"]] == ["approved"]

    def test_start_run_without_active_after_withdraw(self, service):
        v = _approved_version(service)
        service.publish(v["id"])
        service.withdraw("chan-a")
        with pytest.raises(StrategyLifecycleException):
            service.start_run("chan-a")

    def test_run_pinned_even_when_new_version_published(self, service):
        v1 = _approved_version(service, params_v=10)
        service.publish(v1["id"])
        snap = service.start_run("chan-a", run_id="RUN-1")

        # 运行期间 v2 走完批准链并发布
        v2 = _approved_version(service, params_v=20)
        service.publish(v2["id"])
        assert service.get_active_version("chan-a")["version"] == 2

        # 运行快照仍固定 v1 的参数、风险与指纹
        fresh = service.get_run("RUN-1")
        assert fresh["version_id"] == v1["id"]
        assert fresh["params"] == _params(10)
        assert fresh["content_hash"] == v1["content_hash"]

        # 新任务固定 v2
        snap2 = service.start_run("chan-a", run_id="RUN-2")
        assert snap2["version_id"] == v2["id"]

    def test_start_run_idempotent_same_run_id(self, service):
        v = _approved_version(service)
        service.publish(v["id"])
        first = service.start_run("chan-a", run_id="RUN-X")
        second = service.start_run("chan-a", run_id="RUN-X")
        assert first["id"] == second["id"]
        service.stop_run("RUN-X")
        with pytest.raises(StrategyLifecycleException):
            service.start_run("chan-a", run_id="RUN-X")

    def test_scheduled_switch_applies_on_start(self, service, clock):
        v1 = _approved_version(service, params_v=10)
        service.publish(v1["id"])
        v2 = _approved_version(service, params_v=20)
        service.publish(v2["id"], effective_at=clock.now + timedelta(minutes=5))

        clock.advance(300)
        # 尚未跑 process_due，但 start_run 会先激活到期事件
        snap = service.start_run("chan-a", run_id="RUN-T")
        assert snap["version_id"] == v2["id"]

    def test_crash_recovery_marks_stale_runs(self, service, clock):
        v = _approved_version(service)
        service.publish(v["id"])
        service.start_run("chan-a", run_id="RUN-1")
        service.heartbeat("RUN-1")

        clock.advance(61)
        crashed = service.recover_stale_runs(stale_seconds=60)
        assert len(crashed) == 1 and crashed[0]["run_id"] == "RUN-1"
        assert service.get_run("RUN-1")["status"] == RunStatus.CRASHED
        # 再次恢复幂等
        assert service.recover_stale_runs(stale_seconds=60) == []

    def test_list_runs_filter(self, service):
        v = _approved_version(service)
        service.publish(v["id"])
        service.start_run("chan-a", run_id="RUN-1")
        service.stop_run("RUN-1", status="stopped")
        service.start_run("chan-a", run_id="RUN-2")
        assert len(service.list_runs("chan-a")) == 2
        running = service.list_runs("chan-a", status=RunStatus.RUNNING)
        assert [r["run_id"] for r in running] == ["RUN-2"]


# ---------------------------------------------------------------------------
# 信号 / 订单留痕与 provenance
# ---------------------------------------------------------------------------

class TestProvenance:

    @pytest.fixture
    def started(self, service):
        v1 = _approved_version(service, params_v=10)
        service.publish(v1["id"])
        service.start_run("chan-a", run_id="RUN-1")
        return v1

    def _order(self, order_id="ORD-1", status="filled"):
        return {
            "order_id": order_id,
            "stock_code": "000001",
            "side": "buy",
            "order_type": "limit",
            "quantity": 100,
            "price": 10.0,
            "status": status,
        }

    def test_order_provenance_reconstructs_approval_basis(self, service, started):
        sig = service.record_signal("RUN-1", "000001", "BUY_1", 0.8, 10.0)
        service.record_order("RUN-1", self._order(), signal_id=sig["signal_id"])

        prov = service.order_provenance("ORD-1")
        assert prov["order"]["order_id"] == "ORD-1"
        assert prov["order"]["version_id"] == started["id"]
        assert prov["order"]["content_hash"] == started["content_hash"]
        assert prov["signal"]["signal_type"] == "BUY_1"
        assert prov["run_snapshot"]["run_id"] == "RUN-1"
        assert prov["run_snapshot"]["risk_params"] == _risk(100000)
        assert prov["version"]["version"] == 1
        assert [a["reviewer"] for a in prov["approval_chain"]] == ["alice", "bob"]
        assert all(a["decision"] == "approved" for a in prov["approval_chain"])

    def test_order_audit_is_write_once(self, service, started):
        service.record_order("RUN-1", self._order())
        with pytest.raises(StrategyLifecycleException) as exc:
            service.record_order("RUN-1", self._order())
        assert exc.value.status_code == 409

    def test_signal_must_belong_to_run(self, service, started):
        other_v = _approved_version(service, key="chan-b")
        service.publish(other_v["id"])
        service.start_run("chan-b", run_id="RUN-2")
        foreign = service.record_signal("RUN-2", "000002", "BUY_1", 0.5, 11.0)
        with pytest.raises(StrategyLifecycleException):
            service.record_order("RUN-1", self._order("ORD-9"), signal_id=foreign["signal_id"])

    def test_no_writes_after_run_ends(self, service, started):
        service.stop_run("RUN-1")
        with pytest.raises(StrategyLifecycleException):
            service.record_signal("RUN-1", "000001", "BUY_1", 0.8, 10.0)
        with pytest.raises(StrategyLifecycleException):
            service.record_order("RUN-1", self._order("ORD-2"))

    def test_provenance_unknown_order_404(self, service):
        with pytest.raises(StrategyLifecycleException) as exc:
            service.order_provenance("MISSING")
        assert exc.value.status_code == 404

    def test_run_signals_and_orders_listing(self, service, started):
        sig1 = service.record_signal("RUN-1", "000001", "BUY_1", 0.8, 10.0)
        service.record_order("RUN-1", self._order("ORD-1"), signal_id=sig1["signal_id"])
        sig2 = service.record_signal("RUN-1", "000001", "SELL_1", 0.6, 11.0)
        service.record_order("RUN-1", self._order("ORD-2", "cancelled"), signal_id=sig2["signal_id"])

        assert [s["signal_type"] for s in service.list_run_signals("RUN-1")] == ["BUY_1", "SELL_1"]
        assert [o["order_id"] for o in service.list_run_orders("RUN-1")] == ["ORD-1", "ORD-2"]

    def test_signal_id_is_write_once(self, service, started):
        service.record_signal("RUN-1", "000001", "BUY_1", 0.8, 10.0, signal_id="SIG-FIX")
        with pytest.raises(StrategyLifecycleException):
            service.record_signal("RUN-1", "000001", "BUY_1", 0.9, 10.0, signal_id="SIG-FIX")


# ---------------------------------------------------------------------------
# 后台推进器
# ---------------------------------------------------------------------------

class TestTimelineTicker:

    def test_tick_activates_events_and_recovers_runs(self, service, clock):
        from app.services.strategy_scheduler import StrategyTimelineTicker

        v1 = _approved_version(service, params_v=10)
        service.publish(v1["id"])
        v2 = _approved_version(service, params_v=20)
        service.publish(v2["id"], effective_at=clock.now + timedelta(seconds=1))
        service.start_run("chan-a", run_id="RUN-1")

        ticker = StrategyTimelineTicker(
            interval_seconds=1, stale_run_seconds=30, service=service
        )
        clock.advance(2)
        ticker.tick()

        assert service.get_active_version("chan-a")["version"] == 2
        clock.advance(40)
        ticker.tick()
        assert service.get_run("RUN-1")["status"] == RunStatus.CRASHED

        # tick 幂等：再跑一次不产生变化/异常
        ticker.tick()
        assert service.get_run("RUN-1")["status"] == RunStatus.CRASHED

