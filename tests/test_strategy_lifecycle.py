"""策略草稿、复核、发布、回滚、撤回、定时切换、运行快照全生命周期测试。"""

import json
import threading
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import configure_sqlite_engine
from app.entities.strategy import (
    DECISION_APPROVED,
    DECISION_REJECTED,
    PUB_CANCELLED,
    PUB_EFFECTIVE,
    PUB_RECALLED,
    PUB_SCHEDULED,
    PUB_SUPERSEDED,
    RUN_STOPPED,
    VERSION_APPROVED,
    VERSION_DRAFT,
    VERSION_IN_REVIEW,
    VERSION_PUBLISHED,
    VERSION_REJECTED,
    VERSION_WITHDRAWN,
    StrategyVersion,
)
from app.mappers.strategy_mapper import StrategyMapper
from app.services.strategy_service import (
    StrategyConflictException,
    StrategyException,
    StrategyService,
    canonical_hash,
)


CHAIN = [
    {"role": "analyst_lead", "approver": "alice"},
    {"role": "risk_manager", "approver": "bob"},
    {"role": "cio", "approver": "carol"},
]

PARAMS_V1 = {
    "buy_points": ["BUY_1", "BUY_2"],
    "position_ratio": 0.1,
    "duan_strict": True,
}
RISK_V1 = {
    "max_single_order_amount": 50000,
    "max_daily_amount": 200000,
    "max_position_ratio": 0.3,
    "stop_loss_ratio": 0.08,
    "take_profit_ratio": 0.15,
}
PARAMS_V2 = {**PARAMS_V1, "position_ratio": 0.2}
RISK_V2 = {**RISK_V1, "stop_loss_ratio": 0.05}


class Clock:
    """可控时钟，保证定时切换测试的确定性。"""

    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture
def db_factory(tmp_path):
    db_path = tmp_path / "strategy.db"
    engine = create_engine(f"sqlite:///{db_path}")
    configure_sqlite_engine(engine)
    from app.entities.strategy import Base
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    yield factory
    engine.dispose()


@pytest.fixture
def service(db_factory):
    clock = Clock(datetime(2026, 10, 7, 9, 30, 0))
    return StrategyService(session_factory=db_factory, time_func=clock)


def make_draft(service, name="chan_a", params=None, risk=None, chain=None,
               change_note="初版", description="测试策略"):
    return service.create_draft(
        strategy_name=name,
        params=params or PARAMS_V1,
        risk_config=risk or RISK_V1,
        required_approvers=chain or CHAIN,
        description=description,
        change_note=change_note,
        created_by="drafter",
    )


def approve_full(service, version_id, chain=None):
    chain = chain or CHAIN
    service.submit_for_review(version_id, submitted_by="drafter")
    for step in chain:
        service.record_review(
            version_id, approver=step["approver"],
            decision=DECISION_APPROVED, comment="同意",
        )


# ---------------------------------------------------------------------------
# 草稿与内容冻结
# ---------------------------------------------------------------------------

class TestDraft:

    def test_create_draft_numbered_from_one(self, service):
        v = make_draft(service)
        assert v["version_no"] == 1
        assert v["status"] == VERSION_DRAFT
        assert v["content_hash"] == canonical_hash(PARAMS_V1, RISK_V1)

    def test_content_hash_key_order_independent(self, service):
        v1 = make_draft(service, name="s1", params={"a": 1, "b": 2},
                        risk={"x": 1})
        v2 = make_draft(service, name="s2", params={"b": 2, "a": 1},
                        risk={"x": 1})
        assert v1["content_hash"] == v2["content_hash"]

    def test_only_one_draft_per_strategy(self, service):
        make_draft(service)
        with pytest.raises(StrategyConflictException):
            make_draft(service)

    def test_update_draft_refreshes_hash(self, service):
        v = make_draft(service)
        updated = service.update_draft(v["id"], params=PARAMS_V2)
        assert updated["params"] == PARAMS_V2
        assert updated["content_hash"] == canonical_hash(PARAMS_V2, RISK_V1)

    def test_frozen_after_submit(self, service):
        v = make_draft(service)
        service.submit_for_review(v["id"])
        with pytest.raises(StrategyConflictException):
            service.update_draft(v["id"], params=PARAMS_V2)
        with pytest.raises(StrategyConflictException):
            service.delete_draft(v["id"])

    def test_invalid_payloads_rejected(self, service):
        with pytest.raises(StrategyException):
            service.create_draft("s", params={}, risk_config=RISK_V1,
                                 required_approvers=CHAIN)
        with pytest.raises(StrategyException):
            service.create_draft("s", params=PARAMS_V1, risk_config={},
                                 required_approvers=CHAIN)
        with pytest.raises(StrategyException):
            service.create_draft("s", params=PARAMS_V1, risk_config=RISK_V1,
                                 required_approvers=[])
        with pytest.raises(StrategyException):
            service.create_draft("s", params=PARAMS_V1, risk_config=RISK_V1,
                                 required_approvers=[{"role": "r"}])


# ---------------------------------------------------------------------------
# 批准链
# ---------------------------------------------------------------------------

class TestApprovalChain:

    def test_full_chain_approves(self, service):
        v = make_draft(service)
        approve_full(service, v["id"])
        detail = service.get_version(v["id"])
        assert detail["status"] == VERSION_APPROVED
        assert [a["step_no"] for a in detail["approvals"]] == [1, 2, 3]
        assert all(a["decision"] == DECISION_APPROVED for a in detail["approvals"])
        # 每一步批准都固定了当时的内容哈希
        assert all(a["content_hash"] == v["content_hash"]
                   for a in detail["approvals"])

    def test_wrong_approver_rejected(self, service):
        v = make_draft(service)
        service.submit_for_review(v["id"])
        with pytest.raises(StrategyConflictException):
            service.record_review(v["id"], approver="mallory",
                                  decision=DECISION_APPROVED)
        # 被拒之门外后版本仍在复核中，链上没有脏记录
        detail = service.get_version(v["id"])
        assert detail["status"] == VERSION_IN_REVIEW
        assert detail["approvals"] == []

    def test_must_follow_chain_order(self, service):
        v = make_draft(service)
        service.submit_for_review(v["id"])
        service.record_review(v["id"], "alice", DECISION_APPROVED)
        # 跳过 bob 直接用 carol
        with pytest.raises(StrategyConflictException):
            service.record_review(v["id"], "carol", DECISION_APPROVED)

    def test_rejection_terminates_chain(self, service):
        v = make_draft(service)
        service.submit_for_review(v["id"])
        service.record_review(v["id"], "alice", DECISION_APPROVED)
        result = service.record_review(v["id"], "bob", DECISION_REJECTED,
                                       comment="风险超限")
        assert result["status"] == VERSION_REJECTED
        with pytest.raises(StrategyConflictException):
            service.publish(v["id"], actor="bob")

    def test_withdraw_from_review(self, service):
        v = make_draft(service)
        service.submit_for_review(v["id"])
        service.record_review(v["id"], "alice", DECISION_APPROVED)
        result = service.withdraw_from_review(v["id"], actor="drafter")
        assert result["status"] == VERSION_WITHDRAWN
        with pytest.raises(StrategyConflictException):
            service.record_review(v["id"], "bob", DECISION_APPROVED)

    def test_cannot_review_twice_or_reopen(self, service):
        v = make_draft(service)
        approve_full(service, v["id"])
        with pytest.raises(StrategyConflictException):
            service.record_review(v["id"], "alice", DECISION_APPROVED)
        with pytest.raises(StrategyConflictException):
            service.submit_for_review(v["id"])

    def test_cannot_publish_draft(self, service):
        v = make_draft(service)
        with pytest.raises(StrategyConflictException):
            service.publish(v["id"])


# ---------------------------------------------------------------------------
# 发布 / 回滚 / 撤回
# ---------------------------------------------------------------------------

class TestPublishLifecycle:

    def test_publish_creates_effective(self, service):
        v = make_draft(service)
        approve_full(service, v["id"])
        pub = service.publish(v["id"], actor="carol")
        assert pub["status"] == PUB_EFFECTIVE
        assert pub["action"] == "publish"
        detail = service.get_version(v["id"])
        assert detail["status"] == VERSION_PUBLISHED

        effective = service.get_effective("chan_a")
        assert effective["version_id"] == v["id"]

    def test_new_version_supersedes_old(self, service):
        v1 = make_draft(service)
        approve_full(service, v1["id"])
        service.publish(v1["id"])

        v2 = make_draft(service, params=PARAMS_V2, risk=RISK_V2,
                        change_note="加仓+收紧止损")
        approve_full(service, v2["id"])
        service.publish(v2["id"])

        assert service.get_effective("chan_a")["version_id"] == v2["id"]
        pubs = service.list_publications("chan_a")
        assert pubs[0]["status"] == PUB_SUPERSEDED
        assert pubs[1]["status"] == PUB_EFFECTIVE
        # 事件流只增不改：历史行仍可查
        assert pubs[0]["version_id"] == v1["id"]

    def test_republish_same_version_conflicts(self, service):
        v = make_draft(service)
        approve_full(service, v["id"])
        service.publish(v["id"])
        with pytest.raises(StrategyConflictException):
            service.publish(v["id"])

    def test_rollback_to_previous_version(self, service):
        v1 = make_draft(service)
        approve_full(service, v1["id"])
        service.publish(v1["id"])
        v2 = make_draft(service, params=PARAMS_V2, risk=RISK_V2)
        approve_full(service, v2["id"])
        service.publish(v2["id"])

        pub = service.rollback("chan_a", actor="cio", note="紧急回滚")
        assert pub["action"] == "rollback"
        assert pub["status"] == PUB_EFFECTIVE
        assert pub["version_id"] == v1["id"]
        assert service.get_effective("chan_a")["version_id"] == v1["id"]

    def test_rollback_without_history_conflicts(self, service):
        with pytest.raises(StrategyConflictException):
            service.rollback("chan_a")
        v1 = make_draft(service)
        approve_full(service, v1["id"])
        service.publish(v1["id"])
        with pytest.raises(StrategyConflictException):
            service.rollback("chan_a")

    def test_recall_then_rollback_restores(self, service):
        v1 = make_draft(service)
        approve_full(service, v1["id"])
        service.publish(v1["id"])
        v2 = make_draft(service, params=PARAMS_V2, risk=RISK_V2)
        approve_full(service, v2["id"])
        service.publish(v2["id"])

        service.recall("chan_a", actor="cio", note="暂停交易")
        assert service.get_effective("chan_a") is None
        pubs = service.list_publications("chan_a")
        assert pubs[-1]["action"] == "recall"
        assert pubs[-1]["status"] == PUB_RECALLED

        # 撤回后新任务无法启动
        with pytest.raises(StrategyConflictException):
            service.start_run("chan_a")

        # 回滚到 v1 后可恢复
        restored = service.rollback("chan_a", actor="cio")
        assert restored["version_id"] == v1["id"]
        assert service.get_effective("chan_a")["version_id"] == v1["id"]


# ---------------------------------------------------------------------------
# 定时切换
# ---------------------------------------------------------------------------

class TestScheduledSwitch:

    def test_scheduled_stays_pending_until_due(self, service):
        v = make_draft(service)
        approve_full(service, v["id"])
        due = service._time_func() + timedelta(minutes=5)
        pub = service.publish(v["id"], scheduled_for=due)
        assert pub["status"] == PUB_SCHEDULED
        assert service.get_effective("chan_a") is None

        activated = service.activate_due_scheduled(service._time_func())
        assert activated == []
        assert service.get_effective("chan_a") is None

    def test_scheduled_activates_deterministically_at_due(self, service):
        v1 = make_draft(service)
        approve_full(service, v1["id"])
        service.publish(v1["id"])
        v2 = make_draft(service, params=PARAMS_V2, risk=RISK_V2)
        approve_full(service, v2["id"])
        due = service._time_func() + timedelta(minutes=10)
        service.publish(v2["id"], scheduled_for=due)

        service._time_func.now = due
        activated = service.activate_due_scheduled()
        assert activated  # 返回新生效事件 id
        assert service.get_effective("chan_a")["version_id"] == v2["id"]
        # 原始定时行保留为 cancelled，并指向新生效事件
        pubs = [p for p in service.list_publications("chan_a")
                if p["action"] == "publish"]
        scheduled_row = [p for p in pubs if p["scheduled_for"] is not None][0]
        assert scheduled_row["status"] == PUB_CANCELLED
        assert "已按时生效" in scheduled_row["note"]

    def test_only_one_pending_schedule_per_strategy(self, service):
        v1 = make_draft(service)
        approve_full(service, v1["id"])
        service.publish(v1["id"])
        v2 = make_draft(service, params=PARAMS_V2, risk=RISK_V2)
        approve_full(service, v2["id"])
        due = service._time_func() + timedelta(minutes=10)
        service.publish(v2["id"], scheduled_for=due)
        with pytest.raises(StrategyConflictException):
            service.publish(v2["id"], scheduled_for=due + timedelta(minutes=1))

    def test_cancel_scheduled(self, service):
        v = make_draft(service)
        approve_full(service, v["id"])
        due = service._time_func() + timedelta(minutes=10)
        pub = service.publish(v["id"], scheduled_for=due)
        cancelled = service.cancel_scheduled(pub["id"], actor="ops")
        assert cancelled["status"] == PUB_CANCELLED

        service._time_func.now = due + timedelta(minutes=1)
        assert service.activate_due_scheduled() == []
        assert service.get_effective("chan_a") is None

    def test_overdue_schedule_recovered_on_restart(self, service):
        # 模拟宕机：定时点已过但从未执行激活
        v = make_draft(service)
        approve_full(service, v["id"])
        due = service._time_func() + timedelta(minutes=10)
        service.publish(v["id"], scheduled_for=due)
        service._time_func.now = due + timedelta(minutes=30)
        activated = service.activate_due_scheduled()
        assert len(activated) == 1
        assert service.get_effective("chan_a")["version_id"] == v["id"]


# ---------------------------------------------------------------------------
# 运行快照：固定策略与风险，新版本不得越过批准链影响运行中任务
# ---------------------------------------------------------------------------

class TestRunSnapshot:

    def _v1_effective(self, service, name="chan_a"):
        v1 = make_draft(service, name=name)
        approve_full(service, v1["id"])
        service.publish(v1["id"])
        return v1

    def test_start_without_published_version_fails(self, service):
        make_draft(service)
        with pytest.raises(StrategyConflictException):
            service.start_run("chan_a")

    def test_snapshot_pins_version_and_risk(self, service):
        v1 = self._v1_effective(service)
        snap = service.start_run("chan_a", run_id="RUN-1")
        assert snap["version_id"] == v1["id"]
        assert snap["params"] == PARAMS_V1
        assert snap["risk_config"] == RISK_V1
        assert snap["content_hash"] == v1["content_hash"]
        assert len(snap["approval_evidence"]) == 3
        pin = service.get_pin("RUN-1")
        assert pin is not None
        assert pin.version_no == 1

    def test_running_task_immune_to_new_publication(self, service):
        v1 = self._v1_effective(service)
        service.start_run("chan_a", run_id="RUN-1")

        v2 = make_draft(service, params=PARAMS_V2, risk=RISK_V2)
        approve_full(service, v2["id"])
        service.publish(v2["id"])

        # 已运行任务仍固定在 v1
        pin_after = service.get_pin("RUN-1")
        assert pin_after.version_id == v1["id"]
        assert pin_after.risk_config == RISK_V1
        assert pin_after.params == PARAMS_V1
        # 新任务固定到 v2
        snap2 = service.start_run("chan_a", run_id="RUN-2")
        assert snap2["version_id"] == v2["id"]
        assert service.get_pin("RUN-2").risk_config == RISK_V2
        # 快照历史并列存在、互不影响
        runs = service.list_runs("chan_a")
        assert {r["run_id"] for r in runs} == {"RUN-1", "RUN-2"}

    def test_recall_does_not_touch_running_task(self, service):
        self._v1_effective(service)
        service.start_run("chan_a", run_id="RUN-1")
        service.recall("chan_a", actor="cio")
        pin = service.get_pin("RUN-1")
        assert pin is not None
        assert pin.version_no == 1

    def test_stop_preserves_snapshot_history(self, service):
        self._v1_effective(service)
        service.start_run("chan_a", run_id="RUN-1")
        result = service.stop_run("RUN-1")
        assert result["status"] == RUN_STOPPED
        assert result["ended_at"] is not None
        assert service.get_pin("RUN-1") is None
        # 历史快照仍可查
        assert service.get_snapshot("RUN-1")["status"] == RUN_STOPPED

    def test_recover_after_crash_rebinds_without_new_version(self, service,
                                                             db_factory):
        v1 = self._v1_effective(service)
        service.start_run("chan_a", run_id="RUN-1")

        v2 = make_draft(service, params=PARAMS_V2, risk=RISK_V2)
        approve_full(service, v2["id"])
        service.publish(v2["id"])  # 宕机前已发布 v2

        # 全新进程：缓存为空，仅靠快照表恢复
        restarted = StrategyService(
            session_factory=db_factory,
            time_func=Clock(datetime(2026, 10, 7, 10, 0, 0)),
        )
        recovered = restarted.recover_active_runs()
        assert recovered == ["RUN-1"]
        pin = restarted.get_pin("RUN-1")
        assert pin.version_id == v1["id"]       # 不拾取新版本
        assert pin.params == PARAMS_V1
        assert pin.risk_config == RISK_V1

    def test_duplicate_run_id_rejected(self, service):
        self._v1_effective(service)
        service.start_run("chan_a", run_id="RUN-1")
        with pytest.raises(StrategyConflictException):
            service.start_run("chan_a", run_id="RUN-1")


# ---------------------------------------------------------------------------
# 信号/订单留痕与批准依据还原
# ---------------------------------------------------------------------------

class TestTraceAndImmutability:

    def test_order_and_signal_trace_intact(self, service):
        v1 = make_draft(service)
        approve_full(service, v1["id"])
        service.publish(v1["id"])
        service.start_run("chan_a", run_id="RUN-1")
        pin = service.get_pin("RUN-1")

        service.record_signal(
            pin, "000001", "BUY_1", 0.8, 10.0,
            outcome="ordered", resulting_order_id="ORD_1",
        )

        class _Order:
            order_id = "ORD_1"
            stock_code = "000001"
            class side:
                value = "buy"
            quantity = 100
            price = __import__("decimal").Decimal("10.0")
            signal_type = "BUY_1"
            signal_strength = 0.8
            class status:
                value = "filled"

        service.record_order(pin, _Order())

        trace = service.trace_order("ORD_1")
        assert trace["intact"] is True
        assert trace["order"]["run_id"] == "RUN-1"
        assert trace["order"]["version_no"] == 1
        assert trace["version"]["params"] == PARAMS_V1
        assert trace["snapshot"]["risk_config"] == RISK_V1
        assert trace["signal"]["signal_type"] == "BUY_1"
        assert trace["publication"]["status"] == PUB_EFFECTIVE
        assert len(trace["approvals"]) == 3
        assert trace["hash_checks"]["version_hash_matches"] is True
        assert trace["hash_checks"]["all_approvals_hash_match"] is True

    def test_trace_missing_order_is_404(self, service):
        from app.middleware.exception_handler import NotFoundException
        with pytest.raises(NotFoundException):
            service.trace_order("NOPE")

    def test_tampered_version_is_detected(self, service, db_factory):
        v1 = make_draft(service)
        approve_full(service, v1["id"])
        service.publish(v1["id"])
        service.start_run("chan_a", run_id="RUN-1")
        pin = service.get_pin("RUN-1")

        class _Order:
            order_id = "ORD_9"
            stock_code = "600000"
            class side:
                value = "buy"
            quantity = 200
            price = __import__("decimal").Decimal("11")
            signal_type = "BUY_2"
            signal_strength = 0.5
            class status:
                value = "filled"

        service.record_order(pin, _Order())

        # 攻击者直接改库中已发布版本的参数（绕过服务）
        session = db_factory()
        try:
            row = session.get(StrategyVersion, v1["id"])
            tampered = {**PARAMS_V1, "position_ratio": 0.9}
            row.params_json = json.dumps(tampered, ensure_ascii=False)
            session.commit()
        finally:
            session.close()

        trace = service.trace_order("ORD_9")
        assert trace["intact"] is False
        assert trace["hash_checks"]["version_hash_matches"] is False
        # 订单留痕本身没有被改写，仍指向原始哈希
        assert trace["snapshot"]["content_hash"] == pin.content_hash


# ---------------------------------------------------------------------------
# 并发：数据库层串行化，任意时刻每策略只有一个生效版本
# ---------------------------------------------------------------------------

class TestConcurrency:

    def _published(self, factory, name, params, risk):
        svc = StrategyService(session_factory=factory,
                              time_func=Clock(datetime(2026, 10, 7, 9, 0, 0)))
        v = make_draft(svc, name=name, params=params, risk=risk)
        approve_full(svc, v["id"])
        return svc, v

    def test_concurrent_publish_same_version_single_winner(self, db_factory):
        svc, v = self._published(db_factory, "hot", PARAMS_V1, RISK_V1)

        results = []

        def worker():
            try:
                svc.publish(v["id"], actor=f"t{threading.get_ident()}")
                results.append("ok")
            except StrategyConflictException:
                results.append("conflict")
            except Exception as exc:  # pragma: no cover - 不应出现锁错误
                results.append(f"error:{type(exc).__name__}")

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results.count("ok") == 1
        assert results.count("conflict") == 7
        assert not [r for r in results if r.startswith("error")]

        session = db_factory()
        try:
            mapper = StrategyMapper(session)
            effective = [p for p in mapper.list_publications("hot")
                         if p.status == PUB_EFFECTIVE]
            assert len(effective) == 1
        finally:
            session.close()

    def test_concurrent_publish_distinct_versions_serializes(self, db_factory,
                                                             tmp_path):
        # 两个独立引擎（模拟跨进程），并发发布不同版本
        db_file = tmp_path / "xproc.db"
        from sqlalchemy import create_engine as ce
        from app.entities.strategy import Base
        engines, factories = [], []
        for _ in range(2):
            eng = ce(f"sqlite:///{db_file}")
            configure_sqlite_engine(eng)
            engines.append(eng)
            factories.append(sessionmaker(bind=eng))
        Base.metadata.create_all(engines[0])

        setup = StrategyService(
            session_factory=factories[0],
            time_func=Clock(datetime(2026, 10, 7, 9, 0, 0)),
        )
        v1 = make_draft(setup, name="xproc")
        approve_full(setup, v1["id"])
        setup.publish(v1["id"])
        v2 = make_draft(setup, params=PARAMS_V2, risk=RISK_V2)
        approve_full(setup, v2["id"])
        v3 = make_draft(setup, name="xproc",
                        params={**PARAMS_V2, "duan_strict": False},
                        risk=RISK_V2)
        approve_full(setup, v3["id"])

        barrier = threading.Barrier(2)
        outcomes = []

        def worker(factory, version_id):
            svc = StrategyService(session_factory=factory)
            barrier.wait()
            try:
                svc.publish(version_id, actor="concurrent")
                outcomes.append(("ok", version_id))
            except StrategyConflictException:
                outcomes.append(("conflict", version_id))

        t1 = threading.Thread(target=worker, args=(factories[0], v2["id"]))
        t2 = threading.Thread(target=worker, args=(factories[1], v3["id"]))
        t1.start(); t2.start(); t1.join(); t2.join()

        session = factories[0]()
        try:
            effective = [p for p in StrategyMapper(session).list_publications("xproc")
                         if p.status == PUB_EFFECTIVE]
            assert len(effective) == 1
            # 无论谁赢，生效版本必然是两个候选之一
            assert effective[0].version_id in {v2["id"], v3["id"]}
        finally:
            session.close()
        for eng in engines:
            eng.dispose()

    def test_concurrent_draft_creation_keeps_single_draft(self, db_factory):
        svc = StrategyService(session_factory=db_factory)
        errors = []

        def worker():
            try:
                make_draft(svc, name="race")
            except StrategyConflictException:
                errors.append("conflict")

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        drafts = svc.list_versions("race", VERSION_DRAFT)
        assert len(drafts) == 1
        assert drafts[0]["version_no"] == 1

    def test_cross_process_concurrent_publish_single_winner(self, tmp_path):
        """跨 OS 进程并发发布：恰好一个成功，且只有一个生效版本。"""
        multiprocessing = pytest.importorskip("multiprocessing")
        db_url = f"sqlite:///{tmp_path / 'xproc.db'}"
        engine = create_engine(db_url)
        configure_sqlite_engine(engine)
        from app.entities.strategy import Base
        Base.metadata.create_all(engine)

        setup = StrategyService(session_factory=sessionmaker(bind=engine))
        version_ids = []
        for i in range(5):
            v = make_draft(
                setup, name="hot",
                params={"slot": i}, risk=RISK_V1,
            )
            approve_full(setup, v["id"])
            version_ids.append(v["id"])
        setup.publish(version_ids[0])
        engine.dispose()

        ctx = multiprocessing.get_context("spawn")
        queue = ctx.Queue()
        from tests._concurrent_publish_worker import publish_worker

        procs = [
            ctx.Process(target=publish_worker, args=(db_url, vid, queue))
            for vid in version_ids[1:]
        ]
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=60)
            assert not p.is_alive(), "跨进程发布出现死锁/超时"
        results = sorted(queue.get() for _ in procs)

        assert results.count("ok") == 1
        assert not [r for r in results if r.startswith("error")]

        check_engine = create_engine(db_url)
        configure_sqlite_engine(check_engine)
        check = StrategyService(session_factory=sessionmaker(bind=check_engine))
        effective = [
            p for p in check.list_publications("hot")
            if p["status"] == PUB_EFFECTIVE
        ]
        assert len(effective) == 1
        assert effective[0]["version_id"] in version_ids[1:]
        check_engine.dispose()
