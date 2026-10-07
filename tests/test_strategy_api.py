"""策略生命周期管理接口与自动交易固定链路的集成测试。"""

import pytest
from fastapi.testclient import TestClient

from app.config import get_engine, init_database
from app.entities.strategy import Base as StrategyBase
from app.main import app


@pytest.fixture(scope="module")
def client():
    # tests/conftest.py 已关闭后台推进器，时间线推进由测试显式调用。
    init_database()
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def reset_strategy_tables():
    engine = get_engine()
    StrategyBase.metadata.drop_all(bind=engine)
    StrategyBase.metadata.create_all(bind=engine)
    yield
    StrategyBase.metadata.drop_all(bind=engine)
    StrategyBase.metadata.create_all(bind=engine)


def _publish_version(client, key="chan-a", params_v=10, reviewers=("alice", "bob")):
    """走完整草稿 -> 复核 -> 发布流程，返回版本 JSON。"""
    r = client.post(f"/api/strategies/{key}/versions", json={
        "params": {"ma_period": params_v},
        "risk_params": {"max_single_order_amount": 100000, "stop_loss_ratio": 0.08},
        "created_by": "quant",
    })
    assert r.status_code == 200, r.text
    version = r.json()

    r = client.post(f"/api/strategies/versions/{version['id']}/submit", json={
        "reviewers": list(reviewers),
    })
    assert r.status_code == 200, r.text
    for reviewer in reviewers:
        r = client.post(f"/api/strategies/versions/{version['id']}/decisions", json={
            "reviewer": reviewer,
            "decision": "approved",
        })
        assert r.status_code == 200, r.text

    r = client.post(f"/api/strategies/versions/{version['id']}/publish", json={"actor": "ops"})
    assert r.status_code == 200, r.text
    return version


class TestStrategyLifecycleAPI:

    def test_draft_review_publish_flow(self, client):
        r = client.post("/api/strategies/chan-x/versions", json={
            "params": {"k": 1},
            "risk_params": {"stop_loss_ratio": 0.05},
        })
        assert r.status_code == 200
        version = r.json()
        assert version["version"] == 1
        assert version["status"] == "draft"
        assert len(version["content_hash"]) == 64

        # 越级批准被拒
        client.post(f"/api/strategies/versions/{version['id']}/submit",
                    json={"reviewers": ["alice", "bob"]})
        r = client.post(f"/api/strategies/versions/{version['id']}/decisions", json={
            "reviewer": "bob", "decision": "approved",
        })
        assert r.status_code == 409

        r = client.get(f"/api/strategies/versions/{version['id']}/approvals")
        assert [a["reviewer"] for a in r.json()["approvals"]] == ["alice", "bob"]

        # 批准链未完成不得发布
        client.post(f"/api/strategies/versions/{version['id']}/decisions",
                    json={"reviewer": "alice", "decision": "approved"})
        r = client.post(f"/api/strategies/versions/{version['id']}/publish", json={})
        assert r.status_code == 409

        client.post(f"/api/strategies/versions/{version['id']}/decisions",
                    json={"reviewer": "bob", "decision": "approved"})
        r = client.post(f"/api/strategies/versions/{version['id']}/publish", json={})
        assert r.status_code == 200
        assert r.json()["status"] == "active"

        r = client.get("/api/strategies/chan-x/active")
        assert r.json()["version"]["version"] == 1

    def test_unapproved_version_cannot_publish(self, client):
        r = client.post("/api/strategies/chan-y/versions", json={
            "params": {"k": 1}, "risk_params": {},
        })
        version = r.json()
        r = client.post(f"/api/strategies/versions/{version['id']}/publish", json={})
        assert r.status_code == 409

    def test_rollback_and_withdraw_timeline(self, client):
        v1 = _publish_version(client, params_v=10)
        v2 = _publish_version(client, params_v=20)

        r = client.post("/api/strategies/chan-a/rollback", json={"reason": "v2 异常"})
        assert r.status_code == 200
        assert r.json()["version_id"] == v1["id"]

        r = client.get("/api/strategies/chan-a/active")
        assert r.json()["version"]["version"] == 1

        r = client.post("/api/strategies/chan-a/withdraw", json={})
        assert r.status_code == 200
        r = client.get("/api/strategies/chan-a/active")
        assert r.json()["version"] is None

        r = client.get("/api/strategies/chan-a/timeline")
        actions = [e["action"] for e in r.json()["events"]]
        assert actions == ["publish", "publish", "rollback", "withdraw"]

    def test_history_query_is_deterministic(self, client):
        _publish_version(client, params_v=10)
        _publish_version(client, params_v=20)
        # v1 的生效时刻取自时间线；该时刻线上就是 v1，不被后续发布篡改
        timeline = client.get("/api/strategies/chan-a/timeline").json()["events"]
        v1_effective_at = timeline[0]["effective_at"]
        r = client.get("/api/strategies/chan-a/active", params={"at": v1_effective_at})
        assert r.status_code == 200
        assert r.json()["version"]["version"] == 1
        assert client.get("/api/strategies/chan-a/active").json()["version"]["version"] == 2

    def test_scheduled_event_cancel(self, client):
        _publish_version(client)
        v2 = client.post("/api/strategies/chan-a/versions", json={
            "params": {"ma_period": 20}, "risk_params": {},
        }).json()
        client.post(f"/api/strategies/versions/{v2['id']}/submit",
                    json={"reviewers": ["alice", "bob"]})
        for reviewer in ("alice", "bob"):
            client.post(f"/api/strategies/versions/{v2['id']}/decisions",
                        json={"reviewer": reviewer, "decision": "approved"})

        future = "2099-01-01T00:00:00"
        r = client.post(f"/api/strategies/versions/{v2['id']}/publish",
                        json={"effective_at": future})
        assert r.status_code == 200
        event_id = r.json()["id"]

        r = client.get("/api/strategies/events/scheduled", params={"strategy_key": "chan-a"})
        assert [e["id"] for e in r.json()["events"]] == [event_id]

        r = client.delete(f"/api/strategies/events/{event_id}")
        assert r.status_code == 200
        assert r.json()["status"] == "cancelled"

    def test_recover_stale_runs(self, client):
        _publish_version(client)
        r = client.post("/api/strategies/chan-a/runs", json={"run_id": "RUN-A"})
        assert r.status_code == 200
        r = client.post("/api/strategies/runs/recover-stale", params={"stale_seconds": 0})
        assert r.status_code == 200
        recovered = [run["run_id"] for run in r.json()["recovered"]]
        assert "RUN-A" in recovered
        r = client.get("/api/strategies/runs/RUN-A")
        assert r.json()["status"] == "crashed"


class TestPinnedTradingIntegration:

    def test_orders_pinned_to_snapshot_and_traceable(self, client):
        v1 = _publish_version(client, params_v=10)

        client.post("/api/trading/connect", json={
            "adapter_type": "simulation",
            "config": {"initial_cash": 500000},
        })

        # 未发布版本无法启动任务
        r = client.post("/api/trading/strategy-runs/chan-z/start", json={})
        assert r.status_code == 409

        # 启动任务，固定 v1
        r = client.post("/api/trading/strategy-runs/chan-a/start", json={"run_id": "RUN-T1"})
        assert r.status_code == 200
        snap = r.json()
        assert snap["version_id"] == v1["id"]
        assert snap["params"] == {"ma_period": 10}

        r = client.get("/api/trading/strategy-runs/current")
        assert r.json()["run_snapshot"]["run_id"] == "RUN-T1"

        # 下单成功，并自动写入订单 provenance
        r = client.post("/api/trading/buy", json={
            "stock_code": "000001", "quantity": 100, "price": 10.0,
        })
        assert r.status_code == 200
        order = r.json()
        assert order["strategy_name"] == "chan-a"

        r = client.get(f"/api/strategies/orders/{order['order_id']}/provenance")
        assert r.status_code == 200
        prov = r.json()
        assert prov["order"]["run_id"] == "RUN-T1"
        assert prov["order"]["version_number"] == 1
        assert prov["order"]["content_hash"] == v1["content_hash"]
        assert prov["run_snapshot"]["risk_params"]["stop_loss_ratio"] == 0.08
        assert [a["reviewer"] for a in prov["approval_chain"]] == ["alice", "bob"]
        assert all(a["decision"] == "approved" for a in prov["approval_chain"])

        # 运行期间发布 v2，后续订单仍然固定 v1
        v2 = _publish_version(client, params_v=20)
        assert client.get("/api/strategies/chan-a/active").json()["version"]["version"] == 2

        r = client.post("/api/trading/buy", json={
            "stock_code": "000001", "quantity": 100, "price": 10.0,
        })
        order2 = r.json()
        prov2 = client.get(
            f"/api/strategies/orders/{order2['order_id']}/provenance"
        ).json()
        assert prov2["order"]["version_id"] == v1["id"]
        assert prov2["order"]["version_id"] != v2["id"]

        # 信号交易：信号与订单都可从订单还原
        r = client.post("/api/trading/signal-trade", json={
            "stock_code": "000001",
            "signal_type": "SELL_1",
            "signal_strength": 1.0,
            "price": 10.0,
            "position_ratio": 0.01,
        })
        assert r.status_code == 200
        sig_order = r.json()
        prov3 = client.get(
            f"/api/strategies/orders/{sig_order['order_id']}/provenance"
        ).json()
        assert prov3["signal"] is not None
        assert prov3["signal"]["signal_type"] == "SELL_1"
        assert prov3["signal"]["content_hash"] == v1["content_hash"]

        signals = client.get("/api/strategies/runs/RUN-T1/signals").json()["signals"]
        assert [s["signal_type"] for s in signals] == ["SELL_1"]

        # 停止任务后快照仍可查询，且运行记录列表完整
        r = client.post("/api/trading/strategy-runs/stop", json={"status": "completed"})
        assert r.status_code == 200
        assert client.get("/api/trading/strategy-runs/current").json()["run_snapshot"] is None
        run = client.get("/api/strategies/runs/RUN-T1").json()
        assert run["status"] == "completed"

        orders = client.get("/api/strategies/runs/RUN-T1/orders").json()["orders"]
        assert {o["order_id"] for o in orders} == {order["order_id"], order2["order_id"],
                                                   sig_order["order_id"]}
        # 已完成记录不被篡改：provenance 仍指向 v1
        assert client.get(
            f"/api/strategies/orders/{order['order_id']}/provenance"
        ).json()["order"]["version_id"] == v1["id"]

    def test_audit_never_rewritten(self, client):
        """同一 order_id 重复留痕必须失败，保证已完成记录不被篡改。"""
        _publish_version(client)
        client.post("/api/trading/connect", json={"adapter_type": "simulation"})
        client.post("/api/trading/strategy-runs/chan-a/start", json={"run_id": "RUN-T2"})
        r = client.post("/api/trading/buy", json={
            "stock_code": "000001", "quantity": 100, "price": 10.0,
        })
        order_id = r.json()["order_id"]

        from app.services.strategy_lifecycle_service import (
            StrategyLifecycleException,
            StrategyLifecycleService,
        )
        svc = StrategyLifecycleService.get_instance()
        payload = {
            "order_id": order_id, "stock_code": "000001", "side": "buy",
            "order_type": "limit", "quantity": 100, "price": 10.0, "status": "filled",
        }
        with pytest.raises(StrategyLifecycleException) as exc:
            svc.record_order("RUN-T2", payload)
        assert exc.value.status_code == 409
