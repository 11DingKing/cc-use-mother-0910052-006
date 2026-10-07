"""策略生命周期管理接口与交易侧固定运行的端到端测试。"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import configure_sqlite_engine
from app.entities.strategy import Base
from app.services.strategy_service import StrategyService
from app.controllers import strategy_controller, trading_controller


CHAIN = [
    {"role": "analyst_lead", "approver": "alice"},
    {"role": "risk_manager", "approver": "bob"},
]

PARAMS_V1 = {"buy_points": ["BUY_1"], "position_ratio": 0.1}
RISK_V1 = {
    "max_single_order_amount": 50000,
    "max_daily_amount": 200000,
    "max_position_ratio": 0.9,
    "stop_loss_ratio": 0.08,
    "take_profit_ratio": 0.15,
}
PARAMS_V2 = {**PARAMS_V1, "position_ratio": 0.2}
RISK_V2 = {**RISK_V1, "stop_loss_ratio": 0.05}


class Clock:
    def __init__(self, start):
        self.now = start

    def __call__(self):
        return self.now


@pytest.fixture
def client(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'api.db'}")
    configure_sqlite_engine(engine)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    clock = Clock(__import__("datetime").datetime(2026, 10, 7, 9, 30, 0))
    isolated = StrategyService(session_factory=factory, time_func=clock)

    # 将控制器单例替换为隔离实例
    original_strategy = strategy_controller.strategy_service
    original_trading_strategy = trading_controller.trading_service._strategy_service
    strategy_controller.strategy_service = isolated
    trading_controller.trading_service.set_strategy_service(isolated)

    from app.main import app
    with TestClient(app) as c:
        # 关闭后台定时线程：定时切换由用例显式触发，保证确定性
        isolated.stop_scheduler()
        yield c, isolated, clock

    strategy_controller.strategy_service = original_strategy
    trading_controller.trading_service.set_strategy_service(original_trading_strategy)
    engine.dispose()


def _make_approved_version(c, name="chan_a", params=None, risk=None, chain=None):
    """草稿→完整复核，但不发布；返回版本 dict。"""
    resp = c.post("/api/strategies/drafts", json={
        "strategy_name": name,
        "params": params or PARAMS_V1,
        "risk_config": risk or RISK_V1,
        "required_approvers": chain or CHAIN,
        "created_by": "drafter",
    })
    assert resp.status_code == 201, resp.text
    version = resp.json()
    vid = version["id"]
    assert c.post(f"/api/strategies/versions/{vid}/submit",
                  json={"actor": "drafter"}).status_code == 200
    for step in chain or CHAIN:
        resp = c.post(f"/api/strategies/versions/{vid}/reviews", json={
            "approver": step["approver"], "decision": "approved",
        })
        assert resp.status_code == 200, resp.text
    return version


def _publish_version(c, name="chan_a", params=None, risk=None, chain=None):
    """走完整草稿→复核→发布流程，返回版本 dict。"""
    version = _make_approved_version(c, name, params, risk, chain)
    resp = c.post(f"/api/strategies/versions/{version['id']}/publish",
                  json={"actor": "cio"})
    assert resp.status_code == 200, resp.text
    return version


class TestStrategyDraftReviewAPI:

    def test_full_draft_to_publish(self, client):
        c, _, _ = client
        v = _publish_version(c)
        assert v["version_no"] == 1

        resp = c.get("/api/strategies/chan_a/effective")
        assert resp.status_code == 200
        data = resp.json()
        assert data["effective"] is True
        assert data["version"]["version_no"] == 1
        assert len(data["version"]["approvals"]) == 2

    def test_wrong_approver_is_409(self, client):
        c, _, _ = client
        resp = c.post("/api/strategies/drafts", json={
            "strategy_name": "s", "params": PARAMS_V1,
            "risk_config": RISK_V1, "required_approvers": CHAIN,
        })
        vid = resp.json()["id"]
        c.post(f"/api/strategies/versions/{vid}/submit", json={"actor": "d"})
        resp = c.post(f"/api/strategies/versions/{vid}/reviews", json={
            "approver": "mallory", "decision": "approved",
        })
        assert resp.status_code == 409
        assert resp.json()["error"]["code"] == "STRATEGY_CONFLICT"

    def test_publish_unapproved_is_409(self, client):
        c, _, _ = client
        resp = c.post("/api/strategies/drafts", json={
            "strategy_name": "s2", "params": PARAMS_V1,
            "risk_config": RISK_V1, "required_approvers": CHAIN,
        })
        vid = resp.json()["id"]
        resp = c.post(f"/api/strategies/versions/{vid}/publish", json={})
        assert resp.status_code == 409

    def test_rejected_version_cannot_publish(self, client):
        c, _, _ = client
        resp = c.post("/api/strategies/drafts", json={
            "strategy_name": "s3", "params": PARAMS_V1,
            "risk_config": RISK_V1, "required_approvers": CHAIN,
        })
        vid = resp.json()["id"]
        c.post(f"/api/strategies/versions/{vid}/submit", json={})
        c.post(f"/api/strategies/versions/{vid}/reviews",
               json={"approver": "alice", "decision": "approved"})
        resp = c.post(f"/api/strategies/versions/{vid}/reviews",
                      json={"approver": "bob", "decision": "rejected"})
        assert resp.json()["status"] == "rejected"
        assert c.post(f"/api/strategies/versions/{vid}/publish",
                      json={}).status_code == 409

    def test_version_history_listing(self, client):
        c, _, _ = client
        _publish_version(c, name="hist")
        _publish_version(c, name="hist", params=PARAMS_V2, risk=RISK_V2)
        resp = c.get("/api/strategies/versions", params={"strategy_name": "hist"})
        versions = resp.json()["versions"]
        assert [v["version_no"] for v in versions] == [1, 2]
        # 事件流
        pubs = c.get("/api/strategies/publications",
                     params={"strategy_name": "hist"}).json()["publications"]
        assert [p["status"] for p in pubs] == ["superseded", "effective"]


class TestRollbackRecallAPI:

    def test_rollback_endpoint(self, client):
        c, _, _ = client
        v1 = _publish_version(c)
        _publish_version(c, params=PARAMS_V2, risk=RISK_V2)
        resp = c.post("/api/strategies/chan_a/rollback",
                      json={"actor": "cio", "note": "回退"})
        assert resp.status_code == 200
        assert resp.json()["version_id"] == v1["id"]
        assert resp.json()["action"] == "rollback"

    def test_recall_blocks_new_runs(self, client):
        c, _, _ = client
        _publish_version(c)
        resp = c.post("/api/strategies/chan_a/recall", json={"actor": "cio"})
        assert resp.status_code == 200
        assert c.get("/api/strategies/chan_a/effective").json()["effective"] is None
        resp = c.post("/api/trading/runs/start",
                      json={"strategy_name": "chan_a"})
        assert resp.status_code == 409


class TestScheduledSwitchAPI:

    def test_scheduled_then_activate(self, client):
        c, svc, clock = client
        v1 = _publish_version(c)
        v2 = _make_approved_version(c, params=PARAMS_V2, risk=RISK_V2)

        from datetime import timedelta
        due = clock.now + timedelta(minutes=10)
        resp = c.post(f"/api/strategies/versions/{v2['id']}/publish", json={
            "scheduled_for": due.isoformat(),
        })
        assert resp.json()["status"] == "scheduled"
        # 未到点不生效
        assert c.get("/api/strategies/chan_a/effective").json()[
            "version_id"] == v1["id"]

        clock.now = due
        resp = c.post("/api/strategies/scheduled/activate")
        assert resp.json()["count"] == 1
        assert c.get("/api/strategies/chan_a/effective").json()[
            "version_id"] == v2["id"]

    def test_cancel_scheduled(self, client):
        c, _, clock = client
        _publish_version(c)
        v2 = _make_approved_version(c, params=PARAMS_V2, risk=RISK_V2)
        from datetime import timedelta
        due = clock.now + timedelta(minutes=10)
        pub = c.post(f"/api/strategies/versions/{v2['id']}/publish",
                     json={"scheduled_for": due.isoformat()}).json()
        resp = c.post(
            f"/api/strategies/publications/{pub['id']}/cancel",
            json={"actor": "ops"},
        )
        assert resp.json()["status"] == "cancelled"


class TestPinnedTradingRunAPI:

    def test_run_pins_version_and_provenance_on_orders(self, client):
        c, _, _ = client
        _publish_version(c)
        c.post("/api/trading/connect", json={
            "adapter_type": "simulation",
            "config": {"initial_cash": 100000},
        })

        resp = c.post("/api/trading/runs/start",
                      json={"strategy_name": "chan_a", "run_id": "RUN-A"})
        assert resp.status_code == 200, resp.text
        pinned = resp.json()["snapshot"]
        assert pinned["version_no"] == 1

        # 固定信息可查询
        info = c.get("/api/trading/runs/pinned").json()
        assert info["pinned"] is True
        assert info["version_no"] == 1
        assert info["risk_config"]["stop_loss_ratio"] == 0.08

        # 下单带上版本依据
        order = c.post("/api/trading/buy", json={
            "stock_code": "000001", "quantity": 100, "price": 10.0,
            "signal_type": "BUY_1", "signal_strength": 0.8,
        }).json()
        assert order["run_id"] == "RUN-A"
        assert order["strategy_version_no"] == 1
        assert order["strategy_content_hash"] == pinned["content_hash"]

        # 管理接口可从订单还原批准依据
        trace = c.get(
            f"/api/strategies/orders/{order['order_id']}/trace"
        ).json()
        assert trace["intact"] is True
        assert trace["snapshot"]["run_id"] == "RUN-A"
        assert len(trace["approvals"]) == 2
        assert trace["version"]["params"]["position_ratio"] == 0.1

    def test_new_publication_does_not_change_running_pin(self, client):
        c, _, _ = client
        _publish_version(c)
        c.post("/api/trading/connect", json={
            "adapter_type": "simulation",
            "config": {"initial_cash": 100000},
        })
        c.post("/api/trading/runs/start",
               json={"strategy_name": "chan_a", "run_id": "RUN-A"})

        # 运行中发布 v2
        _publish_version(c, params=PARAMS_V2, risk=RISK_V2)
        info = c.get("/api/trading/runs/pinned").json()
        assert info["version_no"] == 1
        assert info["risk_config"]["stop_loss_ratio"] == 0.08

        # 信号触发自动交易，仍记录为 v1
        resp = c.post("/api/trading/signal-trade", json={
            "stock_code": "000002", "signal_type": "BUY_1",
            "signal_strength": 1.0, "price": 10.0,
        })
        assert resp.status_code == 200
        order = resp.json()
        assert order["strategy_version_no"] == 1

        signals = c.get("/api/strategies/runs/RUN-A/signals").json()["signals"]
        assert signals[0]["signal_type"] == "BUY_1"
        assert signals[0]["version_id"] == info["version_id"]

        # 停止后快照仍可查，且不可变
        stopped = c.post("/api/trading/runs/stop").json()
        assert stopped["success"] is True
        snap = c.get("/api/strategies/runs/RUN-A").json()
        assert snap["status"] == "stopped"
        assert snap["params"] == PARAMS_V1
        assert c.get("/api/trading/runs/pinned").json()["pinned"] is False

    def test_cannot_start_run_without_effective_version(self, client):
        c, _, _ = client
        resp = c.post("/api/trading/runs/start",
                      json={"strategy_name": "nope"})
        assert resp.status_code == 409

    def test_run_orders_listed_under_snapshot(self, client):
        c, _, _ = client
        _publish_version(c)
        c.post("/api/trading/connect", json={
            "adapter_type": "simulation",
            "config": {"initial_cash": 100000},
        })
        c.post("/api/trading/runs/start",
               json={"strategy_name": "chan_a", "run_id": "RUN-B"})
        c.post("/api/trading/buy", json={
            "stock_code": "000001", "quantity": 100, "price": 10.0,
        })
        orders = c.get("/api/strategies/runs/RUN-B/orders").json()["orders"]
        assert len(orders) == 1
        assert orders[0]["content_hash"]

    def test_risk_rejected_signal_is_recorded_not_silent(self, client):
        c, _, _ = client
        # 单笔限额极低、仓位比例极高，使信号计算出的订单必被风控拒绝
        risk = {**RISK_V1, "max_single_order_amount": 100}
        params = {**PARAMS_V1, "position_ratio": 1.0}
        _publish_version(c, name="strict", params=params, risk=risk)
        c.post("/api/trading/connect", json={
            "adapter_type": "simulation",
            "config": {"initial_cash": 100000},
        })
        c.post("/api/trading/runs/start",
               json={"strategy_name": "strict", "run_id": "RUN-R"})
        resp = c.post("/api/trading/signal-trade", json={
            "stock_code": "000001", "signal_type": "BUY_1",
            "signal_strength": 1.0, "price": 10.0,
        })
        assert resp.status_code == 400
        signals = c.get("/api/strategies/runs/RUN-R/signals").json()["signals"]
        assert signals[0]["outcome"] == "rejected"

    def test_recover_endpoint_state_after_restart(self, client, tmp_path):
        c, svc, _ = client
        _publish_version(c)
        c.post("/api/trading/connect", json={
            "adapter_type": "simulation",
            "config": {"initial_cash": 100000},
        })
        c.post("/api/trading/runs/start",
               json={"strategy_name": "chan_a", "run_id": "RUN-C"})

        # 模拟异常重启：新服务实例基于同一数据库恢复
        recovered_runs = svc.recover_active_runs()
        assert "RUN-C" in recovered_runs
        pin = svc.get_pin("RUN-C")
        assert pin is not None and pin.version_no == 1

        # 重新绑定交易服务后可继续交易，依据仍是 v1
        trading_controller.trading_service.bind_recovered_run("RUN-C")
        order = c.post("/api/trading/buy", json={
            "stock_code": "600000", "quantity": 100, "price": 10.0,
        }).json()
        assert order["run_id"] == "RUN-C"
        assert order["strategy_version_no"] == 1
