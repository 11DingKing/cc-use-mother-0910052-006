"""业务模块说明。"""

from typing import Optional
from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.trading_service import TradingService

router = APIRouter(prefix="/api/trading", tags=["trading"])
trading_service = TradingService()


class ConnectRequest(BaseModel):
    """业务模块说明。"""
    adapter_type: str = "simulation"  # simulation, vnpy
    config: Optional[dict] = None


class BuyRequest(BaseModel):
    """业务模块说明。"""
    stock_code: str
    quantity: int
    price: Optional[float] = None
    order_type: str = "limit"  # limit, market
    signal_type: Optional[str] = None
    signal_strength: float = 0.0


class SellRequest(BaseModel):
    """业务模块说明。"""
    stock_code: str
    quantity: int
    price: Optional[float] = None
    order_type: str = "limit"
    signal_type: Optional[str] = None
    signal_strength: float = 0.0


class SignalTradeRequest(BaseModel):
    """业务模块说明。"""
    stock_code: str
    signal_type: str
    signal_strength: float
    price: float
    position_ratio: float = 0.1


class StrategyRunStartRequest(BaseModel):
    """启动自动交易任务：固定已发布的策略版本与风险设置。"""
    run_id: Optional[str] = None


class StrategyRunStopRequest(BaseModel):
    """结束自动交易任务。"""
    status: str = "completed"  # completed / stopped


@router.post("/connect")
async def connect(request: ConnectRequest):
    """业务模块说明。"""
    success = trading_service.connect(request.adapter_type, request.config)
    return {
        "success": success,
        "adapter_type": request.adapter_type,
        "message": "连接成功" if success else "连接失败",
    }


@router.post("/disconnect")
async def disconnect():
    """业务模块说明。"""
    trading_service.disconnect()
    return {"success": True, "message": "已断开连接"}


@router.get("/account")
async def get_account():
    """业务模块说明。"""
    return trading_service.get_account()


@router.get("/positions")
async def get_positions():
    """业务模块说明。"""
    return {"positions": trading_service.get_positions()}


@router.get("/positions/{stock_code}")
async def get_position(stock_code: str):
    """业务模块说明。"""
    position = trading_service.get_position(stock_code)
    if not position:
        return {"error": "未持有该股票"}
    return position


@router.post("/buy")
async def buy(request: BuyRequest):
    """业务模块说明。"""
    return trading_service.buy(
        stock_code=request.stock_code,
        quantity=request.quantity,
        price=request.price,
        order_type=request.order_type,
        signal_type=request.signal_type,
        signal_strength=request.signal_strength,
    )


@router.post("/sell")
async def sell(request: SellRequest):
    """业务模块说明。"""
    return trading_service.sell(
        stock_code=request.stock_code,
        quantity=request.quantity,
        price=request.price,
        order_type=request.order_type,
        signal_type=request.signal_type,
        signal_strength=request.signal_strength,
    )


@router.delete("/orders/{order_id}")
async def cancel_order(order_id: str):
    """业务模块说明。"""
    return trading_service.cancel_order(order_id)


@router.get("/orders/{order_id}")
async def get_order(order_id: str):
    """业务模块说明。"""
    return trading_service.get_order(order_id)


@router.get("/orders")
async def get_orders(
    stock_code: Optional[str] = Query(default=None, description="股票代码"),
    status: Optional[str] = Query(default=None, description="订单状态"),
):
    """业务模块说明。"""
    return {"orders": trading_service.get_orders(stock_code, status)}


@router.get("/quote/{stock_code}")
async def get_quote(stock_code: str):
    """业务模块说明。"""
    return trading_service.get_quote(stock_code)


@router.post("/signal-trade")
async def execute_signal_trade(request: SignalTradeRequest):
    """业务模块说明。"""
    result = trading_service.execute_signal(
        stock_code=request.stock_code,
        signal_type=request.signal_type,
        signal_strength=request.signal_strength,
        price=request.price,
        position_ratio=request.position_ratio,
    )
    
    if result:
        return result
    return {"message": "自动交易未启用或条件不满足"}


@router.post("/auto-trade/enable")
async def enable_auto_trade():
    """业务模块说明。"""
    trading_service.enable_auto_trade(True)
    return {"success": True, "message": "自动交易已启用"}


@router.post("/auto-trade/disable")
async def disable_auto_trade():
    """业务模块说明。"""
    trading_service.enable_auto_trade(False)
    return {"success": True, "message": "自动交易已禁用"}


@router.post("/check-stop-loss")
async def check_stop_loss():
    """业务模块说明。"""
    results = trading_service.check_stop_loss_take_profit()
    return {
        "triggered_count": len(results),
        "orders": results,
    }


@router.post("/strategy-runs/{strategy_key}/start")
async def start_strategy_run(strategy_key: str, request: StrategyRunStartRequest):
    """启动自动交易任务：固定策略版本、风险设置与批准依据为运行快照。

    任务运行期间发布的新版本不会影响该任务；该任务的每笔订单都可追溯到批准链。
    """
    return trading_service.start_strategy_run(strategy_key, run_id=request.run_id)


@router.post("/strategy-runs/stop")
async def stop_strategy_run(request: StrategyRunStopRequest):
    """结束当前策略任务；快照与订单审计永久保留。"""
    return trading_service.stop_strategy_run(status=request.status)


@router.get("/strategy-runs/current")
async def current_strategy_run():
    """当前任务固定的运行快照（含版本号、内容指纹、风险参数、批准链）。"""
    snapshot = trading_service.get_run_snapshot()
    if not snapshot:
        return {"run_snapshot": None}
    return {"run_snapshot": snapshot}


@router.post("/strategy-runs/heartbeat")
async def heartbeat_strategy_run():
    snapshot = trading_service.heartbeat_strategy_run()
    if not snapshot:
        return {"message": "当前没有运行中的策略任务"}
    return snapshot
