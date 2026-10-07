"""业务模块说明。"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any

from app.trading.base import (
    TradingAdapter,
    Order,
    OrderStatus,
    OrderType,
    OrderSide,
    Position,
    Account,
    RiskManager,
)
from app.trading.simulation_adapter import SimulationAdapter
from app.trading.vnpy_adapter import VnpyAdapter
from app.services.analysis_service import AnalysisService
from app.services.strategy_lifecycle_service import StrategyLifecycleService
from app.middleware.exception_handler import AppException

logger = logging.getLogger(__name__)


class TradingException(AppException):
    """业务模块说明。"""
    
    def __init__(
        self,
        message: str,
        order_id: Optional[str] = None,
        stock_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            code="TRADING_ERROR",
            status_code=400,
            details={
                "order_id": order_id,
                "stock_code": stock_code,
                **(details or {}),
            },
        )


class TradingService:
    """业务模块说明。"""
    
    def __init__(self, adapter: Optional[TradingAdapter] = None):
        self.adapter = adapter or SimulationAdapter()
        self.risk_manager = RiskManager()
        self.analysis_service = AnalysisService()
        self._auto_trade_enabled = False
        # 策略运行固定：启动任务后冻结快照与风险参数，运行期间不随后续发布改变。
        self.lifecycle = StrategyLifecycleService.get_instance()
        self._run_snapshot: Optional[Dict[str, Any]] = None
        self._frozen_risk_manager: Optional[RiskManager] = None

    def start_strategy_run(self, strategy_key: str, run_id: Optional[str] = None) -> Dict[str, Any]:
        """启动策略任务：固定已发布版本、风险参数与批准依据为运行快照。"""
        snapshot = self.lifecycle.start_run(strategy_key, run_id=run_id)
        self._run_snapshot = snapshot
        self._frozen_risk_manager = RiskManager(snapshot.get("risk_params") or {})
        self._auto_trade_enabled = True
        logger.info(
            "自动交易任务已固定到策略版本: run_id=%s version=v%s",
            snapshot["run_id"], snapshot["version_number"],
        )
        return snapshot

    def stop_strategy_run(self, status: str = "completed") -> Dict[str, Any]:
        """结束当前策略任务；快照保留可查，之后不再产生新订单。"""
        if not self._run_snapshot:
            raise TradingException("当前没有运行中的策略任务")
        snapshot = self.lifecycle.stop_run(self._run_snapshot["run_id"], status=status)
        self._auto_trade_enabled = False
        self._run_snapshot = None
        self._frozen_risk_manager = None
        return snapshot

    def heartbeat_strategy_run(self) -> Optional[Dict[str, Any]]:
        if not self._run_snapshot:
            return None
        return self.lifecycle.heartbeat(self._run_snapshot["run_id"])

    def get_run_snapshot(self) -> Optional[Dict[str, Any]]:
        """返回当前任务固定的运行快照（含版本、风险参数、批准链）。"""
        return self._run_snapshot

    def _active_risk_manager(self) -> RiskManager:
        # 运行中任务使用启动时冻结的风险参数；手动交易（未启动任务）使用默认风控。
        return self._frozen_risk_manager or self.risk_manager

    def _require_active_run(self) -> Dict[str, Any]:
        if not self._run_snapshot:
            raise TradingException("没有运行中的策略任务，请先启动任务并固定已发布版本")
        fresh = self.lifecycle.get_run(self._run_snapshot["run_id"])
        if fresh["status"] != "running":
            self._auto_trade_enabled = False
            raise TradingException(
                f"策略任务状态为 {fresh['status']}，运行中的新版本未经过批准不得接管交易"
            )
        self._run_snapshot = fresh
        return fresh

    def _audit_order(self, result: Dict[str, Any], signal_id: Optional[str] = None) -> None:
        if self._run_snapshot:
            self.lifecycle.record_order(self._run_snapshot["run_id"], result, signal_id=signal_id)
    
    def connect(self, adapter_type: str = "simulation", config: Optional[Dict] = None) -> bool:
        """业务模块说明。"""
        if adapter_type == "vnpy":
            self.adapter = VnpyAdapter(config or {})
        else:
            self.adapter = SimulationAdapter(config)
        
        return self.adapter.connect()
    
    def disconnect(self) -> None:
        """业务模块说明。"""
        if self.adapter:
            self.adapter.disconnect()
    
    def get_account(self) -> Dict[str, Any]:
        """业务模块说明。"""
        account = self.adapter.get_account()
        if not account:
            raise TradingException("无法获取账户信息，请检查交易连接")
        return account.to_dict()
    
    def get_positions(self) -> List[Dict[str, Any]]:
        """业务模块说明。"""
        positions = self.adapter.get_positions()
        return [p.to_dict() for p in positions]
    
    def get_position(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """业务模块说明。"""
        position = self.adapter.get_position(stock_code)
        return position.to_dict() if position else None
    
    def buy(
        self,
        stock_code: str,
        quantity: int,
        price: Optional[float] = None,
        order_type: str = "limit",
        signal_type: Optional[str] = None,
        signal_strength: float = 0.0,
        signal_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """业务模块说明。"""
        # 数量校验
        if quantity <= 0 or quantity % 100 != 0:
            raise TradingException(
                "买入数量必须是100的整数倍",
                stock_code=stock_code,
            )
        
        # 构建订单
        ot = OrderType.LIMIT if order_type == "limit" else OrderType.MARKET
        
        if ot == OrderType.LIMIT and price is None:
            raise TradingException("限价单必须指定价格", stock_code=stock_code)
        
        order = Order(
            order_id=self.adapter._generate_order_id(),
            stock_code=stock_code,
            side=OrderSide.BUY,
            order_type=ot,
            quantity=quantity,
            price=Decimal(str(price)) if price else None,
            signal_type=signal_type,
            signal_strength=signal_strength,
            strategy_name=self._run_snapshot["strategy_key"] if self._run_snapshot else None,
        )

        # 风控检查：运行中的任务使用启动时冻结的风险参数
        account = self.adapter.get_account()
        positions = self.adapter.get_positions()

        passed, reason = self._active_risk_manager().check_order(order, account, positions)
        if not passed:
            raise TradingException(
                f"风控检查未通过: {reason}",
                stock_code=stock_code,
            )

        # 执行下单
        result = self.adapter.place_order(order)

        if result.status in (OrderStatus.REJECTED, OrderStatus.FAILED):
            raise TradingException(
                f"下单失败: {result.error_message}",
                order_id=result.order_id,
                stock_code=stock_code,
            )

        # 记录交易金额
        if result.status == OrderStatus.FILLED:
            self._active_risk_manager().record_trade(result.filled_price * result.filled_quantity)

        result_dict = result.to_dict()
        self._audit_order(result_dict, signal_id=signal_id)
        return result_dict

    def sell(
        self,
        stock_code: str,
        quantity: int,
        price: Optional[float] = None,
        order_type: str = "limit",
        signal_type: Optional[str] = None,
        signal_strength: float = 0.0,
        signal_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """业务模块说明。"""
        # 持仓检查
        position = self.adapter.get_position(stock_code)
        if not position or position.available_quantity < quantity:
            available = position.available_quantity if position else 0
            raise TradingException(
                f"可用持仓不足，需要 {quantity}，可用 {available}",
                stock_code=stock_code,
            )
        
        # 构建订单
        ot = OrderType.LIMIT if order_type == "limit" else OrderType.MARKET
        
        if ot == OrderType.LIMIT and price is None:
            raise TradingException("限价单必须指定价格", stock_code=stock_code)
        
        order = Order(
            order_id=self.adapter._generate_order_id(),
            stock_code=stock_code,
            side=OrderSide.SELL,
            order_type=ot,
            quantity=quantity,
            price=Decimal(str(price)) if price else None,
            signal_type=signal_type,
            signal_strength=signal_strength,
            strategy_name=self._run_snapshot["strategy_key"] if self._run_snapshot else None,
        )

        # 执行下单
        result = self.adapter.place_order(order)

        if result.status in (OrderStatus.REJECTED, OrderStatus.FAILED):
            raise TradingException(
                f"下单失败: {result.error_message}",
                order_id=result.order_id,
                stock_code=stock_code,
            )

        result_dict = result.to_dict()
        self._audit_order(result_dict, signal_id=signal_id)
        return result_dict
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """业务模块说明。"""
        order = self.adapter.get_order(order_id)
        if not order:
            raise TradingException("订单不存在", order_id=order_id)
        
        if order.status not in (OrderStatus.PENDING, OrderStatus.SUBMITTED):
            raise TradingException(
                f"订单状态为 {order.status.value}，无法撤销",
                order_id=order_id,
            )
        
        success = self.adapter.cancel_order(order_id)
        if not success:
            raise TradingException("撤单失败", order_id=order_id)
        
        order = self.adapter.get_order(order_id)
        return order.to_dict()
    
    def get_order(self, order_id: str) -> Dict[str, Any]:
        """业务模块说明。"""
        order = self.adapter.get_order(order_id)
        if not order:
            raise TradingException("订单不存在", order_id=order_id)
        return order.to_dict()
    
    def get_orders(
        self,
        stock_code: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """业务模块说明。"""
        order_status = OrderStatus(status) if status else None
        orders = self.adapter.get_orders(stock_code, order_status)
        return [o.to_dict() for o in orders]
    
    def get_quote(self, stock_code: str) -> Dict[str, Any]:
        """业务模块说明。"""
        quote = self.adapter.get_quote(stock_code)
        if not quote:
            raise TradingException("无法获取行情数据", stock_code=stock_code)
        return quote
    
    def execute_signal(
        self,
        stock_code: str,
        signal_type: str,
        signal_strength: float,
        price: float,
        position_ratio: float = 0.1,
    ) -> Optional[Dict[str, Any]]:
        """业务模块说明。"""
        if not self._auto_trade_enabled:
            logger.info(f"Auto trade disabled, signal ignored: {signal_type}")
            return None

        # 运行中的任务：信号先留痕，订单再关联信号，便于从订单还原批准依据。
        signal_id = None
        if self._run_snapshot:
            self._require_active_run()
            record = self.lifecycle.record_signal(
                run_id=self._run_snapshot["run_id"],
                stock_code=stock_code,
                signal_type=signal_type,
                signal_strength=signal_strength,
                price=price,
            )
            signal_id = record["signal_id"]

        account = self.adapter.get_account()
        if not account:
            return None

        is_buy = signal_type.startswith("BUY")

        if is_buy:
            # 计算买入数量
            available = float(account.available_cash)
            buy_amount = available * position_ratio * signal_strength
            quantity = int(buy_amount / price / 100) * 100  # 100股整数倍

            if quantity >= 100:
                return self.buy(
                    stock_code=stock_code,
                    quantity=quantity,
                    price=price,
                    order_type="limit",
                    signal_type=signal_type,
                    signal_strength=signal_strength,
                    signal_id=signal_id,
                )
        else:
            # 卖出
            position = self.adapter.get_position(stock_code)
            if position and position.available_quantity > 0:
                # 根据信号强度决定卖出比例
                sell_quantity = int(position.available_quantity * signal_strength / 100) * 100
                if sell_quantity >= 100:
                    return self.sell(
                        stock_code=stock_code,
                        quantity=sell_quantity,
                        price=price,
                        order_type="limit",
                        signal_type=signal_type,
                        signal_strength=signal_strength,
                        signal_id=signal_id,
                    )

        return None
    
    def enable_auto_trade(self, enabled: bool = True) -> None:
        """开启自动交易必须先启动固定了已发布版本的策略任务。"""
        if enabled:
            # 未经复核发布并固定到运行快照的策略不得进入自动交易。
            self._require_active_run()
        self._auto_trade_enabled = enabled
        logger.info(f"Auto trade {'enabled' if enabled else 'disabled'}")
    
    def check_stop_loss_take_profit(self) -> List[Dict[str, Any]]:
        """业务模块说明。"""
        results = []
        positions = self.adapter.get_positions()
        
        for pos in positions:
            if self._active_risk_manager().check_stop_loss(pos):
                # 触发止损
                quote = self.adapter.get_quote(pos.stock_code)
                if quote:
                    try:
                        result = self.sell(
                            stock_code=pos.stock_code,
                            quantity=pos.available_quantity,
                            price=quote["bid_price_1"],
                            order_type="limit",
                            signal_type="STOP_LOSS",
                        )
                        result["trigger"] = "stop_loss"
                        results.append(result)
                    except TradingException as e:
                        logger.error(f"Stop loss failed: {e}")
            
            elif self._active_risk_manager().check_take_profit(pos):
                # 触发止盈
                quote = self.adapter.get_quote(pos.stock_code)
                if quote:
                    try:
                        result = self.sell(
                            stock_code=pos.stock_code,
                            quantity=pos.available_quantity,
                            price=quote["bid_price_1"],
                            order_type="limit",
                            signal_type="TAKE_PROFIT",
                        )
                        result["trigger"] = "take_profit"
                        results.append(result)
                    except TradingException as e:
                        logger.error(f"Take profit failed: {e}")
        
        return results
