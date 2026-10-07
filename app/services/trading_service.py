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

    def __init__(self, adapter: Optional[TradingAdapter] = None,
                 strategy_service=None):
        self.adapter = adapter or SimulationAdapter()
        self.risk_manager = RiskManager()
        self.analysis_service = AnalysisService()
        self._auto_trade_enabled = False
        # 策略生命周期服务（可注入；未绑定运行时交易行为保持原样）
        self._strategy_service = strategy_service
        # 当前任务固定的策略与风险设置；运行期间不随后续发布而改变
        self._pin = None

    def set_strategy_service(self, strategy_service) -> None:
        """注入策略生命周期服务。"""
        self._strategy_service = strategy_service

    @property
    def pinned_strategy(self):
        """当前任务固定的策略快照（未绑定时为 None）。"""
        return self._pin

    def start_pinned_run(self, strategy_name: str,
                         run_id: Optional[str] = None,
                         actor: Optional[str] = None) -> Dict[str, Any]:
        """启动任务：固定此刻生效的已批准策略版本与风险设置。"""
        if self._strategy_service is None:
            raise TradingException("未配置策略生命周期服务，无法固定策略版本")
        snapshot = self._strategy_service.start_run(
            strategy_name, run_id=run_id, actor=actor
        )
        # start_run 已写入快照；从服务缓存取回 PinnedStrategy
        self._pin = self._strategy_service.get_pin(snapshot["run_id"])
        # 风险设置固定为快照中的版本，之后的配置修改进不来
        self.risk_manager = RiskManager(self._pin.risk_config)
        self.enable_auto_trade(True)
        logger.info(
            "Trading run %s pinned to %s v%s (version_id=%s)",
            self._pin.run_id, strategy_name, self._pin.version_no,
            self._pin.version_id,
        )
        return snapshot

    def bind_recovered_run(self, run_id: str) -> Dict[str, Any]:
        """异常重启后把交易服务重新绑定到快照表中仍在运行的任务。"""
        if self._strategy_service is None:
            raise TradingException("未配置策略生命周期服务，无法恢复任务")
        pin = self._strategy_service.get_pin(run_id)
        if pin is None:
            raise TradingException(f"运行 {run_id} 无可恢复的策略快照")
        self._pin = pin
        self.risk_manager = RiskManager(pin.risk_config)
        return pin.to_dict()

    def stop_pinned_run(self) -> Optional[Dict[str, Any]]:
        """结束任务：快照保留为不可变历史，解除固定。"""
        if self._pin is None:
            return None
        run_id = self._pin.run_id
        snapshot = self._strategy_service.stop_run(run_id)
        self._pin = None
        self.risk_manager = RiskManager()
        self.enable_auto_trade(False)
        return snapshot

    def _stamp_order(self, order: Order) -> Order:
        """把运行快照中的版本依据盖到订单上（成交后可据此还原批准链）。"""
        pin = self._pin
        if pin is not None:
            order.strategy_name = pin.strategy_name
            order.run_id = pin.run_id
            order.strategy_version_id = pin.version_id
            order.strategy_version_no = pin.version_no
            order.strategy_content_hash = pin.content_hash
        return order

    def _record_order(self, order: Order) -> None:
        if self._pin is not None and self._strategy_service is not None:
            self._strategy_service.record_order(self._pin, order)
    
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
        )
        self._stamp_order(order)

        # 风控检查（固定运行时使用快照中的风险设置）
        account = self.adapter.get_account()
        positions = self.adapter.get_positions()

        passed, reason = self.risk_manager.check_order(order, account, positions)
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

        # 订单留痕：一经提交即固化版本依据，不随后续状态变化改写
        self._record_order(result)

        # 记录交易金额
        if result.status == OrderStatus.FILLED:
            self.risk_manager.record_trade(result.filled_price * result.filled_quantity)

        return result.to_dict()
    
    def sell(
        self,
        stock_code: str,
        quantity: int,
        price: Optional[float] = None,
        order_type: str = "limit",
        signal_type: Optional[str] = None,
        signal_strength: float = 0.0,
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
        )
        self._stamp_order(order)

        # 执行下单
        result = self.adapter.place_order(order)

        if result.status in (OrderStatus.REJECTED, OrderStatus.FAILED):
            raise TradingException(
                f"下单失败: {result.error_message}",
                order_id=result.order_id,
                stock_code=stock_code,
            )

        # 订单留痕：一经提交即固化版本依据，不随后续状态变化改写
        self._record_order(result)

        return result.to_dict()
    
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
        pin = self._pin

        def _record(outcome: str, order_id: Optional[str] = None) -> None:
            if pin is not None and self._strategy_service is not None:
                self._strategy_service.record_signal(
                    pin, stock_code, signal_type, signal_strength,
                    price, outcome, resulting_order_id=order_id,
                )

        if not self._auto_trade_enabled:
            logger.info(f"Auto trade disabled, signal ignored: {signal_type}")
            _record("ignored_auto_trade_disabled")
            return None

        account = self.adapter.get_account()
        if not account:
            _record("ignored_no_account")
            return None

        # 固定运行时，仓位比例等策略参数以快照为准
        if pin is not None and "position_ratio" in pin.params:
            position_ratio = pin.params["position_ratio"]

        is_buy = signal_type.startswith("BUY")

        if is_buy:
            # 计算买入数量
            available = float(account.available_cash)
            buy_amount = available * position_ratio * signal_strength
            quantity = int(buy_amount / price / 100) * 100  # 100股整数倍

            if quantity >= 100:
                try:
                    order_dict = self.buy(
                        stock_code=stock_code,
                        quantity=quantity,
                        price=price,
                        order_type="limit",
                        signal_type=signal_type,
                        signal_strength=signal_strength,
                    )
                except TradingException as exc:
                    _record("rejected", getattr(exc, "details", {}).get("order_id"))
                    raise
                _record("ordered", order_dict.get("order_id"))
                return order_dict
            _record("skipped_quantity_below_lot")
        else:
            # 卖出
            position = self.adapter.get_position(stock_code)
            if position and position.available_quantity > 0:
                # 根据信号强度决定卖出比例
                sell_quantity = int(position.available_quantity * signal_strength / 100) * 100
                if sell_quantity >= 100:
                    try:
                        order_dict = self.sell(
                            stock_code=stock_code,
                            quantity=sell_quantity,
                            price=price,
                            order_type="limit",
                            signal_type=signal_type,
                            signal_strength=signal_strength,
                        )
                    except TradingException as exc:
                        _record("rejected", getattr(exc, "details", {}).get("order_id"))
                        raise
                    _record("ordered", order_dict.get("order_id"))
                    return order_dict
                _record("skipped_quantity_below_lot")
            else:
                _record("skipped_no_position")

        return None
    
    def enable_auto_trade(self, enabled: bool = True) -> None:
        """业务模块说明。"""
        self._auto_trade_enabled = enabled
        logger.info(f"Auto trade {'enabled' if enabled else 'disabled'}")
    
    def check_stop_loss_take_profit(self) -> List[Dict[str, Any]]:
        """业务模块说明。"""
        results = []
        positions = self.adapter.get_positions()
        
        for pos in positions:
            if self.risk_manager.check_stop_loss(pos):
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
            
            elif self.risk_manager.check_take_profit(pos):
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
