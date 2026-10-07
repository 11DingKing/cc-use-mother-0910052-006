"""策略发布时间线的后台推进器。

单容器部署下使用一个守护线程周期性：
1. 激活所有到期的定时发布/撤回/回滚事件（process_due）；
2. 将心跳超时的运行标记为 crashed（异常重启/崩溃恢复）。

所有实际逻辑都在 StrategyLifecycleService 内并受全局锁保护，
因此 ticker 与手动接口调用并发时结果仍然确定；重复 tick 是幂等的。
"""

import logging
import os
import threading

from typing import Optional

from app.services.strategy_lifecycle_service import StrategyLifecycleService

logger = logging.getLogger(__name__)


class StrategyTimelineTicker:
    """周期推进发布时间线与运行恢复的守护线程。"""

    def __init__(
        self,
        interval_seconds: float = 5.0,
        stale_run_seconds: int = 60,
        service: Optional[StrategyLifecycleService] = None,
    ):
        self.interval_seconds = interval_seconds
        self.stale_run_seconds = stale_run_seconds
        self.service = service or StrategyLifecycleService.get_instance()
        self._stop_event = threading.Event()
        self._thread: threading.Thread = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="strategy-timeline-ticker", daemon=True
        )
        self._thread.start()
        logger.info(
            "策略时间线推进器已启动: interval=%ss stale_run=%ss",
            self.interval_seconds, self.stale_run_seconds,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_seconds + 1)
        self._thread = None

    def tick(self) -> None:
        """执行一次推进：先激活到期事件，再回收僵死运行。"""
        try:
            activated = self.service.process_due()
            for event in activated:
                logger.info(
                    "定时事件已生效: id=%s strategy=%s action=%s version_id=%s",
                    event["id"], event["strategy_key"], event["action"], event["version_id"],
                )
        except Exception:
            logger.exception("推进到期发布事件失败")
        try:
            recovered = self.service.recover_stale_runs(self.stale_run_seconds)
            for run in recovered:
                logger.warning("运行心跳超时，标记为 crashed: run_id=%s", run["run_id"])
        except Exception:
            logger.exception("恢复僵死运行失败")

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            self.tick()


_ticker: StrategyTimelineTicker = None


def start_ticker() -> StrategyTimelineTicker:
    """应用启动时调用；可通过 STRATEGY_TICK_SECONDS=0 关闭。"""
    global _ticker
    interval = float(os.getenv("STRATEGY_TICK_SECONDS", "5"))
    if interval <= 0:
        logger.info("策略时间线推进器已通过 STRATEGY_TICK_SECONDS=0 关闭")
        return None
    stale = int(os.getenv("STRATEGY_STALE_RUN_SECONDS", "60"))
    _ticker = StrategyTimelineTicker(interval_seconds=interval, stale_run_seconds=stale)
    _ticker.start()
    return _ticker


def stop_ticker() -> None:
    global _ticker
    if _ticker is not None:
        _ticker.stop()
        _ticker = None
