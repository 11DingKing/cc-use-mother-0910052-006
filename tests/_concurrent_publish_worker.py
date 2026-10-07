"""跨进程并发发布压力测试的 worker（spawn 要求目标可导入）。"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import configure_sqlite_engine
from app.services.strategy_service import (
    StrategyConflictException,
    StrategyService,
)


def publish_worker(db_url: str, version_id: int, queue) -> None:
    try:
        engine = create_engine(db_url)
        configure_sqlite_engine(engine)
        service = StrategyService(session_factory=sessionmaker(bind=engine))
        service.publish(version_id, actor="pid-worker")
        queue.put("ok")
    except StrategyConflictException:
        queue.put("conflict")
    except Exception as exc:  # pragma: no cover - 任何锁错误都视为失败
        queue.put(f"error:{type(exc).__name__}:{exc}")
