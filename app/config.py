"""业务模块说明。"""

import os
from pathlib import Path
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager


# 项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 数据库配置
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'chan_trading.db'}"
)

# SQLAlchemy 引擎和会话
_engine = None
_SessionLocal = None


def configure_sqlite_engine(engine) -> None:
    """为任意 SQLite 引擎附加确定性的并发语义（供测试与自定义引擎复用）。"""

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
        # 交给 SQLAlchemy 的 begin 事件显式开启事务
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def _begin_immediate(connection):
        connection.exec_driver_sql("BEGIN IMMEDIATE")


def _configure_sqlite(engine) -> None:
    """让默认 SQLite 写事务具备确定性的并发语义：

    - WAL 模式允许读写并发；
    - busy_timeout 让锁竞争等待而不是立即抛 database is locked；
    - 所有写事务以 BEGIN IMMEDIATE 开始，一旦开始即持有写锁，
      跨进程/跨线程的发布、撤回、定时切换在数据库层严格串行，
      配合部分唯一索引杜绝“两个生效版本”。
    """
    if "sqlite" in DATABASE_URL:
        configure_sqlite_engine(engine)


def get_engine():
    """业务模块说明。"""
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
            echo=os.getenv("SQL_ECHO", "false").lower() == "true",
        )
        _configure_sqlite(_engine)
    return _engine


def get_session_factory():
    """业务模块说明。"""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=get_engine(),
        )
    return _SessionLocal


def get_db_session() -> Session:
    """业务模块说明。"""
    SessionLocal = get_session_factory()
    return SessionLocal()


@contextmanager
def db_session_scope():
    """业务模块说明。"""
    session = get_db_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_database():
    """业务模块说明。"""
    from app.entities.stock import Base as StockBase
    from app.entities.analysis_result import Base as AnalysisBase
    from app.entities.watchlist import Base as WatchlistBase
    from app.entities.backtest import Base as BacktestBase
    from app.entities.strategy import Base as StrategyBase

    engine = get_engine()

    # 创建所有表
    StockBase.metadata.create_all(bind=engine)
    AnalysisBase.metadata.create_all(bind=engine)
    WatchlistBase.metadata.create_all(bind=engine)
    BacktestBase.metadata.create_all(bind=engine)
    StrategyBase.metadata.create_all(bind=engine)

# 数据源配置
DATA_SOURCE_CONFIG = {
    # 默认数据源：akshare, yahoo
    "default_source": os.getenv("DEFAULT_DATA_SOURCE", "akshare"),

    # AKShare 配置
    "akshare": {
        "enabled": True,
        "timeout": int(os.getenv("AKSHARE_TIMEOUT", "30")),
    },

    # Yahoo Finance 配置
    "yahoo": {
        "enabled": True,
        "timeout": int(os.getenv("YAHOO_TIMEOUT", "30")),
    },
}

# 支持的K线周期
SUPPORTED_PERIODS = ["daily", "60min", "30min"]

# FastAPI 应用配置
APP_CONFIG = {
    "title": "缠论量化交易系统",
    "description": "基于缠论的全栈量化交易平台 API",
    "version": "1.0.0",
    "host": os.getenv("APP_HOST", "0.0.0.0"),
    "port": int(os.getenv("APP_PORT", "8000")),
    "debug": os.getenv("APP_DEBUG", "true").lower() == "true",
}

# 日志配置
LOG_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}

# CORS 配置
CORS_CONFIG = {
    "allow_origins": os.getenv("CORS_ORIGINS", "*").split(","),
    "allow_credentials": True,
    "allow_methods": ["*"],
    "allow_headers": ["*"],
}
