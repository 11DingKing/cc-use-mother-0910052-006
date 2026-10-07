"""业务模块说明。"""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import APP_CONFIG, CORS_CONFIG, LOG_CONFIG, init_database

# 配置日志
logging.basicConfig(
    level=getattr(logging, LOG_CONFIG["level"]),
    format=LOG_CONFIG["format"],
)
logger = logging.getLogger(__name__)

# 创建 FastAPI 应用实例
app = FastAPI(
    title=APP_CONFIG["title"],
    description=APP_CONFIG["description"],
    version=APP_CONFIG["version"],
)

# 注册 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_CONFIG["allow_origins"],
    allow_credentials=CORS_CONFIG["allow_credentials"],
    allow_methods=CORS_CONFIG["allow_methods"],
    allow_headers=CORS_CONFIG["allow_headers"],
)

# 注册全局异常处理
from app.middleware.exception_handler import register_exception_handlers
register_exception_handlers(app)

# 注册日志中间件
from app.middleware.logging_middleware import register_logging_middleware
register_logging_middleware(app)

# 注册控制器路由
from app.controllers import (
    stock_router,
    analysis_router,
    watchlist_router,
    backtest_router,
    trading_router,
    strategy_router,
)
app.include_router(stock_router)
app.include_router(analysis_router)
app.include_router(watchlist_router)
app.include_router(backtest_router)
app.include_router(trading_router)
app.include_router(strategy_router)


# 健康检查端点
@app.get("/api/health")
async def health_check():
    """业务模块说明。"""
    return {"status": "ok", "message": "缠论量化交易系统运行正常"}


@app.on_event("startup")
async def startup_event():
    """业务模块说明。"""
    logger.info("缠论量化交易系统启动中...")

    # 初始化数据库
    try:
        init_database()
        logger.info("数据库初始化完成")
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")

    # 策略生命周期：先补发宕机期间到点的定时切换、恢复运行中任务的固定快照，
    # 再启动定时切换循环。恢复的任务继续使用各自启动时的版本，不会拾取新版本。
    try:
        from app.controllers.strategy_controller import strategy_service

        recovered = strategy_service.recover_active_runs()
        if recovered:
            logger.info("已恢复 %d 个运行中的策略任务: %s",
                        len(recovered), recovered)
        strategy_service.start_scheduler()
    except Exception as e:
        logger.error(f"策略生命周期恢复失败: {e}")

    logger.info(f"API文档地址: http://{APP_CONFIG['host']}:{APP_CONFIG['port']}/docs")


@app.on_event("shutdown")
async def shutdown_event():
    """业务模块说明。"""
    logger.info("缠论量化交易系统关闭中...")
    try:
        from app.controllers.strategy_controller import strategy_service
        strategy_service.stop_scheduler()
    except Exception:
        pass
