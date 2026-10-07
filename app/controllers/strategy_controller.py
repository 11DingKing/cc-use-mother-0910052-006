"""策略版本生命周期管理接口。

路径前缀 /api/strategies：
- 版本草稿 / 复核批准链 / 发布 / 撤回 / 回滚 / 定时事件
- 运行快照（启动、心跳、停止、异常恢复、历史查询）
- 信号与订单的批准依据还原
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.strategy_lifecycle_service import StrategyLifecycleService

router = APIRouter(prefix="/api/strategies", tags=["strategy-lifecycle"])
lifecycle_service = StrategyLifecycleService.get_instance()


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------

class DraftCreateRequest(BaseModel):
    params: dict
    risk_params: dict = Field(default_factory=dict)
    created_by: Optional[str] = None


class DraftUpdateRequest(BaseModel):
    params: Optional[dict] = None
    risk_params: Optional[dict] = None


class ReviewSubmitRequest(BaseModel):
    reviewers: List[str]
    actor: Optional[str] = None


class DecisionRequest(BaseModel):
    reviewer: str
    decision: str  # approved / rejected
    actor: Optional[str] = None
    comment: Optional[str] = None


class PublishRequest(BaseModel):
    effective_at: Optional[datetime] = None
    reason: Optional[str] = None
    actor: Optional[str] = None


class WithdrawRequest(BaseModel):
    effective_at: Optional[datetime] = None
    reason: Optional[str] = None
    actor: Optional[str] = None


class RollbackRequest(BaseModel):
    effective_at: Optional[datetime] = None
    reason: Optional[str] = None
    actor: Optional[str] = None


class RunStartRequest(BaseModel):
    run_id: Optional[str] = None


class RunStopRequest(BaseModel):
    status: str = "completed"  # completed / stopped


# ---------------------------------------------------------------------------
# 草稿与版本
# ---------------------------------------------------------------------------

@router.post("/{strategy_key}/versions")
async def create_draft(strategy_key: str, request: DraftCreateRequest):
    """创建策略草稿版本（版本号单调递增，内容生成指纹）。"""
    return lifecycle_service.create_draft(
        strategy_key=strategy_key,
        params=request.params,
        risk_params=request.risk_params,
        created_by=request.created_by,
    )


@router.get("/{strategy_key}/versions")
async def list_versions(strategy_key: str):
    """列出某策略的全部版本（含草稿、复核中、已发布、已撤回）。"""
    return {"strategy_key": strategy_key, "versions": lifecycle_service.list_versions(strategy_key)}


@router.get("/versions/{version_id}")
async def get_version(version_id: int):
    return lifecycle_service.get_version(version_id)


@router.patch("/versions/{version_id}")
async def update_draft(version_id: int, request: DraftUpdateRequest):
    """修改草稿；提交复核后内容不可变。"""
    return lifecycle_service.update_draft(
        version_id=version_id,
        params=request.params,
        risk_params=request.risk_params,
    )


# ---------------------------------------------------------------------------
# 复核与批准链
# ---------------------------------------------------------------------------

@router.post("/versions/{version_id}/submit")
async def submit_for_review(version_id: int, request: ReviewSubmitRequest):
    """提交复核并固定有序批准链。"""
    return lifecycle_service.submit_for_review(
        version_id=version_id,
        reviewers=request.reviewers,
        actor=request.actor,
    )


@router.get("/versions/{version_id}/approvals")
async def list_approvals(version_id: int):
    return {"approvals": lifecycle_service.list_approvals(version_id)}


@router.post("/versions/{version_id}/decisions")
async def record_decision(version_id: int, request: DecisionRequest):
    """记录批准/驳回；必须按批准链顺序，越级或驳回即终态。"""
    return lifecycle_service.record_decision(
        version_id=version_id,
        reviewer=request.reviewer,
        decision=request.decision,
        actor=request.actor,
        comment=request.comment,
    )


# ---------------------------------------------------------------------------
# 发布 / 撤回 / 回滚 / 定时
# ---------------------------------------------------------------------------

@router.post("/versions/{version_id}/publish")
async def publish_version(version_id: int, request: PublishRequest):
    """立即或定时发布；只有批准链完整通过的版本可以发布。"""
    return lifecycle_service.publish(
        version_id=version_id,
        effective_at=request.effective_at,
        reason=request.reason,
        actor=request.actor,
    )


@router.post("/{strategy_key}/withdraw")
async def withdraw_strategy(strategy_key: str, request: WithdrawRequest):
    """撤回线上版本（可定时）；撤回后不能启动新任务。"""
    return lifecycle_service.withdraw(
        strategy_key=strategy_key,
        effective_at=request.effective_at,
        reason=request.reason,
        actor=request.actor,
    )


@router.post("/{strategy_key}/rollback")
async def rollback_strategy(strategy_key: str, request: RollbackRequest):
    """回滚到时间线上最近的历史发布版本。"""
    return lifecycle_service.rollback(
        strategy_key=strategy_key,
        effective_at=request.effective_at,
        reason=request.reason,
        actor=request.actor,
    )


@router.get("/{strategy_key}/timeline")
async def get_timeline(strategy_key: str):
    """发布时间线（不可变事件流）及每个节点折叠后的生效版本。"""
    return {"strategy_key": strategy_key, "events": lifecycle_service.list_timeline(strategy_key)}


@router.get("/{strategy_key}/active")
async def get_active_version(
    strategy_key: str,
    at: Optional[datetime] = Query(default=None, description="查询时刻（ISO8601），默认当前"),
):
    """查询某时刻的线上版本；历史查询是时间线的确定性折叠。"""
    version = lifecycle_service.get_active_version(strategy_key, at)
    return {"strategy_key": strategy_key, "at": at.isoformat() if at else None, "version": version}


@router.get("/events/scheduled")
async def list_scheduled(strategy_key: Optional[str] = None):
    return {"events": lifecycle_service.list_scheduled(strategy_key)}


@router.delete("/events/{event_id}")
async def cancel_scheduled(event_id: int):
    """取消尚未生效的定时发布/撤回/回滚事件。"""
    return lifecycle_service.cancel_scheduled(event_id)


@router.post("/events/process-due")
async def process_due(strategy_key: Optional[str] = None):
    """手动激活到期事件（调度器也会周期性调用，重复调用结果确定）。"""
    return {"activated": lifecycle_service.process_due(strategy_key)}


# ---------------------------------------------------------------------------
# 运行快照
# ---------------------------------------------------------------------------

@router.post("/{strategy_key}/runs")
async def start_run(strategy_key: str, request: RunStartRequest):
    """启动任务并冻结策略参数、风险参数与批准依据为运行快照。"""
    return lifecycle_service.start_run(strategy_key, run_id=request.run_id)


@router.post("/runs/recover-stale")
async def recover_stale(stale_seconds: int = Query(default=60, ge=0)):
    """异常重启后恢复：心跳超时的运行标记为 crashed。"""
    return {"recovered": lifecycle_service.recover_stale_runs(stale_seconds)}


@router.get("/runs")
async def list_runs(
    strategy_key: Optional[str] = None,
    status: Optional[str] = None,
):
    return {"runs": lifecycle_service.list_runs(strategy_key, status)}


@router.get("/runs/{run_id}")
async def get_run(run_id: str):
    return lifecycle_service.get_run(run_id)


@router.post("/runs/{run_id}/heartbeat")
async def heartbeat(run_id: str):
    return lifecycle_service.heartbeat(run_id)


@router.post("/runs/{run_id}/stop")
async def stop_run(run_id: str, request: RunStopRequest):
    return lifecycle_service.stop_run(run_id, status=request.status)


@router.get("/runs/{run_id}/orders")
async def list_run_orders(run_id: str):
    return {"run_id": run_id, "orders": lifecycle_service.list_run_orders(run_id)}


@router.get("/runs/{run_id}/signals")
async def list_run_signals(run_id: str):
    return {"run_id": run_id, "signals": lifecycle_service.list_run_signals(run_id)}


# ---------------------------------------------------------------------------
# 信号 / 订单 provenance
# ---------------------------------------------------------------------------

@router.get("/signals/{signal_id}")
async def get_signal(signal_id: str):
    return lifecycle_service.signal_detail(signal_id)


@router.get("/orders/{order_id}/provenance")
async def get_order_provenance(order_id: str):
    """从订单还原：订单快照 -> 信号 -> 运行快照 -> 版本 -> 完整批准链。"""
    return lifecycle_service.order_provenance(order_id)
