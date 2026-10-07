"""策略版本生命周期管理接口：草稿、复核、发布、回滚、撤回、运行快照与留痕查询。"""

from typing import List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.strategy_service import StrategyService

router = APIRouter(prefix="/api/strategies", tags=["strategy"])

# 进程内单例：与 trading_controller 中的交易服务共享
strategy_service = StrategyService()


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------

class ApproverStep(BaseModel):
    """批准链单步。"""
    role: str = Field(..., description="复核角色，如 risk_manager / cio")
    approver: str = Field(..., description="该步骤指定的复核人")


class CreateDraftRequest(BaseModel):
    """创建策略草稿。"""
    strategy_name: str
    params: dict
    risk_config: dict
    required_approvers: List[ApproverStep]
    description: Optional[str] = None
    change_note: Optional[str] = None
    created_by: Optional[str] = None
    parent_version_id: Optional[int] = None


class UpdateDraftRequest(BaseModel):
    """修改草稿（提交复核后不可改）。"""
    params: Optional[dict] = None
    risk_config: Optional[dict] = None
    required_approvers: Optional[List[ApproverStep]] = None
    description: Optional[str] = None
    change_note: Optional[str] = None


class ActorRequest(BaseModel):
    """操作人。"""
    actor: Optional[str] = None


class ReviewRequest(BaseModel):
    """登记一步复核决定。"""
    approver: str
    decision: str = Field(..., description="approved 或 rejected")
    comment: Optional[str] = None


class PublishRequest(BaseModel):
    """发布；scheduled_for 给出时为定时切换。"""
    actor: Optional[str] = None
    scheduled_for: Optional[str] = None
    note: Optional[str] = None


class CancelScheduledRequest(BaseModel):
    """取消待生效的定时发布。"""
    actor: Optional[str] = None
    note: Optional[str] = None


class ActionRequest(BaseModel):
    """回滚/撤回请求。"""
    actor: Optional[str] = None
    note: Optional[str] = None


class StartRunRequest(BaseModel):
    """启动自动交易任务，固定策略与风险设置。"""
    strategy_name: str
    run_id: Optional[str] = None
    actor: Optional[str] = None


# ---------------------------------------------------------------------------
# 草稿
# ---------------------------------------------------------------------------

@router.post("/drafts", status_code=201)
async def create_draft(request: CreateDraftRequest):
    """创建策略草稿。"""
    return strategy_service.create_draft(
        strategy_name=request.strategy_name,
        params=request.params,
        risk_config=request.risk_config,
        required_approvers=[s.model_dump() for s in request.required_approvers],
        description=request.description,
        change_note=request.change_note,
        created_by=request.created_by,
        parent_version_id=request.parent_version_id,
    )


@router.patch("/versions/{version_id}")
async def update_draft(version_id: int, request: UpdateDraftRequest):
    """修改草稿内容（仅 draft 状态）。"""
    fields = {k: v for k, v in request.model_dump().items() if v is not None}
    if "required_approvers" in fields:
        fields["required_approvers"] = [
            s.model_dump() for s in request.required_approvers
        ]
    return strategy_service.update_draft(version_id, **fields)


@router.delete("/versions/{version_id}")
async def delete_draft(version_id: int):
    """删除草稿。"""
    strategy_service.delete_draft(version_id)
    return {"success": True, "message": "草稿已删除"}


# ---------------------------------------------------------------------------
# 复核链
# ---------------------------------------------------------------------------

@router.post("/versions/{version_id}/submit")
async def submit_for_review(version_id: int, request: ActorRequest):
    """提交复核，提交后版本内容冻结。"""
    return strategy_service.submit_for_review(version_id, request.actor)


@router.post("/versions/{version_id}/reviews")
async def record_review(version_id: int, request: ReviewRequest):
    """登记批准链上的一步复核决定（必须按链上顺序、由指定复核人操作）。"""
    return strategy_service.record_review(
        version_id=version_id,
        approver=request.approver,
        decision=request.decision,
        comment=request.comment,
    )


@router.post("/versions/{version_id}/withdraw")
async def withdraw_from_review(version_id: int, request: ActorRequest):
    """撤回复核中的版本。"""
    actor = request.actor or "unknown"
    return strategy_service.withdraw_from_review(version_id, actor)


# ---------------------------------------------------------------------------
# 发布 / 定时 / 回滚 / 撤回
# ---------------------------------------------------------------------------

@router.post("/versions/{version_id}/publish")
async def publish_version(version_id: int, request: PublishRequest):
    """发布已通过完整批准链的版本，可指定定时切换。"""
    return strategy_service.publish(
        version_id=version_id,
        actor=request.actor,
        scheduled_for=request.scheduled_for,
        note=request.note,
    )


@router.post("/publications/{publication_id}/cancel")
async def cancel_scheduled(publication_id: int, request: CancelScheduledRequest):
    """取消待生效的定时发布。"""
    return strategy_service.cancel_scheduled(
        publication_id, actor=request.actor, note=request.note
    )


@router.post("/{strategy_name}/rollback")
async def rollback(strategy_name: str, request: ActionRequest):
    """回滚到上一个曾经生效的版本。"""
    return strategy_service.rollback(
        strategy_name, actor=request.actor, note=request.note
    )


@router.post("/{strategy_name}/recall")
async def recall(strategy_name: str, request: ActionRequest):
    """撤回当前生效版本；运行中的任务继续使用各自快照，不受影响。"""
    return strategy_service.recall(
        strategy_name, actor=request.actor, note=request.note
    )


@router.post("/scheduled/activate")
async def activate_due_scheduled():
    """手动触发到期定时发布的切换（定时器与启动恢复会自动调用）。"""
    activated = strategy_service.activate_due_scheduled()
    return {"activated_publication_ids": activated, "count": len(activated)}


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------

@router.get("/versions")
async def list_versions(
    strategy_name: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    """列出策略版本。"""
    return {
        "versions": strategy_service.list_versions(strategy_name, status)
    }


@router.get("/versions/{version_id}")
async def get_version(version_id: int):
    """查看版本详情（含批准链）。"""
    return strategy_service.get_version(version_id)


@router.get("/publications")
async def list_publications(
    strategy_name: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    """查看发布事件流。"""
    return {
        "publications": strategy_service.list_publications(
            strategy_name, status
        )
    }


@router.get("/{strategy_name}/effective")
async def get_effective(strategy_name: str):
    """查看当前生效版本。"""
    data = strategy_service.get_effective(strategy_name)
    if data is None:
        return {"strategy_name": strategy_name, "effective": None}
    return {"strategy_name": strategy_name, "effective": True, **data}


# ---------------------------------------------------------------------------
# 运行快照
# ---------------------------------------------------------------------------

@router.post("/runs/start")
async def start_run(request: StartRunRequest):
    """启动任务并固定策略与风险设置（交易侧入口见 /api/trading/runs/start）。"""
    return strategy_service.start_run(
        request.strategy_name, run_id=request.run_id, actor=request.actor
    )


@router.post("/runs/{run_id}/stop")
async def stop_run(run_id: str):
    """结束任务；快照作为不可变历史保留。"""
    return strategy_service.stop_run(run_id)


@router.get("/runs")
async def list_runs(
    strategy_name: Optional[str] = Query(default=None),
    version_id: Optional[int] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    """列出运行快照。"""
    return {
        "runs": strategy_service.list_runs(strategy_name, version_id, status)
    }


@router.get("/runs/{run_id}")
async def get_run(run_id: str):
    """查看某次运行的固定快照与批准依据。"""
    return strategy_service.get_snapshot(run_id)


@router.get("/runs/{run_id}/signals")
async def list_run_signals(run_id: str):
    """查看某次运行产生的信号留痕。"""
    return {"signals": strategy_service.list_run_signals(run_id)}


@router.get("/runs/{run_id}/orders")
async def list_run_orders(run_id: str):
    """查看某次运行产生的订单留痕。"""
    return {"orders": strategy_service.list_run_orders(run_id)}


@router.get("/orders/{order_id}/trace")
async def trace_order(order_id: str):
    """从订单还原当时使用的策略版本、运行快照与完整批准依据。"""
    return strategy_service.trace_order(order_id)
