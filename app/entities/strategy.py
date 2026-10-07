"""策略版本生命周期实体：版本、批准链、发布事件、运行快照、信号与订单审计。

所有表只追加业务事实：
- 版本内容（params/risk/hash）一经提交复核即不可变；
- 运行快照一经创建不可变，仅允许状态机字段（status/heartbeat/ended_at）变化；
- 信号记录与订单审计为仅追加表，任何接口都不会回写已完成记录。
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# ---------------------------------------------------------------------------
# 状态常量（单一事实来源，服务层与接口层共用）
# ---------------------------------------------------------------------------

class VersionStatus:
    DRAFT = "draft"                # 草稿，可修改
    IN_REVIEW = "in_review"        # 复核中
    APPROVED = "approved"          # 批准链完成，待发布
    PUBLISHED = "published"        # 当前线上版本
    SUPERSEDED = "superseded"      # 曾发布，已被其他版本替代
    WITHDRAWN = "withdrawn"        # 已被撤回
    REJECTED = "rejected"          # 复核驳回（终态）


class ApprovalDecision:
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class PublishAction:
    PUBLISH = "publish"
    WITHDRAW = "withdraw"
    ROLLBACK = "rollback"


class PublishEventStatus:
    SCHEDULED = "scheduled"        # 定时事件，尚未生效
    ACTIVE = "active"              # 当前生效的时间线节点
    SUPERSEDED = "superseded"      # 已被后续节点替代
    CANCELLED = "cancelled"        # 定时事件生效前被取消


class RunStatus:
    RUNNING = "running"
    COMPLETED = "completed"
    STOPPED = "stopped"
    CRASHED = "crashed"


def _loads(value: Optional[str], default: Any) -> Any:
    if value is None:
        return default
    return json.loads(value)


class StrategyVersion(Base):
    """策略版本：同一 strategy_key 下版本号单调递增，内容以 canonical hash 固化。"""

    __tablename__ = "strategy_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_key = Column(String(100), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False, default=VersionStatus.DRAFT, index=True)

    # 策略参数与风险参数（JSON 原样保存，hash 保证可比对）
    params_json = Column(Text, nullable=False)
    risk_params_json = Column(Text, nullable=False)
    required_approvers_json = Column(Text, nullable=True)
    content_hash = Column(String(64), nullable=False, index=True)

    created_by = Column(String(100), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    submitted_at = Column(DateTime, nullable=True)
    decided_at = Column(DateTime, nullable=True)
    published_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("strategy_key", "version", name="uix_strategy_key_version"),
        Index("ix_strategy_versions_key_status", "strategy_key", "status"),
    )

    def __repr__(self):
        return f"<StrategyVersion({self.strategy_key} v{self.version} {self.status})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "strategy_key": self.strategy_key,
            "version": self.version,
            "status": self.status,
            "params": _loads(self.params_json, {}),
            "risk_params": _loads(self.risk_params_json, {}),
            "required_approvers": _loads(self.required_approvers_json, []),
            "content_hash": self.content_hash,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "decided_at": self.decided_at.isoformat() if self.decided_at else None,
            "published_at": self.published_at.isoformat() if self.published_at else None,
        }


class StrategyApproval(Base):
    """批准链节点：按 step 顺序逐个批准，任一驳回则版本驳回。"""

    __tablename__ = "strategy_approvals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version_id = Column(Integer, nullable=False, index=True)
    step = Column(Integer, nullable=False)
    reviewer = Column(String(100), nullable=False)
    decision = Column(String(20), nullable=False, default=ApprovalDecision.PENDING)
    comment = Column(Text, nullable=True)
    actor = Column(String(100), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    decided_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("version_id", "reviewer", name="uix_approval_version_reviewer"),
        Index("ix_approval_version_step", "version_id", "step"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "version_id": self.version_id,
            "step": self.step,
            "reviewer": self.reviewer,
            "decision": self.decision,
            "comment": self.comment,
            "actor": self.actor,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "decided_at": self.decided_at.isoformat() if self.decided_at else None,
        }


class StrategyPublishEvent(Base):
    """发布时间线节点：发布/撤回/回滚均以不可变事件表达，按 (effective_at, id) 定序。"""

    __tablename__ = "strategy_publish_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_key = Column(String(100), nullable=False, index=True)
    action = Column(String(20), nullable=False)
    version_id = Column(Integer, nullable=True)
    content_hash = Column(String(64), nullable=True)

    effective_at = Column(DateTime, nullable=False)
    activated_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default=PublishEventStatus.SCHEDULED, index=True)
    reason = Column(Text, nullable=True)
    created_by = Column(String(100), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_publish_key_effective", "strategy_key", "effective_at"),
        Index("ix_publish_key_status", "strategy_key", "status"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "strategy_key": self.strategy_key,
            "action": self.action,
            "version_id": self.version_id,
            "content_hash": self.content_hash,
            "effective_at": self.effective_at.isoformat() if self.effective_at else None,
            "activated_at": self.activated_at.isoformat() if self.activated_at else None,
            "status": self.status,
            "reason": self.reason,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class StrategyRunSnapshot(Base):
    """运行快照：任务启动时冻结策略参数、风险参数与批准依据，生命周期内不可变。"""

    __tablename__ = "strategy_run_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(100), nullable=False, unique=True, index=True)
    strategy_key = Column(String(100), nullable=False, index=True)
    version_id = Column(Integer, nullable=False)
    version_number = Column(Integer, nullable=False)
    publish_event_id = Column(Integer, nullable=False)

    params_json = Column(Text, nullable=False)
    risk_params_json = Column(Text, nullable=False)
    content_hash = Column(String(64), nullable=False, index=True)
    approval_basis_json = Column(Text, nullable=False)

    status = Column(String(20), nullable=False, default=RunStatus.RUNNING, index=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    heartbeat_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "run_id": self.run_id,
            "strategy_key": self.strategy_key,
            "version_id": self.version_id,
            "version_number": self.version_number,
            "publish_event_id": self.publish_event_id,
            "params": _loads(self.params_json, {}),
            "risk_params": _loads(self.risk_params_json, {}),
            "content_hash": self.content_hash,
            "approval_basis": _loads(self.approval_basis_json, []),
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "heartbeat_at": self.heartbeat_at.isoformat() if self.heartbeat_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
        }


class StrategySignalRecord(Base):
    """信号留痕：仅追加，携带运行时冻结的版本与内容指纹。"""

    __tablename__ = "strategy_signal_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(String(100), nullable=False, unique=True, index=True)
    run_id = Column(String(100), nullable=False, index=True)
    strategy_key = Column(String(100), nullable=False)
    version_id = Column(Integer, nullable=False)
    content_hash = Column(String(64), nullable=False)

    stock_code = Column(String(20), nullable=False, index=True)
    signal_type = Column(String(50), nullable=False)
    signal_strength = Column(Float, nullable=False, default=0.0)
    price = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_signal_run_created", "run_id", "created_at"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "signal_id": self.signal_id,
            "run_id": self.run_id,
            "strategy_key": self.strategy_key,
            "version_id": self.version_id,
            "content_hash": self.content_hash,
            "stock_code": self.stock_code,
            "signal_type": self.signal_type,
            "signal_strength": self.signal_strength,
            "price": self.price,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class StrategyOrderAudit(Base):
    """订单 provenance：订单提交时写入一次，之后永不更新。"""

    __tablename__ = "strategy_order_audit"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(100), nullable=False, unique=True, index=True)
    signal_id = Column(String(100), nullable=True, index=True)
    run_id = Column(String(100), nullable=False, index=True)
    strategy_key = Column(String(100), nullable=False)
    version_id = Column(Integer, nullable=False)
    version_number = Column(Integer, nullable=False)
    content_hash = Column(String(64), nullable=False)

    stock_code = Column(String(20), nullable=False)
    side = Column(String(10), nullable=False)
    order_type = Column(String(20), nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=True)
    submitted_status = Column(String(20), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_order_audit_run_created", "run_id", "created_at"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "order_id": self.order_id,
            "signal_id": self.signal_id,
            "run_id": self.run_id,
            "strategy_key": self.strategy_key,
            "version_id": self.version_id,
            "version_number": self.version_number,
            "content_hash": self.content_hash,
            "stock_code": self.stock_code,
            "side": self.side,
            "order_type": self.order_type,
            "quantity": self.quantity,
            "price": self.price,
            "submitted_status": self.submitted_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
