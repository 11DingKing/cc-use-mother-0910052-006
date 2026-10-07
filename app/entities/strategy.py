"""策略版本、批准链、发布事件、运行快照与成交留痕的实体定义。

生命周期状态说明：
- StrategyVersion: draft -> in_review -> approved -> published
                  （in_review 可被拒绝 rejected / 撤回 withdrawn，均为终态）
- StrategyApproval: 每个版本批准链上的一步决定，只增不改
- StrategyPublication: 发布事件流（publish/rollback/recall；scheduled/effective/...），
  每个策略任意时刻至多一条 effective 事件（部分唯一索引保证）
- StrategyRunSnapshot: 任务启动时的不可变快照，固定策略参数与风险设置
- StrategySignalRecord / StrategyOrderRecord: 信号与订单留痕，只增不改
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# 版本状态
VERSION_DRAFT = "draft"
VERSION_IN_REVIEW = "in_review"
VERSION_APPROVED = "approved"
VERSION_REJECTED = "rejected"
VERSION_WITHDRAWN = "withdrawn"
VERSION_PUBLISHED = "published"

# 批准决定
DECISION_APPROVED = "approved"
DECISION_REJECTED = "rejected"

# 发布动作
ACTION_PUBLISH = "publish"
ACTION_ROLLBACK = "rollback"
ACTION_RECALL = "recall"

# 发布事件状态
PUB_SCHEDULED = "scheduled"
PUB_EFFECTIVE = "effective"
PUB_SUPERSEDED = "superseded"
PUB_CANCELLED = "cancelled"
PUB_RECALLED = "recalled"

# 运行状态
RUN_RUNNING = "running"
RUN_STOPPED = "stopped"


class StrategyVersion(Base):
    """策略版本。提交复核后内容即冻结，不可再修改。"""

    __tablename__ = "strategy_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String(100), nullable=False)
    version_no = Column(Integer, nullable=False)
    description = Column(Text, nullable=True)
    change_note = Column(Text, nullable=True)

    params_json = Column(Text, nullable=False)
    risk_config_json = Column(Text, nullable=False)
    required_approvers_json = Column(Text, nullable=False, default="[]")
    content_hash = Column(String(64), nullable=False)

    status = Column(String(20), nullable=False, default=VERSION_DRAFT, index=True)
    created_by = Column(String(100), nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    submitted_at = Column(DateTime, nullable=True)
    approved_at = Column(DateTime, nullable=True)
    published_at = Column(DateTime, nullable=True)

    parent_version_id = Column(
        Integer, ForeignKey("strategy_versions.id"), nullable=True
    )

    __table_args__ = (
        UniqueConstraint(
            "strategy_name", "version_no", name="ux_strategy_version_no"
        ),
        Index("ix_strategy_versions_name", "strategy_name"),
    )


class StrategyApproval(Base):
    """批准链上的单步复核记录，只增不改。"""

    __tablename__ = "strategy_approvals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version_id = Column(
        Integer, ForeignKey("strategy_versions.id"), nullable=False
    )
    step_no = Column(Integer, nullable=False)
    role = Column(String(50), nullable=False)
    approver = Column(String(100), nullable=False)
    decision = Column(String(20), nullable=False)
    comment = Column(Text, nullable=True)
    # 决定时所复核内容的哈希，事后可证明确实复核了这一版参数
    content_hash = Column(String(64), nullable=False)
    decided_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("version_id", "step_no", name="ux_approval_version_step"),
        Index("ix_approval_version", "version_id"),
    )


class StrategyPublication(Base):
    """发布事件流：publish / rollback / recall，全部追加。"""

    __tablename__ = "strategy_publications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String(100), nullable=False)
    version_id = Column(
        Integer, ForeignKey("strategy_versions.id"), nullable=True
    )
    action = Column(String(20), nullable=False)
    status = Column(String(20), nullable=False, index=True)

    scheduled_for = Column(DateTime, nullable=True)
    effective_at = Column(DateTime, nullable=True)
    actor = Column(String(100), nullable=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_publication_name_status", "strategy_name", "status"),
        Index("ix_publication_scheduled", "scheduled_for"),
        # 每个策略任意时刻至多一条生效中的发布，跨进程并发也由数据库兜底
        Index(
            "ux_publication_effective",
            "strategy_name",
            unique=True,
            sqlite_where=text("status = 'effective'"),
        ),
    )


class StrategyRunSnapshot(Base):
    """任务启动快照：固定当时的策略参数、风险设置与批准依据，不可变。"""

    __tablename__ = "strategy_run_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(64), nullable=False, unique=True, index=True)
    strategy_name = Column(String(100), nullable=False)
    version_id = Column(
        Integer, ForeignKey("strategy_versions.id"), nullable=False
    )
    version_no = Column(Integer, nullable=False)
    publication_id = Column(
        Integer, ForeignKey("strategy_publications.id"), nullable=False
    )

    params_json = Column(Text, nullable=False)
    risk_config_json = Column(Text, nullable=False)
    content_hash = Column(String(64), nullable=False)
    approval_evidence_json = Column(Text, nullable=False)

    status = Column(String(20), nullable=False, default=RUN_RUNNING, index=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    ended_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_run_snapshot_strategy", "strategy_name"),
        Index("ix_run_snapshot_version", "version_id"),
    )


class StrategySignalRecord(Base):
    """信号留痕：信号触发时即固定到运行快照与版本，只增不改。"""

    __tablename__ = "strategy_signal_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(64), nullable=False, index=True)
    version_id = Column(Integer, nullable=False)
    content_hash = Column(String(64), nullable=False)

    stock_code = Column(String(20), nullable=False)
    signal_type = Column(String(30), nullable=False)
    signal_strength = Column(String(40), nullable=False)
    price = Column(String(40), nullable=True)
    resulting_order_id = Column(String(64), nullable=True, index=True)
    outcome = Column(String(30), nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class StrategyOrderRecord(Base):
    """订单留痕：订单一经提交即追加记录，不随后续状态变化而篡改。"""

    __tablename__ = "strategy_order_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(64), nullable=False, unique=True, index=True)
    run_id = Column(String(64), nullable=False, index=True)
    snapshot_id = Column(Integer, nullable=False)
    version_id = Column(Integer, nullable=False)
    version_no = Column(Integer, nullable=False)
    content_hash = Column(String(64), nullable=False)

    stock_code = Column(String(20), nullable=False)
    side = Column(String(10), nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(String(40), nullable=True)
    signal_type = Column(String(30), nullable=True)
    signal_strength = Column(String(40), nullable=True)
    status_at_record = Column(String(20), nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
