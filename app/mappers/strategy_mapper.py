"""策略生命周期的数据访问层。所有写操作均要求调用方持有写事务。"""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import asc, desc
from sqlalchemy.orm import Session

from app.entities.strategy import (
    PUB_EFFECTIVE,
    PUB_SCHEDULED,
    VERSION_DRAFT,
    StrategyApproval,
    StrategyOrderRecord,
    StrategyPublication,
    StrategyRunSnapshot,
    StrategySignalRecord,
    StrategyVersion,
)


class StrategyMapper:
    """策略版本/批准/发布/快照/留痕的读写封装。"""

    def __init__(self, session: Session):
        self.session = session

    # ------------------------------------------------------------------
    # 版本
    # ------------------------------------------------------------------

    def next_version_no(self, strategy_name: str) -> int:
        """取该策略下一个版本号；行锁由 BEGIN IMMEDIATE 事务保证。"""
        current = (
            self.session.query(StrategyVersion.version_no)
            .filter(StrategyVersion.strategy_name == strategy_name)
            .order_by(desc(StrategyVersion.version_no))
            .first()
        )
        return (current[0] + 1) if current else 1

    def add_version(self, version: StrategyVersion) -> StrategyVersion:
        self.session.add(version)
        self.session.flush()
        return version

    def get_version(self, version_id: int) -> Optional[StrategyVersion]:
        return self.session.get(StrategyVersion, version_id)

    def get_version_by_name_no(
        self, strategy_name: str, version_no: int
    ) -> Optional[StrategyVersion]:
        return (
            self.session.query(StrategyVersion)
            .filter(
                StrategyVersion.strategy_name == strategy_name,
                StrategyVersion.version_no == version_no,
            )
            .first()
        )

    def list_versions(
        self,
        strategy_name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[StrategyVersion]:
        query = self.session.query(StrategyVersion)
        if strategy_name:
            query = query.filter(StrategyVersion.strategy_name == strategy_name)
        if status:
            query = query.filter(StrategyVersion.status == status)
        return query.order_by(
            asc(StrategyVersion.strategy_name),
            asc(StrategyVersion.version_no),
        ).all()

    def get_draft(self, strategy_name: str) -> Optional[StrategyVersion]:
        return (
            self.session.query(StrategyVersion)
            .filter(
                StrategyVersion.strategy_name == strategy_name,
                StrategyVersion.status == VERSION_DRAFT,
            )
            .order_by(desc(StrategyVersion.version_no))
            .first()
        )

    # ------------------------------------------------------------------
    # 批准链
    # ------------------------------------------------------------------

    def add_approval(self, approval: StrategyApproval) -> StrategyApproval:
        self.session.add(approval)
        self.session.flush()
        return approval

    def get_approval(
        self, version_id: int, step_no: int
    ) -> Optional[StrategyApproval]:
        return (
            self.session.query(StrategyApproval)
            .filter(
                StrategyApproval.version_id == version_id,
                StrategyApproval.step_no == step_no,
            )
            .first()
        )

    def list_approvals(self, version_id: int) -> List[StrategyApproval]:
        return (
            self.session.query(StrategyApproval)
            .filter(StrategyApproval.version_id == version_id)
            .order_by(asc(StrategyApproval.step_no))
            .all()
        )

    def latest_approval_step(self, version_id: int) -> int:
        row = (
            self.session.query(StrategyApproval.step_no)
            .filter(StrategyApproval.version_id == version_id)
            .order_by(desc(StrategyApproval.step_no))
            .first()
        )
        return row[0] if row else 0

    # ------------------------------------------------------------------
    # 发布事件
    # ------------------------------------------------------------------

    def add_publication(
        self, publication: StrategyPublication
    ) -> StrategyPublication:
        self.session.add(publication)
        self.session.flush()
        return publication

    def get_publication(self, publication_id: int) -> Optional[StrategyPublication]:
        return self.session.get(StrategyPublication, publication_id)

    def get_effective_publication(
        self, strategy_name: str
    ) -> Optional[StrategyPublication]:
        return (
            self.session.query(StrategyPublication)
            .filter(
                StrategyPublication.strategy_name == strategy_name,
                StrategyPublication.status == PUB_EFFECTIVE,
            )
            .first()
        )

    def list_publications(
        self,
        strategy_name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[StrategyPublication]:
        query = self.session.query(StrategyPublication)
        if strategy_name:
            query = query.filter(
                StrategyPublication.strategy_name == strategy_name
            )
        if status:
            query = query.filter(StrategyPublication.status == status)
        return query.order_by(asc(StrategyPublication.id)).all()

    def list_due_scheduled(self, now: datetime) -> List[StrategyPublication]:
        """到期且仍未取消的定时发布（按创建顺序确定生效顺序）。"""
        return (
            self.session.query(StrategyPublication)
            .filter(
                StrategyPublication.status == PUB_SCHEDULED,
                StrategyPublication.scheduled_for <= now,
            )
            .order_by(asc(StrategyPublication.scheduled_for), asc(StrategyPublication.id))
            .all()
        )

    # ------------------------------------------------------------------
    # 运行快照
    # ------------------------------------------------------------------

    def add_snapshot(self, snapshot: StrategyRunSnapshot) -> StrategyRunSnapshot:
        self.session.add(snapshot)
        self.session.flush()
        return snapshot

    def get_snapshot_by_run(self, run_id: str) -> Optional[StrategyRunSnapshot]:
        return (
            self.session.query(StrategyRunSnapshot)
            .filter(StrategyRunSnapshot.run_id == run_id)
            .first()
        )

    def get_snapshot(self, snapshot_id: int) -> Optional[StrategyRunSnapshot]:
        return self.session.get(StrategyRunSnapshot, snapshot_id)

    def list_snapshots(
        self,
        strategy_name: Optional[str] = None,
        version_id: Optional[int] = None,
        status: Optional[str] = None,
    ) -> List[StrategyRunSnapshot]:
        query = self.session.query(StrategyRunSnapshot)
        if strategy_name:
            query = query.filter(
                StrategyRunSnapshot.strategy_name == strategy_name
            )
        if version_id is not None:
            query = query.filter(StrategyRunSnapshot.version_id == version_id)
        if status:
            query = query.filter(StrategyRunSnapshot.status == status)
        return query.order_by(desc(StrategyRunSnapshot.id)).all()

    # ------------------------------------------------------------------
    # 信号 / 订单留痕
    # ------------------------------------------------------------------

    def add_signal_record(
        self, record: StrategySignalRecord
    ) -> StrategySignalRecord:
        self.session.add(record)
        self.session.flush()
        return record

    def add_order_record(
        self, record: StrategyOrderRecord
    ) -> StrategyOrderRecord:
        self.session.add(record)
        self.session.flush()
        return record

    def get_order_record(self, order_id: str) -> Optional[StrategyOrderRecord]:
        return (
            self.session.query(StrategyOrderRecord)
            .filter(StrategyOrderRecord.order_id == order_id)
            .first()
        )

    def get_signal_record_by_order(
        self, order_id: str
    ) -> Optional[StrategySignalRecord]:
        return (
            self.session.query(StrategySignalRecord)
            .filter(StrategySignalRecord.resulting_order_id == order_id)
            .first()
        )

    def list_signal_records(self, run_id: str) -> List[StrategySignalRecord]:
        return (
            self.session.query(StrategySignalRecord)
            .filter(StrategySignalRecord.run_id == run_id)
            .order_by(asc(StrategySignalRecord.id))
            .all()
        )

    def list_order_records_by_run(self, run_id: str) -> List[StrategyOrderRecord]:
        return (
            self.session.query(StrategyOrderRecord)
            .filter(StrategyOrderRecord.run_id == run_id)
            .order_by(asc(StrategyOrderRecord.id))
            .all()
        )
