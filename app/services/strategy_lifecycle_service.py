"""策略版本生命周期服务。

覆盖：草稿 -> 复核（有序批准链）-> 发布/定时发布 -> 撤回 -> 回滚，
以及任务启动快照、异常重启恢复、信号与订单的批准依据追溯。

确定性保证：
- 所有写操作在进程级 RLock 下串行化（单容器部署），并在事务内重新校验状态；
- 发布时间线按 (effective_at, id) 定序，任何时刻的“当前版本”是时间线折叠的
  纯函数，历史查询不随后续发布改变；
- 版本号按 (strategy_key, max(version)+1) 分配，数据库唯一约束兜底；
- 快照/信号/订单审计一经写入不再更新，查询只做只读拼接。
"""

import hashlib
import json
import logging
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import asc
from sqlalchemy.orm import Session

from app.config import db_session_scope
from app.entities.strategy import (
    ApprovalDecision,
    PublishAction,
    PublishEventStatus,
    RunStatus,
    StrategyApproval,
    StrategyOrderAudit,
    StrategyPublishEvent,
    StrategyRunSnapshot,
    StrategySignalRecord,
    StrategyVersion,
    VersionStatus,
)
from app.middleware.exception_handler import AppException

logger = logging.getLogger(__name__)

_SET_ACTIONS = (PublishAction.PUBLISH, PublishAction.ROLLBACK)


class StrategyLifecycleException(AppException):
    """策略生命周期相关错误。"""

    def __init__(self, message: str, status_code: int = 400, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            code="STRATEGY_ERROR",
            status_code=status_code,
            details=details or {},
        )


def canonical_content_hash(params: Dict[str, Any], risk_params: Dict[str, Any]) -> str:
    """对策略参数与风险参数计算稳定的内容指纹。"""
    payload = json.dumps(
        {"params": params, "risk_params": risk_params},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class StrategyLifecycleService:
    """策略版本与运行快照的编排服务。"""

    # 进程级全局锁：不同服务实例（管理接口、交易服务）共享同一数据库，
    # 必须共用同一把锁才能保证“读-判-写”跨实例串行。
    _global_lock = threading.RLock()
    _singleton: "Optional[StrategyLifecycleService]" = None

    @classmethod
    def get_instance(cls) -> "StrategyLifecycleService":
        """进程内单例，供管理接口与交易服务共享（同一把锁、同一时钟）。"""
        if cls._singleton is None:
            with cls._global_lock:
                if cls._singleton is None:
                    cls._singleton = cls()
        return cls._singleton

    def __init__(self, clock: Optional[Callable[[], datetime]] = None):
        self._clock = clock or datetime.utcnow
        self._lock = self._global_lock

    def _now(self) -> datetime:
        return self._naive(self._clock())

    @staticmethod
    def _naive(value: Optional[datetime]) -> Optional[datetime]:
        """统一使用 naive UTC 存储；带时区的输入先换算再剥离。"""
        if value is None:
            return None
        if value.tzinfo is not None:
            value = value.astimezone(tz=None).replace(tzinfo=None)
        return value

    # ------------------------------------------------------------------
    # 草稿
    # ------------------------------------------------------------------

    def create_draft(
        self,
        strategy_key: str,
        params: Dict[str, Any],
        risk_params: Dict[str, Any],
        created_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._validate_key(strategy_key)
        self._validate_params(params, risk_params)
        content_hash = canonical_content_hash(params, risk_params)

        with self._lock, db_session_scope() as session:
            version_number = self._next_version_number(session, strategy_key)
            version = StrategyVersion(
                strategy_key=strategy_key,
                version=version_number,
                status=VersionStatus.DRAFT,
                params_json=json.dumps(params, ensure_ascii=False, default=str),
                risk_params_json=json.dumps(risk_params, ensure_ascii=False, default=str),
                content_hash=content_hash,
                created_by=created_by,
            )
            session.add(version)
            session.flush()
            return version.to_dict()

    def update_draft(
        self,
        version_id: int,
        params: Optional[Dict[str, Any]] = None,
        risk_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        with self._lock, db_session_scope() as session:
            version = self._get_version(session, version_id)
            if version.status != VersionStatus.DRAFT:
                raise StrategyLifecycleException(
                    f"版本当前状态为 {version.status}，只有草稿可以修改",
                    status_code=409,
                    details={"version_id": version_id, "status": version.status},
                )

            new_params = json.loads(version.params_json) if params is None else params
            new_risk = json.loads(version.risk_params_json) if risk_params is None else risk_params
            self._validate_params(new_params, new_risk)

            version.params_json = json.dumps(new_params, ensure_ascii=False, default=str)
            version.risk_params_json = json.dumps(new_risk, ensure_ascii=False, default=str)
            version.content_hash = canonical_content_hash(new_params, new_risk)
            session.flush()
            return version.to_dict()

    def list_versions(self, strategy_key: str) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            versions = (
                session.query(StrategyVersion)
                .filter(StrategyVersion.strategy_key == strategy_key)
                .order_by(asc(StrategyVersion.version))
                .all()
            )
            return [v.to_dict() for v in versions]

    def get_version(self, version_id: int) -> Dict[str, Any]:
        with db_session_scope() as session:
            return self._get_version(session, version_id).to_dict()

    # ------------------------------------------------------------------
    # 复核与批准链
    # ------------------------------------------------------------------

    def submit_for_review(
        self,
        version_id: int,
        reviewers: List[str],
        actor: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not reviewers or any(not isinstance(r, str) or not r.strip() for r in reviewers):
            raise StrategyLifecycleException("复核人列表不能为空，且必须是非空字符串")
        if len(set(reviewers)) != len(reviewers):
            raise StrategyLifecycleException("复核人列表中存在重复成员")
        reviewers = [r.strip() for r in reviewers]

        with self._lock, db_session_scope() as session:
            version = self._get_version(session, version_id)
            if version.status != VersionStatus.DRAFT:
                raise StrategyLifecycleException(
                    f"版本当前状态为 {version.status}，只有草稿可以提交复核",
                    status_code=409,
                    details={"version_id": version_id, "status": version.status},
                )

            now = self._now()
            version.status = VersionStatus.IN_REVIEW
            version.submitted_at = now
            version.required_approvers_json = json.dumps(reviewers, ensure_ascii=False)
            for step, reviewer in enumerate(reviewers):
                session.add(
                    StrategyApproval(
                        version_id=version_id,
                        step=step,
                        reviewer=reviewer,
                        decision=ApprovalDecision.PENDING,
                        actor=actor,
                    )
                )
            session.flush()
            return version.to_dict()

    def record_decision(
        self,
        version_id: int,
        reviewer: str,
        decision: str,
        actor: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        if decision not in (ApprovalDecision.APPROVED, ApprovalDecision.REJECTED):
            raise StrategyLifecycleException("decision 只能是 approved 或 rejected")

        with self._lock, db_session_scope() as session:
            version = self._get_version(session, version_id)
            if version.status != VersionStatus.IN_REVIEW:
                raise StrategyLifecycleException(
                    f"版本当前状态为 {version.status}，无法记录复核决定",
                    status_code=409,
                    details={"version_id": version_id, "status": version.status},
                )

            approvals = self._approvals_ordered(session, version_id)
            current = next(
                (a for a in approvals if a.decision == ApprovalDecision.PENDING),
                None,
            )
            if current is None:
                raise StrategyLifecycleException("批准链不存在待处理节点", status_code=409)
            if current.reviewer != reviewer:
                raise StrategyLifecycleException(
                    f"当前轮到 {current.reviewer} 复核（第 {current.step + 1} 级），"
                    f"{reviewer} 不得越级批准",
                    status_code=409,
                    details={"expected_reviewer": current.reviewer, "actual_reviewer": reviewer},
                )

            now = self._now()
            current.decision = decision
            current.comment = comment
            current.actor = actor or reviewer
            current.decided_at = now

            if decision == ApprovalDecision.REJECTED:
                version.status = VersionStatus.REJECTED
                version.decided_at = now
            elif all(a.decision == ApprovalDecision.APPROVED for a in approvals):
                version.status = VersionStatus.APPROVED
                version.decided_at = now

            session.flush()
            return version.to_dict()

    def list_approvals(self, version_id: int) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            self._get_version(session, version_id)
            return [a.to_dict() for a in self._approvals_ordered(session, version_id)]

    # ------------------------------------------------------------------
    # 发布时间线：发布 / 撤回 / 回滚 / 定时
    # ------------------------------------------------------------------

    def _resolve_effective(self, now: datetime, effective_at: Optional[datetime]) -> datetime:
        """生效时间只允许“现在”或“未来”：禁止回填，保证历史不被改写。"""
        effective_at = self._naive(effective_at) or now
        if effective_at < now:
            raise StrategyLifecycleException(
                "生效时间不能早于当前时间，禁止回填发布事件（历史记录不可变）",
                status_code=422,
                details={"effective_at": effective_at.isoformat(), "now": now.isoformat()},
            )
        return effective_at

    def publish(
        self,
        version_id: int,
        effective_at: Optional[datetime] = None,
        reason: Optional[str] = None,
        actor: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock, db_session_scope() as session:
            now = self._now()
            effective_at = self._resolve_effective(now, effective_at)
            version = self._get_version(session, version_id)
            if version.status not in (
                VersionStatus.APPROVED,
                VersionStatus.PUBLISHED,
                VersionStatus.SUPERSEDED,
                VersionStatus.WITHDRAWN,
            ):
                raise StrategyLifecycleException(
                    f"版本当前状态为 {version.status}，未完成批准链的版本不得发布",
                    status_code=409,
                    details={"version_id": version_id, "status": version.status},
                )
            self._assert_chain_complete(session, version)

            event = self._add_event(
                session,
                strategy_key=version.strategy_key,
                action=PublishAction.PUBLISH,
                version=version,
                effective_at=effective_at,
                reason=reason,
                actor=actor,
            )

            if effective_at <= now:
                self._apply_due_locked(session, version.strategy_key, now)
                session.flush()
                event = session.get(StrategyPublishEvent, event.id)
            return event.to_dict()

    def withdraw(
        self,
        strategy_key: str,
        effective_at: Optional[datetime] = None,
        reason: Optional[str] = None,
        actor: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock, db_session_scope() as session:
            now = self._now()
            effective_at = self._resolve_effective(now, effective_at)
            projected = self._active_node_at(session, strategy_key, effective_at)
            if projected is None or projected.action == PublishAction.WITHDRAW:
                raise StrategyLifecycleException(
                    "该时刻策略没有线上版本（或已是撤回状态），撤回操作无效",
                    status_code=409,
                    details={"strategy_key": strategy_key},
                )
            event = self._add_event(
                session,
                strategy_key=strategy_key,
                action=PublishAction.WITHDRAW,
                version=None,
                effective_at=effective_at,
                reason=reason,
                actor=actor,
            )
            if effective_at <= now:
                self._apply_due_locked(session, strategy_key, now)
                session.flush()
                event = session.get(StrategyPublishEvent, event.id)
            return event.to_dict()

    def rollback(
        self,
        strategy_key: str,
        effective_at: Optional[datetime] = None,
        reason: Optional[str] = None,
        actor: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock, db_session_scope() as session:
            now = self._now()
            effective_at = self._resolve_effective(now, effective_at)
            # 目标版本 = 生效时刻之前最近的一个“设置版本”的节点（时间线确定性折叠）。
            node = self._active_node_at(session, strategy_key, effective_at)
            if node is None or node.action == PublishAction.WITHDRAW:
                raise StrategyLifecycleException(
                    "该时刻没有线上版本（或已撤回），无法回滚",
                    status_code=409,
                    details={"strategy_key": strategy_key},
                )
            target_version = self._rollback_target(
                session, strategy_key, effective_at, current_node=node
            )
            if target_version is None or target_version.id == node.version_id:
                raise StrategyLifecycleException(
                    "没有与当前线上版本不同的历史版本可回滚",
                    status_code=409,
                    details={"strategy_key": strategy_key},
                )

            event = self._add_event(
                session,
                strategy_key=strategy_key,
                action=PublishAction.ROLLBACK,
                version=target_version,
                effective_at=effective_at,
                reason=reason,
                actor=actor,
            )
            if effective_at <= now:
                self._apply_due_locked(session, strategy_key, now)
                session.flush()
                event = session.get(StrategyPublishEvent, event.id)
            return event.to_dict()

    def cancel_scheduled(self, event_id: int) -> Dict[str, Any]:
        with self._lock, db_session_scope() as session:
            event = session.get(StrategyPublishEvent, event_id)
            if event is None:
                raise StrategyLifecycleException(
                    "发布事件不存在", status_code=404, details={"event_id": event_id}
                )
            if event.status != PublishEventStatus.SCHEDULED:
                raise StrategyLifecycleException(
                    f"事件状态为 {event.status}，只有待生效的定时事件可以取消",
                    status_code=409,
                    details={"event_id": event_id, "status": event.status},
                )
            if event.effective_at <= self._now():
                raise StrategyLifecycleException(
                    "事件已到生效时间，无法取消，请改用撤回",
                    status_code=409,
                    details={"event_id": event_id},
                )
            event.status = PublishEventStatus.CANCELLED
            session.flush()
            return event.to_dict()

    def list_scheduled(self, strategy_key: Optional[str] = None) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            q = session.query(StrategyPublishEvent).filter(
                StrategyPublishEvent.status == PublishEventStatus.SCHEDULED
            )
            if strategy_key:
                q = q.filter(StrategyPublishEvent.strategy_key == strategy_key)
            events = q.order_by(
                asc(StrategyPublishEvent.effective_at), asc(StrategyPublishEvent.id)
            ).all()
            return [e.to_dict() for e in events]

    def list_timeline(self, strategy_key: str) -> List[Dict[str, Any]]:
        """返回完整发布时间线，并标注每个节点折叠后的生效版本。"""
        with db_session_scope() as session:
            events = (
                session.query(StrategyPublishEvent)
                .filter(StrategyPublishEvent.strategy_key == strategy_key)
                .order_by(
                    asc(StrategyPublishEvent.effective_at), asc(StrategyPublishEvent.id)
                )
                .all()
            )
            current_version_id: Optional[int] = None
            result = []
            for event in events:
                if event.status == PublishEventStatus.CANCELLED:
                    effective_version_id = current_version_id
                elif event.action in _SET_ACTIONS:
                    current_version_id = event.version_id
                    effective_version_id = current_version_id
                else:  # withdraw
                    current_version_id = None
                    effective_version_id = None
                item = event.to_dict()
                item["effective_version_id"] = effective_version_id
                result.append(item)
            return result

    def get_active_version(
        self, strategy_key: str, at: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """某一时刻的线上版本；该时刻之前无发布或已撤回时返回 None。"""
        with db_session_scope() as session:
            node = self._active_node_at(
                session, strategy_key, self._naive(at) or self._now()
            )
            if node is None or node.version_id is None:
                return None
            return self._get_version(session, node.version_id).to_dict()

    def process_due(self, strategy_key: Optional[str] = None) -> List[Dict[str, Any]]:
        """激活所有到期的定时事件（调度器周期调用，也可由管理接口手动触发）。"""
        now = self._now()
        with self._lock, db_session_scope() as session:
            q = session.query(StrategyPublishEvent).filter(
                StrategyPublishEvent.status == PublishEventStatus.SCHEDULED,
                StrategyPublishEvent.effective_at <= now,
            )
            if strategy_key:
                q = q.filter(StrategyPublishEvent.strategy_key == strategy_key)
            due_ids = [e.id for e in q.all()]
            if not due_ids:
                return []

            keys = [
                row[0]
                for row in session.query(StrategyPublishEvent.strategy_key)
                .filter(StrategyPublishEvent.id.in_(due_ids))
                .distinct()
                .order_by(asc(StrategyPublishEvent.strategy_key))
                .all()
            ]
            for key in keys:
                self._apply_due_locked(session, key, now)

            activated = (
                session.query(StrategyPublishEvent)
                .filter(StrategyPublishEvent.id.in_(due_ids))
                .order_by(asc(StrategyPublishEvent.effective_at), asc(StrategyPublishEvent.id))
                .all()
            )
            # 在会话内完成序列化，返回值不依赖持久化对象。
            return [
                e.to_dict() for e in activated if e.status == PublishEventStatus.ACTIVE
            ]

    # ------------------------------------------------------------------
    # 运行快照
    # ------------------------------------------------------------------

    def start_run(
        self,
        strategy_key: str,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """启动任务：激活到期事件后冻结当前发布版本与风险参数。"""
        with self._lock, db_session_scope() as session:
            now = self._now()
            self._apply_due_locked(session, strategy_key, now)

            run_id = run_id or f"RUN_{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
            existing = (
                session.query(StrategyRunSnapshot)
                .filter(StrategyRunSnapshot.run_id == run_id)
                .one_or_none()
            )
            if existing is not None:
                # 幂等：相同 run_id 永远映射到同一快照，重复启动/重启不产生第二份。
                if existing.status == RunStatus.RUNNING:
                    existing.heartbeat_at = now
                    session.flush()
                    return existing.to_dict()
                raise StrategyLifecycleException(
                    f"运行 {run_id} 已处于 {existing.status} 状态，不能重复启动",
                    status_code=409,
                    details={"run_id": run_id, "status": existing.status},
                )

            node = self._active_node_at(session, strategy_key, now)
            if node is None or node.version_id is None:
                raise StrategyLifecycleException(
                    "策略当前没有已发布且生效的版本，不能启动自动交易任务",
                    status_code=409,
                    details={"strategy_key": strategy_key},
                )
            version = self._get_version(session, node.version_id)
            if version.content_hash != node.content_hash:
                # 纯防御：时间线指纹必须与版本内容一致。
                raise StrategyLifecycleException(
                    "发布事件的内容指纹与版本不一致，拒绝启动",
                    status_code=409,
                )

            approval_basis = [
                {
                    "step": a.step,
                    "reviewer": a.reviewer,
                    "decision": a.decision,
                    "comment": a.comment,
                    "actor": a.actor,
                    "decided_at": a.decided_at.isoformat() if a.decided_at else None,
                }
                for a in self._approvals_ordered(session, version.id)
            ]
            snapshot = StrategyRunSnapshot(
                run_id=run_id,
                strategy_key=strategy_key,
                version_id=version.id,
                version_number=version.version,
                publish_event_id=node.id,
                params_json=version.params_json,
                risk_params_json=version.risk_params_json,
                content_hash=version.content_hash,
                approval_basis_json=json.dumps(approval_basis, ensure_ascii=False, default=str),
                status=RunStatus.RUNNING,
                started_at=now,
                heartbeat_at=now,
            )
            session.add(snapshot)
            session.flush()
            logger.info(
                "策略运行已固定: run_id=%s strategy=%s version=v%s hash=%s",
                run_id, strategy_key, version.version, version.content_hash[:12],
            )
            return snapshot.to_dict()

    def heartbeat(self, run_id: str) -> Dict[str, Any]:
        with self._lock, db_session_scope() as session:
            snapshot = self._get_run(session, run_id)
            if snapshot.status != RunStatus.RUNNING:
                raise StrategyLifecycleException(
                    f"运行状态为 {snapshot.status}，无法上报心跳",
                    status_code=409,
                    details={"run_id": run_id, "status": snapshot.status},
                )
            snapshot.heartbeat_at = self._now()
            session.flush()
            return snapshot.to_dict()

    def stop_run(self, run_id: str, status: str = RunStatus.COMPLETED) -> Dict[str, Any]:
        if status not in (RunStatus.COMPLETED, RunStatus.STOPPED):
            raise StrategyLifecycleException("stop_run 只允许 completed 或 stopped")
        with self._lock, db_session_scope() as session:
            snapshot = self._get_run(session, run_id)
            if snapshot.status != RunStatus.RUNNING:
                raise StrategyLifecycleException(
                    f"运行状态为 {snapshot.status}，无需停止",
                    status_code=409,
                    details={"run_id": run_id, "status": snapshot.status},
                )
            snapshot.status = status
            snapshot.ended_at = self._now()
            session.flush()
            return snapshot.to_dict()

    def recover_stale_runs(self, stale_seconds: int = 60) -> List[Dict[str, Any]]:
        """异常重启恢复：心跳超时的运行标记为 crashed，其快照与审计保持原样可查。"""
        if stale_seconds < 0:
            raise StrategyLifecycleException("stale_seconds 不能为负数")
        cutoff = self._now() - timedelta(seconds=stale_seconds)
        with self._lock, db_session_scope() as session:
            stale = (
                session.query(StrategyRunSnapshot)
                .filter(StrategyRunSnapshot.status == RunStatus.RUNNING)
                .all()
            )
            recovered = []
            for snapshot in stale:
                last_seen = snapshot.heartbeat_at or snapshot.started_at
                if last_seen is not None and last_seen < cutoff:
                    snapshot.status = RunStatus.CRASHED
                    snapshot.ended_at = self._now()
                    recovered.append(snapshot)
            session.flush()
            return [s.to_dict() for s in recovered]

    def get_run(self, run_id: str) -> Dict[str, Any]:
        with db_session_scope() as session:
            return self._get_run(session, run_id).to_dict()

    def list_runs(
        self,
        strategy_key: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            q = session.query(StrategyRunSnapshot)
            if strategy_key:
                q = q.filter(StrategyRunSnapshot.strategy_key == strategy_key)
            if status:
                q = q.filter(StrategyRunSnapshot.status == status)
            return [s.to_dict() for s in q.order_by(asc(StrategyRunSnapshot.id)).all()]

    # ------------------------------------------------------------------
    # 信号与订单留痕
    # ------------------------------------------------------------------

    def record_signal(
        self,
        run_id: str,
        stock_code: str,
        signal_type: str,
        signal_strength: float = 0.0,
        price: Optional[float] = None,
        signal_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock, db_session_scope() as session:
            snapshot = self._get_run(session, run_id)
            if snapshot.status != RunStatus.RUNNING:
                raise StrategyLifecycleException(
                    f"运行状态为 {snapshot.status}，运行结束后不得追加信号",
                    status_code=409,
                    details={"run_id": run_id, "status": snapshot.status},
                )
            signal_id = signal_id or f"SIG_{uuid.uuid4().hex[:12]}"
            if (
                session.query(StrategySignalRecord)
                .filter(StrategySignalRecord.signal_id == signal_id)
                .first()
            ):
                raise StrategyLifecycleException(
                    f"信号 {signal_id} 已存在，信号记录只能追加不能覆盖",
                    status_code=409,
                    details={"signal_id": signal_id},
                )
            record = StrategySignalRecord(
                signal_id=signal_id,
                run_id=run_id,
                strategy_key=snapshot.strategy_key,
                version_id=snapshot.version_id,
                content_hash=snapshot.content_hash,
                stock_code=stock_code,
                signal_type=signal_type,
                signal_strength=signal_strength,
                price=price,
            )
            session.add(record)
            session.flush()
            return record.to_dict()

    def record_order(self, run_id: str, order: Dict[str, Any], signal_id: Optional[str] = None) -> Dict[str, Any]:
        """订单提交后写入一次 provenance；同一 order_id 永不二次写入。"""
        with self._lock, db_session_scope() as session:
            snapshot = self._get_run(session, run_id)
            if snapshot.status != RunStatus.RUNNING:
                raise StrategyLifecycleException(
                    f"运行状态为 {snapshot.status}，运行结束后不得追加订单审计",
                    status_code=409,
                    details={"run_id": run_id, "status": snapshot.status},
                )
            order_id = order.get("order_id")
            if not order_id:
                raise StrategyLifecycleException("订单缺少 order_id，无法留痕")
            existing = (
                session.query(StrategyOrderAudit)
                .filter(StrategyOrderAudit.order_id == order_id)
                .one_or_none()
            )
            if existing is not None:
                raise StrategyLifecycleException(
                    f"订单 {order_id} 的审计记录已存在，禁止改写已完成记录",
                    status_code=409,
                    details={"order_id": order_id},
                )
            if signal_id:
                signal = (
                    session.query(StrategySignalRecord)
                    .filter(StrategySignalRecord.signal_id == signal_id)
                    .one_or_none()
                )
                if signal is None or signal.run_id != run_id:
                    raise StrategyLifecycleException(
                        f"信号 {signal_id} 不属于运行 {run_id}",
                        status_code=404,
                        details={"signal_id": signal_id, "run_id": run_id},
                    )

            audit = StrategyOrderAudit(
                order_id=order_id,
                signal_id=signal_id,
                run_id=run_id,
                strategy_key=snapshot.strategy_key,
                version_id=snapshot.version_id,
                version_number=snapshot.version_number,
                content_hash=snapshot.content_hash,
                stock_code=order["stock_code"],
                side=order["side"],
                order_type=order["order_type"],
                quantity=order["quantity"],
                price=order.get("price"),
                submitted_status=order.get("status", "unknown"),
            )
            session.add(audit)
            session.flush()
            return audit.to_dict()

    def order_provenance(self, order_id: str) -> Dict[str, Any]:
        """从订单还原：订单审计 -> 信号 -> 运行快照 -> 版本 -> 批准链。"""
        with db_session_scope() as session:
            audit = (
                session.query(StrategyOrderAudit)
                .filter(StrategyOrderAudit.order_id == order_id)
                .one_or_none()
            )
            if audit is None:
                raise StrategyLifecycleException(
                    f"订单 {order_id} 没有策略审计记录",
                    status_code=404,
                    details={"order_id": order_id},
                )
            snapshot = (
                session.query(StrategyRunSnapshot)
                .filter(StrategyRunSnapshot.run_id == audit.run_id)
                .one()
            )
            version = self._get_version(session, audit.version_id)
            signal = None
            if audit.signal_id:
                signal = (
                    session.query(StrategySignalRecord)
                    .filter(StrategySignalRecord.signal_id == audit.signal_id)
                    .one_or_none()
                )
            approvals = self._approvals_ordered(session, audit.version_id)
            return {
                "order": audit.to_dict(),
                "signal": signal.to_dict() if signal else None,
                "run_snapshot": snapshot.to_dict(),
                "version": version.to_dict(),
                "approval_chain": [a.to_dict() for a in approvals],
            }

    def signal_detail(self, signal_id: str) -> Dict[str, Any]:
        with db_session_scope() as session:
            record = (
                session.query(StrategySignalRecord)
                .filter(StrategySignalRecord.signal_id == signal_id)
                .one_or_none()
            )
            if record is None:
                raise StrategyLifecycleException(
                    f"信号 {signal_id} 不存在",
                    status_code=404,
                    details={"signal_id": signal_id},
                )
            return record.to_dict()

    def list_run_orders(self, run_id: str) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            self._get_run(session, run_id)
            rows = (
                session.query(StrategyOrderAudit)
                .filter(StrategyOrderAudit.run_id == run_id)
                .order_by(asc(StrategyOrderAudit.id))
                .all()
            )
            return [r.to_dict() for r in rows]

    def list_run_signals(self, run_id: str) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            self._get_run(session, run_id)
            rows = (
                session.query(StrategySignalRecord)
                .filter(StrategySignalRecord.run_id == run_id)
                .order_by(asc(StrategySignalRecord.id))
                .all()
            )
            return [r.to_dict() for r in rows]

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    def _add_event(
        self,
        session: Session,
        strategy_key: str,
        action: str,
        version: Optional[StrategyVersion],
        effective_at: datetime,
        reason: Optional[str],
        actor: Optional[str],
    ) -> StrategyPublishEvent:
        if action == PublishAction.PUBLISH:
            projected = self._active_node_at(session, strategy_key, effective_at)
            if projected is not None and projected.version_id == version.id:
                raise StrategyLifecycleException(
                    f"版本 v{version.version} 在该时刻已是线上版本，禁止重复发布",
                    status_code=409,
                    details={"strategy_key": strategy_key, "version_id": version.id},
                )
        event = StrategyPublishEvent(
            strategy_key=strategy_key,
            action=action,
            version_id=version.id if version else None,
            content_hash=version.content_hash if version else None,
            effective_at=effective_at,
            status=PublishEventStatus.SCHEDULED,
            reason=reason,
            created_by=actor,
        )
        session.add(event)
        session.flush()
        return event

    def _apply_due_locked(self, session: Session, strategy_key: str, now: datetime) -> None:
        """激活指定策略所有到期事件，按 (effective_at, id) 顺序折叠到时间线上。"""
        due = (
            session.query(StrategyPublishEvent)
            .filter(
                StrategyPublishEvent.strategy_key == strategy_key,
                StrategyPublishEvent.status == PublishEventStatus.SCHEDULED,
                StrategyPublishEvent.effective_at <= now,
            )
            .order_by(
                asc(StrategyPublishEvent.effective_at), asc(StrategyPublishEvent.id)
            )
            .all()
        )
        for event in due:
            current_active = (
                session.query(StrategyPublishEvent)
                .filter(
                    StrategyPublishEvent.strategy_key == strategy_key,
                    StrategyPublishEvent.status == PublishEventStatus.ACTIVE,
                )
                .one_or_none()
            )
            if current_active is not None and current_active.id != event.id:
                current_active.status = PublishEventStatus.SUPERSEDED
                if current_active.version_id is not None:
                    prev_version = session.get(StrategyVersion, current_active.version_id)
                    if prev_version and prev_version.status == VersionStatus.PUBLISHED:
                        # 撤回使线上版本进入 withdrawn；换版（发布/回滚）则为 superseded。
                        prev_version.status = (
                            VersionStatus.WITHDRAWN
                            if event.action == PublishAction.WITHDRAW
                            else VersionStatus.SUPERSEDED
                        )

            event.status = PublishEventStatus.ACTIVE
            event.activated_at = now

            if event.action in _SET_ACTIONS and event.version_id is not None:
                new_version = session.get(StrategyVersion, event.version_id)
                new_version.status = VersionStatus.PUBLISHED
                new_version.published_at = new_version.published_at or now

    def _active_node_at(
        self,
        session: Session,
        strategy_key: str,
        at: datetime,
    ) -> Optional[StrategyPublishEvent]:
        """时间线在 at 时刻的当前节点。

        已取消事件不计入；其余事件按 (effective_at, id) 折叠 —— 包括
        effective_at 已到但尚未被调度器物化的定时事件（它在该时刻本就应生效），
        这样“当前/历史版本”是时间线的确定性纯函数，不依赖调度器是否已跑。
        """
        events = (
            session.query(StrategyPublishEvent)
            .filter(
                StrategyPublishEvent.strategy_key == strategy_key,
                StrategyPublishEvent.status != PublishEventStatus.CANCELLED,
                StrategyPublishEvent.effective_at <= at,
            )
            .order_by(
                asc(StrategyPublishEvent.effective_at), asc(StrategyPublishEvent.id)
            )
            .all()
        )
        return events[-1] if events else None

    def _rollback_target(
        self,
        session: Session,
        strategy_key: str,
        at: datetime,
        current_node: Optional[StrategyPublishEvent] = None,
    ) -> Optional[StrategyVersion]:
        node = current_node or self._active_node_at(session, strategy_key, at)
        if node is None:
            return None
        # 在当前节点之前（严格更早的时间线位置）找最近一个设置版本的节点。
        previous = (
            session.query(StrategyPublishEvent)
            .filter(
                StrategyPublishEvent.strategy_key == strategy_key,
                StrategyPublishEvent.status != PublishEventStatus.CANCELLED,
                StrategyPublishEvent.action.in_(_SET_ACTIONS),
                StrategyPublishEvent.version_id.isnot(None),
                StrategyPublishEvent.effective_at <= at,
            )
            .order_by(
                asc(StrategyPublishEvent.effective_at), asc(StrategyPublishEvent.id)
            )
            .all()
        )
        previous = [
            e
            for e in previous
            if (e.effective_at, e.id) < (node.effective_at, node.id)
        ]
        if not previous:
            return None
        return session.get(StrategyVersion, previous[-1].version_id)

    def _assert_chain_complete(self, session: Session, version: StrategyVersion) -> None:
        # PUBLISHED/SUPERSEDED 说明历史上已完整通过；APPROVED 需要验证批准链。
        if version.status in (VersionStatus.PUBLISHED, VersionStatus.SUPERSEDED):
            return
        approvals = self._approvals_ordered(session, version.id)
        required = json.loads(version.required_approvers_json or "[]")
        if not approvals or len(approvals) != len(required):
            raise StrategyLifecycleException(
                "批准链不完整，版本不得发布", status_code=409
            )
        if any(a.decision != ApprovalDecision.APPROVED for a in approvals):
            raise StrategyLifecycleException(
                "批准链存在未通过节点，版本不得发布", status_code=409
            )

    def _approvals_ordered(
        self, session: Session, version_id: int
    ) -> List[StrategyApproval]:
        return (
            session.query(StrategyApproval)
            .filter(StrategyApproval.version_id == version_id)
            .order_by(asc(StrategyApproval.step), asc(StrategyApproval.id))
            .all()
        )

    def _get_version(self, session: Session, version_id: int) -> StrategyVersion:
        version = session.get(StrategyVersion, version_id)
        if version is None:
            raise StrategyLifecycleException(
                f"策略版本 {version_id} 不存在",
                status_code=404,
                details={"version_id": version_id},
            )
        return version

    def _get_run(self, session: Session, run_id: str) -> StrategyRunSnapshot:
        snapshot = (
            session.query(StrategyRunSnapshot)
            .filter(StrategyRunSnapshot.run_id == run_id)
            .one_or_none()
        )
        if snapshot is None:
            raise StrategyLifecycleException(
                f"运行快照 {run_id} 不存在",
                status_code=404,
                details={"run_id": run_id},
            )
        return snapshot

    def _next_version_number(self, session: Session, strategy_key: str) -> int:
        row = (
            session.query(StrategyVersion.version)
            .filter(StrategyVersion.strategy_key == strategy_key)
            .order_by(StrategyVersion.version.desc())
            .first()
        )
        return (row[0] + 1) if row else 1

    @staticmethod
    def _validate_key(strategy_key: str) -> None:
        if not isinstance(strategy_key, str) or not strategy_key.strip():
            raise StrategyLifecycleException("strategy_key 必须是非空字符串")

    @staticmethod
    def _validate_params(params: Any, risk_params: Any) -> None:
        if not isinstance(params, dict):
            raise StrategyLifecycleException("策略参数 params 必须是对象")
        if not isinstance(risk_params, dict):
            raise StrategyLifecycleException("风险参数 risk_params 必须是对象")
        try:
            json.dumps(params, default=str)
            json.dumps(risk_params, default=str)
        except (TypeError, ValueError) as exc:
            raise StrategyLifecycleException(f"参数无法序列化为 JSON: {exc}")
