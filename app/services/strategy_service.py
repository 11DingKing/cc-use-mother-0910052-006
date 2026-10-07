"""策略草稿、复核、发布、回滚、撤回、定时切换与运行快照的生命周期编排。

确定性保证：
1. 进程内所有生命周期变更由一把可重入锁串行化（FastAPI 同步端点运行在线程池）；
2. 写事务使用 BEGIN IMMEDIATE，跨进程/跨连接在 SQLite 层串行化，
   “同一策略至多一条 effective 发布”由数据库部分唯一索引兜底；
3. 版本内容在提交复核时冻结，params/risk_config 之后不可修改；
4. 运行快照、批准链、信号与订单留痕均为追加记录，读取方永远不会改写历史。
"""

import hashlib
import json
import logging
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from app import config as app_config
from app.config import db_session_scope as _default_session_scope
from app.entities.strategy import (
    ACTION_PUBLISH,
    ACTION_RECALL,
    ACTION_ROLLBACK,
    DECISION_APPROVED,
    DECISION_REJECTED,
    PUB_CANCELLED,
    PUB_EFFECTIVE,
    PUB_RECALLED,
    PUB_SCHEDULED,
    PUB_SUPERSEDED,
    RUN_RUNNING,
    RUN_STOPPED,
    VERSION_APPROVED,
    VERSION_DRAFT,
    VERSION_IN_REVIEW,
    VERSION_PUBLISHED,
    VERSION_REJECTED,
    VERSION_WITHDRAWN,
    StrategyApproval,
    StrategyOrderRecord,
    StrategyPublication,
    StrategyRunSnapshot,
    StrategySignalRecord,
    StrategyVersion,
)
from app.mappers.strategy_mapper import StrategyMapper
from app.middleware.exception_handler import AppException, NotFoundException

logger = logging.getLogger(__name__)


class StrategyException(AppException):
    """策略生命周期相关的业务异常。"""

    def __init__(self, message: str, status_code: int = 400,
                 code: str = "STRATEGY_ERROR", details=None):
        super().__init__(
            message=message,
            code=code,
            status_code=status_code,
            details=details or {},
        )


class StrategyConflictException(StrategyException):
    """并发冲突或非法状态迁移（409）。"""

    def __init__(self, message: str, details=None):
        super().__init__(
            message, status_code=409, code="STRATEGY_CONFLICT", details=details
        )


def canonical_hash(params: dict, risk_config: dict) -> str:
    """对策略参数与风险设置计算稳定的 SHA-256 摘要。"""
    payload = json.dumps(
        {"params": params, "risk_config": risk_config},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _loads(raw: str):
    return json.loads(raw) if raw else None


def _parse_iso_datetime(value, field: str = "scheduled_for") -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is None else value.replace(tzinfo=None)
    if not isinstance(value, str):
        raise StrategyException(f"{field} 必须是 ISO 8601 时间字符串")
    text = value.strip().replace("Z", "+00:00")
    try:
        from dateutil.parser import isoparse

        parsed = isoparse(text)
    except (ValueError, TypeError):
        raise StrategyException(f"{field} 时间格式不正确: {value}")
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


# 乐观比较令牌的“无期望”哨兵：None 本身表示“期望当前无生效版本”
_NO_EXPECTATION = object()


class PinnedStrategy:
    """任务运行期间固定的策略与风险设置；发布新版本不影响既有实例。"""

    def __init__(self, snapshot: StrategyRunSnapshot):
        self.run_id = snapshot.run_id
        self.snapshot_id = snapshot.id
        self.strategy_name = snapshot.strategy_name
        self.version_id = snapshot.version_id
        self.version_no = snapshot.version_no
        self.publication_id = snapshot.publication_id
        self.params = _loads(snapshot.params_json)
        self.risk_config = _loads(snapshot.risk_config_json)
        self.content_hash = snapshot.content_hash
        self.approval_evidence = _loads(snapshot.approval_evidence_json)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "snapshot_id": self.snapshot_id,
            "strategy_name": self.strategy_name,
            "version_id": self.version_id,
            "version_no": self.version_no,
            "publication_id": self.publication_id,
            "params": self.params,
            "risk_config": self.risk_config,
            "content_hash": self.content_hash,
        }


class StrategyService:
    """策略版本生命周期服务。"""

    def __init__(self, session_factory=None, time_func=datetime.utcnow):
        self._session_factory = session_factory
        self._time_func = time_func
        # 观察阶段使用的引擎：读走 DBAPI 自动提交（无 IMMEDIATE 写锁），
        # 与写事务的 WAL 并发，不会被串行化
        bound_engine = None
        if session_factory is not None:
            bound_engine = session_factory.kw.get("bind")
        self._engine = bound_engine or app_config.get_engine()
        # 生命周期变更的进程内串行点；锁可重入，便于组合操作
        self._lock = threading.RLock()
        # run_id -> PinnedStrategy，仅缓存；真相在快照表（崩溃后可重建）
        self._active_pins: dict[str, PinnedStrategy] = {}
        self._scheduler_started = False

    def _observe_query(self, sql: str, params: tuple = ()):
        """在写事务之外做一次无锁只读观察（DBAPI 自动提交，仅瞬间共享锁）。"""
        connection = self._engine.raw_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            row = cursor.fetchone()
            cursor.close()
            return row
        finally:
            connection.close()

    @contextmanager
    def _session_scope(self):
        if self._session_factory is not None:
            session = self._session_factory()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        else:
            with _default_session_scope() as session:
                yield session

    # ------------------------------------------------------------------
    # 草稿
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_payload(params, risk_config) -> None:
        if not isinstance(params, dict) or not params:
            raise StrategyException("策略参数 params 必须是非空对象")
        if not isinstance(risk_config, dict) or not risk_config:
            raise StrategyException("风险设置 risk_config 必须是非空对象")
        for name, value in (("params", params), ("risk_config", risk_config)):
            try:
                json.dumps(value, ensure_ascii=False, sort_keys=True)
            except (TypeError, ValueError):
                raise StrategyException(f"{name} 必须是可 JSON 序列化的对象")

    @staticmethod
    def _validate_approvers(required_approvers) -> list:
        if not isinstance(required_approvers, list) or not required_approvers:
            raise StrategyException(
                "required_approvers 必须是非空数组，按批准链顺序给出复核人"
            )
        chain = []
        for i, step in enumerate(required_approvers, start=1):
            if not isinstance(step, dict):
                raise StrategyException(f"批准链第 {i} 步必须是对象")
            role = step.get("role")
            approver = step.get("approver")
            if not role or not approver:
                raise StrategyException(
                    f"批准链第 {i} 步必须包含 role 与 approver"
                )
            chain.append({"step_no": i, "role": str(role),
                          "approver": str(approver)})
        return chain

    def create_draft(
        self,
        strategy_name: str,
        params: dict,
        risk_config: dict,
        required_approvers: list,
        description: str | None = None,
        change_note: str | None = None,
        created_by: str | None = None,
        parent_version_id: int | None = None,
    ) -> dict:
        if not strategy_name or not isinstance(strategy_name, str):
            raise StrategyException("strategy_name 不能为空")
        self._validate_payload(params, risk_config)
        chain = self._validate_approvers(required_approvers)

        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            if mapper.get_draft(strategy_name) is not None:
                raise StrategyConflictException(
                    f"策略 {strategy_name} 已存在草稿，请先提交、修改或删除该草稿"
                )
            version = StrategyVersion(
                strategy_name=strategy_name,
                version_no=mapper.next_version_no(strategy_name),
                description=description,
                change_note=change_note,
                params_json=_dumps(params),
                risk_config_json=_dumps(risk_config),
                required_approvers_json=_dumps(chain),
                content_hash=canonical_hash(params, risk_config),
                status=VERSION_DRAFT,
                created_by=created_by,
                parent_version_id=parent_version_id,
            )
            mapper.add_version(version)
            return self._version_dict(version)

    def update_draft(self, version_id: int, **fields) -> dict:
        allowed = {
            "params", "risk_config", "required_approvers",
            "description", "change_note",
        }
        updates = {k: v for k, v in fields.items() if k in allowed and v is not None}
        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            version = mapper.get_version(version_id)
            if version is None:
                raise NotFoundException(
                    "策略版本不存在", "StrategyVersion", str(version_id)
                )
            if version.status != VERSION_DRAFT:
                raise StrategyConflictException(
                    f"版本当前状态为 {version.status}，仅草稿可修改"
                )

            if "params" in updates or "risk_config" in updates:
                params = updates.get("params", _loads(version.params_json))
                risk_config = updates.get(
                    "risk_config", _loads(version.risk_config_json)
                )
                self._validate_payload(params, risk_config)
                version.params_json = _dumps(params)
                version.risk_config_json = _dumps(risk_config)
                version.content_hash = canonical_hash(params, risk_config)
            if "required_approvers" in updates:
                chain = self._validate_approvers(updates["required_approvers"])
                version.required_approvers_json = _dumps(chain)
            if "description" in updates:
                version.description = updates["description"]
            if "change_note" in updates:
                version.change_note = updates["change_note"]
            session.flush()
            return self._version_dict(version)

    def delete_draft(self, version_id: int) -> bool:
        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            version = mapper.get_version(version_id)
            if version is None:
                raise NotFoundException(
                    "策略版本不存在", "StrategyVersion", str(version_id)
                )
            if version.status != VERSION_DRAFT:
                raise StrategyConflictException(
                    f"版本当前状态为 {version.status}，仅草稿可删除"
                )
            session.delete(version)
            return True

    # ------------------------------------------------------------------
    # 提交复核 / 复核链
    # ------------------------------------------------------------------

    def submit_for_review(self, version_id: int, submitted_by: str | None = None) -> dict:
        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            version = mapper.get_version(version_id)
            if version is None:
                raise NotFoundException(
                    "策略版本不存在", "StrategyVersion", str(version_id)
                )
            if version.status != VERSION_DRAFT:
                raise StrategyConflictException(
                    f"版本当前状态为 {version.status}，仅草稿可提交复核"
                )
            version.status = VERSION_IN_REVIEW
            version.submitted_at = self._time_func()
            if submitted_by:
                version.created_by = version.created_by or submitted_by
            session.flush()
            return self._version_dict(version, mapper)

    def record_review(
        self,
        version_id: int,
        approver: str,
        decision: str,
        comment: str | None = None,
    ) -> dict:
        if decision not in (DECISION_APPROVED, DECISION_REJECTED):
            raise StrategyException("decision 只能是 approved 或 rejected")
        if not approver:
            raise StrategyException("approver 不能为空")

        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            version = mapper.get_version(version_id)
            if version is None:
                raise NotFoundException(
                    "策略版本不存在", "StrategyVersion", str(version_id)
                )
            if version.status != VERSION_IN_REVIEW:
                raise StrategyConflictException(
                    f"版本当前状态为 {version.status}，仅复核中版本可登记复核决定"
                )

            chain = _loads(version.required_approvers_json)
            next_step = mapper.latest_approval_step(version_id) + 1
            if next_step > len(chain):
                raise StrategyConflictException("批准链所有步骤均已完成")
            expected = chain[next_step - 1]
            if approver != expected["approver"]:
                raise StrategyConflictException(
                    f"当前第 {next_step} 步要求复核人 {expected['approver']}"
                    f"（{expected['role']}），实际为 {approver}",
                    details={"step_no": next_step, "expected": expected},
                )

            approval = StrategyApproval(
                version_id=version_id,
                step_no=next_step,
                role=expected["role"],
                approver=approver,
                decision=decision,
                comment=comment,
                content_hash=version.content_hash,
                decided_at=self._time_func(),
            )
            mapper.add_approval(approval)

            if decision == DECISION_REJECTED:
                version.status = VERSION_REJECTED
            elif next_step == len(chain):
                version.status = VERSION_APPROVED
                version.approved_at = self._time_func()
            session.flush()
            return self._version_dict(version, mapper)

    def withdraw_from_review(self, version_id: int, actor: str) -> dict:
        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            version = mapper.get_version(version_id)
            if version is None:
                raise NotFoundException(
                    "策略版本不存在", "StrategyVersion", str(version_id)
                )
            if version.status != VERSION_IN_REVIEW:
                raise StrategyConflictException(
                    f"版本当前状态为 {version.status}，仅复核中版本可撤回"
                )
            version.status = VERSION_WITHDRAWN
            session.flush()
            return self._version_dict(version, mapper)

    # ------------------------------------------------------------------
    # 发布 / 定时发布
    # ------------------------------------------------------------------

    def _assert_approved(self, version: StrategyVersion) -> None:
        if version.status not in (VERSION_APPROVED, VERSION_PUBLISHED):
            raise StrategyConflictException(
                f"版本当前状态为 {version.status}，只有完成整条批准链的版本可发布"
            )

    def _current_effective_id(self, strategy_name: str) -> int | None:
        """无锁只读当前生效事件 id，作为乐观并发的比较令牌。"""
        row = self._observe_query(
            "SELECT id FROM strategy_publications "
            "WHERE strategy_name = ? AND status = 'effective' LIMIT 1",
            (strategy_name,),
        )
        return row[0] if row else None

    def _read_version_pre_phase(self, version_id: int) -> dict:
        """加锁前的无锁只读预检，返回比较令牌与版本基础信息。"""
        row = self._observe_query(
            "SELECT id, strategy_name, version_no, status "
            "FROM strategy_versions WHERE id = ?",
            (version_id,),
        )
        if row is None:
            raise NotFoundException(
                "策略版本不存在", "StrategyVersion", str(version_id)
            )
        version_id_, strategy_name, version_no, status = row
        return {
            "version_id": version_id_,
            "strategy_name": strategy_name,
            "version_no": version_no,
            "status": status,
            "expected_id": self._current_effective_id(strategy_name),
        }

    def _activate_publication(
        self,
        session,
        mapper: StrategyMapper,
        strategy_name: str,
        version: StrategyVersion,
        action: str,
        actor: str | None,
        note: str | None,
        effective_at: datetime,
        expected_publication_id=_NO_EXPECTATION,
    ) -> StrategyPublication:
        """在已串行化的写事务中切换生效版本（带比较令牌的 CAS）。"""
        current = mapper.get_effective_publication(strategy_name)
        current_id = current.id if current is not None else None

        # 乐观并发：调用方在决定操作时看到的生效事件必须仍是当前事件，
        # 否则说明已被并发的发布/回滚抢先，本次请求确定性地失败。
        if (
            expected_publication_id is not _NO_EXPECTATION
            and current_id != expected_publication_id
        ):
            raise StrategyConflictException(
                f"策略 {strategy_name} 的生效版本已被并发修改"
                f"（期望事件 {expected_publication_id}，当前 "
                f"{current_id}），请刷新后重试"
            )

        # 目标版本已是当前生效版本：幂等冲突
        if current is not None and current.version_id == version.id:
            raise StrategyConflictException(
                f"策略 {strategy_name} 当前生效版本已是 v{version.version_no}"
            )
        if current is not None:
            current.status = PUB_SUPERSEDED
            session.flush()

        publication = StrategyPublication(
            strategy_name=strategy_name,
            version_id=version.id,
            action=action,
            status=PUB_EFFECTIVE,
            effective_at=effective_at,
            actor=actor,
            note=note,
        )
        try:
            mapper.add_publication(publication)
        except IntegrityError:
            # 数据库部分唯一索引兜底：跨进程极端竞态下拒绝第二个生效版本
            raise StrategyConflictException(
                f"策略 {strategy_name} 的生效版本正被并发修改，请重试"
            )

        if version.status == VERSION_APPROVED:
            version.status = VERSION_PUBLISHED
            version.published_at = effective_at
        session.flush()
        return publication

    def publish(
        self,
        version_id: int,
        actor: str | None = None,
        scheduled_for: datetime | str | None = None,
        note: str | None = None,
    ) -> dict:
        schedule_at = None
        if scheduled_for is not None:
            schedule_at = _parse_iso_datetime(scheduled_for)

        # 观察阶段：记录操作发起时看到的生效事件，作为写阶段的比较令牌
        pre = self._read_version_pre_phase(version_id)

        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            version = mapper.get_version(version_id)
            self._assert_approved(version)

            now = self._time_func()
            if schedule_at is not None and schedule_at > now:
                # 已在生效的版本不允许再挂定时切换
                current = mapper.get_effective_publication(version.strategy_name)
                if current is not None and current.version_id == version.id:
                    raise StrategyConflictException(
                        f"策略 {version.strategy_name} 当前生效版本已是 "
                        f"v{version.version_no}，无需定时切换"
                    )
                # 每个策略至多挂起一个定时发布，避免到期顺序不确定
                pending = [
                    p for p in mapper.list_publications(
                        version.strategy_name, PUB_SCHEDULED
                    )
                ]
                if pending:
                    raise StrategyConflictException(
                        f"策略 {version.strategy_name} 已存在待生效的定时发布 "
                        f"(publication_id={pending[0].id})，请先取消",
                        details={"existing_publication_id": pending[0].id},
                    )
                publication = StrategyPublication(
                    strategy_name=version.strategy_name,
                    version_id=version.id,
                    action=ACTION_PUBLISH,
                    status=PUB_SCHEDULED,
                    scheduled_for=schedule_at,
                    actor=actor,
                    note=note,
                )
                mapper.add_publication(publication)
                session.flush()
                return self._publication_dict(publication)

            publication = self._activate_publication(
                session, mapper, version.strategy_name, version,
                action=ACTION_PUBLISH, actor=actor, note=note,
                effective_at=now,
                expected_publication_id=pre["expected_id"],
            )
            return self._publication_dict(publication)

    def activate_due_scheduled(self, now: datetime | None = None) -> list:
        """让所有到点的定时发布确定性地依次生效。启动恢复与定时器都调用它。"""
        activated = []
        now = now or self._time_func()
        with self._lock:
            with self._session_scope() as session:
                due = StrategyMapper(session).list_due_scheduled(now)
                # 先快照待处理清单（id），逐条独立事务生效
                due_ids = [(p.id, p.strategy_name) for p in due]

            for publication_id, strategy_name in due_ids:
                try:
                    with self._session_scope() as session:
                        mapper = StrategyMapper(session)
                        scheduled = mapper.get_publication(publication_id)
                        if scheduled is None or scheduled.status != PUB_SCHEDULED:
                            continue
                        version = mapper.get_version(scheduled.version_id)
                        if version is None or version.status not in (
                            VERSION_APPROVED, VERSION_PUBLISHED
                        ):
                            scheduled.status = PUB_CANCELLED
                            scheduled.note = (
                                (scheduled.note or "")
                                + " [自动取消: 版本已不可发布]"
                            ).strip()
                            session.flush()
                            continue
                        already = mapper.get_effective_publication(strategy_name)
                        if already is not None and already.version_id == version.id:
                            # 该版本已通过直接发布生效，定时任务无需再切
                            scheduled.status = PUB_CANCELLED
                            scheduled.note = (
                                (scheduled.note or "")
                                + f" [自动取消: v{version.version_no} 已生效 "
                                f"publication_id={already.id}]"
                            ).strip()
                            session.flush()
                            continue
                        effective = self._activate_publication(
                            session, mapper, strategy_name, version,
                            action=ACTION_PUBLISH,
                            actor=scheduled.actor,
                            note=scheduled.note,
                            effective_at=max(now, scheduled.scheduled_for),
                        )
                        # 定时事件行本身转为 cancelled（已被新的 effective 行取代），
                        # 保留原始请求人与计划时间作为审计依据。
                        scheduled.status = PUB_CANCELLED
                        scheduled.note = (
                            (scheduled.note or "")
                            + f" [已按时生效: publication_id={effective.id}]"
                        ).strip()
                        session.flush()
                        activated.append(effective.id)
                except StrategyException as exc:
                    logger.warning(
                        "scheduled publication %s skipped: %s",
                        publication_id, exc.message,
                    )
        return activated

    def cancel_scheduled(
        self, publication_id: int, actor: str | None = None, note: str | None = None
    ) -> dict:
        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            publication = mapper.get_publication(publication_id)
            if publication is None:
                raise NotFoundException(
                    "发布事件不存在", "StrategyPublication", str(publication_id)
                )
            if publication.status != PUB_SCHEDULED:
                raise StrategyConflictException(
                    f"发布事件状态为 {publication.status}，仅待生效的定时发布可取消"
                )
            publication.status = PUB_CANCELLED
            publication.note = (
                (publication.note or "")
                + (f" [取消人={actor}]" if actor else "")
                + (f" {note}" if note else "")
            ).strip()
            session.flush()
            return self._publication_dict(publication)

    def rollback(
        self, strategy_name: str, actor: str | None = None, note: str | None = None
    ) -> dict:
        # 观察阶段：记录操作发起时看到的生效事件
        expected_id = self._current_effective_id(strategy_name)

        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            current = mapper.get_effective_publication(strategy_name)
            current_version_id = current.version_id if current else None

            # 最近一个被取代（superseded）的生效事件就是“上一版”；
            # 撤回（recall）之后当前无生效版本，同样可以回滚到它。
            superseded = [
                p for p in mapper.list_publications(strategy_name)
                if p.status == PUB_SUPERSEDED and p.version_id is not None
            ]
            target = None
            for pub in reversed(superseded):
                if pub.version_id != current_version_id:
                    target = pub
                    break
            if target is None:
                raise StrategyConflictException(
                    f"策略 {strategy_name} 没有可回滚的历史版本"
                )

            target_version = mapper.get_version(target.version_id)
            publication = self._activate_publication(
                session, mapper, strategy_name, target_version,
                action=ACTION_ROLLBACK, actor=actor,
                note=note or f"回滚至 v{target_version.version_no}",
                effective_at=self._time_func(),
                expected_publication_id=expected_id,
            )
            return self._publication_dict(publication)

    def recall(
        self, strategy_name: str, actor: str | None = None, note: str | None = None
    ) -> dict:
        """撤回当前生效版本：该策略立即变为无生效版本，运行中的任务不受影响。"""
        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            current = mapper.get_effective_publication(strategy_name)
            if current is None:
                raise StrategyConflictException(
                    f"策略 {strategy_name} 当前没有生效版本，无法撤回"
                )
            recalled_version_id = current.version_id
            current.status = PUB_RECALLED
            current.note = (
                (current.note or "")
                + f" [撤回人={actor}]"
                + (f" {note}" if note else "")
            ).strip()
            event = StrategyPublication(
                strategy_name=strategy_name,
                version_id=recalled_version_id,
                action=ACTION_RECALL,
                status=PUB_RECALLED,
                effective_at=self._time_func(),
                actor=actor,
                note=note,
            )
            mapper.add_publication(event)
            session.flush()
            return self._publication_dict(event)

    # ------------------------------------------------------------------
    # 运行快照
    # ------------------------------------------------------------------

    def start_run(
        self,
        strategy_name: str,
        run_id: str | None = None,
        actor: str | None = None,
    ) -> dict:
        run_id = run_id or f"RUN_{self._time_func().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        with self._lock:
            # 启动瞬间先让到点的定时切换落地，保证固定的是“此刻生效”的版本
            self.activate_due_scheduled()
            with self._session_scope() as session:
                mapper = StrategyMapper(session)
                if mapper.get_snapshot_by_run(run_id) is not None:
                    raise StrategyConflictException(f"run_id {run_id} 已存在")
                publication = mapper.get_effective_publication(strategy_name)
                if publication is None or publication.version_id is None:
                    raise StrategyConflictException(
                        f"策略 {strategy_name} 没有已发布且生效的版本，不能启动任务"
                    )
                version = mapper.get_version(publication.version_id)
                approvals = mapper.list_approvals(version.id)
                chain = _loads(version.required_approvers_json)
                if len(approvals) != len(chain) or any(
                    a.decision != DECISION_APPROVED for a in approvals
                ):
                    raise StrategyConflictException(
                        "生效版本的批准链不完整，拒绝启动任务"
                    )

                evidence = [
                    {
                        "step_no": a.step_no,
                        "role": a.role,
                        "approver": a.approver,
                        "decision": a.decision,
                        "comment": a.comment,
                        "content_hash": a.content_hash,
                        "decided_at": a.decided_at.isoformat() if a.decided_at else None,
                    }
                    for a in approvals
                ]
                snapshot = StrategyRunSnapshot(
                    run_id=run_id,
                    strategy_name=strategy_name,
                    version_id=version.id,
                    version_no=version.version_no,
                    publication_id=publication.id,
                    params_json=version.params_json,
                    risk_config_json=version.risk_config_json,
                    content_hash=version.content_hash,
                    approval_evidence_json=_dumps(evidence),
                    status=RUN_RUNNING,
                    started_at=self._time_func(),
                )
                mapper.add_snapshot(snapshot)
                session.flush()
                pin = PinnedStrategy(snapshot)
                self._active_pins[run_id] = pin
                return self._snapshot_dict(snapshot, evidence)

    def stop_run(self, run_id: str) -> dict:
        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            snapshot = mapper.get_snapshot_by_run(run_id)
            if snapshot is None:
                raise NotFoundException("运行快照不存在", "StrategyRun", run_id)
            if snapshot.status == RUN_RUNNING:
                snapshot.status = RUN_STOPPED
                snapshot.ended_at = self._time_func()
                session.flush()
            self._active_pins.pop(run_id, None)
            return self._snapshot_dict(
                snapshot, _loads(snapshot.approval_evidence_json)
            )

    def get_pin(self, run_id: str) -> PinnedStrategy | None:
        return self._active_pins.get(run_id)

    def recover_active_runs(self) -> list:
        """异常重启后，从快照表重建仍在运行任务的固定策略（不会拾取新版本）。"""
        # 重启后先处理在宕机期间到点的定时发布
        self.activate_due_scheduled()
        recovered = []
        with self._lock, self._session_scope() as session:
            mapper = StrategyMapper(session)
            for snapshot in mapper.list_snapshots(status=RUN_RUNNING):
                pin = PinnedStrategy(snapshot)
                self._active_pins[pin.run_id] = pin
                recovered.append(pin.run_id)
        logger.info("recovered %d active strategy runs: %s",
                    len(recovered), recovered)
        return recovered

    # ------------------------------------------------------------------
    # 信号 / 订单留痕（只增不改）
    # ------------------------------------------------------------------

    def record_signal(
        self,
        pin: PinnedStrategy,
        stock_code: str,
        signal_type: str,
        signal_strength: float,
        price: float | None,
        outcome: str,
        resulting_order_id: str | None = None,
    ) -> int:
        with self._session_scope() as session:
            record = StrategySignalRecord(
                run_id=pin.run_id,
                version_id=pin.version_id,
                content_hash=pin.content_hash,
                stock_code=stock_code,
                signal_type=signal_type,
                signal_strength=str(signal_strength),
                price=None if price is None else str(price),
                resulting_order_id=resulting_order_id,
                outcome=outcome,
            )
            StrategyMapper(session).add_signal_record(record)
            return record.id

    def record_order(self, pin: PinnedStrategy, order) -> int:
        with self._session_scope() as session:
            record = StrategyOrderRecord(
                order_id=order.order_id,
                run_id=pin.run_id,
                snapshot_id=pin.snapshot_id,
                version_id=pin.version_id,
                version_no=pin.version_no,
                content_hash=pin.content_hash,
                stock_code=order.stock_code,
                side=order.side.value,
                quantity=order.quantity,
                price=str(order.price) if order.price is not None else None,
                signal_type=order.signal_type,
                signal_strength=(
                    str(order.signal_strength)
                    if order.signal_strength is not None
                    else None
                ),
                status_at_record=order.status.value,
            )
            StrategyMapper(session).add_order_record(record)
            return record.id

    # ------------------------------------------------------------------
    # 历史查询 / 批准依据还原（只读，绝不修改已完成记录）
    # ------------------------------------------------------------------

    def get_version(self, version_id: int, with_approvals: bool = True) -> dict:
        with self._session_scope() as session:
            mapper = StrategyMapper(session)
            version = mapper.get_version(version_id)
            if version is None:
                raise NotFoundException(
                    "策略版本不存在", "StrategyVersion", str(version_id)
                )
            data = self._version_dict(version, mapper if with_approvals else None)
            return data

    def list_versions(
        self, strategy_name: str | None = None, status: str | None = None
    ) -> list:
        with self._session_scope() as session:
            mapper = StrategyMapper(session)
            versions = mapper.list_versions(strategy_name, status)
            return [self._version_dict(v) for v in versions]

    def get_effective(self, strategy_name: str) -> dict | None:
        with self._session_scope() as session:
            mapper = StrategyMapper(session)
            publication = mapper.get_effective_publication(strategy_name)
            if publication is None:
                return None
            data = self._publication_dict(publication)
            version = mapper.get_version(publication.version_id)
            data["version"] = self._version_dict(version, mapper)
            return data

    def list_publications(
        self, strategy_name: str | None = None, status: str | None = None
    ) -> list:
        with self._session_scope() as session:
            mapper = StrategyMapper(session)
            return [
                self._publication_dict(p)
                for p in mapper.list_publications(strategy_name, status)
            ]

    def get_snapshot(self, run_id: str) -> dict:
        with self._session_scope() as session:
            mapper = StrategyMapper(session)
            snapshot = mapper.get_snapshot_by_run(run_id)
            if snapshot is None:
                raise NotFoundException("运行快照不存在", "StrategyRun", run_id)
            return self._snapshot_dict(
                snapshot, _loads(snapshot.approval_evidence_json)
            )

    def list_runs(
        self,
        strategy_name: str | None = None,
        version_id: int | None = None,
        status: str | None = None,
    ) -> list:
        with self._session_scope() as session:
            mapper = StrategyMapper(session)
            return [
                self._snapshot_dict(s, _loads(s.approval_evidence_json))
                for s in mapper.list_snapshots(strategy_name, version_id, status)
            ]

    def trace_order(self, order_id: str) -> dict:
        """从订单还原当时使用的策略版本、参数快照与完整批准依据。"""
        with self._session_scope() as session:
            mapper = StrategyMapper(session)
            order_record = mapper.get_order_record(order_id)
            if order_record is None:
                raise NotFoundException(
                    "未找到该订单的策略留痕", "StrategyOrderRecord", order_id
                )
            snapshot = mapper.get_snapshot(order_record.snapshot_id)
            if snapshot is None:
                raise NotFoundException(
                    "订单引用的运行快照不存在", "StrategyRun",
                    order_record.run_id,
                )
            version = mapper.get_version(snapshot.version_id)
            signal = mapper.get_signal_record_by_order(order_id)
            approvals = mapper.list_approvals(version.id)
            publication = mapper.get_publication(snapshot.publication_id)

            recomputed = canonical_hash(
                _loads(version.params_json), _loads(version.risk_config_json)
            )
            hash_checks = {
                "version_hash_recomputed": recomputed,
                "version_hash_matches": recomputed == version.content_hash,
                "snapshot_hash_matches": snapshot.content_hash
                == version.content_hash,
                "order_hash_matches": order_record.content_hash
                == version.content_hash,
                "all_approvals_hash_match": all(
                    a.content_hash == version.content_hash for a in approvals
                ),
            }

            return {
                "order": {
                    "order_id": order_record.order_id,
                    "run_id": order_record.run_id,
                    "snapshot_id": order_record.snapshot_id,
                    "version_id": order_record.version_id,
                    "version_no": order_record.version_no,
                    "content_hash": order_record.content_hash,
                    "stock_code": order_record.stock_code,
                    "side": order_record.side,
                    "quantity": order_record.quantity,
                    "price": order_record.price,
                    "signal_type": order_record.signal_type,
                    "signal_strength": order_record.signal_strength,
                    "status_at_record": order_record.status_at_record,
                    "created_at": order_record.created_at.isoformat()
                    if order_record.created_at else None,
                },
                "signal": None if signal is None else {
                    "id": signal.id,
                    "signal_type": signal.signal_type,
                    "signal_strength": signal.signal_strength,
                    "price": signal.price,
                    "outcome": signal.outcome,
                    "created_at": signal.created_at.isoformat()
                    if signal.created_at else None,
                },
                "snapshot": self._snapshot_dict(
                    snapshot, _loads(snapshot.approval_evidence_json)
                ),
                "version": self._version_dict(version),
                "publication": self._publication_dict(publication),
                "approvals": [self._approval_dict(a) for a in approvals],
                "hash_checks": hash_checks,
                "intact": all(
                    [
                        hash_checks["version_hash_matches"],
                        hash_checks["snapshot_hash_matches"],
                        hash_checks["order_hash_matches"],
                        hash_checks["all_approvals_hash_match"],
                    ]
                ),
            }

    def list_run_signals(self, run_id: str) -> list:
        with self._session_scope() as session:
            mapper = StrategyMapper(session)
            if mapper.get_snapshot_by_run(run_id) is None:
                raise NotFoundException("运行快照不存在", "StrategyRun", run_id)
            return [
                {
                    "id": r.id,
                    "run_id": r.run_id,
                    "version_id": r.version_id,
                    "content_hash": r.content_hash,
                    "stock_code": r.stock_code,
                    "signal_type": r.signal_type,
                    "signal_strength": r.signal_strength,
                    "price": r.price,
                    "resulting_order_id": r.resulting_order_id,
                    "outcome": r.outcome,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
                for r in mapper.list_signal_records(run_id)
            ]

    def list_run_orders(self, run_id: str) -> list:
        with self._session_scope() as session:
            mapper = StrategyMapper(session)
            if mapper.get_snapshot_by_run(run_id) is None:
                raise NotFoundException("运行快照不存在", "StrategyRun", run_id)
            return [
                {
                    "id": r.id,
                    "order_id": r.order_id,
                    "run_id": r.run_id,
                    "snapshot_id": r.snapshot_id,
                    "version_id": r.version_id,
                    "version_no": r.version_no,
                    "content_hash": r.content_hash,
                    "stock_code": r.stock_code,
                    "side": r.side,
                    "quantity": r.quantity,
                    "price": r.price,
                    "signal_type": r.signal_type,
                    "signal_strength": r.signal_strength,
                    "status_at_record": r.status_at_record,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
                for r in mapper.list_order_records_by_run(run_id)
            ]

    # ------------------------------------------------------------------
    # 定时切换循环
    # ------------------------------------------------------------------

    def start_scheduler(self, interval_seconds: float = 1.0) -> None:
        if self._scheduler_started:
            return
        self._scheduler_started = True

        def _loop():
            logger.info("strategy scheduled-switch scheduler started")
            while self._scheduler_started:
                try:
                    self.activate_due_scheduled()
                except Exception:
                    logger.exception("scheduled activation tick failed")
                threading.Event().wait(interval_seconds)

        thread = threading.Thread(
            target=_loop, name="strategy-scheduler", daemon=True
        )
        thread.start()

    def stop_scheduler(self) -> None:
        self._scheduler_started = False

    # ------------------------------------------------------------------
    # 序列化
    # ------------------------------------------------------------------

    @staticmethod
    def _approval_dict(a: StrategyApproval) -> dict:
        return {
            "id": a.id,
            "step_no": a.step_no,
            "role": a.role,
            "approver": a.approver,
            "decision": a.decision,
            "comment": a.comment,
            "content_hash": a.content_hash,
            "decided_at": a.decided_at.isoformat() if a.decided_at else None,
        }

    def _version_dict(
        self, v: StrategyVersion, mapper: StrategyMapper | None = None
    ) -> dict:
        data = {
            "id": v.id,
            "strategy_name": v.strategy_name,
            "version_no": v.version_no,
            "description": v.description,
            "change_note": v.change_note,
            "params": _loads(v.params_json),
            "risk_config": _loads(v.risk_config_json),
            "required_approvers": _loads(v.required_approvers_json),
            "content_hash": v.content_hash,
            "status": v.status,
            "created_by": v.created_by,
            "created_at": v.created_at.isoformat() if v.created_at else None,
            "submitted_at": v.submitted_at.isoformat() if v.submitted_at else None,
            "approved_at": v.approved_at.isoformat() if v.approved_at else None,
            "published_at": v.published_at.isoformat() if v.published_at else None,
            "parent_version_id": v.parent_version_id,
        }
        if mapper is not None:
            data["approvals"] = [
                self._approval_dict(a) for a in mapper.list_approvals(v.id)
            ]
        return data

    @staticmethod
    def _publication_dict(p: StrategyPublication) -> dict:
        return {
            "id": p.id,
            "strategy_name": p.strategy_name,
            "version_id": p.version_id,
            "action": p.action,
            "status": p.status,
            "scheduled_for": p.scheduled_for.isoformat() if p.scheduled_for else None,
            "effective_at": p.effective_at.isoformat() if p.effective_at else None,
            "actor": p.actor,
            "note": p.note,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        }

    @staticmethod
    def _snapshot_dict(s: StrategyRunSnapshot, evidence: list) -> dict:
        return {
            "id": s.id,
            "run_id": s.run_id,
            "strategy_name": s.strategy_name,
            "version_id": s.version_id,
            "version_no": s.version_no,
            "publication_id": s.publication_id,
            "params": _loads(s.params_json),
            "risk_config": _loads(s.risk_config_json),
            "content_hash": s.content_hash,
            "approval_evidence": evidence,
            "status": s.status,
            "started_at": s.started_at.isoformat() if s.started_at else None,
            "ended_at": s.ended_at.isoformat() if s.ended_at else None,
        }
