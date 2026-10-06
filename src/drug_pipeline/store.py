"""合作管线与里程碑的本地持久化边界。

设计要点：
- 项目、适应症、地区权利、里程碑、证据均为追加式版本表，只插不改；
  当前值取最大版本，历史天然保留。
- 状态类当前值（milestone_state / evidence_state / payment）单独存放，
  每次变化同时写 audit_log，任何结论都能回放。
- 写操作不自动提交，由 Service 的 transaction() 统一提交或回滚，
  保证多表写入要么全部生效要么全部放弃（故障恢复的前提）。
"""
import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from .domain import Record

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_milestone (
    record_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    role TEXT,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS project_version (
    project_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    name TEXT NOT NULL,
    parties TEXT NOT NULL,
    confirm_roles TEXT NOT NULL,
    finance_roles TEXT NOT NULL,
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    PRIMARY KEY (project_id, version)
);
CREATE TABLE IF NOT EXISTS indication_version (
    indication_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    name TEXT NOT NULL,
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    PRIMARY KEY (indication_id, version)
);
CREATE TABLE IF NOT EXISTS region_right_version (
    right_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    region TEXT NOT NULL,
    party TEXT NOT NULL,
    status TEXT NOT NULL,
    supersedes TEXT NOT NULL DEFAULT '',
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    PRIMARY KEY (right_id, version)
);
CREATE TABLE IF NOT EXISTS milestone_version (
    milestone_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    indication_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    title TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    deadline_utc TEXT NOT NULL,
    requirements TEXT NOT NULL,
    version INTEGER NOT NULL,
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    PRIMARY KEY (milestone_id, version)
);
CREATE TABLE IF NOT EXISTS milestone_state (
    milestone_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    achievement TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence_version (
    evidence_id TEXT NOT NULL,
    milestone_id TEXT NOT NULL,
    requirement TEXT NOT NULL,
    version INTEGER NOT NULL,
    party TEXT NOT NULL,
    result TEXT NOT NULL,
    summary TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    within_deadline INTEGER NOT NULL,
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    PRIMARY KEY (evidence_id, version)
);
CREATE TABLE IF NOT EXISTS evidence_state (
    evidence_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS confirmation (
    confirmation_id TEXT PRIMARY KEY,
    evidence_id TEXT NOT NULL,
    evidence_version INTEGER NOT NULL,
    party TEXT NOT NULL,
    role TEXT NOT NULL,
    decision TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    revoke_ts TEXT,
    revoke_actor TEXT,
    revoke_reason TEXT
);
CREATE TABLE IF NOT EXISTS payment (
    payment_id TEXT PRIMARY KEY,
    milestone_id TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    state TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    note TEXT NOT NULL DEFAULT '',
    requested_by TEXT NOT NULL,
    request_party TEXT NOT NULL,
    request_ts TEXT NOT NULL,
    confirmed_by TEXT,
    confirm_party TEXT,
    confirm_ts TEXT
);
CREATE TABLE IF NOT EXISTS payment_event (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    payment_id TEXT NOT NULL,
    event TEXT NOT NULL,
    actor TEXT NOT NULL,
    ts TEXT NOT NULL,
    detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS payment_decision (
    payment_id TEXT PRIMARY KEY,
    snapshot TEXT NOT NULL,
    ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reversal (
    reversal_id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL,
    amount REAL NOT NULL,
    reason TEXT NOT NULL,
    actor TEXT NOT NULL,
    ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS callback_log (
    idempotency_key TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL,
    result TEXT NOT NULL,
    response TEXT NOT NULL,
    ts TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        # check_same_thread=False 配合锁，支持双方并行提交的场景演练。
        self.connection = sqlite3.connect(str(path), check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.connection.executescript(SCHEMA)
            self.connection.commit()

    @contextmanager
    def transaction(self):
        """一次业务操作的多表写入整体提交或回滚。"""
        with self.lock:
            try:
                yield self.connection
            except Exception:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()

    def _execute(self, sql: str, params=()):
        with self.lock:
            return self.connection.execute(sql, params)

    def _one(self, sql: str, params=()):
        row = self._execute(sql, params).fetchone()
        return dict(row) if row else None

    def _all(self, sql: str, params=()):
        return [dict(r) for r in self._execute(sql, params).fetchall()]

    # ---- 基线记录（保持兼容） ----

    def save(self, record: Record) -> Record:
        value = record.with_timestamp()
        with self.lock:
            self.connection.execute(
                "INSERT INTO pipeline_milestone(record_id, owner_id, state, created_at) VALUES(?,?,?,?)",
                (value.record_id, value.owner_id, value.state, value.created_at),
            )
            self.connection.commit()
        return value

    def get(self, record_id: str) -> Record | None:
        row = self._one(
            "SELECT record_id, owner_id, state, created_at FROM pipeline_milestone WHERE record_id=?",
            (record_id,),
        )
        return Record(**row) if row else None

    # ---- 审计 ----

    def log_audit(self, ts: str, actor: str, role: str | None, action: str,
                  entity_type: str, entity_id: str, detail: dict) -> None:
        self._execute(
            "INSERT INTO audit_log(ts, actor, role, action, entity_type, entity_id, detail)"
            " VALUES(?,?,?,?,?,?,?)",
            (ts, actor, role, action, entity_type, entity_id,
             json.dumps(detail, ensure_ascii=False, sort_keys=True)),
        )

    def audit_trail(self, entity_type: str | None = None, entity_id: str | None = None):
        sql = "SELECT * FROM audit_log"
        cond, params = [], []
        if entity_type:
            cond.append("entity_type=?")
            params.append(entity_type)
        if entity_id:
            cond.append("entity_id=?")
            params.append(entity_id)
        if cond:
            sql += " WHERE " + " AND ".join(cond)
        return self._all(sql + " ORDER BY seq", params)

    # ---- 项目 / 适应症 ----

    def insert_project_version(self, project_id, version, name, parties,
                               confirm_roles, finance_roles, ts, actor) -> None:
        self._execute(
            "INSERT INTO project_version VALUES(?,?,?,?,?,?,?,?)",
            (project_id, version, name, json.dumps(list(parties), ensure_ascii=False),
             json.dumps(list(confirm_roles), ensure_ascii=False),
             json.dumps(list(finance_roles), ensure_ascii=False), ts, actor),
        )

    def get_project(self, project_id):
        return self._one(
            "SELECT * FROM project_version WHERE project_id=? ORDER BY version DESC LIMIT 1",
            (project_id,),
        )

    def insert_indication_version(self, indication_id, project_id, version, name, ts, actor) -> None:
        self._execute(
            "INSERT INTO indication_version VALUES(?,?,?,?,?,?)",
            (indication_id, project_id, version, name, ts, actor),
        )

    def get_indication(self, indication_id):
        return self._one(
            "SELECT * FROM indication_version WHERE indication_id=? ORDER BY version DESC LIMIT 1",
            (indication_id,),
        )

    # ---- 地区权利 ----

    def insert_right_version(self, right_id, project_id, version, region, party,
                             status, supersedes, ts, actor) -> None:
        self._execute(
            "INSERT INTO region_right_version VALUES(?,?,?,?,?,?,?,?,?)",
            (right_id, project_id, version, region, party, status, supersedes, ts, actor),
        )

    def get_right(self, right_id):
        return self._one(
            "SELECT * FROM region_right_version WHERE right_id=? ORDER BY version DESC LIMIT 1",
            (right_id,),
        )

    def active_rights(self, project_id):
        return self._all(
            "SELECT r.* FROM region_right_version r"
            " JOIN (SELECT right_id, MAX(version) v FROM region_right_version GROUP BY right_id) t"
            "   ON t.right_id=r.right_id AND t.v=r.version"
            " WHERE r.project_id=? AND r.status='active' ORDER BY r.region",
            (project_id,),
        )

    def right_history(self, project_id):
        return self._all(
            "SELECT * FROM region_right_version WHERE project_id=?"
            " ORDER BY right_id, version",
            (project_id,),
        )

    # ---- 里程碑 ----

    def insert_milestone_version(self, milestone_id, project_id, indication_id, seq,
                                 title, amount, currency, deadline_utc, requirements,
                                 version, ts, actor) -> None:
        self._execute(
            "INSERT INTO milestone_version VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (milestone_id, project_id, indication_id, seq, title, amount, currency,
             deadline_utc, json.dumps(list(requirements), ensure_ascii=False),
             version, ts, actor),
        )

    def get_milestone(self, milestone_id):
        return self._one(
            "SELECT * FROM milestone_version WHERE milestone_id=? ORDER BY version DESC LIMIT 1",
            (milestone_id,),
        )

    def milestones_for_indication(self, project_id, indication_id):
        return self._all(
            "SELECT mv.*, ms.state, ms.achievement FROM milestone_version mv"
            " JOIN (SELECT milestone_id, MAX(version) v FROM milestone_version GROUP BY milestone_id) t"
            "   ON t.milestone_id=mv.milestone_id AND t.v=mv.version"
            " LEFT JOIN milestone_state ms ON ms.milestone_id=mv.milestone_id"
            " WHERE mv.project_id=? AND mv.indication_id=? ORDER BY mv.seq",
            (project_id, indication_id),
        )

    def set_milestone_state(self, milestone_id, state, achievement, ts) -> None:
        self._execute(
            "INSERT INTO milestone_state(milestone_id, state, achievement, updated_at) VALUES(?,?,?,?)"
            " ON CONFLICT(milestone_id) DO UPDATE SET state=excluded.state,"
            " achievement=excluded.achievement, updated_at=excluded.updated_at",
            (milestone_id, state, achievement, ts),
        )

    def get_milestone_state(self, milestone_id):
        return self._one("SELECT * FROM milestone_state WHERE milestone_id=?", (milestone_id,))

    # ---- 证据 ----

    def insert_evidence_version(self, evidence_id, milestone_id, requirement, version,
                                party, result, summary, submitted_at, within_deadline,
                                ts, actor) -> None:
        self._execute(
            "INSERT INTO evidence_version VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (evidence_id, milestone_id, requirement, version, party, result, summary,
             submitted_at, 1 if within_deadline else 0, ts, actor),
        )

    def get_evidence_version(self, evidence_id, version=None):
        if version is None:
            return self._one(
                "SELECT * FROM evidence_version WHERE evidence_id=? ORDER BY version DESC LIMIT 1",
                (evidence_id,),
            )
        return self._one(
            "SELECT * FROM evidence_version WHERE evidence_id=? AND version=?",
            (evidence_id, version),
        )

    def evidence_versions(self, evidence_id):
        return self._all(
            "SELECT * FROM evidence_version WHERE evidence_id=? ORDER BY version",
            (evidence_id,),
        )

    def set_evidence_state(self, evidence_id, state, ts) -> None:
        self._execute(
            "INSERT INTO evidence_state(evidence_id, state, updated_at) VALUES(?,?,?)"
            " ON CONFLICT(evidence_id) DO UPDATE SET state=excluded.state, updated_at=excluded.updated_at",
            (evidence_id, state, ts),
        )

    def get_evidence_state(self, evidence_id):
        row = self._one("SELECT * FROM evidence_state WHERE evidence_id=?", (evidence_id,))
        return row["state"] if row else None

    def confirmed_evidence(self, milestone_id, requirement):
        """某里程碑某要求下，当前仍有效的已确认证据版本（含确认单号）。"""
        return self._all(
            "SELECT ev.*, c.confirmation_id FROM evidence_version ev"
            " JOIN evidence_state es ON es.evidence_id=ev.evidence_id AND es.state='confirmed'"
            " JOIN confirmation c ON c.evidence_id=ev.evidence_id AND c.evidence_version=ev.version"
            "   AND c.status='active' AND c.decision='confirm'"
            " WHERE ev.milestone_id=? AND ev.requirement=?",
            (milestone_id, requirement),
        )

    # ---- 确认 ----

    def insert_confirmation(self, confirmation_id, evidence_id, evidence_version,
                            party, role, decision, status, reason, ts, actor) -> None:
        self._execute(
            "INSERT INTO confirmation(confirmation_id, evidence_id, evidence_version, party,"
            " role, decision, status, reason, ts, actor) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (confirmation_id, evidence_id, evidence_version, party, role,
             decision, status, reason, ts, actor),
        )

    def get_confirmation(self, confirmation_id):
        return self._one("SELECT * FROM confirmation WHERE confirmation_id=?", (confirmation_id,))

    def mark_confirmation_revoked(self, confirmation_id, status, ts, actor, reason) -> None:
        self._execute(
            "UPDATE confirmation SET status=?, revoke_ts=?, revoke_actor=?, revoke_reason=?"
            " WHERE confirmation_id=?",
            (status, ts, actor, reason, confirmation_id),
        )

    def confirmations_for(self, evidence_id, version=None, active_only=False):
        sql = "SELECT * FROM confirmation WHERE evidence_id=?"
        params = [evidence_id]
        if version is not None:
            sql += " AND evidence_version=?"
            params.append(version)
        if active_only:
            sql += " AND status='active'"
        return self._all(sql + " ORDER BY ts, confirmation_id", params)

    # ---- 付款 ----

    def insert_payment(self, payment_id, milestone_id, amount, currency, state,
                       idempotency_key, note, requested_by, request_party, request_ts) -> None:
        self._execute(
            "INSERT INTO payment(payment_id, milestone_id, amount, currency, state,"
            " idempotency_key, note, requested_by, request_party, request_ts)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (payment_id, milestone_id, amount, currency, state,
             idempotency_key, note, requested_by, request_party, request_ts),
        )

    def get_payment(self, payment_id):
        return self._one("SELECT * FROM payment WHERE payment_id=?", (payment_id,))

    def payment_by_key(self, idempotency_key):
        return self._one("SELECT * FROM payment WHERE idempotency_key=?", (idempotency_key,))

    def set_payment_state(self, payment_id, state, confirmed_by=None,
                          confirm_party=None, confirm_ts=None) -> None:
        self._execute(
            "UPDATE payment SET state=?, confirmed_by=COALESCE(?, confirmed_by),"
            " confirm_party=COALESCE(?, confirm_party), confirm_ts=COALESCE(?, confirm_ts)"
            " WHERE payment_id=?",
            (state, confirmed_by, confirm_party, confirm_ts, payment_id),
        )

    def active_payment_total(self, milestone_id) -> float:
        row = self._one(
            "SELECT COALESCE(SUM(amount), 0) total FROM payment"
            " WHERE milestone_id=? AND state IN ('requested','confirmed','paid')",
            (milestone_id,),
        )
        return float(row["total"])

    def insert_payment_event(self, payment_id, event, actor, ts, detail: dict) -> None:
        self._execute(
            "INSERT INTO payment_event(payment_id, event, actor, ts, detail) VALUES(?,?,?,?,?)",
            (payment_id, event, actor, ts, json.dumps(detail, ensure_ascii=False, sort_keys=True)),
        )

    def payment_events(self, payment_id):
        return self._all(
            "SELECT * FROM payment_event WHERE payment_id=? ORDER BY seq", (payment_id,),
        )

    def insert_payment_decision(self, payment_id, snapshot: dict, ts) -> None:
        self._execute(
            "INSERT INTO payment_decision(payment_id, snapshot, ts) VALUES(?,?,?)",
            (payment_id, json.dumps(snapshot, ensure_ascii=False, sort_keys=True), ts),
        )

    def get_payment_decision(self, payment_id):
        row = self._one("SELECT * FROM payment_decision WHERE payment_id=?", (payment_id,))
        if row:
            row["snapshot"] = json.loads(row["snapshot"])
        return row

    def insert_reversal(self, reversal_id, payment_id, amount, reason, actor, ts) -> None:
        self._execute(
            "INSERT INTO reversal VALUES(?,?,?,?,?,?)",
            (reversal_id, payment_id, amount, reason, actor, ts),
        )

    def reversals_for(self, payment_id):
        return self._all("SELECT * FROM reversal WHERE payment_id=? ORDER BY ts", (payment_id,))

    # ---- 回调 ----

    def get_callback(self, idempotency_key):
        return self._one("SELECT * FROM callback_log WHERE idempotency_key=?", (idempotency_key,))

    def insert_callback(self, idempotency_key, payment_id, result, response: dict, ts) -> None:
        self._execute(
            "INSERT INTO callback_log VALUES(?,?,?,?,?)",
            (idempotency_key, payment_id, result,
             json.dumps(response, ensure_ascii=False, sort_keys=True), ts),
        )

    def close(self) -> None:
        with self.lock:
            self.connection.close()
