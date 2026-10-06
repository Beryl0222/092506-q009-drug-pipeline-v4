"""合作管线的持久化边界。

核心是一张只追加的事件日志：所有状态都由事件回放得到，任何命令都不会
UPDATE 或 DELETE 历史事件，撤回与冲正也只是追加新事件。在此之上还有：

- commands：命令幂等表。同一个 cmd_id 无论被重试多少次（并行提交、
  网络重试），只生效一次，重复请求拿到首次结果。
- outbox：事务发件箱。事件与待投递回调在同一个数据库事务里写入，
  进程在回调发出前崩溃，重启后仍能从发件箱恢复，结论不依赖回调是否送达。
- deliveries：每个目的地的投递游标。回调至少投递一次（at-least-once），
  目的地按 event_id 幂等消费，因此重复投递不会产生第二个副作用。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from .domain import Record, now_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_milestone (
    record_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    cmd_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT '',
    actor_roles TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS commands (
    cmd_id TEXT PRIMARY KEY,
    result TEXT NOT NULL DEFAULT '',
    first_seq INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_seq INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    destination TEXT PRIMARY KEY,
    last_delivered_seq INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT ''
);
"""


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        # 多线程可能并行提交；连接跨线程共享，所有写操作由 self.lock 串行化。
        self.connection = sqlite3.connect(str(path), check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        # autocommit 模式，事务边界由 begin/commit 显式控制。
        self.connection.isolation_level = None
        # 并行命令竞争写锁时等待，而不是立刻抛 database is locked。
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.lock = threading.RLock()
        self.connection.executescript(SCHEMA)

    # ------------------------------------------------------------------
    # 事务边界
    # ------------------------------------------------------------------
    def begin(self) -> None:
        self.connection.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    # ------------------------------------------------------------------
    # 遗留登记能力（保持既有行为）
    # ------------------------------------------------------------------
    def save(self, record: Record) -> Record:
        value = record.with_timestamp()
        with self.lock:
            self.begin()
            try:
                self.connection.execute(
                    "INSERT INTO pipeline_milestone(record_id, owner_id, state, created_at) "
                    "VALUES(?,?,?,?)",
                    (value.record_id, value.owner_id, value.state, value.created_at),
                )
                self.commit()
            except Exception:
                self.rollback()
                raise
        return value

    def get(self, record_id: str) -> Record | None:
        row = self.connection.execute(
            "SELECT record_id, owner_id, state, created_at "
            "FROM pipeline_milestone WHERE record_id=?",
            (record_id,),
        ).fetchone()
        return Record(**dict(row)) if row else None

    # ------------------------------------------------------------------
    # 事件日志（只追加）
    # ------------------------------------------------------------------
    def insert_event(self, event) -> int:
        cur = self.connection.execute(
            "INSERT INTO event_log(event_id, cmd_id, event_type, payload, actor, "
            "actor_roles, created_at) VALUES(?,?,?,?,?,?,?)",
            (
                event.event_id,
                event.cmd_id,
                event.event_type,
                json.dumps(event.payload, ensure_ascii=False, sort_keys=True),
                event.actor,
                json.dumps(list(event.actor_roles), ensure_ascii=False),
                event.created_at,
            ),
        )
        return int(cur.lastrowid)

    def all_events(self, after_seq: int = 0) -> list[dict]:
        rows = self.connection.execute(
            "SELECT seq, event_id, cmd_id, event_type, payload, actor, actor_roles, created_at "
            "FROM event_log WHERE seq > ? ORDER BY seq",
            (after_seq,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item["payload"])
            item["actor_roles"] = tuple(json.loads(item["actor_roles"]))
            result.append(item)
        return result

    def get_event(self, seq: int) -> dict | None:
        rows = self.all_events(seq - 1)
        return rows[0] if rows else None

    # ------------------------------------------------------------------
    # 幂等命令
    # ------------------------------------------------------------------
    def find_command(self, cmd_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT cmd_id, result, first_seq, created_at FROM commands WHERE cmd_id=?",
            (cmd_id,),
        ).fetchone()
        return dict(row) if row else None

    def insert_command(self, cmd_id: str, first_seq: int, result: dict) -> None:
        self.connection.execute(
            "INSERT INTO commands(cmd_id, result, first_seq, created_at) VALUES(?,?,?,?)",
            (cmd_id, json.dumps(result, ensure_ascii=False, sort_keys=True),
             first_seq, now_iso()),
        )

    def update_command_result(self, cmd_id: str, result: dict) -> None:
        self.connection.execute(
            "UPDATE commands SET result=? WHERE cmd_id=?",
            (json.dumps(result, ensure_ascii=False, sort_keys=True), cmd_id),
        )

    # ------------------------------------------------------------------
    # 事务发件箱与投递游标
    # ------------------------------------------------------------------
    def add_outbox(self, event_seq: int, event_id: str, destination: str,
                   payload: dict) -> None:
        ts = now_iso()
        self.connection.execute(
            "INSERT INTO outbox(event_seq, event_id, destination, payload, status, "
            "attempts, created_at, updated_at) VALUES(?,?,?,?, 'pending', 0, ?, ?)",
            (event_seq, event_id, destination,
             json.dumps(payload, ensure_ascii=False, sort_keys=True), ts, ts),
        )

    def pending_outbox(self, destination: str | None = None) -> list[dict]:
        if destination is None:
            rows = self.connection.execute(
                "SELECT id, event_seq, event_id, destination, payload, attempts, last_error "
                "FROM outbox WHERE status='pending' ORDER BY id").fetchall()
        else:
            rows = self.connection.execute(
                "SELECT id, event_seq, event_id, destination, payload, attempts, last_error "
                "FROM outbox WHERE status='pending' AND destination=? ORDER BY id",
                (destination,)).fetchall()
        items = [dict(r) for r in rows]
        for item in items:
            item["payload"] = json.loads(item["payload"])
        return items

    def mark_outbox_done(self, outbox_id: int) -> None:
        self.connection.execute(
            "UPDATE outbox SET status='done', updated_at=? WHERE id=?",
            (now_iso(), outbox_id))

    def mark_outbox_attempt(self, outbox_id: int, error: str) -> None:
        self.connection.execute(
            "UPDATE outbox SET attempts=attempts+1, last_error=?, updated_at=? WHERE id=?",
            (error[:500], now_iso(), outbox_id))

    def delivery_cursor(self, destination: str) -> int:
        row = self.connection.execute(
            "SELECT last_delivered_seq FROM deliveries WHERE destination=?",
            (destination,)).fetchone()
        return int(row["last_delivered_seq"]) if row else 0

    def advance_delivery(self, destination: str, seq: int) -> None:
        self.connection.execute(
            "INSERT INTO deliveries(destination, last_delivered_seq, updated_at) "
            "VALUES(?,?,?) ON CONFLICT(destination) DO UPDATE SET "
            "last_delivered_seq=excluded.last_delivered_seq, updated_at=excluded.updated_at",
            (destination, seq, now_iso()))

    def close(self) -> None:
        self.connection.close()
