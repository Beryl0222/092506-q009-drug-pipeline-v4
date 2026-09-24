"""合作管线与里程碑的本地持久化边界。"""
import sqlite3
from pathlib import Path
from .domain import Record


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.connection = sqlite3.connect(str(path))
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_milestone (
                record_id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        self.connection.commit()

    def save(self, record: Record) -> Record:
        value = record.with_timestamp()
        self.connection.execute(
            "INSERT INTO pipeline_milestone(record_id, owner_id, state, created_at) VALUES(?,?,?,?)",
            (value.record_id, value.owner_id, value.state, value.created_at),
        )
        self.connection.commit()
        return value

    def get(self, record_id: str) -> Record | None:
        row = self.connection.execute(
            "SELECT record_id, owner_id, state, created_at FROM pipeline_milestone WHERE record_id=?",
            (record_id,),
        ).fetchone()
        return Record(**dict(row)) if row else None

    def close(self) -> None:
        self.connection.close()
