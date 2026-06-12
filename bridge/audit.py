from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Optional

from .protocol import now_seconds


SCHEMA = """
CREATE TABLE IF NOT EXISTS transfer_tasks (
    task_id TEXT PRIMARY KEY,
    broker TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    requester TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_id TEXT,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    downloaded_at INTEGER,
    cleaned_at INTEGER,
    file_size INTEGER,
    sha256 TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    local_path TEXT,
    part_path TEXT,
    received_chunks INTEGER NOT NULL DEFAULT 0,
    received_bytes INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_transfer_tasks_status
ON transfer_tasks(status);
"""


class AuditStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(SCHEMA)
            self._migrate(connection)

    @staticmethod
    def _migrate(connection: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(transfer_tasks)").fetchall()
        }
        additions = {
            "part_path": "TEXT",
            "received_chunks": "INTEGER NOT NULL DEFAULT 0",
            "received_bytes": "INTEGER NOT NULL DEFAULT 0",
        }
        for name, definition in additions.items():
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE transfer_tasks ADD COLUMN {name} {definition}"
                )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def create(
        self,
        task_id: str,
        broker: str,
        agent_id: str,
        file_id: str,
        requester: str,
        created_at: int,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO transfer_tasks (
                    task_id, broker, agent_id, file_id, requester, status, created_at
                ) VALUES (?, ?, ?, ?, ?, 'queued', ?)
                """,
                (task_id, broker, agent_id, file_id, requester, created_at),
            )

    def update(self, task_id: str, status: Optional[str] = None, **fields: Any) -> None:
        values = dict(fields)
        if status is not None:
            values["status"] = status
        if not values:
            return
        assignments = ", ".join(f"{name} = ?" for name in values)
        with self._connect() as connection:
            connection.execute(
                f"UPDATE transfer_tasks SET {assignments} WHERE task_id = ?",
                (*values.values(), task_id),
            )

    def transition(self, task_id: str, status: str, **fields: Any) -> None:
        timestamp_fields = {
            "claimed": "started_at",
            "running": "started_at",
            "stored_local": "completed_at",
            "downloaded": "downloaded_at",
            "cleaned": "cleaned_at",
        }
        timestamp_field = timestamp_fields.get(status)
        if timestamp_field and timestamp_field not in fields:
            fields[timestamp_field] = now_seconds()
        self.update(task_id, status=status, **fields)

    def get(self, task_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM transfer_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        return dict(row) if row else {}

    def active(self) -> list[dict[str, Any]]:
        terminal = ("downloaded", "cleaned", "failed", "cancelled", "expired")
        placeholders = ",".join("?" for _ in terminal)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM transfer_tasks
                WHERE status NOT IN ({placeholders})
                ORDER BY created_at
                """,
                terminal,
            ).fetchall()
        return [dict(row) for row in rows]

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT task_id, broker, agent_id, file_id, requester, status,
                       attempt_id, created_at, started_at, completed_at,
                       downloaded_at, cleaned_at, file_size, sha256,
                       attempt_count, error_message, received_chunks,
                       received_bytes
                FROM transfer_tasks
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (max(1, min(limit, 200)),),
            ).fetchall()
        return [dict(row) for row in rows]
