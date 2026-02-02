from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from broodmind.config.settings import Settings
from broodmind.store.base import Store
from broodmind.store.models import (
    AuditEvent,
    IntentRecord,
    MemoryEntry,
    PermitRecord,
    WorkerRecord,
    WorkerTemplateRecord,
)
from broodmind.utils import utc_now


class SQLiteStore(Store):
    def __init__(self, settings: Settings) -> None:
        settings.state_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = settings.state_dir / "broodmind.db"
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                task TEXT NOT NULL,
                granted_caps_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                summary TEXT,
                output_json TEXT,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS intents (
                id TEXT PRIMARY KEY,
                worker_id TEXT NOT NULL,
                type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                risk TEXT NOT NULL,
                requires_approval INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS permits (
                id TEXT PRIMARY KEY,
                intent_id TEXT NOT NULL,
                intent_type TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                consumed_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_events (
                id TEXT PRIMARY KEY,
                ts TEXT NOT NULL,
                correlation_id TEXT,
                level TEXT NOT NULL,
                event_type TEXT NOT NULL,
                data_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS memory_entries (
                id INTEGER PRIMARY KEY,
                uuid TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                embedding_json TEXT,
                created_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS worker_templates (
                id TEXT PRIMARY KEY,
...
            );

            CREATE INDEX IF NOT EXISTS ix_workers_status_updated_at ON workers (status, updated_at);
            CREATE INDEX IF NOT EXISTS ix_memory_entries_id ON memory_entries (id);
            """
        )
        self._conn.commit()
        self._ensure_schema_upgrades()

    def _ensure_schema_upgrades(self) -> None:
        try:
            self._conn.execute("ALTER TABLE permits ADD COLUMN intent_type TEXT NOT NULL DEFAULT ''")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE chat_state ADD COLUMN bootstrap_hash TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        
        # Migration for memory_entries table for robust ordering
        try:
            cursor = self._conn.execute("PRAGMA table_info(memory_entries)")
            columns = [row['name'] for row in cursor.fetchall()]
            is_old_schema = 'uuid' not in columns and 'id' in columns
            
            if is_old_schema:
                self._conn.executescript("""
                    ALTER TABLE memory_entries RENAME TO _memory_entries_old;
                    
                    CREATE TABLE memory_entries (
                        id INTEGER PRIMARY KEY,
                        uuid TEXT NOT NULL UNIQUE,
                        role TEXT NOT NULL,
                        content TEXT NOT NULL,
                        embedding_json TEXT,
                        created_at TEXT NOT NULL,
                        metadata_json TEXT NOT NULL
                    );

                    INSERT INTO memory_entries (uuid, role, content, embedding_json, created_at, metadata_json)
                    SELECT id, role, content, embedding_json, created_at, metadata_json FROM _memory_entries_old;
                    
                    DROP TABLE _memory_entries_old;

                    DROP INDEX IF EXISTS ix_memory_entries_created_at;
                """)
                self._conn.commit()
                # The new index on id will be created by the main _init_schema script
        except Exception as e:
            # This might fail if run in a transaction, but we commit after.
            # It's a complex operation, so we log if it fails.
            import logging
            logging.getLogger(__name__).warning("Memory schema migration failed (this may be ok if table was empty): %s", e)


        # Add worker result fields
        try:
            self._conn.execute("ALTER TABLE workers ADD COLUMN summary TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE workers ADD COLUMN output_json TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE workers ADD COLUMN error TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass

    def create_worker(self, record: WorkerRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO workers (id, status, task, granted_caps_json, created_at, updated_at, summary, output_json, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.status,
                record.task,
                json.dumps(record.granted_caps),
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                record.summary,
                json.dumps(record.output) if record.output else None,
                record.error,
            ),
        )
        self._conn.commit()

    def update_worker_status(self, worker_id: str, status: str) -> None:
        self._conn.execute(
            "UPDATE workers SET status = ?, updated_at = ? WHERE id = ?",
            (status, utc_now().isoformat(), worker_id),
        )
        self._conn.commit()

    def update_worker_result(
        self,
        worker_id: str,
        summary: str | None = None,
        output: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        updates = ["updated_at = ?"]
        params = [utc_now().isoformat()]

        if summary is not None:
            updates.append("summary = ?")
            params.append(summary)
        if output is not None:
            updates.append("output_json = ?")
            params.append(json.dumps(output))
        if error is not None:
            updates.append("error = ?")
            params.append(error)

        params.append(worker_id)
        self._conn.execute(
            f"UPDATE workers SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        self._conn.commit()

    def get_worker(self, worker_id: str) -> WorkerRecord | None:
        cursor = self._conn.execute("SELECT * FROM workers WHERE id = ?", (worker_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_worker(row)

    def get_active_workers(self, older_than_minutes: int = 10) -> list[WorkerRecord]:
        """Get workers that are still running or recently completed."""
        cursor = self._conn.execute(
            """
            SELECT * FROM workers
            WHERE status IN ('started', 'running')
               OR updated_at > datetime('now', '-' || ? || ' minutes')
            ORDER BY updated_at DESC
            """,
            (older_than_minutes,),
        )
        return [self._row_to_worker(row) for row in cursor.fetchall()]

    def cleanup_old_workers(self, keep_recent_hours: int = 24, keep_completed_count: int = 100) -> int:
        """
        Cleanup old worker records to prevent database bloat and reduce context confusion.

        Keeps:
        - All workers from the last N hours (default: 24)
        - The last N completed workers (default: 100)
        - All failed/stopped workers (for debugging)

        Returns: Number of workers deleted
        """
        # Delete old completed workers that are not in the recent time window
        # and not in the last N completed workers
        cursor = self._conn.execute(
            f"""
            DELETE FROM workers
            WHERE status = 'completed'
              AND updated_at < datetime('now', '-{keep_recent_hours} hours')
              AND id NOT IN (
                  SELECT id FROM workers
                  WHERE status = 'completed'
                  ORDER BY updated_at DESC
                  LIMIT {keep_completed_count}
              )
            """
        )
        deleted_count = cursor.rowcount
        self._conn.commit()
        return deleted_count

    def list_workers(self) -> list[WorkerRecord]:
        cursor = self._conn.execute("SELECT * FROM workers ORDER BY created_at DESC")
        return [self._row_to_worker(row) for row in cursor.fetchall()]

    def save_intent(self, record: IntentRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO intents (id, worker_id, type, payload_json, payload_hash, risk, requires_approval, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.worker_id,
                record.type,
                json.dumps(record.payload),
                record.payload_hash,
                record.risk,
                1 if record.requires_approval else 0,
                record.status,
                record.created_at.isoformat(),
            ),
        )
        self._conn.commit()

    def update_intent_status(self, intent_id: str, status: str) -> None:
        self._conn.execute(
            "UPDATE intents SET status = ? WHERE id = ?",
            (status, intent_id),
        )
        self._conn.commit()

    def create_permit(self, record: PermitRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO permits (id, intent_id, intent_type, worker_id, payload_hash, expires_at, consumed_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.intent_id,
                record.intent_type,
                record.worker_id,
                record.payload_hash,
                record.expires_at.isoformat(),
                record.consumed_at.isoformat() if record.consumed_at else None,
                record.created_at.isoformat(),
            ),
        )
        self._conn.commit()

    def consume_permit_atomic(self, permit_id: str, now: datetime) -> bool:
        cursor = self._conn.execute(
            """
            UPDATE permits
            SET consumed_at = ?
            WHERE id = ? AND consumed_at IS NULL AND expires_at > ?
            """,
            (now.isoformat(), permit_id, now.isoformat()),
        )
        self._conn.commit()
        return cursor.rowcount > 0

    def get_permit(self, permit_id: str, now: datetime) -> PermitRecord | None:
        cursor = self._conn.execute("SELECT * FROM permits WHERE id = ?", (permit_id,))
        row = cursor.fetchone()
        if not row:
            return None
        record = self._row_to_permit(row)
        if record.consumed_at is not None:
            return None
        if record.expires_at <= now:
            return None
        return record

    def append_audit(self, event: AuditEvent) -> None:
        self._conn.execute(
            """
            INSERT INTO audit_events (id, ts, correlation_id, level, event_type, data_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.id,
                event.ts.isoformat(),
                event.correlation_id,
                event.level,
                event.event_type,
                json.dumps(event.data),
            ),
        )
        self._conn.commit()

    def list_audit(self, limit: int = 100) -> list[AuditEvent]:
        cursor = self._conn.execute(
            "SELECT * FROM audit_events ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return [self._row_to_audit(row) for row in cursor.fetchall()]

    def get_audit(self, event_id: str) -> AuditEvent | None:
        cursor = self._conn.execute("SELECT * FROM audit_events WHERE id = ?", (event_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_audit(row)

    def upsert_worker_template(self, record: WorkerTemplateRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO worker_templates (
                id, name, description, system_prompt, available_tools_json,
                required_permissions_json, max_thinking_steps, default_timeout_seconds,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                system_prompt = excluded.system_prompt,
                available_tools_json = excluded.available_tools_json,
                required_permissions_json = excluded.required_permissions_json,
                max_thinking_steps = excluded.max_thinking_steps,
                default_timeout_seconds = excluded.default_timeout_seconds,
                updated_at = excluded.updated_at
            """,
            (
                record.id,
                record.name,
                record.description,
                record.system_prompt,
                json.dumps(record.available_tools),
                json.dumps(record.required_permissions),
                record.max_thinking_steps,
                record.default_timeout_seconds,
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
            ),
        )
        self._conn.commit()

    def list_worker_templates(self) -> list[WorkerTemplateRecord]:
        cursor = self._conn.execute("SELECT * FROM worker_templates ORDER BY updated_at DESC")
        return [self._row_to_worker_template(row) for row in cursor.fetchall()]

    def get_worker_template(self, template_id: str) -> WorkerTemplateRecord | None:
        cursor = self._conn.execute("SELECT * FROM worker_templates WHERE id = ?", (template_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_worker_template(row)

        return self._row_to_worker_template(row)

    def delete_worker_template(self, template_id: str) -> None:
        """Delete a worker template by ID."""
        self._conn.execute("DELETE FROM worker_templates WHERE id = ?", (template_id,))
        self._conn.commit()

    def add_memory_entry(self, entry: MemoryEntry) -> None:
        self._conn.execute(
            """
            INSERT INTO memory_entries (uuid, role, content, embedding_json, created_at, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                entry.id, # The dataclass id is the UUID
                entry.role,
                entry.content,
                json.dumps(entry.embedding) if entry.embedding is not None else None,
                entry.created_at.isoformat(),
                json.dumps(entry.metadata),
            ),
        )
        self._conn.commit()

    def list_memory_entries(self, limit: int = 200) -> list[MemoryEntry]:
        cursor = self._conn.execute(
            "SELECT * FROM memory_entries ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [self._row_to_memory(row) for row in cursor.fetchall()]

    def list_memory_entries_by_chat(self, chat_id: int, limit: int = 50) -> list[MemoryEntry]:
        needle = f"\"chat_id\": {chat_id}"
        cursor = self._conn.execute(
            "SELECT * FROM memory_entries WHERE metadata_json LIKE ? ORDER BY id DESC LIMIT ?",
            (f"%{needle}%", limit),
        )
        return [self._row_to_memory(row) for row in cursor.fetchall()]

    def is_chat_bootstrapped(self, chat_id: int) -> bool:
        cursor = self._conn.execute(
            "SELECT bootstrapped_at, bootstrap_hash FROM chat_state WHERE chat_id = ?",
            (chat_id,),
        )
        row = cursor.fetchone()
        return bool(row and (row["bootstrap_hash"] or row["bootstrapped_at"]))

    def mark_chat_bootstrapped(self, chat_id: int, ts: datetime) -> None:
        self._conn.execute(
            """
            INSERT INTO chat_state (chat_id, bootstrapped_at)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET bootstrapped_at = excluded.bootstrapped_at
            """,
            (chat_id, ts.isoformat()),
        )
        self._conn.commit()

    def get_chat_bootstrap_hash(self, chat_id: int) -> str | None:
        cursor = self._conn.execute(
            "SELECT bootstrap_hash FROM chat_state WHERE chat_id = ?",
            (chat_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return row["bootstrap_hash"]

    def set_chat_bootstrap_hash(self, chat_id: int, bootstrap_hash: str, ts: datetime) -> None:
        self._conn.execute(
            """
            INSERT INTO chat_state (chat_id, bootstrapped_at, bootstrap_hash)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                bootstrapped_at = excluded.bootstrapped_at,
                bootstrap_hash = excluded.bootstrap_hash
            """,
            (chat_id, ts.isoformat(), bootstrap_hash),
        )
        self._conn.commit()

    def _row_to_worker(self, row: sqlite3.Row) -> WorkerRecord:
        return WorkerRecord(
            id=row["id"],
            status=row["status"],
            task=row["task"],
            granted_caps=_loads_json(row["granted_caps_json"]),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
            summary=row["summary"] if "summary" in row.keys() else None,
            output=_loads_json(row["output_json"]) if "output_json" in row.keys() and row["output_json"] else None,
            error=row["error"] if "error" in row.keys() else None,
        )

    def _row_to_intent(self, row: sqlite3.Row) -> IntentRecord:
        return IntentRecord(
            id=row["id"],
            worker_id=row["worker_id"],
            type=row["type"],
            payload=_loads_json(row["payload_json"]),
            payload_hash=row["payload_hash"],
            risk=row["risk"],
            requires_approval=bool(row["requires_approval"]),
            status=row["status"],
            created_at=_parse_dt(row["created_at"]),
        )

    def _row_to_permit(self, row: sqlite3.Row) -> PermitRecord:
        intent_type = row["intent_type"] if "intent_type" in row.keys() else ""
        return PermitRecord(
            id=row["id"],
            intent_id=row["intent_id"],
            intent_type=intent_type,
            worker_id=row["worker_id"],
            payload_hash=row["payload_hash"],
            expires_at=_parse_dt(row["expires_at"]),
            consumed_at=_parse_dt(row["consumed_at"]) if row["consumed_at"] else None,
            created_at=_parse_dt(row["created_at"]),
        )

    def _row_to_audit(self, row: sqlite3.Row) -> AuditEvent:
        return AuditEvent(
            id=row["id"],
            ts=_parse_dt(row["ts"]),
            correlation_id=row["correlation_id"],
            level=row["level"],
            event_type=row["event_type"],
            data=_loads_json(row["data_json"]),
        )

    def _row_to_memory(self, row: sqlite3.Row) -> MemoryEntry:
        embedding = None
        if row["embedding_json"]:
            embedding = json.loads(row["embedding_json"])
        return MemoryEntry(
            id=row["uuid"],
            role=row["role"],
            content=row["content"],
            embedding=embedding,
            created_at=_parse_dt(row["created_at"]),
            metadata=_loads_json(row["metadata_json"]),
        )

    def _row_to_worker_template(self, row: sqlite3.Row) -> WorkerTemplateRecord:
        return WorkerTemplateRecord(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            system_prompt=row["system_prompt"],
            available_tools=_loads_json(row["available_tools_json"]),
            required_permissions=_loads_json(row["required_permissions_json"]),
            max_thinking_steps=_row_get(row, "max_thinking_steps", 10),
            default_timeout_seconds=_row_get(row, "default_timeout_seconds", 300),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
        )


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    """Safely get a value from a sqlite3.Row with a default."""
    try:
        return row[key]
    except (IndexError, KeyError):
        return default


def _loads_json(value: Any) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return json.loads(value)
