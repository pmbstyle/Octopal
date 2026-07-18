from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any

from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.store.base import UNSET, Store
from octopal.infrastructure.store.models import (
    AdaptationCandidateRecord,
    AdaptationEvaluationRecord,
    AdaptationFailureClusterRecord,
    AuditEvent,
    ExecutionEpisodeEvidenceMetadata,
    ExecutionEpisodeEvidenceRecord,
    ExecutionEpisodeRecord,
    IntentRecord,
    MCPTaskRecord,
    MemoryEntry,
    MemoryFactRecord,
    MemoryFactSourceRecord,
    OctoDiaryEntryRecord,
    OperationalMemoryItemRecord,
    PermitRecord,
    PlanEventRecord,
    PlanRunRecord,
    PlanStepRecord,
    ProceduralRecipeEvaluationRecord,
    ProceduralRecipeRecord,
    WorkerRecord,
    WorkerTemplateRecord,
)
from octopal.runtime.workers.loader import discover_worker_templates
from octopal.runtime.workers.loader import get_worker_template as get_template_from_fs
from octopal.utils import utc_now


class EvidenceSecurePurgeIncomplete(RuntimeError):
    """The row was deleted, but SQLite could not purge stale WAL pages."""


class LockedCursor:
    def __init__(self, cursor: sqlite3.Cursor, lock: threading.RLock) -> None:
        self._cursor = cursor
        self._lock = lock

    def fetchone(self) -> Any:
        with self._lock:
            return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        with self._lock:
            return self._cursor.fetchall()

    def fetchmany(self, size: int | None = None) -> list[Any]:
        with self._lock:
            if size is None:
                return self._cursor.fetchmany()
            return self._cursor.fetchmany(size)

    @property
    def rowcount(self) -> int:
        with self._lock:
            return self._cursor.rowcount

    def __iter__(self):
        with self._lock:
            return iter(list(self._cursor))

    def __getattr__(self, item: str) -> Any:
        return getattr(self._cursor, item)


class LockedConnection:
    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock) -> None:
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_lock", lock)

    def execute(self, *args: Any, **kwargs: Any) -> LockedCursor:
        with self._lock:
            cursor = self._conn.execute(*args, **kwargs)
            return LockedCursor(cursor, self._lock)

    def executescript(self, *args: Any, **kwargs: Any) -> LockedCursor:
        with self._lock:
            cursor = self._conn.executescript(*args, **kwargs)
            return LockedCursor(cursor, self._lock)

    def commit(self) -> None:
        with self._lock:
            self._conn.commit()

    def rollback(self) -> None:
        with self._lock:
            self._conn.rollback()

    def __getattr__(self, item: str) -> Any:
        return getattr(self._conn, item)

    def __setattr__(self, key: str, value: Any) -> None:
        if key in {"_conn", "_lock"}:
            object.__setattr__(self, key, value)
        else:
            setattr(self._conn, key, value)


class SQLiteStore(Store):
    def __init__(self, settings: Settings) -> None:
        settings.state_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = settings.state_dir / "octopal.db"
        self._workspace_dir = settings.workspace_dir
        self._lock = threading.RLock()
        self._evidence_secure_purge_pending = False
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn = LockedConnection(conn, self._lock)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.execute("PRAGMA secure_delete=ON;")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                task TEXT NOT NULL,
                granted_caps_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                summary TEXT,
                output_json TEXT,
                error TEXT,
                tools_used_json TEXT,
                lineage_id TEXT,
                parent_worker_id TEXT,
                root_task_id TEXT,
                spawn_depth INTEGER NOT NULL DEFAULT 0,
                template_id TEXT,
                template_name TEXT
            );

            CREATE TABLE IF NOT EXISTS intents (
                id TEXT PRIMARY KEY,
                worker_id TEXT NOT NULL,
                type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                risk TEXT NOT NULL,
                requires_approval INTEGER NOT NULL,
                memory_influence_ids_json TEXT NOT NULL DEFAULT '[]',
                procedural_recipe_ids_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS execution_episodes (
                id TEXT PRIMARY KEY,
                worker_run_id TEXT NOT NULL,
                task_fingerprint TEXT NOT NULL,
                environment_fingerprint TEXT NOT NULL,
                capability_fingerprint TEXT NOT NULL,
                result_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                trust_state TEXT NOT NULL,
                correlation_id TEXT,
                template_id TEXT,
                model TEXT,
                trajectory_refs_json TEXT NOT NULL,
                result_metadata_json TEXT NOT NULL,
                verification_json TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_execution_episodes_worker_created
                ON execution_episodes (worker_run_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS ix_execution_episodes_task_created
                ON execution_episodes (task_fingerprint, created_at DESC);

            CREATE TABLE IF NOT EXISTS adaptation_failure_clusters (
                id TEXT PRIMARY KEY,
                signature TEXT NOT NULL,
                source_summary_fingerprint TEXT NOT NULL,
                failure_categories_json TEXT NOT NULL,
                scenario_ids_json TEXT NOT NULL,
                task_fingerprints_json TEXT NOT NULL,
                trial_refs_json TEXT NOT NULL,
                trial_count INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_adaptation_failure_clusters_signature
                ON adaptation_failure_clusters (signature, created_at DESC);

            CREATE TRIGGER IF NOT EXISTS adaptation_failure_clusters_immutable
            BEFORE UPDATE ON adaptation_failure_clusters
            BEGIN
                SELECT RAISE(ABORT, 'adaptation failure clusters are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS adaptation_failure_clusters_no_delete
            BEFORE DELETE ON adaptation_failure_clusters
            BEGIN
                SELECT RAISE(ABORT, 'adaptation failure clusters cannot be deleted');
            END;

            CREATE TABLE IF NOT EXISTS adaptation_candidates (
                id TEXT PRIMARY KEY,
                family_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                kind TEXT NOT NULL,
                target TEXT NOT NULL,
                artifact_fingerprint TEXT NOT NULL,
                definition_fingerprint TEXT NOT NULL UNIQUE,
                hypothesis TEXT NOT NULL,
                change_json TEXT NOT NULL,
                source_cluster_ids_json TEXT NOT NULL,
                parent_id TEXT,
                status TEXT NOT NULL,
                evaluation_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(family_id, version),
                FOREIGN KEY(parent_id) REFERENCES adaptation_candidates(id)
            );

            CREATE INDEX IF NOT EXISTS ix_adaptation_candidates_family_version
                ON adaptation_candidates (family_id, version DESC);
            CREATE INDEX IF NOT EXISTS ix_adaptation_candidates_status_updated
                ON adaptation_candidates (status, updated_at DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS ux_adaptation_candidates_active_family
                ON adaptation_candidates (family_id)
                WHERE status = 'active';

            CREATE TRIGGER IF NOT EXISTS adaptation_candidates_definition_immutable
            BEFORE UPDATE ON adaptation_candidates
            WHEN NEW.id != OLD.id
              OR NEW.family_id != OLD.family_id
              OR NEW.version != OLD.version
              OR NEW.kind != OLD.kind
              OR NEW.target != OLD.target
              OR NEW.artifact_fingerprint != OLD.artifact_fingerprint
              OR NEW.definition_fingerprint != OLD.definition_fingerprint
              OR NEW.hypothesis != OLD.hypothesis
              OR NEW.change_json != OLD.change_json
              OR NEW.source_cluster_ids_json != OLD.source_cluster_ids_json
              OR COALESCE(NEW.parent_id, '') != COALESCE(OLD.parent_id, '')
              OR NEW.created_at != OLD.created_at
            BEGIN
                SELECT RAISE(ABORT, 'adaptation candidate definition is immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS adaptation_candidates_no_delete
            BEFORE DELETE ON adaptation_candidates
            BEGIN
                SELECT RAISE(ABORT, 'adaptation candidates cannot be deleted');
            END;

            CREATE TABLE IF NOT EXISTS adaptation_evaluations (
                id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                baseline_fingerprint TEXT NOT NULL,
                candidate_fingerprint TEXT NOT NULL,
                scenario_set_fingerprint TEXT NOT NULL,
                common_trial_count INTEGER NOT NULL,
                distinct_scenario_count INTEGER NOT NULL,
                baseline_success_rate REAL NOT NULL,
                candidate_success_rate REAL NOT NULL,
                success_rate_delta REAL NOT NULL,
                regression_count INTEGER NOT NULL,
                improvement_count INTEGER NOT NULL,
                held_out INTEGER NOT NULL,
                passed INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(candidate_id) REFERENCES adaptation_candidates(id)
            );

            CREATE INDEX IF NOT EXISTS ix_adaptation_evaluations_candidate_created
                ON adaptation_evaluations (candidate_id, created_at DESC);

            CREATE TRIGGER IF NOT EXISTS adaptation_evaluations_immutable
            BEFORE UPDATE ON adaptation_evaluations
            BEGIN
                SELECT RAISE(ABORT, 'adaptation evaluations are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS adaptation_evaluations_no_delete
            BEFORE DELETE ON adaptation_evaluations
            BEGIN
                SELECT RAISE(ABORT, 'adaptation evaluations cannot be deleted');
            END;

            CREATE TABLE IF NOT EXISTS procedural_recipes (
                id TEXT PRIMARY KEY,
                intent_fingerprint TEXT NOT NULL,
                definition_fingerprint TEXT NOT NULL,
                applicability_conditions_json TEXT NOT NULL,
                required_capabilities_json TEXT NOT NULL,
                required_permissions_json TEXT NOT NULL,
                strategy_steps_json TEXT NOT NULL,
                verification_contract_json TEXT NOT NULL,
                known_failures_json TEXT NOT NULL,
                invalidating_conditions_json TEXT NOT NULL,
                source_episode_ids_json TEXT NOT NULL,
                success_count INTEGER NOT NULL,
                failure_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                last_validated_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_procedural_recipes_intent_status
                ON procedural_recipes (intent_fingerprint, status, updated_at DESC);

            CREATE UNIQUE INDEX IF NOT EXISTS ux_procedural_recipes_active_intent
                ON procedural_recipes (intent_fingerprint)
                WHERE status = 'active';

            CREATE TRIGGER IF NOT EXISTS procedural_recipes_definition_immutable
            BEFORE UPDATE ON procedural_recipes
            WHEN NEW.id != OLD.id
              OR NEW.intent_fingerprint != OLD.intent_fingerprint
              OR NEW.definition_fingerprint != OLD.definition_fingerprint
              OR NEW.applicability_conditions_json != OLD.applicability_conditions_json
              OR NEW.required_capabilities_json != OLD.required_capabilities_json
              OR NEW.required_permissions_json != OLD.required_permissions_json
              OR NEW.strategy_steps_json != OLD.strategy_steps_json
              OR NEW.verification_contract_json != OLD.verification_contract_json
              OR NEW.known_failures_json != OLD.known_failures_json
              OR NEW.invalidating_conditions_json != OLD.invalidating_conditions_json
              OR NEW.source_episode_ids_json != OLD.source_episode_ids_json
              OR NEW.created_at != OLD.created_at
            BEGIN
                SELECT RAISE(ABORT, 'procedural recipe definition is immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS procedural_recipes_no_delete
            BEFORE DELETE ON procedural_recipes
            BEGIN
                SELECT RAISE(ABORT, 'procedural recipes cannot be deleted');
            END;

            CREATE TABLE IF NOT EXISTS procedural_recipe_evaluations (
                id TEXT PRIMARY KEY,
                recipe_id TEXT NOT NULL,
                baseline_fingerprint TEXT NOT NULL,
                candidate_fingerprint TEXT NOT NULL,
                scenario_set_fingerprint TEXT NOT NULL,
                common_trial_count INTEGER NOT NULL,
                baseline_success_rate REAL NOT NULL,
                candidate_success_rate REAL NOT NULL,
                regression_count INTEGER NOT NULL,
                improvement_count INTEGER NOT NULL,
                passed INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(recipe_id) REFERENCES procedural_recipes(id)
            );

            CREATE INDEX IF NOT EXISTS ix_procedural_recipe_evaluations_recipe_created
                ON procedural_recipe_evaluations (recipe_id, created_at DESC);

            CREATE TRIGGER IF NOT EXISTS procedural_recipe_evaluations_immutable
            BEFORE UPDATE ON procedural_recipe_evaluations
            BEGIN
                SELECT RAISE(ABORT, 'procedural recipe evaluations are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS procedural_recipe_evaluations_no_delete
            BEFORE DELETE ON procedural_recipe_evaluations
            BEGIN
                SELECT RAISE(ABORT, 'procedural recipe evaluations cannot be deleted');
            END;

            CREATE TABLE IF NOT EXISTS procedural_recipe_outcomes (
                recipe_id TEXT NOT NULL,
                episode_id TEXT NOT NULL,
                succeeded INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (recipe_id, episode_id),
                FOREIGN KEY(recipe_id) REFERENCES procedural_recipes(id),
                FOREIGN KEY(episode_id) REFERENCES execution_episodes(id)
            );

            CREATE TRIGGER IF NOT EXISTS procedural_recipe_outcomes_immutable
            BEFORE UPDATE ON procedural_recipe_outcomes
            BEGIN
                SELECT RAISE(ABORT, 'procedural recipe outcomes are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS procedural_recipe_outcomes_no_delete
            BEFORE DELETE ON procedural_recipe_outcomes
            BEGIN
                SELECT RAISE(ABORT, 'procedural recipe outcomes cannot be deleted');
            END;

            CREATE TRIGGER IF NOT EXISTS execution_episodes_reject_update
            BEFORE UPDATE ON execution_episodes
            BEGIN
                SELECT RAISE(ABORT, 'execution episodes are immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS execution_episodes_reject_delete
            BEFORE DELETE ON execution_episodes
            BEGIN
                SELECT RAISE(ABORT, 'execution episodes are immutable');
            END;

            CREATE TABLE IF NOT EXISTS execution_episode_evidence (
                episode_id TEXT PRIMARY KEY,
                algorithm TEXT NOT NULL,
                key_id TEXT NOT NULL,
                nonce BLOB NOT NULL,
                ciphertext BLOB NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY(episode_id) REFERENCES execution_episodes(id)
            );

            CREATE INDEX IF NOT EXISTS ix_execution_episode_evidence_expires
                ON execution_episode_evidence (expires_at);

            CREATE TRIGGER IF NOT EXISTS execution_episode_evidence_reject_update
            BEFORE UPDATE ON execution_episode_evidence
            BEGIN
                SELECT RAISE(ABORT, 'execution episode evidence is immutable');
            END;

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

            CREATE TABLE IF NOT EXISTS mcp_tasks (
                id TEXT PRIMARY KEY,
                server_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                protocol TEXT NOT NULL,
                remote_status TEXT NOT NULL,
                runtime_status TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                auth_context_id TEXT NOT NULL,
                correlation_id TEXT,
                trace_id TEXT,
                span_id TEXT,
                worker_run_id TEXT,
                chat_id INTEGER,
                chat_turn_id TEXT,
                plan_run_id TEXT,
                plan_step_id TEXT,
                status_message TEXT,
                ttl_ms INTEGER,
                poll_interval_ms INTEGER,
                input_requests_json TEXT NOT NULL,
                responded_input_keys_json TEXT NOT NULL DEFAULT '[]',
                result_json TEXT,
                error_json TEXT,
                remote_created_at TEXT NOT NULL,
                remote_updated_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(server_id, task_id, auth_context_id)
            );

            CREATE INDEX IF NOT EXISTS ix_mcp_tasks_recovery
                ON mcp_tasks (server_id, auth_context_id, remote_status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_mcp_tasks_worker
                ON mcp_tasks (worker_run_id, updated_at DESC);

            CREATE TABLE IF NOT EXISTS memory_entries (
                id INTEGER PRIMARY KEY,
                uuid TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                embedding_json TEXT,
                created_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS memory_embeddings (
                entry_uuid TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                vector_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(entry_uuid) REFERENCES memory_entries(uuid) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS canon_embeddings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                content TEXT NOT NULL,
                model TEXT NOT NULL,
                vector_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_canon_embeddings_filename ON canon_embeddings (filename);

            CREATE TABLE IF NOT EXISTS memory_facts (
                id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                subject TEXT NOT NULL,
                key TEXT NOT NULL,
                value_text TEXT NOT NULL,
                value_json TEXT,
                fact_type TEXT NOT NULL,
                confidence REAL NOT NULL,
                status TEXT NOT NULL,
                trust_state TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT,
                facets_json TEXT NOT NULL,
                source_kind TEXT,
                source_ref TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_memory_facts_owner_subject_key_status
                ON memory_facts (owner_id, subject, key, status);
            CREATE INDEX IF NOT EXISTS ix_memory_facts_owner_status_valid_to
                ON memory_facts (owner_id, status, valid_to);
            CREATE INDEX IF NOT EXISTS ix_memory_facts_source
                ON memory_facts (source_kind, source_ref, status);

            CREATE TABLE IF NOT EXISTS memory_fact_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fact_id TEXT NOT NULL,
                memory_entry_uuid TEXT,
                canon_filename TEXT,
                source_note TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(fact_id) REFERENCES memory_facts(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS ix_memory_fact_sources_fact_id ON memory_fact_sources (fact_id);

            CREATE TABLE IF NOT EXISTS octo_diary_entries (
                id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                chat_id INTEGER,
                kind TEXT NOT NULL,
                summary TEXT NOT NULL,
                details_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_octo_diary_entries_owner_chat_created
                ON octo_diary_entries (owner_id, chat_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS operational_memory_items (
                id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                chat_id INTEGER,
                kind TEXT NOT NULL,
                statement TEXT NOT NULL,
                next_action TEXT,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 2,
                confidence REAL NOT NULL,
                source_kind TEXT,
                source_ref TEXT,
                plan_run_id TEXT,
                plan_step_id TEXT,
                evidence_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                resolved_at TEXT
            );

            CREATE INDEX IF NOT EXISTS ix_operational_memory_owner_chat_status
                ON operational_memory_items (owner_id, chat_id, status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_operational_memory_plan
                ON operational_memory_items (plan_run_id, status);

            CREATE TABLE IF NOT EXISTS chat_state (
                chat_id INTEGER PRIMARY KEY,
                bootstrapped_at TEXT,
                bootstrap_hash TEXT
            );

            CREATE TABLE IF NOT EXISTS scheduled_tasks (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                frequency TEXT NOT NULL,
                worker_id TEXT,
                task_text TEXT NOT NULL,
                inputs_json TEXT,
                last_run_at TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                metadata_json TEXT,
                next_run_at TEXT,
                lease_owner TEXT,
                lease_expires_at TEXT,
                attempt_id TEXT,
                attempt_no INTEGER NOT NULL DEFAULT 0,
                last_outcome TEXT,
                last_error_class TEXT,
                last_started_at TEXT,
                last_completed_at TEXT,
                idempotency_key TEXT
            );

            CREATE TABLE IF NOT EXISTS plan_runs (
                id TEXT PRIMARY KEY,
                goal TEXT NOT NULL,
                status TEXT NOT NULL,
                chat_id INTEGER,
                source TEXT NOT NULL,
                correlation_id TEXT,
                current_step_id TEXT,
                plan_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS plan_steps (
                run_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                kind TEXT NOT NULL,
                title TEXT NOT NULL,
                status TEXT NOT NULL,
                task TEXT,
                executor TEXT,
                worker_run_id TEXT,
                input_json TEXT NOT NULL,
                output_json TEXT NOT NULL,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                PRIMARY KEY (run_id, step_id),
                FOREIGN KEY(run_id) REFERENCES plan_runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS plan_events (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                step_id TEXT,
                event_type TEXT NOT NULL,
                data_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES plan_runs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS ix_workers_status_updated_at ON workers (status, updated_at);
            CREATE INDEX IF NOT EXISTS ix_audit_events_correlation_ts
                ON audit_events (correlation_id, ts DESC);
            CREATE INDEX IF NOT EXISTS ix_memory_entries_id ON memory_entries (id);
            CREATE INDEX IF NOT EXISTS ix_plan_runs_status_updated_at
                ON plan_runs (status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_plan_runs_chat_status
                ON plan_runs (chat_id, status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_plan_steps_worker_run_id
                ON plan_steps (worker_run_id);
            CREATE INDEX IF NOT EXISTS ix_plan_events_run_created
                ON plan_events (run_id, created_at ASC);
            """)
        self._conn.commit()
        self._ensure_schema_upgrades()

    def _ensure_schema_upgrades(self) -> None:
        try:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS memory_embeddings (
                    entry_uuid TEXT PRIMARY KEY,
                    model TEXT NOT NULL,
                    vector_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(entry_uuid) REFERENCES memory_entries(uuid) ON DELETE CASCADE
                )
                """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS canon_embeddings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    model TEXT NOT NULL,
                    vector_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_canon_embeddings_filename ON canon_embeddings (filename)"
            )
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS memory_facts (
                    id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value_text TEXT NOT NULL,
                    value_json TEXT,
                    fact_type TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    status TEXT NOT NULL,
                    trust_state TEXT NOT NULL,
                    valid_from TEXT,
                    valid_to TEXT,
                    facets_json TEXT NOT NULL,
                    source_kind TEXT,
                    source_ref TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_memory_facts_owner_subject_key_status ON memory_facts (owner_id, subject, key, status)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_memory_facts_owner_status_valid_to ON memory_facts (owner_id, status, valid_to)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_memory_facts_source ON memory_facts (source_kind, source_ref, status)"
            )
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS memory_fact_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fact_id TEXT NOT NULL,
                    memory_entry_uuid TEXT,
                    canon_filename TEXT,
                    source_note TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(fact_id) REFERENCES memory_facts(id) ON DELETE CASCADE
                )
                """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_memory_fact_sources_fact_id ON memory_fact_sources (fact_id)"
            )
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS octo_diary_entries (
                    id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    chat_id INTEGER,
                    kind TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_octo_diary_entries_owner_chat_created ON octo_diary_entries (owner_id, chat_id, created_at DESC)"
            )
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS operational_memory_items (
                    id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    chat_id INTEGER,
                    kind TEXT NOT NULL,
                    statement TEXT NOT NULL,
                    next_action TEXT,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 2,
                    confidence REAL NOT NULL,
                    source_kind TEXT,
                    source_ref TEXT,
                    plan_run_id TEXT,
                    plan_step_id TEXT,
                    evidence_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    resolved_at TEXT
                )
                """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_operational_memory_owner_chat_status ON operational_memory_items (owner_id, chat_id, status, updated_at DESC)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_operational_memory_plan ON operational_memory_items (plan_run_id, status)"
            )
            self._conn.commit()
        except sqlite3.OperationalError:
            pass

        try:
            self._conn.execute(
                "ALTER TABLE permits ADD COLUMN intent_type TEXT NOT NULL DEFAULT ''"
            )
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute(
                "ALTER TABLE mcp_tasks ADD COLUMN "
                "responded_input_keys_json TEXT NOT NULL DEFAULT '[]'"
            )
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute(
                "ALTER TABLE intents ADD COLUMN memory_influence_ids_json TEXT NOT NULL DEFAULT '[]'"
            )
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute(
                "ALTER TABLE intents ADD COLUMN procedural_recipe_ids_json TEXT NOT NULL DEFAULT '[]'"
            )
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE chat_state ADD COLUMN bootstrap_hash TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass

        for column, definition in (
            ("next_run_at", "TEXT"),
            ("lease_owner", "TEXT"),
            ("lease_expires_at", "TEXT"),
            ("attempt_id", "TEXT"),
            ("attempt_no", "INTEGER NOT NULL DEFAULT 0"),
            ("last_outcome", "TEXT"),
            ("last_error_class", "TEXT"),
            ("last_started_at", "TEXT"),
            ("last_completed_at", "TEXT"),
            ("idempotency_key", "TEXT"),
        ):
            try:
                self._conn.execute(f"ALTER TABLE scheduled_tasks ADD COLUMN {column} {definition}")
                self._conn.commit()
            except sqlite3.OperationalError:
                pass
        try:
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_scheduled_tasks_due_lease "
                "ON scheduled_tasks (enabled, next_run_at, lease_expires_at)"
            )
            self._conn.commit()
        except sqlite3.OperationalError:
            pass

        try:
            self._conn.execute("ALTER TABLE memory_entries ADD COLUMN owner_id TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE memory_entries ADD COLUMN chat_id INTEGER")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_memory_entries_owner_id_id ON memory_entries (owner_id, id DESC)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_memory_entries_owner_chat_id_id ON memory_entries (owner_id, chat_id, id DESC)"
            )
            self._conn.commit()
        except sqlite3.OperationalError:
            pass

        # Migration for memory_entries table for robust ordering
        try:
            cursor = self._conn.execute("PRAGMA table_info(memory_entries)")
            columns = [row["name"] for row in cursor.fetchall()]
            is_old_schema = "uuid" not in columns and "id" in columns

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
            logging.getLogger(__name__).warning(
                "Memory schema migration failed (this may be ok if table was empty): %s", e
            )

        try:
            self._conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS memory_entries_fts USING fts5(
                    content,
                    owner_id UNINDEXED,
                    chat_id UNINDEXED,
                    entry_uuid UNINDEXED
                )
                """)
            self._conn.commit()
        except sqlite3.OperationalError:
            pass

        self._backfill_memory_scope_columns()
        self._rebuild_memory_fts()

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
        try:
            self._conn.execute("ALTER TABLE workers ADD COLUMN tools_used_json TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE workers ADD COLUMN lineage_id TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE workers ADD COLUMN parent_worker_id TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE workers ADD COLUMN root_task_id TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute(
                "ALTER TABLE workers ADD COLUMN spawn_depth INTEGER NOT NULL DEFAULT 0"
            )
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE workers ADD COLUMN template_id TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self._conn.execute("ALTER TABLE workers ADD COLUMN template_name TEXT")
            self._conn.commit()
        except sqlite3.OperationalError:
            pass

        self._ensure_memory_fact_trust_schema()

    def _ensure_memory_fact_trust_schema(self) -> None:
        columns = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(memory_facts)").fetchall()
        }
        trust_column_added = "trust_state" not in columns
        if trust_column_added:
            self._conn.execute(
                "ALTER TABLE memory_facts ADD COLUMN trust_state TEXT NOT NULL "
                "DEFAULT 'quarantined_candidate'"
            )

        allowed_origins = {
            "direct_user",
            "assistant_inference",
            "local_runtime_evidence",
            "worker",
            "connector",
            "mcp",
            "web",
            "document",
            "imported_canon",
        }
        origin_placeholders = ", ".join("?" for _ in allowed_origins)
        legacy_rows = self._conn.execute(
            "SELECT id, source_kind, source_ref FROM memory_facts "
            f"WHERE source_kind IS NULL OR source_kind NOT IN ({origin_placeholders})",
            tuple(sorted(allowed_origins)),
        ).fetchall()
        migrated_origins: dict[str, str] = {}
        for row in legacy_rows:
            source_kind = str(row["source_kind"] or "").strip()
            if source_kind == "canon":
                migrated_origins[str(row["id"])] = "imported_canon"
                continue
            if source_kind in allowed_origins:
                continue

            origin = "assistant_inference"
            if source_kind == "memory" and row["source_ref"]:
                memory_row = self._conn.execute(
                    "SELECT role, metadata_json FROM memory_entries WHERE uuid = ?",
                    (row["source_ref"],),
                ).fetchone()
                if memory_row is not None:
                    metadata = _loads_json(memory_row["metadata_json"], {})
                    explicit = str(metadata.get("memory_origin") or "").strip().lower()
                    if explicit in allowed_origins:
                        origin = explicit
                    elif metadata.get("worker_result"):
                        origin = "worker"
                    elif metadata.get("mcp_long_task"):
                        origin = "mcp"
                    elif memory_row["role"] == "user":
                        origin = "direct_user"
                    elif memory_row["role"] == "system":
                        origin = "local_runtime_evidence"
            migrated_origins[str(row["id"])] = origin

        for fact_id, origin in migrated_origins.items():
            self._conn.execute(
                "UPDATE memory_facts SET source_kind = ? WHERE id = ?",
                (origin, fact_id),
            )

        if trust_column_added:
            rows = self._conn.execute("SELECT id, status, source_kind FROM memory_facts").fetchall()
            quarantined_origins = {
                "assistant_inference",
                "worker",
                "connector",
                "mcp",
                "web",
                "document",
            }
            for row in rows:
                status = str(row["status"] or "").strip().lower()
                source_kind = str(row["source_kind"] or "").strip()
                if status == "superseded":
                    trust_state = "superseded"
                elif status in {"deprecated", "invalidated"}:
                    trust_state = "deprecated"
                elif status == "active" and source_kind == "imported_canon":
                    trust_state = "trusted"
                elif source_kind in quarantined_origins:
                    trust_state = "quarantined_candidate"
                else:
                    trust_state = "observed"
                self._conn.execute(
                    "UPDATE memory_facts SET trust_state = ? WHERE id = ?",
                    (trust_state, row["id"]),
                )

        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_memory_facts_owner_trust_status "
            "ON memory_facts (owner_id, trust_state, status, updated_at DESC)"
        )
        self._conn.commit()

    def create_worker(self, record: WorkerRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO workers (
                id, status, task, granted_caps_json, created_at, updated_at,
                summary, output_json, error, tools_used_json,
                lineage_id, parent_worker_id, root_task_id, spawn_depth,
                template_id, template_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                json.dumps(record.tools_used) if record.tools_used else None,
                record.lineage_id,
                record.parent_worker_id,
                record.root_task_id,
                int(record.spawn_depth or 0),
                record.template_id,
                record.template_name,
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
        tools_used: list[str] | None = None,
    ) -> None:
        updates = ["updated_at = ?"]
        params = [utc_now().isoformat()]

        if summary is not None:
            updates.append("summary = ?")
            params.append(summary)
        if output is not None:
            updates.append("output_json = ?")
            params.append(_safe_json_dumps(output))
        if error is not None:
            updates.append("error = ?")
            params.append(error)
        if tools_used is not None:
            updates.append("tools_used_json = ?")
            params.append(_safe_json_dumps([str(item) for item in tools_used]))

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
        cutoff = utc_now() - timedelta(minutes=max(0, older_than_minutes))
        cursor = self._conn.execute(
            """
            SELECT * FROM workers
            WHERE status IN ('started', 'running', 'waiting_for_children', 'awaiting_instruction')
               OR julianday(updated_at) > julianday(?)
            ORDER BY updated_at DESC
            """,
            (cutoff.isoformat(),),
        )
        return [self._row_to_worker(row) for row in cursor.fetchall()]

    def cleanup_old_workers(
        self, keep_recent_hours: int = 24, keep_completed_count: int = 100
    ) -> int:
        """
        Cleanup old worker records to prevent database bloat and reduce context confusion.

        Keeps:
        - All workers from the last N hours (default: 24)
        - The last N completed workers (default: 100)
        - All failed/stopped workers (for debugging)

        Returns: Number of workers deleted
        """
        safe_hours = max(0, keep_recent_hours)
        safe_keep_count = max(0, keep_completed_count)
        cutoff = utc_now() - timedelta(hours=safe_hours)

        # Delete old completed workers that are not in the recent time window
        # and not in the last N completed workers
        cursor = self._conn.execute(
            """
            DELETE FROM workers
            WHERE status = 'completed'
              AND julianday(updated_at) < julianday(?)
              AND id NOT IN (
                  SELECT id FROM workers
                  WHERE status = 'completed'
                  ORDER BY updated_at DESC
                  LIMIT ?
              )
            """,
            (cutoff.isoformat(), safe_keep_count),
        )
        deleted_count = cursor.rowcount
        self._conn.commit()
        return deleted_count

    def list_workers(self) -> list[WorkerRecord]:
        cursor = self._conn.execute("SELECT * FROM workers ORDER BY created_at DESC")
        return [self._row_to_worker(row) for row in cursor.fetchall()]

    def list_recent_workers(self, limit: int = 100) -> list[WorkerRecord]:
        safe_limit = max(1, int(limit))
        cursor = self._conn.execute(
            "SELECT * FROM workers ORDER BY created_at DESC LIMIT ?",
            (safe_limit,),
        )
        return [self._row_to_worker(row) for row in cursor.fetchall()]

    def add_execution_episode(self, record: ExecutionEpisodeRecord) -> None:
        self._insert_execution_episode(record)
        self._conn.commit()

    def _insert_execution_episode(self, record: ExecutionEpisodeRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO execution_episodes (
                id, worker_run_id, task_fingerprint, environment_fingerprint,
                capability_fingerprint, result_fingerprint, status, source_kind,
                trust_state, correlation_id, template_id, model, trajectory_refs_json,
                result_metadata_json, verification_json, provenance_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.worker_run_id,
                record.task_fingerprint,
                record.environment_fingerprint,
                record.capability_fingerprint,
                record.result_fingerprint,
                record.status,
                record.source_kind,
                record.trust_state,
                record.correlation_id,
                record.template_id,
                record.model,
                _safe_json_dumps(record.trajectory_refs),
                _safe_json_dumps(record.result_metadata),
                _safe_json_dumps(record.verification),
                _safe_json_dumps(record.provenance),
                record.created_at.isoformat(),
            ),
        )

    def add_execution_episode_bundle(
        self,
        record: ExecutionEpisodeRecord,
        evidence: ExecutionEpisodeEvidenceRecord,
    ) -> None:
        if evidence.episode_id != record.id:
            raise ValueError("execution episode evidence must reference the bundled episode")
        with self._lock:
            try:
                self._insert_execution_episode(record)
                self._conn.execute(
                    """
                    INSERT INTO execution_episode_evidence (
                        episode_id, algorithm, key_id, nonce, ciphertext, created_at, expires_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        evidence.episode_id,
                        evidence.algorithm,
                        evidence.key_id,
                        evidence.nonce,
                        evidence.ciphertext,
                        evidence.created_at.isoformat(),
                        evidence.expires_at.isoformat(),
                    ),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def get_execution_episode(self, episode_id: str) -> ExecutionEpisodeRecord | None:
        cursor = self._conn.execute("SELECT * FROM execution_episodes WHERE id = ?", (episode_id,))
        row = cursor.fetchone()
        return self._row_to_execution_episode(row) if row else None

    def list_execution_episodes(
        self,
        *,
        worker_run_id: str | None = None,
        limit: int = 100,
    ) -> list[ExecutionEpisodeRecord]:
        safe_limit = max(1, int(limit))
        if worker_run_id is None:
            cursor = self._conn.execute(
                "SELECT * FROM execution_episodes ORDER BY created_at DESC LIMIT ?",
                (safe_limit,),
            )
        else:
            cursor = self._conn.execute(
                """
                SELECT * FROM execution_episodes
                WHERE worker_run_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (worker_run_id, safe_limit),
            )
        return [self._row_to_execution_episode(row) for row in cursor.fetchall()]

    def list_execution_episodes_for_task(
        self,
        task_fingerprint: str,
        *,
        capability_fingerprint: str | None = None,
        limit: int = 16,
    ) -> list[ExecutionEpisodeRecord]:
        safe_limit = max(1, min(int(limit), 16))
        if capability_fingerprint is None:
            cursor = self._conn.execute(
                """
                SELECT * FROM execution_episodes
                WHERE task_fingerprint = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (task_fingerprint, safe_limit),
            )
        else:
            cursor = self._conn.execute(
                """
                SELECT * FROM execution_episodes
                WHERE task_fingerprint = ? AND capability_fingerprint = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (task_fingerprint, capability_fingerprint, safe_limit),
            )
        return [self._row_to_execution_episode(row) for row in cursor.fetchall()]

    def add_adaptation_failure_cluster_with_audit(
        self, record: AdaptationFailureClusterRecord, event: AuditEvent
    ) -> bool:
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO adaptation_failure_clusters (
                        id, signature, source_summary_fingerprint,
                        failure_categories_json, scenario_ids_json,
                        task_fingerprints_json, trial_refs_json, trial_count, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.id,
                        record.signature,
                        record.source_summary_fingerprint,
                        _safe_json_dumps(record.failure_categories),
                        _safe_json_dumps(record.scenario_ids),
                        _safe_json_dumps(record.task_fingerprints),
                        _safe_json_dumps(record.trial_refs),
                        record.trial_count,
                        record.created_at.isoformat(),
                    ),
                )
                self._insert_audit(event)
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                self._conn.rollback()
                if self.get_adaptation_failure_cluster(record.id) is not None:
                    return False
                raise
            except Exception:
                self._conn.rollback()
                raise

    def get_adaptation_failure_cluster(
        self, cluster_id: str
    ) -> AdaptationFailureClusterRecord | None:
        row = self._conn.execute(
            "SELECT * FROM adaptation_failure_clusters WHERE id = ?",
            (cluster_id,),
        ).fetchone()
        return self._row_to_adaptation_failure_cluster(row) if row else None

    def add_adaptation_candidate_with_audit(
        self, record: AdaptationCandidateRecord, event: AuditEvent
    ) -> bool:
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO adaptation_candidates (
                        id, family_id, version, kind, target, artifact_fingerprint,
                        definition_fingerprint, hypothesis, change_json,
                        source_cluster_ids_json, parent_id, status, evaluation_id,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.id,
                        record.family_id,
                        record.version,
                        record.kind,
                        record.target,
                        record.artifact_fingerprint,
                        record.definition_fingerprint,
                        record.hypothesis,
                        _safe_json_dumps(record.change),
                        _safe_json_dumps(record.source_cluster_ids),
                        record.parent_id,
                        record.status,
                        record.evaluation_id,
                        record.created_at.isoformat(),
                        record.updated_at.isoformat(),
                    ),
                )
                self._insert_audit(event)
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                self._conn.rollback()
                return False
            except Exception:
                self._conn.rollback()
                raise

    def get_adaptation_candidate(self, candidate_id: str) -> AdaptationCandidateRecord | None:
        row = self._conn.execute(
            "SELECT * FROM adaptation_candidates WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        return self._row_to_adaptation_candidate(row) if row else None

    def list_adaptation_candidates(
        self,
        *,
        kind: str | None = None,
        target: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[AdaptationCandidateRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (("kind", kind), ("target", target), ("status", status)):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        query = "SELECT * FROM adaptation_candidates"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, version DESC LIMIT ?"
        params.append(max(1, min(int(limit), 1000)))
        rows = self._conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_adaptation_candidate(row) for row in rows]

    def add_adaptation_evaluation_with_audit(
        self, record: AdaptationEvaluationRecord, event: AuditEvent
    ) -> bool:
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO adaptation_evaluations (
                        id, candidate_id, baseline_fingerprint, candidate_fingerprint,
                        scenario_set_fingerprint, common_trial_count,
                        distinct_scenario_count, baseline_success_rate,
                        candidate_success_rate, success_rate_delta, regression_count,
                        improvement_count, held_out, passed, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.id,
                        record.candidate_id,
                        record.baseline_fingerprint,
                        record.candidate_fingerprint,
                        record.scenario_set_fingerprint,
                        record.common_trial_count,
                        record.distinct_scenario_count,
                        record.baseline_success_rate,
                        record.candidate_success_rate,
                        record.success_rate_delta,
                        record.regression_count,
                        record.improvement_count,
                        int(record.held_out),
                        int(record.passed),
                        record.created_at.isoformat(),
                    ),
                )
                self._insert_audit(event)
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                self._conn.rollback()
                if self.get_adaptation_evaluation(record.id) is not None:
                    return False
                raise
            except Exception:
                self._conn.rollback()
                raise

    def get_adaptation_evaluation(self, evaluation_id: str) -> AdaptationEvaluationRecord | None:
        row = self._conn.execute(
            "SELECT * FROM adaptation_evaluations WHERE id = ?",
            (evaluation_id,),
        ).fetchone()
        return self._row_to_adaptation_evaluation(row) if row else None

    def get_latest_adaptation_evaluation(
        self, candidate_id: str
    ) -> AdaptationEvaluationRecord | None:
        row = self._conn.execute(
            """
            SELECT * FROM adaptation_evaluations
            WHERE candidate_id = ?
            ORDER BY created_at DESC, id DESC LIMIT 1
            """,
            (candidate_id,),
        ).fetchone()
        return self._row_to_adaptation_evaluation(row) if row else None

    def activate_adaptation_candidate_with_audit(
        self,
        candidate_id: str,
        *,
        expected_statuses: list[str],
        evaluation_id: str,
        updated_at: datetime,
        event: AuditEvent,
    ) -> bool:
        if not expected_statuses:
            return False
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                candidate = self._conn.execute(
                    "SELECT * FROM adaptation_candidates WHERE id = ?",
                    (candidate_id,),
                ).fetchone()
                if candidate is None or candidate["status"] not in expected_statuses:
                    self._conn.rollback()
                    return False
                evaluation = self._conn.execute(
                    """
                    SELECT id FROM adaptation_evaluations
                    WHERE id = ? AND candidate_id = ? AND passed = 1
                    """,
                    (evaluation_id, candidate_id),
                ).fetchone()
                if evaluation is None:
                    self._conn.rollback()
                    return False
                self._conn.execute(
                    """
                    UPDATE adaptation_candidates
                    SET status = 'retired', updated_at = ?
                    WHERE family_id = ? AND status = 'active' AND id != ?
                    """,
                    (updated_at.isoformat(), candidate["family_id"], candidate_id),
                )
                placeholders = ", ".join("?" for _item in expected_statuses)
                cursor = self._conn.execute(
                    f"""
                    UPDATE adaptation_candidates
                    SET status = 'active', evaluation_id = ?, updated_at = ?
                    WHERE id = ? AND status IN ({placeholders})
                    """,
                    (
                        evaluation_id,
                        updated_at.isoformat(),
                        candidate_id,
                        *expected_statuses,
                    ),
                )
                if cursor.rowcount != 1:
                    self._conn.rollback()
                    return False
                self._insert_audit(event)
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def add_procedural_recipe_with_audit(
        self, record: ProceduralRecipeRecord, event: AuditEvent
    ) -> bool:
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO procedural_recipes (
                        id, intent_fingerprint, definition_fingerprint,
                        applicability_conditions_json, required_capabilities_json,
                        required_permissions_json, strategy_steps_json,
                        verification_contract_json, known_failures_json,
                        invalidating_conditions_json, source_episode_ids_json,
                        success_count, failure_count, status, last_validated_at,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.id,
                        record.intent_fingerprint,
                        record.definition_fingerprint,
                        _safe_json_dumps(record.applicability_conditions),
                        _safe_json_dumps(record.required_capabilities),
                        _safe_json_dumps(record.required_permissions),
                        _safe_json_dumps(record.strategy_steps),
                        _safe_json_dumps(record.verification_contract),
                        _safe_json_dumps(record.known_failures),
                        _safe_json_dumps(record.invalidating_conditions),
                        _safe_json_dumps(record.source_episode_ids),
                        record.success_count,
                        record.failure_count,
                        record.status,
                        record.last_validated_at.isoformat(),
                        record.created_at.isoformat(),
                        record.updated_at.isoformat(),
                    ),
                )
                self._insert_audit(event)
                self._conn.commit()
                return True
            except sqlite3.IntegrityError as exc:
                self._conn.rollback()
                if "procedural_recipes.id" in str(exc):
                    return False
                raise
            except Exception:
                self._conn.rollback()
                raise

    def get_procedural_recipe(self, recipe_id: str) -> ProceduralRecipeRecord | None:
        cursor = self._conn.execute("SELECT * FROM procedural_recipes WHERE id = ?", (recipe_id,))
        row = cursor.fetchone()
        return self._row_to_procedural_recipe(row) if row else None

    def list_procedural_recipes(
        self, *, status: str | None = None, limit: int = 100
    ) -> list[ProceduralRecipeRecord]:
        safe_limit = max(1, min(int(limit), 1000))
        if status is None:
            cursor = self._conn.execute(
                "SELECT * FROM procedural_recipes ORDER BY updated_at DESC LIMIT ?",
                (safe_limit,),
            )
        else:
            cursor = self._conn.execute(
                """
                SELECT * FROM procedural_recipes
                WHERE status = ?
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (status, safe_limit),
            )
        return [self._row_to_procedural_recipe(row) for row in cursor.fetchall()]

    def transition_procedural_recipe_with_audit(
        self,
        recipe_id: str,
        *,
        expected_statuses: list[str],
        new_status: str,
        updated_at: datetime,
        event: AuditEvent,
    ) -> bool:
        if not expected_statuses:
            return False
        placeholders = ",".join("?" for _ in expected_statuses)
        with self._lock:
            try:
                cursor = self._conn.execute(
                    f"""
                    UPDATE procedural_recipes
                    SET status = ?, updated_at = ?
                    WHERE id = ? AND status IN ({placeholders})
                    """,
                    (new_status, updated_at.isoformat(), recipe_id, *expected_statuses),
                )
                if cursor.rowcount != 1:
                    self._conn.rollback()
                    return False
                self._insert_audit(event)
                self._conn.commit()
                return True
            except sqlite3.IntegrityError as exc:
                self._conn.rollback()
                if "procedural_recipes.intent_fingerprint" in str(exc):
                    return False
                raise
            except Exception:
                self._conn.rollback()
                raise

    def add_procedural_recipe_evaluation_with_audit(
        self, record: ProceduralRecipeEvaluationRecord, event: AuditEvent
    ) -> bool:
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO procedural_recipe_evaluations (
                        id, recipe_id, baseline_fingerprint, candidate_fingerprint,
                        scenario_set_fingerprint, common_trial_count,
                        baseline_success_rate, candidate_success_rate,
                        regression_count, improvement_count, passed, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.id,
                        record.recipe_id,
                        record.baseline_fingerprint,
                        record.candidate_fingerprint,
                        record.scenario_set_fingerprint,
                        record.common_trial_count,
                        record.baseline_success_rate,
                        record.candidate_success_rate,
                        record.regression_count,
                        record.improvement_count,
                        1 if record.passed else 0,
                        record.created_at.isoformat(),
                    ),
                )
                self._insert_audit(event)
                self._conn.commit()
                return True
            except sqlite3.IntegrityError as exc:
                self._conn.rollback()
                if "procedural_recipe_evaluations.id" in str(exc):
                    return False
                raise
            except Exception:
                self._conn.rollback()
                raise

    def get_latest_procedural_recipe_evaluation(
        self, recipe_id: str
    ) -> ProceduralRecipeEvaluationRecord | None:
        row = self._conn.execute(
            """
            SELECT * FROM procedural_recipe_evaluations
            WHERE recipe_id = ?
            ORDER BY created_at DESC, rowid DESC
            LIMIT 1
            """,
            (recipe_id,),
        ).fetchone()
        return self._row_to_procedural_recipe_evaluation(row) if row else None

    def get_procedural_recipe_evaluation(
        self, evaluation_id: str
    ) -> ProceduralRecipeEvaluationRecord | None:
        row = self._conn.execute(
            "SELECT * FROM procedural_recipe_evaluations WHERE id = ?",
            (evaluation_id,),
        ).fetchone()
        return self._row_to_procedural_recipe_evaluation(row) if row else None

    def record_procedural_recipe_outcome_with_audit(
        self,
        recipe_id: str,
        *,
        episode_id: str,
        succeeded: bool,
        validated_at: datetime,
        event: AuditEvent,
    ) -> bool:
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO procedural_recipe_outcomes (
                        recipe_id, episode_id, succeeded, created_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        recipe_id,
                        episode_id,
                        1 if succeeded else 0,
                        validated_at.isoformat(),
                    ),
                )
                cursor = self._conn.execute(
                    """
                    UPDATE procedural_recipes
                    SET success_count = success_count + ?,
                        failure_count = failure_count + ?,
                        last_validated_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        1 if succeeded else 0,
                        0 if succeeded else 1,
                        validated_at.isoformat(),
                        validated_at.isoformat(),
                        recipe_id,
                    ),
                )
                if cursor.rowcount != 1:
                    self._conn.rollback()
                    raise ValueError("procedural recipe not found")
                self._insert_audit(event)
                self._conn.commit()
                return True
            except sqlite3.IntegrityError as exc:
                self._conn.rollback()
                if "procedural_recipe_outcomes.recipe_id" in str(
                    exc
                ) and "procedural_recipe_outcomes.episode_id" in str(exc):
                    return False
                raise
            except Exception:
                self._conn.rollback()
                raise

    def get_execution_episode_evidence(
        self, episode_id: str
    ) -> ExecutionEpisodeEvidenceRecord | None:
        cursor = self._conn.execute(
            "SELECT * FROM execution_episode_evidence WHERE episode_id = ?",
            (episode_id,),
        )
        row = cursor.fetchone()
        return self._row_to_execution_episode_evidence(row) if row else None

    def get_execution_episode_evidence_metadata(
        self, episode_id: str
    ) -> ExecutionEpisodeEvidenceMetadata | None:
        cursor = self._conn.execute(
            """
            SELECT episode_id, algorithm, key_id, created_at, expires_at
            FROM execution_episode_evidence
            WHERE episode_id = ?
            """,
            (episode_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return ExecutionEpisodeEvidenceMetadata(
            episode_id=row["episode_id"],
            algorithm=row["algorithm"],
            key_id=row["key_id"],
            created_at=_parse_dt(row["created_at"]),
            expires_at=_parse_dt(row["expires_at"]),
        )

    def delete_execution_episode_evidence(self, episode_id: str) -> bool:
        cursor = self._conn.execute(
            "DELETE FROM execution_episode_evidence WHERE episode_id = ?",
            (episode_id,),
        )
        self._conn.commit()
        deleted = cursor.rowcount > 0
        if deleted:
            self._evidence_secure_purge_pending = True
            self.secure_purge_evidence_storage()
        return deleted

    def delete_execution_episode_evidence_with_audit(
        self,
        episode_id: str,
        event: AuditEvent,
    ) -> bool:
        with self._lock:
            try:
                cursor = self._conn.execute(
                    "DELETE FROM execution_episode_evidence WHERE episode_id = ?",
                    (episode_id,),
                )
                if cursor.rowcount <= 0:
                    self._conn.rollback()
                    return False
                self._insert_audit(event)
                self._conn.commit()
                self._evidence_secure_purge_pending = True
                self.secure_purge_evidence_storage()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def cleanup_expired_execution_episode_evidence(self, now: datetime) -> int:
        cursor = self._conn.execute(
            """
            DELETE FROM execution_episode_evidence
            WHERE julianday(expires_at) <= julianday(?)
            """,
            (now.isoformat(),),
        )
        self._conn.commit()
        deleted = cursor.rowcount
        if deleted > 0:
            self._evidence_secure_purge_pending = True
        if self._evidence_secure_purge_pending:
            self.secure_purge_evidence_storage()
        return deleted

    def secure_purge_evidence_storage(self) -> None:
        """Checkpoint and truncate WAL so securely deleted evidence pages are discarded."""
        with self._lock:
            row = self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if row is None or int(row[0]) != 0:
                raise EvidenceSecurePurgeIncomplete(
                    "SQLite WAL checkpoint was blocked after evidence deletion"
                )
            self._evidence_secure_purge_pending = False

    def count_workers_created_since(self, since: datetime) -> int:
        cursor = self._conn.execute(
            "SELECT COUNT(*) AS cnt FROM workers WHERE julianday(created_at) >= julianday(?)",
            (since.isoformat(),),
        )
        row = cursor.fetchone()
        if not row:
            return 0
        try:
            return int(row["cnt"])
        except Exception:
            return 0

    def save_intent(self, record: IntentRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO intents (
                id, worker_id, type, payload_json, payload_hash, risk,
                requires_approval, memory_influence_ids_json,
                procedural_recipe_ids_json, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.worker_id,
                record.type,
                json.dumps(record.payload),
                record.payload_hash,
                record.risk,
                1 if record.requires_approval else 0,
                json.dumps(record.memory_influence_ids),
                json.dumps(record.procedural_recipe_ids),
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
        self._insert_audit(event)
        self._conn.commit()

    def upsert_mcp_task(
        self, record: MCPTaskRecord
    ) -> tuple[MCPTaskRecord, MCPTaskRecord | None, bool]:
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                row = self._conn.execute(
                    "SELECT * FROM mcp_tasks WHERE id = ?",
                    (record.id,),
                ).fetchone()
                previous = self._row_to_mcp_task(row) if row else None
                if previous is not None and _mcp_task_transition_is_stale(previous, record):
                    self._conn.rollback()
                    return previous, previous, False

                effective = (
                    _merge_mcp_task_records(previous, record) if previous is not None else record
                )
                if previous is not None and _mcp_task_records_equivalent(previous, effective):
                    self._conn.rollback()
                    return previous, previous, False
                self._conn.execute(
                    """
            INSERT INTO mcp_tasks (
                id, server_id, task_id, protocol, remote_status, runtime_status,
                tool_name, auth_context_id, correlation_id, trace_id, span_id,
                worker_run_id, chat_id, chat_turn_id, plan_run_id, plan_step_id,
                status_message, ttl_ms, poll_interval_ms, input_requests_json,
                responded_input_keys_json, result_json, error_json, remote_created_at,
                remote_updated_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                remote_status = excluded.remote_status,
                runtime_status = excluded.runtime_status,
                status_message = excluded.status_message,
                ttl_ms = excluded.ttl_ms,
                poll_interval_ms = excluded.poll_interval_ms,
                input_requests_json = excluded.input_requests_json,
                responded_input_keys_json = excluded.responded_input_keys_json,
                result_json = excluded.result_json,
                error_json = excluded.error_json,
                remote_updated_at = excluded.remote_updated_at,
                updated_at = excluded.updated_at
            """,
                    (
                        effective.id,
                        effective.server_id,
                        effective.task_id,
                        effective.protocol,
                        effective.remote_status,
                        effective.runtime_status,
                        effective.tool_name,
                        effective.auth_context_id,
                        effective.correlation_id,
                        effective.trace_id,
                        effective.span_id,
                        effective.worker_run_id,
                        effective.chat_id,
                        effective.chat_turn_id,
                        effective.plan_run_id,
                        effective.plan_step_id,
                        effective.status_message,
                        effective.ttl_ms,
                        effective.poll_interval_ms,
                        json.dumps(effective.input_requests),
                        json.dumps(effective.responded_input_keys),
                        json.dumps(effective.result) if effective.result is not None else None,
                        json.dumps(effective.error) if effective.error is not None else None,
                        effective.remote_created_at.isoformat(),
                        effective.remote_updated_at.isoformat(),
                        effective.created_at.isoformat(),
                        effective.updated_at.isoformat(),
                    ),
                )
                self._conn.commit()
                return effective, previous, True
            except Exception:
                self._conn.rollback()
                raise

    def get_mcp_task(self, task_record_id: str) -> MCPTaskRecord | None:
        row = self._conn.execute(
            "SELECT * FROM mcp_tasks WHERE id = ?",
            (task_record_id,),
        ).fetchone()
        return self._row_to_mcp_task(row) if row else None

    def list_recoverable_mcp_tasks(
        self,
        *,
        server_id: str | None = None,
        auth_context_id: str | None = None,
        limit: int = 100,
    ) -> list[MCPTaskRecord]:
        clauses = ["runtime_status IN ('running', 'awaiting_instruction')"]
        params: list[Any] = []
        if server_id is not None:
            clauses.append("server_id = ?")
            params.append(server_id)
        if auth_context_id is not None:
            clauses.append("auth_context_id = ?")
            params.append(auth_context_id)
        params.append(max(1, min(int(limit), 1000)))
        rows = self._conn.execute(
            f"SELECT * FROM mcp_tasks WHERE {' AND '.join(clauses)} "
            "ORDER BY updated_at ASC LIMIT ?",
            tuple(params),
        ).fetchall()
        return [self._row_to_mcp_task(row) for row in rows]

    def _insert_audit(self, event: AuditEvent) -> None:
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

    def list_audit(self, limit: int = 100) -> list[AuditEvent]:
        cursor = self._conn.execute(
            "SELECT * FROM audit_events ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return [self._row_to_audit(row) for row in cursor.fetchall()]

    def list_audit_for_correlation(self, correlation_id: str, limit: int = 100) -> list[AuditEvent]:
        cursor = self._conn.execute(
            "SELECT * FROM audit_events WHERE correlation_id = ? ORDER BY ts ASC, rowid ASC LIMIT ?",
            (correlation_id, limit),
        )
        return [self._row_to_audit(row) for row in cursor.fetchall()]

    def get_audit(self, event_id: str) -> AuditEvent | None:
        cursor = self._conn.execute("SELECT * FROM audit_events WHERE id = ?", (event_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_audit(row)

    def upsert_worker_template(self, record: WorkerTemplateRecord) -> None:
        """Upsert a worker template by writing to filesystem.

        Note: This is now a no-op for database storage. Workers are managed
        as files under the configured workspace in workers/{id}/worker.json.
        """
        # Worker templates are now managed as files in the workspace
        # This method is kept for API compatibility but does nothing
        pass

    def list_worker_templates(self) -> list[WorkerTemplateRecord]:
        """List all worker templates from filesystem."""
        return discover_worker_templates(self._workspace_dir)

    def get_worker_template(self, template_id: str) -> WorkerTemplateRecord | None:
        """Get a worker template from filesystem by ID."""
        return get_template_from_fs(self._workspace_dir, template_id)

    def delete_worker_template(self, template_id: str) -> None:
        """Delete a worker template by ID.

        Note: This is not implemented. Workers should be managed via file operations.
        """
        # Worker templates are now managed as files - use file operations to delete
        raise NotImplementedError(
            "Worker templates are managed as files. Delete the worker directory directly: "
            f"{self._workspace_dir / 'workers' / template_id}"
        )

    def add_memory_entry(self, entry: MemoryEntry) -> None:
        owner_id = str((entry.metadata or {}).get("owner_id", "default"))
        chat_id = (entry.metadata or {}).get("chat_id")
        if chat_id is not None:
            try:
                chat_id = int(chat_id)
            except (TypeError, ValueError):
                chat_id = None

        self._conn.execute(
            """
            INSERT INTO memory_entries (uuid, role, content, embedding_json, created_at, metadata_json, owner_id, chat_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.id,  # The dataclass id is the UUID
                entry.role,
                entry.content,
                json.dumps(entry.embedding) if entry.embedding is not None else None,
                entry.created_at.isoformat(),
                json.dumps(entry.metadata),
                owner_id,
                chat_id,
            ),
        )

        # Also write to new embeddings table if embedding exists
        if entry.embedding:
            self._conn.execute(
                """
                INSERT INTO memory_embeddings (entry_uuid, model, vector_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    entry.id,
                    str((entry.metadata or {}).get("embedding_model") or "unknown"),
                    json.dumps(entry.embedding),
                    entry.created_at.isoformat(),
                ),
            )

        with suppress(sqlite3.OperationalError):
            self._conn.execute(
                """
                INSERT INTO memory_entries_fts (content, owner_id, chat_id, entry_uuid)
                VALUES (?, ?, ?, ?)
                """,
                (entry.content, owner_id, chat_id, entry.id),
            )

        self._conn.commit()

    def list_memory_entries(self, limit: int = 200) -> list[MemoryEntry]:
        # Join with memory_embeddings to get the vector.
        # Prefer the new table, fallback to the old column if needed (handled in _row_to_memory logic implicitly if we select correctly)
        # For now, let's select from memory_entries and we can patch _row_to_memory if we want to switch the source of truth entirely.
        # But wait, _row_to_memory reads 'embedding_json'.
        # Let's do a LEFT JOIN to get the vector from the new table if available.
        cursor = self._conn.execute(
            """
            SELECT m.*, e.vector_json as new_embedding_json
            FROM memory_entries m
            LEFT JOIN memory_embeddings e ON m.uuid = e.entry_uuid
            ORDER BY m.id DESC LIMIT ?
            """,
            (limit,),
        )
        return [self._row_to_memory(row) for row in cursor.fetchall()]

    def list_memory_entries_for_owner(self, owner_id: str, limit: int = 200) -> list[MemoryEntry]:
        cursor = self._conn.execute(
            """
            SELECT m.*, e.vector_json as new_embedding_json
            FROM memory_entries m
            LEFT JOIN memory_embeddings e ON m.uuid = e.entry_uuid
            WHERE m.owner_id = ?
            ORDER BY m.id DESC LIMIT ?
            """,
            (owner_id, limit),
        )
        return [self._row_to_memory(row) for row in cursor.fetchall()]

    def list_memory_entries_requiring_embedding_migration(
        self, owner_id: str, model: str, limit: int = 100
    ) -> list[MemoryEntry]:
        cursor = self._conn.execute(
            """
            SELECT m.*, e.vector_json as new_embedding_json
            FROM memory_entries m
            LEFT JOIN memory_embeddings e ON m.uuid = e.entry_uuid
            WHERE m.owner_id = ?
              AND (e.entry_uuid IS NULL OR e.model != ?)
            ORDER BY m.id ASC
            LIMIT ?
            """,
            (owner_id, model, limit),
        )
        return [self._row_to_memory(row) for row in cursor.fetchall()]

    def replace_memory_embeddings(
        self, model: str, embeddings: list[tuple[str, list[float]]]
    ) -> None:
        if not embeddings:
            return
        now = utc_now().isoformat()
        for entry_id, embedding in embeddings:
            vector_json = json.dumps(embedding)
            self._conn.execute(
                "UPDATE memory_entries SET embedding_json = ? WHERE uuid = ?",
                (vector_json, entry_id),
            )
            self._conn.execute(
                """
                INSERT INTO memory_embeddings (entry_uuid, model, vector_json, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(entry_uuid) DO UPDATE SET
                    model = excluded.model,
                    vector_json = excluded.vector_json,
                    created_at = excluded.created_at
                """,
                (entry_id, model, vector_json, now),
            )
        self._conn.commit()

    def list_memory_entries_by_chat(self, chat_id: int, limit: int = 50) -> list[MemoryEntry]:
        cursor = self._conn.execute(
            "SELECT * FROM memory_entries WHERE chat_id = ? ORDER BY created_at DESC, rowid DESC LIMIT ?",
            (chat_id, limit),
        )
        return [self._row_to_memory(row) for row in cursor.fetchall()]

    def search_memory_entries_lexical(
        self,
        owner_id: str,
        query: str,
        limit: int = 80,
        exclude_chat_id: int | None = None,
    ) -> list[MemoryEntry]:
        trimmed = query.strip()
        if not trimmed:
            return []

        query_sql = """
            SELECT m.*, e.vector_json as new_embedding_json
            FROM memory_entries_fts f
            JOIN memory_entries m ON m.uuid = f.entry_uuid
            LEFT JOIN memory_embeddings e ON m.uuid = e.entry_uuid
            WHERE f.content MATCH ?
              AND m.owner_id = ?
            """
        params: list[Any] = [trimmed, owner_id]
        if exclude_chat_id is not None:
            query_sql += " AND (m.chat_id IS NULL OR m.chat_id != ?)"
            params.append(exclude_chat_id)
        query_sql += " ORDER BY bm25(memory_entries_fts), m.id DESC LIMIT ?"
        params.append(limit)

        try:
            cursor = self._conn.execute(query_sql, tuple(params))
        except sqlite3.OperationalError:
            return []
        return [self._row_to_memory(row) for row in cursor.fetchall()]

    def add_canon_embedding(
        self, filename: str, chunk_index: int, content: str, model: str, vector: list[float]
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO canon_embeddings (filename, chunk_index, content, model, vector_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (filename, chunk_index, content, model, json.dumps(vector), utc_now().isoformat()),
        )
        self._conn.commit()

    def clear_canon_embeddings(self, filename: str) -> None:
        self._conn.execute("DELETE FROM canon_embeddings WHERE filename = ?", (filename,))
        self._conn.commit()

    def list_canon_embeddings(self, filename: str | None = None) -> list[dict[str, Any]]:
        if filename:
            cursor = self._conn.execute(
                "SELECT * FROM canon_embeddings WHERE filename = ?", (filename,)
            )
        else:
            cursor = self._conn.execute("SELECT * FROM canon_embeddings")

        rows = cursor.fetchall()
        return [
            {
                "filename": row["filename"],
                "content": row["content"],
                "model": row["model"],
                "vector": json.loads(row["vector_json"]),
            }
            for row in rows
        ]

    def upsert_memory_fact(self, record: MemoryFactRecord) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO memory_facts (
                id, owner_id, subject, key, value_text, value_json, fact_type, confidence,
                status, trust_state, valid_from, valid_to, facets_json, source_kind,
                source_ref, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.owner_id,
                record.subject,
                record.key,
                record.value_text,
                json.dumps(record.value_json) if record.value_json is not None else None,
                record.fact_type,
                float(record.confidence),
                record.status,
                record.trust_state,
                record.valid_from.isoformat() if record.valid_from else None,
                record.valid_to.isoformat() if record.valid_to else None,
                json.dumps(record.facets),
                record.source_kind,
                record.source_ref,
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
            ),
        )
        self._conn.commit()

    def list_memory_facts(
        self,
        owner_id: str,
        *,
        limit: int = 100,
        status: str | None = None,
        subject: str | None = None,
        key: str | None = None,
        source_kind: str | None = None,
        source_ref: str | None = None,
        trust_states: list[str] | None = None,
    ) -> list[MemoryFactRecord]:
        query = ["SELECT * FROM memory_facts WHERE owner_id = ?"]
        params: list[Any] = [owner_id]
        if status is not None:
            query.append("AND status = ?")
            params.append(status)
        if subject is not None:
            query.append("AND subject = ?")
            params.append(subject)
        if key is not None:
            query.append("AND key = ?")
            params.append(key)
        if source_kind is not None:
            query.append("AND source_kind = ?")
            params.append(source_kind)
        if source_ref is not None:
            query.append("AND source_ref = ?")
            params.append(source_ref)
        if trust_states:
            placeholders = ", ".join("?" for _ in trust_states)
            query.append(f"AND trust_state IN ({placeholders})")
            params.extend(trust_states)
        query.append("ORDER BY updated_at DESC LIMIT ?")
        params.append(limit)
        cursor = self._conn.execute(" ".join(query), tuple(params))
        return [self._row_to_memory_fact(row) for row in cursor.fetchall()]

    def invalidate_memory_fact(
        self, fact_id: str, valid_to: datetime, status: str = "invalidated"
    ) -> None:
        trust_state = {
            "superseded": "superseded",
            "deprecated": "deprecated",
            "invalidated": "deprecated",
        }.get(status)
        self._conn.execute(
            """
            UPDATE memory_facts
            SET status = ?, trust_state = COALESCE(?, trust_state), valid_to = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, trust_state, valid_to.isoformat(), utc_now().isoformat(), fact_id),
        )
        self._conn.commit()

    def add_memory_fact_source(self, record: MemoryFactSourceRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO memory_fact_sources (fact_id, memory_entry_uuid, canon_filename, source_note, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                record.fact_id,
                record.memory_entry_uuid,
                record.canon_filename,
                record.source_note,
                record.created_at.isoformat(),
            ),
        )
        self._conn.commit()

    def list_memory_fact_sources(self, fact_id: str) -> list[MemoryFactSourceRecord]:
        cursor = self._conn.execute(
            "SELECT * FROM memory_fact_sources WHERE fact_id = ? ORDER BY id ASC",
            (fact_id,),
        )
        return [self._row_to_memory_fact_source(row) for row in cursor.fetchall()]

    def add_octo_diary_entry(self, record: OctoDiaryEntryRecord) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO octo_diary_entries (id, owner_id, chat_id, kind, summary, details_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.owner_id,
                record.chat_id,
                record.kind,
                record.summary,
                json.dumps(record.details),
                record.created_at.isoformat(),
            ),
        )
        self._conn.commit()

    def list_octo_diary_entries(
        self,
        owner_id: str,
        *,
        chat_id: int | None = None,
        limit: int = 20,
    ) -> list[OctoDiaryEntryRecord]:
        if chat_id is None:
            cursor = self._conn.execute(
                """
                SELECT * FROM octo_diary_entries
                WHERE owner_id = ?
                ORDER BY created_at DESC LIMIT ?
                """,
                (owner_id, limit),
            )
        else:
            cursor = self._conn.execute(
                """
                SELECT * FROM octo_diary_entries
                WHERE owner_id = ? AND (chat_id = ? OR chat_id IS NULL)
                ORDER BY created_at DESC LIMIT ?
                """,
                (owner_id, chat_id, limit),
            )
        return [self._row_to_octo_diary_entry(row) for row in cursor.fetchall()]

    def upsert_operational_memory_item(self, record: OperationalMemoryItemRecord) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO operational_memory_items (
                id, owner_id, chat_id, kind, statement, next_action, status, priority,
                confidence, source_kind, source_ref, plan_run_id, plan_step_id,
                evidence_json, metadata_json, created_at, updated_at, resolved_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.owner_id,
                record.chat_id,
                record.kind,
                record.statement,
                record.next_action,
                record.status,
                int(record.priority),
                float(record.confidence),
                record.source_kind,
                record.source_ref,
                record.plan_run_id,
                record.plan_step_id,
                _safe_json_dumps(record.evidence),
                _safe_json_dumps(record.metadata),
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                record.resolved_at.isoformat() if record.resolved_at else None,
            ),
        )
        self._conn.commit()

    def list_operational_memory_items(
        self,
        owner_id: str,
        *,
        chat_id: int | None = None,
        statuses: list[str] | None = None,
        kinds: list[str] | None = None,
        limit: int = 50,
    ) -> list[OperationalMemoryItemRecord]:
        clauses = ["owner_id = ?"]
        params: list[Any] = [owner_id]
        if chat_id is not None:
            clauses.append("(chat_id = ? OR chat_id IS NULL)")
            params.append(chat_id)
        normalized_statuses = [str(item).strip() for item in (statuses or []) if str(item).strip()]
        if normalized_statuses:
            placeholders = ", ".join("?" for _ in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
        normalized_kinds = [str(item).strip() for item in (kinds or []) if str(item).strip()]
        if normalized_kinds:
            placeholders = ", ".join("?" for _ in normalized_kinds)
            clauses.append(f"kind IN ({placeholders})")
            params.extend(normalized_kinds)
        params.append(max(1, int(limit)))
        cursor = self._conn.execute(
            f"""
            SELECT * FROM operational_memory_items
            WHERE {' AND '.join(clauses)}
            ORDER BY priority DESC, updated_at DESC
            LIMIT ?
            """,
            tuple(params),
        )
        return [self._row_to_operational_memory_item(row) for row in cursor.fetchall()]

    def update_operational_memory_item(
        self,
        item_id: str,
        *,
        status: str | None = None,
        plan_run_id: str | None = None,
        plan_step_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        resolved_at: datetime | None = None,
    ) -> None:
        updates = ["updated_at = ?"]
        params: list[Any] = [utc_now().isoformat()]
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if plan_run_id is not None:
            updates.append("plan_run_id = ?")
            params.append(plan_run_id)
        if plan_step_id is not None:
            updates.append("plan_step_id = ?")
            params.append(plan_step_id)
        if metadata is not None:
            updates.append("metadata_json = ?")
            params.append(_safe_json_dumps(metadata))
        if resolved_at is not None:
            updates.append("resolved_at = ?")
            params.append(resolved_at.isoformat())
        params.append(item_id)
        self._conn.execute(
            f"UPDATE operational_memory_items SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        self._conn.commit()

    def resolve_operational_memory_items_for_plan(
        self,
        plan_run_id: str,
        *,
        status: str,
        resolved_at: datetime,
    ) -> int:
        resolved_value = None if status == "blocked" else resolved_at.isoformat()
        cursor = self._conn.execute(
            """
            UPDATE operational_memory_items
            SET status = ?, resolved_at = ?, updated_at = ?
            WHERE plan_run_id = ?
              AND status IN ('active', 'in_progress', 'blocked')
            """,
            (status, resolved_value, utc_now().isoformat(), plan_run_id),
        )
        self._conn.commit()
        return int(cursor.rowcount or 0)

    def create_plan_run(self, run: PlanRunRecord, steps: list[PlanStepRecord]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO plan_runs (
                    id, goal, status, chat_id, source, correlation_id,
                    current_step_id, plan_json, metadata_json,
                    created_at, updated_at, completed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.id,
                    run.goal,
                    run.status,
                    run.chat_id,
                    run.source,
                    run.correlation_id,
                    run.current_step_id,
                    _safe_json_dumps(run.plan),
                    _safe_json_dumps(run.metadata),
                    run.created_at.isoformat(),
                    run.updated_at.isoformat(),
                    run.completed_at.isoformat() if run.completed_at else None,
                ),
            )
            for step in steps:
                self._conn.execute(
                    """
                    INSERT INTO plan_steps (
                        run_id, step_id, seq, kind, title, status, task, executor,
                        worker_run_id, input_json, output_json, error,
                        created_at, updated_at, started_at, completed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        step.run_id,
                        step.step_id,
                        step.seq,
                        step.kind,
                        step.title,
                        step.status,
                        step.task,
                        step.executor,
                        step.worker_run_id,
                        _safe_json_dumps(step.input),
                        _safe_json_dumps(step.output),
                        step.error,
                        step.created_at.isoformat(),
                        step.updated_at.isoformat(),
                        step.started_at.isoformat() if step.started_at else None,
                        step.completed_at.isoformat() if step.completed_at else None,
                    ),
                )
            self._conn.commit()

    def update_plan_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        current_step_id: str | None = None,
        plan: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        completed_at: datetime | None | object = UNSET,
    ) -> None:
        updates = ["updated_at = ?"]
        params: list[Any] = [utc_now().isoformat()]
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if current_step_id is not None:
            updates.append("current_step_id = ?")
            params.append(current_step_id)
        if plan is not None:
            updates.append("plan_json = ?")
            params.append(_safe_json_dumps(plan))
        if metadata is not None:
            updates.append("metadata_json = ?")
            params.append(_safe_json_dumps(metadata))
        if completed_at is not UNSET:
            updates.append("completed_at = ?")
            params.append(completed_at.isoformat() if isinstance(completed_at, datetime) else None)
        params.append(run_id)
        self._conn.execute(
            f"UPDATE plan_runs SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        self._conn.commit()

    def get_plan_run(self, run_id: str) -> PlanRunRecord | None:
        cursor = self._conn.execute("SELECT * FROM plan_runs WHERE id = ?", (run_id,))
        row = cursor.fetchone()
        return self._row_to_plan_run(row) if row else None

    def list_plan_runs(
        self,
        *,
        chat_id: int | None = None,
        statuses: list[str] | None = None,
        limit: int = 50,
    ) -> list[PlanRunRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if chat_id is not None:
            clauses.append("chat_id = ?")
            params.append(chat_id)
        normalized_statuses = [str(item).strip() for item in (statuses or []) if str(item).strip()]
        if normalized_statuses:
            placeholders = ", ".join("?" for _ in normalized_statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)
        query = "SELECT * FROM plan_runs"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(max(1, int(limit)))
        cursor = self._conn.execute(query, params)
        return [self._row_to_plan_run(row) for row in cursor.fetchall()]

    def get_plan_steps(self, run_id: str) -> list[PlanStepRecord]:
        cursor = self._conn.execute(
            "SELECT * FROM plan_steps WHERE run_id = ? ORDER BY seq ASC",
            (run_id,),
        )
        return [self._row_to_plan_step(row) for row in cursor.fetchall()]

    def get_plan_step_by_worker_run_id(
        self,
        worker_run_id: str,
        *,
        chat_id: int | None = None,
    ) -> PlanStepRecord | None:
        normalized_worker_run_id = str(worker_run_id or "").strip()
        if not normalized_worker_run_id:
            return None
        params: list[Any] = [normalized_worker_run_id]
        query = """
            SELECT plan_steps.*
            FROM plan_steps
            JOIN plan_runs ON plan_runs.id = plan_steps.run_id
            WHERE plan_steps.worker_run_id = ?
        """
        if chat_id is not None:
            query += " AND plan_runs.chat_id = ?"
            params.append(chat_id)
        query += " ORDER BY plan_steps.updated_at DESC LIMIT 1"
        cursor = self._conn.execute(query, params)
        row = cursor.fetchone()
        return self._row_to_plan_step(row) if row else None

    def update_plan_step(
        self,
        run_id: str,
        step_id: str,
        *,
        status: str | None = None,
        worker_run_id: str | None = None,
        output: dict[str, Any] | None = None,
        error: str | None | object = UNSET,
        started_at: datetime | None = None,
        completed_at: datetime | None | object = UNSET,
    ) -> None:
        updates = ["updated_at = ?"]
        params: list[Any] = [utc_now().isoformat()]
        if status is not None:
            updates.append("status = ?")
            params.append(status)
        if worker_run_id is not None:
            updates.append("worker_run_id = ?")
            params.append(worker_run_id)
        if output is not None:
            updates.append("output_json = ?")
            params.append(_safe_json_dumps(output))
        if error is not UNSET:
            updates.append("error = ?")
            params.append(error)
        if started_at is not None:
            updates.append("started_at = ?")
            params.append(started_at.isoformat())
        if completed_at is not UNSET:
            updates.append("completed_at = ?")
            params.append(completed_at.isoformat() if isinstance(completed_at, datetime) else None)
        params.extend([run_id, step_id])
        self._conn.execute(
            f"UPDATE plan_steps SET {', '.join(updates)} WHERE run_id = ? AND step_id = ?",
            params,
        )
        self._conn.commit()

    def append_plan_event(self, event: PlanEventRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO plan_events (id, run_id, step_id, event_type, data_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.id,
                event.run_id,
                event.step_id,
                event.event_type,
                _safe_json_dumps(event.data),
                event.created_at.isoformat(),
            ),
        )
        self._conn.commit()

    def list_plan_events(self, run_id: str, limit: int = 100) -> list[PlanEventRecord]:
        cursor = self._conn.execute(
            """
            SELECT * FROM plan_events
            WHERE run_id = ?
            ORDER BY created_at ASC, rowid ASC
            LIMIT ?
            """,
            (run_id, max(1, int(limit))),
        )
        return [self._row_to_plan_event(row) for row in cursor.fetchall()]

    def cleanup_old_memory(self, keep_days: int = 30, keep_count: int = 1000) -> int:
        """
        Cleanup old memory entries to prevent database bloat.

        Keeps:
        - All entries from the last N days (default: 30)
        - The last N entries total (default: 1000)

        Returns: Number of entries deleted
        """
        safe_days = max(0, keep_days)
        safe_keep_count = max(0, keep_count)
        cutoff = utc_now() - timedelta(days=safe_days)

        cursor = self._conn.execute(
            """
            DELETE FROM memory_entries
            WHERE julianday(created_at) < julianday(?)
              AND id NOT IN (
                  SELECT id FROM memory_entries
                  ORDER BY created_at DESC
                  LIMIT ?
              )
            """,
            (cutoff.isoformat(), safe_keep_count),
        )
        deleted_count = cursor.rowcount
        with suppress(sqlite3.OperationalError):
            self._conn.execute("""
                DELETE FROM memory_entries_fts
                WHERE entry_uuid NOT IN (SELECT uuid FROM memory_entries)
                """)
        self._conn.commit()
        return deleted_count

    def delete_memory_entries_by_chat(self, chat_id: int, keep_recent: int = 0) -> int:
        safe_keep = max(0, int(keep_recent))
        if safe_keep > 0:
            cursor = self._conn.execute(
                """
                DELETE FROM memory_entries
                WHERE chat_id = ?
                  AND id NOT IN (
                      SELECT id FROM memory_entries
                      WHERE chat_id = ?
                      ORDER BY id DESC
                      LIMIT ?
                  )
                """,
                (chat_id, chat_id, safe_keep),
            )
        else:
            cursor = self._conn.execute(
                "DELETE FROM memory_entries WHERE chat_id = ?",
                (chat_id,),
            )

        deleted_count = cursor.rowcount
        with suppress(sqlite3.OperationalError):
            self._conn.execute("""
                DELETE FROM memory_entries_fts
                WHERE entry_uuid NOT IN (SELECT uuid FROM memory_entries)
                """)
        self._conn.commit()
        return deleted_count

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

    def upsert_scheduled_task(
        self,
        task_id: str,
        name: str,
        frequency: str,
        task_text: str,
        description: str | None = None,
        worker_id: str | None = None,
        inputs: dict | None = None,
        enabled: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO scheduled_tasks (id, name, description, frequency, worker_id, task_text, inputs_json, enabled, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                frequency = excluded.frequency,
                next_run_at = CASE
                    WHEN frequency IS NOT excluded.frequency THEN NULL
                    ELSE next_run_at
                END,
                idempotency_key = CASE
                    WHEN frequency IS NOT excluded.frequency THEN NULL
                    ELSE idempotency_key
                END,
                worker_id = excluded.worker_id,
                task_text = excluded.task_text,
                inputs_json = excluded.inputs_json,
                enabled = excluded.enabled,
                metadata_json = excluded.metadata_json
            """,
            (
                task_id,
                name,
                description,
                frequency,
                worker_id,
                task_text,
                json.dumps(inputs) if inputs else None,
                1 if enabled else 0,
                json.dumps(metadata) if metadata else None,
            ),
        )
        self._conn.commit()

    def update_task_last_run(self, task_id: str, ts: datetime) -> None:
        self._conn.execute(
            "UPDATE scheduled_tasks SET last_run_at = ? WHERE id = ?",
            (ts.isoformat(), task_id),
        )
        self._conn.commit()

    def claim_scheduled_task(
        self,
        task_id: str,
        *,
        lease_owner: str,
        lease_expires_at: datetime,
        attempt_id: str,
        idempotency_key: str,
        started_at: datetime,
        expected_last_run_at: str | None,
        expected_next_run_at: str | None,
    ) -> bool:
        cursor = self._conn.execute(
            """
            UPDATE scheduled_tasks
            SET lease_owner = ?,
                lease_expires_at = ?,
                attempt_id = ?,
                attempt_no = COALESCE(attempt_no, 0) + 1,
                last_outcome = 'running',
                last_error_class = NULL,
                last_started_at = ?,
                idempotency_key = ?
            WHERE id = ?
              AND enabled = 1
              AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
              AND last_run_at IS ?
              AND next_run_at IS ?
              AND (next_run_at IS NULL OR next_run_at <= ?)
            """,
            (
                lease_owner,
                lease_expires_at.isoformat(),
                attempt_id,
                started_at.isoformat(),
                idempotency_key,
                task_id,
                started_at.isoformat(),
                expected_last_run_at,
                expected_next_run_at,
                started_at.isoformat(),
            ),
        )
        self._conn.commit()
        return cursor.rowcount == 1

    def finish_scheduled_task_attempt(
        self,
        task_id: str,
        *,
        attempt_id: str,
        outcome: str,
        finished_at: datetime,
        completed: bool,
        next_run_at: datetime | None,
        error_class: str | None = None,
    ) -> bool:
        cursor = self._conn.execute(
            """
            UPDATE scheduled_tasks
            SET last_run_at = CASE WHEN ? THEN ? ELSE last_run_at END,
                next_run_at = ?,
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_outcome = ?,
                last_error_class = ?,
                last_completed_at = ?
            WHERE id = ? AND attempt_id = ?
            """,
            (
                1 if completed else 0,
                finished_at.isoformat(),
                next_run_at.isoformat() if next_run_at is not None else None,
                outcome,
                error_class,
                finished_at.isoformat(),
                task_id,
                attempt_id,
            ),
        )
        self._conn.commit()
        return cursor.rowcount == 1

    def update_scheduled_task_metadata(
        self,
        task_id: str,
        metadata: dict[str, Any] | None,
    ) -> None:
        self._conn.execute(
            "UPDATE scheduled_tasks SET metadata_json = ? WHERE id = ?",
            (json.dumps(metadata) if metadata else None, task_id),
        )
        self._conn.commit()

    def get_scheduled_tasks(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        query = "SELECT * FROM scheduled_tasks"
        if enabled_only:
            query += " WHERE enabled = 1"
        cursor = self._conn.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    def delete_scheduled_task(self, task_id: str) -> None:
        self._conn.execute("DELETE FROM scheduled_tasks WHERE id = ?", (task_id,))
        self._conn.commit()

    def _row_to_worker(self, row: sqlite3.Row) -> WorkerRecord:
        return WorkerRecord(
            id=row["id"],
            status=row["status"],
            task=row["task"],
            granted_caps=_loads_json(row["granted_caps_json"], []),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
            summary=_row_get(row, "summary"),
            output=_loads_json(row["output_json"]),
            error=_row_get(row, "error"),
            tools_used=_loads_json(_row_get(row, "tools_used_json"), []),
            lineage_id=_row_get(row, "lineage_id"),
            parent_worker_id=_row_get(row, "parent_worker_id"),
            root_task_id=_row_get(row, "root_task_id"),
            spawn_depth=int(_row_get(row, "spawn_depth", 0) or 0),
            template_id=_row_get(row, "template_id"),
            template_name=_row_get(row, "template_name"),
        )

    def _row_to_execution_episode(self, row: sqlite3.Row) -> ExecutionEpisodeRecord:
        return ExecutionEpisodeRecord(
            id=row["id"],
            worker_run_id=row["worker_run_id"],
            task_fingerprint=row["task_fingerprint"],
            environment_fingerprint=row["environment_fingerprint"],
            capability_fingerprint=row["capability_fingerprint"],
            result_fingerprint=row["result_fingerprint"],
            status=row["status"],
            source_kind=row["source_kind"],
            trust_state=row["trust_state"],
            correlation_id=row["correlation_id"],
            template_id=row["template_id"],
            model=row["model"],
            trajectory_refs=_loads_json(row["trajectory_refs_json"], {}),
            result_metadata=_loads_json(row["result_metadata_json"], {}),
            verification=_loads_json(row["verification_json"], {}),
            provenance=_loads_json(row["provenance_json"], {}),
            created_at=_parse_dt(row["created_at"]),
        )

    def _row_to_adaptation_failure_cluster(
        self, row: sqlite3.Row
    ) -> AdaptationFailureClusterRecord:
        return AdaptationFailureClusterRecord(
            id=row["id"],
            signature=row["signature"],
            source_summary_fingerprint=row["source_summary_fingerprint"],
            failure_categories=_loads_json(row["failure_categories_json"], []),
            scenario_ids=_loads_json(row["scenario_ids_json"], []),
            task_fingerprints=_loads_json(row["task_fingerprints_json"], []),
            trial_refs=_loads_json(row["trial_refs_json"], []),
            trial_count=int(row["trial_count"]),
            created_at=_parse_dt(row["created_at"]),
        )

    def _row_to_adaptation_candidate(self, row: sqlite3.Row) -> AdaptationCandidateRecord:
        return AdaptationCandidateRecord(
            id=row["id"],
            family_id=row["family_id"],
            version=int(row["version"]),
            kind=row["kind"],
            target=row["target"],
            artifact_fingerprint=row["artifact_fingerprint"],
            definition_fingerprint=row["definition_fingerprint"],
            hypothesis=row["hypothesis"],
            change=_loads_json(row["change_json"], {}),
            source_cluster_ids=_loads_json(row["source_cluster_ids_json"], []),
            parent_id=row["parent_id"],
            status=row["status"],
            evaluation_id=row["evaluation_id"],
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
        )

    def _row_to_adaptation_evaluation(self, row: sqlite3.Row) -> AdaptationEvaluationRecord:
        return AdaptationEvaluationRecord(
            id=row["id"],
            candidate_id=row["candidate_id"],
            baseline_fingerprint=row["baseline_fingerprint"],
            candidate_fingerprint=row["candidate_fingerprint"],
            scenario_set_fingerprint=row["scenario_set_fingerprint"],
            common_trial_count=int(row["common_trial_count"]),
            distinct_scenario_count=int(row["distinct_scenario_count"]),
            baseline_success_rate=float(row["baseline_success_rate"]),
            candidate_success_rate=float(row["candidate_success_rate"]),
            success_rate_delta=float(row["success_rate_delta"]),
            regression_count=int(row["regression_count"]),
            improvement_count=int(row["improvement_count"]),
            held_out=bool(row["held_out"]),
            passed=bool(row["passed"]),
            created_at=_parse_dt(row["created_at"]),
        )

    def _row_to_procedural_recipe(self, row: sqlite3.Row) -> ProceduralRecipeRecord:
        return ProceduralRecipeRecord(
            id=row["id"],
            intent_fingerprint=row["intent_fingerprint"],
            definition_fingerprint=row["definition_fingerprint"],
            applicability_conditions=_loads_json(row["applicability_conditions_json"], []),
            required_capabilities=_loads_json(row["required_capabilities_json"], []),
            required_permissions=_loads_json(row["required_permissions_json"], []),
            strategy_steps=_loads_json(row["strategy_steps_json"], []),
            verification_contract=_loads_json(row["verification_contract_json"], {}),
            known_failures=_loads_json(row["known_failures_json"], []),
            invalidating_conditions=_loads_json(row["invalidating_conditions_json"], []),
            source_episode_ids=_loads_json(row["source_episode_ids_json"], []),
            success_count=int(row["success_count"]),
            failure_count=int(row["failure_count"]),
            status=row["status"],
            last_validated_at=_parse_dt(row["last_validated_at"]),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
        )

    def _row_to_procedural_recipe_evaluation(
        self, row: sqlite3.Row
    ) -> ProceduralRecipeEvaluationRecord:
        return ProceduralRecipeEvaluationRecord(
            id=row["id"],
            recipe_id=row["recipe_id"],
            baseline_fingerprint=row["baseline_fingerprint"],
            candidate_fingerprint=row["candidate_fingerprint"],
            scenario_set_fingerprint=row["scenario_set_fingerprint"],
            common_trial_count=int(row["common_trial_count"]),
            baseline_success_rate=float(row["baseline_success_rate"]),
            candidate_success_rate=float(row["candidate_success_rate"]),
            regression_count=int(row["regression_count"]),
            improvement_count=int(row["improvement_count"]),
            passed=bool(row["passed"]),
            created_at=_parse_dt(row["created_at"]),
        )

    def _row_to_execution_episode_evidence(
        self, row: sqlite3.Row
    ) -> ExecutionEpisodeEvidenceRecord:
        return ExecutionEpisodeEvidenceRecord(
            episode_id=row["episode_id"],
            algorithm=row["algorithm"],
            key_id=row["key_id"],
            nonce=bytes(row["nonce"]),
            ciphertext=bytes(row["ciphertext"]),
            created_at=_parse_dt(row["created_at"]),
            expires_at=_parse_dt(row["expires_at"]),
        )

    def _row_to_permit(self, row: sqlite3.Row) -> PermitRecord:
        intent_type = _row_get(row, "intent_type", "")
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

    def _row_to_mcp_task(self, row: sqlite3.Row) -> MCPTaskRecord:
        return MCPTaskRecord(
            id=row["id"],
            server_id=row["server_id"],
            task_id=row["task_id"],
            protocol=row["protocol"],
            remote_status=row["remote_status"],
            runtime_status=row["runtime_status"],
            tool_name=row["tool_name"],
            auth_context_id=row["auth_context_id"],
            correlation_id=_row_get(row, "correlation_id"),
            trace_id=_row_get(row, "trace_id"),
            span_id=_row_get(row, "span_id"),
            worker_run_id=_row_get(row, "worker_run_id"),
            chat_id=_row_get(row, "chat_id"),
            chat_turn_id=_row_get(row, "chat_turn_id"),
            plan_run_id=_row_get(row, "plan_run_id"),
            plan_step_id=_row_get(row, "plan_step_id"),
            status_message=_row_get(row, "status_message"),
            ttl_ms=_row_get(row, "ttl_ms"),
            poll_interval_ms=_row_get(row, "poll_interval_ms"),
            input_requests=_loads_json(row["input_requests_json"], {}),
            responded_input_keys=_loads_json(
                _row_get(row, "responded_input_keys_json"),
                [],
            ),
            result=_loads_json(_row_get(row, "result_json")),
            error=_loads_json(_row_get(row, "error_json")),
            remote_created_at=_parse_dt(row["remote_created_at"]),
            remote_updated_at=_parse_dt(row["remote_updated_at"]),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
        )

    def _row_to_memory(self, row: sqlite3.Row) -> MemoryEntry:
        embedding = None
        # Check for new embedding column from JOIN first
        if "new_embedding_json" in row and row["new_embedding_json"]:
            embedding = json.loads(row["new_embedding_json"])
        elif row["embedding_json"]:
            embedding = json.loads(row["embedding_json"])

        return MemoryEntry(
            id=row["uuid"],
            role=row["role"],
            content=row["content"],
            embedding=embedding,
            created_at=_parse_dt(row["created_at"]),
            metadata=_loads_json(row["metadata_json"]),
        )

    def _row_to_memory_fact(self, row: sqlite3.Row) -> MemoryFactRecord:
        return MemoryFactRecord(
            id=row["id"],
            owner_id=row["owner_id"],
            subject=row["subject"],
            key=row["key"],
            value_text=row["value_text"],
            value_json=_loads_json(row["value_json"]),
            fact_type=row["fact_type"],
            confidence=float(row["confidence"]),
            status=row["status"],
            trust_state=row["trust_state"],
            valid_from=_parse_dt(row["valid_from"]) if row["valid_from"] else None,
            valid_to=_parse_dt(row["valid_to"]) if row["valid_to"] else None,
            facets=_loads_json(row["facets_json"], []),
            source_kind=_row_get(row, "source_kind"),
            source_ref=_row_get(row, "source_ref"),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
        )

    def _row_to_memory_fact_source(self, row: sqlite3.Row) -> MemoryFactSourceRecord:
        return MemoryFactSourceRecord(
            fact_id=row["fact_id"],
            memory_entry_uuid=_row_get(row, "memory_entry_uuid"),
            canon_filename=_row_get(row, "canon_filename"),
            source_note=_row_get(row, "source_note"),
            created_at=_parse_dt(row["created_at"]),
        )

    def _row_to_octo_diary_entry(self, row: sqlite3.Row) -> OctoDiaryEntryRecord:
        return OctoDiaryEntryRecord(
            id=row["id"],
            owner_id=row["owner_id"],
            chat_id=_row_get(row, "chat_id"),
            kind=row["kind"],
            summary=row["summary"],
            details=_loads_json(row["details_json"], {}),
            created_at=_parse_dt(row["created_at"]),
        )

    def _row_to_operational_memory_item(self, row: sqlite3.Row) -> OperationalMemoryItemRecord:
        return OperationalMemoryItemRecord(
            id=row["id"],
            owner_id=row["owner_id"],
            chat_id=_row_get(row, "chat_id"),
            kind=row["kind"],
            statement=row["statement"],
            next_action=_row_get(row, "next_action"),
            status=row["status"],
            priority=int(row["priority"]),
            confidence=float(row["confidence"]),
            source_kind=_row_get(row, "source_kind"),
            source_ref=_row_get(row, "source_ref"),
            plan_run_id=_row_get(row, "plan_run_id"),
            plan_step_id=_row_get(row, "plan_step_id"),
            evidence=_loads_json(row["evidence_json"], []),
            metadata=_loads_json(row["metadata_json"], {}),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
            resolved_at=_parse_dt(row["resolved_at"]) if row["resolved_at"] else None,
        )

    def _row_to_plan_run(self, row: sqlite3.Row) -> PlanRunRecord:
        return PlanRunRecord(
            id=row["id"],
            goal=row["goal"],
            status=row["status"],
            chat_id=_row_get(row, "chat_id"),
            source=row["source"],
            correlation_id=_row_get(row, "correlation_id"),
            current_step_id=_row_get(row, "current_step_id"),
            plan=_loads_json(row["plan_json"], {}),
            metadata=_loads_json(row["metadata_json"], {}),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
            completed_at=_parse_dt(row["completed_at"]) if row["completed_at"] else None,
        )

    def _row_to_plan_step(self, row: sqlite3.Row) -> PlanStepRecord:
        return PlanStepRecord(
            run_id=row["run_id"],
            step_id=row["step_id"],
            seq=int(row["seq"]),
            kind=row["kind"],
            title=row["title"],
            status=row["status"],
            task=_row_get(row, "task"),
            executor=_row_get(row, "executor"),
            worker_run_id=_row_get(row, "worker_run_id"),
            input=_loads_json(row["input_json"], {}),
            output=_loads_json(row["output_json"], {}),
            error=_row_get(row, "error"),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
            started_at=_parse_dt(row["started_at"]) if row["started_at"] else None,
            completed_at=_parse_dt(row["completed_at"]) if row["completed_at"] else None,
        )

    def _row_to_plan_event(self, row: sqlite3.Row) -> PlanEventRecord:
        return PlanEventRecord(
            id=row["id"],
            run_id=row["run_id"],
            step_id=_row_get(row, "step_id"),
            event_type=row["event_type"],
            data=_loads_json(row["data_json"], {}),
            created_at=_parse_dt(row["created_at"]),
        )

    def _backfill_memory_scope_columns(self) -> None:
        cursor = self._conn.execute("""
            SELECT id, metadata_json
            FROM memory_entries
            WHERE owner_id IS NULL OR owner_id = '' OR chat_id IS NULL
            """)
        rows = cursor.fetchall()
        if not rows:
            return

        for row in rows:
            metadata = _loads_json(row["metadata_json"])
            owner_id = str(metadata.get("owner_id", "default"))
            chat_id = metadata.get("chat_id")
            if chat_id is not None:
                try:
                    chat_id = int(chat_id)
                except (TypeError, ValueError):
                    chat_id = None
            self._conn.execute(
                "UPDATE memory_entries SET owner_id = ?, chat_id = ? WHERE id = ?",
                (owner_id, chat_id, row["id"]),
            )
        self._conn.commit()

    def _rebuild_memory_fts(self) -> None:
        try:
            self._conn.execute("DELETE FROM memory_entries_fts")
            self._conn.execute("""
                INSERT INTO memory_entries_fts (content, owner_id, chat_id, entry_uuid)
                SELECT content, COALESCE(owner_id, 'default'), chat_id, uuid
                FROM memory_entries
                """)
            self._conn.commit()
        except sqlite3.OperationalError:
            pass


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _mcp_task_transition_is_stale(
    current: MCPTaskRecord,
    incoming: MCPTaskRecord,
) -> bool:
    if incoming.remote_created_at != current.remote_created_at:
        return True
    if incoming.remote_updated_at < current.remote_updated_at:
        return True
    if current.remote_status in {"completed", "failed", "cancelled"}:
        return incoming.remote_status != current.remote_status
    return (
        current.runtime_status == "failed"
        and incoming.runtime_status != "failed"
        and incoming.remote_updated_at <= current.remote_updated_at
    )


def _merge_mcp_task_records(
    current: MCPTaskRecord,
    incoming: MCPTaskRecord,
) -> MCPTaskRecord:
    responded_input_keys = sorted({*current.responded_input_keys, *incoming.responded_input_keys})
    input_requests = {
        key: value
        for key, value in incoming.input_requests.items()
        if key not in responded_input_keys
    }
    preserve_terminal_payload = (
        current.remote_status == incoming.remote_status
        and current.remote_status in {"completed", "failed", "cancelled"}
    )
    return incoming.model_copy(
        update={
            "correlation_id": current.correlation_id,
            "trace_id": current.trace_id,
            "span_id": current.span_id,
            "worker_run_id": current.worker_run_id,
            "chat_id": current.chat_id,
            "chat_turn_id": current.chat_turn_id,
            "plan_run_id": current.plan_run_id,
            "plan_step_id": current.plan_step_id,
            "responded_input_keys": responded_input_keys,
            "input_requests": input_requests,
            "result": (
                current.result
                if preserve_terminal_payload and incoming.result is None
                else incoming.result
            ),
            "error": (
                current.error
                if preserve_terminal_payload and incoming.error is None
                else incoming.error
            ),
            "remote_created_at": current.remote_created_at,
            "created_at": current.created_at,
        }
    )


def _mcp_task_records_equivalent(
    current: MCPTaskRecord,
    incoming: MCPTaskRecord,
) -> bool:
    return current.model_dump(exclude={"updated_at"}) == incoming.model_dump(exclude={"updated_at"})


def _row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    """Safely get a value from a sqlite3.Row with a default."""
    try:
        return row[key]
    except (IndexError, KeyError):
        return default


def _loads_json(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return default


def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return json.dumps({"repr": repr(value)}, ensure_ascii=False)
