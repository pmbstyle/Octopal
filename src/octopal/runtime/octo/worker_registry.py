from __future__ import annotations

from datetime import timedelta
from typing import Any

import structlog

from octopal.runtime.octo.worker_records import _is_active_worker_status
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)


class OctoWorkerRegistryMixin:
    def _reconcile_stale_worker_records(self) -> None:
        """Normalize stale DB worker states that no longer exist in runtime."""
        runtime = self.runtime
        if not runtime or not hasattr(runtime, "is_worker_running"):
            return
        workers = self.store.get_active_workers(older_than_minutes=120)
        if not workers:
            return
        grace_cutoff = utc_now() - timedelta(minutes=2)
        reconciled = 0
        for worker in workers:
            if worker.status not in {"started", "running"}:
                continue
            if worker.updated_at >= grace_cutoff:
                continue
            if runtime.is_worker_running(worker.id):
                continue
            self.store.update_worker_status(worker.id, "stopped")
            self.store.update_worker_result(
                worker.id,
                error="Worker process not found in runtime; stale running state reconciled.",
            )
            self._mark_worker_inactive(worker.id)
            reconciled += 1
        if reconciled > 0:
            logger.info("Reconciled stale worker records", reconciled_workers=reconciled)

    def _restore_worker_registry_state(self) -> None:
        """Restore lineage/child bookkeeping from persisted workers."""
        if not hasattr(self.store, "list_workers"):
            return
        try:
            workers = list(self.store.list_workers() or [])
        except Exception:
            logger.debug("Skipping worker registry restore: list_workers failed", exc_info=True)
            return
        if not workers:
            return

        self._worker_children.clear()
        self._worker_lineage.clear()
        self._worker_depth.clear()
        self._lineage_children_total.clear()
        self._lineage_children_active.clear()
        self._worker_correlation_by_run_id.clear()
        self._worker_chat_by_run_id.clear()
        self._scheduled_notify_user_by_run_id.clear()
        self._active_workers_by_correlation.clear()
        self._pending_internal_results_by_correlation.clear()

        worker_by_id: dict[str, Any] = {}
        for worker in workers:
            run_id = str(getattr(worker, "id", "") or "").strip()
            if not run_id:
                continue
            worker_by_id[run_id] = worker
            lineage_id = str(getattr(worker, "lineage_id", "") or run_id).strip() or run_id
            depth = max(0, int(getattr(worker, "spawn_depth", 0) or 0))
            correlation_id = str(getattr(worker, "correlation_id", "") or "").strip() or None
            self._worker_lineage[run_id] = lineage_id
            self._worker_depth[run_id] = depth
            self._worker_chat_by_run_id[run_id] = int(getattr(worker, "chat_id", 0) or 0)
            if correlation_id:
                self._worker_correlation_by_run_id[run_id] = correlation_id

        orphan_reconciled = 0
        for run_id, worker in worker_by_id.items():
            parent_worker_id = str(getattr(worker, "parent_worker_id", "") or "").strip()
            if not parent_worker_id:
                continue
            if parent_worker_id not in worker_by_id:
                if _is_active_worker_status(getattr(worker, "status", "")):
                    self.store.update_worker_status(run_id, "stopped")
                    self.store.update_worker_result(
                        run_id,
                        error=(
                            "Orphaned child worker reconciled during startup: "
                            "parent worker record is missing."
                        ),
                    )
                    orphan_reconciled += 1
                continue
            lineage_id = self._worker_lineage.get(run_id, run_id)
            self._worker_children.setdefault(parent_worker_id, set()).add(run_id)
            self._lineage_children_total[lineage_id] = (
                int(self._lineage_children_total.get(lineage_id, 0)) + 1
            )
            if _is_active_worker_status(getattr(worker, "status", "")):
                correlation_id = self._worker_correlation_by_run_id.get(run_id)
                if correlation_id:
                    self._active_workers_by_correlation.setdefault(correlation_id, set()).add(
                        run_id
                    )
                self._lineage_children_active.setdefault(lineage_id, set()).add(run_id)

        stale_reconciled = self._reconcile_startup_stale_workers(worker_by_id)
        if orphan_reconciled or stale_reconciled:
            logger.info(
                "Restored worker registry state",
                workers_seen=len(worker_by_id),
                orphan_reconciled=orphan_reconciled,
                stale_reconciled=stale_reconciled,
            )

    def _reconcile_startup_stale_workers(self, worker_by_id: dict[str, Any]) -> int:
        runtime = self.runtime
        if not runtime or not hasattr(runtime, "is_worker_running"):
            return 0
        reconciled = 0
        for run_id, worker in worker_by_id.items():
            if not _is_active_worker_status(getattr(worker, "status", "")):
                continue
            if runtime.is_worker_running(run_id):
                continue
            self.store.update_worker_status(run_id, "stopped")
            self.store.update_worker_result(
                run_id,
                error="Worker process not found in runtime during startup reconciliation.",
            )
            self._mark_worker_inactive(run_id)
            reconciled += 1
        return reconciled
