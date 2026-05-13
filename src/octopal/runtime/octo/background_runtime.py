from __future__ import annotations

import asyncio
import sys

import structlog

from octopal.browser.manager import get_browser_manager as _default_get_browser_manager
from octopal.runtime.housekeeping import (
    cleanup_ephemeral_worker_dirs,
    cleanup_workspace_tmp,
    rotate_canon_events,
)
from octopal.runtime.metrics import update_component_gauges
from octopal.runtime.octo.runtime_config import _env_int

logger = structlog.get_logger(__name__)


def _get_browser_manager():
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is not None:
        resolver = getattr(core_module, "get_browser_manager", None)
        if callable(resolver):
            return resolver()
    return _default_get_browser_manager()


class OctoBackgroundRuntimeMixin:
    async def _periodic_cleanup(self, interval_seconds: int):
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                deleted = await asyncio.to_thread(self.store.cleanup_old_workers)
                if deleted > 0:
                    logger.info("Periodic cleanup complete", deleted_workers=deleted)

                cfg = self._housekeeping_cfg or {}
                worker_result = await asyncio.to_thread(
                    cleanup_ephemeral_worker_dirs,
                    self.canon.workspace_dir,
                    retention_minutes=int(cfg.get("worker_dir_retention_minutes", 15)),
                    docker_cleanup_image=getattr(self.runtime.launcher, "image", None),
                )
                if worker_result.deleted_dirs or worker_result.errors:
                    logger.info(
                        "Ephemeral worker dir cleanup complete",
                        deleted_dirs=worker_result.deleted_dirs,
                        errors=worker_result.errors,
                    )

                tmp_result = await asyncio.to_thread(
                    cleanup_workspace_tmp,
                    self.canon.workspace_dir,
                    retention_hours=int(cfg.get("tmp_retention_hours", 48)),
                )
                if tmp_result.deleted_files or tmp_result.deleted_dirs or tmp_result.errors:
                    logger.info(
                        "Workspace tmp cleanup complete",
                        deleted_files=tmp_result.deleted_files,
                        deleted_dirs=tmp_result.deleted_dirs,
                        errors=tmp_result.errors,
                    )

                rotate_result = await asyncio.to_thread(
                    rotate_canon_events,
                    self.canon.workspace_dir,
                    max_bytes=int(cfg.get("canon_events_max_bytes", 2_000_000)),
                    keep_archives=int(cfg.get("canon_events_keep_archives", 7)),
                )
                if rotate_result.rotated or rotate_result.deleted_archives:
                    logger.info(
                        "Canon events rotation complete",
                        rotated=rotate_result.rotated,
                        archived_file=rotate_result.archived_file,
                        deleted_archives=rotate_result.deleted_archives,
                        bootstrap_entries=rotate_result.bootstrap_entries,
                    )
            except Exception:
                logger.exception("Periodic worker cleanup failed")

    async def _periodic_metrics_publish(self, interval_seconds: int):
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await asyncio.to_thread(self._reconcile_stale_worker_records)
                mcp_status = {}
                if self.mcp_manager:
                    mcp_status = self.mcp_manager.get_server_statuses()

                update_component_gauges("connectivity", {"mcp_servers": mcp_status})
            except Exception:
                logger.debug("Failed to publish periodic metrics", exc_info=True)

    async def _periodic_scheduler_tick(self, interval_seconds: int, *, max_tasks: int = 10) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            await self._run_scheduler_tick_once(chat_id=0, max_tasks=max_tasks)

    def start_background_tasks(
        self,
        cleanup_interval_seconds: int = 3600,
        *,
        scheduler_interval_seconds: int | None = None,
    ):
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(
                self._periodic_cleanup(cleanup_interval_seconds)
            )
            logger.info("Started periodic worker cleanup task")
        if self._metrics_task is None or self._metrics_task.done():
            self._metrics_task = asyncio.create_task(self._periodic_metrics_publish(10))
            logger.info("Started periodic metrics publishing task")
        if self.scheduler and (self._scheduler_task is None or self._scheduler_task.done()):
            resolved_interval = scheduler_interval_seconds or _env_int(
                "OCTOPAL_SCHEDULER_TICK_INTERVAL_SECONDS", 60, minimum=5
            )
            max_tasks = _env_int("OCTOPAL_SCHEDULER_TICK_MAX_TASKS", 10, minimum=1)
            self._scheduler_interval_seconds = int(resolved_interval)
            self._scheduler_max_tasks = int(max_tasks)
            self._publish_scheduler_metrics(
                running=True,
                interval_seconds=resolved_interval,
                max_tasks=max_tasks,
                last_tick_status="starting",
            )
            self._scheduler_task = asyncio.create_task(
                self._periodic_scheduler_tick(resolved_interval, max_tasks=max_tasks)
            )
            logger.info(
                "Started periodic scheduler tick task",
                interval_seconds=resolved_interval,
                max_tasks=max_tasks,
            )
        if self._self_control_task is None or self._self_control_task.done():
            self._self_control_task = asyncio.create_task(self._periodic_self_control_requests())
            logger.info("Started self-control request executor")

    async def stop_background_tasks(self):
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic worker cleanup task")

        if self._metrics_task and not self._metrics_task.done():
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic metrics publishing task")
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                logger.info("Stopped periodic scheduler tick task")
        if self.scheduler is not None:
            self._publish_scheduler_metrics(running=False, last_tick_status="stopped")
        if self._self_control_task and not self._self_control_task.done():
            self._self_control_task.cancel()
            try:
                await self._self_control_task
            except asyncio.CancelledError:
                logger.info("Stopped self-control request executor")

        if self.mcp_manager:
            await self.mcp_manager.shutdown()

        await _get_browser_manager().shutdown()
