from __future__ import annotations

import sys
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import structlog

from octopal.infrastructure.observability.helpers import safe_preview
from octopal.runtime.octo import followup_delivery as _followup_delivery
from octopal.runtime.octo.control_replies import (
    _SCHEDULED_OCTO_CONTROL_BLOCKED,
    _SCHEDULED_OCTO_CONTROL_DONE,
    _normalize_scheduled_octo_control_notify_policy,
    _normalize_scheduled_octo_control_reply,
)
from octopal.runtime.octo.delivery import DeliveryMode, resolve_user_delivery
from octopal.runtime.octo.router import (
    route_scheduled_octo_control as _default_route_scheduled_octo_control,
)
from octopal.runtime.octo.router import (
    route_scheduled_octo_task as _default_route_scheduled_octo_task,
)
from octopal.runtime.octo.runtime_config import _env_int
from octopal.runtime.octo.scheduler_helpers import (
    _coerce_positive_chat_id,
    _coerce_signed_chat_id,
)
from octopal.runtime.scheduler.service import (
    SCHEDULED_TASK_BLOCKED_REASON_KEY,
    SCHEDULED_TASK_BLOCKED_UNTIL_KEY,
    SCHEDULED_TASK_DELIVERY_CHAT_ID_KEY,
    SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY,
    SCHEDULED_TASK_TARGET_CHAT_ID_KEY,
    normalize_notify_user_policy,
    parse_scheduled_task_blocked_until,
)
from octopal.runtime.workers.allowed_paths import (
    infer_allowed_paths_from_values,
    normalize_allowed_paths,
)
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)

_default_send_scheduler_control_update = _followup_delivery._send_scheduler_control_update
_DEFAULT_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS = float(
    _env_int(
        "OCTOPAL_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS",
        1800,
        minimum=0,
    )
)


def _core_callable(name: str, default: Callable[..., Any]) -> Callable[..., Any]:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is not None:
        candidate = getattr(core_module, name, None)
        if callable(candidate):
            return candidate
    return default


def _core_value(name: str, default: Any) -> Any:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is not None and hasattr(core_module, name):
        return getattr(core_module, name)
    return default


def _scheduled_octo_control_backoff_seconds() -> float:
    return float(
        _core_value(
            "_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS",
            _DEFAULT_SCHEDULED_OCTO_CONTROL_BACKOFF_SECONDS,
        )
    )


def _scheduled_workspace_dir(octo: Any) -> Any:
    scheduler = getattr(octo, "scheduler", None)
    workspace_dir = getattr(scheduler, "workspace_dir", None)
    if workspace_dir is not None:
        return workspace_dir
    canon = getattr(octo, "canon", None)
    return getattr(canon, "workspace_dir", None)


def _scheduled_task_allowed_paths(
    octo: Any,
    task: dict[str, Any],
    *,
    task_text: str,
    inputs: dict[str, Any],
) -> list[str] | None:
    workspace_dir = _scheduled_workspace_dir(octo)
    metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    explicit = normalize_allowed_paths(
        metadata.get("allowed_paths") if isinstance(metadata, dict) else None,
        workspace_dir=workspace_dir,
    )
    if explicit:
        return explicit
    explicit = normalize_allowed_paths(
        inputs.get("allowed_paths"),
        workspace_dir=workspace_dir,
    )
    if explicit:
        return explicit
    return infer_allowed_paths_from_values(task_text, inputs, workspace_dir=workspace_dir)


class OctoScheduledRuntimeMixin:
    def _get_scheduled_octo_control_backoff(self, task_id: str) -> tuple[float, str] | None:
        task_id_value = str(task_id or "").strip()
        if not task_id_value:
            return None
        backoff_map = self._scheduled_octo_control_backoff_by_task
        if not isinstance(backoff_map, dict):
            return None
        entry = backoff_map.get(task_id_value)
        if not entry:
            return None
        deadline, reason = entry
        remaining = float(deadline) - time.monotonic()
        if remaining <= 0:
            backoff_map.pop(task_id_value, None)
            return None
        return remaining, str(reason or "").strip() or "runtime_backoff"

    def _set_scheduled_octo_control_backoff(self, task_id: str, *, reason: str) -> None:
        task_id_value = str(task_id or "").strip()
        backoff_seconds = _scheduled_octo_control_backoff_seconds()
        if not task_id_value or backoff_seconds <= 0:
            return
        backoff_map = self._scheduled_octo_control_backoff_by_task
        if backoff_map is None:
            backoff_map = {}
            self._scheduled_octo_control_backoff_by_task = backoff_map
        backoff_map[task_id_value] = (
            time.monotonic() + backoff_seconds,
            str(reason or "").strip() or "runtime_backoff",
        )

    def _get_persisted_scheduled_octo_control_backoff(
        self,
        task: dict[str, Any],
    ) -> tuple[float, str] | None:
        metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
        if not isinstance(metadata, dict):
            return None
        blocked_until = parse_scheduled_task_blocked_until(metadata)
        if blocked_until is None:
            return None
        remaining = (blocked_until - utc_now()).total_seconds()
        if remaining <= 0:
            self._update_scheduled_octo_control_backoff_metadata(
                task,
                blocked_until=None,
                reason=None,
            )
            return None
        reason = (
            str(metadata.get(SCHEDULED_TASK_BLOCKED_REASON_KEY) or "").strip() or "blocked_by_route"
        )
        return remaining, reason

    def _update_scheduled_octo_control_backoff_metadata(
        self,
        task: dict[str, Any],
        *,
        blocked_until: datetime | None,
        reason: str | None,
    ) -> None:
        scheduler = self.scheduler
        store = getattr(scheduler, "store", None)
        update_metadata = getattr(store, "update_scheduled_task_metadata", None)
        if scheduler is None or not callable(update_metadata):
            return
        task_id = str(task.get("id") or "").strip()
        if not task_id:
            return
        metadata = (
            dict(task.get("metadata") or {}) if isinstance(task.get("metadata"), dict) else {}
        )
        if blocked_until is None:
            metadata.pop(SCHEDULED_TASK_BLOCKED_UNTIL_KEY, None)
        else:
            metadata[SCHEDULED_TASK_BLOCKED_UNTIL_KEY] = blocked_until.isoformat()
        reason_value = str(reason or "").strip()
        if reason_value:
            metadata[SCHEDULED_TASK_BLOCKED_REASON_KEY] = reason_value
        else:
            metadata.pop(SCHEDULED_TASK_BLOCKED_REASON_KEY, None)
        if reason_value == "blocked_by_route":
            if str(task.get("worker_id") or "").strip():
                metadata[SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY] = "worker"
            else:
                metadata[SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY] = "octo_task"
        elif blocked_until is None:
            metadata.pop(SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY, None)
        try:
            update_metadata(task_id, metadata or None)
        except Exception:
            logger.exception(
                "Failed to persist scheduled Octo control backoff metadata",
                task_id=task_id or None,
            )
            return
        task["metadata"] = metadata
        task["blocked_until"] = metadata.get(SCHEDULED_TASK_BLOCKED_UNTIL_KEY)
        task["blocked_reason"] = metadata.get(SCHEDULED_TASK_BLOCKED_REASON_KEY)
        task["suggested_execution_mode"] = metadata.get(SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY)

    def _resolve_scheduled_task_delivery_chat_id(
        self,
        task: dict[str, Any],
        *,
        requested_chat_id: int = 0,
    ) -> tuple[int | None, str]:
        notify_user = normalize_notify_user_policy(task.get("notify_user"))
        if notify_user == "never":
            return 0, "notify_never"

        metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
        explicit = _coerce_signed_chat_id(
            task.get("delivery_chat_id")
            or metadata.get(SCHEDULED_TASK_DELIVERY_CHAT_ID_KEY)
            or metadata.get(SCHEDULED_TASK_TARGET_CHAT_ID_KEY)
        )
        if explicit is not None:
            return explicit, "task_metadata"

        requested = _coerce_signed_chat_id(requested_chat_id)
        if requested is not None:
            return requested, "request_context"

        configured = [
            chat_id
            for item in (self._scheduled_delivery_chat_ids or [])
            if (chat_id := _coerce_positive_chat_id(item)) is not None
        ]
        unique_configured = list(dict.fromkeys(configured))
        if len(unique_configured) == 1:
            return unique_configured[0], "single_configured_recipient"

        return None, "missing_delivery_target"

    def _resolve_scheduler_delivery_chat_id(
        self,
        *,
        requested_chat_id: int = 0,
    ) -> tuple[int | None, str]:
        requested = _coerce_signed_chat_id(requested_chat_id)
        if requested is not None:
            return requested, "request_context"

        configured = [
            chat_id
            for item in (self._scheduled_delivery_chat_ids or [])
            if (chat_id := _coerce_positive_chat_id(item)) is not None
        ]
        unique_configured = list(dict.fromkeys(configured))
        if len(unique_configured) == 1:
            return unique_configured[0], "single_configured_recipient"

        return None, "missing_delivery_target"

    async def _dispatch_due_scheduled_tasks_once(
        self,
        *,
        chat_id: int = 0,
        max_tasks: int = 10,
    ) -> dict[str, Any]:
        scheduler = self.scheduler
        summary: dict[str, Any] = {
            "due_count": 0,
            "attempted": 0,
            "started": 0,
            "completed": 0,
            "duplicates": 0,
            "rejected_by_policy": 0,
            "policy_reasons": {},
            "errors": 0,
        }
        if scheduler is None:
            return summary

        due_tasks = list(scheduler.get_actionable_tasks() or [])
        summary["due_count"] = len(due_tasks)
        for task in due_tasks[: max(1, int(max_tasks))]:
            task_id = str(task.get("id") or "").strip()
            execution_mode = str(task.get("execution_mode") or "").strip().lower()
            worker_id = str(task.get("worker_id") or "").strip()
            task_text = str(task.get("task_text") or "").strip()
            inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
            dispatch_chat_id, delivery_target_source = (
                self._resolve_scheduled_task_delivery_chat_id(
                    task,
                    requested_chat_id=chat_id,
                )
            )
            if dispatch_chat_id is None:
                summary["rejected_by_policy"] += 1
                reason = "missing_delivery_target"
                policy_reasons = summary.setdefault("policy_reasons", {})
                if isinstance(policy_reasons, dict):
                    policy_reasons[reason] = int(policy_reasons.get(reason, 0) or 0) + 1
                logger.warning(
                    "Rejected scheduled task by delivery policy",
                    task_id=task_id or None,
                    notify_user=task.get("notify_user"),
                    delivery_target_source=delivery_target_source,
                )
                continue
            if execution_mode == "octo_control":
                persisted_backoff = self._get_persisted_scheduled_octo_control_backoff(task)
                if persisted_backoff is not None:
                    remaining_seconds, backoff_reason = persisted_backoff
                    logger.info(
                        "Skipping scheduled Octo control task during persisted backoff",
                        task_id=task_id or None,
                        chat_id=dispatch_chat_id,
                        backoff_reason=backoff_reason,
                        cooldown_seconds=round(remaining_seconds, 1),
                    )
                    continue
                backoff = self._get_scheduled_octo_control_backoff(task_id)
                if backoff is not None:
                    remaining_seconds, backoff_reason = backoff
                    logger.info(
                        "Skipping scheduled Octo control task during runtime backoff",
                        task_id=task_id or None,
                        chat_id=dispatch_chat_id,
                        backoff_reason=backoff_reason,
                        cooldown_seconds=round(remaining_seconds, 1),
                    )
                    continue
                summary["attempted"] += 1
                try:
                    result = await self._run_scheduled_octo_control_task_once(
                        task=task,
                        chat_id=dispatch_chat_id,
                    )
                except Exception:
                    summary["errors"] += 1
                    logger.exception(
                        "Scheduled Octo control task failed",
                        task_id=task_id or None,
                    )
                    continue
                if bool(result.get("completed")):
                    summary["completed"] += 1
                elif str(result.get("status") or "").strip().lower() == "failed":
                    summary["errors"] += 1
                continue
            if execution_mode == "octo_task":
                summary["attempted"] += 1
                try:
                    result = await self._run_scheduled_octo_task_once(
                        task=task,
                        chat_id=dispatch_chat_id,
                    )
                except Exception:
                    summary["errors"] += 1
                    logger.exception(
                        "Scheduled Octo task failed",
                        task_id=task_id or None,
                    )
                    continue
                if bool(result.get("completed")):
                    summary["completed"] += 1
                elif str(result.get("status") or "").strip().lower() == "failed":
                    summary["errors"] += 1
                continue
            if not worker_id or not task_text:
                summary["rejected_by_policy"] += 1
                reason = "missing_worker_id" if not worker_id else "missing_task_text"
                policy_reasons = summary.setdefault("policy_reasons", {})
                if isinstance(policy_reasons, dict):
                    policy_reasons[reason] = int(policy_reasons.get(reason, 0) or 0) + 1
                logger.warning(
                    "Rejected scheduled task by dispatch policy",
                    task_id=task_id or None,
                    policy_reason=reason,
                    worker_id=worker_id or None,
                    has_task_text=bool(task_text),
                )
                continue

            summary["attempted"] += 1
            try:
                allowed_paths = _scheduled_task_allowed_paths(
                    self,
                    task,
                    task_text=task_text,
                    inputs=inputs,
                )
                result = await self._start_worker_async(
                    worker_id=worker_id,
                    task=task_text,
                    chat_id=dispatch_chat_id,
                    inputs=inputs,
                    tools=None,
                    model=None,
                    timeout_seconds=None,
                    allowed_paths=allowed_paths,
                    scheduled_task_id=task_id or None,
                )
            except Exception:
                summary["errors"] += 1
                logger.exception(
                    "Scheduled task dispatch failed",
                    task_id=task_id or None,
                    worker_id=worker_id,
                )
                continue

            status = str(result.get("status") or "").strip().lower()
            if status == "started":
                summary["started"] += 1
            elif status == "skipped_duplicate":
                summary["duplicates"] += 1
            elif status in {"rejected", "failed"}:
                summary["errors"] += 1

        return summary

    async def _run_scheduled_octo_control_task_once(
        self,
        *,
        task: dict[str, Any],
        chat_id: int = 0,
    ) -> dict[str, Any]:
        scheduler = self.scheduler
        task_id = str(task.get("id") or "").strip()
        notify_user = _normalize_scheduled_octo_control_notify_policy(task.get("notify_user"))
        route_scheduled_octo_control = _core_callable(
            "route_scheduled_octo_control",
            _default_route_scheduled_octo_control,
        )
        reply_text = await route_scheduled_octo_control(
            self,
            task,
            chat_id=chat_id,
        )
        normalized_reply = await _normalize_scheduled_octo_control_reply(self.provider, reply_text)
        route_blocked = normalized_reply == _SCHEDULED_OCTO_CONTROL_BLOCKED
        if route_blocked:
            backoff_seconds = _scheduled_octo_control_backoff_seconds()
            self._set_scheduled_octo_control_backoff(task_id, reason="blocked_by_route")
            self._update_scheduled_octo_control_backoff_metadata(
                task,
                blocked_until=utc_now() + timedelta(seconds=backoff_seconds),
                reason="blocked_by_route",
            )
            logger.warning(
                "Scheduled Octo control task blocked by bounded route",
                task_id=task_id or None,
                chat_id=chat_id,
                raw_reply_preview=safe_preview(reply_text, limit=200),
                cooldown_seconds=backoff_seconds,
            )
            return {
                "status": "failed",
                "completed": False,
                "reason": "blocked_by_route",
                "cooldown_seconds": backoff_seconds,
            }
        if normalized_reply == "NO_USER_RESPONSE":
            logger.warning(
                "Scheduled Octo control task missing explicit completion signal",
                task_id=task_id or None,
                chat_id=chat_id,
                raw_reply_preview=safe_preview(reply_text, limit=200),
            )
            return {
                "status": "failed",
                "completed": False,
                "reason": "missing_completion_signal",
            }
        if normalized_reply == _SCHEDULED_OCTO_CONTROL_DONE:
            self._update_scheduled_octo_control_backoff_metadata(
                task,
                blocked_until=None,
                reason=None,
            )
            if scheduler is not None and task_id:
                scheduler.mark_executed(task_id)
            logger.info(
                "Scheduled Octo control task completed silently",
                task_id=task_id or None,
                notify_user=notify_user,
                chat_id=chat_id,
            )
            return {
                "status": "completed",
                "completed": True,
                "user_visible_sent": False,
                "delivery_mode": DeliveryMode.SILENT,
            }
        delivery = resolve_user_delivery(normalized_reply)
        user_visible_sent = False
        if delivery.user_visible and notify_user != "never":
            send_scheduler_control_update = _core_callable(
                "_send_scheduler_control_update",
                _default_send_scheduler_control_update,
            )
            await send_scheduler_control_update(
                self,
                chat_id,
                task_id or None,
                delivery.text,
            )
            user_visible_sent = True
        elif delivery.user_visible:
            logger.info(
                "Scheduled Octo control update suppressed by notify policy",
                task_id=task_id or None,
                notify_user=notify_user,
                chat_id=chat_id,
            )
        if scheduler is not None and task_id:
            scheduler.mark_executed(task_id)
        logger.info(
            "Scheduled Octo control task completed",
            task_id=task_id or None,
            notify_user=notify_user,
            chat_id=chat_id,
            user_visible_sent=user_visible_sent,
            delivery_mode=delivery.mode,
        )
        return {
            "status": "completed",
            "completed": True,
            "user_visible_sent": user_visible_sent,
            "delivery_mode": delivery.mode,
        }

    async def _run_scheduled_octo_task_once(
        self,
        *,
        task: dict[str, Any],
        chat_id: int = 0,
    ) -> dict[str, Any]:
        scheduler = self.scheduler
        task_id = str(task.get("id") or "").strip()
        notify_user = normalize_notify_user_policy(task.get("notify_user"))
        route_scheduled_octo_task = _core_callable(
            "route_scheduled_octo_task",
            _default_route_scheduled_octo_task,
        )
        reply_text = await route_scheduled_octo_task(
            self,
            task,
            chat_id=chat_id,
        )
        normalized_reply = await _normalize_scheduled_octo_control_reply(
            self.provider,
            reply_text,
            bounded_control=False,
        )
        route_blocked = normalized_reply == _SCHEDULED_OCTO_CONTROL_BLOCKED
        if route_blocked:
            logger.warning(
                "Scheduled Octo task reported blocked",
                task_id=task_id or None,
                chat_id=chat_id,
                raw_reply_preview=safe_preview(reply_text, limit=200),
            )
            return {
                "status": "failed",
                "completed": False,
                "reason": "blocked",
            }
        if normalized_reply == "NO_USER_RESPONSE":
            logger.warning(
                "Scheduled Octo task missing explicit completion signal",
                task_id=task_id or None,
                chat_id=chat_id,
                raw_reply_preview=safe_preview(reply_text, limit=200),
            )
            return {
                "status": "failed",
                "completed": False,
                "reason": "missing_completion_signal",
            }
        if normalized_reply == _SCHEDULED_OCTO_CONTROL_DONE:
            if scheduler is not None and task_id:
                scheduler.mark_executed(task_id)
            logger.info(
                "Scheduled Octo task completed silently",
                task_id=task_id or None,
                notify_user=notify_user,
                chat_id=chat_id,
            )
            return {
                "status": "completed",
                "completed": True,
                "user_visible_sent": False,
                "delivery_mode": DeliveryMode.SILENT,
            }

        delivery = resolve_user_delivery(normalized_reply)
        user_visible_sent = False
        if delivery.user_visible and notify_user != "never":
            send_scheduler_control_update = _core_callable(
                "_send_scheduler_control_update",
                _default_send_scheduler_control_update,
            )
            await send_scheduler_control_update(
                self,
                chat_id,
                task_id or None,
                delivery.text,
            )
            user_visible_sent = True
        elif delivery.user_visible:
            logger.info(
                "Scheduled Octo task update suppressed by notify policy",
                task_id=task_id or None,
                notify_user=notify_user,
                chat_id=chat_id,
            )
        if scheduler is not None and task_id:
            scheduler.mark_executed(task_id)
        logger.info(
            "Scheduled Octo task completed",
            task_id=task_id or None,
            notify_user=notify_user,
            chat_id=chat_id,
            user_visible_sent=user_visible_sent,
            delivery_mode=delivery.mode,
        )
        return {
            "status": "completed",
            "completed": True,
            "user_visible_sent": user_visible_sent,
            "delivery_mode": delivery.mode,
        }
