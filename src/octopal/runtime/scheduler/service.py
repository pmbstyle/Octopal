from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import structlog

from octopal.infrastructure.store.base import Store
from octopal.runtime.octo.scheduler_helpers import _coerce_signed_chat_id
from octopal.runtime.workers.allowed_paths import normalize_allowed_paths
from octopal.runtime.workers.loader import get_worker_template
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)

_EVERY_MINUTES_RE = re.compile(r"^every\s+(\d+)\s+minutes?$", re.IGNORECASE)
_EVERY_HOURS_RE = re.compile(r"^every\s+(\d+)\s+hours?$", re.IGNORECASE)
_DAILY_AT_RE = re.compile(r"^daily\s+at\s+(\d{1,2}):(\d{2})$", re.IGNORECASE)
_NOTIFY_USER_POLICIES = {"never", "if_significant", "always"}
_EXECUTION_MODES = {"worker", "octo_control", "octo_task"}
SCHEDULED_TASK_DELIVERY_CHAT_ID_KEY = "delivery_chat_id"
SCHEDULED_TASK_TARGET_CHAT_ID_KEY = "target_chat_id"
SCHEDULED_TASK_BLOCKED_UNTIL_KEY = "blocked_until"
SCHEDULED_TASK_BLOCKED_REASON_KEY = "blocked_reason"
SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY = "suggested_execution_mode"


def normalize_notify_user_policy(notify_user: str | None) -> str:
    value = str(notify_user or "if_significant").strip().lower()
    if value not in _NOTIFY_USER_POLICIES:
        allowed = ", ".join(sorted(_NOTIFY_USER_POLICIES))
        raise ValueError(f"notify_user must be one of: {allowed}.")
    return value


def normalize_execution_mode(
    execution_mode: str | None,
    *,
    worker_id: str | None = None,
) -> str:
    value = str(execution_mode or "").strip().lower()
    aliases = {
        "octo": "octo_task",
        "octo_self": "octo_task",
        "self": "octo_task",
    }
    value = aliases.get(value, value)
    if not value:
        return "worker" if str(worker_id or "").strip() else "octo_task"
    if value not in _EXECUTION_MODES:
        allowed = ", ".join(sorted(_EXECUTION_MODES))
        raise ValueError(f"execution_mode must be one of: {allowed}.")
    return value


def parse_scheduled_task_blocked_until(metadata: dict[str, Any]) -> datetime | None:
    raw_value = metadata.get(SCHEDULED_TASK_BLOCKED_UNTIL_KEY)
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def parse_scheduled_task_suggested_execution_mode(metadata: dict[str, Any]) -> str | None:
    value = str(metadata.get(SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY) or "").strip().lower()
    if not value:
        return None
    return value if value in _EXECUTION_MODES else None


def normalize_delivery_chat_id(delivery_chat_id: Any) -> str | None:
    if delivery_chat_id is None:
        return None
    raw_value = str(delivery_chat_id).strip()
    if not raw_value:
        return None
    chat_id = _coerce_signed_chat_id(raw_value)
    if chat_id is None:
        raise ValueError("delivery_chat_id must be a non-zero integer chat ID.")
    return str(chat_id)


class SchedulerService:
    def __init__(self, store: Store, workspace_dir: Path) -> None:
        self.store = store
        self.workspace_dir = workspace_dir
        self.heartbeat_md = workspace_dir / "HEARTBEAT.md"

    def schedule_task(
        self,
        name: str,
        frequency: str,
        task_text: str,
        description: str | None = None,
        worker_id: str | None = None,
        inputs: dict | None = None,
        allowed_paths: list[str] | None = None,
        notify_user: str | None = None,
        execution_mode: str | None = None,
        delivery_chat_id: Any = None,
    ) -> str:
        """Add or update a scheduled task."""
        normalized_frequency = self._validate_and_normalize_frequency(frequency)
        normalized_notify_user = normalize_notify_user_policy(notify_user)
        normalized_execution_mode = normalize_execution_mode(
            execution_mode,
            worker_id=worker_id,
        )
        worker_id_value = str(worker_id or "").strip() or None
        if normalized_execution_mode == "octo_control" and normalized_notify_user == "if_significant":
            normalized_notify_user = "never"
        if normalized_execution_mode == "worker" and not worker_id_value:
            raise ValueError("worker_id is required when execution_mode=worker.")
        if normalized_execution_mode in {"octo_control", "octo_task"} and worker_id_value:
            raise ValueError(
                f"worker_id must be omitted when execution_mode={normalized_execution_mode}."
            )
        normalized_allowed_paths = normalize_allowed_paths(
            allowed_paths,
            workspace_dir=self.workspace_dir,
        )
        if normalized_allowed_paths and normalized_execution_mode != "worker":
            raise ValueError("allowed_paths can only be used when execution_mode=worker.")
        normalized_delivery_chat_id = normalize_delivery_chat_id(delivery_chat_id)
        task_id = self._generate_id(name)
        metadata: dict[str, Any] = {
            "notify_user": normalized_notify_user,
            "execution_mode": normalized_execution_mode,
        }
        if normalized_allowed_paths:
            metadata["allowed_paths"] = normalized_allowed_paths
        if normalized_delivery_chat_id is not None:
            metadata[SCHEDULED_TASK_DELIVERY_CHAT_ID_KEY] = normalized_delivery_chat_id
        self.store.upsert_scheduled_task(
            task_id=task_id,
            name=name,
            frequency=normalized_frequency,
            task_text=task_text,
            description=description,
            worker_id=worker_id_value,
            inputs=inputs,
            metadata=metadata,
        )
        self.sync_to_markdown()
        return task_id

    def remove_task(self, task_id: str) -> None:
        self.store.delete_scheduled_task(task_id)
        self.sync_to_markdown()

    def repair_suggested_tasks(
        self,
        *,
        apply: bool = False,
        task_ids: list[str] | None = None,
        worker_id: str | None = None,
        allow_worker_id_override: bool = True,
        required_blocked_reason: str | None = None,
    ) -> dict[str, Any]:
        selected_ids = {str(item or "").strip() for item in (task_ids or []) if str(item or "").strip()}
        provided_worker_id = str(worker_id or "").strip() or None
        if not allow_worker_id_override:
            provided_worker_id = None
        required_blocked_reason_value = str(required_blocked_reason or "").strip() or None
        described = self.describe_tasks(enabled_only=False)
        candidates: list[dict[str, Any]] = []
        applied_items: list[dict[str, Any]] = []
        skipped_items: list[dict[str, Any]] = []

        for task in described:
            task_id = str(task.get("id") or "").strip()
            if selected_ids and task_id not in selected_ids:
                continue
            suggested_mode = str(task.get("suggested_execution_mode") or "").strip().lower()
            if not suggested_mode:
                continue
            resolved_worker_id = str(task.get("worker_id") or "").strip() or provided_worker_id
            blocked_reason = str(task.get("blocked_reason") or "").strip() or None
            can_apply = True
            skip_reason = None
            if required_blocked_reason_value and blocked_reason != required_blocked_reason_value:
                can_apply = False
                skip_reason = "blocked_reason_mismatch"
            if suggested_mode == "worker":
                if not resolved_worker_id:
                    can_apply = False
                    skip_reason = skip_reason or "missing_worker_id"
                elif get_worker_template(self.workspace_dir, resolved_worker_id) is None:
                    can_apply = False
                    skip_reason = skip_reason or "unknown_worker_id"
            elif suggested_mode == "octo_task":
                resolved_worker_id = None
            candidate = {
                "task_id": task_id,
                "name": task.get("name"),
                "execution_mode": task.get("execution_mode"),
                "suggested_execution_mode": suggested_mode,
                "worker_id": task.get("worker_id"),
                "resolved_worker_id": resolved_worker_id,
                "dispatch_policy_reason": task.get("dispatch_policy_reason"),
                "blocked_reason": blocked_reason,
                "can_apply": can_apply,
            }
            if skip_reason:
                candidate["skip_reason"] = skip_reason
            candidates.append(candidate)
            if not apply:
                continue
            if not can_apply:
                skipped_items.append(
                    {
                        "task_id": task_id,
                        "name": task.get("name"),
                        "reason": skip_reason or "cannot_apply",
                    }
                )
                continue

            metadata = dict(task.get("metadata") or {}) if isinstance(task.get("metadata"), dict) else {}
            metadata["notify_user"] = task.get("notify_user")
            metadata["execution_mode"] = suggested_mode
            metadata.pop(SCHEDULED_TASK_BLOCKED_UNTIL_KEY, None)
            metadata.pop(SCHEDULED_TASK_BLOCKED_REASON_KEY, None)
            metadata.pop(SCHEDULED_TASK_SUGGESTED_EXECUTION_MODE_KEY, None)

            repaired_worker_id = resolved_worker_id if suggested_mode == "worker" else None
            self.store.upsert_scheduled_task(
                task_id=task_id,
                name=str(task.get("name") or ""),
                frequency=str(task.get("frequency") or ""),
                task_text=str(task.get("task_text") or ""),
                description=str(task.get("description") or "").strip() or None,
                worker_id=repaired_worker_id,
                inputs=task.get("inputs") if isinstance(task.get("inputs"), dict) else None,
                enabled=bool(int(task.get("enabled", 1) or 0) == 1),
                metadata=metadata or None,
            )
            applied_items.append(
                {
                    "task_id": task_id,
                    "name": task.get("name"),
                    "execution_mode": suggested_mode,
                    "worker_id": repaired_worker_id,
                }
            )

        if apply and applied_items:
            self.sync_to_markdown()

        return {
            "status": "applied" if apply else "preview",
            "candidate_count": len(candidates),
            "applied_count": len(applied_items),
            "skipped_count": len(skipped_items),
            "candidates": candidates,
            "applied": applied_items,
            "skipped": skipped_items,
        }

    def get_actionable_tasks(self) -> list[dict[str, Any]]:
        """Find tasks that are due to run."""
        all_tasks = self.store.get_scheduled_tasks(enabled_only=True)
        now = utc_now()
        actionable = []

        for task in all_tasks:
            normalized = self._normalize_task_record(task)
            if not bool(normalized.get("dispatch_ready")):
                continue
            if self._should_run(normalized, now):
                actionable.append(normalized)

        return actionable

    def describe_tasks(self, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        """Return normalized tasks plus next-run preview and due-state hints."""
        tasks = self.store.get_scheduled_tasks(enabled_only=enabled_only)
        now = utc_now()
        described: list[dict[str, Any]] = []
        for task in tasks:
            normalized = self._normalize_task_record(task)
            preview = self._build_task_preview(normalized, now)
            normalized.update(preview)
            described.append(normalized)
        described.sort(
            key=lambda item: (
                0 if bool(item.get("due_now")) else 1,
                str(item.get("next_run_at") or "9999-99-99T99:99:99"),
                str(item.get("name") or ""),
            )
        )
        return described

    def mark_executed(self, task_id: str) -> None:
        self.store.update_task_last_run(task_id, utc_now())
        self.sync_to_markdown()

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        for task in self.store.get_scheduled_tasks():
            if str(task.get("id") or "") != task_id:
                continue
            return self._normalize_task_record(task)
        return None

    def sync_to_markdown(self) -> None:
        """Update HEARTBEAT.md to reflect the database state."""
        tasks = self.store.get_scheduled_tasks()

        lines = ["# HEARTBEAT - Scheduled Tasks\n", "## Tasks\n"]

        for t in tasks:
            lines.append(f"### {t['name']}")
            lines.append(f"- **ID**: {t['id']}")
            if t['description']:
                lines.append(f"- **Description**: {t['description']}")
            lines.append(f"- **Frequency**: {t['frequency']}")
            normalized = self._normalize_task_record(t)
            lines.append(f"- **Notify user**: {normalized['notify_user']}")
            if normalized.get("delivery_chat_id"):
                lines.append(f"- **Delivery chat**: {normalized['delivery_chat_id']}")
            lines.append(f"- **Execution mode**: {normalized['execution_mode']}")
            dispatch_line = "ready"
            if not bool(normalized.get("dispatch_ready")):
                dispatch_line = f"rejected by policy ({normalized.get('dispatch_policy_reason') or 'unknown'})"
            lines.append(f"- **Dispatch**: {dispatch_line}")
            if normalized.get("suggested_execution_mode"):
                lines.append(
                    f"- **Suggested execution mode**: {normalized['suggested_execution_mode']}"
                )
            if t['worker_id']:
                lines.append(f"- **Worker**: {t['worker_id']}")
            lines.append(f"- **Task**: {t['task_text']}")
            lines.append(f"- **Last execution**: {t['last_run_at'] or 'Never'}")
            lines.append(f"- **Status**: {'Enabled' if t['enabled'] else 'Disabled'}")
            lines.append("")

        lines.append("## Tracking")
        for t in tasks:
            lines.append(f"- {t['id']}_last_run: {t['last_run_at'] or 'Never'}")

        content = "\n".join(lines)
        try:
            self.heartbeat_md.write_text(content, encoding="utf-8")
        except Exception:
            logger.exception("Failed to sync HEARTBEAT.md")

    def _generate_id(self, name: str) -> str:
        return re.sub(r'[^a-z0-9_]', '', name.lower().replace(' ', '_'))

    def _should_run(self, task: dict[str, Any], now: datetime) -> bool:
        last_run_str = task.get("last_run_at")
        if not last_run_str:
            return True  # Never run before

        last_run = datetime.fromisoformat(last_run_str)
        freq = task["frequency"].lower()

        # Pattern: Every X minutes
        minute_match = _EVERY_MINUTES_RE.search(freq)
        if minute_match:
            minutes = int(minute_match.group(1))
            return now >= last_run + timedelta(minutes=minutes)

        # Pattern: Every X hours
        hour_match = _EVERY_HOURS_RE.search(freq)
        if hour_match:
            hours = int(hour_match.group(1))
            return now >= last_run + timedelta(hours=hours)

        # Pattern: Daily at HH:MM (UTC)
        daily_match = _DAILY_AT_RE.search(freq)
        if daily_match:
            target_h = int(daily_match.group(1))
            target_m = int(daily_match.group(2))

            # Check if we've already run today after the target time
            target_today = now.replace(hour=target_h, minute=target_m, second=0, microsecond=0)

            # If target time for today hasn't passed yet, we don't run
            if now < target_today:
                return False

            # If we haven't run today yet (last run was before today's target time)
            return last_run < target_today

        return False

    def _validate_and_normalize_frequency(self, frequency: str) -> str:
        freq = (frequency or "").strip()
        if not freq:
            raise ValueError("frequency is required.")

        minute_match = _EVERY_MINUTES_RE.fullmatch(freq)
        if minute_match:
            minutes = int(minute_match.group(1))
            if minutes < 1:
                raise ValueError("Every X minutes requires X >= 1.")
            return f"Every {minutes} minute{'s' if minutes != 1 else ''}"

        hour_match = _EVERY_HOURS_RE.fullmatch(freq)
        if hour_match:
            hours = int(hour_match.group(1))
            if hours < 1:
                raise ValueError("Every X hours requires X >= 1.")
            return f"Every {hours} hour{'s' if hours != 1 else ''}"

        daily_match = _DAILY_AT_RE.fullmatch(freq)
        if daily_match:
            hour = int(daily_match.group(1))
            minute = int(daily_match.group(2))
            if hour < 0 or hour > 23:
                raise ValueError("Daily at HH:MM requires HH between 00 and 23.")
            if minute < 0 or minute > 59:
                raise ValueError("Daily at HH:MM requires MM between 00 and 59.")
            return f"Daily at {hour:02d}:{minute:02d}"

        raise ValueError("Unsupported frequency. Use 'Every X minutes', 'Every X hours', or 'Daily at HH:MM' (UTC).")

    def _normalize_task_record(self, task: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(task)
        raw_inputs = normalized.get("inputs_json")
        raw_metadata = normalized.get("metadata_json")
        inputs: dict[str, Any] = {}
        metadata: dict[str, Any] = {}
        if isinstance(raw_inputs, str) and raw_inputs.strip():
            try:
                parsed = json.loads(raw_inputs)
                if isinstance(parsed, dict):
                    inputs = parsed
            except json.JSONDecodeError:
                logger.warning("Invalid scheduled task inputs_json", task_id=normalized.get("id"))
        if isinstance(raw_metadata, str) and raw_metadata.strip():
            try:
                parsed = json.loads(raw_metadata)
                if isinstance(parsed, dict):
                    metadata = parsed
            except json.JSONDecodeError:
                logger.warning("Invalid scheduled task metadata_json", task_id=normalized.get("id"))
        normalized["inputs"] = inputs
        normalized["metadata"] = metadata
        try:
            normalized["notify_user"] = normalize_notify_user_policy(metadata.get("notify_user"))
        except ValueError:
            logger.warning("Invalid scheduled task notify_user policy", task_id=normalized.get("id"))
            normalized["notify_user"] = "if_significant"
        try:
            normalized["execution_mode"] = normalize_execution_mode(
                metadata.get("execution_mode"),
                worker_id=normalized.get("worker_id"),
            )
        except ValueError:
            logger.warning("Invalid scheduled task execution_mode", task_id=normalized.get("id"))
            normalized["execution_mode"] = normalize_execution_mode(
                None,
                worker_id=normalized.get("worker_id"),
            )
        if normalized["execution_mode"] == "octo_control" and normalized["notify_user"] == "if_significant":
            normalized["notify_user"] = "never"
        normalized["delivery_chat_id"] = (
            str(
                metadata.get(SCHEDULED_TASK_DELIVERY_CHAT_ID_KEY)
                or metadata.get(SCHEDULED_TASK_TARGET_CHAT_ID_KEY)
                or ""
            ).strip()
            or None
        )
        blocked_until = parse_scheduled_task_blocked_until(metadata)
        blocked_reason = str(metadata.get(SCHEDULED_TASK_BLOCKED_REASON_KEY) or "").strip() or None
        suggested_execution_mode = parse_scheduled_task_suggested_execution_mode(metadata)
        if (
            normalized["execution_mode"] == "octo_control"
            and blocked_reason == "blocked_by_route"
            and not str(normalized.get("worker_id") or "").strip()
        ):
            suggested_execution_mode = "octo_task"
        normalized["blocked_until"] = blocked_until.isoformat() if blocked_until is not None else None
        normalized["blocked_reason"] = blocked_reason
        normalized["suggested_execution_mode"] = suggested_execution_mode
        dispatch_ready, dispatch_policy_reason = self._dispatch_readiness(normalized)
        normalized["dispatch_ready"] = dispatch_ready
        normalized["dispatch_policy_reason"] = dispatch_policy_reason
        return normalized

    def _dispatch_readiness(self, task: dict[str, Any]) -> tuple[bool, str | None]:
        execution_mode = str(task.get("execution_mode") or "").strip().lower()
        suggested_execution_mode = str(task.get("suggested_execution_mode") or "").strip().lower()
        if execution_mode == "octo_control" and suggested_execution_mode in {
            "worker",
            "octo_task",
        }:
            return False, str(task.get("blocked_reason") or "").strip() or "blocked_by_route"
        blocked_until_value = str(task.get("blocked_until") or "").strip()
        if blocked_until_value:
            try:
                blocked_until = datetime.fromisoformat(blocked_until_value)
            except ValueError:
                blocked_until = None
            if blocked_until is not None and blocked_until.tzinfo is not None and blocked_until > utc_now():
                return False, str(task.get("blocked_reason") or "").strip() or "blocked_by_route_backoff"
        if execution_mode in {"octo_control", "octo_task"}:
            task_text = str(task.get("task_text") or "").strip()
            if not task_text:
                return False, "missing_task_text"
            return True, None
        worker_id = str(task.get("worker_id") or "").strip()
        if not worker_id:
            return False, "missing_worker_id"
        task_text = str(task.get("task_text") or "").strip()
        if not task_text:
            return False, "missing_task_text"
        return True, None

    def _build_task_preview(self, task: dict[str, Any], now: datetime) -> dict[str, Any]:
        due_now = bool(
            int(task.get("enabled", 1)) == 1
            and bool(task.get("dispatch_ready", True))
            and self._should_run(task, now)
        )
        next_run_at = self._estimate_next_run(task, now)
        last_run_at = task.get("last_run_at")
        overdue = False
        if due_now and last_run_at:
            try:
                overdue = next_run_at is not None and next_run_at <= now
            except TypeError:
                overdue = True
        return {
            "due_now": due_now,
            "overdue": overdue,
            "next_run_at": next_run_at.isoformat() if next_run_at else None,
        }

    def _estimate_next_run(self, task: dict[str, Any], now: datetime) -> datetime | None:
        freq = str(task.get("frequency", "") or "").lower()
        last_run_str = task.get("last_run_at")
        last_run = None
        if isinstance(last_run_str, str) and last_run_str.strip():
            try:
                last_run = datetime.fromisoformat(last_run_str)
            except ValueError:
                last_run = None

        minute_match = _EVERY_MINUTES_RE.search(freq)
        if minute_match:
            minutes = int(minute_match.group(1))
            if last_run is None:
                return now
            return last_run + timedelta(minutes=minutes)

        hour_match = _EVERY_HOURS_RE.search(freq)
        if hour_match:
            hours = int(hour_match.group(1))
            if last_run is None:
                return now
            return last_run + timedelta(hours=hours)

        daily_match = _DAILY_AT_RE.search(freq)
        if daily_match:
            target_h = int(daily_match.group(1))
            target_m = int(daily_match.group(2))
            target_today = now.replace(hour=target_h, minute=target_m, second=0, microsecond=0)
            if last_run is None:
                return target_today if now <= target_today else target_today + timedelta(days=1)
            if now < target_today:
                return target_today
            if last_run < target_today:
                return target_today
            return target_today + timedelta(days=1)

        return None
