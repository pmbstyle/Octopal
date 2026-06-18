from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog

from octopal.infrastructure.store.models import AuditEvent
from octopal.runtime.memory.memchain import memchain_record
from octopal.runtime.octo.context_health import _coerce_float, _coerce_int
from octopal.runtime.octo.context_reset import (
    persist_context_reset_files as _persist_context_reset_files,
)
from octopal.runtime.octo.worker_records import _normalize_string_list
from octopal.runtime.self_control import (
    SELF_RESTART_ACTION,
    SELF_RESTART_REQUESTED_BY,
    SELF_UPDATE_ACTION,
    SELF_UPDATE_REQUESTED_BY,
)
from octopal.runtime.self_control import (
    append_control_ack as _default_append_control_ack,
)
from octopal.runtime.self_control import (
    append_control_request as _default_append_control_request,
)
from octopal.runtime.self_control import (
    check_update_status as _default_check_update_status,
)
from octopal.runtime.self_control import (
    due_self_restart_requests as _default_due_self_restart_requests,
)
from octopal.runtime.self_control import (
    due_self_update_requests as _default_due_self_update_requests,
)
from octopal.runtime.self_control import (
    find_recent_control_action as _default_find_recent_control_action,
)
from octopal.runtime.self_control import (
    launch_restart_helper as _default_launch_restart_helper,
)
from octopal.runtime.self_control import (
    launch_update_helper as _default_launch_update_helper,
)
from octopal.runtime.self_control import (
    write_pending_restart_resume as _default_write_pending_restart_resume,
)
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)


def _core_callable(name: str, default: Callable[..., Any]) -> Callable[..., Any]:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is not None:
        candidate = getattr(core_module, name, None)
        if callable(candidate):
            return candidate
    return default


class OctoSelfLifecycleMixin:
    async def _periodic_self_control_requests(self, interval_seconds: int = 1) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await self._run_self_control_requests_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Self-control request executor failed")

    async def _run_self_control_requests_once(self) -> None:
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        if runtime_settings is None:
            return
        state_dir = Path(runtime_settings.state_dir)
        due_self_restart_requests = _core_callable(
            "due_self_restart_requests", _default_due_self_restart_requests
        )
        due_self_update_requests = _core_callable(
            "due_self_update_requests", _default_due_self_update_requests
        )
        append_control_ack = _core_callable("append_control_ack", _default_append_control_ack)
        launch_restart_helper = _core_callable(
            "launch_restart_helper", _default_launch_restart_helper
        )
        launch_update_helper = _core_callable("launch_update_helper", _default_launch_update_helper)

        for request in await asyncio.to_thread(due_self_restart_requests, state_dir):
            request_id = str(request.get("request_id", "") or "").strip()
            if not request_id:
                continue
            append_control_ack(
                state_dir,
                request_id,
                status="accepted",
                source="octo_self_control",
                message="Self-restart request accepted; launching restart helper.",
            )
            try:
                launch_restart_helper(
                    state_dir,
                    request_id=request_id,
                    project_root=Path(__file__).resolve().parents[4],
                    delay_seconds=1,
                )
            except Exception as exc:
                append_control_ack(
                    state_dir,
                    request_id,
                    status="error",
                    source="octo_self_control",
                    message=f"Failed to launch restart helper: {exc}",
                )
                logger.exception(
                    "Failed to launch self-restart helper",
                    request_id=request_id,
                )
        for request in await asyncio.to_thread(due_self_update_requests, state_dir):
            request_id = str(request.get("request_id", "") or "").strip()
            if not request_id:
                continue
            append_control_ack(
                state_dir,
                request_id,
                status="accepted",
                source="octo_self_control",
                message="Self-update request accepted; launching update helper.",
            )
            try:
                launch_update_helper(
                    state_dir,
                    request_id=request_id,
                    project_root=Path(__file__).resolve().parents[4],
                    delay_seconds=1,
                )
            except Exception as exc:
                append_control_ack(
                    state_dir,
                    request_id,
                    status="error",
                    source="octo_self_control",
                    message=f"Failed to launch update helper: {exc}",
                )
                logger.exception(
                    "Failed to launch self-update helper",
                    request_id=request_id,
                )

    async def request_update_check(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        project_root = Path(__file__).resolve().parents[4]
        if runtime_settings is None:
            return {"status": "error", "message": "runtime settings are unavailable"}
        check_update_status = _core_callable("check_update_status", _default_check_update_status)
        update_status = await asyncio.to_thread(check_update_status, project_root)
        await asyncio.to_thread(
            self.store.append_audit,
            AuditEvent(
                id=str(uuid4()),
                ts=utc_now(),
                level="info",
                event_type="octo.update_check",
                data={"chat_id": chat_id, "update": update_status},
            ),
        )
        return update_status

    async def request_self_restart(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        if runtime_settings is None:
            return {"status": "error", "message": "runtime settings are unavailable"}
        reason = str(args.get("reason", "") or "").strip()
        if not reason:
            return {"status": "error", "message": "reason is required"}
        if not bool(args.get("confirm", False)):
            return {
                "status": "needs_confirmation",
                "action": "octo_restart_self",
                "message": "Self restart requires confirm=true.",
            }

        state_dir = Path(runtime_settings.state_dir)
        if not bool(args.get("force", False)):
            find_recent_control_action = _core_callable(
                "find_recent_control_action", _default_find_recent_control_action
            )
            duplicate = await asyncio.to_thread(
                find_recent_control_action,
                state_dir,
                action=SELF_RESTART_ACTION,
                requested_by=SELF_RESTART_REQUESTED_BY,
                chat_id=chat_id,
            )
            if duplicate is not None:
                return {
                    "status": "duplicate_recent_control_action",
                    "action": "octo_restart_self",
                    "message": (
                        "A recent self restart for this chat is already pending or completed. "
                        "Use force=true only when the user explicitly asks for another restart."
                    ),
                    "duplicate": duplicate,
                }

        confidence = _coerce_float(args.get("confidence"), default=0.8)
        delay_seconds = _coerce_int(args.get("delay_seconds"), default=5, minimum=3, maximum=60)
        health = await self.get_context_health_snapshot(chat_id)
        handoff = {
            "chat_id": chat_id,
            "created_at": utc_now().isoformat(),
            "mode": "self_restart",
            "source": "octo_restart_self",
            "reason": reason,
            "confidence": confidence,
            "goal_now": str(args.get("goal_now", "") or "").strip(),
            "done": _normalize_string_list(args.get("done")),
            "open_threads": _normalize_string_list(args.get("open_threads")),
            "critical_constraints": _normalize_string_list(args.get("critical_constraints")),
            "next_step": str(args.get("next_step", "") or "").strip(),
            "current_interest": str(args.get("current_interest", "") or "").strip(),
            "pending_human_input": str(args.get("pending_human_input", "") or "").strip(),
            "cognitive_state": str(args.get("cognitive_state", "") or "focused").strip().lower(),
            "health_snapshot": health,
        }
        if not handoff["goal_now"]:
            handoff["goal_now"] = "Resume the current user task after Octo restarts."
        if not handoff["next_step"]:
            handoff["next_step"] = "Read the restart handoff and continue or clarify."

        workspace_dir = Path(
            getattr(
                runtime_settings,
                "workspace_dir",
                os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace"),
            )
        ).resolve()
        file_info = await asyncio.to_thread(_persist_context_reset_files, workspace_dir, handoff)
        append_control_request = _core_callable(
            "append_control_request", _default_append_control_request
        )
        request = await asyncio.to_thread(
            append_control_request,
            state_dir,
            action=SELF_RESTART_ACTION,
            reason=reason,
            requested_by=SELF_RESTART_REQUESTED_BY,
            delay_seconds=delay_seconds,
            metadata={"chat_id": chat_id, "handoff_file": file_info.get("handoff_md", "")},
        )
        resume_payload = {
            "status": "pending",
            "request_id": request["request_id"],
            "created_at": utc_now().isoformat(),
            "handoff": handoff,
            "files": file_info,
        }
        write_pending_restart_resume = _core_callable(
            "write_pending_restart_resume", _default_write_pending_restart_resume
        )
        await asyncio.to_thread(write_pending_restart_resume, state_dir, resume_payload)

        try:
            memchain_info = await asyncio.to_thread(
                memchain_record,
                workspace_dir,
                reason="self_restart",
                meta={
                    "chat_id": chat_id,
                    "source": "octo_restart_self",
                    "request_id": request["request_id"],
                },
            )
        except Exception as exc:
            memchain_info = {"status": "error", "message": str(exc)}
            logger.warning("Memchain record failed during self restart request", error=str(exc))

        await asyncio.to_thread(
            self.store.append_audit,
            AuditEvent(
                id=str(uuid4()),
                ts=utc_now(),
                level="info",
                event_type="octo.self_restart_requested",
                data={
                    "chat_id": chat_id,
                    "reason": reason,
                    "request": request,
                    "files": file_info,
                    "memchain": memchain_info,
                },
            ),
        )
        return {
            "status": "restart_requested",
            "request": request,
            "handoff": handoff,
            "files": file_info,
            "memchain": memchain_info,
            "message": "Self restart requested. Handoff is durable and the restart helper will run shortly.",
        }

    async def request_self_update(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        runtime_settings = getattr(getattr(self, "runtime", None), "settings", None)
        if runtime_settings is None:
            return {"status": "error", "message": "runtime settings are unavailable"}
        reason = str(args.get("reason", "") or "").strip()
        if not reason:
            return {"status": "error", "message": "reason is required"}
        if not bool(args.get("confirm", False)):
            return {
                "status": "needs_confirmation",
                "action": "octo_update_self",
                "message": "Self update requires confirm=true.",
            }

        state_dir = Path(runtime_settings.state_dir)
        if not bool(args.get("force", False)):
            find_recent_control_action = _core_callable(
                "find_recent_control_action", _default_find_recent_control_action
            )
            duplicate = await asyncio.to_thread(
                find_recent_control_action,
                state_dir,
                action=SELF_UPDATE_ACTION,
                requested_by=SELF_UPDATE_REQUESTED_BY,
                chat_id=chat_id,
            )
            if duplicate is not None:
                return {
                    "status": "duplicate_recent_control_action",
                    "action": "octo_update_self",
                    "message": (
                        "A recent self update for this chat is already pending or completed. "
                        "Use force=true only when the user explicitly asks for another update."
                    ),
                    "duplicate": duplicate,
                }

        project_root = Path(__file__).resolve().parents[4]
        check_update_status = _core_callable("check_update_status", _default_check_update_status)
        update_status = await asyncio.to_thread(check_update_status, project_root)
        if not bool(update_status.get("can_update")):
            return {
                "status": "blocked",
                "message": "Update is blocked by the current checkout state.",
                "update": update_status,
            }

        confidence = _coerce_float(args.get("confidence"), default=0.8)
        delay_seconds = _coerce_int(args.get("delay_seconds"), default=5, minimum=3, maximum=60)
        health = await self.get_context_health_snapshot(chat_id)
        handoff = {
            "chat_id": chat_id,
            "created_at": utc_now().isoformat(),
            "mode": "self_update",
            "source": "octo_update_self",
            "reason": reason,
            "confidence": confidence,
            "goal_now": str(args.get("goal_now", "") or "").strip(),
            "done": _normalize_string_list(args.get("done")),
            "open_threads": _normalize_string_list(args.get("open_threads")),
            "critical_constraints": _normalize_string_list(args.get("critical_constraints")),
            "next_step": str(args.get("next_step", "") or "").strip(),
            "current_interest": str(args.get("current_interest", "") or "").strip(),
            "pending_human_input": str(args.get("pending_human_input", "") or "").strip(),
            "cognitive_state": str(args.get("cognitive_state", "") or "focused").strip().lower(),
            "health_snapshot": health,
            "update_status": update_status,
        }
        if not handoff["goal_now"]:
            handoff["goal_now"] = "Resume the current user task after Octo updates and restarts."
        if not handoff["next_step"]:
            handoff["next_step"] = (
                "Read the update handoff and report whether update and restart completed."
            )

        workspace_dir = Path(
            getattr(
                runtime_settings,
                "workspace_dir",
                os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace"),
            )
        ).resolve()
        file_info = await asyncio.to_thread(_persist_context_reset_files, workspace_dir, handoff)
        append_control_request = _core_callable(
            "append_control_request", _default_append_control_request
        )
        request = await asyncio.to_thread(
            append_control_request,
            state_dir,
            action=SELF_UPDATE_ACTION,
            reason=reason,
            requested_by=SELF_UPDATE_REQUESTED_BY,
            delay_seconds=delay_seconds,
            metadata={
                "chat_id": chat_id,
                "handoff_file": file_info.get("handoff_md", ""),
                "update": update_status,
            },
        )
        resume_payload = {
            "status": "pending",
            "request_id": request["request_id"],
            "created_at": utc_now().isoformat(),
            "handoff": handoff,
            "files": file_info,
            "update": update_status,
        }
        write_pending_restart_resume = _core_callable(
            "write_pending_restart_resume", _default_write_pending_restart_resume
        )
        await asyncio.to_thread(write_pending_restart_resume, state_dir, resume_payload)

        try:
            memchain_info = await asyncio.to_thread(
                memchain_record,
                workspace_dir,
                reason="self_update",
                meta={
                    "chat_id": chat_id,
                    "source": "octo_update_self",
                    "request_id": request["request_id"],
                    "local_version": update_status.get("local_version"),
                    "latest_version": update_status.get("latest_version"),
                },
            )
        except Exception as exc:
            memchain_info = {"status": "error", "message": str(exc)}
            logger.warning("Memchain record failed during self update request", error=str(exc))

        await asyncio.to_thread(
            self.store.append_audit,
            AuditEvent(
                id=str(uuid4()),
                ts=utc_now(),
                level="info",
                event_type="octo.self_update_requested",
                data={
                    "chat_id": chat_id,
                    "reason": reason,
                    "request": request,
                    "files": file_info,
                    "update": update_status,
                    "memchain": memchain_info,
                },
            ),
        )
        return {
            "status": "update_requested",
            "request": request,
            "handoff": handoff,
            "files": file_info,
            "update": update_status,
            "memchain": memchain_info,
            "message": "Self update requested. Handoff is durable and the update helper will run shortly.",
        }
