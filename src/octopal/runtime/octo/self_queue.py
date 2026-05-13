from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import structlog

from octopal.runtime.octo.workspace_paths import _workspace_dir as _default_workspace_dir
from octopal.runtime.scheduler.service import SchedulerService
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)


def _build_opportunity_card(
    *,
    kind: str,
    title: str,
    why_now: str,
    impact: str,
    effort: str,
    confidence: float,
    next_action: str,
    risk: str = "low",
    suggested_worker_id: str | None = None,
    task: str | None = None,
    dedupe_key: str | None = None,
    inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    card = {
        "opportunity_id": str(uuid4()),
        "kind": kind,
        "title": title,
        "why_now": why_now,
        "impact": impact,
        "effort": effort,
        "confidence": round(max(0.0, min(confidence, 1.0)), 2),
        "risk": risk if risk in {"low", "medium", "high"} else "low",
        "next_action": next_action,
        "created_at": utc_now().isoformat(),
    }
    if suggested_worker_id:
        card["suggested_worker_id"] = suggested_worker_id
    if task:
        card["task"] = task
    if dedupe_key:
        card["dedupe_key"] = dedupe_key
    if inputs:
        card["inputs"] = inputs
    return card


def _active_self_queue_dedupe_keys(queue: list[dict[str, Any]]) -> set[str]:
    active_statuses = {"pending", "claimed", "running"}
    return {
        str(item.get("dedupe_key") or "").strip()
        for item in queue
        if str(item.get("dedupe_key") or "").strip()
        and str(item.get("status", "pending") or "pending").strip().lower() in active_statuses
    }


def _scheduler_opportunity_cards(
    scheduler: SchedulerService | None,
    queue: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if scheduler is None:
        return []
    try:
        tasks = scheduler.describe_tasks(enabled_only=True)
    except Exception:
        logger.debug("Unable to scan scheduler opportunities", exc_info=True)
        return []

    active_dedupe_keys = _active_self_queue_dedupe_keys(queue)
    cards: list[dict[str, Any]] = []
    for scheduled_task in tasks:
        task_id = str(scheduled_task.get("id") or "").strip()
        if not task_id:
            continue
        suggested_mode = str(scheduled_task.get("suggested_execution_mode") or "").strip().lower()
        if suggested_mode != "worker":
            continue
        dedupe_key = f"scheduled-task:{task_id}:suggested-worker"
        if dedupe_key in active_dedupe_keys:
            continue

        name = str(scheduled_task.get("name") or task_id).strip()
        worker_id = str(scheduled_task.get("worker_id") or "").strip() or "ops_sre"
        blocked_reason = str(
            scheduled_task.get("blocked_reason")
            or scheduled_task.get("dispatch_policy_reason")
            or "suggested_execution_mode=worker"
        ).strip()
        cards.append(
            _build_opportunity_card(
                kind="scheduled_task_repair",
                title=f"Unblock scheduled task: {name}",
                why_now=f"task_id={task_id}, reason={blocked_reason}",
                impact="high",
                effort="medium",
                confidence=0.88,
                risk="medium",
                suggested_worker_id=worker_id,
                task=(
                    f"Inspect scheduled task {task_id!r} ({name!r}) blocked from its current route. "
                    "Find the least-risk repair or migration path, verify whether worker execution is appropriate, "
                    "and report the exact recommended change."
                ),
                dedupe_key=dedupe_key,
                inputs={
                    "scheduled_task_id": task_id,
                    "blocked_reason": blocked_reason,
                    "suggested_execution_mode": suggested_mode,
                },
                next_action="Queue a diagnostic repair item with the suggested worker and dedupe key.",
            )
        )
    return cards


def _self_queue_opportunity_cards(queue: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active_dedupe_keys = _active_self_queue_dedupe_keys(queue)
    cards: list[dict[str, Any]] = []
    stale_before = utc_now() - timedelta(hours=6)
    for item in queue:
        status = str(item.get("status", "pending") or "pending").strip().lower()
        if status != "claimed":
            continue
        updated_at = _parse_iso_datetime(item.get("updated_at"))
        if updated_at is None or updated_at > stale_before:
            continue
        task_id = str(item.get("task_id") or "").strip()
        if not task_id:
            continue
        dedupe_key = f"self-queue:{task_id}:stale-claimed"
        if dedupe_key in active_dedupe_keys:
            continue
        title = str(item.get("title") or task_id).strip()
        cards.append(
            _build_opportunity_card(
                kind="self_queue_recovery",
                title=f"Recover stale self-queue item: {title}",
                why_now=f"task_id={task_id}, status=claimed, updated_at={updated_at.isoformat()}",
                impact="medium",
                effort="low",
                confidence=0.83,
                risk="low",
                task=(
                    f"Review stale claimed self-queue item {task_id!r}; either execute it if still valid, "
                    "or mark it blocked/cancelled with a concise reason."
                ),
                dedupe_key=dedupe_key,
                inputs={"self_queue_task_id": task_id, "stale_status": status},
                next_action="Use octo_self_queue_update or execute_self_queue_item to resolve the stale claim.",
            )
        )
    return cards


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def _workspace_dir() -> Path:
    core_module = sys.modules.get("octopal.runtime.octo.core")
    if core_module is not None:
        resolver = getattr(core_module, "_workspace_dir", None)
        if callable(resolver):
            return Path(resolver())
    return _default_workspace_dir()


class OctoSelfQueueMixin:
    async def get_self_queue(self, chat_id: int) -> list[dict[str, Any]]:
        await self._ensure_self_queue_loaded(chat_id)
        queue = list((self._self_queue_by_chat or {}).get(chat_id, []))
        return [dict(item) for item in queue]

    async def _ensure_self_queue_loaded(self, chat_id: int) -> None:
        if chat_id in self._self_queue_by_chat:
            return
        loaded = await asyncio.to_thread(_load_self_queue, _workspace_dir(), chat_id)
        self._self_queue_by_chat[chat_id] = loaded

    async def add_self_queue_item(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        title = str((args or {}).get("title", "") or "").strip()
        task = str((args or {}).get("task", "") or "").strip()
        if not title or not task:
            return {"status": "error", "message": "title and task are required"}
        priority = max(1, min(5, int((args or {}).get("priority", 3) or 3)))
        source = str((args or {}).get("source", "octo") or "octo").strip()[:64]
        dedupe_key = str((args or {}).get("dedupe_key", "") or "").strip()[:160]
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        if dedupe_key:
            for existing in queue:
                if str(existing.get("dedupe_key", "") or "") != dedupe_key:
                    continue
                if str(existing.get("status", "pending")) in {"pending", "claimed", "running"}:
                    return {
                        "status": "duplicate",
                        "item": dict(existing),
                        "queue_size": len(queue),
                    }
        inputs = (args or {}).get("inputs")
        if not isinstance(inputs, dict):
            inputs = {}
        worker_id = str((args or {}).get("worker_id", "") or "").strip()
        risk = str((args or {}).get("risk", "low") or "low").strip().lower()
        if risk not in {"low", "medium", "high"}:
            risk = "low"
        item = {
            "task_id": str(uuid4()),
            "title": title,
            "task": task,
            "priority": priority,
            "source": source,
            "status": "pending",
            "created_at": utc_now().isoformat(),
            "updated_at": utc_now().isoformat(),
            "notes": str((args or {}).get("notes", "") or "").strip(),
            "risk": risk,
            "inputs": inputs,
        }
        if worker_id:
            item["worker_id"] = worker_id
        if dedupe_key:
            item["dedupe_key"] = dedupe_key
        queue.append(item)
        queue.sort(key=lambda i: (-int(i.get("priority", 3)), str(i.get("created_at", ""))))
        await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
        return {"status": "ok", "item": item, "queue_size": len(queue)}

    async def take_next_self_queue_item(self, chat_id: int) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        for item in queue:
            if str(item.get("status", "pending")) == "pending":
                item["status"] = "claimed"
                item["updated_at"] = utc_now().isoformat()
                await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
                return {"status": "ok", "item": dict(item)}
        return {"status": "empty", "message": "no pending self-queue items"}

    async def update_self_queue_item(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        task_id = str((args or {}).get("task_id", "") or "").strip()
        new_status = str((args or {}).get("status", "") or "").strip().lower()
        valid_statuses = {"pending", "claimed", "running", "blocked", "done", "cancelled"}
        if not task_id or new_status not in valid_statuses:
            return {"status": "error", "message": "task_id and valid status are required"}
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        for item in queue:
            if str(item.get("task_id", "")) == task_id:
                item["status"] = new_status
                if "notes" in (args or {}):
                    item["notes"] = str((args or {}).get("notes", "") or "").strip()
                item["updated_at"] = utc_now().isoformat()
                await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
                return {"status": "ok", "item": dict(item)}
        return {"status": "not_found", "task_id": task_id}

    async def execute_self_queue_item(self, chat_id: int, args: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_self_queue_loaded(chat_id)
        task_id = str((args or {}).get("task_id", "") or "").strip()
        dry_run = bool((args or {}).get("dry_run", False))
        queue = self._self_queue_by_chat.setdefault(chat_id, [])

        item: dict[str, Any] | None = None
        for candidate in queue:
            if task_id and str(candidate.get("task_id", "") or "") != task_id:
                continue
            if task_id or str(candidate.get("status", "pending")) in {"pending", "claimed"}:
                item = candidate
                break
        if item is None:
            return {"status": "empty" if not task_id else "not_found", "task_id": task_id or None}

        item_status = str(item.get("status", "pending") or "pending").strip().lower()
        if item_status not in {"pending", "claimed"}:
            return {
                "status": "blocked",
                "reason": f"self-queue item is not executable from status={item_status}",
                "item": dict(item),
            }

        worker_id = str(item.get("worker_id", "") or "").strip()
        risk = str(item.get("risk", "low") or "low").strip().lower()
        if dry_run:
            payload = {
                "status": "dry_run",
                "task_id": item.get("task_id"),
                "worker_id": worker_id or None,
                "risk": risk,
                "task": item.get("task"),
                "item": dict(item),
            }
            if not worker_id:
                payload["would_block_reason"] = "missing_worker_id"
            elif risk == "high":
                payload["would_block_reason"] = "high_risk_requires_user_input"
            return payload

        if not worker_id:
            item["status"] = "blocked"
            item["blocked_reason"] = "missing_worker_id"
            item["updated_at"] = utc_now().isoformat()
            await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
            return {
                "status": "blocked",
                "reason": "missing_worker_id",
                "item": dict(item),
            }

        if risk == "high":
            item["status"] = "blocked"
            item["blocked_reason"] = "high_risk_requires_user_input"
            item["updated_at"] = utc_now().isoformat()
            await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
            return {
                "status": "blocked",
                "reason": "high_risk_requires_user_input",
                "item": dict(item),
            }

        task_text = str(item.get("task", "") or "").strip()
        inputs = item.get("inputs")
        if not isinstance(inputs, dict):
            inputs = {}
        item["status"] = "claimed"
        item["updated_at"] = utc_now().isoformat()
        await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)

        try:
            launch = await self._start_worker_async(
                worker_id=worker_id,
                task=task_text,
                chat_id=chat_id,
                inputs=inputs,
                tools=None,
                model=None,
                timeout_seconds=None,
                scheduled_task_id=None,
            )
        except Exception as exc:
            item["status"] = "blocked"
            item["blocked_reason"] = str(exc)
            item["updated_at"] = utc_now().isoformat()
            await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
            return {
                "status": "error",
                "reason": "worker_launch_failed",
                "error": str(exc),
                "item": dict(item),
            }

        launch_status = str(launch.get("status", "") or "").strip().lower()
        if launch_status == "started":
            run_id = str(launch.get("run_id") or launch.get("worker_id") or "").strip()
            item["status"] = "running"
            item["run_id"] = run_id
            item["started_at"] = utc_now().isoformat()
            item["updated_at"] = utc_now().isoformat()
            await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
            return {
                "status": "started",
                "task_id": item.get("task_id"),
                "worker_template_id": worker_id,
                "worker_id": run_id,
                "run_id": run_id,
                "followup_required": True,
                "next_best_action": "wait_for_worker_progress",
                "item": dict(item),
            }

        item["status"] = "blocked"
        item["blocked_reason"] = launch_status or "worker_not_started"
        item["updated_at"] = utc_now().isoformat()
        await asyncio.to_thread(_persist_self_queue, _workspace_dir(), chat_id, queue)
        return {
            "status": "blocked",
            "reason": item["blocked_reason"],
            "launch": launch,
            "item": dict(item),
        }

    async def scan_opportunities(self, chat_id: int, limit: int = 3) -> dict[str, Any]:
        health = await self.get_context_health_snapshot(chat_id)
        await self._ensure_self_queue_loaded(chat_id)
        queue = self._self_queue_by_chat.setdefault(chat_id, [])
        opportunities: list[dict[str, Any]] = []

        opportunities.extend(_scheduler_opportunity_cards(self.scheduler, queue))
        opportunities.extend(_self_queue_opportunity_cards(queue))

        context_size = int(health.get("context_size_estimate", 0) or 0)
        repetition = float(health.get("repetition_score", 0.0) or 0.0)
        no_progress = int(health.get("no_progress_turns", 0) or 0)
        resets_since_progress = int(health.get("resets_since_progress", 0) or 0)
        context_health = str(health.get("context_health", "OK") or "OK")

        if context_health == "RESET_SOON":
            opportunities.append(
                _build_opportunity_card(
                    kind="stability",
                    title="Context compaction before quality drops",
                    why_now=f"context_health={context_health}, size={context_size}",
                    impact="high",
                    effort="low",
                    confidence=0.93,
                    next_action="Call octo_context_reset(mode='soft') with concise handoff.",
                )
            )
        if no_progress >= 3 or repetition >= 0.70:
            opportunities.append(
                _build_opportunity_card(
                    kind="momentum",
                    title="Break stagnation with a replan cycle",
                    why_now=(
                        f"no_progress_turns={no_progress}, "
                        f"repetition_score={round(repetition, 3)}"
                    ),
                    impact="high",
                    effort="medium",
                    confidence=0.82,
                    next_action="Create 1 focused task in self-queue and execute immediately.",
                )
            )
        if resets_since_progress >= 1:
            opportunities.append(
                _build_opportunity_card(
                    kind="recovery",
                    title="Post-reset recovery check",
                    why_now=f"resets_since_progress={resets_since_progress}",
                    impact="medium",
                    effort="low",
                    confidence=0.78,
                    next_action="Audit open threads and lock one measurable next step.",
                )
            )
        if not opportunities:
            opportunities.append(
                _build_opportunity_card(
                    kind="improvement",
                    title="Proactive improvement pass",
                    why_now="system is stable; use spare cycle for compounding gains",
                    impact="medium",
                    effort="medium",
                    confidence=0.74,
                    next_action="Pick one automation or cleanup task and add it to self-queue.",
                )
            )

        opportunities = opportunities[: max(1, min(limit, 5))]
        self._last_opportunities_by_chat[chat_id] = [dict(item) for item in opportunities]
        await asyncio.to_thread(
            _persist_last_opportunities, _workspace_dir(), chat_id, opportunities
        )
        pending_count = sum(1 for item in queue if str(item.get("status", "pending")) == "pending")
        return {
            "status": "ok",
            "chat_id": chat_id,
            "opportunities": opportunities,
            "queue_pending": pending_count,
            "generated_at": utc_now().isoformat(),
        }


def _persist_self_queue(workspace_dir: Path, chat_id: int, queue: list[dict[str, Any]]) -> str:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    path = memory_dir / f"self-queue-{chat_id}.json"
    path.write_text(json.dumps(queue, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _load_self_queue(workspace_dir: Path, chat_id: int) -> list[dict[str, Any]]:
    path = (workspace_dir / "memory" / f"self-queue-{chat_id}.json").resolve()
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    items: list[dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict) and str(item.get("task_id", "")).strip():
            items.append(dict(item))
    items.sort(key=lambda i: (-int(i.get("priority", 3) or 3), str(i.get("created_at", ""))))
    return items


def _persist_last_opportunities(
    workspace_dir: Path, chat_id: int, opportunities: list[dict[str, Any]]
) -> str:
    memory_dir = workspace_dir / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    path = memory_dir / f"opportunities-{chat_id}.json"
    path.write_text(json.dumps(opportunities, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)
