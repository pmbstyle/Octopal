from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.runtime.memory.service import MemoryService

logger = structlog.get_logger(__name__)

_MAX_PLAN_STEPS = 10


async def _build_plan(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    has_tools: bool,
    *,
    complete_text_fn: Callable[
        [InferenceProvider, list[Message | dict[str, Any]]],
        Awaitable[str],
    ],
) -> dict[str, Any] | None:
    planning_prompt = (
        "Create a brief execution plan for this turn. Return JSON only with keys: "
        '{"mode":"execute|reply","steps":["..."],"response":"..."}.\n'
        "- Use mode=reply when no tools/workers are needed and a direct answer is sufficient.\n"
        "- Use mode=execute when tools/workers are needed; provide 1-8 concrete steps.\n"
        "- If mode=reply, include response.\n"
        "- If mode=execute, response is optional."
    )
    planner_messages = list(messages) + [Message(role="system", content=planning_prompt)]
    try:
        raw = await complete_text_fn(provider, planner_messages)
    except Exception:
        logger.debug("Planner step skipped due to provider error", exc_info=True)
        return None

    payload = _extract_json_object(raw)
    if not isinstance(payload, dict):
        return None
    return _normalize_plan_payload(payload, has_tools)


async def _persist_plan(memory: MemoryService, chat_id: int, plan: dict[str, Any]) -> None:
    mode = str(plan.get("mode", "execute"))
    steps = [str(step) for step in plan.get("steps", []) if str(step).strip()]
    response = str(plan.get("response", "")).strip()
    plan_summary = f"Planner mode={mode}; steps={len(steps)}" + (
        f"; response_len={len(response)}" if response else ""
    )
    try:
        await memory.add_message(
            "system",
            plan_summary,
            {
                "chat_id": chat_id,
                "planner": True,
                "mode": mode,
                "steps": steps,
            },
        )
    except Exception:
        logger.debug("Failed to persist planner trace", exc_info=True)


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    if not raw:
        return None
    candidates = [raw.strip()]
    stripped = raw.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            candidates.append("\n".join(lines[1:-1]).strip())
    for match in re.finditer(
        r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.IGNORECASE | re.DOTALL
    ):
        candidates.append(match.group(1).strip())
    candidates.extend(_iter_balanced_json_object_candidates(raw))
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            continue
    return None


def _iter_balanced_json_object_candidates(raw: str) -> list[str]:
    candidates: list[str] = []
    in_string = False
    escape = False
    depth = 0
    start: int | None = None

    for idx, char in enumerate(raw):
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue
        if char == "{":
            if depth == 0:
                start = idx
            depth += 1
            continue
        if char == "}" and depth:
            depth -= 1
            if depth == 0 and start is not None:
                candidates.append(raw[start : idx + 1].strip())
                start = None
    return candidates


def _normalize_plan_payload(payload: dict[str, Any], has_tools: bool) -> dict[str, Any] | None:
    mode = str(payload.get("mode", "execute")).strip().lower()
    steps_raw = payload.get("steps")
    steps: list[str] = []
    if isinstance(steps_raw, list):
        steps = [str(step).strip() for step in steps_raw if str(step).strip()]
    response = str(payload.get("response", "")).strip()

    if mode not in {"reply", "execute"}:
        mode = "execute"

    if mode == "reply":
        if not response:
            return None
        return {"mode": "reply", "response": response, "steps": []}

    if not has_tools and response:
        return {"mode": "reply", "response": response, "steps": []}

    if not steps:
        return None
    return {"mode": "execute", "steps": steps[:_MAX_PLAN_STEPS], "response": ""}
