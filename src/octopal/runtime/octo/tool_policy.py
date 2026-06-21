from __future__ import annotations

import re
import shlex
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from octopal.infrastructure.observability.helpers import hash_payload
from octopal.runtime.capability_outcomes import (
    CAPABILITY_OUTCOME_KEY,
    CapabilityOutcomeKind,
    capability_outcome,
)
from octopal.runtime.intents.types import ActionIntent
from octopal.tools.diagnostics import ToolResolutionReport
from octopal.tools.registry import ToolSpec

logger = structlog.get_logger(__name__)
_COMPUTER_USE_MUTATING_ACTIONS = {"click", "type", "key", "scroll"}


async def _maybe_request_octo_tool_approval(
    *,
    spec: ToolSpec,
    args: dict[str, Any],
    ctx: dict[str, object],
) -> dict[str, Any] | None:
    tool_name = str(getattr(spec, "name", "") or "")
    if tool_name == "computer_use":
        return await _maybe_request_computer_use_approval(spec=spec, args=args, ctx=ctx)
    if tool_name != "exec_run":
        return None
    reason = _exec_run_approval_reason(args)
    if reason is None:
        return None

    command = str(args.get("command", "") or "").strip()
    action = str(args.get("action", "start") or "start").strip().lower()
    intent = ActionIntent(
        id=str(uuid.uuid4()),
        type="exec.run",
        payload={
            "action": action,
            "command": command,
            "background": bool(args.get("background", False)),
            "reason": reason,
        },
        payload_hash=hash_payload(
            {
                "action": action,
                "command": command,
                "background": bool(args.get("background", False)),
                "reason": reason,
            }
        ),
        risk="high",
        requires_approval=True,
        worker_id="octo",
    )

    requester = _resolve_octo_approval_requester(ctx)
    if requester is None:
        return {
            "type": "approval_required",
            "tool": spec.name,
            "reason": reason,
            "message": "Dangerous exec_run command requires direct user approval, but no approval channel is available.",
            CAPABILITY_OUTCOME_KEY: capability_outcome(
                "needs_approval",
                reason=reason,
                next_action=(
                    "Ask the user for direct approval, or choose a safer non-dangerous tool path."
                ),
                tool=spec.name,
            ),
        }

    try:
        approved = await requester(intent)
    except Exception as exc:
        logger.exception("Octo exec approval requester failed")
        return {
            "type": "approval_required",
            "tool": spec.name,
            "reason": reason,
            "message": f"Dangerous exec_run command approval failed: {exc}",
            CAPABILITY_OUTCOME_KEY: capability_outcome(
                "needs_approval",
                reason=reason,
                next_action=(
                    "Retry the approval request if appropriate, or choose a safer non-dangerous tool path."
                ),
                tool=spec.name,
            ),
        }
    if approved:
        return None
    return {
        "type": "approval_denied",
        "tool": spec.name,
        "reason": reason,
        "message": "Dangerous exec_run command was not approved by the user.",
        CAPABILITY_OUTCOME_KEY: capability_outcome(
            "policy_denied",
            reason="user_denied_approval",
            next_action=(
                "Stop this action and choose a safer alternative, or report the concrete approval denial."
            ),
            tool=spec.name,
            policy_reason="user_denied_approval",
        ),
    }


async def _maybe_request_computer_use_approval(
    *,
    spec: ToolSpec,
    args: dict[str, Any],
    ctx: dict[str, object],
) -> dict[str, Any] | None:
    action = str(args.get("action", "") or "").strip().lower()
    if action not in _COMPUTER_USE_MUTATING_ACTIONS:
        return None

    payload = {
        "action": action,
        "pid": args.get("pid"),
        "window_id": args.get("window_id"),
        "element_index": args.get("element_index"),
        "x": args.get("x"),
        "y": args.get("y"),
        "key": args.get("key"),
        "modifiers": args.get("modifiers"),
        "direction": args.get("direction"),
        "text_preview": str(args.get("text", "") or "")[:120],
        "reason": f"desktop action `{action}` can modify the host UI",
    }
    payload = {key: value for key, value in payload.items() if value not in (None, "", [])}
    intent = ActionIntent(
        id=str(uuid.uuid4()),
        type="desktop.control",
        payload=payload,
        payload_hash=hash_payload(payload),
        risk="high",
        requires_approval=True,
        worker_id="octo",
    )

    requester = _resolve_octo_approval_requester(ctx)
    if requester is None:
        return {
            "type": "approval_required",
            "tool": spec.name,
            "reason": str(payload["reason"]),
            "message": "Mutating computer_use action requires direct user approval, but no approval channel is available.",
            CAPABILITY_OUTCOME_KEY: capability_outcome(
                "needs_approval",
                reason=str(payload["reason"]),
                next_action=(
                    "Ask the user for direct approval, or choose a read-only desktop inspection path."
                ),
                tool=spec.name,
            ),
        }

    try:
        approved = await requester(intent)
    except Exception as exc:
        logger.exception("Octo computer_use approval requester failed")
        return {
            "type": "approval_required",
            "tool": spec.name,
            "reason": str(payload["reason"]),
            "message": f"Mutating computer_use action approval failed: {exc}",
            CAPABILITY_OUTCOME_KEY: capability_outcome(
                "needs_approval",
                reason=str(payload["reason"]),
                next_action=(
                    "Retry the approval request if appropriate, or choose a read-only desktop inspection path."
                ),
                tool=spec.name,
            ),
        }
    if approved:
        return None
    return {
        "type": "approval_denied",
        "tool": spec.name,
        "reason": str(payload["reason"]),
        "message": "Mutating computer_use action was not approved by the user.",
        CAPABILITY_OUTCOME_KEY: capability_outcome(
            "policy_denied",
            reason="user_denied_approval",
            next_action=(
                "Stop this desktop action and choose a safer alternative, or report the approval denial."
            ),
            tool=spec.name,
            policy_reason="user_denied_approval",
        ),
    }


def _resolve_octo_approval_requester(
    ctx: dict[str, object],
) -> Callable[[ActionIntent], Awaitable[bool]] | None:
    requester = ctx.get("approval_requester")
    if callable(requester):
        return requester

    octo = ctx.get("octo")
    chat_id = int(ctx.get("chat_id", 0) or 0)
    approval_requesters = getattr(octo, "_approval_requesters", None)
    if isinstance(approval_requesters, dict):
        requester = approval_requesters.get(chat_id)
        if callable(requester):
            return requester

    approvals = getattr(octo, "approvals", None)
    if chat_id > 0 and getattr(approvals, "bot", None):

        async def _telegram_requester(intent: ActionIntent) -> bool:
            return await approvals.request_approval(chat_id, intent)

        return _telegram_requester
    return None


def _exec_run_approval_reason(args: dict[str, Any]) -> str | None:
    action = str(args.get("action", "start") or "start").strip().lower()
    if action == "start":
        command = str(args.get("command", "") or "").strip()
        return _dangerous_exec_command_reason(command)
    if action == "write":
        input_data = str(args.get("input_data", "") or "")
        reason = _dangerous_exec_command_reason(input_data)
        if reason is not None:
            return f"interactive input looks dangerous: {reason}"
    return None


def _dangerous_exec_command_reason(command: str) -> str | None:
    normalized = str(command or "").strip()
    if not normalized:
        return None
    lowered = normalized.lower()
    command_words = _shell_command_words(lowered)
    if not command_words:
        return None

    dangerous_tokens = {
        "sudo",
        "su",
        "doas",
        "rm",
        "rmdir",
        "unlink",
        "shred",
        "dd",
        "shutdown",
        "reboot",
        "halt",
        "poweroff",
        "kill",
        "pkill",
        "killall",
    }
    for token in command_words:
        if token in dangerous_tokens or token.startswith("mkfs"):
            return f"uses dangerous command `{token}`"

    dangerous_patterns = (
        (r"\bgit\s+reset\s+--hard\b", "uses `git reset --hard`"),
        (r"\bgit\s+clean\b.*\s-[^\s]*[fd]", "uses destructive `git clean`"),
        (r"\bdocker\s+system\s+prune\b", "uses `docker system prune`"),
        (r"\bdocker\s+(container\s+)?rm\b", "removes Docker containers"),
        (r"\bdocker\s+compose\b.*\bdown\b", "stops Docker compose services"),
        (r"\bkubectl\s+delete\b", "deletes Kubernetes resources"),
        (r"\bchmod\s+.*\b777\b", "sets broad chmod permissions"),
        (r"\bchown\s+.*\s-r\b|\bchown\s+-r\b", "recursively changes ownership"),
        (r"\bdiskutil\s+erase", "erases a disk"),
        (r">\s*/dev/(?!null(?:$|[\s;&|)]))", "writes to a device path"),
    )
    for pattern, reason in dangerous_patterns:
        if re.search(pattern, lowered):
            return reason
    return None


def _shell_tokens(command: str) -> list[str]:
    try:
        return [str(token).strip().lower() for token in shlex.split(command) if str(token).strip()]
    except ValueError:
        return [token for token in re.split(r"\s+", command) if token]


def _shell_command_words(command: str) -> list[str]:
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|()")
        lexer.whitespace_split = True
        tokens = [str(token).strip().lower() for token in lexer if str(token).strip()]
    except ValueError:
        tokens = _shell_tokens(command)

    command_words: list[str] = []
    expect_command = True
    command_prefixes = {"command", "builtin", "env", "time", "nohup"}
    separators = {";", "&", "&&", "|", "||", "(", ")"}
    for token in tokens:
        if token in separators:
            expect_command = True
            continue
        if not expect_command:
            continue
        if "=" in token and not token.startswith("="):
            continue
        command_words.append(token)
        expect_command = token in command_prefixes
    return command_words


def _build_octo_tool_policy_summary(
    active_tools: list[ToolSpec],
    report: ToolResolutionReport | None,
) -> str:
    available_counts = {"safe": 0, "guarded": 0, "dangerous": 0}
    for spec in active_tools:
        available_counts[str(spec.metadata.risk)] = (
            available_counts.get(str(spec.metadata.risk), 0) + 1
        )

    blocked_dangerous = 0
    blocked_guarded = 0
    if report is not None:
        for entry in report.blocked_tools:
            risk = str(entry.tool.metadata.risk)
            if risk == "dangerous":
                blocked_dangerous += 1
            elif risk == "guarded":
                blocked_guarded += 1

    return (
        "Tool policy contract:\n"
        "- Use safe tools by default.\n"
        "- Use guarded tools only when they materially advance the task.\n"
        "- Do not choose dangerous tools as the first path, even if available.\n"
        "- If a tool is blocked by policy, do not repeat the same call; choose a safer alternative or explain the constraint.\n"
        "- Do not bypass a blocked tool with an equivalent risky workaround.\n"
        "Current tool policy snapshot:\n"
        f"- active_safe={available_counts['safe']}\n"
        f"- active_guarded={available_counts['guarded']}\n"
        f"- active_dangerous={available_counts['dangerous']}\n"
        f"- blocked_guarded={blocked_guarded}\n"
        f"- blocked_dangerous={blocked_dangerous}"
    )


def _resolve_octo_policy_block(tool_name: str, ctx: dict[str, object]) -> dict[str, Any] | None:
    normalized_name = str(tool_name or "").strip().lower()
    if not normalized_name:
        return None

    report = ctx.get("tool_resolution_report")
    if not isinstance(report, ToolResolutionReport):
        return None

    for entry in report.blocked_tools:
        if str(entry.tool.name).strip().lower() != normalized_name:
            continue
        return {
            "type": "policy_block",
            "tool": entry.tool.name,
            "reason": entry.reasons[0] if entry.reasons else "blocked_by_policy",
            "risk": entry.tool.metadata.risk,
            "message": f"Tool '{entry.tool.name}' is blocked by the current Octo tool policy.",
            "hint": _policy_block_hint(entry.tool),
            CAPABILITY_OUTCOME_KEY: capability_outcome(
                "policy_denied",
                reason=entry.reasons[0] if entry.reasons else "blocked_by_policy",
                next_action=_policy_block_hint(entry.tool),
                tool=entry.tool.name,
                policy_reason=entry.reasons[0] if entry.reasons else "blocked_by_policy",
            ),
        }
    return None


def _resolve_octo_unavailable_tool(
    *,
    tool_name: str,
    active_tools: list[ToolSpec],
    ctx: dict[str, object],
) -> dict[str, Any] | None:
    normalized_name = str(tool_name or "").strip().lower()
    if not normalized_name:
        return None

    active_names = {str(tool.name).strip().lower() for tool in active_tools}
    if normalized_name in active_names:
        return None

    spec = _find_known_tool_spec(normalized_name, ctx)
    if spec is None:
        return None

    active_tool_names = {str(tool.name).strip().lower() for tool in active_tools}
    if "octo_continue_from_control_route" in active_tool_names:
        kind: CapabilityOutcomeKind = "needs_continuation"
        next_action = (
            "Call octo_continue_from_control_route with one concrete continuation task "
            "that can use the broader Octo toolset."
        )
    elif "tool_catalog_search" in active_tool_names:
        kind = "needs_continuation"
        next_action = (
            "Use tool_catalog_search to activate the missing tool if it fits the task; "
            "otherwise choose a safe alternative."
        )
    elif "worker_spawn" in tuple(getattr(spec.metadata, "capabilities", ()) or ()):
        kind = "needs_worker"
        next_action = "Delegate the work through an available worker path."
    else:
        kind = "needs_continuation"
        next_action = "Continue through a route that exposes the required capability."

    return {
        "type": "tool_unavailable",
        "tool": tool_name,
        "message": f"Tool '{tool_name}' exists but is not active in this execution contract.",
        CAPABILITY_OUTCOME_KEY: capability_outcome(
            kind,
            reason="known_tool_not_active",
            next_action=next_action,
            missing_tool=tool_name,
            details={
                "category": str(getattr(spec.metadata, "category", "") or ""),
                "capabilities": list(getattr(spec.metadata, "capabilities", ()) or ()),
            },
        ),
    }


def _find_known_tool_spec(normalized_name: str, ctx: dict[str, object]) -> ToolSpec | None:
    report = ctx.get("tool_resolution_report")
    if isinstance(report, ToolResolutionReport):
        candidates = list(report.available_tools) + [entry.tool for entry in report.blocked_tools]
        for spec in candidates:
            if str(spec.name).strip().lower() == normalized_name:
                return spec

    for key in ("all_tool_specs", "known_tool_specs"):
        for spec in ctx.get(key) or ():
            if isinstance(spec, ToolSpec) and str(spec.name).strip().lower() == normalized_name:
                return spec
    return None


def _policy_block_hint(tool: ToolSpec) -> str:
    risk = str(tool.metadata.risk)
    if risk == "dangerous":
        return (
            "Try a safer read-only or worker-driven path first, then explain what remains blocked."
        )
    if risk == "guarded":
        return (
            "Use a lower-risk alternative if one exists, or explain why the guarded path matters."
        )
    return "Use another available tool path."
