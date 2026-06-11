from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import structlog

from octopal.infrastructure.providers.base import InferenceProvider, Message

logger = structlog.get_logger(__name__)

GroupAddressingAction = Literal["ignore", "respond_self", "respond_all_agents", "continue_thread"]

_SEMANTIC_REVIEW_REQUIRED_KEYS = {
    "is_direct_request_to_this_agent",
    "adds_new_information_or_decision_point",
    "would_reply_change_conversation_state",
    "loop_risk",
    "silence_is_better",
}


@dataclass(frozen=True)
class GroupAddressingIdentity:
    agent_name: str
    agent_aliases: list[str]
    collective_aliases: list[str]


@dataclass(frozen=True)
class GroupAddressingDecision:
    action: GroupAddressingAction
    reason: str = ""
    confidence: float = 0.0
    semantic_review: dict[str, Any] = field(default_factory=dict)

    @property
    def should_process(self) -> bool:
        return self.action != "ignore"


def resolve_group_addressing_identity(settings: Any) -> GroupAddressingIdentity:
    configured_name = str(getattr(settings, "group_agent_name", "") or "").strip()
    a2a_config = getattr(settings, "a2a", None)
    a2a_name = str(getattr(a2a_config, "agent_name", "") or "").strip()
    agent_name = configured_name or a2a_name or "Octopal"

    agent_aliases = _split_aliases(getattr(settings, "group_agent_aliases", ""))
    collective_aliases = _split_aliases(getattr(settings, "group_collective_aliases", ""))

    agent_aliases = _dedupe([agent_name, *agent_aliases])
    collective_aliases = _dedupe(collective_aliases)
    return GroupAddressingIdentity(
        agent_name=agent_name,
        agent_aliases=agent_aliases,
        collective_aliases=collective_aliases,
    )


async def decide_group_addressing(
    *,
    provider: InferenceProvider | None,
    settings: Any,
    channel: str,
    chat_id: int,
    text: str,
    has_attachments: bool = False,
    reply_to_agent: bool = False,
    sender_label: str | None = None,
    recent_context: Sequence[tuple[str, str, str]] | None = None,
) -> GroupAddressingDecision:
    if not bool(getattr(settings, "group_addressing_enabled", True)):
        return GroupAddressingDecision("respond_self", "group addressing disabled", 1.0)

    clean_text = (text or "").strip()
    if not clean_text:
        if reply_to_agent and has_attachments:
            clean_text = "Message replies to this agent with attachment(s) and no text."
        else:
            reason = "attachment-only group message without an explicit reply"
            if has_attachments:
                return GroupAddressingDecision("ignore", reason, 1.0)
            return GroupAddressingDecision("ignore", "empty group message", 1.0)

    if provider is None:
        if reply_to_agent:
            return GroupAddressingDecision(
                "ignore",
                "message replies to this agent but no provider is available for loop-safe review",
                0.0,
            )
        return GroupAddressingDecision("ignore", "no provider available for group addressing", 0.0)

    identity = resolve_group_addressing_identity(settings)
    messages = [
        Message(
            role="system",
            content=(
                "You are a strict group-chat addressing gate for an AI agent. "
                "Decide whether the incoming group-chat message is addressed to this agent, "
                "to all agents, continues this agent's active thread, or is addressed to nobody. "
                "Use semantic understanding, not substring rules. "
                "Return only compact JSON with keys action, confidence, reason, semantic_review. "
                "action must be one of: ignore, respond_self, respond_all_agents, continue_thread. "
                "semantic_review must be an object with these keys: "
                "is_direct_request_to_this_agent, adds_new_information_or_decision_point, "
                "would_reply_change_conversation_state, loop_risk, silence_is_better. "
                "Use booleans for all semantic_review fields except loop_risk, which must be "
                "low, medium, or high. "
                "Use respond_self for clear direct requests to this agent by name, alias, role, or "
                "unambiguous second-person address. Also use respond_self when another agent or "
                "assistant directly asks this agent a substantive question or asks this agent to "
                "perform a concrete action and this agent's answer is semantically necessary for "
                "the conversation to make progress. "
                "Use respond_all_agents when the user addresses all agents collectively. "
                "Use continue_thread when the recent context shows this agent was the active "
                "assistant in the current exchange and the new message is a natural follow-up, "
                "answer, correction, or continuation even if it does not repeat this agent's name. "
                "Avoid agent-to-agent loops: ignore agent or assistant messages that are merely "
                "acknowledgements, thanks, status echoes, social chatter, rhetorical questions, "
                "broad invitations to keep discussing, or repeated pings after this agent has "
                "already answered. If recent context shows multiple agent/assistant turns since "
                "the last clear human turn and the new message does not add a concrete new "
                "question, missing information, decision point, or action for this agent, choose "
                "ignore. Prefer silence whenever answering would only keep agents talking to "
                "themselves without changing the useful state of the conversation. "
                "Use ignore when the message is for other named agents, for humans, or is ambient "
                "group conversation. If the recent context is stale, unrelated, or dominated by "
                "another agent, ignore ambiguous messages."
            ),
        ),
        Message(
            role="user",
            content=json.dumps(
                {
                    "channel": channel,
                    "chat_id": chat_id,
                    "sender": sender_label or "",
                    "agent_name": identity.agent_name,
                    "agent_aliases": identity.agent_aliases,
                    "collective_aliases": identity.collective_aliases,
                    "has_attachments": has_attachments,
                    "reply_to_agent": reply_to_agent,
                    "agent_loop_guard": {
                        "enabled": True,
                        "reply_to_other_agents_only_for_substantive_new_work": True,
                    },
                    "recent_context": _format_recent_context(recent_context),
                    "message": clean_text,
                },
                ensure_ascii=False,
            ),
        ),
    ]

    try:
        raw = await provider.complete(messages)
    except Exception:
        logger.warning("Group addressing provider call failed", chat_id=chat_id, exc_info=True)
        return GroupAddressingDecision("ignore", "group addressing provider failed", 0.0)

    decision = _parse_decision(raw)
    logger.debug(
        "Group addressing decision",
        channel=channel,
        chat_id=chat_id,
        action=decision.action,
        confidence=decision.confidence,
        reason=decision.reason,
        semantic_review=decision.semantic_review,
    )
    return decision


async def load_recent_group_context(
    octo: Any,
    *,
    chat_id: int,
    limit: int = 8,
) -> list[tuple[str, str, str]]:
    memory = getattr(octo, "memory", None)
    get_recent_history = getattr(memory, "get_recent_history", None)
    if not callable(get_recent_history):
        return []
    try:
        history = await get_recent_history(chat_id, limit=limit)
    except Exception:
        logger.debug("Failed to load recent group context", chat_id=chat_id, exc_info=True)
        return []
    return [
        (str(role), str(content), str(created_at))
        for role, content, created_at in history
        if str(content or "").strip()
    ]


def _parse_decision(raw: str) -> GroupAddressingDecision:
    payload = _extract_json_object(raw)
    if payload is None:
        return GroupAddressingDecision("ignore", "invalid group addressing JSON", 0.0)

    action = str(payload.get("action", "") or "").strip().lower()
    if action not in {"ignore", "respond_self", "respond_all_agents", "continue_thread"}:
        return GroupAddressingDecision("ignore", "unknown group addressing action", 0.0)

    confidence = _coerce_confidence(payload.get("confidence"))
    reason = str(payload.get("reason", "") or "").strip()
    semantic_review = _coerce_semantic_review(payload.get("semantic_review"))
    if action != "ignore" and not _has_complete_semantic_review(semantic_review):
        return GroupAddressingDecision(
            "ignore",
            _append_guard_reason(reason, "missing required semantic review"),
            min(confidence, 0.5),
            semantic_review=semantic_review,
        )
    action, reason, confidence = _apply_loop_guard_consistency(
        action=action,
        reason=reason,
        confidence=confidence,
        semantic_review=semantic_review,
    )
    return GroupAddressingDecision(
        action=action, reason=reason, confidence=confidence, semantic_review=semantic_review
    )  # type: ignore[arg-type]


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    text = (raw or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    candidates = [text]
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidates.append(text[start : end + 1])

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _coerce_confidence(raw: object) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, value))


def _coerce_semantic_review(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    review: dict[str, Any] = {}
    for key in _SEMANTIC_REVIEW_REQUIRED_KEYS:
        value = raw.get(key)
        if isinstance(value, bool):
            review[key] = value
        elif key == "loop_risk":
            risk = str(value or "").strip().lower()
            if risk in {"low", "medium", "high"}:
                review[key] = risk
    return review


def _has_complete_semantic_review(semantic_review: dict[str, Any]) -> bool:
    return _SEMANTIC_REVIEW_REQUIRED_KEYS.issubset(semantic_review.keys())


def _apply_loop_guard_consistency(
    *,
    action: str,
    reason: str,
    confidence: float,
    semantic_review: dict[str, Any],
) -> tuple[str, str, float]:
    if action == "ignore" or not semantic_review:
        return action, reason, confidence

    silence_is_better = semantic_review.get("silence_is_better") is True
    changes_state = semantic_review.get("would_reply_change_conversation_state") is True
    adds_new = semantic_review.get("adds_new_information_or_decision_point") is True
    loop_risk = str(semantic_review.get("loop_risk", "") or "").strip().lower()

    if silence_is_better and not changes_state:
        return (
            "ignore",
            _append_guard_reason(reason, "semantic review says silence is better"),
            min(confidence, 0.65),
        )
    if loop_risk == "high" and not changes_state and not adds_new:
        return (
            "ignore",
            _append_guard_reason(reason, "semantic review indicates high loop risk"),
            min(confidence, 0.65),
        )
    return action, reason, confidence


def _append_guard_reason(reason: str, guard_reason: str) -> str:
    clean_reason = str(reason or "").strip()
    if not clean_reason:
        return guard_reason
    if guard_reason in clean_reason:
        return clean_reason
    return f"{clean_reason}; {guard_reason}"


def _format_recent_context(
    recent_context: Sequence[tuple[str, str, str]] | None,
) -> list[dict[str, str]]:
    formatted: list[dict[str, str]] = []
    for role, content, created_at in list(recent_context or [])[-8:]:
        text = str(content or "").strip()
        if not text:
            continue
        if len(text) > 700:
            text = text[:697].rstrip() + "..."
        formatted.append(
            {
                "role": str(role or ""),
                "content": text,
                "created_at": str(created_at or ""),
            }
        )
    return formatted


def _split_aliases(raw: object) -> list[str]:
    if isinstance(raw, (list, tuple)):
        values = [str(item).strip() for item in raw]
    else:
        values = [chunk.strip() for chunk in str(raw or "").split(",")]
    return [value for value in values if value]


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.casefold()
        if not value or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out
