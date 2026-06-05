from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

import structlog

from octopal.infrastructure.providers.base import InferenceProvider, Message

logger = structlog.get_logger(__name__)

GroupAddressingAction = Literal["ignore", "respond_self", "respond_all_agents", "continue_thread"]


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
) -> GroupAddressingDecision:
    if not bool(getattr(settings, "group_addressing_enabled", True)):
        return GroupAddressingDecision("respond_self", "group addressing disabled", 1.0)
    if reply_to_agent:
        return GroupAddressingDecision("continue_thread", "message replies to this agent", 1.0)

    clean_text = (text or "").strip()
    if not clean_text:
        reason = "attachment-only group message without an explicit reply"
        if has_attachments:
            return GroupAddressingDecision("ignore", reason, 1.0)
        return GroupAddressingDecision("ignore", "empty group message", 1.0)

    if provider is None:
        return GroupAddressingDecision("ignore", "no provider available for group addressing", 0.0)

    identity = resolve_group_addressing_identity(settings)
    messages = [
        Message(
            role="system",
            content=(
                "You are a strict group-chat addressing gate for an AI agent. "
                "Decide whether the incoming group-chat message is addressed to this agent, "
                "to all agents, or to nobody. Use semantic understanding, not substring rules. "
                "Return only compact JSON with keys action, confidence, reason. "
                "action must be one of: ignore, respond_self, respond_all_agents, continue_thread. "
                "Use respond_self for clear direct requests to this agent by name, alias, role, or "
                "unambiguous second-person address. Use respond_all_agents when the user addresses "
                "all agents collectively. Use ignore when the message is for other named agents, "
                "for humans, or is ambient group conversation."
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
    )
    return decision


def _parse_decision(raw: str) -> GroupAddressingDecision:
    payload = _extract_json_object(raw)
    if payload is None:
        return GroupAddressingDecision("ignore", "invalid group addressing JSON", 0.0)

    action = str(payload.get("action", "") or "").strip().lower()
    if action not in {"ignore", "respond_self", "respond_all_agents", "continue_thread"}:
        return GroupAddressingDecision("ignore", "unknown group addressing action", 0.0)

    confidence = _coerce_confidence(payload.get("confidence"))
    reason = str(payload.get("reason", "") or "").strip()
    return GroupAddressingDecision(action=action, reason=reason, confidence=confidence)  # type: ignore[arg-type]


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
