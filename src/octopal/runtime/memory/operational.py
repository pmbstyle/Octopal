from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

import structlog

from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import OperationalMemoryItemRecord
from octopal.utils import utc_now

logger = structlog.get_logger(__name__)

ACTIVE_OPERATIONAL_STATUSES = ["active", "in_progress", "blocked"]
OPERATIONAL_ITEM_KINDS = {
    "fact_obligation",
    "decision_rule",
    "assistant_commitment",
    "user_request_open",
    "blocker",
    "followup",
}

_EXTRACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "items": {
            "type": "array",
            "maxItems": 6,
            "items": {
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": sorted(OPERATIONAL_ITEM_KINDS),
                    },
                    "statement": {"type": "string"},
                    "next_action": {"type": "string"},
                    "priority": {"type": "integer", "minimum": 0, "maximum": 3},
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "requires_plan": {"type": "boolean"},
                    "evidence": {
                        "type": "array",
                        "maxItems": 3,
                        "items": {"type": "string"},
                    },
                },
                "required": ["kind", "statement", "confidence"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["items"],
    "additionalProperties": False,
}


@dataclass
class OperationalMemoryService:
    store: Store
    provider: InferenceProvider
    owner_id: str = "default"
    min_confidence: float = 0.58

    async def extract_and_store_turn(
        self,
        *,
        chat_id: int,
        user_message: str,
        assistant_message: str,
        channel: str | None = None,
        conversation_scope: str | None = None,
        source_ref: str | None = None,
    ) -> list[OperationalMemoryItemRecord]:
        if chat_id == 0 or not assistant_message.strip():
            return []
        active = await asyncio.to_thread(
            self.store.list_operational_memory_items,
            self.owner_id,
            chat_id=chat_id,
            statuses=ACTIVE_OPERATIONAL_STATUSES,
            limit=12,
        )
        payload = await self._extract_items(
            chat_id=chat_id,
            user_message=user_message,
            assistant_message=assistant_message,
            channel=channel,
            conversation_scope=conversation_scope,
            active_items=active,
        )
        records: list[OperationalMemoryItemRecord] = []
        for item in payload:
            record = self._record_from_payload(
                item,
                chat_id=chat_id,
                channel=channel,
                conversation_scope=conversation_scope,
                source_ref=source_ref,
            )
            if record is None:
                continue
            await asyncio.to_thread(self.store.upsert_operational_memory_item, record)
            records.append(record)
        return records

    def active_context(self, chat_id: int, *, limit: int = 8) -> str:
        items = self.store.list_operational_memory_items(
            self.owner_id,
            chat_id=chat_id,
            statuses=ACTIVE_OPERATIONAL_STATUSES,
            limit=limit,
        )
        if not items:
            return ""
        compact = [
            {
                "id": item.id,
                "kind": item.kind,
                "statement": item.statement,
                "next_action": item.next_action,
                "status": item.status,
                "priority": item.priority,
                "confidence": round(float(item.confidence), 2),
                "plan_run_id": item.plan_run_id,
                "plan_step_id": item.plan_step_id,
            }
            for item in items
        ]
        return (
            "Operational memory commitments are active for this chat. These are durable "
            "obligations, rules, blockers, or follow-ups inferred semantically from prior turns.\n"
            "- Reconcile related active items before answering about progress or next actions.\n"
            "- If you create a runtime plan for one of these items, include its id in plan metadata as `commitment_ids`.\n"
            "- Do not claim an item is done unless its linked plan/state is terminal or you have direct evidence.\n"
            "<operational_memory>\n"
            f"{json.dumps(compact, ensure_ascii=False)}\n"
            "</operational_memory>"
        )

    async def _extract_items(
        self,
        *,
        chat_id: int,
        user_message: str,
        assistant_message: str,
        channel: str | None,
        conversation_scope: str | None,
        active_items: list[OperationalMemoryItemRecord],
    ) -> list[dict[str, Any]]:
        prompt = _build_extraction_prompt(
            chat_id=chat_id,
            user_message=user_message,
            assistant_message=assistant_message,
            channel=channel,
            conversation_scope=conversation_scope,
            active_items=active_items,
        )
        try:
            raw = await self._complete_json(prompt)
        except Exception:
            logger.debug("Operational memory extraction skipped due to provider error", exc_info=True)
            return []
        parsed = _extract_json_object(raw)
        if not isinstance(parsed, dict):
            return []
        raw_items = parsed.get("items")
        if not isinstance(raw_items, list):
            return []
        return [item for item in raw_items if isinstance(item, dict)]

    async def _complete_json(self, prompt: str) -> str:
        messages = [
            Message(
                role="system",
                content=(
                    "You extract operational memory from assistant turns. Return JSON only. "
                    "The text may be in any language or mixed languages. Judge semantic speech acts, "
                    "not keywords. Do not infer commitments from hypotheticals, capabilities, jokes, "
                    "or politeness unless the speaker has taken responsibility for future action."
                ),
            ),
            Message(role="user", content=prompt),
        ]
        response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "operational_memory_extraction",
                "schema": _EXTRACTION_SCHEMA,
            },
        }
        try:
            return await self.provider.complete(
                messages,
                temperature=0,
                max_tokens=900,
                response_format=response_format,
            )
        except Exception as exc:
            if not _looks_like_response_format_error(exc):
                raise
            logger.debug("Retrying operational memory extraction without response_format")
            return await self.provider.complete(messages, temperature=0, max_tokens=900)

    def _record_from_payload(
        self,
        item: dict[str, Any],
        *,
        chat_id: int,
        channel: str | None,
        conversation_scope: str | None,
        source_ref: str | None,
    ) -> OperationalMemoryItemRecord | None:
        kind = str(item.get("kind") or "").strip()
        if kind not in OPERATIONAL_ITEM_KINDS:
            return None
        statement = _clean_text(item.get("statement"), limit=600)
        if not statement:
            return None
        next_action = _clean_text(item.get("next_action"), limit=600) or None
        try:
            confidence = float(item.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence < self.min_confidence:
            return None
        try:
            priority = int(item.get("priority", 2) or 2)
        except (TypeError, ValueError):
            priority = 2
        priority = max(0, min(priority, 3))
        evidence = [
            _clean_text(value, limit=220)
            for value in item.get("evidence", [])
            if _clean_text(value, limit=220)
        ][:3]
        now = utc_now()
        record_id = _stable_item_id(
            self.owner_id,
            chat_id,
            source_ref,
            kind,
            statement,
            next_action,
        )
        return OperationalMemoryItemRecord(
            id=record_id,
            owner_id=self.owner_id,
            chat_id=chat_id,
            kind=kind,
            statement=statement,
            next_action=next_action,
            status="active",
            priority=priority,
            confidence=confidence,
            source_kind="turn",
            source_ref=source_ref,
            evidence=evidence,
            metadata={
                "channel": channel,
                "conversation_scope": conversation_scope,
                "requires_plan": bool(item.get("requires_plan")),
            },
            created_at=now,
            updated_at=now,
        )


def _build_extraction_prompt(
    *,
    chat_id: int,
    user_message: str,
    assistant_message: str,
    channel: str | None,
    conversation_scope: str | None,
    active_items: list[OperationalMemoryItemRecord],
) -> str:
    active_payload = [
        {
            "id": item.id,
            "kind": item.kind,
            "statement": item.statement,
            "next_action": item.next_action,
            "status": item.status,
        }
        for item in active_items
    ]
    return (
        "Extract only durable operational memory created or reinforced by this turn.\n"
        "Operational memory means a fact, decision, promise, open user request, blocker, or follow-up "
        "that should affect future action. Return no item for ordinary conversation, completed work, "
        "vague intent, speculation, or unsupported interpretation.\n"
        "Use concise English for `statement` and `next_action`, even when source text is in another language.\n"
        "Allowed kinds:\n"
        "- fact_obligation: a factual state creates a needed check or action.\n"
        "- decision_rule: a durable decision should guide future behavior.\n"
        "- assistant_commitment: the assistant took responsibility for future work.\n"
        "- user_request_open: the user requested work that is not yet completed.\n"
        "- blocker: a known obstacle needs resolution or user input.\n"
        "- followup: a future return/reminder/check is needed.\n\n"
        f"chat_id={chat_id}\n"
        f"channel={channel or ''}\n"
        f"conversation_scope={conversation_scope or ''}\n\n"
        "<active_items>\n"
        f"{json.dumps(active_payload, ensure_ascii=False)}\n"
        "</active_items>\n\n"
        "<user_message>\n"
        f"{user_message}\n"
        "</user_message>\n\n"
        "<assistant_message>\n"
        f"{assistant_message}\n"
        "</assistant_message>\n\n"
        "Return JSON matching this shape exactly:\n"
        '{"items":[{"kind":"assistant_commitment","statement":"...","next_action":"...","priority":2,'
        '"confidence":0.0,"requires_plan":false,"evidence":["short source span"]}]}'
    )


def _stable_item_id(
    owner_id: str,
    chat_id: int,
    source_ref: str | None,
    kind: str,
    statement: str,
    next_action: str | None,
) -> str:
    raw = "|".join(
        [
            owner_id,
            str(chat_id),
            source_ref or "",
            kind,
            _normalize_for_hash(statement),
            _normalize_for_hash(next_action or ""),
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"omem-{digest}"


def _normalize_for_hash(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _clean_text(value: Any, *, limit: int) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) > limit:
        text = text[: limit - 3].rstrip() + "..."
    return text


def _looks_like_response_format_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "response_format" in text or "json_schema" in text


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None
