from __future__ import annotations

import asyncio
import math
import re
import uuid
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from octopal.infrastructure.providers.embeddings import EmbeddingsProvider
from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import MemoryEntry
from octopal.utils import utc_now

if TYPE_CHECKING:
    from octopal.runtime.memory.facts import FactsService

_ASSERTION_RE = re.compile(
    r"^\s*(?P<subject>.+?)\s+is\s+(?P<neg>not\s+)?(?P<predicate>.+?)\s*[.!?]?\s*$",
    re.IGNORECASE,
)
_DECISION_PATTERNS = (
    re.compile(r"\b(decide|decided|choose|chose|picked|settled on|went with|switch(?:ed)? to|migrat(?:e|ed) to)\b", re.IGNORECASE),
    re.compile(r"\b(instead of|rather than|trade-?off|the reason is|the reason was|because)\b", re.IGNORECASE),
)
_PREFERENCE_PATTERNS = (
    re.compile(r"\b(i prefer|we prefer|prefer to|always use|never use|please always|please never)\b", re.IGNORECASE),
    re.compile(r"\b(i like|i don't like|i dont like|we always|we never)\b", re.IGNORECASE),
)
_MILESTONE_PATTERNS = (
    re.compile(r"\b(it works|it worked|got it working|fixed|solved|resolved|figured it out)\b", re.IGNORECASE),
    re.compile(r"\b(implemented|shipped|deployed|launched|breakthrough|released)\b", re.IGNORECASE),
)
_PROBLEM_PATTERNS = (
    re.compile(r"\b(bug|error|crash|broken|issue|problem|failed|failing|stuck)\b", re.IGNORECASE),
    re.compile(r"\b(doesn't work|doesnt work|not working|won't work|wont work|root cause|workaround)\b", re.IGNORECASE),
    re.compile(r"\b(not healthy|unhealthy|degraded|down|outage)\b", re.IGNORECASE),
)
_EMOTIONAL_PATTERNS = (
    re.compile(r"\b(worried|afraid|proud|happy|sad|sorry|angry|grateful|excited)\b", re.IGNORECASE),
    re.compile(r"\b(frustrated|confused|love|hate|i feel|i need|i wish)\b", re.IGNORECASE),
)


@dataclass
class MemoryService:
    store: Store
    embeddings: EmbeddingsProvider | None
    owner_id: str = "default"
    top_k: int = 5
    prefilter_k: int = 80
    min_score: float = 0.25
    max_chars: int = 32000
    facts: FactsService | None = None

    async def add_message(
        self,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        trimmed = content.strip()
        if not trimmed:
            return
        if len(trimmed) > self.max_chars:
            trimmed = trimmed[: self.max_chars]
        embedding = None
        if self.embeddings is not None:
            try:
                vectors = await self.embeddings.embed([trimmed])
                embedding = vectors[0] if vectors else None
            except Exception:
                embedding = None

        merged_metadata = dict(metadata or {})
        merged_metadata.setdefault("owner_id", self.owner_id)
        merged_metadata.setdefault("confidence", _default_confidence(role))
        _merge_enrichment_metadata(merged_metadata, trimmed)

        chat_id = _coerce_chat_id(merged_metadata.get("chat_id"))
        owner_id = str(merged_metadata.get("owner_id", self.owner_id))

        if chat_id is not None:
            recent_entries = await asyncio.to_thread(self.store.list_memory_entries_by_chat, chat_id, 50)
            owner_recent = [
                entry
                for entry in recent_entries
                if str((entry.metadata or {}).get("owner_id", self.owner_id)) == owner_id
            ]
            if _is_recent_duplicate(owner_recent, role, trimmed):
                return
            contradictions = _find_contradictions(owner_recent, trimmed)
            if contradictions:
                merged_metadata["contradiction_detected"] = True
                merged_metadata["contradiction_with"] = contradictions[:5]
                base_conf = _coerce_confidence(merged_metadata.get("confidence"), default=0.5)
                merged_metadata["confidence"] = max(0.1, base_conf * 0.6)

        entry = MemoryEntry(
            id=str(uuid.uuid4()),
            role=role,
            content=trimmed,
            embedding=embedding,
            created_at=utc_now(),
            metadata=merged_metadata,
        )
        await asyncio.to_thread(self.store.add_memory_entry, entry)
        if self.facts is not None:
            with suppress(Exception):
                await asyncio.to_thread(self.facts.record_candidate_from_memory, entry)

    async def get_context(self, query: str, exclude_chat_id: int | None = None) -> list[str]:
        return await self.get_context_by_facets(
            query,
            exclude_chat_id=exclude_chat_id,
            memory_facets=None,
        )

    async def get_context_by_facets(
        self,
        query: str,
        *,
        exclude_chat_id: int | None = None,
        memory_facets: list[str] | None = None,
    ) -> list[str]:
        if self.embeddings is None:
            return []
        trimmed = query.strip()
        if not trimmed:
            return []
        try:
            vectors = await self.embeddings.embed([trimmed])
        except Exception:
            return []
        if not vectors:
            return []
        query_embedding = vectors[0]
        candidates = await asyncio.to_thread(
            self.store.search_memory_entries_lexical,
            self.owner_id,
            trimmed,
            self.prefilter_k,
            exclude_chat_id,
        )
        if not candidates:
            candidates = await asyncio.to_thread(
                self.store.list_memory_entries_for_owner,
                self.owner_id,
                max(self.prefilter_k, 200),
            )
        facet_filtered = _filter_entries_by_facets(candidates, memory_facets)
        if facet_filtered:
            candidates = facet_filtered
        scored: list[tuple[float, MemoryEntry]] = []
        for entry in candidates:
            if not entry.embedding:
                continue

            # Skip entries from the current chat to avoid duplication with recent history
            if exclude_chat_id is not None:
                entry_chat_id = entry.metadata.get("chat_id")
                if entry_chat_id == exclude_chat_id:
                    continue

            score = _cosine_similarity(query_embedding, entry.embedding)
            score *= _coerce_confidence(entry.metadata.get("confidence"), default=0.5)
            score *= _recency_weight(entry.created_at)
            if entry.metadata.get("contradiction_detected"):
                score *= 0.75
            if score >= self.min_score:
                scored.append((score, entry))
        scored.sort(key=lambda item: item[0], reverse=True)
        top = scored[: self.top_k]
        return [f"{entry.role}: {entry.content}" for _, entry in top]

    async def get_recent_history(
        self,
        chat_id: int,
        limit: int = 6,
        *,
        conversation_scope: str | None = None,
    ) -> list[tuple[str, str, str]]:
        fetch_limit = max(limit * 5, 50)
        if conversation_scope:
            owner_fetch_limit = max(fetch_limit * 4, 200)
            owner_entries = await asyncio.to_thread(
                self.store.list_memory_entries_for_owner,
                self.owner_id,
                owner_fetch_limit,
            )
            entries = [
                entry
                for entry in owner_entries
                if _entry_matches_conversation_scope(
                    entry,
                    chat_id=chat_id,
                    conversation_scope=conversation_scope,
                )
            ]
            entries.sort(key=lambda entry: entry.created_at, reverse=True)
            entries = entries[:fetch_limit]
        else:
            entries = await asyncio.to_thread(
                self.store.list_memory_entries_by_chat, chat_id, limit=fetch_limit
            )
        entries = [entry for entry in entries if _is_conversational_history_entry(entry)][:limit]
        entries.reverse()
        return [(entry.role, entry.content, entry.created_at.isoformat()) for entry in entries]


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


@dataclass(frozen=True)
class _Assertion:
    subject: str
    predicate: str
    negated: bool


def _normalize_text(value: str) -> str:
    lowered = (value or "").strip().lower()
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered


def _is_conversational_history_entry(entry: MemoryEntry) -> bool:
    metadata = entry.metadata or {}
    if bool(metadata.get("passive_group_observation")):
        return entry.role == "system"
    if entry.role not in {"user", "assistant"}:
        return False
    internal_flags = (
        "worker_result",
        "planner",
        "scheduler",
        "control_plane",
        "heartbeat",
    )
    return not any(bool(metadata.get(flag)) for flag in internal_flags)


def _entry_matches_conversation_scope(
    entry: MemoryEntry,
    *,
    chat_id: int,
    conversation_scope: str,
) -> bool:
    metadata = entry.metadata or {}
    if str(metadata.get("conversation_scope", "") or "") == conversation_scope:
        return True
    if metadata.get("chat_id") == chat_id:
        return True
    channel = str(metadata.get("channel", "") or "").strip().lower()
    return metadata.get("conversation_scope") is None and channel in {
        "telegram",
        "whatsapp",
        "desktop",
        "chat",
        "a2a",
    }


def _extract_assertion(value: str) -> _Assertion | None:
    match = _ASSERTION_RE.match(value or "")
    if not match:
        return None
    subject = _normalize_text(match.group("subject"))
    predicate = _normalize_text(match.group("predicate"))
    if not subject or not predicate:
        return None
    return _Assertion(
        subject=subject,
        predicate=predicate,
        negated=bool(match.group("neg")),
    )


def _is_recent_duplicate(entries: list[MemoryEntry], role: str, content: str) -> bool:
    normalized_content = _normalize_text(content)
    for entry in entries:
        if entry.role != role:
            continue
        if _normalize_text(entry.content) == normalized_content:
            return True
    return False


def _find_contradictions(entries: list[MemoryEntry], content: str) -> list[str]:
    current = _extract_assertion(content)
    if not current:
        return []
    contradictory_ids: list[str] = []
    for entry in entries:
        other = _extract_assertion(entry.content)
        if not other:
            continue
        if current.subject == other.subject and current.predicate == other.predicate and current.negated != other.negated:
            contradictory_ids.append(entry.id)
    return contradictory_ids


def _default_confidence(role: str) -> float:
    by_role = {
        "system": 0.85,
        "assistant": 0.7,
        "user": 0.65,
        "tool": 0.9,
    }
    return by_role.get((role or "").lower(), 0.6)


def _merge_enrichment_metadata(metadata: dict[str, Any], content: str) -> None:
    facets = set(_coerce_str_list(metadata.get("memory_facets")))
    facets.update(infer_memory_facets(content))
    if metadata.get("fact_candidate") is False:
        facets.discard("fact_candidate")
    if facets:
        metadata["memory_facets"] = sorted(facets)

    assertion = _extract_assertion(content)
    if assertion is None:
        return

    metadata.setdefault("fact_candidate", True)
    metadata.setdefault("fact_subject_hint", assertion.subject)
    metadata.setdefault("fact_value_hint", assertion.predicate)
    metadata.setdefault("fact_negated", assertion.negated)


def infer_memory_facets(content: str) -> set[str]:
    facets: set[str] = set()
    for facet, patterns in (
        ("decision", _DECISION_PATTERNS),
        ("preference", _PREFERENCE_PATTERNS),
        ("milestone", _MILESTONE_PATTERNS),
        ("problem", _PROBLEM_PATTERNS),
        ("emotional", _EMOTIONAL_PATTERNS),
    ):
        if any(pattern.search(content) for pattern in patterns):
            facets.add(facet)
    if _extract_assertion(content) is not None:
        facets.add("fact_candidate")
    return facets


def _filter_entries_by_facets(
    entries: list[MemoryEntry],
    memory_facets: list[str] | None,
) -> list[MemoryEntry]:
    requested = set(_coerce_str_list(memory_facets))
    if not requested:
        return []

    matches: list[MemoryEntry] = []
    for entry in entries:
        entry_facets = set(_coerce_str_list((entry.metadata or {}).get("memory_facets")))
        if entry_facets & requested:
            matches.append(entry)
    return matches


def _coerce_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            result.append(text)
    return result


def _coerce_confidence(value: Any, default: float) -> float:
    try:
        conf = float(value)
    except Exception:
        conf = default
    return max(0.05, min(1.0, conf))


def _coerce_chat_id(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _recency_weight(created_at) -> float:
    age_seconds = max(0.0, (utc_now() - created_at).total_seconds())
    # Exponential decay with ~14 day horizon while keeping a floor for stable long-term memory.
    decay = math.exp(-(age_seconds / (14.0 * 24.0 * 3600.0)))
    return 0.65 + (0.35 * decay)
