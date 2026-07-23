from __future__ import annotations

import asyncio
import math
import re
import uuid
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from octopal.infrastructure.providers.embeddings import EmbeddingsProvider
from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import MemoryEmbeddingCandidate, MemoryEntry
from octopal.runtime.memory.retrieval import MemoryRetrievalTrace
from octopal.utils import utc_now

if TYPE_CHECKING:
    from octopal.runtime.memory.facts import FactsService

_ASSERTION_RE = re.compile(
    r"^\s*(?P<subject>.+?)\s+is\s+(?P<neg>not\s+)?(?P<predicate>.+?)\s*[.!?]?\s*$",
    re.IGNORECASE,
)
_DECISION_PATTERNS = (
    re.compile(
        r"\b(decide|decided|choose|chose|picked|settled on|went with|switch(?:ed)? to|migrat(?:e|ed) to)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(instead of|rather than|trade-?off|the reason is|the reason was|because)\b",
        re.IGNORECASE,
    ),
)
_PREFERENCE_PATTERNS = (
    re.compile(
        r"\b(i prefer|we prefer|prefer to|always use|never use|please always|please never)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(i like|i don't like|i dont like|we always|we never)\b", re.IGNORECASE),
)
_MILESTONE_PATTERNS = (
    re.compile(
        r"\b(it works|it worked|got it working|fixed|solved|resolved|figured it out)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(implemented|shipped|deployed|launched|breakthrough|released)\b", re.IGNORECASE),
)
_PROBLEM_PATTERNS = (
    re.compile(r"\b(bug|error|crash|broken|issue|problem|failed|failing|stuck)\b", re.IGNORECASE),
    re.compile(
        r"\b(doesn't work|doesnt work|not working|won't work|wont work|root cause|workaround)\b",
        re.IGNORECASE,
    ),
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
                embed_documents = getattr(self.embeddings, "embed_documents", None)
                if not callable(embed_documents):
                    embed_documents = self.embeddings.embed
                vectors = await embed_documents([trimmed])
                embedding = vectors[0] if vectors else None
            except Exception:
                embedding = None

        merged_metadata = dict(metadata or {})
        merged_metadata.setdefault("owner_id", self.owner_id)
        if self.embeddings is not None and embedding is not None:
            merged_metadata.setdefault(
                "embedding_model", str(getattr(self.embeddings, "model_id", "unknown"))
            )
        merged_metadata.setdefault("confidence", _default_confidence(role))
        _merge_enrichment_metadata(merged_metadata, trimmed)

        chat_id = _coerce_chat_id(merged_metadata.get("chat_id"))
        owner_id = str(merged_metadata.get("owner_id", self.owner_id))

        if chat_id is not None:
            recent_entries = await asyncio.to_thread(
                self.store.list_memory_entries_by_chat, chat_id, 50
            )
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

    async def migrate_embeddings(self, *, batch_size: int = 64) -> int:
        """Rebuild persisted vectors that were produced by a previous embedding model."""
        if self.embeddings is None:
            return 0
        model_id = str(getattr(self.embeddings, "model_id", "unknown"))
        if model_id == "unknown":
            raise RuntimeError("local embedding provider did not report a model identifier")
        embed_documents = getattr(self.embeddings, "embed_documents", None)
        if not callable(embed_documents):
            embed_documents = self.embeddings.embed

        migrated = 0
        while True:
            entries = await asyncio.to_thread(
                self.store.list_memory_entries_requiring_embedding_migration,
                self.owner_id,
                model_id,
                batch_size,
            )
            if not entries:
                return migrated
            vectors = await embed_documents([entry.content for entry in entries])
            if len(vectors) != len(entries):
                raise RuntimeError("embedding provider returned an incomplete migration batch")
            updates = [(entry.id, vector) for entry, vector in zip(entries, vectors, strict=True)]
            await asyncio.to_thread(self.store.replace_memory_embeddings, model_id, updates)
            migrated += len(updates)

    async def get_context_by_facets(
        self,
        query: str,
        *,
        exclude_chat_id: int | None = None,
        memory_facets: list[str] | None = None,
    ) -> list[str]:
        entries = await self.get_context_entries_by_facets(
            query,
            exclude_chat_id=exclude_chat_id,
            memory_facets=memory_facets,
        )
        return [f"{entry.role}: {entry.content}" for entry in entries]

    async def get_context_entries_by_facets(
        self,
        query: str,
        *,
        exclude_chat_id: int | None = None,
        memory_facets: list[str] | None = None,
    ) -> list[MemoryEntry]:
        retrievals = await self.get_context_retrievals_by_facets(
            query,
            exclude_chat_id=exclude_chat_id,
            memory_facets=memory_facets,
        )
        return [retrieval.entry for retrieval in retrievals]

    async def get_context_retrievals_by_facets(
        self,
        query: str,
        *,
        exclude_chat_id: int | None = None,
        memory_facets: list[str] | None = None,
    ) -> list[MemoryRetrieval]:
        trimmed = query.strip()
        if not trimmed:
            return []
        lexical_candidates = await asyncio.to_thread(
            self.store.search_memory_entries_lexical,
            self.owner_id,
            trimmed,
            self.prefilter_k,
            exclude_chat_id,
        )
        if exclude_chat_id is not None:
            lexical_candidates = [
                entry
                for entry in lexical_candidates
                if entry.metadata.get("chat_id") != exclude_chat_id
            ]
        if self.embeddings is None:
            lexical_candidates = _prefer_matching_facets(
                lexical_candidates,
                requested_facets=memory_facets,
            )
            return _rank_lexical_candidates(lexical_candidates, top_k=self.top_k)
        try:
            embed_queries = getattr(self.embeddings, "embed_queries", None)
            if not callable(embed_queries):
                embed_queries = self.embeddings.embed
            vectors = await embed_queries([trimmed])
        except Exception:
            lexical_candidates = _prefer_matching_facets(
                lexical_candidates,
                requested_facets=memory_facets,
            )
            return _rank_lexical_candidates(lexical_candidates, top_k=self.top_k)
        if not vectors:
            lexical_candidates = _prefer_matching_facets(
                lexical_candidates,
                requested_facets=memory_facets,
            )
            return _rank_lexical_candidates(lexical_candidates, top_k=self.top_k)
        query_embedding = vectors[0]
        model_id = str(getattr(self.embeddings, "model_id", "unknown") or "unknown")
        if model_id != "unknown":
            semantic_pool = await asyncio.to_thread(
                self.store.list_memory_embedding_candidates,
                self.owner_id,
                model_id,
                exclude_chat_id,
            )
        else:
            semantic_pool = []

        lexical_candidates, semantic_pool = _prefer_matching_facets_across_sources(
            lexical_candidates,
            semantic_pool,
            requested_facets=memory_facets,
        )

        semantic_scores, semantic_candidates = await asyncio.to_thread(
            _rank_semantic_candidates,
            semantic_pool,
            query_embedding=query_embedding,
            exclude_chat_id=exclude_chat_id,
            limit=self.prefilter_k,
        )
        semantic_entries = await asyncio.to_thread(
            self.store.list_memory_entries_by_ids,
            self.owner_id,
            [candidate.id for candidate in semantic_candidates],
        )

        candidate_by_id = {entry.id: entry for entry in lexical_candidates}
        candidate_by_id.update({entry.id: entry for entry in semantic_entries})
        lexical_rank = {entry.id: rank for rank, entry in enumerate(lexical_candidates)}
        semantic_rank = {entry.id: rank for rank, entry in enumerate(semantic_candidates)}
        scored: list[MemoryRetrieval] = []
        for entry in candidate_by_id.values():
            quality_weight = _memory_quality_weight(entry)
            semantic_similarity = semantic_scores.get(entry.id)
            semantic_score = (semantic_similarity or 0.0) * quality_weight
            lexical_index = lexical_rank.get(entry.id)
            lexical_score = (
                (0.05 / (lexical_index + 1)) * quality_weight if lexical_index is not None else 0.0
            )
            hybrid_score = semantic_score + lexical_score
            if lexical_index is None and semantic_score < self.min_score:
                continue
            mode: Literal["hybrid", "semantic", "lexical"] = (
                "hybrid"
                if lexical_index is not None and semantic_similarity is not None
                else "lexical" if lexical_index is not None else "semantic"
            )
            scored.append(
                MemoryRetrieval(
                    entry=entry,
                    score=hybrid_score,
                    mode=mode,
                    semantic_similarity=semantic_similarity,
                    semantic_rank=semantic_rank.get(entry.id),
                    lexical_rank=lexical_index,
                    quality_weight=quality_weight,
                    embedding_model=model_id if semantic_similarity is not None else None,
                )
            )
        scored.sort(
            key=lambda item: (item.score, item.entry.created_at, item.entry.id),
            reverse=True,
        )
        return scored[: max(0, min(self.top_k, 20))]

    async def get_recent_history(
        self,
        chat_id: int,
        limit: int = 6,
        *,
        conversation_scope: str | None = None,
    ) -> list[tuple[str, str, str]]:
        entries = await self.get_recent_history_entries(
            chat_id,
            limit=limit,
            conversation_scope=conversation_scope,
        )
        return [(entry.role, entry.content, entry.created_at.isoformat()) for entry in entries]

    async def get_recent_history_entries(
        self,
        chat_id: int,
        limit: int = 6,
        *,
        conversation_scope: str | None = None,
    ) -> list[MemoryEntry]:
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
        return entries


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _memory_quality_weight(entry: MemoryEntry | MemoryEmbeddingCandidate) -> float:
    weight = _coerce_confidence(entry.metadata.get("confidence"), default=0.5)
    weight *= _recency_weight(entry.created_at)
    if entry.metadata.get("contradiction_detected"):
        weight *= 0.75
    return weight


@dataclass(frozen=True)
class MemoryRetrieval:
    entry: MemoryEntry
    score: float
    mode: Literal["hybrid", "semantic", "lexical"]
    semantic_similarity: float | None
    semantic_rank: int | None
    lexical_rank: int | None
    quality_weight: float
    embedding_model: str | None

    def to_trace(self, *, rank: int) -> MemoryRetrievalTrace:
        return MemoryRetrievalTrace(
            memory_id=f"memory_entry:{self.entry.id}",
            rank=rank,
            score=max(0.0, min(2.0, self.score)),
            mode=self.mode,
            semantic_similarity=(
                max(0.0, min(1.0, self.semantic_similarity))
                if self.semantic_similarity is not None
                else None
            ),
            semantic_rank=(self.semantic_rank + 1 if self.semantic_rank is not None else None),
            lexical_rank=(self.lexical_rank + 1 if self.lexical_rank is not None else None),
            quality_weight=max(0.0, min(1.0, self.quality_weight)),
            embedding_model=self.embedding_model,
        )


def _rank_semantic_candidates(
    candidates: list[MemoryEmbeddingCandidate],
    *,
    query_embedding: list[float],
    exclude_chat_id: int | None,
    limit: int,
) -> tuple[dict[str, float], list[MemoryEmbeddingCandidate]]:
    scores: dict[str, float] = {}
    ranked: list[tuple[float, MemoryEmbeddingCandidate]] = []
    for entry in candidates:
        if not entry.embedding:
            continue
        if exclude_chat_id is not None and entry.metadata.get("chat_id") == exclude_chat_id:
            continue
        score = max(0.0, _cosine_similarity(query_embedding, entry.embedding))
        scores[entry.id] = score
        ranked.append((score, entry))
    ranked.sort(
        key=lambda item: (item[0], item[1].created_at, item[1].id),
        reverse=True,
    )
    return scores, [entry for _, entry in ranked[: max(0, limit)]]


def _rank_lexical_candidates(
    candidates: list[MemoryEntry],
    *,
    top_k: int,
) -> list[MemoryRetrieval]:
    scored = []
    for rank, entry in enumerate(candidates):
        quality_weight = _memory_quality_weight(entry)
        scored.append(
            MemoryRetrieval(
                entry=entry,
                score=(1.0 / (rank + 1)) * quality_weight,
                mode="lexical",
                semantic_similarity=None,
                semantic_rank=None,
                lexical_rank=rank,
                quality_weight=quality_weight,
                embedding_model=None,
            )
        )
    scored.sort(
        key=lambda item: (item.score, item.entry.created_at, item.entry.id),
        reverse=True,
    )
    return scored[: max(0, min(top_k, 20))]


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
        if (
            current.subject == other.subject
            and current.predicate == other.predicate
            and current.negated != other.negated
        ):
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


def _entry_matches_facets(
    entry: MemoryEntry | MemoryEmbeddingCandidate,
    requested: set[str],
) -> bool:
    entry_facets = set(_coerce_str_list((entry.metadata or {}).get("memory_facets")))
    return bool(entry_facets & requested)


def _prefer_matching_facets[T: (MemoryEntry, MemoryEmbeddingCandidate)](
    entries: list[T],
    *,
    requested_facets: list[str] | None,
) -> list[T]:
    requested = set(_coerce_str_list(requested_facets))
    if not requested:
        return entries
    matches = [entry for entry in entries if _entry_matches_facets(entry, requested)]
    return matches or entries


def _prefer_matching_facets_across_sources(
    lexical_entries: list[MemoryEntry],
    semantic_entries: list[MemoryEmbeddingCandidate],
    *,
    requested_facets: list[str] | None,
) -> tuple[list[MemoryEntry], list[MemoryEmbeddingCandidate]]:
    requested = set(_coerce_str_list(requested_facets))
    if not requested:
        return lexical_entries, semantic_entries
    if not any(
        _entry_matches_facets(entry, requested) for entry in [*lexical_entries, *semantic_entries]
    ):
        return lexical_entries, semantic_entries
    return (
        [entry for entry in lexical_entries if _entry_matches_facets(entry, requested)],
        [entry for entry in semantic_entries if _entry_matches_facets(entry, requested)],
    )


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
