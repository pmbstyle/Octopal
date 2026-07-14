from __future__ import annotations

import asyncio
import hashlib
import json
import math
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import uuid4

import structlog
from pydantic import BaseModel, ConfigDict

from octopal.infrastructure.store.models import MemoryOrigin, MemoryTrustState
from octopal.utils import utc_now

if TYPE_CHECKING:
    from octopal.infrastructure.providers.embeddings import EmbeddingsProvider
    from octopal.infrastructure.store.base import Store
    from octopal.runtime.memory.facts import FactsService


logger = structlog.get_logger(__name__)

_MEMORY_ORIGINS = {
    "direct_user",
    "assistant_inference",
    "local_runtime_evidence",
    "worker",
    "connector",
    "mcp",
    "web",
    "document",
    "imported_canon",
}
_PROMOTABLE_TRUST_STATES = {"observed", "quarantined_candidate", "corroborated"}
_DEFAULT_CANON_CONTENT = {
    "facts.md": "# Facts\n\n",
    "decisions.md": "# Decisions\n\n",
    "failures.md": "# Failures\n\n",
}


class CanonProposal(BaseModel):
    """One provenance-bearing canon write with its current trust state."""

    model_config = ConfigDict(frozen=True)

    id: str
    filename: str
    mode: Literal["append", "overwrite"]
    content: str
    source_kind: MemoryOrigin
    source_ref: str | None = None
    trust_state: MemoryTrustState
    created_at: datetime
    updated_at: datetime

    def metadata_payload(self, *, reveal_source_ref: bool = False) -> dict[str, Any]:
        payload = {
            "id": self.id,
            "filename": self.filename,
            "mode": self.mode,
            "source_kind": self.source_kind,
            "source_ref_present": self.source_ref is not None,
            "source_ref_sha256": (
                hashlib.sha256(self.source_ref.encode("utf-8")).hexdigest()
                if self.source_ref is not None
                else None
            ),
            "trust_state": self.trust_state,
            "content_chars": len(self.content),
            "content_sha256": hashlib.sha256(self.content.encode("utf-8")).hexdigest(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }
        if reveal_source_ref:
            payload["source_ref"] = self.source_ref
        return payload


@dataclass(frozen=True)
class _CanonRebuild:
    content_by_filename: dict[str, str]
    provenance_by_filename: dict[str, list[dict[str, Any]]]
    managed_filenames: frozenset[str]


@dataclass
class CanonService:
    workspace_dir: Path
    store: Store
    embeddings: EmbeddingsProvider | None = None
    max_file_chars: int = 4000  # Guardrail for canon bloat
    max_event_chars: int = 16000
    facts: FactsService | None = None
    _event_lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)
    _cached_ledger_signature: tuple[int, int] | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _cached_rebuild: _CanonRebuild | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.canon_dir = self.workspace_dir / "memory" / "canon"
        self.canon_dir.mkdir(parents=True, exist_ok=True)
        self.events_file = self.canon_dir / "events.jsonl"
        # Ensure default files exist
        for filename, default_content in _DEFAULT_CANON_CONTENT.items():
            path = self.canon_dir / filename
            if not path.exists():
                path.write_text(default_content, encoding="utf-8")
        self._ensure_event_log_bootstrap()
        rebuilt = self._compact_from_events()
        if self.facts is not None:
            try:
                self.facts.prune_unsupported_canon_facts()
            except Exception:
                logger.exception("Failed to prune unsupported canon facts on startup")
            for filename in self.list_files():
                try:
                    self.facts.sync_verified_facts_from_canon(
                        filename,
                        self.read_canon(filename),
                        provenance=rebuilt.provenance_by_filename.get(filename),
                    )
                except Exception:
                    logger.exception("Failed to sync canon facts on startup", filename=filename)

    def _normalize_filename(self, filename: str) -> str:
        candidate = filename.strip()
        if not candidate:
            raise ValueError("filename is required")
        if not candidate.endswith(".md"):
            candidate += ".md"
        if (
            len(candidate) > 128
            or any(ord(char) < 32 for char in candidate)
            or "/" in candidate
            or "\\" in candidate
            or ".." in candidate
        ):
            raise ValueError("invalid filename")
        return candidate

    def read_canon(self, filename: str) -> str:
        """Reads a canonical memory file."""
        filename = self._normalize_filename(filename)
        rebuilt = self._compact_from_events()
        return rebuilt.content_by_filename.get(filename, "")

    async def write_canon(
        self,
        filename: str,
        content: str,
        mode: Literal["append", "overwrite"] = "append",
        *,
        source_kind: MemoryOrigin = "imported_canon",
        source_ref: str | None = None,
    ) -> str:
        """Append a provenance-bearing write; only trusted writes enter canonical context."""
        filename = self._normalize_filename(filename)
        if mode not in {"append", "overwrite"}:
            raise ValueError("mode must be append or overwrite")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("content is required")
        if len(content) > self.max_event_chars:
            raise ValueError(f"content exceeds the {self.max_event_chars}-character event limit")
        source_kind = _normalize_memory_origin(source_kind)
        source_ref = _normalize_source_ref(source_ref)
        proposal_id = f"canon_{uuid4().hex}"
        trust_state: MemoryTrustState = (
            "trusted" if source_kind == "imported_canon" else "quarantined_candidate"
        )
        event = {
            "event_id": proposal_id,
            "event_type": "write",
            "ts": utc_now().isoformat(),
            "filename": filename,
            "mode": mode,
            "content": content,
            "source_kind": source_kind,
            "source_ref": source_ref,
            "trust_state": trust_state,
        }
        rebuilt = await asyncio.to_thread(self._append_and_compact, event)
        new_content = rebuilt.content_by_filename.get(filename, "")

        await self._sync_rebuilt_file(filename, new_content, rebuilt)

        if trust_state != "trusted":
            return f"Quarantined canon proposal: {proposal_id}"

        if len(new_content) > self.max_file_chars:
            return f"WARNING: {filename} size ({len(new_content)} chars) exceeds limit ({self.max_file_chars}). Please summarize/compact it immediately."
        return "Success"

    def list_proposals(
        self,
        *,
        trust_state: MemoryTrustState | None = None,
        limit: int = 100,
    ) -> list[CanonProposal]:
        safe_limit = max(1, min(int(limit), 1000))
        with self._event_lock:
            proposals = self._proposal_records(self._read_events())
        if trust_state is not None:
            proposals = [item for item in proposals if item.trust_state == trust_state]
        proposals.sort(key=lambda item: item.updated_at, reverse=True)
        return proposals[:safe_limit]

    def get_proposal(self, proposal_id: str) -> CanonProposal | None:
        normalized = _normalize_proposal_id(proposal_id)
        with self._event_lock:
            for proposal in self._proposal_records(self._read_events()):
                if proposal.id == normalized:
                    return proposal
        return None

    async def promote_proposal(
        self,
        proposal_id: str,
        *,
        actor_ref: str = "operator_cli",
    ) -> CanonProposal:
        proposal = self.get_proposal(proposal_id)
        if proposal is None:
            raise ValueError("canon proposal not found")
        if proposal.trust_state not in _PROMOTABLE_TRUST_STATES:
            raise ValueError(f"canon proposal cannot be promoted from {proposal.trust_state}")
        return await self._transition_proposal(proposal, "trusted", actor_ref=actor_ref)

    async def deprecate_proposal(
        self,
        proposal_id: str,
        *,
        actor_ref: str = "operator_cli",
    ) -> CanonProposal:
        proposal = self.get_proposal(proposal_id)
        if proposal is None:
            raise ValueError("canon proposal not found")
        if proposal.trust_state == "deprecated":
            raise ValueError("canon proposal is already deprecated")
        return await self._transition_proposal(proposal, "deprecated", actor_ref=actor_ref)

    async def _transition_proposal(
        self,
        proposal: CanonProposal,
        trust_state: Literal["trusted", "deprecated"],
        *,
        actor_ref: str,
    ) -> CanonProposal:
        transition = {
            "event_id": f"canon_transition_{uuid4().hex}",
            "event_type": "trust_transition",
            "ts": utc_now().isoformat(),
            "target_event_id": proposal.id,
            "trust_state": trust_state,
            "actor": "operator_cli",
            "actor_ref": _normalize_source_ref(actor_ref),
        }
        rebuilt = await asyncio.to_thread(self._append_and_compact, transition)
        await self._sync_rebuilt_file(
            proposal.filename,
            rebuilt.content_by_filename.get(proposal.filename, ""),
            rebuilt,
        )
        updated = self.get_proposal(proposal.id)
        if updated is None:
            raise RuntimeError("canon proposal disappeared after transition")
        return updated

    async def _sync_rebuilt_file(
        self,
        filename: str,
        content: str,
        rebuilt: _CanonRebuild,
    ) -> None:
        if self.facts is not None:
            try:
                await asyncio.to_thread(
                    self.facts.sync_verified_facts_from_canon,
                    filename,
                    content,
                    provenance=rebuilt.provenance_by_filename.get(filename),
                )
            except Exception:
                logger.exception("Failed to sync canon facts", filename=filename)

        if self.embeddings:
            asyncio.create_task(self.index_canon(filename))

    async def index_canon(self, filename: str) -> None:
        """Chunks and embeds a canonical file."""
        if not self.embeddings:
            return

        content = self.read_canon(filename)
        if not content.strip():
            await asyncio.to_thread(self.store.clear_canon_embeddings, filename)
            return

        # Simple chunking by paragraph/headers
        chunks = [c.strip() for c in content.split("\n\n") if c.strip()]
        if not chunks:
            return

        try:
            vectors = await self.embeddings.embed(chunks)
            await asyncio.to_thread(self.store.clear_canon_embeddings, filename)

            for i, (chunk, vector) in enumerate(zip(chunks, vectors, strict=False)):
                await asyncio.to_thread(
                    self.store.add_canon_embedding,
                    filename=filename,
                    chunk_index=i,
                    content=chunk,
                    model="openai-text-embedding-3-small",
                    vector=vector,
                )
            logger.info("Canon file indexed", filename=filename, chunks=len(chunks))
        except Exception:
            logger.exception("Failed to index canon file", filename=filename)

    async def search_canon(self, query: str, top_k: int = 3) -> list[str]:
        """Searches across all indexed canon files."""
        if not self.embeddings:
            return []

        try:
            query_vectors = await self.embeddings.embed([query])
            if not query_vectors:
                return []
            query_vector = query_vectors[0]

            all_entries = await asyncio.to_thread(self.store.list_canon_embeddings)
            scored: list[tuple[float, str]] = []
            trusted_content: dict[str, str] = {}

            for entry in all_entries:
                filename = str(entry.get("filename") or "").strip()
                content = str(entry.get("content") or "")
                if not filename or not content:
                    continue
                if filename not in trusted_content:
                    trusted_content[filename] = self.read_canon(filename)
                if content not in trusted_content[filename]:
                    continue
                score = _cosine_similarity(query_vector, entry["vector"])
                if score > 0.3:  # Minimum threshold
                    scored.append((score, content))

            scored.sort(key=lambda x: x[0], reverse=True)
            return [content for _, content in scored[:top_k]]
        except Exception:
            logger.exception("Canon search failed")
            return []

    def get_tier1_context(self) -> str:
        """Returns the high-priority canonical context (decisions and failures)."""
        context, _ = self.get_tier1_context_with_ids()
        return context

    def get_tier1_context_with_ids(self) -> tuple[str, list[str]]:
        """Return tier-one canon and the trusted event ids represented in it."""

        rebuilt = self._compact_from_events()
        raw_decisions = rebuilt.content_by_filename.get("decisions.md", "")
        raw_failures = rebuilt.content_by_filename.get("failures.md", "")
        decisions = raw_decisions.strip()
        failures = raw_failures.strip()
        decisions_offset = len(raw_decisions) - len(raw_decisions.lstrip())
        failures_offset = len(raw_failures) - len(raw_failures.lstrip())

        context_parts = []
        selected_ids: list[str] = []
        if decisions and len(decisions) > len("# Decisions"):
            rendered, visible_start = _bound_canon_context_window(
                decisions,
                rebuilt.provenance_by_filename.get("decisions.md", []),
                content_offset=decisions_offset,
            )
            context_parts.append(f"<canon_decisions>\n{rendered}\n</canon_decisions>")
            selected_ids.extend(
                _visible_canon_event_ids(
                    rebuilt.provenance_by_filename.get("decisions.md", []),
                    visible_start=decisions_offset + visible_start,
                    visible_end=decisions_offset + len(decisions),
                )
            )

        if failures and len(failures) > len("# Failures"):
            rendered, visible_start = _bound_canon_context_window(
                failures,
                rebuilt.provenance_by_filename.get("failures.md", []),
                content_offset=failures_offset,
            )
            context_parts.append(f"<canon_failures>\n{rendered}\n</canon_failures>")
            selected_ids.extend(
                _visible_canon_event_ids(
                    rebuilt.provenance_by_filename.get("failures.md", []),
                    visible_start=failures_offset + visible_start,
                    visible_end=failures_offset + len(failures),
                )
            )

        return "\n\n".join(context_parts), list(dict.fromkeys(selected_ids))

    def list_files(self) -> list[str]:
        return sorted(p.name for p in self.canon_dir.glob("*.md"))

    def _ensure_event_log_bootstrap(self) -> None:
        if self.events_file.exists():
            return

        entries: list[dict[str, str]] = []
        for path in sorted(self.canon_dir.glob("*.md")):
            content = path.read_text(encoding="utf-8")
            if not content.strip():
                continue
            entries.append(
                {
                    "ts": utc_now().isoformat(),
                    "filename": path.name,
                    "mode": "overwrite",
                    "content": content,
                }
            )

        if not entries:
            self.events_file.write_text("", encoding="utf-8")
            return

        with self.events_file.open("w", encoding="utf-8") as f:
            for entry in entries:
                f.write(json.dumps(entry, ensure_ascii=True))
                f.write("\n")

    def _append_event(self, event: dict[str, Any]) -> None:
        with self.events_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=True) + "\n")

    def _append_and_compact(self, event: dict[str, Any]) -> _CanonRebuild:
        with self._event_lock:
            self._append_event(event)
            return self._compact_from_events()

    def _read_events(self) -> list[dict[str, Any]]:
        if not self.events_file.exists():
            return []

        events: list[dict[str, Any]] = []
        for raw in self.events_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(entry, dict):
                events.append(entry)
        return events

    def _proposal_records(self, events: list[dict[str, Any]]) -> list[CanonProposal]:
        transitions: dict[str, tuple[MemoryTrustState, datetime]] = {}
        for entry in events:
            if entry.get("event_type") != "trust_transition":
                continue
            target = str(entry.get("target_event_id") or "").strip()
            trust_state = str(entry.get("trust_state") or "").strip()
            ts = _parse_event_ts(entry.get("ts"))
            if target and trust_state in {"trusted", "deprecated"} and ts is not None:
                transitions[target] = (cast(MemoryTrustState, trust_state), ts)

        proposals: list[CanonProposal] = []
        for entry in events:
            event_id = str(entry.get("event_id") or "").strip()
            if not event_id or entry.get("event_type") != "write":
                continue
            filename = _event_filename(entry)
            mode = str(entry.get("mode") or "").strip()
            source_kind = str(entry.get("source_kind") or "").strip()
            initial_trust = str(entry.get("trust_state") or "").strip()
            created_at = _parse_event_ts(entry.get("ts"))
            if (
                filename is None
                or mode not in {"append", "overwrite"}
                or source_kind not in _MEMORY_ORIGINS
                or initial_trust
                not in {
                    "observed",
                    "quarantined_candidate",
                    "corroborated",
                    "trusted",
                    "deprecated",
                }
                or created_at is None
                or (initial_trust == "trusted" and source_kind != "imported_canon")
            ):
                continue
            effective_trust, updated_at = transitions.get(
                event_id,
                (cast(MemoryTrustState, initial_trust), created_at),
            )
            proposals.append(
                CanonProposal(
                    id=event_id,
                    filename=filename,
                    mode=cast(Literal["append", "overwrite"], mode),
                    content=str(entry.get("content") or ""),
                    source_kind=cast(MemoryOrigin, source_kind),
                    source_ref=_optional_string(entry.get("source_ref")),
                    trust_state=effective_trust,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )
        return proposals

    def _compact_from_events(self) -> _CanonRebuild:
        with self._event_lock:
            ledger_signature = self._ledger_signature()
            if (
                self._cached_rebuild is not None
                and self._cached_ledger_signature == ledger_signature
            ):
                self._reconcile_materialized_files(self._cached_rebuild)
                return self._cached_rebuild

            events = self._read_events()
            effective_states = {
                item.id: item.trust_state for item in self._proposal_records(events)
            }
            activation_positions: dict[str, int] = {}
            for position, entry in enumerate(events):
                event_type = str(entry.get("event_type") or "write")
                if event_type == "write":
                    event_id = str(entry.get("event_id") or "").strip()
                    if event_id and entry.get("trust_state") == "trusted":
                        activation_positions[event_id] = position
                    continue
                if event_type != "trust_transition":
                    continue
                target = str(entry.get("target_event_id") or "").strip()
                transition_state = str(entry.get("trust_state") or "").strip()
                if transition_state == "trusted":
                    activation_positions[target] = position
                elif transition_state == "deprecated":
                    activation_positions.pop(target, None)

            state: dict[str, str] = {}
            provenance: dict[str, list[dict[str, Any]]] = {}
            known_filenames: set[str] = set(_DEFAULT_CANON_CONTENT)
            trusted_writes: list[tuple[int, dict[str, Any], str]] = []
            for position, entry in enumerate(events):
                event_type = str(entry.get("event_type") or "write")
                if event_type != "write":
                    continue
                filename = _event_filename(entry)
                if filename is None:
                    continue
                known_filenames.add(filename)
                event_id = str(entry.get("event_id") or "").strip()
                if not event_id and _is_legacy_trusted_write(entry):
                    event_id = _legacy_canon_event_id(entry, position)
                trust_state = effective_states.get(
                    event_id,
                    "trusted" if _is_legacy_trusted_write(entry) else "observed",
                )
                if trust_state != "trusted":
                    continue
                activation_position = activation_positions.get(event_id, position)
                trusted_writes.append((activation_position, entry, event_id))

            trusted_writes.sort(key=lambda item: item[0])
            for _activation_position, entry, event_id in trusted_writes:
                filename = _event_filename(entry)
                if filename is None:
                    continue
                mode = str(entry.get("mode", "append"))
                if mode not in {"append", "overwrite"}:
                    continue
                content = str(entry.get("content", ""))
                source_kind = str(entry.get("source_kind") or "imported_canon")
                source_ref = _optional_string(entry.get("source_ref"))
                event_provenance: dict[str, Any] = {
                    "event_id": event_id or None,
                    "source_kind": (
                        source_kind if source_kind in _MEMORY_ORIGINS else "imported_canon"
                    ),
                    "source_ref": source_ref,
                    "content": content,
                }

                if mode == "overwrite":
                    state[filename] = content
                    event_provenance["start_char"] = 0
                    event_provenance["end_char"] = len(content)
                    provenance[filename] = [event_provenance]
                    continue

                current = state.get(filename, "")
                if current and not current.endswith("\n"):
                    current += "\n"
                event_provenance["start_char"] = len(current)
                event_provenance["end_char"] = len(current) + len(content)
                state[filename] = current + content
                provenance.setdefault(filename, []).append(event_provenance)

            for filename, default_content in _DEFAULT_CANON_CONTENT.items():
                if filename not in state:
                    state[filename] = default_content
                    provenance[filename] = []

            rebuilt = _CanonRebuild(
                content_by_filename=state,
                provenance_by_filename=provenance,
                managed_filenames=frozenset(known_filenames),
            )
            self._cached_ledger_signature = ledger_signature
            self._cached_rebuild = rebuilt
            self._reconcile_materialized_files(rebuilt)
            return rebuilt

    def _ledger_signature(self) -> tuple[int, int]:
        try:
            stat = self.events_file.stat()
        except FileNotFoundError:
            return (0, 0)
        return (stat.st_mtime_ns, stat.st_size)

    def _reconcile_materialized_files(self, rebuilt: _CanonRebuild) -> None:
        for filename in rebuilt.managed_filenames:
            path = self.canon_dir / filename
            expected = rebuilt.content_by_filename.get(filename)
            if expected is None:
                if path.exists():
                    path.unlink()
                continue
            try:
                current = path.read_text(encoding="utf-8")
            except (FileNotFoundError, OSError, UnicodeError):
                current = None
            if current != expected:
                path.write_text(expected, encoding="utf-8")


def _truncate_canon_context(content: str, *, max_chars: int = 2000) -> tuple[str, int]:
    if len(content) <= max_chars:
        return content, 0
    cut_idx = len(content) - max_chars
    safe_cut = content.find("\n", cut_idx)
    if safe_cut != -1:
        return "...(older entries omitted)\n" + content[safe_cut + 1 :], safe_cut + 1
    return "...(older entries omitted)\n" + content[-max_chars:], len(content) - max_chars


def _bound_canon_context_window(
    content: str,
    provenance: list[dict[str, Any]],
    *,
    content_offset: int,
    max_events: int = 32,
) -> tuple[str, int]:
    rendered, visible_start = _truncate_canon_context(content)
    absolute_start = content_offset + visible_start
    absolute_end = content_offset + len(content)
    visible_events = [
        event
        for event in provenance
        if _canon_event_overlaps(event, visible_start=absolute_start, visible_end=absolute_end)
    ]
    if len(visible_events) <= max_events:
        return rendered, visible_start
    retained_start = int(visible_events[-max_events]["start_char"])
    bounded_start = max(absolute_start, retained_start)
    relative_start = max(0, bounded_start - content_offset)
    return "...(older entries omitted)\n" + content[relative_start:], relative_start


def _visible_canon_event_ids(
    provenance: list[dict[str, Any]],
    *,
    visible_start: int,
    visible_end: int,
) -> list[str]:
    selected: list[str] = []
    for event in provenance:
        event_id = str(event.get("event_id") or "").strip()
        if event_id and _canon_event_overlaps(
            event, visible_start=visible_start, visible_end=visible_end
        ):
            selected.append(f"canon_event:{event_id}")
    return selected


def _canon_event_overlaps(event: dict[str, Any], *, visible_start: int, visible_end: int) -> bool:
    start = event.get("start_char")
    end = event.get("end_char")
    return bool(
        isinstance(start, int)
        and not isinstance(start, bool)
        and isinstance(end, int)
        and not isinstance(end, bool)
        and end > visible_start
        and start < visible_end
    )


def _normalize_memory_origin(value: object) -> MemoryOrigin:
    normalized = str(value or "").strip().lower()
    if normalized not in _MEMORY_ORIGINS:
        raise ValueError("invalid canon source kind")
    return cast(MemoryOrigin, normalized)


def _normalize_source_ref(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if len(normalized) > 256 or any(ord(char) < 32 for char in normalized):
        raise ValueError("source reference must be one printable line up to 256 characters")
    return normalized


def _normalize_proposal_id(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized.startswith("canon_") or len(normalized) > 96:
        raise ValueError("invalid canon proposal id")
    if not all(char.isalnum() or char == "_" for char in normalized):
        raise ValueError("invalid canon proposal id")
    return normalized


def _event_filename(entry: dict[str, Any]) -> str | None:
    filename = str(entry.get("filename") or "").strip()
    if (
        not filename
        or len(filename) > 128
        or any(ord(char) < 32 for char in filename)
        or not filename.endswith(".md")
        or "/" in filename
        or "\\" in filename
        or ".." in filename
    ):
        return None
    return filename


def _is_legacy_trusted_write(entry: dict[str, Any]) -> bool:
    return (
        not entry.get("event_id")
        and "event_type" not in entry
        and "source_kind" not in entry
        and "trust_state" not in entry
    )


def _legacy_canon_event_id(entry: dict[str, Any], position: int) -> str:
    payload = json.dumps(
        {"position": position, "event": entry},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return "legacy_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_event_ts(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _optional_string(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
