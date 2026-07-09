from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from octopal.infrastructure.store.base import Store
from octopal.infrastructure.store.models import (
    MemoryEntry,
    MemoryFactRecord,
    MemoryFactSourceRecord,
)
from octopal.runtime.memory.service import infer_memory_facets
from octopal.utils import utc_now

_ASSERTION_RE = re.compile(
    r"^\s*(?P<subject>.+?)\s+is\s+(?P<neg>not\s+)?(?P<predicate>.+?)\s*[.!?]?\s*$",
    re.IGNORECASE,
)
_TOKEN_RE = re.compile(r"[a-z0-9_]{3,}")
_SUPPORTED_CANON_FACT_FILES = {"facts.md", "decisions.md", "failures.md"}
_LOW_SIGNAL_SUBJECTS = {
    "this",
    "that",
    "it",
    "they",
    "these",
    "those",
    "here",
    "there",
}


@dataclass
class FactsService:
    store: Store
    owner_id: str = "default"

    def record_candidate_from_memory(self, entry: MemoryEntry) -> MemoryFactRecord | None:
        metadata = entry.metadata or {}
        if not metadata.get("fact_candidate"):
            return None

        subject = _normalize_component(metadata.get("fact_subject_hint"))
        value_text = _normalize_component(metadata.get("fact_value_hint"))
        if not subject or not value_text:
            extracted = _extract_assertion(entry.content)
            if extracted is None:
                return None
            subject = subject or extracted["subject"]
            value_text = value_text or extracted["value_text"]

        now = utc_now()
        record = MemoryFactRecord(
            id=_fact_id(self.owner_id, "memory", entry.id, subject, "is", value_text, "candidate"),
            owner_id=self.owner_id,
            subject=subject,
            key="is",
            value_text=value_text,
            value_json=None,
            fact_type="assertion",
            confidence=float(metadata.get("confidence", 0.5) or 0.5),
            status="candidate",
            valid_from=entry.created_at,
            valid_to=None,
            facets=sorted(set(_clean_facets(metadata.get("memory_facets")))),
            source_kind="memory",
            source_ref=entry.id,
            created_at=entry.created_at,
            updated_at=now,
        )
        self.store.upsert_memory_fact(record)
        self.store.add_memory_fact_source(
            MemoryFactSourceRecord(
                fact_id=record.id,
                memory_entry_uuid=entry.id,
                canon_filename=None,
                source_note="memory_candidate",
                created_at=now,
            )
        )
        return record

    def sync_verified_facts_from_canon(self, filename: str, content: str) -> dict[str, int]:
        parsed = self._parse_canon_facts(filename, content)
        parsed_by_id = {record.id: record for record in parsed}
        existing_active = self.store.list_memory_facts(
            self.owner_id,
            limit=500,
            status="active",
            source_kind="canon",
            source_ref=filename,
        )

        superseded = 0
        now = utc_now()
        existing_ids = {record.id for record in existing_active}
        for existing in existing_active:
            if existing.id in parsed_by_id:
                continue
            self.store.invalidate_memory_fact(existing.id, now, status="superseded")
            superseded += 1

        for record in parsed:
            self.store.upsert_memory_fact(record)
            if record.id not in existing_ids:
                self.store.add_memory_fact_source(
                    MemoryFactSourceRecord(
                        fact_id=record.id,
                        memory_entry_uuid=None,
                        canon_filename=filename,
                        source_note="canon_verified",
                        created_at=now,
                    )
                )

        return {"active": len(parsed), "superseded": superseded}

    def prune_unsupported_canon_facts(self) -> int:
        active = self.store.list_memory_facts(
            self.owner_id,
            limit=500,
            status="active",
            source_kind="canon",
        )
        now = utc_now()
        pruned = 0
        for record in active:
            if (record.source_ref or "") in _SUPPORTED_CANON_FACT_FILES:
                continue
            self.store.invalidate_memory_fact(record.id, now, status="superseded")
            pruned += 1
        return pruned

    def get_relevant_facts(
        self,
        query: str,
        *,
        memory_facets: list[str] | None = None,
        limit: int = 3,
    ) -> list[str]:
        tokens = _tokenize(query)
        if not tokens:
            return []

        active = self.store.list_memory_facts(
            self.owner_id,
            limit=max(limit * 12, 50),
            status="active",
        )
        if not active:
            return []

        requested_facets = set(_clean_facets(memory_facets))
        filtered = (
            [record for record in active if set(record.facets) & requested_facets]
            if requested_facets
            else []
        )
        candidates = filtered or active

        scored: list[tuple[tuple[int, float, str], MemoryFactRecord]] = []
        for record in candidates:
            haystack = " ".join(
                [
                    record.subject,
                    record.key,
                    record.value_text,
                    record.fact_type,
                    " ".join(record.facets),
                ]
            ).lower()
            overlap = sum(1 for token in tokens if token in haystack)
            if overlap <= 0:
                continue
            scored.append(
                ((overlap, float(record.confidence), record.updated_at.isoformat()), record)
            )

        scored.sort(key=lambda item: item[0], reverse=True)
        return [_format_fact(record) for _, record in scored[:limit]]

    def _parse_canon_facts(self, filename: str, content: str) -> list[MemoryFactRecord]:
        if filename not in _SUPPORTED_CANON_FACT_FILES:
            return []
        lines = [line.strip() for line in content.splitlines()]
        now = utc_now()
        records: list[MemoryFactRecord] = []
        for raw_line in lines:
            cleaned = _clean_canon_line(raw_line)
            if not cleaned:
                continue
            assertion = _extract_assertion(cleaned)
            if assertion is None:
                continue
            if not _should_accept_verified_assertion(cleaned, assertion):
                continue
            facets = set(_clean_facets(infer_memory_facets(cleaned)))
            facets.discard("fact_candidate")
            if filename == "decisions.md":
                facets.add("decision")
            elif filename == "failures.md":
                facets.add("problem")

            record = MemoryFactRecord(
                id=_fact_id(
                    self.owner_id,
                    "canon",
                    filename,
                    assertion["subject"],
                    "is_not" if assertion["negated"] else "is",
                    assertion["value_text"],
                    "active",
                ),
                owner_id=self.owner_id,
                subject=assertion["subject"],
                key="is_not" if assertion["negated"] else "is",
                value_text=assertion["value_text"],
                value_json=None,
                fact_type=filename.replace(".md", ""),
                confidence=0.95,
                status="active",
                valid_from=now,
                valid_to=None,
                facets=sorted(facets),
                source_kind="canon",
                source_ref=filename,
                created_at=now,
                updated_at=now,
            )
            records.append(record)
        return records


def _format_fact(record: MemoryFactRecord) -> str:
    source = f" ({record.source_ref})" if record.source_ref else ""
    return f"{record.subject} {record.key.replace('_', ' ')} {record.value_text}{source}"


def _fact_id(
    owner_id: str,
    source_kind: str,
    source_ref: str,
    subject: str,
    key: str,
    value_text: str,
    status: str,
) -> str:
    payload = "|".join([owner_id, source_kind, source_ref, subject, key, value_text, status])
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"fact_{digest[:24]}"


def _extract_assertion(value: str) -> dict[str, str | bool] | None:
    match = _ASSERTION_RE.match(value or "")
    if not match:
        return None
    subject = _normalize_component(match.group("subject"))
    predicate = _normalize_component(match.group("predicate"))
    if not subject or not predicate:
        return None
    return {
        "subject": subject,
        "value_text": predicate,
        "negated": bool(match.group("neg")),
    }


def _clean_canon_line(value: str) -> str:
    line = (value or "").strip()
    if not line or line.startswith("#"):
        return ""
    line = re.sub(r"^[-*]\s+", "", line)
    line = re.sub(r"^\d+[.)]\s+", "", line)
    line = line.replace("**", "").replace("__", "").replace("`", "")
    return line.strip()


def _normalize_component(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _clean_facets(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            result.append(text)
    return result


def _tokenize(value: str) -> set[str]:
    return {match.group(0) for match in _TOKEN_RE.finditer((value or "").lower())}


def _should_accept_verified_assertion(
    original_line: str,
    assertion: dict[str, str | bool],
) -> bool:
    line = _normalize_component(original_line)
    subject = str(assertion["subject"])
    predicate = str(assertion["value_text"])

    if "?" in line:
        return False
    if "open question" in line:
        return False
    if re.search(r"[.!?]\s+\S", line):
        return False
    if len(line) > 160:
        return False

    subject_tokens = _split_words(subject)
    predicate_tokens = _split_words(predicate)
    if not subject_tokens or not predicate_tokens:
        return False
    if len(subject_tokens) > 8 or len(predicate_tokens) > 16:
        return False
    if subject in _LOW_SIGNAL_SUBJECTS:
        return False
    if '"' in subject or "'" in subject:
        return False
    return '"' not in predicate


def _split_words(value: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", (value or "").lower())
