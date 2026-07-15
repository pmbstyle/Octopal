from __future__ import annotations

import re
from collections.abc import Iterable

_INFLUENCE_ID_RE = re.compile(
    r"^(?:canon_event|memory_fact|memory_entry|octo_diary|operational_memory):"
    r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$"
)
MAX_MEMORY_INFLUENCE_IDS = 128


def normalize_memory_influence_ids(values: Iterable[object] | None) -> list[str]:
    """Return a bounded, content-free set of typed memory identifiers."""

    if values is None:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        candidate = str(value or "").strip()
        if not _INFLUENCE_ID_RE.fullmatch(candidate) or candidate in seen:
            continue
        seen.add(candidate)
        result.append(candidate)
        if len(result) >= MAX_MEMORY_INFLUENCE_IDS:
            break
    return result


def require_complete_memory_influence_ids(values: Iterable[object]) -> list[str]:
    unique_values = list(dict.fromkeys(str(value or "").strip() for value in values))
    normalized = normalize_memory_influence_ids(unique_values)
    if len(normalized) != len(unique_values):
        raise ValueError("memory influence manifest is invalid or exceeds its bound")
    return normalized
