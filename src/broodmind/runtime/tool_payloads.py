from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

_COMPACTION_META_KEY = "__broodmind_compaction__"
_MAX_RENDER_CHARS = 4_000
_MAX_CONTAINER_ITEMS = 24
_MAX_DEPTH = 6
_MAX_STRING_CHARS = 1_200


@dataclass(frozen=True)
class RenderedToolResult:
    text: str
    was_compacted: bool


def render_tool_result_for_llm(
    result: Any,
    *,
    max_chars: int = _MAX_RENDER_CHARS,
) -> RenderedToolResult:
    compacted, was_compacted = _compact_tool_value(result, depth=0)
    if isinstance(compacted, str):
        rendered = compacted.strip()
    else:
        rendered = json.dumps(compacted, ensure_ascii=False, default=str)

    final_text = rendered.strip()
    if not final_text:
        return RenderedToolResult(text="", was_compacted=was_compacted)

    if len(final_text) <= max_chars:
        return RenderedToolResult(text=final_text, was_compacted=was_compacted)

    omitted = len(final_text) - max_chars
    truncated = final_text[: max(0, max_chars - 32)].rstrip()
    suffix = f"... [truncated {omitted} chars]"
    return RenderedToolResult(text=f"{truncated}{suffix}", was_compacted=True)


def _compact_tool_value(value: Any, *, depth: int) -> tuple[Any, bool]:
    if depth >= _MAX_DEPTH:
        return _depth_marker(value), True

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return "", False
        parsed = _parse_json_like_string(stripped)
        if parsed is not None:
            compacted, _changed = _compact_tool_value(parsed, depth=depth + 1)
            return compacted, True
        if len(stripped) <= _MAX_STRING_CHARS:
            return stripped, False
        return _truncate_string(stripped), True

    if isinstance(value, dict):
        compacted_items: dict[str, Any] = {}
        changed = False
        items = list(value.items())
        for key, raw_item in items[:_MAX_CONTAINER_ITEMS]:
            compacted_item, item_changed = _compact_tool_value(raw_item, depth=depth + 1)
            compacted_items[str(key)] = compacted_item
            changed = changed or item_changed
        omitted = len(items) - len(compacted_items)
        compacted: dict[str, Any] = {}
        if changed or omitted > 0:
            meta: dict[str, Any] = {"compacted": True}
            if omitted > 0:
                meta.update({"omitted_keys": omitted, "original_keys": len(items)})
            _attach_compaction_meta(compacted, meta)
        compacted.update(compacted_items)
        if omitted > 0:
            changed = True
        return compacted, changed

    if isinstance(value, list | tuple | set):
        sequence = list(value)
        compacted_items: list[Any] = []
        changed = False
        for item in sequence[:_MAX_CONTAINER_ITEMS]:
            compacted_item, item_changed = _compact_tool_value(item, depth=depth + 1)
            compacted_items.append(compacted_item)
            changed = changed or item_changed
        omitted = len(sequence) - len(compacted_items)
        if omitted > 0:
            changed = True
            compacted_items.append(
                f"... [{omitted} more {type(value).__name__} items omitted]"
            )
        return compacted_items, changed

    return value, False


def _parse_json_like_string(value: str) -> Any | None:
    if not value or value[0] not in "{[":
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def _truncate_string(value: str) -> str:
    omitted = len(value) - _MAX_STRING_CHARS
    preview = value[: _MAX_STRING_CHARS - 32].rstrip()
    return f"{preview}... [truncated {omitted} chars]"


def _depth_marker(value: Any) -> dict[str, Any]:
    return {
        _COMPACTION_META_KEY: {
            "reason": "max_depth_reached",
            "value_type": type(value).__name__,
        }
    }


def _attach_compaction_meta(target: dict[str, Any], meta: dict[str, Any]) -> None:
    key = _COMPACTION_META_KEY
    while key in target:
        key = f"_{key}"
    target[key] = meta
