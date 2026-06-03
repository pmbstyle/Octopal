from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from octopal.runtime.capability_outcomes import extract_capability_outcome

_COMPACTION_META_KEY = "__octopal_compaction__"
_MAX_RENDER_CHARS = 32_000
_MAX_CONTAINER_ITEMS = 48
_MAX_DEPTH = 8
_MAX_STRING_CHARS = 16_000
_CONTENT_HEAVY_MAX_RENDER_CHARS = 64_000
_CONTENT_HEAVY_MAX_CONTAINER_ITEMS = 96
_CONTENT_HEAVY_MAX_STRING_CHARS = 32_000
_SKILL_GUIDANCE_MAX_RENDER_CHARS = 200_000
_SKILL_GUIDANCE_MAX_STRING_CHARS = 200_000
_CONTENT_HEAVY_TOOL_NAMES = {
    "browser_extract",
    "markdown_new_fetch",
    "web_fetch",
}
_CONTENT_HEAVY_TOOL_TOKENS = (
    "fetch",
    "extract",
    "read",
    "download",
    "transcript",
    "thread",
    "message",
    "comment",
    "post",
    "article",
    "document",
    "email",
)
_CONTENT_HEAVY_TOOL_ACTION_PREFIXES = (
    "get_",
    "list_",
    "search_",
    "batch_get_",
)
_RAW_TEXT_TOOL_NAMES = {
    "fs_read",
    "manage_canon",
    "search_canon",
}
_RAW_TEXT_FIELDS_BY_TOOL_NAME = {
    "drive_read_text_file": {"content"},
}
_PATH_KEY_RE = re.compile(r"(?:^|_)(?:path|paths|file|files|url|urls)$", re.IGNORECASE)
_MAX_PATH_HINTS = 6


@dataclass(frozen=True)
class RenderedToolResult:
    text: str
    was_compacted: bool


@dataclass(frozen=True)
class ToolRenderBudget:
    max_chars: int
    max_container_items: int
    max_depth: int
    max_string_chars: int


_DEFAULT_BUDGET = ToolRenderBudget(
    max_chars=_MAX_RENDER_CHARS,
    max_container_items=_MAX_CONTAINER_ITEMS,
    max_depth=_MAX_DEPTH,
    max_string_chars=_MAX_STRING_CHARS,
)
_CONTENT_HEAVY_BUDGET = ToolRenderBudget(
    max_chars=_CONTENT_HEAVY_MAX_RENDER_CHARS,
    max_container_items=_CONTENT_HEAVY_MAX_CONTAINER_ITEMS,
    max_depth=_MAX_DEPTH,
    max_string_chars=_CONTENT_HEAVY_MAX_STRING_CHARS,
)
_SKILL_GUIDANCE_BUDGET = ToolRenderBudget(
    max_chars=_SKILL_GUIDANCE_MAX_RENDER_CHARS,
    max_container_items=_MAX_CONTAINER_ITEMS,
    max_depth=_MAX_DEPTH,
    max_string_chars=_SKILL_GUIDANCE_MAX_STRING_CHARS,
)
_EXACT_TOOL_BUDGET_OVERRIDES: dict[str, ToolRenderBudget] = {
    "browser_extract": _CONTENT_HEAVY_BUDGET,
    "markdown_new_fetch": _CONTENT_HEAVY_BUDGET,
    "use_skill": _SKILL_GUIDANCE_BUDGET,
    "web_fetch": _CONTENT_HEAVY_BUDGET,
}


def render_tool_result_for_llm(
    result: Any,
    *,
    tool_name: str | None = None,
    max_chars: int | None = None,
) -> RenderedToolResult:
    budget = _budget_for_tool(tool_name, max_chars=max_chars)
    if isinstance(result, str) and _should_preserve_raw_text(tool_name):
        return _render_raw_text_result(result, max_chars=budget.max_chars)
    compacted, was_compacted = _compact_tool_value(
        result,
        depth=0,
        budget=budget,
        raw_text_field_names=_raw_text_field_names_for_tool(tool_name),
    )
    parsed_json_text = isinstance(result, str) and not isinstance(compacted, str)
    if isinstance(compacted, str):
        rendered = compacted.strip()
    else:
        rendered = json.dumps(compacted, ensure_ascii=False, default=str, separators=(",", ":"))

    final_text = rendered.strip()
    if not final_text:
        return RenderedToolResult(text="", was_compacted=was_compacted)

    summary_prefix = (
        "" if parsed_json_text else _build_summary_prefix(result, was_compacted=was_compacted)
    )
    if summary_prefix:
        final_text = f"{summary_prefix}\n{final_text}"

    if len(final_text) <= budget.max_chars:
        return RenderedToolResult(text=final_text, was_compacted=was_compacted)

    omitted = len(final_text) - budget.max_chars
    truncated = final_text[: max(0, budget.max_chars - 32)].rstrip()
    suffix = f"... [truncated {omitted} chars]"
    return RenderedToolResult(text=f"{truncated}{suffix}", was_compacted=True)


def _budget_for_tool(tool_name: str | None, *, max_chars: int | None) -> ToolRenderBudget:
    normalized_name = str(tool_name or "").strip().lower()
    base_budget = _EXACT_TOOL_BUDGET_OVERRIDES.get(normalized_name)
    if base_budget is None:
        if normalized_name.startswith("skill_"):
            base_budget = _SKILL_GUIDANCE_BUDGET
        elif _is_content_heavy_tool_name(normalized_name):
            base_budget = _CONTENT_HEAVY_BUDGET
        else:
            base_budget = _DEFAULT_BUDGET
    if max_chars is None:
        return base_budget
    return ToolRenderBudget(
        max_chars=max_chars,
        max_container_items=base_budget.max_container_items,
        max_depth=base_budget.max_depth,
        max_string_chars=base_budget.max_string_chars,
    )


def _should_preserve_raw_text(tool_name: str | None) -> bool:
    normalized_name = str(tool_name or "").strip().lower()
    return normalized_name in _RAW_TEXT_TOOL_NAMES


def _is_content_heavy_tool_name(normalized_name: str) -> bool:
    if not normalized_name:
        return False
    if normalized_name in _CONTENT_HEAVY_TOOL_NAMES:
        return True

    candidate = normalized_name
    if candidate.startswith("mcp_"):
        _, _, candidate = candidate.partition("_")
        if "_" in candidate:
            candidate = candidate.split("_", 1)[1]

    if candidate.startswith(_CONTENT_HEAVY_TOOL_ACTION_PREFIXES):
        return any(token in candidate for token in _CONTENT_HEAVY_TOOL_TOKENS)

    return False


def _raw_text_field_names_for_tool(tool_name: str | None) -> frozenset[str]:
    normalized_name = str(tool_name or "").strip().lower()
    field_names = set(_RAW_TEXT_FIELDS_BY_TOOL_NAME.get(normalized_name, ()))
    if normalized_name == "use_skill" or normalized_name.startswith("skill_"):
        field_names.add("guidance")
    return frozenset(field_names)


def _render_raw_text_result(value: str, *, max_chars: int) -> RenderedToolResult:
    compacted, was_compacted = _preserve_raw_text(value, max_chars=max_chars)
    return RenderedToolResult(text=compacted, was_compacted=was_compacted)


def _preserve_raw_text(value: str, *, max_chars: int) -> tuple[str, bool]:
    final_text = value.strip()
    if not final_text:
        return "", False
    if len(final_text) <= max_chars:
        return final_text, False
    omitted = len(final_text) - max_chars
    truncated = final_text[: max(0, max_chars - 32)].rstrip()
    suffix = f"... [truncated {omitted} chars]"
    return f"{truncated}{suffix}", True


def _build_summary_prefix(value: Any, *, was_compacted: bool) -> str:
    summary_parts: list[str] = []

    capability_outcome = extract_capability_outcome(value)
    if capability_outcome:
        kind = _summary_token(capability_outcome.get("kind"))
        next_action = _summary_text(capability_outcome.get("next_action"), limit=160)
        if next_action:
            summary_parts.append(f"[capability_outcome kind={kind} next_action={next_action}]")
        else:
            summary_parts.append(f"[capability_outcome kind={kind}]")

    if isinstance(value, dict):
        keys = [str(key) for key in value]
        preview_keys = ", ".join(keys[:10]) if keys else "(none)"
        summary_parts.append(
            f"[tool_result_summary type=dict keys={len(keys)} top_keys={preview_keys}]"
        )
    elif isinstance(value, list):
        summary_parts.append(f"[tool_result_summary type=list items={len(value)}]")
    elif isinstance(value, tuple):
        summary_parts.append(f"[tool_result_summary type=tuple items={len(value)}]")

    path_hints = _collect_path_hints(value)
    if path_hints:
        summary_parts.append("[tool_result_paths " + ", ".join(path_hints) + "]")

    if was_compacted and summary_parts:
        summary_parts.append("[tool_result_compacted=true]")

    return "\n".join(summary_parts)


def _summary_token(value: Any) -> str:
    token = str(value or "").strip()
    return re.sub(r"[^a-zA-Z0-9_.:-]+", "_", token)[:80] or "unknown"


def _summary_text(value: Any, *, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _compact_tool_value(
    value: Any,
    *,
    depth: int,
    budget: ToolRenderBudget,
    raw_text_field_names: frozenset[str] = frozenset(),
    parent_key: str | None = None,
) -> tuple[Any, bool]:
    if depth >= budget.max_depth:
        return _depth_marker(value), True

    if isinstance(value, str):
        if parent_key and parent_key.strip().lower() in raw_text_field_names:
            return _preserve_raw_text(value, max_chars=budget.max_string_chars)
        stripped = value.strip()
        if not stripped:
            return "", False
        parsed = _parse_json_like_string(stripped)
        if parsed is not None:
            compacted, changed = _compact_tool_value(
                parsed,
                depth=depth + 1,
                budget=budget,
                raw_text_field_names=raw_text_field_names,
            )
            return compacted, changed
        if len(stripped) <= budget.max_string_chars:
            return stripped, False
        return _truncate_string(stripped, max_chars=budget.max_string_chars), True

    if isinstance(value, dict):
        compacted_items: dict[str, Any] = {}
        changed = False
        items = list(value.items())
        for key, raw_item in items[: budget.max_container_items]:
            compacted_item, item_changed = _compact_tool_value(
                raw_item,
                depth=depth + 1,
                budget=budget,
                raw_text_field_names=raw_text_field_names,
                parent_key=str(key),
            )
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
        for item in sequence[: budget.max_container_items]:
            compacted_item, item_changed = _compact_tool_value(
                item,
                depth=depth + 1,
                budget=budget,
                raw_text_field_names=raw_text_field_names,
                parent_key=parent_key,
            )
            compacted_items.append(compacted_item)
            changed = changed or item_changed
        omitted = len(sequence) - len(compacted_items)
        if omitted > 0:
            changed = True
            compacted_items.append(f"... [{omitted} more {type(value).__name__} items omitted]")
        return compacted_items, changed

    return value, False


def _parse_json_like_string(value: str) -> Any | None:
    if not value or value[0] not in "{[":
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def _collect_path_hints(value: Any) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()

    def _walk(current: Any, parent_key: str | None = None, depth: int = 0) -> None:
        if len(found) >= _MAX_PATH_HINTS or depth > 3:
            return
        if isinstance(current, dict):
            for key, item in current.items():
                _walk(item, str(key), depth + 1)
            return
        if isinstance(current, list | tuple | set):
            for item in list(current)[:_MAX_PATH_HINTS]:
                _walk(item, parent_key, depth + 1)
            return
        if not isinstance(current, str):
            return
        candidate = current.strip()
        if not candidate or candidate in seen:
            return
        if _looks_like_path_hint(candidate, parent_key=parent_key):
            seen.add(candidate)
            found.append(candidate)

    _walk(value)
    return found


def _looks_like_path_hint(value: str, *, parent_key: str | None) -> bool:
    key = str(parent_key or "").strip()
    if key and _PATH_KEY_RE.search(key):
        return True
    if value.startswith(("http://", "https://", "/", "./", "../")):
        return True
    if "\\" in value or "/" in value:
        return True
    return bool(re.search(r"\.[A-Za-z0-9]{1,8}$", value))


def _truncate_string(value: str, *, max_chars: int) -> str:
    omitted = len(value) - max_chars
    preview = value[: max_chars - 32].rstrip()
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
