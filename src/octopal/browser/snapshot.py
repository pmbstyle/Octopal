from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import TypedDict

import structlog
from playwright.async_api import Page

logger = structlog.get_logger(__name__)


class ElementRef(TypedDict):
    role: str
    name: str | None
    nth: int


class SnapshotResult(TypedDict):
    snapshot: str
    refs: dict[str, ElementRef]


INTERACTIVE_ROLES = {
    "button",
    "link",
    "textbox",
    "checkbox",
    "radio",
    "combobox",
    "listbox",
    "menuitem",
    "option",
    "searchbox",
    "slider",
    "spinbutton",
    "switch",
    "tab",
    "treeitem",
}


def _get_indent_level(line: str) -> int:
    match = re.match(r"^(\s*)", line)
    return len(match.group(1)) // 2 if match else 0


class _FallbackSnapshotParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.refs: dict[str, ElementRef] = {}
        self.lines: list[str] = []
        self._ref_counter = 1
        self._text_parts: list[str] = []
        self._stack: list[dict[str, str | None]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = {key.lower(): value for key, value in attrs}
        normalized_role = self._normalized_role(tag, attrs_map)
        label = self._preferred_label(tag, attrs_map)
        entry = {"tag": tag.lower(), "role": normalized_role, "label": label, "text": ""}
        self._stack.append(entry)

        if normalized_role is None or label:
            return
        if entry["tag"] in {"input", "textarea", "select"}:
            self._record_ref(normalized_role, label)

    def handle_endtag(self, tag: str) -> None:
        if not self._stack:
            return
        entry = self._stack.pop()
        if str(entry.get("tag")) != tag.lower():
            return

        role = entry.get("role")
        if role is None:
            return

        label = str(entry.get("label") or "").strip()
        if not label:
            label = str(entry.get("text") or "").strip() or None
        self._record_ref(str(role), label)

    def handle_data(self, data: str) -> None:
        text = re.sub(r"\s+", " ", data).strip()
        if not text:
            return
        self._text_parts.append(text)
        for entry in self._stack:
            existing = str(entry.get("text") or "").strip()
            entry["text"] = f"{existing} {text}".strip() if existing else text

    def snapshot_result(self) -> SnapshotResult:
        text = " ".join(self._text_parts).strip()
        if text:
            preview = text[:400]
            suffix = "..." if len(text) > 400 else ""
            self.lines.append(f"Text: {preview}{suffix}")

        if not self.lines:
            self.lines.append("Page snapshot unavailable")

        return {"snapshot": "\n".join(self.lines), "refs": self.refs}

    def _record_ref(self, role: str, name: str | None) -> None:
        ref_id = f"e{self._ref_counter}"
        clean_name = name.strip() if isinstance(name, str) else None
        self.refs[ref_id] = {"role": role, "name": clean_name or None, "nth": 0}
        if clean_name:
            self.lines.append(f'- {role} "{clean_name}" [ref={ref_id}]')
        else:
            self.lines.append(f"- {role} [ref={ref_id}]")
        self._ref_counter += 1

    @staticmethod
    def _normalized_role(tag: str, attrs: dict[str, str | None]) -> str | None:
        tag = tag.lower()
        if tag == "a":
            return "link"
        if tag == "button":
            return "button"
        if tag == "textarea":
            return "textbox"
        if tag == "select":
            return "combobox"
        if tag == "input":
            input_type = str(attrs.get("type") or "text").lower()
            return {
                "button": "button",
                "submit": "button",
                "reset": "button",
                "checkbox": "checkbox",
                "radio": "radio",
                "search": "searchbox",
                "range": "slider",
                "number": "spinbutton",
            }.get(input_type, "textbox")
        return None

    @staticmethod
    def _preferred_label(tag: str, attrs: dict[str, str | None]) -> str | None:
        for key in ("aria-label", "title", "value", "placeholder", "alt", "name"):
            value = attrs.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        if tag.lower() == "a":
            return None
        return None


def _fallback_snapshot_from_html(html: str) -> SnapshotResult:
    parser = _FallbackSnapshotParser()
    parser.feed(re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", html))
    parser.close()
    return parser.snapshot_result()


async def capture_aria_snapshot(page: Page) -> SnapshotResult:
    """Capture an ARIA snapshot and inject stable references."""
    if hasattr(page, "aria_snapshot"):
        raw_snapshot = await page.aria_snapshot()
    else:
        logger.info("Page aria_snapshot unavailable; falling back to DOM-based snapshot")
        html = await page.content()
        return _fallback_snapshot_from_html(html)

    # Playwright's aria_snapshot returns a YAML-like string
    lines = raw_snapshot.splitlines()

    result_lines = []
    refs: dict[str, ElementRef] = {}

    # Track role+name counts to handle duplicates with nth()
    role_name_counts: dict[str, int] = {}

    ref_counter = 1

    for line in lines:
        # Match pattern: "  - role \"name\"" or "  - role"
        match = re.match(r'^(\s*-\s*)(\w+)(?:\s+"([^"]*)")?(.*)$', line)
        if not match:
            result_lines.append(line)
            continue

        prefix, role, name, suffix = match.groups()
        role = role.lower()

        # We only assign refs to interactive roles or things with names
        if role in INTERACTIVE_ROLES or name:
            ref_id = f"e{ref_counter}"

            # Track duplicates
            key = f"{role}:{name or ''}"
            nth = role_name_counts.get(key, 0)
            role_name_counts[key] = nth + 1

            refs[ref_id] = {"role": role, "name": name, "nth": nth}

            # Inject ref into the snapshot line for the LLM
            ref_tag = f" [ref={ref_id}]"
            if nth > 0:
                ref_tag += f" [nth={nth}]"

            new_line = f"{prefix}{role}"
            if name:
                new_line += f' "{name}"'
            new_line += f"{ref_tag}{suffix}"
            result_lines.append(new_line)
            ref_counter += 1
        else:
            result_lines.append(line)

    return {"snapshot": "\n".join(result_lines), "refs": refs}
