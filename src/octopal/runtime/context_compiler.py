"""Deterministic, metadata-only prompt context budgeting."""

from __future__ import annotations

from dataclasses import dataclass

_TRUNCATION_SUFFIX = "\n[...context section truncated...]"


class ContextBudgetExceededError(ValueError):
    """Raised when immutable prompt context cannot fit the configured budget."""


@dataclass(frozen=True)
class ContextSection:
    """One ordered prompt section with an explicit inclusion policy."""

    name: str
    content: str
    priority: int = 0
    required: bool = False


@dataclass(frozen=True)
class CompiledContext:
    """Rendered content plus non-sensitive accounting for a compiler run."""

    content: str
    sections: dict[str, str]
    manifest: dict[str, object]


def estimate_tokens(content: str) -> int:
    """Return a UTF-8 byte upper bound for provider-agnostic token budgeting.

    A byte may expand to at most one tokenizer unit, so byte length is a safe
    fallback when the active provider tokenizer is unavailable. It deliberately
    overestimates common Latin text instead of underestimating Cyrillic, JSON,
    or other multibyte content.
    """

    return len(content.encode("utf-8"))


def compile_context(
    sections: list[ContextSection],
    *,
    token_budget: int,
) -> CompiledContext:
    """Select ordered sections within a token budget without dropping required text.

    Required sections are immutable safety/protocol context. If they cannot fit,
    fail closed rather than sending an oversized prompt or truncating the contract.
    """

    if token_budget <= 0:
        raise ValueError("token_budget must be positive")

    selected: dict[str, str] = {}
    accounting: dict[str, dict[str, object]] = {}
    required_tokens = 0
    for section in sections:
        source_tokens = estimate_tokens(section.content)
        accounting[section.name] = {
            "estimated_tokens": source_tokens,
            "included_tokens": 0,
            "required": section.required,
            "priority": section.priority,
            "decision": "empty" if source_tokens == 0 else "omitted",
        }
        if section.required and section.content:
            selected[section.name] = section.content
            required_tokens += source_tokens
            accounting[section.name]["included_tokens"] = source_tokens
            accounting[section.name]["decision"] = "included"

    if required_tokens > token_budget:
        raise ContextBudgetExceededError(
            "required prompt context exceeds token budget " f"({required_tokens} > {token_budget})"
        )

    remaining = token_budget - required_tokens
    optional_sections = sorted(
        (section for section in sections if not section.required and section.content),
        key=lambda section: (-section.priority, section.name),
    )
    for section in optional_sections:
        source_tokens = estimate_tokens(section.content)
        if remaining <= 0:
            continue
        if source_tokens <= remaining:
            selected[section.name] = section.content
            accounting[section.name]["included_tokens"] = source_tokens
            accounting[section.name]["decision"] = "included"
            remaining -= source_tokens
            continue
        clipped = _truncate_to_token_budget(section.content, remaining)
        if clipped:
            selected[section.name] = clipped
            accounting[section.name]["included_tokens"] = estimate_tokens(clipped)
            accounting[section.name]["decision"] = "truncated"
        remaining = 0

    rendered_sections = {
        section.name: selected[section.name] for section in sections if section.name in selected
    }
    included_tokens = sum(estimate_tokens(content) for content in rendered_sections.values())
    manifest = {
        "version": 1,
        "token_budget": token_budget,
        "estimated_tokens": included_tokens,
        "required_tokens": required_tokens,
        "remaining_tokens": max(0, token_budget - included_tokens),
        "included_sections": list(rendered_sections),
        "truncated_sections": [
            name for name, detail in accounting.items() if detail["decision"] == "truncated"
        ],
        "omitted_sections": [
            name for name, detail in accounting.items() if detail["decision"] == "omitted"
        ],
        "sections": accounting,
    }
    return CompiledContext(
        content="\n\n".join(rendered_sections.values()),
        sections=rendered_sections,
        manifest=manifest,
    )


def _truncate_to_token_budget(content: str, token_budget: int) -> str:
    if token_budget <= 0:
        return ""
    if estimate_tokens(content) <= token_budget:
        return content
    suffix_bytes = _TRUNCATION_SUFFIX.encode("utf-8")
    content_bytes = content.encode("utf-8")
    if token_budget <= len(suffix_bytes):
        return content_bytes[:token_budget].decode("utf-8", errors="ignore")
    prefix = content_bytes[: token_budget - len(suffix_bytes)].decode("utf-8", errors="ignore")
    return prefix.rstrip() + _TRUNCATION_SUFFIX
