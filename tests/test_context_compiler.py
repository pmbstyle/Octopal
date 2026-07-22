from __future__ import annotations

import pytest

from octopal.runtime.context_compiler import (
    ContextBudgetExceededError,
    ContextSection,
    compile_context,
    estimate_tokens,
)


def test_compiler_keeps_required_sections_and_prioritizes_optional_context() -> None:
    compiled = compile_context(
        [
            ContextSection("policy", "policy", required=True),
            ContextSection("low", "l" * 12, priority=1),
            ContextSection("high", "h" * 12, priority=10),
        ],
        token_budget=18,
    )

    assert compiled.sections == {"policy": "policy", "high": "h" * 12}
    assert compiled.manifest["included_sections"] == ["policy", "high"]
    assert compiled.manifest["omitted_sections"] == ["low"]
    assert compiled.manifest["estimated_tokens"] <= 18


def test_compiler_truncates_one_optional_section_to_the_remaining_budget() -> None:
    compiled = compile_context(
        [
            ContextSection("policy", "policy", required=True),
            ContextSection("memory", "m" * 40, priority=1),
        ],
        token_budget=18,
    )

    assert compiled.manifest["truncated_sections"] == ["memory"]
    assert estimate_tokens(compiled.sections["memory"]) <= 12
    assert compiled.manifest["estimated_tokens"] <= 18


def test_compiler_never_silently_truncates_required_context() -> None:
    with pytest.raises(ContextBudgetExceededError, match="required prompt context"):
        compile_context(
            [
                ContextSection("policy", "p" * 20, required=True),
                ContextSection("memory", "memory", priority=1),
            ],
            token_budget=2,
        )


def test_compiler_uses_utf8_byte_budget_for_multibyte_text() -> None:
    content = "Привет, 世界" * 10
    compiled = compile_context(
        [ContextSection("memory", content, priority=1)],
        token_budget=30,
    )

    assert estimate_tokens("Привет") == len("Привет".encode())
    assert estimate_tokens(compiled.sections["memory"]) <= 30
