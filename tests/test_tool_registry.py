from __future__ import annotations

import pytest

from octopal.tools.registry import ToolSpec


def _spec(
    *,
    usage_examples: tuple[dict, ...] = (),
    usage_example_evidence: str | None = None,
) -> ToolSpec:
    return ToolSpec(
        name="fs_read",
        description="Read a workspace file.",
        parameters={
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": False,
        },
        permission="filesystem_read",
        handler=lambda _args, _ctx: "",
        usage_examples=usage_examples,
        usage_example_evidence=usage_example_evidence,
    )


def test_tool_spec_renders_usage_examples_into_counted_description() -> None:
    spec = _spec(
        usage_examples=(
            {"path": "reports/latest.md"},
            {"path": "memory/handoff.json"},
        ),
        usage_example_evidence="eval:filesystem-path-ambiguity-v1",
    )

    description = spec.to_openai_tool()["function"]["description"]

    assert description == (
        'Read a workspace file.\nExample arguments:\n- {"path":"reports/latest.md"}'
        '\n- {"path":"memory/handoff.json"}'
    )
    assert spec.usage_example_prompt_chars() == len(description) - len(spec.description)


def test_tool_spec_freezes_validated_example_prompt_text() -> None:
    example = {"path": "reports/latest.md"}
    spec = _spec(
        usage_examples=(example,),
        usage_example_evidence="eval:filesystem-path-ambiguity-v1",
    )

    example["path"] = "x" * 1_000

    assert spec.to_openai_tool()["function"]["description"].endswith('{"path":"reports/latest.md"}')


@pytest.mark.parametrize(
    "usage_examples",
    [
        ({},),
        ({"": "value"},),
        ({"value": float("nan")},),
        ({"value": 1}, {"value": 2}, {"value": 3}),
        ({"value": "x" * 600},),
    ],
)
def test_tool_spec_rejects_unbounded_or_invalid_usage_examples(
    usage_examples: tuple[dict, ...],
) -> None:
    with pytest.raises(ValueError):
        _spec(
            usage_examples=usage_examples,
            usage_example_evidence="eval:invalid-example-fixture",
        )


def test_tool_spec_requires_evidence_for_usage_examples() -> None:
    with pytest.raises(ValueError, match="evidence"):
        _spec(usage_examples=({"path": "reports/latest.md"},))

    with pytest.raises(ValueError, match="requires at least one example"):
        _spec(usage_example_evidence="eval:orphaned-evidence")

    with pytest.raises(ValueError, match="single printable line"):
        _spec(
            usage_examples=({"path": "reports/latest.md"},),
            usage_example_evidence="eval:bad\nevidence",
        )
