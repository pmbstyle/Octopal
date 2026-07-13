from __future__ import annotations

import pytest

from octopal.tools.catalog import get_tools
from octopal.tools.metadata import (
    ProgrammaticReadContract,
    ProgrammaticResultShape,
    ToolMetadata,
)
from octopal.tools.programmatic import (
    ProgrammaticReadResultError,
    filter_programmatic_read_tools,
    resolve_programmatic_read_tool,
    validate_programmatic_read_result,
)
from octopal.tools.registry import ToolSpec


def _tool(
    name: str = "lookup",
    *,
    permission: str = "network",
    metadata: ToolMetadata | None = None,
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description="Lookup data",
        parameters={"type": "object", "properties": {}},
        permission=permission,
        handler=lambda _args, _ctx: "{}",
        metadata=metadata or ToolMetadata(),
    )


def _contract(*, idempotent: bool = True) -> ProgrammaticReadContract:
    return ProgrammaticReadContract(
        idempotent=idempotent,
        max_parallel_calls=2,
        result_shape="json_object",
        max_result_bytes=16_384,
    )


def test_programmatic_contract_rejects_unbounded_values() -> None:
    with pytest.raises(ValueError, match="max_parallel_calls"):
        ProgrammaticReadContract(
            idempotent=True,
            max_parallel_calls=9,
            result_shape="text",
            max_result_bytes=1,
        )

    with pytest.raises(ValueError, match="max_result_bytes"):
        ProgrammaticReadContract(
            idempotent=True,
            max_parallel_calls=1,
            result_shape="text",
            max_result_bytes=1_000_001,
        )


def test_programmatic_contract_rejects_unknown_result_shape() -> None:
    invalid_shape: ProgrammaticResultShape = "bytes"  # type: ignore[assignment]

    with pytest.raises(ValueError, match="unsupported result_shape"):
        ProgrammaticReadContract(
            idempotent=True,
            max_parallel_calls=1,
            result_shape=invalid_shape,
            max_result_bytes=1,
        )


def test_programmatic_resolver_accepts_explicit_bounded_core_read_tool() -> None:
    tool = _tool(
        metadata=ToolMetadata(
            category="web",
            read_only=True,
            programmatic_read=_contract(),
        )
    )

    decision = resolve_programmatic_read_tool(tool)

    assert decision.allowed is True
    assert decision.reasons == ()
    assert decision.contract == _contract()


def test_programmatic_resolver_is_default_deny_and_explains_rejections() -> None:
    tool = _tool(metadata=ToolMetadata(risk="guarded", owner="plugin"))

    decision = resolve_programmatic_read_tool(tool)

    assert decision.allowed is False
    assert decision.reasons == (
        "programmatic_read_contract_missing",
        "tool_not_declared_read_only",
        "tool_risk_not_safe",
        "tool_owner_not_core",
    )


@pytest.mark.parametrize(
    ("category", "permission", "reason"),
    [
        ("communication", "network", "tool_category_forbidden"),
        ("browser", "network", "tool_category_forbidden"),
        ("desktop", "desktop_control", "tool_category_forbidden"),
        ("mcp", "mcp_exec", "tool_category_forbidden"),
        ("filesystem", "filesystem_write", "tool_permission_not_programmatic_read"),
        ("ops", "deploy_control", "tool_permission_not_programmatic_read"),
        ("runtime", "self_control", "tool_permission_not_programmatic_read"),
        ("approval", "exec", "tool_permission_not_programmatic_read"),
        ("ops", "service_control", "tool_permission_not_programmatic_read"),
        ("workers", "worker_manage", "tool_permission_not_programmatic_read"),
    ],
)
def test_programmatic_resolver_blocks_forbidden_surfaces(
    category: str, permission: str, reason: str
) -> None:
    tool = _tool(
        permission=permission,
        metadata=ToolMetadata(
            category=category,
            read_only=True,
            programmatic_read=_contract(),
        ),
    )

    decision = resolve_programmatic_read_tool(tool)

    assert decision.allowed is False
    assert reason in decision.reasons


def test_programmatic_resolver_requires_explicit_idempotence() -> None:
    tool = _tool(
        metadata=ToolMetadata(
            read_only=True,
            programmatic_read=_contract(idempotent=False),
        )
    )

    assert resolve_programmatic_read_tool(tool).reasons == ("tool_not_declared_idempotent",)


def test_programmatic_result_validator_accepts_declared_json_object() -> None:
    tool = _tool(metadata=ToolMetadata(read_only=True, programmatic_read=_contract()))

    validated = validate_programmatic_read_result(tool, '{"query":"café"}')

    assert validated.tool_name == "lookup"
    assert validated.result_shape == "json_object"
    assert validated.byte_count == len('{"query":"café"}'.encode())
    assert validated.value == {"query": "café"}


@pytest.mark.parametrize(
    ("result", "code"),
    [
        ([], "result_not_text"),
        ("not json", "result_invalid_json"),
        ('{"value":NaN}', "result_invalid_json"),
        ("[]", "result_shape_mismatch"),
        ("null", "result_shape_mismatch"),
    ],
)
def test_programmatic_result_validator_rejects_wrong_shape(result: object, code: str) -> None:
    tool = _tool(metadata=ToolMetadata(read_only=True, programmatic_read=_contract()))

    with pytest.raises(ProgrammaticReadResultError) as exc_info:
        validate_programmatic_read_result(tool, result)

    assert exc_info.value.code == code


@pytest.mark.parametrize("result", ["secret:not-json", "\ud800"])
def test_programmatic_result_errors_do_not_retain_raw_exception_context(result: str) -> None:
    tool = _tool(metadata=ToolMetadata(read_only=True, programmatic_read=_contract()))

    with pytest.raises(ProgrammaticReadResultError) as exc_info:
        validate_programmatic_read_result(tool, result)

    error = exc_info.value
    assert result not in str(error)
    assert error.__cause__ is None
    assert error.__context__ is None


def test_programmatic_result_validator_enforces_utf8_byte_limit_without_leaking_result() -> None:
    secret_result = '{"secret":"éé"}'
    tool = _tool(
        metadata=ToolMetadata(
            read_only=True,
            programmatic_read=ProgrammaticReadContract(
                idempotent=True,
                max_parallel_calls=1,
                result_shape="json_object",
                max_result_bytes=len(secret_result),
            ),
        )
    )

    with pytest.raises(ProgrammaticReadResultError) as exc_info:
        validate_programmatic_read_result(tool, secret_result)

    error = exc_info.value
    assert error.code == "result_too_large"
    assert error.details == (
        f"actual_bytes={len(secret_result.encode('utf-8'))}",
        f"max_result_bytes={len(secret_result)}",
    )
    assert secret_result not in str(error)


def test_programmatic_result_validator_rejects_ineligible_tool_before_result() -> None:
    secret_result = "do not include this"

    with pytest.raises(ProgrammaticReadResultError) as exc_info:
        validate_programmatic_read_result(_tool(), secret_result)

    error = exc_info.value
    assert error.code == "tool_not_eligible"
    assert error.details == (
        "programmatic_read_contract_missing",
        "tool_not_declared_read_only",
    )
    assert secret_result not in str(error)


def test_catalog_opts_in_only_web_search_for_programmatic_reads() -> None:
    tools = get_tools(mcp_manager=None)

    assert [tool.name for tool in filter_programmatic_read_tools(tools)] == ["web_search"]
    web_search = next(tool for tool in tools if tool.name == "web_search")
    decision = resolve_programmatic_read_tool(web_search)
    assert decision.allowed is True
    assert decision.contract == ProgrammaticReadContract(
        idempotent=True,
        max_parallel_calls=2,
        result_shape="json_object",
        max_result_bytes=64_000,
    )

    validated = validate_programmatic_read_result(web_search, web_search.handler({}, {}))
    assert isinstance(validated.value, dict)
    assert validated.value["error"] == "query is required"
