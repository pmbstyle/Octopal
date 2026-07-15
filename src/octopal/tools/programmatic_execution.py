from __future__ import annotations

import asyncio
import inspect
import itertools
import json
import math
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from jsonschema import SchemaError
from jsonschema.validators import validator_for

from octopal.tools.programmatic import (
    ProgrammaticReadResultError,
    ProgrammaticReadValue,
    resolve_programmatic_read_tool,
    validate_programmatic_read_result,
)
from octopal.tools.registry import ToolSpec

PROGRAMMATIC_READ_MAX_BATCH_CALLS = 16
PROGRAMMATIC_READ_MAX_BATCH_PARALLELISM = 8
PROGRAMMATIC_READ_MAX_CALL_TIMEOUT_SECONDS = 60.0
PROGRAMMATIC_READ_MAX_ARGUMENT_BYTES = 256_000

_CALL_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_TOOL_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

ProgrammaticReadCallStatus = Literal["completed", "failed", "timed_out"]


@dataclass(frozen=True)
class ProgrammaticReadBatchLimits:
    """Hard input and scheduling ceilings for one host-side batch."""

    max_calls: int = 8
    max_parallel_calls: int = 4
    call_timeout_seconds: float = 30.0
    max_argument_bytes: int = 16_000

    def __post_init__(self) -> None:
        if type(self.max_calls) is not int or not (
            1 <= self.max_calls <= PROGRAMMATIC_READ_MAX_BATCH_CALLS
        ):
            raise ValueError(f"max_calls must be between 1 and {PROGRAMMATIC_READ_MAX_BATCH_CALLS}")
        if type(self.max_parallel_calls) is not int or not (
            1 <= self.max_parallel_calls <= PROGRAMMATIC_READ_MAX_BATCH_PARALLELISM
        ):
            raise ValueError(
                "max_parallel_calls must be between 1 and "
                f"{PROGRAMMATIC_READ_MAX_BATCH_PARALLELISM}"
            )
        if (
            isinstance(self.call_timeout_seconds, bool)
            or not isinstance(self.call_timeout_seconds, (int, float))
            or not math.isfinite(self.call_timeout_seconds)
            or not 0 < self.call_timeout_seconds <= PROGRAMMATIC_READ_MAX_CALL_TIMEOUT_SECONDS
        ):
            raise ValueError(
                "call_timeout_seconds must be greater than 0 and at most "
                f"{PROGRAMMATIC_READ_MAX_CALL_TIMEOUT_SECONDS:g}"
            )
        if type(self.max_argument_bytes) is not int or not (
            1 <= self.max_argument_bytes <= PROGRAMMATIC_READ_MAX_ARGUMENT_BYTES
        ):
            raise ValueError(
                "max_argument_bytes must be between 1 and "
                f"{PROGRAMMATIC_READ_MAX_ARGUMENT_BYTES}"
            )


@dataclass(frozen=True)
class ProgrammaticReadCall:
    call_id: str
    tool_name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class ProgrammaticReadCallResult:
    call_id: str
    tool_name: str
    status: ProgrammaticReadCallStatus
    elapsed_ms: int
    value: ProgrammaticReadValue | None = None
    byte_count: int | None = None
    error_code: str | None = None
    error_details: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProgrammaticReadBatchResult:
    results: tuple[ProgrammaticReadCallResult, ...]
    elapsed_ms: int

    @property
    def completed_count(self) -> int:
        return sum(result.status == "completed" for result in self.results)

    @property
    def failed_count(self) -> int:
        return sum(result.status == "failed" for result in self.results)

    @property
    def timed_out_count(self) -> int:
        return sum(result.status == "timed_out" for result in self.results)


class ProgrammaticReadBatchError(ValueError):
    """Reject a whole batch before execution without retaining call arguments."""

    def __init__(self, code: str, *, details: tuple[str, ...] = ()) -> None:
        self.code = code
        self.details = details
        super().__init__(code)


async def execute_programmatic_read_batch(
    tools: Iterable[ToolSpec],
    calls: Iterable[ProgrammaticReadCall],
    *,
    ctx: Mapping[str, Any] | None = None,
    limits: ProgrammaticReadBatchLimits | None = None,
) -> ProgrammaticReadBatchResult:
    """Execute an atomically preflighted batch with bounded concurrency and no retries.

    Only native coroutine handlers are accepted so timeout cancellation cannot
    leave a synchronous thread running after its concurrency lease is released.
    """
    started_ns = time.monotonic_ns()
    effective_limits = limits or ProgrammaticReadBatchLimits()
    prepared = _preflight_batch(tools, calls, limits=effective_limits)
    if not prepared:
        return ProgrammaticReadBatchResult(results=(), elapsed_ms=0)

    global_semaphore = asyncio.Semaphore(effective_limits.max_parallel_calls)
    tool_semaphores = {
        tool.name: asyncio.Semaphore(tool.metadata.programmatic_read.max_parallel_calls)
        for _, tool, _ in prepared
        if tool.metadata.programmatic_read is not None
    }
    shared_ctx = dict(ctx or {})

    async def run_one(
        call: ProgrammaticReadCall, tool: ToolSpec, arguments: dict[str, Any]
    ) -> ProgrammaticReadCallResult:
        async with tool_semaphores[tool.name], global_semaphore:
            return await _execute_programmatic_read_call(
                call,
                tool,
                arguments,
                ctx=dict(shared_ctx),
                timeout_seconds=effective_limits.call_timeout_seconds,
            )

    results = await asyncio.gather(
        *(run_one(call, tool, arguments) for call, tool, arguments in prepared)
    )
    return ProgrammaticReadBatchResult(
        results=tuple(results),
        elapsed_ms=_elapsed_ms(started_ns),
    )


def _preflight_batch(
    tools: Iterable[ToolSpec],
    calls: Iterable[ProgrammaticReadCall],
    *,
    limits: ProgrammaticReadBatchLimits,
) -> list[tuple[ProgrammaticReadCall, ToolSpec, dict[str, Any]]]:
    tool_by_name: dict[str, ToolSpec] = {}
    for tool in tools:
        if tool.name in tool_by_name:
            raise ProgrammaticReadBatchError(
                "duplicate_tool_definition", details=(f"tool_name={tool.name}",)
            )
        tool_by_name[tool.name] = tool

    call_list = list(itertools.islice(calls, limits.max_calls + 1))
    if len(call_list) > limits.max_calls:
        raise ProgrammaticReadBatchError(
            "batch_call_limit_exceeded",
            details=(f"observed_calls={len(call_list)}", f"max_calls={limits.max_calls}"),
        )

    seen_call_ids: set[str] = set()
    prepared: list[tuple[ProgrammaticReadCall, ToolSpec, dict[str, Any]]] = []
    for call in call_list:
        if not isinstance(call.call_id, str) or not _CALL_ID_PATTERN.fullmatch(call.call_id):
            raise ProgrammaticReadBatchError("call_id_invalid")
        if call.call_id in seen_call_ids:
            raise ProgrammaticReadBatchError(
                "duplicate_call_id", details=(f"call_id={call.call_id}",)
            )
        seen_call_ids.add(call.call_id)
        if not isinstance(call.tool_name, str) or not _TOOL_NAME_PATTERN.fullmatch(call.tool_name):
            raise ProgrammaticReadBatchError(
                "tool_name_invalid", details=(f"call_id={call.call_id}",)
            )
        tool = tool_by_name.get(call.tool_name)
        if tool is None:
            raise ProgrammaticReadBatchError(
                "tool_not_found",
                details=(f"call_id={call.call_id}", f"tool_name={call.tool_name}"),
            )
        decision = resolve_programmatic_read_tool(tool)
        if not decision.allowed:
            raise ProgrammaticReadBatchError(
                "tool_not_eligible",
                details=(
                    f"call_id={call.call_id}",
                    f"tool_name={call.tool_name}",
                    *decision.reasons,
                ),
            )
        if not tool.is_async or not inspect.iscoroutinefunction(tool.handler):
            raise ProgrammaticReadBatchError(
                "handler_not_cancellable",
                details=(f"call_id={call.call_id}", f"tool_name={call.tool_name}"),
            )
        if not isinstance(call.arguments, dict):
            raise ProgrammaticReadBatchError(
                "arguments_not_object", details=(f"call_id={call.call_id}",)
            )
        arguments = _normalize_arguments(
            call.arguments,
            call_id=call.call_id,
            max_argument_bytes=limits.max_argument_bytes,
        )
        _validate_arguments(tool, arguments, call_id=call.call_id)
        prepared.append((call, tool, arguments))
    return prepared


def _normalize_arguments(
    arguments: dict[str, Any], *, call_id: str, max_argument_bytes: int
) -> dict[str, Any]:
    try:
        serialized = json.dumps(
            arguments,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        encoded = serialized.encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise ProgrammaticReadBatchError(
            "arguments_not_json", details=(f"call_id={call_id}",)
        ) from None
    if len(encoded) > max_argument_bytes:
        raise ProgrammaticReadBatchError(
            "arguments_too_large",
            details=(
                f"call_id={call_id}",
                f"actual_bytes={len(encoded)}",
                f"max_argument_bytes={max_argument_bytes}",
            ),
        )
    normalized = json.loads(serialized)
    if not isinstance(normalized, dict):
        raise ProgrammaticReadBatchError("arguments_not_object", details=(f"call_id={call_id}",))
    return normalized


def _validate_arguments(tool: ToolSpec, arguments: dict[str, Any], *, call_id: str) -> None:
    try:
        validator_class = validator_for(tool.parameters)
        validator_class.check_schema(tool.parameters)
        validation_error = next(validator_class(tool.parameters).iter_errors(arguments), None)
    except SchemaError:
        raise ProgrammaticReadBatchError(
            "tool_schema_invalid",
            details=(f"call_id={call_id}", f"tool_name={tool.name}"),
        ) from None
    if validation_error is not None:
        raise ProgrammaticReadBatchError(
            "arguments_invalid",
            details=(f"call_id={call_id}", f"tool_name={tool.name}"),
        )


async def _execute_programmatic_read_call(
    call: ProgrammaticReadCall,
    tool: ToolSpec,
    arguments: dict[str, Any],
    *,
    ctx: dict[str, Any],
    timeout_seconds: float,
) -> ProgrammaticReadCallResult:
    started_ns = time.monotonic_ns()
    try:
        raw_result = await asyncio.wait_for(
            _invoke_handler(tool, arguments, ctx),
            timeout=timeout_seconds,
        )
    except TimeoutError:
        return ProgrammaticReadCallResult(
            call_id=call.call_id,
            tool_name=tool.name,
            status="timed_out",
            elapsed_ms=_elapsed_ms(started_ns),
            error_code="call_timed_out",
        )
    except Exception:
        return ProgrammaticReadCallResult(
            call_id=call.call_id,
            tool_name=tool.name,
            status="failed",
            elapsed_ms=_elapsed_ms(started_ns),
            error_code="handler_error",
        )

    try:
        validated = validate_programmatic_read_result(tool, raw_result)
    except ProgrammaticReadResultError as exc:
        return ProgrammaticReadCallResult(
            call_id=call.call_id,
            tool_name=tool.name,
            status="failed",
            elapsed_ms=_elapsed_ms(started_ns),
            error_code=exc.code,
            error_details=exc.details,
        )
    return ProgrammaticReadCallResult(
        call_id=call.call_id,
        tool_name=tool.name,
        status="completed",
        elapsed_ms=_elapsed_ms(started_ns),
        value=validated.value,
        byte_count=validated.byte_count,
    )


async def _invoke_handler(tool: ToolSpec, arguments: dict[str, Any], ctx: dict[str, Any]) -> Any:
    return await tool.handler(arguments, ctx)


def _elapsed_ms(started_ns: int) -> int:
    return max(0, (time.monotonic_ns() - started_ns) // 1_000_000)
