"""LiteLLM-based inference provider with retry logic and fallbacks."""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
import random
import re
from collections.abc import Awaitable, Callable
from typing import Any

from litellm import acompletion

from octopal.infrastructure.config.models import LLMConfig
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.observability.base import (
    TraceSink,
    bind_trace_context,
    get_current_trace_context,
    now_ms,
    reset_trace_context,
)
from octopal.infrastructure.observability.helpers import safe_preview, summarize_exception
from octopal.infrastructure.observability.noop import NoopTraceSink
from octopal.infrastructure.providers.base import Message
from octopal.infrastructure.providers.profile_resolver import resolve_litellm_profile

logger = logging.getLogger(__name__)

_LOG_MAX_CHARS = 400  # Reduced from 2000


class LiteLLMProvider:
    """LiteLLM-based inference provider with automatic retries and fallbacks.
    Supports both OpenRouter and z.ai (custom OpenAI-compatible endpoints).
    """

    _semaphores_by_limit: dict[int, asyncio.Semaphore] = {}
    _rate_limit_cooldowns: dict[tuple[str, str, str], float] = {}
    _tool_response_format_modes: dict[tuple[str, str, str], str] = {}
    _tool_choice_modes: dict[tuple[str, str, str], object] = {}

    def __init__(
        self,
        settings: Settings,
        model: str | None = None,
        config: LLMConfig | None = None,
        trace_sink: TraceSink | None = None,
    ) -> None:
        self._settings = settings
        self._trace_sink = trace_sink or NoopTraceSink()
        self._profile = resolve_litellm_profile(
            settings, model_override=model, config_override=config
        )
        self._model = self._profile.model
        self._api_base = (self._profile.api_base or "").rstrip("/") or None
        self._api_key = self._profile.api_key

        logger.info(
            "LiteLLM configured: provider=%s source=%s model=%s base_url=%s",
            self._profile.provider_id,
            self._profile.source,
            self._model,
            self._api_base or "<default>",
        )

        # Parse fallbacks from JSON string if provided
        self._fallbacks: list[dict[str, Any]] | None = None
        if settings.litellm_fallbacks:
            try:
                self._fallbacks = json.loads(settings.litellm_fallbacks)
            except Exception as exc:
                logger.warning("Failed to parse LITELLM_FALLBACKS JSON: %s", exc)

        # Configure litellm at module level
        import litellm

        # Suppress LiteLLM's verbose logging including "Provider List" messages
        litellm.set_verbose = False
        litellm.suppress_debug_info = True
        litellm.turn_off_message_logging = True

        if settings.litellm_num_retries > 0:
            litellm.num_retries = settings.litellm_num_retries

        if settings.litellm_drop_params:
            litellm.drop_params = settings.litellm_drop_params

        if settings.litellm_caching:
            litellm.caching = True
            logger.info("LiteLLM caching is enabled.")

        max_concurrency = max(1, int(settings.litellm_max_concurrency))
        self._semaphore = self._semaphores_by_limit.setdefault(
            max_concurrency, asyncio.Semaphore(max_concurrency)
        )
        self._rate_limit_max_retries = max(0, int(settings.litellm_rate_limit_max_retries))
        self._rate_limit_base_delay = max(
            0.1, float(settings.litellm_rate_limit_base_delay_seconds)
        )
        self._rate_limit_max_delay = max(
            self._rate_limit_base_delay, float(settings.litellm_rate_limit_max_delay_seconds)
        )

    @property
    def provider_id(self) -> str:
        return self._profile.provider_id

    def _shared_rate_limit_key(self) -> tuple[str, str, str]:
        return (
            self._profile.provider_id,
            self._model,
            self._api_base or "",
        )

    def _tool_response_format_key(self) -> tuple[str, str, str]:
        return self._shared_rate_limit_key()

    def _now(self) -> float:
        return asyncio.get_running_loop().time()

    async def _wait_for_shared_rate_limit_cooldown(self) -> None:
        cooldown_until = self._rate_limit_cooldowns.get(self._shared_rate_limit_key(), 0.0)
        remaining = cooldown_until - self._now()
        if remaining <= 0:
            return
        logger.info(
            "LiteLLM shared rate-limit cooldown active; delaying request for %.2fs",
            remaining,
        )
        await asyncio.sleep(remaining)

    def _record_shared_rate_limit_cooldown(self, delay: float) -> None:
        if delay <= 0:
            return
        key = self._shared_rate_limit_key()
        until = self._now() + delay
        existing = self._rate_limit_cooldowns.get(key, 0.0)
        if until > existing:
            self._rate_limit_cooldowns[key] = until

    async def complete(self, messages: list[Message | dict], **kwargs: object) -> str:
        """Complete a chat request without tools."""
        if self._profile.requires_api_key and not self._api_key:
            raise RuntimeError(
                "API key is not configured for the active LiteLLM provider. "
                "Set OCTOPAL_LITELLM_API_KEY or configure a provider-specific legacy key."
            )

        serialized_messages = _normalize_plain_messages([_serialize_message(m) for m in messages])
        payload_str = json.dumps({"messages": serialized_messages}, ensure_ascii=False)
        trace_ctx, trace_token, trace_started_at_ms = await self._start_observability_span(
            "complete",
            messages=serialized_messages,
            tools=None,
        )
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        trace_metadata: dict[str, Any] = {}

        logger.debug(
            "LiteLLM request: model=%s, messages=%d, total_chars=%d",
            self._model,
            len(serialized_messages),
            len(payload_str),
        )

        if self._settings.debug_prompts:
            logger.debug("LiteLLM payload: %s", _truncate(payload_str))

        request_kwargs = _build_request_kwargs(
            kwargs,
            temperature=float(kwargs.get("temperature", 0.3)),
            timeout=self._settings.litellm_timeout,
            fallbacks=self._fallbacks,
        )
        try:
            response = await self._acompletion_with_resilience(
                messages=serialized_messages,
                **request_kwargs,
            )
            content = _extract_content(response)
            usage = _extract_usage(response)
            trace_output = {
                "output_chars": len(content),
                "usage": usage,
            }
            trace_metadata.update(
                {
                    "finish_reason": _extract_finish_reason(response),
                    "usage_input_tokens": usage.get("prompt_tokens"),
                    "usage_output_tokens": usage.get("completion_tokens"),
                }
            )
            logger.debug("LiteLLM response: %s", _truncate(content))
            return content
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            if _looks_like_illegal_messages_error(exc):
                retry_messages = _build_strict_retry_messages(serialized_messages)
                logger.warning(
                    "Retrying LiteLLM completion with strict message normalization after provider rejected messages payload",
                )
                try:
                    response = await self._acompletion_with_resilience(
                        messages=retry_messages,
                        **request_kwargs,
                    )
                    content = _extract_content(response)
                    usage = _extract_usage(response)
                    trace_status = "ok"
                    trace_output = {
                        "output_chars": len(content),
                        "usage": usage,
                    }
                    trace_metadata.update(
                        {
                            "strict_retry": True,
                            "finish_reason": _extract_finish_reason(response),
                            "usage_input_tokens": usage.get("prompt_tokens"),
                            "usage_output_tokens": usage.get("completion_tokens"),
                        }
                    )
                    logger.debug("LiteLLM response (strict retry): %s", _truncate(content))
                    return content
                except Exception:
                    logger.error(
                        "LiteLLM strict-retry payload shape on error: %s",
                        _summarize_messages(retry_messages),
                    )
            err_str = str(exc)
            logger.error(
                "LiteLLM completion payload shape on error: %s",
                _summarize_messages(serialized_messages),
            )
            if "finish_reason" in err_str and "abort" in err_str:
                logger.error("LLM provider returned invalid 'abort' finish_reason.")
                raise RuntimeError(
                    "LLM provider (z.ai) returned an invalid response code ('abort'). "
                    "This usually means the model was cut off or is having internal issues. "
                    "Please try your request again."
                ) from exc
            logger.exception("LiteLLM completion failed")
            raise RuntimeError(f"LiteLLM completion failed: {exc}") from exc
        finally:
            await self._finish_observability_span(
                trace_ctx,
                trace_token,
                trace_started_at_ms,
                status=trace_status,
                output=trace_output,
                metadata=trace_metadata,
            )

    async def complete_stream(
        self,
        messages: list[Message | dict],
        *,
        on_partial: Callable[[str], Awaitable[None]],
        **kwargs: object,
    ) -> str:
        """Complete a chat request with streamed partial text callbacks."""
        if self._profile.requires_api_key and not self._api_key:
            raise RuntimeError(
                "API key is not configured for the active LiteLLM provider. "
                "Set OCTOPAL_LITELLM_API_KEY or configure a provider-specific legacy key."
            )

        serialized_messages = _normalize_plain_messages([_serialize_message(m) for m in messages])
        payload_str = json.dumps(
            {"messages": serialized_messages, "stream": True}, ensure_ascii=False
        )
        trace_ctx, trace_token, trace_started_at_ms = await self._start_observability_span(
            "complete_stream",
            messages=serialized_messages,
            tools=None,
        )
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        trace_metadata: dict[str, Any] = {}
        logger.debug(
            "LiteLLM stream request: model=%s, messages=%d, total_chars=%d",
            self._model,
            len(serialized_messages),
            len(payload_str),
        )
        if self._settings.debug_prompts:
            logger.debug("LiteLLM stream payload: %s", _truncate(payload_str))

        request_kwargs = _build_request_kwargs(
            kwargs,
            temperature=float(kwargs.get("temperature", 0.3)),
            timeout=self._settings.litellm_timeout,
            fallbacks=self._fallbacks,
        )
        request_kwargs["stream"] = True
        try:
            response = await self._acompletion_with_resilience(
                messages=serialized_messages, **request_kwargs
            )
            if not hasattr(response, "__aiter__"):
                # Provider did not return an async stream; gracefully fall back to non-stream.
                text = _extract_content(response)
                if text:
                    try:
                        await on_partial(text)
                    except Exception:
                        logger.debug("LiteLLM partial callback failed", exc_info=True)
                trace_output = {"output_chars": len(text)}
                return text

            accumulated = ""
            async for chunk in response:  # type: ignore[assignment]
                delta = _extract_stream_delta(chunk)
                if not delta:
                    continue
                accumulated += delta
                try:
                    await on_partial(accumulated)
                except Exception:
                    logger.debug("LiteLLM partial callback failed", exc_info=True)

            logger.debug("LiteLLM streamed response: %s", _truncate(accumulated))
            trace_output = {"output_chars": len(accumulated)}
            return accumulated
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            logger.error(
                "LiteLLM stream payload shape on error: %s",
                _summarize_messages(serialized_messages),
            )
            logger.exception("LiteLLM stream completion failed")
            raise RuntimeError(f"LiteLLM stream completion failed: {exc}") from exc
        finally:
            await self._finish_observability_span(
                trace_ctx,
                trace_token,
                trace_started_at_ms,
                status=trace_status,
                output=trace_output,
                metadata=trace_metadata,
            )

    async def complete_with_tools(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict],
        tool_choice: object = "auto",
        **kwargs: object,
    ) -> dict:
        """Complete a chat request with tool/function calling."""
        if self._profile.requires_api_key and not self._api_key:
            raise RuntimeError(
                "API key is not configured for the active LiteLLM provider. "
                "Set OCTOPAL_LITELLM_API_KEY or configure a provider-specific legacy key."
            )

        serialized_messages = [_serialize_message(m) for m in messages]
        normalized_tools = _sanitize_tools_for_provider(tools, self._profile.provider_id)
        payload_str = json.dumps(
            {
                "messages": serialized_messages,
                "tools": normalized_tools,
                "tool_choice": tool_choice,
            },
            ensure_ascii=False,
        )
        tool_names = [t.get("function", {}).get("name") for t in normalized_tools]
        trace_ctx, trace_token, trace_started_at_ms = await self._start_observability_span(
            "complete_with_tools",
            messages=serialized_messages,
            tools=normalized_tools,
        )
        trace_status = "ok"
        trace_output: dict[str, Any] | None = None
        trace_metadata: dict[str, Any] = {"tool_choice": tool_choice}

        logger.debug(
            "LiteLLM request (tools): model=%s, messages=%d, tools=%s, total_chars=%d",
            self._model,
            len(serialized_messages),
            tool_names,
            len(payload_str),
        )

        if self._settings.debug_prompts:
            logger.debug("LiteLLM payload (tools): %s", _truncate(payload_str))

        try:
            request_kwargs = _build_request_kwargs(
                kwargs,
                temperature=float(kwargs.get("temperature", 0.3)),
                timeout=self._settings.litellm_timeout,
                fallbacks=self._fallbacks,
            )
            request_kwargs = _sanitize_request_kwargs_for_provider(
                request_kwargs,
                provider_id=self._profile.provider_id,
            )
            response, response_format_mode = (
                await self._complete_with_tools_adaptive_response_format(
                    messages=serialized_messages,
                    tools=normalized_tools,
                    tool_choice=tool_choice,
                    request_kwargs=request_kwargs,
                )
            )

            content = _extract_content(response)
            tool_calls = _extract_tool_calls(response)
            usage = _extract_usage(response)

            if response_format_mode != "json_schema" and "response_format" in request_kwargs:
                logger.info(
                    "LiteLLM tool response_format downgraded for route: provider=%s model=%s mode=%s",
                    self._profile.provider_id,
                    self._model,
                    response_format_mode,
                )

            if content:
                logger.debug("LiteLLM response (tools) content: %s", _truncate(content))
            if tool_calls:
                tool_call_names = [tc.get("function", {}).get("name") for tc in tool_calls]
                logger.debug("LiteLLM response: tool_calls=%s", tool_call_names)
                if self._settings.debug_prompts:
                    logger.debug(
                        "LiteLLM tool_calls payload: %s",
                        _truncate(json.dumps(tool_calls, ensure_ascii=False)),
                    )

            trace_output = {
                "output_chars": len(content),
                "tool_call_count": len(tool_calls),
                "tool_call_names": [tc.get("function", {}).get("name") for tc in tool_calls],
            }
            trace_metadata.update(
                {
                    "usage_input_tokens": usage.get("prompt_tokens"),
                    "usage_output_tokens": usage.get("completion_tokens"),
                    "finish_reason": _extract_finish_reason(response),
                    "response_format_mode": response_format_mode,
                }
            )
            return {"content": content, "tool_calls": tool_calls, "usage": usage}
        except Exception as exc:
            trace_status = "error"
            trace_metadata.update(summarize_exception(exc))
            err_str = str(exc)
            logger.error(
                "LiteLLM completion-with-tools payload shape on error: messages=%s tool_count=%s",
                _summarize_messages(serialized_messages),
                len(tools),
            )
            if "finish_reason" in err_str and "abort" in err_str:
                logger.error(
                    "LLM provider returned invalid 'abort' finish_reason. This is a known compatibility issue with some providers."
                )
                raise RuntimeError(
                    "LLM provider (z.ai) returned an invalid response code ('abort'). "
                    "This usually means the model was cut off or is having internal issues. "
                    "Please try your request again or check the provider status."
                ) from exc
            logger.exception("LiteLLM completion with tools failed")
            raise RuntimeError(f"LiteLLM completion with tools failed: {exc}") from exc
        finally:
            await self._finish_observability_span(
                trace_ctx,
                trace_token,
                trace_started_at_ms,
                status=trace_status,
                output=trace_output,
                metadata=trace_metadata,
            )

    async def _start_observability_span(
        self,
        call_type: str,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
    ) -> tuple[Any, Any, float | None]:
        parent_ctx = get_current_trace_context()
        if parent_ctx is None or self._trace_sink is None:
            return None, None, None
        metadata: dict[str, Any] = {
            "provider_id": self._profile.provider_id,
            "model": self._model,
            "call_type": call_type,
            "messages_count": len(messages),
            "input_chars": sum(
                len(json.dumps(message, ensure_ascii=False, default=str)) for message in messages
            ),
            "tool_count": len(tools or []),
            "tool_names": [tool.get("function", {}).get("name") for tool in tools or []],
        }
        if self._settings.observability_capture_content:
            metadata["input_preview"] = safe_preview(
                messages, limit=self._settings.observability_preview_chars
            )
        span_ctx = await self._trace_sink.start_span(parent_ctx, name="llm.call", metadata=metadata)
        token = bind_trace_context(span_ctx)
        return span_ctx, token, now_ms()

    async def _finish_observability_span(
        self,
        span_ctx: Any,
        token: Any,
        started_at_ms: float | None,
        *,
        status: str,
        output: dict[str, Any] | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        try:
            if span_ctx is not None and self._trace_sink is not None:
                finish_meta = dict(metadata or {})
                if started_at_ms is not None:
                    finish_meta["duration_ms"] = round(now_ms() - started_at_ms, 2)
                await self._trace_sink.finish_span(
                    span_ctx,
                    status=status,
                    output=output,
                    metadata=finish_meta,
                )
        finally:
            if token is not None:
                reset_trace_context(token)

    async def _complete_with_tools_adaptive_response_format(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        tool_choice: object,
        request_kwargs: dict[str, object],
    ) -> tuple[Any, str]:
        requested_response_format = request_kwargs.get("response_format")
        route_key = self._tool_response_format_key()
        preferred_mode = self._tool_response_format_modes.get(route_key)
        preferred_tool_choice = self._tool_choice_modes.get(route_key, tool_choice)
        candidates = _response_format_fallback_modes(
            requested_response_format, preferred_mode=preferred_mode
        )
        last_exc: Exception | None = None

        for index, mode in enumerate(candidates):
            attempt_kwargs = dict(request_kwargs)
            _apply_response_format_mode(attempt_kwargs, requested_response_format, mode)
            attempt_tool_choice = preferred_tool_choice
            try:
                response = await self._acompletion_with_resilience(
                    messages=messages,
                    tools=tools,
                    tool_choice=attempt_tool_choice,
                    **attempt_kwargs,
                )
                self._tool_response_format_modes[route_key] = mode
                return response, mode
            except Exception as exc:
                last_exc = exc
                if (
                    attempt_tool_choice != "auto"
                    and _is_tool_choice_required_unsupported_in_thinking_error(exc)
                ):
                    logger.info(
                        "LiteLLM route rejected tool_choice=%r in thinking mode; retrying with tool_choice=auto for provider=%s model=%s",
                        attempt_tool_choice,
                        self._profile.provider_id,
                        self._model,
                    )
                    response = await self._acompletion_with_resilience(
                        messages=messages,
                        tools=tools,
                        tool_choice="auto",
                        **attempt_kwargs,
                    )
                    self._tool_choice_modes[route_key] = "auto"
                    self._tool_response_format_modes[route_key] = mode
                    return response, mode
                has_lower_mode = index < len(candidates) - 1
                if not has_lower_mode or not _is_response_format_unsupported_error(exc):
                    raise
                logger.info(
                    "LiteLLM route rejected response_format mode=%s; downgrading for provider=%s model=%s",
                    mode,
                    self._profile.provider_id,
                    self._model,
                )
                continue

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(
            "LiteLLM completion with tools failed before issuing a provider request."
        )

    async def _acompletion_with_resilience(self, **kwargs: object) -> Any:
        attempt = 0
        while True:
            try:
                await self._wait_for_shared_rate_limit_cooldown()
                return await self._acompletion_guarded(**kwargs)
            except Exception as exc:
                if not _is_rate_limit_error(exc) or attempt >= self._rate_limit_max_retries:
                    raise
                delay = _compute_rate_limit_delay(
                    exc=exc,
                    attempt=attempt,
                    base_delay=self._rate_limit_base_delay,
                    max_delay=self._rate_limit_max_delay,
                )
                logger.warning(
                    "LiteLLM rate limited (attempt %s/%s). Retrying in %.2fs",
                    attempt + 1,
                    self._rate_limit_max_retries,
                    delay,
                )
                self._record_shared_rate_limit_cooldown(delay)
                await asyncio.sleep(delay)
                attempt += 1

    async def _acompletion_guarded(self, **kwargs: object) -> Any:
        async with self._semaphore:
            timeout_seconds = _coerce_timeout_seconds(kwargs.get("timeout"))
            try:
                response = await _await_with_runtime_timeout(
                    acompletion(
                        model=self._model,
                        api_base=self._api_base,
                        api_key=self._api_key,
                        **kwargs,
                    ),
                    timeout_seconds=timeout_seconds,
                )
            except Exception as exc:
                if not _is_closed_client_error(exc):
                    raise
                logger.warning(
                    "LiteLLM client was closed mid-request; retrying once with a fresh completion call"
                )
                response = await _await_with_runtime_timeout(
                    acompletion(
                        model=self._model,
                        api_base=self._api_base,
                        api_key=self._api_key,
                        **kwargs,
                    ),
                    timeout_seconds=timeout_seconds,
                )
            # LiteLLM can occasionally return a nested awaitable object on
            # provider-error paths (seen on Python 3.14). Unwrap it to avoid
            # "coroutine ... was never awaited" warnings and leaked coroutines.
            while inspect.isawaitable(response):
                response = await _await_with_runtime_timeout(
                    response,
                    timeout_seconds=timeout_seconds,
                )
            return response


def _serialize_message(message: Message | dict) -> dict:
    """Serialize a message to dict format."""
    serialized = dict(message) if isinstance(message, dict) else message.to_dict()

    # Some OpenAI-compatible providers reject assistant tool-call messages when
    # content is null instead of an empty string.
    if serialized.get("tool_calls") and serialized.get("content") is None:
        serialized["content"] = ""
    return serialized


def _coerce_timeout_seconds(value: object) -> float | None:
    try:
        timeout = float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    if timeout is None or timeout <= 0:
        return None
    return timeout


async def _await_with_runtime_timeout(awaitable: Any, *, timeout_seconds: float | None) -> Any:
    if timeout_seconds is None:
        return await awaitable
    return await asyncio.wait_for(awaitable, timeout=timeout_seconds)


def _normalize_plain_messages(messages: list[dict[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for message in messages:
        role_raw = str(message.get("role", "assistant")).strip().lower()
        role = role_raw if role_raw in {"system", "user", "assistant"} else "assistant"
        content = _coerce_content_text(message.get("content"))
        if not content:
            continue
        normalized.append({"role": role, "content": content})

    if not normalized:
        return [{"role": "user", "content": "Continue."}]

    if not any(msg.get("role") == "user" for msg in normalized):
        system_parts = [msg["content"] for msg in normalized if msg["role"] == "system"]
        assistant_parts = [msg["content"] for msg in normalized if msg["role"] == "assistant"]
        composed = []
        if system_parts:
            composed.append("Instructions:\n" + "\n\n".join(system_parts))
        if assistant_parts:
            composed.append("Context:\n" + "\n\n".join(assistant_parts))
        if not composed:
            composed.append("Continue.")
        normalized.append({"role": "user", "content": "\n\n".join(composed).strip()})

    return normalized


def _build_strict_retry_messages(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    lines: list[str] = []
    for msg in messages:
        role = str(msg.get("role", "assistant")).strip().lower()
        content = str(msg.get("content", "")).strip()
        if not content:
            continue
        lines.append(f"{role}: {content}")
    payload = "\n\n".join(lines).strip() or "Continue."
    return [{"role": "user", "content": payload}]


def _coerce_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                item_type = str(item.get("type", "")).lower()
                if item_type == "text":
                    text = str(item.get("text", "")).strip()
                    if text:
                        parts.append(text)
                elif item_type == "image_url":
                    parts.append("[image omitted]")
            elif item is not None:
                raw = str(item).strip()
                if raw:
                    parts.append(raw)
        return "\n".join(parts).strip()
    if content is None:
        return ""
    try:
        return json.dumps(content, ensure_ascii=False).strip()
    except Exception:
        return str(content).strip()


def _looks_like_illegal_messages_error(exc: Exception) -> bool:
    err = str(exc).lower()
    return (
        "messages parameter is illegal" in err or "'code': '1214'" in err or '"code": "1214"' in err
    )


def _is_rate_limit_error(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    err = str(exc).lower()
    markers = (
        "ratelimit",
        "rate limit",
        "error code: 429",
        "status code 429",
        'http_code":"529"',
        "status code 529",
        "server error '529",
        "overloaded_error",
        "under high load",
    )
    return any(marker in name or marker in err for marker in markers)


def _is_closed_client_error(exc: Exception) -> bool:
    err = str(exc).lower()
    return (
        "client has been closed" in err
        or "cannot send a request, as the client has been closed" in err
    )


def _extract_retry_after_seconds(exc: Exception) -> float | None:
    err = str(exc).lower()
    patterns = (
        r"retry[-_ ]after['\":= ]+([0-9]+(?:\.[0-9]+)?)",
        r"try again in ([0-9]+(?:\.[0-9]+)?)s",
        r"wait ([0-9]+(?:\.[0-9]+)?)s",
    )
    for pattern in patterns:
        match = re.search(pattern, err)
        if match:
            try:
                return float(match.group(1))
            except Exception:
                return None
    return None


def _compute_rate_limit_delay(
    *,
    exc: Exception,
    attempt: int,
    base_delay: float,
    max_delay: float,
) -> float:
    hinted_delay = _extract_retry_after_seconds(exc)
    if hinted_delay is not None:
        return max(0.1, min(max_delay, hinted_delay))
    exponential = base_delay * (2**attempt)
    jitter = random.uniform(0.0, base_delay * 0.35)
    return max(0.1, min(max_delay, exponential + jitter))


def _extract_content(response: Any) -> str:
    """Extract content from LiteLLM response."""
    try:
        # LiteLLM returns a Message object or dict
        if hasattr(response, "choices"):
            # Response object
            choice = response.choices[0]
            if hasattr(choice, "message"):
                return choice.message.content or ""
        elif isinstance(response, dict):
            # Dict response
            return response.get("choices", [{}])[0].get("message", {}).get("content") or ""
        return ""
    except Exception as exc:
        logger.warning("Failed to extract content from response: %s", exc)
        return ""


def _extract_tool_calls(response: Any) -> list[dict]:
    """Extract tool calls from LiteLLM response."""
    try:
        tool_calls = []

        if hasattr(response, "choices"):
            # Response object
            choice = response.choices[0]
            if hasattr(choice, "message"):
                message = choice.message
                if hasattr(message, "tool_calls") and message.tool_calls:
                    # Convert to OpenAI format
                    for tc in message.tool_calls:
                        tool_calls.append(
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                },
                            }
                        )
        elif isinstance(response, dict):
            # Dict response - OpenAI format
            message = response.get("choices", [{}])[0].get("message", {})
            raw_tool_calls = message.get("tool_calls")
            if raw_tool_calls:
                if isinstance(raw_tool_calls, list):
                    tool_calls = raw_tool_calls
                elif isinstance(raw_tool_calls, dict):
                    tool_calls = [raw_tool_calls]

        return tool_calls
    except Exception as exc:
        logger.warning("Failed to extract tool calls from response: %s", exc)
        return []


def _extract_stream_delta(chunk: Any) -> str:
    """Extract incremental text from a LiteLLM stream chunk."""
    try:
        choices = getattr(chunk, "choices", None)
        if choices is None and isinstance(chunk, dict):
            choices = chunk.get("choices")
        if not choices:
            return ""
        first = choices[0]
        delta_obj = getattr(first, "delta", None)
        if delta_obj is None and isinstance(first, dict):
            delta_obj = first.get("delta")
        if delta_obj is None:
            return ""
        content = getattr(delta_obj, "content", None)
        if content is None and isinstance(delta_obj, dict):
            content = delta_obj.get("content")
        if content is None:
            return ""
        return str(content)
    except Exception:
        return ""


def _truncate(text: str) -> str:
    """Truncate text for logging."""
    if text is None:
        return ""
    if len(text) <= _LOG_MAX_CHARS:
        return text
    return text[:_LOG_MAX_CHARS] + f"...[truncated {len(text)} bytes]"


def _extract_usage(response: Any) -> dict[str, int]:
    """Extract token usage if available."""
    try:
        usage_obj = None
        if hasattr(response, "usage"):
            usage_obj = response.usage
        elif isinstance(response, dict):
            usage_obj = response.get("usage")
        if not usage_obj:
            return {}
        if isinstance(usage_obj, dict):
            return {
                k: int(v)
                for k, v in usage_obj.items()
                if isinstance(k, str) and isinstance(v, int | float)
            }
        result: dict[str, int] = {}
        for field in ("prompt_tokens", "completion_tokens", "total_tokens"):
            value = getattr(usage_obj, field, None)
            if isinstance(value, int | float):
                result[field] = int(value)
        return result
    except Exception:
        return {}


def _extract_finish_reason(response: Any) -> str | None:
    try:
        choices = getattr(response, "choices", None)
        if choices is None and isinstance(response, dict):
            choices = response.get("choices")
        if not choices:
            return None
        first = choices[0]
        finish_reason = getattr(first, "finish_reason", None)
        if finish_reason is None and isinstance(first, dict):
            finish_reason = first.get("finish_reason")
        return str(finish_reason) if finish_reason is not None else None
    except Exception:
        return None


def _build_request_kwargs(kwargs: dict[str, object], **defaults: object) -> dict[str, object]:
    request_kwargs: dict[str, object] = {
        "temperature": defaults["temperature"],
        "timeout": defaults["timeout"],
        "fallbacks": defaults["fallbacks"],
    }
    passthrough_keys = (
        "response_format",
        "max_tokens",
        "top_p",
        "seed",
        "stop",
        "presence_penalty",
        "frequency_penalty",
    )
    for key in passthrough_keys:
        if key in kwargs and kwargs[key] is not None:
            request_kwargs[key] = kwargs[key]
    return request_kwargs


def _sanitize_request_kwargs_for_provider(
    request_kwargs: dict[str, object],
    *,
    provider_id: str,
) -> dict[str, object]:
    if provider_id != "minimax":
        return request_kwargs

    sanitized = dict(request_kwargs)
    response_format = sanitized.get("response_format")
    if isinstance(response_format, dict):
        sanitized["response_format"] = _sanitize_response_format_for_minimax(response_format)
    return sanitized


def _sanitize_tools_for_provider(
    tools: list[dict[str, Any]],
    provider_id: str,
) -> list[dict[str, Any]]:
    if provider_id != "minimax":
        return tools

    sanitized_tools: list[dict[str, Any]] = []
    for tool in tools:
        next_tool = copy.deepcopy(tool)
        function = next_tool.get("function")
        if isinstance(function, dict) and isinstance(function.get("parameters"), dict):
            function["parameters"] = _sanitize_schema_for_minimax(function["parameters"])
        sanitized_tools.append(next_tool)
    return sanitized_tools


def _sanitize_response_format_for_minimax(response_format: dict[str, Any]) -> dict[str, Any]:
    sanitized = copy.deepcopy(response_format)
    json_schema = sanitized.get("json_schema")
    if not isinstance(json_schema, dict):
        return sanitized

    schema = json_schema.get("schema")
    if isinstance(schema, dict):
        json_schema["schema"] = _sanitize_schema_for_minimax(schema)
    return sanitized


def _sanitize_schema_for_minimax(schema: Any) -> Any:
    if isinstance(schema, list):
        return [_sanitize_schema_for_minimax(item) for item in schema]
    if not isinstance(schema, dict):
        return schema

    sanitized = {key: _sanitize_schema_for_minimax(value) for key, value in schema.items()}

    for union_key in ("anyOf", "oneOf"):
        variants = sanitized.get(union_key)
        if isinstance(variants, list):
            flattened = _flatten_schema_union_for_minimax(variants)
            if flattened is not None:
                for meta_key in ("title", "description", "default"):
                    if meta_key in sanitized and meta_key not in flattened:
                        flattened[meta_key] = sanitized[meta_key]
                sanitized = flattened
                break

    type_value = sanitized.get("type")
    if isinstance(type_value, list):
        sanitized["type"] = _select_minimax_type(type_value)

    return sanitized


def _flatten_schema_union_for_minimax(variants: list[Any]) -> dict[str, Any] | None:
    cleaned_variants = [variant for variant in variants if not _is_null_schema_variant(variant)]
    if len(cleaned_variants) == 1 and isinstance(cleaned_variants[0], dict):
        return copy.deepcopy(cleaned_variants[0])

    enum_values = _extract_enum_values_from_variants(cleaned_variants)
    if enum_values is not None:
        flattened: dict[str, Any] = {"enum": enum_values}
        common_type = _detect_common_enum_type(enum_values)
        if common_type:
            flattened["type"] = common_type
        return flattened

    object_variants = [variant for variant in cleaned_variants if isinstance(variant, dict)]
    if object_variants and len(object_variants) == len(cleaned_variants):
        merged_properties: dict[str, Any] = {}
        required_counts: dict[str, int] = {}
        for variant in object_variants:
            properties = variant.get("properties")
            if isinstance(properties, dict):
                for key, value in properties.items():
                    if key not in merged_properties:
                        merged_properties[key] = value
            required = variant.get("required")
            if isinstance(required, list):
                for key in required:
                    if isinstance(key, str):
                        required_counts[key] = required_counts.get(key, 0) + 1

        flattened = {
            "type": "object",
            "properties": merged_properties,
            "additionalProperties": sanitized_bool_from_variants(
                object_variants, "additionalProperties", True
            ),
        }
        required = [key for key, count in required_counts.items() if count == len(object_variants)]
        if required:
            flattened["required"] = required
        return flattened

    return None


def sanitized_bool_from_variants(variants: list[dict[str, Any]], key: str, default: bool) -> bool:
    for variant in variants:
        value = variant.get(key)
        if isinstance(value, bool):
            return value
    return default


def _is_null_schema_variant(variant: Any) -> bool:
    if not isinstance(variant, dict):
        return False
    if variant.get("type") == "null":
        return True
    enum_values = variant.get("enum")
    return isinstance(enum_values, list) and enum_values == [None]


def _extract_enum_values_from_variants(variants: list[Any]) -> list[Any] | None:
    values: list[Any] = []
    for variant in variants:
        if not isinstance(variant, dict):
            return None
        if "const" in variant:
            values.append(variant["const"])
            continue
        enum_values = variant.get("enum")
        if isinstance(enum_values, list) and len(enum_values) == 1:
            values.append(enum_values[0])
            continue
        return None
    return values


def _detect_common_enum_type(values: list[Any]) -> str | None:
    type_names = {type(value).__name__ for value in values}
    mapping = {
        frozenset({"str"}): "string",
        frozenset({"int"}): "integer",
        frozenset({"float"}): "number",
        frozenset({"bool"}): "boolean",
    }
    return mapping.get(frozenset(type_names))


def _select_minimax_type(type_values: list[Any]) -> str:
    normalized = [str(value).strip().lower() for value in type_values if str(value).strip()]
    for preferred in ("object", "array", "string", "integer", "number", "boolean", "null"):
        if preferred in normalized:
            return preferred
    return normalized[0] if normalized else "string"


def _response_format_fallback_modes(
    response_format: object,
    *,
    preferred_mode: str | None = None,
) -> list[str]:
    format_type = _response_format_type(response_format)
    if format_type == "json_schema":
        ordered = ["json_schema", "json_object", "none"]
    elif format_type == "json_object":
        ordered = ["json_object", "none"]
    else:
        ordered = ["none"]

    if preferred_mode in ordered:
        ordered.remove(preferred_mode)
        ordered.insert(0, preferred_mode)
    return ordered


def _response_format_type(response_format: object) -> str | None:
    if not isinstance(response_format, dict):
        return None
    raw_type = response_format.get("type")
    if not isinstance(raw_type, str):
        return None
    value = raw_type.strip().lower()
    return value or None


def _apply_response_format_mode(
    request_kwargs: dict[str, object],
    requested_response_format: object,
    mode: str,
) -> None:
    if mode == "json_schema" and requested_response_format is not None:
        request_kwargs["response_format"] = requested_response_format
        return
    if mode == "json_object":
        request_kwargs["response_format"] = {"type": "json_object"}
        return
    request_kwargs.pop("response_format", None)


def _is_response_format_unsupported_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = (
        "response_format",
        "json_schema",
        "structured output",
        "must be text or json_object",
        "must be text or json object",
        "must be json_object",
        "must be text",
        "unsupported",
        "not supported",
        "invalid_request_error",
        "input_invalid",
    )
    return any(marker in text for marker in markers)


def _is_tool_choice_required_unsupported_in_thinking_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "tool_choice" in text
        and "thinking mode" in text
        and ("required" in text or "object" in text)
    )


def _summarize_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for message in messages[:24]:
        content = message.get("content")
        summary.append(
            {
                "role": message.get("role"),
                "content_type": type(content).__name__,
                "content_len": len(content) if isinstance(content, str) else None,
                "has_tool_calls": bool(message.get("tool_calls")),
            }
        )
    return summary
