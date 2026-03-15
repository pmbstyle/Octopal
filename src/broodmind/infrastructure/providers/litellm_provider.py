"""LiteLLM-based inference provider with retry logic and fallbacks."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
import re
from collections.abc import Awaitable, Callable
from typing import Any

from litellm import acompletion

from broodmind.infrastructure.config.settings import Settings
from broodmind.infrastructure.providers.base import Message
from broodmind.infrastructure.providers.profile_resolver import resolve_litellm_profile

logger = logging.getLogger(__name__)

_LOG_MAX_CHARS = 400  # Reduced from 2000


class LiteLLMProvider:
    """LiteLLM-based inference provider with automatic retries and fallbacks.
    Supports both OpenRouter and z.ai (custom OpenAI-compatible endpoints).
    """

    _semaphores_by_limit: dict[int, asyncio.Semaphore] = {}

    def __init__(self, settings: Settings, model: str | None = None) -> None:
        self._settings = settings
        self._profile = resolve_litellm_profile(settings, model_override=model)
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
        self._semaphore = self._semaphores_by_limit.setdefault(max_concurrency, asyncio.Semaphore(max_concurrency))
        self._rate_limit_max_retries = max(0, int(settings.litellm_rate_limit_max_retries))
        self._rate_limit_base_delay = max(0.1, float(settings.litellm_rate_limit_base_delay_seconds))
        self._rate_limit_max_delay = max(self._rate_limit_base_delay, float(settings.litellm_rate_limit_max_delay_seconds))

    @property
    def provider_id(self) -> str:
        return self._profile.provider_id

    async def complete(self, messages: list[Message | dict], **kwargs: object) -> str:
        """Complete a chat request without tools."""
        if self._profile.requires_api_key and not self._api_key:
            raise RuntimeError(
                "API key is not configured for the active LiteLLM provider. "
                "Set BROODMIND_LITELLM_API_KEY or configure a provider-specific legacy key."
            )

        serialized_messages = _normalize_plain_messages([_serialize_message(m) for m in messages])
        payload_str = json.dumps({"messages": serialized_messages}, ensure_ascii=False)

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
            logger.debug("LiteLLM response: %s", _truncate(content))
            return content
        except Exception as exc:
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
                "Set BROODMIND_LITELLM_API_KEY or configure a provider-specific legacy key."
            )

        serialized_messages = _normalize_plain_messages([_serialize_message(m) for m in messages])
        payload_str = json.dumps({"messages": serialized_messages, "stream": True}, ensure_ascii=False)
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
            response = await self._acompletion_with_resilience(messages=serialized_messages, **request_kwargs)
            if not hasattr(response, "__aiter__"):
                # Provider did not return an async stream; gracefully fall back to non-stream.
                text = _extract_content(response)
                if text:
                    try:
                        await on_partial(text)
                    except Exception:
                        logger.debug("LiteLLM partial callback failed", exc_info=True)
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
            return accumulated
        except Exception as exc:
            logger.error(
                "LiteLLM stream payload shape on error: %s",
                _summarize_messages(serialized_messages),
            )
            logger.exception("LiteLLM stream completion failed")
            raise RuntimeError(f"LiteLLM stream completion failed: {exc}") from exc

    async def complete_with_tools(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict],
        tool_choice: str = "auto",
        **kwargs: object,
    ) -> dict:
        """Complete a chat request with tool/function calling."""
        if self._profile.requires_api_key and not self._api_key:
            raise RuntimeError(
                "API key is not configured for the active LiteLLM provider. "
                "Set BROODMIND_LITELLM_API_KEY or configure a provider-specific legacy key."
            )

        serialized_messages = [_serialize_message(m) for m in messages]
        payload_str = json.dumps(
            {"messages": serialized_messages, "tools": tools, "tool_choice": tool_choice},
            ensure_ascii=False,
        )
        tool_names = [t.get("function", {}).get("name") for t in tools]

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
            response = await self._acompletion_with_resilience(
                messages=serialized_messages,
                tools=tools,
                tool_choice=tool_choice,
                **request_kwargs,
            )

            content = _extract_content(response)
            tool_calls = _extract_tool_calls(response)
            usage = _extract_usage(response)

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

            return {"content": content, "tool_calls": tool_calls, "usage": usage}
        except Exception as exc:
            err_str = str(exc)
            logger.error(
                "LiteLLM completion-with-tools payload shape on error: messages=%s tool_count=%s",
                _summarize_messages(serialized_messages),
                len(tools),
            )
            if "finish_reason" in err_str and "abort" in err_str:
                logger.error("LLM provider returned invalid 'abort' finish_reason. This is a known compatibility issue with some providers.")
                raise RuntimeError(
                    "LLM provider (z.ai) returned an invalid response code ('abort'). "
                    "This usually means the model was cut off or is having internal issues. "
                    "Please try your request again or check the provider status."
                ) from exc
            
            logger.exception("LiteLLM completion with tools failed")
            raise RuntimeError(f"LiteLLM completion with tools failed: {exc}") from exc

    async def _acompletion_with_resilience(self, **kwargs: object) -> Any:
        attempt = 0
        while True:
            try:
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
                await asyncio.sleep(delay)
                attempt += 1

    async def _acompletion_guarded(self, **kwargs: object) -> Any:
        async with self._semaphore:
            try:
                response = await acompletion(
                    model=self._model,
                    api_base=self._api_base,
                    api_key=self._api_key,
                    **kwargs,
                )
            except Exception as exc:
                if not _is_closed_client_error(exc):
                    raise
                logger.warning("LiteLLM client was closed mid-request; retrying once with a fresh completion call")
                response = await acompletion(
                    model=self._model,
                    api_base=self._api_base,
                    api_key=self._api_key,
                    **kwargs,
                )
            # LiteLLM can occasionally return a nested awaitable object on
            # provider-error paths (seen on Python 3.14). Unwrap it to avoid
            # "coroutine ... was never awaited" warnings and leaked coroutines.
            while inspect.isawaitable(response):
                response = await response
            return response


def _serialize_message(message: Message | dict) -> dict:
    """Serialize a message to dict format."""
    if isinstance(message, dict):
        serialized = dict(message)
    else:
        serialized = message.to_dict()

    # Some OpenAI-compatible providers reject assistant tool-call messages when
    # content is null instead of an empty string.
    if serialized.get("tool_calls") and serialized.get("content") is None:
        serialized["content"] = ""
    return serialized


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
    return "messages parameter is illegal" in err or "'code': '1214'" in err or '"code": "1214"' in err


def _is_rate_limit_error(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    err = str(exc).lower()
    return "ratelimit" in name or "rate limit" in err or "error code: 429" in err or "status code 429" in err


def _is_closed_client_error(exc: Exception) -> bool:
    err = str(exc).lower()
    return "client has been closed" in err or "cannot send a request, as the client has been closed" in err


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
                if isinstance(k, str) and isinstance(v, (int, float))
            }
        result: dict[str, int] = {}
        for field in ("prompt_tokens", "completion_tokens", "total_tokens"):
            value = getattr(usage_obj, field, None)
            if isinstance(value, (int, float)):
                result[field] = int(value)
        return result
    except Exception:
        return {}


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
