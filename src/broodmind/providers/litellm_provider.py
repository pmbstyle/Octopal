"""LiteLLM-based inference provider with retry logic and fallbacks."""

from __future__ import annotations

import json
import logging
from typing import Any

from litellm import acompletion

from broodmind.config.settings import Settings
from broodmind.providers.base import Message

logger = logging.getLogger(__name__)

_LOG_MAX_CHARS = 400  # Reduced from 2000


class LiteLLMProvider:
    """LiteLLM-based inference provider with automatic retries and fallbacks.
    Supports both OpenRouter and z.ai (custom OpenAI-compatible endpoints).
    """

    def __init__(self, settings: Settings, model: str | None = None) -> None:
        self._settings = settings

        # Auto-detect which provider to use based on settings
        # Priority: 1) llm_provider setting, 2) API key presence
        use_openrouter = settings.llm_provider == "openrouter" or (
            settings.openrouter_api_key and not settings.zai_api_key
        )

        if use_openrouter:
            # Use OpenRouter via LiteLLM
            model_name = model or settings.openrouter_model
            # If the provided model already has the provider prefix, use it as is
            if model_name.startswith("openrouter/"):
                self._model = model_name
            else:
                self._model = f"openrouter/{model_name}"
            self._api_base = settings.openrouter_base_url.rstrip("/")
            self._api_key = settings.openrouter_api_key
            logger.info("LiteLLM configured for OpenRouter: model=%s, base_url=%s", self._model, self._api_base)
        else:
            # Use z.ai (custom OpenAI-compatible endpoint)
            model_name = model or settings.zai_model
            # If the provided model already has a prefix, use it as is
            if "/" in model_name:
                self._model = model_name
            else:
                # Default to openai/ prefix for compatibility with LiteLLM's custom endpoints
                self._model = f"openai/{model_name}"
            self._api_base = settings.zai_base_url.rstrip("/")
            self._api_key = settings.zai_api_key
            logger.info("LiteLLM configured for z.ai: model=%s, base_url=%s", self._model, self._api_base)

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

    async def complete(self, messages: list[Message | dict], **kwargs: object) -> str:
        """Complete a chat request without tools."""
        if not self._api_key:
            raise RuntimeError("API key is not configured. Set OPENROUTER_API_KEY or ZAI_API_KEY.")

        serialized_messages = [_serialize_message(m) for m in messages]
        payload_str = json.dumps({"messages": serialized_messages}, ensure_ascii=False)

        logger.debug(
            "LiteLLM request: model=%s, messages=%d, total_chars=%d",
            self._model,
            len(serialized_messages),
            len(payload_str),
        )

        if self._settings.debug_prompts:
            logger.debug("LiteLLM payload: %s", _truncate(payload_str))

        try:
            request_kwargs = _build_request_kwargs(
                kwargs,
                temperature=float(kwargs.get("temperature", 0.3)),
                timeout=self._settings.litellm_timeout,
                fallbacks=self._fallbacks,
            )
            response = await acompletion(
                model=self._model,
                messages=serialized_messages,
                api_base=self._api_base,
                api_key=self._api_key,
                **request_kwargs,
            )
            content = _extract_content(response)
            logger.debug("LiteLLM response: %s", _truncate(content))
            return content
        except Exception as exc:
            err_str = str(exc)
            if "finish_reason" in err_str and "abort" in err_str:
                logger.error("LLM provider returned invalid 'abort' finish_reason.")
                raise RuntimeError(
                    "LLM provider (z.ai) returned an invalid response code ('abort'). "
                    "This usually means the model was cut off or is having internal issues. "
                    "Please try your request again."
                ) from exc
            
            logger.exception("LiteLLM completion failed")
            raise RuntimeError(f"LiteLLM completion failed: {exc}") from exc

    async def complete_with_tools(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict],
        tool_choice: str = "auto",
        **kwargs: object,
    ) -> dict:
        """Complete a chat request with tool/function calling."""
        if not self._api_key:
            raise RuntimeError("API key is not configured. Set OPENROUTER_API_KEY or ZAI_API_KEY.")

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
            response = await acompletion(
                model=self._model,
                messages=serialized_messages,
                tools=tools,
                tool_choice=tool_choice,
                api_base=self._api_base,
                api_key=self._api_key,
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
            if "finish_reason" in err_str and "abort" in err_str:
                logger.error("LLM provider returned invalid 'abort' finish_reason. This is a known compatibility issue with some providers.")
                raise RuntimeError(
                    "LLM provider (z.ai) returned an invalid response code ('abort'). "
                    "This usually means the model was cut off or is having internal issues. "
                    "Please try your request again or check the provider status."
                ) from exc
            
            logger.exception("LiteLLM completion with tools failed")
            raise RuntimeError(f"LiteLLM completion with tools failed: {exc}") from exc


def _serialize_message(message: Message | dict) -> dict:
    """Serialize a message to dict format."""
    if isinstance(message, dict):
        return message
    return message.to_dict()


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
