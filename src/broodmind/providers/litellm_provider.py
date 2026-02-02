"""LiteLLM-based inference provider with retry logic and fallbacks."""

from __future__ import annotations

import json
import logging
from typing import Any

from litellm import acompletion

from broodmind.config.settings import Settings
from broodmind.providers.base import Message

logger = logging.getLogger(__name__)

_LOG_MAX_CHARS = 2000


class LiteLLMProvider:
    """LiteLLM-based inference provider with automatic retries and fallbacks."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Construct model name for custom OpenAI-compatible endpoint
        # Format: openai/{model} for custom base URL
        self._model = f"openai/{settings.zai_model}"
        # LiteLLM automatically appends /chat/completions for openai/ model format
        # So we only use the base URL here, not including the chat path
        self._api_base = settings.zai_base_url.rstrip("/")
        self._api_key = settings.zai_api_key

        # Parse fallbacks from JSON string if provided
        self._fallbacks: list[dict[str, Any]] | None = None
        if settings.litellm_fallbacks:
            try:
                self._fallbacks = json.loads(settings.litellm_fallbacks)
            except Exception as exc:
                logger.warning("Failed to parse LITELLM_FALLBACKS JSON: %s", exc)

        # Configure litellm at module level
        import litellm
        
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
            raise RuntimeError("ZAI_API_KEY is not set")

        serialized_messages = [_serialize_message(m) for m in messages]

        if self._settings.debug_prompts:
            logger.debug(
                "LiteLLM payload: %s",
                _truncate(json.dumps({"messages": serialized_messages}, ensure_ascii=False)),
            )

        logger.debug(
            "LiteLLM request: model=%s messages=%s",
            self._model,
            len(serialized_messages),
        )

        try:
            response = await acompletion(
                model=self._model,
                messages=serialized_messages,
                api_base=self._api_base,
                api_key=self._api_key,
                temperature=float(kwargs.get("temperature", 0.3)),
                timeout=self._settings.litellm_timeout,
                fallbacks=self._fallbacks,
            )
            content = _extract_content(response)
            logger.debug("LiteLLM response: %s", _truncate(content))
            return content
        except Exception as exc:
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
            raise RuntimeError("ZAI_API_KEY is not set")

        serialized_messages = [_serialize_message(m) for m in messages]

        if self._settings.debug_prompts:
            logger.debug(
                "LiteLLM payload (tools): %s",
                _truncate(
                    json.dumps(
                        {"messages": serialized_messages, "tools": tools, "tool_choice": tool_choice},
                        ensure_ascii=False,
                    )
                ),
            )

        logger.debug(
            "LiteLLM request (tools): model=%s messages=%s tools=%s",
            self._model,
            len(serialized_messages),
            len(tools),
        )

        try:
            response = await acompletion(
                model=self._model,
                messages=serialized_messages,
                tools=tools,
                tool_choice=tool_choice,
                api_base=self._api_base,
                api_key=self._api_key,
                temperature=float(kwargs.get("temperature", 0.3)),
                timeout=self._settings.litellm_timeout,
                fallbacks=self._fallbacks,
            )

            content = _extract_content(response)
            tool_calls = _extract_tool_calls(response)

            if content:
                logger.debug("LiteLLM response (tools) content: %s", _truncate(content))
            if tool_calls:
                logger.debug("LiteLLM response (tools) tool_calls=%s", len(tool_calls))
                if self._settings.debug_prompts:
                    logger.debug(
                        "LiteLLM response (tools) tool_calls payload: %s",
                        _truncate(json.dumps(tool_calls, ensure_ascii=False)),
                    )

            return {"content": content, "tool_calls": tool_calls}
        except Exception as exc:
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
    return text[:_LOG_MAX_CHARS] + "...[truncated]"
