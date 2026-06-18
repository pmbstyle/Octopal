from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from octopal.infrastructure.providers.base import InferenceProvider, Message
from octopal.runtime.tool_payloads import render_tool_result_for_llm

logger = structlog.get_logger(__name__)


async def _complete_text(
    provider: InferenceProvider,
    messages: list[Message | dict[str, Any]],
    *,
    context: str,
    on_partial: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    sanitized = _sanitize_messages_for_complete(messages)
    try:
        if callable(on_partial):
            stream_callable = getattr(provider, "complete_stream", None)
            if callable(stream_callable):
                return await stream_callable(sanitized, on_partial=on_partial)
        text = await provider.complete(sanitized)
        if callable(on_partial) and text:
            try:
                await on_partial(text)
            except Exception:
                logger.debug(
                    "Partial callback failed on non-stream completion",
                    context=context,
                    exc_info=True,
                )
        return text
    except Exception:
        logger.debug(
            "Text completion failed after sanitization",
            context=context,
            message_shape=_message_shape(sanitized),
            exc_info=True,
        )
        raise


def _sanitize_messages_for_complete(
    messages: list[Message | dict[str, Any]],
) -> list[dict[str, str]]:
    sanitized: list[dict[str, str]] = []
    for msg in messages:
        role: str
        content: Any
        tool_name = ""
        if isinstance(msg, Message):
            role = msg.role
            content = msg.content
        else:
            role = str(msg.get("role", "assistant"))
            content = msg.get("content", "")
            if role == "tool":
                tool_name = str(msg.get("name", "") or msg.get("tool_name", "") or "")

        normalized_role = role if role in {"system", "user", "assistant"} else "assistant"
        if role == "tool":
            normalized_content = _coerce_tool_message_to_text(content, tool_name=tool_name)
            if not normalized_content:
                continue
            sanitized.append({"role": "assistant", "content": normalized_content})
            continue

        normalized_content = _coerce_content_to_text(content)
        if not normalized_content:
            continue

        sanitized.append({"role": normalized_role, "content": normalized_content})

    if not sanitized:
        sanitized.append({"role": "user", "content": "Continue."})
    elif not any(msg.get("role") == "user" for msg in sanitized):
        sanitized.append(
            {
                "role": "user",
                "content": "Please follow the instructions above and provide the best supported response.",
            }
        )
    return sanitized


def _coerce_content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type", "")).lower()
            if item_type == "text":
                text_val = str(item.get("text", "")).strip()
                if text_val:
                    text_parts.append(text_val)
            elif item_type == "image_url":
                text_parts.append("[image omitted for text-only completion]")
        return "\n".join(text_parts).strip()
    try:
        return json.dumps(content, ensure_ascii=False)
    except Exception:
        return str(content or "")


def _coerce_tool_message_to_text(content: Any, *, tool_name: str = "") -> str:
    rendered = render_tool_result_for_llm(content, max_chars=16000).text
    if not rendered:
        return ""
    label = tool_name.strip() or "tool"
    return f"Tool result ({label}): {rendered}"


def _message_shape(messages: list[dict[str, str]]) -> list[dict[str, Any]]:
    shape: list[dict[str, Any]] = []
    for msg in messages[:24]:
        content = msg.get("content", "")
        shape.append(
            {
                "role": msg.get("role"),
                "content_type": type(content).__name__,
                "content_len": len(content) if isinstance(content, str) else None,
            }
        )
    return shape
