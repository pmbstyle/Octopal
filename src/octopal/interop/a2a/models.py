from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class A2APart(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    text: str | None = None
    raw: str | None = None
    url: str | None = None
    data: Any = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    filename: str | None = None
    media_type: str | None = Field(default=None, alias="mediaType")

    @model_validator(mode="after")
    def validate_content_oneof(self) -> A2APart:
        present = [
            name for name in ("text", "raw", "url", "data") if getattr(self, name) is not None
        ]
        if len(present) > 1:
            raise ValueError("A2A part must contain exactly one of text, raw, url, or data")
        return self


class A2AMessage(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    role: str = "ROLE_USER"
    parts: list[A2APart] = Field(default_factory=list)
    message_id: str | None = Field(default=None, alias="messageId")
    context_id: str | None = Field(default=None, alias="contextId")
    task_id: str | None = Field(default=None, alias="taskId")
    metadata: dict[str, Any] = Field(default_factory=dict)


class A2AMessageSendRequest(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    message: A2AMessage
    metadata: dict[str, Any] = Field(default_factory=dict)


def message_text(message: A2AMessage) -> str:
    parts = [str(part.text or "").strip() for part in message.parts]
    return "\n\n".join(part for part in parts if part).strip()


def message_content_for_octo(message: A2AMessage) -> str:
    rendered: list[str] = []
    for index, part in enumerate(message.parts, start=1):
        if part.text is not None:
            text = str(part.text or "").strip()
            if text:
                rendered.append(text)
            continue
        if part.data is not None:
            rendered.append(f"Structured data part {index}:\n{_json_preview(part.data)}")
            continue
        if part.url is not None:
            url = str(part.url or "").strip()
            if url:
                label = _part_label(part)
                rendered.append(f"File URL part {index}{label}: {url}")
            continue
        if part.raw is not None:
            label = _part_label(part)
            rendered.append(f"Raw file part {index}{label}: saved as a local attachment.")
    return "\n\n".join(item for item in rendered if item).strip()


def message_payload_size(message: A2AMessage) -> int:
    total = 0
    for part in message.parts:
        if part.text is not None:
            total += len(str(part.text))
        elif part.raw is not None:
            total += len(str(part.raw))
        elif part.url is not None:
            total += len(str(part.url))
        elif part.data is not None:
            total += len(_json_preview(part.data))
    return total


def _json_preview(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    except TypeError:
        return str(value)


def _part_label(part: A2APart) -> str:
    labels = []
    if part.filename:
        labels.append(f"filename={part.filename}")
    if part.media_type:
        labels.append(f"mediaType={part.media_type}")
    return f" ({', '.join(labels)})" if labels else ""
