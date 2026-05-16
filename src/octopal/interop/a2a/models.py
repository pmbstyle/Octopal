from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class A2APart(BaseModel):
    model_config = ConfigDict(extra="allow")

    text: str | None = None


class A2AMessage(BaseModel):
    model_config = ConfigDict(extra="allow")

    role: str = "ROLE_USER"
    parts: list[A2APart] = Field(default_factory=list)
    message_id: str | None = Field(default=None, alias="messageId")
    context_id: str | None = Field(default=None, alias="contextId")
    task_id: str | None = Field(default=None, alias="taskId")
    metadata: dict[str, Any] = Field(default_factory=dict)


class A2AMessageSendRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: A2AMessage
    metadata: dict[str, Any] = Field(default_factory=dict)


def message_text(message: A2AMessage) -> str:
    parts = [str(part.text or "").strip() for part in message.parts]
    return "\n\n".join(part for part in parts if part).strip()

