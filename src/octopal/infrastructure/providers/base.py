from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol


class ProviderAdmissionDeferred(RuntimeError):
    """The provider turn was not started because its admission slot was busy."""

    def __init__(
        self,
        *,
        lane: str,
        wait_seconds: float,
        retry_after_seconds: float,
    ) -> None:
        self.lane = lane
        self.wait_seconds = max(0.0, float(wait_seconds))
        self.retry_after_seconds = max(0.0, float(retry_after_seconds))
        super().__init__(
            f"provider admission deferred for lane={lane!r} " f"after {self.wait_seconds:.3f}s"
        )


@dataclass(frozen=True)
class Message:
    role: str
    content: str | list[dict[str, Any]]

    def to_dict(self) -> dict:
        return {"role": self.role, "content": self.content}


class InferenceProvider(Protocol):
    async def complete(self, messages: list[Message | dict], **kwargs: object) -> str: ...

    async def complete_stream(
        self,
        messages: list[Message | dict],
        *,
        on_partial: Callable[[str], Awaitable[None]],
        **kwargs: object,
    ) -> str: ...

    async def complete_with_tools(
        self,
        messages: list[Message | dict],
        *,
        tools: list[dict],
        tool_choice: str = "auto",
        **kwargs: object,
    ) -> dict: ...
