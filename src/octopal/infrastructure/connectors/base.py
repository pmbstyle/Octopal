from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Connector(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """The unique name of the connector (e.g., 'google')."""
        pass

    @abstractmethod
    async def get_status(self) -> dict[str, Any]:
        """Return the current status of the connector."""
        pass

    @abstractmethod
    async def configure(self, settings: dict[str, Any]) -> None:
        """Configure the connector with the given settings."""
        pass

    @abstractmethod
    async def authorize(self) -> dict[str, Any]:
        """Run an authorization flow for an already-enabled connector."""
        pass

    @abstractmethod
    async def start(self) -> None:
        """Start any runtime integrations needed by the connector."""
        pass
