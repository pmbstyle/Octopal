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
    async def setup(self) -> dict[str, Any]:
        """Start the setup/auth process. Returns info for the user (e.g., auth URL)."""
        pass

    @abstractmethod
    async def complete_setup(self, data: dict[str, Any]) -> dict[str, Any]:
        """Complete the setup/auth process with data (e.g., auth code)."""
        pass
