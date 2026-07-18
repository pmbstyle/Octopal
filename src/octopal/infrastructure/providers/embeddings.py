from __future__ import annotations

from typing import Protocol


class EmbeddingsProvider(Protocol):
    model_id: str

    async def embed(self, texts: list[str]) -> list[list[float]]: ...

    async def embed_documents(self, texts: list[str]) -> list[list[float]]: ...

    async def embed_queries(self, texts: list[str]) -> list[list[float]]: ...
