from __future__ import annotations

import logging

import httpx

from octopal.infrastructure.config.settings import Settings

logger = logging.getLogger(__name__)


class OpenAIEmbeddingsProvider:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not self._settings.openai_api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")

        payload = {
            "model": self._settings.openai_embed_model,
            "input": texts,
            "encoding_format": "float",
        }

        timeout = httpx.Timeout(30.0, connect=10.0)
        async with httpx.AsyncClient(
            base_url=self._settings.openai_base_url, timeout=timeout
        ) as client:
            response = await client.post(
                "/embeddings",
                headers={"Authorization": f"Bearer {self._settings.openai_api_key}"},
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

        try:
            embeddings = [item["embedding"] for item in data["data"]]
        except (KeyError, TypeError) as exc:
            logger.exception("Unexpected OpenAI embeddings response: %s", data)
            raise RuntimeError("Unexpected embeddings response format") from exc
        return embeddings
