from __future__ import annotations

from pathlib import Path

from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.providers.embeddings import EmbeddingsProvider
from octopal.infrastructure.providers.local_onnx_embeddings import LocalOnnxEmbeddingsProvider


def build_local_embeddings_provider(settings: Settings) -> EmbeddingsProvider:
    """Load the mandatory local semantic-memory runtime during Octopal startup."""
    configured_dir = settings.memory_local_embedding_model_dir
    model_dir = (
        Path(configured_dir)
        if configured_dir is not None
        else (settings.state_dir / "models" / "multilingual-e5-small")
    )
    provider = LocalOnnxEmbeddingsProvider(
        model_dir,
        threads=settings.memory_local_embedding_threads,
    )
    try:
        provider.warm_up()
    except RuntimeError as exc:
        raise RuntimeError(
            "Local semantic embeddings are required but unavailable: "
            f"{exc}. Run `uv sync` and retry startup. Assets are stored in {model_dir}."
        ) from exc
    return provider
