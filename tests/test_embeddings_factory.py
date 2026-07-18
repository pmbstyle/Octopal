from __future__ import annotations

from pathlib import Path

import pytest

from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.providers.embeddings_factory import build_local_embeddings_provider
from octopal.infrastructure.providers.local_onnx_embeddings import (
    LocalOnnxAvailability,
    LocalOnnxEmbeddingsProvider,
)


def test_local_embeddings_fail_startup_when_runtime_is_unavailable(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        LocalOnnxEmbeddingsProvider,
        "ensure_assets",
        classmethod(lambda cls, _path: None),
    )
    monkeypatch.setattr(
        LocalOnnxEmbeddingsProvider,
        "availability",
        classmethod(lambda cls, _path: LocalOnnxAvailability(False, "dependency missing")),
    )
    settings = Settings(OCTOPAL_MEMORY_LOCAL_EMBEDDING_MODEL_DIR=tmp_path)

    with pytest.raises(RuntimeError, match="Local semantic embeddings are required"):
        build_local_embeddings_provider(settings)


def test_local_embeddings_warm_up_the_configured_model(tmp_path, monkeypatch) -> None:
    model_dir = Path(tmp_path)
    warmed: list[bool] = []
    monkeypatch.setattr(LocalOnnxEmbeddingsProvider, "warm_up", lambda _self: warmed.append(True))
    settings = Settings(
        OCTOPAL_MEMORY_LOCAL_EMBEDDING_MODEL_DIR=model_dir,
        OCTOPAL_MEMORY_LOCAL_EMBEDDING_THREADS=2,
    )

    provider = build_local_embeddings_provider(settings)

    assert isinstance(provider, LocalOnnxEmbeddingsProvider)
    assert warmed == [True]
