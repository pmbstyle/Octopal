from __future__ import annotations

import asyncio
import hashlib
from types import SimpleNamespace

import pytest

from octopal.infrastructure.providers import local_onnx_embeddings
from octopal.infrastructure.providers.local_onnx_embeddings import (
    LocalOnnxEmbeddingsProvider,
    _PinnedAsset,
)


def test_local_onnx_provider_reports_missing_assets(tmp_path) -> None:
    availability = LocalOnnxEmbeddingsProvider.availability(tmp_path)

    assert availability.available is False
    assert availability.reason is not None
    assert "model.onnx" in availability.reason
    assert "tokenizer.json" in availability.reason


def test_local_onnx_provider_downloads_and_validates_pinned_asset(tmp_path, monkeypatch) -> None:
    payload = b"verified model bytes"
    asset = _PinnedAsset(
        filename="model.onnx",
        repository_path="onnx/model.onnx",
        sha256=hashlib.sha256(payload).hexdigest(),
    )

    class Response:
        def __init__(self) -> None:
            self._offset = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self, size: int) -> bytes:
            chunk = payload[self._offset : self._offset + size]
            self._offset += len(chunk)
            return chunk

    monkeypatch.setattr(
        local_onnx_embeddings.urllib.request,
        "urlopen",
        lambda _request, timeout: Response(),
    )

    target = tmp_path / asset.filename
    local_onnx_embeddings._download_asset(asset, target)

    assert target.read_bytes() == payload
    assert not list(tmp_path.glob("*.download"))


def test_local_onnx_provider_rejects_symlinked_asset(tmp_path, monkeypatch) -> None:
    asset = _PinnedAsset(filename="model.onnx", repository_path="model.onnx", sha256="a" * 64)
    target = tmp_path / asset.filename
    target.symlink_to(tmp_path / "elsewhere")
    monkeypatch.setattr(local_onnx_embeddings, "_PINNED_ASSETS", (asset,))

    with pytest.raises(RuntimeError, match="symlinked"):
        LocalOnnxEmbeddingsProvider.ensure_assets(tmp_path)


def test_local_onnx_provider_prefixes_e5_documents_and_queries(tmp_path) -> None:
    numpy = pytest.importorskip("numpy")
    provider = LocalOnnxEmbeddingsProvider(tmp_path)
    encoded_texts: list[str] = []

    class Tokenizer:
        def encode_batch(self, texts: list[str]):
            encoded_texts.extend(texts)
            return [
                SimpleNamespace(ids=[1, 2], attention_mask=[1, 1], type_ids=[0, 0])
                for _text in texts
            ]

    class Runtime:
        def get_inputs(self):
            return [
                SimpleNamespace(name="input_ids"),
                SimpleNamespace(name="attention_mask"),
                SimpleNamespace(name="token_type_ids"),
            ]

        def run(self, _outputs, _inputs):
            return [numpy.asarray([[[3.0, 4.0], [3.0, 4.0]]], dtype=numpy.float32)]

    provider._get_runtime = lambda: (Runtime(), Tokenizer(), numpy)  # type: ignore[method-assign]

    document_vectors = asyncio.run(provider.embed_documents(["deploy guide"]))
    query_vectors = asyncio.run(provider.embed_queries(["how do I deploy?"]))

    assert encoded_texts == ["passage: deploy guide", "query: how do I deploy?"]
    assert document_vectors[0] == pytest.approx([0.6, 0.8])
    assert query_vectors[0] == pytest.approx([0.6, 0.8])
