from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import os
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_MODEL_FILENAME = "model.onnx"
_TOKENIZER_FILENAME = "tokenizer.json"
_MODEL_ID = "intfloat/multilingual-e5-small@onnx"
_MODEL_REVISION = "919cfbe11fbf4f1b9bb321007c46c14feaf84227"
_MODEL_REPOSITORY = "intfloat/multilingual-e5-small"
_DOWNLOAD_TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class _PinnedAsset:
    filename: str
    repository_path: str
    sha256: str

    @property
    def url(self) -> str:
        return (
            f"https://huggingface.co/{_MODEL_REPOSITORY}/resolve/"
            f"{_MODEL_REVISION}/{self.repository_path}?download=true"
        )


_PINNED_ASSETS = (
    _PinnedAsset(
        filename=_MODEL_FILENAME,
        repository_path="onnx/model.onnx",
        sha256="ca456c06b3a9505ddfd9131408916dd79290368331e7d76bb621f1cba6bc8665",
    ),
    _PinnedAsset(
        filename=_TOKENIZER_FILENAME,
        repository_path="onnx/tokenizer.json",
        sha256="0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39",
    ),
)
_ASSET_LOCK = threading.Lock()


@dataclass(frozen=True)
class LocalOnnxAvailability:
    available: bool
    reason: str | None = None


class LocalOnnxEmbeddingsProvider:
    """CPU-only multilingual E5 embeddings without a PyTorch runtime.

    On first use, the provider fetches its pinned ONNX graph and tokenizer into
    the configured directory, validates their digests, then serves CPU inference.
    """

    model_id = _MODEL_ID

    def __init__(
        self,
        model_dir: Path,
        *,
        threads: int = 0,
        max_tokens: int = 512,
    ) -> None:
        self._model_dir = Path(model_dir).expanduser().resolve()
        self._model_path = self._model_dir / _MODEL_FILENAME
        self._tokenizer_path = self._model_dir / _TOKENIZER_FILENAME
        self._threads = max(0, int(threads))
        self._max_tokens = max(1, int(max_tokens))
        self._runtime: tuple[Any, Any, Any] | None = None
        self._runtime_lock = threading.Lock()
        self._inference_lock = threading.Lock()

    @classmethod
    def availability(cls, model_dir: Path | None) -> LocalOnnxAvailability:
        if model_dir is None:
            return LocalOnnxAvailability(False, "memory.local_model_dir is not configured")
        resolved_dir = Path(model_dir).expanduser()
        missing = [
            name
            for name in (_MODEL_FILENAME, _TOKENIZER_FILENAME)
            if not (resolved_dir / name).is_file()
        ]
        if missing:
            return LocalOnnxAvailability(
                False,
                f"missing local embedding asset(s): {', '.join(missing)}",
            )
        dependencies = [
            package
            for package in ("numpy", "onnxruntime", "tokenizers")
            if importlib.util.find_spec(package) is None
        ]
        if dependencies:
            return LocalOnnxAvailability(
                False,
                "missing local embedding dependency: " + ", ".join(dependencies),
            )
        return LocalOnnxAvailability(True)

    @classmethod
    def ensure_assets(cls, model_dir: Path) -> None:
        """Download a pinned bundle atomically and verify every byte before use."""
        target_dir = Path(model_dir).expanduser().resolve()
        target_dir.mkdir(parents=True, exist_ok=True)
        with _ASSET_LOCK:
            for asset in _PINNED_ASSETS:
                target = target_dir / asset.filename
                if _asset_matches(target, asset.sha256):
                    continue
                if target.is_symlink():
                    raise RuntimeError(f"refusing to replace symlinked embedding asset: {target}")
                _download_asset(asset, target)

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Compatibility entrypoint; E5 treats unspecified content as documents."""
        return await self.embed_documents(texts)

    async def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return await self._embed(texts, prefix="passage: ")

    async def embed_queries(self, texts: list[str]) -> list[list[float]]:
        return await self._embed(texts, prefix="query: ")

    def warm_up(self) -> None:
        """Load and validate the local model before the runtime begins serving work."""
        self.ensure_assets(self._model_dir)
        self._get_runtime()

    async def _embed(self, texts: list[str], *, prefix: str) -> list[list[float]]:
        if not texts:
            return []
        normalized = [str(text).strip() for text in texts]
        if any(not text for text in normalized):
            raise ValueError("cannot embed empty text")
        return await asyncio.to_thread(self._embed_sync, [prefix + text for text in normalized])

    def _embed_sync(self, texts: list[str]) -> list[list[float]]:
        runtime, tokenizer, numpy = self._get_runtime()
        with self._inference_lock:
            encodings = tokenizer.encode_batch(texts)
            max_length = max(len(encoding.ids) for encoding in encodings)
            input_ids = numpy.zeros((len(encodings), max_length), dtype=numpy.int64)
            attention_mask = numpy.zeros((len(encodings), max_length), dtype=numpy.int64)
            token_type_ids = numpy.zeros((len(encodings), max_length), dtype=numpy.int64)
            for index, encoding in enumerate(encodings):
                length = len(encoding.ids)
                input_ids[index, :length] = encoding.ids
                attention_mask[index, :length] = encoding.attention_mask
                token_type_ids[index, :length] = encoding.type_ids

            input_names = {item.name for item in runtime.get_inputs()}
            required = {"input_ids", "attention_mask"}
            missing = sorted(required - input_names)
            if missing:
                raise RuntimeError(
                    f"local ONNX embedding model is missing inputs: {', '.join(missing)}"
                )
            model_inputs = {
                "input_ids": input_ids,
                "attention_mask": attention_mask,
            }
            if "token_type_ids" in input_names:
                model_inputs["token_type_ids"] = token_type_ids
            outputs = runtime.run(None, model_inputs)

        if not outputs:
            raise RuntimeError("local ONNX embedding model returned no outputs")
        hidden_states = numpy.asarray(outputs[0], dtype=numpy.float32)
        if hidden_states.ndim != 3 or hidden_states.shape[0] != len(texts):
            raise RuntimeError("local ONNX embedding model returned an unexpected output shape")
        mask = attention_mask[..., None].astype(numpy.float32)
        token_counts = numpy.maximum(mask.sum(axis=1), 1.0)
        pooled = (hidden_states * mask).sum(axis=1) / token_counts
        norms = numpy.linalg.norm(pooled, axis=1, keepdims=True)
        normalized = pooled / numpy.maximum(norms, 1e-12)
        return normalized.astype(numpy.float32).tolist()

    def _get_runtime(self) -> tuple[Any, Any, Any]:
        with self._runtime_lock:
            if self._runtime is not None:
                return self._runtime
            availability = self.availability(self._model_dir)
            if not availability.available:
                raise RuntimeError(availability.reason or "local ONNX embeddings are unavailable")
            for asset in _PINNED_ASSETS:
                if not _asset_matches(self._model_dir / asset.filename, asset.sha256):
                    raise RuntimeError(
                        f"local embedding asset failed integrity validation: {asset.filename}"
                    )

            import numpy
            import onnxruntime
            from tokenizers import Tokenizer

            options = onnxruntime.SessionOptions()
            if self._threads:
                options.intra_op_num_threads = self._threads
                options.inter_op_num_threads = 1
            runtime = onnxruntime.InferenceSession(
                str(self._model_path),
                sess_options=options,
                providers=["CPUExecutionProvider"],
            )
            tokenizer = Tokenizer.from_file(str(self._tokenizer_path))
            tokenizer.enable_truncation(max_length=self._max_tokens)
            self._runtime = (runtime, tokenizer, numpy)
            return self._runtime


def _asset_matches(path: Path, expected_sha256: str) -> bool:
    if not path.is_file() or path.is_symlink():
        return False
    return _sha256(path) == expected_sha256


def _download_asset(asset: _PinnedAsset, target: Path) -> None:
    temporary = target.with_name(f".{target.name}.{os.getpid()}.download")
    temporary.unlink(missing_ok=True)
    request = urllib.request.Request(
        asset.url,
        headers={"User-Agent": "octopal-local-embeddings/1"},
    )
    digest = hashlib.sha256()
    try:
        with (
            urllib.request.urlopen(request, timeout=_DOWNLOAD_TIMEOUT_SECONDS) as response,
            temporary.open("wb") as output,
        ):
            while chunk := response.read(1024 * 1024):
                digest.update(chunk)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        if digest.hexdigest() != asset.sha256:
            raise RuntimeError(
                f"downloaded embedding asset failed SHA-256 validation: {asset.filename}"
            )
        os.replace(temporary, target)
    except (OSError, urllib.error.URLError) as exc:
        raise RuntimeError(
            f"failed to download local embedding asset {asset.filename}: {exc}"
        ) from exc
    finally:
        temporary.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as input_file:
        for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
