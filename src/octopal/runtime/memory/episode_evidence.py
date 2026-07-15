from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
from datetime import timedelta
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from octopal.infrastructure.store.models import (
    ExecutionEpisodeEvidenceRecord,
    ExecutionEpisodeRecord,
)
from octopal.runtime.workers.contracts import WorkerResult, WorkerSpec
from octopal.utils import utc_now

_ALGORITHM = "AES-256-GCM"
_KEY_BYTES = 32
_NONCE_BYTES = 12
_AAD_PREFIX = b"octopal.execution_episode_evidence.v1\0"


class EpisodeEvidenceCipher:
    """Encrypt and decrypt raw episode evidence with a dedicated AEAD key."""

    def __init__(self, key: bytes) -> None:
        if len(key) != _KEY_BYTES:
            raise ValueError("episode evidence key must decode to exactly 32 bytes")
        normalized_key = bytes(key)
        self._cipher = AESGCM(normalized_key)
        self.key_id = hashlib.sha256(normalized_key).hexdigest()[:16]

    @classmethod
    def from_encoded_key(cls, encoded_key: str) -> EpisodeEvidenceCipher:
        text = str(encoded_key or "").strip()
        if not text:
            raise ValueError("episode evidence key is empty")
        try:
            key = base64.b64decode(text, altchars=b"-_", validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("episode evidence key must be URL-safe base64") from exc
        return cls(key)

    def encrypt(
        self,
        *,
        episode_id: str,
        payload: dict[str, Any],
        retention_days: int,
    ) -> ExecutionEpisodeEvidenceRecord:
        if retention_days <= 0:
            raise ValueError("episode evidence retention_days must be positive")
        plaintext = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        nonce = os.urandom(_NONCE_BYTES)
        created_at = utc_now()
        ciphertext = self._cipher.encrypt(nonce, plaintext, _aad(episode_id))
        return ExecutionEpisodeEvidenceRecord(
            episode_id=episode_id,
            algorithm=_ALGORITHM,
            key_id=self.key_id,
            nonce=nonce,
            ciphertext=ciphertext,
            created_at=created_at,
            expires_at=created_at + timedelta(days=retention_days),
        )

    def decrypt(self, evidence: ExecutionEpisodeEvidenceRecord) -> dict[str, Any]:
        if evidence.algorithm != _ALGORITHM:
            raise ValueError(f"unsupported episode evidence algorithm: {evidence.algorithm}")
        if evidence.key_id != self.key_id:
            raise ValueError("episode evidence was encrypted with a different key")
        plaintext = self._cipher.decrypt(
            evidence.nonce,
            evidence.ciphertext,
            _aad(evidence.episode_id),
        )
        payload = json.loads(plaintext.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("decrypted episode evidence must be a JSON object")
        if payload.get("episode_id") != evidence.episode_id:
            raise ValueError("decrypted episode evidence id does not match its envelope")
        return payload


def build_encrypted_worker_episode_evidence(
    *,
    cipher: EpisodeEvidenceCipher,
    episode: ExecutionEpisodeRecord,
    spec: WorkerSpec,
    result: WorkerResult,
    stored_output: dict[str, Any] | None,
    retention_days: int,
) -> ExecutionEpisodeEvidenceRecord:
    result_payload = result.model_dump(mode="json")
    result_payload["output"] = stored_output
    llm_config = spec.llm_config
    payload = {
        "version": 1,
        "episode_id": episode.id,
        "worker_run_id": spec.id,
        "task": {
            "text": spec.task,
            "inputs": spec.inputs,
        },
        "prompt": {
            "system": spec.system_prompt,
        },
        "execution": {
            "template_id": spec.template_id,
            "template_name": spec.template_name,
            "available_tools": spec.available_tools,
            "required_tool_calls": spec.required_tool_calls,
            "mcp_tools": spec.mcp_tools,
            "granted_capabilities": spec.granted_capabilities,
            "effective_permissions": spec.effective_permissions,
            "allowed_paths": spec.allowed_paths,
            "adaptations": [item.model_dump(mode="json") for item in spec.adaptations],
            "provider_id": llm_config.provider_id if llm_config else None,
            "model": (llm_config.model if llm_config else None) or spec.model,
            "launcher_lifecycle": spec.lifecycle,
        },
        "result": result_payload,
    }
    return cipher.encrypt(
        episode_id=episode.id,
        payload=payload,
        retention_days=retention_days,
    )


def _aad(episode_id: str) -> bytes:
    return _AAD_PREFIX + str(episode_id).encode("utf-8")
