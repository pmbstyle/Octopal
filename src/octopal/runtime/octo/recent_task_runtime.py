from __future__ import annotations

import time

from octopal.runtime.octo.runtime_config import _env_int

_RECENT_WORKER_TASK_TTL_SECONDS = float(
    _env_int(
        "OCTOPAL_RECENT_WORKER_TASK_TTL_SECONDS",
        1800,
        minimum=60,
    )
)
_CONTINUATION_RESOURCE_SCOPE = "chat:continuation-resource"


class OctoRecentTaskRuntimeMixin:
    def _recent_task_exact_key(
        self,
        *,
        chat_id: int,
        correlation_id: str | None,
        task_signature: str,
    ) -> tuple[int, str, str]:
        scope = str(correlation_id or f"chat:{chat_id}")
        return (chat_id, scope, task_signature)

    def _recent_task_cross_scope_key(
        self,
        *,
        chat_id: int,
        cross_scope_signature: str,
        task_signature: str,
    ) -> tuple[int, str, str]:
        return (
            chat_id,
            _CONTINUATION_RESOURCE_SCOPE,
            f"{cross_scope_signature}\0{task_signature}",
        )

    def _has_recent_task_cross_scope_reservation(
        self,
        *,
        chat_id: int,
        cross_scope_signature: str,
    ) -> bool:
        prefix = f"{cross_scope_signature}\0"
        return any(
            key_chat_id == chat_id
            and key_scope == _CONTINUATION_RESOURCE_SCOPE
            and key_signature.startswith(prefix)
            for key_chat_id, key_scope, key_signature in self._recent_tasks
        )

    def _reserve_recent_task(
        self,
        *,
        chat_id: int,
        correlation_id: str | None,
        task_signature: str,
        cross_scope_signature: str | None = None,
    ) -> bool:
        self._prune_recent_tasks()
        exact_key = self._recent_task_exact_key(
            chat_id=chat_id,
            correlation_id=correlation_id,
            task_signature=task_signature,
        )
        duplicate_keys = [exact_key]
        cross_scope_key = None
        if cross_scope_signature:
            cross_scope_key = self._recent_task_cross_scope_key(
                chat_id=chat_id,
                cross_scope_signature=cross_scope_signature,
                task_signature=task_signature,
            )
        if any(key in self._recent_tasks for key in duplicate_keys):
            return False
        if (
            cross_scope_signature
            and str(correlation_id or "").startswith("control-handoff-")
            and self._has_recent_task_cross_scope_reservation(
                chat_id=chat_id,
                cross_scope_signature=cross_scope_signature,
            )
        ):
            return False
        now = time.monotonic()
        self._recent_tasks[exact_key] = now
        if cross_scope_key is not None:
            self._recent_tasks[cross_scope_key] = now
        return True

    def _release_recent_task(
        self,
        *,
        chat_id: int,
        correlation_id: str | None,
        task_signature: str,
        cross_scope_signature: str | None = None,
    ) -> None:
        self._recent_tasks.pop(
            self._recent_task_exact_key(
                chat_id=chat_id,
                correlation_id=correlation_id,
                task_signature=task_signature,
            ),
            None,
        )
        if cross_scope_signature:
            self._recent_tasks.pop(
                self._recent_task_cross_scope_key(
                    chat_id=chat_id,
                    cross_scope_signature=cross_scope_signature,
                    task_signature=task_signature,
                ),
                None,
            )

    def _prune_recent_tasks(self) -> None:
        now = time.monotonic()
        cutoff = now - _RECENT_WORKER_TASK_TTL_SECONDS
        stale_keys = [key for key, seen_at in self._recent_tasks.items() if seen_at < cutoff]
        for key in stale_keys:
            self._recent_tasks.pop(key, None)
