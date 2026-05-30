from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import structlog

from octopal.runtime.octo import followup_pipeline as _followup_pipeline

logger = structlog.get_logger(__name__)

_publish_runtime_metrics = _followup_pipeline._publish_runtime_metrics


class OctoOutputRuntimeMixin:
    @property
    def is_ws_active(self) -> bool:
        return self._ws_active

    def set_output_channel(
        self,
        is_ws: bool,
        send: callable | None = None,
        send_file: callable | None = None,
        progress: callable | None = None,
        worker_event: callable | None = None,
        typing: callable | None = None,
        message_event: callable | None = None,
        owner_id: str | None = None,
        force: bool = False,
    ) -> bool:
        """Attach or detach the WebSocket mirror without changing the primary channel."""
        if is_ws:
            if self._ws_active and self._ws_owner and owner_id and self._ws_owner != owner_id:
                if force:
                    logger.warning(
                        "Forcing WebSocket channel takeover",
                        current_owner=self._ws_owner,
                        new_owner=owner_id,
                    )
                else:
                    logger.warning(
                        "Rejected WebSocket channel switch due to existing owner",
                        current_owner=self._ws_owner,
                        attempted_owner=owner_id,
                    )
                    return False
        else:
            if self._ws_owner and owner_id and self._ws_owner != owner_id:
                if force:
                    logger.warning(
                        "Forcing output channel reset from non-owner",
                        current_owner=self._ws_owner,
                        attempted_owner=owner_id,
                    )
                else:
                    logger.warning(
                        "Rejected output channel reset from non-owner",
                        current_owner=self._ws_owner,
                        attempted_owner=owner_id,
                    )
                    return False

        self._ws_active = is_ws
        if is_ws:
            self._ws_send = send
            self._ws_send_file = send_file
            self._ws_progress = progress
            self._ws_worker_event = worker_event
            self._ws_typing = typing
            self._ws_message_event = message_event
            self._ws_owner = owner_id or "ws-default"
            logger.info("Octo attached WebSocket mirror channel")
        else:
            self._ws_send = None
            self._ws_send_file = None
            self._ws_progress = None
            self._ws_worker_event = None
            self._ws_typing = None
            self._ws_message_event = None
            self._ws_owner = None
            logger.info("Octo detached WebSocket mirror channel")

        # Update system status file if possible
        try:
            from octopal.infrastructure.config.settings import load_settings
            from octopal.runtime.state import _status_path, read_status

            settings = load_settings()
            status_data = read_status(settings) or {}
            status_data["websocket_mirror_active"] = is_ws
            _status_path(settings).write_text(
                json.dumps(status_data, indent=2),
                encoding="utf-8",
            )
        except Exception:
            logger.debug("Failed to update status file with active channel", exc_info=True)
        return True

    async def emit_ws_chat_event(
        self,
        *,
        direction: str,
        role: str,
        channel: str,
        chat_id: int,
        text: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        if not self._ws_active or not callable(self._ws_message_event):
            return
        payload = {
            "type": "chat_message",
            "direction": direction,
            "role": role,
            "channel": channel,
            "chat_id": chat_id,
            "text": text,
            "meta": meta or {},
            "created_at": datetime.now(UTC).isoformat(),
        }
        try:
            await self._ws_message_event(chat_id, payload)
        except Exception:
            logger.debug(
                "Failed to emit WebSocket chat mirror event", chat_id=chat_id, exc_info=True
            )

    async def emit_ws_progress(
        self,
        chat_id: int,
        state: str,
        text: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        if not self._ws_active or not callable(self._ws_progress):
            return
        try:
            await self._ws_progress(chat_id, state, text, meta or {})
        except Exception:
            logger.debug(
                "Failed to emit WebSocket progress mirror event", chat_id=chat_id, exc_info=True
            )

    async def emit_ws_worker_event(
        self,
        chat_id: int,
        event: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if not self._ws_active or not callable(self._ws_worker_event):
            return
        try:
            await self._ws_worker_event(chat_id, event, payload or {})
        except Exception:
            logger.debug(
                "Failed to emit WebSocket worker mirror event", chat_id=chat_id, exc_info=True
            )

    async def emit_ws_file(self, chat_id: int, file_path: str, caption: str | None = None) -> None:
        if not self._ws_active or not callable(self._ws_send_file):
            return
        try:
            await self._ws_send_file(chat_id, file_path, caption)
        except Exception:
            logger.debug(
                "Failed to emit WebSocket file mirror event", chat_id=chat_id, exc_info=True
            )

    async def emit_ws_typing(self, chat_id: int, active: bool) -> None:
        if not self._ws_active or not callable(self._ws_typing):
            return
        try:
            await self._ws_typing(chat_id, active)
        except Exception:
            logger.debug(
                "Failed to emit WebSocket typing mirror event", chat_id=chat_id, exc_info=True
            )

    async def set_thinking(self, active: bool) -> None:
        """Toggle global thinking indicator."""
        if active:
            self._thinking_count += 1
        else:
            self._thinking_count = max(0, self._thinking_count - 1)
        _publish_runtime_metrics(self._thinking_count)

    async def set_typing(self, chat_id: int, active: bool):
        """Toggle typing indicator for a specific chat."""
        if self.internal_typing_control:
            try:
                await self.internal_typing_control(chat_id, active)
            except Exception:
                logger.debug(
                    "Failed to set typing status",
                    chat_id=chat_id,
                    active=active,
                    exc_info=True,
                )
        await self.emit_ws_typing(chat_id, active)
