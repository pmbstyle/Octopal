from __future__ import annotations

import asyncio
import base64
import uuid
from pathlib import Path
from typing import Any

import structlog

from broodmind.runtime.app import build_queen
from broodmind.infrastructure.config.settings import Settings
from broodmind.runtime.pending_turns import PendingTurnAggregator
from broodmind.runtime.queen.core import Queen
from broodmind.runtime.metrics import update_component_gauges
from broodmind.runtime.state import update_last_message
from broodmind.utils import should_suppress_user_delivery
from broodmind.channels.whatsapp.bridge import WhatsAppBridgeController
from broodmind.channels.whatsapp.ids import (
    normalize_whatsapp_number,
    parse_allowed_whatsapp_numbers,
    whatsapp_chat_id,
)

logger = structlog.get_logger(__name__)


class WhatsAppRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.bridge = WhatsAppBridgeController(settings)
        self.queen: Queen = build_queen(settings)
        self._number_by_chat_id: dict[int, str] = {}
        self._lock_by_chat_id: dict[int, asyncio.Lock] = {}
        self._pending_turns = PendingTurnAggregator(
            grace_seconds=getattr(settings, "user_message_grace_seconds", 5.0),
            flush_callback=self._flush_pending_turn,
        )
        self._publish_metrics()

    def attach_queen_output(self) -> None:
        async def _internal_send(chat_id: int, text: str) -> None:
            if should_suppress_user_delivery(text):
                return
            to = self._number_by_chat_id.get(chat_id)
            if not to:
                logger.warning("Missing WhatsApp recipient mapping", chat_id=chat_id)
                return
            for chunk in _chunk_text(text, limit=4000):
                self.bridge.send_message(to, chunk)

        async def _internal_progress_send(
            chat_id: int,
            state: str,
            text: str,
            meta: dict[str, object],
        ) -> None:
            if state != "partial":
                return
            if text.strip():
                logger.debug("Suppressed WhatsApp partial progress preview", chat_id=chat_id, text_len=len(text))

        async def _internal_typing_control(chat_id: int, active: bool) -> None:
            logger.debug("WhatsApp typing indicator not implemented", chat_id=chat_id, active=active)

        self.queen.internal_send = _internal_send
        self.queen.internal_progress_send = _internal_progress_send
        self.queen.internal_typing_control = _internal_typing_control
        self.queen._tg_send = _internal_send
        self.queen._tg_progress = _internal_progress_send
        self.queen._tg_typing = _internal_typing_control

    async def start(self) -> Queen:
        self.attach_queen_output()
        callback_url = (
            f"http://127.0.0.1:{self.settings.gateway_port}/api/channels/whatsapp/inbound"
        )
        self.bridge.start(callback_url=callback_url)
        allowed_numbers = parse_allowed_whatsapp_numbers(self.settings.allowed_whatsapp_numbers)
        for number in allowed_numbers:
            self._number_by_chat_id[whatsapp_chat_id(number)] = number
        await self.queen.initialize_system(
            bot=None,
            allowed_chat_ids=[whatsapp_chat_id(number) for number in allowed_numbers],
        )
        self._publish_metrics(connected=True)
        return self.queen

    async def stop(self) -> None:
        self.bridge.stop()
        await self._pending_turns.stop()
        await self.queen.stop_background_tasks()
        self._publish_metrics(connected=False)

    async def handle_inbound(self, payload: dict[str, Any]) -> dict[str, Any]:
        sender = str(payload.get("sender", "")).strip()
        conversation = str(payload.get("conversation", "")).strip() or sender
        self_number = normalize_whatsapp_number(str(payload.get("self", "")).strip())
        from_me = bool(payload.get("fromMe"))
        self_chat = bool(payload.get("selfChat"))
        text = str(payload.get("text", "") or "").strip()
        images, saved_file_paths = self._extract_images(payload)
        if not sender or (not text and not images):
            return {"accepted": False, "reason": "missing_sender_or_content"}
        if from_me and not self._is_personal_mode():
            logger.debug("Ignoring WhatsApp fromMe message outside personal mode", sender=sender, conversation=conversation)
            return {"accepted": False, "reason": "from_me_ignored"}
        if from_me and not self_chat:
            logger.debug("Ignoring WhatsApp fromMe message outside self chat", sender=sender, conversation=conversation)
            return {"accepted": False, "reason": "not_self_chat"}
        allowed = parse_allowed_whatsapp_numbers(self.settings.allowed_whatsapp_numbers)
        if allowed and sender not in allowed:
            logger.warning("Rejected WhatsApp message from unauthorized sender", sender=sender)
            return {"accepted": False, "reason": "unauthorized"}

        if self_chat and self_number and sender != self_number:
            logger.warning(
                "Rejected WhatsApp self-chat payload with mismatched sender",
                sender=sender,
                self_number=self_number,
            )
            return {"accepted": False, "reason": "invalid_self_chat_sender"}

        chat_number = normalize_whatsapp_number(conversation) or conversation
        chat_id = whatsapp_chat_id(chat_number)
        self._number_by_chat_id[chat_id] = chat_number
        await self._pending_turns.submit(
            chat_id=chat_id,
            text=text,
            images=images,
            saved_file_paths=saved_file_paths,
        )
        self._publish_metrics(last_sender=sender)
        return {"accepted": True, "chat_id": chat_id}

    def status(self) -> dict[str, Any]:
        status = self.bridge.status()
        status["mapped_chats"] = len(self._number_by_chat_id)
        return status

    def _publish_metrics(self, *, connected: bool | None = None, last_sender: str | None = None) -> None:
        gauges: dict[str, Any] = {
            "chat_mappings": len(self._number_by_chat_id),
        }
        if connected is not None:
            gauges["connected"] = int(bool(connected))
        if last_sender:
            gauges["last_sender"] = last_sender
        update_component_gauges("whatsapp", gauges)

    def _is_personal_mode(self) -> bool:
        return str(getattr(self.settings, "whatsapp_mode", "separate") or "separate").strip().lower() == "personal"

    def _extract_images(self, payload: dict[str, Any]) -> tuple[list[str], list[str]]:
        image_data_url = str(payload.get("imageDataUrl", "") or "").strip()
        if not image_data_url:
            return [], []

        mime_type = str(payload.get("imageMimeType", "") or "").strip() or "image/jpeg"
        try:
            _, b64_payload = image_data_url.split(",", 1)
        except ValueError:
            return [], []

        try:
            binary = base64.b64decode(b64_payload)
        except Exception:
            logger.warning("Failed to decode inbound WhatsApp image payload")
            return [], []

        workspace_root = Path(self.settings.workspace_dir).resolve()
        image_dir = workspace_root / "tmp" / "whatsapp_images"
        image_dir.mkdir(parents=True, exist_ok=True)

        suffix = ".jpg"
        lowered_mime = mime_type.lower()
        if "png" in lowered_mime:
            suffix = ".png"
        elif "webp" in lowered_mime:
            suffix = ".webp"

        file_path = (image_dir / f"img_{uuid.uuid4()}{suffix}").resolve()
        try:
            file_path.write_bytes(binary)
        except Exception:
            logger.exception("Failed to persist inbound WhatsApp image", path=str(file_path))
            return [], []

        return [image_data_url], [str(file_path)]

    async def _flush_pending_turn(
        self,
        chat_id: int,
        text: str,
        images: list[str],
        saved_file_paths: list[str],
        metadata: dict[str, Any],
    ) -> None:
        del metadata
        lock = self._lock_by_chat_id.setdefault(chat_id, asyncio.Lock())
        async with lock:
            reply = await self.queen.handle_message(
                text,
                chat_id,
                images=images,
                saved_file_paths=saved_file_paths,
            )
        update_last_message(self.settings)
        immediate = getattr(reply, "immediate", "")
        if immediate and not should_suppress_user_delivery(immediate):
            await self.queen.internal_send(chat_id, immediate)


def _chunk_text(text: str, limit: int) -> list[str]:
    if len(text) <= limit:
        return [text]
    parts: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            parts.append(remaining)
            break
        cut = remaining.rfind("\n", 0, limit)
        if cut <= 0:
            cut = limit
        parts.append(remaining[:cut].strip())
        remaining = remaining[cut:].lstrip()
    return [part for part in parts if part]
