from __future__ import annotations

import asyncio
import base64
import mimetypes
import re
import secrets
import uuid
from pathlib import Path
from typing import Any

import structlog

from octopal.channels.whatsapp.bridge import WhatsAppBridgeController
from octopal.channels.whatsapp.ids import (
    normalize_whatsapp_number,
    parse_allowed_whatsapp_numbers,
    whatsapp_chat_id,
)
from octopal.infrastructure.config.settings import Settings
from octopal.runtime.app import build_octo
from octopal.runtime.metrics import read_metrics_snapshot, update_component_gauges
from octopal.runtime.octo.core import Octo
from octopal.runtime.octo.delivery import resolve_user_delivery
from octopal.runtime.octo_status import build_octo_status
from octopal.runtime.pending_turns import PendingTurnAggregator
from octopal.runtime.state import update_last_message
from octopal.utils import (
    extract_reaction_and_strip,
    normalize_reaction_emoji,
    sanitize_user_facing_text,
)

logger = structlog.get_logger(__name__)

_WHATSAPP_IMAGE_MIME_PREFIXES = ("image/",)


class WhatsAppRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.bridge = WhatsAppBridgeController(settings)
        self.octo: Octo = build_octo(settings)
        self._number_by_chat_id: dict[int, str] = {}
        self._lock_by_chat_id: dict[int, asyncio.Lock] = {}
        self._pending_turns = PendingTurnAggregator(
            grace_seconds=getattr(settings, "user_message_grace_seconds", 5.0),
            flush_callback=self._flush_pending_turn,
        )
        self._publish_metrics()

    def attach_octo_output(self) -> None:
        async def _internal_send(chat_id: int, text: str) -> None:
            decision = resolve_user_delivery(text)
            if not decision.user_visible:
                return

            emoji, final_text = extract_reaction_and_strip(decision.text)
            if emoji:
                to = self._number_by_chat_id.get(chat_id)
                # We need a message ID to react. This simple runtime currently doesn't
                # track last inbound message ID globally per chat in a way that's easily
                # accessible here without more refactoring, but we can try to use
                # the one from the bridge if we had it.
                # For now, final reactions are handled in _flush_pending_turn.
                pass

            if not final_text:
                return

            clean_text = sanitize_user_facing_text(final_text)
            if not clean_text:
                return

            to = self._number_by_chat_id.get(chat_id)
            if not to:
                logger.warning("Missing WhatsApp recipient mapping", chat_id=chat_id)
                return
            for chunk in _chunk_text(clean_text, limit=4000):
                self.bridge.send_message(to, chunk)

        async def _internal_send_file(
            chat_id: int, file_path: str, caption: str | None = None
        ) -> None:
            to = self._number_by_chat_id.get(chat_id)
            if not to:
                logger.warning(
                    "Missing WhatsApp recipient mapping for file delivery", chat_id=chat_id
                )
                return
            clean_caption = sanitize_user_facing_text(caption or "") or None
            self.bridge.send_file(to, file_path, caption=clean_caption)

        async def _internal_progress_send(
            chat_id: int,
            state: str,
            text: str,
            meta: dict[str, object],
        ) -> None:
            # WhatsApp doesn't support easy 'typing' but we can send status reactions
            # if we have the message ID.
            logger.info("WhatsApp progress event", chat_id=chat_id, state=state, text=text)

        async def _internal_worker_event_send(
            chat_id: int, event: str, payload: dict[str, Any]
        ) -> None:
            logger.info("WhatsApp worker event", chat_id=chat_id, event=event, payload=payload)

        async def _internal_typing_control(chat_id: int, active: bool) -> None:
            logger.debug(
                "WhatsApp typing indicator not implemented", chat_id=chat_id, active=active
            )

        self.octo.internal_send = _internal_send
        self.octo.internal_send_file = _internal_send_file
        self.octo.internal_progress_send = _internal_progress_send
        self.octo.internal_worker_event_send = _internal_worker_event_send
        self.octo.internal_typing_control = _internal_typing_control
        self.octo._tg_send = _internal_send
        self.octo._tg_send_file = _internal_send_file
        self.octo._tg_progress = _internal_progress_send
        self.octo._tg_worker_event = _internal_worker_event_send
        self.octo._tg_typing = _internal_typing_control

    def _ensure_callback_token(self) -> None:
        if self.settings.whatsapp_callback_token.strip():
            return
        self.settings.whatsapp_callback_token = secrets.token_urlsafe(32)
        logger.warning(
            "Generated ephemeral WhatsApp callback token; configure OCTOPAL_WHATSAPP_CALLBACK_TOKEN "
            "to keep a stable token across restarts"
        )

    async def start(self) -> Octo:
        self.attach_octo_output()
        self._ensure_callback_token()
        callback_url = (
            f"http://127.0.0.1:{self.settings.gateway_port}/api/channels/whatsapp/inbound"
        )
        self.bridge.start(callback_url=callback_url)
        allowed_numbers = parse_allowed_whatsapp_numbers(self.settings.allowed_whatsapp_numbers)
        for number in allowed_numbers:
            self._number_by_chat_id[whatsapp_chat_id(number)] = number
        await self.octo.initialize_system(
            bot=None,
            allowed_chat_ids=[whatsapp_chat_id(number) for number in allowed_numbers],
        )
        self._publish_metrics(connected=True)
        return self.octo

    async def stop(self) -> None:
        self.bridge.stop()
        await self._pending_turns.stop()
        await self.octo.stop_background_tasks()
        self._publish_metrics(connected=False)

    async def handle_inbound(self, payload: dict[str, Any]) -> dict[str, Any]:
        sender = str(payload.get("sender", "")).strip()
        conversation = str(payload.get("conversation", "")).strip() or sender
        self_number = normalize_whatsapp_number(str(payload.get("self", "")).strip())
        from_me = bool(payload.get("fromMe"))
        self_chat = bool(payload.get("selfChat"))
        text = str(payload.get("text", "") or "").strip()
        images, saved_file_paths = self._extract_media(payload)
        if not sender or (not text and not images and not saved_file_paths):
            return {"accepted": False, "reason": "missing_sender_or_content"}
        if from_me and not self._is_personal_mode():
            logger.debug(
                "Ignoring WhatsApp fromMe message outside personal mode",
                sender=sender,
                conversation=conversation,
            )
            return {"accepted": False, "reason": "from_me_ignored"}
        if from_me and not self_chat:
            logger.debug(
                "Ignoring WhatsApp fromMe message outside self chat",
                sender=sender,
                conversation=conversation,
            )
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
            metadata={
                "message_id": str(payload.get("messageId", "") or "").strip(),
                "remote_jid": str(payload.get("remoteJid", "") or "").strip(),
                "target_from_me": from_me,
            },
        )
        self._publish_metrics(last_sender=sender)
        return {"accepted": True, "chat_id": chat_id}

    def status(self) -> dict[str, Any]:
        status = self.bridge.status()
        status["mapped_chats"] = len(self._number_by_chat_id)
        metrics = read_metrics_snapshot(Path(self.settings.state_dir))
        status["octo"] = build_octo_status((metrics or {}).get("octo", {}))
        return status

    def _publish_metrics(
        self, *, connected: bool | None = None, last_sender: str | None = None
    ) -> None:
        gauges: dict[str, Any] = {
            "chat_mappings": len(self._number_by_chat_id),
        }
        if connected is not None:
            gauges["connected"] = int(bool(connected))
        if last_sender:
            gauges["last_sender"] = last_sender
        update_component_gauges("whatsapp", gauges)

    def _is_personal_mode(self) -> bool:
        return (
            str(getattr(self.settings, "whatsapp_mode", "separate") or "separate").strip().lower()
            == "personal"
        )

    def _extract_media(self, payload: dict[str, Any]) -> tuple[list[str], list[str]]:
        media_data_url = str(payload.get("mediaDataUrl", "") or "").strip()
        if not media_data_url:
            media_data_url = str(payload.get("imageDataUrl", "") or "").strip()
        if not media_data_url:
            return [], []

        mime_type = (
            str(payload.get("mediaMimeType", "") or "").strip()
            or str(payload.get("imageMimeType", "") or "").strip()
            or "application/octet-stream"
        )
        file_name = str(payload.get("mediaFileName", "") or "").strip() or None
        media_kind = str(payload.get("mediaKind", "") or "").strip().lower()
        try:
            _, b64_payload = media_data_url.split(",", 1)
        except ValueError:
            return [], []

        try:
            binary = base64.b64decode(b64_payload)
        except Exception:
            logger.warning("Failed to decode inbound WhatsApp media payload")
            return [], []

        try:
            saved_path = _persist_whatsapp_media_payload(
                workspace_root=Path(self.settings.workspace_dir).resolve(),
                binary=binary,
                mime_type=mime_type,
                file_name=file_name,
                media_kind=media_kind,
            )
        except Exception:
            logger.exception("Failed to persist inbound WhatsApp media")
            return [], []

        is_image = media_kind == "image" or mime_type.lower().startswith(
            _WHATSAPP_IMAGE_MIME_PREFIXES
        )
        images = [media_data_url] if is_image else []
        return images, [saved_path]

    async def _flush_pending_turn(
        self,
        chat_id: int,
        text: str,
        images: list[str],
        saved_file_paths: list[str],
        metadata: dict[str, Any],
    ) -> None:
        lock = self._lock_by_chat_id.setdefault(chat_id, asyncio.Lock())
        to = self._number_by_chat_id.get(chat_id)
        message_id = str(metadata.get("message_id", "") or "").strip()
        remote_jid = str(metadata.get("remote_jid", "") or "").strip() or None
        target_from_me = bool(metadata.get("target_from_me"))

        # Immediate feedback
        if to and message_id:
            try:
                self.bridge.send_reaction(
                    to,
                    "🤔",
                    message_id=message_id,
                    remote_jid=remote_jid,
                    target_from_me=target_from_me,
                )
            except Exception:
                logger.debug("Failed to set WhatsApp thinking reaction", chat_id=chat_id)

        async with lock:
            reply = await self.octo.handle_message(
                text,
                chat_id,
                images=images,
                saved_file_paths=saved_file_paths,
            )
        update_last_message(self.settings)
        immediate = getattr(reply, "immediate", "")
        if immediate:
            emoji, final_text = extract_reaction_and_strip(immediate)
            to = self._number_by_chat_id.get(chat_id)
            message_id = str(metadata.get("message_id", "") or "").strip()
            remote_jid = str(metadata.get("remote_jid", "") or "").strip() or None
            target_from_me = bool(metadata.get("target_from_me"))
            if emoji and to and message_id:
                try:
                    self.bridge.send_reaction(
                        to,
                        normalize_reaction_emoji(emoji),
                        message_id=message_id,
                        remote_jid=remote_jid,
                        target_from_me=target_from_me,
                    )
                except Exception:
                    logger.warning(
                        "Failed to apply WhatsApp reaction",
                        chat_id=chat_id,
                        emoji=emoji,
                        message_id=message_id,
                        exc_info=True,
                    )

            decision = resolve_user_delivery(final_text)
            if decision.user_visible:
                await self.octo.internal_send(chat_id, decision.text)


def _persist_whatsapp_media_payload(
    *,
    workspace_root: Path,
    binary: bytes,
    mime_type: str | None,
    file_name: str | None,
    media_kind: str | None,
) -> str:
    normalized_kind = str(media_kind or "").strip().lower() or "file"
    media_dir = workspace_root / "tmp" / "whatsapp_media"
    media_dir.mkdir(parents=True, exist_ok=True)

    suffix = Path(file_name or "").suffix.lower()
    if not suffix:
        guessed = mimetypes.guess_extension(str(mime_type or "").split(";", 1)[0].strip().lower())
        suffix = guessed or ".bin"

    safe_stem = Path(file_name or "").stem.strip() or normalized_kind
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", safe_stem).strip("._") or normalized_kind
    file_path = (media_dir / f"{safe_stem}_{uuid.uuid4()}{suffix}").resolve()
    file_path.write_bytes(binary)
    return str(file_path)


def _chunk_text(text: str, limit: int) -> list[str]:
    if len(text) <= limit:
        return [_whatsappify(text)]
    parts: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            parts.append(_whatsappify(remaining))
            break
        cut = remaining.rfind("\n", 0, limit)
        if cut <= 0:
            cut = limit
        parts.append(_whatsappify(remaining[:cut].strip()))
        remaining = remaining[cut:].lstrip()
    return [part for part in parts if part]


def _whatsappify(text: str) -> str:
    """Convert basic Markdown to WhatsApp format."""
    if not text:
        return ""
    # Convert bold **text** to *text*
    text = re.sub(r"\*\*(.*?)\*\*", r"*\1*", text)
    # Convert italic _text_ or *text* to _text_
    # We use a cautious approach here to avoid breaking things
    text = re.sub(r"__(.*?)__", r"_\1_", text)
    # Convert inline code `text` to ```text```
    text = re.sub(r"`([^`\n]+)`", r"```\1```", text)
    return text
