from __future__ import annotations

import asyncio
import base64
import mimetypes
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status

from octopal.runtime.octo.core import Octo, OctoReply
from octopal.runtime.octo.delivery import resolve_user_delivery
from octopal.utils import get_tailscale_ips, should_suppress_user_delivery

logger = structlog.get_logger(__name__)


async def _ws_send_json(
    session: _ActiveWsSession,
    payload: dict[str, Any],
    *,
    event_name: str,
    chat_id: int | None = None,
) -> None:
    async with session.send_lock:
        try:
            logger.debug(
                "Sending WebSocket payload",
                connection_id=session.connection_id,
                event_name=event_name,
                chat_id=chat_id,
                payload_type=payload.get("type"),
            )
            await session.socket.send_json(payload)
            logger.debug(
                "Sent WebSocket payload",
                connection_id=session.connection_id,
                event_name=event_name,
                chat_id=chat_id,
                payload_type=payload.get("type"),
            )
        except Exception:
            logger.exception(
                "Failed to send WebSocket payload",
                connection_id=session.connection_id,
                event_name=event_name,
                chat_id=chat_id,
                payload_type=payload.get("type"),
            )
            raise


def _build_ws_file_payload(file_path: str, caption: str | None = None) -> dict[str, Any]:
    path = Path(file_path).resolve()
    data = path.read_bytes()
    mime_type, _ = mimetypes.guess_type(str(path))
    return {
        "name": path.name,
        "path": str(path),
        "mime_type": mime_type or "application/octet-stream",
        "size_bytes": len(data),
        "encoding": "base64",
        "data": base64.b64encode(data).decode("ascii"),
        "caption": caption or None,
    }


def _serialize_worker_snapshot(rows: list[Any]) -> list[dict[str, Any]]:
    snapshot: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "model_dump"):
            try:
                snapshot.append(row.model_dump(mode="json"))
            except TypeError:
                snapshot.append(row.model_dump())
            continue
        if isinstance(row, dict):
            snapshot.append(dict(row))
    return snapshot


def _resolve_ws_chat_id(settings: Any) -> int:
    if settings.allowed_telegram_chat_ids:
        first = settings.allowed_telegram_chat_ids.split(",")[0].strip()
        if first:
            try:
                value = int(first)
                if value > 0:
                    return value
            except ValueError:
                pass
    # Keep WS-only sessions on a positive ID so worker follow-ups are not suppressed.
    return 1_000_000_000 + (uuid.uuid4().int % 100_000_000)


@dataclass
class _ActiveWsSession:
    connection_id: str
    socket: WebSocket
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    closed: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class WsApprovalManager:
    send: callable
    timeout_seconds: int = 60
    _pending: dict[str, asyncio.Future] = field(default_factory=dict)

    async def request_approval(self, intent) -> bool:
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[intent.id] = future
        await self.send(
            {
                "type": "approval_request",
                "intent": intent.model_dump(),
            }
        )
        try:
            return await asyncio.wait_for(future, timeout=self.timeout_seconds)
        except TimeoutError:
            self._pending.pop(intent.id, None)
            return False

    def resolve(self, intent_id: str, approved: bool) -> bool:
        future = self._pending.pop(intent_id, None)
        if not future or future.done():
            return False
        future.set_result(approved)
        return True


def register_ws_routes(app: FastAPI) -> None:
    app.state.ws_session_lock = getattr(app.state, "ws_session_lock", asyncio.Lock())
    app.state.active_ws_session = getattr(app.state, "active_ws_session", None)

    @app.websocket("/ws")
    async def websocket_endpoint(socket: WebSocket) -> None:
        # 1. IP Validation (Tailscale)
        client_host = socket.client.host
        settings = app.state.settings

        # Merge configured and automatically discovered IPs
        allowed_ips = [ip.strip() for ip in settings.tailscale_ips.split(",") if ip.strip()]
        if not allowed_ips:
            # Fallback: try to discover automatically if nothing is configured
            allowed_ips = get_tailscale_ips()
            if allowed_ips:
                logger.info("Automatically discovered Tailscale IPs", ips=allowed_ips)

        is_local = client_host in ("127.0.0.1", "::1", "localhost")

        if allowed_ips and not is_local and client_host not in allowed_ips:
             logger.warning("Rejected WebSocket connection from unauthorized IP", host=client_host)
             await socket.close(code=status.WS_1008_POLICY_VIOLATION)
             return

        await socket.accept()
        logger.info("WebSocket connection established", host=client_host)
        connection_id = f"ws-{uuid.uuid4().hex}"
        session_chat_id = _resolve_ws_chat_id(settings)
        session = _ActiveWsSession(connection_id=connection_id, socket=socket)

        octo: Octo | None = getattr(app.state, "octo", None)
        if not octo:
            logger.error("Octo not initialized in app state")
            await _ws_send_json(
                session,
                {"type": "error", "message": "Octo not initialized"},
                event_name="init_error",
            )
            await socket.close(code=status.WS_1011_INTERNAL_ERROR)
            return

        # Define WS-specific output channel
        async def _ws_send(chat_id: int, text: str) -> None:
            if should_suppress_user_delivery(text):
                logger.debug("Suppressed control response for WebSocket delivery", chat_id=chat_id)
                return
            await _ws_send_json(
                session,
                {"type": "message", "text": text},
                event_name="channel_message",
                chat_id=chat_id,
            )

        async def _ws_progress(chat_id: int, state: str, text: str, meta: dict) -> None:
            await _ws_send_json(
                session,
                {"type": "progress", "state": state, "text": text, "meta": meta},
                event_name="progress",
                chat_id=chat_id,
            )

        async def _ws_typing(chat_id: int, active: bool) -> None:
            await _ws_send_json(
                session,
                {"type": "typing", "active": active},
                event_name="typing",
                chat_id=chat_id,
            )

        async def _ws_send_file(chat_id: int, file_path: str, caption: str | None = None) -> None:
            payload = _build_ws_file_payload(file_path, caption=caption)
            await _ws_send_json(
                session,
                {"type": "file", **payload},
                event_name="file",
                chat_id=chat_id,
            )

        async def _ws_worker_event(chat_id: int, event: str, payload: dict[str, Any]) -> None:
            await _ws_send_json(
                session,
                {"type": "worker_event", "event": event, "payload": payload},
                event_name="worker_event",
                chat_id=chat_id,
            )

        # A newer WS client takes over the interactive channel from any older session.
        async with app.state.ws_session_lock:
            previous_session: _ActiveWsSession | None = getattr(app.state, "active_ws_session", None)
            if previous_session and previous_session.connection_id != connection_id and not previous_session.closed.is_set():
                logger.info(
                    "Taking over active WebSocket session",
                    host=client_host,
                    previous_owner=previous_session.connection_id,
                    new_owner=connection_id,
                )
                try:
                    await _ws_send_json(
                        previous_session,
                        {
                            "type": "warning",
                            "message": "Another WebSocket client connected and took over this session.",
                        },
                        event_name="takeover_warning",
                    )
                except Exception:
                    logger.debug("Failed to notify previous WebSocket session before takeover", exc_info=True)

                try:
                    await previous_session.socket.close(code=status.WS_1000_NORMAL_CLOSURE)
                except Exception:
                    logger.debug("Failed to close previous WebSocket session during takeover", exc_info=True)

                try:
                    await asyncio.wait_for(previous_session.closed.wait(), timeout=2.0)
                except TimeoutError:
                    logger.warning(
                        "Timed out waiting for previous WebSocket session to close",
                        previous_owner=previous_session.connection_id,
                        new_owner=connection_id,
                    )

            claimed = octo.set_output_channel(
                True,
                send=_ws_send,
                send_file=_ws_send_file,
                progress=_ws_progress,
                typing=_ws_typing,
                worker_event=_ws_worker_event,
                owner_id=connection_id,
                force=True,
            )
            if claimed:
                app.state.active_ws_session = session

        if not claimed:
            await _ws_send_json(
                session,
                {"type": "error", "message": "Another WebSocket session is currently active."},
                event_name="session_conflict",
            )
            await socket.close(code=status.WS_1013_TRY_AGAIN_LATER)
            return

        try:
            active_workers = await asyncio.to_thread(octo.store.get_active_workers)
        except Exception:
            logger.debug("Failed to load active workers snapshot for WebSocket session", exc_info=True)
            active_workers = []
        await _ws_send_json(
            session,
            {
                "type": "workers_snapshot",
                "workers": _serialize_worker_snapshot(active_workers),
            },
            event_name="workers_snapshot",
        )

        approvals = WsApprovalManager(send=lambda payload: _ws_send_json(session, payload, event_name="approval_request"))
        message_lock = asyncio.Lock()
        tasks: set[asyncio.Task] = set()

        try:
            while True:
                message = await socket.receive_json()
                msg_type = message.get("type")

                if msg_type == "message":
                    # Use a positive chat_id so internal worker follow-ups are delivered.
                    chat_id = session_chat_id
                    payload_chat_id = message.get("chat_id")
                    if isinstance(payload_chat_id, int) and payload_chat_id > 0:
                        chat_id = payload_chat_id

                    task = asyncio.create_task(
                        _handle_message(session, octo, approvals, message, chat_id, message_lock)
                    )
                    tasks.add(task)
                    task.add_done_callback(lambda t: tasks.discard(t))
                    continue

                if msg_type == "approval_response":
                    approvals.resolve(
                        str(message.get("intent_id")),
                        bool(message.get("approved")),
                    )
                    continue

                if msg_type == "ping":
                    await _ws_send_json(session, {"type": "pong"}, event_name="pong")
        except WebSocketDisconnect:
            logger.info("WebSocket disconnected", host=client_host)
        finally:
            # Switch back to Telegram when WS closes
            octo.set_output_channel(False, owner_id=connection_id)
            session.closed.set()
            active_session: _ActiveWsSession | None = getattr(app.state, "active_ws_session", None)
            if active_session and active_session.connection_id == connection_id:
                app.state.active_ws_session = None
            for task in tasks:
                task.cancel()


async def _handle_message(
    session: _ActiveWsSession,
    octo: Octo,
    approvals: WsApprovalManager,
    payload: dict[str, Any],
    chat_id: int,
    message_lock: asyncio.Lock,
) -> None:
    text = str(payload.get("text", ""))
    try:
        async with message_lock:
            response = await octo.handle_message(
                text,
                chat_id,
                approval_requester=approvals.request_approval,
                is_ws=True,
            )
    except Exception as exc:
        logger.exception("Octo failed to handle WS message")
        response = f"Error: {exc}"

    text_out = response.immediate if isinstance(response, OctoReply) else str(response)
    decision = resolve_user_delivery(text_out)
    if not decision.user_visible:
        logger.debug("Suppressed control response for WebSocket reply", chat_id=chat_id)
        return
    logger.info(
        "Sending final WebSocket reply",
        chat_id=chat_id,
        connection_id=session.connection_id,
        text_len=len(decision.text),
    )
    await _ws_send_json(
        session,
        {"type": "message", "text": decision.text},
        event_name="final_reply",
        chat_id=chat_id,
    )
