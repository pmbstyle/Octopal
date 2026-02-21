from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
import structlog

from broodmind.queen.core import Queen, QueenReply
from broodmind.utils import get_tailscale_ips

logger = structlog.get_logger(__name__)


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

        queen: Queen | None = getattr(app.state, "queen", None)
        if not queen:
            logger.error("Queen not initialized in app state")
            await socket.send_json({"type": "error", "message": "Queen not initialized"})
            await socket.close(code=status.WS_1011_INTERNAL_ERROR)
            return
        
        # Define WS-specific output channel
        async def _ws_send(chat_id: int, text: str) -> None:
            await socket.send_json({"type": "message", "text": text})

        async def _ws_progress(chat_id: int, state: str, text: str, meta: dict) -> None:
            await socket.send_json({"type": "progress", "state": state, "text": text, "meta": meta})

        async def _ws_typing(chat_id: int, active: bool) -> None:
            await socket.send_json({"type": "typing", "active": active})

        # Switch Queen to use WebSocket for output
        claimed = queen.set_output_channel(
            True,
            send=_ws_send,
            progress=_ws_progress,
            typing=_ws_typing,
            owner_id=connection_id,
        )
        if not claimed:
            await socket.send_json({"type": "error", "message": "Another WebSocket session is currently active."})
            await socket.close(code=status.WS_1013_TRY_AGAIN_LATER)
            return
        
        approvals = WsApprovalManager(send=lambda payload: socket.send_json(payload))
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

                    task = asyncio.create_task(_handle_message(socket, queen, approvals, message, chat_id))
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
                    await socket.send_json({"type": "pong"})
        except WebSocketDisconnect:
            logger.info("WebSocket disconnected", host=client_host)
        finally:
            # Switch back to Telegram when WS closes
            queen.set_output_channel(False, owner_id=connection_id)
            for task in tasks:
                task.cancel()


async def _handle_message(
    socket: WebSocket,
    queen: Queen,
    approvals: WsApprovalManager,
    payload: dict[str, Any],
    chat_id: int,
) -> None:
    text = str(payload.get("text", ""))
    try:
        response = await queen.handle_message(
            text,
            chat_id,
            approval_requester=approvals.request_approval,
            is_ws=True,
        )
    except Exception as exc:
        logger.exception("Queen failed to handle WS message")
        response = f"Error: {exc}"

    text_out = response.immediate if isinstance(response, QueenReply) else str(response)
    await socket.send_json({"type": "message", "text": text_out})
