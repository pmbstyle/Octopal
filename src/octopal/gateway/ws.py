from __future__ import annotations

import asyncio
import base64
import mimetypes
import os
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status

from octopal.runtime.intents.approval_format import approval_display_payload
from octopal.runtime.octo.core import Octo, OctoReply
from octopal.runtime.octo.delivery import resolve_user_delivery
from octopal.utils import get_tailscale_ips, should_suppress_user_delivery

logger = structlog.get_logger(__name__)
DESKTOP_WS_CHAT_ID = 1_000_000_000


def _is_local_ws_client(client_host: str) -> bool:
    return client_host in ("127.0.0.1", "::1", "localhost", "testclient")


def _provided_ws_token(socket: WebSocket) -> str:
    auth_header = socket.headers.get("authorization", "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return str(socket.query_params.get("token", "")).strip()


async def _reject_ws(socket: WebSocket, *, host: str, reason: str) -> None:
    logger.warning("Rejected WebSocket connection", host=host, reason=reason)
    await socket.close(code=status.WS_1008_POLICY_VIOLATION)


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


def _resolve_ws_attachment_roots(settings: Any | None = None) -> tuple[Path, ...]:
    workspace_dir = getattr(settings, "workspace_dir", None)
    if workspace_dir is None:
        workspace_dir = os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")
    return ((Path(workspace_dir).expanduser().resolve() / "tmp" / "desktop_chat"),)


def _path_is_inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _extract_ws_saved_file_paths(
    payload: dict[str, Any],
    *,
    allowed_roots: Iterable[Path | str] | None = None,
) -> list[str]:
    attachments = payload.get("attachments")
    if not isinstance(attachments, list):
        return []

    roots = tuple(Path(root).expanduser().resolve() for root in (allowed_roots or ()))
    if not roots:
        roots = _resolve_ws_attachment_roots()

    saved_paths: list[str] = []
    for attachment in attachments[:8]:
        raw_path: Any = None
        if isinstance(attachment, str):
            raw_path = attachment
        elif isinstance(attachment, dict):
            raw_path = attachment.get("path")
        path_text = str(raw_path or "").strip()
        if not path_text:
            continue
        path = Path(path_text).expanduser().resolve()
        if not any(_path_is_inside(path, root) for root in roots):
            logger.warning(
                "Ignoring WebSocket attachment outside allowed roots",
                path=str(path),
                allowed_roots=[str(root) for root in roots],
            )
            continue
        if not path.is_file():
            logger.warning("Ignoring missing WebSocket attachment", path=str(path))
            continue
        saved_paths.append(str(path))
    return saved_paths


def _serialize_worker_snapshot(rows: list[Any], *, store: Any | None = None) -> list[dict[str, Any]]:
    snapshot: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "model_dump"):
            try:
                payload = row.model_dump(mode="json")
            except TypeError:
                payload = row.model_dump()
            plan_binding = _worker_plan_binding_payload(store, str(payload.get("id") or ""))
            if plan_binding:
                payload["plan_binding"] = plan_binding
            snapshot.append(payload)
            continue
        if isinstance(row, dict):
            payload = dict(row)
            plan_binding = _worker_plan_binding_payload(store, str(payload.get("id") or ""))
            if plan_binding:
                payload["plan_binding"] = plan_binding
            snapshot.append(payload)
    return snapshot


def _worker_plan_binding_payload(store: Any | None, worker_id: str) -> dict[str, Any] | None:
    if store is None or not worker_id:
        return None
    getter = getattr(store, "get_plan_step_by_worker_run_id", None)
    if not callable(getter):
        return None
    try:
        step = getter(worker_id)
    except Exception:
        logger.debug("Failed to load worker plan binding for WebSocket snapshot", exc_info=True)
        return None
    if step is None:
        return None
    return {
        "run_id": getattr(step, "run_id", None),
        "step_id": getattr(step, "step_id", None),
        "status": getattr(step, "status", None),
        "title": getattr(step, "title", None),
        "kind": getattr(step, "kind", None),
    }


def _is_ws_history_entry(entry: Any) -> bool:
    role = str(getattr(entry, "role", "") or "").strip().lower()
    if role not in {"user", "assistant"}:
        return False
    content = str(getattr(entry, "content", "") or "").strip()
    if not content:
        return False
    metadata = getattr(entry, "metadata", None) or {}
    internal_flags = (
        "heartbeat",
        "worker_result",
        "planner",
        "scheduler",
        "control_plane",
    )
    return not any(bool(metadata.get(flag)) for flag in internal_flags)


def _serialize_ws_chat_history(octo: Octo, chat_id: int, limit: int = 10) -> list[dict[str, Any]]:
    store = getattr(octo, "store", None)
    list_entries = getattr(store, "list_memory_entries_by_chat", None)
    if not callable(list_entries):
        return []

    try:
        entries = list_entries(chat_id, limit=max(limit * 5, 50))
    except Exception:
        logger.debug("Failed to load WebSocket chat history", chat_id=chat_id, exc_info=True)
        return []

    history_entries = [entry for entry in entries if _is_ws_history_entry(entry)][:limit]
    history_entries.reverse()
    history: list[dict[str, Any]] = []
    for entry in history_entries:
        role = str(getattr(entry, "role", "") or "").strip().lower()
        metadata = getattr(entry, "metadata", None) or {}
        channel = str(metadata.get("channel") or metadata.get("source_channel") or "chat").strip()
        created_at = getattr(entry, "created_at", None)
        history.append(
            {
                "id": str(getattr(entry, "id", "") or ""),
                "type": "chat_message",
                "direction": "inbound" if role == "user" else "outbound",
                "role": role,
                "channel": channel or "chat",
                "chat_id": chat_id,
                "text": str(getattr(entry, "content", "") or ""),
                "meta": {
                    "history": True,
                    "saved_file_paths": list(metadata.get("saved_file_paths") or []),
                    "has_images": bool(metadata.get("has_images")),
                    "has_files": bool(metadata.get("has_files")),
                },
                "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else None,
            }
        )
    return history


def _resolve_ws_chat_id(settings: Any) -> int:
    if str(getattr(settings, "user_channel", "") or "").strip().lower() == "whatsapp":
        try:
            from octopal.channels.whatsapp.ids import (
                parse_allowed_whatsapp_numbers,
                whatsapp_chat_id,
            )

            numbers = parse_allowed_whatsapp_numbers(
                str(getattr(settings, "allowed_whatsapp_numbers", "") or "")
            )
            if numbers:
                return whatsapp_chat_id(numbers[0])
        except Exception:
            logger.debug("Failed to resolve WhatsApp chat id for WebSocket session", exc_info=True)

    allowed_telegram_chat_ids = str(getattr(settings, "allowed_telegram_chat_ids", "") or "")
    if allowed_telegram_chat_ids:
        first = allowed_telegram_chat_ids.split(",")[0].strip()
        if first:
            try:
                value = int(first)
                if value > 0:
                    return value
            except ValueError:
                pass
    # Keep WS-only sessions on a stable positive ID so desktop chat history survives reconnects.
    return DESKTOP_WS_CHAT_ID


@dataclass
class _ActiveWsSession:
    connection_id: str
    socket: WebSocket
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    closed: asyncio.Event = field(default_factory=asyncio.Event)
    mirrored_assistant_messages: int = 0


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
                "display": approval_display_payload(intent),
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

        is_local = _is_local_ws_client(client_host)
        if not is_local and not allowed_ips:
            await _reject_ws(socket, host=client_host, reason="no Tailscale allowlist available")
            return

        if allowed_ips and not is_local and client_host not in allowed_ips:
            await _reject_ws(socket, host=client_host, reason="host not in Tailscale allowlist")
            return

        expected_token = str(getattr(settings, "dashboard_token", "") or "").strip()
        if expected_token and _provided_ws_token(socket) != expected_token:
            await _reject_ws(socket, host=client_host, reason="invalid dashboard token")
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

        async def _ws_chat_message(chat_id: int, payload: dict[str, Any]) -> None:
            await _ws_send_json(
                session,
                payload,
                event_name="chat_message",
                chat_id=chat_id,
            )
            if payload.get("direction") == "outbound" and payload.get("role") == "assistant":
                session.mirrored_assistant_messages += 1

        # A newer WS client takes over the interactive channel from any older session.
        async with app.state.ws_session_lock:
            previous_session: _ActiveWsSession | None = getattr(
                app.state, "active_ws_session", None
            )
            if (
                previous_session
                and previous_session.connection_id != connection_id
                and not previous_session.closed.is_set()
            ):
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
                    logger.debug(
                        "Failed to notify previous WebSocket session before takeover", exc_info=True
                    )

                try:
                    await previous_session.socket.close(code=status.WS_1000_NORMAL_CLOSURE)
                except Exception:
                    logger.debug(
                        "Failed to close previous WebSocket session during takeover", exc_info=True
                    )

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
                message_event=_ws_chat_message,
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

        worker_store = getattr(octo, "store", None)
        try:
            active_workers = await asyncio.to_thread(worker_store.get_active_workers)
        except Exception:
            logger.debug(
                "Failed to load active workers snapshot for WebSocket session", exc_info=True
            )
            active_workers = []
        await _ws_send_json(
            session,
            {
                "type": "workers_snapshot",
                "workers": _serialize_worker_snapshot(active_workers, store=worker_store),
            },
            event_name="workers_snapshot",
        )
        chat_history = await asyncio.to_thread(
            _serialize_ws_chat_history,
            octo,
            session_chat_id,
            10,
        )
        if chat_history:
            await _ws_send_json(
                session,
                {
                    "type": "chat_history",
                    "messages": chat_history,
                },
                event_name="chat_history",
                chat_id=session_chat_id,
            )

        approvals = WsApprovalManager(
            send=lambda payload: _ws_send_json(session, payload, event_name="approval_request")
        )
        attachment_roots = _resolve_ws_attachment_roots(settings)
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
                        _handle_message(
                            session,
                            octo,
                            approvals,
                            message,
                            chat_id,
                            message_lock,
                            attachment_roots,
                        )
                    )
                    tasks.add(task)
                    task.add_done_callback(lambda t: tasks.discard(t))
                    continue

                if msg_type == "approval_response":
                    intent_id = str(message.get("intent_id") or "").strip()
                    approved = bool(message.get("approved"))
                    resolved = approvals.resolve(intent_id, approved) if intent_id else False
                    await _ws_send_json(
                        session,
                        {
                            "type": "approval_result",
                            "intent_id": intent_id,
                            "approved": approved,
                            "ok": resolved,
                            "message": (
                                "Approval response accepted."
                                if resolved
                                else "Approval request is no longer pending."
                            ),
                        },
                        event_name="approval_result",
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
    attachment_roots: Iterable[Path | str] | None = None,
) -> None:
    text = str(payload.get("text", ""))
    saved_file_paths = _extract_ws_saved_file_paths(payload, allowed_roots=attachment_roots)
    mirrored_before = session.mirrored_assistant_messages
    try:
        async with message_lock:
            emit_typing = getattr(octo, "emit_ws_typing", None)
            if callable(emit_typing):
                await emit_typing(chat_id, True)
            response = await octo.handle_message(
                text,
                chat_id,
                approval_requester=approvals.request_approval,
                is_ws=True,
                saved_file_paths=saved_file_paths,
                show_typing=False,
                source_channel="desktop",
            )
            if callable(emit_typing):
                await emit_typing(chat_id, False)
    except Exception as exc:
        logger.exception("Octo failed to handle WS message")
        response = f"Error: {exc}"
        emit_typing = getattr(octo, "emit_ws_typing", None)
        if callable(emit_typing):
            await emit_typing(chat_id, False)

    text_out = response.immediate if isinstance(response, OctoReply) else str(response)
    decision = resolve_user_delivery(text_out)
    if not decision.user_visible:
        logger.debug("Suppressed control response for WebSocket reply", chat_id=chat_id)
        return
    if session.mirrored_assistant_messages > mirrored_before:
        logger.debug(
            "Skipped legacy final WebSocket reply after mirrored assistant event", chat_id=chat_id
        )
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
