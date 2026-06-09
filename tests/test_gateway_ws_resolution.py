from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from octopal.gateway.ws import (
    DESKTOP_WS_CHAT_ID,
    WsApprovalManager,
    _ActiveWsSession,
    _build_ws_file_payload,
    _extract_ws_saved_file_paths,
    _handle_message,
    _is_local_ws_client,
    _resolve_ws_chat_id,
    _serialize_worker_snapshot,
    _serialize_ws_chat_history,
    register_ws_routes,
)
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.store.models import MemoryEntry, WorkerRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.octo.core import OctoReply
from octopal.runtime.plans import PlanRunService
from octopal.utils import utc_now


def test_resolve_ws_chat_id_returns_positive_when_no_allowlist() -> None:
    settings = SimpleNamespace(allowed_telegram_chat_ids="")
    assert _resolve_ws_chat_id(settings) == DESKTOP_WS_CHAT_ID
    assert _resolve_ws_chat_id(settings) == _resolve_ws_chat_id(settings)


def test_resolve_ws_chat_id_uses_first_allowed_id_when_valid() -> None:
    settings = SimpleNamespace(allowed_telegram_chat_ids="42,100")
    assert _resolve_ws_chat_id(settings) == 42


def test_resolve_ws_chat_id_uses_primary_whatsapp_chat_when_configured() -> None:
    from octopal.channels.whatsapp.ids import whatsapp_chat_id

    settings = SimpleNamespace(
        user_channel="whatsapp",
        allowed_whatsapp_numbers="+15551234567,+15557654321",
        allowed_telegram_chat_ids="42",
    )

    assert _resolve_ws_chat_id(settings) == whatsapp_chat_id("+15551234567")


def test_websocket_client_host_helper_rejects_lan_addresses() -> None:
    assert _is_local_ws_client("127.0.0.1")
    assert _is_local_ws_client("::1")
    assert _is_local_ws_client("testclient")
    assert not _is_local_ws_client("192.168.1.55")


def test_websocket_requires_dashboard_token_when_configured() -> None:
    class DummyOcto:
        def set_output_channel(self, is_ws: bool, **kwargs) -> bool:
            return True

    app = FastAPI()
    app.state.settings = SimpleNamespace(
        tailscale_ips="testclient",
        allowed_telegram_chat_ids="",
        dashboard_token="secret-token",
    )
    app.state.octo = DummyOcto()
    register_ws_routes(app)

    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect), client.websocket_connect("/ws"):
            pass

        with client.websocket_connect("/ws?token=secret-token") as ws:
            assert ws.receive_json() == {"type": "workers_snapshot", "workers": []}


def test_serialize_worker_snapshot_exposes_plan_binding(tmp_path: Path) -> None:
    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        OCTOPAL_STATE_DIR=tmp_path / "state",
        OCTOPAL_WORKSPACE_DIR=tmp_path / "workspace",
    )
    store = SQLiteStore(settings)
    now = utc_now()
    worker_id = "worker-plan-1"
    worker = WorkerRecord(
        id=worker_id,
        status="running",
        task="Patch parser",
        granted_caps=[],
        created_at=now,
        updated_at=now,
    )
    store.create_worker(worker)
    plan = PlanRunService(store).create_run(
        goal="Patch parser bug",
        chat_id=123,
        steps=[{"id": "patch", "kind": "worker", "title": "Patch parser"}],
    )
    PlanRunService(store).bind_worker_step(plan.id, "patch", worker_id)

    snapshot = _serialize_worker_snapshot([worker], store=store)

    assert snapshot[0]["plan_binding"] == {
        "run_id": plan.id,
        "step_id": "patch",
        "status": "awaiting_worker",
        "title": "Patch parser",
        "kind": "worker",
    }


def test_new_websocket_connection_takes_over_previous_session() -> None:
    class DummyOcto:
        def __init__(self) -> None:
            self.owner: str | None = None

        def set_output_channel(self, is_ws: bool, **kwargs) -> bool:
            owner_id = kwargs.get("owner_id")
            force = bool(kwargs.get("force"))
            if is_ws:
                if self.owner and owner_id and self.owner != owner_id and not force:
                    return False
                self.owner = owner_id
                return True
            if self.owner and owner_id and self.owner != owner_id and not force:
                return False
            self.owner = None
            return True

    app = FastAPI()
    app.state.settings = SimpleNamespace(tailscale_ips="testclient", allowed_telegram_chat_ids="")
    app.state.octo = DummyOcto()
    register_ws_routes(app)

    with TestClient(app) as client, client.websocket_connect("/ws") as ws_one:
        assert ws_one.receive_json() == {"type": "workers_snapshot", "workers": []}
        with client.websocket_connect("/ws") as ws_two:
            assert ws_two.receive_json() == {"type": "workers_snapshot", "workers": []}
            payload = ws_one.receive_json()
            assert payload["type"] == "warning"
            assert "took over" in payload["message"]
            ws_two.send_json({"type": "ping"})
            assert ws_two.receive_json() == {"type": "pong"}


def test_build_ws_file_payload_includes_base64_metadata(tmp_path: Path) -> None:
    file_path = tmp_path / "report.txt"
    file_path.write_text("hello ws", encoding="utf-8")

    payload = _build_ws_file_payload(str(file_path), caption="Attached")

    assert payload["name"] == "report.txt"
    assert payload["mime_type"] == "text/plain"
    assert payload["encoding"] == "base64"
    assert payload["caption"] == "Attached"
    assert payload["path"] == str(file_path.resolve())


def test_extract_ws_saved_file_paths_keeps_existing_files_inside_allowed_root(
    tmp_path: Path,
) -> None:
    allowed_root = tmp_path / "workspace" / "tmp" / "desktop_chat"
    allowed_root.mkdir(parents=True)
    file_path = allowed_root / "report.txt"
    file_path.write_text("hello ws", encoding="utf-8")

    paths = _extract_ws_saved_file_paths(
        {
            "attachments": [
                {"path": str(file_path)},
                {"path": str(tmp_path / "missing.txt")},
                "",
            ]
        },
        allowed_roots=[allowed_root],
    )

    assert paths == [str(file_path.resolve())]


def test_extract_ws_saved_file_paths_rejects_files_outside_allowed_root(tmp_path: Path) -> None:
    allowed_root = tmp_path / "workspace" / "tmp" / "desktop_chat"
    allowed_root.mkdir(parents=True)
    file_path = tmp_path / "report.txt"
    file_path.write_text("hello ws", encoding="utf-8")

    paths = _extract_ws_saved_file_paths(
        {"attachments": [{"path": str(file_path)}]},
        allowed_roots=[allowed_root],
    )

    assert paths == []


def test_serialize_ws_chat_history_uses_recent_persisted_messages() -> None:
    def entry(
        entry_id: str,
        role: str,
        content: str,
        *,
        channel: str = "telegram",
        heartbeat: bool = False,
    ) -> MemoryEntry:
        return MemoryEntry(
            id=entry_id,
            role=role,
            content=content,
            embedding=None,
            created_at=datetime(2026, 6, 1, 12, int(entry_id), tzinfo=UTC),
            metadata={"chat_id": 42, "channel": channel, "heartbeat": heartbeat},
        )

    class DummyStore:
        def list_memory_entries_by_chat(self, chat_id: int, limit: int = 50):
            assert chat_id == 42
            assert limit == 50
            return [
                entry("4", "assistant", "new reply", channel="desktop"),
                entry("3", "assistant", "hidden heartbeat", heartbeat=True),
                entry("2", "user", "old question", channel="telegram"),
                entry("1", "system", "hidden system"),
            ]

    history = _serialize_ws_chat_history(SimpleNamespace(store=DummyStore()), 42, limit=10)

    assert [(item["role"], item["text"], item["channel"]) for item in history] == [
        ("user", "old question", "telegram"),
        ("assistant", "new reply", "desktop"),
    ]
    assert all(item["type"] == "chat_message" for item in history)
    assert all(item["meta"]["history"] is True for item in history)


@pytest.mark.asyncio
async def test_ws_approval_manager_resolves_pending_intent_once() -> None:
    sent: list[dict] = []

    async def send(payload: dict) -> None:
        sent.append(payload)

    manager = WsApprovalManager(send=send)
    intent = SimpleNamespace(
        id="intent-1",
        type="exec.run",
        payload={"command": "echo ok"},
        risk="high",
        requires_approval=True,
        model_dump=lambda: {"id": "intent-1"},
    )

    task = asyncio.create_task(manager.request_approval(intent))
    await asyncio.sleep(0)

    assert sent[0]["type"] == "approval_request"
    assert manager.resolve("intent-1", True) is True
    assert await task is True
    assert manager.resolve("intent-1", False) is False


def test_websocket_message_is_delivered_as_client_expects() -> None:
    class DummyStore:
        def get_active_workers(self):
            return []

    class DummyOcto:
        def __init__(self) -> None:
            self.owner: str | None = None
            self.store = DummyStore()

        def set_output_channel(self, is_ws: bool, **kwargs) -> bool:
            owner_id = kwargs.get("owner_id")
            force = bool(kwargs.get("force"))
            if is_ws:
                if self.owner and owner_id and self.owner != owner_id and not force:
                    return False
                self.owner = owner_id
                return True
            if self.owner and owner_id and self.owner != owner_id and not force:
                return False
            self.owner = None
            return True

        async def handle_message(self, text: str, chat_id: int, **kwargs) -> OctoReply:
            assert text == "hello"
            assert chat_id > 0
            return OctoReply(immediate="Hi from Octo", followup=None, followup_required=False)

    app = FastAPI()
    app.state.settings = SimpleNamespace(tailscale_ips="testclient", allowed_telegram_chat_ids="")
    app.state.octo = DummyOcto()
    register_ws_routes(app)

    with TestClient(app) as client, client.websocket_connect("/ws") as ws:
        assert ws.receive_json() == {"type": "workers_snapshot", "workers": []}
        ws.send_json({"type": "message", "text": "hello"})
        assert ws.receive_json() == {"type": "message", "text": "Hi from Octo"}


@pytest.mark.asyncio
async def test_websocket_message_handler_serializes_octo_turns() -> None:
    class FakeSocket:
        def __init__(self) -> None:
            self.sent: list[dict] = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    class DummyOcto:
        def __init__(self) -> None:
            self.active = 0
            self.max_active = 0

        async def handle_message(self, text: str, chat_id: int, **kwargs) -> OctoReply:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            await asyncio.sleep(0.01)
            self.active -= 1
            return OctoReply(immediate=f"reply:{text}", followup=None, followup_required=False)

    socket = FakeSocket()
    session = _ActiveWsSession(connection_id="test", socket=socket)  # type: ignore[arg-type]
    approvals = WsApprovalManager(send=lambda payload: None)
    octo = DummyOcto()
    lock = asyncio.Lock()

    await asyncio.gather(
        _handle_message(session, octo, approvals, {"type": "message", "text": "one"}, 42, lock),
        _handle_message(session, octo, approvals, {"type": "message", "text": "two"}, 42, lock),
    )

    assert octo.max_active == 1
    assert [payload["text"] for payload in socket.sent] == ["reply:one", "reply:two"]


@pytest.mark.asyncio
async def test_websocket_message_handler_passes_attachment_paths(tmp_path: Path) -> None:
    class FakeSocket:
        def __init__(self) -> None:
            self.sent: list[dict] = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    class DummyOcto:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def handle_message(self, text: str, chat_id: int, **kwargs) -> OctoReply:
            self.calls.append({"text": text, "chat_id": chat_id, "kwargs": kwargs})
            return OctoReply(immediate="got it", followup=None, followup_required=False)

    attachment_root = tmp_path / "workspace" / "tmp" / "desktop_chat"
    attachment_root.mkdir(parents=True)
    file_path = attachment_root / "report.txt"
    file_path.write_text("hello ws", encoding="utf-8")
    socket = FakeSocket()
    session = _ActiveWsSession(connection_id="test", socket=socket)  # type: ignore[arg-type]
    approvals = WsApprovalManager(send=lambda payload: None)
    octo = DummyOcto()

    await _handle_message(
        session,
        octo,
        approvals,
        {"type": "message", "text": "read this", "attachments": [{"path": str(file_path)}]},
        42,
        asyncio.Lock(),
        [attachment_root],
    )

    assert octo.calls[0]["kwargs"]["saved_file_paths"] == [str(file_path.resolve())]
    assert socket.sent == [{"type": "message", "text": "got it"}]


@pytest.mark.asyncio
async def test_websocket_message_handler_skips_legacy_reply_after_mirror_event() -> None:
    class FakeSocket:
        def __init__(self) -> None:
            self.sent: list[dict] = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    socket = FakeSocket()
    session = _ActiveWsSession(connection_id="test", socket=socket)  # type: ignore[arg-type]

    class DummyOcto:
        async def handle_message(self, text: str, chat_id: int, **kwargs) -> OctoReply:
            del text, chat_id, kwargs
            session.mirrored_assistant_messages += 1
            return OctoReply(immediate="already mirrored", followup=None, followup_required=False)

    await _handle_message(
        session,
        DummyOcto(),  # type: ignore[arg-type]
        WsApprovalManager(send=lambda payload: None),
        {"type": "message", "text": "hello"},
        42,
        asyncio.Lock(),
    )

    assert socket.sent == []
