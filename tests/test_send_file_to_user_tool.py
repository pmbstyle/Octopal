from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx

from octopal.tools.communication.send_file import send_file_to_user


class _FakeOcto:
    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []

    async def internal_send_file(
        self, chat_id: int, file_path: str, *, caption: str | None = None
    ) -> None:
        self.sent.append({"chat_id": chat_id, "file_path": file_path, "caption": caption})


def test_send_file_to_user_sends_existing_workspace_file(tmp_path: Path) -> None:
    octo = _FakeOcto()
    target = tmp_path / "reports" / "summary.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("hello", encoding="utf-8")

    payload = asyncio.run(
        send_file_to_user(
            {"path": "reports/summary.txt", "caption": "Here you go"},
            {"octo": octo, "chat_id": 42, "base_dir": tmp_path},
        )
    )
    data = json.loads(payload)

    assert data["status"] == "success"
    assert data["source"] == "path"
    assert octo.sent == [
        {"chat_id": 42, "file_path": str(target.resolve()), "caption": "Here you go"}
    ]


def test_send_file_to_user_accepts_negative_group_chat_id(tmp_path: Path) -> None:
    octo = _FakeOcto()
    target = tmp_path / "reports" / "group.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("hello group", encoding="utf-8")

    payload = asyncio.run(
        send_file_to_user(
            {"path": "reports/group.txt"},
            {"octo": octo, "chat_id": -1001234567890, "base_dir": tmp_path},
        )
    )
    data = json.loads(payload)

    assert data["status"] == "success"
    assert octo.sent == [
        {"chat_id": -1001234567890, "file_path": str(target.resolve()), "caption": None}
    ]


def test_send_file_to_user_rejects_path_outside_workspace(tmp_path: Path) -> None:
    octo = _FakeOcto()

    payload = asyncio.run(
        send_file_to_user(
            {"path": "../secret.txt"},
            {"octo": octo, "chat_id": 42, "base_dir": tmp_path},
        )
    )
    data = json.loads(payload)

    assert data["status"] == "error"
    assert "unsafe file path" in data["message"]
    assert octo.sent == []


def test_send_file_to_user_downloads_url_before_sending(monkeypatch, tmp_path: Path) -> None:
    octo = _FakeOcto()

    class _FakeStreamResponse:
        headers = {"content-type": "text/plain"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        def raise_for_status(self) -> None:
            return None

        async def aiter_bytes(self):
            yield b"hello world"

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        def stream(self, method: str, url: str):
            assert method == "GET"
            assert url == "https://example.com/files/report.txt"
            return _FakeStreamResponse()

    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)

    payload = asyncio.run(
        send_file_to_user(
            {"url": "https://example.com/files/report.txt", "caption": "Downloaded"},
            {"octo": octo, "chat_id": 7, "base_dir": tmp_path},
        )
    )
    data = json.loads(payload)

    assert data["status"] == "success"
    assert data["source"] == "url"
    sent_path = Path(str(octo.sent[0]["file_path"]))
    assert sent_path.is_file()
    assert sent_path.read_text(encoding="utf-8") == "hello world"
    assert sent_path.parent == (tmp_path / "tmp" / "outbound_files")
