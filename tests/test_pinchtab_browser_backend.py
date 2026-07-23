from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path

import httpx

from octopal.browser.pinchtab import PinchTabBrowserBackend


def test_pinchtab_backend_preserves_chat_tab_isolation_and_auth() -> None:
    requests: list[tuple[str, str, dict[str, object], str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        agent = request.headers.get("X-Agent-Id", "")
        auth = request.headers.get("Authorization", "")
        requests.append((request.method, request.url.path, body, agent, auth))
        if request.url.path == "/navigate":
            tab_id = "tab-7" if agent == "octopal-7" else "tab-8"
            return httpx.Response(200, json={"tabId": tab_id, "url": body["url"]})
        if request.url.path == "/tabs":
            return httpx.Response(
                200,
                json={
                    "tabs": [
                        {"id": "tab-7", "url": "https://seven.test", "title": "Seven"},
                        {"id": "tab-8", "url": "https://eight.test", "title": "Eight"},
                        {"id": "foreign", "url": "https://foreign.test", "title": "No"},
                    ]
                },
            )
        if request.url.path.endswith("/close"):
            return httpx.Response(200, json={"ok": True})
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    backend = PinchTabBrowserBackend(
        base_url="http://pinchtab.test",
        timeout_seconds=5,
        browser="chrome",
        session="ses_secret",
        transport=httpx.MockTransport(handler),
    )

    async def scenario() -> None:
        opened_7 = await backend.open({"url": "https://seven.test"}, {"chat_id": 7})
        opened_8 = await backend.open({"url": "https://eight.test"}, {"chat_id": 8})
        assert opened_7["target_id"] == "tab-7"
        assert opened_8["target_id"] == "tab-8"

        tabs_7 = await backend.tabs({}, {"chat_id": 7})
        tabs_8 = await backend.tabs({}, {"chat_id": 8})
        assert [page["target_id"] for page in tabs_7["pages"]] == ["tab-7"]
        assert [page["target_id"] for page in tabs_8["pages"]] == ["tab-8"]

        denied = await backend.close({"target_id": "tab-8"}, {"chat_id": 7})
        assert denied.startswith("Error closing tab-8")
        closed = await backend.close({}, {"chat_id": 7})
        assert closed == "Browser session closed"

    asyncio.run(scenario())

    assert all(row[4] == "Session ses_secret" for row in requests)
    assert ("POST", "/tabs/tab-7/close", {}, "octopal-7", "Session ses_secret") in requests
    assert not any(row[1] == "/close" for row in requests)


def test_pinchtab_backend_runs_snapshot_actions_wait_and_extract() -> None:
    calls: list[tuple[str, str, dict[str, object]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        calls.append((request.method, request.url.path, body))
        if request.url.path == "/navigate":
            return httpx.Response(200, json={"tabId": "tab-one", "url": body["url"]})
        if request.url.path.endswith("/snapshot"):
            return httpx.Response(
                200, json={"snapshot": "[e1] button Save\n- textbox Email [ref=e2]"}
            )
        if request.url.path.endswith("/action") or request.url.path.endswith("/wait"):
            return httpx.Response(200, json={"ok": True})
        if request.url.path.endswith("/text"):
            return httpx.Response(
                200,
                json={"url": "https://example.test", "title": "Example", "text": "Page body"},
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    backend = PinchTabBrowserBackend(
        base_url="http://pinchtab.test",
        timeout_seconds=5,
        browser="cloak",
        token="server-token",
        transport=httpx.MockTransport(handler),
    )

    async def scenario() -> None:
        await backend.open({"url": "https://example.test"}, {"chat_id": 7})
        snapshot = await backend.snapshot({}, {"chat_id": 7})
        assert snapshot["refs_count"] == 2
        assert snapshot["target_id"] == "tab-one"
        assert await backend.click({"ref": "e1"}, {"chat_id": 7}) == "Clicked e1"
        assert (
            await backend.type(
                {"ref": "e2", "text": "ada@example.test", "press_enter": True},
                {"chat_id": 7},
            )
            == "Typed into e2"
        )
        assert (
            await backend.wait_for({"text": "Done", "timeout_ms": 1234}, {"chat_id": 7})
            == "Text appeared: Done"
        )
        extracted = await backend.extract({}, {"chat_id": 7})
        assert extracted["text"] == "Page body"
        assert extracted["title"] == "Example"
        assert extracted["content_chars"] == len("Page body")
        assert extracted["truncated"] is False

    asyncio.run(scenario())

    assert calls[0][2] == {
        "url": "https://example.test",
        "browser": "cloak",
        "newTab": True,
    }
    action_payloads = [body for method, path, body in calls if path.endswith("/action")]
    assert action_payloads == [
        {"kind": "click", "selector": "e1"},
        {"kind": "fill", "selector": "e2", "value": "ada@example.test"},
        {"kind": "press", "key": "Enter"},
    ]
    wait_payload = next(body for method, path, body in calls if path.endswith("/wait"))
    assert wait_payload == {"timeout": 1234, "state": "visible", "text": "Done"}


def test_pinchtab_backend_adopts_popup_returned_by_click(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/navigate":
            return httpx.Response(200, json={"tabId": "opener", "url": "https://one.test"})
        if request.url.path.endswith("/action"):
            return httpx.Response(200, json={"ok": True, "switchedToTab": "popup"})
        if request.url.path == "/tabs":
            return httpx.Response(
                200,
                json={
                    "tabs": [
                        {"id": "opener", "url": "https://one.test", "title": "One"},
                        {"id": "popup", "url": "https://two.test", "title": "Two"},
                    ]
                },
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    backend = PinchTabBrowserBackend(
        base_url="http://pinchtab.test",
        timeout_seconds=5,
        browser="chrome",
        ownership_file=tmp_path / "tabs.json",
        transport=httpx.MockTransport(handler),
    )

    async def scenario() -> None:
        await backend.open({"url": "https://one.test"}, {"chat_id": 7})
        assert await backend.click({"ref": "e1"}, {"chat_id": 7}) == "Clicked e1"
        tabs = await backend.tabs({}, {"chat_id": 7})
        assert [page["target_id"] for page in tabs["pages"]] == ["opener", "popup"]
        popup = next(page for page in tabs["pages"] if page["target_id"] == "popup")
        assert popup["is_current"] is True
        assert json.loads((tmp_path / "tabs.json").read_text(encoding="utf-8")) == [
            "opener",
            "popup",
        ]

    asyncio.run(scenario())


def test_pinchtab_backend_writes_valid_inline_screenshot(tmp_path: Path, monkeypatch) -> None:
    image = b"\x89PNG\r\n\x1a\nfixture"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/navigate":
            return httpx.Response(200, json={"tabId": "tab-shot", "url": "https://shot.test"})
        if request.url.path.endswith("/screenshot"):
            return httpx.Response(
                200, json={"format": "png", "base64": base64.b64encode(image).decode()}
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    monkeypatch.setattr("octopal.browser.pinchtab.tempfile.gettempdir", lambda: str(tmp_path))
    backend = PinchTabBrowserBackend(
        base_url="http://pinchtab.test",
        timeout_seconds=5,
        browser="chrome",
        transport=httpx.MockTransport(handler),
    )

    async def scenario() -> None:
        await backend.open({"url": "https://shot.test"}, {"chat_id": 7})
        result = await backend.screenshot({"full_page": True}, {"chat_id": 7})
        assert result["ok"] is True
        assert Path(result["path"]).read_bytes() == image

    asyncio.run(scenario())
