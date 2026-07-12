from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import httpx

from octopal.infrastructure.config.settings import Settings
from octopal.runtime.workers.runtime import WorkerRuntime


def test_worker_runtime_mints_and_revokes_pinchtab_session(tmp_path: Path, monkeypatch) -> None:
    requests: list[tuple[str, dict[str, object], str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        requests.append((request.url.path, body, request.headers.get("Authorization", "")))
        if request.url.path == "/sessions":
            return httpx.Response(
                201,
                json={"id": "ses_public", "sessionToken": "ses_worker_secret"},
            )
        if request.url.path == "/sessions/ses_public/revoke":
            return httpx.Response(200, json={"ok": True})
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    settings = Settings(
        OCTOPAL_BROWSER_BACKEND="pinchtab",
        OCTOPAL_PINCHTAB_BASE_URL="http://pinchtab.test",
        OCTOPAL_PINCHTAB_TOKEN="master-secret",
        OCTOPAL_PINCHTAB_BROWSER="cloak",
    )
    runtime = WorkerRuntime(
        store=object(),
        policy=object(),
        workspace_dir=tmp_path,
        launcher=object(),
        settings=settings,
    )
    monkeypatch.setattr(
        runtime,
        "_pinchtab_client",
        lambda: httpx.AsyncClient(
            base_url="http://pinchtab.test", transport=httpx.MockTransport(handler)
        ),
    )
    spec = SimpleNamespace(id="research-1", available_tools=["browser_snapshot"])

    async def scenario() -> None:
        assert runtime._worker_uses_pinchtab(spec) is True
        session_id, token = await runtime._create_pinchtab_session(spec)
        assert (session_id, token) == ("ses_public", "ses_worker_secret")
        await runtime._revoke_pinchtab_session(session_id)

    asyncio.run(scenario())

    assert requests == [
        (
            "/sessions",
            {
                "agentId": "octopal-worker-research-1",
                "label": "Octopal worker research-1",
                "browser": "cloak",
            },
            "Bearer master-secret",
        ),
        ("/sessions/ses_public/revoke", {}, "Bearer master-secret"),
    ]


def test_worker_runtime_skips_pinchtab_session_without_browser_tools(tmp_path: Path) -> None:
    settings = Settings(OCTOPAL_BROWSER_BACKEND="pinchtab")
    runtime = WorkerRuntime(
        store=object(),
        policy=object(),
        workspace_dir=tmp_path,
        launcher=object(),
        settings=settings,
    )

    assert runtime._worker_uses_pinchtab(SimpleNamespace(available_tools=["web_fetch"])) is False
    assert (
        runtime._worker_uses_pinchtab(SimpleNamespace(available_tools=["fetch_plan_tool"])) is True
    )


def test_worker_runtime_uses_separate_pinchtab_worker_url(tmp_path: Path) -> None:
    settings = Settings(
        OCTOPAL_BROWSER_BACKEND="pinchtab",
        OCTOPAL_PINCHTAB_BASE_URL="http://127.0.0.1:9867",
        OCTOPAL_PINCHTAB_WORKER_BASE_URL="http://host.docker.internal:9867",
    )
    runtime = WorkerRuntime(
        store=object(),
        policy=object(),
        workspace_dir=tmp_path,
        launcher=object(),
        settings=settings,
    )

    env = runtime._build_worker_env(SimpleNamespace(available_tools=["fetch_plan_tool"]))

    assert env["OCTOPAL_PINCHTAB_BASE_URL"] == "http://host.docker.internal:9867"
    assert runtime._pinchtab_client().base_url == httpx.URL("http://127.0.0.1:9867")


def test_worker_runtime_closes_owned_tabs_before_revoke(tmp_path: Path, monkeypatch) -> None:
    ownership_file = tmp_path / "pinchtab-tabs.json"
    ownership_file.write_text(json.dumps(["tab-one", "tab-two"]), encoding="utf-8")
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.url.path, request.headers.get("Authorization", "")))
        return httpx.Response(200, json={"ok": True})

    settings = Settings(
        OCTOPAL_BROWSER_BACKEND="pinchtab",
        OCTOPAL_PINCHTAB_BASE_URL="http://pinchtab.test",
    )
    runtime = WorkerRuntime(
        store=object(),
        policy=object(),
        workspace_dir=tmp_path,
        launcher=object(),
        settings=settings,
    )
    monkeypatch.setattr(
        runtime,
        "_pinchtab_client",
        lambda: httpx.AsyncClient(
            base_url="http://pinchtab.test", transport=httpx.MockTransport(handler)
        ),
    )

    asyncio.run(runtime._close_pinchtab_worker_tabs("ses_worker", ownership_file))

    assert requests == [
        ("/tabs/tab-one/close", "Session ses_worker"),
        ("/tabs/tab-two/close", "Session ses_worker"),
    ]
    assert not ownership_file.exists()
