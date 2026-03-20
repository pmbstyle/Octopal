from __future__ import annotations

import asyncio
import json

import broodmind.tools.web.plan as plan_mod


def test_fetch_plan_tool_uses_browser_extract_fallback(monkeypatch) -> None:
    monkeypatch.setattr(
        plan_mod,
        "markdown_new_fetch",
        lambda args: json.dumps({"ok": False, "error": "thin content"}),
    )
    monkeypatch.setattr(
        plan_mod,
        "web_fetch",
        lambda args: json.dumps({"ok": False, "error": "js heavy"}),
    )

    async def fake_browser_open(args, ctx):
        return "Successfully opened https://example.com"

    async def fake_browser_snapshot(args, ctx):
        return '- heading "Docs" [ref=e1]'

    async def fake_browser_extract(args, ctx):
        return {
            "ok": True,
            "source": "page",
            "title": "Docs",
            "text": "Structured browser content " * 6,
        }

    async def fake_browser_close(args, ctx):
        return "Browser session closed"

    monkeypatch.setattr(plan_mod, "browser_open", fake_browser_open)
    monkeypatch.setattr(plan_mod, "browser_snapshot", fake_browser_snapshot)
    monkeypatch.setattr(plan_mod, "browser_extract", fake_browser_extract)
    monkeypatch.setattr(plan_mod, "browser_close", fake_browser_close)

    async def scenario() -> None:
        result = json.loads(
            await plan_mod.fetch_plan_tool(
                {"url": "https://example.com", "goal": "structured_extract", "min_content_chars": 100},
                {"chat_id": 1},
            )
        )
        assert result["ok"] is True
        assert result["source"] == "browser_extract"
        assert "Structured browser content" in result["snippet"]
        assert "browser_extract" in result["next_best_action"]

    asyncio.run(scenario())


def test_fetch_plan_tool_failure_hint_depends_on_goal(monkeypatch) -> None:
    monkeypatch.setattr(
        plan_mod,
        "markdown_new_fetch",
        lambda args: json.dumps({"ok": False, "error": "thin content"}),
    )
    monkeypatch.setattr(
        plan_mod,
        "web_fetch",
        lambda args: json.dumps({"ok": False, "error": "still thin"}),
    )

    async def scenario() -> None:
        result = json.loads(
            await plan_mod.fetch_plan_tool(
                {"url": "https://example.com", "goal": "structured_extract", "allow_browser": False},
                {"chat_id": 1},
            )
        )
        assert result["ok"] is False
        assert result["next_best_action"] == "retry with allow_browser=true or lower min_content_chars"

    asyncio.run(scenario())


def test_fetch_plan_tool_rejects_thin_browser_fallback(monkeypatch) -> None:
    monkeypatch.setattr(
        plan_mod,
        "markdown_new_fetch",
        lambda args: json.dumps({"ok": False, "error": "thin content"}),
    )
    monkeypatch.setattr(
        plan_mod,
        "web_fetch",
        lambda args: json.dumps({"ok": False, "error": "still thin"}),
    )

    async def fake_browser_open(args, ctx):
        return "Successfully opened https://example.com"

    async def fake_browser_snapshot(args, ctx):
        return '- heading "Docs" [ref=e1]'

    async def fake_browser_extract(args, ctx):
        return {
            "ok": True,
            "source": "page",
            "title": "Docs",
            "text": "tiny",
        }

    async def fake_browser_close(args, ctx):
        return "Browser session closed"

    monkeypatch.setattr(plan_mod, "browser_open", fake_browser_open)
    monkeypatch.setattr(plan_mod, "browser_snapshot", fake_browser_snapshot)
    monkeypatch.setattr(plan_mod, "browser_extract", fake_browser_extract)
    monkeypatch.setattr(plan_mod, "browser_close", fake_browser_close)

    async def scenario() -> None:
        result = json.loads(
            await plan_mod.fetch_plan_tool(
                {"url": "https://example.com", "min_content_chars": 1000},
                {"chat_id": 1},
            )
        )
        assert result["ok"] is False
        assert result["source"] == "fetch_plan_tool"
        assert result["plan"][-1]["tool"] == "browser_plan"
        assert result["plan"][-1]["status"] == "error"
        assert "below min_content_chars" in result["plan"][-1]["reason"]

    asyncio.run(scenario())
