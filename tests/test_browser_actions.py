from __future__ import annotations

import asyncio

import octopal.tools.browser.actions as browser_actions


class _LocatorStub:
    def __init__(self, text: str = "", should_fail: bool = False) -> None:
        self._text = text
        self._should_fail = should_fail
        self.wait_calls: list[tuple[str, int]] = []

    def nth(self, _index: int):
        return self

    @property
    def first(self):
        return self

    async def wait_for(self, *, state: str, timeout: int) -> None:
        self.wait_calls.append((state, timeout))
        if self._should_fail:
            raise RuntimeError("missing")

    async def inner_text(self, timeout: int = 5000) -> str:
        if self._should_fail:
            raise RuntimeError("cannot extract")
        return self._text

    async def click(self, timeout: int = 5000) -> None:
        if self._should_fail:
            raise RuntimeError("cannot click")

    async def fill(self, text: str, timeout: int = 5000) -> None:
        if self._should_fail:
            raise RuntimeError("cannot fill")
        self._text = text

    async def press(self, _key: str) -> None:
        return None


class _PageStub:
    def __init__(self) -> None:
        self.url = "https://example.com/page"
        self.refs = {
            ("button", "Save", True): _LocatorStub(text="Save"),
        }
        self.text_locator = _LocatorStub()
        self.body_locator = _LocatorStub(text="Page body content")

    def get_by_role(self, role: str, name: str | None = None, exact: bool = False):
        return self.refs[(role, name, exact)]

    def get_by_text(self, text: str, exact: bool = False):
        assert text == "Done"
        assert exact is False
        return self.text_locator

    def locator(self, selector: str):
        assert selector == "body"
        return self.body_locator

    async def title(self) -> str:
        return "Example title"

    async def goto(self, url: str, wait_until: str = "domcontentloaded", timeout: int = 30000) -> None:
        assert wait_until == "domcontentloaded"
        assert timeout == 30000
        self.url = url


class _ManagerStub:
    def __init__(self, page: _PageStub) -> None:
        self._page = page
        self._pages = [{"target_id": "t1", "url": page.url, "title": "Example title", "is_current": True}]

    async def get_page(self, chat_id: int, target_id: str | None = None):
        assert chat_id == 7
        assert target_id in {None, "t1"}
        return self._page

    async def list_pages(self, chat_id: int):
        assert chat_id == 7
        return list(self._pages)

    async def focus_page(self, chat_id: int, target_id: str):
        assert chat_id == 7
        assert target_id == "t1"
        return self._page

    async def screenshot_page(self, chat_id: int, target_id: str | None = None, full_page: bool = True):
        assert chat_id == 7
        assert target_id in {None, "t1"}
        return {"target_id": target_id or "t1", "path": "C:/tmp/browser.png"}


def test_browser_wait_for_uses_text_lookup(monkeypatch) -> None:
    page = _PageStub()
    monkeypatch.setattr(browser_actions, "get_browser_manager", lambda: _ManagerStub(page))

    async def scenario() -> None:
        result = await browser_actions.browser_wait_for(
            {"text": "Done", "state": "visible", "timeout_ms": 1234},
            {"chat_id": 7},
        )
        assert result == "Text appeared: Done"
        assert page.text_locator.wait_calls == [("visible", 1234)]

    asyncio.run(scenario())


def test_browser_extract_returns_page_summary(monkeypatch) -> None:
    page = _PageStub()
    monkeypatch.setattr(browser_actions, "get_browser_manager", lambda: _ManagerStub(page))

    async def scenario() -> None:
        result = await browser_actions.browser_extract({"max_chars": 500}, {"chat_id": 7})
        assert result["ok"] is True
        assert result["source"] == "page"
        assert result["title"] == "Example title"
        assert result["target_id"] == "t1"
        assert result["text"] == "Page body content"

    asyncio.run(scenario())


def test_browser_extract_can_use_snapshot_ref(monkeypatch) -> None:
    page = _PageStub()
    monkeypatch.setattr(browser_actions, "get_browser_manager", lambda: _ManagerStub(page))
    monkeypatch.setattr(
        browser_actions,
        "_SESSION_REFS",
        {7: {"t1": {"e1": {"role": "button", "name": "Save", "nth": 0}}}},
    )

    async def scenario() -> None:
        result = await browser_actions.browser_extract({"ref": "e1"}, {"chat_id": 7})
        assert result == {"ok": True, "source": "ref", "ref": "e1", "target_id": "t1", "text": "Save"}

    asyncio.run(scenario())


def test_browser_tabs_and_focus_tab(monkeypatch) -> None:
    page = _PageStub()
    monkeypatch.setattr(browser_actions, "get_browser_manager", lambda: _ManagerStub(page))

    async def scenario() -> None:
        tabs = await browser_actions.browser_tabs({}, {"chat_id": 7})
        assert tabs["ok"] is True
        assert tabs["pages"][0]["target_id"] == "t1"

        focused = await browser_actions.browser_focus_tab({"target_id": "t1"}, {"chat_id": 7})
        assert focused == "Focused t1"

    asyncio.run(scenario())


def test_browser_screenshot_returns_structured_payload(monkeypatch) -> None:
    page = _PageStub()
    monkeypatch.setattr(browser_actions, "get_browser_manager", lambda: _ManagerStub(page))

    async def scenario() -> None:
        result = await browser_actions.browser_screenshot({"full_page": False}, {"chat_id": 7})
        assert result == {
            "ok": True,
            "full_page": False,
            "target_id": "t1",
            "path": "C:/tmp/browser.png",
        }

    asyncio.run(scenario())


def test_browser_workflow_sequences_existing_actions(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    async def _open(args, ctx):
        del ctx
        calls.append(("open", dict(args)))
        return "Successfully opened https://example.com"

    async def _tabs(args, ctx):
        del ctx
        calls.append(("tabs", dict(args)))
        return {"ok": True, "count": 1, "pages": [{"target_id": "t1"}]}

    async def _extract(args, ctx):
        del ctx
        calls.append(("extract", dict(args)))
        return {"ok": True, "source": "page", "text": "Workflow body", "target_id": "t1"}

    monkeypatch.setattr(
        browser_actions,
        "_WORKFLOW_ACTIONS",
        {
            "open": _open,
            "tabs": _tabs,
            "extract": _extract,
        },
    )

    async def scenario() -> None:
        result = await browser_actions.browser_workflow(
            {
                "steps": [
                    {"action": "open", "url": "https://example.com"},
                    {"action": "tabs"},
                    {"action": "extract", "max_chars": 200},
                ]
            },
            {"chat_id": 7},
        )
        assert result["ok"] is True
        assert result["step_count"] == 3
        assert [step["action"] for step in result["steps"]] == ["open", "tabs", "extract"]
        assert result["steps"][2]["text"] == "Workflow body"
        assert calls == [
            ("open", {"url": "https://example.com"}),
            ("tabs", {}),
            ("extract", {"max_chars": 200}),
        ]

    asyncio.run(scenario())
