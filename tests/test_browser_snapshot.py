from __future__ import annotations

import asyncio

from octopal.browser.snapshot import _get_indent_level, capture_aria_snapshot


class _PageStub:
    def __init__(self, snapshot: str) -> None:
        self._snapshot = snapshot

    async def aria_snapshot(self) -> str:
        return self._snapshot


class _PageWithoutAriaSnapshot:
    async def content(self) -> str:
        return """
        <html>
          <body>
            <button class="primary">Save</button>
            <a href="/docs">Docs</a>
            <input type="search" placeholder="Search docs" />
            <p>Hello from fallback snapshot mode.</p>
          </body>
        </html>
        """


def test_get_indent_level_counts_two_space_steps() -> None:
    assert _get_indent_level("") == 0
    assert _get_indent_level("  - button") == 1
    assert _get_indent_level("    - link") == 2


def test_capture_aria_snapshot_injects_refs_and_tracks_duplicates() -> None:
    page = _PageStub(
        "\n".join(
            [
                '- heading "Main settings"',
                '- button "Save" [disabled]',
                '- button "Save"',
                "- paragraph",
                '- img "Logo"',
            ]
        )
    )

    result = asyncio.run(capture_aria_snapshot(page))

    assert result["snapshot"].splitlines() == [
        '- heading "Main settings" [ref=e1]',
        '- button "Save" [ref=e2] [disabled]',
        '- button "Save" [ref=e3] [nth=1]',
        "- paragraph",
        '- img "Logo" [ref=e4]',
    ]
    assert result["refs"] == {
        "e1": {"role": "heading", "name": "Main settings", "nth": 0},
        "e2": {"role": "button", "name": "Save", "nth": 0},
        "e3": {"role": "button", "name": "Save", "nth": 1},
        "e4": {"role": "img", "name": "Logo", "nth": 0},
    }


def test_capture_aria_snapshot_preserves_unmatched_lines() -> None:
    page = _PageStub(
        "\n".join(
            [
                "RootWebArea",
                "  - text: plain text",
                '  - link "Docs"',
            ]
        )
    )

    result = asyncio.run(capture_aria_snapshot(page))

    assert result["snapshot"].splitlines() == [
        "RootWebArea",
        "  - text: plain text",
        '  - link "Docs" [ref=e1]',
    ]
    assert result["refs"] == {
        "e1": {"role": "link", "name": "Docs", "nth": 0},
    }


def test_capture_aria_snapshot_falls_back_when_page_lacks_aria_snapshot() -> None:
    result = asyncio.run(capture_aria_snapshot(_PageWithoutAriaSnapshot()))

    assert 'button "Save" [ref=e1]' in result["snapshot"]
    assert 'link "Docs" [ref=e2]' in result["snapshot"]
    assert 'searchbox "Search docs" [ref=e3]' in result["snapshot"]
    assert "Hello from fallback snapshot mode." in result["snapshot"]
    assert result["refs"]["e1"] == {"role": "button", "name": "Save", "nth": 0}
    assert result["refs"]["e2"] == {"role": "link", "name": "Docs", "nth": 0}
    assert result["refs"]["e3"] == {"role": "searchbox", "name": "Search docs", "nth": 0}
