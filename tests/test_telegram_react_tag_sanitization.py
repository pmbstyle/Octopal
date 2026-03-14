from __future__ import annotations

import sys
import types

if "telegramify_markdown" not in sys.modules:
    sys.modules["telegramify_markdown"] = types.SimpleNamespace(markdownify=lambda text: text)

from broodmind.channels.telegram.handlers import _extract_reaction_and_strip, _strip_reaction_tags


def test_extract_reaction_and_strip_removes_tag() -> None:
    emoji, text = _extract_reaction_and_strip("<react>👍</react> Hello there")
    assert emoji == "👍"
    assert text == "Hello there"


def test_strip_reaction_tags_removes_unknown_react_markup() -> None:
    cleaned = _strip_reaction_tags("Text <react>not-an-emoji</react> remains")
    assert "<react>" not in cleaned
    assert "</react>" not in cleaned
    assert "Text" in cleaned
    assert "remains" in cleaned
