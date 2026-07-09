from __future__ import annotations

import sys
import types

if "telegramify_markdown" not in sys.modules:
    sys.modules["telegramify_markdown"] = types.SimpleNamespace(markdownify=lambda text: text)


from octopal.channels.telegram import handlers


def test_prepare_markdown_v2_converts_double_asterisk_bold(monkeypatch) -> None:
    monkeypatch.setattr(
        handlers.telegramify_markdown,
        "markdownify",
        lambda text: (
            "*Organization assessment:*\n" if text == "**Organization assessment:**" else text
        ),
    )

    assert (
        handlers._prepare_markdown_v2("**Organization assessment:**")
        == "*Organization assessment:*\n"
    )
