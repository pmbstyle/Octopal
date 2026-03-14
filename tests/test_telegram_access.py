from __future__ import annotations

import sys
import types
from pathlib import Path

from broodmind.channels.telegram.access import is_allowed_chat, parse_allowed_chat_ids
from broodmind.infrastructure.config.settings import Settings


def test_parse_allowed_chat_ids_ignores_invalid_values() -> None:
    parsed = parse_allowed_chat_ids("123, abc, , 456")
    assert parsed == {123, 456}


def test_is_allowed_chat_defaults_to_allow_when_empty() -> None:
    assert is_allowed_chat(999, set())
    assert is_allowed_chat(123, {123})
    assert not is_allowed_chat(999, {123})


def test_dispatcher_uses_single_shared_mcp_manager(tmp_path: Path) -> None:
    if "telegramify_markdown" not in sys.modules:
        sys.modules["telegramify_markdown"] = types.SimpleNamespace(markdownify=lambda text: text)

    from broodmind.channels.telegram.bot import build_dispatcher

    settings = Settings(
        TELEGRAM_BOT_TOKEN="123:abc",
        BROODMIND_STATE_DIR=tmp_path / "state",
        BROODMIND_WORKSPACE_DIR=tmp_path / "workspace",
    )
    _dp, queen = build_dispatcher(settings, bot=object())
    assert queen.runtime.mcp_manager is queen.mcp_manager
