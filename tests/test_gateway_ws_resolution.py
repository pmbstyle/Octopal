from __future__ import annotations

from types import SimpleNamespace

from broodmind.gateway.ws import _resolve_ws_chat_id


def test_resolve_ws_chat_id_returns_positive_when_no_allowlist() -> None:
    settings = SimpleNamespace(allowed_telegram_chat_ids="")
    assert _resolve_ws_chat_id(settings) > 0


def test_resolve_ws_chat_id_uses_first_allowed_id_when_valid() -> None:
    settings = SimpleNamespace(allowed_telegram_chat_ids="42,100")
    assert _resolve_ws_chat_id(settings) == 42
