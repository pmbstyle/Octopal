from __future__ import annotations

from octopal.channels.whatsapp.ids import (
    normalize_whatsapp_number,
    parse_allowed_whatsapp_chats,
    parse_allowed_whatsapp_numbers,
    whatsapp_chat_id,
)


def test_normalize_whatsapp_number_strips_noise() -> None:
    assert normalize_whatsapp_number(" +1 (555) 123-4567 ") == "+15551234567"


def test_normalize_whatsapp_number_strips_device_suffix() -> None:
    assert normalize_whatsapp_number("12899808683:11@s.whatsapp.net") == "+12899808683"


def test_parse_allowed_whatsapp_numbers_is_deduplicated() -> None:
    parsed = parse_allowed_whatsapp_numbers("+15551234567, 15551234567, +447700900123")
    assert parsed == ["+15551234567", "+447700900123"]


def test_parse_allowed_whatsapp_chats_preserves_group_jids() -> None:
    parsed = parse_allowed_whatsapp_chats(
        "120363123456789@g.us, +15551234567, 120363123456789@g.us"
    )
    assert parsed == ["120363123456789@g.us", "+15551234567"]


def test_whatsapp_chat_id_is_stable() -> None:
    assert whatsapp_chat_id("+15551234567") == whatsapp_chat_id("+1 (555) 123-4567")


def test_whatsapp_chat_id_keeps_group_jids_distinct_from_phone_numbers() -> None:
    assert whatsapp_chat_id("120363123456789@g.us") != whatsapp_chat_id("+120363123456789")
