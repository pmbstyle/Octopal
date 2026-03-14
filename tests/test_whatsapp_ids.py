from __future__ import annotations

from broodmind.channels.whatsapp.ids import (
    normalize_whatsapp_number,
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


def test_whatsapp_chat_id_is_stable() -> None:
    assert whatsapp_chat_id("+15551234567") == whatsapp_chat_id("+1 (555) 123-4567")
