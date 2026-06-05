from __future__ import annotations

DEFAULT_USER_CHANNEL = "telegram"
SUPPORTED_USER_CHANNELS = ("telegram", "whatsapp", "desktop")


def normalize_user_channel(value: str | None) -> str:
    normalized = (value or DEFAULT_USER_CHANNEL).strip().lower()
    if normalized in SUPPORTED_USER_CHANNELS:
        return normalized
    return DEFAULT_USER_CHANNEL


def user_channel_label(value: str | None) -> str:
    normalized = normalize_user_channel(value)
    if normalized == "whatsapp":
        return "WhatsApp"
    if normalized == "desktop":
        return "Desktop"
    return "Telegram"
