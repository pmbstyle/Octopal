from __future__ import annotations

import hashlib
import re


def normalize_whatsapp_number(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if "@" in raw:
        raw = raw.split("@", 1)[0]
    if ":" in raw:
        raw = raw.split(":", 1)[0]
    digits = "+" + re.sub(r"\D+", "", raw)
    return digits if len(digits) > 1 else ""


def parse_allowed_whatsapp_numbers(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for chunk in (raw or "").split(","):
        normalized = normalize_whatsapp_number(chunk)
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def normalize_whatsapp_chat(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if "@g.us" in raw:
        return raw
    normalized_number = normalize_whatsapp_number(raw)
    if normalized_number:
        return normalized_number
    return raw


def parse_allowed_whatsapp_chats(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for chunk in (raw or "").split(","):
        normalized = normalize_whatsapp_chat(chunk)
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def whatsapp_chat_id(sender: str) -> int:
    raw = (sender or "").strip()
    normalized = raw if "@g.us" in raw else normalize_whatsapp_number(raw) or raw
    digest = hashlib.sha256(normalized.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) & 0x7FFF_FFFF_FFFF_FFFF
