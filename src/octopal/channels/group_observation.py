from __future__ import annotations

from typing import Any

SHARED_CONVERSATION_SCOPE = "default"


async def record_passive_group_observation(
    octo: Any,
    *,
    channel: str,
    chat_id: int,
    text: str,
    images: list[str] | None = None,
    saved_file_paths: list[str] | None = None,
    sender_label: str | None = None,
    addressing_action: str = "ignore",
    addressing_reason: str | None = None,
) -> None:
    memory = getattr(octo, "memory", None)
    add_message = getattr(memory, "add_message", None)
    if not callable(add_message):
        return
    trimmed = (text or "").strip()
    normalized_paths = [
        str(path).strip() for path in (saved_file_paths or []) if str(path).strip()
    ]
    if not trimmed and not images and not normalized_paths:
        return

    lines = [
        "Observed group-chat message.",
        "This message was visible in the group chat but was not handled as a direct user turn for this agent.",
    ]
    if sender_label:
        lines.append(f"Sender: {sender_label}")
    if trimmed:
        lines.append("Message:\n" + trimmed)
    if images:
        lines.append(f"Image attachments: {len(images)}")
    if normalized_paths:
        lines.append("Saved attachments:\n" + "\n".join(f"- {path}" for path in normalized_paths))

    await add_message(
        "system",
        "\n\n".join(lines),
        {
            "chat_id": chat_id,
            "channel": channel,
            "conversation_scope": SHARED_CONVERSATION_SCOPE,
            "chat_kind": "group",
            "passive_group_observation": True,
            "addressing_action": addressing_action,
            "addressing_reason": addressing_reason,
            "sender_label": sender_label,
            "has_images": bool(images),
            "has_files": bool(normalized_paths),
            "saved_file_paths": normalized_paths,
            "fact_candidate": False,
        },
    )
