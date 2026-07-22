from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from octopal.runtime.context_compiler import ContextSection, compile_context
from octopal.runtime.memory.influence import require_complete_memory_influence_ids
from octopal.runtime.memory.memchain import memchain_verify
from octopal.runtime.memory.service import infer_memory_facets

if TYPE_CHECKING:
    from octopal.infrastructure.providers.base import Message
    from octopal.infrastructure.store.base import Store
    from octopal.runtime.memory.canon import CanonService
    from octopal.runtime.memory.facts import FactsService
    from octopal.runtime.memory.reflection import ReflectionService
    from octopal.runtime.memory.service import MemoryService


_OCTO_SYSTEM_PROMPT_CONTENT = ""
_CONTROL_PLANE_CONTEXT_TOKEN_BUDGET = 2200
_CONTROL_PLANE_SYSTEM_PROMPT = """You are Octopal Octo handling an internal operational turn.

Core rules:
- Treat route labels as runtime execution contracts, not as your identity or a user-facing limitation.
- Use tools only when they are clearly needed and visible in the current tool contract.
- If you state an action, perform the matching tool call in the same turn.
- Prefer safe, minimal-permission actions; do not bypass blocked tools.
- Do not invent external facts or claim completed work without evidence.
- If the current tool contract cannot complete the work, use an explicit continuation, repair,
  approval, queue, or clarification path when one is available.
- Never tell the user that you cannot act because of a route, mode, tool surface, or internal
  orchestration context. Return the exact execution contract when one is provided."""


@dataclass
class BootstrapContext:
    content: str

    hash: str

    files: list[tuple[str, int]]


@dataclass
class MemoryContextBundle:
    canon_context: str
    facts_context: list[str]
    memory_context: list[str]
    recent_history: list[tuple[str, str, str | None]]
    prune_stats: dict[str, int]
    selected_facets: list[str]
    selected_ids: list[str]


async def _load_system_prompt_file() -> str:
    """Loads the content of octo_system.md."""

    global _OCTO_SYSTEM_PROMPT_CONTENT

    if not _OCTO_SYSTEM_PROMPT_CONTENT:
        prompt_path = Path(__file__).parent / "prompts" / "octo_system.md"

        try:
            _OCTO_SYSTEM_PROMPT_CONTENT = await asyncio.to_thread(
                prompt_path.read_text, encoding="utf-8"
            )

        except FileNotFoundError:
            _OCTO_SYSTEM_PROMPT_CONTENT = "You are the Octopal Octo. Your configuration files are missing. Tell the user that you are missing the configuration files and ask them to create them."

    return _OCTO_SYSTEM_PROMPT_CONTENT


async def get_prompt_section(title: str) -> list[str]:
    """Extracts a section from the octo_system.md file by its ## title."""

    content = await _load_system_prompt_file()

    marker = f"## {title}"

    start_index = content.find(marker)

    if start_index == -1:
        return []

    end_index = content.find("\n## ", start_index + len(marker))

    section_content = content[start_index : end_index if end_index != -1 else None]

    return [line.strip() for line in section_content.strip().split("\n") if line.strip()] + [""]


async def build_persona_prompt() -> list[str]:
    """Builds the persona prompt from SOUL.md."""

    workspace = os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")

    persona_path = Path(workspace) / "SOUL.md"

    def _read_persona():
        if not persona_path.exists():
            return None

        try:
            return persona_path.read_text(encoding="utf-8").strip()

        except Exception:
            return None

    content = await asyncio.to_thread(_read_persona)

    if content:
        max_chars = _env_int("OCTOPAL_PERSONA_MAX_CHARS", 32_000, minimum=2_000)
        return ["<persona>", _trim_middle(content, max_chars), "</persona>", ""]

    return []


async def build_bootstrap_context_prompt(store: Store, chat_id: int) -> BootstrapContext:
    """Builds the workspace context from various files."""

    def _sync_logic():
        workspace = Path(os.getenv("OCTOPAL_WORKSPACE_DIR", "workspace")).resolve()

        memory_dir = workspace / "memory"

        memory_dir.mkdir(parents=True, exist_ok=True)

        today = datetime.now().date()

        yesterday = today - timedelta(days=1)

        memory_files = [
            memory_dir / f"{today.isoformat()}.md",
            memory_dir / f"{yesterday.isoformat()}.md",
        ]

        for path in memory_files:
            if not path.exists():
                path.write_text("", encoding="utf-8")

        required_files = [
            workspace / "AGENTS.md",
            workspace / "USER.md",
        ]

        optional_files = [
            workspace / "HEARTBEAT.md",
            workspace / "MEMORY.md",
            workspace / "experiments" / "README.md",
        ]

        file_entries: list[tuple[str, str]] = []

        for path in required_files:
            if not path.exists():
                continue

            content = path.read_text(encoding="utf-8")
            file_entries.append((path.name, content))

        for path in optional_files:
            if not path.exists():
                continue

            rel = path.relative_to(workspace).as_posix()
            content = path.read_text(encoding="utf-8")

            if content.strip():
                file_entries.append((rel, content))

        for path in memory_files:
            rel = path.relative_to(workspace).as_posix()
            content = path.read_text(encoding="utf-8")

            file_entries.append((rel, content))

        integrity = memchain_verify(workspace)
        if integrity.status == "broken":
            warning = (
                "MEMCHAIN INTEGRITY ALERT\n"
                f"status={integrity.status}\n"
                f"reason={integrity.message}\n"
                f"broken_at={integrity.broken_at or 0}\n"
                "Treat workspace memory as untrusted until human confirms and memchain is repaired."
            )
            file_entries.insert(0, ("MEMCHAIN_ALERT.md", warning))

        if not file_entries:
            return BootstrapContext(content="", hash="", files=[])

        bundle_hash = hashlib.sha256()

        for name, content in file_entries:
            bundle_hash.update(name.encode("utf-8"))

            bundle_hash.update(b"\n")

            bundle_hash.update(content.encode("utf-8"))

            bundle_hash.update(b"\n")

        hash_value = bundle_hash.hexdigest()

        files_with_sizes = [(name, len(content)) for name, content in file_entries]
        prompt_entries = _bound_context_entries(
            file_entries,
            max_file_chars=_env_int("OCTOPAL_BOOTSTRAP_MAX_FILE_CHARS", 64_000, minimum=2_000),
            max_total_chars=_env_int("OCTOPAL_BOOTSTRAP_MAX_TOTAL_CHARS", 160_000, minimum=4_000),
        )

        parts = ["<workspace>"]

        for name, content in prompt_entries:
            parts.append(f'<file name="{name}">')

            parts.append(content)

            parts.append("</file>")

        parts.append("</workspace>")

        content = "\n".join(parts)

        return BootstrapContext(content=content, hash=hash_value, files=files_with_sizes)

    return await asyncio.to_thread(_sync_logic)


def _current_datetime_prompt() -> str:
    now = datetime.now().astimezone()

    return f"Current date/time: {now.isoformat()}"


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


def _trim_middle(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    marker = "\n...[pruned for context window]...\n"
    if max_chars <= len(marker):
        return text[:max_chars]
    content_budget = max_chars - len(marker)
    head = content_budget // 2
    tail = content_budget - head
    return text[:head] + marker + text[-tail:]


def _bound_context_entries(
    entries: list[tuple[str, str]],
    *,
    max_file_chars: int,
    max_total_chars: int,
) -> list[tuple[str, str]]:
    bounded = [(name, _trim_middle(content, max_file_chars)) for name, content in entries]
    minimum_entry_chars = min(500, max(1, max_total_chars // max(1, len(bounded))))

    while sum(len(content) for _, content in bounded) > max_total_chars:
        largest_index = max(range(len(bounded)), key=lambda index: len(bounded[index][1]))
        name, content = bounded[largest_index]
        excess = sum(len(value) for _, value in bounded) - max_total_chars
        target = max(minimum_entry_chars, len(content) - excess)
        if target >= len(content):
            break
        bounded[largest_index] = (name, _trim_middle(content, target))

    return bounded


def _normalize_recent_history_item(item: Any) -> tuple[str, str, str | None]:
    role, content = item[0], item[1]
    created_at = item[2] if len(item) > 2 else None
    return str(role), str(content), str(created_at) if created_at is not None else None


def _safe_metadata_value(value: str) -> str:
    return value.replace("\n", " ").replace("\r", " ").strip()


def _build_recent_history_metadata_prompt(
    history: list[tuple[str, str, str | None]],
) -> str:
    lines = [
        f"- [{index}] role={_safe_metadata_value(role)} sent_at={_safe_metadata_value(created_at)}"
        for index, (role, _content, created_at) in enumerate(history, start=1)
        if created_at
    ]
    if not lines:
        return ""
    return "\n".join(
        [
            "Recent conversation message metadata:",
            "- The sent_at values are metadata, not text written by the user or assistant.",
            "- Use sent_at only for chronology, recency, and relative-time references.",
            "- Do not quote or restate sent_at values unless the user explicitly asks about message timing.",
            *lines,
        ]
    )


def _prune_recent_history_window(
    history: Sequence[tuple[str, str] | tuple[str, str, str | None]],
    *,
    max_history_chars: int,
    keep_recent: int,
    per_message_chars: int,
) -> tuple[list[tuple[str, str, str | None]], dict[str, int]]:
    trimmed_count = 0
    dropped_count = 0
    normalized: list[tuple[str, str, str | None]] = []
    for item in history:
        role, content, created_at = _normalize_recent_history_item(item)
        trimmed = _trim_middle(content, per_message_chars)
        if trimmed != content:
            trimmed_count += 1
        normalized.append((role, trimmed, created_at))

    total_chars = sum(len(content) for _, content, _ in normalized)
    pruned = list(normalized)

    # Drop oldest messages first while preserving a recent tail.
    keep_recent = max(1, keep_recent)
    while len(pruned) > keep_recent and total_chars > max_history_chars:
        _, removed, _ = pruned.pop(0)
        dropped_count += 1
        total_chars -= len(removed)

    # If still too large, continue dropping oldest until within budget or one message remains.
    while len(pruned) > 1 and total_chars > max_history_chars:
        _, removed, _ = pruned.pop(0)
        dropped_count += 1
        total_chars -= len(removed)

    return pruned, {
        "trimmed": trimmed_count,
        "dropped": dropped_count,
        "total_chars": total_chars,
    }


async def _build_memory_context_bundle(
    memory: MemoryService,
    canon: CanonService,
    user_text: str,
    chat_id: int,
    facts: FactsService | None = None,
    conversation_scope: str | None = None,
) -> MemoryContextBundle:
    selected_ids: list[str] = []
    canon_with_ids = getattr(canon, "get_tier1_context_with_ids", None)
    if callable(canon_with_ids):
        canon_context, canon_ids = await asyncio.to_thread(canon_with_ids)
        selected_ids.extend(canon_ids)
    else:
        canon_context = await asyncio.to_thread(canon.get_tier1_context)

    selected_facets = sorted(
        facet for facet in infer_memory_facets(user_text) if facet != "fact_candidate"
    )
    facts_context: list[str] = []
    if facts is not None:
        try:
            fact_records_getter = getattr(facts, "get_relevant_fact_records", None)
            if callable(fact_records_getter):
                fact_records = await asyncio.to_thread(
                    fact_records_getter,
                    user_text,
                    memory_facets=selected_facets or None,
                )
                facts_context = [
                    (
                        f"{record.subject} {record.key.replace('_', ' ')} {record.value_text}"
                        + (f" ({record.source_ref})" if record.source_ref else "")
                    )
                    for record in fact_records
                ]
                selected_ids.extend(f"memory_fact:{record.id}" for record in fact_records)
            else:
                facts_context = await asyncio.to_thread(
                    facts.get_relevant_facts,
                    user_text,
                    memory_facets=selected_facets or None,
                )
        except Exception:
            facts_context = []
    memory_entries_getter = getattr(memory, "get_context_entries_by_facets", None)
    if callable(memory_entries_getter):
        memory_entries = await memory_entries_getter(
            user_text,
            exclude_chat_id=chat_id,
            memory_facets=selected_facets or None,
        )
        memory_context = [f"{entry.role}: {entry.content}" for entry in memory_entries]
        selected_ids.extend(f"memory_entry:{entry.id}" for entry in memory_entries)
    else:
        memory_getter = getattr(memory, "get_context_by_facets", None)
        if callable(memory_getter):
            memory_context = await memory_getter(
                user_text,
                exclude_chat_id=chat_id,
                memory_facets=selected_facets or None,
            )
        else:
            memory_context = await memory.get_context(user_text, exclude_chat_id=chat_id)

    recent_history_ids: list[str] = []
    recent_entries_getter = getattr(memory, "get_recent_history_entries", None)
    if callable(recent_entries_getter):
        recent_entries = await recent_entries_getter(
            chat_id,
            limit=20,
            conversation_scope=conversation_scope,
        )
        raw_recent_history = [
            (entry.role, entry.content, entry.created_at.isoformat()) for entry in recent_entries
        ]
        recent_history_ids = [f"memory_entry:{entry.id}" for entry in recent_entries]
    else:
        try:
            raw_recent_history = await memory.get_recent_history(
                chat_id,
                limit=20,
                conversation_scope=conversation_scope,
            )
        except TypeError:
            raw_recent_history = await memory.get_recent_history(chat_id, limit=20)
    recent_history = [_normalize_recent_history_item(item) for item in raw_recent_history]
    if recent_history and recent_history[-1][0] == "user" and recent_history[-1][1] == user_text:
        recent_history = recent_history[:-1]
        recent_history_ids = recent_history_ids[:-1]
    max_history_chars = _env_int("OCTOPAL_CONTEXT_PRUNE_MAX_HISTORY_CHARS", 100_000, minimum=2_000)
    keep_recent = _env_int("OCTOPAL_CONTEXT_PRUNE_KEEP_RECENT", 12, minimum=1)
    per_message_chars = _env_int("OCTOPAL_CONTEXT_PRUNE_MESSAGE_CHARS", 32_000, minimum=500)
    recent_history, prune_stats = _prune_recent_history_window(
        recent_history,
        max_history_chars=max_history_chars,
        keep_recent=keep_recent,
        per_message_chars=per_message_chars,
    )
    if prune_stats["dropped"]:
        recent_history_ids = recent_history_ids[prune_stats["dropped"] :]
    selected_ids.extend(recent_history_ids)
    normalized_selected_ids = require_complete_memory_influence_ids(selected_ids)
    return MemoryContextBundle(
        canon_context=canon_context,
        facts_context=facts_context,
        memory_context=memory_context,
        recent_history=recent_history,
        prune_stats=prune_stats,
        selected_facets=selected_facets,
        selected_ids=normalized_selected_ids,
    )


async def build_octo_prompt(
    store: Store,
    memory: MemoryService,
    canon: CanonService,
    user_text: str,
    chat_id: int,
    bootstrap_context: str,
    is_ws: bool = False,
    images: list[str] | None = None,
    saved_file_paths: list[str] | None = None,
    wake_notice: str = "",
    tool_policy_summary: str = "",
    facts: FactsService | None = None,
    reflection: ReflectionService | None = None,
    conversation_scope: str | None = None,
    channel_context: dict[str, object] | None = None,
    memory_influence_ids: list[str] | None = None,
) -> list[Message]:
    """Assembles all the pieces into the final message list for the LLM."""

    from octopal.infrastructure.providers.base import Message

    system_prompt = await _load_system_prompt_file()

    persona_prompt_lines = await build_persona_prompt()

    datetime_prompt = _current_datetime_prompt()

    memory_bundle = await _build_memory_context_bundle(
        memory,
        canon,
        user_text,
        chat_id,
        facts,
        conversation_scope=conversation_scope,
    )
    selected_influence_ids = list(memory_bundle.selected_ids)

    messages: list[Message] = [Message(role="system", content=system_prompt)]
    if persona_prompt_lines:
        messages.append(Message(role="system", content="\n".join(persona_prompt_lines)))
    if bootstrap_context:
        messages.append(Message(role="system", content=bootstrap_context))
    if wake_notice.strip():
        messages.append(
            Message(
                role="system",
                content=(
                    "Wake-up directive after context reset:\n"
                    f"{wake_notice.strip()}\n"
                    "Do not autopilot; first pick one mode: continue / clarify / replan."
                ),
            )
        )
        if reflection is not None:
            reflection_context, reflection_ids = await _load_reflection_context(reflection, chat_id)
            selected_influence_ids.extend(reflection_ids)
            if reflection_context:
                messages.append(Message(role="system", content=reflection_context))
    messages.append(Message(role="system", content=datetime_prompt))
    if tool_policy_summary.strip():
        messages.append(
            Message(
                role="system",
                content=tool_policy_summary.strip(),
            )
        )
    channel_context_prompt = _build_channel_context_prompt(channel_context, conversation_scope)
    if channel_context_prompt:
        messages.append(Message(role="system", content=channel_context_prompt))

    if memory_bundle.canon_context:
        messages.append(Message(role="system", content=memory_bundle.canon_context))

    if memory_bundle.facts_context:
        messages.append(
            Message(
                role="system",
                content="<facts>\n" + "\n".join(memory_bundle.facts_context) + "\n</facts>",
            )
        )

    if memory_bundle.memory_context:
        messages.append(
            Message(
                role="system",
                content="<context>\n" + "\n".join(memory_bundle.memory_context) + "\n</context>",
            )
        )
    if memory_bundle.prune_stats["trimmed"] > 0 or memory_bundle.prune_stats["dropped"] > 0:
        messages.append(
            Message(
                role="system",
                content=(
                    "Context pruning applied before inference:\n"
                    f"- trimmed_messages={memory_bundle.prune_stats['trimmed']}\n"
                    f"- dropped_old_messages={memory_bundle.prune_stats['dropped']}\n"
                    f"- history_chars_after_prune={memory_bundle.prune_stats['total_chars']}"
                ),
            )
        )
    if memory_bundle.recent_history:
        recent_history_metadata = _build_recent_history_metadata_prompt(
            memory_bundle.recent_history
        )
        if recent_history_metadata:
            messages.append(Message(role="system", content=recent_history_metadata))
        for role, content, _created_at in memory_bundle.recent_history:
            messages.append(
                Message(
                    role=role,
                    content=content,
                )
            )

    normalized_paths = [str(path).strip() for path in (saved_file_paths or []) if str(path).strip()]
    if normalized_paths:
        attachment_metadata: dict[str, Any] = {
            "attachment_kind": "image" if images else "file",
            "saved_paths": normalized_paths,
        }
        guidance = (
            "If direct vision is unavailable or a later turn refers back to the image, use the exact saved paths."
            if images
            else "Use the exact saved paths when filesystem inspection is needed."
        )
        messages.append(
            Message(
                role="system",
                content=(
                    "Current turn attachment metadata (runtime-provided, not user-authored):\n"
                    f"{json.dumps(attachment_metadata, ensure_ascii=False)}\n"
                    f"{guidance}"
                ),
            )
        )

    if images:
        text_segments: list[str] = []
        if user_text.strip():
            text_segments.append(user_text.strip())
        else:
            text_segments.append("User uploaded an image.")

        text_content = "\n\n".join(segment for segment in text_segments if segment)
        content_blocks: list[dict[str, Any]] = [{"type": "text", "text": text_content}]
        for img in images:
            content_blocks.append(
                {"type": "image_url", "image_url": {"url": img, "detail": "auto"}}
            )
        messages.append(Message(role="user", content=content_blocks))
    else:
        if normalized_paths:
            messages.append(Message(role="user", content=user_text or "User uploaded file(s)."))
        else:
            messages.append(Message(role="user", content=user_text))

    if memory_influence_ids is not None:
        memory_influence_ids.extend(require_complete_memory_influence_ids(selected_influence_ids))
    return messages


async def _load_reflection_context(
    reflection: ReflectionService, chat_id: int
) -> tuple[str, list[str]]:
    try:
        getter = getattr(reflection, "build_wakeup_context_with_ids", None)
        if callable(getter):
            context, selected_ids = await asyncio.to_thread(getter, chat_id)
            return str(context or ""), require_complete_memory_influence_ids(selected_ids)
        context = await asyncio.to_thread(reflection.build_wakeup_context, chat_id)
        return str(context or ""), []
    except Exception:
        return "", []


def _build_channel_context_prompt(
    channel_context: dict[str, object] | None,
    conversation_scope: str | None,
) -> str:
    if not channel_context and not conversation_scope:
        return ""
    context = dict(channel_context or {})
    source_channel = str(context.get("source_channel", "") or "").strip()
    chat_kind = str(context.get("chat_kind", "") or "").strip()
    addressing_action = str(context.get("addressing_action", "") or "").strip()
    lines = ["Current turn transport context:"]
    if source_channel:
        lines.append(f"- source_channel={source_channel}")
    if chat_kind:
        lines.append(f"- chat_kind={chat_kind}")
    if addressing_action:
        lines.append(f"- group_addressing_action={addressing_action}")
    if conversation_scope:
        lines.append(f"- conversation_scope={conversation_scope}")
    if chat_kind == "group":
        lines.append(
            "This is a valid group-chat turn for this agent because the ingress addressing gate already allowed it."
        )
        lines.append(
            "Treat addressed group-chat messages as part of this agent's normal conversation context; do not claim you cannot see the group chat."
        )
    return "\n".join(lines)


async def build_control_plane_prompt(
    *,
    user_text: str,
    chat_id: int,
    tool_policy_summary: str = "",
    wake_notice: str = "",
    reflection: ReflectionService | None = None,
    mode_label: str = "control-plane",
    mode_rules: str = "",
    memory_influence_ids: list[str] | None = None,
) -> list[Message]:
    """Build a bounded prompt for control-plane turns without full workspace/memory context."""

    from octopal.infrastructure.providers.base import Message

    persona_prompt_lines = await build_persona_prompt()
    datetime_prompt = _current_datetime_prompt()

    selected_influence_ids: list[str] = []
    wake_context = ""
    reflection_context = ""
    reflection_ids: list[str] = []
    if wake_notice.strip():
        wake_context = (
            "Wake-up directive after context reset:\n"
            f"{wake_notice.strip()}\n"
            "Do not autopilot; first pick one mode: continue / clarify / replan."
        )
        if reflection is not None:
            reflection_context, reflection_ids = await _load_reflection_context(reflection, chat_id)

    runtime_contract = (
        f"Runtime execution contract: {mode_label}.\n"
        "This contract constrains tools, budget, and delivery; it is not a user-facing capability story.\n"
        "Keep this turn cheap, deterministic, and operationally safe.\n"
        "If broader work is needed, use a route-provided continuation/repair path when available; "
        "otherwise return the strict contract signal rather than explaining internal route limits."
    )
    compiled_context = compile_context(
        [
            ContextSection("control_plane_base", _CONTROL_PLANE_SYSTEM_PROMPT, required=True),
            ContextSection("persona", "\n".join(persona_prompt_lines), priority=40),
            ContextSection("wake_directive", wake_context, required=bool(wake_context)),
            ContextSection("reflection", reflection_context, priority=90),
            ContextSection("runtime_contract", runtime_contract, required=True),
            ContextSection(
                "tool_policy", tool_policy_summary.strip(), required=bool(tool_policy_summary)
            ),
            ContextSection("mode_rules", mode_rules.strip(), required=bool(mode_rules)),
        ],
        token_budget=_CONTROL_PLANE_CONTEXT_TOKEN_BUDGET,
    )
    messages: list[Message] = [
        Message(role="system", content=content) for content in compiled_context.sections.values()
    ]
    messages.append(Message(role="system", content=datetime_prompt))

    messages.append(Message(role="user", content=user_text))
    if memory_influence_ids is not None:
        if "reflection" in compiled_context.sections:
            selected_influence_ids.extend(reflection_ids)
        memory_influence_ids.extend(require_complete_memory_influence_ids(selected_influence_ids))
    return messages
