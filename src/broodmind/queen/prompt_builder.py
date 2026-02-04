from __future__ import annotations

import asyncio

import hashlib

import json

import os

from dataclasses import dataclass

from datetime import datetime, timedelta

from pathlib import Path

from typing import TYPE_CHECKING



if TYPE_CHECKING:

    from broodmind.memory.service import MemoryService

    from broodmind.providers.base import Message

    from broodmind.store.base import Store



_QUEEN_SYSTEM_PROMPT_CONTENT = ""





@dataclass

class BootstrapContext:

    content: str

    hash: str

    files: list[tuple[str, int]]





async def _load_system_prompt_file() -> str:

    """Loads the content of queen_system.md."""

    global _QUEEN_SYSTEM_PROMPT_CONTENT

    if not _QUEEN_SYSTEM_PROMPT_CONTENT:

        prompt_path = Path(__file__).parent / "prompts" / "queen_system.md"

        try:

            _QUEEN_SYSTEM_PROMPT_CONTENT = await asyncio.to_thread(

                prompt_path.read_text, encoding="utf-8"

            )

        except FileNotFoundError:

            _QUEEN_SYSTEM_PROMPT_CONTENT = "You are the BroodMind Queen. Your configuration files are missing. Tell the user that you are missing the configuration files and ask them to create them."

    return _QUEEN_SYSTEM_PROMPT_CONTENT





async def get_prompt_section(title: str) -> list[str]:

    """Extracts a section from the queen_system.md file by its ## title."""

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

    workspace = os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")

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

        return ["<persona>", content, "</persona>", ""]

    return []





async def build_bootstrap_context_prompt(store: "Store", chat_id: int) -> BootstrapContext:

    """Builds the workspace context from various files."""



    def _sync_logic():

        workspace = Path(os.getenv("BROODMIND_WORKSPACE_DIR", "workspace")).resolve()

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

        optional_files = [workspace / "HEARTBEAT.md", workspace / "MEMORY.md"]

        file_entries: list[tuple[str, str]] = []

        for path in required_files:

            if not path.exists():

                continue

            content = path.read_text(encoding="utf-8")

            file_entries.append((path.name, content))

        for path in optional_files:

            if not path.exists():

                continue

            content = path.read_text(encoding="utf-8")

            if content.strip():

                file_entries.append((path.name, content))

        for path in memory_files:

            content = path.read_text(encoding="utf-8")

            rel = path.relative_to(workspace).as_posix()

            file_entries.append((rel, content))

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

        parts = ["<workspace>"]

        for name, content in file_entries:

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





async def build_queen_prompt(

    store: "Store",

    memory: "MemoryService",

    user_text: str,

    chat_id: int,

    bootstrap_context: str,

) -> list["Message"]:

    """Assembles all the pieces into the final message list for the LLM."""

    from broodmind.providers.base import Message



    system_prompt = await _load_system_prompt_file()

    persona_prompt_lines = await build_persona_prompt()

    datetime_prompt = _current_datetime_prompt()



    memory_context = await memory.get_context(user_text)

    recent_history = await memory.get_recent_history(chat_id, limit=8)
    if recent_history and recent_history[-1][0] == "user" and recent_history[-1][1] == user_text:
        recent_history = recent_history[:-1]

    messages: list[Message] = [Message(role="system", content=system_prompt)]
    if persona_prompt_lines:
        messages.append(Message(role="system", content="\n".join(persona_prompt_lines)))
    if bootstrap_context:
        messages.append(Message(role="system", content=bootstrap_context))
    messages.append(Message(role="system", content=datetime_prompt))
    if memory_context:
        messages.append(
            Message(role="system", content="<context>\n" + "\n".join(memory_context) + "\n</context>")
        )
    if recent_history:
        for role, content in recent_history:
            messages.append(Message(role=role, content=content))
    messages.append(Message(role="user", content=user_text))
    return messages