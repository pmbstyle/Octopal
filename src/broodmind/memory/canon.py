from __future__ import annotations

import asyncio
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import structlog
from broodmind.utils import utc_now

if TYPE_CHECKING:
    from broodmind.providers.embeddings import EmbeddingsProvider
    from broodmind.store.base import Store


logger = structlog.get_logger(__name__)

@dataclass
class CanonService:
    workspace_dir: Path
    store: Store
    embeddings: EmbeddingsProvider | None = None
    max_file_chars: int = 4000  # Guardrail for canon bloat

    def __post_init__(self) -> None:
        self.canon_dir = self.workspace_dir / "memory" / "canon"
        self.canon_dir.mkdir(parents=True, exist_ok=True)
        self.events_file = self.canon_dir / "events.jsonl"
        # Ensure default files exist
        for filename in ["facts.md", "decisions.md", "failures.md"]:
            path = self.canon_dir / filename
            if not path.exists():
                path.write_text(f"# {filename.replace('.md', '').title()}\n\n", encoding="utf-8")
        self._ensure_event_log_bootstrap()

    def _normalize_filename(self, filename: str) -> str:
        candidate = filename.strip()
        if not candidate:
            raise ValueError("filename is required")
        if not candidate.endswith(".md"):
            candidate += ".md"
        if "/" in candidate or "\\" in candidate or ".." in candidate:
            raise ValueError("invalid filename")
        return candidate

    def read_canon(self, filename: str) -> str:
        """Reads a canonical memory file."""
        filename = self._normalize_filename(filename)
        path = self.canon_dir / filename
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8")

    async def write_canon(self, filename: str, content: str, mode: Literal["append", "overwrite"] = "append") -> str:
        """Writes to a canonical memory file and triggers re-indexing."""
        filename = self._normalize_filename(filename)
        await asyncio.to_thread(self._append_event, filename, content, mode)
        rebuilt = await asyncio.to_thread(self._compact_from_events)
        new_content = rebuilt.get(filename, "")

        # Trigger async re-indexing
        if self.embeddings:
            asyncio.create_task(self.index_canon(filename))

        if len(new_content) > self.max_file_chars:
            return f"WARNING: {filename} size ({len(new_content)} chars) exceeds limit ({self.max_file_chars}). Please summarize/compact it immediately."
        return "Success"

    async def index_canon(self, filename: str) -> None:
        """Chunks and embeds a canonical file."""
        if not self.embeddings:
            return

        content = self.read_canon(filename)
        if not content.strip():
            return

        # Simple chunking by paragraph/headers
        chunks = [c.strip() for c in content.split("\n\n") if c.strip()]
        if not chunks:
            return

        try:
            vectors = await self.embeddings.embed(chunks)
            await asyncio.to_thread(self.store.clear_canon_embeddings, filename)

            for i, (chunk, vector) in enumerate(zip(chunks, vectors, strict=False)):
                await asyncio.to_thread(
                    self.store.add_canon_embedding,
                    filename=filename,
                    chunk_index=i,
                    content=chunk,
                    model="openai-text-embedding-3-small",
                    vector=vector
                )
            logger.info("Canon file indexed", filename=filename, chunks=len(chunks))
        except Exception:
            logger.exception("Failed to index canon file", filename=filename)

    async def search_canon(self, query: str, top_k: int = 3) -> list[str]:
        """Searches across all indexed canon files."""
        if not self.embeddings:
            return []

        try:
            query_vectors = await self.embeddings.embed([query])
            if not query_vectors:
                return []
            query_vector = query_vectors[0]

            all_entries = await asyncio.to_thread(self.store.list_canon_embeddings)
            scored: list[tuple[float, str]] = []

            for entry in all_entries:
                score = _cosine_similarity(query_vector, entry["vector"])
                if score > 0.3: # Minimum threshold
                    scored.append((score, entry["content"]))

            scored.sort(key=lambda x: x[0], reverse=True)
            return [content for _, content in scored[:top_k]]
        except Exception:
            logger.exception("Canon search failed")
            return []

    def get_tier1_context(self) -> str:
        """Returns the high-priority canonical context (decisions and failures)."""
        decisions = self.read_canon("decisions.md").strip()
        failures = self.read_canon("failures.md").strip()

        context_parts = []
        if decisions and len(decisions) > len("# Decisions"):
            # Simple truncation for now - keep last N chars but try to align with lines
            if len(decisions) > 2000:
                 # Find a newline to cut safely
                 cut_idx = len(decisions) - 2000
                 safe_cut = decisions.find("\n", cut_idx)
                 if safe_cut != -1:
                     decisions = "...(older decisions omitted)\n" + decisions[safe_cut+1:]
                 else:
                     decisions = "...(older decisions omitted)\n" + decisions[-2000:]
            context_parts.append(f"<canon_decisions>\n{decisions}\n</canon_decisions>")

        if failures and len(failures) > len("# Failures"):
             if len(failures) > 2000:
                 cut_idx = len(failures) - 2000
                 safe_cut = failures.find("\n", cut_idx)
                 if safe_cut != -1:
                     failures = "...(older failures omitted)\n" + failures[safe_cut+1:]
                 else:
                     failures = "...(older failures omitted)\n" + failures[-2000:]
             context_parts.append(f"<canon_failures>\n{failures}\n</canon_failures>")

        return "\n\n".join(context_parts)

    def list_files(self) -> list[str]:
        return sorted(p.name for p in self.canon_dir.glob("*.md"))

    def _ensure_event_log_bootstrap(self) -> None:
        if self.events_file.exists():
            return

        entries: list[dict[str, str]] = []
        for path in sorted(self.canon_dir.glob("*.md")):
            content = path.read_text(encoding="utf-8")
            if not content.strip():
                continue
            entries.append(
                {
                    "ts": utc_now().isoformat(),
                    "filename": path.name,
                    "mode": "overwrite",
                    "content": content,
                }
            )

        if not entries:
            self.events_file.write_text("", encoding="utf-8")
            return

        with self.events_file.open("w", encoding="utf-8") as f:
            for entry in entries:
                f.write(json.dumps(entry, ensure_ascii=True))
                f.write("\n")

    def _append_event(self, filename: str, content: str, mode: Literal["append", "overwrite"]) -> None:
        event = {
            "ts": utc_now().isoformat(),
            "filename": filename,
            "mode": mode,
            "content": content,
        }
        with self.events_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=True))
            f.write("\n")

    def _compact_from_events(self) -> dict[str, str]:
        if not self.events_file.exists():
            return {}

        state: dict[str, str] = {}
        for raw in self.events_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            filename = str(entry.get("filename", "")).strip()
            if (
                not filename
                or not filename.endswith(".md")
                or "/" in filename
                or "\\" in filename
                or ".." in filename
            ):
                continue
            mode = str(entry.get("mode", "append"))
            content = str(entry.get("content", ""))

            if mode == "overwrite":
                state[filename] = content
                continue

            current = state.get(filename, "")
            if current and not current.endswith("\n"):
                current += "\n"
            state[filename] = current + content

        for filename, content in state.items():
            path = self.canon_dir / filename
            path.write_text(content, encoding="utf-8")
        return state

def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
