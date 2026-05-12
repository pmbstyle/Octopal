from __future__ import annotations

from typing import Any

_MANAGE_CANON_FILENAMES = {"facts.md", "decisions.md", "failures.md"}


async def manage_canon(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Octo tool to manage canonical memory."""
    octo = ctx["octo"]
    canon = octo.canon

    action = args.get("action")
    filename = args.get("filename")
    content = args.get("content", "")
    mode = args.get("mode", "append")

    if action == "list":
        files = [name for name in canon.list_files() if name in _MANAGE_CANON_FILENAMES]
        return f"Canonical Files: {', '.join(files)}"

    if action == "read":
        if not filename:
            return "Error: filename required for read action."
        filename = _normalize_manage_canon_filename(filename)
        if filename not in _MANAGE_CANON_FILENAMES:
            return "Error: manage_canon only supports facts.md, decisions.md, and failures.md."
        try:
            return canon.read_canon(filename)
        except ValueError as exc:
            return f"Error: {exc}"

    if action == "write":
        if not filename:
            return "Error: filename required for write action."
        filename = _normalize_manage_canon_filename(filename)
        if filename not in _MANAGE_CANON_FILENAMES:
            return "Error: manage_canon only supports facts.md, decisions.md, and failures.md."
        if not content:
            return "Error: Content required for write action."
        try:
            return await canon.write_canon(filename, content, mode)
        except ValueError as exc:
            return f"Error: {exc}"

    return f"Unknown action: {action}"


def _normalize_manage_canon_filename(filename: Any) -> str:
    candidate = str(filename or "").strip()
    if candidate and not candidate.endswith(".md"):
        candidate += ".md"
    return candidate


async def search_canon(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Octo tool to search canonical memory."""
    octo = ctx["octo"]
    canon = octo.canon

    query = args.get("query")
    if not query:
        return "Error: query required for search action."

    results = await canon.search_canon(query)
    if not results:
        return "No relevant canonical knowledge found."

    return "Found canonical facts:\n\n" + "\n\n---\n\n".join(results)
