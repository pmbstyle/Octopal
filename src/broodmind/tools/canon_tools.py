from __future__ import annotations

from typing import Any


async def manage_canon(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Queen tool to manage canonical memory."""
    queen = ctx["queen"]
    canon = queen.canon

    action = args.get("action")
    filename = args.get("filename")
    content = args.get("content", "")
    mode = args.get("mode", "append")

    if action == "list":
        files = canon.list_files()
        return f"Canonical Files: {', '.join(files)}"

    if action == "read":
        if not filename:
             return "Error: filename required for read action."
        try:
            return canon.read_canon(filename)
        except ValueError as exc:
            return f"Error: {exc}"

    if action == "write":
        if not filename:
             return "Error: filename required for write action."
        if not content:
            return "Error: Content required for write action."
        try:
            return await canon.write_canon(filename, content, mode)
        except ValueError as exc:
            return f"Error: {exc}"

    return f"Unknown action: {action}"

async def search_canon(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """Queen tool to search canonical memory."""
    queen = ctx["queen"]
    canon = queen.canon

    query = args.get("query")
    if not query:
        return "Error: query required for search action."

    results = await canon.search_canon(query)
    if not results:
        return "No relevant canonical knowledge found."

    return "Found canonical facts:\n\n" + "\n\n---\n\n".join(results)
