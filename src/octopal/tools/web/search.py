from __future__ import annotations

from typing import Any

from octopal.tools.web.providers.registry import (
    resolve_search_provider,
    run_search,
    run_search_async,
)


def web_search(args: dict[str, Any]) -> str:
    query = str(args.get("query", "")).strip()
    if not query:
        return _to_json(
            {
                "ok": False,
                "degraded": False,
                "fallback_used": False,
                "rate_limited": False,
                "source": "web_search",
                "provider": resolve_search_provider(args),
                "error": "query is required",
            }
        )
    return _to_json(run_search(args))


async def web_search_async(args: dict[str, Any], _ctx: dict[str, Any] | None = None) -> str:
    """Cancellable web search handler for workers and programmatic batches."""
    query = str(args.get("query", "")).strip()
    if not query:
        return _to_json(
            {
                "ok": False,
                "degraded": False,
                "fallback_used": False,
                "rate_limited": False,
                "source": "web_search",
                "provider": resolve_search_provider(args),
                "error": "query is required",
            }
        )
    return _to_json(await run_search_async(args))


def _to_json(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False)
