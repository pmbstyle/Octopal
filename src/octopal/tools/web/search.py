from __future__ import annotations

from collections.abc import Mapping
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
    credentials = _search_credentials(_ctx)
    query = str(args.get("query", "")).strip()
    if not query:
        return _to_json(
            {
                "ok": False,
                "degraded": False,
                "fallback_used": False,
                "rate_limited": False,
                "source": "web_search",
                "provider": resolve_search_provider(args, credentials=credentials),
                "error": "query is required",
            }
        )
    if credentials is None:
        return _to_json(await run_search_async(args))
    return _to_json(await run_search_async(args, credentials=credentials))


def _search_credentials(ctx: dict[str, Any] | None) -> Mapping[str, str] | None:
    raw = ctx.get("search_credentials") if isinstance(ctx, dict) else None
    if not isinstance(raw, Mapping):
        return None
    return {
        str(name).strip().lower(): str(value).strip()
        for name, value in raw.items()
        if str(name).strip() and str(value).strip()
    }


def _to_json(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False)
