from __future__ import annotations

import os
from typing import Any

import httpx

BRAVE_SEARCH_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
DEFAULT_COUNT = 5
MAX_COUNT = 10


def is_configured() -> bool:
    return bool((os.getenv("BRAVE_API_KEY") or "").strip())


def search(args: dict[str, Any]) -> dict[str, Any]:
    prepared = _prepare_request(args)
    if prepared is None:
        return _error("missing BRAVE_API_KEY")
    query, params, api_key = prepared

    try:
        with httpx.Client(timeout=20.0) as client:
            response = client.get(
                BRAVE_SEARCH_ENDPOINT,
                params=params,
                headers={"X-Subscription-Token": api_key, "Accept": "application/json"},
            )
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        return _error(str(exc))

    return _success(query, data)


async def search_async(args: dict[str, Any]) -> dict[str, Any]:
    """Cancellable Brave search for bounded programmatic execution."""
    prepared = _prepare_request(args)
    if prepared is None:
        return _error("missing BRAVE_API_KEY")
    query, params, api_key = prepared

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(
                BRAVE_SEARCH_ENDPOINT,
                params=params,
                headers={"X-Subscription-Token": api_key, "Accept": "application/json"},
            )
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        return _error(str(exc))

    return _success(query, data)


def _prepare_request(args: dict[str, Any]) -> tuple[str, dict[str, str], str] | None:
    query = str(args.get("query", "")).strip()
    count = _bounded_count(args.get("count", DEFAULT_COUNT))
    country = str(args.get("country", "")).strip() or None
    search_lang = str(args.get("search_lang", "")).strip() or None
    ui_lang = str(args.get("ui_lang", "")).strip() or None
    freshness = str(args.get("freshness", "")).strip() or None

    api_key = (os.getenv("BRAVE_API_KEY") or "").strip()
    if not api_key:
        return None

    params = {"q": query, "count": str(count)}
    if country:
        params["country"] = country
    if search_lang:
        params["search_lang"] = search_lang
    if ui_lang:
        params["ui_lang"] = ui_lang
    if freshness:
        params["freshness"] = freshness
    return query, params, api_key


def _success(query: str, data: dict[str, Any]) -> dict[str, Any]:
    results = []
    for entry in (data.get("web", {}) or {}).get("results", []) or []:
        results.append(
            {
                "title": entry.get("title") or "",
                "url": entry.get("url") or "",
                "description": entry.get("description") or "",
                "published": entry.get("age"),
            }
        )

    return {
        "ok": True,
        "degraded": False,
        "fallback_used": False,
        "rate_limited": False,
        "source": "brave_search",
        "provider": "brave",
        "query": query,
        "count": len(results),
        "results": results,
    }


def _error(message: str) -> dict[str, Any]:
    lowered = message.lower()
    return {
        "ok": False,
        "degraded": False,
        "fallback_used": False,
        "rate_limited": "429" in lowered or "rate limit" in lowered,
        "source": "brave_search",
        "provider": "brave",
        "error": message,
    }


def _bounded_count(value: Any) -> int:
    try:
        count = int(value)
    except Exception:
        count = DEFAULT_COUNT
    return max(1, min(MAX_COUNT, count))
