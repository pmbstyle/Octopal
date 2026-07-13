from __future__ import annotations

import os
from typing import Any

import httpx

FIRECRAWL_SEARCH_ENDPOINT = "https://api.firecrawl.dev/v2/search"
DEFAULT_COUNT = 5
MAX_COUNT = 10


def is_configured(api_key: str | None = None) -> bool:
    return bool((api_key if api_key is not None else os.getenv("FIRECRAWL_API_KEY") or "").strip())


def search(args: dict[str, Any]) -> dict[str, Any]:
    prepared = _prepare_request(args)
    if prepared is None:
        return _error("missing FIRECRAWL_API_KEY")
    query, body, api_key = prepared

    try:
        with httpx.Client(timeout=25.0) as client:
            response = client.post(
                FIRECRAWL_SEARCH_ENDPOINT,
                json=body,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        return _error(str(exc))

    return _success(query, data)


async def search_async(args: dict[str, Any], *, api_key: str | None = None) -> dict[str, Any]:
    """Cancellable Firecrawl search for bounded programmatic execution."""
    prepared = _prepare_request(args, api_key=api_key)
    if prepared is None:
        return _error("missing FIRECRAWL_API_KEY")
    query, body, api_key = prepared

    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            response = await client.post(
                FIRECRAWL_SEARCH_ENDPOINT,
                json=body,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        return _error(str(exc))

    return _success(query, data)


def _prepare_request(
    args: dict[str, Any], *, api_key: str | None = None
) -> tuple[str, dict[str, Any], str] | None:
    query = str(args.get("query", "")).strip()
    count = _bounded_count(args.get("count", DEFAULT_COUNT))
    country = str(args.get("country", "")).strip() or "US"
    location = str(args.get("location", "")).strip() or None
    freshness = str(args.get("freshness", "")).strip() or None

    resolved_api_key = (
        api_key if api_key is not None else os.getenv("FIRECRAWL_API_KEY") or ""
    ).strip()
    if not resolved_api_key:
        return None

    body: dict[str, Any] = {
        "query": query,
        "limit": count,
        "sources": [{"type": "web"}],
        "country": country,
        "timeout": 20000,
    }
    if location:
        body["location"] = location
    if freshness:
        body["tbs"] = freshness
    return query, body, resolved_api_key


def _success(query: str, data: dict[str, Any]) -> dict[str, Any]:
    payload = data.get("data", {}) if isinstance(data, dict) else {}
    results = []
    for entry in payload.get("web", []) or []:
        results.append(
            {
                "title": entry.get("title") or "",
                "url": entry.get("url") or entry.get("sourceURL") or "",
                "description": entry.get("description") or "",
                "published": entry.get("age"),
            }
        )

    return {
        "ok": True,
        "degraded": False,
        "fallback_used": False,
        "rate_limited": False,
        "source": "firecrawl_search",
        "provider": "firecrawl",
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
        "source": "firecrawl_search",
        "provider": "firecrawl",
        "error": message,
    }


def _bounded_count(value: Any) -> int:
    try:
        count = int(value)
    except Exception:
        count = DEFAULT_COUNT
    return max(1, min(MAX_COUNT, count))
