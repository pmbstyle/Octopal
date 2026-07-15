from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from octopal.tools.web.providers import brave_provider, firecrawl_provider

SEARCH_PROVIDER_ORDER = ("brave", "firecrawl")


def resolve_search_provider(
    args: dict[str, Any], *, credentials: Mapping[str, str] | None = None
) -> str | None:
    requested = str(args.get("provider", "auto") or "auto").strip().lower()
    if requested in {"brave", "firecrawl"}:
        return requested
    if requested != "auto":
        return None

    for provider in SEARCH_PROVIDER_ORDER:
        if provider == "brave" and brave_provider.is_configured(_credential(credentials, "brave")):
            return provider
        if provider == "firecrawl" and firecrawl_provider.is_configured(
            _credential(credentials, "firecrawl")
        ):
            return provider
    return None


def run_search(args: dict[str, Any]) -> dict[str, Any]:
    requested = str(args.get("provider", "auto") or "auto").strip().lower()
    provider = resolve_search_provider(args)
    if provider is None:
        return {
            "ok": False,
            "degraded": False,
            "fallback_used": False,
            "rate_limited": False,
            "source": "web_search",
            "provider": None,
            "error": "no configured search provider available",
        }

    if requested in {"brave", "firecrawl"}:
        return _run_provider(provider, args)

    return _run_candidates(args, provider)


async def run_search_async(
    args: dict[str, Any], *, credentials: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Run the provider fallback chain through cancellable async clients."""
    requested = str(args.get("provider", "auto") or "auto").strip().lower()
    provider = resolve_search_provider(args, credentials=credentials)
    if provider is None:
        return {
            "ok": False,
            "degraded": False,
            "fallback_used": False,
            "rate_limited": False,
            "source": "web_search",
            "provider": None,
            "error": "no configured search provider available",
        }

    if requested in {"brave", "firecrawl"}:
        return await _run_provider_async(provider, args, credentials=credentials)

    return await _run_candidates_async(args, provider, credentials=credentials)


def _run_candidates(args: dict[str, Any], provider: str) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    ordered_candidates = _ordered_candidates(provider)

    for index, candidate in enumerate(ordered_candidates):
        result = _run_provider(candidate, args)
        if result.get("ok"):
            return _decorate_success(result, index=index, candidate=candidate, errors=errors)
        errors.append(_provider_error(candidate, result))

    return _all_failed(provider, ordered_candidates, errors)


async def _run_candidates_async(
    args: dict[str, Any],
    provider: str,
    *,
    credentials: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    ordered_candidates = _ordered_candidates(provider, credentials=credentials)

    for index, candidate in enumerate(ordered_candidates):
        result = await _run_provider_async(candidate, args, credentials=credentials)
        if result.get("ok"):
            return _decorate_success(result, index=index, candidate=candidate, errors=errors)
        errors.append(_provider_error(candidate, result))

    return _all_failed(provider, ordered_candidates, errors)


def _ordered_candidates(
    provider: str, *, credentials: Mapping[str, str] | None = None
) -> list[str]:
    configured = [
        name
        for name in SEARCH_PROVIDER_ORDER
        if name == provider or _is_configured(name, credentials=credentials)
    ]
    return list(dict.fromkeys(configured))


def _decorate_success(
    result: dict[str, Any],
    *,
    index: int,
    candidate: str,
    errors: list[dict[str, Any]],
) -> dict[str, Any]:
    if index > 0:
        result["fallback_used"] = True
        result["degraded"] = True
        result["fallback_provider"] = candidate
        result["attempted_providers"] = [entry["provider"] for entry in errors] + [candidate]
    return result


def _provider_error(candidate: str, result: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider": candidate,
        "error": result.get("error"),
        "rate_limited": bool(result.get("rate_limited", False)),
    }


def _all_failed(
    provider: str, ordered_candidates: list[str], errors: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "ok": False,
        "degraded": len(errors) > 1,
        "fallback_used": len(errors) > 1,
        "rate_limited": any(entry["rate_limited"] for entry in errors),
        "source": "web_search",
        "provider": ordered_candidates[0] if ordered_candidates else provider,
        "error": "all configured search providers failed",
        "attempted_providers": [entry["provider"] for entry in errors],
        "provider_errors": errors,
    }


def _is_configured(provider: str, *, credentials: Mapping[str, str] | None = None) -> bool:
    if provider == "brave":
        return brave_provider.is_configured(_credential(credentials, "brave"))
    if provider == "firecrawl":
        return firecrawl_provider.is_configured(_credential(credentials, "firecrawl"))
    return False


def _run_provider(provider: str, args: dict[str, Any]) -> dict[str, Any]:
    if provider == "brave":
        return brave_provider.search(args)
    if provider == "firecrawl":
        return firecrawl_provider.search(args)
    return {
        "ok": False,
        "degraded": False,
        "fallback_used": False,
        "rate_limited": False,
        "source": "web_search",
        "provider": provider,
        "error": f"unsupported search provider: {provider}",
    }


async def _run_provider_async(
    provider: str,
    args: dict[str, Any],
    *,
    credentials: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if provider == "brave":
        api_key = _credential(credentials, "brave")
        if api_key is None:
            return await brave_provider.search_async(args)
        return await brave_provider.search_async(args, api_key=api_key)
    if provider == "firecrawl":
        api_key = _credential(credentials, "firecrawl")
        if api_key is None:
            return await firecrawl_provider.search_async(args)
        return await firecrawl_provider.search_async(args, api_key=api_key)
    return {
        "ok": False,
        "degraded": False,
        "fallback_used": False,
        "rate_limited": False,
        "source": "web_search",
        "provider": provider,
        "error": f"unsupported search provider: {provider}",
    }


def _credential(credentials: Mapping[str, str] | None, provider: str) -> str | None:
    if credentials is None:
        return None
    return str(credentials.get(provider) or "").strip()
