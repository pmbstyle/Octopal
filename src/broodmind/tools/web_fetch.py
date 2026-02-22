from __future__ import annotations

import os
from html.parser import HTMLParser
from typing import Any
import json as pyjson
from urllib.parse import urlparse

import httpx

DEFAULT_MAX_CHARS = 20000
ALLOWED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}
MARKDOWN_NEW_ENDPOINT = "https://markdown.new/"


class _HTMLTextExtractor(HTMLParser):
    """Extract readable text from HTML, excluding scripts and styles."""

    def __init__(self) -> None:
        super().__init__()
        self._text_parts: list[str] = []
        self._skip_tag = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style", "head", "meta", "link"}:
            self._skip_tag = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "head", "meta", "link"}:
            self._skip_tag = False
        elif tag.lower() in {"p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "br"}:
            self._text_parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip_tag:
            stripped = data.strip()
            if stripped:
                self._text_parts.append(stripped)

    def get_text(self) -> str:
        return " ".join(self._text_parts)


def web_fetch(args: dict[str, Any]) -> str:
    url = str(args.get("url", "")).strip()
    if not url:
        return "web_fetch error: url is required."
    if not _is_safe_url(url):
        return "web_fetch error: url not allowed."
    method = str(args.get("method", "GET")).strip().upper()
    if method not in ALLOWED_METHODS:
        return "web_fetch error: unsupported method. Allowed: GET, POST, PUT, PATCH, DELETE."
    max_chars_raw = args.get("max_chars", DEFAULT_MAX_CHARS)
    try:
        max_chars = int(max_chars_raw)
    except Exception:
        max_chars = DEFAULT_MAX_CHARS
    max_chars = max(200, min(200000, max_chars))

    # Support custom headers (e.g. for API tokens)
    custom_headers = args.get("headers")
    if not isinstance(custom_headers, dict):
        custom_headers = {}
    params = args.get("params")
    if not isinstance(params, dict):
        params = None
    json_body = args.get("json")
    body = args.get("body")

    # Try Firecrawl first if configured for HTML GET fetches
    firecrawl_key = os.getenv("FIRECRAWL_API_KEY")
    if firecrawl_key and method == "GET" and json_body is None and body is None:
        try:
            return _fetch_firecrawl(url, firecrawl_key, max_chars, custom_headers)
        except Exception:
            # Fall back to basic fetch if Firecrawl fails
            pass

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BroodMind/1.0)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    # Merge custom headers
    if custom_headers:
        headers.update(custom_headers)

    try:
        with httpx.Client(timeout=20.0, headers=headers) as client:
            request_kwargs: dict[str, Any] = {"params": params}
            if json_body is not None:
                request_kwargs["json"] = json_body
            elif body is not None:
                if isinstance(body, (dict, list)):
                    request_kwargs["content"] = pyjson.dumps(body, ensure_ascii=False)
                    if "Content-Type" not in headers and "content-type" not in headers:
                        headers["Content-Type"] = "application/json"
                else:
                    request_kwargs["content"] = str(body)
            resp = client.request(method, url, **request_kwargs)
        content = resp.text
        # Extract readable text if HTML
        content_type = (resp.headers.get("content-type") or "").lower()
        if "text/html" in content_type:
            extractor = _HTMLTextExtractor()
            try:
                extractor.feed(content)
                text = extractor.get_text()
            except Exception:
                text = content  # Fall back to raw content if parsing fails
        else:
            text = content
        snippet = text[:max_chars]
        payload = {
            "url": url,
            "method": method,
            "status_code": resp.status_code,
            "content_type": resp.headers.get("content-type"),
            "snippet": snippet,
        }
        return _to_json(payload)
    except Exception as exc:
        return f"web_fetch error: {exc}"


def markdown_new_fetch(args: dict[str, Any]) -> str:
    """Fetch URL content as markdown via markdown.new with graceful fallback to web_fetch."""
    url = str(args.get("url", "")).strip()
    if not url:
        return "markdown_new_fetch error: url is required."
    if not _is_safe_url(url):
        return "markdown_new_fetch error: url not allowed."

    method = str(args.get("method", "auto")).strip().lower()
    if method not in {"auto", "ai", "browser"}:
        return "markdown_new_fetch error: unsupported method. Allowed: auto, ai, browser."

    retain_images = bool(args.get("retain_images", False))
    fallback_to_web_fetch = bool(args.get("fallback_to_web_fetch", True))

    max_chars_raw = args.get("max_chars", DEFAULT_MAX_CHARS)
    try:
        max_chars = int(max_chars_raw)
    except Exception:
        max_chars = DEFAULT_MAX_CHARS
    max_chars = max(200, min(200000, max_chars))

    timeout_raw = args.get("timeout_seconds", 60)
    try:
        timeout_seconds = float(timeout_raw)
    except Exception:
        timeout_seconds = 60.0
    timeout_seconds = max(5.0, min(300.0, timeout_seconds))

    endpoint = str(args.get("endpoint", MARKDOWN_NEW_ENDPOINT)).strip() or MARKDOWN_NEW_ENDPOINT

    payload = {
        "url": url,
        "method": method,
        "retain_images": retain_images,
    }

    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            resp = client.post(
                endpoint,
                json=payload,
                headers={
                    "Accept": "text/markdown",
                    "Content-Type": "application/json",
                    "User-Agent": "BroodMind/1.0",
                },
            )
    except Exception as exc:
        return _markdown_new_with_fallback(
            url=url,
            max_chars=max_chars,
            fallback_to_web_fetch=fallback_to_web_fetch,
            reason=f"request_failed: {exc}",
        )

    rate_limit_remaining = resp.headers.get("x-rate-limit-remaining")
    markdown_tokens = resp.headers.get("x-markdown-tokens")

    if resp.status_code == 200:
        snippet = resp.text[:max_chars]
        result = {
            "ok": True,
            "degraded": False,
            "fallback_used": False,
            "rate_limited": False,
            "source": "markdown.new",
            "url": url,
            "status_code": resp.status_code,
            "content_type": resp.headers.get("content-type"),
            "snippet": snippet,
            "method": method,
            "retain_images": retain_images,
            "rate_limit_remaining": rate_limit_remaining,
            "markdown_tokens": _safe_int(markdown_tokens),
        }
        return _to_json(result)

    if resp.status_code == 429:
        return _markdown_new_with_fallback(
            url=url,
            max_chars=max_chars,
            fallback_to_web_fetch=fallback_to_web_fetch,
            reason="rate_limited",
            upstream_status=429,
            rate_limit_remaining=rate_limit_remaining,
        )

    if resp.status_code >= 500:
        return _markdown_new_with_fallback(
            url=url,
            max_chars=max_chars,
            fallback_to_web_fetch=fallback_to_web_fetch,
            reason=f"upstream_{resp.status_code}",
            upstream_status=resp.status_code,
            rate_limit_remaining=rate_limit_remaining,
        )

    return _to_json(
        {
            "ok": False,
            "degraded": True,
            "fallback_used": False,
            "rate_limited": resp.status_code == 429,
            "source": "markdown.new",
            "url": url,
            "status_code": resp.status_code,
            "error": f"markdown.new request failed with status {resp.status_code}",
            "body_snippet": resp.text[:500],
            "rate_limit_remaining": rate_limit_remaining,
        }
    )


def _fetch_firecrawl(url: str, api_key: str, max_chars: int, target_headers: dict[str, Any] | None = None) -> str:
    """Fetch content using Firecrawl API."""
    endpoint = "https://api.firecrawl.dev/v1/scrape"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "url": url,
        "formats": ["markdown"],
        "onlyMainContent": True,
        "timeout": 30000,
    }
    if target_headers:
        payload["headers"] = target_headers

    with httpx.Client(timeout=40.0) as client:
        resp = client.post(endpoint, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    if not data.get("success"):
        raise ValueError(f"Firecrawl failed: {data.get('error')}")

    markdown = data.get("data", {}).get("markdown", "")
    snippet = markdown[:max_chars]

    result = {
        "url": url,
        "status_code": 200,
        "content_type": "text/markdown",
        "snippet": snippet,
        "source": "firecrawl",
    }
    return _to_json(result)


def _is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").lower()
    return host not in {"localhost", "127.0.0.1", "0.0.0.0", "::1"}


def _to_json(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False)


def _markdown_new_with_fallback(
    *,
    url: str,
    max_chars: int,
    fallback_to_web_fetch: bool,
    reason: str,
    upstream_status: int | None = None,
    rate_limit_remaining: str | None = None,
) -> str:
    if fallback_to_web_fetch:
        fallback_raw = web_fetch({"url": url, "method": "GET", "max_chars": max_chars})
        fallback_json: dict[str, Any] | None = None
        try:
            parsed = pyjson.loads(fallback_raw)
            if isinstance(parsed, dict):
                fallback_json = parsed
        except Exception:
            fallback_json = None

        if fallback_json is not None:
            fallback_json.update(
                {
                    "ok": True,
                    "degraded": True,
                    "fallback_used": True,
                    "rate_limited": reason == "rate_limited",
                    "source": fallback_json.get("source") or "web_fetch_fallback",
                    "fallback_reason": reason,
                    "upstream_status": upstream_status,
                    "rate_limit_remaining": rate_limit_remaining,
                }
            )
            return _to_json(fallback_json)

        return _to_json(
            {
                "ok": False,
                "degraded": True,
                "fallback_used": True,
                "rate_limited": reason == "rate_limited",
                "source": "web_fetch_fallback",
                "url": url,
                "error": "markdown.new failed and fallback failed",
                "fallback_reason": reason,
                "fallback_error": fallback_raw[:500],
                "upstream_status": upstream_status,
                "rate_limit_remaining": rate_limit_remaining,
            }
        )

    return _to_json(
        {
            "ok": False,
            "degraded": True,
            "fallback_used": False,
            "rate_limited": reason == "rate_limited",
            "source": "markdown.new",
            "url": url,
            "error": "markdown.new failed and fallback disabled",
            "failure_reason": reason,
            "upstream_status": upstream_status,
            "rate_limit_remaining": rate_limit_remaining,
        }
    )


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(str(value).strip())
    except Exception:
        return None
