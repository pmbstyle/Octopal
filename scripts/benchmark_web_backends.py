#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from octopal.tools.browser.actions import browser_close, browser_extract, browser_open
from octopal.tools.web.fetch import markdown_new_fetch, web_fetch
from octopal.tools.web.webclaw import webclaw_fetch

DEFAULT_CORPUS = Path(__file__).resolve().parents[1] / "benchmarks" / "web" / "corpus.json"
SUPPORTED_BACKENDS = ("basic", "markdown_new", "webclaw", "browser")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare Octopal web extraction backends without a running Octopal instance."
    )
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument(
        "--backends",
        default="basic,webclaw",
        help=f"Comma-separated backends: {','.join(SUPPORTED_BACKENDS)}",
    )
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--max-chars", type=int, default=20000)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--webclaw-binary", default=os.getenv("OCTOPAL_WEBCLAW_BINARY", "webclaw"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    corpus = _load_corpus(args.corpus)
    backends = _parse_backends(args.backends)
    repeat = max(1, min(args.repeat, 10))
    rows = asyncio.run(
        _run_benchmark(
            corpus,
            backends=backends,
            repeat=repeat,
            max_chars=max(200, min(args.max_chars, 200000)),
            timeout_seconds=max(1.0, min(args.timeout_seconds, 300.0)),
            webclaw_binary=args.webclaw_binary,
        )
    )
    report = {
        "schema_version": 1,
        "corpus": str(args.corpus),
        "backends": backends,
        "repeat": repeat,
        "summary": _summarize(rows),
        "runs": rows,
    }
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        sys.stdout.write(rendered + "\n")
    return 0


def _load_corpus(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not payload:
        raise ValueError("corpus must be a non-empty JSON array")
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict) or not str(item.get("url") or "").strip():
            raise ValueError(f"corpus entry {index} must contain a URL")
        rows.append(item)
    return rows


def _parse_backends(raw: str) -> list[str]:
    values = [part.strip().lower() for part in raw.split(",") if part.strip()]
    unknown = sorted(set(values) - set(SUPPORTED_BACKENDS))
    if unknown:
        raise ValueError(f"unsupported backends: {', '.join(unknown)}")
    return list(dict.fromkeys(values))


async def _run_benchmark(
    corpus: list[dict[str, Any]],
    *,
    backends: list[str],
    repeat: int,
    max_chars: int,
    timeout_seconds: float,
    webclaw_binary: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for iteration in range(1, repeat + 1):
        for entry in corpus:
            for backend in backends:
                rows.append(
                    await _run_one(
                        entry,
                        backend=backend,
                        iteration=iteration,
                        max_chars=max_chars,
                        timeout_seconds=timeout_seconds,
                        webclaw_binary=webclaw_binary,
                    )
                )
    return rows


async def _run_one(
    entry: dict[str, Any],
    *,
    backend: str,
    iteration: int,
    max_chars: int,
    timeout_seconds: float,
    webclaw_binary: str,
) -> dict[str, Any]:
    url = str(entry["url"])
    started = time.perf_counter()
    try:
        if backend == "browser":
            payload = await _browser_fetch(url, max_chars=max_chars)
        else:
            payload = _sync_fetch(
                backend,
                url=url,
                max_chars=max_chars,
                timeout_seconds=timeout_seconds,
                webclaw_binary=webclaw_binary,
            )
    except Exception as exc:
        payload = {"ok": False, "error": str(exc), "source": backend}
    duration_ms = int((time.perf_counter() - started) * 1000)
    snippet = str(payload.get("snippet") or payload.get("text") or "")
    expected_terms = [str(term) for term in entry.get("expected_terms", [])]
    matched_terms = [term for term in expected_terms if term.lower() in snippet.lower()]
    return {
        "id": entry.get("id"),
        "category": entry.get("category"),
        "url": url,
        "backend": backend,
        "iteration": iteration,
        "ok": bool(payload.get("ok")),
        "source": payload.get("source"),
        "duration_ms": duration_ms,
        "content_chars": len(snippet),
        "expected_terms": expected_terms,
        "matched_terms": matched_terms,
        "fidelity": len(matched_terms) / len(expected_terms) if expected_terms else None,
        "failure_reason": payload.get("failure_reason"),
        "error": str(payload.get("error") or "")[:500] or None,
    }


def _sync_fetch(
    backend: str,
    *,
    url: str,
    max_chars: int,
    timeout_seconds: float,
    webclaw_binary: str,
) -> dict[str, Any]:
    handler: Callable[[dict[str, Any]], str]
    args: dict[str, Any]
    if backend == "basic":
        handler = web_fetch
        args = {"url": url, "method": "GET", "max_chars": max_chars}
        with _without_environment("FIRECRAWL_API_KEY"):
            return _parse_payload(handler(args))
    if backend == "markdown_new":
        handler = markdown_new_fetch
        args = {
            "url": url,
            "method": "auto",
            "max_chars": max_chars,
            "timeout_seconds": timeout_seconds,
            "fallback_to_web_fetch": False,
        }
        return _parse_payload(handler(args))
    if backend == "webclaw":
        handler = webclaw_fetch
        args = {
            "url": url,
            "max_chars": max_chars,
            "timeout_seconds": timeout_seconds,
            "binary": webclaw_binary,
            "enabled": True,
        }
        return _parse_payload(handler(args))
    raise ValueError(f"unsupported synchronous backend: {backend}")


async def _browser_fetch(url: str, *, max_chars: int) -> dict[str, Any]:
    chat_id = abs(hash(url)) % 2_000_000_000 + 1
    try:
        opened = await browser_open({"url": url}, {"chat_id": chat_id})
        if str(opened).lower().startswith("error"):
            return {"ok": False, "source": "browser", "error": str(opened)}
        extracted = await browser_extract({"max_chars": max_chars}, {"chat_id": chat_id})
        if isinstance(extracted, dict):
            payload = dict(extracted)
            payload["snippet"] = payload.get("text", "")
            return payload
        return _parse_payload(extracted)
    finally:
        await browser_close({}, {"chat_id": chat_id})


def _parse_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    parsed = json.loads(str(raw))
    if not isinstance(parsed, dict):
        raise ValueError("backend returned a non-object payload")
    return parsed


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for backend in sorted({str(row["backend"]) for row in rows}):
        matching = [row for row in rows if row["backend"] == backend]
        durations = [int(row["duration_ms"]) for row in matching]
        fidelities = [float(row["fidelity"]) for row in matching if row["fidelity"] is not None]
        summary[backend] = {
            "runs": len(matching),
            "success_rate": sum(bool(row["ok"]) for row in matching) / len(matching),
            "median_duration_ms": statistics.median(durations),
            "mean_fidelity": statistics.fmean(fidelities) if fidelities else None,
            "median_content_chars": statistics.median(
                int(row["content_chars"]) for row in matching
            ),
        }
    return summary


@contextmanager
def _without_environment(*names: str) -> Iterator[None]:
    previous = {name: os.environ.get(name) for name in names}
    try:
        for name in names:
            os.environ.pop(name, None)
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


if __name__ == "__main__":
    raise SystemExit(main())
