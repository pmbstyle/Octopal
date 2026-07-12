from __future__ import annotations

import base64
import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from octopal.infrastructure.config.settings import Settings, load_settings

_REF_RE = re.compile(r"\b(e\d+)(?=[:\]\s])")


class PinchTabError(RuntimeError):
    pass


@dataclass
class _ChatState:
    current_tab: str | None = None
    tabs: set[str] = field(default_factory=set)


class PinchTabBrowserBackend:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
        browser: str,
        token: str | None = None,
        session: str | None = None,
        ownership_file: Path | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._browser = browser.strip() or "chrome"
        self._token = token
        self._session = session
        self._ownership_file = ownership_file
        self._transport = transport
        self._chats: dict[int, _ChatState] = {}

    def _state(self, chat_id: int) -> _ChatState:
        return self._chats.setdefault(chat_id, _ChatState())

    def _persist_owned_tabs(self) -> None:
        if self._ownership_file is None:
            return
        owned = sorted({tab_id for state in self._chats.values() for tab_id in state.tabs})
        self._ownership_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._ownership_file.with_name(self._ownership_file.name + ".tmp")
        temporary.write_text(json.dumps(owned), encoding="utf-8")
        temporary.replace(self._ownership_file)

    def _headers(self, chat_id: int) -> dict[str, str]:
        headers = {"X-Agent-Id": f"octopal-{chat_id}"}
        if self._session:
            headers["Authorization"] = f"Session {self._session}"
        elif self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _redact(self, value: str) -> str:
        for secret in (self._token, self._session):
            if secret:
                value = value.replace(secret, "[redacted]")
        return value

    async def _request(
        self,
        method: str,
        path: str,
        *,
        chat_id: int,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> tuple[Any, str]:
        try:
            async with httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout_seconds,
                transport=self._transport,
            ) as client:
                response = await client.request(
                    method,
                    path,
                    params=params,
                    json=json,
                    headers=self._headers(chat_id),
                )
        except httpx.TimeoutException as exc:
            raise PinchTabError(f"PinchTab timed out after {self._timeout_seconds:g}s") from exc
        except httpx.HTTPError as exc:
            raise PinchTabError(f"PinchTab connection failed: {self._redact(str(exc))}") from exc

        text = response.text.strip()
        if response.is_error:
            detail = text
            try:
                payload = response.json()
                if isinstance(payload, dict):
                    detail = str(payload.get("error") or payload.get("message") or text)
            except ValueError:
                pass
            raise PinchTabError(f"PinchTab HTTP {response.status_code}: {self._redact(detail)}")

        try:
            return response.json(), text
        except ValueError:
            return text, text

    @staticmethod
    def _chat_id(ctx: dict[str, Any]) -> int:
        return int(ctx.get("chat_id") or 0)

    def _resolve_tab(self, args: dict[str, Any], chat_id: int) -> str:
        state = self._state(chat_id)
        target_id = str(args.get("target_id") or "").strip() or state.current_tab
        if not target_id:
            raise PinchTabError("No browser target. Run browser_open first.")
        if target_id not in state.tabs:
            raise PinchTabError(f"Unknown browser target '{target_id}' for this chat")
        state.current_tab = target_id
        return target_id

    @staticmethod
    def _tab_path(tab_id: str, suffix: str) -> str:
        return f"/tabs/{quote(tab_id, safe='')}/{suffix}"

    async def open(self, args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
        url = str(args.get("url") or "").strip()
        if not url:
            return {"ok": False, "error": "url is required"}
        chat_id = self._chat_id(ctx)
        state = self._state(chat_id)
        target_id = str(args.get("target_id") or "").strip()
        try:
            payload: dict[str, Any] = {"url": url, "browser": self._browser}
            if bool(args.get("new_tab", False)):
                payload["newTab"] = True
            elif target_id:
                if target_id not in state.tabs:
                    raise PinchTabError(f"Unknown browser target '{target_id}' for this chat")
                payload["tabId"] = target_id
            elif state.current_tab:
                payload["tabId"] = state.current_tab
            else:
                payload["newTab"] = True

            result, _ = await self._request("POST", "/navigate", chat_id=chat_id, json=payload)
            if not isinstance(result, dict):
                raise PinchTabError("PinchTab returned an invalid navigate response")
            resolved = str(result.get("tabId") or target_id or state.current_tab or "").strip()
            if not resolved:
                raise PinchTabError("PinchTab navigate response did not include tabId")
            state.tabs.add(resolved)
            state.current_tab = resolved
            self._persist_owned_tabs()
            return {
                "ok": True,
                "message": f"Successfully opened {url}",
                "url": str(result.get("url") or url),
                "target_id": resolved,
            }
        except Exception as exc:
            return {"ok": False, "url": url, "error": f"Error opening {url}: {exc}"}

    async def tabs(self, args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
        del args
        chat_id = self._chat_id(ctx)
        state = self._state(chat_id)
        try:
            payload, _ = await self._request("GET", "/tabs", chat_id=chat_id)
            rows = payload.get("tabs", []) if isinstance(payload, dict) else []
            pages = []
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, dict):
                    continue
                tab_id = str(row.get("id") or row.get("tabId") or "").strip()
                if tab_id not in state.tabs:
                    continue
                pages.append(
                    {
                        "target_id": tab_id,
                        "url": str(row.get("url") or ""),
                        "title": str(row.get("title") or ""),
                        "is_current": tab_id == state.current_tab,
                    }
                )
            live_ids = {str(page["target_id"]) for page in pages}
            state.tabs.intersection_update(live_ids)
            if state.current_tab not in state.tabs:
                state.current_tab = next(iter(state.tabs), None)
            return {"ok": True, "count": len(pages), "pages": pages}
        except Exception as exc:
            return {"ok": False, "count": 0, "pages": [], "error": str(exc)}

    async def focus_tab(self, args: dict[str, Any], ctx: dict[str, Any]) -> str:
        chat_id = self._chat_id(ctx)
        target_id = str(args.get("target_id") or "").strip()
        if not target_id:
            return "Error: target_id is required"
        state = self._state(chat_id)
        if target_id not in state.tabs:
            return f"Error focusing {target_id}: Unknown browser target for this chat"
        state.current_tab = target_id
        return f"Focused {target_id}"

    async def navigate(self, args: dict[str, Any], ctx: dict[str, Any]) -> str:
        url = str(args.get("url") or "").strip()
        if not url:
            return "Error: url is required"
        chat_id = self._chat_id(ctx)
        try:
            tab_id = self._resolve_tab(args, chat_id)
            await self._request(
                "POST",
                self._tab_path(tab_id, "navigate"),
                chat_id=chat_id,
                json={"url": url},
            )
            return f"Navigated to {url}"
        except Exception as exc:
            return f"Error navigating to {url}: {exc}"

    async def snapshot(self, args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
        chat_id = self._chat_id(ctx)
        target_id = str(args.get("target_id") or "").strip() or None
        try:
            tab_id = self._resolve_tab(args, chat_id)
            payload, raw = await self._request(
                "GET",
                self._tab_path(tab_id, "snapshot"),
                chat_id=chat_id,
                params={"filter": "interactive", "format": "compact"},
            )
            snapshot = (
                str(payload.get("snapshot") or payload.get("text") or raw)
                if isinstance(payload, dict)
                else str(payload)
            )
            return {
                "ok": True,
                "target_id": tab_id,
                "snapshot": snapshot,
                "refs_count": len(set(_REF_RE.findall(snapshot))),
            }
        except Exception as exc:
            return {
                "ok": False,
                "target_id": target_id,
                "error": f"Error taking snapshot: {exc}",
            }

    async def _action(
        self, args: dict[str, Any], ctx: dict[str, Any], payload: dict[str, Any]
    ) -> None:
        chat_id = self._chat_id(ctx)
        tab_id = self._resolve_tab(args, chat_id)
        result, _ = await self._request(
            "POST", self._tab_path(tab_id, "action"), chat_id=chat_id, json=payload
        )
        if isinstance(result, dict):
            switched_to = str(result.get("switchedToTab") or "").strip()
            if switched_to:
                state = self._state(chat_id)
                state.tabs.add(switched_to)
                state.current_tab = switched_to
                self._persist_owned_tabs()

    async def click(self, args: dict[str, Any], ctx: dict[str, Any]) -> str:
        ref = str(args.get("ref") or "").strip()
        if not ref:
            return "Error: ref is required"
        try:
            await self._action(args, ctx, {"kind": "click", "selector": ref})
            return f"Clicked {ref}"
        except Exception as exc:
            return f"Error clicking {ref}: {exc}"

    async def type(self, args: dict[str, Any], ctx: dict[str, Any]) -> str:
        ref = str(args.get("ref") or "").strip()
        text = args.get("text")
        if not ref or text is None:
            return "Error: ref and text are required"
        try:
            await self._action(args, ctx, {"kind": "fill", "selector": ref, "value": str(text)})
            if bool(args.get("press_enter", False)):
                await self._action(args, ctx, {"kind": "press", "key": "Enter"})
            return f"Typed into {ref}"
        except Exception as exc:
            return f"Error typing into {ref}: {exc}"

    async def close(self, args: dict[str, Any], ctx: dict[str, Any]) -> str:
        chat_id = self._chat_id(ctx)
        state = self._state(chat_id)
        target_id = str(args.get("target_id") or "").strip()
        if target_id:
            if target_id not in state.tabs:
                return f"Error closing {target_id}: Unknown browser target for this chat"
            try:
                await self._request(
                    "POST",
                    self._tab_path(target_id, "close"),
                    chat_id=chat_id,
                    json={},
                )
                state.tabs.discard(target_id)
                if state.current_tab == target_id:
                    state.current_tab = next(iter(state.tabs), None)
                self._persist_owned_tabs()
                return f"Closed browser target {target_id}"
            except Exception as exc:
                return f"Error closing {target_id}: {exc}"

        errors = []
        for tab_id in tuple(state.tabs):
            try:
                await self._request(
                    "POST", self._tab_path(tab_id, "close"), chat_id=chat_id, json={}
                )
            except Exception as exc:
                errors.append(f"{tab_id}: {exc}")
        if errors:
            return "Error closing browser session: " + "; ".join(errors)
        self._chats.pop(chat_id, None)
        self._persist_owned_tabs()
        return "Browser session closed"

    async def wait_for(self, args: dict[str, Any], ctx: dict[str, Any]) -> str:
        ref = str(args.get("ref") or "").strip()
        text = str(args.get("text") or "").strip()
        state = str(args.get("state") or "visible").strip() or "visible"
        timeout_ms = int(args.get("timeout_ms") or 10000)
        if not ref and not text:
            return "Error: ref or text is required"
        chat_id = self._chat_id(ctx)
        try:
            tab_id = self._resolve_tab(args, chat_id)
            payload: dict[str, Any] = {"timeout": timeout_ms, "state": state}
            if ref:
                payload["selector"] = ref
            else:
                payload["text"] = text
            await self._request(
                "POST", self._tab_path(tab_id, "wait"), chat_id=chat_id, json=payload
            )
            return f"Element {ref} is now {state}" if ref else f"Text appeared: {text}"
        except Exception as exc:
            return f"Error waiting for {ref or text}: {exc}"

    async def extract(self, args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
        ref = str(args.get("ref") or "").strip()
        max_chars = max(100, min(int(args.get("max_chars") or 4000), 20000))
        chat_id = self._chat_id(ctx)
        target_id = str(args.get("target_id") or "").strip() or None
        try:
            tab_id = self._resolve_tab(args, chat_id)
            params: dict[str, Any] = {"mode": "raw", "maxChars": max_chars}
            if ref:
                params["selector"] = ref
            payload, raw = await self._request(
                "GET", self._tab_path(tab_id, "text"), chat_id=chat_id, params=params
            )
            if isinstance(payload, dict):
                extracted = str(payload.get("text") or "")
                url = str(payload.get("url") or "")
                title = str(payload.get("title") or "")
            else:
                extracted, url, title = raw, "", ""
            result: dict[str, Any] = {
                "ok": True,
                "source": "ref" if ref else "page",
                "target_id": tab_id,
                "text": _truncate(extracted.strip(), max_chars),
            }
            if ref:
                result["ref"] = ref
            else:
                result.update({"url": url, "title": title})
            return result
        except Exception as exc:
            return {
                "ok": False,
                "source": "ref" if ref else "page",
                "ref": ref or None,
                "target_id": target_id,
                "error": str(exc),
            }

    async def screenshot(self, args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
        chat_id = self._chat_id(ctx)
        full_page = bool(args.get("full_page", True))
        target_id = str(args.get("target_id") or "").strip() or None
        try:
            tab_id = self._resolve_tab(args, chat_id)
            payload, _ = await self._request(
                "GET",
                self._tab_path(tab_id, "screenshot"),
                chat_id=chat_id,
                params={
                    "output": "inline",
                    "beyondViewport": str(full_page).lower(),
                    "format": "png",
                },
            )
            if not isinstance(payload, dict) or not payload.get("base64"):
                raise PinchTabError("PinchTab returned an invalid screenshot response")
            image = base64.b64decode(str(payload["base64"]), validate=True)
            output_dir = Path(tempfile.gettempdir()) / "octopal-browser"
            output_dir.mkdir(parents=True, exist_ok=True)
            safe_tab = re.sub(r"[^A-Za-z0-9_.-]", "_", tab_id)[:80]
            path = output_dir / f"pinchtab-{chat_id}-{safe_tab}.png"
            path.write_bytes(image)
            return {
                "ok": True,
                "full_page": full_page,
                "target_id": tab_id,
                "path": str(path),
            }
        except Exception as exc:
            return {"ok": False, "target_id": target_id, "error": str(exc)}


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    omitted = len(text) - max_chars
    return text[: max_chars - 32].rstrip() + f"... [truncated {omitted} chars]"


_BACKEND: PinchTabBrowserBackend | None = None
_BACKEND_KEY: tuple[Any, ...] | None = None
_RUNTIME_SETTINGS: Settings | None = None


def configure_pinchtab_backend(settings: Settings | None) -> None:
    """Bind resolved runtime settings without persisting the managed server token."""
    global _BACKEND, _BACKEND_KEY, _RUNTIME_SETTINGS
    _BACKEND = None
    _BACKEND_KEY = None
    _RUNTIME_SETTINGS = settings


def get_pinchtab_backend(settings: Settings | None = None) -> PinchTabBrowserBackend | None:
    global _BACKEND, _BACKEND_KEY
    settings = settings or _RUNTIME_SETTINGS or load_settings()
    if settings.browser_backend.strip().lower() != "pinchtab":
        return None
    ownership_path = str(os.getenv("OCTOPAL_PINCHTAB_OWNERSHIP_FILE") or "").strip()
    key = (
        settings.pinchtab_base_url,
        settings.pinchtab_timeout_seconds,
        settings.pinchtab_browser,
        settings.pinchtab_token,
        settings.pinchtab_session,
        ownership_path,
    )
    if _BACKEND is None or key != _BACKEND_KEY:
        _BACKEND = PinchTabBrowserBackend(
            base_url=settings.pinchtab_base_url,
            timeout_seconds=settings.pinchtab_timeout_seconds,
            browser=settings.pinchtab_browser,
            token=settings.pinchtab_token,
            session=settings.pinchtab_session,
            ownership_file=Path(ownership_path) if ownership_path else None,
        )
        _BACKEND_KEY = key
    return _BACKEND
