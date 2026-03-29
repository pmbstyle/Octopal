from __future__ import annotations

from typing import Any

from playwright.async_api import Locator, Page

from octopal.browser.manager import get_browser_manager
from octopal.browser.snapshot import capture_aria_snapshot

_SESSION_REFS: dict[int, dict[str, dict[str, Any]]] = {}


def _get_chat_id(ctx: dict[str, Any]) -> int:
    return int(ctx.get("chat_id") or 0)


async def _get_locator(page: Page, ref: str, chat_id: int, target_id: str) -> Locator:
    refs = _SESSION_REFS.get(chat_id, {}).get(target_id, {})
    if ref not in refs:
        raise ValueError(f"Unknown reference '{ref}'. Run browser_snapshot first.")

    info = refs[ref]
    role = info["role"]
    name = info.get("name")
    nth = info.get("nth", 0)

    locator = page.get_by_role(role, name=name, exact=True) if name else page.get_by_role(role)
    return locator.nth(nth)


async def _get_page_and_target(args: dict[str, Any], ctx: dict[str, Any]) -> tuple[Page, str]:
    chat_id = _get_chat_id(ctx)
    target_id = str(args.get("target_id") or "").strip() or None
    manager = get_browser_manager()
    page = await manager.get_page(chat_id, target_id=target_id)
    if target_id:
        return page, target_id

    pages = await manager.list_pages(chat_id)
    current = next((row for row in pages if row.get("is_current")), None)
    resolved_target = str((current or {}).get("target_id") or "")
    return page, resolved_target


async def browser_open(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    url = args.get("url")
    if not url:
        return "Error: url is required"

    chat_id = _get_chat_id(ctx)
    manager = get_browser_manager()
    target_id = str(args.get("target_id") or "").strip() or None
    if bool(args.get("new_tab", False)):
        opened = await manager.create_page(chat_id)
        target_id = opened["target_id"]
    page = await manager.get_page(chat_id, target_id=target_id)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        return f"Successfully opened {url}"
    except Exception as e:
        return f"Error opening {url}: {e}"


async def browser_tabs(args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    del args
    chat_id = _get_chat_id(ctx)
    pages = await get_browser_manager().list_pages(chat_id)
    return {"ok": True, "count": len(pages), "pages": pages}


async def browser_focus_tab(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    chat_id = _get_chat_id(ctx)
    target_id = str(args.get("target_id") or "").strip()
    if not target_id:
        return "Error: target_id is required"
    try:
        await get_browser_manager().focus_page(chat_id, target_id)
        return f"Focused {target_id}"
    except Exception as e:
        return f"Error focusing {target_id}: {e}"


async def browser_navigate(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    url = str(args.get("url") or "").strip()
    if not url:
        return "Error: url is required"
    page, _target_id = await _get_page_and_target(args, ctx)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        return f"Navigated to {url}"
    except Exception as e:
        return f"Error navigating to {url}: {e}"


async def browser_snapshot(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    chat_id = _get_chat_id(ctx)
    page, target_id = await _get_page_and_target(args, ctx)

    try:
        result = await capture_aria_snapshot(page)
        _SESSION_REFS.setdefault(chat_id, {})[target_id] = result["refs"]
        return result["snapshot"]
    except Exception as e:
        return f"Error taking snapshot: {e}"


async def browser_click(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    ref = args.get("ref")
    if not ref:
        return "Error: ref is required"

    chat_id = _get_chat_id(ctx)
    page, target_id = await _get_page_and_target(args, ctx)

    try:
        locator = await _get_locator(page, ref, chat_id, target_id)
        await locator.click(timeout=5000)
        return f"Clicked {ref}"
    except Exception as e:
        return f"Error clicking {ref}: {e}"


async def browser_type(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    ref = args.get("ref")
    text = args.get("text")
    press_enter = args.get("press_enter", False)

    if not ref or text is None:
        return "Error: ref and text are required"

    chat_id = _get_chat_id(ctx)
    page, target_id = await _get_page_and_target(args, ctx)

    try:
        locator = await _get_locator(page, ref, chat_id, target_id)
        await locator.fill(text, timeout=5000)
        if press_enter:
            await locator.press("Enter")
        return f"Typed into {ref}"
    except Exception as e:
        return f"Error typing into {ref}: {e}"


async def browser_close(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    chat_id = _get_chat_id(ctx)
    target_id = str(args.get("target_id") or "").strip()
    if target_id:
        try:
            await get_browser_manager().close_page(chat_id, target_id=target_id)
            _SESSION_REFS.get(chat_id, {}).pop(target_id, None)
            return f"Closed browser target {target_id}"
        except Exception as e:
            return f"Error closing {target_id}: {e}"

    await get_browser_manager().close_chat_session(chat_id)
    _SESSION_REFS.pop(chat_id, None)
    return "Browser session closed"


async def browser_wait_for(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    ref = str(args.get("ref") or "").strip()
    text = str(args.get("text") or "").strip()
    state = str(args.get("state") or "visible").strip() or "visible"
    timeout_ms = int(args.get("timeout_ms") or 10000)

    if not ref and not text:
        return "Error: ref or text is required"

    chat_id = _get_chat_id(ctx)
    page, target_id = await _get_page_and_target(args, ctx)

    try:
        if ref:
            locator = await _get_locator(page, ref, chat_id, target_id)
            await locator.wait_for(state=state, timeout=timeout_ms)
            return f"Element {ref} is now {state}"

        locator = page.get_by_text(text, exact=False).first
        await locator.wait_for(state=state, timeout=timeout_ms)
        return f"Text appeared: {text}"
    except Exception as e:
        target = ref or text
        return f"Error waiting for {target}: {e}"


async def browser_extract(args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    ref = str(args.get("ref") or "").strip()
    max_chars = max(100, min(int(args.get("max_chars") or 4000), 20000))
    chat_id = _get_chat_id(ctx)
    page, target_id = await _get_page_and_target(args, ctx)

    try:
        if ref:
            locator = await _get_locator(page, ref, chat_id, target_id)
            text = (await locator.inner_text(timeout=5000)).strip()
            return {
                "ok": True,
                "source": "ref",
                "ref": ref,
                "target_id": target_id,
                "text": _truncate_text(text, max_chars=max_chars),
            }

        title = await page.title()
        body = await page.locator("body").inner_text(timeout=5000)
        return {
            "ok": True,
            "source": "page",
            "url": getattr(page, "url", ""),
            "title": title,
            "target_id": target_id,
            "text": _truncate_text(body.strip(), max_chars=max_chars),
        }
    except Exception as e:
        return {
            "ok": False,
            "source": "ref" if ref else "page",
            "ref": ref or None,
            "target_id": target_id,
            "error": str(e),
        }


async def browser_screenshot(args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    chat_id = _get_chat_id(ctx)
    target_id = str(args.get("target_id") or "").strip() or None
    full_page = bool(args.get("full_page", True))
    try:
        result = await get_browser_manager().screenshot_page(
            chat_id, target_id=target_id, full_page=full_page
        )
        return {"ok": True, "full_page": full_page, **result}
    except Exception as e:
        return {"ok": False, "target_id": target_id, "error": str(e)}


async def browser_workflow(args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    raw_steps = args.get("steps")
    stop_on_error = bool(args.get("stop_on_error", True))
    if not isinstance(raw_steps, list) or not raw_steps:
        return {"ok": False, "error": "steps is required and must be a non-empty array"}

    results: list[dict[str, Any]] = []
    for index, raw_step in enumerate(raw_steps, start=1):
        if not isinstance(raw_step, dict):
            results.append({"index": index, "ok": False, "error": "Each workflow step must be an object."})
            if stop_on_error:
                break
            continue

        action = str(raw_step.get("action") or "").strip().lower()
        step_args = {key: value for key, value in raw_step.items() if key != "action"}
        handler = _WORKFLOW_ACTIONS.get(action)
        if handler is None:
            results.append(
                {
                    "index": index,
                    "action": action,
                    "ok": False,
                    "error": f"Unsupported browser workflow action: {action}",
                }
            )
            if stop_on_error:
                break
            continue

        outcome = await handler(step_args, ctx)
        normalized = _normalize_workflow_outcome(index=index, action=action, outcome=outcome)
        results.append(normalized)
        if stop_on_error and not normalized["ok"]:
            break

    return {
        "ok": all(step["ok"] for step in results) if results else False,
        "step_count": len(results),
        "stop_on_error": stop_on_error,
        "steps": results,
    }


def _truncate_text(text: str, *, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    omitted = len(text) - max_chars
    return text[: max_chars - 32].rstrip() + f"... [truncated {omitted} chars]"


def _normalize_workflow_outcome(*, index: int, action: str, outcome: Any) -> dict[str, Any]:
    if isinstance(outcome, dict):
        payload = dict(outcome)
        payload.setdefault("ok", bool(payload.get("ok", True)))
        payload["index"] = index
        payload["action"] = action
        return payload

    text = str(outcome or "")
    ok = not text.lower().startswith("error")
    return {"index": index, "action": action, "ok": ok, "message": text}


_WORKFLOW_ACTIONS = {
    "open": browser_open,
    "tabs": browser_tabs,
    "focus_tab": browser_focus_tab,
    "navigate": browser_navigate,
    "snapshot": browser_snapshot,
    "screenshot": browser_screenshot,
    "click": browser_click,
    "type": browser_type,
    "wait_for": browser_wait_for,
    "extract": browser_extract,
    "close": browser_close,
}
