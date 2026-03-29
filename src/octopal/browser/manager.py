from __future__ import annotations

import asyncio
import tempfile
from contextlib import suppress
from pathlib import Path

import structlog
from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

logger = structlog.get_logger(__name__)


class BrowserManager:
    """Manages Playwright browser instances and contexts for multiple agents/chats."""

    def __init__(self):
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._contexts: dict[int, BrowserContext] = {}
        self._pages: dict[int, dict[str, Page]] = {}
        self._current_targets: dict[int, str] = {}
        self._target_counters: dict[int, int] = {}
        self._lock = asyncio.Lock()

    async def _ensure_browser(self):
        async with self._lock:
            if self._playwright is None:
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(headless=True)
                logger.info("Playwright browser started")

    async def get_page(self, chat_id: int, target_id: str | None = None) -> Page:
        """Get or create an isolated page for a specific chat/agent."""
        await self._ensure_browser()

        async with self._lock:
            if chat_id not in self._contexts:
                context = await self._browser.new_context(
                    viewport={"width": 1280, "height": 720},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                )
                self._contexts[chat_id] = context
                self._pages[chat_id] = {}
                await self._create_page_locked(chat_id, context)
                logger.info("Created new browser context and page", chat_id=chat_id)

            pages = self._pages.setdefault(chat_id, {})
            if not pages:
                await self._create_page_locked(chat_id, self._contexts[chat_id])
                pages = self._pages[chat_id]

            resolved_target = target_id or self._current_targets.get(chat_id)
            if resolved_target and resolved_target in pages:
                self._current_targets[chat_id] = resolved_target
                return pages[resolved_target]

            first_target = next(iter(pages))
            self._current_targets[chat_id] = first_target
            return pages[first_target]

    async def create_page(self, chat_id: int) -> dict[str, str]:
        await self._ensure_browser()
        if chat_id not in self._contexts:
            await self.get_page(chat_id)
        async with self._lock:
            target_id, page = await self._create_page_locked(chat_id, self._contexts[chat_id])
            return {"target_id": target_id, "url": page.url}

    async def list_pages(self, chat_id: int) -> list[dict[str, str | bool | None]]:
        await self.get_page(chat_id)
        async with self._lock:
            pages = dict(self._pages.get(chat_id, {}))
            current_target = self._current_targets.get(chat_id)

        result: list[dict[str, str | bool | None]] = []
        for target_id, page in pages.items():
            title = None
            with suppress(Exception):
                title = await page.title()
            result.append(
                {
                    "target_id": target_id,
                    "url": page.url,
                    "title": title,
                    "is_current": target_id == current_target,
                }
            )
        return result

    async def focus_page(self, chat_id: int, target_id: str) -> Page:
        page = await self.get_page(chat_id, target_id=target_id)
        async with self._lock:
            self._current_targets[chat_id] = target_id
        return page

    async def close_page(self, chat_id: int, target_id: str | None = None) -> str | None:
        async with self._lock:
            pages = self._pages.get(chat_id, {})
            resolved_target = target_id or self._current_targets.get(chat_id)
            if not resolved_target or resolved_target not in pages:
                raise ValueError(f"Unknown browser target '{resolved_target}'.")

            page = pages.pop(resolved_target)
            await page.close()

            if pages:
                self._current_targets[chat_id] = next(iter(pages))
            else:
                self._current_targets.pop(chat_id, None)
            return self._current_targets.get(chat_id)

    async def screenshot_page(
        self, chat_id: int, target_id: str | None = None, full_page: bool = True
    ) -> dict[str, str]:
        page = await self.get_page(chat_id, target_id=target_id)
        resolved_target = target_id or self._current_targets.get(chat_id) or "unknown"
        out_dir = Path(tempfile.gettempdir()) / "octopal-browser"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{chat_id}-{resolved_target}.png"
        await page.screenshot(path=str(path), full_page=full_page)
        return {"target_id": resolved_target, "path": str(path)}

    async def close_chat_session(self, chat_id: int):
        """Close the context and page for a specific chat."""
        async with self._lock:
            pages = self._pages.pop(chat_id, {})
            context = self._contexts.pop(chat_id, None)
            self._current_targets.pop(chat_id, None)
            self._target_counters.pop(chat_id, None)

            for page in pages.values():
                await page.close()
            if context:
                await context.close()

            logger.info("Closed browser session", chat_id=chat_id)

    async def shutdown(self):
        """Shutdown the entire browser manager."""
        async with self._lock:
            for pages in self._pages.values():
                for page in pages.values():
                    await page.close()
            for context in self._contexts.values():
                await context.close()

            self._pages.clear()
            self._contexts.clear()
            self._current_targets.clear()
            self._target_counters.clear()

            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()

            self._browser = None
            self._playwright = None
            logger.info("Browser manager shut down")

    async def _create_page_locked(self, chat_id: int, context: BrowserContext) -> tuple[str, Page]:
        page = await context.new_page()
        counter = self._target_counters.get(chat_id, 0) + 1
        self._target_counters[chat_id] = counter
        target_id = f"t{counter}"
        self._pages.setdefault(chat_id, {})[target_id] = page
        self._current_targets[chat_id] = target_id
        return target_id, page


_manager = BrowserManager()


def get_browser_manager() -> BrowserManager:
    return _manager
