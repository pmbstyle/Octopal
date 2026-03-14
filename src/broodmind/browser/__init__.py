"""Browser automation helpers used by runtime tools."""

from broodmind.browser.manager import BrowserManager, get_browser_manager
from broodmind.browser.snapshot import SnapshotResult, capture_aria_snapshot

__all__ = [
    "BrowserManager",
    "SnapshotResult",
    "capture_aria_snapshot",
    "get_browser_manager",
]
