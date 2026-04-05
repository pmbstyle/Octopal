from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

if "telegramify_markdown" not in sys.modules:
    sys.modules["telegramify_markdown"] = types.SimpleNamespace(markdownify=lambda text: text)

from octopal.channels.telegram import handlers


class _FakeBot:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, str, str | None]] = []

    async def send_photo(self, chat_id: int, photo, caption: str | None = None) -> None:
        self.calls.append(("photo", chat_id, str(Path(photo.path)), caption))

    async def send_animation(self, chat_id: int, animation, caption: str | None = None) -> None:
        self.calls.append(("animation", chat_id, str(Path(animation.path)), caption))

    async def send_video(self, chat_id: int, video, caption: str | None = None) -> None:
        self.calls.append(("video", chat_id, str(Path(video.path)), caption))

    async def send_audio(self, chat_id: int, audio, caption: str | None = None) -> None:
        self.calls.append(("audio", chat_id, str(Path(audio.path)), caption))

    async def send_document(self, chat_id: int, document, caption: str | None = None) -> None:
        self.calls.append(("document", chat_id, str(Path(document.path)), caption))


def test_send_file_safe_uses_photo_for_images(tmp_path: Path) -> None:
    bot = _FakeBot()
    image_path = tmp_path / "preview.png"
    image_path.write_bytes(b"png")

    asyncio.run(handlers._send_file_safe(bot, 42, str(image_path), caption="Look"))

    assert bot.calls == [("photo", 42, str(image_path), "Look")]


def test_send_file_safe_keeps_documents_for_non_images(tmp_path: Path) -> None:
    bot = _FakeBot()
    file_path = tmp_path / "report.txt"
    file_path.write_text("hello", encoding="utf-8")

    asyncio.run(handlers._send_file_safe(bot, 7, str(file_path), caption="Attached"))

    assert bot.calls == [("document", 7, str(file_path), "Attached")]


def test_send_file_safe_uses_animation_for_gif(tmp_path: Path) -> None:
    bot = _FakeBot()
    gif_path = tmp_path / "loop.gif"
    gif_path.write_bytes(b"gif")

    asyncio.run(handlers._send_file_safe(bot, 9, str(gif_path), caption="Loop"))

    assert bot.calls == [("animation", 9, str(gif_path), "Loop")]


def test_send_file_safe_uses_video_for_videos(tmp_path: Path) -> None:
    bot = _FakeBot()
    video_path = tmp_path / "clip.mp4"
    video_path.write_bytes(b"mp4")

    asyncio.run(handlers._send_file_safe(bot, 9, str(video_path), caption="Watch"))

    assert bot.calls == [("video", 9, str(video_path), "Watch")]


def test_send_file_safe_uses_audio_for_audio_files(tmp_path: Path) -> None:
    bot = _FakeBot()
    audio_path = tmp_path / "note.mp3"
    audio_path.write_bytes(b"mp3")

    asyncio.run(handlers._send_file_safe(bot, 9, str(audio_path), caption="Listen"))

    assert bot.calls == [("audio", 9, str(audio_path), "Listen")]
