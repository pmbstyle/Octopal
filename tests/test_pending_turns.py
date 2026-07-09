from __future__ import annotations

import asyncio

from octopal.runtime.pending_turns import PendingTurnAggregator


def test_pending_turn_aggregator_merges_messages() -> None:
    flushed: list[dict] = []

    async def _flush(chat_id: int, text: str, images: list[str], saved_file_paths: list[str], metadata: dict) -> None:
        flushed.append(
            {
                "chat_id": chat_id,
                "text": text,
                "images": images,
                "saved_file_paths": saved_file_paths,
                "metadata": metadata,
            }
        )

    async def scenario() -> None:
        aggregator = PendingTurnAggregator(grace_seconds=0.02, flush_callback=_flush)
        await aggregator.submit(chat_id=7, text="first", metadata={"reply_to_message_id": 10})
        await aggregator.submit(
            chat_id=7,
            text="second",
            images=["img://1"],
            saved_file_paths=["C:/tmp/one.png"],
            metadata={"reply_to_message_id": 11},
        )
        await asyncio.sleep(0.06)

        assert flushed == [
            {
                "chat_id": 7,
                "text": "first\n\nsecond",
                "images": ["img://1"],
                "saved_file_paths": ["C:/tmp/one.png"],
                "metadata": {"reply_to_message_id": 11},
            }
        ]

        await aggregator.stop()

    asyncio.run(scenario())


def test_pending_turn_aggregator_restarts_timer() -> None:
    flushed: list[str] = []

    async def _flush(chat_id: int, text: str, images: list[str], saved_file_paths: list[str], metadata: dict) -> None:
        del chat_id, images, saved_file_paths, metadata
        flushed.append(text)

    async def scenario() -> None:
        aggregator = PendingTurnAggregator(grace_seconds=0.05, flush_callback=_flush)
        await aggregator.submit(chat_id=9, text="first")
        await asyncio.sleep(0.03)
        await aggregator.submit(chat_id=9, text="second")
        await asyncio.sleep(0.03)
        assert flushed == []

        await asyncio.sleep(0.05)
        assert flushed == ["first\n\nsecond"]

        await aggregator.stop()

    asyncio.run(scenario())


def test_pending_turn_aggregator_flushes_file_only_payloads() -> None:
    flushed: list[dict] = []

    async def _flush(chat_id: int, text: str, images: list[str], saved_file_paths: list[str], metadata: dict) -> None:
        flushed.append(
            {
                "chat_id": chat_id,
                "text": text,
                "images": images,
                "saved_file_paths": saved_file_paths,
                "metadata": metadata,
            }
        )

    async def scenario() -> None:
        aggregator = PendingTurnAggregator(grace_seconds=0.0, flush_callback=_flush)
        await aggregator.submit(
            chat_id=12,
            text="",
            saved_file_paths=["C:/tmp/report.pdf"],
            metadata={"source": "test"},
        )
        assert flushed == [
            {
                "chat_id": 12,
                "text": "",
                "images": [],
                "saved_file_paths": ["C:/tmp/report.pdf"],
                "metadata": {"source": "test"},
            }
        ]
        await aggregator.stop()

    asyncio.run(scenario())


def test_pending_turn_aggregator_keeps_group_senders_separate() -> None:
    flushed: list[tuple[int, str, str]] = []

    async def _flush(
        chat_id: int,
        text: str,
        images: list[str],
        saved_file_paths: list[str],
        metadata: dict,
    ) -> None:
        del images, saved_file_paths
        flushed.append((chat_id, text, metadata["sender_label"]))

    async def scenario() -> None:
        aggregator = PendingTurnAggregator(grace_seconds=0.02, flush_callback=_flush)
        await aggregator.submit(
            chat_id=15,
            sender_id=101,
            text="from Alice",
            metadata={"sender_label": "Alice"},
        )
        await aggregator.submit(
            chat_id=15,
            sender_id=202,
            text="from Bob",
            metadata={"sender_label": "Bob"},
        )
        await asyncio.sleep(0.06)

        assert sorted(flushed) == [
            (15, "from Alice", "Alice"),
            (15, "from Bob", "Bob"),
        ]
        await aggregator.stop()

    asyncio.run(scenario())


def test_pending_turn_aggregator_retries_failed_flush_without_losing_messages() -> None:
    attempts: list[str] = []

    async def _flush(
        chat_id: int,
        text: str,
        images: list[str],
        saved_file_paths: list[str],
        metadata: dict,
    ) -> None:
        del chat_id, images, saved_file_paths, metadata
        attempts.append(text)
        if len(attempts) == 1:
            raise RuntimeError("temporary failure")

    async def scenario() -> None:
        aggregator = PendingTurnAggregator(
            grace_seconds=0.0,
            retry_seconds=0.01,
            flush_callback=_flush,
        )
        await aggregator.submit(chat_id=18, sender_id="sender", text="keep me")
        await asyncio.sleep(0.04)

        assert attempts == ["keep me", "keep me"]
        await aggregator.stop()

    asyncio.run(scenario())


def test_pending_turn_aggregator_merges_new_arrivals_into_failed_retry() -> None:
    first_flush_started = asyncio.Event()
    release_first_flush = asyncio.Event()
    attempts: list[str] = []

    async def _flush(
        chat_id: int,
        text: str,
        images: list[str],
        saved_file_paths: list[str],
        metadata: dict,
    ) -> None:
        del chat_id, images, saved_file_paths, metadata
        attempts.append(text)
        if len(attempts) == 1:
            first_flush_started.set()
            await release_first_flush.wait()
            raise RuntimeError("temporary failure")

    async def scenario() -> None:
        aggregator = PendingTurnAggregator(
            grace_seconds=0.01,
            retry_seconds=0.01,
            flush_callback=_flush,
        )
        await aggregator.submit(chat_id=21, sender_id="sender", text="first")
        await first_flush_started.wait()
        await aggregator.submit(chat_id=21, sender_id="sender", text="second")
        release_first_flush.set()
        await asyncio.sleep(0.04)

        assert attempts == ["first", "first\n\nsecond"]
        await aggregator.stop()

    asyncio.run(scenario())


def test_pending_turn_aggregator_stops_after_bounded_failures_and_notifies() -> None:
    attempts: list[str] = []
    terminal_failures: list[tuple[int, str, str]] = []
    terminal_notified = asyncio.Event()

    async def _flush(
        chat_id: int,
        text: str,
        images: list[str],
        saved_file_paths: list[str],
        metadata: dict,
    ) -> None:
        del chat_id, images, saved_file_paths, metadata
        attempts.append(text)
        if text == "poison turn":
            raise RuntimeError("permanent failure")

    async def _terminal_failure(
        chat_id: int,
        text: str,
        images: list[str],
        saved_file_paths: list[str],
        metadata: dict,
        exc: Exception,
    ) -> None:
        del images, saved_file_paths, metadata
        terminal_failures.append((chat_id, text, str(exc)))
        terminal_notified.set()

    async def scenario() -> None:
        aggregator = PendingTurnAggregator(
            grace_seconds=0.0,
            retry_seconds=0.01,
            max_retry_seconds=0.02,
            max_flush_attempts=3,
            flush_callback=_flush,
            terminal_failure_callback=_terminal_failure,
        )
        await aggregator.submit(chat_id=25, sender_id="sender", text="poison turn")
        await asyncio.wait_for(terminal_notified.wait(), timeout=1.0)
        await asyncio.sleep(0.05)

        assert attempts == ["poison turn", "poison turn", "poison turn"]
        assert terminal_failures == [(25, "poison turn", "permanent failure")]

        await aggregator.submit(chat_id=25, sender_id="sender", text="fresh turn")
        await asyncio.sleep(0.05)
        assert attempts[-1] == "fresh turn"
        assert terminal_failures == [(25, "poison turn", "permanent failure")]
        await aggregator.stop()

    asyncio.run(scenario())
