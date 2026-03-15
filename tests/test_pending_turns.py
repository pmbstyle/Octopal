from __future__ import annotations

import asyncio

from broodmind.runtime.pending_turns import PendingTurnAggregator


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
