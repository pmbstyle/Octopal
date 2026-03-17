from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from broodmind.runtime.queen.core import Queen
from broodmind.runtime.workers.contracts import WorkerResult


def test_queen_output_channel_uses_owner_lease() -> None:
    class _Memory:
        async def add_message(self, role: str, content: str, metadata: dict):
            return None

    queen = Queen(
        provider=object(),
        store=object(),
        policy=object(),
        runtime=object(),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    assert queen.set_output_channel(True, owner_id="ws-a")
    assert not queen.set_output_channel(True, owner_id="ws-b")
    assert not queen.set_output_channel(False, owner_id="ws-b")
    assert queen.set_output_channel(False, owner_id="ws-a")


def test_queen_passes_approval_requester_to_runtime(monkeypatch) -> None:
    class DummyRuntime:
        def __init__(self) -> None:
            self.captured = None

        async def run_task(self, task_request, approval_requester=None):
            self.captured = approval_requester
            return WorkerResult(summary="ok")

    class DummyApprovals:
        bot = None

    class DummyMemory:
        async def add_message(self, role: str, text: str, metadata: dict):
            return None

    async def fake_bootstrap_context(store, chat_id: int):
        from broodmind.runtime.queen.prompt_builder import BootstrapContext

        return BootstrapContext(content="", hash="", files=[])

    async def fake_route_or_reply(
        queen,
        provider,
        memory,
        user_text: str,
        chat_id: int,
        bootstrap_context: str,
        show_typing: bool = True,
        saved_file_paths=None,
    ):
        return "ok"

    import broodmind.runtime.queen.core as queen_core

    monkeypatch.setattr(queen_core, "build_bootstrap_context_prompt", fake_bootstrap_context)
    monkeypatch.setattr(queen_core, "route_or_reply", fake_route_or_reply)

    runtime = DummyRuntime()
    queen = Queen(
        provider=object(),
        store=object(),
        policy=object(),
        runtime=runtime,
        approvals=DummyApprovals(),
        memory=DummyMemory(),
        canon=object(),
    )

    async def requester(intent) -> bool:
        return True

    async def scenario() -> None:
        await queen.handle_message("hello", 123, approval_requester=requester)
        await queen._start_worker_async(
            worker_id="coder",
            task="do thing",
            chat_id=123,
            inputs={},
            tools=None,
            model=None,
            timeout_seconds=5,
        )
        await asyncio.sleep(0.05)
        assert runtime.captured is requester

    asyncio.run(scenario())


def test_recent_task_reservations_are_scoped_by_chat_and_correlation() -> None:
    class _Memory:
        async def add_message(self, role: str, content: str, metadata: dict):
            return None

    queen = Queen(
        provider=object(),
        store=object(),
        policy=object(),
        runtime=object(),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    assert queen._reserve_recent_task(chat_id=1, correlation_id="corr-1", task_signature="sig")
    assert not queen._reserve_recent_task(chat_id=1, correlation_id="corr-1", task_signature="sig")
    assert queen._reserve_recent_task(chat_id=1, correlation_id="corr-2", task_signature="sig")
    assert queen._reserve_recent_task(chat_id=2, correlation_id="corr-1", task_signature="sig")


def test_start_worker_async_releases_duplicate_reservation_after_run(monkeypatch) -> None:
    class _Memory:
        async def add_message(self, role: str, content: str, metadata: dict):
            return None

    class _Store:
        def get_worker(self, worker_id: str):
            return SimpleNamespace(status="completed")

    class _Runtime:
        def __init__(self) -> None:
            self.gate = asyncio.Event()
            self.calls = 0

        async def run_task(self, task_request, approval_requester=None):
            self.calls += 1
            await self.gate.wait()
            return WorkerResult(summary="ok")

    import broodmind.runtime.queen.core as queen_core
    from broodmind.infrastructure.logging import correlation_id_var

    monkeypatch.setattr(queen_core, "_enqueue_internal_result", lambda *args, **kwargs: None)

    runtime = _Runtime()
    queen = Queen(
        provider=object(),
        store=_Store(),
        policy=object(),
        runtime=runtime,
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    async def scenario() -> None:
        token = correlation_id_var.set("corr-1")
        try:
            first = await queen._start_worker_async(
                worker_id="analyst",
                task="check inbox",
                chat_id=1,
                inputs={},
                tools=None,
                model=None,
                timeout_seconds=30,
            )
            duplicate = await queen._start_worker_async(
                worker_id="analyst",
                task="check inbox",
                chat_id=1,
                inputs={},
                tools=None,
                model=None,
                timeout_seconds=30,
            )
            assert first["status"] == "started"
            assert duplicate["status"] == "skipped_duplicate"

            runtime.gate.set()
            await asyncio.sleep(0.05)

            relaunched = await queen._start_worker_async(
                worker_id="analyst",
                task="check inbox",
                chat_id=1,
                inputs={},
                tools=None,
                model=None,
                timeout_seconds=30,
            )
            assert relaunched["status"] == "started"
            await asyncio.sleep(0.05)
            assert runtime.calls == 2
        finally:
            correlation_id_var.reset(token)

    asyncio.run(scenario())


def test_start_worker_async_emits_failed_progress_when_store_marks_failed(monkeypatch) -> None:
    class _Memory:
        async def add_message(self, role: str, content: str, metadata: dict):
            return None

    class _Store:
        def get_worker(self, worker_id: str):
            return SimpleNamespace(status="failed")

    class _Runtime:
        async def run_task(self, task_request, approval_requester=None):
            return WorkerResult(
                status="failed",
                summary="Task failed: remote MCP tool response schema is incompatible.",
                output={"error": "schema mismatch"},
            )

    import broodmind.runtime.queen.core as queen_core

    monkeypatch.setattr(queen_core, "_enqueue_internal_result", lambda *args, **kwargs: None)

    progress_events: list[tuple[str, str, dict]] = []

    async def _progress_sender(chat_id: int, state: str, text: str, meta: dict) -> None:
        progress_events.append((state, text, dict(meta)))

    queen = Queen(
        provider=object(),
        store=_Store(),
        policy=object(),
        runtime=_Runtime(),
        approvals=object(),
        memory=_Memory(),
        canon=object(),
        internal_progress_send=_progress_sender,
    )

    async def scenario() -> None:
        launch = await queen._start_worker_async(
            worker_id="analyst",
            task="check inbox",
            chat_id=1,
            inputs={},
            tools=None,
            model=None,
            timeout_seconds=30,
        )
        assert launch["status"] == "started"
        await asyncio.sleep(0.05)

    asyncio.run(scenario())

    final_state, final_text, final_meta = progress_events[-1]
    assert final_state == "failed"
    assert "failed" in final_text.lower()
    assert final_meta["worker_status"] == "failed"


def test_start_worker_async_infers_longer_timeout_for_context_heavy_network_tasks(monkeypatch) -> None:
    class _Memory:
        async def add_message(self, role: str, content: str, metadata: dict):
            return None

    template = SimpleNamespace(
        id="openbotcity_citizen",
        name="OpenBotCity Citizen",
        available_tools=["openbotcity_dm_read", "openbotcity_dm_reply"],
        required_permissions=["network"],
        default_timeout_seconds=60,
    )

    class _Store:
        def get_worker_template(self, template_id: str):
            return template

        def get_worker(self, worker_id: str):
            return SimpleNamespace(status="completed")

    class _Runtime:
        def __init__(self) -> None:
            self.captured_timeout = None

        async def run_task(self, task_request, approval_requester=None):
            self.captured_timeout = task_request.timeout_seconds
            return WorkerResult(summary="ok")

    import broodmind.runtime.queen.core as queen_core

    monkeypatch.setattr(queen_core, "_enqueue_internal_result", lambda *args, **kwargs: None)

    runtime = _Runtime()
    queen = Queen(
        provider=object(),
        store=_Store(),
        policy=object(),
        runtime=runtime,
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    async def scenario() -> None:
        launch = await queen._start_worker_async(
            worker_id="openbotcity_citizen",
            task=(
                "Read the full DM conversation with Atlas2, review the entire thread history, "
                "and then respond thoughtfully with a careful reply."
            ),
            chat_id=1,
            inputs={},
            tools=None,
            model=None,
            timeout_seconds=None,
        )
        assert launch["status"] == "started"
        await asyncio.sleep(0.05)

    asyncio.run(scenario())

    assert runtime.captured_timeout is not None
    assert runtime.captured_timeout > 60


def test_start_worker_async_keeps_explicit_timeout_override(monkeypatch) -> None:
    class _Memory:
        async def add_message(self, role: str, content: str, metadata: dict):
            return None

    template = SimpleNamespace(
        id="openbotcity_citizen",
        name="OpenBotCity Citizen",
        available_tools=["openbotcity_dm_read", "openbotcity_dm_reply"],
        required_permissions=["network"],
        default_timeout_seconds=60,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )

    class _Store:
        def get_worker_template(self, template_id: str):
            return template

        def get_worker(self, worker_id: str):
            return SimpleNamespace(status="completed")

    class _Runtime:
        def __init__(self) -> None:
            self.captured_timeout = None

        async def run_task(self, task_request, approval_requester=None):
            self.captured_timeout = task_request.timeout_seconds
            return WorkerResult(summary="ok")

    import broodmind.runtime.queen.core as queen_core

    monkeypatch.setattr(queen_core, "_enqueue_internal_result", lambda *args, **kwargs: None)

    runtime = _Runtime()
    queen = Queen(
        provider=object(),
        store=_Store(),
        policy=object(),
        runtime=runtime,
        approvals=object(),
        memory=_Memory(),
        canon=object(),
    )

    async def scenario() -> None:
        launch = await queen._start_worker_async(
            worker_id="openbotcity_citizen",
            task="Read the full DM conversation and reply thoughtfully.",
            chat_id=1,
            inputs={},
            tools=None,
            model=None,
            timeout_seconds=45,
        )
        assert launch["status"] == "started"
        await asyncio.sleep(0.05)

    asyncio.run(scenario())

    assert runtime.captured_timeout == 45
