from __future__ import annotations

import asyncio
from pathlib import Path

from octopal.infrastructure.config.settings import Settings
from octopal.runtime.workers.contracts import WorkerResult, WorkerSpec
from octopal.runtime.workers.runtime import (
    WorkerRuntime,
    _classify_recoverable_error,
    _classify_worker_text_log_level,
    _is_process_group_leader,
    _sanitize_task_text,
)


class _StoreStub:
    def __init__(self) -> None:
        self.status_updates: list[str] = []
        self.result_errors: list[str] = []
        self.result_summaries: list[str] = []

    def create_worker(self, record):
        return None

    def update_worker_status(self, _worker_id: str, status: str):
        self.status_updates.append(status)

    def update_worker_result(self, _worker_id: str, **kwargs):
        if kwargs.get("summary"):
            self.result_summaries.append(str(kwargs["summary"]))
        if kwargs.get("error"):
            self.result_errors.append(str(kwargs["error"]))

    def append_audit(self, _event):
        return None


class _PolicyStub:
    def grant_capabilities(self, caps):
        return caps


class _FakeProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.returncode = None
        self.stdin = None
        self.stdout = None
        self.wait_calls = 0
        self.terminate_calls = 0
        self.kill_calls = 0

    def kill(self) -> None:
        self.kill_calls += 1
        self.returncode = -9

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.returncode = -15

    async def wait(self) -> int:
        self.wait_calls += 1
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


class _FakeReader:
    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)

    async def read(self, _n: int = -1) -> bytes:
        if not self._lines:
            return b""
        chunk = b"".join(self._lines)
        self._lines = []
        return chunk

    async def readline(self) -> bytes:
        if self._lines:
            return self._lines.pop(0)
        return b""


class _LauncherStub:
    def __init__(self) -> None:
        self.calls = 0

    async def launch(self, spec_path: str, cwd: str, env: dict[str, str]):
        self.calls += 1
        # Use a clearly fake pid so Linux cleanup paths do not accidentally
        # inspect or signal a real low-numbered system process during tests.
        return _FakeProcess(pid=500_000 + self.calls)


def _spec() -> WorkerSpec:
    return WorkerSpec(
        id="w1",
        task="do task",
        inputs={},
        system_prompt="sys",
        available_tools=[],
        mcp_tools=[],
        model=None,
        granted_capabilities=[],
        timeout_seconds=2,
        max_thinking_steps=5,
        run_id="w1",
        lifecycle="ephemeral",
        correlation_id=None,
    )


def test_recovery_error_classifier() -> None:
    recoverable, reason = _classify_recoverable_error(RuntimeError("Worker exited without result"))
    assert recoverable is True
    assert reason == "exited_without_result"
    recoverable2, reason2 = _classify_recoverable_error(RuntimeError("hard failure"))
    assert recoverable2 is False
    assert reason2 == "non_recoverable"


def test_runtime_recovers_after_transient_failure(tmp_path: Path) -> None:
    store = _StoreStub()
    launcher = _LauncherStub()
    runtime = WorkerRuntime(
        store=store,
        policy=_PolicyStub(),
        workspace_dir=tmp_path,
        launcher=launcher,
        mcp_manager=None,
        settings=Settings(),
    )
    calls = {"count": 0}

    async def fake_read_loop(spec, process, approval_requester=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("Worker stalled without output")
        return WorkerResult(summary="ok", output={})

    runtime._read_loop = fake_read_loop  # type: ignore[method-assign]

    async def scenario():
        return await runtime.run(_spec())

    result = asyncio.run(scenario())
    assert result.summary == "ok"
    assert launcher.calls == 2
    assert isinstance(result.output, dict)
    assert result.output["_recovery"]["recovered"] is True


def test_runtime_fails_after_recovery_exhausted(tmp_path: Path) -> None:
    store = _StoreStub()
    launcher = _LauncherStub()
    runtime = WorkerRuntime(
        store=store,
        policy=_PolicyStub(),
        workspace_dir=tmp_path,
        launcher=launcher,
        mcp_manager=None,
        settings=Settings(),
    )

    async def fake_read_loop(spec, process, approval_requester=None):
        raise RuntimeError("Worker exited without result")

    runtime._read_loop = fake_read_loop  # type: ignore[method-assign]

    async def scenario():
        await runtime.run(_spec())

    try:
        asyncio.run(scenario())
        raise AssertionError("Expected RuntimeError")
    except RuntimeError as exc:
        assert "after recovery attempts" in str(exc)
    assert launcher.calls == 2


def test_worker_mcp_call_restores_configured_session(tmp_path: Path) -> None:
    store = _StoreStub()
    launcher = _LauncherStub()

    class _MCP:
        def __init__(self) -> None:
            self.sessions = {}
            self.ensure_calls: list[list[str]] = []

        async def ensure_configured_servers_connected(self, server_ids=None):
            self.ensure_calls.append(list(server_ids or []))
            self.sessions["demo"] = object()
            return {"demo": "connected"}

        async def call_tool(self, server_id: str, tool_name: str, args: dict, allow_name_fallback: bool = False):
            class _Result:
                content = ["ok"]

            return _Result()

    runtime = WorkerRuntime(
        store=store,
        policy=_PolicyStub(),
        workspace_dir=tmp_path,
        launcher=launcher,
        mcp_manager=_MCP(),
        settings=Settings(),
    )

    writes: list[dict] = []

    async def _fake_write(_process, payload: dict):
        writes.append(payload)

    runtime._write_to_worker = _fake_write  # type: ignore[method-assign]

    process = _FakeProcess(pid=1)
    process.stdout = _FakeReader(
        [
            b'{"type":"mcp_call","server_id":"demo","tool_name":"read_data","arguments":{"q":"x"}}\n',
            b'{"type":"result","result":{"summary":"done","output":{}}}\n',
        ]
    )

    async def scenario():
        return await runtime._read_loop(_spec(), process)

    result = asyncio.run(scenario())
    assert result.summary == "done"
    assert runtime.mcp_manager.ensure_calls == [["demo"]]
    assert writes[0]["type"] == "mcp_result"


def test_worker_failed_result_marks_store_failed(tmp_path: Path) -> None:
    store = _StoreStub()
    launcher = _LauncherStub()
    runtime = WorkerRuntime(
        store=store,
        policy=_PolicyStub(),
        workspace_dir=tmp_path,
        launcher=launcher,
        mcp_manager=None,
        settings=Settings(),
    )

    process = _FakeProcess(pid=1)
    process.stdout = _FakeReader(
        [
            b'{"type":"result","result":{"status":"failed","summary":"Worker failed: MCP schema mismatch","output":{"error":"schema mismatch"}}}\n',
        ]
    )

    async def scenario():
        return await runtime._read_loop(_spec(), process)

    result = asyncio.run(scenario())
    assert result.status == "failed"
    assert store.status_updates == ["failed"]
    assert store.result_summaries == ["Worker failed: MCP schema mismatch"]
    assert store.result_errors == ["schema mismatch"]


def test_stderr_loop_batches_traceback_into_single_log(tmp_path: Path) -> None:
    runtime = WorkerRuntime(
        store=_StoreStub(),
        policy=_PolicyStub(),
        workspace_dir=tmp_path,
        launcher=_LauncherStub(),
        mcp_manager=None,
        settings=Settings(),
    )
    captured: list[tuple[str, str | None, str]] = []

    def _fake_emit(source: str, worker_id: str | None, text: str) -> None:
        captured.append((source, worker_id, text))

    runtime._emit_worker_text_log = _fake_emit  # type: ignore[method-assign]
    stderr = _FakeReader(
        [
            b"Traceback (most recent call last):\n",
            b'  File "worker.py", line 1, in <module>\n',
            b"RuntimeError: boom\n",
        ]
    )

    asyncio.run(runtime._read_stderr_loop("w1", stderr))

    assert captured == [
        (
            "stderr",
            "w1",
            'Traceback (most recent call last):\nFile "worker.py", line 1, in <module>\nRuntimeError: boom',
        )
    ]


def test_worker_text_log_level_downgrades_retry_noise() -> None:
    assert _classify_worker_text_log_level(
        "LiteLLM rate limited (attempt 2/6). Retrying in 2.20s",
        source="stderr",
    ) == "info"
    assert _classify_worker_text_log_level(
        "Traceback (most recent call last):\nRuntimeError: boom",
        source="stderr",
    ) == "error"


def test_sanitize_task_text_redacts_embedded_secrets() -> None:
    task = (
        "Account: AliceGhost API key: moltbook_sk_QTfg76PsXsO5QIvgJIC54xPr "
        "Authorization: Bearer super-secret-token"
    )

    sanitized = _sanitize_task_text(task, limit=500)

    assert "moltbook_sk_" not in sanitized
    assert "super-secret-token" not in sanitized
    assert sanitized.count("[REDACTED_SECRET]") >= 2


def test_cleanup_worker_dir_retries_permission_errors(tmp_path: Path) -> None:
    runtime = WorkerRuntime(
        store=_StoreStub(),
        policy=_PolicyStub(),
        workspace_dir=tmp_path,
        launcher=_LauncherStub(),
        mcp_manager=None,
        settings=Settings(),
    )
    worker_dir = tmp_path / "workers" / "w1"
    worker_dir.mkdir(parents=True)

    calls = {"count": 0}

    def _flaky_rmtree(path: Path) -> None:
        assert path == worker_dir
        calls["count"] += 1
        if calls["count"] < 3:
            raise PermissionError("directory is busy")

    import octopal.runtime.workers.runtime as runtime_mod

    original_rmtree = runtime_mod.shutil.rmtree
    runtime_mod.shutil.rmtree = _flaky_rmtree
    try:
        asyncio.run(runtime._cleanup_worker_dir(worker_dir))
    finally:
        runtime_mod.shutil.rmtree = original_rmtree

    assert calls["count"] == 3


def test_runtime_waits_for_worker_exit_after_result(tmp_path: Path) -> None:
    store = _StoreStub()
    launcher = _LauncherStub()
    runtime = WorkerRuntime(
        store=store,
        policy=_PolicyStub(),
        workspace_dir=tmp_path,
        launcher=launcher,
        mcp_manager=None,
        settings=Settings(),
    )
    process = _FakeProcess(pid=123)

    async def fake_launch(spec_path: str, cwd: str, env: dict[str, str]):
        return process

    launcher.launch = fake_launch  # type: ignore[method-assign]

    async def fake_read_loop(spec, proc, approval_requester=None):
        assert proc is process
        return WorkerResult(summary="ok", output={})

    runtime._read_loop = fake_read_loop  # type: ignore[method-assign]

    result = asyncio.run(runtime.run(_spec()))

    assert result.summary == "ok"
    assert process.wait_calls >= 1


def test_wait_for_worker_exit_terminates_stuck_process(tmp_path: Path) -> None:
    runtime = WorkerRuntime(
        store=_StoreStub(),
        policy=_PolicyStub(),
        workspace_dir=tmp_path,
        launcher=_LauncherStub(),
        mcp_manager=None,
        settings=Settings(),
    )

    class _StuckProcess(_FakeProcess):
        async def wait(self) -> int:
            self.wait_calls += 1
            if self.returncode is not None:
                return self.returncode
            await asyncio.sleep(10)
            return 0

    process = _StuckProcess(pid=456)
    terminated: list[int] = []

    async def _fake_terminate(proc) -> None:
        terminated.append(proc.pid)
        proc.returncode = -9

    runtime._terminate_process_tree = _fake_terminate  # type: ignore[method-assign]

    asyncio.run(runtime._wait_for_worker_exit("w1", process))

    assert terminated == [456]


def test_is_process_group_leader_handles_lookup_failures(monkeypatch) -> None:
    import octopal.runtime.workers.runtime as runtime_mod

    def _raise(_pid: int) -> int:
        raise ProcessLookupError()

    monkeypatch.setattr(runtime_mod.os, "getpgid", _raise, raising=False)

    assert _is_process_group_leader(123) is False


def test_terminate_process_tree_falls_back_to_single_process_when_not_group_leader(tmp_path: Path, monkeypatch) -> None:
    import octopal.runtime.workers.runtime as runtime_mod

    runtime = WorkerRuntime(
        store=_StoreStub(),
        policy=_PolicyStub(),
        workspace_dir=tmp_path,
        launcher=_LauncherStub(),
        mcp_manager=None,
        settings=Settings(),
    )
    process = _FakeProcess(pid=789)
    killpg_calls: list[tuple[int, int]] = []

    monkeypatch.setattr(runtime_mod.os, "name", "posix")
    monkeypatch.setattr(runtime_mod.os, "getpgid", lambda _pid: 999, raising=False)
    monkeypatch.setattr(runtime_mod.os, "killpg", lambda pgid, sig: killpg_calls.append((pgid, sig)), raising=False)

    asyncio.run(runtime._terminate_process_tree(process))

    assert killpg_calls == []
    assert process.terminate_calls == 1
