import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest

from octopal.runtime.octo import core as octo_core
from octopal.runtime.octo.delivery import DeliveryMode, resolve_user_delivery, resolve_worker_followup_delivery
from octopal.runtime.octo.core import (
    Octo,
    _build_worker_result_timeout_followup,
    _coerce_control_plane_reply,
    _enqueue_batched_worker_followup,
    _extract_followup_required_marker,
    _merge_worker_followup_texts,
    _schedule_worker_followup_flush,
)
from octopal.runtime.octo.router import (
    build_forced_worker_followup,
    should_force_worker_followup,
    should_send_worker_followup,
)
from octopal.runtime.workers.contracts import WorkerResult
from octopal.utils import (
    extract_heartbeat_user_visible_message,
    has_no_user_response_suffix,
    is_heartbeat_ok,
    looks_like_textual_tool_invocation,
    sanitize_user_facing_text,
    should_suppress_user_delivery,
    utc_now,
)


def test_is_heartbeat_ok():
    assert is_heartbeat_ok("HEARTBEAT_OK") is True
    assert is_heartbeat_ok("heartbeat_ok") is True
    assert is_heartbeat_ok("  HEARTBEAT_OK  ") is True
    assert is_heartbeat_ok("HEARTBEAT_OK 😊") is True
    assert is_heartbeat_ok("Status: HEARTBEAT_OK") is True

    # Multiple lines should fail
    assert is_heartbeat_ok("HEARTBEAT_OK\nNext line") is False
    assert is_heartbeat_ok("HEARTBEAT_OK\n") is True  # strip() handles trailing newline
    assert is_heartbeat_ok("Line 1\nHEARTBEAT_OK") is False

    # Missing HEARTBEAT_OK should fail
    assert is_heartbeat_ok("OK") is False
    assert is_heartbeat_ok("") is False
    assert is_heartbeat_ok(None) is False

def test_should_send_worker_followup():
    assert should_send_worker_followup("HEARTBEAT_OK") is False
    assert should_send_worker_followup("HEARTBEAT_OK 😊") is False
    assert should_send_worker_followup("NO_USER_RESPONSE") is False
    assert should_send_worker_followup("Done. NO_USER_RESPONSE") is False
    assert should_send_worker_followup("Done.\n**NO_USER_RESPONSE**") is False
    assert should_send_worker_followup("Done.\nNO USER RESPONSE") is False
    assert should_send_worker_followup("I have finished the task.") is True
    assert should_send_worker_followup("HEARTBEAT_OK\nI did something else too.") is False


def test_resolve_user_delivery_classifies_control_and_visible_text():
    assert resolve_user_delivery("HEARTBEAT_OK").mode == DeliveryMode.SILENT
    visible = resolve_user_delivery("Готово, вот итог.")
    assert visible.mode == DeliveryMode.IMMEDIATE
    assert visible.text == "Готово, вот итог."


def test_extract_heartbeat_user_visible_message_requires_explicit_wrapper():
    assert extract_heartbeat_user_visible_message("<user_visible>Утренний брифинг готов.</user_visible>") == (
        "Утренний брифинг готов."
    )
    assert extract_heartbeat_user_visible_message("Worker still running. Yielding.") is None
    assert extract_heartbeat_user_visible_message(
        "Ладно, canonical memory не подходит для файлов за пределами canon."
    ) is None


def test_resolve_worker_followup_delivery_uses_deferred_mode_when_suppressed():
    decision = resolve_worker_followup_delivery(
        "Подготовила итог по расписанию.",
        result=WorkerResult(summary="Prepared report.", output={"report_path": "research/report.md"}),
        pending_closure=False,
        suppress_followup=True,
        should_force=False,
        forced_text_factory=build_forced_worker_followup,
    )
    assert decision.mode == DeliveryMode.DEFERRED
    assert decision.reason == "suppressed_turn_followup"


def test_resolve_worker_followup_delivery_honors_scheduled_notify_never():
    decision = resolve_worker_followup_delivery(
        "Подготовила сводку.",
        result=WorkerResult(summary="Prepared report.", output={"report_path": "research/report.md"}),
        pending_closure=False,
        suppress_followup=False,
        should_force=True,
        notify_user="never",
        forced_text_factory=build_forced_worker_followup,
    )
    assert decision.mode == DeliveryMode.SILENT
    assert decision.reason == "scheduled_notify_never"


def test_resolve_worker_followup_delivery_honors_scheduled_notify_always():
    decision = resolve_worker_followup_delivery(
        "NO_USER_RESPONSE",
        result=WorkerResult(summary="Prepared report.", output={"report_path": "research/report.md"}),
        pending_closure=False,
        suppress_followup=False,
        should_force=False,
        notify_user="always",
        forced_text_factory=build_forced_worker_followup,
    )
    assert decision.mode == DeliveryMode.IMMEDIATE
    assert decision.reason == "scheduled_notify_always"
    assert "research/report.md" in decision.text


def test_force_worker_followup_for_substantive_results():
    result = WorkerResult(
        summary="Created research/jobs/2026-03-10.md with seven ranked AI/ML roles across Canada and USA, including salary ranges and fit notes for each company.",
        output={"report_path": "research/jobs/2026-03-10.md"},
    )
    assert should_force_worker_followup(result) is True
    text = build_forced_worker_followup(result)
    assert "research/jobs/2026-03-10.md" in text
    assert should_send_worker_followup(text) is True


def test_do_not_force_worker_followup_for_tiny_internal_results():
    result = WorkerResult(summary="Saved canon entry.")
    assert should_force_worker_followup(result) is False


def test_forced_worker_followup_uses_generic_message_when_only_internal_summary_exists():
    result = WorkerResult(
        summary="Successfully sent DM response to Atlas2 in OpenBotCity",
        output={"report_path": "research/candidates.md"},
    )
    assert build_forced_worker_followup(result) == "Task finished. Output is ready in `research/candidates.md`."


def test_forced_worker_followup_drops_empty_generic_completion():
    result = WorkerResult(
        summary="Fetched latest status and saved internal state.",
        output={},
        tools_used=["exec_run", "fs_read"],
    )
    assert should_force_worker_followup(result) is True
    assert build_forced_worker_followup(result) == ""
    assert should_send_worker_followup(build_forced_worker_followup(result)) is False


def test_followup_required_marker_is_stripped_and_detected():
    text, wants_followup = _extract_followup_required_marker(
        "Проверю статус child worker и вернусь с итогом.\nFOLLOWUP_REQUIRED"
    )
    assert wants_followup is True
    assert text == "Проверю статус child worker и вернусь с итогом."


def test_followup_required_marker_is_not_set_for_normal_reply():
    text, wants_followup = _extract_followup_required_marker("Готово, вот итог.")
    assert wants_followup is False
    assert text == "Готово, вот итог."


def test_pending_conversational_closure_expires():
    octo = Octo(
        approvals=None,
        memory=None,
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )
    correlation_id = "corr-expired"
    octo._pending_conversational_closure_by_correlation[correlation_id] = (
        utc_now() - timedelta(seconds=4000)
    )
    assert octo.has_pending_conversational_closure(correlation_id) is False


def test_suppressed_turn_followups_expire():
    octo = Octo(
        approvals=None,
        memory=None,
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )
    correlation_id = "suppressed-expired"
    octo._suppressed_followups_by_correlation[correlation_id] = (
        utc_now() - timedelta(seconds=4000)
    )
    assert octo.should_suppress_turn_followups(correlation_id) is False


def test_suppressed_turn_followups_can_be_marked_and_cleared():
    octo = Octo(
        approvals=None,
        memory=None,
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )
    correlation_id = "heartbeat-turn"
    octo.suppress_turn_followups(correlation_id)
    assert octo.should_suppress_turn_followups(correlation_id) is True
    octo.clear_suppressed_turn_followups(correlation_id)
    assert octo.should_suppress_turn_followups(correlation_id) is False


def test_no_user_response_suffix_detection():
    assert has_no_user_response_suffix("NO_USER_RESPONSE")
    assert has_no_user_response_suffix("All good. NO_USER_RESPONSE")
    assert has_no_user_response_suffix("All good.\nNO USER RESPONSE")
    assert has_no_user_response_suffix("All good no-user-response")
    assert has_no_user_response_suffix("All good.\n**NO_USER_RESPONSE**")
    assert not has_no_user_response_suffix("NO_USER_RESPONSE done")
    assert not has_no_user_response_suffix("Normal reply")


def test_should_suppress_user_delivery():
    assert should_suppress_user_delivery("")
    assert should_suppress_user_delivery("HEARTBEAT_OK")
    assert should_suppress_user_delivery("NO_USER_RESPONSE")
    assert should_suppress_user_delivery("Result ready. NO_USER_RESPONSE")
    assert should_suppress_user_delivery("Result ready.\n**NO_USER_RESPONSE**")
    assert should_suppress_user_delivery("**HEARTBEAT_OK**")
    assert not should_suppress_user_delivery("Result ready.")
    assert not should_suppress_user_delivery("Проверяю расписание:")
    assert not should_suppress_user_delivery("list_workers")
    assert not should_suppress_user_delivery("fs_read, file: memory/2026-03-11.md")
    assert not should_suppress_user_delivery("Successfully sent DM response to Atlas2 in OpenBotCity")


def test_sanitize_user_facing_text_removes_reasoning_and_tool_traces():
    raw = (
        'Tool result (get_worker_result): {"status":"running","worker_id":"abc"}</think>\n'
        "Workers are taking longer than expected.\n"
        "</think>\n"
        "Отлично! Все задачи завершены."
    )
    cleaned = sanitize_user_facing_text(raw)
    assert "Tool result" not in cleaned
    assert "</think>" not in cleaned
    assert "Отлично! Все задачи завершены." in cleaned


def test_sanitize_user_facing_text_keeps_plain_internal_text_unchanged():
    raw = "Successfully sent DM response to Atlas2 in OpenBotCity"
    assert sanitize_user_facing_text(raw) == raw


def test_sanitize_user_facing_text_collapses_result_payload_to_summary():
    raw = """{
  "type": "result",
  "summary": "Completed exploration of Alice's areas of interest.",
  "output": {"date": "2026-02-21"}
}"""
    assert sanitize_user_facing_text(raw) == "Completed exploration of Alice's areas of interest."


def test_sanitize_user_facing_text_suppresses_worker_status_payload():
    raw = """{
  "status": "running",
  "worker_id": "52189039-eb39-4c77-ba0c-8512705b5400",
  "lineage_id": "52189039-eb39-4c77-ba0c-8512705b5400",
  "root_task_id": "52189039-eb39-4c77-ba0c-8512705b5400",
  "message": "Worker is still running. Result not available yet."
}"""
    assert sanitize_user_facing_text(raw) == ""


def test_detect_textual_tool_invocation():
    assert looks_like_textual_tool_invocation("list_workers")
    assert looks_like_textual_tool_invocation("fs_read, file: memory/2026-03-11.md")
    assert not looks_like_textual_tool_invocation("NO_USER_RESPONSE")
    assert not looks_like_textual_tool_invocation("HEARTBEAT_OK")
    assert not looks_like_textual_tool_invocation("Result ready. NO_USER_RESPONSE")
    assert not looks_like_textual_tool_invocation("Проверяю расписание:")
    assert not looks_like_textual_tool_invocation("Checking schedule... check_schedule")


def test_worker_result_timeout_followup_stays_user_visible():
    text = _build_worker_result_timeout_followup(
        WorkerResult(
            summary="Digest is ready.",
            questions=["Do you want the long version?", "Should I save it to canon?"],
        )
    )
    assert "Worker finished, but the follow-up routing step timed out." in text
    assert "Open questions:" in text
    assert should_send_worker_followup(text) is True


def test_control_plane_reply_is_coerced_to_heartbeat_ok_for_non_control_text():
    assert _coerce_control_plane_reply("Checking schedule now...") == "HEARTBEAT_OK"


def test_control_plane_reply_preserves_no_user_response():
    assert _coerce_control_plane_reply("Done. NO_USER_RESPONSE") == "NO_USER_RESPONSE"


def test_control_plane_reply_preserves_existing_control_text():
    assert _coerce_control_plane_reply("HEARTBEAT_OK") == "HEARTBEAT_OK"


def test_merge_worker_followup_texts_deduplicates_and_joins_updates():
    assert _merge_worker_followup_texts(
        [
            "Подготовила короткий итог.",
            "Подготовила короткий итог.",
            "NO_USER_RESPONSE",
            "Сохранила отчёт в `research/report.md`.",
        ]
    ) == "Подготовила короткий итог.\n\nСохранила отчёт в `research/report.md`."


@pytest.mark.asyncio
async def test_batched_worker_followups_send_single_combined_message(monkeypatch):
    monkeypatch.setattr(octo_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 1.0)
    octo_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []
    memory_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
        internal_send=_send,
    )

    await _enqueue_batched_worker_followup(octo, 123, "corr-1", "Первый апдейт.")
    await _enqueue_batched_worker_followup(octo, 123, "corr-1", "Второй апдейт.")
    await octo_core._flush_worker_followup_batch(octo, 123, "corr-1")

    assert sent_messages == [(123, "Первый апдейт.\n\nВторой апдейт.")]
    assert memory_messages == [
        (
            "assistant",
            "Первый апдейт.\n\nВторой апдейт.",
            {"chat_id": 123, "worker_followup": True, "batched_count": 2},
        )
    ]
    assert octo_core._WORKER_FOLLOWUP_BATCHES == {}


@pytest.mark.asyncio
async def test_suppressed_worker_followup_is_deferred_until_internal_turn_finishes(monkeypatch):
    monkeypatch.setattr(octo_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 0.01)
    monkeypatch.setattr(octo_core, "_QUEUE_IDLE_TIMEOUT_SECONDS", 0.01)
    octo_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []
    memory_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    async def _route_worker_result_back_to_octo(_octo, _chat_id, _task_text, _result):
        return "Подготовила итог по расписанию."

    monkeypatch.setattr(
        octo_core,
        "route_worker_result_back_to_octo",
        _route_worker_result_back_to_octo,
    )

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
        internal_send=_send,
    )
    correlation_id = "heartbeat-turn"
    octo.suppress_turn_followups(correlation_id)
    octo.mark_internal_result_pending(correlation_id)

    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait(
        (
            "[Scheduled] Prepare summary",
            WorkerResult(summary="Built the scheduled summary.", output={"report_path": "research/digest.md"}),
            correlation_id,
        )
    )

    await octo_core._internal_worker(octo, 123, queue)
    await asyncio.sleep(0.03)

    assert sent_messages == [(123, "Подготовила итог по расписанию.")]
    assert octo.should_suppress_turn_followups(correlation_id) is False
    assert any(
        role == "assistant" and text == "Подготовила итог по расписанию."
        for role, text, _metadata in memory_messages
    )


@pytest.mark.asyncio
async def test_background_delivery_keeps_user_visible_heartbeat_reply_and_records_memory(monkeypatch):
    memory_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    async def _route_or_reply(*args, **kwargs):
        return "<user_visible>Утренний брифинг готов.</user_visible>"

    async def _bootstrap_context(*args, **kwargs):
        return SimpleNamespace(content="", hash="", files=[])

    monkeypatch.setattr(octo_core, "route_or_reply", _route_or_reply)
    monkeypatch.setattr(octo_core, "build_bootstrap_context_prompt", _bootstrap_context)

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )

    reply = await octo.handle_message(
        "heartbeat task",
        123,
        persist_to_memory=False,
        track_progress=False,
        include_wakeup=False,
        background_delivery=True,
    )

    assert reply.delivery_mode == DeliveryMode.IMMEDIATE
    assert reply.immediate == "Утренний брифинг готов."
    assert any(
        role == "assistant"
        and text == "Утренний брифинг готов."
        and metadata.get("background_delivery") is True
        for role, text, metadata in memory_messages
    )


@pytest.mark.asyncio
async def test_background_delivery_suppresses_low_signal_heartbeat_update(monkeypatch):
    memory_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    async def _route_or_reply(*args, **kwargs):
        return "Worker still running. Yielding."

    async def _bootstrap_context(*args, **kwargs):
        return SimpleNamespace(content="", hash="", files=[])

    monkeypatch.setattr(octo_core, "route_or_reply", _route_or_reply)
    monkeypatch.setattr(octo_core, "build_bootstrap_context_prompt", _bootstrap_context)

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )

    reply = await octo.handle_message(
        "heartbeat task",
        123,
        persist_to_memory=False,
        track_progress=False,
        include_wakeup=False,
        background_delivery=True,
    )

    assert reply.delivery_mode == DeliveryMode.SILENT
    assert reply.immediate == "HEARTBEAT_OK"
    assert not any(role == "assistant" for role, _text, _metadata in memory_messages)


@pytest.mark.asyncio
async def test_background_delivery_suppresses_unwrapped_internal_heartbeat_text(monkeypatch):
    memory_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    async def _route_or_reply(*args, **kwargs):
        return (
            "Ладно, canonical memory не подходит для файлов за пределами canon. "
            "Мне нужно использовать tools для записи в research/."
        )

    async def _bootstrap_context(*args, **kwargs):
        return SimpleNamespace(content="", hash="", files=[])

    monkeypatch.setattr(octo_core, "route_or_reply", _route_or_reply)
    monkeypatch.setattr(octo_core, "build_bootstrap_context_prompt", _bootstrap_context)

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )

    reply = await octo.handle_message(
        "heartbeat task",
        123,
        persist_to_memory=False,
        track_progress=False,
        include_wakeup=False,
        background_delivery=True,
    )

    assert reply.delivery_mode == DeliveryMode.SILENT
    assert reply.immediate == "HEARTBEAT_OK"
    assert not any(role == "assistant" for role, _text, _metadata in memory_messages)


@pytest.mark.asyncio
async def test_batched_worker_followups_wait_for_pending_internal_results(monkeypatch):
    monkeypatch.setattr(octo_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 0.01)
    octo_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            return None

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
        internal_send=_send,
    )

    octo.mark_internal_result_pending("corr-queue")
    octo.mark_internal_result_pending("corr-queue")

    await _enqueue_batched_worker_followup(octo, 321, "corr-queue", "Первый апдейт.")
    await asyncio.sleep(0.03)
    assert sent_messages == []

    octo.mark_internal_result_processed("corr-queue")
    _schedule_worker_followup_flush(octo, 321, "corr-queue")
    await _enqueue_batched_worker_followup(octo, 321, "corr-queue", "Второй апдейт.")
    await asyncio.sleep(0.03)
    assert sent_messages == []

    octo.mark_internal_result_processed("corr-queue")
    _schedule_worker_followup_flush(octo, 321, "corr-queue")
    await asyncio.sleep(0.03)

    assert sent_messages == [(321, "Первый апдейт.\n\nВторой апдейт.")]


@pytest.mark.asyncio
async def test_worker_followup_created_during_active_turn_is_dropped_without_followup_marker(monkeypatch):
    monkeypatch.setattr(octo_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 0.01)
    octo_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            return None

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    async def _route_or_reply(*args, **kwargs):
        await asyncio.sleep(0.02)
        return "Готово."

    async def _bootstrap_context(*args, **kwargs):
        return SimpleNamespace(content="", hash="", files=[])

    monkeypatch.setattr(octo_core, "route_or_reply", _route_or_reply)
    monkeypatch.setattr(octo_core, "build_bootstrap_context_prompt", _bootstrap_context)

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
        internal_send=_send,
    )

    token = octo_core.correlation_id_var.set("turn-test")
    try:
        task = asyncio.create_task(octo.handle_message("test", 123))
        await asyncio.sleep(0.005)
        await octo_core._enqueue_batched_worker_followup(octo, 123, "turn-test", "Фоновый итог.")
        await task
    finally:
        octo_core.correlation_id_var.reset(token)
    await asyncio.sleep(0.03)

    assert sent_messages == []
    assert octo_core._WORKER_FOLLOWUP_BATCHES == {}


@pytest.mark.asyncio
async def test_worker_followup_created_during_active_turn_flushes_after_followup_marker(monkeypatch):
    monkeypatch.setattr(octo_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 0.01)
    octo_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            return None

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    async def _route_or_reply(*args, **kwargs):
        await asyncio.sleep(0.02)
        return "Жду результат воркера.\nFOLLOWUP_REQUIRED"

    async def _bootstrap_context(*args, **kwargs):
        return SimpleNamespace(content="", hash="", files=[])

    monkeypatch.setattr(octo_core, "route_or_reply", _route_or_reply)
    monkeypatch.setattr(octo_core, "build_bootstrap_context_prompt", _bootstrap_context)

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
        internal_send=_send,
    )
    octo.mark_internal_result_pending("turn-test")

    token = octo_core.correlation_id_var.set("turn-test")
    try:
        task = asyncio.create_task(octo.handle_message("test", 123))
        await asyncio.sleep(0.005)
        await octo_core._enqueue_batched_worker_followup(octo, 123, "turn-test", "Фоновый итог.")
        await task
    finally:
        octo_core.correlation_id_var.reset(token)
    octo.mark_internal_result_processed("turn-test")
    octo_core._schedule_worker_followup_flush(octo, 123, "turn-test")
    await asyncio.sleep(0.03)

    assert sent_messages == [(123, "Фоновый итог.")]


@pytest.mark.asyncio
async def test_worker_followup_created_during_active_turn_is_dropped_even_with_followup_marker_when_work_already_finished(monkeypatch):
    monkeypatch.setattr(octo_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 0.01)
    octo_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            return None

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    async def _route_or_reply(*args, **kwargs):
        await asyncio.sleep(0.02)
        return "Жду результат воркера.\nFOLLOWUP_REQUIRED"

    async def _bootstrap_context(*args, **kwargs):
        return SimpleNamespace(content="", hash="", files=[])

    monkeypatch.setattr(octo_core, "route_or_reply", _route_or_reply)
    monkeypatch.setattr(octo_core, "build_bootstrap_context_prompt", _bootstrap_context)

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
        internal_send=_send,
    )

    token = octo_core.correlation_id_var.set("turn-test")
    try:
        task = asyncio.create_task(octo.handle_message("test", 123))
        await asyncio.sleep(0.005)
        await octo_core._enqueue_batched_worker_followup(octo, 123, "turn-test", "Фоновый итог.")
        await task
    finally:
        octo_core.correlation_id_var.reset(token)
    await asyncio.sleep(0.03)

    assert sent_messages == []
    assert octo_core._WORKER_FOLLOWUP_BATCHES == {}


def test_octo_does_not_have_web_fetch():
    from octopal.runtime.octo.router import _get_octo_tools
    class DummyOcto:
        store = None

    tool_specs, _ = _get_octo_tools(DummyOcto(), 0)
    tool_names = [spec.name for spec in tool_specs]
    assert "web_fetch" not in tool_names
    # Sanity check: verify some other tools ARE there
    assert "start_worker" in tool_names
    assert "fs_read" in tool_names


def test_octo_keeps_filesystem_tools_when_research_profile_enabled(monkeypatch):
    from octopal.runtime.octo.router import _get_octo_tools

    class DummyOcto:
        store = None

    monkeypatch.setenv("OCTOPAL_OCTO_TOOL_PROFILE", "research")
    tool_specs, _ = _get_octo_tools(DummyOcto(), 0)
    tool_names = {spec.name for spec in tool_specs}

    assert {"fs_list", "fs_read", "fs_write", "fs_move", "fs_delete"} <= tool_names
