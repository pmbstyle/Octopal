import asyncio
import pytest
from datetime import timedelta

from broodmind.runtime.queen import core as queen_core
from broodmind.runtime.queen.core import (
    Queen,
    _build_worker_result_timeout_followup,
    _enqueue_batched_worker_followup,
    _coerce_control_plane_reply,
    _extract_followup_required_marker,
    _merge_worker_followup_texts,
    _schedule_worker_followup_flush,
)
from broodmind.runtime.queen.router import (
    build_forced_worker_followup,
    should_force_worker_followup,
    should_send_worker_followup,
)
from broodmind.runtime.workers.contracts import WorkerResult
from broodmind.utils import (
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
    queen = Queen(
        approvals=None,
        memory=None,
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )
    correlation_id = "corr-expired"
    queen._pending_conversational_closure_by_correlation[correlation_id] = (
        utc_now() - timedelta(seconds=4000)
    )
    assert queen.has_pending_conversational_closure(correlation_id) is False


def test_suppressed_turn_followups_expire():
    queen = Queen(
        approvals=None,
        memory=None,
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )
    correlation_id = "suppressed-expired"
    queen._suppressed_followups_by_correlation[correlation_id] = (
        utc_now() - timedelta(seconds=4000)
    )
    assert queen.should_suppress_turn_followups(correlation_id) is False


def test_suppressed_turn_followups_can_be_marked_and_cleared():
    queen = Queen(
        approvals=None,
        memory=None,
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )
    correlation_id = "heartbeat-turn"
    queen.suppress_turn_followups(correlation_id)
    assert queen.should_suppress_turn_followups(correlation_id) is True
    queen.clear_suppressed_turn_followups(correlation_id)
    assert queen.should_suppress_turn_followups(correlation_id) is False


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
    monkeypatch.setattr(queen_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 1.0)
    queen_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []
    memory_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    queen = Queen(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
        internal_send=_send,
    )

    await _enqueue_batched_worker_followup(queen, 123, "corr-1", "Первый апдейт.")
    await _enqueue_batched_worker_followup(queen, 123, "corr-1", "Второй апдейт.")
    await queen_core._flush_worker_followup_batch(queen, 123, "corr-1")

    assert sent_messages == [(123, "Первый апдейт.\n\nВторой апдейт.")]
    assert memory_messages == [
        (
            "assistant",
            "Первый апдейт.\n\nВторой апдейт.",
            {"chat_id": 123, "worker_followup": True, "batched_count": 2},
        )
    ]
    assert queen_core._WORKER_FOLLOWUP_BATCHES == {}


@pytest.mark.asyncio
async def test_batched_worker_followups_wait_for_pending_internal_results(monkeypatch):
    monkeypatch.setattr(queen_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 0.01)
    queen_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            return None

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    queen = Queen(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
        internal_send=_send,
    )

    queen.mark_internal_result_pending("corr-queue")
    queen.mark_internal_result_pending("corr-queue")

    await _enqueue_batched_worker_followup(queen, 321, "corr-queue", "Первый апдейт.")
    await asyncio.sleep(0.03)
    assert sent_messages == []

    queen.mark_internal_result_processed("corr-queue")
    _schedule_worker_followup_flush(queen, 321, "corr-queue")
    await _enqueue_batched_worker_followup(queen, 321, "corr-queue", "Второй апдейт.")
    await asyncio.sleep(0.03)
    assert sent_messages == []

    queen.mark_internal_result_processed("corr-queue")
    _schedule_worker_followup_flush(queen, 321, "corr-queue")
    await asyncio.sleep(0.03)

    assert sent_messages == [(321, "Первый апдейт.\n\nВторой апдейт.")]


def test_queen_does_not_have_web_fetch():
    from broodmind.runtime.queen.router import _get_queen_tools
    class DummyQueen:
        store = None

    tool_specs, _ = _get_queen_tools(DummyQueen(), 0)
    tool_names = [spec.name for spec in tool_specs]
    assert "web_fetch" not in tool_names
    # Sanity check: verify some other tools ARE there
    assert "start_worker" in tool_names
    assert "fs_read" in tool_names
