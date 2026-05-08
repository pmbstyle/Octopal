import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest

from octopal.runtime.octo import core as octo_core
from octopal.runtime.octo import router as octo_router
from octopal.runtime.octo.control_plane import RouteMode, RouteRequest, resolve_turn_route_mode
from octopal.runtime.octo.core import (
    Octo,
    _build_forced_worker_followup_batch,
    _build_worker_result_timeout_followup,
    _coerce_control_plane_reply,
    _enqueue_batched_worker_followup,
    _merge_worker_followup_texts,
    _schedule_worker_followup_flush,
)
from octopal.runtime.octo.delivery import (
    DeliveryMode,
    resolve_user_delivery,
    resolve_worker_followup_delivery,
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


def test_resolve_turn_route_mode():
    assert resolve_turn_route_mode(track_progress=True, background_delivery=False) is RouteMode.CONVERSATION
    assert resolve_turn_route_mode(track_progress=False, background_delivery=True) is RouteMode.HEARTBEAT
    assert (
        resolve_turn_route_mode(track_progress=False, background_delivery=False)
        is RouteMode.INTERNAL_MAINTENANCE
    )


def test_route_request_marks_control_plane_modes():
    conversation = RouteRequest(mode=RouteMode.CONVERSATION, user_text="hi", chat_id=1)
    heartbeat = RouteRequest(mode=RouteMode.HEARTBEAT, user_text="tick", chat_id=1)

    assert conversation.is_control_plane is False
    assert heartbeat.is_control_plane is True


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


def test_resolve_user_delivery_extracts_user_visible_block_from_noisy_reply():
    raw = (
        "The worker result has limited output keys and the briefing text isn't directly accessible.\n\n"
        "<user_visible>\n"
        "Утренний брифинг готов.\n"
        "</user_visible>"
    )
    visible = resolve_user_delivery(raw)
    assert visible.mode == DeliveryMode.IMMEDIATE
    assert visible.text == "Утренний брифинг готов."


def test_resolve_user_delivery_ignores_user_visible_inside_hidden_blocks():
    raw = (
        "<think><user_visible>internal note</user_visible></think>\n"
        "<tool_result><user_visible>tool payload</user_visible></tool_result>\n"
        "<user_visible>Показать это.</user_visible>"
    )
    visible = resolve_user_delivery(raw)
    assert visible.mode == DeliveryMode.IMMEDIATE
    assert visible.text == "Показать это."


def test_extract_heartbeat_user_visible_message_ignores_hidden_wrapper():
    raw = "<think><user_visible>internal note</user_visible></think>"
    assert extract_heartbeat_user_visible_message(raw) is None


def test_extract_heartbeat_user_visible_message_requires_explicit_wrapper():
    assert extract_heartbeat_user_visible_message("<user_visible>Утренний брифинг готов.</user_visible>") == (
        "Утренний брифинг готов."
    )
    assert extract_heartbeat_user_visible_message(
        "Internal notes.\n<user_visible>Показать только это.</user_visible>"
    ) == "Показать только это."
    assert extract_heartbeat_user_visible_message("Worker still running. Yielding.") is None
    assert extract_heartbeat_user_visible_message(
        "Ладно, canonical memory не подходит для файлов за пределами canon."
    ) is None


def test_resolve_worker_followup_delivery_uses_deferred_mode_when_suppressed():
    decision = resolve_worker_followup_delivery(
        "Подготовила итог по расписанию.",
        result=WorkerResult(
            summary="Prepared report.",
            output={"report_path": "reports/report.md", "durable_paths": ["reports/report.md"]},
        ),
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
        result=WorkerResult(
            summary="Prepared report.",
            output={"report_path": "reports/report.md", "durable_paths": ["reports/report.md"]},
        ),
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
        result=WorkerResult(
            summary="Prepared report.",
            output={"report_path": "reports/report.md", "durable_paths": ["reports/report.md"]},
        ),
        pending_closure=False,
        suppress_followup=False,
        should_force=False,
        notify_user="always",
        forced_text_factory=build_forced_worker_followup,
    )
    assert decision.mode == DeliveryMode.IMMEDIATE
    assert decision.reason == "scheduled_notify_always"
    assert "reports/report.md" in decision.text


def test_force_worker_followup_for_substantive_results():
    result = WorkerResult(
        summary="Created research/jobs/2026-03-10.md with seven ranked AI/ML roles across Canada and USA, including salary ranges and fit notes for each company.",
        output={
            "report_path": "reports/jobs/2026-03-10.md",
            "durable_paths": ["reports/jobs/2026-03-10.md"],
        },
    )
    assert should_force_worker_followup(result) is True
    text = build_forced_worker_followup(result)
    assert "reports/jobs/2026-03-10.md" in text
    assert should_send_worker_followup(text) is True


def test_do_not_force_worker_followup_for_tiny_internal_results():
    result = WorkerResult(summary="Saved canon entry.")
    assert should_force_worker_followup(result) is False


def test_do_not_force_worker_followup_for_non_durable_legacy_path() -> None:
    result = WorkerResult(
        summary="Prepared local notes.",
        output={"report_path": "research/candidates.md"},
    )
    assert should_force_worker_followup(result) is False
    assert build_forced_worker_followup(result) == ""


def test_forced_worker_followup_uses_generic_message_when_only_internal_summary_exists():
    result = WorkerResult(
        summary="Successfully sent DM response to Atlas2 in OpenBotCity",
        output={"report_path": "research/candidates.md"},
    )
    assert build_forced_worker_followup(result) == ""


def test_forced_worker_followup_uses_durable_report_path_only() -> None:
    result = WorkerResult(
        summary="Successfully prepared the final candidate packet.",
        output={
            "report_path": "reports/candidates.md",
            "durable_paths": ["reports/candidates.md"],
        },
    )
    assert build_forced_worker_followup(result) == "Task finished. Output is ready in `reports/candidates.md`."


def test_forced_worker_followup_drops_empty_generic_completion():
    result = WorkerResult(
        summary="Fetched latest status and saved internal state.",
        output={},
        tools_used=["exec_run", "fs_read"],
    )
    assert should_force_worker_followup(result) is True
    assert build_forced_worker_followup(result) == ""
    assert should_send_worker_followup(build_forced_worker_followup(result)) is False


def test_forced_worker_followup_batch_uses_meaningful_result_summary():
    items = [
        octo_core._PendingWorkerFollowupItem(
            worker_id="",
            task_text="Task A",
            result=WorkerResult(
                summary="Created research/jobs/2026-03-10.md with seven ranked AI/ML roles.",
                output={
                    "report_path": "reports/jobs/2026-03-10.md",
                    "durable_paths": ["reports/jobs/2026-03-10.md"],
                },
            ),
        ),
        octo_core._PendingWorkerFollowupItem(
            worker_id="",
            task_text="Task B",
            result=WorkerResult(
                summary="Prepared a concise Moltbook activity report with two interesting discoveries.",
                output={"status": "completed"},
            ),
        ),
    ]

    text = _build_forced_worker_followup_batch(items)

    assert text.startswith("Completed 2 worker tasks:")
    assert "reports/jobs/2026-03-10.md" in text
    assert "Moltbook activity report" in text
    assert "The results are ready." not in text


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


def test_pending_conversational_closure_round_trip_works_with_empty_store():
    octo = Octo(
        approvals=None,
        memory=None,
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )
    correlation_id = "corr-round-trip"

    octo._pending_conversational_closure_by_correlation = {}

    octo.mark_pending_conversational_closure(correlation_id)

    assert octo.has_pending_conversational_closure(correlation_id) is True

    octo.clear_pending_conversational_closure(correlation_id)

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
    assert not looks_like_textual_tool_invocation("SCHEDULED_TASK_DONE")
    assert not looks_like_textual_tool_invocation("SCHEDULED_TASK_BLOCKED")
    assert not looks_like_textual_tool_invocation("SCHEDULER_IDLE")
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


def test_merge_worker_followup_texts_drops_shorter_overlapping_summary():
    assert _merge_worker_followup_texts(
        [
            (
                "Окей, оба исследования вернулись. Рассказываю!\n\n"
                "Kapoor & Narayanan показали, что consistency у моделей плавает, "
                "а Moltbook после покупки Meta заметно затих."
            ),
            (
                "Окей, покопалась. Вот что нашла.\n\n"
                "Kapoor & Narayanan показали, что consistency у моделей плавает, "
                "а Moltbook после покупки Meta заметно затих. "
                "Главный вывод: reliability растёт медленнее accuracy, "
                "а по Moltbook массовой миграции пока не видно."
            ),
        ]
    ) == (
        "Окей, покопалась. Вот что нашла.\n\n"
        "Kapoor & Narayanan показали, что consistency у моделей плавает, "
        "а Moltbook после покупки Meta заметно затих. "
        "Главный вывод: reliability растёт медленнее accuracy, "
        "а по Moltbook массовой миграции пока не видно."
    )


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

    async def _route_worker_results_back_to_octo(_octo, _chat_id, worker_results):
        assert len(worker_results) == 1
        assert len(worker_results[0]) == 3
        return "Подготовила итог по расписанию."

    monkeypatch.setattr(
        octo_core,
        "route_worker_results_back_to_octo",
        _route_worker_results_back_to_octo,
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
async def test_structured_worker_followups_route_once_per_batch(monkeypatch):
    monkeypatch.setattr(octo_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 0.01)
    octo_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []
    memory_messages = []
    routed_batches = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    async def _route_worker_results_back_to_octo(_octo, _chat_id, worker_results):
        routed_batches.append([(worker_id, task_text, result.summary) for worker_id, task_text, result in worker_results])
        return "Объединила оба результата в один ответ."

    monkeypatch.setattr(
        octo_core,
        "route_worker_results_back_to_octo",
        _route_worker_results_back_to_octo,
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

    await _enqueue_batched_worker_followup(
        octo,
        123,
        "corr-structured",
        task_text="Task A",
        result=WorkerResult(summary="Result A"),
    )
    await _enqueue_batched_worker_followup(
        octo,
        123,
        "corr-structured",
        task_text="Task B",
        result=WorkerResult(summary="Result B"),
    )
    await octo_core._flush_worker_followup_batch(octo, 123, "corr-structured")

    assert routed_batches == [[("", "Task A", "Result A"), ("", "Task B", "Result B")]]
    assert sent_messages == [(123, "Объединила оба результата в один ответ.")]
    assert memory_messages == [
        (
            "assistant",
            "Объединила оба результата в один ответ.",
            {"chat_id": 123, "worker_followup": True, "batched_count": 2},
        )
    ]


@pytest.mark.asyncio
async def test_background_delivery_keeps_user_visible_heartbeat_reply_and_records_memory(monkeypatch):
    memory_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    async def _route_heartbeat(*args, **kwargs):
        return "<user_visible>Утренний брифинг готов.</user_visible>"
    monkeypatch.setattr(octo_core, "route_heartbeat", _route_heartbeat)

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
async def test_background_delivery_rewrites_unwrapped_heartbeat_result_to_explicit_user_visible(monkeypatch):
    memory_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    class DummyProvider:
        async def complete(self, _messages):
            return "<user_visible>Утренний брифинг готов.</user_visible>"

    async def _route_heartbeat(*args, **kwargs):
        return "Утренний брифинг готов."
    monkeypatch.setattr(octo_core, "route_heartbeat", _route_heartbeat)

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=DummyProvider(),
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

    async def _route_heartbeat(*args, **kwargs):
        return "Worker still running. Yielding."
    monkeypatch.setattr(octo_core, "route_heartbeat", _route_heartbeat)

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

    async def _route_heartbeat(*args, **kwargs):
        return (
            "Ладно, canonical memory не подходит для файлов за пределами canon. "
            "Мне нужно использовать tools для записи в research/."
        )
    monkeypatch.setattr(octo_core, "route_heartbeat", _route_heartbeat)

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
async def test_recent_visible_delivery_suppresses_following_heartbeat_send(monkeypatch):
    memory_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            memory_messages.append((role, text, metadata))

    async def _route_heartbeat(*args, **kwargs):
        return "<user_visible>Свежий heartbeat-апдейт.</user_visible>"
    monkeypatch.setattr(octo_core, "route_heartbeat", _route_heartbeat)
    monkeypatch.setattr(octo_core, "_HEARTBEAT_USER_VISIBLE_COOLDOWN_SECONDS", 300)

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=None,
        policy=None,
        runtime=None,
    )
    octo.note_user_visible_delivery(123, "Уже отправила итог воркеров.")

    reply = await octo.handle_message(
        "heartbeat task",
        123,
        persist_to_memory=False,
        track_progress=False,
        include_wakeup=False,
        background_delivery=True,
    )

    assert reply.delivery_mode == DeliveryMode.IMMEDIATE
    assert reply.immediate == "Свежий heartbeat-апдейт."
    assert octo.should_suppress_heartbeat_delivery(123, reply.immediate) is True


@pytest.mark.asyncio
async def test_route_heartbeat_uses_control_plane_prompt_and_skips_planner(monkeypatch):
    calls = {"control_prompt": 0, "complete_route": 0}

    class DummyProvider:
        async def complete(self, _messages):
            return "HEARTBEAT_OK"

    class DummyOcto:
        provider = DummyProvider()
        reflection = None
        mcp_manager = None
        is_ws_active = False

        async def set_typing(self, _chat_id, _active):
            return None

        async def set_thinking(self, _active):
            return None

        def peek_context_wakeup(self, _chat_id):
            return ""

    async def _build_control_plane_prompt(**kwargs):
        calls["control_prompt"] += 1
        return [octo_router.Message(role="user", content=str(kwargs["user_text"]))]

    async def _complete_route_with_tools(**kwargs):
        calls["complete_route"] += 1
        return "HEARTBEAT_OK"

    def _build_octo_prompt_should_not_run(*args, **kwargs):
        raise AssertionError("build_octo_prompt should not run for heartbeat route")

    def _build_plan_should_not_run(*args, **kwargs):
        raise AssertionError("_build_plan should not run for heartbeat route")

    monkeypatch.setattr(octo_router, "build_control_plane_prompt", _build_control_plane_prompt)
    monkeypatch.setattr(octo_router, "_complete_route_with_tools", _complete_route_with_tools)
    monkeypatch.setattr(octo_router, "build_octo_prompt", _build_octo_prompt_should_not_run)
    monkeypatch.setattr(octo_router, "_build_plan", _build_plan_should_not_run)

    result = await octo_router.route_heartbeat(DummyOcto(), 123, "heartbeat task")

    assert result == "HEARTBEAT_OK"
    assert calls == {"control_prompt": 1, "complete_route": 1}


@pytest.mark.asyncio
async def test_route_internal_maintenance_uses_control_plane_prompt_and_skips_planner(
    monkeypatch,
):
    calls = {"control_prompt": 0, "complete_route": 0}

    class DummyOcto:
        provider = object()
        reflection = None

        async def set_thinking(self, value):
            return None

    async def _build_control_plane_prompt(**kwargs):
        calls["control_prompt"] += 1
        assert kwargs["mode_label"] == "internal-maintenance"
        return [octo_router.Message(role="system", content="control plane")]

    async def _complete_route_with_tools(**kwargs):
        calls["complete_route"] += 1
        return "Octo is online."

    def _build_octo_prompt_should_not_run(*args, **kwargs):
        raise AssertionError("build_octo_prompt should not run for internal maintenance")

    def _build_plan_should_not_run(*args, **kwargs):
        raise AssertionError("_build_plan should not run for internal maintenance")

    monkeypatch.setattr(octo_router, "build_control_plane_prompt", _build_control_plane_prompt)
    monkeypatch.setattr(octo_router, "_complete_route_with_tools", _complete_route_with_tools)
    monkeypatch.setattr(octo_router, "build_octo_prompt", _build_octo_prompt_should_not_run)
    monkeypatch.setattr(octo_router, "_build_plan", _build_plan_should_not_run)

    result = await octo_router.route_internal_maintenance(DummyOcto(), 0, "wake up")

    assert result == "Octo is online."
    assert calls == {"control_prompt": 1, "complete_route": 1}


@pytest.mark.asyncio
async def test_initialize_system_uses_internal_maintenance_route(monkeypatch):
    calls = {"internal_maintenance": 0}

    class DummyStore:
        def list_workers(self):
            return []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            return None

    async def _route_internal_maintenance(octo, chat_id, user_text):
        calls["internal_maintenance"] += 1
        assert chat_id == 0
        assert "Inspect runtime health and available workers internally." in user_text
        return "Octo is online."

    async def _bootstrap_should_not_run(*args, **kwargs):
        raise AssertionError("build_bootstrap_context_prompt should not run during startup")

    async def _route_or_reply_should_not_run(*args, **kwargs):
        raise AssertionError("route_or_reply should not run during startup")

    monkeypatch.setattr(octo_core.Octo, "start_background_tasks", lambda self: None)
    monkeypatch.setattr(octo_core, "route_internal_maintenance", _route_internal_maintenance)
    monkeypatch.setattr(octo_core, "build_bootstrap_context_prompt", _bootstrap_should_not_run)
    monkeypatch.setattr(octo_core, "route_or_reply", _route_or_reply_should_not_run)

    octo = Octo(
        approvals=None,
        memory=DummyMemory(),
        canon=None,
        provider=None,
        store=DummyStore(),
        policy=None,
        runtime=None,
        internal_send=None,
    )

    await octo.initialize_system()

    assert calls == {"internal_maintenance": 1}


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
async def test_worker_followup_created_during_active_turn_flushes_after_structured_followup_hint(monkeypatch):
    monkeypatch.setattr(octo_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 0.01)
    octo_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            return None

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    async def _route_or_reply(octo, *args, **kwargs):
        octo.mark_structured_followup_required("turn-test")
        await asyncio.sleep(0.02)
        return "Жду результат воркера."

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
async def test_worker_followup_created_during_active_turn_is_dropped_when_work_already_finished(monkeypatch):
    monkeypatch.setattr(octo_core, "_WORKER_FOLLOWUP_BATCH_WINDOW_SECONDS", 0.01)
    octo_core._WORKER_FOLLOWUP_BATCHES.clear()

    sent_messages = []

    class DummyMemory:
        async def add_message(self, role, text, metadata):
            return None

    async def _send(chat_id, text):
        sent_messages.append((chat_id, text))

    async def _route_or_reply(octo, *args, **kwargs):
        octo.mark_structured_followup_required("turn-test")
        await asyncio.sleep(0.02)
        return "Жду результат воркера."

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
async def test_final_user_reply_drops_active_turn_worker_followup_even_with_pending_internal_results(monkeypatch):
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
        return "Оба поиска готовы."

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
    correlation_id = "turn-test"
    octo.mark_internal_result_pending(correlation_id)

    token = octo_core.correlation_id_var.set(correlation_id)
    try:
        task = asyncio.create_task(octo.handle_message("test", 123))
        await asyncio.sleep(0.005)
        await octo_core._enqueue_batched_worker_followup(octo, 123, correlation_id, "Фоновый итог.")
        reply = await task
    finally:
        octo_core.correlation_id_var.reset(token)
    await asyncio.sleep(0.03)

    assert reply.immediate == "Оба поиска готовы."
    assert sent_messages == []
    assert octo_core._WORKER_FOLLOWUP_BATCHES == {}
    assert octo.should_suppress_turn_followups(correlation_id) is True


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
