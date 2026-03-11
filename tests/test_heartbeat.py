from datetime import timedelta

from broodmind.utils import (
    has_no_user_response_suffix,
    is_heartbeat_ok,
    looks_like_textual_tool_invocation,
    should_suppress_user_delivery,
)
from broodmind.queen.core import (
    _build_worker_result_timeout_followup,
    _extract_followup_required_marker,
    Queen,
)
from broodmind.queen.router import (
    build_forced_worker_followup,
    should_force_worker_followup,
    should_send_worker_followup,
)
from broodmind.workers.contracts import WorkerResult
from broodmind.utils import utc_now

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
    assert should_suppress_user_delivery("list_workers")
    assert should_suppress_user_delivery("check_schedule")
    assert should_suppress_user_delivery("fs_read, file: memory/2026-03-11.md")
    assert not should_suppress_user_delivery("Result ready.")
    assert not should_suppress_user_delivery("Проверяю расписание:")


def test_detect_textual_tool_invocation():
    assert looks_like_textual_tool_invocation("list_workers")
    assert looks_like_textual_tool_invocation("fs_read, file: memory/2026-03-11.md")
    assert not looks_like_textual_tool_invocation("Проверяю расписание:")
    assert not looks_like_textual_tool_invocation("Checking schedule... check_schedule")


def test_worker_result_timeout_followup_stays_user_visible():
    text = _build_worker_result_timeout_followup(
        WorkerResult(
            summary="Digest is ready.",
            questions=["Do you want the long version?", "Should I save it to canon?"],
        )
    )
    assert "Digest is ready." in text
    assert "Open questions:" in text
    assert should_send_worker_followup(text) is True

def test_queen_does_not_have_web_fetch():
    from broodmind.queen.router import _get_queen_tools
    class DummyQueen:
        store = None
    
    tool_specs, _ = _get_queen_tools(DummyQueen(), 0)
    tool_names = [spec.name for spec in tool_specs]
    assert "web_fetch" not in tool_names
    # Sanity check: verify some other tools ARE there
    assert "start_worker" in tool_names
    assert "fs_read" in tool_names
