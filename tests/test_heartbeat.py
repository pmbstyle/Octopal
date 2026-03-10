from broodmind.utils import (
    has_no_user_response_suffix,
    is_heartbeat_ok,
    should_suppress_user_delivery,
)
from broodmind.queen.core import _build_worker_result_timeout_followup
from broodmind.queen.router import should_send_worker_followup
from broodmind.workers.contracts import WorkerResult

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
