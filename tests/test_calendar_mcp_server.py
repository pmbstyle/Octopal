from __future__ import annotations

import httpx

from octopal.mcp_servers.calendar import (
    CalendarApiClient,
    _build_event_patch_body,
    _normalize_busy_slot,
    _normalize_event,
    _parse_google_api_error,
)


def test_normalize_event_keeps_core_calendar_fields() -> None:
    event = {
        "id": "evt-1",
        "status": "confirmed",
        "summary": "Planning",
        "description": "Quarterly planning",
        "location": "Room 1",
        "htmlLink": "https://calendar.google.com/event?eid=123",
        "created": "2026-04-01T10:00:00Z",
        "updated": "2026-04-01T10:05:00Z",
        "start": {"dateTime": "2026-04-02T14:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-04-02T15:00:00Z", "timeZone": "UTC"},
        "organizer": {"email": "team@example.com", "displayName": "Team"},
        "creator": {"email": "alice@example.com", "displayName": "Alice"},
        "attendees": [{"email": "bob@example.com", "responseStatus": "accepted"}],
    }

    normalized = _normalize_event(event)

    assert normalized["id"] == "evt-1"
    assert normalized["summary"] == "Planning"
    assert normalized["start"]["date_time"] == "2026-04-02T14:00:00Z"
    assert normalized["organizer"]["email"] == "team@example.com"
    assert normalized["attendees"][0]["email"] == "bob@example.com"


def test_parse_calendar_api_error_prefers_reason_and_message_from_json_payload() -> None:
    response = httpx.Response(
        403,
        json={
            "error": {
                "code": 403,
                "message": "Request had insufficient authentication scopes.",
                "status": "PERMISSION_DENIED",
                "errors": [{"reason": "insufficientPermissions"}],
            }
        },
    )

    error = _parse_google_api_error(response)

    assert error.status_code == 403
    assert error.reason == "insufficientPermissions"
    assert "Request had insufficient authentication scopes." in str(error)


def test_build_event_patch_body_keeps_only_supplied_fields() -> None:
    body = _build_event_patch_body(
        summary="Updated planning",
        start={"dateTime": "2026-04-02T16:00:00Z"},
        time_zone="UTC",
    )

    assert body == {
        "summary": "Updated planning",
        "start": {"dateTime": "2026-04-02T16:00:00Z", "timeZone": "UTC"},
    }


def test_build_event_patch_body_applies_time_zone_to_start_and_end_when_present() -> None:
    body = _build_event_patch_body(
        start={"dateTime": "2026-04-02T16:00:00Z"},
        end={"dateTime": "2026-04-02T17:00:00Z"},
        time_zone="America/New_York",
    )

    assert body["start"]["timeZone"] == "America/New_York"
    assert body["end"]["timeZone"] == "America/New_York"


def test_calendar_request_returns_empty_payload_for_delete_without_body() -> None:
    client = object.__new__(CalendarApiClient)
    client._credentials = type("_Creds", (), {"valid": True, "token": "token"})()

    class _HTTPClient:
        async def request(self, method, path, params=None, json=None, headers=None):
            return httpx.Response(204, request=httpx.Request(method, f"https://example.test{path}"))

    client._client = _HTTPClient()

    async def _token() -> str:
        return "token"

    client._access_token = _token  # type: ignore[method-assign]

    import asyncio

    payload = asyncio.run(client._request("DELETE", "/calendars/primary/events/evt-1"))

    assert payload == {}


def test_normalize_busy_slot_keeps_start_and_end() -> None:
    slot = _normalize_busy_slot({"start": "2026-04-02T14:00:00Z", "end": "2026-04-02T15:00:00Z", "x": "ignored"})

    assert slot == {
        "start": "2026-04-02T14:00:00Z",
        "end": "2026-04-02T15:00:00Z",
    }
