from __future__ import annotations

import asyncio
import json
import os
from typing import TYPE_CHECKING, Any

import httpx
from mcp.server import FastMCP

_CALENDAR_API_BASE_URL = "https://www.googleapis.com/calendar/v3"
_DEFAULT_SCOPES = ("https://www.googleapis.com/auth/calendar",)

if TYPE_CHECKING:
    from google.oauth2.credentials import Credentials


class CalendarConfigError(RuntimeError):
    """Raised when required Calendar MCP configuration is missing."""


class CalendarApiError(RuntimeError):
    """Raised when Calendar API returns a structured error response."""

    def __init__(
        self,
        *,
        status_code: int,
        reason: str | None,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.status_code = status_code
        self.reason = reason
        self.details = details or {}
        parts = [f"Calendar API {status_code}"]
        if reason:
            parts.append(reason)
        parts.append(message)
        super().__init__(": ".join(parts))


def _parse_google_api_error(response: httpx.Response) -> CalendarApiError:
    payload: dict[str, Any] = {}
    try:
        payload = response.json()
    except json.JSONDecodeError:
        text = response.text.strip() or response.reason_phrase
        return CalendarApiError(
            status_code=response.status_code,
            reason=None,
            message=text,
            details={},
        )

    error = payload.get("error")
    if isinstance(error, dict):
        message = str(error.get("message") or response.reason_phrase or "Unknown Calendar API error").strip()
        errors = error.get("errors")
        reason = None
        if isinstance(errors, list) and errors:
            first_error = errors[0]
            if isinstance(first_error, dict):
                reason = str(first_error.get("reason") or "").strip() or None
        if reason is None:
            status = error.get("status")
            if status:
                reason = str(status).strip()
        return CalendarApiError(
            status_code=response.status_code,
            reason=reason,
            message=message,
            details=error,
        )

    text = response.text.strip() or response.reason_phrase
    return CalendarApiError(
        status_code=response.status_code,
        reason=None,
        message=text,
        details=payload if isinstance(payload, dict) else {},
    )


def _normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    start = event.get("start") or {}
    end = event.get("end") or {}
    organizer = event.get("organizer") or {}
    creator = event.get("creator") or {}
    attendees = event.get("attendees") or []
    return {
        "id": event.get("id"),
        "calendar_id": event.get("organizer", {}).get("email"),
        "status": event.get("status"),
        "summary": event.get("summary"),
        "description": event.get("description"),
        "location": event.get("location"),
        "html_link": event.get("htmlLink"),
        "created": event.get("created"),
        "updated": event.get("updated"),
        "start": {"date_time": start.get("dateTime"), "date": start.get("date"), "time_zone": start.get("timeZone")},
        "end": {"date_time": end.get("dateTime"), "date": end.get("date"), "time_zone": end.get("timeZone")},
        "organizer": {"email": organizer.get("email"), "display_name": organizer.get("displayName")},
        "creator": {"email": creator.get("email"), "display_name": creator.get("displayName")},
        "attendees": [
            {
                "email": attendee.get("email"),
                "display_name": attendee.get("displayName"),
                "response_status": attendee.get("responseStatus"),
                "optional": attendee.get("optional", False),
                "self": attendee.get("self", False),
            }
            for attendee in attendees
        ],
    }


def _build_event_patch_body(
    *,
    summary: str | None = None,
    description: str | None = None,
    location: str | None = None,
    start: dict[str, Any] | None = None,
    end: dict[str, Any] | None = None,
    attendees: list[dict[str, Any]] | None = None,
    time_zone: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if summary is not None:
        body["summary"] = summary
    if description is not None:
        body["description"] = description
    if location is not None:
        body["location"] = location
    if attendees is not None:
        body["attendees"] = attendees
    if start is not None:
        body["start"] = dict(start)
    if end is not None:
        body["end"] = dict(end)
    if time_zone:
        if "start" in body:
            body["start"].setdefault("timeZone", time_zone)
        if "end" in body:
            body["end"].setdefault("timeZone", time_zone)
    return body


def _normalize_busy_slot(slot: dict[str, Any]) -> dict[str, Any]:
    return {
        "start": slot.get("start"),
        "end": slot.get("end"),
    }


class CalendarApiClient:
    def __init__(self) -> None:
        self._credentials = self._load_credentials()
        self._client = httpx.AsyncClient(base_url=_CALENDAR_API_BASE_URL, timeout=30.0)

    def _load_credentials(self) -> Credentials:
        from google.oauth2.credentials import Credentials

        client_id = os.getenv("GOOGLE_CALENDAR_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_CALENDAR_CLIENT_SECRET")
        refresh_token = os.getenv("GOOGLE_CALENDAR_REFRESH_TOKEN")
        if not client_id or not client_secret or not refresh_token:
            raise CalendarConfigError(
                "Missing Calendar MCP credentials. Expected GOOGLE_CALENDAR_CLIENT_ID, "
                "GOOGLE_CALENDAR_CLIENT_SECRET, and GOOGLE_CALENDAR_REFRESH_TOKEN."
            )
        return Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=list(_DEFAULT_SCOPES),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _access_token(self) -> str:
        from google.auth.transport.requests import Request

        if not self._credentials.valid:
            await asyncio.to_thread(self._credentials.refresh, Request())
        token = self._credentials.token
        if not token:
            raise RuntimeError("Failed to refresh Calendar access token.")
        return token

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = await self._access_token()
        response = await self._client.request(
            method,
            path,
            params=params,
            json=json_body,
            headers={"Authorization": f"Bearer {token}"},
        )
        if response.status_code == 401:
            self._credentials.token = None
            token = await self._access_token()
            response = await self._client.request(
                method,
                path,
                params=params,
                json=json_body,
                headers={"Authorization": f"Bearer {token}"},
            )
        if response.is_error:
            raise _parse_google_api_error(response)
        if not response.content or response.status_code == 204:
            return {}
        return response.json()

    async def list_calendars(self) -> dict[str, Any]:
        payload = await self._request("GET", "/users/me/calendarList")
        return {
            "calendars": [
                {
                    "id": item.get("id"),
                    "summary": item.get("summary"),
                    "description": item.get("description"),
                    "primary": item.get("primary", False),
                    "time_zone": item.get("timeZone"),
                    "access_role": item.get("accessRole"),
                }
                for item in (payload.get("items") or [])
            ]
        }

    async def list_events(
        self,
        *,
        calendar_id: str = "primary",
        max_results: int = 10,
        query: str | None = None,
        time_min: str | None = None,
        time_max: str | None = None,
        single_events: bool = True,
        order_by: str | None = "startTime",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "maxResults": max(1, min(max_results, 25)),
            "singleEvents": single_events,
        }
        if query:
            params["q"] = query
        if time_min:
            params["timeMin"] = time_min
        if time_max:
            params["timeMax"] = time_max
        if order_by:
            params["orderBy"] = order_by
        payload = await self._request("GET", f"/calendars/{calendar_id}/events", params=params)
        return {
            "calendar_id": calendar_id,
            "events": [_normalize_event(item) for item in (payload.get("items") or [])],
            "next_page_token": payload.get("nextPageToken"),
        }

    async def search_events(
        self,
        *,
        query: str,
        calendar_id: str = "primary",
        max_results: int = 10,
        time_min: str | None = None,
        time_max: str | None = None,
    ) -> dict[str, Any]:
        return await self.list_events(
            calendar_id=calendar_id,
            max_results=max_results,
            query=query,
            time_min=time_min,
            time_max=time_max,
        )

    async def get_event(self, *, calendar_id: str = "primary", event_id: str) -> dict[str, Any]:
        payload = await self._request("GET", f"/calendars/{calendar_id}/events/{event_id}")
        return _normalize_event(payload)

    async def create_event(
        self,
        *,
        calendar_id: str = "primary",
        summary: str,
        start: dict[str, Any],
        end: dict[str, Any],
        description: str | None = None,
        location: str | None = None,
        attendees: list[dict[str, Any]] | None = None,
        time_zone: str | None = None,
    ) -> dict[str, Any]:
        body = _build_event_patch_body(
            summary=summary,
            description=description,
            location=location,
            start=start,
            end=end,
            attendees=attendees,
            time_zone=time_zone,
        )

        payload = await self._request("POST", f"/calendars/{calendar_id}/events", json_body=body)
        return _normalize_event(payload)

    async def update_event(
        self,
        *,
        calendar_id: str = "primary",
        event_id: str,
        summary: str | None = None,
        description: str | None = None,
        location: str | None = None,
        start: dict[str, Any] | None = None,
        end: dict[str, Any] | None = None,
        attendees: list[dict[str, Any]] | None = None,
        time_zone: str | None = None,
    ) -> dict[str, Any]:
        body = _build_event_patch_body(
            summary=summary,
            description=description,
            location=location,
            start=start,
            end=end,
            attendees=attendees,
            time_zone=time_zone,
        )
        if not body:
            raise ValueError("At least one mutable event field must be provided for update.")
        payload = await self._request(
            "PATCH",
            f"/calendars/{calendar_id}/events/{event_id}",
            json_body=body,
        )
        return _normalize_event(payload)

    async def delete_event(self, *, calendar_id: str = "primary", event_id: str) -> dict[str, Any]:
        await self._request("DELETE", f"/calendars/{calendar_id}/events/{event_id}")
        return {"ok": True, "calendar_id": calendar_id, "event_id": event_id, "status": "deleted"}

    async def freebusy(
        self,
        *,
        calendar_ids: list[str],
        time_min: str,
        time_max: str,
        time_zone: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "timeMin": time_min,
            "timeMax": time_max,
            "items": [{"id": calendar_id} for calendar_id in calendar_ids if str(calendar_id).strip()],
        }
        if time_zone:
            body["timeZone"] = time_zone

        payload = await self._request("POST", "/freeBusy", json_body=body)
        calendars = payload.get("calendars") or {}
        normalized_calendars = {
            calendar_id: {
                "busy": [_normalize_busy_slot(slot) for slot in ((calendar_payload or {}).get("busy") or [])],
                "errors": (calendar_payload or {}).get("errors") or [],
            }
            for calendar_id, calendar_payload in calendars.items()
        }
        return {
            "time_min": payload.get("timeMin", time_min),
            "time_max": payload.get("timeMax", time_max),
            "groups": payload.get("groups") or {},
            "calendars": normalized_calendars,
        }


mcp = FastMCP(
    name="Octopal Google Calendar",
    instructions=(
        "Use these tools to inspect, create, update, and delete events in the connected Google Calendar account, "
        "including free/busy lookups for one or more calendars. "
        "Prefer listing calendars or events before fetching a single event when context is missing."
    ),
    log_level="ERROR",
)

_calendar_client: CalendarApiClient | None = None


def _client() -> CalendarApiClient:
    global _calendar_client
    if _calendar_client is None:
        _calendar_client = CalendarApiClient()
    return _calendar_client


@mcp.tool(name="list_calendars")
async def list_calendars() -> dict[str, Any]:
    """List available calendars for the connected Google account."""
    return await _client().list_calendars()


@mcp.tool(name="list_events")
async def list_events(
    calendar_id: str = "primary",
    max_results: int = 10,
    query: str | None = None,
    time_min: str | None = None,
    time_max: str | None = None,
    single_events: bool = True,
    order_by: str | None = "startTime",
) -> dict[str, Any]:
    """List events for a calendar, optionally filtered by time range or query."""
    return await _client().list_events(
        calendar_id=calendar_id,
        max_results=max_results,
        query=query,
        time_min=time_min,
        time_max=time_max,
        single_events=single_events,
        order_by=order_by,
    )


@mcp.tool(name="search_events")
async def search_events(
    query: str,
    calendar_id: str = "primary",
    max_results: int = 10,
    time_min: str | None = None,
    time_max: str | None = None,
) -> dict[str, Any]:
    """Search events in a calendar by free-text query."""
    return await _client().search_events(
        query=query,
        calendar_id=calendar_id,
        max_results=max_results,
        time_min=time_min,
        time_max=time_max,
    )


@mcp.tool(name="get_event")
async def get_event(event_id: str, calendar_id: str = "primary") -> dict[str, Any]:
    """Get a single event by calendar ID and event ID."""
    return await _client().get_event(calendar_id=calendar_id, event_id=event_id)


@mcp.tool(name="create_event")
async def create_event(
    summary: str,
    start: dict[str, Any],
    end: dict[str, Any],
    calendar_id: str = "primary",
    description: str | None = None,
    location: str | None = None,
    attendees: list[dict[str, Any]] | None = None,
    time_zone: str | None = None,
) -> dict[str, Any]:
    """Create a calendar event."""
    return await _client().create_event(
        calendar_id=calendar_id,
        summary=summary,
        start=start,
        end=end,
        description=description,
        location=location,
        attendees=attendees,
        time_zone=time_zone,
    )


@mcp.tool(name="update_event")
async def update_event(
    event_id: str,
    calendar_id: str = "primary",
    summary: str | None = None,
    description: str | None = None,
    location: str | None = None,
    start: dict[str, Any] | None = None,
    end: dict[str, Any] | None = None,
    attendees: list[dict[str, Any]] | None = None,
    time_zone: str | None = None,
) -> dict[str, Any]:
    """Update a calendar event with partial field changes."""
    return await _client().update_event(
        calendar_id=calendar_id,
        event_id=event_id,
        summary=summary,
        description=description,
        location=location,
        start=start,
        end=end,
        attendees=attendees,
        time_zone=time_zone,
    )


@mcp.tool(name="delete_event")
async def delete_event(event_id: str, calendar_id: str = "primary") -> dict[str, Any]:
    """Delete a calendar event."""
    return await _client().delete_event(calendar_id=calendar_id, event_id=event_id)


@mcp.tool(name="freebusy")
async def freebusy(
    calendar_ids: list[str],
    time_min: str,
    time_max: str,
    time_zone: str | None = None,
) -> dict[str, Any]:
    """Return busy windows for one or more calendars in a time range."""
    return await _client().freebusy(
        calendar_ids=calendar_ids,
        time_min=time_min,
        time_max=time_max,
        time_zone=time_zone,
    )


def main() -> None:
    try:
        mcp.run()
    finally:
        try:
            if _calendar_client is not None:
                asyncio.run(_calendar_client.close())
        except Exception:
            pass


if __name__ == "__main__":
    main()
