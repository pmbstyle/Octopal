from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx
from mcp.server import FastMCP

_GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1"
_DEFAULT_SCOPES = ("https://www.googleapis.com/auth/gmail.modify",)

if TYPE_CHECKING:
    from google.oauth2.credentials import Credentials


class GmailConfigError(RuntimeError):
    """Raised when required Gmail MCP configuration is missing."""


@dataclass
class GmailMessageSummary:
    id: str
    thread_id: str | None
    snippet: str | None
    label_ids: list[str]
    internal_date: str | None
    headers: dict[str, str]
    attachment_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "thread_id": self.thread_id,
            "snippet": self.snippet,
            "label_ids": self.label_ids,
            "internal_date": self.internal_date,
            "headers": self.headers,
            "attachment_count": self.attachment_count,
        }


def _header_map(headers: list[dict[str, Any]] | None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for header in headers or []:
        name = str(header.get("name", "")).strip()
        value = str(header.get("value", "")).strip()
        if not name:
            continue
        lowered = name.lower()
        if lowered in {"subject", "from", "to", "cc", "bcc", "date"}:
            normalized[lowered] = value
    return normalized


def _extract_body(payload: dict[str, Any] | None) -> dict[str, str | None]:
    text_parts: list[str] = []
    html_parts: list[str] = []

    def _walk(part: dict[str, Any] | None) -> None:
        if not part:
            return
        mime_type = str(part.get("mimeType", "") or "")
        body = part.get("body") or {}
        data = body.get("data")
        if data:
            import base64

            decoded = base64.urlsafe_b64decode(str(data).encode("utf-8")).decode("utf-8", errors="replace")
            if mime_type == "text/plain":
                text_parts.append(decoded)
            elif mime_type == "text/html":
                html_parts.append(decoded)
        for child in part.get("parts") or []:
            _walk(child)

    _walk(payload)
    return {
        "text": "\n".join(part for part in text_parts if part).strip() or None,
        "html": "\n".join(part for part in html_parts if part).strip() or None,
    }


def _extract_attachments(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []

    def _walk(part: dict[str, Any] | None) -> None:
        if not part:
            return
        filename = str(part.get("filename", "") or "").strip()
        body = part.get("body") or {}
        attachment_id = body.get("attachmentId")
        if filename and attachment_id:
            attachments.append(
                {
                    "filename": filename,
                    "mime_type": part.get("mimeType"),
                    "size": body.get("size"),
                    "attachment_id": attachment_id,
                }
            )
        for child in part.get("parts") or []:
            _walk(child)

    _walk(payload)
    return attachments


def _message_summary(message: dict[str, Any]) -> GmailMessageSummary:
    payload = message.get("payload") or {}
    headers = _header_map(payload.get("headers"))
    attachments = _extract_attachments(payload)
    return GmailMessageSummary(
        id=str(message.get("id", "")),
        thread_id=message.get("threadId"),
        snippet=message.get("snippet"),
        label_ids=[str(label) for label in (message.get("labelIds") or [])],
        internal_date=message.get("internalDate"),
        headers=headers,
        attachment_count=len(attachments),
    )


def _normalize_message(message: dict[str, Any]) -> dict[str, Any]:
    payload = message.get("payload") or {}
    body = _extract_body(payload)
    attachments = _extract_attachments(payload)
    summary = _message_summary(message)
    return {
        **summary.as_dict(),
        "history_id": message.get("historyId"),
        "size_estimate": message.get("sizeEstimate"),
        "text_body": body["text"],
        "html_body": body["html"],
        "attachments": attachments,
    }


class GmailApiClient:
    def __init__(self) -> None:
        self._credentials = self._load_credentials()
        self._client = httpx.AsyncClient(base_url=_GMAIL_API_BASE_URL, timeout=30.0)

    def _load_credentials(self) -> Credentials:
        from google.oauth2.credentials import Credentials

        client_id = os.getenv("GMAIL_CLIENT_ID")
        client_secret = os.getenv("GMAIL_CLIENT_SECRET")
        refresh_token = os.getenv("GMAIL_REFRESH_TOKEN")
        if not client_id or not client_secret or not refresh_token:
            raise GmailConfigError(
                "Missing Gmail MCP credentials. Expected GMAIL_CLIENT_ID, "
                "GMAIL_CLIENT_SECRET, and GMAIL_REFRESH_TOKEN."
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
            raise RuntimeError("Failed to refresh Gmail access token.")
        return token

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = await self._access_token()
        response = await self._client.request(
            method,
            path,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
        )
        if response.status_code == 401:
            self._credentials.token = None
            token = await self._access_token()
            response = await self._client.request(
                method,
                path,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
            )
        response.raise_for_status()
        return response.json()

    async def get_profile(self) -> dict[str, Any]:
        return await self._request("GET", "/users/me/profile")

    async def list_labels(self) -> dict[str, Any]:
        return await self._request("GET", "/users/me/labels")

    async def list_messages(
        self,
        *,
        query: str | None = None,
        label_ids: list[str] | None = None,
        max_results: int = 10,
        page_token: str | None = None,
        include_spam_trash: bool = False,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "maxResults": max(1, min(max_results, 25)),
            "includeSpamTrash": include_spam_trash,
        }
        if query:
            params["q"] = query
        if label_ids:
            params["labelIds"] = label_ids
        if page_token:
            params["pageToken"] = page_token

        payload = await self._request("GET", "/users/me/messages", params=params)
        messages = payload.get("messages") or []
        hydrated = await asyncio.gather(
            *(self.get_message(message["id"], format="metadata") for message in messages)
        )
        return {
            "messages": hydrated,
            "next_page_token": payload.get("nextPageToken"),
            "result_size_estimate": payload.get("resultSizeEstimate", 0),
        }

    async def search_messages(
        self,
        *,
        query: str,
        max_results: int = 10,
        page_token: str | None = None,
        include_spam_trash: bool = False,
    ) -> dict[str, Any]:
        return await self.list_messages(
            query=query,
            max_results=max_results,
            page_token=page_token,
            include_spam_trash=include_spam_trash,
        )

    async def get_message(self, message_id: str, *, format: str = "full") -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/users/me/messages/{message_id}",
            params={"format": format},
        )
        return _normalize_message(payload)

    async def batch_get_messages(
        self,
        message_ids: list[str],
        *,
        format: str = "metadata",
    ) -> dict[str, Any]:
        limited_ids = [str(message_id).strip() for message_id in message_ids if str(message_id).strip()][:25]
        messages = await asyncio.gather(*(self.get_message(message_id, format=format) for message_id in limited_ids))
        return {"messages": messages, "count": len(messages)}

    async def get_thread(self, thread_id: str, *, format: str = "full") -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/users/me/threads/{thread_id}",
            params={"format": format},
        )
        return {
            "id": payload.get("id"),
            "history_id": payload.get("historyId"),
            "messages": [_normalize_message(message) for message in (payload.get("messages") or [])],
        }

    async def get_unread_count(self, *, label_id: str | None = "INBOX") -> dict[str, Any]:
        query = "is:unread"
        if label_id and label_id not in {"INBOX", ""}:
            query = f"label:{label_id} is:unread"
        payload = await self._request(
            "GET",
            "/users/me/messages",
            params={"q": query, "maxResults": 1},
        )
        return {
            "label_id": label_id or None,
            "unread_count": payload.get("resultSizeEstimate", 0),
        }


mcp = FastMCP(
    name="Octopal Gmail",
    instructions=(
        "Use these tools to inspect the connected Gmail account. Prefer list/search tools "
        "to discover candidate messages before calling get_message or get_thread."
    ),
    log_level="ERROR",
)

_gmail_client: GmailApiClient | None = None


def _client() -> GmailApiClient:
    global _gmail_client
    if _gmail_client is None:
        _gmail_client = GmailApiClient()
    return _gmail_client


@mcp.tool(name="get_profile")
async def get_profile() -> dict[str, Any]:
    """Return basic information about the connected Gmail account."""
    return await _client().get_profile()


@mcp.tool(name="list_labels")
async def list_labels() -> dict[str, Any]:
    """List Gmail labels available in the connected account."""
    payload = await _client().list_labels()
    return {"labels": payload.get("labels") or []}


@mcp.tool(name="list_messages")
async def list_messages(
    query: str | None = None,
    label_ids: list[str] | None = None,
    max_results: int = 10,
    page_token: str | None = None,
    include_spam_trash: bool = False,
) -> dict[str, Any]:
    """List recent Gmail messages, optionally filtered by query or labels."""
    return await _client().list_messages(
        query=query,
        label_ids=label_ids,
        max_results=max_results,
        page_token=page_token,
        include_spam_trash=include_spam_trash,
    )


@mcp.tool(name="search_messages")
async def search_messages(
    query: str,
    max_results: int = 10,
    page_token: str | None = None,
    include_spam_trash: bool = False,
) -> dict[str, Any]:
    """Search Gmail using standard Gmail query syntax."""
    return await _client().search_messages(
        query=query,
        max_results=max_results,
        page_token=page_token,
        include_spam_trash=include_spam_trash,
    )


@mcp.tool(name="get_message")
async def get_message(message_id: str, format: str = "full") -> dict[str, Any]:
    """Get a Gmail message by ID."""
    return await _client().get_message(message_id, format=format)


@mcp.tool(name="batch_get_messages")
async def batch_get_messages(message_ids: list[str], format: str = "metadata") -> dict[str, Any]:
    """Fetch multiple Gmail messages by ID."""
    return await _client().batch_get_messages(message_ids, format=format)


@mcp.tool(name="get_thread")
async def get_thread(thread_id: str, format: str = "full") -> dict[str, Any]:
    """Get a Gmail thread by ID."""
    return await _client().get_thread(thread_id, format=format)


@mcp.tool(name="get_unread_count")
async def get_unread_count(label_id: str | None = "INBOX") -> dict[str, Any]:
    """Return unread message count, optionally scoped to a label."""
    return await _client().get_unread_count(label_id=label_id)


def main() -> None:
    try:
        mcp.run()
    finally:
        try:
            if _gmail_client is not None:
                asyncio.run(_gmail_client.close())
        except Exception:
            pass


if __name__ == "__main__":
    main()
