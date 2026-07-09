from __future__ import annotations

import asyncio
import base64
import json
import os
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formataddr, getaddresses
from typing import TYPE_CHECKING, Any

import httpx
from mcp.server import FastMCP

_GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1"
_DEFAULT_SCOPES = ("https://www.googleapis.com/auth/gmail.modify",)

if TYPE_CHECKING:
    from google.oauth2.credentials import Credentials


class GmailConfigError(RuntimeError):
    """Raised when required Gmail MCP configuration is missing."""


class GmailApiError(RuntimeError):
    """Raised when Gmail API returns a structured error response."""

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
        parts = [f"Gmail API {status_code}"]
        if reason:
            parts.append(reason)
        parts.append(message)
        super().__init__(": ".join(parts))

    def as_dict(self) -> dict[str, Any]:
        return {
            "status_code": self.status_code,
            "reason": self.reason,
            "message": str(self),
            "details": self.details,
        }


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


def _get_header_value(headers: list[dict[str, Any]] | None, name: str) -> str | None:
    target = str(name or "").strip().lower()
    if not target:
        return None
    for header in headers or []:
        header_name = str(header.get("name", "")).strip().lower()
        if header_name == target:
            value = str(header.get("value", "")).strip()
            return value or None
    return None


def _normalize_address_list(values: list[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for display_name, email in getaddresses(values or []):
        address = str(email or "").strip()
        if not address:
            continue
        key = address.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(formataddr((str(display_name or "").strip(), address)))
    return normalized


def _extract_email_address(value: str) -> str:
    parsed = getaddresses([value or ""])
    if parsed:
        _display_name, email = parsed[0]
        email = str(email or "").strip()
        if email:
            return email.lower()
    return str(value or "").strip().lower()


def _split_reference_chain(value: str | None) -> list[str]:
    if not value:
        return []
    return [part for part in str(value).split() if part]


def _normalize_label_key(value: str) -> str:
    return str(value or "").strip().lower()


def _reply_subject(subject: str | None) -> str:
    clean = str(subject or "").strip()
    if not clean:
        return "Re:"
    if clean.lower().startswith("re:"):
        return clean
    return f"Re: {clean}"


def _build_raw_message(
    *,
    to: list[str],
    subject: str,
    body_text: str | None,
    body_html: str | None,
    cc: list[str] | None = None,
    bcc: list[str] | None = None,
    in_reply_to: str | None = None,
    references: list[str] | None = None,
) -> str:
    recipients = _normalize_address_list(to)
    cc_list = _normalize_address_list(cc)
    bcc_list = _normalize_address_list(bcc)
    if not recipients:
        raise ValueError("At least one recipient is required.")
    if not str(subject or "").strip():
        raise ValueError("subject is required.")
    if not str(body_text or "").strip() and not str(body_html or "").strip():
        raise ValueError("Either body_text or body_html is required.")

    message = EmailMessage()
    message["To"] = ", ".join(recipients)
    message["Subject"] = str(subject).strip()
    if cc_list:
        message["Cc"] = ", ".join(cc_list)
    if bcc_list:
        message["Bcc"] = ", ".join(bcc_list)
    if in_reply_to:
        message["In-Reply-To"] = in_reply_to
    if references:
        cleaned_refs = [ref for ref in references if str(ref or "").strip()]
        if cleaned_refs:
            message["References"] = " ".join(cleaned_refs)

    text_part = str(body_text or "").strip()
    html_part = str(body_html or "").strip()
    message.set_content(text_part or " ")
    if html_part:
        message.add_alternative(html_part, subtype="html")

    return base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")


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
            decoded = base64.urlsafe_b64decode(str(data).encode("utf-8")).decode(
                "utf-8", errors="replace"
            )
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


def _parse_google_api_error(response: httpx.Response) -> GmailApiError:
    payload: dict[str, Any] = {}
    try:
        payload = response.json()
    except json.JSONDecodeError:
        text = response.text.strip() or response.reason_phrase
        return GmailApiError(
            status_code=response.status_code,
            reason=None,
            message=text,
            details={},
        )

    error = payload.get("error")
    if isinstance(error, dict):
        message = str(
            error.get("message") or response.reason_phrase or "Unknown Gmail API error"
        ).strip()
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
        return GmailApiError(
            status_code=response.status_code,
            reason=reason,
            message=message,
            details=error,
        )

    text = response.text.strip() or response.reason_phrase
    return GmailApiError(
        status_code=response.status_code,
        reason=None,
        message=text,
        details=payload if isinstance(payload, dict) else {},
    )


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
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = await self._access_token()
        response = await self._client.request(
            method,
            path,
            params=params,
            json=json,
            headers={"Authorization": f"Bearer {token}"},
        )
        if response.status_code == 401:
            self._credentials.token = None
            token = await self._access_token()
            response = await self._client.request(
                method,
                path,
                params=params,
                json=json,
                headers={"Authorization": f"Bearer {token}"},
            )
        if response.is_error:
            raise _parse_google_api_error(response)
        if not response.content:
            return {}
        return response.json()

    async def _get_message_payload(
        self, message_id: str, *, format: str = "full"
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/users/me/messages/{message_id}",
            params={"format": format},
        )

    async def get_profile(self) -> dict[str, Any]:
        return await self._request("GET", "/users/me/profile")

    async def list_labels(self) -> dict[str, Any]:
        return await self._request("GET", "/users/me/labels")

    async def _resolve_label_id(self, label_name_or_id: str) -> str:
        target = str(label_name_or_id or "").strip()
        if not target:
            raise ValueError("label_name is required.")
        payload = await self.list_labels()
        labels = payload.get("labels") or []
        target_key = _normalize_label_key(target)
        for label in labels:
            label_id = str(label.get("id", "")).strip()
            label_name = str(label.get("name", "")).strip()
            if (
                _normalize_label_key(label_id) == target_key
                or _normalize_label_key(label_name) == target_key
            ):
                return label_id or label_name
        raise ValueError(f"Gmail label not found: {target}")

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
        payload = await self._get_message_payload(message_id, format=format)
        return _normalize_message(payload)

    async def batch_get_messages(
        self,
        message_ids: list[str],
        *,
        format: str = "metadata",
    ) -> dict[str, Any]:
        limited_ids = [
            str(message_id).strip() for message_id in message_ids if str(message_id).strip()
        ][:25]
        messages = await asyncio.gather(
            *(self.get_message(message_id, format=format) for message_id in limited_ids)
        )
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
            "messages": [
                _normalize_message(message) for message in (payload.get("messages") or [])
            ],
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

    async def archive_message(self, *, message_id: str) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            f"/users/me/messages/{message_id}/modify",
            json={"removeLabelIds": ["INBOX"]},
        )
        return _normalize_message(payload)

    async def trash_message(self, *, message_id: str) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            f"/users/me/messages/{message_id}/trash",
            json={},
        )
        return _normalize_message(payload)

    async def delete_message(self, *, message_id: str) -> dict[str, Any]:
        await self._request("DELETE", f"/users/me/messages/{message_id}")
        return {"ok": True, "id": message_id, "deleted": True}

    async def mark_message_read(self, *, message_id: str) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            f"/users/me/messages/{message_id}/modify",
            json={"removeLabelIds": ["UNREAD"]},
        )
        return _normalize_message(payload)

    async def mark_message_unread(self, *, message_id: str) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            f"/users/me/messages/{message_id}/modify",
            json={"addLabelIds": ["UNREAD"]},
        )
        return _normalize_message(payload)

    async def modify_message_labels(
        self,
        *,
        message_id: str,
        add_label_ids: list[str] | None = None,
        remove_label_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        add_ids = [str(label).strip() for label in (add_label_ids or []) if str(label).strip()]
        remove_ids = [
            str(label).strip() for label in (remove_label_ids or []) if str(label).strip()
        ]
        if not add_ids and not remove_ids:
            raise ValueError("At least one label change is required.")
        payload = await self._request(
            "POST",
            f"/users/me/messages/{message_id}/modify",
            json={
                "addLabelIds": add_ids,
                "removeLabelIds": remove_ids,
            },
        )
        return _normalize_message(payload)

    async def add_label_by_name(self, *, message_id: str, label_name: str) -> dict[str, Any]:
        label_id = await self._resolve_label_id(label_name)
        payload = await self.modify_message_labels(message_id=message_id, add_label_ids=[label_id])
        payload["resolved_label_id"] = label_id
        payload["resolved_label_name"] = label_name
        return payload

    async def remove_label_by_name(self, *, message_id: str, label_name: str) -> dict[str, Any]:
        label_id = await self._resolve_label_id(label_name)
        payload = await self.modify_message_labels(
            message_id=message_id, remove_label_ids=[label_id]
        )
        payload["resolved_label_id"] = label_id
        payload["resolved_label_name"] = label_name
        return payload

    async def move_message_to_inbox(self, *, message_id: str) -> dict[str, Any]:
        payload = await self.modify_message_labels(
            message_id=message_id,
            add_label_ids=["INBOX"],
            remove_label_ids=["SPAM", "TRASH"],
        )
        payload["moved_to"] = "INBOX"
        return payload

    async def move_message_out_of_inbox(self, *, message_id: str) -> dict[str, Any]:
        payload = await self.archive_message(message_id=message_id)
        payload["moved_to"] = "ARCHIVE"
        return payload

    async def get_attachment(
        self,
        *,
        message_id: str,
        attachment_id: str,
        filename: str | None = None,
    ) -> dict[str, Any]:
        attachment = await self._request(
            "GET",
            f"/users/me/messages/{message_id}/attachments/{attachment_id}",
        )
        data = str(attachment.get("data") or "")
        content = base64.urlsafe_b64decode(data.encode("ascii")) if data else b""
        payload = await self._get_message_payload(message_id, format="full")
        attachments = _extract_attachments(payload.get("payload") or {})
        attachment_meta = next(
            (
                item
                for item in attachments
                if str(item.get("attachment_id", "")).strip() == attachment_id
            ),
            None,
        )
        resolved_name = str(
            filename
            or (attachment_meta or {}).get("filename")
            or attachment.get("filename")
            or attachment_id
        ).strip()
        return {
            "message_id": message_id,
            "attachment_id": attachment_id,
            "filename": resolved_name,
            "mime_type": (attachment_meta or {}).get("mime_type"),
            "size": attachment.get("size") or (attachment_meta or {}).get("size") or len(content),
            "content_base64": base64.b64encode(content).decode("ascii"),
        }

    async def send_message(
        self,
        *,
        to: list[str],
        subject: str,
        body_text: str | None = None,
        body_html: str | None = None,
        cc: list[str] | None = None,
        bcc: list[str] | None = None,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        raw = _build_raw_message(
            to=to,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            cc=cc,
            bcc=bcc,
        )
        payload: dict[str, Any] = {"raw": raw}
        if thread_id:
            payload["threadId"] = thread_id

        sent = await self._request("POST", "/users/me/messages/send", json=payload)
        sent_id = str(sent.get("id", "")).strip()
        if sent_id:
            return await self.get_message(sent_id, format="metadata")
        return _normalize_message(sent)

    async def reply_to_message(
        self,
        *,
        message_id: str,
        body_text: str | None = None,
        body_html: str | None = None,
        cc: list[str] | None = None,
        bcc: list[str] | None = None,
        reply_all: bool = False,
    ) -> dict[str, Any]:
        original = await self._get_message_payload(message_id, format="metadata")
        headers = (original.get("payload") or {}).get("headers") or []
        original_message_id = _get_header_value(headers, "Message-ID")
        if not original_message_id:
            raise RuntimeError(
                "Original Gmail message is missing a Message-ID header required for replies."
            )

        profile = await self.get_profile()
        self_email = str(profile.get("emailAddress", "") or "").strip().lower()

        direct_recipients = _normalize_address_list(
            [
                _get_header_value(headers, "Reply-To") or _get_header_value(headers, "From") or "",
            ]
        )
        reply_to_keys = {_extract_email_address(addr) for addr in direct_recipients}
        to_recipients = list(direct_recipients)
        cc_recipients: list[str] = []

        if reply_all:
            existing_to = _normalize_address_list([_get_header_value(headers, "To") or ""])
            existing_cc = _normalize_address_list([_get_header_value(headers, "Cc") or ""])
            for address in existing_to:
                email = _extract_email_address(address)
                if email == self_email or email in reply_to_keys:
                    continue
                to_recipients.append(address)
            exclude_cc = {
                *(_extract_email_address(addr) for addr in to_recipients),
                self_email,
            }
            for address in existing_cc:
                email = _extract_email_address(address)
                if email in exclude_cc:
                    continue
                cc_recipients.append(address)

        to_recipients = _normalize_address_list(to_recipients)
        cc_recipients = _normalize_address_list(cc_recipients + list(cc or []))
        bcc_recipients = _normalize_address_list(list(bcc or []))
        if self_email:
            to_recipients = [addr for addr in to_recipients if self_email not in addr.lower()]
            cc_recipients = [addr for addr in cc_recipients if self_email not in addr.lower()]
            bcc_recipients = [addr for addr in bcc_recipients if self_email not in addr.lower()]
        if not to_recipients:
            raise RuntimeError(
                "Could not determine reply recipients from the original Gmail message."
            )

        references = _split_reference_chain(_get_header_value(headers, "References"))
        references.append(original_message_id)
        raw = _build_raw_message(
            to=to_recipients,
            subject=_reply_subject(_get_header_value(headers, "Subject")),
            body_text=body_text,
            body_html=body_html,
            cc=cc_recipients,
            bcc=bcc_recipients,
            in_reply_to=original_message_id,
            references=references,
        )
        sent = await self._request(
            "POST",
            "/users/me/messages/send",
            json={
                "raw": raw,
                "threadId": original.get("threadId"),
            },
        )
        sent_id = str(sent.get("id", "")).strip()
        if sent_id:
            return await self.get_message(sent_id, format="metadata")
        return _normalize_message(sent)


mcp = FastMCP(
    name="Octopal Gmail",
    instructions=(
        "Use these tools to inspect the connected Gmail account and send or reply to messages. "
        "Prefer list/search tools to discover candidate messages before calling get_message, "
        "get_thread, or reply_to_message."
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


@mcp.tool(name="send_message")
async def send_message(
    to: list[str],
    subject: str,
    body_text: str | None = None,
    body_html: str | None = None,
    cc: list[str] | None = None,
    bcc: list[str] | None = None,
    thread_id: str | None = None,
) -> dict[str, Any]:
    """Send a Gmail message, optionally attaching it to an existing thread."""
    return await _client().send_message(
        to=to,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        cc=cc,
        bcc=bcc,
        thread_id=thread_id,
    )


@mcp.tool(name="reply_to_message")
async def reply_to_message(
    message_id: str,
    body_text: str | None = None,
    body_html: str | None = None,
    cc: list[str] | None = None,
    bcc: list[str] | None = None,
    reply_all: bool = False,
) -> dict[str, Any]:
    """Reply to a Gmail message by message ID, optionally as reply-all."""
    return await _client().reply_to_message(
        message_id=message_id,
        body_text=body_text,
        body_html=body_html,
        cc=cc,
        bcc=bcc,
        reply_all=reply_all,
    )


@mcp.tool(name="archive_message")
async def archive_message(message_id: str) -> dict[str, Any]:
    """Archive a Gmail message by removing it from Inbox."""
    return await _client().archive_message(message_id=message_id)


@mcp.tool(name="trash_message")
async def trash_message(message_id: str) -> dict[str, Any]:
    """Move a Gmail message to trash."""
    return await _client().trash_message(message_id=message_id)


@mcp.tool(name="delete_message")
async def delete_message(message_id: str) -> dict[str, Any]:
    """Permanently delete a Gmail message."""
    return await _client().delete_message(message_id=message_id)


@mcp.tool(name="mark_message_read")
async def mark_message_read(message_id: str) -> dict[str, Any]:
    """Mark a Gmail message as read."""
    return await _client().mark_message_read(message_id=message_id)


@mcp.tool(name="mark_message_unread")
async def mark_message_unread(message_id: str) -> dict[str, Any]:
    """Mark a Gmail message as unread."""
    return await _client().mark_message_unread(message_id=message_id)


@mcp.tool(name="modify_message_labels")
async def modify_message_labels(
    message_id: str,
    add_label_ids: list[str] | None = None,
    remove_label_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Add and/or remove Gmail labels on a message."""
    return await _client().modify_message_labels(
        message_id=message_id,
        add_label_ids=add_label_ids,
        remove_label_ids=remove_label_ids,
    )


@mcp.tool(name="get_attachment")
async def get_attachment(
    message_id: str,
    attachment_id: str,
    filename: str | None = None,
) -> dict[str, Any]:
    """Download a Gmail attachment and return its content as base64."""
    return await _client().get_attachment(
        message_id=message_id,
        attachment_id=attachment_id,
        filename=filename,
    )


@mcp.tool(name="add_label_by_name")
async def add_label_by_name(message_id: str, label_name: str) -> dict[str, Any]:
    """Add a Gmail label to a message using a human-readable label name or ID."""
    return await _client().add_label_by_name(message_id=message_id, label_name=label_name)


@mcp.tool(name="remove_label_by_name")
async def remove_label_by_name(message_id: str, label_name: str) -> dict[str, Any]:
    """Remove a Gmail label from a message using a human-readable label name or ID."""
    return await _client().remove_label_by_name(message_id=message_id, label_name=label_name)


@mcp.tool(name="move_message_to_inbox")
async def move_message_to_inbox(message_id: str) -> dict[str, Any]:
    """Move a Gmail message back into Inbox and remove Spam/Trash labels if present."""
    return await _client().move_message_to_inbox(message_id=message_id)


@mcp.tool(name="move_message_out_of_inbox")
async def move_message_out_of_inbox(message_id: str) -> dict[str, Any]:
    """Move a Gmail message out of Inbox into Archive."""
    return await _client().move_message_out_of_inbox(message_id=message_id)


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
