from __future__ import annotations

import base64
from email.parser import BytesParser
from email.policy import default

import httpx

from octopal.mcp_servers.gmail import (
    GmailApiClient,
    _build_raw_message,
    _extract_attachments,
    _extract_body,
    _get_header_value,
    _header_map,
    _normalize_label_key,
    _normalize_message,
    _parse_google_api_error,
    _reply_subject,
)


def _b64(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8")


def test_header_map_keeps_expected_headers_only() -> None:
    headers = _header_map(
        [
            {"name": "Subject", "value": "Hello"},
            {"name": "From", "value": "alice@example.com"},
            {"name": "Message-ID", "value": "ignored"},
        ]
    )

    assert headers == {
        "subject": "Hello",
        "from": "alice@example.com",
    }


def test_extract_body_handles_nested_plain_text_and_html_parts() -> None:
    payload = {
        "mimeType": "multipart/alternative",
        "parts": [
            {"mimeType": "text/plain", "body": {"data": _b64("plain body")}},
            {"mimeType": "text/html", "body": {"data": _b64("<p>html body</p>")}},
        ],
    }

    body = _extract_body(payload)

    assert body["text"] == "plain body"
    assert body["html"] == "<p>html body</p>"


def test_extract_attachments_collects_nested_attachment_metadata() -> None:
    payload = {
        "mimeType": "multipart/mixed",
        "parts": [
            {
                "mimeType": "application/pdf",
                "filename": "invoice.pdf",
                "body": {"attachmentId": "att-1", "size": 1234},
            }
        ],
    }

    attachments = _extract_attachments(payload)

    assert attachments == [
        {
            "filename": "invoice.pdf",
            "mime_type": "application/pdf",
            "size": 1234,
            "attachment_id": "att-1",
        }
    ]


def test_normalize_message_returns_headers_bodies_and_attachments() -> None:
    message = {
        "id": "msg-1",
        "threadId": "thread-1",
        "snippet": "Snippet",
        "labelIds": ["INBOX", "UNREAD"],
        "internalDate": "1711987200000",
        "historyId": "77",
        "sizeEstimate": 2048,
        "payload": {
            "headers": [
                {"name": "Subject", "value": "Status update"},
                {"name": "From", "value": "alice@example.com"},
            ],
            "parts": [
                {"mimeType": "text/plain", "body": {"data": _b64("hello text")}},
                {
                    "mimeType": "application/pdf",
                    "filename": "report.pdf",
                    "body": {"attachmentId": "att-9", "size": 55},
                },
            ],
        },
    }

    normalized = _normalize_message(message)

    assert normalized["id"] == "msg-1"
    assert normalized["thread_id"] == "thread-1"
    assert normalized["headers"]["subject"] == "Status update"
    assert normalized["text_body"] == "hello text"
    assert normalized["attachment_count"] == 1
    assert normalized["attachments"][0]["attachment_id"] == "att-9"


def test_parse_google_api_error_prefers_reason_and_message_from_json_payload() -> None:
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


def test_parse_google_api_error_handles_non_json_responses() -> None:
    response = httpx.Response(403, text="Forbidden")

    error = _parse_google_api_error(response)

    assert error.status_code == 403
    assert error.reason is None
    assert str(error) == "Gmail API 403: Forbidden"


def test_build_raw_message_creates_reply_headers_and_multipart_body() -> None:
    raw = _build_raw_message(
        to=["Alice Example <alice@example.com>"],
        subject="Re: Status",
        body_text="Plain hello",
        body_html="<p>Hello</p>",
        cc=["bob@example.com"],
        in_reply_to="<msg-1@example.com>",
        references=["<older@example.com>", "<msg-1@example.com>"],
    )

    parsed = BytesParser(policy=default).parsebytes(base64.urlsafe_b64decode(raw.encode("ascii")))

    assert parsed["To"] == "Alice Example <alice@example.com>"
    assert parsed["Cc"] == "bob@example.com"
    assert parsed["In-Reply-To"] == "<msg-1@example.com>"
    assert parsed["References"] == "<older@example.com> <msg-1@example.com>"
    assert parsed.get_body(preferencelist=("plain",)).get_content().strip() == "Plain hello"
    assert parsed.get_body(preferencelist=("html",)).get_content().strip() == "<p>Hello</p>"


def test_reply_subject_adds_re_prefix_once() -> None:
    assert _reply_subject("Status update") == "Re: Status update"
    assert _reply_subject("Re: Status update") == "Re: Status update"


def test_get_header_value_matches_case_insensitively() -> None:
    headers = [{"name": "Message-ID", "value": "<msg-1@example.com>"}]

    assert _get_header_value(headers, "message-id") == "<msg-1@example.com>"


def test_normalize_label_key_trims_and_lowercases() -> None:
    assert _normalize_label_key("  Inbox  ") == "inbox"


def test_send_message_posts_raw_payload_and_fetches_metadata() -> None:
    client = object.__new__(GmailApiClient)
    client._credentials = type("_Creds", (), {"valid": True, "token": "token"})()
    requests: list[dict[str, object]] = []

    class _HTTPClient:
        async def request(self, method, path, params=None, json=None, headers=None):
            requests.append(
                {"method": method, "path": path, "params": params, "json": json, "headers": headers}
            )
            if method == "POST":
                return httpx.Response(
                    200,
                    json={"id": "sent-1", "threadId": "thread-1"},
                    request=httpx.Request(method, f"https://example.test{path}"),
                )
            return httpx.Response(
                200,
                json={
                    "id": "sent-1",
                    "threadId": "thread-1",
                    "snippet": "Hello there",
                    "labelIds": ["SENT"],
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Hello"},
                            {"name": "To", "value": "bob@example.com"},
                        ]
                    },
                },
                request=httpx.Request(method, f"https://example.test{path}"),
            )

    client._client = _HTTPClient()

    async def _token() -> str:
        return "token"

    client._access_token = _token  # type: ignore[method-assign]

    import asyncio

    payload = asyncio.run(
        client.send_message(
            to=["bob@example.com"],
            subject="Hello",
            body_text="Hi Bob",
            thread_id="thread-1",
        )
    )

    raw = requests[0]["json"]["raw"]  # type: ignore[index]
    parsed = BytesParser(policy=default).parsebytes(
        base64.urlsafe_b64decode(str(raw).encode("ascii"))
    )

    assert requests[0]["path"] == "/users/me/messages/send"
    assert requests[0]["json"]["threadId"] == "thread-1"  # type: ignore[index]
    assert parsed["To"] == "bob@example.com"
    assert parsed["Subject"] == "Hello"
    assert payload["id"] == "sent-1"
    assert payload["thread_id"] == "thread-1"


def test_reply_to_message_uses_thread_headers_and_reply_all_recipients() -> None:
    client = object.__new__(GmailApiClient)
    client._credentials = type("_Creds", (), {"valid": True, "token": "token"})()
    requests: list[dict[str, object]] = []

    class _HTTPClient:
        async def request(self, method, path, params=None, json=None, headers=None):
            requests.append(
                {"method": method, "path": path, "params": params, "json": json, "headers": headers}
            )
            if method == "GET" and path == "/users/me/messages/original-1":
                return httpx.Response(
                    200,
                    json={
                        "id": "original-1",
                        "threadId": "thread-77",
                        "payload": {
                            "headers": [
                                {"name": "Subject", "value": "Project update"},
                                {"name": "From", "value": "Alice Example <alice@example.com>"},
                                {"name": "Reply-To", "value": "Team Inbox <team@example.com>"},
                                {"name": "To", "value": "me@example.com, Bob <bob@example.com>"},
                                {"name": "Cc", "value": "Carol <carol@example.com>"},
                                {"name": "Message-ID", "value": "<msg-77@example.com>"},
                                {"name": "References", "value": "<root@example.com>"},
                            ]
                        },
                    },
                    request=httpx.Request(method, f"https://example.test{path}"),
                )
            if method == "GET" and path == "/users/me/profile":
                return httpx.Response(
                    200,
                    json={"emailAddress": "me@example.com"},
                    request=httpx.Request(method, f"https://example.test{path}"),
                )
            if method == "POST":
                return httpx.Response(
                    200,
                    json={"id": "reply-1", "threadId": "thread-77"},
                    request=httpx.Request(method, f"https://example.test{path}"),
                )
            return httpx.Response(
                200,
                json={
                    "id": "reply-1",
                    "threadId": "thread-77",
                    "snippet": "Thanks!",
                    "labelIds": ["SENT"],
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Re: Project update"},
                            {
                                "name": "To",
                                "value": "Team Inbox <team@example.com>, Bob <bob@example.com>",
                            },
                            {"name": "Cc", "value": "Carol <carol@example.com>, dave@example.com"},
                        ]
                    },
                },
                request=httpx.Request(method, f"https://example.test{path}"),
            )

    client._client = _HTTPClient()

    async def _token() -> str:
        return "token"

    client._access_token = _token  # type: ignore[method-assign]

    import asyncio

    payload = asyncio.run(
        client.reply_to_message(
            message_id="original-1",
            body_text="Thanks!",
            cc=["dave@example.com"],
            reply_all=True,
        )
    )

    raw = requests[2]["json"]["raw"]  # type: ignore[index]
    parsed = BytesParser(policy=default).parsebytes(
        base64.urlsafe_b64decode(str(raw).encode("ascii"))
    )

    assert requests[2]["path"] == "/users/me/messages/send"
    assert requests[2]["json"]["threadId"] == "thread-77"  # type: ignore[index]
    assert parsed["Subject"] == "Re: Project update"
    assert parsed["In-Reply-To"] == "<msg-77@example.com>"
    assert parsed["References"] == "<root@example.com> <msg-77@example.com>"
    assert parsed["To"] == "Team Inbox <team@example.com>, Bob <bob@example.com>"
    assert parsed["Cc"] == "Carol <carol@example.com>, dave@example.com"
    assert payload["id"] == "reply-1"


def test_archive_mark_and_label_changes_use_modify_endpoint() -> None:
    client = object.__new__(GmailApiClient)
    client._credentials = type("_Creds", (), {"valid": True, "token": "token"})()
    requests: list[dict[str, object]] = []

    class _HTTPClient:
        async def request(self, method, path, params=None, json=None, headers=None):
            requests.append({"method": method, "path": path, "params": params, "json": json})
            return httpx.Response(
                200,
                json={
                    "id": "msg-1",
                    "threadId": "thread-1",
                    "labelIds": ["IMPORTANT"],
                    "payload": {"headers": [{"name": "Subject", "value": "Status"}]},
                },
                request=httpx.Request(method, f"https://example.test{path}"),
            )

    client._client = _HTTPClient()

    async def _token() -> str:
        return "token"

    client._access_token = _token  # type: ignore[method-assign]

    import asyncio

    asyncio.run(client.archive_message(message_id="msg-1"))
    asyncio.run(client.mark_message_read(message_id="msg-1"))
    asyncio.run(client.mark_message_unread(message_id="msg-1"))
    asyncio.run(
        client.modify_message_labels(
            message_id="msg-1",
            add_label_ids=["Label_1"],
            remove_label_ids=["INBOX"],
        )
    )

    assert requests[0]["path"] == "/users/me/messages/msg-1/modify"
    assert requests[0]["json"] == {"removeLabelIds": ["INBOX"]}
    assert requests[1]["json"] == {"removeLabelIds": ["UNREAD"]}
    assert requests[2]["json"] == {"addLabelIds": ["UNREAD"]}
    assert requests[3]["json"] == {"addLabelIds": ["Label_1"], "removeLabelIds": ["INBOX"]}


def test_trash_and_delete_message_use_expected_endpoints() -> None:
    client = object.__new__(GmailApiClient)
    client._credentials = type("_Creds", (), {"valid": True, "token": "token"})()
    requests: list[dict[str, object]] = []

    class _HTTPClient:
        async def request(self, method, path, params=None, json=None, headers=None):
            requests.append({"method": method, "path": path, "params": params, "json": json})
            if method == "POST":
                return httpx.Response(
                    200,
                    json={
                        "id": "msg-1",
                        "threadId": "thread-1",
                        "labelIds": ["TRASH"],
                        "payload": {"headers": [{"name": "Subject", "value": "Status"}]},
                    },
                    request=httpx.Request(method, f"https://example.test{path}"),
                )
            return httpx.Response(
                204,
                request=httpx.Request(method, f"https://example.test{path}"),
            )

    client._client = _HTTPClient()

    async def _token() -> str:
        return "token"

    client._access_token = _token  # type: ignore[method-assign]

    import asyncio

    trashed = asyncio.run(client.trash_message(message_id="msg-1"))
    deleted = asyncio.run(client.delete_message(message_id="msg-1"))

    assert requests[0]["path"] == "/users/me/messages/msg-1/trash"
    assert requests[1]["method"] == "DELETE"
    assert requests[1]["path"] == "/users/me/messages/msg-1"
    assert trashed["label_ids"] == ["TRASH"]
    assert deleted == {"ok": True, "id": "msg-1", "deleted": True}


def test_get_attachment_downloads_and_rehydrates_metadata() -> None:
    client = object.__new__(GmailApiClient)
    client._credentials = type("_Creds", (), {"valid": True, "token": "token"})()

    class _HTTPClient:
        async def request(self, method, path, params=None, json=None, headers=None):
            if path == "/users/me/messages/msg-1/attachments/att-1":
                return httpx.Response(
                    200,
                    json={"size": 5, "data": base64.urlsafe_b64encode(b"hello").decode("ascii")},
                    request=httpx.Request(method, f"https://example.test{path}"),
                )
            return httpx.Response(
                200,
                json={
                    "id": "msg-1",
                    "threadId": "thread-1",
                    "payload": {
                        "parts": [
                            {
                                "mimeType": "text/plain",
                                "body": {"data": _b64("body")},
                            },
                            {
                                "mimeType": "application/pdf",
                                "filename": "invoice.pdf",
                                "body": {"attachmentId": "att-1", "size": 5},
                            },
                        ]
                    },
                },
                request=httpx.Request(method, f"https://example.test{path}"),
            )

    client._client = _HTTPClient()

    async def _token() -> str:
        return "token"

    client._access_token = _token  # type: ignore[method-assign]

    import asyncio

    payload = asyncio.run(client.get_attachment(message_id="msg-1", attachment_id="att-1"))

    assert payload["filename"] == "invoice.pdf"
    assert payload["mime_type"] == "application/pdf"
    assert base64.b64decode(payload["content_base64"].encode("ascii")) == b"hello"


def test_add_remove_label_by_name_and_move_aliases_resolve_expected_labels() -> None:
    client = object.__new__(GmailApiClient)
    client._credentials = type("_Creds", (), {"valid": True, "token": "token"})()
    requests: list[dict[str, object]] = []

    class _HTTPClient:
        async def request(self, method, path, params=None, json=None, headers=None):
            requests.append({"method": method, "path": path, "params": params, "json": json})
            if path == "/users/me/labels":
                return httpx.Response(
                    200,
                    json={
                        "labels": [
                            {"id": "INBOX", "name": "INBOX"},
                            {"id": "Label_7", "name": "Finance"},
                        ]
                    },
                    request=httpx.Request(method, f"https://example.test{path}"),
                )
            return httpx.Response(
                200,
                json={
                    "id": "msg-1",
                    "threadId": "thread-1",
                    "labelIds": ["INBOX", "Label_7"],
                    "payload": {"headers": [{"name": "Subject", "value": "Status"}]},
                },
                request=httpx.Request(method, f"https://example.test{path}"),
            )

    client._client = _HTTPClient()

    async def _token() -> str:
        return "token"

    client._access_token = _token  # type: ignore[method-assign]

    import asyncio

    added = asyncio.run(client.add_label_by_name(message_id="msg-1", label_name="finance"))
    removed = asyncio.run(client.remove_label_by_name(message_id="msg-1", label_name="Label_7"))
    moved_in = asyncio.run(client.move_message_to_inbox(message_id="msg-1"))
    moved_out = asyncio.run(client.move_message_out_of_inbox(message_id="msg-1"))

    assert requests[1]["json"] == {"addLabelIds": ["Label_7"], "removeLabelIds": []}
    assert requests[3]["json"] == {"addLabelIds": [], "removeLabelIds": ["Label_7"]}
    assert requests[4]["json"] == {"addLabelIds": ["INBOX"], "removeLabelIds": ["SPAM", "TRASH"]}
    assert requests[5]["json"] == {"removeLabelIds": ["INBOX"]}
    assert added["resolved_label_id"] == "Label_7"
    assert removed["resolved_label_id"] == "Label_7"
    assert moved_in["moved_to"] == "INBOX"
    assert moved_out["moved_to"] == "ARCHIVE"
