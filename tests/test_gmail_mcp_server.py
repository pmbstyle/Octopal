from __future__ import annotations

import base64

from octopal.mcp_servers.gmail import (
    _extract_attachments,
    _extract_body,
    _header_map,
    _normalize_message,
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
