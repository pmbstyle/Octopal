from __future__ import annotations

import asyncio
import base64
import json
import os
import uuid
from typing import TYPE_CHECKING, Any

import httpx
from mcp.server import FastMCP

_DRIVE_API_BASE_URL = "https://www.googleapis.com/drive/v3"
_DRIVE_UPLOAD_API_BASE_URL = "https://www.googleapis.com/upload/drive/v3"
_DEFAULT_SCOPES = ("https://www.googleapis.com/auth/drive",)
_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

if TYPE_CHECKING:
    from google.oauth2.credentials import Credentials


class DriveConfigError(RuntimeError):
    """Raised when required Google Drive MCP configuration is missing."""


class DriveApiError(RuntimeError):
    """Raised when Drive API returns a structured error response."""

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
        parts = [f"Drive API {status_code}"]
        if reason:
            parts.append(reason)
        parts.append(message)
        super().__init__(": ".join(parts))


def _parse_google_api_error(response: httpx.Response) -> DriveApiError:
    payload: dict[str, Any] = {}
    try:
        payload = response.json()
    except json.JSONDecodeError:
        text = response.text.strip() or response.reason_phrase
        return DriveApiError(
            status_code=response.status_code,
            reason=None,
            message=text,
            details={},
        )

    error = payload.get("error")
    if isinstance(error, dict):
        message = str(error.get("message") or response.reason_phrase or "Unknown Drive API error").strip()
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
        return DriveApiError(
            status_code=response.status_code,
            reason=reason,
            message=message,
            details=error,
        )

    text = response.text.strip() or response.reason_phrase
    return DriveApiError(
        status_code=response.status_code,
        reason=None,
        message=text,
        details=payload if isinstance(payload, dict) else {},
    )


def _normalize_file(item: dict[str, Any]) -> dict[str, Any]:
    capabilities = item.get("capabilities") or {}
    owners = item.get("owners") or []
    permissions = item.get("permissions") or []
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "mime_type": item.get("mimeType"),
        "description": item.get("description"),
        "parents": item.get("parents") or [],
        "size": item.get("size"),
        "created_time": item.get("createdTime"),
        "modified_time": item.get("modifiedTime"),
        "trashed": item.get("trashed", False),
        "web_view_link": item.get("webViewLink"),
        "web_content_link": item.get("webContentLink"),
        "md5_checksum": item.get("md5Checksum"),
        "owners": [
            {
                "display_name": owner.get("displayName"),
                "email_address": owner.get("emailAddress"),
            }
            for owner in owners
        ],
        "permissions": [
            {
                "id": permission.get("id"),
                "type": permission.get("type"),
                "role": permission.get("role"),
            }
            for permission in permissions
        ],
        "capabilities": {
            "can_download": capabilities.get("canDownload"),
            "can_edit": capabilities.get("canEdit"),
            "can_share": capabilities.get("canShare"),
            "can_trash": capabilities.get("canTrash"),
        },
    }


def _multipart_related_body(metadata: dict[str, Any], content: bytes, mime_type: str) -> tuple[bytes, str]:
    boundary = f"octopal-drive-{uuid.uuid4().hex}"
    parts = [
        f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".encode("utf-8"),
        json.dumps(metadata, ensure_ascii=False).encode("utf-8"),
        f"\r\n--{boundary}\r\nContent-Type: {mime_type}\r\n\r\n".encode("utf-8"),
        content,
        f"\r\n--{boundary}--\r\n".encode("utf-8"),
    ]
    return b"".join(parts), boundary


class DriveApiClient:
    _FILE_FIELDS = (
        "id,name,mimeType,description,parents,size,createdTime,modifiedTime,trashed,"
        "webViewLink,webContentLink,md5Checksum,owners(displayName,emailAddress),"
        "permissions(id,type,role),capabilities(canDownload,canEdit,canShare,canTrash)"
    )

    def __init__(self) -> None:
        self._credentials = self._load_credentials()
        self._client = httpx.AsyncClient(base_url=_DRIVE_API_BASE_URL, timeout=60.0)
        self._upload_client = httpx.AsyncClient(base_url=_DRIVE_UPLOAD_API_BASE_URL, timeout=120.0)

    def _load_credentials(self) -> Credentials:
        from google.oauth2.credentials import Credentials

        client_id = os.getenv("GOOGLE_DRIVE_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_DRIVE_CLIENT_SECRET")
        refresh_token = os.getenv("GOOGLE_DRIVE_REFRESH_TOKEN")
        if not client_id or not client_secret or not refresh_token:
            raise DriveConfigError(
                "Missing Drive MCP credentials. Expected GOOGLE_DRIVE_CLIENT_ID, "
                "GOOGLE_DRIVE_CLIENT_SECRET, and GOOGLE_DRIVE_REFRESH_TOKEN."
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
        await self._upload_client.aclose()

    async def _access_token(self) -> str:
        from google.auth.transport.requests import Request

        if not self._credentials.valid:
            await asyncio.to_thread(self._credentials.refresh, Request())
        token = self._credentials.token
        if not token:
            raise RuntimeError("Failed to refresh Drive access token.")
        return token

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        client: httpx.AsyncClient | None = None,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
    ) -> Any:
        active_client = client or self._client
        token = await self._access_token()
        request_headers = {"Authorization": f"Bearer {token}"}
        if headers:
            request_headers.update(headers)
        response = await active_client.request(
            method,
            path,
            params=params,
            json=json_body,
            headers=request_headers,
            content=content,
        )
        if response.status_code == 401:
            self._credentials.token = None
            token = await self._access_token()
            request_headers["Authorization"] = f"Bearer {token}"
            response = await active_client.request(
                method,
                path,
                params=params,
                json=json_body,
                headers=request_headers,
                content=content,
            )
        if response.is_error:
            raise _parse_google_api_error(response)
        if not response.content or response.status_code == 204:
            return {}
        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            return response.json()
        return response.content

    async def list_files(
        self,
        *,
        query: str | None = None,
        page_size: int = 25,
        page_token: str | None = None,
        corpora: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "pageSize": max(1, min(page_size, 100)),
            "fields": f"files({self._FILE_FIELDS}),nextPageToken",
            "orderBy": "modifiedTime desc",
        }
        if query:
            params["q"] = query
        if page_token:
            params["pageToken"] = page_token
        if corpora:
            params["corpora"] = corpora
        payload = await self._request("GET", "/files", params=params)
        return {
            "files": [_normalize_file(item) for item in (payload.get("files") or [])],
            "next_page_token": payload.get("nextPageToken"),
        }

    async def search_files(
        self,
        *,
        query: str,
        page_size: int = 25,
        page_token: str | None = None,
        corpora: str | None = None,
    ) -> dict[str, Any]:
        return await self.list_files(query=query, page_size=page_size, page_token=page_token, corpora=corpora)

    async def get_file(self, file_id: str) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            f"/files/{file_id}",
            params={"fields": self._FILE_FIELDS},
        )
        return _normalize_file(payload)

    async def create_folder(self, *, name: str, parent_id: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"name": name, "mimeType": _FOLDER_MIME_TYPE}
        if parent_id:
            body["parents"] = [parent_id]
        payload = await self._request(
            "POST",
            "/files",
            params={"fields": self._FILE_FIELDS},
            json_body=body,
        )
        return _normalize_file(payload)

    async def download_file(self, file_id: str) -> dict[str, Any]:
        metadata = await self.get_file(file_id)
        content = await self._request(
            "GET",
            f"/files/{file_id}",
            params={"alt": "media"},
        )
        if not isinstance(content, (bytes, bytearray)):
            raise RuntimeError("Expected binary file content from Drive download.")
        return {
            "file": metadata,
            "content_base64": base64.b64encode(bytes(content)).decode("ascii"),
            "size": len(content),
        }

    async def export_google_doc(self, *, file_id: str, export_mime_type: str) -> dict[str, Any]:
        metadata = await self.get_file(file_id)
        content = await self._request(
            "GET",
            f"/files/{file_id}/export",
            params={"mimeType": export_mime_type},
        )
        if not isinstance(content, (bytes, bytearray)):
            raise RuntimeError("Expected binary file content from Drive export.")
        return {
            "file": metadata,
            "export_mime_type": export_mime_type,
            "content_base64": base64.b64encode(bytes(content)).decode("ascii"),
            "size": len(content),
        }

    async def upload_file(
        self,
        *,
        name: str,
        content_base64: str,
        mime_type: str = "application/octet-stream",
        parent_id: str | None = None,
    ) -> dict[str, Any]:
        content = base64.b64decode(content_base64.encode("ascii"))
        metadata: dict[str, Any] = {"name": name}
        if parent_id:
            metadata["parents"] = [parent_id]
        body, boundary = _multipart_related_body(metadata, content, mime_type)
        payload = await self._request(
            "POST",
            "/files",
            client=self._upload_client,
            params={"uploadType": "multipart", "fields": self._FILE_FIELDS},
            headers={"Content-Type": f'multipart/related; boundary="{boundary}"'},
            content=body,
        )
        return _normalize_file(payload)


mcp = FastMCP(
    name="Octopal Google Drive",
    instructions=(
        "Use these tools to inspect Google Drive files, download or export file content, "
        "upload new files, and create folders in the connected Drive account."
    ),
    log_level="ERROR",
)

_drive_client: DriveApiClient | None = None


def _client() -> DriveApiClient:
    global _drive_client
    if _drive_client is None:
        _drive_client = DriveApiClient()
    return _drive_client


@mcp.tool(name="list_files")
async def list_files(
    query: str | None = None,
    page_size: int = 25,
    page_token: str | None = None,
    corpora: str | None = None,
) -> dict[str, Any]:
    """List Drive files, optionally filtered by a Drive query."""
    return await _client().list_files(
        query=query,
        page_size=page_size,
        page_token=page_token,
        corpora=corpora,
    )


@mcp.tool(name="search_files")
async def search_files(
    query: str,
    page_size: int = 25,
    page_token: str | None = None,
    corpora: str | None = None,
) -> dict[str, Any]:
    """Search Drive files using Google Drive query syntax."""
    return await _client().search_files(
        query=query,
        page_size=page_size,
        page_token=page_token,
        corpora=corpora,
    )


@mcp.tool(name="get_file")
async def get_file(file_id: str) -> dict[str, Any]:
    """Return Drive file metadata by file ID."""
    return await _client().get_file(file_id)


@mcp.tool(name="create_folder")
async def create_folder(name: str, parent_id: str | None = None) -> dict[str, Any]:
    """Create a folder in Google Drive."""
    return await _client().create_folder(name=name, parent_id=parent_id)


@mcp.tool(name="download_file")
async def download_file(file_id: str) -> dict[str, Any]:
    """Download a Drive file and return base64-encoded content."""
    return await _client().download_file(file_id)


@mcp.tool(name="export_google_doc")
async def export_google_doc(file_id: str, export_mime_type: str) -> dict[str, Any]:
    """Export a Google Docs-native file to another mime type."""
    return await _client().export_google_doc(file_id=file_id, export_mime_type=export_mime_type)


@mcp.tool(name="upload_file")
async def upload_file(
    name: str,
    content_base64: str,
    mime_type: str = "application/octet-stream",
    parent_id: str | None = None,
) -> dict[str, Any]:
    """Upload a new file to Google Drive from base64-encoded content."""
    return await _client().upload_file(
        name=name,
        content_base64=content_base64,
        mime_type=mime_type,
        parent_id=parent_id,
    )


def main() -> None:
    try:
        mcp.run()
    finally:
        try:
            if _drive_client is not None:
                asyncio.run(_drive_client.close())
        except Exception:
            pass


if __name__ == "__main__":
    main()
