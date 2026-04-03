from __future__ import annotations

import base64
import json
import mimetypes
from typing import Any

from octopal.tools.filesystem.files import _get_paths, _resolve_tool_path
from octopal.tools.metadata import ToolMetadata
from octopal.tools.registry import ToolSpec

_DRIVE_SERVER_ID = "google-drive"


def _extract_mcp_payload(result: Any) -> Any:
    content_items = getattr(result, "content", None)
    if not content_items:
        return result

    if len(content_items) == 1:
        item = content_items[0]
        text = getattr(item, "text", None)
        if isinstance(text, str):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        if hasattr(item, "model_dump"):
            return item.model_dump()
        return str(item)

    normalized: list[Any] = []
    for item in content_items:
        text = getattr(item, "text", None)
        if isinstance(text, str):
            try:
                normalized.append(json.loads(text))
            except json.JSONDecodeError:
                normalized.append(text)
            continue
        if hasattr(item, "model_dump"):
            normalized.append(item.model_dump())
            continue
        normalized.append(str(item))
    return normalized


def _resolve_mcp_manager(ctx: dict[str, Any], fallback: Any) -> Any:
    octo = (ctx or {}).get("octo")
    if octo is not None and getattr(octo, "mcp_manager", None) is not None:
        return octo.mcp_manager
    return fallback


async def _drive_mcp_proxy(
    remote_tool_name: str,
    args: dict[str, Any],
    ctx: dict[str, Any],
    *,
    fallback_manager: Any,
) -> Any:
    manager = _resolve_mcp_manager(ctx, fallback_manager)
    if manager is None:
        return {
            "ok": False,
            "error": "Drive tools are unavailable because no MCP manager is active.",
            "hint": "Restart Octopal after authorizing the Google Drive connector.",
        }

    try:
        result = await manager.call_tool(
            _DRIVE_SERVER_ID,
            remote_tool_name,
            args or {},
            allow_name_fallback=True,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "server_id": _DRIVE_SERVER_ID,
            "tool": remote_tool_name,
            "hint": "Check connector status and confirm the Google Drive MCP server is connected.",
        }

    return _extract_mcp_payload(result)


def _drive_tool(
    *,
    name: str,
    remote_tool_name: str,
    description: str,
    parameters: dict[str, Any],
    fallback_manager: Any,
    capabilities: tuple[str, ...],
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=description,
        parameters=parameters,
        permission="mcp_exec",
        handler=lambda args, ctx, _remote=remote_tool_name, _manager=fallback_manager: _drive_mcp_proxy(
            _remote,
            args,
            ctx,
            fallback_manager=_manager,
        ),
        is_async=True,
        server_id=_DRIVE_SERVER_ID,
        remote_tool_name=remote_tool_name,
        metadata=ToolMetadata(
            category="connectors",
            risk="safe",
            profile_tags=("research", "execution"),
            capabilities=capabilities,
        ),
    )


async def drive_download_to_workspace(
    args: dict[str, Any],
    ctx: dict[str, Any],
    *,
    fallback_manager: Any = None,
) -> dict[str, Any]:
    manager = _resolve_mcp_manager(ctx, fallback_manager)
    if manager is None:
        return {
            "ok": False,
            "error": "Drive tools are unavailable because no MCP manager is active.",
            "hint": "Restart Octopal after authorizing the Google Drive connector.",
        }

    path = str((args or {}).get("path", "") or "").strip()
    file_id = str((args or {}).get("file_id", "") or "").strip()
    export_mime_type = str((args or {}).get("export_mime_type", "") or "").strip() or None
    if not path:
        return {"ok": False, "error": "path is required."}
    if not file_id:
        return {"ok": False, "error": "file_id is required."}

    remote_tool_name = "export_google_doc" if export_mime_type else "download_file"
    remote_args = {"file_id": file_id}
    if export_mime_type:
        remote_args["export_mime_type"] = export_mime_type
    payload = await _drive_mcp_proxy(remote_tool_name, remote_args, ctx, fallback_manager=manager)
    if not isinstance(payload, dict) or payload.get("ok") is False:
        return payload

    workspace_root, worker_dir, allowed_paths = _get_paths(ctx)
    try:
        target = _resolve_tool_path(
            path,
            workspace_root=workspace_root,
            worker_dir=worker_dir,
            allowed_paths=allowed_paths,
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    target.parent.mkdir(parents=True, exist_ok=True)
    raw_content = base64.b64decode(str(payload.get("content_base64", "")).encode("ascii"))
    target.write_bytes(raw_content)

    try:
        relative_path = str(target.relative_to(workspace_root))
    except ValueError:
        relative_path = str(target.relative_to(worker_dir))

    return {
        "ok": True,
        "path": relative_path,
        "bytes_written": len(raw_content),
        "file": payload.get("file"),
        "export_mime_type": payload.get("export_mime_type"),
    }


async def drive_upload_from_workspace(
    args: dict[str, Any],
    ctx: dict[str, Any],
    *,
    fallback_manager: Any = None,
) -> dict[str, Any]:
    manager = _resolve_mcp_manager(ctx, fallback_manager)
    if manager is None:
        return {
            "ok": False,
            "error": "Drive tools are unavailable because no MCP manager is active.",
            "hint": "Restart Octopal after authorizing the Google Drive connector.",
        }

    path = str((args or {}).get("path", "") or "").strip()
    if not path:
        return {"ok": False, "error": "path is required."}

    workspace_root, worker_dir, allowed_paths = _get_paths(ctx)
    try:
        source = _resolve_tool_path(
            path,
            workspace_root=workspace_root,
            worker_dir=worker_dir,
            allowed_paths=allowed_paths,
            must_exist=True,
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    content = source.read_bytes()
    upload_args = {
        "name": str((args or {}).get("name", "") or source.name),
        "content_base64": base64.b64encode(content).decode("ascii"),
        "mime_type": str(
            (args or {}).get("mime_type", "")
            or mimetypes.guess_type(source.name)[0]
            or "application/octet-stream"
        ),
    }
    parent_id = str((args or {}).get("parent_id", "") or "").strip()
    if parent_id:
        upload_args["parent_id"] = parent_id

    payload = await _drive_mcp_proxy("upload_file", upload_args, ctx, fallback_manager=manager)
    if isinstance(payload, dict):
        payload.setdefault("uploaded_from", path)
        payload.setdefault("bytes_read", len(content))
    return payload


def get_drive_connector_tools(mcp_manager: Any = None) -> list[ToolSpec]:
    if mcp_manager is None:
        return []

    return [
        _drive_tool(
            name="drive_list_files",
            remote_tool_name="list_files",
            description="List recent Google Drive files, optionally filtered by Drive query syntax.",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page_token": {"type": "string"},
                    "corpora": {"type": "string"},
                },
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("drive_read", "connector_use"),
        ),
        _drive_tool(
            name="drive_search_files",
            remote_tool_name="search_files",
            description="Search Google Drive files using Drive query syntax.",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page_token": {"type": "string"},
                    "corpora": {"type": "string"},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("drive_read", "connector_use"),
        ),
        _drive_tool(
            name="drive_get_file",
            remote_tool_name="get_file",
            description="Read Google Drive file metadata by file ID.",
            parameters={
                "type": "object",
                "properties": {"file_id": {"type": "string"}},
                "required": ["file_id"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("drive_read", "connector_use"),
        ),
        _drive_tool(
            name="drive_create_folder",
            remote_tool_name="create_folder",
            description="Create a new folder in Google Drive.",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "parent_id": {"type": "string"},
                },
                "required": ["name"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("drive_write", "connector_use"),
        ),
        _drive_tool(
            name="drive_download_file_content",
            remote_tool_name="download_file",
            description="Download raw file content from Google Drive and return it as base64.",
            parameters={
                "type": "object",
                "properties": {"file_id": {"type": "string"}},
                "required": ["file_id"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("drive_read", "connector_use"),
        ),
        _drive_tool(
            name="drive_export_google_doc",
            remote_tool_name="export_google_doc",
            description="Export a Google Docs-native file to another mime type and return it as base64.",
            parameters={
                "type": "object",
                "properties": {
                    "file_id": {"type": "string"},
                    "export_mime_type": {"type": "string"},
                },
                "required": ["file_id", "export_mime_type"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("drive_read", "connector_use"),
        ),
        _drive_tool(
            name="drive_upload_file_content",
            remote_tool_name="upload_file",
            description="Upload a new file to Google Drive from base64 content.",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "content_base64": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "parent_id": {"type": "string"},
                },
                "required": ["name", "content_base64"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("drive_write", "connector_use"),
        ),
        ToolSpec(
            name="drive_download_to_workspace",
            description="Download a Google Drive file into the workspace or worker scratch directory.",
            parameters={
                "type": "object",
                "properties": {
                    "file_id": {"type": "string"},
                    "path": {"type": "string"},
                    "export_mime_type": {"type": "string"},
                },
                "required": ["file_id", "path"],
                "additionalProperties": False,
            },
            permission="filesystem_write",
            handler=lambda args, ctx, _manager=mcp_manager: drive_download_to_workspace(
                args,
                ctx,
                fallback_manager=_manager,
            ),
            is_async=True,
            metadata=ToolMetadata(
                category="connectors",
                risk="guarded",
                profile_tags=("execution",),
                capabilities=("drive_read", "filesystem_write", "connector_use"),
            ),
        ),
        ToolSpec(
            name="drive_upload_from_workspace",
            description="Upload a workspace file to Google Drive.",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "name": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "parent_id": {"type": "string"},
                },
                "required": ["path"],
                "additionalProperties": False,
            },
            permission="filesystem_read",
            handler=lambda args, ctx, _manager=mcp_manager: drive_upload_from_workspace(
                args,
                ctx,
                fallback_manager=_manager,
            ),
            is_async=True,
            metadata=ToolMetadata(
                category="connectors",
                risk="guarded",
                profile_tags=("execution",),
                capabilities=("drive_write", "filesystem_read", "connector_use"),
            ),
        ),
    ]
