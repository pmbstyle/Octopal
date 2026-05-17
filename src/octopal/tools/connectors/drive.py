from __future__ import annotations

import base64
import json
import mimetypes
from typing import Any

from octopal.tools.filesystem.files import _get_paths, _resolve_tool_path
from octopal.tools.metadata import ToolMetadata
from octopal.tools.registry import ToolSpec

_DRIVE_SERVER_ID = "google-drive"


def _encode_text_content(content: str, encoding: str) -> str:
    return base64.b64encode(str(content).encode(encoding)).decode("ascii")


def _decode_text_content(content_base64: str, encoding: str) -> str:
    return base64.b64decode(content_base64.encode("ascii")).decode(encoding)


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

    paths = _get_paths(ctx)
    try:
        target = _resolve_tool_path(
            path,
            workspace_root=paths.workspace_root,
            worker_dir=paths.worker_dir,
            allowed_paths=paths.allowed_paths,
            restrict_to_allowed_paths=paths.restrict_to_allowed_paths,
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    target.parent.mkdir(parents=True, exist_ok=True)
    raw_content = base64.b64decode(str(payload.get("content_base64", "")).encode("ascii"))
    target.write_bytes(raw_content)

    try:
        relative_path = str(target.relative_to(paths.workspace_root))
    except ValueError:
        relative_path = str(target.relative_to(paths.worker_dir))

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

    paths = _get_paths(ctx)
    try:
        source = _resolve_tool_path(
            path,
            workspace_root=paths.workspace_root,
            worker_dir=paths.worker_dir,
            allowed_paths=paths.allowed_paths,
            restrict_to_allowed_paths=paths.restrict_to_allowed_paths,
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


async def drive_update_from_workspace(
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
    if not path:
        return {"ok": False, "error": "path is required."}
    if not file_id:
        return {"ok": False, "error": "file_id is required."}

    paths = _get_paths(ctx)
    try:
        source = _resolve_tool_path(
            path,
            workspace_root=paths.workspace_root,
            worker_dir=paths.worker_dir,
            allowed_paths=paths.allowed_paths,
            restrict_to_allowed_paths=paths.restrict_to_allowed_paths,
            must_exist=True,
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    content = source.read_bytes()
    update_args = {
        "file_id": file_id,
        "content_base64": base64.b64encode(content).decode("ascii"),
        "mime_type": str(
            (args or {}).get("mime_type", "")
            or mimetypes.guess_type(source.name)[0]
            or "application/octet-stream"
        ),
    }
    name = str((args or {}).get("name", "") or "").strip()
    if name:
        update_args["name"] = name

    payload = await _drive_mcp_proxy("update_file", update_args, ctx, fallback_manager=manager)
    if isinstance(payload, dict):
        payload.setdefault("updated_from", path)
        payload.setdefault("bytes_read", len(content))
    return payload


async def drive_create_text_file(
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

    name = str((args or {}).get("name", "") or "").strip()
    content = str((args or {}).get("content", "") or "")
    encoding = str((args or {}).get("encoding", "") or "utf-8").strip() or "utf-8"
    mime_type = str((args or {}).get("mime_type", "") or "text/plain").strip() or "text/plain"
    if not name:
        return {"ok": False, "error": "name is required."}

    upload_args = {
        "name": name,
        "content_base64": _encode_text_content(content, encoding),
        "mime_type": mime_type,
    }
    parent_id = str((args or {}).get("parent_id", "") or "").strip()
    if parent_id:
        upload_args["parent_id"] = parent_id

    payload = await _drive_mcp_proxy("upload_file", upload_args, ctx, fallback_manager=manager)
    if isinstance(payload, dict):
        payload.setdefault("encoding", encoding)
        payload.setdefault("text_length", len(content))
    return payload


async def drive_update_text_file(
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

    file_id = str((args or {}).get("file_id", "") or "").strip()
    content = str((args or {}).get("content", "") or "")
    encoding = str((args or {}).get("encoding", "") or "utf-8").strip() or "utf-8"
    mime_type = str((args or {}).get("mime_type", "") or "text/plain").strip() or "text/plain"
    if not file_id:
        return {"ok": False, "error": "file_id is required."}

    update_args = {
        "file_id": file_id,
        "content_base64": _encode_text_content(content, encoding),
        "mime_type": mime_type,
    }
    name = str((args or {}).get("name", "") or "").strip()
    if name:
        update_args["name"] = name

    payload = await _drive_mcp_proxy("update_file", update_args, ctx, fallback_manager=manager)
    if isinstance(payload, dict):
        payload.setdefault("encoding", encoding)
        payload.setdefault("text_length", len(content))
    return payload


async def drive_read_text_file(
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

    file_id = str((args or {}).get("file_id", "") or "").strip()
    encoding = str((args or {}).get("encoding", "") or "utf-8").strip() or "utf-8"
    export_mime_type = str((args or {}).get("export_mime_type", "") or "").strip() or None
    if not file_id:
        return {"ok": False, "error": "file_id is required."}

    remote_tool_name = "export_google_doc" if export_mime_type else "download_file"
    remote_args = {"file_id": file_id}
    if export_mime_type:
        remote_args["export_mime_type"] = export_mime_type

    payload = await _drive_mcp_proxy(remote_tool_name, remote_args, ctx, fallback_manager=manager)
    if not isinstance(payload, dict) or payload.get("ok") is False:
        return payload

    try:
        text = _decode_text_content(str(payload.get("content_base64", "")), encoding)
    except Exception as exc:
        return {
            "ok": False,
            "error": f"Failed to decode Drive file as text: {exc}",
            "file": payload.get("file"),
            "export_mime_type": payload.get("export_mime_type"),
        }

    return {
        "ok": True,
        "file": payload.get("file"),
        "content": text,
        "encoding": encoding,
        "text_length": len(text),
        "export_mime_type": payload.get("export_mime_type"),
    }


async def drive_upload_and_get_link(
    args: dict[str, Any],
    ctx: dict[str, Any],
    *,
    fallback_manager: Any = None,
) -> dict[str, Any]:
    payload = await drive_upload_from_workspace(args, ctx, fallback_manager=fallback_manager)
    if not isinstance(payload, dict):
        return {"ok": False, "error": "Unexpected upload response from Drive connector."}
    if payload.get("ok") is False:
        return payload

    web_view_link = payload.get("web_view_link")
    file_id = payload.get("id")
    return {
        "ok": True,
        "file_id": file_id,
        "name": payload.get("name"),
        "web_view_link": web_view_link,
        "uploaded_from": payload.get("uploaded_from"),
        "bytes_read": payload.get("bytes_read"),
        "note": (
            "This link uses the file's existing Drive permissions. "
            "No sharing settings were changed."
        ),
    }


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
            name="drive_list_children",
            remote_tool_name="list_children",
            description="List files directly inside a Drive folder.",
            parameters={
                "type": "object",
                "properties": {
                    "parent_id": {"type": "string"},
                    "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
                    "page_token": {"type": "string"},
                },
                "required": ["parent_id"],
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
            name="drive_trash_file",
            remote_tool_name="trash_file",
            description="Move a Drive file to trash.",
            parameters={
                "type": "object",
                "properties": {"file_id": {"type": "string"}},
                "required": ["file_id"],
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
        _drive_tool(
            name="drive_update_file_content",
            remote_tool_name="update_file",
            description="Update an existing Drive file from base64 content.",
            parameters={
                "type": "object",
                "properties": {
                    "file_id": {"type": "string"},
                    "content_base64": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "name": {"type": "string"},
                },
                "required": ["file_id", "content_base64"],
                "additionalProperties": False,
            },
            fallback_manager=mcp_manager,
            capabilities=("drive_write", "connector_use"),
        ),
        ToolSpec(
            name="drive_create_text_file",
            description="Create a text file in Google Drive without manually encoding content as base64.",
            parameters={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "content": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "encoding": {"type": "string"},
                    "parent_id": {"type": "string"},
                },
                "required": ["name", "content"],
                "additionalProperties": False,
            },
            permission="mcp_exec",
            handler=lambda args, ctx, _manager=mcp_manager: drive_create_text_file(
                args,
                ctx,
                fallback_manager=_manager,
            ),
            is_async=True,
            metadata=ToolMetadata(
                category="connectors",
                risk="safe",
                profile_tags=("execution", "writing"),
                capabilities=("drive_write", "connector_use"),
            ),
        ),
        ToolSpec(
            name="drive_update_text_file",
            description="Update an existing text file in Google Drive without manually encoding content as base64.",
            parameters={
                "type": "object",
                "properties": {
                    "file_id": {"type": "string"},
                    "content": {"type": "string"},
                    "name": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "encoding": {"type": "string"},
                },
                "required": ["file_id", "content"],
                "additionalProperties": False,
            },
            permission="mcp_exec",
            handler=lambda args, ctx, _manager=mcp_manager: drive_update_text_file(
                args,
                ctx,
                fallback_manager=_manager,
            ),
            is_async=True,
            metadata=ToolMetadata(
                category="connectors",
                risk="safe",
                profile_tags=("execution", "writing"),
                capabilities=("drive_write", "connector_use"),
            ),
        ),
        ToolSpec(
            name="drive_read_text_file",
            description="Read a Drive file as text, optionally exporting a Google Docs-native file first.",
            parameters={
                "type": "object",
                "properties": {
                    "file_id": {"type": "string"},
                    "encoding": {"type": "string"},
                    "export_mime_type": {"type": "string"},
                },
                "required": ["file_id"],
                "additionalProperties": False,
            },
            permission="mcp_exec",
            handler=lambda args, ctx, _manager=mcp_manager: drive_read_text_file(
                args,
                ctx,
                fallback_manager=_manager,
            ),
            is_async=True,
            metadata=ToolMetadata(
                category="connectors",
                risk="safe",
                profile_tags=("research", "writing"),
                capabilities=("drive_read", "connector_use"),
            ),
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
        ToolSpec(
            name="drive_upload_and_get_link",
            description="Upload a workspace file to Google Drive and return its Drive view link without changing permissions.",
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
            handler=lambda args, ctx, _manager=mcp_manager: drive_upload_and_get_link(
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
        ToolSpec(
            name="drive_update_from_workspace",
            description="Update an existing Google Drive file from a workspace file.",
            parameters={
                "type": "object",
                "properties": {
                    "file_id": {"type": "string"},
                    "path": {"type": "string"},
                    "name": {"type": "string"},
                    "mime_type": {"type": "string"},
                },
                "required": ["file_id", "path"],
                "additionalProperties": False,
            },
            permission="filesystem_read",
            handler=lambda args, ctx, _manager=mcp_manager: drive_update_from_workspace(
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
