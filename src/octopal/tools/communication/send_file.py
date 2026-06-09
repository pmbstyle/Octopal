from __future__ import annotations

import json
import mimetypes
import os
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import httpx

from octopal.runtime.octo.delivery import user_delivery_is_suppressed
from octopal.tools.filesystem.path_safety import WorkspacePathError, resolve_workspace_path


def _error(message: str) -> str:
    return json.dumps({"status": "error", "message": message}, ensure_ascii=False)


def _infer_filename_from_url(url: str, content_type: str | None = None) -> str:
    parsed = urlparse(url)
    candidate = Path(unquote(parsed.path or "")).name.strip()
    if candidate:
        return candidate
    extension = mimetypes.guess_extension((content_type or "").split(";", 1)[0].strip()) or ".bin"
    return f"download{extension}"


def _sanitize_filename(filename: str) -> str:
    cleaned = Path(str(filename or "").strip()).name.strip()
    if not cleaned:
        raise ValueError("filename is empty")
    if cleaned in {".", ".."}:
        raise ValueError("filename is invalid")
    return cleaned


def _resolve_existing_workspace_file(base_dir: Path, raw_path: str) -> Path:
    resolved = resolve_workspace_path(base_dir, raw_path, must_exist=True)
    if not resolved.is_file():
        raise WorkspacePathError("path is not a file")
    return resolved


async def _download_to_tmp(
    *,
    base_dir: Path,
    url: str,
    filename: str | None = None,
) -> Path:
    tmp_dir = resolve_workspace_path(base_dir, "tmp/outbound_files")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    async with (
        httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client,
        client.stream("GET", url) as response,
    ):
        response.raise_for_status()
        inferred_name = _sanitize_filename(
            filename or _infer_filename_from_url(url, response.headers.get("content-type"))
        )
        final_name = f"{uuid.uuid4()}_{inferred_name}"
        save_path = resolve_workspace_path(base_dir, f"tmp/outbound_files/{final_name}")
        with open(save_path, "wb") as handle:
            async for chunk in response.aiter_bytes():
                handle.write(chunk)
    return save_path


async def send_file_to_user(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    if user_delivery_is_suppressed():
        return _error("user delivery is suppressed for this continuation")

    octo = ctx.get("octo")
    if octo is None:
        return _error("send_file_to_user requires octo context")

    sender = getattr(octo, "internal_send_file", None)
    if not callable(sender):
        return _error("active user channel does not support file delivery")

    chat_id = int(ctx.get("chat_id", 0) or 0)
    if chat_id == 0:
        return _error("send_file_to_user requires a valid chat_id")

    base_dir = ctx.get("base_dir")
    if not isinstance(base_dir, Path):
        return _error("send_file_to_user requires base_dir context")

    raw_path = str((args or {}).get("path", "") or "").strip()
    raw_url = str((args or {}).get("url", "") or "").strip()
    caption = str((args or {}).get("caption", "") or "").strip() or None
    requested_filename = str((args or {}).get("filename", "") or "").strip() or None

    if bool(raw_path) == bool(raw_url):
        return _error("provide exactly one of 'path' or 'url'")

    try:
        if raw_path:
            file_path = _resolve_existing_workspace_file(base_dir, raw_path)
            source = "path"
        else:
            if urlparse(raw_url).scheme not in {"http", "https"}:
                return _error("url must use http or https")
            file_path = await _download_to_tmp(
                base_dir=base_dir, url=raw_url, filename=requested_filename
            )
            source = "url"
        await sender(chat_id, str(file_path), caption=caption)
        mirror_sender = getattr(octo, "emit_ws_file", None)
        if callable(mirror_sender):
            await mirror_sender(chat_id, str(file_path), caption)
    except WorkspacePathError as exc:
        return _error(f"unsafe file path: {exc}")
    except httpx.HTTPStatusError as exc:
        return _error(f"download failed with HTTP {exc.response.status_code}")
    except ValueError as exc:
        return _error(str(exc))
    except Exception as exc:
        return _error(f"failed to send file: {exc}")

    relative_path = os.path.relpath(file_path, base_dir)
    return json.dumps(
        {
            "status": "success",
            "source": source,
            "path": relative_path.replace("\\", "/"),
            "filename": file_path.name,
            "caption": caption,
        },
        ensure_ascii=False,
    )
