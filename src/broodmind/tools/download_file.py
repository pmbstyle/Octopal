from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import httpx

from broodmind.tools.path_safety import WorkspacePathError, resolve_workspace_path


async def download_file(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    """
    Downloads a file from a URL and saves it to the appropriate downloads directory.
    """
    url = args.get("url")
    if not url or not isinstance(url, str):
        return json.dumps({"error": "download_file error: a valid 'url' string is required."})

    filename = args.get("filename")
    if not filename or not isinstance(filename, str):
        # If no filename is provided, try to get it from the URL
        try:
            filename = os.path.basename(url.split("?")[0])
            if not filename:
                return json.dumps({"error": "download_file error: could not determine filename from URL. Please specify one."})
        except Exception:
            return json.dumps({"error": "download_file error: could not determine filename from URL. Please specify one."})
    filename = filename.strip()
    if not filename:
        return json.dumps({"error": "download_file error: filename is empty."})
    if Path(filename).name != filename:
        return json.dumps({"error": "download_file error: filename must not contain directory components."})

    # Determine the base directory from the context
    # The 'base_dir' in the context is the root workspace for the Queen.
    # A worker should have a more specific 'worker_dir' in its context if it differs.
    # For now, we'll assume a common 'downloads' folder in the main workspace.
    base_dir: Path = ctx.get("base_dir")
    if not base_dir:
        return json.dumps({"error": "download_file error: base_dir not found in context."})

    try:
        download_dir = resolve_workspace_path(base_dir, "downloads")
        download_dir.mkdir(parents=True, exist_ok=True)
        download_dir = resolve_workspace_path(base_dir, "downloads", must_exist=True)
    except Exception as e:
        return json.dumps({"error": f"download_file error: could not create download directory: {e}"})
    try:
        save_path = resolve_workspace_path(base_dir, f"downloads/{filename}")
    except WorkspacePathError as e:
        return json.dumps({"error": f"download_file error: {e}."})

    try:
        async with httpx.AsyncClient() as client, client.stream("GET", url, follow_redirects=True, timeout=30.0) as response:
            response.raise_for_status()

            with open(save_path, "wb") as f:
                async for chunk in response.aiter_bytes():
                    f.write(chunk)

        file_size = save_path.stat().st_size
        return json.dumps({
            "status": "success",
            "path": str(save_path.relative_to(base_dir)),
            "size": file_size,
            "message": f"Successfully downloaded {file_size} bytes to {save_path.relative_to(base_dir)}"
        })

    except httpx.HTTPStatusError as e:
        return json.dumps({"error": f"download_file error: HTTP error {e.response.status_code} for URL {url}"})
    except Exception as e:
        return json.dumps({"error": f"download_file error: An unexpected error occurred: {e}"})
