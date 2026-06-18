from __future__ import annotations

import base64
import uuid

from octopal.runtime.octo.route_loop_helpers import _exception_chain_text
from octopal.runtime.octo.workspace_paths import _workspace_dir


def _is_vision_tool_compatibility_error(exc: Exception) -> bool:
    err = str(exc).lower()
    return "invalid api parameter" in err or "'code': '1210'" in err or '"code": "1210"' in err


def _is_invalid_tool_payload_error(exc: Exception) -> bool:
    err = _exception_chain_text(exc).lower()
    return (
        "invalid api parameter" in err
        or "'code': '1210'" in err
        or '"code": "1210"' in err
        or "tool_choice" in err
        or "tools parameter" in err
    )


def _build_saved_image_fallback_text(user_text: str, saved_paths: list[str]) -> str:
    intro = user_text.strip() or "Please inspect the attached image."
    path_lines = "\n".join(f"- {path}" for path in saved_paths)
    return (
        f"{intro}\n\n"
        "[SYSTEM NOTE: The user sent image attachments. Direct multimodal processing was rejected by the active "
        "provider/model combination, so the images were saved locally for tool-based inspection.\n"
        f"{path_lines}\n"
        "Use any available filesystem, MCP, or image-analysis tools to inspect those files before answering. "
        "If no such tools are available, explain that clearly and ask the user for a brief description.]"
    )


def _normalize_saved_file_paths(saved_file_paths: list[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for path in saved_file_paths or []:
        value = str(path).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _decode_and_save_images(images: list[str]) -> list[str]:
    saved_paths: list[str] = []
    img_dir = _workspace_dir() / "tmp" / "incoming_images"
    img_dir.mkdir(parents=True, exist_ok=True)

    for img_data in images:
        if "," in img_data:
            header, b64_str = img_data.split(",", 1)
            ext = ".jpg"
            if "png" in header:
                ext = ".png"
            elif "webp" in header:
                ext = ".webp"
        else:
            b64_str = img_data
            ext = ".jpg"

        file_name = f"img_{uuid.uuid4()}{ext}"
        file_path = img_dir / file_name
        with open(file_path, "wb") as f:
            f.write(base64.b64decode(b64_str))
        saved_paths.append(str(file_path))
    return saved_paths
