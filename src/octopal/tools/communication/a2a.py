from __future__ import annotations

import hashlib
import json
from typing import Any

from octopal.infrastructure.config.models import A2AConfig
from octopal.interop.a2a.client import A2AClientError, send_peer_message


async def a2a_send_message(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    peer_id = str((args or {}).get("peer_id") or "").strip()
    text = str((args or {}).get("text") or "").strip()
    data = (args or {}).get("data")
    file_urls = (args or {}).get("file_urls")
    raw_files = (args or {}).get("raw_files")
    context_id = str((args or {}).get("context_id") or "").strip() or None
    if not peer_id:
        return _error_payload("peer_id is required.", error_type="validation")
    if not text and data is None and not file_urls and not raw_files:
        return _error_payload(
            "text, data, file_urls, or raw_files is required.",
            error_type="validation",
        )
    if file_urls is not None and not isinstance(file_urls, list):
        return _error_payload("file_urls must be a list.", error_type="validation")
    if raw_files is not None and not isinstance(raw_files, list):
        return _error_payload("raw_files must be a list.", error_type="validation")

    config = _resolve_a2a_config(ctx)
    if not config.enabled:
        return _error_payload("A2A interop is disabled.", error_type="disabled")
    send_signature = _send_signature(
        peer_id=peer_id,
        text=text,
        data=data,
        file_urls=file_urls,
        raw_files=raw_files,
        context_id=context_id,
    )
    sent_signatures = _sent_a2a_signatures(ctx)
    if send_signature in sent_signatures:
        return _json(
            {
                "status": "skipped_duplicate",
                "ok": True,
                "peer_id": peer_id,
                "context_id": context_id or f"octopal-peer-{peer_id}",
                "message": "Duplicate A2A send detected in this turn; skipped sending a second request.",
            }
        )
    try:
        payload = await send_peer_message(
            config,
            peer_id=peer_id,
            text=text,
            data=data,
            file_urls=file_urls,
            raw_files=raw_files,
            context_id=context_id,
        )
    except A2AClientError as exc:
        return _error_payload(str(exc), error_type=_classify_a2a_error(str(exc)))
    except Exception as exc:
        return _error_payload(
            f"A2A request failed: {exc}",
            error_type=_classify_a2a_error(str(exc)),
        )
    sent_signatures.add(send_signature)
    return _json(
        {
            "status": "ok",
            "peer_id": peer_id,
            "context_id": context_id or f"octopal-peer-{peer_id}",
            "task_state": _extract_a2a_task_state(payload),
            "reply_text": _extract_a2a_reply_text(payload),
            "artifacts": _extract_a2a_artifacts(payload),
            "response": payload,
        }
    )


def _error_payload(message: str, *, error_type: str) -> str:
    transport_error = error_type in {
        "transport",
        "upstream_unavailable",
        "rate_limited",
        "auth",
    }
    return _json(
        {
            "status": "error",
            "ok": False,
            "error_type": error_type,
            "transport_error": transport_error,
            "message": message,
            "diagnosis": (
                "Do not describe the A2A bridge as down unless transport_error is true; "
                "report the exact error instead."
            ),
        }
    )


def _classify_a2a_error(message: str) -> str:
    lowered = message.lower()
    if "returned http 401" in lowered or "returned http 403" in lowered:
        return "auth"
    if "returned http 429" in lowered:
        return "rate_limited"
    if any(f"returned http {status}" in lowered for status in range(500, 600)):
        return "upstream_unavailable"
    if any(token in lowered for token in ("timeout", "timed out", "connection", "dns")):
        return "transport"
    if "not configured" in lowered or "disabled" in lowered or "token" in lowered:
        return "configuration"
    if "payload" in lowered or "capability" in lowered or "required" in lowered:
        return "validation"
    if "returned http" in lowered:
        return "peer_http"
    return "unknown"


def a2a_list_peers(args: dict[str, Any], ctx: dict[str, Any]) -> str:
    config = _resolve_a2a_config(ctx)
    peers: list[dict[str, Any]] = []
    for peer_id, peer in sorted(config.peers.items()):
        if not peer.enabled:
            continue
        peers.append(
            {
                "peer_id": peer_id,
                "name": peer.name or peer_id,
                "capabilities": list(peer.capabilities),
                "trust_level": peer.trust_level,
                "has_base_url": bool(str(peer.base_url or "").strip()),
                "has_agent_card_url": bool(str(peer.agent_card_url or "").strip()),
            }
        )
    return _json(
        {
            "status": "ok",
            "enabled": config.enabled,
            "count": len(peers),
            "peers": peers,
        }
    )


def _resolve_a2a_config(ctx: dict[str, Any]) -> A2AConfig:
    octo = (ctx or {}).get("octo")
    runtime_settings = getattr(getattr(octo, "runtime", None), "settings", None)
    candidate = getattr(runtime_settings, "a2a", None)
    if isinstance(candidate, A2AConfig):
        return candidate
    config_obj = getattr(runtime_settings, "config_obj", None)
    candidate = getattr(config_obj, "a2a", None)
    if isinstance(candidate, A2AConfig):
        return candidate
    return A2AConfig()


def _sent_a2a_signatures(ctx: dict[str, Any]) -> set[str]:
    key = "_a2a_send_message_signatures"
    signatures = (ctx or {}).get(key)
    if not isinstance(signatures, set):
        signatures = set()
        if isinstance(ctx, dict):
            ctx[key] = signatures
    return signatures


def _send_signature(
    *,
    peer_id: str,
    text: str,
    data: Any,
    file_urls: list[dict[str, Any]] | None,
    raw_files: list[dict[str, Any]] | None,
    context_id: str | None,
) -> str:
    payload = {
        "peer_id": peer_id,
        "text": text,
        "data": data,
        "file_urls": file_urls or [],
        "raw_files": raw_files or [],
        "context_id": context_id or "",
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _extract_a2a_task_state(payload: dict[str, Any]) -> str:
    for key in ("task_state", "taskState"):
        value = str(payload.get(key) or "").strip() if isinstance(payload, dict) else ""
        if value:
            return value
    task = payload.get("task") if isinstance(payload, dict) else None
    status = task.get("status") if isinstance(task, dict) else None
    return str(status.get("state") or "").strip() if isinstance(status, dict) else ""


def _extract_a2a_reply_text(payload: dict[str, Any]) -> str:
    for key in ("reply_text", "replyText"):
        value = str(payload.get(key) or "").strip() if isinstance(payload, dict) else ""
        if value:
            return value

    task = payload.get("task") if isinstance(payload, dict) else None
    if not isinstance(task, dict):
        return ""

    status = task.get("status")
    if isinstance(status, dict):
        text = _message_parts_text(status.get("message"))
        if text:
            return text

    artifacts = task.get("artifacts")
    if isinstance(artifacts, list):
        for artifact in artifacts:
            text = _message_parts_text(artifact)
            if text:
                return text
    return ""


def _extract_a2a_artifacts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    task = payload.get("task") if isinstance(payload, dict) else None
    artifacts = task.get("artifacts") if isinstance(task, dict) else None
    if not isinstance(artifacts, list):
        return []
    return [_summarize_artifact(artifact) for artifact in artifacts if isinstance(artifact, dict)]


def _summarize_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    parts = artifact.get("parts")
    part_summaries = (
        [_summarize_part(part) for part in parts if isinstance(part, dict)]
        if isinstance(parts, list)
        else []
    )
    return {
        "artifact_id": str(artifact.get("artifactId") or artifact.get("artifact_id") or "").strip(),
        "name": str(artifact.get("name") or "").strip(),
        "description": str(artifact.get("description") or "").strip(),
        "parts": part_summaries,
    }


def _summarize_part(part: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "media_type": str(part.get("mediaType") or part.get("media_type") or "").strip(),
        "filename": str(part.get("filename") or "").strip(),
    }
    if "text" in part:
        text = str(part.get("text") or "")
        summary.update({"kind": "text", "text_preview": text[:500], "chars": len(text)})
    elif "data" in part:
        summary.update({"kind": "data", "data": part.get("data")})
    elif "url" in part:
        summary.update({"kind": "url", "url": str(part.get("url") or "").strip()})
    elif "raw" in part:
        raw = str(part.get("raw") or "")
        summary.update({"kind": "raw", "base64_chars": len(raw)})
    else:
        summary["kind"] = "unknown"
    return {key: value for key, value in summary.items() if value not in ("", None)}


def _message_parts_text(message: Any) -> str:
    if not isinstance(message, dict):
        return ""
    parts = message.get("parts")
    if not isinstance(parts, list):
        return ""
    texts = [
        str(part.get("text") or "").strip()
        for part in parts
        if isinstance(part, dict) and str(part.get("text") or "").strip()
    ]
    return "\n".join(texts)
