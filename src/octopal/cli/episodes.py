from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import typer
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from octopal.infrastructure.config.settings import load_settings
from octopal.infrastructure.store.models import (
    AuditEvent,
    ExecutionEpisodeEvidenceMetadata,
    ExecutionEpisodeRecord,
)
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.episode_evidence import EpisodeEvidenceCipher
from octopal.utils import utc_now

episodes_app = typer.Typer(
    add_completion=False,
    help="Inspect immutable execution episodes and manage encrypted raw evidence.",
)
console = Console()


@episodes_app.command("list")
def episodes_list(
    worker_run_id: str | None = typer.Option(
        None,
        "--worker-run-id",
        help="Only show episodes for one worker run.",
    ),
    limit: int = typer.Option(50, "--limit", min=1, max=1000),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """List metadata-only execution episode views."""
    store = SQLiteStore(load_settings())
    episodes = store.list_execution_episodes(worker_run_id=worker_run_id, limit=limit)
    payload = [
        _episode_list_payload(
            episode,
            store.get_execution_episode_evidence_metadata(episode.id),
        )
        for episode in episodes
    ]
    if json_output:
        _emit_json({"episodes": payload})
        return
    if not payload:
        console.print("[yellow]No execution episodes found.[/yellow]")
        return

    table = Table(title="Execution Episodes", border_style="bright_blue")
    table.add_column("Episode ID", style="dim", width=28)
    table.add_column("Worker Run", width=20)
    table.add_column("Status", width=10)
    table.add_column("Trust", width=22)
    table.add_column("Raw Evidence", width=14)
    table.add_column("Created", width=20)
    for item in payload:
        evidence = item["evidence"]
        table.add_row(
            str(item["id"]),
            str(item["worker_run_id"]),
            str(item["status"]),
            str(item["trust_state"]),
            "encrypted" if evidence["available"] else "metadata only",
            str(item["created_at"]),
        )
    console.print(table)


@episodes_app.command("show")
def episodes_show(
    episode_id: str,
    reveal_evidence: bool = typer.Option(
        False,
        "--reveal-evidence",
        help="Decrypt and print raw task, prompt, tool, and result content.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Show one episode; raw evidence stays hidden unless explicitly requested."""
    settings = load_settings()
    store = SQLiteStore(settings)
    episode = store.get_execution_episode(episode_id)
    if episode is None:
        _fail("episode_not_found", f"Execution episode not found: {episode_id}", json_output)

    metadata = store.get_execution_episode_evidence_metadata(episode_id)
    payload: dict[str, Any] = {
        "episode": episode.model_dump(mode="json"),
        "evidence": _evidence_metadata_payload(metadata),
    }
    if reveal_evidence:
        if metadata is None:
            _fail(
                "evidence_not_found",
                f"Encrypted raw evidence is not available for episode: {episode_id}",
                json_output,
            )
        if not settings.episode_evidence_key:
            _fail(
                "evidence_key_missing",
                "OCTOPAL_EPISODE_EVIDENCE_KEY is required to reveal raw evidence.",
                json_output,
            )
        evidence = store.get_execution_episode_evidence(episode_id)
        if evidence is None:
            _fail(
                "evidence_not_found",
                f"Encrypted raw evidence is not available for episode: {episode_id}",
                json_output,
            )
        try:
            cipher = EpisodeEvidenceCipher.from_encoded_key(settings.episode_evidence_key)
            raw_evidence = cipher.decrypt(evidence)
        except Exception:
            _fail(
                "evidence_decryption_failed",
                "Unable to decrypt raw evidence with the configured key.",
                json_output,
            )
        try:
            store.append_audit(_operator_audit_event("revealed", episode))
        except Exception:
            _fail(
                "evidence_audit_failed",
                "Unable to record sensitive evidence access; raw evidence was not displayed.",
                json_output,
            )
        payload["raw_evidence"] = raw_evidence

    if json_output:
        _emit_json(payload)
        return
    if reveal_evidence:
        console.print(
            "[bold yellow]Raw evidence may contain task, prompt, tool, and result secrets.[/bold yellow]"
        )
    console.print_json(json.dumps(payload, ensure_ascii=False))


@episodes_app.command("erase-evidence")
def episodes_erase_evidence(
    episode_id: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Permanently erase encrypted raw evidence while preserving episode metadata."""
    store = SQLiteStore(load_settings())
    if store.get_execution_episode(episode_id) is None:
        _fail("episode_not_found", f"Execution episode not found: {episode_id}", json_output)
    if store.get_execution_episode_evidence_metadata(episode_id) is None:
        _fail(
            "evidence_not_found",
            f"Encrypted raw evidence is not available for episode: {episode_id}",
            json_output,
        )
    if json_output and not yes:
        _fail(
            "confirmation_required",
            "Use --yes with --json to confirm permanent evidence deletion.",
            json_output,
        )
    if not yes and not Confirm.ask(
        f"Permanently erase encrypted raw evidence for {episode_id}?",
        default=False,
    ):
        console.print("[yellow]Cancelled; no evidence was deleted.[/yellow]")
        return

    episode = store.get_execution_episode(episode_id)
    if episode is None:
        _fail("episode_not_found", f"Execution episode not found: {episode_id}", json_output)
    try:
        deleted = store.delete_execution_episode_evidence_with_audit(
            episode_id,
            _operator_audit_event("erased", episode),
        )
    except Exception:
        _fail(
            "evidence_erase_failed",
            "Unable to atomically erase evidence and record the audit event.",
            json_output,
        )
    if not deleted:
        _fail(
            "evidence_not_found",
            f"Encrypted raw evidence is no longer available for episode: {episode_id}",
            json_output,
        )
    payload = {
        "episode_id": episode_id,
        "evidence_deleted": True,
        "metadata_preserved": store.get_execution_episode(episode_id) is not None,
    }
    if json_output:
        _emit_json(payload)
        return
    console.print(
        f"[green]Encrypted raw evidence erased for {episode_id}; episode metadata preserved.[/green]"
    )


def _episode_list_payload(
    episode: ExecutionEpisodeRecord,
    evidence: ExecutionEpisodeEvidenceMetadata | None,
) -> dict[str, Any]:
    return {
        "id": episode.id,
        "worker_run_id": episode.worker_run_id,
        "status": episode.status,
        "source_kind": episode.source_kind,
        "trust_state": episode.trust_state,
        "template_id": episode.template_id,
        "model": episode.model,
        "created_at": episode.created_at.isoformat(),
        "evidence": _evidence_metadata_payload(evidence),
    }


def _evidence_metadata_payload(
    evidence: ExecutionEpisodeEvidenceMetadata | None,
) -> dict[str, Any]:
    if evidence is None:
        return {"available": False}
    return {
        "available": True,
        "algorithm": evidence.algorithm,
        "key_id": evidence.key_id,
        "created_at": evidence.created_at.isoformat(),
        "expires_at": evidence.expires_at.isoformat(),
    }


def _emit_json(payload: Any) -> None:
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _operator_audit_event(action: str, episode: ExecutionEpisodeRecord) -> AuditEvent:
    return AuditEvent(
        id=str(uuid4()),
        ts=utc_now(),
        correlation_id=episode.worker_run_id,
        level="info",
        event_type=f"execution_episode_evidence_{action}",
        data={"episode_id": episode.id, "interface": "cli"},
    )


def _fail(code: str, message: str, json_output: bool) -> None:
    if json_output:
        _emit_json({"error": {"code": code, "message": message}})
    else:
        console.print(f"[red]{message}[/red]")
    raise typer.Exit(code=1)
