from __future__ import annotations

import asyncio
import json
from typing import Any, NoReturn, cast

import typer
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from octopal.infrastructure.config.settings import load_settings
from octopal.infrastructure.store.models import MemoryTrustState
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.canon import CanonProposal, CanonService
from octopal.runtime.memory.facts import FactsService

memory_proposals_app = typer.Typer(
    add_completion=False,
    help="Inspect, promote, deprecate, and roll back provenance-bearing canon proposals.",
)
console = Console()

_LISTABLE_STATES = {
    "observed",
    "quarantined_candidate",
    "corroborated",
    "trusted",
    "deprecated",
}


@memory_proposals_app.command("list")
def proposals_list(
    state: str | None = typer.Option(None, "--state", help="Filter by trust state."),
    limit: int = typer.Option(50, "--limit", min=1, max=1000),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """List proposal metadata without printing proposed content."""
    trust_state = _parse_trust_state(state, json_output)
    proposals = _load_service().list_proposals(trust_state=trust_state, limit=limit)
    payload = [proposal.metadata_payload() for proposal in proposals]
    if json_output:
        _emit_json({"proposals": payload})
        return
    if not payload:
        console.print("[yellow]No canon proposals found.[/yellow]")
        return

    table = Table(title="Canon Proposals", border_style="bright_blue")
    table.add_column("Proposal ID", style="dim", width=40)
    table.add_column("File", width=18)
    table.add_column("Mode", width=10)
    table.add_column("Origin", width=22)
    table.add_column("Trust", width=24)
    table.add_column("Chars", justify="right", width=8)
    table.add_column("Updated", width=20)
    for item in payload:
        table.add_row(
            str(item["id"]),
            str(item["filename"]),
            str(item["mode"]),
            str(item["source_kind"]),
            str(item["trust_state"]),
            str(item["content_chars"]),
            str(item["updated_at"]),
        )
    console.print(table)


@memory_proposals_app.command("show")
def proposals_show(
    proposal_id: str,
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Show one proposal, including its untrusted raw content."""
    proposal = _get_proposal_or_fail(proposal_id, json_output)
    payload = proposal.metadata_payload(reveal_source_ref=True)
    payload["content"] = proposal.content
    if json_output:
        _emit_json({"proposal": payload})
        return
    console.print(
        "[bold yellow]Proposal content is untrusted until explicitly promoted.[/bold yellow]"
    )
    console.print_json(json.dumps({"proposal": payload}, ensure_ascii=False))


@memory_proposals_app.command("promote")
def proposals_promote(
    proposal_id: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Promote one proposal into active canonical context."""
    service = _load_service()
    proposal = _get_proposal_from_service_or_fail(service, proposal_id, json_output)
    _require_confirmation(
        yes,
        json_output,
        f"Promote {proposal.id} into {proposal.filename}?",
    )
    try:
        updated = asyncio.run(service.promote_proposal(proposal.id))
    except ValueError as exc:
        _fail("proposal_promotion_rejected", str(exc), json_output)
    payload = _transition_payload(updated, rollback_applied=False)
    if json_output:
        _emit_json(payload)
        return
    console.print(f"[green]Promoted {updated.id} into {updated.filename}.[/green]")


@memory_proposals_app.command("deprecate")
def proposals_deprecate(
    proposal_id: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Deprecate a proposal; active content is rolled back by rebuilding canon."""
    service = _load_service()
    proposal = _get_proposal_from_service_or_fail(service, proposal_id, json_output)
    was_trusted = proposal.trust_state == "trusted"
    _require_confirmation(
        yes,
        json_output,
        f"Deprecate {proposal.id} and roll back its active canon content?",
    )
    try:
        updated = asyncio.run(service.deprecate_proposal(proposal.id))
    except ValueError as exc:
        _fail("proposal_deprecation_rejected", str(exc), json_output)
    payload = _transition_payload(updated, rollback_applied=was_trusted)
    if json_output:
        _emit_json(payload)
        return
    action = "rolled back and deprecated" if was_trusted else "deprecated"
    console.print(f"[green]{updated.id} {action}.[/green]")


def _load_service() -> CanonService:
    settings = load_settings()
    store = SQLiteStore(settings)
    facts = FactsService(store=store, owner_id=settings.memory_owner_id)
    return CanonService(
        workspace_dir=settings.workspace_dir,
        store=store,
        embeddings=None,
        facts=facts,
    )


def _get_proposal_or_fail(proposal_id: str, json_output: bool) -> CanonProposal:
    return _get_proposal_from_service_or_fail(_load_service(), proposal_id, json_output)


def _get_proposal_from_service_or_fail(
    service: CanonService,
    proposal_id: str,
    json_output: bool,
) -> CanonProposal:
    try:
        proposal = service.get_proposal(proposal_id)
    except ValueError:
        proposal = None
    if proposal is None:
        _fail("proposal_not_found", f"Canon proposal not found: {proposal_id}", json_output)
    return proposal


def _parse_trust_state(value: str | None, json_output: bool) -> MemoryTrustState | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized not in _LISTABLE_STATES:
        _fail("invalid_trust_state", f"Invalid canon proposal trust state: {value}", json_output)
    return cast(MemoryTrustState, normalized)


def _require_confirmation(yes: bool, json_output: bool, prompt: str) -> None:
    if json_output and not yes:
        _fail(
            "confirmation_required",
            "Use --yes with --json to confirm the canon trust transition.",
            json_output,
        )
    if not yes and not Confirm.ask(prompt, default=False):
        console.print("[yellow]Cancelled; canon was not changed.[/yellow]")
        raise typer.Exit(code=0)


def _transition_payload(
    proposal: CanonProposal,
    *,
    rollback_applied: bool,
) -> dict[str, Any]:
    return {
        "proposal_id": proposal.id,
        "filename": proposal.filename,
        "trust_state": proposal.trust_state,
        "rollback_applied": rollback_applied,
    }


def _emit_json(payload: Any) -> None:
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _fail(code: str, message: str, json_output: bool) -> NoReturn:
    if json_output:
        _emit_json({"error": {"code": code, "message": message}})
    else:
        console.print(f"[red]{message}[/red]")
    raise typer.Exit(code=1)
