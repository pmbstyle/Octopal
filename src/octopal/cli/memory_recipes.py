from __future__ import annotations

import json
from pathlib import Path
from typing import Any, NoReturn, cast

import typer
from pydantic import ValidationError
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from octopal.infrastructure.config.settings import load_settings
from octopal.infrastructure.store.models import ProceduralRecipeRecord, ProceduralRecipeStatus
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.recipes import (
    ProceduralRecipeCandidate,
    ProceduralRecipeService,
    recipe_metadata_payload,
)

memory_recipes_app = typer.Typer(
    add_completion=False,
    help="Inspect and manage episode-backed procedural recipe candidates.",
)
console = Console()


@memory_recipes_app.command("propose")
def recipes_propose(
    definition_file: Path,
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Create an inactive recipe candidate from a bounded JSON definition."""
    definition = _load_definition(definition_file, json_output)
    try:
        record = _load_service().create_candidate(definition)
    except (ValueError, RuntimeError) as exc:
        _fail("recipe_candidate_rejected", str(exc), json_output)
    payload = recipe_metadata_payload(record)
    if json_output:
        _emit_json({"recipe": payload})
        return
    console.print(f"[green]Recorded recipe {record.id} with status {record.status}.[/green]")


@memory_recipes_app.command("list")
def recipes_list(
    status: str | None = typer.Option(None, "--status", help="Filter by recipe status."),
    limit: int = typer.Option(50, "--limit", min=1, max=1000),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """List metadata without printing recipe instructions."""
    normalized_status = _parse_status(status, json_output)
    records = _load_service().list_recipes(status=normalized_status, limit=limit)
    payload = [recipe_metadata_payload(record) for record in records]
    if json_output:
        _emit_json({"recipes": payload})
        return
    if not payload:
        console.print("[yellow]No procedural recipes found.[/yellow]")
        return
    table = Table(title="Procedural Recipes", border_style="bright_blue")
    table.add_column("Recipe ID", style="dim", width=32)
    table.add_column("Status", width=12)
    table.add_column("Episodes", justify="right", width=9)
    table.add_column("Steps", justify="right", width=7)
    table.add_column("Updated", width=20)
    for item in payload:
        table.add_row(
            str(item["id"]),
            str(item["status"]),
            str(item["source_episode_count"]),
            str(item["strategy_step_count"]),
            str(item["updated_at"]),
        )
    console.print(table)


@memory_recipes_app.command("show")
def recipes_show(
    recipe_id: str,
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Show one recipe definition and its immutable source episode ids."""
    record = _get_or_fail(_load_service(), recipe_id, json_output)
    payload = record.model_dump(mode="json")
    if json_output:
        _emit_json({"recipe": payload})
        return
    console.print_json(json.dumps({"recipe": payload}, ensure_ascii=False))


@memory_recipes_app.command("promote")
def recipes_promote(
    recipe_id: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation."),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Promote a recurrent verified candidate to active procedural memory."""
    service = _load_service()
    record = _get_or_fail(service, recipe_id, json_output)
    _require_confirmation(yes, json_output, f"Promote procedural recipe {record.id}?")
    try:
        updated = service.promote(record.id)
    except (ValueError, RuntimeError) as exc:
        _fail("recipe_promotion_rejected", str(exc), json_output)
    _emit_transition(updated, json_output)


@memory_recipes_app.command("deprecate")
def recipes_deprecate(
    recipe_id: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation."),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Deprecate a candidate or active recipe without deleting its evidence chain."""
    service = _load_service()
    record = _get_or_fail(service, recipe_id, json_output)
    _require_confirmation(yes, json_output, f"Deprecate procedural recipe {record.id}?")
    try:
        updated = service.deprecate(record.id)
    except (ValueError, RuntimeError) as exc:
        _fail("recipe_deprecation_rejected", str(exc), json_output)
    _emit_transition(updated, json_output)


def _load_definition(path: Path, json_output: bool) -> ProceduralRecipeCandidate:
    try:
        raw = path.read_bytes()
        if len(raw) > 32_000:
            raise ValueError("recipe definition exceeds 32000 bytes")
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("recipe definition must be a JSON object")
        return cast(
            ProceduralRecipeCandidate,
            ProceduralRecipeCandidate.model_validate(payload),
        )
    except (OSError, UnicodeError, json.JSONDecodeError, ValidationError, ValueError):
        _fail("invalid_recipe_definition", "Recipe definition is invalid.", json_output)


def _load_service() -> ProceduralRecipeService:
    return ProceduralRecipeService(SQLiteStore(load_settings()))


def _get_or_fail(
    service: ProceduralRecipeService, recipe_id: str, json_output: bool
) -> ProceduralRecipeRecord:
    record = service.get(recipe_id)
    if record is None:
        _fail("recipe_not_found", f"Procedural recipe not found: {recipe_id}", json_output)
    return record


def _parse_status(value: str | None, json_output: bool) -> ProceduralRecipeStatus | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized not in {"candidate", "active", "deprecated"}:
        _fail("invalid_recipe_status", f"Invalid procedural recipe status: {value}", json_output)
    return cast(ProceduralRecipeStatus, normalized)


def _require_confirmation(yes: bool, json_output: bool, prompt: str) -> None:
    if json_output and not yes:
        _fail("confirmation_required", "Use --yes with --json to confirm.", json_output)
    if not yes and not Confirm.ask(prompt, default=False):
        console.print("[yellow]Cancelled; recipe was not changed.[/yellow]")
        raise typer.Exit(code=0)


def _emit_transition(record: ProceduralRecipeRecord, json_output: bool) -> None:
    payload = {"recipe_id": record.id, "status": record.status}
    if json_output:
        _emit_json(payload)
        return
    console.print(f"[green]{record.id} is now {record.status}.[/green]")


def _emit_json(payload: Any) -> None:
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _fail(code: str, message: str, json_output: bool) -> NoReturn:
    if json_output:
        _emit_json({"error": {"code": code, "message": message}})
    else:
        console.print(f"[red]{message}[/red]")
    raise typer.Exit(code=1)
