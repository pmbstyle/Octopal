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
from octopal.infrastructure.store.models import AdaptationCandidateRecord
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.adaptation import (
    AdaptationCandidateDefinition,
    AdaptationService,
    adaptation_candidate_metadata,
    adaptation_evaluation_payload,
)

adaptation_app = typer.Typer(
    add_completion=False,
    help="Cluster failures and manage explicitly evaluated adaptation candidates.",
)
console = Console()


@adaptation_app.command("cluster")
def adaptation_cluster(
    benchmark_result: Path,
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Record recurrent metadata-only failure clusters from one worker benchmark result."""
    summary = _load_json_object(benchmark_result, json_output, maximum=4_000_000)
    try:
        records = _load_service().cluster_failures(summary)
    except (ValueError, RuntimeError) as exc:
        _fail("adaptation_clustering_rejected", str(exc), json_output)
    payload = [record.model_dump(mode="json") for record in records]
    if json_output:
        _emit_json({"clusters": payload})
        return
    if not records:
        console.print("[yellow]No recurrent failure clusters found.[/yellow]")
        return
    console.print(f"[green]Recorded {len(records)} recurrent failure cluster(s).[/green]")


@adaptation_app.command("propose")
def adaptation_propose(
    definition_file: Path,
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Create an inactive, operator-authored adaptation hypothesis."""
    definition = _load_definition(definition_file, json_output)
    try:
        record = _load_service().create_candidate(definition)
    except (ValueError, RuntimeError) as exc:
        _fail("adaptation_candidate_rejected", str(exc), json_output)
    payload = adaptation_candidate_metadata(record)
    if json_output:
        _emit_json({"candidate": payload})
        return
    console.print(f"[green]Recorded adaptation {record.id} version {record.version}.[/green]")


@adaptation_app.command("list")
def adaptation_list(
    kind: str | None = typer.Option(None, "--kind", help="Filter by artifact kind."),
    status: str | None = typer.Option(None, "--status", help="Filter by lifecycle status."),
    limit: int = typer.Option(50, "--limit", min=1, max=1000),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """List candidate metadata without printing hypotheses or artifact text."""
    try:
        records = _load_service().list_candidates(kind=kind, status=status, limit=limit)
    except ValueError as exc:
        _fail("invalid_adaptation_filter", str(exc), json_output)
    payload = [adaptation_candidate_metadata(record) for record in records]
    if json_output:
        _emit_json({"candidates": payload})
        return
    if not payload:
        console.print("[yellow]No adaptation candidates found.[/yellow]")
        return
    table = Table(title="Adaptation Candidates", border_style="bright_blue")
    table.add_column("Candidate", style="dim", width=28)
    table.add_column("Kind", width=18)
    table.add_column("Target", width=24)
    table.add_column("Version", justify="right", width=7)
    table.add_column("Status", width=10)
    for item in payload:
        table.add_row(
            str(item["id"]),
            str(item["kind"]),
            str(item["target"]),
            str(item["version"]),
            str(item["status"]),
        )
    console.print(table)


@adaptation_app.command("show")
def adaptation_show(
    candidate_id: str,
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Explicitly reveal one hypothesis, structured change, lineage, and latest evaluation."""
    service = _load_service()
    record = _get_or_fail(service, candidate_id, json_output)
    evaluation = service.latest_evaluation(record.id)
    payload = {
        "candidate": record.model_dump(mode="json"),
        "latest_evaluation": (
            adaptation_evaluation_payload(evaluation) if evaluation is not None else None
        ),
    }
    if json_output:
        _emit_json(payload)
        return
    console.print_json(json.dumps(payload, ensure_ascii=False))


@adaptation_app.command("context")
def adaptation_context(
    candidate_id: str,
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Export exact benchmark-only context for a controlled candidate suite."""
    service = _load_service()
    _get_or_fail(service, candidate_id, json_output)
    try:
        context = service.context_for_evaluation(candidate_id)
    except ValueError as exc:
        _fail("adaptation_context_rejected", str(exc), json_output)
    payload = {"adaptations": [context.model_dump(mode="json")]}
    if json_output:
        _emit_json(payload)
        return
    console.print_json(json.dumps(payload, ensure_ascii=False))


@adaptation_app.command("evaluate")
def adaptation_evaluate(
    candidate_id: str,
    baseline_result: Path,
    candidate_result: Path,
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Record an immutable held-out baseline/candidate comparison."""
    service = _load_service()
    _get_or_fail(service, candidate_id, json_output)
    baseline = _load_json_object(baseline_result, json_output, maximum=4_000_000)
    candidate = _load_json_object(candidate_result, json_output, maximum=4_000_000)
    try:
        evaluation = service.evaluate(candidate_id, baseline=baseline, candidate=candidate)
    except (ValueError, RuntimeError) as exc:
        _fail("adaptation_evaluation_rejected", str(exc), json_output)
    payload = adaptation_evaluation_payload(evaluation)
    if json_output:
        _emit_json({"evaluation": payload})
        return
    color = "green" if evaluation.passed else "red"
    console.print(
        f"[{color}]Evaluation {evaluation.id}: "
        f"{'passed' if evaluation.passed else 'failed'}.[/{color}]"
    )


@adaptation_app.command("promote")
def adaptation_promote(
    candidate_id: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation."),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Mark a passing candidate as the approved version; runtime files remain untouched."""
    service = _load_service()
    record = _get_or_fail(service, candidate_id, json_output)
    _require_confirmation(yes, json_output, f"Promote adaptation {record.id}?")
    try:
        updated = service.promote(record.id)
    except (ValueError, RuntimeError) as exc:
        _fail("adaptation_promotion_rejected", str(exc), json_output)
    _emit_transition(updated, json_output)


@adaptation_app.command("rollback")
def adaptation_rollback(
    candidate_id: str,
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation."),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Restore a previously evaluated retired version as the approved version."""
    service = _load_service()
    record = _get_or_fail(service, candidate_id, json_output)
    _require_confirmation(yes, json_output, f"Rollback adaptation family to {record.id}?")
    try:
        updated = service.rollback(record.id)
    except (ValueError, RuntimeError) as exc:
        _fail("adaptation_rollback_rejected", str(exc), json_output)
    _emit_transition(updated, json_output)


@adaptation_app.command("bandit-readiness")
def adaptation_bandit_readiness(
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
) -> None:
    """Explain why online contextual-bandit routing remains disabled."""
    payload = _load_service().bandit_readiness()
    if json_output:
        _emit_json(payload)
        return
    console.print(f"[yellow]{payload['reason']}[/yellow]")


def _load_service() -> AdaptationService:
    return AdaptationService(SQLiteStore(load_settings()))


def _load_definition(path: Path, json_output: bool) -> AdaptationCandidateDefinition:
    payload = _load_json_object(path, json_output, maximum=24_000)
    try:
        return cast(
            AdaptationCandidateDefinition,
            AdaptationCandidateDefinition.model_validate(payload),
        )
    except ValidationError:
        _fail("invalid_adaptation_definition", "Adaptation definition is invalid.", json_output)


def _load_json_object(path: Path, json_output: bool, *, maximum: int) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        if len(raw) > maximum:
            raise ValueError("input exceeds its byte limit")
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("input must be a JSON object")
        return cast(dict[str, Any], payload)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
        _fail("invalid_adaptation_input", "Adaptation input is invalid.", json_output)


def _get_or_fail(
    service: AdaptationService,
    candidate_id: str,
    json_output: bool,
) -> AdaptationCandidateRecord:
    record = service.get_candidate(candidate_id)
    if record is None:
        _fail(
            "adaptation_not_found", f"Adaptation candidate not found: {candidate_id}", json_output
        )
    return record


def _require_confirmation(yes: bool, json_output: bool, prompt: str) -> None:
    if yes:
        return
    if json_output or not Confirm.ask(prompt, default=False):
        _fail("confirmation_required", "Confirmation required; pass --yes.", json_output)


def _emit_transition(record: AdaptationCandidateRecord, json_output: bool) -> None:
    payload = adaptation_candidate_metadata(record)
    if json_output:
        _emit_json({"candidate": payload})
        return
    console.print(f"[green]Adaptation {record.id} is now {record.status}.[/green]")


def _emit_json(payload: dict[str, Any]) -> None:
    typer.echo(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _fail(code: str, message: str, json_output: bool) -> NoReturn:
    if json_output:
        _emit_json({"ok": False, "error": {"code": code, "message": message}})
    else:
        console.print(f"[red]{message}[/red]")
    raise typer.Exit(code=1)
