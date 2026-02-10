from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm
from rich.table import Table

from broodmind.cli.branding import print_banner
from broodmind.config.settings import Settings, load_settings
from broodmind.gateway.app import build_app
from broodmind.logging_config import configure_logging
from broodmind.runtime_metrics import read_metrics_snapshot
from broodmind.state import (
    is_pid_running,
    list_broodmind_runtime_pids,
    pid_command_line,
    read_status,
    write_start_status,
)
from broodmind.store.sqlite import SQLiteStore
from broodmind.telegram.bot import run_bot
from broodmind.workers.templates import sync_default_templates

app = typer.Typer(add_completion=False)
workers_app = typer.Typer(add_completion=False)
audit_app = typer.Typer(add_completion=False)
memory_app = typer.Typer(add_completion=False)
config_app = typer.Typer(add_completion=False)

console = Console()


@app.command()
def configure() -> None:
    """Run the interactive configuration wizard."""
    from broodmind.cli.configure import configure_wizard
    configure_wizard()


def _init_logging(settings: Settings) -> None:
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    log_dir = settings.state_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(
        log_level=settings.log_level,
        log_dir=log_dir,
        debug_prompts=settings.debug_prompts
    )


@app.command()
def start(
    foreground: bool = typer.Option(False, "--foreground", "-f", help="Run in foreground mode (showing logs)"),
) -> None:
    """Start the BroodMind Queen."""
    try:
        settings = load_settings()
    except Exception as e:
        console.print(f"[bold red]Configuration error:[/bold red] {e}")
        if Confirm.ask("Would you like to run the configuration wizard now?", default=True):
            from broodmind.cli.configure import configure_wizard
            configure_wizard()
            settings = load_settings()
        else:
            raise typer.Exit(code=1) from None

    running_pids = list_broodmind_runtime_pids()
    if running_pids:
        console.print("[bold yellow]BroodMind is already running.[/bold yellow]")
        console.print(f"Active runtime PID(s): {', '.join(str(pid) for pid in running_pids)}")
        console.print("Use [magenta]broodmind stop[/magenta] first, then start again.")
        raise typer.Exit(code=1)

    if not foreground:
        print_banner()
        _start_background()
        return

    _init_logging(settings)

    with console.status("[bold green]Initializing BroodMind Queen...[/bold green]", spinner="dots"):
        write_start_status(settings)
        time.sleep(0.5)

    # Use ASCII checkmark [V] instead of unicode checkmark to avoid encoding issues in background processes
    console.print("[bold green][V] BroodMind Queen started.[/bold green]")
    console.print(f"   [dim]Logs directory:[/dim] [cyan]{settings.state_dir / 'logs'}[/cyan]")
    console.print("[dim]Press Ctrl+C to stop (if in foreground).[/dim]\n")

    try:
        asyncio.run(run_bot(settings))
    except KeyboardInterrupt:
        # Use standard logging here as structlog might be torn down
        logging.getLogger(__name__).info("Shutting down")
        console.print("\n[bold yellow]Shutting down...[/bold yellow]")


def _start_background() -> None:
    import os
    import platform
    import subprocess
    import sys

    console.print("[bold cyan]Starting BroodMind in background...[/bold cyan]")

    # Use the current python executable and run the module with --foreground
    args = [sys.executable, "-m", "broodmind.cli", "start", "--foreground"]

    # Ensure src is in PYTHONPATH for the background process
    project_root = Path(__file__).resolve().parents[3]
    src_dir = project_root / "src"

    env = os.environ.copy()
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_dir}{os.pathsep}{existing_pp}" if existing_pp else str(src_dir)

    # Redirect output to a file for debugging
    log_dir = project_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    with (
        open(log_dir / "startup_stdout.log", "w", encoding="utf-8") as out_file,
        open(log_dir / "startup_stderr.log", "w", encoding="utf-8") as err_file,
    ):
        try:
            if platform.system() == "Windows":
                # DETACHED_PROCESS = 0x00000008
                subprocess.Popen(
                    args,
                    creationflags=0x00000008,
                    stdout=out_file,
                    stderr=err_file,
                    stdin=subprocess.DEVNULL,
                    close_fds=False, # close_fds=True can cause issues with handles on Windows sometimes
                    env=env
                )
            else:
                # Simple nohup-like behavior
                subprocess.Popen(
                    args,
                    stdout=out_file,
                    stderr=err_file,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                    env=env
                )

            # Give it a moment to initialize and write the PID file
            time.sleep(2)
            console.print("[bold green][V] BroodMind started in background.[/bold green]")
            console.print("Use [magenta]broodmind status[/magenta] to check status.")
            console.print("Use [magenta]broodmind logs -f[/magenta] to view logs.")
        except Exception as e:
            console.print(f"[bold red]Failed to start background process:[/bold red] {e}")
            raise typer.Exit(code=1) from e


@app.command()
def stop() -> None:
    settings = load_settings()
    status_data = read_status(settings)
    pid = status_data.get("pid") if status_data else None
    import os
    import platform

    discovered = list_broodmind_runtime_pids()
    targets: list[int] = []
    if pid and is_pid_running(pid):
        targets.append(pid)
    targets.extend(discovered)
    targets = sorted(set(targets))

    if not targets:
        console.print("[yellow]BroodMind is not running.[/yellow]")
        return

    console.print(
        f"[bold yellow]Stopping BroodMind ({len(targets)} process(es)): "
        f"{', '.join(str(p) for p in targets)}[/bold yellow]"
    )

    failures: list[tuple[int, str]] = []
    try:
        if platform.system() == "Windows":
            import subprocess
            for target in targets:
                try:
                    subprocess.run(
                        ["taskkill", "/F", "/PID", str(target)],
                        check=True,
                        capture_output=True,
                    )
                except Exception as exc:
                    failures.append((target, str(exc)))
        else:
            import signal
            deadline = time.time() + 8.0
            for target in targets:
                try:
                    os.kill(target, signal.SIGTERM)
                except ProcessLookupError:
                    continue
                except Exception as exc:
                    failures.append((target, str(exc)))

            while time.time() < deadline:
                alive = [p for p in targets if is_pid_running(p)]
                if not alive:
                    break
                time.sleep(0.2)

            alive = [p for p in targets if is_pid_running(p)]
            for target in alive:
                try:
                    os.kill(target, signal.SIGKILL)
                except ProcessLookupError:
                    continue
                except Exception as exc:
                    failures.append((target, str(exc)))

        still_running = [p for p in targets if is_pid_running(p)]
        if still_running:
            for target in still_running:
                cmdline = pid_command_line(target)
                details = f" ({cmdline})" if cmdline else ""
                failures.append((target, f"still running{details}"))

        if failures:
            console.print("[bold red]Failed to stop all BroodMind processes:[/bold red]")
            for failed_pid, reason in failures:
                console.print(f" - PID {failed_pid}: {reason}")
            raise typer.Exit(code=1)

        console.print("[bold green][V] BroodMind stopped.[/bold green]")
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[bold red]Failed to stop BroodMind: {e}[/bold red]")
        raise typer.Exit(code=1) from e


@app.command()
def restart(
    foreground: bool = typer.Option(False, "--foreground", "-f", help="Run in foreground after restart"),
) -> None:
    """Stop and then start the BroodMind Queen."""
    stop()

    settings = load_settings()
    log_dir = settings.state_dir / "logs"

    with console.status("[bold yellow]Restarting system...[/bold yellow]"):
        # Give it a moment to release file handles
        time.sleep(2)

        # Purge logs
        if log_dir.exists():
            for log_file in log_dir.glob("*"):
                try:
                    if log_file.is_file():
                        log_file.unlink()
                except Exception:
                    pass

    start(foreground=foreground)


@app.command()
def version() -> None:
    """Show the version of BroodMind."""
    from importlib.metadata import version as get_version
    try:
        v = get_version("broodmind")
    except Exception:
        v = "0.1.0 (dev)"
    console.print(f"BroodMind [bold cyan]v{v}[/bold cyan]")


@app.command()
def status() -> None:
    config_ok = True
    settings: Settings | None = None
    error_text = None
    try:
        settings = load_settings()
    except Exception as exc:
        config_ok = False
        error_text = str(exc)

    if not settings:
        console.print("[bold red]Configuration Error[/bold red]")
        console.print(f"Config OK: [red]{config_ok}[/red]")
        if error_text:
            console.print(f"Error: [red]{error_text}[/red]")
        return

    status_data = read_status(settings)
    pid = status_data.get("pid") if status_data else None
    running = is_pid_running(pid)
    last_message = status_data.get("last_message_at") if status_data else None

    status_color = "green" if running else "red"
    status_text = "RUNNING" if running else "STOPPED"

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold white")
    grid.add_column()

    grid.add_row("System Status:", f"[{status_color}]{status_text}[/{status_color}]")
    grid.add_row("Process ID:", str(pid) if pid else "[dim]N/A[/dim]")
    grid.add_row("Last Heartbeat:", str(last_message) if last_message else "[dim]Never[/dim]")
    grid.add_row("Configuration:", "[green]Valid[/green]" if config_ok else "[red]Invalid[/red]")

    metrics = read_metrics_snapshot(settings.state_dir)
    queen_metrics = metrics.get("queen", {}) if isinstance(metrics, dict) else {}
    telegram_metrics = metrics.get("telegram", {}) if isinstance(metrics, dict) else {}
    exec_metrics = metrics.get("exec_run", {}) if isinstance(metrics, dict) else {}
    if metrics:
        grid.add_row(
            "Queen Queues:",
            (
                f"followup={queen_metrics.get('followup_queues', 0)} "
                f"internal={queen_metrics.get('internal_queues', 0)}"
            ),
        )
        grid.add_row(
            "Telegram Queues:",
            (
                f"queues={telegram_metrics.get('chat_queues', 0)} "
                f"send_tasks={telegram_metrics.get('send_tasks', 0)}"
            ),
        )
        grid.add_row(
            "Exec Sessions:",
            (
                f"running={exec_metrics.get('background_sessions_running', 0)} "
                f"total={exec_metrics.get('background_sessions_total', 0)}"
            ),
        )
    else:
        grid.add_row("Runtime Metrics:", "[dim]No telemetry snapshot yet[/dim]")

    console.print(Panel(
        grid,
        title="[bold cyan]BroodMind System Status[/bold cyan]",
        border_style="blue",
        expand=False,
        padding=(1, 2)
    ))


@workers_app.command("list")
def workers_list() -> None:
    settings = load_settings()
    store = SQLiteStore(settings)
    workers = store.list_workers()
    if not workers:
        console.print("[yellow]No workers found.[/yellow]")
        return

    table = Table(title="Registered Workers", border_style="blue", show_header=True, header_style="bold cyan")
    table.add_column("Worker ID", style="dim")
    table.add_column("Status")
    table.add_column("Current Task")

    for worker in workers:
        status_style = "green" if worker.status == "idle" else "yellow" if worker.status == "working" else "red"
        table.add_row(
            worker.id,
            f"[{status_style}]{worker.status}[/{status_style}]",
            worker.task or "[dim]-[/dim]"
        )
    console.print(table)


@audit_app.command("list")
def audit_list(limit: int = 50) -> None:
    settings = load_settings()
    store = SQLiteStore(settings)
    events = store.list_audit(limit=limit)
    if not events:
        console.print("[yellow]No audit events found.[/yellow]")
        return

    table = Table(title=f"Audit Log (Last {limit})", border_style="blue", header_style="bold cyan")
    table.add_column("ID", style="dim", width=10)
    table.add_column("Timestamp", style="white")
    table.add_column("Level", width=10)
    table.add_column("Type", style="green")
    table.add_column("Correlation ID", style="dim")

    for event in events:
        level_style = "red" if event.level in ("ERROR", "CRITICAL") else "yellow" if event.level == "WARNING" else "blue"
        table.add_row(
            event.id,
            event.ts.isoformat(timespec='seconds'),
            f"[{level_style}]{event.level}[/{level_style}]",
            event.event_type,
            event.correlation_id or ""
        )
    console.print(table)


@audit_app.command("show")
def audit_show(event_id: str) -> None:
    settings = load_settings()
    store = SQLiteStore(settings)
    event = store.get_audit(event_id)
    if not event:
        console.print(f"[red]Audit event not found: {event_id}[/red]")
        raise typer.Exit(code=1)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan")
    grid.add_column()

    grid.add_row("ID:", event.id)
    grid.add_row("Timestamp:", event.ts.isoformat())
    grid.add_row("Level:", f"[{'red' if event.level == 'ERROR' else 'green'}]{event.level}[/]")
    grid.add_row("Type:", event.event_type)
    grid.add_row("Correlation ID:", event.correlation_id or "-")

    console.print(Panel(grid, title="Audit Event Details", border_style="blue"))

    import json

    from rich.syntax import Syntax

    # Try to pretty print data if it's a dict or similar
    data_str = str(event.data)
    try:
        if isinstance(event.data, dict | list):
            data_str = json.dumps(event.data, indent=2)
            syntax = Syntax(data_str, "json", theme="monokai", background_color="default")
            console.print(Panel(syntax, title="Data Payload", border_style="white"))
        else:
             console.print(Panel(data_str, title="Data Payload", border_style="white"))
    except Exception:
        console.print(Panel(str(event.data), title="Data Payload", border_style="white"))


@memory_app.command("stats")
def memory_stats() -> None:
    """Show memory/RAG statistics."""
    settings = load_settings()
    store = SQLiteStore(settings)

    with console.status("[bold green]Analyzing memory...[/bold green]"):
        entries = store.list_memory_entries(limit=1000000)  # Get all for stats

    total = len(entries)

    if total == 0:
        console.print("[yellow]No memory entries found.[/yellow]")
        return

    # Count by role
    by_role: dict[str, int] = {}
    # Count by chat_id
    by_chat: dict[int, int] = {}

    for entry in entries:
        by_role[entry.role] = by_role.get(entry.role, 0) + 1
        chat_id = entry.metadata.get("chat_id") if entry.metadata else None
        if chat_id:
            by_chat[chat_id] = by_chat.get(chat_id, 0) + 1

    console.print(f"\n[bold]Total Memory Entries:[/bold] [cyan]{total}[/cyan]")

    role_table = Table(title="Entries by Role", border_style="blue", show_header=True)
    role_table.add_column("Role", style="magenta")
    role_table.add_column("Count", style="green", justify="right")

    for role, count in sorted(by_role.items()):
        role_table.add_row(role, str(count))

    console.print(role_table)
    console.print(f"[bold]Unique Chats:[/bold] [cyan]{len(by_chat)}[/cyan]\n")


@memory_app.command("cleanup")
def memory_cleanup(
    keep_days: int = typer.Option(30, "--keep-days", "-d", help="Keep entries newer than this (default: 30)"),
    keep_count: int = typer.Option(1000, "--keep-count", "-c", help="Keep this many most recent entries (default: 1000)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without deleting"),
) -> None:
    """Clean up old memory entries."""
    settings = load_settings()
    store = SQLiteStore(settings)

    if dry_run:
        # Show what would be deleted
        all_entries = store.list_memory_entries(limit=1000000)
        to_delete = []
        cutoff_date = _calculate_cutoff_date(keep_days)

        # Get most recent N entries
        recent_ids = {e.id for e in sorted(all_entries, key=lambda e: e.created_at, reverse=True)[:keep_count]}

        for entry in all_entries:
            if entry.id in recent_ids:
                continue
            if entry.created_at < cutoff_date:
                to_delete.append(entry)

        console.print(f"[yellow]Would delete {len(to_delete)} entries (dry run)[/yellow]")
        console.print(f"[dim]Parameters: keep_days={keep_days}, keep_count={keep_count}[/dim]")
        return

    deleted = store.cleanup_old_memory(keep_days=keep_days, keep_count=keep_count)
    console.print(f"[green]Deleted {deleted} old memory entries.[/green]")
    console.print(f"[dim]Parameters: keep_days={keep_days}, keep_count={keep_count}[/dim]")


def _calculate_cutoff_date(days: int):
    """Calculate cutoff date for cleanup."""
    from datetime import timedelta

    from broodmind.utils import utc_now
    return utc_now() - timedelta(days=days)


@config_app.command("show")
def config_show(reveal_secrets: bool = typer.Option(False, "--reveal-secrets", help="Show API keys and tokens")) -> None:
    """Show current configuration settings."""
    settings = load_settings()

    table = Table(title="BroodMind Configuration", border_style="blue", show_header=True)
    table.add_column("Setting", style="cyan")
    table.add_column("Value")

    secret_keywords = ("token", "key", "secret", "api_key")

    # Get values from settings, using aliases if possible
    for field_name, field in settings.model_fields.items():
        value = getattr(settings, field_name)

        is_secret = any(k in field_name.lower() or (field.alias and k in field.alias.lower()) for k in secret_keywords)

        if is_secret and not reveal_secrets and value:
            display_value = "[dim]********[/dim]"
        elif value is None:
            display_value = "[dim]None[/dim]"
        else:
            display_value = str(value)

        table.add_row(field.alias or field_name, display_value)

    console.print(table)


@app.command()
def logs(follow: bool = typer.Option(False, "--follow", "-f")) -> None:
    settings = load_settings()
    log_path = settings.state_dir / "logs" / "broodmind.log"
    if not log_path.exists():
        console.print(f"[red]Log file not found: {log_path}[/red]")
        raise typer.Exit(code=1)
    if not follow:
        console.print(log_path.read_text(encoding="utf-8"))
        return

    console.print(f"[dim]Tailing logs from {log_path} (Ctrl+C to stop)...[/dim]")
    with log_path.open("r", encoding="utf-8") as handle:
        handle.seek(0, 2)
        while True:
            line = handle.readline()
            if line:
                console.print(line.rstrip("\n"))
            else:
                time.sleep(0.5)


@app.command()
def gateway() -> None:
    settings = load_settings()
    app_instance = build_app(settings)
    import uvicorn

    uvicorn.run(app_instance, host=settings.gateway_host, port=settings.gateway_port)


@app.command()
def build_worker_image(tag: str = "broodmind-worker:latest") -> None:
    # Validate settings before building
    load_settings()
    project_root = Path(__file__).resolve().parents[3]
    dockerfile = project_root / "docker" / "Dockerfile"
    if not dockerfile.exists():
        console.print(f"[red]Dockerfile not found: {dockerfile}[/red]")
        raise typer.Exit(code=1)
    cmd = [
        "docker",
        "build",
        "--target",
        "worker",
        "-t",
        tag,
        "-f",
        str(dockerfile),
        str(project_root),
    ]
    console.print(f"[bold cyan]Running:[/bold cyan] {' '.join(cmd)}")
    raise SystemExit(__import__("subprocess").call(cmd))


@app.command("dashboard")
def dashboard(
    watch: bool = typer.Option(False, "--watch", "-w", help="Continuously refresh dashboard"),
    interval: float = typer.Option(2.0, "--interval", "-i", help="Refresh interval in seconds for --watch"),
    last: int = typer.Option(8, "--last", help="Number of recent workers to show"),
    json_output: bool = typer.Option(False, "--json", help="Print JSON snapshot instead of dashboard view"),
) -> None:
    """Show a live-style runtime dashboard (system, queen, workers, control channel)."""
    settings = load_settings()
    last = max(1, min(50, last))
    interval = max(0.5, min(30.0, interval))

    if json_output and watch:
        console.print("[red]--json cannot be used with --watch[/red]")
        raise typer.Exit(code=1)

    def _render_once() -> None:
        snapshot = _build_dashboard_snapshot(settings, last)
        if json_output:
            console.print(json.dumps(snapshot, ensure_ascii=False, indent=2))
            return
        _print_dashboard(snapshot)

    if not watch:
        _render_once()
        return

    try:
        while True:
            console.clear()
            _render_once()
            time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[dim]Dashboard watch stopped.[/dim]")


@app.command("sync-worker-templates")
def sync_worker_templates(
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing workspace worker templates"),
) -> None:
    """Copy default worker templates into workspace/workers."""
    settings = load_settings()
    result = sync_default_templates(settings.workspace_dir, overwrite=overwrite)
    console.print(
        "[green]Worker template sync complete[/green]: "
        f"copied={result['copied']} updated={result['updated']} skipped={result['skipped']}"
    )
    console.print(f"[dim]Target:[/dim] {settings.workspace_dir / 'workers'}")


app.add_typer(workers_app, name="workers")
app.add_typer(audit_app, name="audit")
app.add_typer(memory_app, name="memory")
app.add_typer(config_app, name="config")


def _build_dashboard_snapshot(settings: Settings, last: int) -> dict:
    status_data = read_status(settings) or {}
    pid = status_data.get("pid")
    running = is_pid_running(pid)
    metrics = read_metrics_snapshot(settings.state_dir) or {}
    queen_metrics = metrics.get("queen", {}) if isinstance(metrics, dict) else {}
    telegram_metrics = metrics.get("telegram", {}) if isinstance(metrics, dict) else {}
    exec_metrics = metrics.get("exec_run", {}) if isinstance(metrics, dict) else {}

    store = SQLiteStore(settings)
    workers = store.list_workers()
    now = _now_utc()
    cutoff = now.timestamp() - 24 * 60 * 60

    by_status: dict[str, int] = {}
    spawned_24h = 0
    for worker in workers:
        by_status[worker.status] = by_status.get(worker.status, 0) + 1
        if worker.created_at.timestamp() >= cutoff:
            spawned_24h += 1

    running_workers = by_status.get("running", 0) + by_status.get("started", 0)
    failed_workers = by_status.get("failed", 0)
    completed_workers = by_status.get("completed", 0)
    stopped_workers = by_status.get("stopped", 0)

    followup_q = int(queen_metrics.get("followup_queues", 0) or 0)
    internal_q = int(queen_metrics.get("internal_queues", 0) or 0)
    if running_workers > 0:
        queen_state = "tooling"
    elif (followup_q + internal_q) > 0:
        queen_state = "thinking"
    else:
        queen_state = "idle"

    requests = _read_jsonl(settings.state_dir / "control_requests.jsonl")
    acks = _read_jsonl(settings.state_dir / "control_acks.jsonl")
    acked_ids = {str(a.get("request_id", "")) for a in acks}
    pending_requests = [r for r in requests if str(r.get("request_id", "")) not in acked_ids]
    last_ack = acks[-1] if acks else None

    return {
        "system": {
            "running": running,
            "pid": pid,
            "started_at": status_data.get("started_at"),
            "last_heartbeat": status_data.get("last_message_at"),
            "uptime": _uptime_human(status_data.get("started_at")),
        },
        "queen": {
            "state": queen_state,
            "followup_queues": followup_q,
            "internal_queues": internal_q,
            "followup_tasks": int(queen_metrics.get("followup_tasks", 0) or 0),
            "internal_tasks": int(queen_metrics.get("internal_tasks", 0) or 0),
        },
        "queues": {
            "telegram_send_tasks": int(telegram_metrics.get("send_tasks", 0) or 0),
            "telegram_queues": int(telegram_metrics.get("chat_queues", 0) or 0),
            "exec_sessions_running": int(exec_metrics.get("background_sessions_running", 0) or 0),
            "exec_sessions_total": int(exec_metrics.get("background_sessions_total", 0) or 0),
        },
        "workers": {
            "spawned_24h": spawned_24h,
            "running": running_workers,
            "completed": completed_workers,
            "failed": failed_workers,
            "stopped": stopped_workers,
            "recent": [
                {
                    "id": w.id,
                    "status": w.status,
                    "task": w.task,
                    "updated_at": w.updated_at.isoformat(),
                    "summary": w.summary or "",
                    "error": w.error or "",
                }
                for w in workers[:last]
            ],
        },
        "control": {
            "pending_requests": len(pending_requests),
            "last_ack": last_ack,
        },
    }


def _print_dashboard(snapshot: dict) -> None:
    system = snapshot["system"]
    queen = snapshot["queen"]
    queues = snapshot["queues"]
    workers = snapshot["workers"]
    control = snapshot["control"]

    title = "[bold cyan]BROODMIND LIVE DASHBOARD[/bold cyan]"
    console.print(Panel(title, border_style="cyan", expand=True))

    sys_state = "[green]RUNNING[/green]" if system["running"] else "[red]STOPPED[/red]"
    queen_color = (
        "green"
        if queen["state"] == "idle"
        else "yellow" if queen["state"] == "thinking" else "cyan"
    )
    top = Table.grid(padding=(0, 2))
    top.add_column(style="bold white")
    top.add_column()
    top.add_row("[SYS] System:", f"{sys_state}  PID={system['pid'] or 'N/A'}  Uptime={system['uptime']}")
    top.add_row("[QN ] Queen:", f"[{queen_color}]{queen['state']}[/{queen_color}]  Last heartbeat={system['last_heartbeat'] or 'Never'}")
    console.print(Panel(top, border_style="blue", title="System"))

    qgrid = Table.grid(padding=(0, 2))
    qgrid.add_column(style="bold white")
    qgrid.add_column()
    qgrid.add_row("[Q ] Queen queues:", f"followup={queen['followup_queues']} internal={queen['internal_queues']}")
    qgrid.add_row("[TG] Telegram queues:", f"queues={queues['telegram_queues']} send_tasks={queues['telegram_send_tasks']}")
    qgrid.add_row("[EX] Exec sessions:", f"running={queues['exec_sessions_running']} total={queues['exec_sessions_total']}")
    console.print(Panel(qgrid, border_style="blue", title="Queues"))

    wgrid = Table.grid(padding=(0, 2))
    wgrid.add_column(style="bold white")
    wgrid.add_column()
    wgrid.add_row(
        "[WK] Worker summary:",
        (
            f"spawned_24h={workers['spawned_24h']}  "
            f"running={workers['running']}  completed={workers['completed']}  "
            f"failed={workers['failed']}  stopped={workers['stopped']}"
        ),
    )
    console.print(Panel(wgrid, border_style="blue", title="Workers"))

    recent = Table(title="Recent Workers", border_style="blue", show_header=True, header_style="bold cyan")
    recent.add_column("ID", style="dim", width=12)
    recent.add_column("Status", width=10)
    recent.add_column("Task")
    recent.add_column("Updated", style="dim", width=20)
    for row in workers["recent"]:
        status = row["status"]
        color = "green" if status == "completed" else "red" if status == "failed" else "yellow"
        recent.add_row(
            str(row["id"])[:12],
            f"[{color}]{status}[/{color}]",
            str(row["task"])[:80],
            str(row["updated_at"])[:19].replace("T", " "),
        )
    console.print(recent)

    cgrid = Table.grid(padding=(0, 2))
    cgrid.add_column(style="bold white")
    cgrid.add_column()
    cgrid.add_row("[CTL] Pending requests:", str(control["pending_requests"]))
    last_ack = control.get("last_ack")
    if isinstance(last_ack, dict):
        cgrid.add_row(
            "[CTL] Last ack:",
            (
                f"{last_ack.get('action', '?')} "
                f"{last_ack.get('status', '?')} "
                f"{str(last_ack.get('acked_at', ''))[:19].replace('T', ' ')}"
            ),
        )
    else:
        cgrid.add_row("[CTL] Last ack:", "none")
    console.print(Panel(cgrid, border_style="blue", title="Control Channel"))


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
            if isinstance(item, dict):
                out.append(item)
        except Exception:
            continue
    return out


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _uptime_human(started_at: str | None) -> str:
    if not started_at:
        return "N/A"
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        delta = _now_utc() - start
        total = int(delta.total_seconds())
        if total < 0:
            return "N/A"
        hours = total // 3600
        minutes = (total % 3600) // 60
        seconds = total % 60
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    except Exception:
        return "N/A"


if __name__ == "__main__":
    app()
