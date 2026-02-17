from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

import typer
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
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
from broodmind.telegram.bot import run_bot, build_dispatcher
from broodmind.workers.templates import sync_default_templates
from aiogram import Bot

app = typer.Typer(add_completion=False)
workers_app = typer.Typer(add_completion=False)
audit_app = typer.Typer(add_completion=False)
memory_app = typer.Typer(add_completion=False)
config_app = typer.Typer(add_completion=False)

console = Console()
logger = logging.getLogger(__name__)


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
    console.print(f"   [dim]Gateway:[/dim] [cyan]http://{settings.gateway_host}:{settings.gateway_port}[/cyan]")
    console.print("[dim]Press Ctrl+C to stop (if in foreground).[/dim]\n")

    async def run_all():
        bot_instance = Bot(token=settings.telegram_bot_token)
        dp, queen = build_dispatcher(settings, bot_instance)
        
        # Build and run Gateway alongside bot
        gateway_app = build_app(settings, queen)
        import uvicorn
        config = uvicorn.Config(gateway_app, host=settings.gateway_host, port=settings.gateway_port, log_level="info")
        server = uvicorn.Server(config)
        
        # Background task for uvicorn
        gateway_task = asyncio.create_task(server.serve())
        
        try:
            await run_bot(settings, existing_queen=queen)
        finally:
            server.should_exit = True
            await gateway_task

    try:
        asyncio.run(run_all())
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

    status_color = "bright_green" if running else "bright_red"
    status_text = "RUNNING" if running else "STOPPED"

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan", justify="right")
    grid.add_column(style="white")

    grid.add_row("System Status", f"[{status_color}]{status_text}[/{status_color}]")
    grid.add_row("Active Channel", f"[bold]{status_data.get('active_channel', 'Telegram')}[/bold]")
    grid.add_row("Process ID", f"[bold]{pid}[/bold]" if pid else "[dim]N/A[/dim]")
    grid.add_row("Last Heartbeat", str(last_message) if last_message else "[dim]Never[/dim]")
    grid.add_row("Configuration", "[bright_green]Valid[/bright_green]" if config_ok else "[bright_red]Invalid[/bright_red]")

    metrics = read_metrics_snapshot(settings.state_dir)
    queen_metrics = metrics.get("queen", {}) if isinstance(metrics, dict) else {}
    telegram_metrics = metrics.get("telegram", {}) if isinstance(metrics, dict) else {}
    exec_metrics = metrics.get("exec_run", {}) if isinstance(metrics, dict) else {}
    if metrics:
        grid.add_row("")
        grid.add_row("Queen Queues", f"[dim]followup=[/dim]{queen_metrics.get('followup_queues', 0)} [dim]internal=[/dim]{queen_metrics.get('internal_queues', 0)}")
        grid.add_row("Telegram Chat", f"[dim]queues=[/dim]{telegram_metrics.get('chat_queues', 0)} [dim]tasks=[/dim]{telegram_metrics.get('send_tasks', 0)}")
        grid.add_row("Exec Sessions", f"[dim]running=[/dim]{exec_metrics.get('background_sessions_running', 0)} [dim]total=[/dim]{exec_metrics.get('background_sessions_total', 0)}")
    else:
        grid.add_row("Metrics", "[dim]Not available[/dim]")

    console.print("\n")
    console.print(Align.center(Panel(
        grid,
        title="[bold white]BroodMind System Status[/bold white]",
        border_style="bright_blue",
        expand=False,
        padding=(1, 3)
    )))
    console.print("\n")


@workers_app.command("list")
def workers_list() -> None:
    settings = load_settings()
    store = SQLiteStore(settings)
    workers = store.list_workers()
    if not workers:
        console.print("[yellow]No workers found.[/yellow]")
        return

    table = Table(title="Registered Workers", border_style="bright_blue", show_header=True, header_style="bold cyan", expand=False)
    table.add_column("Worker ID", style="dim", width=20)
    table.add_column("Status", width=12)
    table.add_column("Current Task", width=50)

    for worker in workers:
        status_style = "bright_green" if worker.status == "completed" else "yellow" if worker.status in ("running", "working") else "bright_red" if worker.status == "failed" else "dim white"
        table.add_row(
            worker.id,
            f"[{status_style}]{worker.status}[/{status_style}]",
            (worker.task[:47] + "...") if worker.task and len(worker.task) > 50 else (worker.task or "[dim]-[/dim]")
        )
    
    console.print("\n")
    console.print(Align.center(table))
    console.print("\n")


@audit_app.command("list")
def audit_list(limit: int = 50) -> None:
    settings = load_settings()
    store = SQLiteStore(settings)
    events = store.list_audit(limit=limit)
    if not events:
        console.print("[yellow]No audit events found.[/yellow]")
        return

    table = Table(title=f"Audit Log (Last {limit})", border_style="bright_blue", header_style="bold cyan", expand=False)
    table.add_column("ID", style="dim", width=10)
    table.add_column("Timestamp", style="white", width=20)
    table.add_column("Level", width=10)
    table.add_column("Type", style="bright_green", width=20)
    table.add_column("Correlation ID", style="dim", width=12)

    for event in events:
        level_style = "bright_red" if event.level in ("ERROR", "CRITICAL") else "yellow" if event.level == "WARNING" else "bright_blue"
        table.add_row(
            event.id[:10],
            event.ts.isoformat(timespec='seconds').replace("T", " "),
            f"[{level_style}]{event.level}[/{level_style}]",
            event.event_type,
            (event.correlation_id[:12] if event.correlation_id else "")
        )
    
    console.print("\n")
    console.print(Align.center(table))
    console.print("\n")


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

    console.print("\n")
    console.print(Align.center(Panel(grid, title="[bold white]Audit Event Details[/bold white]", border_style="bright_blue", expand=False, padding=(1, 4))))

    import json

    from rich.syntax import Syntax

    # Try to pretty print data if it's a dict or similar
    data_str = str(event.data)
    try:
        if isinstance(event.data, dict | list):
            data_str = json.dumps(event.data, indent=2)
            syntax = Syntax(data_str, "json", theme="monokai", background_color="default")
            console.print(Align.center(Panel(syntax, title="[bold white]Data Payload[/bold white]", border_style="white", expand=False, padding=(1, 2))))
        else:
             console.print(Align.center(Panel(data_str, title="[bold white]Data Payload[/bold white]", border_style="white", expand=False, padding=(1, 2))))
    except Exception:
        console.print(Align.center(Panel(str(event.data), title="[bold white]Data Payload[/bold white]", border_style="white", expand=False, padding=(1, 2))))
    console.print("\n")


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

    console.print("\n")
    console.print(Align.center(f"[bold white]Total Memory Entries:[/bold white] [bright_cyan]{total}[/bright_cyan] [dim]|[/dim] [bold white]Unique Chats:[/bold white] [bright_cyan]{len(by_chat)}[/bright_cyan]"))

    role_table = Table(title="Entries by Role", border_style="bright_blue", show_header=True, expand=False)
    role_table.add_column("Role", style="magenta", width=20)
    role_table.add_column("Count", style="bright_green", justify="right", width=10)

    for role, count in sorted(by_role.items()):
        role_table.add_row(role, str(count))

    console.print("\n")
    console.print(Align.center(role_table))
    console.print("\n")


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

    table = Table(title="BroodMind Configuration", border_style="bright_blue", show_header=True, expand=False)
    table.add_column("Setting", style="bright_cyan", width=35)
    table.add_column("Value", width=45)

    secret_keywords = ("token", "key", "secret", "api_key")

    # Get values from settings, using aliases if possible
    for field_name, field in settings.model_fields.items():
        value = getattr(settings, field_name)

        is_secret = any(k in field_name.lower() or (field.alias and k in field.alias.lower()) for k in secret_keywords)

        if is_secret and not reveal_secrets and value:
            display_value = "[dim]●●●●●●●●[/dim]"
        elif value is None:
            display_value = "[dim]None[/dim]"
        else:
            display_value = str(value)

        table.add_row(field.alias or field_name, display_value)

    console.print("\n")
    console.print(Align.center(table))
    console.print("\n")


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
    watch: bool = typer.Option(True, "--watch/--once", "-w/-o", help="Continuously refresh dashboard (default) or show once"),
    interval: float = typer.Option(2.0, "--interval", "-i", help="Refresh interval in seconds for live mode"),
    last: int = typer.Option(8, "--last", help="Number of recent workers to show"),
    compact: bool = typer.Option(False, "--compact", help="Use compact view optimized for narrow terminals"),
    json_output: bool = typer.Option(False, "--json", help="Print JSON snapshot instead of dashboard view"),
) -> None:
    """Show a live runtime dashboard (system, queen, workers, control channel)."""
    settings = load_settings()
    last = max(1, min(50, last))
    refresh_interval = max(0.5, min(30.0, interval))

    # JSON output always implies --once
    if json_output:
        watch = False

    # Initialize store once for reuse
    store = SQLiteStore(settings)

    if not watch:
        snapshot = _build_dashboard_snapshot(settings, last, store=store)
        if json_output:
            console.print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        else:
            _print_dashboard(snapshot, compact=compact)
        return

    # Live watch mode
    try:
        with Live(
            _build_dashboard_renderable(_build_dashboard_snapshot(settings, last, store=store), compact=compact),
            console=console,
            refresh_per_second=1/refresh_interval,
            screen=True
        ) as live:
            while True:
                time.sleep(refresh_interval)
                try:
                    snapshot = _build_dashboard_snapshot(settings, last, store=store)
                    live.update(_build_dashboard_renderable(snapshot, compact=compact))
                except Exception as e:
                    # Log error internally but keep the dashboard alive
                    logger.debug(f"Dashboard refresh error: {e}")
    except KeyboardInterrupt:
        pass


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


def _build_dashboard_snapshot(settings: Settings, last: int, store: SQLiteStore | None = None) -> dict:
    status_data = read_status(settings) or {}
    pid = status_data.get("pid")
    running = is_pid_running(pid)
    metrics = read_metrics_snapshot(settings.state_dir) or {}
    queen_metrics = metrics.get("queen", {}) if isinstance(metrics, dict) else {}
    telegram_metrics = metrics.get("telegram", {}) if isinstance(metrics, dict) else {}
    exec_metrics = metrics.get("exec_run", {}) if isinstance(metrics, dict) else {}
    connectivity_metrics = metrics.get("connectivity", {}) if isinstance(metrics, dict) else {}

    if store is None:
        store = SQLiteStore(settings)
    
    # Use active workers for health metrics to avoid stale 'running' states
    active_workers = store.get_active_workers(older_than_minutes=5)
    all_recent_workers = store.list_workers()
    
    now = _now_utc()
    cutoff = now.timestamp() - 24 * 60 * 60

    by_status: dict[str, int] = {}
    spawned_24h = 0
    # Process all workers for 24h stats
    for worker in all_recent_workers:
        if worker.created_at.timestamp() >= cutoff:
            spawned_24h += 1
            
    # Process only active/recent workers for status counts
    for worker in active_workers:
        by_status[worker.status] = by_status.get(worker.status, 0) + 1

    running_workers = by_status.get("running", 0) + by_status.get("started", 0)
    failed_workers = by_status.get("failed", 0)
    completed_workers = by_status.get("completed", 0)
    stopped_workers = by_status.get("stopped", 0)

    followup_q = int(queen_metrics.get("followup_queues", 0) or 0)
    internal_q = int(queen_metrics.get("internal_queues", 0) or 0)
    thinking_count = int(queen_metrics.get("thinking_count", 0) or 0)

    if thinking_count > 0 or (followup_q + internal_q) > 0:
        queen_state = "thinking"
    else:
        queen_state = "idle"

    requests = _read_jsonl(settings.state_dir / "control_requests.jsonl")
    acks = _read_jsonl(settings.state_dir / "control_acks.jsonl")
    acked_ids = {str(a.get("request_id", "")) for a in acks}
    pending_requests = [r for r in requests if str(r.get("request_id", "")) not in acked_ids]
    last_ack = acks[-1] if acks else None

    # Fetch recent log lines for real-time visibility
    log_path = settings.state_dir / "logs" / "broodmind.log"
    recent_logs = []
    if log_path.exists():
        try:
            # Get last 12 lines
            all_lines = log_path.read_text(encoding="utf-8").splitlines()
            for line in all_lines[-12:]:
                try:
                    data = json.loads(line)
                    event = data.get("event", "")
                    level = data.get("level", "info")
                    # Simplified log entry for dashboard
                    recent_logs.append({"event": event, "level": level})
                except Exception:
                    if line.strip():
                        recent_logs.append({"event": line.strip()[:100], "level": "info"})
        except Exception:
            pass

    return {
        "system": {
            "running": running,
            "pid": pid,
            "active_channel": status_data.get("active_channel", "Telegram"),
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
        "connectivity": {
            "mcp_servers": connectivity_metrics.get("mcp_servers", {})
        },
        "logs": recent_logs,
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
                    "tools_used": w.tools_used or [],
                }
                for w in all_recent_workers[:last]
            ],
        },
        "control": {
            "pending_requests": len(pending_requests),
            "last_ack": last_ack,
        },
    }


def _build_dashboard_renderable(snapshot: dict, compact: bool = False) -> Align:
    system = snapshot["system"]
    queen = snapshot["queen"]
    queues = snapshot["queues"]
    workers = snapshot["workers"]
    control = snapshot["control"]
    connectivity = snapshot.get("connectivity", {})
    logs = snapshot.get("logs", [])

    if console.size.width < 120:
        compact = True

    def _fmt_status(label: str, status: str) -> str:
        icon = _status_icon(status)
        return f"[dim]{label}:[/dim] {icon} [dim]({status})[/dim]"

    sys_status = "running" if system["running"] else "stopped"
    worker_status = "running" if workers["running"] > 0 else "idle"
    active_channel = system.get("active_channel", "Telegram")
    channel_color = "bright_magenta" if active_channel == "WebSocket" else "bright_blue"
    
    header_text = (
        f"[bold bright_cyan]BROODMIND DASHBOARD[/bold bright_cyan]   "
        f"{_fmt_status('Sys', sys_status)}   "
        f"{_fmt_status('Queen', queen['state'])}   "
        f"{_fmt_status('Workers', worker_status)}   "
        f"[dim]Channel[/dim] [{channel_color}]{active_channel}[/{channel_color}]   "
        f"[dim]PID[/dim] {system['pid'] or 'N/A'}   "
        f"[dim]Uptime[/dim] {system['uptime']}"
    )
    header = Panel(header_text, border_style="bright_blue", padding=(0, 1))

    health = Table.grid(padding=(0, 2))
    health.add_column(style="bold cyan", justify="right")
    health.add_column(style="white")
    health.add_row("Heartbeat", str(system["last_heartbeat"] or "Never"))
    health.add_row(
        "Queues",
        (
            f"followup={queen['followup_queues']} internal={queen['internal_queues']} "
            f"telegram={queues['telegram_queues']} send_tasks={queues['telegram_send_tasks']}"
        ),
    )
    health.add_row(
        "Workers",
        (
            f"running={workers['running']} completed={workers['completed']} "
            f"failed={workers['failed']} stopped={workers['stopped']} spawned_24h={workers['spawned_24h']}"
        ),
    )
    health_panel = Panel(health, title="[bold white]Runtime Health[/bold white]", border_style="blue")

    # MCP Connectivity Panel
    mcp_grid = Table.grid(padding=(0, 1))
    mcp_grid.add_column(style="bold white")
    mcp_grid.add_column()
    mcp_servers = connectivity.get("mcp_servers", {})
    if not mcp_servers:
        mcp_grid.add_row("[dim]No servers configured[/dim]")
    else:
        for s_id, s_data in mcp_servers.items():
            status = s_data.get("status", "unknown")
            color = "bright_green" if status == "connected" else "bright_red" if status == "error" else "yellow"
            mcp_grid.add_row(f"{s_data.get('name', s_id)}:", f"[{color}]{status.upper()}[/{color}] [dim]({s_data.get('tool_count', 0)} tools)[/dim]")
    connectivity_panel = Panel(mcp_grid, title="[bold white]MCP Connectivity[/bold white]", border_style="cyan")

    # Recent Logs Panel
    log_grid = Table.grid(padding=(0, 1))
    log_grid.add_column()
    if not logs:
        log_grid.add_row("[dim]No recent logs[/dim]")
    else:
        for entry in logs:
            lvl = entry.get("level", "info").lower()
            color = "red" if lvl in ("error", "critical") else "yellow" if lvl == "warning" else "white"
            log_grid.add_row(f"[{color}]•[/{color}] {entry.get('event', '')[:80]}")
    logs_panel = Panel(log_grid, title="[bold white]Recent Events[/bold white]", border_style="bright_black")

    recent = Table(show_header=True, header_style="bold cyan", expand=True)
    recent.add_column("ID", style="dim", width=8, no_wrap=True)
    recent.add_column("S", width=3, justify="center", no_wrap=True)
    recent.add_column("Age", width=8, justify="right", no_wrap=True)
    recent.add_column("Task", overflow="ellipsis")
    recent.add_column("Current Activity", style="italic", width=25, overflow="ellipsis")
    if not compact:
        recent.add_column("Updated (UTC)", style="dim", width=12, no_wrap=True)

    for row in workers["recent"]:
        updated_at_str = str(row["updated_at"])
        last_tool = row.get("tools_used", [])[-1] if row.get("tools_used") else "-"
        
        # Format worker ID: first set before first dash OR name truncated to 8
        wid = str(row["id"])
        if "-" in wid:
            display_id = wid.split("-")[0]
        else:
            display_id = _truncate(wid, 8)

        # Updated (UTC) format: month/day hour:minute
        try:
            dt = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
            ts_display = dt.strftime("%m/%d %H:%M")
        except Exception:
            ts_display = updated_at_str[:19].replace("T", " ")

        recent.add_row(
            display_id,
            _status_icon(str(row["status"])),
            _age_human(updated_at_str),
            _truncate(str(row["task"]), 60 if compact else 80),
            str(last_tool),
            *([ts_display] if not compact else []),
        )
    workers_panel = Panel(recent, title=f"[bold white]Recent Workers ({len(workers['recent'])})[/bold white]", border_style="blue")

    alerts = _build_alert_lines(snapshot)
    alerts_table = Table.grid(padding=(0, 1))
    alerts_table.add_column()
    for line in alerts:
        alerts_table.add_row(line)
    alerts_panel = Panel(alerts_table, title="[bold white]Attention[/bold white]", border_style="yellow")

    footer = Panel(
        "[bold cyan]Hints:[/bold cyan] broodmind dashboard -w | broodmind logs -f | broodmind workers list",
        border_style="bright_black",
        padding=(0, 1),
    )

    root = Layout(name="root")
    root.split_column(
        Layout(header, size=3, name="header"),
        Layout(name="body"),
        Layout(footer, size=3, name="footer"),
    )

    if compact:
        root["body"].split_column(
            Layout(health_panel, size=8),
            Layout(connectivity_panel, size=6),
            Layout(logs_panel, size=10),
            Layout(workers_panel),
        )
    else:
        root["body"].split_row(
            Layout(workers_panel, ratio=3),
            Layout(name="side", ratio=2),
        )
        root["body"]["side"].split_column(
            Layout(health_panel, size=7),
            Layout(connectivity_panel, size=8),
            Layout(alerts_panel, size=6),
            Layout(logs_panel),
        )

    return Align.center(root)


def _print_dashboard(snapshot: dict, compact: bool = False) -> None:
    console.print(_build_dashboard_renderable(snapshot, compact=compact))


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


def _status_icon(status: str) -> str:
    s = (status or "").strip().lower()
    if s in {"completed", "ok"}:
        return "[bright_green]✔[/bright_green]"
    if s in {"running", "started"}:
        return "[bright_green]▶[/bright_green]"
    if s in {"thinking", "tooling", "idle"}:
        return "[yellow]●[/yellow]"
    if s in {"stopped"}:
        return "[yellow]■[/yellow]"
    return "[bright_red]✘[/bright_red]"


def _age_human(ts: str | None) -> str:
    if not ts:
        return "-"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        delta = _now_utc() - dt
        total = int(max(0, delta.total_seconds()))
        if total < 60:
            return f"{total}s"
        if total < 3600:
            return f"{total // 60}m"
        return f"{total // 3600}h"
    except Exception:
        return "-"


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _build_alert_lines(snapshot: dict) -> list[str]:
    workers = snapshot["workers"]
    control = snapshot["control"]
    system = snapshot["system"]

    lines: list[str] = []
    if not system["running"]:
        lines.append("[bright_red]System is stopped.[/bright_red]")
    if workers["failed"] > 0:
        lines.append(f"[bright_red]{workers['failed']} failed worker(s).[/bright_red]")
    if control["pending_requests"] > 0:
        lines.append(f"[yellow]{control['pending_requests']} pending control request(s).[/yellow]")

    for row in workers["recent"]:
        if str(row.get("status", "")).lower() == "failed":
            wid = str(row.get("id", ""))[:12]
            err = _truncate(str(row.get("error") or row.get("summary") or "failure"), 60)
            lines.append(f"[yellow]{wid}[/yellow]: {err}")
            if len(lines) >= 4:
                break

    if not lines:
        lines.append("[bright_green]No active alerts.[/bright_green]")
    return lines[:6]


if __name__ == "__main__":
    app()
