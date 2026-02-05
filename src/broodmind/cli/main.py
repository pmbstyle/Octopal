from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import print as rprint

from broodmind.cli.branding import print_banner
from broodmind.config.settings import Settings, load_settings
from broodmind.gateway.app import build_app
from broodmind.logging_config import configure_logging
from broodmind.state import is_pid_running, read_status, write_start_status
from broodmind.store.sqlite import SQLiteStore
from broodmind.telegram.bot import run_bot

app = typer.Typer(add_completion=False)
workers_app = typer.Typer(add_completion=False)
audit_app = typer.Typer(add_completion=False)
memory_app = typer.Typer(add_completion=False)
config_app = typer.Typer(add_completion=False)

console = Console()


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
def start() -> None:
    settings = load_settings()
    _init_logging(settings)
    
    print_banner()
    
    with console.status("[bold green]Initializing BroodMind Queen...[/bold green]", spinner="dots"):
        write_start_status(settings)
        time.sleep(0.5) # A little pause for dramatic effect / to let FS settle
    
    console.print(f"[bold green]✓ BroodMind Queen started.[/bold green]")
    console.print(f"   [dim]Logs directory:[/dim] [cyan]{settings.state_dir / 'logs'}[/cyan]")
    console.print("[dim]Press Ctrl+C to stop.[/dim]\n")

    try:
        asyncio.run(run_bot(settings))
    except KeyboardInterrupt:
        # Use standard logging here as structlog might be torn down
        logging.getLogger(__name__).info("Shutting down")
        console.print("\n[bold yellow]Shutting down...[/bold yellow]")


@app.command()
def stop() -> None:
    settings = load_settings()
    status_data = read_status(settings)
    pid = status_data.get("pid") if status_data else None
    
    if not pid or not is_pid_running(pid):
        console.print("[yellow]BroodMind is not running.[/yellow]")
        return
        
    console.print(f"[bold yellow]Stopping BroodMind (PID: {pid})...[/bold yellow]")
    import os
    import platform
    
    try:
        if platform.system() == "Windows":
            import subprocess
            subprocess.run(["taskkill", "/F", "/PID", str(pid)], check=True, capture_output=True)
        else:
            import signal
            os.kill(pid, signal.SIGTERM)
        console.print("[bold green]✓ BroodMind stopped.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Failed to stop BroodMind: {e}[/bold red]")


@app.command()
def restart() -> None:
    """Stop and then start the BroodMind Queen."""
    stop()
    with console.status("[bold yellow]Waiting for system to settle...[/bold yellow]"):
        time.sleep(2)
    start()


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
    
    from rich.syntax import Syntax
    import json
    
    # Try to pretty print data if it's a dict or similar
    data_str = str(event.data)
    try:
        if isinstance(event.data, (dict, list)):
            data_str = json.dumps(event.data, indent=2)
            syntax = Syntax(data_str, "json", theme="monokai", background_color="default")
            console.print(Panel(syntax, title="Data Payload", border_style="white"))
        else:
             console.print(Panel(data_str, title="Data Payload", border_style="white"))
    except:
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
        recent_ids = set(e.id for e in sorted(all_entries, key=lambda e: e.created_at, reverse=True)[:keep_count])

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
    settings = load_settings()
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


app.add_typer(workers_app, name="workers")
app.add_typer(audit_app, name="audit")
app.add_typer(memory_app, name="memory")
app.add_typer(config_app, name="config")


if __name__ == "__main__":
    app()
