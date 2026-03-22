from __future__ import annotations

import asyncio
import json
import logging
import shutil
import subprocess
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path

import typer
from rich import box
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.prompt import Confirm
from rich.table import Table

from broodmind.channels import normalize_user_channel, user_channel_label
from broodmind.cli.branding import print_banner
from broodmind.infrastructure.config.settings import Settings, load_settings, save_config
from broodmind.gateway.app import build_app
from broodmind.infrastructure.logging import configure_logging
from broodmind.infrastructure.providers.profile_resolver import resolve_litellm_profile
from broodmind.runtime.metrics import read_metrics_snapshot
from broodmind.runtime.state import (
    is_pid_running,
    list_broodmind_runtime_pids,
    pid_command_line,
    read_status,
    write_start_status,
)
from broodmind.infrastructure.store.sqlite import SQLiteStore
from broodmind.channels.whatsapp.bridge import WhatsAppBridgeController, WhatsAppBridgeError
from broodmind.channels.whatsapp.ids import parse_allowed_whatsapp_numbers
from broodmind.channels.whatsapp.runtime import WhatsAppRuntime
from broodmind.runtime.workers.templates import sync_default_templates
from broodmind.tools.skills.installer import (
    install_skill_from_source,
    update_installed_skill,
)
from broodmind.tools.skills.management import list_skill_inventory, remove_skill, set_skill_trust
from broodmind.tools.skills.runtime_envs import (
    prepare_skill_env,
    remove_skill_env,
)
from broodmind.tools import get_tools, resolve_tool_diagnostics
from broodmind.tools.registry import ToolPolicy, ToolPolicyPipelineStep, ToolSpec
from aiogram import Bot

app = typer.Typer(add_completion=False)
workers_app = typer.Typer(add_completion=False)
audit_app = typer.Typer(add_completion=False)
memory_app = typer.Typer(add_completion=False)
config_app = typer.Typer(add_completion=False)
whatsapp_app = typer.Typer(add_completion=False)
tools_app = typer.Typer(add_completion=False)
skill_app = typer.Typer(add_completion=False)

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


def _maybe_enable_tailscale_serve(settings: Settings) -> None:
    if not settings.tailscale_auto_serve:
        return

    target_host = "127.0.0.1"
    target = f"http://{target_host}:{settings.gateway_port}"
    expected_proxy = f"proxy http://{target_host}:{settings.gateway_port}"

    def _run(cmd: list[str], timeout: int = 8) -> subprocess.CompletedProcess[str] | None:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except FileNotFoundError:
            return None
        except Exception as exc:
            logger.debug("Tailscale command failed: cmd=%s error=%s", cmd, exc)
            return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr=str(exc))

    status_proc = _run(["tailscale", "serve", "status"])
    if status_proc is None:
        return
    current_status = (status_proc.stdout or "").strip() if status_proc.returncode == 0 else ""
    if expected_proxy in current_status:
        logger.debug("Tailscale serve already mapped to expected target: %s", target)
    else:
        attempts = [
            ["tailscale", "serve", "--bg", target],
            ["tailscale", "serve", "--bg", str(settings.gateway_port)],
        ]
        ok = False
        last_err = ""
        for cmd in attempts:
            proc = _run(cmd)
            if proc is None:
                return
            if proc.returncode == 0:
                ok = True
                break
            last_err = (proc.stderr or proc.stdout or "").strip()

        # Self-heal stale serve mappings that point to old ports/processes.
        if not ok:
            reset_proc = _run(["tailscale", "serve", "reset"])
            repair_proc = _run(["tailscale", "serve", "--bg", target])
            if repair_proc is not None and repair_proc.returncode == 0:
                ok = True
            else:
                if reset_proc is not None:
                    last_err = (reset_proc.stderr or reset_proc.stdout or "").strip() or last_err
                if repair_proc is not None:
                    last_err = (repair_proc.stderr or repair_proc.stdout or "").strip() or last_err

        verify_proc = _run(["tailscale", "serve", "status"])
        verified_status = (verify_proc.stdout or "").strip() if (verify_proc and verify_proc.returncode == 0) else ""
        if expected_proxy not in verified_status:
            ok = False
            if verify_proc is not None:
                last_err = (verify_proc.stderr or verify_proc.stdout or "").strip() or last_err

        if not ok:
            if last_err:
                logger.debug("Tailscale serve auto-config skipped: %s", last_err)
            return

    public_hint = ""
    try:
        status_proc = subprocess.run(
            ["tailscale", "status", "--json"],
            capture_output=True,
            text=True,
            timeout=8,
        )
        if status_proc.returncode == 0 and status_proc.stdout:
            status_data = json.loads(status_proc.stdout)
            dns_name = str(status_data.get("Self", {}).get("DNSName", "")).strip().rstrip(".")
            if dns_name:
                public_hint = f"https://{dns_name}"
    except Exception:
        pass

    if public_hint:
        console.print(
            f"[bold green][V] Tailscale Serve enabled[/bold green] -> "
            f"[cyan]{public_hint}[/cyan] (proxy to {target})"
        )
    else:
        console.print(
            f"[bold green][V] Tailscale Serve enabled[/bold green] "
            f"(proxy to [cyan]{target}[/cyan])"
        )


def _resolve_webapp_paths(settings: Settings) -> tuple[Path, Path]:
    project_root = Path(__file__).resolve().parents[3]
    webapp_dir = project_root / "webapp"
    if settings.webapp_dist_dir is not None:
        dist_dir = Path(settings.webapp_dist_dir)
        if not dist_dir.is_absolute():
            dist_dir = project_root / dist_dir
    else:
        dist_dir = webapp_dir / "dist"
    return webapp_dir, dist_dir


def _latest_mtime(paths: list[Path]) -> float:
    latest = 0.0
    for path in paths:
        if not path.exists():
            continue
        if path.is_file():
            latest = max(latest, path.stat().st_mtime)
            continue
        for file_path in path.rglob("*"):
            if file_path.is_file():
                latest = max(latest, file_path.stat().st_mtime)
    return latest


def _is_webapp_build_stale(webapp_dir: Path, dist_dir: Path) -> bool:
    index_html = dist_dir / "index.html"
    if not index_html.exists():
        return True

    source_paths = [
        webapp_dir / "index.html",
        webapp_dir / "package.json",
        webapp_dir / "package-lock.json",
        webapp_dir / "vite.config.ts",
        webapp_dir / "tsconfig.json",
        webapp_dir / "src",
    ]
    dist_paths = [dist_dir]
    latest_source = _latest_mtime(source_paths)
    latest_dist = _latest_mtime(dist_paths)
    return latest_source > latest_dist


def _run_webapp_command(cmd: list[str], *, cwd: Path) -> None:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        return
    detail = (proc.stderr or proc.stdout or "").strip()
    message = f"Webapp command failed: {' '.join(cmd)}"
    if detail:
        message = f"{message}\n{detail}"
    raise RuntimeError(message)


def _has_webapp_build_toolchain(webapp_dir: Path) -> bool:
    node_modules_dir = webapp_dir / "node_modules"
    required_packages = (
        "typescript",
        "vite",
        "tailwindcss",
        "postcss",
        "autoprefixer",
    )
    return all((node_modules_dir / package / "package.json").is_file() for package in required_packages)


def _ensure_webapp_built(settings: Settings) -> None:
    if not settings.webapp_enabled:
        return

    webapp_dir, dist_dir = _resolve_webapp_paths(settings)
    if not webapp_dir.exists():
        console.print("[bold red]Webapp directory not found:[/bold red] " + str(webapp_dir))
        raise typer.Exit(code=1)

    npm_path = shutil.which("npm")
    if not npm_path:
        console.print("[bold red]npm is required to build dashboard assets but was not found.[/bold red]")
        raise typer.Exit(code=1)

    build_needed = _is_webapp_build_stale(webapp_dir, dist_dir)
    if not build_needed:
        console.print("[dim]Web dashboard assets are up to date.[/dim]")
        return

    console.print("[bold cyan]Preparing web dashboard assets...[/bold cyan]")
    node_modules_dir = webapp_dir / "node_modules"
    try:
        if not node_modules_dir.exists() or not _has_webapp_build_toolchain(webapp_dir):
            has_lock = (webapp_dir / "package-lock.json").is_file()
            install_attempts: list[list[str]] = []
            if has_lock:
                install_attempts.extend(
                    [
                        [npm_path, "ci", "--include=dev", "--no-audit", "--no-fund"],
                        [npm_path, "ci", "--production=false", "--no-audit", "--no-fund"],
                    ]
                )
            install_attempts.extend(
                [
                    [npm_path, "install", "--include=dev", "--no-audit", "--no-fund"],
                    [npm_path, "install", "--production=false", "--no-audit", "--no-fund"],
                ]
            )

            install_error: RuntimeError | None = None
            for install_cmd in install_attempts:
                try:
                    _run_webapp_command(install_cmd, cwd=webapp_dir)
                    install_error = None
                    break
                except RuntimeError as exc:
                    install_error = exc
            if install_error is not None:
                raise install_error
        _run_webapp_command([npm_path, "run", "build"], cwd=webapp_dir)
    except RuntimeError as exc:
        console.print(f"[bold red]Failed to build web dashboard:[/bold red]\n{exc}")
        raise typer.Exit(code=1) from None

    if not (dist_dir / "index.html").exists():
        console.print("[bold red]Web dashboard build completed but dist/index.html is missing.[/bold red]")
        raise typer.Exit(code=1)
    console.print("[bold green][V] Web dashboard assets built.[/bold green]")


@app.command()
def start(
    foreground: bool = typer.Option(False, "--foreground", "-f", help="Run in foreground mode (showing logs)"),
) -> None:
    """Start the BroodMind Queen."""
    from broodmind.channels.telegram.bot import run_bot, build_dispatcher

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
    _ensure_webapp_built(settings)
    _maybe_enable_tailscale_serve(settings)

    with console.status("[bold green]Initializing BroodMind Queen...[/bold green]", spinner="dots"):
        write_start_status(settings)
        time.sleep(0.5)

    # Use ASCII checkmark [V] instead of unicode checkmark to avoid encoding issues in background processes
    console.print("[bold green][V] BroodMind Queen started.[/bold green]")
    console.print(f"   [dim]Logs directory:[/dim] [cyan]{settings.state_dir / 'logs'}[/cyan]")
    console.print(f"   [dim]Gateway:[/dim] [cyan]http://{settings.gateway_host}:{settings.gateway_port}[/cyan]")
    console.print("[dim]Press Ctrl+C to stop (if in foreground).[/dim]\n")

    async def run_all():
        selected_channel = normalize_user_channel(settings.user_channel)
        if selected_channel == "whatsapp":
            whatsapp_runtime = WhatsAppRuntime(settings)
            queen = await whatsapp_runtime.start()
            gateway_app = build_app(settings, queen)
            gateway_app.state.whatsapp_runtime = whatsapp_runtime
        else:
            bot_instance = Bot(token=settings.telegram_bot_token)
            _dp, queen = build_dispatcher(settings, bot_instance)
            gateway_app = build_app(settings, queen)
        import uvicorn
        config = uvicorn.Config(gateway_app, host=settings.gateway_host, port=settings.gateway_port, log_level="info")
        server = uvicorn.Server(config)
        gateway_task = asyncio.create_task(server.serve())
        try:
            if selected_channel == "whatsapp":
                await asyncio.Event().wait()
            else:
                await run_bot(settings, existing_queen=queen)
        finally:
            server.should_exit = True
            await gateway_task
            runtime = getattr(gateway_app.state, "whatsapp_runtime", None)
            if isinstance(runtime, WhatsAppRuntime):
                await runtime.stop()

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
                    env=env,
                    cwd=str(project_root),
                )
            else:
                # Simple nohup-like behavior
                subprocess.Popen(
                    args,
                    stdout=out_file,
                    stderr=err_file,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                    env=env,
                    cwd=str(project_root),
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
    grid.add_row("Active Channel", f"[bold]{status_data.get('active_channel', user_channel_label(settings.user_channel))}[/bold]")
    grid.add_row("Process ID", f"[bold]{pid}[/bold]" if pid else "[dim]N/A[/dim]")
    grid.add_row("Last Heartbeat", str(last_message) if last_message else "[dim]Never[/dim]")
    grid.add_row("Configuration", "[bright_green]Valid[/bright_green]" if config_ok else "[bright_red]Invalid[/bright_red]")

    metrics = read_metrics_snapshot(settings.state_dir)
    queen_metrics = metrics.get("queen", {}) if isinstance(metrics, dict) else {}
    telegram_metrics = metrics.get("telegram", {}) if isinstance(metrics, dict) else {}
    whatsapp_metrics = metrics.get("whatsapp", {}) if isinstance(metrics, dict) else {}
    whatsapp_metrics = metrics.get("whatsapp", {}) if isinstance(metrics, dict) else {}
    exec_metrics = metrics.get("exec_run", {}) if isinstance(metrics, dict) else {}
    selected_channel = normalize_user_channel(settings.user_channel)
    if metrics:
        grid.add_row("")
        grid.add_row("Queen Queues", f"[dim]followup=[/dim]{queen_metrics.get('followup_queues', 0)} [dim]internal=[/dim]{queen_metrics.get('internal_queues', 0)}")
        if selected_channel == "whatsapp":
            grid.add_row(
                "WhatsApp",
                f"[dim]mapped chats=[/dim]{whatsapp_metrics.get('chat_mappings', 0)} [dim]connected=[/dim]{whatsapp_metrics.get('connected', 0)}",
            )
        else:
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


@config_app.command("migrate")
def config_migrate() -> None:
    """Migrate current .env settings to structured config.json."""
    print_banner()
    settings = load_settings()
    if not settings.config_obj:
        console.print("[red]Error: Could not initialize configuration object.[/red]")
        return
    
    config_path = Path.cwd() / "config.json"
    if config_path.exists():
        if not Confirm.ask(f"[yellow]config.json already exists. Overwrite?[/yellow]"):
            return

    save_config(settings.config_obj)
    console.print(f"[green]Successfully migrated settings to {config_path}[/green]")
    console.print("[dim]You can now use config.json for advanced settings like worker overrides.[/dim]")


@config_app.command("show")
def config_show(reveal_secrets: bool = typer.Option(False, "--reveal-secrets", help="Show API keys and tokens")) -> None:
    """Show current configuration settings."""
    print_banner()
    settings = load_settings()
    accent = "bright_cyan"
    surface = "cyan"

    secret_keywords = ("token", "key", "secret", "api_key")
    groups: list[tuple[str, set[str]]] = [
        (
            "User Channel",
            {
                "BROODMIND_USER_CHANNEL",
                "TELEGRAM_BOT_TOKEN",
                "ALLOWED_TELEGRAM_CHAT_IDS",
                "BROODMIND_TELEGRAM_PARSE_MODE",
                "BROODMIND_WHATSAPP_MODE",
                "ALLOWED_WHATSAPP_NUMBERS",
                "BROODMIND_WHATSAPP_AUTH_DIR",
                "BROODMIND_WHATSAPP_BRIDGE_HOST",
                "BROODMIND_WHATSAPP_BRIDGE_PORT",
                "BROODMIND_WHATSAPP_CALLBACK_TOKEN",
                "BROODMIND_WHATSAPP_NODE_COMMAND",
            },
        ),
        (
            "Provider",
            {
                "BROODMIND_LLM_PROVIDER",
                "BROODMIND_LITELLM_PROVIDER_ID",
                "BROODMIND_LITELLM_MODEL",
                "BROODMIND_LITELLM_API_KEY",
                "BROODMIND_LITELLM_API_BASE",
                "BROODMIND_LITELLM_MODEL_PREFIX",
                "OPENROUTER_API_KEY",
                "OPENROUTER_BASE_URL",
                "OPENROUTER_MODEL",
                "OPENROUTER_TIMEOUT",
                "ZAI_API_KEY",
                "ZAI_BASE_URL",
                "ZAI_MODEL",
                "LITELLM_NUM_RETRIES",
                "LITELLM_TIMEOUT",
                "LITELLM_MAX_CONCURRENCY",
            },
        ),
        ("Tools", {"BRAVE_API_KEY", "FIRECRAWL_API_KEY", "OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_EMBED_MODEL"}),
        (
            "Runtime",
            {
                "BROODMIND_STATE_DIR",
                "BROODMIND_WORKSPACE_DIR",
                "BROODMIND_LOG_LEVEL",
                "BROODMIND_HEARTBEAT_INTERVAL_SECONDS",
                "BROODMIND_GATEWAY_HOST",
                "BROODMIND_GATEWAY_PORT",
                "BROODMIND_DASHBOARD_TOKEN",
                "BROODMIND_TAILSCALE_IPS",
                "BROODMIND_TAILSCALE_AUTO_SERVE",
            },
        ),
        (
            "Workers",
            {
                "BROODMIND_WORKER_LAUNCHER",
                "BROODMIND_WORKER_DOCKER_IMAGE",
                "BROODMIND_WORKER_DOCKER_WORKSPACE",
                "BROODMIND_WORKER_MAX_SPAWN_DEPTH",
                "BROODMIND_WORKER_MAX_CHILDREN_TOTAL",
                "BROODMIND_WORKER_MAX_CHILDREN_CONCURRENT",
            },
        ),
    ]

    alias_to_row: dict[str, tuple[str, object, bool]] = {}
    for field_name, field in settings.model_fields.items():
        alias = field.alias or field_name
        value = getattr(settings, field_name)
        is_secret = any(k in field_name.lower() or (field.alias and k in field.alias.lower()) for k in secret_keywords)
        alias_to_row[alias] = (field_name, value, is_secret)

    def _fmt_value(value: object, is_secret: bool) -> str:
        if is_secret and not reveal_secrets and value:
            return "[dim]********[/dim]"
        if value is None:
            return "[dim]None[/dim]"
        raw = str(value)
        return raw if raw.strip() else "[dim](empty)[/dim]"

    header = Table.grid(padding=(0, 2))
    header.add_column(style="bold white")
    header.add_column()
    resolved_profile = resolve_litellm_profile(settings)
    provider = str(getattr(settings, "llm_provider", "litellm"))
    selected_channel = normalize_user_channel(settings.user_channel)
    if selected_channel == "whatsapp":
        profile_status = (
            "[bright_green]READY[/bright_green]"
            if parse_allowed_whatsapp_numbers(settings.allowed_whatsapp_numbers)
            else "[bright_red]SETUP NEEDED[/bright_red]"
        )
    else:
        profile_status = "[bright_green]READY[/bright_green]" if settings.telegram_bot_token.strip() else "[bright_red]SETUP NEEDED[/bright_red]"
    header.add_row("Profile", profile_status)
    header.add_row("Provider", f"[bright_cyan]{resolved_profile.label}[/bright_cyan] [dim]({resolved_profile.provider_id})[/dim]")
    header.add_row("Model", f"[bright_cyan]{resolved_profile.model or '(unset)'}[/bright_cyan]")
    header.add_row("Config Source", f"[dim]{resolved_profile.source}[/dim] via [bright_cyan]{provider}[/bright_cyan]")
    header.add_row("Secrets", "Visible" if reveal_secrets else "Masked")

    panels: list[Panel] = [
        Panel(header, title="[bold white]Configuration Overview[/bold white]", border_style=surface, padding=(1, 2))
    ]

    covered: set[str] = set()
    for group_name, aliases in groups:
        table = Table(box=box.SIMPLE, show_header=True, header_style=f"bold {accent}", expand=False)
        table.add_column("Setting", style="white", width=38)
        table.add_column("Value", style="dim", width=46)
        rows = 0
        for alias in sorted(aliases):
            row = alias_to_row.get(alias)
            if not row:
                continue
            _, value, is_secret = row
            table.add_row(alias, _fmt_value(value, is_secret))
            covered.add(alias)
            rows += 1
        if rows:
            panels.append(Panel(table, title=f"[bold white]{group_name}[/bold white]", border_style=surface, padding=(0, 1)))

    extras = sorted(a for a in alias_to_row.keys() if a not in covered)
    if extras:
        table = Table(box=box.SIMPLE, show_header=True, header_style=f"bold {accent}", expand=False)
        table.add_column("Setting", style="white", width=38)
        table.add_column("Value", style="dim", width=46)
        for alias in extras:
            _, value, is_secret = alias_to_row[alias]
            table.add_row(alias, _fmt_value(value, is_secret))
        panels.append(Panel(table, title="[bold white]Additional[/bold white]", border_style=surface, padding=(0, 1)))

    checks: list[str] = []
    if selected_channel == "whatsapp":
        if not parse_allowed_whatsapp_numbers(settings.allowed_whatsapp_numbers):
            checks.append("[bright_red]Set ALLOWED_WHATSAPP_NUMBERS for WhatsApp access[/bright_red]")
    else:
        if not settings.telegram_bot_token.strip():
            checks.append("[bright_red]Missing TELEGRAM_BOT_TOKEN[/bright_red]")
        if not settings.allowed_telegram_chat_ids.strip():
            checks.append("[yellow]ALLOWED_TELEGRAM_CHAT_IDS is not set[/yellow]")
    if resolved_profile.requires_api_key and not (resolved_profile.api_key or "").strip():
        checks.append(
            f"[bright_red]Set BROODMIND_LITELLM_API_KEY (or legacy key) for {resolved_profile.label}[/bright_red]"
        )
    if not (resolved_profile.model or "").strip():
        checks.append("[bright_red]BROODMIND_LITELLM_MODEL is not set[/bright_red]")
    if not checks:
        checks.append("[bright_green]Core configuration checks passed[/bright_green]")
    checks_table = Table.grid(padding=(0, 1))
    checks_table.add_column()
    for line in checks:
        checks_table.add_row(f"- {line}")
    panels.append(Panel(checks_table, title="[bold white]Readiness Checks[/bold white]", border_style=surface, padding=(0, 1)))

    console.print()
    for panel in panels:
        console.print(Align.center(panel))
    console.print()


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
    _maybe_enable_tailscale_serve(settings)
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


@whatsapp_app.command("install-bridge")
def whatsapp_install_bridge() -> None:
    settings = load_settings()
    bridge = WhatsAppBridgeController(settings)
    try:
        bridge.install_bridge()
    except Exception as exc:
        console.print(f"[bold red]Failed to install WhatsApp bridge:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc
    console.print("[bold green][V] WhatsApp bridge dependencies installed.[/bold green]")


@whatsapp_app.command("link")
def whatsapp_link(timeout_seconds: int = typer.Option(180, "--timeout", help="How long to wait for linking")) -> None:
    settings = load_settings()
    bridge = WhatsAppBridgeController(settings)
    try:
        bridge.start(callback_url=None)
    except WhatsAppBridgeError as exc:
        console.print(f"[bold red]{exc}[/bold red]")
        raise typer.Exit(code=1) from exc

    console.print("[bold cyan]Waiting for WhatsApp QR link...[/bold cyan]")
    seen_qr = ""
    deadline = time.time() + max(30, timeout_seconds)
    try:
        while time.time() < deadline:
            status = bridge.status()
            if status.get("connected"):
                console.print("[bold green][V] WhatsApp linked and connected.[/bold green]")
                return
            qr_terminal = bridge.qr_terminal()
            qr_text = str(qr_terminal.get("terminal", "") or "").strip()
            qr_raw = str(qr_terminal.get("qr", "") or "").strip()
            if qr_raw and qr_raw != seen_qr:
                seen_qr = qr_raw
                console.print()
                if qr_text:
                    console.print(qr_text)
                else:
                    console.print(qr_raw)
                console.print()
                console.print("[dim]Scan the QR with WhatsApp on your phone.[/dim]")
            time.sleep(2)
    finally:
        bridge.stop()
    console.print("[bold red]Timed out waiting for WhatsApp link.[/bold red]")
    raise typer.Exit(code=1)


@whatsapp_app.command("status")
def whatsapp_status() -> None:
    settings = load_settings()
    bridge = WhatsAppBridgeController(settings)
    if not bridge.bridge_installed():
        console.print("[yellow]WhatsApp bridge dependencies are not installed.[/yellow]")
        return
    try:
        status = bridge.status()
    except Exception as exc:
        console.print(f"[bold red]Failed to read WhatsApp status:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan", justify="right")
    grid.add_column(style="white")
    grid.add_row("Connected", str(bool(status.get("connected"))))
    grid.add_row("Linked", str(bool(status.get("linked"))))
    grid.add_row("Auth Dir", str(status.get("authDir", "")))
    grid.add_row("Self", str(status.get("self", "") or "[dim]unknown[/dim]"))
    console.print(Panel(grid, title="[bold white]WhatsApp Status[/bold white]", border_style="bright_blue"))


@whatsapp_app.command("logout")
def whatsapp_logout() -> None:
    settings = load_settings()
    bridge = WhatsAppBridgeController(settings)
    try:
        bridge.start(callback_url=None)
        bridge.logout()
    except Exception as exc:
        console.print(f"[bold red]Failed to logout WhatsApp session:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc
    finally:
        bridge.stop()
    console.print("[bold green][V] WhatsApp session cleared.[/bold green]")


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
    """Copy default worker templates into the configured workspace workers directory."""
    settings = load_settings()
    result = sync_default_templates(settings.workspace_dir, overwrite=overwrite)
    console.print(
        "[green]Worker template sync complete[/green]: "
        f"copied={result['copied']} updated={result['updated']} skipped={result['skipped']}"
    )
    console.print(f"[dim]Target:[/dim] {settings.workspace_dir / 'workers'}")


@tools_app.command("resolve")
def tools_resolve(
    profile: str | None = typer.Option(None, "--profile", help="Apply a named tool profile such as research or coding."),
    preset: str = typer.Option("all", "--preset", help="Permission preset to simulate: all or queen."),
    blocked: bool = typer.Option(True, "--blocked/--available-only", help="Show blocked tools or only available ones."),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
) -> None:
    """Explain which tools are available and why others are blocked."""
    tool_specs = get_tools(mcp_manager=None)
    snapshot = _build_tool_resolution_snapshot(
        tool_specs,
        preset=preset,
        profile_name=profile,
        include_blocked=blocked,
    )

    if json_output:
        console.print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        return

    console.print()
    console.print(
        Align.center(
            Panel(
                _build_tool_resolution_summary_grid(snapshot),
                title="[bold white]Tool Resolution[/bold white]",
                border_style="bright_blue",
                expand=False,
                padding=(1, 3),
            )
        )
    )
    console.print()
    console.print(_build_tool_resolution_table(snapshot["available"], title="Available Tools"))
    if blocked and snapshot["blocked"]:
        console.print()
        console.print(_build_tool_resolution_table(snapshot["blocked"], title="Blocked Tools", include_reason=True))
    console.print()


@skill_app.command("install")
def skill_install(
    source: str = typer.Argument(..., help="ClawHub slug, SKILL.md URL, zip URL, or local bundle path."),
    clawhub_site: str = typer.Option("https://clawhub.ai", "--clawhub-site", help="Base ClawHub site URL."),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
) -> None:
    """Install a skill bundle from ClawHub, URL, or local path."""
    settings = load_settings()
    workspace_dir = settings.workspace_dir.resolve()
    try:
        payload = install_skill_from_source(
            source,
            workspace_dir=workspace_dir,
            clawhub_site=clawhub_site,
        )
    except Exception as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "message": str(exc), "source": source}, ensure_ascii=False))
        else:
            console.print(f"[bold red]Skill install failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from None

    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    console.print(f"[bold green][V] Installed skill[/bold green] {payload['skill_id']}")
    console.print(f"[dim]Source:[/dim] {payload['source']}")
    console.print(f"[dim]Path:[/dim] {payload['path']}")
    if payload.get("env_kind"):
        status_text = "prepared" if bool(payload.get("env_prepared", False)) else "not prepared"
        console.print(f"[dim]Runtime env:[/dim] {payload['env_kind']} ({status_text})")
    env_error = str(payload.get("env_error", "")).strip()
    if env_error:
        console.print(f"[yellow]Env prepare warning:[/yellow] {env_error}")
    if not bool(payload.get("trusted", True)):
        console.print("[yellow]Scripts from this imported skill are untrusted until you run `broodmind skill trust <id>`.[/yellow]")
    next_steps = [str(item).strip() for item in payload.get("next_steps", []) if str(item).strip()]
    for next_step in next_steps:
        console.print(f"[yellow]Next step:[/yellow] {next_step}")


@skill_app.command("list")
def skill_list(
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
) -> None:
    """List local and installer-managed skills."""
    settings = load_settings()
    payload = list_skill_inventory(settings.workspace_dir.resolve())

    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    skills = payload.get("skills", [])
    if not skills:
        console.print("[dim]No skills discovered yet.[/dim]")
        return

    table = Table(title="Skills", box=box.SIMPLE_HEAVY)
    table.add_column("Skill")
    table.add_column("Source")
    table.add_column("Origin")
    table.add_column("Trust")
    table.add_column("Runtime")
    table.add_column("Env")
    table.add_column("Scan")
    table.add_column("Path")
    for item in skills:
        if not isinstance(item, dict):
            continue
        origin = "installed" if bool(item.get("installer_managed", False)) else "local"
        source = str(item.get("installed_source", "")).strip() if bool(item.get("installer_managed", False)) else "local"
        runtime_kind = str(item.get("runtime_kind", "")).strip() or "-"
        if not bool(item.get("runtime_required", False)):
            env_status = "-"
        else:
            env_status = "prepared" if bool(item.get("runtime_prepared", False)) else "missing"
        trust_state = "trusted" if bool(item.get("trusted", True)) else "untrusted"
        table.add_row(
            str(item.get("id", "")),
            source,
            origin,
            trust_state,
            runtime_kind,
            env_status,
            str(item.get("scan_status", "")) or "-",
            str(item.get("path", "")),
        )
    console.print(table)


@skill_app.command("update")
def skill_update(
    skill_id: str = typer.Argument(..., help="Installer-managed skill id."),
    clawhub_site: str | None = typer.Option(None, "--clawhub-site", help="Override ClawHub site URL."),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
) -> None:
    """Update an installer-managed skill from its stored source."""
    settings = load_settings()
    try:
        payload = update_installed_skill(
            skill_id,
            workspace_dir=settings.workspace_dir.resolve(),
            clawhub_site=clawhub_site,
        )
    except Exception as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "message": str(exc), "skill_id": skill_id}, ensure_ascii=False))
        else:
            console.print(f"[bold red]Skill update failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from None

    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    console.print(f"[bold green][V] Updated skill[/bold green] {payload['skill_id']}")
    console.print(f"[dim]Source:[/dim] {payload['source']}")


@skill_app.command("trust")
def skill_trust(
    skill_id: str = typer.Argument(..., help="Skill id."),
    force: bool = typer.Option(False, "--force", help="Allow trusting a skill even when scan findings require manual review."),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
) -> None:
    """Mark a skill as trusted for script execution."""
    settings = load_settings()
    try:
        payload = set_skill_trust(
            skill_id,
            workspace_dir=settings.workspace_dir.resolve(),
            trusted=True,
            force=force,
        )
    except Exception as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "message": str(exc), "skill_id": skill_id}, ensure_ascii=False))
        else:
            console.print(f"[bold red]Skill trust failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from None

    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    console.print(f"[bold green][V] Trusted skill[/bold green] {payload['skill_id']}")


@skill_app.command("untrust")
def skill_untrust(
    skill_id: str = typer.Argument(..., help="Skill id."),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
) -> None:
    """Mark a skill as untrusted for script execution."""
    settings = load_settings()
    try:
        payload = set_skill_trust(
            skill_id,
            workspace_dir=settings.workspace_dir.resolve(),
            trusted=False,
        )
    except Exception as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "message": str(exc), "skill_id": skill_id}, ensure_ascii=False))
        else:
            console.print(f"[bold red]Skill untrust failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from None

    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    console.print(f"[bold green][V] Untrusted skill[/bold green] {payload['skill_id']}")


@skill_app.command("prepare-env")
def skill_prepare_env(
    skill_id: str = typer.Argument(..., help="Skill id."),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
) -> None:
    """Prepare an isolated runtime env for a skill."""
    settings = load_settings()
    try:
        payload = prepare_skill_env(
            skill_id,
            workspace_dir=settings.workspace_dir.resolve(),
        )
    except Exception as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "message": str(exc), "skill_id": skill_id}, ensure_ascii=False))
        else:
            console.print(f"[bold red]Skill env prepare failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from None

    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    console.print(f"[bold green][V] Prepared skill env[/bold green] {payload['skill_id']}")
    if payload.get("kind"):
        console.print(f"[dim]Runtime:[/dim] {payload['kind']}")
    if payload.get("env_dir"):
        console.print(f"[dim]Env dir:[/dim] {payload['env_dir']}")


@skill_app.command("remove-env")
def skill_remove_env(
    skill_id: str = typer.Argument(..., help="Skill id."),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
) -> None:
    """Remove an isolated runtime env for a skill."""
    settings = load_settings()
    try:
        payload = remove_skill_env(
            skill_id,
            workspace_dir=settings.workspace_dir.resolve(),
        )
    except Exception as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "message": str(exc), "skill_id": skill_id}, ensure_ascii=False))
        else:
            console.print(f"[bold red]Skill env remove failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from None

    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    console.print(f"[bold green][V] Removed skill env[/bold green] {payload['skill_id']}")


@skill_app.command("remove")
def skill_remove(
    skill_id: str = typer.Argument(..., help="Skill id."),
    json_output: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
) -> None:
    """Remove a local or installer-managed skill bundle."""
    settings = load_settings()
    try:
        payload = remove_skill(
            skill_id,
            workspace_dir=settings.workspace_dir.resolve(),
        )
    except Exception as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "message": str(exc), "skill_id": skill_id}, ensure_ascii=False))
        else:
            console.print(f"[bold red]Skill remove failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from None

    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    console.print(f"[bold green][V] Removed skill[/bold green] {payload['skill_id']}")


app.add_typer(workers_app, name="workers")
app.add_typer(audit_app, name="audit")
app.add_typer(memory_app, name="memory")
app.add_typer(config_app, name="config")
app.add_typer(whatsapp_app, name="whatsapp")
app.add_typer(tools_app, name="tools")
app.add_typer(skill_app, name="skill")


def _build_tool_resolution_snapshot(
    tool_specs: list[ToolSpec],
    *,
    preset: str,
    profile_name: str | None,
    include_blocked: bool,
) -> dict[str, object]:
    normalized_preset = str(preset or "all").strip().lower()
    if normalized_preset not in {"all", "queen"}:
        raise typer.BadParameter(f"Unsupported tools preset: {preset}")

    permissions = (
        _queen_tool_permissions()
        if normalized_preset == "queen"
        else _all_enabled_permissions(tool_specs)
    )
    policy_steps = _queen_tool_policy_steps() if normalized_preset == "queen" else []
    report = resolve_tool_diagnostics(
        tool_specs,
        permissions=permissions,
        profile_name=profile_name,
        policy_pipeline_steps=policy_steps,
    )
    available_rows = [_tool_row(entry.tool) for entry in report.entries if entry.available]
    blocked_rows = [
        _tool_row(entry.tool, reason=", ".join(entry.reasons))
        for entry in report.blocked_tools
    ] if include_blocked else []
    return {
        "preset": normalized_preset,
        "profile": profile_name or "",
        "available_count": len(available_rows),
        "blocked_count": len(report.blocked_tools),
        "permissions": permissions,
        "policy_steps": [step.label for step in policy_steps],
        "available": available_rows,
        "blocked": blocked_rows,
    }


def _all_enabled_permissions(tool_specs: list[ToolSpec]) -> dict[str, bool]:
    return {str(tool.permission): True for tool in tool_specs}


def _queen_tool_permissions() -> dict[str, bool]:
    return {
        "filesystem_read": True,
        "filesystem_write": True,
        "worker_manage": True,
        "llm_subtask": True,
        "canon_manage": True,
        "network": True,
        "exec": True,
        "service_read": True,
        "service_control": True,
        "deploy_control": True,
        "db_admin": True,
        "security_audit": True,
        "self_control": True,
        "mcp_exec": True,
        "skill_use": True,
        "skill_exec": True,
        "skill_manage": True,
    }


def _queen_tool_policy_steps() -> list[ToolPolicyPipelineStep]:
    return [
        ToolPolicyPipelineStep(
            label="queen.raw_fetch_denylist",
            policy=ToolPolicy(deny=["web_fetch", "markdown_new_fetch", "fetch_plan_tool"]),
        )
    ]


def _tool_row(tool: ToolSpec, *, reason: str = "") -> dict[str, str]:
    return {
        "name": tool.name,
        "permission": tool.permission,
        "category": tool.metadata.category or "-",
        "risk": tool.metadata.risk,
        "owner": tool.metadata.owner,
        "reason": reason,
    }


def _build_tool_resolution_summary_grid(snapshot: dict[str, object]) -> Table:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold cyan", justify="right")
    grid.add_column(style="white")
    grid.add_row("Preset", str(snapshot["preset"]))
    grid.add_row("Profile", str(snapshot["profile"] or "[dim]none[/dim]"))
    grid.add_row("Available", str(snapshot["available_count"]))
    grid.add_row("Blocked", str(snapshot["blocked_count"]))
    grid.add_row("Policy Steps", ", ".join(snapshot["policy_steps"]) or "[dim]none[/dim]")
    return grid


def _build_tool_resolution_table(
    rows: list[dict[str, str]],
    *,
    title: str,
    include_reason: bool = False,
) -> Table:
    table = Table(title=title, border_style="bright_blue", header_style="bold cyan", expand=False)
    table.add_column("Tool", style="white", width=28)
    table.add_column("Category", style="bright_green", width=14)
    table.add_column("Risk", width=12)
    table.add_column("Permission", style="dim", width=18)
    table.add_column("Owner", width=12)
    if include_reason:
        table.add_column("Reason", style="yellow", width=36)

    for row in rows:
        values = [
            row["name"],
            row["category"],
            row["risk"],
            row["permission"],
            row["owner"],
        ]
        if include_reason:
            values.append(row["reason"])
        table.add_row(*values)

    if not rows:
        empty_values = ["[dim]none[/dim]", "-", "-", "-", "-"]
        if include_reason:
            empty_values.append("-")
        table.add_row(*empty_values)
    return table


def _build_dashboard_snapshot(settings: Settings, last: int, store: SQLiteStore | None = None) -> dict:
    status_data = read_status(settings) or {}
    pid = status_data.get("pid")
    running = is_pid_running(pid)
    metrics = read_metrics_snapshot(settings.state_dir) or {}
    queen_metrics = metrics.get("queen", {}) if isinstance(metrics, dict) else {}
    telegram_metrics = metrics.get("telegram", {}) if isinstance(metrics, dict) else {}
    whatsapp_metrics = metrics.get("whatsapp", {}) if isinstance(metrics, dict) else {}
    exec_metrics = metrics.get("exec_run", {}) if isinstance(metrics, dict) else {}
    connectivity_metrics = metrics.get("connectivity", {}) if isinstance(metrics, dict) else {}

    if store is None:
        store = SQLiteStore(settings)
    
    # Use active workers for health metrics to avoid stale 'running' states
    active_workers = store.get_active_workers(older_than_minutes=5)
    recent_workers = (
        store.list_recent_workers(max(50, last))
        if hasattr(store, "list_recent_workers")
        else store.list_workers()[: max(50, last)]
    )
    
    now = _now_utc()
    cutoff = now.timestamp() - 24 * 60 * 60

    by_status: dict[str, int] = {}
    if hasattr(store, "count_workers_created_since"):
        spawned_24h = int(store.count_workers_created_since(datetime.fromtimestamp(cutoff, tz=UTC)))
    else:
        spawned_24h = sum(1 for worker in recent_workers if worker.created_at.timestamp() >= cutoff)
            
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
            for line in _read_last_lines(log_path, max_lines=12):
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
            "active_channel": status_data.get("active_channel", user_channel_label(settings.user_channel)),
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
            "whatsapp_mapped_chats": int(whatsapp_metrics.get("chat_mappings", 0) or 0),
            "whatsapp_connected": int(whatsapp_metrics.get("connected", 0) or 0),
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
                for w in recent_workers[:last]
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
            color = (
                "bright_green"
                if status == "connected"
                else "bright_red"
                if status == "error"
                else "bright_yellow"
                if status == "reconnecting"
                else "yellow"
            )
            details = str(s_data.get("reason", "") or "").strip()
            transport = str(s_data.get("transport", "auto"))
            attempts = int(s_data.get("reconnect_attempts", 0) or 0)
            suffix = f"[dim]({s_data.get('tool_count', 0)} tools, {transport})[/dim]"
            if attempts > 0 and status == "reconnecting":
                suffix += f" [dim]retry {attempts}[/dim]"
            if details:
                suffix += f" [dim]- {details}[/dim]"
            mcp_grid.add_row(f"{s_data.get('name', s_id)}:", f"[{color}]{str(status).upper()}[/{color}] {suffix}")
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
    out: list[dict] = []
    for line in _read_last_lines(path, max_lines=250):
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


def _read_last_lines(path: Path, max_lines: int = 200, max_bytes: int = 256 * 1024) -> list[str]:
    if not path.exists():
        return []
    if max_lines <= 0:
        return []
    try:
        size = path.stat().st_size
    except Exception:
        return []
    start = max(0, size - max(1, max_bytes))
    dq: deque[str] = deque(maxlen=max_lines)
    try:
        with path.open("rb") as fh:
            if start > 0:
                fh.seek(start)
                _ = fh.readline()  # drop partial line
            for raw in fh:
                try:
                    decoded = raw.decode("utf-8", errors="ignore").rstrip("\n\r")
                except Exception:
                    continue
                dq.append(decoded)
    except Exception:
        return []
    return list(dq)


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
