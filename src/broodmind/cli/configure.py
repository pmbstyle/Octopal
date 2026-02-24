from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from broodmind.cli.branding import print_banner
from broodmind.config.manager import ConfigManager

console = Console()
ACCENT = "bright_cyan"
SURFACE = "cyan"
SUCCESS = "green"


def configure_wizard() -> None:
    """Run the interactive configuration wizard."""
    print_banner()

    console.print(
        Panel(
            Text("BroodMind Configuration Studio\n", style="bold bright_cyan")
            + Text(
                "Production-ready setup for access control, model routing, runtime paths, and service security.",
                style="dim",
            ),
            title="[bold white]Onboarding[/bold white]",
            subtitle="[dim]Estimated time: 3-5 minutes[/dim]",
            border_style=SURFACE,
            padding=(1, 2),
        )
    )

    config = ConfigManager()
    changes: list[tuple[str, str]] = []

    console.print(Rule(f"[bold {ACCENT}]Step 1/7  Telegram Access[/bold {ACCENT}]"))
    console.print("Get your bot token from @BotFather.")
    current_token = config.get("TELEGRAM_BOT_TOKEN", "")
    token = Prompt.ask(
        "Telegram Bot Token",
        default=current_token,
        password=bool(current_token),
    )
    if token:
        _set_if_changed(config, changes, "TELEGRAM_BOT_TOKEN", token)

    current_parse_mode = config.get("BROODMIND_TELEGRAM_PARSE_MODE", "MarkdownV2")
    parse_mode = Prompt.ask(
        "Telegram parse mode",
        choices=["MarkdownV2", "HTML", "Markdown", "none"],
        default=current_parse_mode if current_parse_mode else "MarkdownV2",
    )
    parse_mode_value = "" if parse_mode == "none" else parse_mode
    _set_if_changed(config, changes, "BROODMIND_TELEGRAM_PARSE_MODE", parse_mode_value)

    console.print(Rule(f"[bold {ACCENT}]Step 2/7  Access Control[/bold {ACCENT}]"))
    console.print("Define who can interact with the Queen.")
    console.print("[dim]Tip: message @userinfobot on Telegram to get your chat ID.[/dim]")
    current_ids = config.get("ALLOWED_TELEGRAM_CHAT_IDS", "")
    allowed_ids = Prompt.ask(
        "Allowed Chat IDs (comma-separated)",
        default=current_ids,
    )
    if allowed_ids:
        _set_if_changed(config, changes, "ALLOWED_TELEGRAM_CHAT_IDS", allowed_ids)

    console.print(Rule(f"[bold {ACCENT}]Step 3/7  LLM Provider[/bold {ACCENT}]"))
    current_provider = config.get("BROODMIND_LLM_PROVIDER", "litellm")
    provider = Prompt.ask(
        "LLM provider",
        choices=["litellm", "openrouter"],
        default=current_provider,
    )
    _set_if_changed(config, changes, "BROODMIND_LLM_PROVIDER", provider)

    if provider == "openrouter":
        console.print("[bold]OpenRouter profile[/bold]")
        current_or_key = config.get("OPENROUTER_API_KEY", "")
        or_key = Prompt.ask(
            "OpenRouter API Key",
            default=current_or_key,
            password=bool(current_or_key),
        )
        if or_key:
            _set_if_changed(config, changes, "OPENROUTER_API_KEY", or_key)

        current_or_model = config.get("OPENROUTER_MODEL", "anthropic/claude-sonnet-4")
        or_model = Prompt.ask("Default OpenRouter model", default=current_or_model)
        _set_if_changed(config, changes, "OPENROUTER_MODEL", or_model)

        current_or_base = config.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
        or_base = Prompt.ask("OpenRouter base URL", default=current_or_base)
        _set_if_changed(config, changes, "OPENROUTER_BASE_URL", or_base)

        current_or_timeout = str(config.get("OPENROUTER_TIMEOUT", "120"))
        or_timeout = Prompt.ask("OpenRouter timeout (seconds)", default=current_or_timeout)
        _set_if_changed(config, changes, "OPENROUTER_TIMEOUT", or_timeout)
    else:
        console.print("[bold]LiteLLM / z.ai profile[/bold]")
        current_zai_key = config.get("ZAI_API_KEY", "")
        zai_key = Prompt.ask(
            "Z.ai (or OpenAI-compatible) API Key",
            default=current_zai_key,
            password=bool(current_zai_key),
        )
        if zai_key:
            _set_if_changed(config, changes, "ZAI_API_KEY", zai_key)

        current_zai_base = config.get("ZAI_BASE_URL", "https://api.z.ai/api/coding/paas/v4")
        zai_base = Prompt.ask("Z.ai base URL", default=current_zai_base)
        _set_if_changed(config, changes, "ZAI_BASE_URL", zai_base)

        current_zai_model = config.get("ZAI_MODEL", "glm-5")
        zai_model = Prompt.ask("Default model name", default=current_zai_model)
        _set_if_changed(config, changes, "ZAI_MODEL", zai_model)

    console.print(Rule(f"[bold {ACCENT}]Step 4/7  Storage[/bold {ACCENT}]"))
    current_workspace = config.get("BROODMIND_WORKSPACE_DIR", "workspace")
    workspace = Prompt.ask("Workspace directory", default=current_workspace)
    _set_if_changed(config, changes, "BROODMIND_WORKSPACE_DIR", workspace)
    workspace_result = _ensure_workspace_bootstrap(Path(workspace))

    current_state = config.get("BROODMIND_STATE_DIR", "data")
    state_dir = Prompt.ask("State directory (DB, logs)", default=current_state)
    _set_if_changed(config, changes, "BROODMIND_STATE_DIR", state_dir)

    console.print(Rule(f"[bold {ACCENT}]Step 5/7  Optional Tools[/bold {ACCENT}]"))

    if Confirm.ask("Enable Web Search? (requires Brave API key)", default=False):
        current_brave = config.get("BRAVE_API_KEY", "")
        brave_key = Prompt.ask("Brave API Key", default=current_brave, password=bool(current_brave))
        if brave_key:
            _set_if_changed(config, changes, "BRAVE_API_KEY", brave_key)

    if Confirm.ask("Enable Firecrawl-backed web fetch?", default=False):
        current_firecrawl = config.get("FIRECRAWL_API_KEY", "")
        firecrawl_key = Prompt.ask("Firecrawl API Key", default=current_firecrawl, password=bool(current_firecrawl))
        if firecrawl_key:
            _set_if_changed(config, changes, "FIRECRAWL_API_KEY", firecrawl_key)

    if Confirm.ask("Enable Semantic Memory? (requires OpenAI API key)", default=False):
        current_openai = config.get("OPENAI_API_KEY", "")
        openai_key = Prompt.ask("OpenAI API Key", default=current_openai, password=bool(current_openai))
        if openai_key:
            _set_if_changed(config, changes, "OPENAI_API_KEY", openai_key)

            current_base_url = config.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
            openai_base_url = Prompt.ask("OpenAI Base URL", default=current_base_url)
            _set_if_changed(config, changes, "OPENAI_BASE_URL", openai_base_url)

            current_embed_model = config.get("OPENAI_EMBED_MODEL", "text-embedding-3-small")
            openai_embed_model = Prompt.ask("OpenAI Embedding Model", default=current_embed_model)
            _set_if_changed(config, changes, "OPENAI_EMBED_MODEL", openai_embed_model)

    console.print(Rule(f"[bold {ACCENT}]Step 6/7  Gateway and Security[/bold {ACCENT}]"))
    auto_serve_default = _env_bool(config.get("BROODMIND_TAILSCALE_AUTO_SERVE", "1"), default=True)
    auto_serve = Confirm.ask("Enable automatic `tailscale serve` at startup?", default=auto_serve_default)
    _set_if_changed(config, changes, "BROODMIND_TAILSCALE_AUTO_SERVE", "1" if auto_serve else "0")

    current_tailscale_ips = config.get("BROODMIND_TAILSCALE_IPS", "")
    tailscale_ips = Prompt.ask(
        "Optional trusted Tailscale IPs (comma-separated, leave blank to auto-discover)",
        default=current_tailscale_ips,
    )
    _set_if_changed(config, changes, "BROODMIND_TAILSCALE_IPS", tailscale_ips)

    protect_dashboard = Confirm.ask("Require token authentication for dashboard API?", default=True)
    if protect_dashboard:
        current_dashboard_token = config.get("BROODMIND_DASHBOARD_TOKEN", "")
        dashboard_token = Prompt.ask(
            "Dashboard token",
            default=current_dashboard_token,
            password=bool(current_dashboard_token),
        )
        if dashboard_token:
            _set_if_changed(config, changes, "BROODMIND_DASHBOARD_TOKEN", dashboard_token)

    console.print(Rule(f"[bold {ACCENT}]Step 7/7  Runtime Defaults[/bold {ACCENT}]"))
    current_log_level = config.get("BROODMIND_LOG_LEVEL", "INFO")
    log_level = Prompt.ask(
        "Log level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=current_log_level,
    )
    _set_if_changed(config, changes, "BROODMIND_LOG_LEVEL", log_level)

    current_heartbeat = int(config.get("BROODMIND_HEARTBEAT_INTERVAL_SECONDS", "900") or 900)
    heartbeat = IntPrompt.ask("Heartbeat interval (seconds)", default=current_heartbeat)
    _set_if_changed(config, changes, "BROODMIND_HEARTBEAT_INTERVAL_SECONDS", str(max(60, heartbeat)))

    current_launcher = config.get("BROODMIND_WORKER_LAUNCHER", "same_env")
    launcher = Prompt.ask("Worker launcher", choices=["same_env", "docker"], default=current_launcher)
    _set_if_changed(config, changes, "BROODMIND_WORKER_LAUNCHER", launcher)

    if launcher == "docker":
        current_image = config.get("BROODMIND_WORKER_DOCKER_IMAGE", "broodmind-worker:latest")
        image = Prompt.ask("Worker Docker image", default=current_image)
        _set_if_changed(config, changes, "BROODMIND_WORKER_DOCKER_IMAGE", image)

        current_docker_workspace = config.get("BROODMIND_WORKER_DOCKER_WORKSPACE", "/workspace")
        docker_workspace = Prompt.ask("Worker Docker workspace path", default=current_docker_workspace)
        _set_if_changed(config, changes, "BROODMIND_WORKER_DOCKER_WORKSPACE", docker_workspace)

    console.print()
    if workspace_result["created_files"]:
        created_lines = "\n".join(f"- {path}" for path in workspace_result["created_files"])
    else:
        created_lines = "- none (all files already existed)"

    console.print(
        Panel(
            "[bold cyan]Workspace bootstrap complete[/bold cyan]\n"
            f"Created files:\n{created_lines}\n\n"
            f"Skipped existing files: {workspace_result['skipped_files']}",
            border_style=SURFACE,
        )
    )

    summary = Table(box=box.SIMPLE, show_header=True, header_style=f"bold {ACCENT}", expand=False)
    summary.add_column("Setting", style="white", width=40)
    summary.add_column("New Value", style="dim", width=40)
    if changes:
        for key, value in changes:
            summary.add_row(key, value)
    else:
        summary.add_row("[dim]No changes[/dim]", "[dim]Existing values kept[/dim]")

    console.print(
        Panel(
            summary,
            title="[bold white]Configuration Summary[/bold white]",
            border_style=SURFACE,
            padding=(1, 2),
        )
    )
    console.print(
        Panel(
            f"[bold {SUCCESS}][V] Configuration complete[/bold {SUCCESS}]\n"
            f"Saved to: [cyan]{config.env_path.absolute()}[/cyan]\n\n"
            "[bold]Next:[/bold]\n"
            "[magenta]uv run broodmind start[/magenta]\n"
            "[magenta]uv run broodmind status[/magenta]\n"
            "[magenta]uv run broodmind config show[/magenta]",
            border_style=SUCCESS,
            padding=(1, 2),
        )
    )


def _mask_value(key: str, value: Any) -> str:
    text = str(value)
    lowered = key.lower()
    if any(token in lowered for token in ("token", "key", "secret")) and text:
        if len(text) <= 8:
            return "********"
        return f"{text[:4]}...{text[-4:]}"
    return text


def _set_if_changed(config: ConfigManager, changes: list[tuple[str, str]], key: str, value: Any) -> None:
    new_value = str(value if value is not None else "")
    current_value = str(config.get(key, "") or "")
    if new_value == current_value:
        return
    config.set(key, new_value)
    changes.append((key, _mask_value(key, new_value)))


def _env_bool(raw: Any, default: bool = False) -> bool:
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _ensure_workspace_bootstrap(workspace_dir: Path) -> dict[str, int | list[str]]:
    workspace_dir.mkdir(parents=True, exist_ok=True)
    template_root = Path(__file__).resolve().parents[3] / "workspace_templates"
    if not template_root.exists():
        raise FileNotFoundError(f"workspace template folder not found: {template_root}")

    created_files: list[str] = []
    skipped_files = 0

    for source in sorted(template_root.rglob("*")):
        rel = source.relative_to(template_root)
        target = workspace_dir / rel
        if source.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            skipped_files += 1
            continue
        shutil.copy2(source, target)
        created_files.append(rel.as_posix())

    return {
        "created_files": created_files,
        "skipped_files": skipped_files,
    }
