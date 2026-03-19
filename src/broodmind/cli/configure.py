from __future__ import annotations

import shutil
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from broodmind.channels import normalize_user_channel
from broodmind.cli.branding import print_banner
from broodmind.infrastructure.config.models import BroodMindConfig, LLMConfig
from broodmind.infrastructure.config.settings import (
    _resolve_env_file,
    load_config,
    save_config,
)
from broodmind.infrastructure.providers.catalog import (
    get_provider_catalog_entry,
)

console = Console()
ACCENT = "bright_cyan"
SURFACE = "cyan"
SUCCESS = "green"

_PROVIDER_GROUPS: dict[str, tuple[str, ...]] = {
    "Routers and Gateways": ("openrouter", "minimax", "custom"),
    "Hosted APIs": ("zai", "openai", "anthropic", "google", "mistral", "together", "groq"),
    "Local": ("ollama",),
}


def configure_wizard() -> None:
    """Run the modern interactive configuration wizard."""
    print_banner()

    console.print(
        Panel(
            Text("BroodMind Configuration Studio\n", style="bold bright_cyan")
            + Text(
                "Guided setup for your communication channels, LLM providers, and system behavior.",
                style="dim",
            ),
            title="[bold white]Setup Wizard[/bold white]",
            subtitle="[dim]Configuration will be saved to config.json[/dim]",
            border_style=SURFACE,
            padding=(1, 2),
        )
    )

    config = load_config()

    # Check if migration is needed
    env_file = _resolve_env_file()
    if (
        env_file
        and env_file.exists()
        and not Path("config.json").exists()
        and Confirm.ask("[yellow]Found legacy .env but no config.json. Migrate now?[/yellow]", default=True)
    ):
        save_config(config)
        console.print("[green]Migration complete. Continuing with wizard...[/green]")

    setup_mode = Prompt.ask(
        "Setup mode",
        choices=["quick", "advanced"],
        default="quick",
    )
    advanced_mode = setup_mode == "advanced"

    # 1. User Channel
    _configure_user_channel(config, advanced_mode)

    # 2. Queen LLM
    _configure_llm(config, "Queen", config.llm, advanced_mode)

    # 3. Worker LLM
    if Confirm.ask("Configure separate LLM settings for Workers?", default=False):
        _configure_llm(config, "Worker (Default)", config.worker_llm_default, advanced_mode)

        if advanced_mode and Confirm.ask("Add specific worker overrides? (e.g. for 'researcher')", default=False):
            _configure_worker_overrides(config)

    # 4. Storage & Workspace
    _configure_storage(config)

    # 5. Features & Tools
    _configure_features(config)

    # 6. Advanced Runtime
    if advanced_mode:
        _configure_runtime_advanced(config)

    # Review and Save
    console.print(Rule(f"[bold {ACCENT}]Final Review[/bold {ACCENT}]"))
    _print_review(config)

    if Confirm.ask("Save configuration?", default=True):
        save_config(config)
        console.print(f"[bold {SUCCESS}]Settings saved to config.json![/bold {SUCCESS}]")
        _print_next_steps(config)
    else:
        console.print("[yellow]Configuration cancelled. No changes were written.[/yellow]")


def _configure_user_channel(config: BroodMindConfig, advanced: bool) -> None:
    console.print(Rule(f"[bold {ACCENT}]Channel Access[/bold {ACCENT}]"))

    channel = Prompt.ask(
        "Primary communication channel",
        choices=["telegram", "whatsapp"],
        default=normalize_user_channel(config.user_channel),
    )
    config.user_channel = channel

    if channel == "whatsapp":
        console.print("[dim]WhatsApp uses a linked session (WhatsApp Web).[/dim]")
        config.whatsapp.mode = Prompt.ask(
            "WhatsApp mode",
            choices=["personal", "separate"],
            default=config.whatsapp.mode,
        )

        nums = ",".join(config.whatsapp.allowed_numbers)
        allowed = Prompt.ask("Allowed WhatsApp numbers (comma-separated)", default=nums)
        config.whatsapp.allowed_numbers = [n.strip() for n in allowed.split(",") if n.strip()]

        if advanced:
            config.whatsapp.bridge_host = Prompt.ask("Bridge host", default=config.whatsapp.bridge_host)
            config.whatsapp.bridge_port = IntPrompt.ask("Bridge port", default=config.whatsapp.bridge_port)
    else:
        console.print("[dim]Get your bot token from @BotFather on Telegram.[/dim]")
        config.telegram.bot_token = Prompt.ask(
            "Telegram Bot Token",
            default=config.telegram.bot_token,
            password=bool(config.telegram.bot_token)
        )

        ids = ",".join(config.telegram.allowed_chat_ids)
        allowed_ids = Prompt.ask("Allowed Telegram Chat IDs (comma-separated)", default=ids)
        config.telegram.allowed_chat_ids = [i.strip() for i in allowed_ids.split(",") if i.strip()]

        if advanced:
            config.telegram.parse_mode = Prompt.ask(
                "Parse mode",
                choices=["MarkdownV2", "HTML", "Markdown"],
                default=config.telegram.parse_mode
            )


def _configure_llm(master_config: BroodMindConfig, label: str, config: LLMConfig, advanced: bool) -> None:
    console.print(Rule(f"[bold {ACCENT}]{label} LLM Settings[/bold {ACCENT}]"))

    provider_choices = _render_provider_select_list()
    current_id = config.provider_id or "zai"

    default_choice = 1
    for i, pid in enumerate(provider_choices, start=1):
        if pid == current_id:
            default_choice = i
            break

    selected_idx = IntPrompt.ask(
        f"Choose provider for {label}",
        choices=[str(i) for i in range(1, len(provider_choices) + 1)],
        default=default_choice,
    )

    provider_id = provider_choices[selected_idx - 1]
    entry = get_provider_catalog_entry(provider_id)
    config.provider_id = provider_id

    console.print(Panel(f"[bold]{entry.label}[/bold]\n{entry.description}", border_style=SURFACE))

    if entry.requires_api_key or Confirm.ask(f"Configure {entry.api_key_label}?", default=bool(config.api_key)):
        config.api_key = Prompt.ask(entry.api_key_label, default=config.api_key, password=bool(config.api_key))

    config.model = Prompt.ask(f"{entry.model_label} (default: {entry.default_model})", default=config.model or entry.default_model)

    if entry.supports_custom_base_url and (advanced or provider_id in {"custom", "ollama"}):
        config.api_base = Prompt.ask(entry.base_url_label, default=config.api_base or entry.default_api_base)

    if advanced:
        if entry.supports_model_prefix_override:
            config.model_prefix = Prompt.ask("Provider prefix (LiteLLM)", default=config.model_prefix or entry.model_prefix)

        # Runtime settings (global for now, but could be per-provider)
        if label == "Queen":
            master_config.litellm.timeout = IntPrompt.ask("Request timeout (sec)", default=int(master_config.litellm.timeout))
            master_config.litellm.num_retries = IntPrompt.ask("Max retries", default=master_config.litellm.num_retries)


def _configure_worker_overrides(config: BroodMindConfig) -> None:
    while True:
        name = Prompt.ask("Worker template name to override (e.g. 'researcher', or empty to finish)")
        if not name:
            break

        if name not in config.worker_llm_overrides:
            config.worker_llm_overrides[name] = LLMConfig()

        _configure_llm(config, f"Override: {name}", config.worker_llm_overrides[name], False)

        if not Confirm.ask("Add another override?", default=True):
            break


def _configure_storage(config: BroodMindConfig) -> None:
    console.print(Rule(f"[bold {ACCENT}]Storage & Workspace[/bold {ACCENT}]"))

    config.storage.workspace_dir = Path(Prompt.ask("Workspace directory", default=str(config.storage.workspace_dir)))
    config.storage.state_dir = Path(Prompt.ask("State directory (logs, DB)", default=str(config.storage.state_dir)))

    # Bootstrap workspace if needed
    _ensure_workspace_bootstrap(config.storage.workspace_dir)


def _configure_features(config: BroodMindConfig) -> None:
    console.print(Rule(f"[bold {ACCENT}]Tools & Search[/bold {ACCENT}]"))

    if Confirm.ask("Enable Brave Search?", default=bool(config.search.brave_api_key)):
        config.search.brave_api_key = Prompt.ask("Brave API Key", default=config.search.brave_api_key, password=True)

    if Confirm.ask("Enable Firecrawl (web fetching)?", default=bool(config.search.firecrawl_api_key)):
        config.search.firecrawl_api_key = Prompt.ask("Firecrawl API Key", default=config.search.firecrawl_api_key, password=True)


def _configure_runtime_advanced(config: BroodMindConfig) -> None:
    console.print(Rule(f"[bold {ACCENT}]Advanced Runtime[/bold {ACCENT}]"))

    config.log_level = Prompt.ask("Log level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default=config.log_level)
    config.workers.launcher = Prompt.ask("Worker launcher", choices=["same_env", "docker"], default=config.workers.launcher)

    if config.workers.launcher == "docker":
        config.workers.docker_image = Prompt.ask("Docker image", default=config.workers.docker_image)

    config.gateway.webapp_enabled = Confirm.ask("Enable Web Dashboard UI?", default=config.gateway.webapp_enabled)
    if config.gateway.webapp_enabled:
        config.gateway.dashboard_token = Prompt.ask("Dashboard access token (optional)", default=config.gateway.dashboard_token)


def _render_provider_select_list() -> list[str]:
    provider_choices: list[str] = []
    lines: list[str] = []

    for category, provider_ids in _PROVIDER_GROUPS.items():
        lines.append(f"[bold yellow]{category}[/bold yellow]")
        for pid in provider_ids:
            entry = get_provider_catalog_entry(pid)
            provider_choices.append(pid)
            lines.append(f"  {len(provider_choices)}. {entry.label} [dim]({pid})[/dim]")
        lines.append("")

    console.print(Panel("\n".join(lines).strip(), title="Available Providers", border_style=SURFACE, padding=(1, 2)))
    return provider_choices


def _print_review(config: BroodMindConfig) -> None:
    table = Table(box=None, show_header=False)
    table.add_column(style="bold cyan")
    table.add_column()

    # Summarize key points
    llm_info = f"{config.llm.provider_id} / {config.llm.model}"
    table.add_row("Queen LLM", llm_info)

    if config.worker_llm_default.provider_id:
        table.add_row("Worker LLM", f"{config.worker_llm_default.provider_id} / {config.worker_llm_default.model}")
    else:
        table.add_row("Worker LLM", "[dim]Using Queen defaults[/dim]")

    if config.worker_llm_overrides:
        table.add_row("Overrides", f"{len(config.worker_llm_overrides)} templates")

    table.add_row("Workspace", str(config.storage.workspace_dir))
    table.add_row("Log Level", config.log_level)

    console.print(Panel(table, title="Configuration Summary", border_style=SUCCESS, padding=(1, 2)))


def _print_next_steps(config: BroodMindConfig) -> None:
    console.print("\n[bold]Suggested next steps:[/bold]")
    console.print("  [magenta]broodmind start[/magenta] - Launch the Queen")
    console.print("  [magenta]broodmind status[/magenta] - Check connectivity")


def _ensure_workspace_bootstrap(workspace_dir: Path) -> None:
    workspace_dir.mkdir(parents=True, exist_ok=True)
    template_root = Path(__file__).resolve().parents[3] / "workspace_templates"
    if not template_root.exists():
        return

    for source in sorted(template_root.rglob("*")):
        rel = source.relative_to(template_root)
        target = workspace_dir / rel
        if source.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        if not target.exists():
            shutil.copy2(source, target)
