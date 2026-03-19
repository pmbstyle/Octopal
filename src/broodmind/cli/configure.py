from __future__ import annotations

import shutil
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt
from rich.rule import Rule
from rich.table import Table

from broodmind.channels import normalize_user_channel
from broodmind.cli.branding import print_banner
from broodmind.cli.wizard import (
    WizardConfirmParams,
    WizardMultiSelectParams,
    WizardSection,
    WizardSelectOption,
    WizardSelectParams,
    WizardTextParams,
    create_wizard_prompter,
)
from broodmind.infrastructure.config.models import BroodMindConfig, LLMConfig
from broodmind.infrastructure.config.settings import (
    _resolve_env_file,
    load_config,
    save_config,
)
from broodmind.infrastructure.providers.catalog import get_provider_catalog_entry

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
    prompter = create_wizard_prompter(console)

    prompter.intro(
        "BroodMind Configuration Studio",
        "Guided setup for your communication channels, LLM providers, and system behavior.",
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

    setup_mode = prompter.select(
        WizardSelectParams(
            message="Setup mode",
            initial_value="quick",
            options=[
                WizardSelectOption(
                    value="quick",
                    label="Quick Setup",
                    hint="Configure the essentials and keep the defaults moving.",
                ),
                WizardSelectOption(
                    value="advanced",
                    label="Advanced Setup",
                    hint="Tune transport, runtime, and provider details step by step.",
                ),
            ],
        )
    )
    advanced_mode = setup_mode == "advanced"

    sections = _build_sections(config, advanced_mode, prompter)
    for section in sections:
        if section.run is not None:
            section.run(config)

    while True:
        console.print(Rule(f"[bold {ACCENT}]Final Review[/bold {ACCENT}]"))
        _print_review(config)
        action = prompter.select(
            WizardSelectParams(
                message="What would you like to do?",
                initial_value="save",
                options=[
                    WizardSelectOption(
                        value="save",
                        label="Save configuration",
                        hint="Write the reviewed settings to config.json.",
                    ),
                    WizardSelectOption(
                        value="edit",
                        label="Edit a section",
                        hint="Jump back into one part of the wizard without starting over.",
                    ),
                    WizardSelectOption(
                        value="cancel",
                        label="Cancel",
                        hint="Exit without writing anything.",
                    ),
                ],
            )
        )

        if action == "save":
            save_config(config)
            console.print(f"[bold {SUCCESS}]Settings saved to config.json![/bold {SUCCESS}]")
            _print_next_steps(config)
            break

        if action == "cancel":
            console.print("[yellow]Configuration cancelled. No changes were written.[/yellow]")
            break

        _edit_section(config, sections, prompter)


def _configure_user_channel(config: BroodMindConfig, advanced: bool, prompter) -> None:
    console.print(Rule(f"[bold {ACCENT}]Channel Access[/bold {ACCENT}]"))

    channel = prompter.select(
        WizardSelectParams(
            message="Primary communication channel",
            initial_value=normalize_user_channel(config.user_channel),
            options=[
                WizardSelectOption(
                    value="telegram",
                    label="Telegram",
                    hint="Bot token + allowlist, best when you want a clean bot entrypoint.",
                ),
                WizardSelectOption(
                    value="whatsapp",
                    label="WhatsApp",
                    hint="Linked session via WhatsApp Web for a more personal chat flow.",
                ),
            ],
        )
    )
    config.user_channel = channel

    if channel == "whatsapp":
        prompter.note(
            "WhatsApp",
            [
                "WhatsApp uses a linked session through WhatsApp Web.",
                "Choose personal if BroodMind should live inside your own account.",
                "Choose separate if you plan to isolate it behind a dedicated session.",
            ],
        )
        config.whatsapp.mode = prompter.select(
            WizardSelectParams(
                message="WhatsApp mode",
                initial_value=config.whatsapp.mode,
                options=[
                    WizardSelectOption(
                        value="personal",
                        label="Personal",
                        hint="Best default when BroodMind is assisting you directly.",
                    ),
                    WizardSelectOption(
                        value="separate",
                        label="Separate",
                        hint="Use a dedicated linked session for cleaner boundaries.",
                    ),
                ],
            )
        )

        nums = ",".join(config.whatsapp.allowed_numbers)
        allowed = prompter.text(
            WizardTextParams(
                message="Allowed WhatsApp numbers (comma-separated)",
                initial_value=nums,
                placeholder="+15551234567,+15557654321",
            )
        )
        config.whatsapp.allowed_numbers = [n.strip() for n in allowed.split(",") if n.strip()]

        if advanced:
            config.whatsapp.bridge_host = prompter.text(
                WizardTextParams(
                    message="Bridge host",
                    initial_value=config.whatsapp.bridge_host,
                )
            )
            config.whatsapp.bridge_port = IntPrompt.ask("Bridge port", default=config.whatsapp.bridge_port)
    else:
        prompter.note(
            "Telegram",
            [
                "Create or manage your bot with @BotFather in Telegram.",
                "Paste the bot token here and list the chat IDs that are allowed to talk to BroodMind.",
            ],
        )
        config.telegram.bot_token = prompter.text(
            WizardTextParams(
                message="Telegram Bot Token",
                initial_value=config.telegram.bot_token,
                secret=bool(config.telegram.bot_token),
            )
        )

        ids = ",".join(config.telegram.allowed_chat_ids)
        allowed_ids = prompter.text(
            WizardTextParams(
                message="Allowed Telegram Chat IDs (comma-separated)",
                initial_value=ids,
                placeholder="123456789,987654321",
            )
        )
        config.telegram.allowed_chat_ids = [i.strip() for i in allowed_ids.split(",") if i.strip()]

        if advanced:
            config.telegram.parse_mode = prompter.select(
                WizardSelectParams(
                    message="Parse mode",
                    initial_value=config.telegram.parse_mode,
                    options=[
                        WizardSelectOption(value="MarkdownV2", label="MarkdownV2"),
                        WizardSelectOption(value="HTML", label="HTML"),
                        WizardSelectOption(value="Markdown", label="Markdown"),
                    ],
                )
            )


def _configure_llm(
    master_config: BroodMindConfig,
    label: str,
    config: LLMConfig,
    advanced: bool,
    prompter,
) -> None:
    console.print(Rule(f"[bold {ACCENT}]{label} LLM Settings[/bold {ACCENT}]"))

    provider_choices = _render_provider_select_list(prompter)
    current_id = config.provider_id or "zai"
    provider_id = prompter.select(
        WizardSelectParams(
            message=f"{label} provider",
            initial_value=current_id,
            options=provider_choices,
            searchable=True,
        )
    )
    entry = get_provider_catalog_entry(provider_id)
    config.provider_id = provider_id

    prompter.note(
        entry.label,
        [
            entry.description,
            f"Suggested default model: {entry.default_model}",
        ],
    )

    if entry.requires_api_key or prompter.confirm(
        WizardConfirmParams(
            message=f"Configure {entry.api_key_label}?",
            initial_value=bool(config.api_key),
        )
    ):
        config.api_key = prompter.text(
            WizardTextParams(
                message=entry.api_key_label,
                initial_value=config.api_key,
                secret=bool(config.api_key),
            )
        )

    config.model = prompter.text(
        WizardTextParams(
            message=f"{entry.model_label}",
            initial_value=config.model or entry.default_model,
            placeholder=entry.default_model,
        )
    )

    if entry.supports_custom_base_url:
        current_base = config.api_base or entry.default_api_base or ""
        if advanced:
            config.api_base = prompter.text(
                WizardTextParams(
                    message=entry.base_url_label,
                    initial_value=current_base,
                )
            )
        else:
            use_default_base = prompter.confirm(
                WizardConfirmParams(
                    message=f"Use recommended endpoint for {label}?",
                    initial_value=True,
                )
            )
            if use_default_base:
                config.api_base = current_base or None
            else:
                config.api_base = prompter.text(
                    WizardTextParams(
                        message=entry.base_url_label,
                        initial_value=current_base,
                    )
                )

    if advanced:
        if entry.supports_model_prefix_override:
            config.model_prefix = prompter.text(
                WizardTextParams(
                    message="Provider prefix (LiteLLM)",
                    initial_value=config.model_prefix or entry.model_prefix,
                )
            )

        # Runtime settings (global for now, but could be per-provider)
        if label == "Queen":
            master_config.litellm.timeout = IntPrompt.ask("Request timeout (sec)", default=int(master_config.litellm.timeout))
            master_config.litellm.num_retries = IntPrompt.ask("Max retries", default=master_config.litellm.num_retries)


def _configure_worker_settings(config: BroodMindConfig, advanced: bool, prompter) -> None:
    wants_separate = prompter.confirm(
        WizardConfirmParams(
            message="Configure separate LLM settings for Workers?",
            initial_value=bool(config.worker_llm_default.provider_id or config.worker_llm_overrides),
        )
    )
    if not wants_separate:
        config.worker_llm_default = LLMConfig()
        config.worker_llm_overrides = {}
        return

    _configure_llm(config, "Worker (Default)", config.worker_llm_default, advanced, prompter)

    if advanced and prompter.confirm(
        WizardConfirmParams(
            message="Add specific worker overrides? (e.g. for 'researcher')",
            initial_value=bool(config.worker_llm_overrides),
        )
    ):
        _configure_worker_overrides(config, prompter)


def _configure_worker_overrides(config: BroodMindConfig, prompter) -> None:
    while True:
        name = prompter.text(
            WizardTextParams(
                message="Worker template name to override (e.g. 'researcher', or empty to finish)",
                initial_value="",
            )
        )
        if not name:
            break

        if name not in config.worker_llm_overrides:
            config.worker_llm_overrides[name] = LLMConfig()

        _configure_llm(config, f"Override: {name}", config.worker_llm_overrides[name], False, prompter)

        if not Confirm.ask("Add another override?", default=True):
            break


def _configure_storage(config: BroodMindConfig, prompter) -> None:
    console.print(Rule(f"[bold {ACCENT}]Storage & Workspace[/bold {ACCENT}]"))

    prompter.note(
        "Storage",
        [
            "Workspace holds the Queen and worker scratch area.",
            "State directory is where BroodMind stores logs, DB files, and runtime state.",
        ],
    )
    config.storage.workspace_dir = Path(
        prompter.text(
            WizardTextParams(
                message="Workspace directory",
                initial_value=str(config.storage.workspace_dir),
            )
        )
    )
    config.storage.state_dir = Path(
        prompter.text(
            WizardTextParams(
                message="State directory (logs, DB)",
                initial_value=str(config.storage.state_dir),
            )
        )
    )

    # Bootstrap workspace if needed
    _ensure_workspace_bootstrap(config.storage.workspace_dir)


def _configure_features(config: BroodMindConfig, prompter) -> None:
    console.print(Rule(f"[bold {ACCENT}]Tools & Search[/bold {ACCENT}]"))

    enabled_tools = prompter.multiselect(
        WizardMultiSelectParams(
            message="Enable optional web tools",
            initial_values=[
                tool
                for tool, enabled in (
                    ("brave", bool(config.search.brave_api_key)),
                    ("firecrawl", bool(config.search.firecrawl_api_key)),
                )
                if enabled
            ],
            options=[
                WizardSelectOption(
                    value="brave",
                    label="Brave Search",
                    hint="Live web search results through Brave Search API.",
                ),
                WizardSelectOption(
                    value="firecrawl",
                    label="Firecrawl",
                    hint="Deeper web fetching and extraction.",
                ),
            ],
        )
    )
    enabled_set = set(enabled_tools)

    if "brave" in enabled_set:
        config.search.brave_api_key = prompter.text(
            WizardTextParams(
                message="Brave API Key",
                initial_value=config.search.brave_api_key,
                secret=True,
            )
        )
    else:
        config.search.brave_api_key = ""

    if "firecrawl" in enabled_set:
        config.search.firecrawl_api_key = prompter.text(
            WizardTextParams(
                message="Firecrawl API Key",
                initial_value=config.search.firecrawl_api_key,
                secret=True,
            )
        )
    else:
        config.search.firecrawl_api_key = ""


def _configure_runtime_advanced(config: BroodMindConfig, prompter) -> None:
    console.print(Rule(f"[bold {ACCENT}]Advanced Runtime[/bold {ACCENT}]"))

    config.log_level = prompter.select(
        WizardSelectParams(
            message="Log level",
            initial_value=config.log_level,
            options=[
                WizardSelectOption(value="DEBUG", label="DEBUG", hint="Most verbose."),
                WizardSelectOption(value="INFO", label="INFO", hint="Balanced default."),
                WizardSelectOption(value="WARNING", label="WARNING", hint="Only potential problems."),
                WizardSelectOption(value="ERROR", label="ERROR", hint="Only failures."),
            ],
        )
    )
    config.workers.launcher = prompter.select(
        WizardSelectParams(
            message="Worker launcher",
            initial_value=config.workers.launcher,
            options=[
                WizardSelectOption(value="same_env", label="same_env", hint="Run workers in the current Python environment."),
                WizardSelectOption(value="docker", label="docker", hint="Launch workers in Docker containers."),
            ],
        )
    )

    if config.workers.launcher == "docker":
        config.workers.docker_image = prompter.text(
            WizardTextParams(
                message="Docker image",
                initial_value=config.workers.docker_image,
            )
        )

    config.gateway.webapp_enabled = prompter.confirm(
        WizardConfirmParams(
            message="Enable Web Dashboard UI?",
            initial_value=config.gateway.webapp_enabled,
        )
    )
    if config.gateway.webapp_enabled:
        config.gateway.dashboard_token = prompter.text(
            WizardTextParams(
                message="Dashboard access token (optional)",
                initial_value=config.gateway.dashboard_token,
                secret=bool(config.gateway.dashboard_token),
            )
        )
    else:
        config.gateway.dashboard_token = ""


def _build_sections(config: BroodMindConfig, advanced: bool, prompter) -> list[WizardSection]:
    sections = [
        WizardSection(
            key="channel",
            title="Channel Access",
            render_status=lambda cfg: f"{cfg.user_channel}",
            run=lambda cfg: _configure_user_channel(cfg, advanced, prompter),
        ),
        WizardSection(
            key="queen-llm",
            title="Queen LLM",
            render_status=lambda cfg: f"{cfg.llm.provider_id or 'unset'} / {cfg.llm.model or 'unset'}",
            run=lambda cfg: _configure_llm(cfg, "Queen", cfg.llm, advanced, prompter),
        ),
        WizardSection(
            key="worker-llm",
            title="Worker LLM",
            render_status=lambda cfg: (
                "Using Queen defaults"
                if not cfg.worker_llm_default.provider_id
                else f"{cfg.worker_llm_default.provider_id} / {cfg.worker_llm_default.model or 'unset'}"
            ),
            run=lambda cfg: _configure_worker_settings(cfg, advanced, prompter),
        ),
        WizardSection(
            key="storage",
            title="Storage & Workspace",
            render_status=lambda cfg: str(cfg.storage.workspace_dir),
            run=lambda cfg: _configure_storage(cfg, prompter),
        ),
        WizardSection(
            key="features",
            title="Tools & Search",
            render_status=lambda cfg: _features_status(cfg),
            run=lambda cfg: _configure_features(cfg, prompter),
        ),
    ]
    if advanced:
        sections.append(
            WizardSection(
                key="runtime",
                title="Advanced Runtime",
                render_status=lambda cfg: f"{cfg.log_level} / {cfg.workers.launcher}",
                run=lambda cfg: _configure_runtime_advanced(cfg, prompter),
            )
        )
    return sections


def _features_status(config: BroodMindConfig) -> str:
    enabled = [
        label
        for label, is_enabled in (
            ("Brave", bool(config.search.brave_api_key)),
            ("Firecrawl", bool(config.search.firecrawl_api_key)),
        )
        if is_enabled
    ]
    return ", ".join(enabled) if enabled else "No optional tools enabled"


def _edit_section(config: BroodMindConfig, sections: list[WizardSection], prompter) -> None:
    options = [
        WizardSelectOption(
            value=section.key,
            label=section.title,
            hint=section.render_status(config) if section.render_status is not None else None,
        )
        for section in sections
    ]
    selected_key = prompter.select(
        WizardSelectParams(
            message="Choose a section to edit",
            options=options,
            searchable=True,
        )
    )
    for section in sections:
        if section.key == selected_key and section.run is not None:
            section.run(config)
            return


def _render_provider_select_list(prompter) -> list[WizardSelectOption[str]]:
    provider_choices: list[WizardSelectOption[str]] = []
    help_lines = [
        "Type part of a provider name, then use the arrow keys to pick one.",
        "The description is shown after you select it, so this step stays compact.",
    ]

    for category, provider_ids in _PROVIDER_GROUPS.items():
        for pid in provider_ids:
            entry = get_provider_catalog_entry(pid)
            provider_choices.append(
                WizardSelectOption(
                    value=pid,
                    label=entry.label,
                    hint=f"{category} · {pid}",
                )
            )

    prompter.note("Provider Search", help_lines)
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
