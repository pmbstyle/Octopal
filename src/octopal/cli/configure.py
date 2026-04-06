from __future__ import annotations

import shutil
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.rule import Rule
from rich.table import Table

from octopal.channels import normalize_user_channel
from octopal.cli.branding import print_banner
from octopal.cli.wizard import (
    WizardConfirmParams,
    WizardMultiSelectParams,
    WizardPrompter,
    WizardSection,
    WizardSelectOption,
    WizardSelectParams,
    WizardTextParams,
    create_wizard_prompter,
)
from octopal.infrastructure.config.models import LLMConfig, OctopalConfig
from octopal.infrastructure.config.settings import (
    _resolve_env_file,
    load_config,
    save_config,
)
from octopal.infrastructure.providers.catalog import get_provider_catalog_entry
from octopal.runtime.workers.launcher_factory import detect_docker_cli

console = Console()
ACCENT = "#6aafae"
SURFACE = "#5fa8c8"
SUCCESS = "#7dd36b"

_PROVIDER_GROUPS: dict[str, tuple[str, ...]] = {
    "Routers and Gateways": ("openrouter", "minimax", "custom"),
    "Hosted APIs": ("zai", "openai", "anthropic", "google", "mistral", "together", "groq"),
    "Local": ("ollama",),
}


def _print_section_header(title: str) -> None:
    console.print()
    console.print(Rule(f"[bold {ACCENT}]{title}[/bold {ACCENT}]"))
    console.print()


class _LegacyWizardPrompter(WizardPrompter):
    def intro(self, title: str, body: str | None = None) -> None:
        rendered = title if not body else f"{title}\n{body}"
        console.print(Panel(rendered, border_style=SURFACE, padding=(1, 2)))
        console.print()

    def note(self, title: str, lines: list[str]) -> None:
        console.print()
        console.print(
            Panel(
                "\n".join(lines),
                title=f"[bold]{title}[/bold]",
                border_style=SURFACE,
                padding=(1, 2),
            )
        )
        console.print()

    def select(self, params: WizardSelectParams):
        visible_options = [option for option in params.options if option.enabled]
        for index, option in enumerate(visible_options, start=1):
            hint = f" [dim]- {option.hint}[/dim]" if option.hint else ""
            console.print(f"  {index}. {option.label}{hint}")

        default_index = 1
        if params.initial_value is not None:
            for index, option in enumerate(visible_options, start=1):
                if option.value == params.initial_value:
                    default_index = index
                    break

        selected_idx = IntPrompt.ask(
            params.message,
            choices=[str(i) for i in range(1, len(visible_options) + 1)],
            default=default_index,
        )
        return visible_options[selected_idx - 1].value

    def multiselect(self, params: WizardMultiSelectParams):
        visible_options = [option for option in params.options if option.enabled]
        initial_values = set(params.initial_values)
        console.print(f"[bold]{params.message}[/bold]")
        default_indices: list[str] = []
        for index, option in enumerate(visible_options, start=1):
            selected = option.value in initial_values
            marker = "[green]x[/green]" if selected else " "
            hint = f" [dim]- {option.hint}[/dim]" if option.hint else ""
            console.print(f"  [{marker}] {index}. {option.label}{hint}")
            if selected:
                default_indices.append(str(index))

        raw = Prompt.ask("Selections", default=",".join(default_indices))
        if not raw.strip():
            return []

        selected = []
        for chunk in raw.split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            selected.append(visible_options[int(chunk) - 1].value)
        return selected

    def text(self, params: WizardTextParams) -> str:
        return Prompt.ask(
            params.message,
            default=params.initial_value,
            password=params.secret,
        )

    def confirm(self, params: WizardConfirmParams) -> bool:
        return Confirm.ask(params.message, default=params.initial_value)


def _resolve_prompter(prompter: WizardPrompter | None) -> WizardPrompter:
    return prompter if prompter is not None else _LegacyWizardPrompter()


def configure_wizard() -> None:
    """Run the modern interactive configuration wizard."""
    print_banner()
    prompter = create_wizard_prompter(console)

    prompter.intro(
        "Octopal Configuration Studio",
        "Guided setup for your communication channels, LLM providers, and system behavior.",
    )

    config = load_config()
    original_config = config.model_copy(deep=True)

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

    sections = _build_sections(config, prompter)
    for section in sections:
        if section.run is not None:
            section.run(config)

    while True:
        _print_section_header("Final Review")
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
            _print_next_steps(config, original_config)
            break

        if action == "cancel":
            console.print("[yellow]Configuration cancelled. No changes were written.[/yellow]")
            break

        _edit_section(config, sections, prompter)


def _configure_user_channel(config: OctopalConfig, prompter) -> None:
    _print_section_header("Channel Access")

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
                "Choose personal if Octopal should live inside your own account.",
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
                        hint="Best default when Octopal is assisting you directly.",
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
                "Paste the bot token here and list the chat IDs that are allowed to talk to Octopal.",
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
    master_config: OctopalConfig,
    label: str,
    config: LLMConfig,
    prompter: WizardPrompter | None = None,
) -> None:
    prompter = _resolve_prompter(prompter)
    _print_section_header(f"{label} LLM Settings")

    provider_choices = _render_provider_select_list(prompter)
    previous_provider_id = config.provider_id or "zai"
    current_id = previous_provider_id
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
        provider_changed = provider_id != previous_provider_id
        current_base = entry.default_api_base or ""
        if not provider_changed:
            current_base = config.api_base or entry.default_api_base or ""
        config.api_base = prompter.text(
            WizardTextParams(
                message=entry.base_url_label,
                initial_value=current_base,
            )
        )

    if entry.supports_model_prefix_override:
        config.model_prefix = prompter.text(
            WizardTextParams(
                message="Provider prefix (LiteLLM)",
                initial_value=config.model_prefix or entry.model_prefix,
            )
        )

    # Runtime settings (global for now, but could be per-provider)
    if label == "Octo":
        master_config.litellm.timeout = IntPrompt.ask("Request timeout (sec)", default=int(master_config.litellm.timeout))
        master_config.litellm.num_retries = IntPrompt.ask("Max retries", default=master_config.litellm.num_retries)


def _configure_worker_settings(config: OctopalConfig, prompter) -> None:
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

    _configure_llm(config, "Worker (Default)", config.worker_llm_default, prompter)

    if prompter.confirm(
        WizardConfirmParams(
            message="Add specific worker overrides? (e.g. for 'researcher')",
            initial_value=bool(config.worker_llm_overrides),
        )
    ):
        _configure_worker_overrides(config, prompter)


def _configure_worker_overrides(config: OctopalConfig, prompter) -> None:
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

        _configure_llm(config, f"Override: {name}", config.worker_llm_overrides[name], prompter)

        if not Confirm.ask("Add another override?", default=True):
            break


def _configure_storage(config: OctopalConfig, prompter) -> None:
    _print_section_header("Storage & Workspace")

    prompter.note(
        "Storage",
        [
            "Workspace holds the Octo and worker scratch area.",
            "State directory is where Octopal stores logs, DB files, and runtime state.",
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


def _configure_features(config: OctopalConfig, prompter) -> None:
    _print_section_header("Tools & Search")

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


def _configure_dashboard(config: OctopalConfig, prompter) -> None:
    _print_section_header("Dashboard")

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


def _configure_runtime_advanced(config: OctopalConfig, prompter) -> None:
    _print_section_header("Advanced Runtime")
    docker_ok, docker_detail = detect_docker_cli()

    if docker_ok:
        prompter.note(
            "Worker Launcher",
            [
                "Docker workers are the recommended default for isolation.",
                f"Docker detected: {docker_detail}",
            ],
        )
    else:
        prompter.note(
            "Worker Launcher",
            [
                "Docker workers are the recommended default, but Docker CLI was not detected on this machine.",
                "If you keep the docker launcher selected, Octopal will fall back to same_env until Docker is installed.",
            ],
        )

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
                WizardSelectOption(value="docker", label="docker", hint="Launch workers in Docker containers."),
                WizardSelectOption(value="same_env", label="same_env", hint="Run workers in the current Python environment."),
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


def _configure_connectors(config: OctopalConfig, prompter) -> None:
    _print_section_header("Connectors")

    prompter.note(
        "Connectors (experimental)",
        [
            "Connectors allow Octo to link with external services through explicit CLI setup.",
            "Google connector support covers Gmail, Calendar, and Drive.",
            "GitHub connector support covers repositories, issues, and pull requests.",
            "After saving, Octopal will tell you which CLI command to run next for authorization.",
        ],
    )

    available_connectors = [
        WizardSelectOption(
            value="google",
            label="Google",
            hint="Integrate with Gmail and Google Calendar. More Google services can land on the same connector flow later.",
        ),
        WizardSelectOption(
            value="github",
            label="GitHub",
            hint="Inspect repositories, issues, and pull requests with a personal access token.",
        ),
    ]

    initial_values = [
        name for name, instance in config.connectors.instances.items()
        if instance.enabled
    ]

    selected = prompter.multiselect(
        WizardMultiSelectParams(
            message="Select connectors to enable",
            initial_values=initial_values,
            options=available_connectors,
        )
    )

    selected_set = set(selected)
    from octopal.infrastructure.config.models import ConnectorInstanceConfig

    # Update enabled status for all available connectors
    for option in available_connectors:
        name = option.value
        is_enabled = name in selected_set

        if name not in config.connectors.instances:
            config.connectors.instances[name] = ConnectorInstanceConfig(enabled=is_enabled)
        else:
            config.connectors.instances[name].enabled = is_enabled

        # Granular settings for Google
        if name == "google" and is_enabled:
            google_services = [
                WizardSelectOption(value="gmail", label="Gmail"),
                WizardSelectOption(value="calendar", label="Calendar"),
                WizardSelectOption(value="drive", label="Drive"),
            ]

            current_google_services = config.connectors.instances[name].enabled_services or ["gmail"]
            current_google_services = [
                service for service in current_google_services if service in {"gmail", "calendar", "drive"}
            ] or ["gmail"]

            selected_google = prompter.multiselect(
                WizardMultiSelectParams(
                    message="Select specific Google services to enable",
                    initial_values=current_google_services,
                    options=google_services,
                )
            )
            config.connectors.instances[name].enabled_services = selected_google
        if name == "github" and is_enabled:
            github_services = [
                WizardSelectOption(value="repos", label="Repositories"),
                WizardSelectOption(value="issues", label="Issues"),
                WizardSelectOption(value="pull_requests", label="Pull Requests"),
            ]

            current_github_services = config.connectors.instances[name].enabled_services or ["repos"]
            current_github_services = [
                service for service in current_github_services if service in {"repos", "issues", "pull_requests"}
            ] or ["repos"]

            selected_github = prompter.multiselect(
                WizardMultiSelectParams(
                    message="Select specific GitHub services to enable",
                    initial_values=current_github_services,
                    options=github_services,
                )
            )
            config.connectors.instances[name].enabled_services = selected_github


def _build_sections(config: OctopalConfig, prompter) -> list[WizardSection]:
    sections = [
        WizardSection(
            key="channel",
            title="Channel Access",
            render_status=lambda cfg: f"{cfg.user_channel}",
            run=lambda cfg: _configure_user_channel(cfg, prompter),
        ),
        WizardSection(
            key="octo-llm",
            title="Octo LLM",
            render_status=lambda cfg: f"{cfg.llm.provider_id or 'unset'} / {cfg.llm.model or 'unset'}",
            run=lambda cfg: _configure_llm(cfg, "Octo", cfg.llm, prompter),
        ),
        WizardSection(
            key="worker-llm",
            title="Worker LLM",
            render_status=lambda cfg: (
                "Using Octo defaults"
                if not cfg.worker_llm_default.provider_id
                else f"{cfg.worker_llm_default.provider_id} / {cfg.worker_llm_default.model or 'unset'}"
            ),
            run=lambda cfg: _configure_worker_settings(cfg, prompter),
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
        WizardSection(
            key="connectors",
            title="Connectors",
            render_status=lambda cfg: _connectors_status(cfg),
            run=lambda cfg: _configure_connectors(cfg, prompter),
        ),
        WizardSection(
            key="dashboard",
            title="Dashboard",
            render_status=lambda cfg: _dashboard_status(cfg),
            run=lambda cfg: _configure_dashboard(cfg, prompter),
        ),
        WizardSection(
            key="runtime",
            title="Advanced Runtime",
            render_status=lambda cfg: f"{cfg.log_level} / {cfg.workers.launcher}",
            run=lambda cfg: _configure_runtime_advanced(cfg, prompter),
        ),
    ]
    return sections


def _features_status(config: OctopalConfig) -> str:
    enabled = [
        label
        for label, is_enabled in (
            ("Brave", bool(config.search.brave_api_key)),
            ("Firecrawl", bool(config.search.firecrawl_api_key)),
        )
        if is_enabled
    ]
    return ", ".join(enabled) if enabled else "No optional tools enabled"


def _connectors_status(config: OctopalConfig) -> str:
    enabled = [
        name.capitalize()
        for name, instance in config.connectors.instances.items()
        if instance.enabled
    ]
    return ", ".join(enabled) if enabled else "No connectors enabled"


def _dashboard_status(config: OctopalConfig) -> str:
    if not config.gateway.webapp_enabled:
        return "Disabled"
    if config.gateway.dashboard_token:
        return "Enabled with token"
    return "Enabled without token"


def _edit_section(config: OctopalConfig, sections: list[WizardSection], prompter) -> None:
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


def _print_review(config: OctopalConfig) -> None:
    table = Table(box=None, show_header=False)
    table.add_column(style="bold cyan")
    table.add_column()

    # Summarize key points
    llm_info = f"{config.llm.provider_id} / {config.llm.model}"
    table.add_row("Octo LLM", llm_info)

    if config.worker_llm_default.provider_id:
        table.add_row("Worker LLM", f"{config.worker_llm_default.provider_id} / {config.worker_llm_default.model}")
    else:
        table.add_row("Worker LLM", "[dim]Using Octo defaults[/dim]")

    if config.worker_llm_overrides:
        table.add_row("Overrides", f"{len(config.worker_llm_overrides)} templates")

    table.add_row("Workspace", str(config.storage.workspace_dir))
    table.add_row("Log Level", config.log_level)

    console.print(Panel(table, title="Configuration Summary", border_style=SUCCESS, padding=(1, 2)))


def _enabled_services(config: OctopalConfig, connector_name: str) -> list[str]:
    instance = config.connectors.instances.get(connector_name)
    if not instance or not instance.enabled:
        return []
    return [str(service).strip().lower() for service in instance.enabled_services if str(service).strip()]


def _authorized_services(config: OctopalConfig, connector_name: str) -> list[str]:
    instance = config.connectors.instances.get(connector_name)
    if not instance:
        return []
    return [
        str(service).strip().lower()
        for service in instance.auth.authorized_services
        if str(service).strip()
    ]


def _collect_connector_next_steps(config: OctopalConfig, previous_config: OctopalConfig | None = None) -> list[str]:
    lines: list[str] = []
    previous_config = previous_config or OctopalConfig()

    google = config.connectors.instances.get("google")
    if google and google.enabled:
        current_services = set(_enabled_services(config, "google"))
        previous_services = set(_enabled_services(previous_config, "google"))
        authorized_services = set(_authorized_services(config, "google"))
        has_refresh_token = bool(google.auth.refresh_token)
        previous_google = previous_config.connectors.instances.get("google")

        needs_auth = not has_refresh_token or not current_services.issubset(authorized_services)
        services_added = sorted(current_services - previous_services)
        newly_enabled = previous_google is None or not previous_google.enabled

        if needs_auth or services_added or newly_enabled:
            lines.append(
                "  [magenta]octopal connector auth google[/magenta] - Authorize Google for the enabled services."
            )
            lines.append(
                "  [magenta]octopal connector status[/magenta] - Verify connector status after authorization."
            )
            lines.append(
                "  [magenta]octopal restart[/magenta] - Reload Octopal after connector authorization."
            )

    github = config.connectors.instances.get("github")
    if github and github.enabled:
        current_services = set(_enabled_services(config, "github"))
        previous_services = set(_enabled_services(previous_config, "github"))
        authorized_services = set(_authorized_services(config, "github"))
        has_token = bool(github.auth.access_token)
        previous_github = previous_config.connectors.instances.get("github")

        needs_auth = not has_token or not current_services.issubset(authorized_services)
        services_added = sorted(current_services - previous_services)
        newly_enabled = previous_github is None or not previous_github.enabled

        if needs_auth or services_added or newly_enabled:
            lines.append(
                "  [magenta]octopal connector auth github[/magenta] - Authorize GitHub for the enabled services."
            )
            lines.append(
                "  [magenta]octopal connector status[/magenta] - Verify connector status after authorization."
            )
            lines.append(
                "  [magenta]octopal restart[/magenta] - Reload Octopal after connector authorization."
            )

    return lines


def _print_next_steps(config: OctopalConfig, previous_config: OctopalConfig | None = None) -> None:
    console.print("\n[bold]Suggested next steps:[/bold]")
    console.print("  [magenta]octopal start[/magenta] - Launch the Octo")
    console.print("  [magenta]octopal status[/magenta] - Check connectivity")
    for line in _collect_connector_next_steps(config, previous_config):
        console.print(line)


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
