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

from broodmind.channels import normalize_user_channel, user_channel_label
from broodmind.cli.branding import print_banner
from broodmind.config.manager import ConfigManager
from broodmind.providers.catalog import get_provider_catalog_entry, list_registered_provider_ids
from broodmind.channels.whatsapp.ids import normalize_whatsapp_number, parse_allowed_whatsapp_numbers

console = Console()
ACCENT = "bright_cyan"
SURFACE = "cyan"
SUCCESS = "green"

_PROVIDER_GROUPS: dict[str, tuple[str, ...]] = {
    "Routers and Gateways": ("openrouter", "custom"),
    "Hosted APIs": ("zai", "openai", "anthropic", "google", "mistral", "together", "groq"),
    "Local": ("ollama",),
}

StagedConfig = dict[str, str | None]

def configure_wizard() -> None:
    """Run the interactive configuration wizard."""
    print_banner()

    console.print(
        Panel(
            Text("BroodMind Configuration Studio\n", style="bold bright_cyan")
            + Text(
                "Guided setup for your user channel, provider profiles, storage paths, and runtime defaults.",
                style="dim",
            ),
            title="[bold white]Onboarding[/bold white]",
            subtitle="[dim]Quick setup for first run, advanced setup for finer control[/dim]",
            border_style=SURFACE,
            padding=(1, 2),
        )
    )

    config = ConfigManager()
    staged: StagedConfig = {}

    setup_mode = Prompt.ask(
        "Setup mode",
        choices=["quick", "advanced"],
        default="quick",
    )
    advanced_mode = setup_mode == "advanced"
    total_steps = 6 if advanced_mode else 5

    access_summary = _configure_user_channel_access(
        config,
        staged,
        step_index=1,
        total_steps=total_steps,
        advanced_mode=advanced_mode,
    )
    provider_summary = _configure_provider_profile(
        config,
        staged,
        step_index=2,
        total_steps=total_steps,
        advanced_mode=advanced_mode,
    )
    workspace_result, storage_summary = _configure_storage(
        config,
        staged,
        step_index=3,
        total_steps=total_steps,
    )
    features_summary = _configure_optional_tools(
        config,
        staged,
        step_index=4,
        total_steps=total_steps,
    )

    gateway_summary: dict[str, str] = {}
    runtime_summary: dict[str, str] = {}
    if advanced_mode:
        gateway_summary = _configure_gateway_security(
            config,
            staged,
            step_index=5,
            total_steps=total_steps,
        )
        runtime_summary = _configure_runtime_defaults(
            config,
            staged,
            step_index=6,
            total_steps=total_steps,
        )

    step_index = total_steps
    console.print(Rule(f"[bold {ACCENT}]Step {step_index}/{total_steps}  Review and Save[/bold {ACCENT}]"))
    _print_workspace_bootstrap(workspace_result)
    _print_review(
        config=config,
        staged=staged,
        access_summary=access_summary,
        provider_summary=provider_summary,
        storage_summary=storage_summary,
        features_summary=features_summary,
        gateway_summary=gateway_summary,
        runtime_summary=runtime_summary,
        advanced_mode=advanced_mode,
    )

    if not Confirm.ask("Save these changes to .env?", default=True):
        console.print("[yellow]Configuration cancelled. No changes were written.[/yellow]")
        return

    _apply_staged_changes(config, staged)
    saved_channel = normalize_user_channel(_effective_value(config, staged, "BROODMIND_USER_CHANNEL", "telegram"))
    _print_saved_summary(config, staged, user_channel=saved_channel)


def _configure_user_channel_access(
    config: ConfigManager,
    staged: StagedConfig,
    *,
    step_index: int,
    total_steps: int,
    advanced_mode: bool,
) -> dict[str, str]:
    console.print(Rule(f"[bold {ACCENT}]Step {step_index}/{total_steps}  User Channel[/bold {ACCENT}]"))
    current_channel = normalize_user_channel(_effective_value(config, staged, "BROODMIND_USER_CHANNEL", "telegram"))
    channel = Prompt.ask(
        "User communication channel",
        choices=["telegram", "whatsapp"],
        default=current_channel,
    )
    _set_if_changed(config, staged, "BROODMIND_USER_CHANNEL", channel)

    if channel == "whatsapp":
        current_mode = _effective_value(config, staged, "BROODMIND_WHATSAPP_MODE", "separate").strip().lower() or "separate"
        if current_mode not in {"personal", "separate"}:
            current_mode = "separate"
        console.print("Use WhatsApp Web with a linked session. You can use your own number or a separate BroodMind number.")
        whatsapp_mode = Prompt.ask(
            "WhatsApp setup mode",
            choices=["personal", "separate"],
            default=current_mode,
        )
        _set_if_changed(config, staged, "BROODMIND_WHATSAPP_MODE", whatsapp_mode)
        current_numbers = _effective_value(config, staged, "ALLOWED_WHATSAPP_NUMBERS", "")
        allowed_numbers = current_numbers
        if whatsapp_mode == "personal":
            existing_personal = parse_allowed_whatsapp_numbers(current_numbers)
            default_personal = existing_personal[0] if existing_personal else ""
            console.print("Personal mode lets you message BroodMind from the same WhatsApp account you linked.")
            personal_number = Prompt.ask(
                "Your personal WhatsApp number (the phone you will message from, e.g. +15551234567)",
                default=default_personal,
            )
            normalized_personal = normalize_whatsapp_number(personal_number)
            allowed_numbers = normalized_personal or personal_number.strip()
        else:
            allowed_numbers = Prompt.ask(
                "Allowed WhatsApp numbers (comma-separated, e.g. +15551234567)",
                default=current_numbers,
            )
        if allowed_numbers:
            _set_if_changed(config, staged, "ALLOWED_WHATSAPP_NUMBERS", allowed_numbers)
        current_auth_dir = _effective_value(config, staged, "BROODMIND_WHATSAPP_AUTH_DIR", "data/whatsapp-auth")
        auth_dir = Prompt.ask("WhatsApp auth/session directory", default=current_auth_dir)
        _set_if_changed(config, staged, "BROODMIND_WHATSAPP_AUTH_DIR", auth_dir)
        if advanced_mode:
            current_bridge_host = _effective_value(config, staged, "BROODMIND_WHATSAPP_BRIDGE_HOST", "127.0.0.1")
            bridge_host = Prompt.ask("WhatsApp bridge host", default=current_bridge_host)
            _set_if_changed(config, staged, "BROODMIND_WHATSAPP_BRIDGE_HOST", bridge_host)
            current_bridge_port = _effective_value(config, staged, "BROODMIND_WHATSAPP_BRIDGE_PORT", "8765")
            bridge_port = Prompt.ask("WhatsApp bridge port", default=current_bridge_port)
            _set_if_changed(config, staged, "BROODMIND_WHATSAPP_BRIDGE_PORT", bridge_port)
        return {
            "channel": user_channel_label(channel),
            "mode": whatsapp_mode.capitalize(),
            "recipients": allowed_numbers or "[configure later]",
            "session_dir": auth_dir,
        }

    console.print("Get your bot token from @BotFather and lock BroodMind to your Telegram chat IDs.")

    current_token = _effective_value(config, staged, "TELEGRAM_BOT_TOKEN", "")
    token = Prompt.ask("Telegram Bot Token", default=current_token, password=bool(current_token))
    if token:
        _set_if_changed(config, staged, "TELEGRAM_BOT_TOKEN", token)

    current_ids = _effective_value(config, staged, "ALLOWED_TELEGRAM_CHAT_IDS", "")
    allowed_ids = Prompt.ask("Allowed Chat IDs (comma-separated)", default=current_ids)
    if allowed_ids:
        _set_if_changed(config, staged, "ALLOWED_TELEGRAM_CHAT_IDS", allowed_ids)

    if advanced_mode:
        current_parse_mode = _effective_value(config, staged, "BROODMIND_TELEGRAM_PARSE_MODE", "MarkdownV2")
        parse_mode = Prompt.ask(
            "Telegram parse mode",
            choices=["MarkdownV2", "HTML", "Markdown", "none"],
            default=current_parse_mode if current_parse_mode else "MarkdownV2",
        )
        _set_if_changed(config, staged, "BROODMIND_TELEGRAM_PARSE_MODE", "" if parse_mode == "none" else parse_mode)
    return {
        "channel": "Telegram",
        "recipients": allowed_ids or "[configure later]",
        "session_dir": "[managed by Telegram]",
    }


def _configure_provider_profile(
    config: ConfigManager,
    staged: StagedConfig,
    *,
    step_index: int,
    total_steps: int,
    advanced_mode: bool,
) -> dict[str, str]:
    console.print(Rule(f"[bold {ACCENT}]Step {step_index}/{total_steps}  Model Provider[/bold {ACCENT}]"))
    console.print("Choose the upstream model provider BroodMind should use through LiteLLM.")

    provider_choices = _render_provider_select_list()
    current_provider_id = _resolve_configured_provider_id(config, staged)
    default_choice = 1
    for index, provider_choice in enumerate(provider_choices, start=1):
        if provider_choice == current_provider_id:
            default_choice = index
            break
    selected_index = IntPrompt.ask(
        "Choose provider",
        choices=[str(index) for index in range(1, len(provider_choices) + 1)],
        default=default_choice,
    )
    provider_id = provider_choices[selected_index - 1]
    provider_entry = get_provider_catalog_entry(provider_id)
    _set_if_changed(config, staged, "BROODMIND_LLM_PROVIDER", "litellm")
    _set_if_changed(config, staged, "BROODMIND_LITELLM_PROVIDER_ID", provider_id)

    console.print(
        Panel(
            f"[bold]{provider_entry.label}[/bold]\n"
            f"{provider_entry.description}\n\n"
            f"[dim]Key:[/dim] {provider_entry.api_key_label}\n"
            f"[dim]Model:[/dim] {provider_entry.model_label}\n"
            f"[dim]Endpoint:[/dim] "
            f"{provider_entry.default_api_base or 'Provider-managed default'}",
            border_style=SURFACE,
            padding=(1, 2),
        )
    )

    current_api_key = _default_profile_value(config, staged, provider_id, "api_key")
    if provider_entry.requires_api_key:
        api_key = Prompt.ask(
            provider_entry.api_key_label,
            default=current_api_key,
            password=bool(current_api_key),
        )
        _set_if_changed(config, staged, "BROODMIND_LITELLM_API_KEY", api_key)
    else:
        configure_optional_key = Confirm.ask(
            f"Configure {provider_entry.api_key_label.lower()}?",
            default=bool(current_api_key),
        )
        api_key = current_api_key
        if configure_optional_key:
            api_key = Prompt.ask(
                provider_entry.api_key_label,
                default=current_api_key,
                password=bool(current_api_key),
            )
        elif not current_api_key:
            api_key = ""
        _set_if_changed(config, staged, "BROODMIND_LITELLM_API_KEY", api_key)

    current_model = _default_profile_value(config, staged, provider_id, "model")
    selected_model = _prompt_for_model(provider_id, current_model)
    _set_if_changed(config, staged, "BROODMIND_LITELLM_MODEL", selected_model)

    current_api_base = _default_profile_value(config, staged, provider_id, "api_base")
    if provider_entry.supports_custom_base_url:
        ask_base_url = advanced_mode or provider_id in {"custom", "ollama"}
        use_recommended_endpoint = True
        if not ask_base_url:
            recommended_endpoint = current_api_base or provider_entry.default_api_base or "provider-managed default"
            use_recommended_endpoint = Confirm.ask(
                f"Use the recommended endpoint URL ({recommended_endpoint})?",
                default=True,
            )
        if ask_base_url or not use_recommended_endpoint:
            api_base = Prompt.ask(provider_entry.base_url_label, default=current_api_base)
            _set_if_changed(config, staged, "BROODMIND_LITELLM_API_BASE", api_base)
        else:
            _set_if_changed(config, staged, "BROODMIND_LITELLM_API_BASE", current_api_base)

    _stage_provider_model_prefix(
        config,
        staged,
        provider_id=provider_id,
        supports_model_prefix_override=provider_entry.supports_model_prefix_override,
        advanced_mode=advanced_mode,
    )

    if advanced_mode:
        current_timeout = _effective_value(config, staged, "LITELLM_TIMEOUT", "120")
        litellm_timeout = Prompt.ask("LiteLLM timeout (seconds)", default=current_timeout)
        _set_if_changed(config, staged, "LITELLM_TIMEOUT", litellm_timeout)

        current_retries = _effective_value(config, staged, "LITELLM_NUM_RETRIES", "3")
        retries = Prompt.ask("LiteLLM retries", default=current_retries)
        _set_if_changed(config, staged, "LITELLM_NUM_RETRIES", retries)

        current_concurrency = _effective_value(config, staged, "LITELLM_MAX_CONCURRENCY", "2")
        concurrency = Prompt.ask("LiteLLM max concurrency", default=current_concurrency)
        _set_if_changed(config, staged, "LITELLM_MAX_CONCURRENCY", concurrency)

    return {
        "provider": provider_entry.label,
        "provider_id": provider_id,
        "model": selected_model,
        "api_key_set": "yes" if bool(api_key.strip()) else "no",
        "api_base": _effective_value(config, staged, "BROODMIND_LITELLM_API_BASE", current_api_base),
    }


def _configure_storage(
    config: ConfigManager,
    staged: StagedConfig,
    *,
    step_index: int,
    total_steps: int,
) -> tuple[dict[str, int | list[str]], dict[str, str]]:
    console.print(Rule(f"[bold {ACCENT}]Step {step_index}/{total_steps}  Storage[/bold {ACCENT}]"))
    console.print("Choose where BroodMind stores runtime state and the shared workspace.")

    current_workspace = _effective_value(config, staged, "BROODMIND_WORKSPACE_DIR", "workspace")
    workspace = Prompt.ask("Workspace directory", default=current_workspace)
    _set_if_changed(config, staged, "BROODMIND_WORKSPACE_DIR", workspace)
    workspace_result = _ensure_workspace_bootstrap(Path(workspace))

    current_state = _effective_value(config, staged, "BROODMIND_STATE_DIR", "data")
    state_dir = Prompt.ask("State directory (DB, logs)", default=current_state)
    _set_if_changed(config, staged, "BROODMIND_STATE_DIR", state_dir)

    return workspace_result, {"workspace": workspace, "state_dir": state_dir}


def _configure_optional_tools(
    config: ConfigManager,
    staged: StagedConfig,
    *,
    step_index: int,
    total_steps: int,
) -> dict[str, str]:
    console.print(Rule(f"[bold {ACCENT}]Step {step_index}/{total_steps}  Tools and Features[/bold {ACCENT}]"))
    console.print("Turn on the extra services you want BroodMind to use.")

    summary: dict[str, str] = {}

    current_brave = _effective_value(config, staged, "BRAVE_API_KEY", "")
    if Confirm.ask("Configure Brave web search?", default=bool(current_brave)):
        brave_key = Prompt.ask("Brave API Key", default=current_brave, password=bool(current_brave))
        _set_if_changed(config, staged, "BRAVE_API_KEY", brave_key)
        summary["Brave search"] = "enabled" if brave_key.strip() else "not configured"
    else:
        summary["Brave search"] = "disabled"

    current_firecrawl = _effective_value(config, staged, "FIRECRAWL_API_KEY", "")
    if Confirm.ask("Configure Firecrawl web fetch?", default=bool(current_firecrawl)):
        firecrawl_key = Prompt.ask(
            "Firecrawl API Key",
            default=current_firecrawl,
            password=bool(current_firecrawl),
        )
        _set_if_changed(config, staged, "FIRECRAWL_API_KEY", firecrawl_key)
        summary["Firecrawl"] = "enabled" if firecrawl_key.strip() else "not configured"
    else:
        summary["Firecrawl"] = "disabled"

    current_openai = _effective_value(config, staged, "OPENAI_API_KEY", "")
    if Confirm.ask("Configure semantic memory embeddings?", default=bool(current_openai)):
        openai_key = Prompt.ask("OpenAI API Key", default=current_openai, password=bool(current_openai))
        _set_if_changed(config, staged, "OPENAI_API_KEY", openai_key)

        if openai_key.strip():
            current_base_url = _effective_value(config, staged, "OPENAI_BASE_URL", "https://api.openai.com/v1")
            openai_base_url = Prompt.ask("OpenAI Base URL", default=current_base_url)
            _set_if_changed(config, staged, "OPENAI_BASE_URL", openai_base_url)

            current_embed_model = _effective_value(config, staged, "OPENAI_EMBED_MODEL", "text-embedding-3-small")
            openai_embed_model = Prompt.ask("OpenAI Embedding Model", default=current_embed_model)
            _set_if_changed(config, staged, "OPENAI_EMBED_MODEL", openai_embed_model)
            summary["Semantic memory"] = openai_embed_model
        else:
            summary["Semantic memory"] = "not configured"
    else:
        summary["Semantic memory"] = "disabled"

    return summary


def _configure_gateway_security(
    config: ConfigManager,
    staged: StagedConfig,
    *,
    step_index: int,
    total_steps: int,
) -> dict[str, str]:
    console.print(Rule(f"[bold {ACCENT}]Step {step_index}/{total_steps}  Gateway and Security[/bold {ACCENT}]"))
    console.print("Tune network exposure and dashboard protection.")

    auto_serve_default = _env_bool(_effective_value(config, staged, "BROODMIND_TAILSCALE_AUTO_SERVE", "1"), default=True)
    auto_serve = Confirm.ask("Enable automatic `tailscale serve` at startup?", default=auto_serve_default)
    _set_if_changed(config, staged, "BROODMIND_TAILSCALE_AUTO_SERVE", "1" if auto_serve else "0")

    current_tailscale_ips = _effective_value(config, staged, "BROODMIND_TAILSCALE_IPS", "")
    tailscale_ips = Prompt.ask(
        "Trusted Tailscale IPs (comma-separated, leave blank to auto-discover)",
        default=current_tailscale_ips,
    )
    _set_if_changed(config, staged, "BROODMIND_TAILSCALE_IPS", tailscale_ips)

    protect_dashboard = Confirm.ask(
        "Require dashboard token authentication?",
        default=bool(_effective_value(config, staged, "BROODMIND_DASHBOARD_TOKEN", "")),
    )
    dashboard_token = _effective_value(config, staged, "BROODMIND_DASHBOARD_TOKEN", "")
    if protect_dashboard:
        dashboard_token = Prompt.ask(
            "Dashboard token",
            default=dashboard_token,
            password=bool(dashboard_token),
        )
    _set_if_changed(config, staged, "BROODMIND_DASHBOARD_TOKEN", dashboard_token if protect_dashboard else "")

    return {
        "tailscale_serve": "enabled" if auto_serve else "disabled",
        "dashboard_auth": "enabled" if protect_dashboard and bool(dashboard_token.strip()) else "disabled",
    }


def _configure_runtime_defaults(
    config: ConfigManager,
    staged: StagedConfig,
    *,
    step_index: int,
    total_steps: int,
) -> dict[str, str]:
    console.print(Rule(f"[bold {ACCENT}]Step {step_index}/{total_steps}  Runtime Defaults[/bold {ACCENT}]"))
    console.print("Set defaults for logs, heartbeat behavior, and worker execution.")

    current_log_level = _effective_value(config, staged, "BROODMIND_LOG_LEVEL", "INFO")
    log_level = Prompt.ask(
        "Log level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=current_log_level,
    )
    _set_if_changed(config, staged, "BROODMIND_LOG_LEVEL", log_level)

    current_heartbeat = int(_effective_value(config, staged, "BROODMIND_HEARTBEAT_INTERVAL_SECONDS", "900") or 900)
    heartbeat = IntPrompt.ask("Heartbeat interval (seconds)", default=current_heartbeat)
    _set_if_changed(config, staged, "BROODMIND_HEARTBEAT_INTERVAL_SECONDS", str(max(60, heartbeat)))

    current_launcher = _effective_value(config, staged, "BROODMIND_WORKER_LAUNCHER", "same_env")
    launcher = Prompt.ask("Worker launcher", choices=["same_env", "docker"], default=current_launcher)
    _set_if_changed(config, staged, "BROODMIND_WORKER_LAUNCHER", launcher)

    if launcher == "docker":
        current_image = _effective_value(config, staged, "BROODMIND_WORKER_DOCKER_IMAGE", "broodmind-worker:latest")
        image = Prompt.ask("Worker Docker image", default=current_image)
        _set_if_changed(config, staged, "BROODMIND_WORKER_DOCKER_IMAGE", image)

        current_docker_workspace = _effective_value(
            config,
            staged,
            "BROODMIND_WORKER_DOCKER_WORKSPACE",
            "/workspace",
        )
        docker_workspace = Prompt.ask("Worker Docker workspace path", default=current_docker_workspace)
        _set_if_changed(config, staged, "BROODMIND_WORKER_DOCKER_WORKSPACE", docker_workspace)
    else:
        image = _effective_value(config, staged, "BROODMIND_WORKER_DOCKER_IMAGE", "broodmind-worker:latest")

    return {
        "log_level": log_level,
        "heartbeat": f"{max(60, heartbeat)}s",
        "launcher": launcher,
        "docker_image": image,
    }


def _prompt_for_model(provider_id: str, current_model: str) -> str:
    provider_entry = get_provider_catalog_entry(provider_id)
    example_model = provider_entry.default_model
    prompt = f"{provider_entry.model_label} (example: {example_model})"
    return Prompt.ask(prompt, default=current_model or provider_entry.default_model)


def _print_workspace_bootstrap(workspace_result: dict[str, int | list[str]]) -> None:
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


def _print_review(
    *,
    config: ConfigManager,
    staged: StagedConfig,
    access_summary: dict[str, str],
    provider_summary: dict[str, str],
    storage_summary: dict[str, str],
    features_summary: dict[str, str],
    gateway_summary: dict[str, str],
    runtime_summary: dict[str, str],
    advanced_mode: bool,
) -> None:
    overview = Table.grid(padding=(0, 2))
    overview.add_column(style="bold white")
    overview.add_column()
    overview.add_row("Mode", "Advanced" if advanced_mode else "Quick")
    overview.add_row("User channel", access_summary["channel"])
    overview.add_row("Provider", f"{provider_summary['provider']} [dim]({provider_summary['provider_id']})[/dim]")
    overview.add_row("Model", provider_summary["model"])
    overview.add_row("Provider key", provider_summary["api_key_set"])
    overview.add_row("Workspace", storage_summary["workspace"])
    overview.add_row("State dir", storage_summary["state_dir"])

    console.print(
        Panel(
            overview,
            title="[bold white]Configuration Review[/bold white]",
            border_style=SURFACE,
            padding=(1, 2),
        )
    )

    sections: list[tuple[str, dict[str, str]]] = [
        ("User Channel", access_summary),
        ("Provider Profile", provider_summary),
        ("Features", features_summary),
        ("Runtime Paths", storage_summary),
    ]
    if gateway_summary:
        sections.append(("Gateway and Security", gateway_summary))
    if runtime_summary:
        sections.append(("Runtime Defaults", runtime_summary))

    for title, values in sections:
        table = Table(box=box.SIMPLE, show_header=True, header_style=f"bold {ACCENT}", expand=False)
        table.add_column("Setting", style="white", width=28)
        table.add_column("Value", style="dim", width=52)
        for key, value in values.items():
            table.add_row(key.replace("_", " ").capitalize(), value or "[dim](empty)[/dim]")
        console.print(Panel(table, title=f"[bold white]{title}[/bold white]", border_style=SURFACE, padding=(0, 1)))

    summary = Table(box=box.SIMPLE, show_header=True, header_style=f"bold {ACCENT}", expand=False)
    summary.add_column("Setting", style="white", width=40)
    summary.add_column("New Value", style="dim", width=40)
    if staged:
        for key, value in staged.items():
            summary.add_row(key, _mask_value(key, value))
    else:
        summary.add_row("[dim]No changes[/dim]", "[dim]Existing values kept[/dim]")

    console.print(
        Panel(
            summary,
            title="[bold white]Pending .env Changes[/bold white]",
            border_style=SURFACE,
            padding=(1, 2),
        )
    )


def _print_saved_summary(config: ConfigManager, staged: StagedConfig, *, user_channel: str) -> None:
    next_steps = _saved_summary_next_steps(user_channel)
    console.print(
        Panel(
            f"[bold {SUCCESS}][V] Configuration complete[/bold {SUCCESS}]\n"
            f"Saved to: [cyan]{config.env_path.absolute()}[/cyan]\n\n"
            f"Updated settings: [bold]{len(staged)}[/bold]\n\n"
            "[bold]Next:[/bold]\n"
            + "\n".join(f"[magenta]{step}[/magenta]" for step in next_steps),
            border_style=SUCCESS,
            padding=(1, 2),
        )
    )


def _saved_summary_next_steps(user_channel: str) -> list[str]:
    normalized_channel = normalize_user_channel(user_channel)
    if normalized_channel == "whatsapp":
        return [
            "uv run broodmind whatsapp install-bridge",
            "uv run broodmind whatsapp link",
            "uv run broodmind start",
            "uv run broodmind whatsapp status",
        ]
    return [
        "uv run broodmind start",
        "uv run broodmind status",
        "uv run broodmind config show",
    ]


def _mask_value(key: str, value: Any) -> str:
    if value is None:
        return "[dim](removed)[/dim]"
    text = str(value)
    lowered = key.lower()
    if any(token in lowered for token in ("token", "key", "secret")) and text:
        if len(text) <= 8:
            return "********"
        return f"{text[:4]}...{text[-4:]}"
    return text if text else "[dim](empty)[/dim]"


def _set_if_changed(config: ConfigManager, staged: StagedConfig, key: str, value: Any) -> None:
    new_value = str(value if value is not None else "")
    current_value = str(config.get(key, "") or "")
    if key in staged:
        current_value = str(config.get(key, "") or "")
        if new_value == current_value:
            staged.pop(key, None)
            return
    if new_value == current_value:
        return
    staged[key] = new_value


def _unset_if_present(config: ConfigManager, staged: StagedConfig, key: str) -> None:
    current_value = str(config.get(key, "") or "")
    if not current_value:
        staged.pop(key, None)
        return
    staged[key] = None


def _apply_staged_changes(config: ConfigManager, staged: StagedConfig) -> None:
    for key, value in staged.items():
        if value is None:
            config.unset(key)
            continue
        config.set(key, value)


def _effective_value(config: ConfigManager, staged: StagedConfig, key: str, default: str = "") -> str:
    if key in staged:
        return default if staged[key] is None else staged[key] or default
    return str(config.get(key, default) or default)


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


def _render_provider_select_list() -> list[str]:
    provider_choices: list[str] = []
    rendered_ids: set[str] = set()
    lines: list[str] = []

    for category, provider_ids in _PROVIDER_GROUPS.items():
        lines.append(f"[bold]{category}[/bold]")
        for provider_id in provider_ids:
            entry = get_provider_catalog_entry(provider_id)
            provider_choices.append(provider_id)
            rendered_ids.add(provider_id)
            lines.append(f"{len(provider_choices)}. {entry.label} [dim]({entry.description})[/dim]")
        lines.append("")

    for provider_id in list_registered_provider_ids(include_custom=True):
        if provider_id in rendered_ids:
            continue
        entry = get_provider_catalog_entry(provider_id)
        provider_choices.append(provider_id)
        lines.append(f"{len(provider_choices)}. {entry.label} [dim]({entry.description})[/dim]")

    console.print(Panel("\n".join(lines).strip(), border_style=SURFACE, padding=(1, 2)))
    return provider_choices


def _resolve_configured_provider_id(config: ConfigManager, staged: StagedConfig) -> str:
    explicit = _effective_value(config, staged, "BROODMIND_LITELLM_PROVIDER_ID", "").strip().lower()
    if explicit:
        return explicit

    legacy_provider = _effective_value(config, staged, "BROODMIND_LLM_PROVIDER", "litellm").strip().lower()
    if legacy_provider == "openrouter":
        return "openrouter"

    if _effective_value(config, staged, "ZAI_API_KEY", "").strip():
        return "zai"
    if _effective_value(config, staged, "OPENROUTER_API_KEY", "").strip():
        return "openrouter"
    return "zai"


def _default_provider_for_profiles(provider_id: str) -> str:
    normalized = (provider_id or "").strip().lower()
    return normalized if normalized in set(list_registered_provider_ids(include_custom=True)) else "custom"


def _default_profile_value(config: ConfigManager, staged: StagedConfig, provider_id: str, field_name: str) -> str:
    entry = get_provider_catalog_entry(provider_id)
    normalized_provider = _default_provider_for_profiles(provider_id)

    unified_keys = {
        "api_key": "BROODMIND_LITELLM_API_KEY",
        "api_base": "BROODMIND_LITELLM_API_BASE",
        "model": "BROODMIND_LITELLM_MODEL",
        "model_prefix": "BROODMIND_LITELLM_MODEL_PREFIX",
    }
    explicit_value = _effective_value(config, staged, unified_keys[field_name], "").strip()
    if explicit_value:
        return explicit_value

    if normalized_provider == "openrouter":
        legacy_map = {
            "api_key": "OPENROUTER_API_KEY",
            "api_base": "OPENROUTER_BASE_URL",
            "model": "OPENROUTER_MODEL",
            "model_prefix": "BROODMIND_LITELLM_MODEL_PREFIX",
        }
        legacy_value = _effective_value(config, staged, legacy_map[field_name], "").strip()
        if legacy_value:
            return legacy_value

    if normalized_provider == "zai":
        legacy_map = {
            "api_key": "ZAI_API_KEY",
            "api_base": "ZAI_BASE_URL",
            "model": "ZAI_MODEL",
            "model_prefix": "BROODMIND_LITELLM_MODEL_PREFIX",
        }
        legacy_value = _effective_value(config, staged, legacy_map[field_name], "").strip()
        if legacy_value:
            return legacy_value

    defaults = {
        "api_key": "",
        "api_base": entry.default_api_base or "",
        "model": entry.default_model,
        "model_prefix": entry.model_prefix or "",
    }
    return defaults[field_name]


def _stage_provider_model_prefix(
    config: ConfigManager,
    staged: StagedConfig,
    *,
    provider_id: str,
    supports_model_prefix_override: bool,
    advanced_mode: bool,
) -> None:
    current_prefix = _default_profile_value(config, staged, provider_id, "model_prefix")
    if provider_id == "ollama":
        _unset_if_present(config, staged, "BROODMIND_LITELLM_MODEL_PREFIX")
        return
    if supports_model_prefix_override or advanced_mode:
        if supports_model_prefix_override:
            selected_prefix = Prompt.ask("LiteLLM model prefix", default=current_prefix)
            _set_if_changed(config, staged, "BROODMIND_LITELLM_MODEL_PREFIX", selected_prefix)
        elif current_prefix:
            _set_if_changed(config, staged, "BROODMIND_LITELLM_MODEL_PREFIX", current_prefix)
        return
    if current_prefix:
        _set_if_changed(config, staged, "BROODMIND_LITELLM_MODEL_PREFIX", current_prefix)
