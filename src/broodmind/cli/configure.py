from __future__ import annotations

import shutil
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.text import Text

from broodmind.cli.branding import print_banner
from broodmind.config.manager import ConfigManager

console = Console()


def configure_wizard() -> None:
    """Run the interactive configuration wizard."""
    print_banner()

    console.print(Panel(
        Text("Welcome to the BroodMind Configuration Wizard!\n", style="bold green") +
        Text("This tool will help you set up your environment to run the BroodMind Queen and Workers.", style="dim"),
        title="[bold cyan]Onboarding[/bold cyan]",
        border_style="blue",
        padding=(1, 2)
    ))

    config = ConfigManager()

    # 1. Telegram Bot Token
    console.print("\n[bold]Step 1: Telegram Integration[/bold]")
    console.print("You need a Telegram Bot Token from @BotFather.")
    current_token = config.get("TELEGRAM_BOT_TOKEN", "")
    token = Prompt.ask(
        "Enter your Telegram Bot Token",
        default=current_token,
        password=bool(current_token)
    )
    if token:
        config.set("TELEGRAM_BOT_TOKEN", token)

    # 2. Allowed Chat IDs
    console.print("\n[bold]Step 2: Access Control[/bold]")
    console.print("Which Telegram users/groups are allowed to talk to the Queen?")
    console.print("[dim]You can find your ID by messaging @userinfobot on Telegram.[/dim]")
    current_ids = config.get("ALLOWED_TELEGRAM_CHAT_IDS", "")
    allowed_ids = Prompt.ask(
        "Enter allowed Chat IDs (comma-separated)",
        default=current_ids
    )
    if allowed_ids:
        config.set("ALLOWED_TELEGRAM_CHAT_IDS", allowed_ids)

    # 3. LLM Provider
    console.print("\n[bold]Step 3: LLM Provider[/bold]")
    current_provider = config.get("BROODMIND_LLM_PROVIDER", "litellm")
    provider = Prompt.ask(
        "Choose LLM Provider",
        choices=["litellm", "openrouter"],
        default=current_provider
    )
    config.set("BROODMIND_LLM_PROVIDER", provider)

    if provider == "openrouter":
        console.print("\n[bold]Step 3a: OpenRouter Configuration[/bold]")
        current_or_key = config.get("OPENROUTER_API_KEY", "")
        or_key = Prompt.ask(
            "Enter OpenRouter API Key",
            default=current_or_key,
            password=bool(current_or_key)
        )
        if or_key:
            config.set("OPENROUTER_API_KEY", or_key)

        current_or_model = config.get("OPENROUTER_MODEL", "anthropic/claude-sonnet-4")
        or_model = Prompt.ask("Enter default OpenRouter model", default=current_or_model)
        config.set("OPENROUTER_MODEL", or_model)
    else:
        console.print("\n[bold]Step 3a: LiteLLM / Z.ai Configuration[/bold]")
        current_zai_key = config.get("ZAI_API_KEY", "")
        zai_key = Prompt.ask(
            "Enter Z.ai (or OpenAI-compatible) API Key",
            default=current_zai_key,
            password=bool(current_zai_key)
        )
        if zai_key:
            config.set("ZAI_API_KEY", zai_key)

        current_zai_base = config.get("ZAI_BASE_URL", "https://api.z.ai/api/coding/paas/v4")
        zai_base = Prompt.ask("Enter Z.ai Base URL", default=current_zai_base)
        config.set("ZAI_BASE_URL", zai_base)

        current_zai_model = config.get("ZAI_MODEL", "glm-5")
        zai_model = Prompt.ask("Enter default model name", default=current_zai_model)
        config.set("ZAI_MODEL", zai_model)

    # 4. Workspace and State
    console.print("\n[bold]Step 4: Storage[/bold]")
    current_workspace = config.get("BROODMIND_WORKSPACE_DIR", "workspace")
    workspace = Prompt.ask("Enter workspace directory path", default=current_workspace)
    config.set("BROODMIND_WORKSPACE_DIR", workspace)
    workspace_result = _ensure_workspace_bootstrap(Path(workspace))

    current_state = config.get("BROODMIND_STATE_DIR", "data")
    state_dir = Prompt.ask("Enter state directory (DB, logs)", default=current_state)
    config.set("BROODMIND_STATE_DIR", state_dir)

    # 5. Optional Features
    console.print("\n[bold]Step 5: Optional Features[/bold]")

    # Brave Search
    if Confirm.ask("Do you want to enable Web Search (requires Brave API key)?", default=False):
        current_brave = config.get("BRAVE_API_KEY", "")
        brave_key = Prompt.ask("Enter Brave API Key", default=current_brave)
        if brave_key:
            config.set("BRAVE_API_KEY", brave_key)

    # OpenAI Embeddings
    if Confirm.ask("Do you want to enable Semantic Memory (requires OpenAI API key)?", default=False):
        current_openai = config.get("OPENAI_API_KEY", "")
        openai_key = Prompt.ask("Enter OpenAI API Key", default=current_openai)
        if openai_key:
            config.set("OPENAI_API_KEY", openai_key)
            
            # Ensure defaults or current values for base URL and embed model are saved
            current_base_url = config.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
            openai_base_url = Prompt.ask("Enter OpenAI Base URL", default=current_base_url)
            config.set("OPENAI_BASE_URL", openai_base_url)
            
            current_embed_model = config.get("OPENAI_EMBED_MODEL", "text-embedding-3-small")
            openai_embed_model = Prompt.ask("Enter OpenAI Embedding Model", default=current_embed_model)
            config.set("OPENAI_EMBED_MODEL", openai_embed_model)

    console.print()
    if workspace_result["created_files"]:
        created_lines = "\n".join(f"- {path}" for path in workspace_result["created_files"])
    else:
        created_lines = "- none (all files already existed)"

    console.print(Panel(
        "[bold cyan]Workspace bootstrap complete[/bold cyan]\n"
        f"Created files:\n{created_lines}\n\n"
        f"Skipped existing files: {workspace_result['skipped_files']}",
        border_style="blue"
    ))

    console.print(Panel(
        "[bold green][V] Configuration complete![/bold green]\n"
        f"Settings saved to: [cyan]{config.env_path.absolute()}[/cyan]\n\n"
        "You can now start the BroodMind Queen with:\n"
        "[bold magenta]uv run broodmind start[/bold magenta]",
        border_style="green"
    ))


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
