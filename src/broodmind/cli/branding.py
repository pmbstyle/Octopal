import textwrap
from importlib.metadata import PackageNotFoundError, version as get_version

from rich import print
from rich.align import Align
from rich.panel import Panel
from rich.text import Text


def print_banner() -> None:
    banner_text = textwrap.dedent(r"""
    ██████╗ ██████╗  ██████╗  ██████╗ ██████╗ ███╗   ███╗██╗███╗   ██╗██████╗
    ██╔══██╗██╔══██╗██╔═══██╗██╔═══██╗██╔══██╗████╗ ████║██║████╗  ██║██╔══██╗
    ██████╔╝██████╔╝██║   ██║██║   ██║██║  ██║██╔████╔██║██║██╔██╗ ██║██║  ██║
    ██╔══██╗██╔══██╗██║   ██║██║   ██║██║  ██║██║╚██╔╝██║██║██║╚██╗██║██║  ██║
    ██████╔╝██║  ██║╚██████╔╝╚██████╔╝██████╔╝██║ ╚═╝ ██║██║██║ ╚████║██████╔╝
    ╚═════╝ ╚═╝  ╚═╝ ╚═════╝  ╚═════╝ ╚═════╝ ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═════╝
    """).strip()

    try:
        current_version = get_version("broodmind")
    except PackageNotFoundError:
        current_version = "dev"

    tagline = Text("Professional multi-agent orchestration", style="italic bright_white")
    subline = Text("Fast setup. Safe defaults. Clear operations.", style="dim")

    # Create the content stack
    content = (
        Text(banner_text, style="bright_cyan", justify="center")
        + Text("\n\n")
        + tagline
        + Text("\n")
        + subline
    )

    panel = Panel(
        Align.center(content),
        border_style="cyan",
        title="[bold white]BroodMind CLI[/bold white]",
        subtitle=f"[bold bright_white]v{current_version}[/bold bright_white]",
        padding=(1, 2),
        expand=False,
        width=100,
    )
    print("\n")
    print(Align.center(panel))
    print("\n")
