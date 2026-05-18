import sys
import textwrap

from rich import print
from rich.align import Align
from rich.console import Group
from rich.text import Text
from octopal import __version__

OCTO_SILVER = "#6aafae"
OCTO_BLUE = "#0f4e5d"
SUBTLE_STEEL = "#6aafae"
OCTO_WHITE = "#ebebeb"


def _split_brand_text(block: str) -> Text:
    lines = block.splitlines()
    split_at = max(1, int(max(len(line.rstrip()) for line in lines) * 0.58))
    rendered = Text()
    for line in lines:
        rendered.append(line[:split_at], style=OCTO_SILVER)
        rendered.append(line[split_at:], style=OCTO_BLUE)
        rendered.append("\n")
    return rendered


def print_banner() -> None:
    banner_text = textwrap.dedent(r"""
░█████╗░░█████╗░████████╗░█████╗░██████╗░░█████╗░██╗░░░░░
██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██║░░░░░
██║░░██║██║░░╚═╝░░░██║░░░██║░░██║██████╔╝███████║██║░░░░░
██║░░██║██║░░██╗░░░██║░░░██║░░██║██╔═══╝░██╔══██║██║░░░░░
╚█████╔╝╚█████╔╝░░░██║░░░╚█████╔╝██║░░░░░██║░░██║███████╗
░╚════╝░░╚════╝░░░░╚═╝░░░░╚════╝░╚═╝░░░░░╚═╝░░╚═╝╚══════╝
    """).strip()

    output_encoding = (sys.stdout.encoding or "utf-8").lower()
    try:
        banner_text.encode(output_encoding, errors="strict")
    except UnicodeEncodeError:
        banner_text = "OCTOPAL"

    tagline = Text("Your trusted AI pal", style=f"italic {OCTO_SILVER}")
    subline = Text("SECURE MULTI-AGENT EXECUTION RUNTIME", style=OCTO_WHITE)

    content = Group(
        Align.center(_split_brand_text(banner_text)),
        Text(""),
        Align.center(tagline),
        Align.center(subline),
        Align.center(Text(f"v{__version__}", style=f"bold {OCTO_SILVER}")),
    )
    print("\n")
    print(Align.center(content))
    print("\n")
