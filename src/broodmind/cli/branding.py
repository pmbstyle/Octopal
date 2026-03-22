import sys
import textwrap
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as get_version

from rich import print
from rich.align import Align
from rich.console import Group
from rich.table import Table
from rich.text import Text

BROOD_SILVER = "#d8dde2"
MIND_GOLD = "#f0c15d"
WASP_GOLD = "#f0c15d"
SUBTLE_STEEL = "#5f8ea3"


def _split_brand_text(block: str) -> Text:
    lines = block.splitlines()
    split_at = max(1, int(max(len(line.rstrip()) for line in lines) * 0.58))
    rendered = Text()
    for line in lines:
        rendered.append(line[:split_at], style=BROOD_SILVER)
        rendered.append(line[split_at:], style=MIND_GOLD)
        rendered.append("\n")
    return rendered


def print_banner() -> None:
    wasp_text = textwrap.dedent(r"""
    ⠀⠀⠀⠀⠀⢀⣀⠀⠀⠀⢀⣠⡴⠶⠶⣤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⢠⣶⠟⢹⡏⠉⠛⠻⢿⣧⣀⠀⠀⠀⠙⣦⡀⠀⠀⠀⠀⠰⣤⡀⠀⠀⠀⠀
    ⠀⢠⡿⠁⠀⢸⣧⣄⡀⠀⠀⠈⠙⠻⣦⣄⠀⠈⢷⡀⠸⠷⢶⣦⣌⠻⣦⠀⠀⠀
    ⠀⣾⣧⣄⣀⢸⣯⠉⠛⠻⠶⣦⣄⣀⠀⠙⠿⣦⡈⢷⡄⠀⠀⠀⣙⣿⣾⣧⠀⠀
    ⠀⢹⣇⠉⠙⠛⢿⣄⠀⠀⠀⠀⣩⡿⠛⠳⢶⣼⣿⣿⣿⣿⠀⢸⡏⠉⢹⣿⡇⠀
    ⠀⠀⠛⢷⣦⣤⣤⣽⣷⣤⣤⣾⣯⣤⣤⣴⣾⣿⣿⣿⣿⣿⣇⠘⢷⣶⣾⣿⡇⠀
    ⠀⠀⠀⠀⠀⠀⠀⠉⠉⠉⠉⠉⠁⠀⠀⠀⠻⢿⣿⣿⣿⠟⠁⠀⠀⠙⠻⠿⠇⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣶⣿⣿⣶⣤⣤⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣄⠙⠛⠿⠿⠿⠿⠿⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⣷⣶⣶⣶⣶⣶⠆⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⠿⣿⣿⠿⠟⢁⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠳⣶⣤⣤⣤⣴⣾⣿⣷⠀⠀⣠⣤⣤⣤⣶⡞⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠙⠛⠛⠛⠋⣁⣴⣿⣿⣿⡿⠟⠋⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉⠉⠀⠀⠀⠀⠀⠀
    """).strip()
    wasp_text_ascii = textwrap.dedent(r"""
       \  _  /
    ---=(o o)=---
        / V \
      Brood Wasp
    """).strip()
    banner_text = textwrap.dedent(r"""
    ██████╗ ██████╗  ██████╗  ██████╗ ██████╗ ███╗   ███╗██╗███╗   ██╗██████╗
    ██╔══██╗██╔══██╗██╔═══██╗██╔═══██╗██╔══██╗████╗ ████║██║████╗  ██║██╔══██╗
    ██████╔╝██████╔╝██║   ██║██║   ██║██║  ██║██╔████╔██║██║██╔██╗ ██║██║  ██║
    ██╔══██╗██╔══██╗██║   ██║██║   ██║██║  ██║██║╚██╔╝██║██║██║╚██╗██║██║  ██║
    ██████╔╝██║  ██║╚██████╔╝╚██████╔╝██████╔╝██║ ╚═╝ ██║██║██║ ╚████║██████╔╝
    ╚═════╝ ╚═╝  ╚═╝ ╚═════╝  ╚═════╝ ╚═════╝ ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═════╝
    """).strip()
    banner_text_ascii = textwrap.dedent(r"""
    ____                      _ __  __ _           _
   | __ ) _ __ ___   ___   __| |  \/  (_)_ __   __| |
   |  _ \| '__/ _ \ / _ \ / _` | |\/| | | '_ \ / _` |
   | |_) | | | (_) | (_) | (_| | |  | | | | | | (_| |
   |____/|_|  \___/ \___/ \__,_|_|  |_|_|_| |_|\__,_|
    """).strip()

    try:
        current_version = get_version("broodmind")
    except PackageNotFoundError:
        current_version = "dev"

    output_encoding = (sys.stdout.encoding or "utf-8").lower()
    selected_wasp_text = wasp_text
    selected_banner_text = banner_text
    try:
        wasp_text.encode(output_encoding, errors="strict")
    except UnicodeEncodeError:
        selected_wasp_text = wasp_text_ascii
    try:
        banner_text.encode(output_encoding, errors="strict")
    except UnicodeEncodeError:
        selected_banner_text = banner_text_ascii

    wasp_lines = selected_wasp_text.splitlines()
    banner_lines = selected_banner_text.splitlines()
    target_lines = max(len(wasp_lines), len(banner_lines))

    def _pad_center(lines: list[str], target: int) -> str:
        top_pad = max(0, (target - len(lines)) // 2)
        bottom_pad = max(0, target - len(lines) - top_pad)
        padded = ([""] * top_pad) + lines + ([""] * bottom_pad)
        return "\n".join(padded)

    wasp_centered = _pad_center(wasp_lines, target_lines)
    banner_centered = _pad_center(banner_lines, target_lines)

    header = Table.grid(padding=(0, 2))
    header.add_column(justify="left", no_wrap=True)
    header.add_column(justify="left", no_wrap=True)
    header.add_row(
        Text(wasp_centered, style=WASP_GOLD),
        _split_brand_text(banner_centered.rstrip("\n")),
    )

    tagline = Text("Multi-agent orchestration", style=f"italic {BROOD_SILVER}")
    subline = Text("RUN YOUR OWN AI HIVE, FAST AND SECURE!", style=MIND_GOLD)

    content = Group(
        Align.center(header),
        Text(""),
        Align.center(tagline),
        Align.center(subline),
        Align.center(Text(f"v{current_version}", style=f"bold {BROOD_SILVER}")),
    )
    print("\n")
    print(Align.center(content))
    print("\n")
