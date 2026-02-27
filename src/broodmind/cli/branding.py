import textwrap
import sys
from importlib.metadata import PackageNotFoundError, version as get_version

from rich import print
from rich.align import Align
from rich.console import Group
from rich.table import Table
from rich.text import Text


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

    tagline = Text("Multi-agent orchestration", style="italic bright_white")
    subline = Text("Fast setup. Safe defaults. Clear operations.", style="dim")

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
        Text(wasp_centered, style="bright_yellow"),
        Text(banner_centered, style="bright_cyan"),
    )

    content = Group(
        Align.center(header),
        Text(""),
        Align.center(tagline),
        Align.center(subline),
        Align.center(Text(f"v{current_version}", style="bold bright_white")),
    )
    print("\n")
    print(Align.center(content))
    print("\n")
