import textwrap
from rich import print
from rich.panel import Panel
from rich.align import Align
from rich.text import Text

def print_banner() -> None:
    # ASCII Art for "BroodMind"
    # Added explicit spacing between Brood and Mind to prevent visual collapsing
    banner_text = textwrap.dedent(r"""
      ____                     _ __  __             _ 
     |  _ \                   | |  \/  |           | |
     | |_) |_ __ ___   ___  __| | \  / |_ _ __   __| |
     |  _ <| '__/ _ \ / _ \/ _` | |\/| | | '_ \ / _` |
     | |_) | | | (_) | (_) | (_|| |  | | | | | | (_| |
     |____/|_|  \___/ \___/\__,_|_|  |_|_|_| |_|\__,_|
    """).strip()
    
    tagline = Text("Friendly and safe agent for clever humans", style="bold cyan italic")
    
    # Create the content stack
    content = Text(banner_text, style="bold magenta", justify="center") + Text("\n\n") + tagline
    
    panel = Panel(
        Align.center(content),
        border_style="cyan",
        subtitle="v0.1.0",
        padding=(1, 2),
        expand=True
    )
    print(panel)