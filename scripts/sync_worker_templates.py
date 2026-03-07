from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from broodmind.workers.templates import sync_default_templates


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(
        description="Sync worker templates from workspace_templates/workers into workspace/workers."
    )
    parser.add_argument(
        "--workspace",
        default=os.getenv("BROODMIND_WORKSPACE_DIR", "workspace"),
        help="Workspace directory (default: BROODMIND_WORKSPACE_DIR or ./workspace)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing workspace worker templates.",
    )
    args = parser.parse_args()

    workspace_dir = Path(args.workspace).resolve()
    result = sync_default_templates(workspace_dir, overwrite=args.overwrite)
    logger.info(
        "Worker template sync complete: "
        f"copied={result['copied']} updated={result['updated']} skipped={result['skipped']} "
        f"target={workspace_dir / 'workers'}"
    )


if __name__ == "__main__":
    main()
