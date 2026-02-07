#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f "pyproject.toml" ]]; then
  echo "Run this script from the repository root (pyproject.toml not found)." >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found. Installing..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi

if ! command -v uv >/dev/null 2>&1; then
  if [[ -f "$HOME/.local/bin/env" ]]; then
    # shellcheck source=/dev/null
    source "$HOME/.local/bin/env"
  fi
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is still not available on PATH." >&2
  echo "Run: source \"$HOME/.local/bin/env\"  and retry." >&2
  exit 1
fi

echo "Using $(uv --version)"
uv sync

echo
echo "Bootstrap complete."
echo "Next steps:"
echo "  uv run broodmind configure"
echo "  uv run broodmind start"
