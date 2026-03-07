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

# Ensure ~/.local/bin is in PATH persistently
UV_BIN_DIR="$HOME/.local/bin"
if [[ -d "$UV_BIN_DIR" ]] && [[ ":$PATH:" != *":$UV_BIN_DIR:"* ]]; then
  SHELL_CONFIG=""
  if [[ "$SHELL" == */zsh ]]; then
    SHELL_CONFIG="$HOME/.zshrc"
  elif [[ "$SHELL" == */bash ]]; then
    SHELL_CONFIG="$HOME/.bashrc"
  fi

  if [[ -n "$SHELL_CONFIG" ]]; then
    if ! grep -q "$UV_BIN_DIR" "$SHELL_CONFIG" 2>/dev/null; then
      echo "Adding $UV_BIN_DIR to PATH in $SHELL_CONFIG"
      echo "export PATH=\"\$PATH:$UV_BIN_DIR\"" >> "$SHELL_CONFIG"
    fi
  fi
  export PATH="$PATH:$UV_BIN_DIR"
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is still not available on PATH." >&2
  echo "Run: source \"$HOME/.local/bin/env\"  and retry." >&2
  exit 1
fi

echo "Using $(uv --version)"
uv sync

echo "Installing Playwright browser binaries..."
uv run playwright install chromium

echo
echo "Launching onboarding..."
uv run broodmind configure

echo
echo "Bootstrap complete."
echo "Next steps:"
echo "  uv run broodmind start"
