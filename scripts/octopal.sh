#!/bin/bash
# Octopal Installation Script for Unix (macOS / Linux)
set -e

REPO_URL="https://github.com/pmbstyle/Octopal.git"
LATEST_RELEASE_API_URL="https://api.github.com/repos/pmbstyle/Octopal/releases/latest"

install_git() {
  echo "git not found. Attempting to install..."
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update && sudo apt-get install -y git
  elif command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y git
  elif command -v yum >/dev/null 2>&1; then
    sudo yum install -y git
  elif command -v pacman >/dev/null 2>&1; then
    sudo pacman -S --noconfirm git
  elif command -v brew >/dev/null 2>&1; then
    brew install git
  else
    echo "Error: Could not auto-install git. Please install git manually and try again." >&2
    exit 1
  fi
}

get_latest_release_tag() {
  local release_tag=""

  if command -v curl >/dev/null 2>&1; then
    release_tag="$(curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      -H "User-Agent: octopal-installer" \
      "$LATEST_RELEASE_API_URL" \
      | grep -m 1 '"tag_name"' \
      | sed -E 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
  elif command -v wget >/dev/null 2>&1; then
    release_tag="$(wget -qO- \
      --header="Accept: application/vnd.github+json" \
      --header="User-Agent: octopal-installer" \
      "$LATEST_RELEASE_API_URL" \
      | grep -m 1 '"tag_name"' \
      | sed -E 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
  fi

  if [[ -n "$release_tag" ]]; then
    printf '%s\n' "$release_tag"
    return 0
  fi

  echo "Could not fetch the latest GitHub release. Falling back to the latest tag..." >&2
  release_tag="$(git ls-remote --tags --sort=-version:refname --refs "$REPO_URL" | head -n 1 | sed 's#.*refs/tags/##')"

  if [[ -z "$release_tag" ]]; then
    echo "Error: Could not determine the latest Octopal release tag." >&2
    exit 1
  fi

  printf '%s\n' "$release_tag"
}

if ! command -v git >/dev/null 2>&1; then
  install_git
fi

release_tag="$(get_latest_release_tag)"

echo "Installing Octopal $release_tag..."
git -c advice.detachedHead=false clone --branch "$release_tag" --depth 1 "$REPO_URL" octopal
cd octopal

if [[ -f "scripts/bootstrap.sh" ]]; then
  echo "Running bootstrap script..."
  bash scripts/bootstrap.sh
else
  echo "Error: bootstrap script not found in repository." >&2
  exit 1
fi
