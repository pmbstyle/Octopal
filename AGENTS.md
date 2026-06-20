# Repository Guidelines

## Project Structure & Module Organization

- `src/octopal/` contains the main Python package: CLI, channels, gateway, memory, policy, providers, Octo runtime, scheduler, WhatsApp/Telegram integrations, workers, and shared utilities.
- `webapp/` holds the Vite-based dashboard frontend. `src/` contains app code and `dist/` contains built assets.
- `tests/` contains the pytest suite for CLI, dashboard, runtime, worker orchestration, memory, and channel behavior.
- `scripts/` contains setup and maintenance helpers such as bootstrap and worker-template sync utilities.
- `docker/` contains container assets, including the worker image Dockerfile.
- `data/` is runtime state storage for SQLite, metrics, auth state, and logs; avoid committing generated contents.
- `workspace/` is the default Octo/worker workspace and scratch area.
- `workspace_templates/` contains bootstrap content copied into new workspaces.
- `docs/` stores additional project documentation.

## Build, Test, and Development Commands

- `uv sync` installs the project and dev dependencies for day-to-day development.
- `python -m venv .venv` and `pip install -e .[dev]` are the non-`uv` editable setup path.
- `uv run octopal configure` runs the interactive configuration wizard and bootstraps missing workspace files.
- `uv run octopal config show [--reveal-secrets]` prints the effective config for inspection, and `uv run octopal config migrate` writes the current `.env`-backed settings into `config.json`.
- `uv run octopal start` starts Octopal in background mode.
- `uv run octopal start --foreground` runs the Octo and gateway in the foreground.
- `uv run octopal stop`, `uv run octopal restart`, and `uv run octopal status` manage the local runtime.
- `uv run octopal update` applies the latest release update flow for an existing install.
- `uv run octopal logs` prints `data/logs/octopal.log`, and `uv run octopal logs --follow` tails it.
- `uv run octopal gateway` starts the FastAPI gateway directly.
- `uv run octopal dashboard --once` prints one dashboard snapshot; `uv run octopal dashboard --watch` runs the live terminal dashboard; `uv run octopal dashboard --json` emits a machine-readable snapshot; `--compact` and `--last <N>` tune terminal output.
- `uv run octopal workers list` shows registered workers, their current status, and the current task when available.
- `uv run octopal connector status [--json]` checks connector authorization/readiness after connector setup flows; `uv run octopal connector auth <name>` and `uv run octopal connector disconnect <name> [--forget-credentials]` handle CLI-based connector auth maintenance.
- `uv run octopal sync-worker-templates --overwrite` refreshes default worker templates into `workspace/workers`.
- `uv run octopal memory stats` and `uv run octopal memory cleanup [--keep-days <days>] [--keep-count <count>] [--dry-run]` cover common memory maintenance flows.
- `uv run octopal whatsapp install-bridge`, `uv run octopal whatsapp link [--timeout <seconds>]`, `uv run octopal whatsapp status`, and `uv run octopal whatsapp logout` manage the WhatsApp bridge lifecycle.
- `uv run octopal tools resolve [--profile <name>] [--preset all|octo] [--available-only] [--json]` explains which tools are available and why others are blocked.
- `uv run octopal skill list`, `uv run octopal skill install <source>`, `uv run octopal skill update <skill-id>`, `uv run octopal skill verify <skill-id>`, `uv run octopal skill enable <skill-id>`, `uv run octopal skill disable <skill-id>`, `uv run octopal skill trust <skill-id>`, `uv run octopal skill untrust <skill-id>`, `uv run octopal skill prepare-env <skill-id>`, `uv run octopal skill remove-env <skill-id>`, and `uv run octopal skill remove <skill-id>` cover the installed skill workflow.
- `uv run octopal build-worker-image --tag octopal-worker:latest` builds the Docker worker image.
- `uv run pytest` runs the test suite.
- `uv run ruff check .`, `uv run black --check .`, and `uv run mypy src` are the configured lint/format/type-check commands.
- `npm install` and `npm run build` from `webapp/` build the dashboard bundle manually when needed.

## Coding Style & Naming Conventions

- Python code lives under `src/` with imports rooted at `octopal`.
- Use 4-space indentation, type hints on new or changed Python code, and descriptive module names.
- Follow the configured tooling in `pyproject.toml`: Black for formatting, Ruff for linting/import order, and MyPy for type checks.
- Keep CLI entrypoints in `src/octopal/cli/` and group related runtime code under focused packages such as `gateway/`, `memory/`, `octo/`, and `workers/`.
- Frontend code in `webapp/src/` should stay TypeScript-first and match the existing Vite/Tailwind setup.

## Testing Guidelines

- Add Python tests under `tests/` using `test_<module>.py` naming.
- Prefer focused pytest coverage near the behavior you change, especially for CLI flows, runtime safety checks, worker orchestration, and dashboard APIs.
- Run `uv run pytest` before finishing substantial changes. For frontend-only changes, also run `npm run build` in `webapp/`.
- When you add new tooling or test workflows, update this file and `README.md`.
- GitHub releases use date-based versioning in `src/octopal/_version.py` and tags like `vYYYY.MM.DD` or `vYYYY.MM.DD.N`.

## Commit & Pull Request Guidelines

- Use concise, imperative commit subjects such as `update AGENTS guide` or `harden worker status recovery`.
- Keep commits scoped to one logical change when practical.
- PRs should include a short description, linked issue if relevant, and logs or screenshots for user-facing CLI/dashboard changes.

## Security & Configuration Tips

- Use `uv run octopal configure` to manage the primary `config.json` settings.
- Connector setup is CLI-first: enable services in `uv run octopal configure`, run `uv run octopal connector auth <name>`, check `uv run octopal connector status`, then restart Octopal if the CLI asks for it.
- If Docker CLI/daemon or the worker image is unavailable, Octopal may temporarily fall back to `same_env`; verify the effective launcher in `uv run octopal status` or the dashboard and treat it as a local-development fallback, not the preferred isolation mode.
- **Tailscale Integration:** Managed in the `gateway` section of `config.json` via `tailscale_auto_serve` (boolean) and `tailscale_ips` (comma-separated string).
- Important settings include channel credentials, provider API keys, dashboard protection, and the storage paths in `config.json`.
- Treat `data/`, WhatsApp auth state, and generated workspace files as local runtime artifacts unless the repo explicitly needs fixtures.

## Octo Context Reset Policy

- The Octo can invoke `octo_context_reset` to compact or reset overloaded chat context.
- Preferred default is `mode=soft` with structured handoff fields: `goal_now`, `done`, `open_threads`, `critical_constraints`, and `next_step`.
- Persist reset artifacts in workspace memory:
  - `memory/handoff.md`, `memory/handoff.json`
  - `memory/context-audit.md`, `memory/context-audit.jsonl`
- Confirmation is required when:
  - `mode=hard`
  - `confidence < 0.7`
  - repeated resets occur without progress (`N=2`)
- After reset, force a wake-up choice (`continue / clarify / replan`) before major actions.
