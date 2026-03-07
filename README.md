# BroodMind

BroodMind is an AI orchestration system built around a **Queen + Workers** model.

- **Queen** talks to the user, plans work, tracks context, and chooses tools.
- **Workers** run focused tasks with bounded permissions and time limits.

It is designed for long-running assistant workflows (Telegram-first), with memory, scheduling, and operational guardrails.

## What It Can Do

- Handle Telegram conversations with planning + execution flow
- Delegate tasks to specialized workers
- Run filesystem/web/exec tools under policy controls
- Keep persistent memory and canon files in `workspace/memory/canon/`
- Track context health and proactively reset context when overloaded
- Expose a private dashboard/gateway for ops visibility

## Quick Start

### 1. Prerequisites

- Python 3.12+
- `uv` (recommended)
- Telegram bot token from [@BotFather](https://t.me/botfather)
- At least one LLM API key:
  - `BROODMIND_LITELLM_API_KEY` for the provider you select in `broodmind configure`, or
  - legacy `ZAI_API_KEY` / `OPENROUTER_API_KEY` if you are upgrading an existing setup

Install `uv` if needed:

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```powershell
# Windows PowerShell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. One-shot bootstrap

```bash
git clone https://github.com/pmbstyle/BroodMind.git
cd BroodMind
```

```bash
# macOS/Linux
chmod +x ./scripts/bootstrap.sh
./scripts/bootstrap.sh
```

```powershell
# Windows PowerShell
./scripts/bootstrap.ps1
```

This is the main starting path. The bootstrap script installs dependencies, installs Playwright Chromium, and then launches `broodmind configure`.

### 3. Open the web dashboard

After bootstrap, start BroodMind and then open the dashboard in your browser:

```bash
uv run broodmind start
```

Open [http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard).

If you enabled dashboard protection during `broodmind configure`, use the value of `BROODMIND_DASHBOARD_TOKEN` from `.env` when the dashboard or dashboard API asks for it.

If the page says the dashboard is unavailable, build and enable the web app first:

```bash
cd webapp
npm run build
```

Then set `BROODMIND_WEBAPP_ENABLED=true` in `.env` and start BroodMind again.

### 4. Manual setup

If you do not want the bootstrap script, use the manual path below.

```bash
git clone https://github.com/pmbstyle/BroodMind.git
cd BroodMind
uv sync
uv run broodmind configure
```

Alternative without `uv`:

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
pip install -e .
```

Then run:

```bash
broodmind configure
```

`configure` creates/updates `.env` and bootstraps workspace files if missing.

### 5. Start

```bash
# background mode
uv run broodmind start

# foreground mode
uv run broodmind start --foreground
```

### 6. Check Health

```bash
uv run broodmind status
uv run broodmind logs --follow
```

## Core Commands

```bash
# lifecycle
uv run broodmind start
uv run broodmind stop
uv run broodmind restart
uv run broodmind status
uv run broodmind logs --follow

# gateway/dashboard
uv run broodmind gateway
uv run broodmind dashboard
uv run broodmind dashboard --watch

# worker templates
uv run broodmind sync-worker-templates
uv run broodmind sync-worker-templates --overwrite

# memory maintenance
uv run broodmind memory stats
uv run broodmind memory cleanup --dry-run
```

## Optional: Docker Worker Launcher

Default runtime is non-Docker. If you want Dockerized workers:

```bash
uv run broodmind build-worker-image --tag broodmind-worker:latest
```

Then set in `.env`:

```env
BROODMIND_WORKER_LAUNCHER=docker
BROODMIND_WORKER_DOCKER_IMAGE=broodmind-worker:latest
```

Restart BroodMind after config changes.

## Troubleshooting

### Bot starts but does not reply

- Verify `TELEGRAM_BOT_TOKEN`
- Verify your chat ID is in `ALLOWED_TELEGRAM_CHAT_IDS`
- Check `uv run broodmind status` and `uv run broodmind logs --follow`

### LLM errors

- Run `uv run broodmind configure` and pick the provider you want to use.
- For unified LiteLLM config: set `BROODMIND_LITELLM_PROVIDER_ID`, `BROODMIND_LITELLM_MODEL`, and `BROODMIND_LITELLM_API_KEY`.
- Existing `ZAI_*` and `OPENROUTER_*` variables still work as legacy fallbacks.

### Web search/fetch issues

- Add `BRAVE_API_KEY` for `web_search`
- Add `FIRECRAWL_API_KEY` for richer page extraction

## License

MIT
