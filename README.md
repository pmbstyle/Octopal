# BroodMind

BroodMind is an AI orchestration platform built on a **Queen + Workers** architecture.
The Queen handles conversation, planning, memory, and delegation. Workers execute specialized tasks with bounded tools.

## Quick Start (Local, Non-Docker)

### Prerequisites

- Python 3.12+
- `uv` (recommended package/environment manager)
- Telegram bot token (from [@BotFather](https://t.me/botfather))
- At least one LLM API key:
  - `ZAI_API_KEY` (default LiteLLM path), or
  - `OPENROUTER_API_KEY` (if using OpenRouter)

If `uv` is not installed:

```bash
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 1. Clone

```bash
git clone <repo-url>
cd BroodMind
```

### 2. Install dependencies

Use the bootstrap script (recommended):

```bash
# macOS/Linux
bash scripts/bootstrap.sh
```

```powershell
# Windows PowerShell
powershell -ExecutionPolicy ByPass -File .\scripts\bootstrap.ps1
```

Or run `uv` manually:

```bash
uv sync
```

Alternative with `venv` + `pip`:

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
pip install -e .
```

### 3. Run interactive configuration

```bash
# If using uv
uv run broodmind configure

# If using venv/pip
broodmind configure
```

`configure` handles:

- Creating/updating `.env` (manual `.env` copy is not required)
- Prompting for required credentials/settings
- Bootstrapping workspace files from `workspace_templates/` (copy-if-missing)

On first generation, it creates:

- `workspace/AGENTS.md` (pre-populated default guidance)
- `workspace/MEMORY.md` (starter content)
- `workspace/SOUL.md` (blank)
- `workspace/USER.md` (blank)
- `workspace/HEARTBEAT.md` (blank)
- `workspace/memory/canon/facts.md`
- `workspace/memory/canon/decisions.md`
- `workspace/memory/canon/failures.md`

### 4. Start BroodMind

```bash
# Background mode (default)
uv run broodmind start

# Foreground mode
uv run broodmind start --foreground
```

Equivalent without `uv`:

```bash
broodmind start
broodmind start --foreground
```

### 5. Verify status/logs

```bash
uv run broodmind status
uv run broodmind logs --follow
```

## Runtime Model

### Queen

- Handles Telegram messages
- Builds prompt context from memory + workspace files
- Uses a planner/executor split per turn (brief plan first, then execution)
- Runs a verification pass before final user-facing responses
- Calls tools directly or delegates to workers
- Synthesizes final user-facing responses
- Supports self-directed context compaction/reset via `queen_context_reset` with structured handoff

#### Context Reset Flow

- Tool: `queen_context_reset` (`mode=soft|hard`)
- Persists handoff and reset history:
  - `workspace/memory/handoff.md`
  - `workspace/memory/handoff.json`
  - `workspace/memory/context-audit.md`
  - `workspace/memory/context-audit.jsonl`
- `soft` reset: clears chat memory context and continues with bootstrap + wake-up handoff
- `hard` reset: same as soft + resets stored bootstrap hash for chat state
- Guardrails:
  - `hard` requires `confirm=true`
  - low handoff confidence (`<0.7`) requires `confirm=true`
  - repeated resets without progress trigger confirmation (`N=2`)
- Decision thresholds (for proactive reset):
  - `WATCH` if any one rises: `context_size_estimate >= 90000`, `repetition_score >= 0.70`, `error_streak >= 4`, `no_progress_turns >= 6`
  - `RESET_SOON` if any one is severe: `context_size_estimate >= 150000`, `repetition_score >= 0.82`, `error_streak >= 7`, `no_progress_turns >= 10`
  - also `RESET_SOON` when 2+ WATCH signals persist across heartbeats
- After reset, the next turn gets a wake-up directive to choose mode: `continue / clarify / replan`
- Heartbeat now includes context-health metrics:
  - `context_size_estimate`, `repetition_score`, `error_streak`, `no_progress_turns`, `resets_since_progress`, `overload_score`
  - available directly as `context_health` in `check_schedule` JSON

### Workers

- Worker templates are file-based under `workspace/workers/<id>/worker.json`
- Spawned per task with timeout + lifecycle tracking
- Includes automatic recovery retries for transient stuck/failure states
- Report result back to Queen runtime
- Worker templates can opt in to child spawning with strict policy controls:
  - `can_spawn_children` (default `false`)
  - `allowed_child_templates` (explicit whitelist)
- Child-worker runs include lineage metadata:
  - `lineage_id`, `parent_worker_id`, `root_task_id`, `spawn_depth`
- On parent worker failure, orphan child workers are automatically stopped

### Tools

Examples:

- Filesystem: `fs_read`, `fs_write`, `fs_list`, `fs_move`, `fs_delete`
- Web: `web_search`, `web_fetch`
- Execution: `exec_run`
- Worker management: `list_workers`, `start_worker`, `start_child_worker`, `start_workers_parallel`, `synthesize_worker_results`, `get_worker_status`, `get_worker_result`
- Self-management: `queen_context_health`, `queen_context_reset`, `self_control`

`start_worker` supports worker specialization routing:

- pass `worker_id="auto"` (or omit `worker_id`) to let the router pick the best template
- optionally constrain routing with `required_tools` and `required_permissions`

`start_child_worker` is for worker-context delegation with policy enforcement:

- parent template must set `can_spawn_children=true`
- child template must be included in parent `allowed_child_templates`
- child permissions must be a subset of parent permissions
- queen-level spawn limits are enforced globally per lineage

## CLI Commands

```bash
# Setup
uv run broodmind configure

# Lifecycle
uv run broodmind start
uv run broodmind start --foreground
uv run broodmind stop
uv run broodmind restart
uv run broodmind status

# Logs / dashboard
uv run broodmind logs --follow
uv run broodmind dashboard
uv run broodmind dashboard --watch

# Gateway
uv run broodmind gateway

# Worker templates
uv run broodmind sync-worker-templates
uv run broodmind sync-worker-templates --overwrite

# Memory
uv run broodmind memory stats
uv run broodmind memory cleanup --dry-run
uv run broodmind memory cleanup --keep-days 30 --keep-count 1000
```

## Configuration

Primary settings are stored in `.env` and loaded via `pydantic-settings`.

### Common variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot token |
| `ALLOWED_TELEGRAM_CHAT_IDS` | Yes (recommended) | Comma-separated allowed chat IDs |
| `BROODMIND_LLM_PROVIDER` | No | `litellm` (default) or `openrouter` |
| `ZAI_API_KEY` | Conditionally | Required for default LiteLLM/z.ai path |
| `OPENROUTER_API_KEY` | Conditionally | Required if provider is `openrouter` |
| `OPENROUTER_BASE_URL` | No | OpenRouter endpoint (default: `https://openrouter.ai/api/v1`) |
| `OPENROUTER_MODEL` | No | Defaults to `anthropic/claude-sonnet-4` |
| `BRAVE_API_KEY` | No | Enables `web_search` |
| `FIRECRAWL_API_KEY` | No | Enables Firecrawl-backed HTML fetch in `web_fetch` |
| `OPENAI_API_KEY` | No | Enables embedding-based semantic memory |
| `BROODMIND_MEMORY_OWNER_ID` | No | Memory namespace key (default: `default`) |
| `BROODMIND_MEMORY_PREFILTER_K` | No | Lexical prefilter candidate count before vector rerank (default: `80`) |
| `BROODMIND_WORKSPACE_DIR` | No | Defaults to `workspace` |
| `BROODMIND_STATE_DIR` | No | Defaults to `data` |
| `BROODMIND_DASHBOARD_TOKEN` | No (recommended) | Token required by `/api/dashboard/*` when set |
| `BROODMIND_TAILSCALE_IPS` | No | Optional comma-separated Tailscale IP allowlist for gateway hints |
| `BROODMIND_TAILSCALE_AUTO_SERVE` | No | `1` (default): auto-run `tailscale serve` on startup when available |
| `BROODMIND_HEARTBEAT_INTERVAL_SECONDS` | No | Bot heartbeat interval (default: `900`) |
| `BROODMIND_TELEGRAM_PARSE_MODE` | No | `MarkdownV2` (default), `HTML`, `Markdown`, or empty for plain text |
| `BROODMIND_WORKER_LAUNCHER` | No | `same_env` (default) or `docker` |
| `BROODMIND_WORKER_MAX_SPAWN_DEPTH` | No | Max child spawn depth (default: `2`) |
| `BROODMIND_WORKER_MAX_CHILDREN_TOTAL` | No | Max children per lineage lifetime (default: `20`) |
| `BROODMIND_WORKER_MAX_CHILDREN_CONCURRENT` | No | Max concurrent children per lineage (default: `10`) |

## Optional: Docker Worker Launcher

Local non-dockerized runtime is the default and recommended for first setup.

If you want workers to run in Docker containers:

1. Build worker image:

```bash
uv run broodmind build-worker-image --tag broodmind-worker:latest
```

2. Set in config:

```env
BROODMIND_WORKER_LAUNCHER=docker
BROODMIND_WORKER_DOCKER_IMAGE=broodmind-worker:latest
```

3. Restart BroodMind.

## Private Web Dashboard (Tailscale)

For Dashboard + WebSocket clients together, run full BroodMind runtime:

```bash
uv run broodmind start
```

Use `uv run broodmind gateway` only for HTTP gateway-only scenarios (without Telegram/Queen runtime).

Run gateway-only:

```bash
uv run broodmind gateway
```

Open:

- `http://127.0.0.1:8000/dashboard`
- API snapshot: `http://127.0.0.1:8000/api/dashboard/snapshot`

Recommended private setup:

1. Bind local-only:
   `BROODMIND_GATEWAY_HOST=127.0.0.1`
2. Set auth token:
   `BROODMIND_DASHBOARD_TOKEN=<strong-random-token>`
3. Publish only to tailnet via `tailscale serve` (no Funnel).

By default, BroodMind attempts to auto-configure `tailscale serve` at startup and repair stale proxy mappings to the configured gateway port (best effort, skipped if unavailable). Disable with:

`BROODMIND_TAILSCALE_AUTO_SERVE=0`

## Development

### Install dev dependencies

```bash
uv sync --dev
```

### Run checks

```bash
uv run ruff check src tests
uv run pytest -q
```

### Project structure

```text
src/broodmind/
├── cli/
├── config/
├── gateway/
├── intents/
├── memory/
├── policy/
├── providers/
├── queen/
├── store/
├── telegram/
├── tools/
├── worker_sdk/
└── workers/
```

## Troubleshooting

### Bot starts but no Telegram replies

- Check `TELEGRAM_BOT_TOKEN`
- Check `ALLOWED_TELEGRAM_CHAT_IDS` includes your chat ID
- Verify with `broodmind status` and logs

### LLM errors

- Ensure provider/key pairing is correct:
  - `BROODMIND_LLM_PROVIDER=litellm` + `ZAI_API_KEY`, or
  - `BROODMIND_LLM_PROVIDER=openrouter` + `OPENROUTER_API_KEY`

### Web search not working

- Add `BRAVE_API_KEY`

### Semantic memory missing

- Add `OPENAI_API_KEY`

## License

MIT License (see repository license file).
