<p align="center">
   <img src="https://github.com/pmbstyle/OctoPal/blob/main/logo.png?raw=true" width="500" alt="Octapal"/>
</p>

<p align="center">
  <strong>SECURE MULTI-AGENT EXECUTION RUNTIME</strong>
</p>

Octopal is a local AI runtime that executes autonomous agents in isolated environments.

It acts as a persistent operator that plans work, spawns specialized workers, and runs tasks on your behalf — without exposing your system to risk.

The **Octo** is the long-running coordinator: it holds memory, plans work, chooses tools, and delegates execution.  
**Workers** are short-lived specialists with bounded context, strict permissions, and isolated execution.

This architecture enforces a hard boundary between reasoning and execution:
the Octo never performs external actions directly — all side effects happen inside controlled worker environments.

## 🔒 Secure by Default Execution

Octopal is designed with a strict isolation model for all workers.

- Workers run in ephemeral Docker containers by default
- No access to your system or workspace unless explicitly granted
- Filesystem access is restricted via allowlisted paths
- Environment variables and secrets are never exposed to workers
- Each execution is sandboxed and fully disposable

Even when interacting with untrusted content (web pages, scripts, external tools), workers operate in a controlled environment that prevents system compromise.

### Why this matters

Modern AI agents frequently interact with untrusted data (web pages, APIs, generated code).
Without isolation, a simple tool call (e.g. shell execution or remote fetch) can expose your system.

Octopal prevents this by design:
workers cannot access your system, secrets, or filesystem unless explicitly allowed.

## 🪛 What It Can Do

- Run as a persistent AI operator over Telegram or WhatsApp
- Plan work and delegate tasks to specialized workers
- Execute filesystem, web, browser, shell, and MCP tools under policy controls
- Create and reuse worker templates, MCP server connections, and `SKILL.md`-based skills
- Maintain persistent memory, canon, and user/system identity files
- Monitor context health and trigger structured context resets when needed
- Schedule recurring tasks and background routines
- Expose a private gateway and dashboard for status, workers, and system visibility
- A set of canonical memory files shapes the system environment


```
User
   │
Channels (Telegram / WhatsApp / WS)
   │
 Octo
   │
 Worker Pool
   │
 Tools / MCP / Skills
   │
 External Systems
```

**Example workflow:**

User:
"Research the latest Gemini model and write a summary."

Octo:
1. Spawns Web Researcher
2. Researcher fetches sources
3. Writer worker generates a summary
4. Octo stores canon entry
5. Result returned to the user

## 🚀 Quick Start

### Install with one line

```bash
# macOS/Linux
curl -fsSL https://octopal.ca/octopal.sh | bash
```
```powershell
# Windows PowerShell
irm https://octopal.ca/octopal.ps1 | iex
```

### 1. Install from git

- Python 3.12+
- `uv` (recommended)
- Node 20+ for web ui 
- One user channel:
  Telegram bot token from [@BotFather](https://t.me/botfather), or
  WhatsApp Web linking via QR
- Bring your own LLM API key:
  OpenRouter, OpenAI, Anthropic, Google Gemini, Mistral AI, Together AI, Groq, Z.ai, Custom OpenAI-compatible, Ollama
- [Tailscale](https://tailscale.com/) (optional) if you want to access the dashboard remotely or connect via Websocket client

Install `uv` if needed:

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```powershell
# Windows PowerShell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Bootstrap script

```bash
git clone https://github.com/pmbstyle/Octopal.git
cd Octopal
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

This is the main starting path. The bootstrap script installs dependencies, installs Playwright Chromium, and then launches `octopal configure`.

### 3. Open the web dashboard

After bootstrap, start Octopal and then open the dashboard in your browser:

```bash
uv run octopal start
```

Open [http://127.0.0.1:8001/dashboard](http://127.0.0.1:8001/dashboard) (change to Tailscale IP for remote access)

If you enabled dashboard protection during `octopal configure`, use the `gateway.dashboard_token` value from `config.json` when the dashboard or dashboard API asks for it.

If the page says the dashboard is unavailable, build and enable the web app first:

```bash
cd webapp
npm run build
```

Then enable the dashboard bundle in `config.json` by setting `"gateway": { "webapp_enabled": true }` and start Octopal again.

<img alt="Octopal dashboard" src="https://github.com/user-attachments/assets/0fcf993b-97c6-4f90-840a-63011f0d55f0" />


### 4. Manual setup

If you do not want the bootstrap script, use the manual path below.

```bash
git clone https://github.com/pmbstyle/Octopal.git
cd Octopal
uv sync
uv run octopal configure
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
octopal configure
```

`configure` creates or updates `config.json` and bootstraps workspace files if missing.

### Configuration model

`config.json` is now the primary configuration file.

- `uv run octopal configure` writes the structured config there.
- Runtime loads `config.json` first and maps it into legacy settings for older code paths.
- If both files exist, `config.json` wins for overlapping settings.

In practice: use the wizard and treat `.env.example` as a compatibility reference, not the main setup path.

### 5. Start

```bash
# background mode
uv run octopal start

# foreground mode
uv run octopal start --foreground
```

## Core Commands

```bash
uv run octopal start
uv run octopal stop
uv run octopal restart
uv run octopal status
uv run octopal logs --f
```

## Docker Worker Launcher

Docker workers are the default and recommended runtime. You can build the worker image up front:

```bash
uv run octopal build-worker-image --tag octopal-worker:latest
```

Then set in `config.json`:

```json
{
  "workers": {
    "launcher": "docker",
    "docker_image": "octopal-worker:latest"
  }
}
```

Restart Octopal after config changes.

If Docker CLI and the Docker daemon are available but the configured worker image is missing, Octopal will try to build it automatically on startup. If Docker is unavailable or the automatic build fails, Octopal will temporarily fall back to `same_env` and surface the reason in `octopal status` and the dashboard.

Workers keep their own scratch workspace by default. To share files from Octo's main workspace with a worker, pass explicit `allowed_paths`; if `allowed_paths` is omitted, the worker does not get broad workspace access.

## Optional: WhatsApp setup

After you configure your WhatsApp number in the config link Octopal as a new device

```bash
uv run octopal whatsapp link
```


## ✨ Key Features

### 💻 Local and Cloud deployment

Octopal can work from any environment that supports Python execution.
Fast and simple bootstrap onboarding helps you to start using Octopal right away.

- deploy on your local PC (Linux, Windows, MacOS)
- deploy on a VPS
- deploy in Docker

Octopal works from a specified directory and has no access to your system components.

### 🧠 Delegation-driven architecture

Octo, which holds all system context and sensitive data, never communicates with the outside world on its own.
Instead, the Octo delegates tasks to workers with limited context and predefined tool/skill sets.
Workers can spawn subworkers for multi-step tasks. Workers can only return response of their tasks or question/error responses. 

- Octo delegates external operations to workers, which ensures context isolation, enhances security, and provides async task execution
- workers execute in an isolated environment, which gets deleted after each execution
- workers can act as orchestrators and create sub-workers for multi-tasking
- workers operate with a predefined set of tools, MCP, and skills in their config as well as `max_thinking_steps` and `execution_timeout`
- the Octo can create new workers for a specific task (ex. use a skill to work with an external resource)
- Prebuilt worker templates include:
  - Web Researcher
  - Web Fetcher
  - Data Analyst
  - Code Worker
  - Writer
  - DevOps / Release Manager
  - Security Auditor
  - Test Runner
  - System Self-Controller

### 📃 Multilayer memory system

Octo operates with a local vector database to store communication history and file-based context:

- **MEMORY.md** – working memory and durable context; important facts, current state, and notes the system may need across sessions
- **memory/canon/** – curated long-term knowledge that has been reviewed and promoted into trusted reference material
- **USER.md** – user profile, preferences, habits, and interaction style
- **SOUL.md** – system identity, values, tone, and core behavioral principles
- **HEARTBEAT.md** – recurring duties, monitoring loops, schedules, and background obligations

### 🤖 Multi-channel user communication

Octopal supports:
- Telegram (Botfather)
- WhatsApp (Dedicated or personal numbers)
- WS API gateway (Build or bring your own client)

Communication channels, by default, provide full support of functions like:
- text communication
- image attachments
- message reactions
- 5s grace window for user messages:

  You can send a followback message before the Octo executes it - this helps to prevent typos, wrong commands, etc.

### ⚙️ Web dashboard

The Dashboard provides a real-time, comprehensive view of the system's state, active workers, and communication logs. It is built as a modern Vite + React web application.

- **Secure by default:** Built-in token-based authentication and optional Tailscale integration.
- **Real-time updates:** Uses WebSockets for live streaming of agent thoughts and tool executions.
- **Terminal mode:** Access a live view directly from your CLI via `octopal dashboard --watch`.

### 🔒 Remote Access & Security (Tailscale)

Octopal features first-class integration with **Tailscale** to provide secure remote access without opening ports or configuring complex firewalls:

- **Automatic Tunneling:** If Tailscale is installed, Octopal can automatically run `tailscale serve` to expose the gateway to your private tailnet.
- **IP-Based Authorization:** The WebSocket and Dashboard APIs automatically verify that incoming connections originate from trusted Tailscale nodes or your local machine.
- **Easy Configuration:** Managed via `config.json` in the `gateway` section.

```json
{
  "gateway": {
    "tailscale_auto_serve": true,
    "tailscale_ips": "100.x.y.z,100.a.b.c"
  }
}
```

### 🧩 Skills and skill bundles

Octopal supports workspace-local skill bundles under `workspace/skills/<skill-id>/`.

- auto-discovers `SKILL.md` bundles
- keeps `skills/registry.json` as a compatibility layer
- supports optional `scripts/`, `references/`, and `assets/`
- exposes readiness checks for required binaries and env vars
- runs bundled scripts through a dedicated safe runner instead of raw shell
- can install external skills with ClawHub-style commands like `uv run octopal skill install <publisher>/<skill-pack>`
- also accepts direct `SKILL.md` URLs and local bundle paths
- supports installer lifecycle commands: `skill install`, `skill list`, `skill update`, `skill trust`, `skill untrust`, `skill remove`
- shows both local workspace skills and installer-managed skills in `skill list`
- requires isolated per-skill runtime envs for Python and JS/TS script-backed skills
- auto-verifies imported scripts and auto-prepares isolated envs during install/update when possible

See [docs/skills.md](docs/skills.md) for the current format and behavior.

### 🛜 Connectors (experimental)

Connectors are the integration layer between Octopal and external services.

Currently available `Google` connector with `Gmail`(read-only).

See [docs/connectors.md](docs/connectors.md) for more info.

## Troubleshooting

### Telegram bot starts but does not reply

- Verify `telegram.bot_token` in `config.json`
- Verify your chat ID is listed in `telegram.allowed_chat_ids`
- Check `uv run octopal status` and `uv run octopal logs --follow`

### WhatsApp is selected, but not receiving messages

- Verify `user_channel` is set to `whatsapp` in `config.json`
- Verify your phone number is listed in `whatsapp.allowed_numbers`
- Run `uv run octopal whatsapp install-bridge`
- Run `uv run octopal whatsapp link`
- Start Octopal again and check `uv run octopal whatsapp status`

### LLM errors

- Run `uv run octopal configure` and pick the provider you want to use.
- In your config file, check `llm.provider_id`, `llm.model`, and `llm.api_key` in `config.json`.

### Web search/fetch issues

Add the preferred search engine API key in your `config.json`

```json
"search": {
    "brave_api_key": null,
    "firecrawl_api_key": null
},
```

