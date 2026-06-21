<p align="center">
   <img src="https://github.com/pmbstyle/OctoPal/blob/main/logo.png?raw=true" alt="Octapal"/>
</p>

<p align="center">
  <strong>A safer personal AI agent that can actually get work done</strong>
</p>

<p align="center">
   <img src="https://img.shields.io/github/v/release/pmbstyle/Octopal">
   <a href="LICENSE"><img src="https://img.shields.io/github/license/pmbstyle/Octopal?svg=true"></a>
   <a href="https://deepwiki.com/pmbstyle/Octopal"><img src="https://deepwiki.com/badge.svg"></a>
</p>

➡️ <em>AI agents are cool</em>

➡️ <em>AI agents are productive</em>

➡️ <em>AI agents makes your life easier</em>

➡️ <em>AI agents are not safe and trustworthy ... here is where Octopal comes in</em>


## 🐙 Why Octopal

Octopal is your local autonomous AI agent - Octo, who can live on your computer or on a server. You can communicate with Octo through Telegram, WhatsApp, or a dedicated desktop app.

Octopal is built to perform any task, run scheduled activities, host automation pipelines, research the web, work with API, MCP, A2A, skills, and much more.

The core Octopal idea is to have an AI agent that you can trust. 

Octo, as the main reasoning agent, lives inside your system and dedicates outside-world communication to specialized workers (sub-agents). Octo can create new workers or use predefined ones. Workers are short-lived disposable agents that spawn only to achieve a particular task goal. They live in isolated environments (docker) and don't hold system / private context. After the worker finishes their task, their instance gets deleted.

This architecture prevents context poisoning, prompt injections, and hostile environment actions. Even if a worker installs and runs the miliscues code, it will stay inside its container and will not hurt the main system.

**give the agent real hands, but do not hand it your whole house.**

## 🚀 Quick Start

### Desktop App

Download the latest desktop build from the [Octopal releases page](https://github.com/pmbstyle/Octopal/releases/latest).

<img alt="Octopal Desktop" src="https://github.com/user-attachments/assets/a995f7ab-28a6-45ee-b63a-2a7c45dedd3b" />

<!-- STABLE_DOWNLOADS -->
| Platform | Download |
|----------|----------|
| **Windows** | [Octopal-Desktop-2026.6.11-win-x64.exe](https://github.com/pmbstyle/Octopal/releases/download/v2026.06.11/Octopal-Desktop-2026.6.11-win-x64.exe) |
| **macOS Intel** | [Octopal-Desktop-2026.6.11-mac-x64.dmg](https://github.com/pmbstyle/Octopal/releases/download/v2026.06.11/Octopal-Desktop-2026.6.11-mac-x64.dmg) |
| **macOS Apple Silicon** | [Octopal-Desktop-2026.6.11-mac-arm64.dmg](https://github.com/pmbstyle/Octopal/releases/download/v2026.06.11/Octopal-Desktop-2026.6.11-mac-arm64.dmg) |
| **Linux** | [Octopal-Desktop-2026.6.11-linux-x86_64.AppImage](https://github.com/pmbstyle/Octopal/releases/download/v2026.06.11/Octopal-Desktop-2026.6.11-linux-x86_64.AppImage) |
<!-- STABLE_DOWNLOADS_END -->


### Server Install

The installer scripts live in this repository under [`scripts/`](scripts/) so you can review them before running.

macOS / Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/pmbstyle/Octopal/main/scripts/octopal.sh | bash
```

Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/pmbstyle/Octopal/main/scripts/octopal.ps1 | iex
```

Then configure and start:

```bash
uv run octopal configure
uv run octopal start
```

Open the dashboard:

```text
http://127.0.0.1:8000/dashboard
```

If you enabled dashboard protection, use the `gateway.dashboard_token` value from `config.json`.

### Manual Install

```bash
git clone https://github.com/pmbstyle/Octopal.git
cd Octopal
uv sync
uv run octopal configure
uv run octopal start
```

Without `uv`:

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -e .
octopal configure
octopal start
```

<img alt="Octopal dashboard" src="https://github.com/user-attachments/assets/55360901-a319-4c8c-932b-df3c519da375" />


## 🤖 Model Providers

Octopal lets you choose one model provider for Octo and, when useful, a different provider for workers. That makes it easy to keep the coordinator on the model you trust most while sending cheaper, faster, or more specialized tasks to worker agents.

Supported provider catalog:

- **Frontier APIs:** OpenAI, Anthropic, Google Gemini, Mistral, OpenRouter, Together AI, Groq
- **Subscription and plan-based routes:** Codex, Z.ai, Minimax.
- **Local and custom routes:** Ollama, Custom OpenAI-compatible.

## What You Can Use Octopal For

| Use case | What Octopal does |
|:--|:--|
| Personal operator | Runs from Telegram, WhatsApp, or a private WebSocket/dashboard client. |
| Research | Delegates browsing, fetching, synthesis, and source-heavy work to workers. |
| Coding and repo work | Uses code workers, test runners, repo researchers, release managers, and bug investigators. |
| Agent-to-agent teamwork | Connects trusted peer agents over A2A so autonomous systems can ask, answer, and split work together. |
| Recurring routines | Schedules background reports, checks, reminders, and operational tasks. |
| Memory-heavy work | Keeps conversation history, canon, user preferences, identity files, and durable project context. |
| Tool orchestration | Uses MCP servers, skills, shell, filesystem, web, browser/search, and connectors under policy controls. |
| Private operations | Exposes a token-protected dashboard and optional Tailscale access instead of requiring public ports. |

Example:

```text
You:
Research the latest Gemini model changes and tell me what matters for our agent stack.

Octo:
1. Spawns a web researcher with scoped tools.
2. The researcher gathers sources and writes a compact summary.
3. Octo stores the durable takeaways in memory.
4. You get the answer without keeping a browser/tool worker alive forever.
```

Another example:

```text
You:
Every Friday, review the repo history, summarize what changed, and flag README drift.

Octo:
1. Creates a scheduled task.
2. Runs a repo-focused worker on the schedule.
3. Sends the report back through your chosen channel.
4. Keeps the routine visible in the dashboard.
```

## 💬 Agent-To-Agent Collaboration

Octo can communicate with other instances of Octopal or other agents via A2A.

That turns separate agents into a working team:

- Several Octopal instances can cooperate as a private agent network.
- Other agents and coding runtimes, such as Codex, Claude Code, Gemini, and similar tools, can connect as trusted peers.
- One agent can ask Octopal to use its memory, workers, tools, or local context.
- Octopal can receive peer messages, route them through local policy, and answer from the same coordinator/worker runtime it uses for you.
- Specialized agents can hand off work to each other instead of duplicating every connector, skill, and long-running routine.
- Multi-agent workflows can stay autonomous while still keeping clear trust boundaries around which peers are allowed to call in.

The current A2A integration is intentionally private by default. Peers are configured explicitly, authenticate with bearer tokens, discover Octopal through its agent card, and exchange text plus explicitly allowed structured JSON, file URL, and raw file parts over the A2A HTTP+JSON interface.

See [docs/a2a_setup.md](docs/a2a_setup.md) for a step-by-step private Tailscale setup.

## 🧩 How It Compares

Octopal lives in the same neighborhood as OpenClaw, Hermes Agent, NanoClaw, OpenHands, and other autonomous agent runtimes. The difference is the default posture.

| Project style | Strong idea | Tradeoff Octopal is designed around |
|:--|:--|:--|
| OpenClaw-style personal assistants | Broad channel coverage, always-on local assistant, polished ecosystem. | Octopal makes Docker workers and scoped execution the default mental model instead of treating host execution as the comfortable path. |
| Hermes-style self-improving agents | Strong memory, skills, model/provider flexibility, and a clear "agent that learns you" story. | Octopal focuses on separating the thinking loop from the side-effect loop, so sensitive memory and risky tools do not have to live in the same execution boundary. |
| NanoClaw-style minimal agents | Small, understandable, container-first design. | Octopal chooses a larger integrated runtime: dashboard, channels, worker templates, MCP, skills, memory, connectors, scheduled tasks, and operational controls in one place. |
| Coding-agent sandboxes | Great for repo work and software tasks. | Octopal is meant to be a persistent personal/operator runtime, not only an IDE-adjacent coding agent. |

If you want the smallest possible codebase, NanoClaw may feel cleaner. If you want the broadest social surface, OpenClaw is impressive. If you want a memory-and-skills-first agent, Hermes has a strong pitch.

Octopal is for the person who wants an always-reachable AI operator with a serious execution boundary, visible operations, and enough built-in runtime to trust it with real ongoing work.

## 🏗️ Architecture

```text
User
  |
Channels: Telegram / WhatsApp / WebSocket
  |
Octo coordinator
  |
Worker pool
  |
Tools / MCP / Skills / Connectors
  |
External systems
```

Octo is the long-lived coordinator. It owns the conversation, memory, policy, routing, and high-level decisions.

Workers are the execution layer. They run tools, touch files, browse, call MCP servers, use skills, and return structured results, questions, or errors. Workers can also spawn subworkers for multi-step tasks when their policy allows it.

Prebuilt worker templates include:

- Web Researcher
- Web Fetcher
- Data Analyst
- Code Worker
- Writer
- DevOps / Release Manager
- Security Auditor
- Test Runner
- System Self-Controller
- DB Maintainer
- Repo Researcher
- Bug Investigator

## 🔒 Security Model

Octopal is designed around a coordinator/worker split:

- Docker is the default worker launcher.
- Workers get their own scratch workspace.
- Access to Octo's main workspace is explicit through `allowed_paths`.
- Worker environments are disposable.
- Dashboard and gateway APIs can be protected with a token.
- Tailscale integration can expose the dashboard privately without opening public ports.

Docker is not just packaging here. It is the default execution boundary for work that may touch untrusted inputs: websites, generated code, third-party scripts, documents, shell commands, and tool outputs.

If Docker CLI or the Docker daemon is unavailable, or the worker image cannot be built, Octopal can temporarily fall back to `same_env` and surface the reason in `octopal status` and the dashboard. That fallback exists so the system remains usable; it is not the recommended security posture.

Build the worker image manually when needed:

```bash
uv run octopal build-worker-image --tag octopal-worker:latest
```

Configure the launcher:

```json
{
  "workers": {
    "launcher": "docker",
    "docker_image": "octopal-worker:latest"
  }
}
```

Restart after config changes:

```bash
uv run octopal restart
```

See [docs/worker_isolation.md](docs/worker_isolation.md) for the detailed worker isolation model.

## 🧠 Memory That Feels Useful

Octopal keeps memory in layers so the assistant can stay personal without turning every prompt into a junk drawer:

- `MEMORY.md` keeps working memory and durable notes.
- `memory/canon/` stores reviewed long-term knowledge.
- `USER.md` stores user preferences and interaction style.
- `SOUL.md` stores assistant identity, tone, and behavioral principles.
- `HEARTBEAT.md` stores recurring duties, monitoring loops, and background obligations.

See [docs/memory.md](docs/memory.md).

## 📡 Channels And Dashboard

Octopal can talk through:

- Telegram
- WhatsApp
- Desktop app

Channels support text, image/file attachments, reactions where supported, and a short grace window so follow-up messages can be folded into the same turn before Octo starts executing.

The dashboard gives you a live operational surface:

- Octo status and gateway health
- active workers and worker history
- communication logs
- dashboard actions
- token-based access protection
- optional Tailscale-aware private access

Run:

```bash
uv run octopal dashboard --watch
```

or open:

```text
http://127.0.0.1:8000/dashboard
```

If the dashboard bundle is missing:

```bash
cd webapp
npm install
npm run build
```

Then set:

```json
{
  "gateway": {
    "webapp_enabled": true
  }
}
```

## 🦾 Skills And Connectors

Octopal supports workspace-local skill bundles under `workspace/skills/<skill-id>/`.

Skills can include:

- `SKILL.md` instructions
- scripts
- references
- assets
- readiness checks
- isolated Python or JS/TS runtime environments

Common commands:

```bash
uv run octopal skill list
uv run octopal skill install <publisher>/<skill-pack>
uv run octopal skill update <skill-id>
uv run octopal skill verify <skill-id>
uv run octopal skill disable <skill-id>
uv run octopal skill enable <skill-id>
uv run octopal skill trust <skill-id>
uv run octopal skill remove <skill-id>
```

See [docs/skills.md](docs/skills.md).

Connectors are the integration layer for external services. The current connector surface includes Google services and GitHub-oriented workflows, with more integration work happening around the runtime.

See [docs/connectors.md](docs/connectors.md).

## 📋 Core Commands

```bash
uv run octopal configure
uv run octopal start
uv run octopal start --foreground
uv run octopal stop
uv run octopal restart
uv run octopal status
uv run octopal update
uv run octopal logs --follow
```

WhatsApp setup:

```bash
uv run octopal whatsapp install-bridge
uv run octopal whatsapp link
uv run octopal whatsapp status
```

Worker templates:

```bash
uv run octopal sync-worker-templates --overwrite
```

Memory maintenance:

```bash
uv run octopal memory stats
uv run octopal memory cleanup --dry-run
```

## 💻 Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run black --check .
uv run mypy src
```

Build the dashboard manually:

```bash
cd webapp
npm install
npm run build
```

GitHub releases use date-based versions in `src/octopal/_version.py` and tags like `vYYYY.MM.DD` or `vYYYY.MM.DD.N`.

## ⁉️ Troubleshooting

### Telegram bot starts but does not reply

- Verify `telegram.bot_token` in `config.json`.
- Verify your chat ID is listed in `telegram.allowed_chat_ids`.
- Check `uv run octopal status`.
- Tail logs with `uv run octopal logs --follow`.

### WhatsApp is selected but messages do not arrive

- Verify `user_channel` is set to `whatsapp` in `config.json`.
- Verify your phone number is listed in `whatsapp.allowed_numbers`.
- Run `uv run octopal whatsapp install-bridge`.
- Run `uv run octopal whatsapp link`.
- Start Octopal again and check `uv run octopal whatsapp status`.

### LLM errors

- Run `uv run octopal configure` and pick the provider you want.
- Check `llm.provider_id`, `llm.model`, `llm.api_key`, and `llm.api_base` in `config.json`.

### Web search or fetch issues

Add the preferred search provider key in `config.json`:

```json
{
  "search": {
    "brave_api_key": null,
    "firecrawl_api_key": null
  }
}
```

## License

MIT. See [LICENSE](LICENSE).
