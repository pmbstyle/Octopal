<p align="center"><img alt="logo" src="https://github.com/user-attachments/assets/75aca977-9475-471a-bfec-78822aa9fd98" /></p>


# BroodMind

A distributed AI agent orchestration platform built on a **Queen + Workers** architecture. The Queen coordinates multiple specialized workers to accomplish complex tasks through intelligent delegation, while maintaining context and memory across sessions.

## Architecture

### Core Components

**Queen (Orchestrator)**
- Interprets user intent and delegates to appropriate workers
- Manages conversation context and memory retrieval
- Handles tool usage: filesystem, worker management, status checking
- Maintains bootstrap context from workspace files (AGENTS.md, USER.md)
- Sends ready notifications on system initialization

**Workers (Specialized Agents)**
- Pre-defined agent templates with specific capabilities
- Tool access: web search, web fetch, filesystem operations
- Run in isolated Docker containers or same environment
- Timeout-based execution with automatic cleanup
- State tracking: pending, running, completed, failed

**Tool System**
- `web_search`: Brave Search API integration
- `web_fetch`: HTTP content retrieval with HTML parsing
- `fs_read/fs_write/fs_list/fs_move/fs_delete`: Workspace filesystem access
- `exec_run`: Shell command execution in workspace
- Permission-based access control (network, filesystem, exec)

**Memory & Context**
- Vector-based semantic memory with embeddings (OpenAI)
- Conversation history per chat ID
- Bootstrap context injection from workspace files
- Automatic context retrieval for relevant queries
- CLI commands for memory management and cleanup

**LLM Providers**
- **LiteLLM** (default): Supports 100+ providers through unified API
- **OpenRouter**: Access to Claude, GPT-4, and other models via OpenRouter
- Easy switching via `BROODMIND_LLM_PROVIDER` environment variable

**Communication**
- Telegram bot integration for user interaction
- Admin chat notifications for system events
- Message queuing with chunked responses
- Typing indicators for better UX

## Key Features

- **Intelligent Delegation**: Queen automatically delegates tasks to workers when beneficial
- **System Initialization**: Queen wakes up on every container restart, reads workspace files, and notifies ready users
- **Tool-Based Architecture**: Workers use declarative tools instead of ad-hoc code
- **Memory System**: Semantic search retrieves relevant context from past conversations
- **Memory Management**: CLI commands for cleanup and statistics
- **No Worker Spam**: Only Queen messages appear in Telegram, workers provide context only
- **Duplicate Prevention**: Automatic detection of duplicate worker tasks
- **Multi-Provider Support**: LiteLLM and OpenRouter with easy switching
- **Isolated Execution**: Workers run in Docker containers with workspace mounts
- **Permission Control**: Granular permissions for network, filesystem, and execution
- **Multi-User Support**: Configure allowed Telegram chat IDs for access control

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Telegram bot token (from [@BotFather](https://t.me/botfather))
- LLM provider API key (LiteLLM-compatible, OpenRouter, or OpenAI)
- Optional: Brave Search API key for web search
- Optional: OpenAI API key for embeddings

### Setup

1. **Clone and configure**
   ```bash
   git clone <repo-url>
   cd BroodMind
   cp .env.example .env
   ```

2. **Edit `.env` with your credentials**
   ```bash
   # Required
   TELEGRAM_BOT_TOKEN=your_bot_token_here

   # LLM Provider (choose one)
   BROODMIND_LLM_PROVIDER=litellm  # Options: litellm, openrouter

   # For LiteLLM provider (default)
   ZAI_API_KEY=your_provider_api_key

   # For OpenRouter provider
   OPENROUTER_API_KEY=your_openrouter_key
   OPENROUTER_MODEL=anthropic/claude-sonnet-4  # Optional: custom model

   # Optional - for web search
   BRAVE_API_KEY=your_brave_search_key

   # Optional - for semantic memory
   OPENAI_API_KEY=your_openai_key

   # Telegram users allowed to interact (comma-separated)
   # Get your chat ID from @userinfobot on Telegram
   ALLOWED_TELEGRAM_CHAT_IDS=123456789,987654321
   ```

3. **Start the system**
   ```bash
   docker compose up -d --build
   ```

4. **Check logs**
   ```bash
   docker logs broodmind-broodmind-1 -f
   ```

The Queen will initialize, read workspace files, and send "Queen ready. All systems operational." to all allowed Telegram users.

## CLI Commands

BroodMind includes a command-line interface for management tasks:

```bash
# Show system status
broodmind status

# View logs
broodmind logs -f  # Follow logs

# Worker management
broodmind workers list              # List all workers
broodmind audit list                 # List audit events
broodmind audit show <event_id>      # Show audit event details

# Memory management
broodmind memory stats               # Show memory/RAG statistics
broodmind memory cleanup             # Clean up old memory entries (30 days, keep 1000)
broodmind memory cleanup -d 7 -c 500  # Custom cleanup: keep 7 days, keep last 500
broodmind memory cleanup --dry-run   # Preview what would be deleted

# Start services
broodmind start                      # Start Telegram bot
broodmind stop                       # Stop running BroodMind process
broodmind restart                    # Restart BroodMind Queen
broodmind gateway                    # Start gateway server
```

### Memory Cleanup

The RAG memory system can grow over time. Use cleanup commands to manage it:

- `broodmind memory stats` - See total entries, breakdown by role, unique chats
- `broodmind memory cleanup -d <days> -c <count>` - Delete entries older than N days, but keep N most recent
- `--dry-run` flag - Preview deletions without executing

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | Yes | Bot token from @BotFather |
| `BROODMIND_LLM_PROVIDER` | No | LLM provider: `litellm` (default) or `openrouter` |
| `ZAI_API_KEY` | No* | LiteLLM-compatible API key (required if using litellm) |
| `OPENROUTER_API_KEY` | No* | OpenRouter API key (required if using openrouter) |
| `OPENROUTER_MODEL` | No | OpenRouter model (default: `anthropic/claude-sonnet-4`) |
| `ALLOWED_TELEGRAM_CHAT_IDS` | No | Comma-separated Telegram chat IDs |
| `BRAVE_API_KEY` | No | Brave Search API for web_search tool |
| `OPENAI_API_KEY` | No | OpenAI API for semantic memory |
| `BROODMIND_WORKSPACE_DIR` | No | Workspace directory (default: ./workspace) |
| `BROODMIND_STATE_DIR` | No | Database directory (default: ./data) |
| `BROODMIND_WORKER_LAUNCHER` | No | Worker launcher: `same_env` or `docker` |

*One of `ZAI_API_KEY` or `OPENROUTER_API_KEY` is required depending on your chosen provider.

### Workspace Files

- **`workspace/AGENTS.md`**: Documentation of available workers and their capabilities (injected into Queen's context)
- **`workspace/USER.md`**: User preferences and instructions (injected into Queen's context)

### Worker Templates

Workers are defined in the database and can be listed via the `list_workers` tool:

```
- web_fetcher: Web content retrieval
- web_researcher: Web search and analysis
- data_analyst: Data processing and analysis
- writer: Content creation
- coder: Code generation and filesystem operations
```

## Usage

### Basic Conversation

Message your Telegram bot:
```
You: What's the weather like?
Queen: I'll check that for you.
[start_worker call to web_fetcher]
Queen: Here's the weather information...
```

### Worker Delegation

The Queen automatically decides when to delegate:

```
You: Read this repo and tell me about it https://github.com/user/repo
Queen: I'll delegate this to a worker.
[start_worker(worker_id="web_fetcher", task="...")]
Queen: Here's what I found...
```

**Key improvements:**
- Worker results are stored in memory for context
- Only the Queen's messages appear in Telegram (no worker spam)
- Duplicate worker detection prevents redundant tasks
- Queen synthesizes worker results into coherent responses

### Tool Usage

Workers use tools declaratively:
```
Worker: Using tool: web_fetch
[Tool executes, returns result]
Worker: Using tool: fs_read
[Tool executes, returns result]
Worker: Task completed with summary
```

## System Lifecycle

### Initialization (Every Container Start)

1. Queen wakes up and reads workspace files (AGENTS.md, USER.md)
2. Builds internal understanding of available workers
3. Sends "Queen ready. All systems operational." to allowed Telegram users
4. Ready to handle incoming messages

### Message Handling

1. User sends message via Telegram
2. Queen interprets intent with memory and bootstrap context
3. Queen may:
   - Reply directly (simple queries)
   - Delegate to worker async (complex tasks)
   - Ask for approval (risky operations)
4. Response sent back to user

### Worker Execution

1. Queen calls `start_worker` with worker_id, task, inputs
2. WorkerRuntime launches worker in container/same_env
3. Worker executes with tools, returns `WorkerResult`
4. Result stored in memory (context for Queen)
5. Queen synthesizes response based on worker results
6. Worker directory cleaned up

**Note:** Workers no longer send messages directly to Telegram. All communication goes through the Queen for a unified conversation experience.

## Development

### Project Structure

```
src/broodmind/
├── config/         # Settings and configuration
├── memory/         # Semantic memory service
├── policy/         # Permission engine
├── providers/      # LLM providers (LiteLLM)
├── queen/          # Queen orchestrator logic
├── store/          # SQLite database layer
├── telegram/       # Telegram bot integration
├── tools/          # Tool implementations
├── workers/        # Worker runtime and templates
└── worker_sdk/     # Worker SDK for task execution
```

### Adding a New Tool

1. Implement tool function in `src/broodmind/tools/`
2. Register in `src/broodmind/tools/tools.py`
3. Add permission check if needed
4. Update worker templates to include tool

### Adding a New Worker

1. Create worker spec with:
   - `worker_id`: Unique identifier
   - `system_prompt`: Worker's purpose and behavior
   - `available_tools`: List of tool names
   - `max_thinking_steps`: Reasoning iterations limit
2. Register in database via `initialize_templates()`
3. Document in `workspace/AGENTS.md`

## Troubleshooting

### Queen doesn't send ready message
- Check `ALLOWED_TELEGRAM_CHAT_IDS` in `.env`
- Verify chat IDs (get from @userinfobot)
- Check logs for "Queen ready message sent successfully"

### Worker tool errors
- Check tool permissions in worker spec
- Verify API keys (BRAVE_API_KEY, etc.)
- Check logs for specific error messages
- Ensure tool handler is sync/async compatible

### Memory not working
- Verify `OPENAI_API_KEY` is set
- Check `BROODMIND_MEMORY_*` settings
- Ensure embeddings are enabled

## License

MIT License - see LICENSE file for details
