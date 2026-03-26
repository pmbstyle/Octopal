# Worker Isolation and Security

Octopal ensures that child workers operate within strict, controlled environments to protect your workspace and underlying system. The platform employs a "Least Privilege" model for both filesystem access and execution environments.

## Explicit Path Allowlisting

Every worker always keeps its own private scratch workspace at `workspace/workers/<worker_id>`. This is the default place for temporary files, notes, intermediate outputs, and any worker-local edits.

`allowed_paths` is an optional add-on that shares selected paths from Octo's main workspace with the worker.

### How it Works:
1. **Worker Scratch Stays Local:** Relative filesystem paths continue to resolve inside the worker's own directory, preserving existing worker behavior.
2. **Shared Paths Are Explicit:** When Octo launches a worker via `start_worker`, `start_child_worker`, or `start_workers_parallel`, it may pass `allowed_paths` such as `["src/api", "tests/test_api.py"]`.
3. **Python-Level Security (`same_env`):** Filesystem tools keep unrestricted access to the worker's scratch directory, but any path that targets Octo's shared workspace must match the `allowed_paths` allowlist.
4. **Docker-Level Security (`docker`):** Restricted workers get mounts for their own scratch directory plus only the explicitly shared paths. Unshared parts of the Octo workspace are not mounted.

If `allowed_paths` is omitted entirely, worker launch behavior stays unchanged from legacy mode.

## Execution Environments

Octopal supports two worker launcher modes, configurable via `config.json` (`worker_launcher`):

### 1. Docker Launcher (`docker`) - Recommended for Production
This is the most secure and robust way to run workers, especially when executing untrusted code or fetching data from the open web.

- **Ephemeral Containers:** Each worker run spins up a fresh, isolated Docker container based on the `octopal-worker` image. 
- **Volume Mounting:** Restricted workers get their scratch directory plus the explicitly shared `allowed_paths`. Legacy launches without `allowed_paths` keep the full workspace mount for compatibility.
- **Environment Scrubbing:** The worker container is stripped of sensitive host environment variables. API keys, Telegram tokens, and host configurations are not passed down.
- **Clean Death:** Once the worker completes its task or fails, the `--rm` flag ensures the container is immediately destroyed, leaving no background processes or state behind.

### 2. Same Environment Launcher (`same_env`) - For Local Development
This mode runs workers as subprocesses directly on the host machine. It is faster to start (no container overhead) but inherently less secure.

- **Speed:** Instant startup, ideal for quick iterations or local debugging.
- **Python-Level Bounds:** Filesystem access is still protected by `allowed_paths` via the Python tool wrappers, preventing accidental file modifications.
- **Warning:** In this mode, workers run under the same OS user and inherit the host's capabilities. Malicious code executed by a worker (e.g., via the `exec_run` tool) could theoretically bypass Python-level path restrictions or access host environment variables. Use this mode only when you fully trust the tasks and tools available to the worker.

## Worker Temporary Directories

Regardless of the launcher used, every worker is assigned a unique, ephemeral directory (`workspace/workers/<worker_id>`). 
- This directory houses the worker's instruction set (`spec.json`) and acts as a safe scratchpad for temporary files.
- This directory is always available to the worker, even when `allowed_paths` is used.
- Once the worker concludes its lifecycle (success or failure), the Octopal orchestrator automatically deletes this directory, keeping the workspace clean.
