# Ops Tools And Templates

This document lists the new operations-focused tools and worker templates added to BroodMind.

## New Tool Categories

### Service and runtime operations

- `service_health`
  - Health checks for HTTP endpoints, TCP ports, process name presence, and docker containers.
- `service_logs`
  - Fetch logs from docker containers or workspace files, with optional grep filtering.
- `process_inspect`
  - Process list and listening-port inspection.
- `docker_compose_control`
  - Allowlisted docker compose actions: `ps`, `up`, `down`, `restart`, `logs`, `exec`.

### Deployment and release

- `git_ops`
  - Safe git operations: `status`, `fetch`, `pull`, `branch`, `log`, `show`.
- `release_snapshot`
  - Create/list release snapshots for rollback planning.
- `rollback_release`
  - Checkout a previous snapshot commit.

### Database maintenance (SQLite)

- `db_backup`
  - Backup database file to state backups directory.
- `db_restore`
  - Restore from backup file.
- `db_maintenance`
  - `integrity_check` / `vacuum`.
- `db_query_readonly`
  - Read-only SQL (`SELECT`) diagnostics.

### Security and quality

- `secret_scan`
  - Scan files for likely secret patterns/private keys.
- `config_audit`
  - Basic env/config presence checks.
- `test_run`
  - Run allowlisted test/lint commands (`pytest`/`ruff`/`mypy`).
- `coverage_report`
  - Read coverage summary from `coverage.xml` (if present).
- `artifact_collect`
  - Collect files by glob pattern.

### Self control (supervised)

- `self_control`
  - Queue supervised actions: `restart_service`, `graceful_shutdown`, `reload_config`, and query `status`.
  - Writes requests/acks under state dir:
    - `control_requests.jsonl`
    - `control_acks.jsonl`

## Safety Confirmation

High-impact actions require explicit confirmation by passing `confirm=true`:

- `docker_compose_control` for `down`, `restart`, `exec`
- `db_restore`
- `rollback_release`
- `self_control` for restart/shutdown/reload actions

## New Worker Templates

Default templates were added under `src/broodmind/workers/default_templates/`:

- `ops_sre`
- `deploy_manager`
- `db_maintainer`
- `security_auditor`
- `test_runner`
- `release_notes_writer`
- `self_controller`

Note: BroodMind discovers active templates from `workspace/workers/`.  
Copy desired defaults into `workspace/workers/<worker_id>/worker.json` to activate them.

Convenience sync options:

- CLI: `broodmind sync-worker-templates`
- CLI with overwrite: `broodmind sync-worker-templates --overwrite`
- Script: `python scripts/sync_worker_templates.py`
- Script with overwrite: `python scripts/sync_worker_templates.py --overwrite`

## Worker Child Spawning (V1)

Workers can delegate to other workers using a strict opt-in policy.

### Template fields

Add these fields in `worker.json`:

- `can_spawn_children` (boolean, default `false`)
- `allowed_child_templates` (array of template IDs, default `[]`)

Example coordinator template:

```json
{
  "id": "rss_coordinator",
  "name": "RSS Coordinator",
  "description": "Coordinates feed-specific child workers and synthesizes a final digest.",
  "system_prompt": "Spawn child workers for each feed and return one compressed final summary.",
  "available_tools": ["start_child_worker", "start_workers_parallel", "synthesize_worker_results"],
  "required_permissions": ["worker_manage", "network"],
  "max_thinking_steps": 10,
  "default_timeout_seconds": 300,
  "can_spawn_children": true,
  "allowed_child_templates": ["rss_fetcher"]
}
```

Example child template:

```json
{
  "id": "rss_fetcher",
  "name": "RSS Fetcher",
  "description": "Fetches and summarizes one RSS source.",
  "system_prompt": "Read one source and return a concise structured summary.",
  "available_tools": ["web_fetch"],
  "required_permissions": ["network"],
  "max_thinking_steps": 6,
  "default_timeout_seconds": 180,
  "can_spawn_children": false,
  "allowed_child_templates": []
}
```

### Runtime enforcement

- Parent template must have `can_spawn_children=true`.
- Child template must be in parent `allowed_child_templates`.
- Child permissions must be a subset of the parent's effective permissions.
- Queen enforces global lineage limits:
  - `BROODMIND_WORKER_MAX_SPAWN_DEPTH` (default `2`)
  - `BROODMIND_WORKER_MAX_CHILDREN_TOTAL` (default `20`)
  - `BROODMIND_WORKER_MAX_CHILDREN_CONCURRENT` (default `10`)

### Tooling and metadata

- Worker context delegation tool: `start_child_worker`
- Run metadata returned in status/result/list APIs:
  - `lineage_id`
  - `parent_worker_id`
  - `root_task_id`
  - `spawn_depth`
- Orphan cleanup: if a parent worker fails, runtime stops active child workers from that parent chain.

## New Permission Types

The following permissions are now recognized by policy and tool filtering:

- `service_read`
- `service_control`
- `deploy_control`
- `db_admin`
- `security_audit`
- `self_control`

## Supervisor For Self Control

`self_control` is intentionally mediated and does not directly kill/restart the current process.

Use the provided supervisor script:

- `scripts/self_control_supervisor.py`

It watches queued control requests and executes restart/stop actions via CLI, then writes acknowledgements.
