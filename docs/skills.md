# Skills In BroodMind

BroodMind now supports a bundle-style skill layout inside the workspace.

## Bundle layout

Each skill lives in its own directory under `workspace/skills/<skill-id>/`.

Recommended structure:

```text
workspace/
  skills/
    image-lab/
      SKILL.md
      scripts/
      references/
      assets/
```

- `SKILL.md` is the main guidance file.
- `scripts/` holds executable helpers for deterministic tasks.
- `references/` holds supporting docs or examples.
- `assets/` holds non-text assets used by the skill.

## Frontmatter

`SKILL.md` can define metadata in frontmatter:

```md
---
name: image-lab
description: Generate or edit images
scope: worker
metadata:
  {
    "broodmind": {
      "primaryEnv": "OPENAI_API_KEY",
      "requires": {
        "bins": ["python"],
        "env": ["OPENAI_API_KEY"]
      }
    }
  }
---
```

Supported fields today:

- `name`
- `description`
- `scope`
- `metadata.broodmind.primaryEnv`
- `metadata.broodmind.requires.bins`
- `metadata.broodmind.requires.env`
- `metadata.broodmind.requires.config`

`metadata.openclaw` is also understood for compatibility when porting skill packs.

## Discovery and registry

BroodMind builds the skill inventory from two sources:

1. Auto-discovered bundle directories under `workspace/skills/*/SKILL.md`
2. Legacy `workspace/skills/registry.json`

`registry.json` still works and remains the compatibility layer for:

- manual registration
- legacy skill paths
- `enabled`
- `scope`

When a valid bundle is present, `name` and `description` come from `SKILL.md`.

## Readiness

Skills now expose readiness information:

- `ready`: available for execution
- `disabled`: present but manually disabled
- `not_ready`: present but missing requirements

Current requirement checks:

- missing binaries from `requires.bins`
- missing environment variables from `requires.env`
- missing config keys from `requires.config`

Config requirements are currently checked via env vars named like:

```text
BROODMIND_SKILL_CONFIG_<KEY>
```

Example:

```text
BROODMIND_SKILL_CONFIG_GITHUB_OWNER=my-org
```

## Script execution

Use `run_skill_script` to execute helpers from `scripts/`.

Properties:

- runs only files inside the selected skill's `scripts/` directory
- uses `shell=False`
- enforces workspace-bounded working directories
- supports explicit runners like `python`, `node`, `bash`, `pwsh`
- infers the runner for `.py`, `.js`, `.ps1`, and `.sh`

Example call shape:

```json
{
  "skill_id": "image-lab",
  "script": "render.py",
  "args": ["prompt.txt"],
  "workdir": ".",
  "timeout_seconds": 60
}
```

## Current limits

This is the current foundation, not the final platform.

Still planned:

- richer config sources beyond env vars
- install/status UX
- hot reload / watchers
- optional import helpers for external skill packs
- more explicit script/runtime metadata
