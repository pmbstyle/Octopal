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

## Installing skills

BroodMind can install external skills into the local workspace with:

```bash
uv run broodmind skill install <source>
```

Supported source styles today:

```bash
uv run broodmind skill install zanblayde/agent-commons
uv run broodmind skill install clawhub:zanblayde/agent-commons
uv run broodmind skill install https://example.com/skills/writer/SKILL.md
uv run broodmind skill install https://example.com/bundles/writer.zip
uv run broodmind skill install ./local-skill
```

Installed skills are copied into `workspace/skills/<skill-id>/` and tracked in:

```text
workspace/skills/installed.json
```

You can inspect installer-managed entries with:

```bash
uv run broodmind skill list
uv run broodmind skill list --json
```

Lifecycle commands:

```bash
uv run broodmind skill install <source>
uv run broodmind skill list
uv run broodmind skill update <skill-id>
uv run broodmind skill trust <skill-id>
uv run broodmind skill untrust <skill-id>
uv run broodmind skill remove <skill-id>
```

`update` reinstalls from the stored source recorded in `installed.json`.
`remove` only affects installer-managed skills and will not delete unmanaged local bundles.

### Trust model for imported scripts

BroodMind treats imported script-backed skills more carefully than local ones.

- local installs from a folder, local `SKILL.md`, or local `.zip` are trusted by default
- external installs from ClawHub or remote URLs are untrusted by default when they include `scripts/`
- untrusted imported scripts stay visible in the skill inventory, but `run_skill_script` will refuse to execute them

When you want to allow script execution for an imported skill:

```bash
uv run broodmind skill trust <skill-id>
```

To block script execution again:

```bash
uv run broodmind skill untrust <skill-id>
```

This trust flag only affects script execution. The skill guidance in `SKILL.md` can still be read and used.

## ClawHub compatibility

BroodMind is compatible with the ClawHub install workflow at the UX level:

- ClawHub-style slug input like `zanblayde/agent-commons`
- `clawhub:<slug>` explicit source prefix
- frontmatter compatibility for `metadata.openclaw`

This is intentionally not a Node wrapper around `npx clawhub@latest`.
BroodMind uses its own installer and then normalizes the result into BroodMind's local skill bundle system.

That means:

- install UX is familiar to ClawHub/OpenClaw users
- installed skills become normal BroodMind skill bundles
- BroodMind keeps its own manifest and runtime policy model
- imported script-backed skills can be reviewed locally before being trusted

## Direct SKILL.md URLs

When the source points directly to `SKILL.md`, BroodMind creates a minimal bundle from that file.

This works best for markdown-only skills.

If the remote skill depends on:

- `scripts/`
- `references/`
- `assets/`

then a `.zip` bundle or ClawHub install source is preferred, because a raw `SKILL.md` URL cannot carry the supporting files.

## Readiness

Skills now expose readiness information:

- `ready`: available for execution
- `disabled`: present but manually disabled
- `not_ready`: present but missing requirements

Current requirement checks:

- missing binaries from `requires.bins`
- missing environment variables from `requires.env`
- missing config keys from `requires.config`
- untrusted imported script bundles

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
