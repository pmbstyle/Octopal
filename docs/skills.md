# Skills In BroodMind

BroodMind now supports a bundle-style skill layout inside the workspace.

## Bundle layout

Each skill lives in its own directory under `workspace/skills/<skill-id>/`.

Recommended structure:

```text
workspace/
  skills/
    my-skill/
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
name: my-skill
description: Describe what this skill does
scope: worker
metadata:
  {
    "broodmind": {
      "primaryEnv": "MY_API_KEY",
      "requires": {
        "bins": ["python"],
        "env": ["MY_API_KEY"]
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
- `metadata.broodmind.runtime.python.packages`
- `metadata.broodmind.runtime.node.packages`
- `metadata.broodmind.runtime.node.packageManager`

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
uv run broodmind skill install publisher/skill-pack
uv run broodmind skill install clawhub:publisher/skill-pack
uv run broodmind skill install https://host/path/to/SKILL.md
uv run broodmind skill install https://host/path/to/skill.zip
uv run broodmind skill install ./local-skill
```

Installed skills are copied into `workspace/skills/<skill-id>/` and tracked in:

```text
workspace/skills/installed.json
```

You can inspect all discovered skills, including local bundles and installer-managed entries, with:

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
`remove` works for both installer-managed skills and local workspace skills. For local bundles, it removes the bundle from `workspace/skills/<skill-id>/` when possible.
For script-backed skills, install and update automatically scan scripts and prepare an isolated runtime env when possible.

### Isolated runtime envs

Script-backed skills now require isolated runtime envs stored under:

```text
workspace/.skill-envs/<skill-id>/
```

This keeps the main BroodMind environment clean while still allowing per-skill dependencies.
Any skill with scripts must be prepared before `run_skill_script` will execute it, but installer-managed skills now do this automatically during install and update.

Typical flow:

```bash
uv run broodmind skill install <source>
uv run broodmind skill trust <skill-id>
```

Python skills can declare packages like:

```md
metadata:
  {
    "broodmind": {
      "runtime": {
        "python": {
          "packages": ["python-jobspy"]
        }
      }
    }
  }
```

Node or TypeScript skills can declare packages like:

```md
metadata:
  {
    "broodmind": {
      "runtime": {
        "node": {
          "packages": ["tsx"],
          "packageManager": "npm"
        }
      }
    }
  }
```

For third-party skills without BroodMind runtime metadata, BroodMind also falls back to:

- `requirements.txt` for Python package lists
- `package.json` `dependencies` and `devDependencies` for Node or TypeScript skills

### Trust model for imported scripts

BroodMind treats imported script-backed skills more carefully than local ones.

- local workspace skills are trusted by default
- local installs from a folder, local `SKILL.md`, or local `.zip` are trusted by default
- external installs from ClawHub or remote URLs are untrusted by default when they include `scripts/`
- untrusted imported scripts stay visible in the skill inventory, but `run_skill_script` will refuse to execute them

When you want to allow script execution for an imported skill:

```bash
uv run broodmind skill trust <skill-id>
```

If the verification scan reports findings that need manual review, trust will ask you to confirm intent explicitly:

```bash
uv run broodmind skill trust <skill-id> --force
```

To block script execution again:

```bash
uv run broodmind skill untrust <skill-id>
```

This trust flag affects script execution for both imported and local script-backed skills. The skill guidance in `SKILL.md` can still be read and used.

### Verification scan

Installer-managed skills now keep a lightweight verification report in `installed.json`.

Install and update run this scan automatically. The scan currently records:

- hashes and sizes for files inside `scripts/`
- heuristic findings for network calls
- process spawning and shell execution
- destructive file operations
- runtime package installation

If you need to repair a local or manually edited skill env, these maintenance commands are still available:

```bash
uv run broodmind skill prepare-env <skill-id>
uv run broodmind skill remove-env <skill-id>
```

This is a review aid, not a sandbox or malware detector.

## ClawHub compatibility

BroodMind is compatible with the ClawHub install workflow at the UX level:

- ClawHub-style slug input like `publisher/skill-pack`
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
  "skill_id": "my-skill",
  "script": "render.py",
  "args": ["prompt.txt"],
  "workdir": ".",
  "timeout_seconds": 60
}
```
