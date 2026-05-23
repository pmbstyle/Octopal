# A2A Setup Guide

Octopal can connect trusted agents over the A2A HTTP+JSON interface. The intended setup is private by default: expose each agent on a private Tailscale URL or Tailscale IP, configure explicit peers, and authenticate each peer with a bearer token.

## Requirements

Each Octopal instance needs:

- `gateway.host` set to `0.0.0.0` so peers can reach it over the private network.
- `gateway.port` set to the port this instance should serve.
- `a2a.enabled` set to `true`.
- `a2a.public_base_url` set to this instance's reachable Tailscale URL, for example `http://100.64.10.20:8001`.
- One `a2a.peers` entry for every trusted remote agent it may talk to.

Use a private Tailscale URL or Tailscale IP for each agent instead of a public internet address unless you have put a separate access layer in front of it.

## Generate A Peer Token

Generate a shared bearer token for each peer relationship:

```bash
python -c 'import secrets; print(secrets.token_urlsafe(48))'
```

The token is the secret that authenticates the caller. When one agent sends a message to a peer, it sends the token configured under that peer entry. The receiving agent accepts the request if the same token is configured for the caller's peer entry.

For stricter separation, use different tokens for each direction if both sides are configured accordingly.

## Example: Connect Two Agents

This example connects two agents named `atlas` and `nova`.

`atlas` is reachable at:

```text
http://100.64.10.20:8001
```

`nova` is reachable at:

```text
http://100.64.10.21:8001
```

### Atlas Config

On `atlas`, add `nova` as a peer:

```json
{
  "gateway": {
    "host": "0.0.0.0",
    "port": 8001
  },
  "a2a": {
    "enabled": true,
    "public_base_url": "http://100.64.10.20:8001",
    "agent_name": "Atlas",
    "peers": {
      "nova": {
        "enabled": true,
        "name": "Nova",
        "base_url": "http://100.64.10.21:8001/a2a/v1",
        "token": "shared-token-for-atlas-and-nova",
        "capabilities": ["chat"],
        "trust_level": "trusted"
      }
    }
  }
}
```

### Nova Config

On `nova`, add the reciprocal `atlas` peer:

```json
{
  "gateway": {
    "host": "0.0.0.0",
    "port": 8001
  },
  "a2a": {
    "enabled": true,
    "public_base_url": "http://100.64.10.21:8001",
    "agent_name": "Nova",
    "peers": {
      "atlas": {
        "enabled": true,
        "name": "Atlas",
        "base_url": "http://100.64.10.20:8001/a2a/v1",
        "token": "shared-token-for-atlas-and-nova",
        "capabilities": ["chat"],
        "trust_level": "trusted"
      }
    }
  }
}
```

## Restart

Restart both agents after editing `config.json`:

```bash
uv run octopal restart
```

## Verify

Verify each agent card from the other machine:

```bash
curl http://100.64.10.20:8001/.well-known/agent-card.json
curl http://100.64.10.21:8001/.well-known/agent-card.json
```

Then ask either agent to list peers with `a2a_list_peers` and send a test message with `a2a_send_message`.
