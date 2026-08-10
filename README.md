# UNITARES Discord Bridge

[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![discord.py 2.4+](https://img.shields.io/badge/discord.py-2.4%2B-5865F2.svg)](https://github.com/Rapptz/discord.py)

Discord bot that surfaces UNITARES governance events and Lumen state in Discord.

## What It Does

- **Governance events** — Check-ins, verdicts, dialectic sessions
- **Lumen state** — Sensor readings, creature status from the embodied agent
- **HUD updates** — Live status channels
- **Slash commands** — Status, health, resume, and Lumen snapshots

## Prerequisites

1. A running UNITARES governance MCP server
2. A running Anima/Lumen MCP server (optional, for sensor/creature data)
3. A Discord bot token and guild ID

## Installation

```bash
git clone https://github.com/CIRWEL/unitares-discord-bridge.git
cd unitares-discord-bridge
pip install -e ".[dev]"
```

## Configuration

Copy `.env.example` to `.env` and set:

| Variable | Description |
|----------|-------------|
| `DISCORD_BOT_TOKEN` | Discord bot token |
| `DISCORD_GUILD_ID` | Discord server (guild) ID |
| `GOVERNANCE_MCP_URL` | Governance MCP server URL (default: `http://localhost:8767`) |
| `ANIMA_MCP_URL` | Anima/Lumen MCP URL (optional) |

Optional: `GOVERNANCE_API_TOKEN`, `ANIMA_API_TOKEN` for authenticated MCP calls.

`GOVERNANCE_OPERATOR_TOKEN` is separate from `GOVERNANCE_API_TOKEN` and is sent
as the `X-Unitares-Operator` header. It is what the **live HUD needs to show
EISV at all**: governance redacts other agents' UUIDs from `list_agents` for
non-operator callers and substitutes a display handle, and a display handle is
not a valid `agent_id` for `get_governance_metrics`. Without the operator token
the HUD lists agents but reports "no state" for every one of them.

### Resident channels

Findings are routed by their author. Residents listed in
`BRIDGE_RESIDENT_FINDING_CHANNELS` (default `sentinel,doctor`) get a text
channel of their own in the `GOVERNANCE` category, created on startup; every
other resident's findings keep landing in the shared `#residents` feed. Set the
variable to an empty string to put everyone back on `#residents`.

Channel topics are declared in code and reconciled on every startup, so a
channel whose job changes stops advertising the old one. Set
`BRIDGE_SYNC_CHANNEL_TOPICS=false` to keep hand-edited topics instead. The bot
needs Manage Channels to rewrite a topic; without it the edit is logged and
skipped.

## Run

```bash
unitares-bridge
# or
python -m bridge.bot
```

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
