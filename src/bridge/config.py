import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
GUILD_ID = int(os.environ.get("DISCORD_GUILD_ID", "0"))

GOVERNANCE_URL = os.environ.get("GOVERNANCE_MCP_URL", "http://localhost:8767")
ANIMA_URL = os.environ.get("ANIMA_MCP_URL", "")

GOVERNANCE_TOKEN = os.environ.get("GOVERNANCE_API_TOKEN", "")
ANIMA_TOKEN = os.environ.get("ANIMA_API_TOKEN", "")

EVENT_POLL_INTERVAL = int(os.environ.get("EVENT_POLL_INTERVAL", "10"))
HUD_UPDATE_INTERVAL = int(os.environ.get("HUD_UPDATE_INTERVAL", "30"))
SENSOR_POLL_INTERVAL = int(os.environ.get("SENSOR_POLL_INTERVAL", "300"))

# Lumen Q&A digest: periodic summary of Lumen's self-asked questions and how
# often relational (aloneness/presence) themes recur. Disabled if the
# lumen-digest channel is absent. Defaults: post weekly, summarise the last
# week, re-check hourly.
DIGEST_ENABLED = os.environ.get("LUMEN_DIGEST_ENABLED", "true").lower() in ("1", "true", "yes")
DIGEST_INTERVAL = int(os.environ.get("LUMEN_DIGEST_INTERVAL", str(7 * 24 * 3600)))
DIGEST_WINDOW = int(os.environ.get("LUMEN_DIGEST_WINDOW", str(7 * 24 * 3600)))
DIGEST_CHECK_INTERVAL = int(os.environ.get("LUMEN_DIGEST_CHECK_INTERVAL", "3600"))
DIGEST_RELATIONAL_THRESHOLD = int(os.environ.get("LUMEN_DIGEST_RELATIONAL_THRESHOLD", "2"))

DB_PATH = os.environ.get("BRIDGE_DB_PATH", "data/bridge.db")

# Per-class routing — when enabled, broadcaster events that map to a
# violation class (via /v1/taxonomy reverse-lookup) are mirrored to a
# class-specific text channel in addition to the main #events channel.
# Lets operators subscribe to specific violation classes and mute the rest.
# Disabled by default so existing deployments don't get new channels without
# opt-in.
CLASS_ROUTING_ENABLED = os.environ.get(
    "BRIDGE_CLASS_ROUTING_ENABLED", ""
).lower() in ("1", "true", "yes", "on")

# Operator-managed channel for lease-plane Phase B promotion-eligibility
# transitions (event_type=lease_plane_phase_b_transition emitted by Sentinel).
# Empty/0 disables routing — events still flow to the default activity/signals
# channels via the standard dispatch path. Channel must already exist; the
# bridge does NOT auto-create it.
LEASE_PLANE_PHASE_B_CHANNEL_ID = int(
    os.environ.get("DISCORD_LEASE_PLANE_PHASE_B_CHANNEL_ID", "0") or "0"
)
