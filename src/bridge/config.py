import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
GUILD_ID = int(os.environ.get("DISCORD_GUILD_ID", "0"))

GOVERNANCE_URL = os.environ.get("GOVERNANCE_MCP_URL", "http://localhost:8767")
ANIMA_URL = os.environ.get("ANIMA_MCP_URL", "")

GOVERNANCE_TOKEN = os.environ.get("GOVERNANCE_API_TOKEN", "")
ANIMA_TOKEN = os.environ.get("ANIMA_API_TOKEN", "")

# Operator-tier token, sent as the X-Unitares-Operator header. Distinct from
# GOVERNANCE_API_TOKEN (bearer auth): governance redacts every other agent's
# UUID from list_agents for non-operator callers and substitutes a display
# handle, and a display handle is not a valid agent_id for
# get_governance_metrics. Without this the HUD can name agents but can never
# read their state. The bridge is one of the clients this tier was introduced
# for (see src/mcp_handlers/identity/operator.py upstream). Unset → the HUD
# degrades loudly rather than silently: see hud.py.
GOVERNANCE_OPERATOR_TOKEN = os.environ.get("GOVERNANCE_OPERATOR_TOKEN", "")

EVENT_POLL_INTERVAL = int(os.environ.get("EVENT_POLL_INTERVAL", "10"))
HUD_UPDATE_INTERVAL = int(os.environ.get("HUD_UPDATE_INTERVAL", "30"))
SENSOR_POLL_INTERVAL = int(os.environ.get("SENSOR_POLL_INTERVAL", "300"))
SELF_ITERATION_POLL_INTERVAL = int(
    os.environ.get("LUMEN_SELF_ITERATION_POLL_INTERVAL", "60")
)
SELF_ITERATION_ENABLED = os.environ.get(
    "LUMEN_SELF_ITERATION_ENABLED", "true"
).lower() in ("1", "true", "yes", "on")

# Reaction(s) that count as acknowledging a delivered event. Comma-separated so
# a deployment can accept more than one gesture. Kept to an explicit set rather
# than "any reaction" so an unrelated reaction cannot silently clear a
# high-severity alert from the attention surface.
ACK_EMOJI = frozenset(
    e.strip() for e in os.environ.get("BRIDGE_ACK_EMOJI", "✅").split(",") if e.strip()
)
# Optional salt for operator_id_hash. Discord user ids are low-entropy, so an
# unsalted digest is reversible by anyone who can enumerate the guild.
ACK_HASH_SALT = os.environ.get("BRIDGE_ACK_HASH_SALT", "")

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

# Event types dropped before posting to Discord (comma-separated). Defaults to
# knowledge_read — high-volume, low-signal reads (often agent=system) that spam
# #signals even when no one is active. Knowledge *writes* (discoveries/updates)
# are NOT suppressed. Set BRIDGE_SUPPRESSED_EVENT_TYPES="" to disable filtering.
SUPPRESSED_EVENT_TYPES = {
    t.strip()
    for t in os.environ.get("BRIDGE_SUPPRESSED_EVENT_TYPES", "knowledge_read").split(",")
    if t.strip()
}

# Repeat-drift suppression — identity_drift / trajectory_drift re-fire on every
# check-in while the underlying condition is static (unitares#1370: a client
# cutover held lineage_similarity flat at 0.12 and produced 300+ identical
# embeds/day). The first event posts; repeats for the same (type, agent) are
# suppressed until the metric moves more than BRIDGE_DRIFT_REPEAT_DELTA or the
# window elapses (periodic re-reminder). Suppressed events still reach the
# ring buffer for /digest. Set BRIDGE_DRIFT_REPEAT_WINDOW_SECONDS=0 to disable.
# The *_resolved twins are included because the resolved side floods the same
# way when a server-side guard misbehaves (unitares#1421: identity_drift_resolved
# re-emitted on every 3-minute check-in, ~480 posts/day to #signals).
DRIFT_REPEAT_WINDOW_SECONDS = float(
    os.environ.get("BRIDGE_DRIFT_REPEAT_WINDOW_SECONDS", "21600")
)
DRIFT_REPEAT_DELTA = float(os.environ.get("BRIDGE_DRIFT_REPEAT_DELTA", "0.05"))
DRIFT_REPEAT_TYPES = {
    t.strip()
    for t in os.environ.get(
        "BRIDGE_DRIFT_REPEAT_TYPES",
        "identity_drift,trajectory_drift,"
        "identity_drift_resolved,trajectory_drift_resolved",
    ).split(",")
    if t.strip()
}

def _channel_key(raw: str) -> str:
    """Normalise a resident key into a Discord-legal text channel name."""
    return "-".join(raw.strip().lower().split())


# Residents that get their own findings channel instead of sharing #residents.
# A finding is attributed to its author by event-type prefix (sentinel_finding
# and sentinel_alarm_finding -> "sentinel") or, for a bare `finding` type, by
# the event's agent id/label. Each key becomes a text channel of the same name
# in the GOVERNANCE category; residents without one keep falling back to
# #residents, so Vigil/Watcher behaviour is unchanged. Set the env var to ""
# to put every resident back in the shared channel.
RESIDENT_FINDING_CHANNELS: tuple[str, ...] = tuple(
    dict.fromkeys(
        _channel_key(k)
        for k in os.environ.get(
            "BRIDGE_RESIDENT_FINDING_CHANNELS", "sentinel,doctor"
        ).split(",")
        if _channel_key(k)
    )
)

# Channel topics are otherwise frozen at creation time, so a channel whose job
# changed keeps advertising the old one (#residents still read "Sentinel /
# Vigil / Watcher findings" after Sentinel got its own channel). When enabled,
# startup rewrites any bridge-managed topic that has drifted from the declared
# structure. Turn off to keep hand-edited topics.
SYNC_CHANNEL_TOPICS = os.environ.get(
    "BRIDGE_SYNC_CHANNEL_TOPICS", "true"
).lower() in ("1", "true", "yes", "on")

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

# Liveness heartbeat: the event poll loop rewrites this file every iteration so
# an external watchdog can distinguish a *hung* event loop (process alive, loop
# wedged — the 2026-06-19 silent hang) from a healthy one, independent of log
# verbosity. Consumed by scripts/ops/bridge_liveness_watchdog.sh in the unitares
# repo. Empty disables the heartbeat write.
BRIDGE_HEARTBEAT_PATH = os.path.expanduser(
    os.environ.get("BRIDGE_HEARTBEAT_PATH", "~/.unitares/discord-bridge.heartbeat")
)
