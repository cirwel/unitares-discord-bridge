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
# UUID from agent(action="list") for non-operator callers and substitutes a display
# handle, and a display handle is not a valid agent_id for
# get_governance_metrics. Without this the HUD can name agents but can never
# read their state. The bridge is one of the clients this tier was introduced
# for (see src/mcp_handlers/identity/operator.py upstream). Unset → the HUD
# degrades loudly rather than silently: see hud.py.
GOVERNANCE_OPERATOR_TOKEN = os.environ.get("GOVERNANCE_OPERATOR_TOKEN", "")

# Mention prefixed to the "Lumen Offline" / "Lumen Online" posts ONLY — e.g.
# "<@123456789>" for a user or "<@&987654321>" for a role. Empty (the default)
# keeps the current behaviour and the repo user-agnostic.
#
# Every other surface in this bridge is deliberately silent: routine embeds and
# self-iteration attention items post without pinging (iterations.py). That is
# right for a status feed and wrong for an outage. Lumen went dark for ~3.5 days
# in July 2026 while the Pi was unplugged; the bridge detected it correctly and
# posted an embed nobody was looking at, so the outage was found by hand on the
# operator's return. A channel post is a record, not an alert — only a mention
# reaches a phone. Scoped to the offline/recovery transitions (which fire at most
# twice per outage) so this cannot become a notification firehose.
LUMEN_OFFLINE_MENTION = os.environ.get("LUMEN_OFFLINE_MENTION", "").strip()

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

def _csv_set(name: str, default: str) -> frozenset[str]:
    """Parse a comma-separated env var into a lowercased set. "" means empty."""
    return frozenset(
        t.strip().lower()
        for t in os.environ.get(name, default).split(",")
        if t.strip()
    )


# Resident-only event types. agent_new and risk_threshold fire for every
# short-lived codex/claude session (~72 posts/day, ~40% of all bridge traffic
# over 14 days, 2026-10). They are suppressed only on a POSITIVE answer from
# governance's own agent record (agent(action="get").tags): the agent carries
# one of BRIDGE_NONRESIDENT_AGENT_TAGS (governance stamps `ephemeral` on
# throwaway sessions) and none of BRIDGE_RESIDENT_AGENT_TAGS (residents carry
# `persistent`). Empty tags, unknown tags or a failed lookup post as before.
# A critical event (risk >= 70%) is never dropped: for a non-resident it still
# goes to #alerts, just not to #signals as well. "" disables the filter.
RESIDENT_ONLY_EVENT_TYPES = _csv_set(
    "BRIDGE_RESIDENT_ONLY_EVENT_TYPES", "agent_new,risk_threshold"
)
RESIDENT_AGENT_TAGS = _csv_set(
    "BRIDGE_RESIDENT_AGENT_TAGS", "persistent,protected,embodied"
)
NONRESIDENT_AGENT_TAGS = _csv_set("BRIDGE_NONRESIDENT_AGENT_TAGS", "ephemeral")

# Embodied-agent state chatter. drift_oscillation (governance's detector calls
# it self-correcting) and trajectory_adjustment for an embodied agent (Lumen)
# go to the Lumen channel family instead of #signals. Capability, not name:
# the subject agent must carry one of BRIDGE_EMBODIED_AGENT_TAGS. If the named
# channel does not exist the events keep their old destination.
EMBODIED_STATE_EVENT_TYPES = _csv_set(
    "BRIDGE_EMBODIED_STATE_EVENT_TYPES", "drift_oscillation,trajectory_adjustment"
)
EMBODIED_AGENT_TAGS = _csv_set("BRIDGE_EMBODIED_AGENT_TAGS", "embodied")
EMBODIED_STATE_CHANNEL = os.environ.get(
    "BRIDGE_EMBODIED_STATE_CHANNEL", "lumen-sensors"
).strip()

# How long a looked-up agent tag set is trusted before it is re-read. Resident
# answers are stable; anything else is re-read sooner so an agent promoted to
# resident (or tagged after its first check-in) is not mis-filtered for long.
# Failed lookups are remembered briefly so a governance outage is not hammered.
AGENT_TAGS_TTL_SECONDS = float(os.environ.get("BRIDGE_AGENT_TAGS_TTL_SECONDS", "3600"))
AGENT_TAGS_NONRESIDENT_TTL_SECONDS = float(
    os.environ.get("BRIDGE_AGENT_TAGS_NONRESIDENT_TTL_SECONDS", "300")
)
AGENT_TAGS_FAILURE_TTL_SECONDS = float(
    os.environ.get("BRIDGE_AGENT_TAGS_FAILURE_TTL_SECONDS", "60")
)

# Resident findings post ONCE to ONE channel. A finding whose severity is in
# this set goes to #alerts (it needs the operator) unless its class is one of
# the explicit noisy pattern-hit classes in embeds._FINDING_ALERT_EXCLUSIONS
# (Watcher pattern hits, Sentinel held-by-other lease conflicts, deploy-drift
# "merged commits not pulled" lag); every other finding goes to its resident
# channel only. `high` is included because it is the top severity several
# genuine alarms use (Sentinel verdict_shift / coordinated degradation /
# forced release). Before 2026-10 high/critical findings were posted to both
# channels, so Watcher's routine high-severity pattern hits doubled in #alerts.
FINDING_ALERT_SEVERITIES = _csv_set("BRIDGE_FINDING_ALERT_SEVERITIES", "high,critical")

# A resolution finding whose action is "dismissed" (Watcher's "[dismissed] ..."
# closures, ~8/day) records that nothing needs doing. It is receipted as
# suppressed and not posted. Confirmed resolutions still post, to the resident
# channel only, never #alerts.
SUPPRESS_DISMISSED_RESOLUTIONS = os.environ.get(
    "BRIDGE_SUPPRESS_DISMISSED_RESOLUTIONS", "true"
).lower() in ("1", "true", "yes", "on")

# Transient Discord connection failures (DNS blips: "Cannot connect to host
# discord.com:443") — see bridge/delivery.py. Each send is tried
# BRIDGE_SEND_ATTEMPTS times with exponential backoff starting at
# BRIDGE_SEND_RETRY_BACKOFF_SECONDS; a message that still fails is re-queued up
# to BRIDGE_SEND_REDELIVERIES times, BRIDGE_SEND_REDELIVERY_DELAY_SECONDS apart
# (defaults ride out roughly a five-minute outage), before the loss is recorded
# as bridge.delivery_failed / discord_unreachable.
SEND_ATTEMPTS = int(os.environ.get("BRIDGE_SEND_ATTEMPTS", "3"))
SEND_RETRY_BACKOFF_SECONDS = float(
    os.environ.get("BRIDGE_SEND_RETRY_BACKOFF_SECONDS", "1")
)
SEND_REDELIVERIES = int(os.environ.get("BRIDGE_SEND_REDELIVERIES", "5"))
SEND_REDELIVERY_DELAY_SECONDS = float(
    os.environ.get("BRIDGE_SEND_REDELIVERY_DELAY_SECONDS", "60")
)


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
