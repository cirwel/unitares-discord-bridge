"""Auto-create Discord server structure (channels, categories, roles)."""

import logging
from typing import Iterable

import discord

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Desired server structure
# ---------------------------------------------------------------------------

CHANNEL_STRUCTURE: dict[str, dict[str, dict[str, str]]] = {
    "GOVERNANCE": {
        "activity": {"type": "text", "topic": "Routine agent activity — onboards, idle, lifecycle_created/resumed/archived, knowledge writes"},
        "signals": {"type": "text", "topic": "Operator attention — verdict changes, drift, risk, identity assurance, circuit breakers, confidence clamps"},
        "alerts": {"type": "text", "topic": "Critical only — pause, reject, stuck, silent critical, circuit breaker trip"},
        "residents": {"type": "text", "topic": "Resident findings without a channel of their own"},
        "governance-hud": {"type": "text", "topic": "Auto-updating system status"},
    },
    "LUMEN": {
        "lumen-art": {"type": "text", "topic": "Lumen's drawings"},
        "lumen-sensors": {"type": "text", "topic": "Environmental sensor readings"},
        "lumen-digest": {"type": "text", "topic": "Weekly Q&A digest — what Lumen wondered about, and when to visit live"},
    },
    "CONTROL": {
        "commands": {"type": "text", "topic": "Slash commands for governance actions"},
        "audit-log": {"type": "text", "topic": "All bot actions logged here"},
    },
}

ROLES: dict[str, discord.Colour] = {
    "Governance Admin": discord.Colour.dark_teal(),
    "observer": discord.Colour.light_grey(),
    "lumen": discord.Colour.blue(),
}


# ---------------------------------------------------------------------------
# Ensure everything exists
# ---------------------------------------------------------------------------

def _violation_class_channels(taxonomy: dict | None) -> dict[str, dict[str, str]]:
    """Build a ``{channel_name: {type, topic}}`` mapping for each active
    violation class in the taxonomy.

    Channels are named ``gov-<class-id-lowercased>`` (e.g. ``gov-int``) so
    operators can scan the channel list and mute or subscribe per class.
    Topic includes the class name + description.
    """
    if not taxonomy:
        return {}
    out: dict[str, dict[str, str]] = {}
    for cls in taxonomy.get("classes") or []:
        if cls.get("status") != "active":
            continue
        cid = (cls.get("id") or "").lower()
        if not cid:
            continue
        name = f"gov-{cid}"
        topic_parts = [cls.get("name") or cid.upper()]
        desc = (cls.get("description") or "").strip()
        if desc:
            topic_parts.append(desc)
        out[name] = {
            "type": "text",
            "topic": " — ".join(topic_parts)[:1000],
        }
    return out


def _resident_finding_channels(
    keys: Iterable[str],
    taken: Iterable[str],
) -> dict[str, dict[str, str]]:
    """Build a ``{channel_name: {type, topic}}`` mapping for each resident that
    gets its own findings channel (Sentinel, Doctor).

    ``taken`` is the set of channel names already claimed by the base
    structure — a resident key colliding with one of those (``alerts``, say)
    is skipped rather than silently rewriting an existing channel's role.
    """
    reserved = set(taken)
    out: dict[str, dict[str, str]] = {}
    for key in keys:
        if not key or key in reserved or key in out:
            continue
        out[key] = {
            "type": "text",
            "topic": f"{key.replace('-', ' ').title()} findings only — "
                     "split out of #residents so they can be read or muted on their own",
        }
    return out


def build_channel_structure(
    taxonomy: dict | None = None,
    resident_channels: Iterable[str] = (),
) -> dict[str, dict[str, dict[str, str]]]:
    """Return the desired ``{category: {channel: cfg}}`` for this session.

    Starts from :data:`CHANNEL_STRUCTURE`, appends a per-resident findings
    channel next to ``#residents`` for each key in *resident_channels*, and
    adds a ``VIOLATIONS`` category when a taxonomy is supplied.
    """
    structure: dict[str, dict[str, dict[str, str]]] = {
        k: dict(v) for k, v in CHANNEL_STRUCTURE.items()
    }

    taken = {name for chans in structure.values() for name in chans}
    resident = _resident_finding_channels(resident_channels, taken)
    if resident:
        # Insert directly after #residents so the fallback feed and the
        # dedicated ones read as one group in the sidebar.
        governance = structure["GOVERNANCE"]
        rebuilt: dict[str, dict[str, str]] = {}
        for name, cfg in governance.items():
            rebuilt[name] = cfg
            if name == "residents":
                rebuilt.update(resident)
        structure["GOVERNANCE"] = rebuilt

    violation_channels = _violation_class_channels(taxonomy)
    if violation_channels:
        structure["VIOLATIONS"] = violation_channels

    return structure


async def ensure_server_structure(
    guild: discord.Guild,
    taxonomy: dict | None = None,
    resident_channels: Iterable[str] = (),
) -> dict[str, discord.abc.GuildChannel]:
    """Ensure all required roles, categories, and channels exist in *guild*.

    Returns a mapping of ``channel_name -> channel`` for every channel in the
    structure (whether it already existed or was freshly created).

    If ``taxonomy`` is provided, a ``VIOLATIONS`` category is created with one
    text channel per active class (``gov-int``, ``gov-ent``, etc.) so the
    ws_events subscriber can mirror class-matched events into class-specific
    channels. Passing ``None`` skips the violations category entirely.

    ``resident_channels`` names the residents that get their own findings
    channel in ``GOVERNANCE`` (e.g. ``("sentinel", "doctor")``); the default
    empty tuple leaves every resident on the shared ``#residents`` feed.
    """

    # ---- Roles -------------------------------------------------------------
    existing_roles = {r.name: r for r in guild.roles}
    for role_name, colour in ROLES.items():
        if role_name not in existing_roles:
            await guild.create_role(name=role_name, colour=colour)
            log.info("Created role: %s", role_name)

    # ---- Categories & channels ---------------------------------------------
    existing_categories = {c.name: c for c in guild.categories}
    channel_map: dict[str, discord.abc.GuildChannel] = {}

    # Built fresh per session (resident + violation channels are runtime
    # config) — module-level CHANNEL_STRUCTURE is never mutated.
    structure = build_channel_structure(taxonomy, resident_channels)

    for category_name, channels in structure.items():
        # Ensure category exists
        category = existing_categories.get(category_name)
        if category is None:
            category = await guild.create_category(category_name)
            log.info("Created category: %s", category_name)

        # Index channels already present in this category
        existing_channels = {ch.name: ch for ch in category.channels}

        for ch_name, ch_cfg in channels.items():
            channel = existing_channels.get(ch_name)
            if channel is None:
                ch_type = ch_cfg["type"]
                topic = ch_cfg.get("topic", "")
                if ch_type == "forum":
                    channel = await guild.create_forum(
                        name=ch_name, category=category, topic=topic,
                    )
                    log.info("Created forum channel: %s/%s", category_name, ch_name)
                else:
                    channel = await guild.create_text_channel(
                        name=ch_name, category=category, topic=topic,
                    )
                    log.info("Created text channel: %s/%s", category_name, ch_name)

            channel_map[ch_name] = channel

    log.info(
        "Server structure verified — %d channels mapped", len(channel_map),
    )
    return channel_map
