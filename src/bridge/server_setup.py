"""Auto-create Discord server structure (channels, categories, roles)."""

import logging

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
        "residents": {"type": "text", "topic": "Sentinel / Vigil / Watcher findings"},
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


async def ensure_server_structure(
    guild: discord.Guild,
    taxonomy: dict | None = None,
) -> dict[str, discord.abc.GuildChannel]:
    """Ensure all required roles, categories, and channels exist in *guild*.

    Returns a mapping of ``channel_name -> channel`` for every channel in the
    structure (whether it already existed or was freshly created).

    If ``taxonomy`` is provided, a ``VIOLATIONS`` category is created with one
    text channel per active class (``gov-int``, ``gov-ent``, etc.) so the
    ws_events subscriber can mirror class-matched events into class-specific
    channels. Passing ``None`` skips the violations category entirely.
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

    # Shallow-copy the structure so we can add a VIOLATIONS category for the
    # current session without mutating module-level state.
    structure: dict[str, dict[str, dict[str, str]]] = {
        k: dict(v) for k, v in CHANNEL_STRUCTURE.items()
    }
    violation_channels = _violation_class_channels(taxonomy)
    if violation_channels:
        structure["VIOLATIONS"] = violation_channels

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
