"""Reaction-driven acknowledgement receipts.

Governance already accepts `bridge.ack` and matches it against deliveries on
`discord_message_id` (see unitares `src/bridge_events.py`), but nothing ever
emitted one — so `observe(action='bridge')`'s `unacked_critical` could only
ever report *everything* as unacked. This module closes the producer side.

The acknowledging gesture is a reaction rather than a slash command: it is one
tap, works on mobile, and Discord's raw reaction event carries the message id
directly, which is exactly the key the summary matches on.

Raw events (`on_raw_reaction_add`) are used deliberately — the cached variant
only fires for messages in the bot's message cache, so acks on anything
delivered before the last restart would be silently dropped.
"""

from __future__ import annotations

import hashlib
import logging

import discord

from bridge.config import ACK_EMOJI, ACK_HASH_SALT
from bridge.mcp_client import GovernanceClient

log = logging.getLogger("bridge")


def _operator_id_hash(user_id: int | str) -> str:
    """Stable pseudonymous id for the acking operator.

    Discord user ids are low-entropy, so an unsalted digest is reversible by
    anyone who can enumerate the guild. ACK_HASH_SALT lets a deployment make
    the mapping non-recoverable; it is optional so this works out of the box.
    """
    return hashlib.sha256(f"{ACK_HASH_SALT}{user_id}".encode()).hexdigest()


def build_ack_payload(
    payload: discord.RawReactionActionEvent,
    channel_key: str | None = None,
) -> dict:
    """Build a `bridge.ack` receipt from a raw reaction event.

    `discord_message_id` is the match key; everything else is optional context
    that governance's normalizer keeps or drops on its own allowlist.
    """
    ack: dict = {
        "event_type": "bridge.ack",
        "discord_message_id": str(payload.message_id),
        "discord_channel_id": str(payload.channel_id),
        "operator_id_hash": _operator_id_hash(payload.user_id),
    }
    if payload.guild_id is not None:
        ack["discord_guild_id"] = str(payload.guild_id)
    if channel_key:
        ack["channel_key"] = channel_key
    return ack


def is_ack_emoji(emoji: discord.PartialEmoji | str) -> bool:
    """Only a configured emoji counts as acknowledgement.

    Treating *any* reaction as an ack would let an unrelated 😂 silently clear
    a high-severity alert from the attention surface.
    """
    return str(emoji) in ACK_EMOJI


def setup_acks(bot: discord.ext.commands.Bot, gov_client: GovernanceClient) -> None:
    """Register the reaction listener that emits `bridge.ack` receipts."""

    @bot.event
    async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
        # Never let the bot ack its own deliveries.
        if bot.user is not None and payload.user_id == bot.user.id:
            return
        if not is_ack_emoji(payload.emoji):
            return

        channel = bot.get_channel(payload.channel_id)
        channel_key = getattr(channel, "name", None)

        try:
            await gov_client.record_bridge_event(
                build_ack_payload(payload, channel_key)
            )
        except Exception as exc:  # noqa: BLE001 - receipts are best-effort
            # Matches record_bridge_event's own contract: observability writes
            # must never take down delivery or the event loop.
            log.debug("bridge ack receipt failed: %s", exc)
