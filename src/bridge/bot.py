import asyncio
import logging
import os
from datetime import datetime, timezone

import discord
from discord.ext import commands

from bridge.config import (
    DISCORD_TOKEN, GUILD_ID, GOVERNANCE_URL, ANIMA_URL,
    GOVERNANCE_TOKEN, ANIMA_TOKEN,
    EVENT_POLL_INTERVAL, HUD_UPDATE_INTERVAL, SENSOR_POLL_INTERVAL, DB_PATH,
    CLASS_ROUTING_ENABLED, LEASE_PLANE_PHASE_B_CHANNEL_ID,
    RESIDENT_FINDING_CHANNELS,
    DIGEST_ENABLED, DIGEST_INTERVAL, DIGEST_WINDOW, DIGEST_CHECK_INTERVAL,
    DIGEST_RELATIONAL_THRESHOLD,
)
from bridge.cache import BridgeCache
from bridge.mcp_client import GovernanceClient, AnimaClient
from bridge.server_setup import ensure_server_structure
from bridge.event_poller import EventPoller
from bridge.hud import HUDUpdater
from bridge.lumen import LumenPoller
from bridge.digest import LumenDigestPoller
from bridge.acks import setup_acks
from bridge.commands import setup_commands
from bridge.ws_events import WSEventSubscriber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("bridge")


class BridgeBot(commands.Bot):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._shutdown_started = False

    async def close(self) -> None:
        if not self._shutdown_started:
            self._shutdown_started = True
            await shutdown_bridge()
        await super().close()


intents = discord.Intents.default()
intents.message_content = True
bot = BridgeBot(command_prefix="!", intents=intents)

gov_client = GovernanceClient(GOVERNANCE_URL, token=GOVERNANCE_TOKEN)
anima_client = AnimaClient(ANIMA_URL, token=ANIMA_TOKEN)

cache: BridgeCache | None = None
event_poller: EventPoller | None = None
ws_subscriber: WSEventSubscriber | None = None
hud_updater: HUDUpdater | None = None
lumen_poller: LumenPoller | None = None
digest_poller: LumenDigestPoller | None = None
audit_channel: discord.TextChannel | None = None
_initialized: bool = False


@bot.event
async def on_ready():
    global cache, event_poller, ws_subscriber, hud_updater, lumen_poller, digest_poller, audit_channel, _initialized

    if _initialized:
        log.info("Reconnected as %s (skipping re-init)", bot.user)
        return

    log.info("Bridge online as %s", bot.user)
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        log.error("Guild %d not found", GUILD_ID)
        return

    # Open persistent HTTP clients before fetching the taxonomy so the HTTP
    # client is ready (ensure_server_structure is Discord-only and doesn't
    # need them).
    await gov_client.open()
    await anima_client.open()

    # Mint this process-instance's governance identity (declared lineage to
    # the previous instance via the sidecar file). Best-effort: an unbound
    # bridge still works, it just polls anonymously.
    identity_path = os.path.join(
        os.path.dirname(DB_PATH) or "data", "governance_identity.json",
    )
    await gov_client.onboard(identity_path)

    # Fetch the violation taxonomy before creating channels so the VIOLATIONS
    # category is populated (if class routing is enabled). Best-effort — a
    # missing taxonomy just skips the violations category.
    taxonomy = None
    if CLASS_ROUTING_ENABLED:
        taxonomy = await gov_client.fetch_taxonomy()
        if taxonomy:
            log.info(
                "Class routing enabled — %d classes loaded from governance",
                len(taxonomy.get("classes") or []),
            )
        else:
            log.warning("Class routing enabled but taxonomy fetch failed")

    channels = await ensure_server_structure(
        guild,
        taxonomy=taxonomy,
        resident_channels=RESIDENT_FINDING_CHANNELS,
    )
    log.info("Server structure ready: %d channels", len(channels))

    # Open the SQLite cache for event cursors, HUD state, etc.
    os.makedirs(os.path.dirname(DB_PATH) or "data", exist_ok=True)
    cache = BridgeCache(DB_PATH)
    await cache.__aenter__()

    # Start the event poller if all required channels exist
    activity_ch = channels.get("activity")
    signals_ch = channels.get("signals")
    alerts_ch = channels.get("alerts")
    residents_ch = channels.get("residents")
    audit_channel = channels.get("audit-log")

    # Residents with a channel of their own (Sentinel, Doctor) — their findings
    # go there instead of the shared #residents feed. A key whose channel is
    # missing just falls back, so a partial map is safe.
    resident_channels: dict[str, discord.TextChannel] = {
        key: ch
        for key in RESIDENT_FINDING_CHANNELS
        if isinstance(ch := channels.get(key), discord.TextChannel)
    }

    if activity_ch and signals_ch and alerts_ch:
        event_poller = EventPoller(
            gov_client, cache, activity_ch, signals_ch, alerts_ch,
            EVENT_POLL_INTERVAL,
            audit_channel=audit_channel,
            residents_channel=residents_ch,
            resident_channels=resident_channels,
        )
        await event_poller.start()
        log.info(
            "Event poller started (resident channels: %s)",
            ", ".join(f"#{k}" for k in resident_channels) or "none",
        )

        # Build the per-class channel map for the WS subscriber. Keys are
        # class ids (e.g. "INT"), values are Discord TextChannel objects.
        # Empty dict when class routing is disabled — subscriber falls back
        # to posting to #events only.
        class_channels: dict[str, discord.TextChannel] = {}
        if taxonomy:
            for cls in taxonomy.get("classes") or []:
                if cls.get("status") != "active":
                    continue
                cid = cls.get("id")
                if not cid:
                    continue
                ch = channels.get(f"gov-{cid.lower()}")
                if ch is not None:
                    class_channels[cid] = ch

        # Also subscribe to the broadcaster WebSocket for typed governance
        # events (lifecycle_*, knowledge_*, etc.) that the REST /api/events
        # path does not surface. Runs in parallel; either can fail without
        # taking the other down.
        # Operator-managed Phase B transition channel — looked up by ID
        # (not name) because the operator names it. None when env unset.
        phase_b_ch: discord.TextChannel | None = None
        if LEASE_PLANE_PHASE_B_CHANNEL_ID:
            ch = bot.get_channel(LEASE_PLANE_PHASE_B_CHANNEL_ID)
            if isinstance(ch, discord.TextChannel):
                phase_b_ch = ch
            else:
                log.warning(
                    "DISCORD_LEASE_PLANE_PHASE_B_CHANNEL_ID=%d resolved to %r; "
                    "expected TextChannel — Phase B routing disabled",
                    LEASE_PLANE_PHASE_B_CHANNEL_ID,
                    ch,
                )

        ws_subscriber = WSEventSubscriber(
            GOVERNANCE_URL,
            activity_ch,
            signals_ch,
            alerts_ch,
            class_channels=class_channels,
            taxonomy_reverse=(taxonomy or {}).get("reverse") or {},
            lease_plane_phase_b_channel=phase_b_ch,
            gov_client=gov_client,
            resident_channels=resident_channels,
        )
        await ws_subscriber.start()
        log.info(
            "WS event subscriber started (class routing: %s; phase-B channel: %s)",
            f"{len(class_channels)} classes" if class_channels else "disabled",
            "configured" if phase_b_ch else "disabled",
        )

    # Start the HUD updater if the channel exists
    hud_ch = channels.get("governance-hud")
    if hud_ch:
        hud_updater = HUDUpdater(
            gov_client, cache, hud_ch, HUD_UPDATE_INTERVAL,
            anima_client=anima_client,
        )
        await hud_updater.start()
        log.info("HUD updater started")

    # Start the Lumen poller if both channels exist
    art_ch = channels.get("lumen-art")
    sensor_ch = channels.get("lumen-sensors")
    if art_ch and sensor_ch:
        lumen_poller = LumenPoller(
            anima_client, art_ch, sensor_ch, SENSOR_POLL_INTERVAL,
        )
        await lumen_poller.start()
        log.info("Lumen poller started")

    # Start the Lumen Q&A digest if enabled and its channel exists
    digest_ch = channels.get("lumen-digest")
    if DIGEST_ENABLED and digest_ch and cache is not None:
        digest_poller = LumenDigestPoller(
            anima_client, digest_ch, cache,
            interval=DIGEST_INTERVAL,
            window=DIGEST_WINDOW,
            check_interval=DIGEST_CHECK_INTERVAL,
            relational_nudge_threshold=DIGEST_RELATIONAL_THRESHOLD,
        )
        await digest_poller.start()
        log.info("Lumen digest poller started")

    # Sync the slash command tree
    try:
        await bot.tree.sync()
        log.info("Slash command tree synced")
    except Exception as exc:
        log.error("Failed to sync command tree: %s", exc)

    # Post startup message to audit log
    if audit_channel:
        active = [n for n, c in [("events", event_poller), ("ws", ws_subscriber), ("HUD", hud_updater), ("Lumen", lumen_poller), ("digest", digest_poller)] if c]
        embed = discord.Embed(
            title="Bridge Online",
            description=f"Systems active: {', '.join(active) or 'none'}",
            colour=discord.Colour.green(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text=str(bot.user))
        try:
            await audit_channel.send(embed=embed)
        except discord.HTTPException as exc:
            log.warning("Failed to post startup message: %s", exc)

    _initialized = True
    log.info("All systems ready — events, HUD, Lumen, commands active")


async def shutdown_bridge() -> None:
    """Graceful shutdown: stop all background tasks and close the cache."""
    global cache, event_poller, ws_subscriber, hud_updater, lumen_poller, digest_poller, audit_channel, _initialized
    log.info("Shutting down bridge...")

    # Post shutdown message while connection is still alive
    if audit_channel is not None:
        embed = discord.Embed(
            title="Bridge Offline",
            description="Shutting down gracefully.",
            colour=discord.Colour.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        try:
            await audit_channel.send(embed=embed)
        except Exception:
            pass  # Best-effort — connection may already be closing

    # Stop all background pollers
    for name, component in [
        ("event_poller", event_poller),
        ("ws_subscriber", ws_subscriber),
        ("hud_updater", hud_updater),
        ("lumen_poller", lumen_poller),
        ("digest_poller", digest_poller),
    ]:
        if component is not None:
            try:
                await component.stop()
                log.info("Stopped %s", name)
            except Exception as exc:
                log.warning("Error stopping %s: %s", name, exc)

    # Close the SQLite cache
    if cache is not None:
        try:
            await cache.__aexit__(None, None, None)
            log.info("Cache closed")
        except Exception as exc:
            log.warning("Error closing cache: %s", exc)

    # Close persistent HTTP clients
    await gov_client.close()
    await anima_client.close()

    cache = None
    event_poller = None
    ws_subscriber = None
    hud_updater = None
    lumen_poller = None
    digest_poller = None
    audit_channel = None
    _initialized = False

    log.info("Shutdown complete")


def main():
    if not DISCORD_TOKEN:
        raise ValueError("DISCORD_BOT_TOKEN environment variable is required")
    setup_commands(bot, gov_client, anima_client)
    setup_acks(bot, gov_client)
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
