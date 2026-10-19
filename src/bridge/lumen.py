"""Lumen poller — sensor embeds, drawing detection, offline alerts."""

from __future__ import annotations

import asyncio
import io
import logging
from datetime import datetime, timezone

import discord

from bridge.cache import BridgeCache
from bridge.delivery import send_with_retry
from bridge.mcp_client import AnimaClient
from bridge.tasks import cancel_tasks, create_logged_task

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Embed builders
# ---------------------------------------------------------------------------

def build_sensor_embed(state: dict) -> discord.Embed:
    """Build a Discord embed from Lumen's sensor/anima state."""
    neural = state.get("neural", {})
    embed = discord.Embed(
        title="Lumen Environment",
        colour=discord.Colour.teal(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.description = (
        f"**Temp:** {state.get('ambient_temp', '?')}\u00b0C  "
        f"**Humidity:** {state.get('humidity', '?')}%\n"
        f"**Pressure:** {state.get('pressure', '?')} hPa  "
        f"**Light:** {state.get('light', '?')} lux\n"
        f"**CPU:** {state.get('cpu_temp', '?')}\u00b0C  "
        f"**Memory:** {state.get('memory_percent', '?')}%\n\n"
        f"**Neural:** \u03b4={neural.get('delta', 0):.1f} \u03b8={neural.get('theta', 0):.1f} "
        f"\u03b1={neural.get('alpha', 0):.1f} \u03b2={neural.get('beta', 0):.1f} \u03b3={neural.get('gamma', 0):.1f}"
    )
    embed.add_field(name="Warmth", value=f"{state.get('warmth', 0):.2f}", inline=True)
    embed.add_field(name="Clarity", value=f"{state.get('clarity', 0):.2f}", inline=True)
    embed.add_field(name="Stability", value=f"{state.get('stability', 0):.2f}", inline=True)
    embed.add_field(name="Presence", value=f"{state.get('presence', 0):.2f}", inline=True)
    embed.set_footer(text="Colorado")
    return embed


def build_drawing_embed(drawing: dict) -> discord.Embed:
    """Build a Discord embed for a completed Lumen drawing."""
    era = drawing.get("era", "unknown")
    manual = drawing.get("manual", False)
    embed = discord.Embed(
        title="Drawing Complete",
        colour=discord.Colour.purple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.description = f"Era: **{era}**"
    embed.add_field(name="Manual", value="Yes" if manual else "No", inline=True)
    return embed


# ---------------------------------------------------------------------------
# Lumen Poller
# ---------------------------------------------------------------------------

class LumenPoller:
    """Background tasks that poll Lumen's sensor state and drawing gallery."""

    def __init__(
        self,
        anima_client: AnimaClient,
        art_channel: discord.TextChannel,
        sensor_channel: discord.TextChannel,
        sensor_interval: int = 300,
        offline_threshold: int = 2,
        offline_mention: str = "",
        cache: BridgeCache | None = None,
        card_recent_window: int = 10,
        card_check_every: int = 6,
    ) -> None:
        self.anima = anima_client
        self.art_channel = art_channel
        self.sensor_channel = sensor_channel
        self.sensor_interval = sensor_interval
        # Persists the card's message id so a restart edits the same card
        # instead of posting a fresh one and leaving the old one frozen.
        self.cache = cache
        # Keep the card near the bottom of the channel: every Nth successful
        # tick, re-post it if it is no longer among the last K messages.
        self.card_recent_window = max(1, card_recent_window)
        self.card_check_every = max(1, card_check_every)
        self._ticks_since_card_check = 0
        # Set by the offline/recovery transitions: the next card goes below
        # the notice, and the old card is deleted rather than left frozen.
        self._card_needs_repost = False
        # Prefixed to the offline/recovery posts so they actually notify. A bare
        # embed in a channel is a record, not an alert (see config
        # LUMEN_OFFLINE_MENTION). Empty → unchanged silent behaviour.
        self.offline_mention = (offline_mention or "").strip()
        # Require N consecutive failed ticks before announcing offline. A single
        # transient transport blip (cloudflared / IPv6-loopback-proxy stutter)
        # used to flip _was_offline and produce a false "Lumen Offline → Lumen
        # Online" cycle in Discord. Threshold ≥ 2 debounces those spurious flips.
        self.offline_threshold = max(1, offline_threshold)
        self._last_drawing: str | None = None
        self._was_offline: bool = False
        self._consecutive_failures: int = 0
        self._sensor_msg: discord.Message | None = None
        self._sensor_task: asyncio.Task | None = None
        self._drawing_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Spawn the sensor and drawing poll loops."""
        # Seed last drawing from gallery to avoid posting stale art on restart
        try:
            gallery = await self.anima.fetch_gallery(limit=1)
            if gallery and isinstance(gallery, list) and gallery:
                self._last_drawing = gallery[0].get("filename")
        except Exception as exc:
            log.warning("Failed to seed last drawing from gallery: %s", exc)
        await self._restore_sensor_card()
        self._sensor_task = create_logged_task(self._sensor_loop(), name="lumen-sensor")
        self._drawing_task = create_logged_task(self._drawing_loop(), name="lumen-drawing")

    async def stop(self) -> None:
        """Cancel both background tasks."""
        await cancel_tasks(self._sensor_task, self._drawing_task)

    def _alert_kwargs(self) -> dict:
        """send() kwargs that make an outage post actually notify.

        Empty dict when no mention is configured, so the default deployment
        keeps today's silent behaviour. `allowed_mentions` is set explicitly
        rather than inherited: it pins @everyone/@here off regardless of any
        future client-wide default, while still letting the configured user or
        role through. Only the offline and recovery transitions use this.
        """
        if not self.offline_mention:
            return {}
        return {
            "content": self.offline_mention,
            "allowed_mentions": discord.AllowedMentions(
                everyone=False, users=True, roles=True
            ),
        }

    # -- Sensor loop --------------------------------------------------------

    async def _sensor_loop(self) -> None:
        while True:
            await self._sensor_tick()
            await asyncio.sleep(self.sensor_interval)

    async def _sensor_tick(self) -> None:
        try:
            state = await self.anima.fetch_state()
            if state is None:
                self._consecutive_failures += 1
                # Debounce: only declare offline once N consecutive ticks have
                # failed. Single transient blips don't count as outages.
                if (
                    not self._was_offline
                    and self._consecutive_failures >= self.offline_threshold
                ):
                    embed = discord.Embed(
                        title="Lumen Offline",
                        colour=discord.Colour.dark_red(),
                        timestamp=datetime.now(timezone.utc),
                    )
                    embed.description = "Unable to reach Lumen's sensor interface."
                    # Retried on a DNS blip; if it still fails, _was_offline
                    # stays False and the next tick tries again.
                    await send_with_retry(
                        lambda: self.sensor_channel.send(
                            embed=embed, **self._alert_kwargs()
                        ),
                        what="Lumen offline notice",
                    )
                    self._was_offline = True
                    self._card_needs_repost = True
            else:
                self._consecutive_failures = 0
                if self._was_offline:
                    recovery = discord.Embed(
                        title="Lumen Online",
                        colour=discord.Colour.green(),
                        timestamp=datetime.now(timezone.utc),
                    )
                    recovery.description = "Lumen's sensor interface is reachable again."
                    # Ping the recovery too: whoever got woken by the outage
                    # should not have to poll Discord to learn it is over.
                    await send_with_retry(
                        lambda: self.sensor_channel.send(
                            embed=recovery, **self._alert_kwargs()
                        ),
                        what="Lumen recovery notice",
                    )
                    self._card_needs_repost = True
                self._was_offline = False
                await self._update_sensor_card(build_sensor_embed(state))
        except Exception as exc:
            log.error("Sensor loop error: %s", exc)

    # -- Sensor card --------------------------------------------------------

    async def _restore_sensor_card(self) -> None:
        """Re-attach to the card posted before a restart, if it still exists."""
        if self.cache is None:
            return
        try:
            cached = await self.cache.get_sensor_message()
        except Exception as exc:
            log.warning("Could not read cached sensor card: %s", exc)
            return
        if cached is None:
            return
        channel_id, message_id = cached
        if channel_id != getattr(self.sensor_channel, "id", None):
            log.info("Cached sensor card is in another channel; will post a new one")
            return
        try:
            self._sensor_msg = await self.sensor_channel.fetch_message(message_id)
            log.info("Restored sensor card %d from cache", message_id)
        except discord.NotFound:
            log.info("Cached sensor card %d not found; will post a new one", message_id)
        except Exception as exc:
            log.warning("Could not restore sensor card %d: %s", message_id, exc)

    async def _card_is_recent(self, card: discord.Message) -> bool:
        """True when the card is among the channel's last K messages.

        Fails open (True): a history read error must not churn the card.
        """
        try:
            async for message in self.sensor_channel.history(
                limit=self.card_recent_window
            ):
                if message.id == card.id:
                    return True
        except Exception as exc:
            log.debug("Sensor card history check failed: %s", exc)
            return True
        return False

    async def _update_sensor_card(self, embed: discord.Embed) -> None:
        """Edit the one live card in place; re-post it only when it is buried."""
        card = self._sensor_msg
        repost = self._card_needs_repost
        if card is not None and not repost:
            try:
                await card.edit(embed=embed)
            except discord.NotFound:
                card = self._sensor_msg = None
            else:
                self._ticks_since_card_check += 1
                if self._ticks_since_card_check < self.card_check_every:
                    return
                self._ticks_since_card_check = 0
                if await self._card_is_recent(card):
                    return
                repost = True

        new_card = await self.sensor_channel.send(embed=embed)
        self._sensor_msg = new_card
        self._card_needs_repost = False
        self._ticks_since_card_check = 0
        if self.cache is not None:
            try:
                await self.cache.set_sensor_message(
                    self.sensor_channel.id, new_card.id
                )
            except Exception as exc:
                log.warning("Could not persist sensor card id: %s", exc)
        if card is not None and repost:
            # Only the bot's own previous card; leaving it would show a second,
            # frozen "Lumen Environment" reading above the live one.
            try:
                await card.delete()
            except discord.NotFound:
                pass
            except Exception as exc:
                log.warning("Could not delete superseded sensor card: %s", exc)

    # -- Drawing loop -------------------------------------------------------

    async def _drawing_loop(self) -> None:
        while True:
            try:
                gallery = await self.anima.fetch_gallery(limit=1)
                if gallery and isinstance(gallery, list) and len(gallery) > 0:
                    newest = gallery[0]
                    filename = newest.get("filename", "")
                    if filename and filename != self._last_drawing:
                        self._last_drawing = filename
                        # Fetch the actual image bytes
                        image_data = await self.anima.fetch_drawing_image(filename)
                        embed = build_drawing_embed(newest)
                        if image_data:
                            file = discord.File(
                                io.BytesIO(image_data), filename=filename,
                            )
                            embed.set_image(url=f"attachment://{filename}")
                            await self.art_channel.send(embed=embed, file=file)
                        else:
                            await self.art_channel.send(embed=embed)
            except Exception as exc:
                log.error("Drawing loop error: %s", exc)
            await asyncio.sleep(60)
