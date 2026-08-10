"""Live HUD embed builder and auto-updating loop for governance dashboard."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import discord

from bridge.cache import BridgeCache
from bridge.mcp_client import AnimaClient, GovernanceClient, fetch_agents, fetch_metrics
from bridge.tasks import cancel_tasks, create_logged_task

log = logging.getLogger(__name__)

VERDICT_EMOJI = {
    "proceed": "\U0001f7e2",   # green circle
    "guide": "\U0001f7e1",     # yellow circle
    "pause": "\U0001f534",     # red circle
    "reject": "\u26d4",        # no entry
}

NO_STATE_EMOJI = "⚪"  # white circle

# Historical shape, retained for callers that still pass an explicit default.
# The HUD itself no longer substitutes it: an agent with no metrics renders as
# "no state", not as four zeros. Rendering a placeholder vector in the same
# columns as a real reading is what made the 2026-08-10 incident invisible —
# 50 agents sat at a constant E=0.70 I=0.80 S=0.20 V=0.00 (the governance ODE
# seed, returned because the HUD was querying redacted display handles that
# resolved to no agent) and it read as "quiet fleet" rather than "broken join".
DEFAULT_METRICS = {"E": 0.0, "I": 0.0, "S": 0.0, "V": 0.0, "verdict": "guide"}


def build_hud_embed(
    agents: list[dict],
    metrics: dict[str, dict],
    connection_status: dict[str, bool] | None = None,
) -> discord.Embed:
    """Build a Discord embed summarising all active agents and their EISV metrics.

    Parameters
    ----------
    agents:
        List of dicts with ``"id"`` and ``"label"`` keys.
    metrics:
        Dict keyed by agent id, values are dicts with ``"E"``, ``"I"``,
        ``"S"``, ``"V"``, and ``"verdict"`` keys.
    """
    embed = discord.Embed(
        title="UNITARES Governance \u2014 Live",
        colour=discord.Colour.blurple(),
    )

    conn_line = ""
    if connection_status:
        parts = []
        for svc, ok in connection_status.items():
            parts.append(f"{svc}: {'OK' if ok else 'DOWN'}")
        conn_line = " | ".join(parts) + "\n"

    if not agents:
        embed.description = "No active agents"
        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        embed.set_footer(text=f"{conn_line}0 agents | 0 paused | 0 boundary | Updated {now}")
        return embed

    lines: list[str] = []
    paused = 0
    boundary = 0
    no_state = 0

    for agent in agents:
        agent_id = agent["id"]
        label = agent["label"]
        m = metrics.get(agent_id)

        if m is None:
            # No reading. Say so. Printing zeros here would put a fabricated
            # vector in the same columns as a measured one.
            no_state += 1
            lines.append(f"{NO_STATE_EMOJI} **{label}**  no state")
            continue

        verdict = m.get("verdict", "guide")
        emoji = VERDICT_EMOJI.get(verdict, "\u2753")

        if verdict == "pause":
            paused += 1
        if verdict == "guide":
            boundary += 1

        e_val = m.get("E", 0.0)
        i_val = m.get("I", 0.0)
        s_val = m.get("S", 0.0)
        v_val = m.get("V", 0.0)

        lines.append(
            f"{emoji} **{label}**  "
            f"E={e_val:.2f}  I={i_val:.2f}  S={s_val:.2f}  V={v_val:.2f}"
        )

    embed.description = "\n".join(lines)
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    footer = f"{conn_line}{len(agents)} agents | {paused} paused | {boundary} boundary"
    if no_state:
        footer += f" | {no_state} no state"
    embed.set_footer(text=f"{footer} | Updated {now}")
    return embed


class HUDUpdater:
    """Periodically updates a Discord embed with live governance metrics."""

    def __init__(
        self,
        gov_client: GovernanceClient,
        cache: BridgeCache,
        hud_channel: discord.TextChannel,
        interval: int = 30,
        anima_client: AnimaClient | None = None,
    ) -> None:
        self.gov = gov_client
        self.anima = anima_client
        self.cache = cache
        self.hud_channel = hud_channel
        self.interval = interval
        self._task: asyncio.Task | None = None
        self._message: discord.Message | None = None

    async def start(self) -> None:
        """Restore the HUD message from cache or create a new one, then start the loop."""
        cached = await self.cache.get_hud_message()
        if cached is not None:
            channel_id, message_id = cached
            try:
                self._message = await self.hud_channel.fetch_message(message_id)
                log.info("Restored HUD message %d from cache", message_id)
            except discord.NotFound:
                log.info("Cached HUD message %d not found, will create new", message_id)
                self._message = None

        if self._message is None:
            embed = build_hud_embed([], {})
            self._message = await self.hud_channel.send(embed=embed)
            await self.cache.set_hud_message(self.hud_channel.id, self._message.id)
            log.info("Created new HUD message %d", self._message.id)

        self._task = create_logged_task(self._update_loop(), name="hud-update")

    async def stop(self) -> None:
        """Cancel the update loop task."""
        await cancel_tasks(self._task)

    async def _update_loop(self) -> None:
        while True:
            try:
                agents = await fetch_agents(self.gov)
                metrics = await fetch_metrics(self.gov, agents)
                conn = {
                    "Governance": self.gov.consecutive_failures == 0,
                }
                if self.anima is not None:
                    conn["Lumen"] = self.anima.is_online
                embed = build_hud_embed(agents, metrics, connection_status=conn)
                if self._message is not None:
                    try:
                        await self._message.edit(embed=embed)
                    except discord.NotFound:
                        # The HUD message was deleted externally; post a fresh one and
                        # persist the new message ID so subsequent loops use it.
                        log.warning("HUD message was deleted; creating a new one")
                        self._message = await self.hud_channel.send(embed=embed)
                        await self.cache.set_hud_message(
                            self.hud_channel.id, self._message.id
                        )
            except Exception as exc:
                log.error("HUD update error: %s", exc)
            await asyncio.sleep(self.interval)

