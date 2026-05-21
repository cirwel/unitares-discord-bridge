"""Poll governance-mcp for events and dispatch Discord embeds."""

from __future__ import annotations

import asyncio
import logging

import discord

from bridge.cache import BridgeCache
from bridge.embeds import classify_rest_event, event_to_embed, is_critical_event
from bridge.mcp_client import GovernanceClient
from bridge.tasks import cancel_tasks, create_logged_task

log = logging.getLogger(__name__)


class EventPoller:
    """Periodically fetches governance events and queues Discord messages."""

    def __init__(
        self,
        gov_client: GovernanceClient,
        cache: BridgeCache,
        activity_channel: discord.TextChannel,
        signals_channel: discord.TextChannel,
        alerts_channel: discord.TextChannel,
        interval: int = 10,
        audit_channel: discord.TextChannel | None = None,
        residents_channel: discord.TextChannel | None = None,
    ) -> None:
        self.gov = gov_client
        self.cache = cache
        self.activity_channel = activity_channel
        self.signals_channel = signals_channel
        self.alerts_channel = alerts_channel
        self.interval = interval
        self.audit_channel = audit_channel
        self.residents_channel = residents_channel
        self._task: asyncio.Task | None = None
        self._send_task: asyncio.Task | None = None
        self._gov_alert_sent: bool = False
        self._message_queue: asyncio.Queue[tuple[discord.TextChannel, discord.Embed]] = (
            asyncio.Queue(maxsize=100)
        )

    async def start(self) -> None:
        """Spawn the poll and send loops as background tasks."""
        self._task = create_logged_task(self._poll_loop(), name="event-poll")
        self._send_task = create_logged_task(self._send_loop(), name="event-send")

    async def stop(self) -> None:
        """Cancel both background tasks."""
        await cancel_tasks(self._task, self._send_task)

    async def _poll_loop(self) -> None:
        while True:
            await self._poll_loop_once()
            await asyncio.sleep(self.interval)

    async def _poll_loop_once(self) -> None:
        try:
            cursor = await self.cache.get_event_cursor()
            events = await self.gov.fetch_events(since=cursor)
            # The REST /api/events endpoint supplements in-memory events from
            # the audit DB, which uses UUID event_ids. The cursor protocol is
            # int-only, so those events are incompatible — skip them at ingest
            # to avoid re-posting the same batch every poll cycle. They remain
            # visible on the governance dashboard.
            skipped_non_int = sum(
                1 for e in events if not isinstance(e.get("event_id"), int)
            )
            if skipped_non_int:
                log.debug(
                    "Skipped %d non-int-id event(s) from REST batch (audit supplement)",
                    skipped_non_int,
                )
            events = [e for e in events if isinstance(e.get("event_id"), int)]
            # Governance's in-memory event counter resets to 1 on restart, but
            # our cursor persists in SQLite — so a cursor captured before a
            # restart silently filters every subsequent poll to empty until
            # the counter catches back up. Detect the gap by probing from 0
            # when the filtered result is empty: if the server's max int
            # event_id is now less than our cursor, reset to 0.
            if not events and cursor > 0:
                probe = await self.gov.fetch_events(since=0, limit=50)
                probe_ints = [
                    e["event_id"] for e in probe
                    if isinstance(e.get("event_id"), int)
                ]
                server_max = max(probe_ints, default=0)
                if server_max < cursor:
                    log.warning(
                        "Stale event cursor (%d > server max %d); "
                        "resetting — governance likely restarted",
                        cursor, server_max,
                    )
                    await self.cache.set_event_cursor(0)
                    return
            for event in events:
                # Per-event try/except: a single malformed event (e.g. a
                # drift_alert with value=null that used to crash the embed
                # builder) must not stall the whole batch or block the cursor
                # — otherwise the next poll fetches the same poisoned batch
                # and the feed stays silent forever.
                try:
                    embed = event_to_embed(event)
                    is_finding = event.get("type", "").endswith("_finding")
                    if is_finding and self.residents_channel is not None:
                        await self._message_queue.put((self.residents_channel, embed))
                    else:
                        bucket = classify_rest_event(event)
                        target = (
                            self.activity_channel if bucket == "activity"
                            else self.signals_channel
                        )
                        await self._message_queue.put((target, embed))
                    if is_critical_event(event):
                        await self._message_queue.put((self.alerts_channel, embed))
                except Exception as exc:
                    log.error(
                        "Failed to dispatch event %s (type=%s): %s",
                        event.get("event_id"), event.get("type"), exc,
                        exc_info=exc,
                    )
            if events:
                # Advance past every fetched event — including ones that failed
                # to dispatch — so a poison-pill can't lock the cursor.
                await self.cache.set_event_cursor(
                    max(e["event_id"] for e in events)
                )
            if self.gov.consecutive_failures >= 3 and not self._gov_alert_sent:
                self._gov_alert_sent = True
                warn = discord.Embed(
                    title="Governance MCP Unreachable",
                    colour=discord.Colour.dark_red(),
                )
                await self._message_queue.put((self.alerts_channel, warn))
            elif self.gov.consecutive_failures == 0 and self._gov_alert_sent:
                self._gov_alert_sent = False
                recovered = discord.Embed(
                    title="Governance MCP Recovered",
                    colour=discord.Colour.green(),
                )
                await self._message_queue.put((self.alerts_channel, recovered))
        except Exception as exc:
            log.error("Event poll error: %s", exc, exc_info=exc)

    async def _send_loop(self) -> None:
        while True:
            channel, embed = await self._message_queue.get()
            try:
                await channel.send(embed=embed)
            except discord.RateLimited as exc:
                # Raised when max_ratelimit_timeout is set and the retry-after exceeds it.
                # Respect Discord's back-off and re-queue rather than dropping the message.
                log.warning("Global rate limit hit; retrying in %.1fs", exc.retry_after)
                await asyncio.sleep(exc.retry_after)
                await self._message_queue.put((channel, embed))
            except discord.HTTPException as exc:
                if exc.status == 429:
                    # Per-route 429 that discord.py's internal limiter did not absorb.
                    # Parse Retry-After from the response headers, fall back to 5 s.
                    retry_after = float(exc.response.headers.get("Retry-After", 5))
                    log.warning("Rate limited (HTTP 429); retrying in %.1fs", retry_after)
                    await asyncio.sleep(retry_after)
                    await self._message_queue.put((channel, embed))
                else:
                    log.warning("Discord send failed: %s", exc)
            # 150 ms pacing between sends to stay well under Discord's per-route burst
            # limit — this is not a retry delay; rate limit retries are handled above.
            await asyncio.sleep(0.15)
