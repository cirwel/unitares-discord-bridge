"""Poll governance-mcp for events and dispatch Discord embeds."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

import discord

from bridge import config
from bridge.agent_tags import AgentTagResolver, has_any, is_positively_nonresident
from bridge.cache import BridgeCache, FindingDeliveryCache
from bridge.embeds import (
    attention_severity,
    classify_rest_event,
    event_to_embed,
    finding_destination,
    is_critical_event,
    is_dismissed_resolution,
    is_finding_event,
)
from bridge.delivery import is_transient_send_error, send_with_retry
from bridge.mcp_client import GovernanceClient, fetch_agents
from bridge.tasks import cancel_tasks, create_logged_task

log = logging.getLogger(__name__)

_RESIDENT_NAME_KEYS = (
    "resident_name",
    "resident_label",
    "agent_name",
    "agent_label",
)


def _needs_resident_label(event: dict) -> bool:
    return bool(
        (event.get("resident_id") or event.get("agent_id"))
        and not any(event.get(key) for key in _RESIDENT_NAME_KEYS)
    )


def _with_resident_label(event: dict, label_by_id: dict[str, str]) -> dict:
    agent_id = event.get("resident_id") or event.get("agent_id")
    if not agent_id or not _needs_resident_label(event):
        return event
    label = label_by_id.get(str(agent_id))
    if not label:
        return event
    return {**event, "agent_label": label}


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
        resident_channels: dict[str, discord.TextChannel] | None = None,
        delivery_cache: FindingDeliveryCache | None = None,
        tag_resolver: AgentTagResolver | None = None,
        embodied_channel: discord.TextChannel | None = None,
    ) -> None:
        self.gov = gov_client
        # Reads the subject agent's governance tags for resident-only and
        # embodied routing. None disables both (every event posts as before).
        self.tag_resolver = tag_resolver
        # Lumen-family channel for an embodied agent's state chatter
        # (config.EMBODIED_STATE_EVENT_TYPES). None keeps it in #signals.
        self.embodied_channel = embodied_channel
        self.cache = cache
        self.delivery_cache = delivery_cache
        self.activity_channel = activity_channel
        self.signals_channel = signals_channel
        self.alerts_channel = alerts_channel
        self.interval = interval
        self.audit_channel = audit_channel
        self.residents_channel = residents_channel
        # {"sentinel": channel, "doctor": channel} — a resident with its own
        # channel gets its findings there; everyone else falls back to
        # ``residents_channel``.
        self.resident_channels = resident_channels or {}
        self._task: asyncio.Task | None = None
        self._send_task: asyncio.Task | None = None
        self._gov_alert_sent: bool = False
        # Items are (channel, embed, source_event) or, once re-queued after
        # Discord was unreachable, (channel, embed, source_event, redeliveries).
        self._message_queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=100)
        self._redelivery_tasks: set[asyncio.Task] = set()

    def _finding_channel(self, event: dict) -> discord.TextChannel | None:
        """The one channel a finding is posted to, or ``None`` if it isn't one.

        #alerts when the finding needs the operator (critical by default);
        otherwise a resident with its own channel (Sentinel, Doctor) wins and
        every other resident falls back to the shared #residents feed. ``None``
        is also returned when neither exists, so the caller routes by
        activity/signals as before.
        """
        return finding_destination(
            event,
            alerts_channel=self.alerts_channel,
            resident_channels=self.resident_channels,
            residents_channel=self.residents_channel,
        )

    async def _subject_tags(self, event: dict) -> frozenset[str] | None:
        """Governance tags of the event's subject agent, when routing needs them."""
        if self.tag_resolver is None:
            return None
        t = str(event.get("type") or "").lower()
        if t not in config.RESIDENT_ONLY_EVENT_TYPES and t not in config.EMBODIED_STATE_EVENT_TYPES:
            return None
        return await self.tag_resolver.tags(event.get("agent_id"))

    @staticmethod
    def _is_nonresident_subject(event: dict, tags: frozenset[str] | None) -> bool:
        """A resident-only event type whose subject governance positively marks
        non-resident (e.g. tagged ``ephemeral``). Unknown, empty or failed
        lookups are NOT non-resident: they post as before."""
        t = str(event.get("type") or "").lower()
        return t in config.RESIDENT_ONLY_EVENT_TYPES and is_positively_nonresident(
            tags,
            nonresident_tags=config.NONRESIDENT_AGENT_TAGS,
            resident_tags=config.RESIDENT_AGENT_TAGS,
        )

    @classmethod
    def _suppression_reason(cls, event: dict, tags: frozenset[str] | None) -> str | None:
        """Why this event is receipted but not posted, or ``None`` to post it.

        A critical event is never suppressed here: a non-resident's critical
        risk crossing still goes to #alerts (see the routing below).
        """
        if config.SUPPRESS_DISMISSED_RESOLUTIONS and is_dismissed_resolution(event):
            return "dismissed_resolution"
        if cls._is_nonresident_subject(event, tags) and not is_critical_event(event):
            return "non_resident_agent"
        return None

    async def start(self) -> None:
        """Spawn the poll and send loops as background tasks."""
        self._task = create_logged_task(self._poll_loop(), name="event-poll")
        self._send_task = create_logged_task(self._send_loop(), name="event-send")

    async def stop(self) -> None:
        """Cancel both background tasks."""
        await cancel_tasks(self._task, self._send_task, *self._redelivery_tasks)

    async def _poll_loop(self) -> None:
        while True:
            await self._poll_loop_once()
            self._write_heartbeat()
            await asyncio.sleep(self.interval)

    def _write_heartbeat(self) -> None:
        """Rewrite the liveness heartbeat after each poll iteration. An external
        watchdog reads its mtime to tell a healthy loop from a wedged one
        (process alive but not iterating) — independent of log verbosity.
        Best-effort: heartbeat I/O must never break the poll loop."""
        path = config.BRIDGE_HEARTBEAT_PATH
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as fh:
                fh.write(datetime.now(timezone.utc).isoformat())
        except Exception:  # noqa: BLE001 — liveness must not depend on disk
            log.debug("heartbeat write failed", exc_info=True)

    async def _poll_loop_once(self) -> None:
        try:
            cursor = await self.cache.get_event_cursor()
            events = await self.gov.fetch_events(since=cursor)
            if events is None:
                # Transient governance failure — wait for the next poll. Do NOT
                # fall through to the stale-cursor probe: during a governance
                # stall the probe fails too, and treating that as "server max
                # 0" reset the cursor and replayed the entire feed to Discord
                # on every stall window (live incident 2026-07-28).
                return
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
                if probe is None:
                    # The probe itself failed — no evidence about the server's
                    # feed either way. Try again next poll; never reset on a
                    # failed read.
                    return
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
            label_by_id: dict[str, str] = {}
            if any(_needs_resident_label(event) for event in events):
                try:
                    label_by_id = {
                        str(agent["id"]): str(agent["label"])
                        for agent in await fetch_agents(self.gov)
                        if agent.get("id") and agent.get("label")
                    }
                except Exception as exc:
                    log.warning("Failed to resolve resident labels for events: %s", exc)
            for event in events:
                # Drop suppressed (noisy) event types — e.g. knowledge_read —
                # before posting. The cursor still advances over them below, so
                # they're skipped, not re-fetched.
                if event.get("type") in config.SUPPRESSED_EVENT_TYPES:
                    await self._record_receipt(
                        "bridge.suppressed",
                        source_event=event,
                        reason="suppressed_event_type",
                    )
                    continue
                # Per-event try/except: a single malformed event (e.g. a
                # drift_alert with value=null that used to crash the embed
                # builder) must not stall the whole batch or block the cursor
                # — otherwise the next poll fetches the same poisoned batch
                # and the feed stays silent forever.
                try:
                    tags = await self._subject_tags(event)
                    reason = self._suppression_reason(event, tags)
                    if reason is not None:
                        await self._record_receipt(
                            "bridge.suppressed", source_event=event, reason=reason,
                        )
                        continue
                    event_for_embed = _with_resident_label(event, label_by_id)
                    embed = event_to_embed(event_for_embed)
                    if self._is_nonresident_subject(event, tags):
                        # Critical (else it was suppressed above): page the
                        # operator once, skip the routine #signals copy.
                        await self._message_queue.put((self.alerts_channel, embed, event))
                        continue
                    target = self._finding_channel(event)
                    if target is None and (
                        self.embodied_channel is not None
                        and str(event.get("type") or "").lower() in config.EMBODIED_STATE_EVENT_TYPES
                        and has_any(tags, config.EMBODIED_AGENT_TAGS)
                    ):
                        target = self.embodied_channel
                    if target is None:
                        bucket = classify_rest_event(event)
                        target = (
                            self.activity_channel if bucket == "activity"
                            else self.signals_channel
                        )
                    await self._message_queue.put((target, embed, event))
                    # A finding already went to its one channel (#alerts when
                    # critical); everything else mirrors critical to #alerts.
                    if not is_finding_event(event) and is_critical_event(event):
                        await self._message_queue.put((self.alerts_channel, embed, event))
                except Exception as exc:
                    log.error(
                        "Failed to dispatch event %s (type=%s): %s",
                        event.get("event_id"), event.get("type"), exc,
                        exc_info=exc,
                    )
                    await self._record_receipt(
                        "bridge.delivery_failed",
                        source_event=event,
                        reason="embed_dispatch_failed",
                        error=str(exc),
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
                await self._message_queue.put((self.alerts_channel, warn, None))
            elif self.gov.consecutive_failures == 0 and self._gov_alert_sent:
                self._gov_alert_sent = False
                recovered = discord.Embed(
                    title="Governance MCP Recovered",
                    colour=discord.Colour.green(),
                )
                await self._message_queue.put((self.alerts_channel, recovered, None))
        except Exception as exc:
            log.error("Event poll error: %s", exc, exc_info=exc)

    def _receipt_payload(
        self,
        event_type: str,
        source_event: dict,
        channel: discord.TextChannel | None = None,
        *,
        discord_message_id: int | str | None = None,
        reason: str | None = None,
        error: str | None = None,
    ) -> dict:
        payload = {
            "event_type": event_type,
            "source_event_id": source_event.get("event_id"),
            "source_event_type": source_event.get("type"),
            "source_agent_id": source_event.get("agent_id"),
            "source_severity": attention_severity(
                source_event, critical=is_critical_event(source_event),
            ),
        }
        if channel is not None:
            payload.update({
                "channel_key": getattr(channel, "name", None),
                "discord_channel_id": str(getattr(channel, "id", "")),
            })
        if discord_message_id is not None:
            payload["discord_message_id"] = str(discord_message_id)
        if reason:
            payload["reason"] = reason
        if error:
            payload["error"] = error
        return payload

    async def _record_receipt(
        self,
        event_type: str,
        *,
        source_event: dict | None,
        channel: discord.TextChannel | None = None,
        discord_message_id: int | str | None = None,
        reason: str | None = None,
        error: str | None = None,
    ) -> None:
        if not source_event:
            return
        await self.gov.record_bridge_event(
            self._receipt_payload(
                event_type,
                source_event,
                channel,
                discord_message_id=discord_message_id,
                reason=reason,
                error=error,
            )
        )

    async def _deliver(self, channel, embed, source_event) -> tuple[int | None, bool]:
        """One logical send, retrying transient connection errors."""
        async def attempt() -> tuple[int | None, bool]:
            if self.delivery_cache is not None:
                return await self.delivery_cache.send(channel, embed, source_event)
            message = await channel.send(embed=embed)
            return getattr(message, "id", None), False

        return await send_with_retry(attempt, what="event poller send")

    async def _redeliver_later(self, item: tuple, delay: float) -> None:
        await asyncio.sleep(delay)
        try:
            self._message_queue.put_nowait(item)
        except asyncio.QueueFull:
            channel, _embed, source_event, *_ = item
            await self._record_receipt(
                "bridge.delivery_failed",
                source_event=source_event,
                channel=channel,
                reason="discord_unreachable",
                error="send queue full at redelivery",
            )

    async def _handle_unreachable(self, item: tuple, exc: BaseException) -> None:
        """Re-queue a message Discord could not be reached for, a bounded number
        of times; record the loss durably once redelivery is exhausted."""
        channel, embed, source_event, *rest = item
        redeliveries = rest[0] if rest else 0
        if redeliveries < config.SEND_REDELIVERIES:
            log.warning(
                "Discord unreachable (%s); redelivery %d/%d in %.0fs",
                exc, redeliveries + 1, config.SEND_REDELIVERIES,
                config.SEND_REDELIVERY_DELAY_SECONDS,
            )
            task = create_logged_task(
                self._redeliver_later(
                    (channel, embed, source_event, redeliveries + 1),
                    config.SEND_REDELIVERY_DELAY_SECONDS,
                ),
                name="event-redeliver",
            )
            self._redelivery_tasks.add(task)
            task.add_done_callback(self._redelivery_tasks.discard)
            return
        log.error("Discord unreachable (%s); giving up on message", exc)
        await self._record_receipt(
            "bridge.delivery_failed",
            source_event=source_event,
            channel=channel,
            reason="discord_unreachable",
            error=str(exc),
        )

    async def _send_loop(self) -> None:
        while True:
            item = await self._message_queue.get()
            channel, embed, source_event, *_ = item
            try:
                message_id, duplicate = await self._deliver(channel, embed, source_event)
                await self._record_receipt(
                    "bridge.suppressed" if duplicate else "bridge.delivery",
                    source_event=source_event,
                    channel=channel,
                    discord_message_id=message_id,
                    reason="duplicate_finding_delivery" if duplicate else None,
                )
            except discord.RateLimited as exc:
                # Raised when max_ratelimit_timeout is set and the retry-after exceeds it.
                # Respect Discord's back-off and re-queue rather than dropping the message.
                log.warning("Global rate limit hit; retrying in %.1fs", exc.retry_after)
                await self._record_receipt(
                    "bridge.rate_limited",
                    source_event=source_event,
                    channel=channel,
                    reason="discord_rate_limited",
                )
                await asyncio.sleep(exc.retry_after)
                await self._message_queue.put((channel, embed, source_event))
            except discord.HTTPException as exc:
                if exc.status == 429:
                    # Per-route 429 that discord.py's internal limiter did not absorb.
                    # Parse Retry-After from the response headers, fall back to 5 s.
                    retry_after = float(exc.response.headers.get("Retry-After", 5))
                    log.warning("Rate limited (HTTP 429); retrying in %.1fs", retry_after)
                    await self._record_receipt(
                        "bridge.rate_limited",
                        source_event=source_event,
                        channel=channel,
                        reason="discord_http_429",
                    )
                    await asyncio.sleep(retry_after)
                    await self._message_queue.put((channel, embed, source_event))
                else:
                    log.warning("Discord send failed: %s", exc)
                    await self._record_receipt(
                        "bridge.delivery_failed",
                        source_event=source_event,
                        channel=channel,
                        reason="discord_http_exception",
                        error=str(exc),
                    )
            except Exception as exc:
                if is_transient_send_error(exc):
                    # Retries inside _deliver are spent; re-queue (bounded)
                    # instead of dropping the message on a DNS blip.
                    await self._handle_unreachable(item, exc)
                    await asyncio.sleep(0.15)
                    continue
                # Anything not covered above (malformed embed state, etc.)
                # must not kill the send loop: the poll loop keeps
                # running and the heartbeat stays fresh, so a dead sender is a
                # silent Discord outage the watchdog can't see. Log, record,
                # drop this message, keep draining the queue.
                log.error(
                    "Unexpected send error (%s); dropping message",
                    exc, exc_info=exc,
                )
                await self._record_receipt(
                    "bridge.delivery_failed",
                    source_event=source_event,
                    channel=channel,
                    reason="send_loop_unexpected_error",
                    error=str(exc),
                )
            # 150 ms pacing between sends to stay well under Discord's per-route burst
            # limit — this is not a retry delay; rate limit retries are handled above.
            await asyncio.sleep(0.15)
