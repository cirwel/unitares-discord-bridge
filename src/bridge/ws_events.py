"""WebSocket subscriber for governance broadcaster events.

Complements :mod:`bridge.event_poller` — which polls ``/api/events`` and
surfaces *synthesized* high-level events (verdict_change, drift_alert,
etc.) — by listening directly to the broadcaster firehose at
``/ws/eisv``. The broadcaster emits typed governance events that the
REST path does not see:

- ``lifecycle_*`` (paused, resumed, archived, created, loop_detected,
  stuck_detected, silent_critical)
- ``identity_*`` (drift, assurance_change)
- ``knowledge_*`` (write, confidence_clamped)
- ``circuit_breaker_*`` (trip, reset)

Every one of these was invisible in Discord before this module existed.

``eisv_update`` messages are intentionally dropped here because the
existing event_poller + HUD already cover the per-check-in path; this
subscriber only handles the classes of event that had no surface.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from typing import Iterable, Optional

import discord
import websockets
import websockets.exceptions

from bridge import config
from bridge.mcp_client import GovernanceClient

from bridge.tasks import cancel_tasks, create_logged_task

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Process-local broadcaster-event ring buffer
# ---------------------------------------------------------------------------
# The bridge's WS subscriber receives every typed broadcaster event. Instead
# of asking governance to replay them later, we retain the last ~1000 events
# in-memory so slash commands like /digest can aggregate over a recent window.
# Bounded size — oldest entries drop out automatically.

_EVENT_RING_MAX = 1000
_event_ring: "deque[tuple[float, dict]]" = deque(maxlen=_EVENT_RING_MAX)

# Send-queue capacity for the Discord poster. A single critical event fans out
# to up to 3 queue entries (signals + alerts mirror + class-routed channel),
# and Sentinel's fleet scan emits typed events in bursts every 10 minutes. With
# the old maxsize=100 and 150ms pacing, every scan burst saturated the queue
# and silently dropped alerts (~170 lifecycle_silent_critical drops over 4
# days). 1000 absorbs a worst-case fleet-wide burst without dropping.
_SEND_QUEUE_MAX = 1000


def record_event(event: dict) -> None:
    """Append an event to the ring buffer with a wall-clock receive timestamp."""
    _event_ring.append((time.time(), event))


def recent_events(within_seconds: float) -> list[dict]:
    """Return events received in the last ``within_seconds``, oldest first."""
    cutoff = time.time() - within_seconds
    return [evt for ts, evt in _event_ring if ts >= cutoff]


def event_ring_size() -> int:
    """Expose buffer size for tests + /digest reporting."""
    return len(_event_ring)


def _reset_event_ring_for_tests() -> None:
    """Clear the ring buffer — tests only."""
    _event_ring.clear()


# ---------------------------------------------------------------------------
# Pure helpers (testable without a network)
# ---------------------------------------------------------------------------


def ws_url_from_http(http_url: str) -> str:
    """Convert ``http(s)://host[:port]`` to ``ws(s)://host[:port]/ws/eisv``."""
    u = http_url.rstrip("/")
    if u.startswith("http://"):
        return "ws://" + u[len("http://"):] + "/ws/eisv"
    if u.startswith("https://"):
        return "wss://" + u[len("https://"):] + "/ws/eisv"
    # Assume already a ws(s) URL or host:port — best-effort append.
    return u + "/ws/eisv"


def _colour_for_tags(tags: list[str]) -> discord.Colour:
    lower = {str(t).lower() for t in tags}
    if "critical" in lower:
        return discord.Colour.red()
    if "high" in lower:
        return discord.Colour.orange()
    return discord.Colour.blue()


def broadcaster_event_to_embed(event: dict) -> Optional[discord.Embed]:
    """Map a broadcaster event dict to a Discord embed.

    Returns ``None`` if the event should be skipped entirely (most
    importantly ``eisv_update``, which is already handled elsewhere).
    Unknown event types fall through to a generic renderer so new event
    classes remain visible rather than being silently dropped.
    """
    t = event.get("type") or ""
    if not t or t == "eisv_update":
        return None

    agent_id = event.get("resident_id") or event.get("agent_id") or ""
    short_id = str(agent_id)[:12] if agent_id else ""
    agent_name = (
        event.get("resident_name")
        or event.get("resident_label")
        or event.get("agent_label")
        or event.get("agent_name")
    )
    if agent_name and short_id and str(agent_name) not in (str(agent_id), short_id):
        agent = f"{agent_name} ({short_id})"
    elif agent_name:
        agent = str(agent_name)
    else:
        agent = short_id or "system"
    ts = event.get("timestamp")

    title = t.replace("_", " ")
    description = ""
    colour = discord.Colour.blue()

    if t.startswith("lifecycle_"):
        phase = t[len("lifecycle_"):]
        title = f"Lifecycle: {phase.replace('_', ' ')}"
        description = event.get("reason") or ""
        if phase in ("paused", "stuck_detected", "silent_critical"):
            colour = discord.Colour.red()
        elif phase == "loop_detected":
            colour = discord.Colour.orange()
        elif phase == "resumed":
            colour = discord.Colour.green()
        else:
            colour = discord.Colour.blurple()
    elif t.startswith("identity_"):
        sub = t[len("identity_"):]
        title = f"Identity: {sub.replace('_', ' ')}"
        description = event.get("detail") or ""
        if t == "identity_drift":
            colour = discord.Colour.orange()
        elif t == "identity_assurance_change":
            old_tier = event.get("old_tier")
            new_tier = event.get("new_tier")
            tier_name = event.get("tier_name")
            if old_tier is not None and new_tier is not None:
                description = f"Tier {old_tier} -> {new_tier}"
                if tier_name:
                    description += f" ({tier_name})"
    elif t.startswith("knowledge_"):
        if t == "knowledge_write":
            dtype = event.get("discovery_type") or "discovery"
            summary = event.get("summary") or ""
            if len(summary) > 200:
                summary = summary[:197] + "..."
            title = f"Knowledge write: {dtype}"
            description = summary
            tags = event.get("tags") or []
            if tags:
                colour = _colour_for_tags(tags)
        elif t == "knowledge_confidence_clamped":
            title = "Knowledge: confidence clamped"
            description = event.get("summary") or ""
            colour = discord.Colour.orange()
    elif t.startswith("circuit_breaker_"):
        action = "tripped" if t == "circuit_breaker_trip" else "reset"
        title = f"Circuit breaker {action}"
        description = event.get("reason") or ""
        colour = discord.Colour.red() if action == "tripped" else discord.Colour.green()
    elif t == "lease_plane_phase_b_transition":
        surface_kind = event.get("surface_kind") or "?"
        promotable_now = event.get("promotable_now")
        promotable_before = event.get("promotable_before")
        if promotable_now is True and promotable_before is not True:
            title = f"Phase B PROMOTABLE: {surface_kind}"
            colour = discord.Colour.green()
        elif promotable_before is True and promotable_now is not True:
            title = f"Phase B regression: {surface_kind}"
            colour = discord.Colour.red()
        else:
            title = f"Phase B criterion change: {surface_kind}"
            colour = discord.Colour.blurple()
        description = event.get("message") or ""

    # Discord embed description cap is 4096 chars; be defensive.
    if description and len(description) > 1000:
        description = description[:997] + "..."

    embed = discord.Embed(title=title, description=description, colour=colour)
    embed.add_field(name="Agent", value=str(agent), inline=True)
    embed.add_field(name="Type", value=t, inline=True)
    if ts:
        embed.set_footer(text=str(ts))
    return embed


_ACTIVITY_BROADCAST_TYPES = frozenset({
    "lifecycle_created",
    "lifecycle_resumed",
    "lifecycle_archived",
    "identity_assurance_change",
    "knowledge_write",
})


def classify_broadcaster_event(event: dict) -> str:
    """Return ``"activity"`` for routine broadcaster events, ``"signals"`` otherwise.

    Routine = creation/resume/archive lifecycle, trust-tier bookkeeping, and
    knowledge writes. Signals = anything operators would want to read promptly
    (pauses, drift, circuit breaker trips, confidence clamps).
    """
    t = event.get("type") or ""
    if t in _ACTIVITY_BROADCAST_TYPES:
        return "activity"
    return "signals"


def is_critical_broadcaster_event(event: dict) -> bool:
    """Also mirror to alerts channel if this fires."""
    t = event.get("type") or ""
    if t == "circuit_breaker_trip":
        return True
    if t in (
        "lifecycle_paused",
        "lifecycle_stuck_detected",
        "lifecycle_silent_critical",
    ):
        return True
    tags = event.get("tags") or []
    if any(str(tag).lower() == "critical" for tag in tags):
        return True
    return False


# ---------------------------------------------------------------------------
# Subscriber
# ---------------------------------------------------------------------------


def resolve_violation_class(event: dict, taxonomy_reverse: Optional[dict]) -> Optional[str]:
    """Map a broadcaster event to a violation class id using the taxonomy.

    Precedence:
    1. Explicit ``violation_class`` on the payload (Watcher emits this now).
    2. Reverse-lookup by event type in ``broadcast_events``.

    Returns the class id (e.g. ``"INT"``) or ``None`` if no mapping.
    """
    explicit = event.get("violation_class")
    if isinstance(explicit, str) and explicit:
        return explicit
    if not taxonomy_reverse:
        return None
    t = event.get("type") or ""
    return (taxonomy_reverse.get("broadcast_events") or {}).get(t)


class WSEventSubscriber:
    """Subscribe to ``/ws/eisv``, dispatch typed events to Discord.

    Runs alongside :class:`bridge.event_poller.EventPoller`. If either
    fails independently, the other keeps working.
    """

    def __init__(
        self,
        governance_url: str,
        activity_channel: discord.TextChannel,
        signals_channel: discord.TextChannel,
        alerts_channel: discord.TextChannel,
        reconnect_initial: float = 1.0,
        reconnect_max: float = 30.0,
        connect_kwargs: Optional[dict] = None,
        class_channels: Optional[dict[str, discord.TextChannel]] = None,
        taxonomy_reverse: Optional[dict] = None,
        lease_plane_phase_b_channel: Optional[discord.TextChannel] = None,
        gov_client: Optional[GovernanceClient] = None,
    ) -> None:
        self.ws_url = ws_url_from_http(governance_url)
        self.gov = gov_client
        self.activity_channel = activity_channel
        self.signals_channel = signals_channel
        self.alerts_channel = alerts_channel
        # Per-class channels: {"INT": channel, "ENT": channel, ...}. When a
        # matched event has a class in this map, it's ALSO posted to that
        # channel (in addition to the main #events feed). Disabled by passing
        # None or an empty dict.
        self.class_channels = class_channels or {}
        self.taxonomy_reverse = taxonomy_reverse or {}
        # Operator-managed channel for lease_plane_phase_b_transition events
        # emitted by Sentinel after a §6.1 criterion (or overall promotable)
        # flips status. None disables; events still flow via the default
        # activity/signals path.
        self.lease_plane_phase_b_channel = lease_plane_phase_b_channel
        self.reconnect_initial = reconnect_initial
        self.reconnect_max = reconnect_max
        self._connect_kwargs = connect_kwargs or {
            "ping_interval": 20,
            "ping_timeout": 20,
        }
        self._sub_task: Optional[asyncio.Task] = None
        self._send_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._send_queue: asyncio.Queue[tuple[discord.TextChannel, discord.Embed, dict | None]] = (
            asyncio.Queue(maxsize=_SEND_QUEUE_MAX)
        )
        # (event_type, agent_id) -> (monotonic ts of last post, metric value)
        # for repeat-drift suppression; see _suppress_repeat_drift().
        self._drift_last: dict[tuple[str, str], tuple[float, Optional[float]]] = {}

    def _suppress_repeat_drift(self, event: dict) -> bool:
        """True when a drift event repeats a recently-posted one for its agent.

        identity_drift / trajectory_drift are emitted on every check-in while
        the underlying condition is static, so an unchanged metric would post
        hundreds of identical embeds a day (unitares#1370). Post when the
        metric moved more than DRIFT_REPEAT_DELTA or the re-reminder window
        elapsed; suppress the static repeats in between.
        """
        if config.DRIFT_REPEAT_WINDOW_SECONDS <= 0:
            return False
        t = event.get("type") or ""
        if t not in config.DRIFT_REPEAT_TYPES:
            return False
        key = (t, str(event.get("agent_id") or event.get("resident_id") or ""))
        raw = event.get("lineage_similarity")
        try:
            value: Optional[float] = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            value = None
        now = time.monotonic()
        prev = self._drift_last.get(key)
        if prev is not None:
            prev_ts, prev_value = prev
            within_window = (now - prev_ts) < config.DRIFT_REPEAT_WINDOW_SECONDS
            unchanged = (value is None and prev_value is None) or (
                value is not None
                and prev_value is not None
                and abs(value - prev_value) <= config.DRIFT_REPEAT_DELTA
            )
            if within_window and unchanged:
                return True
        self._drift_last[key] = (now, value)
        return False

    async def start(self) -> None:
        self._stop_event.clear()
        self._sub_task = create_logged_task(
            self._subscribe_loop(), name="ws-events-sub"
        )
        self._send_task = create_logged_task(
            self._send_loop(), name="ws-events-send"
        )

    async def stop(self) -> None:
        self._stop_event.set()
        await cancel_tasks(self._sub_task, self._send_task)

    async def _subscribe_loop(self) -> None:
        delay = self.reconnect_initial
        while not self._stop_event.is_set():
            try:
                log.info("WS events: connecting to %s", self.ws_url)
                async with websockets.connect(
                    self.ws_url, **self._connect_kwargs,
                ) as ws:
                    delay = self.reconnect_initial
                    log.info("WS events: connected")
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        try:
                            event = json.loads(raw)
                        except (json.JSONDecodeError, TypeError):
                            continue
                        if not isinstance(event, dict):
                            continue
                        # Isolate per-event failures so a single malformed
                        # payload can't tear down the WS and trigger a
                        # reconnect churn.
                        try:
                            await self._dispatch(event)
                        except Exception as exc:
                            log.error(
                                "WS events: dispatch failed for type=%s: %s",
                                event.get("type"), exc, exc_info=exc,
                            )
            except asyncio.CancelledError:
                raise
            except websockets.exceptions.ConnectionClosed as exc:
                log.info("WS events: connection closed (%s)", exc)
            except Exception as exc:
                log.warning("WS events: error (%s); retrying in %.1fs", exc, delay)
            # Backoff wait — interruptible by stop()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, self.reconnect_max)

    async def _dispatch(self, event: dict) -> None:
        # Record every typed event (including ones we don't turn into an
        # embed) so /digest can aggregate them later. This is the single
        # authoritative ingest point, so no other code needs to touch the
        # ring buffer.
        if event.get("type") and event.get("type") != "eisv_update":
            record_event(event)
        # Suppressed (noisy) types — e.g. knowledge_read — are recorded above for
        # /digest aggregation but NOT posted to Discord. This is the WS twin of
        # the REST poller's filter; knowledge_read arrives via this path.
        if event.get("type") in config.SUPPRESSED_EVENT_TYPES:
            await self._record_receipt(
                "bridge.suppressed",
                source_event=event,
                reason="suppressed_event_type",
            )
            return
        # Static drift conditions re-fire per check-in; only posts when the
        # metric moves or the re-reminder window lapses (recorded above for
        # /digest either way).
        if self._suppress_repeat_drift(event):
            await self._record_receipt(
                "bridge.suppressed",
                source_event=event,
                reason="repeat_drift",
            )
            return
        embed = broadcaster_event_to_embed(event)
        if embed is None:
            return
        bucket = classify_broadcaster_event(event)
        target = self.activity_channel if bucket == "activity" else self.signals_channel
        try:
            self._send_queue.put_nowait((target, embed, event))
        except asyncio.QueueFull:
            # Drop rather than block the websocket reader. The dashboard
            # is the authoritative event record anyway; Discord is a
            # human-facing surface with rate limits.
            log.warning("WS events: send queue full, dropping event %s",
                        event.get("type"))
            await self._record_receipt(
                "bridge.delivery_failed",
                source_event=event,
                channel=target,
                reason="send_queue_full",
            )
            return
        if is_critical_broadcaster_event(event):
            try:
                self._send_queue.put_nowait((self.alerts_channel, embed, event))
            except asyncio.QueueFull:
                await self._record_receipt(
                    "bridge.delivery_failed",
                    source_event=event,
                    channel=self.alerts_channel,
                    reason="send_queue_full",
                )
        # Per-class mirror: when an event maps to a violation class and the
        # bridge has a channel for it, post there too. This is the core value
        # of class routing — operators can subscribe to a subset of classes
        # without seeing every event in #events.
        class_id = resolve_violation_class(event, self.taxonomy_reverse)
        if class_id:
            cls_channel = self.class_channels.get(class_id)
            if cls_channel is not None:
                try:
                    self._send_queue.put_nowait((cls_channel, embed, event))
                except asyncio.QueueFull:
                    await self._record_receipt(
                        "bridge.delivery_failed",
                        source_event=event,
                        channel=cls_channel,
                        reason="send_queue_full",
                    )

        # Lease-plane Phase B transition mirror: a dedicated channel for the
        # narrow stream of "criterion N flipped" / "PROMOTABLE" / "REGRESSED"
        # findings emitted by Sentinel. Low volume (≤5 messages per
        # surface_kind over the entire pre-promotion lifetime) but high
        # signal — keep them out of the noisy general feed.
        if (
            event.get("type") == "lease_plane_phase_b_transition"
            and self.lease_plane_phase_b_channel is not None
        ):
            try:
                self._send_queue.put_nowait(
                    (self.lease_plane_phase_b_channel, embed, event)
                )
            except asyncio.QueueFull:
                await self._record_receipt(
                    "bridge.delivery_failed",
                    source_event=event,
                    channel=self.lease_plane_phase_b_channel,
                    reason="send_queue_full",
                )

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
            "source_severity": source_event.get("severity", "info"),
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
        if not self.gov or not source_event:
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

    async def _send_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                channel, embed, source_event = await self._send_queue.get()
            except asyncio.CancelledError:
                raise
            try:
                message = await channel.send(embed=embed)
                await self._record_receipt(
                    "bridge.delivery",
                    source_event=source_event,
                    channel=channel,
                    discord_message_id=getattr(message, "id", None),
                )
            except discord.RateLimited as exc:
                log.warning("WS events: rate limited, retry in %.1fs",
                            exc.retry_after)
                await self._record_receipt(
                    "bridge.rate_limited",
                    source_event=source_event,
                    channel=channel,
                    reason="discord_rate_limited",
                )
                await asyncio.sleep(exc.retry_after)
                try:
                    self._send_queue.put_nowait((channel, embed, source_event))
                except asyncio.QueueFull:
                    await self._record_receipt(
                        "bridge.delivery_failed",
                        source_event=source_event,
                        channel=channel,
                        reason="send_queue_full",
                    )
            except discord.HTTPException as exc:
                if exc.status == 429:
                    retry = float(exc.response.headers.get("Retry-After", 5))
                    await self._record_receipt(
                        "bridge.rate_limited",
                        source_event=source_event,
                        channel=channel,
                        reason="discord_http_429",
                    )
                    await asyncio.sleep(retry)
                    try:
                        self._send_queue.put_nowait((channel, embed, source_event))
                    except asyncio.QueueFull:
                        await self._record_receipt(
                            "bridge.delivery_failed",
                            source_event=source_event,
                            channel=channel,
                            reason="send_queue_full",
                        )
                else:
                    log.warning("WS events: discord send failed (%s)", exc)
                    await self._record_receipt(
                        "bridge.delivery_failed",
                        source_event=source_event,
                        channel=channel,
                        reason="discord_http_exception",
                        error=str(exc),
                    )
            except Exception as exc:
                # Anything not covered above (transport-layer errors, malformed
                # embed state) must not kill the send loop: the poll/subscribe
                # side keeps running and the heartbeat stays fresh, so a dead
                # sender is a silent Discord outage the watchdog can't see.
                # Log, record, drop this message, keep draining the queue.
                log.error(
                    "WS events: unexpected send error (%s); dropping message",
                    exc, exc_info=exc,
                )
                await self._record_receipt(
                    "bridge.delivery_failed",
                    source_event=source_event,
                    channel=channel,
                    reason="send_loop_unexpected_error",
                    error=str(exc),
                )
            # 150 ms pacing between sends — matches event_poller to stay
            # well under Discord's per-route burst limits.
            await asyncio.sleep(0.15)
