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
from bridge.agent_labels import AgentLabelCache
from bridge.cache import FindingDeliveryCache
from bridge.delivery import DiscordSender
from bridge.embeds import (
    attention_severity, event_to_embed, finding_destination,
    PHASE_B_TYPES, finding_needs_operator, is_critical_event,
    is_dismissed_resolution, is_finding_event, parse_event_timestamp,
    phase_b_title_colour,
)
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


# Keys that name an event's subject. ``label`` is what governance's lifecycle
# broadcasts (lifecycle_silent*, lifecycle_resident_paused) carry.
_AGENT_NAME_KEYS = (
    "resident_name",
    "resident_label",
    "agent_label",
    "agent_name",
    "label",
)


def _event_agent_name(event: dict) -> Optional[str]:
    for key in _AGENT_NAME_KEYS:
        value = event.get(key)
        if value:
            return str(value)
    return None


def _format_minutes(value: object) -> str:
    """``95m`` / ``3.5h`` / ``2.1d`` for a minute count; ``str()`` otherwise."""
    try:
        minutes = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return str(value)
    if minutes < 120:
        return f"{minutes:.0f}m"
    if minutes < 2880:
        return f"{minutes / 60:.1f}h"
    return f"{minutes / 1440:.1f}d"


def _lifecycle_description(phase: str, event: dict) -> str:
    """Say how long and why, instead of an empty ``reason``."""
    if phase in ("silent", "silent_critical"):
        silence = event.get("silence_duration_minutes")
        if silence is not None:
            text = f"Silent {_format_minutes(silence)}"
            expected = event.get("expected_interval_minutes")
            if expected:
                text += f" (expected every {_format_minutes(expected)})"
            if event.get("proxy_alive"):
                text += "; host proxy is alive"
            return text
    elif phase == "resident_paused":
        paused = event.get("paused_minutes")
        text = f"Paused {_format_minutes(paused)}" if paused is not None else "Paused (duration unknown)"
        note = event.get("note")
        return f"{text}: {note}" if note else text
    return event.get("reason") or ""


def _normalised(text: str) -> str:
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _title_repeats_type(title: str, event_type: str) -> bool:
    """True when the title already says what the raw event type says."""
    return _normalised(event_type) in _normalised(title)


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
    # Phase B has its own renderer whichever name it arrives under; the
    # `_finding` one would otherwise take the generic finding path.
    if is_finding_event(event) and t not in PHASE_B_TYPES:
        return event_to_embed({
            **event,
            "severity": attention_severity(event, critical=is_critical_broadcaster_event(event)),
        })

    agent_id = event.get("resident_id") or event.get("agent_id") or ""
    short_id = str(agent_id)[:12] if agent_id else ""
    agent_name = _event_agent_name(event)
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
        description = _lifecycle_description(phase, event)
        if phase in ("paused", "stuck_detected", "silent_critical", "resident_paused"):
            colour = discord.Colour.red()
        elif phase in ("loop_detected", "silent"):
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
    elif t in PHASE_B_TYPES:
        title, colour = phase_b_title_colour(event)
        description = event.get("message") or ""

    # Discord embed description cap is 4096 chars; be defensive.
    if description and len(description) > 1000:
        description = description[:997] + "..."

    embed = discord.Embed(title=title, description=description, colour=colour)
    embed.add_field(name="Agent", value=str(agent), inline=True)
    if not _title_repeats_type(title, t):
        embed.add_field(name="Type", value=t, inline=True)
    if is_critical_broadcaster_event(event):
        embed.add_field(name="Severity", value=attention_severity(event, critical=True), inline=True)
    if t == "lifecycle_silent_critical":
        embed.add_field(
            name="Next check",
            value="Verify expected cadence and last completed work. Silence alone is not proof of a service outage.",
            inline=False,
        )
    # Time goes in the embed timestamp (rendered in the reader's zone); the
    # footer is the stable machine type, not a raw ISO string.
    embed.timestamp = parse_event_timestamp(ts)
    embed.set_footer(text=t)
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
    """Also mirror to alerts channel if this fires.

    A finding is never mirrored: this answers whether its one destination is
    #alerts, by severity alone (the REST twin, :func:`is_critical_event`).
    """
    if is_finding_event(event):
        return finding_needs_operator(event)
    if is_critical_event(event):
        return True
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
        resident_channels: Optional[dict[str, discord.TextChannel]] = None,
        residents_channel: Optional[discord.TextChannel] = None,
        delivery_cache: FindingDeliveryCache | None = None,
        label_cache: AgentLabelCache | None = None,
    ) -> None:
        self.ws_url = ws_url_from_http(governance_url)
        self.gov = gov_client
        # Names a subject that arrives with only an agent_id; None leaves the
        # UUID fragment.
        self.label_cache = label_cache
        self.delivery_cache = delivery_cache
        self.residents_channel = residents_channel
        self.activity_channel = activity_channel
        self.signals_channel = signals_channel
        self.alerts_channel = alerts_channel
        # Per-class channels: {"INT": channel, "ENT": channel, ...}. When a
        # matched event has a class in this map, it's ALSO posted to that
        # channel (in addition to its #activity/#signals post). Disabled by passing
        # None or an empty dict.
        self.class_channels = class_channels or {}
        # Per-resident finding channels: {"sentinel": channel, ...}. A finding
        # from a resident in this map goes to its channel instead of the shared
        # signals feed — the WS twin of the REST poller's routing. Residents
        # without an entry use the same #residents fallback as REST when wired.
        self.resident_channels = resident_channels or {}
        self.taxonomy_reverse = taxonomy_reverse or {}
        # Operator-managed channel (#lease-plane-phase-b, looked up by id) for
        # the PHASE_B_TYPES events Sentinel emits after a §6.1 criterion (or
        # overall promotable) flips status. None disables; events still flow
        # via the default signals/resident-findings path.
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
        # Shared queue + supervised send loop (bridge/delivery.py): stable
        # nonces, outage circuit breaker, #alerts express lane. Items put on
        # the queue are (channel, embed, source_event) tuples.
        self._sender = DiscordSender(
            name="WS events",
            record_receipt=self._record_receipt,
            maxsize=_SEND_QUEUE_MAX,
            priority_channel=lambda: self.alerts_channel,
            delivery_cache=delivery_cache,
        )
        # (event_type, agent_id) -> (monotonic ts of last post, metric value)
        # for repeat-drift suppression; see _suppress_repeat_drift().
        self._drift_last: dict[tuple[str, str], tuple[float, Optional[float]]] = {}

    @property
    def _send_queue(self) -> asyncio.Queue:
        """The sender's queue (kept as an attribute name for callers/tests)."""
        return self._sender.queue

    @_send_queue.setter
    def _send_queue(self, queue: asyncio.Queue) -> None:
        self._sender.queue = queue

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
        if self.label_cache is not None:
            await self.label_cache.close()

    async def _with_agent_label(self, event: dict) -> dict:
        """Add ``agent_label`` from the label cache when the event names no one."""
        if self.label_cache is None or _event_agent_name(event):
            return event
        agent_id = event.get("resident_id") or event.get("agent_id")
        if not agent_id:
            return event
        label = await self.label_cache.label_for(agent_id)
        return {**event, "agent_label": label} if label else event

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

    def _resident_channel(self, event: dict) -> Optional[discord.TextChannel]:
        """The one channel a finding goes to — the REST poller's rule exactly."""
        return finding_destination(
            event,
            alerts_channel=self.alerts_channel,
            resident_channels=self.resident_channels,
            residents_channel=self.residents_channel,
        )

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
        if config.SUPPRESS_DISMISSED_RESOLUTIONS and is_dismissed_resolution(event):
            await self._record_receipt(
                "bridge.suppressed",
                source_event=event,
                reason="dismissed_resolution",
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
        if not event.get("type") or event.get("type") == "eisv_update":
            return
        embed = broadcaster_event_to_embed(await self._with_agent_label(event))
        if embed is None:
            return
        bucket = classify_broadcaster_event(event)
        target = self.activity_channel if bucket == "activity" else self.signals_channel
        resident_ch = self._resident_channel(event)
        if resident_ch is not None:
            target = resident_ch
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
            # No return: the #alerts mirror below can still evict a routine
            # post (SendQueue) and must not be lost with the routine copy.
        # Findings were routed to their one channel above (#alerts when they
        # need the operator); only non-findings mirror.
        if not is_finding_event(event) and is_critical_broadcaster_event(event):
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
        # without reading all of #activity and #signals.
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
            event.get("type") in PHASE_B_TYPES
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
            "source_severity": attention_severity(
                source_event, critical=is_critical_broadcaster_event(source_event),
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
        """Drain the send queue (see :class:`bridge.delivery.DiscordSender`)."""
        await self._sender.run()
