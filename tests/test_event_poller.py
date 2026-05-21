"""Tests for EventPoller finding/lifecycle routing."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from bridge.event_poller import EventPoller


def _make_poller(
    events: list[dict],
    *,
    residents_channel: discord.TextChannel | None = None,
    cursor: int = 0,
    probe_events: list[dict] | None = None,
) -> tuple[EventPoller, list[tuple[str, discord.Embed]]]:
    """Build an EventPoller wired to fake gov client + capture queue.

    If ``probe_events`` is provided, ``fetch_events`` returns ``events`` on
    the first call (the real poll) and ``probe_events`` on subsequent calls
    (the restart-detection probe from ``since=0``).
    """
    gov = MagicMock()
    if probe_events is None:
        gov.fetch_events = AsyncMock(return_value=events)
    else:
        gov.fetch_events = AsyncMock(side_effect=[events, probe_events])
    gov.consecutive_failures = 0

    cache = MagicMock()
    cache.get_event_cursor = AsyncMock(return_value=cursor)
    cache.set_event_cursor = AsyncMock()

    activity_ch = MagicMock(spec=discord.TextChannel)
    activity_ch.name = "activity"
    signals_ch = MagicMock(spec=discord.TextChannel)
    signals_ch.name = "signals"
    alerts_ch = MagicMock(spec=discord.TextChannel)
    alerts_ch.name = "alerts"

    poller = EventPoller(
        gov, cache, activity_ch, signals_ch, alerts_ch,
        residents_channel=residents_channel,
    )

    routed: list[tuple[str, discord.Embed]] = []

    async def capture_put(item):
        channel, embed = item
        routed.append((channel.name, embed))

    poller._message_queue.put = capture_put
    return poller, routed


def _make_residents_channel() -> MagicMock:
    ch = MagicMock(spec=discord.TextChannel)
    ch.name = "residents"
    return ch


@pytest.mark.asyncio
async def test_finding_routes_to_residents_not_main_feed():
    residents_ch = _make_residents_channel()
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "sentinel_finding", "severity": "high",
          "message": "m", "agent_id": "s", "agent_name": "S"}],
        residents_channel=residents_ch,
    )
    await poller._poll_loop_once()
    channels_hit = [name for name, _ in routed]
    assert "residents" in channels_hit
    assert "activity" not in channels_hit
    assert "signals" not in channels_hit


@pytest.mark.asyncio
async def test_verdict_change_routes_to_signals():
    residents_ch = _make_residents_channel()
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "verdict_change", "severity": "warning",
          "message": "m", "agent_id": "a", "agent_name": "A",
          "from": "proceed", "to": "guide"}],
        residents_channel=residents_ch,
    )
    await poller._poll_loop_once()
    channels_hit = [name for name, _ in routed]
    assert "signals" in channels_hit
    assert "activity" not in channels_hit
    assert "residents" not in channels_hit


@pytest.mark.asyncio
async def test_agent_new_routes_to_activity():
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "agent_new", "severity": "info",
          "message": "m", "agent_id": "n", "agent_name": "N"}],
    )
    await poller._poll_loop_once()
    channels_hit = [name for name, _ in routed]
    assert "activity" in channels_hit
    assert "signals" not in channels_hit


@pytest.mark.asyncio
async def test_agent_idle_routes_to_activity():
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "agent_idle", "severity": "info",
          "message": "m", "agent_id": "i", "agent_name": "I"}],
    )
    await poller._poll_loop_once()
    channels_hit = [name for name, _ in routed]
    assert "activity" in channels_hit
    assert "signals" not in channels_hit


@pytest.mark.asyncio
async def test_drift_alert_routes_to_signals():
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "drift_alert", "severity": "warning",
          "message": "m", "agent_id": "d", "agent_name": "D"}],
    )
    await poller._poll_loop_once()
    channels_hit = [name for name, _ in routed]
    assert "signals" in channels_hit
    assert "activity" not in channels_hit


@pytest.mark.asyncio
async def test_critical_finding_routes_to_residents_and_alerts():
    residents_ch = _make_residents_channel()
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "watcher_finding", "severity": "critical",
          "message": "m", "agent_id": "w", "agent_name": "W"}],
        residents_channel=residents_ch,
    )
    await poller._poll_loop_once()
    channels_hit = [name for name, _ in routed]
    assert "residents" in channels_hit
    assert "alerts" in channels_hit
    assert "activity" not in channels_hit
    assert "signals" not in channels_hit


@pytest.mark.asyncio
async def test_finding_falls_back_to_signals_without_residents_channel():
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "sentinel_finding", "severity": "info",
          "message": "m", "agent_id": "s", "agent_name": "S"}],
        residents_channel=None,
    )
    await poller._poll_loop_once()
    channels_hit = [name for name, _ in routed]
    assert "signals" in channels_hit
    assert "activity" not in channels_hit
    assert "residents" not in channels_hit


@pytest.mark.asyncio
async def test_int_event_id_advances_cursor():
    poller, _ = _make_poller(
        [{"event_id": 5, "type": "agent_new", "severity": "info",
          "message": "m", "agent_id": "a", "agent_name": "A"},
         {"event_id": 7, "type": "agent_idle", "severity": "info",
          "message": "m", "agent_id": "b", "agent_name": "B"}],
    )
    await poller._poll_loop_once()
    poller.cache.set_event_cursor.assert_awaited_once_with(7)


@pytest.mark.asyncio
async def test_non_int_event_id_is_skipped_entirely():
    # REST /api/events supplements from the audit DB, which uses UUID
    # event_ids. Those events are incompatible with the cursor protocol
    # and get re-fetched every poll — so we drop them at ingest rather
    # than spam Discord and stall the cursor.
    poller, routed = _make_poller(
        [{"event_id": "fcd718be-0243-4a26-b503-79d4a3d7bfb1",
          "type": "cross_device_call", "severity": "info",
          "message": "m", "agent_id": "a", "agent_name": "A"}],
    )
    await poller._poll_loop_once()
    assert routed == []
    poller.cache.set_event_cursor.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_cursor_resets_when_server_counter_regressed():
    # Simulates governance MCP restart: our cached cursor is 45 but the
    # server's in-memory int counter is back at 1, so every `since=45` poll
    # filters to empty. The poller probes with since=0 and, on finding a
    # lower server max, resets the cursor to 0.
    poller, _ = _make_poller(
        events=[],
        cursor=45,
        probe_events=[
            {"event_id": 1, "type": "agent_new", "severity": "info",
             "message": "m", "agent_id": "a", "agent_name": "A"},
            {"event_id": "uuid-thing", "type": "cross_device_call",
             "severity": "info", "message": "m",
             "agent_id": "b", "agent_name": "B"},
        ],
    )
    await poller._poll_loop_once()
    poller.cache.set_event_cursor.assert_awaited_once_with(0)


@pytest.mark.asyncio
async def test_stale_cursor_check_skipped_when_cursor_is_zero():
    # No cursor yet → nothing to detect as stale. Probe must not run.
    poller, _ = _make_poller(events=[], cursor=0)
    await poller._poll_loop_once()
    # Exactly one fetch (the normal poll); no probe.
    assert poller.gov.fetch_events.await_count == 1
    poller.cache.set_event_cursor.assert_not_awaited()


@pytest.mark.asyncio
async def test_cursor_not_reset_when_server_still_ahead():
    # Empty filtered result but server max >= cursor → healthy, no reset.
    # Happens when all recent in-memory events have rolled off and only
    # UUID audit-DB events remain above the cursor.
    poller, _ = _make_poller(
        events=[],
        cursor=45,
        probe_events=[
            {"event_id": 50, "type": "agent_new", "severity": "info",
             "message": "m", "agent_id": "a", "agent_name": "A"},
        ],
    )
    await poller._poll_loop_once()
    poller.cache.set_event_cursor.assert_not_awaited()


@pytest.mark.asyncio
async def test_poison_pill_event_does_not_block_cursor_or_batch():
    # Regression: a single event that raises inside the dispatch path
    # (here simulated by patching event_to_embed to raise on event_id=2)
    # must not stall the rest of the batch and — critically — must not
    # prevent the cursor from advancing. Otherwise the next poll re-fetches
    # the same poisoned batch and the entire REST feed goes silent.
    from unittest.mock import patch
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "agent_new", "severity": "info",
          "message": "m", "agent_id": "a", "agent_name": "A"},
         {"event_id": 2, "type": "drift_alert", "severity": "warning",
          "message": "m", "agent_id": "b", "agent_name": "B",
          "axis": "I", "value": None},
         {"event_id": 3, "type": "agent_idle", "severity": "info",
          "message": "m", "agent_id": "c", "agent_name": "C"}],
    )

    real_event_to_embed = __import__(
        "bridge.embeds", fromlist=["event_to_embed"]
    ).event_to_embed

    def fake_embed(event):
        if event.get("event_id") == 2:
            raise TypeError("simulated poison-pill")
        return real_event_to_embed(event)

    with patch("bridge.event_poller.event_to_embed", side_effect=fake_embed):
        await poller._poll_loop_once()

    # Events 1 and 3 reached their channels; event 2 was skipped.
    channels_hit = [name for name, _ in routed]
    assert channels_hit.count("activity") == 2
    # Cursor advances past the poison-pill so we don't re-fetch it next poll.
    poller.cache.set_event_cursor.assert_awaited_once_with(3)


@pytest.mark.asyncio
async def test_mixed_event_ids_renders_int_only_and_advances_cursor():
    poller, routed = _make_poller(
        [{"event_id": "uuid-thing", "type": "cross_device_call", "severity": "info",
          "message": "m", "agent_id": "a", "agent_name": "A"},
         {"event_id": 3, "type": "agent_idle", "severity": "info",
          "message": "m", "agent_id": "b", "agent_name": "B"}],
    )
    await poller._poll_loop_once()
    # Only the int-id event renders
    channels_hit = [name for name, _ in routed]
    assert channels_hit == ["activity"]
    poller.cache.set_event_cursor.assert_awaited_once_with(3)
