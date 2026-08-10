"""Tests for EventPoller finding/lifecycle routing."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from bridge.event_poller import EventPoller


_UNSET = object()  # distinguishes "no probe configured" from probe_events=None
# (None now means "the probe FETCH FAILED" — fetch_events' error value)


def _make_poller(
    events: list[dict] | None,
    *,
    residents_channel: discord.TextChannel | None = None,
    resident_channels: dict[str, discord.TextChannel] | None = None,
    cursor: int = 0,
    probe_events: object = _UNSET,
) -> tuple[EventPoller, list[tuple[str, discord.Embed]]]:
    """Build an EventPoller wired to fake gov client + capture queue.

    If ``probe_events`` is provided (including None = failed fetch),
    ``fetch_events`` returns ``events`` on the first call (the real poll) and
    ``probe_events`` on subsequent calls (the restart-detection probe from
    ``since=0``).
    """
    gov = MagicMock()
    if probe_events is _UNSET:
        gov.fetch_events = AsyncMock(return_value=events)
    else:
        gov.fetch_events = AsyncMock(side_effect=[events, probe_events])
    gov.record_bridge_event = AsyncMock(return_value=True)
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
        resident_channels=resident_channels,
    )

    routed: list[tuple[str, discord.Embed]] = []

    async def capture_put(item):
        channel, embed, _source_event = item
        routed.append((channel.name, embed))

    poller._message_queue.put = capture_put
    return poller, routed


def _make_residents_channel() -> MagicMock:
    ch = MagicMock(spec=discord.TextChannel)
    ch.name = "residents"
    return ch


def _make_channel(name: str) -> MagicMock:
    ch = MagicMock(spec=discord.TextChannel)
    ch.name = name
    return ch


def _resident_channels(*names: str) -> dict[str, MagicMock]:
    return {name: _make_channel(name) for name in names}


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
async def test_sentinel_finding_routes_to_its_own_channel():
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "sentinel_finding", "severity": "medium",
          "message": "m", "agent_id": "sentinel", "agent_name": "Sentinel"}],
        residents_channel=_make_residents_channel(),
        resident_channels=_resident_channels("sentinel", "doctor"),
    )
    await poller._poll_loop_once()
    channels_hit = [name for name, _ in routed]
    assert channels_hit == ["sentinel"]
    assert "residents" not in channels_hit
    assert "doctor" not in channels_hit


@pytest.mark.asyncio
async def test_doctor_finding_routes_to_its_own_channel():
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "doctor_finding", "severity": "medium",
          "message": "m", "agent_id": "doctor", "agent_name": "Doctor"}],
        residents_channel=_make_residents_channel(),
        resident_channels=_resident_channels("sentinel", "doctor"),
    )
    await poller._poll_loop_once()
    assert [name for name, _ in routed] == ["doctor"]


@pytest.mark.asyncio
async def test_qualified_finding_type_still_routes_by_author():
    # sentinel_alarm_finding is Sentinel's too — the author is the type prefix
    # up to the first underscore, not the whole pre-_finding string.
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "sentinel_alarm_finding", "severity": "medium",
          "message": "m", "agent_id": "sentinel", "agent_name": "Sentinel"}],
        residents_channel=_make_residents_channel(),
        resident_channels=_resident_channels("sentinel"),
    )
    await poller._poll_loop_once()
    assert [name for name, _ in routed] == ["sentinel"]


@pytest.mark.asyncio
async def test_other_residents_still_use_shared_channel():
    # Vigil/Watcher have no channel of their own — #residents is unchanged.
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "vigil_finding", "severity": "medium",
          "message": "m", "agent_id": "vigil", "agent_name": "Vigil"}],
        residents_channel=_make_residents_channel(),
        resident_channels=_resident_channels("sentinel", "doctor"),
    )
    await poller._poll_loop_once()
    assert [name for name, _ in routed] == ["residents"]


@pytest.mark.asyncio
async def test_own_channel_finding_still_mirrors_to_alerts():
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "sentinel_finding", "severity": "high",
          "message": "m", "agent_id": "sentinel", "agent_name": "Sentinel"}],
        residents_channel=_make_residents_channel(),
        resident_channels=_resident_channels("sentinel"),
    )
    await poller._poll_loop_once()
    channels_hit = [name for name, _ in routed]
    assert "sentinel" in channels_hit
    assert "alerts" in channels_hit
    assert "residents" not in channels_hit


@pytest.mark.asyncio
async def test_own_channel_missing_falls_back_to_residents():
    # Channel creation failed / operator deleted it — findings must still land
    # somewhere rather than vanish.
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "doctor_finding", "severity": "info",
          "message": "m", "agent_id": "doctor", "agent_name": "Doctor"}],
        residents_channel=_make_residents_channel(),
        resident_channels=_resident_channels("sentinel"),
    )
    await poller._poll_loop_once()
    assert [name for name, _ in routed] == ["residents"]


@pytest.mark.asyncio
async def test_non_finding_from_sentinel_stays_in_signals():
    # Only findings are routed by author. A verdict change *about* Sentinel is
    # ordinary governance traffic and belongs in the main feed.
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "verdict_change", "severity": "warning",
          "message": "m", "agent_id": "sentinel", "agent_name": "Sentinel",
          "from": "proceed", "to": "guide"}],
        residents_channel=_make_residents_channel(),
        resident_channels=_resident_channels("sentinel"),
    )
    await poller._poll_loop_once()
    assert [name for name, _ in routed] == ["signals"]


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
async def test_suppressed_event_records_bridge_receipt(monkeypatch):
    from bridge import config

    monkeypatch.setattr(config, "SUPPRESSED_EVENT_TYPES", {"knowledge_read"})
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "knowledge_read", "severity": "info",
          "message": "m", "agent_id": "a", "agent_name": "A"}],
    )

    await poller._poll_loop_once()

    assert routed == []
    poller.gov.record_bridge_event.assert_awaited_once()
    payload = poller.gov.record_bridge_event.await_args.args[0]
    assert payload["event_type"] == "bridge.suppressed"
    assert payload["source_event_id"] == 1
    assert payload["reason"] == "suppressed_event_type"


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
async def test_failed_fetch_never_resets_cursor():
    # Live incident 2026-07-28: during a governance stall BOTH the normal
    # fetch and the since=0 probe fail. A failed fetch (None) must read as
    # "no evidence", not as "server feed is empty" — otherwise every stall
    # resets the cursor and replays the whole feed to Discord.
    poller, _ = _make_poller(events=None, cursor=45)
    await poller._poll_loop_once()
    poller.cache.set_event_cursor.assert_not_awaited()
    # No probe either: exactly one fetch attempt this cycle.
    assert poller.gov.fetch_events.await_count == 1


@pytest.mark.asyncio
async def test_failed_probe_never_resets_cursor():
    # Normal fetch succeeds-empty (feed rolled off), but the probe fails →
    # still no evidence about the server's counter → no reset.
    poller, _ = _make_poller(events=[], cursor=45, probe_events=None)
    await poller._poll_loop_once()
    poller.cache.set_event_cursor.assert_not_awaited()
    assert poller.gov.fetch_events.await_count == 2


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


def test_write_heartbeat_touches_file(tmp_path, monkeypatch):
    """The liveness heartbeat is written with a parseable, fresh timestamp."""
    import os
    import time
    from datetime import datetime

    from bridge import config

    hb = tmp_path / "sub" / "heartbeat"  # nested dir must be created
    monkeypatch.setattr(config, "BRIDGE_HEARTBEAT_PATH", str(hb))

    poller, _ = _make_poller([])
    poller._write_heartbeat()

    assert hb.exists()
    # content parses as an ISO-8601 instant, and the file is fresh.
    datetime.fromisoformat(hb.read_text())
    assert time.time() - os.path.getmtime(hb) < 5


def test_write_heartbeat_is_best_effort(monkeypatch):
    """A heartbeat write failure must never propagate out of the poll loop."""
    from bridge import config

    # An unwritable path (root of a non-existent device-ish path) must be swallowed.
    monkeypatch.setattr(config, "BRIDGE_HEARTBEAT_PATH", "/proc/nonexistent/heartbeat")
    poller, _ = _make_poller([])
    poller._write_heartbeat()  # must not raise


def test_write_heartbeat_disabled_when_path_empty(monkeypatch):
    """Empty BRIDGE_HEARTBEAT_PATH disables the write (no error)."""
    from bridge import config

    monkeypatch.setattr(config, "BRIDGE_HEARTBEAT_PATH", "")
    poller, _ = _make_poller([])
    poller._write_heartbeat()  # no-op, must not raise


@pytest.mark.asyncio
async def test_id_only_signal_is_enriched_with_resident_label():
    poller, routed = _make_poller(
        [{"event_id": 1, "type": "coherence_drop", "severity": "warning",
          "message": "m",
          "agent_id": "fe5975a6-23c7-4e55-9a9d-9c4bdb9b45a7"}],
    )
    poller.gov.call_tool = AsyncMock(return_value={
        "result": {
            "agents": [
                {
                    "agent_id": "fe5975a6-23c7-4e55-9a9d-9c4bdb9b45a7",
                    "label": "opus_hikewa",
                },
            ],
        },
    })

    await poller._poll_loop_once()

    assert routed[0][0] == "signals"
    assert routed[0][1].fields[0].name == "Agent"
    assert routed[0][1].fields[0].value == "opus_hikewa (fe5975a6-23c)"


@pytest.mark.asyncio
async def test_send_loop_survives_unexpected_send_error():
    """A non-HTTP exception from channel.send must not kill the send loop —
    the poll loop keeps writing the heartbeat, so a dead sender would be a
    silent Discord outage the watchdog can't see."""
    import contextlib

    poller, _routed = _make_poller([])
    # Undo the capture override — this test drives the real queue + send loop.
    poller._message_queue = asyncio.Queue()

    channel = MagicMock(spec=discord.TextChannel)
    channel.name = "signals"
    channel.send = AsyncMock(side_effect=[RuntimeError("boom"), MagicMock(id=1)])

    embed = discord.Embed(title="t")
    await poller._message_queue.put((channel, embed, {"type": "x"}))
    await poller._message_queue.put((channel, embed, {"type": "y"}))

    task = asyncio.create_task(poller._send_loop())
    try:
        for _ in range(100):
            if channel.send.await_count >= 2:
                break
            await asyncio.sleep(0.05)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert channel.send.await_count == 2
