"""Noise routing: resident-only events, embodied state chatter, one post per finding."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from bridge.agent_tags import AgentTagResolver, has_any
from bridge.cache import BridgeCache, FindingDeliveryCache
from bridge.event_poller import EventPoller

RESIDENT = frozenset({"persistent", "autonomous"})
EMBODIED = frozenset({"persistent", "autonomous", "embodied", "lumen"})
EPHEMERAL = frozenset({"ephemeral"})


def _channel(name: str, cid: int = 0) -> MagicMock:
    ch = MagicMock(spec=discord.TextChannel)
    ch.name = name
    ch.id = cid
    return ch


class _Tags:
    """Stand-in resolver: agent_id -> tags, None for unknown."""

    def __init__(self, table: dict[str, frozenset[str] | None]):
        self.table = table
        self.calls: list[str] = []

    async def tags(self, agent_id):
        self.calls.append(agent_id)
        return self.table.get(agent_id)


def _poller(events, *, tags=None, embodied=True, residents=True):
    gov = MagicMock()
    gov.fetch_events = AsyncMock(return_value=events)
    gov.record_bridge_event = AsyncMock(return_value=True)
    gov.consecutive_failures = 0
    cache = MagicMock()
    cache.get_event_cursor = AsyncMock(return_value=0)
    cache.set_event_cursor = AsyncMock()
    poller = EventPoller(
        gov, cache, _channel("activity"), _channel("signals"), _channel("alerts"),
        residents_channel=_channel("residents") if residents else None,
        resident_channels={"sentinel": _channel("sentinel")},
        tag_resolver=tags,
        embodied_channel=_channel("lumen-sensors") if embodied else None,
    )
    routed: list[str] = []

    async def capture(item):
        routed.append(item[0].name)

    poller._message_queue.put = capture
    return poller, routed


def _receipts(poller) -> list[dict]:
    return [c.args[0] for c in poller.gov.record_bridge_event.await_args_list]


def _ev(etype: str, agent: str, **extra) -> dict:
    return {"event_id": 1, "type": etype, "severity": "info", "message": "m",
            "agent_id": agent, "agent_name": agent, **extra}


# --- (a) resident-only events -------------------------------------------------


@pytest.mark.asyncio
async def test_agent_new_for_ephemeral_session_is_receipted_not_posted():
    poller, routed = _poller([_ev("agent_new", "codex-1")], tags=_Tags({"codex-1": EPHEMERAL}))
    await poller._poll_loop_once()
    assert routed == []
    (receipt,) = _receipts(poller)
    assert receipt["event_type"] == "bridge.suppressed"
    assert receipt["reason"] == "non_resident_agent"
    poller.cache.set_event_cursor.assert_awaited_once_with(1)  # still consumed


@pytest.mark.asyncio
async def test_agent_new_for_resident_still_posts_to_activity():
    poller, routed = _poller([_ev("agent_new", "vigil")], tags=_Tags({"vigil": RESIDENT}))
    await poller._poll_loop_once()
    assert routed == ["activity"]


@pytest.mark.asyncio
async def test_critical_risk_threshold_for_ephemeral_session_skips_signals_and_alerts():
    event = _ev("risk_threshold", "claude-9", severity="critical", value=0.72, direction="up")
    poller, routed = _poller([event], tags=_Tags({"claude-9": EPHEMERAL}))
    await poller._poll_loop_once()
    assert routed == []


@pytest.mark.asyncio
async def test_critical_risk_threshold_for_resident_reaches_signals_and_alerts():
    event = _ev("risk_threshold", "lumen", severity="critical", value=0.72, direction="up")
    poller, routed = _poller([event], tags=_Tags({"lumen": EMBODIED}))
    await poller._poll_loop_once()
    assert routed == ["signals", "alerts"]


@pytest.mark.asyncio
async def test_unknown_tags_post_as_before():
    # Lookup failed: never let a governance hiccup hide the event.
    poller, routed = _poller([_ev("agent_new", "mystery")], tags=_Tags({}))
    await poller._poll_loop_once()
    assert routed == ["activity"]


@pytest.mark.asyncio
async def test_without_resolver_everything_posts_as_before():
    poller, routed = _poller([_ev("agent_new", "codex-1")], tags=None)
    await poller._poll_loop_once()
    assert routed == ["activity"]


@pytest.mark.asyncio
async def test_resident_filter_disabled_by_empty_type_list(monkeypatch):
    from bridge import config

    monkeypatch.setattr(config, "RESIDENT_ONLY_EVENT_TYPES", frozenset())
    tags = _Tags({"codex-1": EPHEMERAL})
    poller, routed = _poller([_ev("agent_new", "codex-1")], tags=tags)
    await poller._poll_loop_once()
    assert routed == ["activity"]
    assert tags.calls == []  # no lookup when nothing needs it


@pytest.mark.asyncio
async def test_unrelated_events_never_trigger_a_lookup():
    tags = _Tags({})
    event = _ev("verdict_change", "codex-1", severity="warning", **{"from": "proceed", "to": "guide"})
    poller, routed = _poller([event], tags=tags)
    await poller._poll_loop_once()
    assert routed == ["signals"]
    assert tags.calls == []


# --- (b) embodied state chatter ---------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("etype", ["drift_oscillation", "trajectory_adjustment"])
async def test_embodied_state_chatter_goes_to_lumen_channel(etype):
    poller, routed = _poller([_ev(etype, "lumen")], tags=_Tags({"lumen": EMBODIED}))
    await poller._poll_loop_once()
    assert routed == ["lumen-sensors"]


@pytest.mark.asyncio
async def test_non_embodied_resident_trajectory_adjustment_stays_in_signals():
    poller, routed = _poller(
        [_ev("trajectory_adjustment", "vigil", severity="warning")],
        tags=_Tags({"vigil": RESIDENT}),
    )
    await poller._poll_loop_once()
    assert routed == ["signals"]


@pytest.mark.asyncio
async def test_embodied_chatter_without_lumen_channel_stays_in_signals():
    poller, routed = _poller(
        [_ev("drift_oscillation", "lumen")], tags=_Tags({"lumen": EMBODIED}), embodied=False,
    )
    await poller._poll_loop_once()
    assert routed == ["signals"]


@pytest.mark.asyncio
async def test_embodied_drift_alert_is_not_rerouted():
    # Sustained drift (drift_alert) is a real signal, not self-correcting chatter.
    poller, routed = _poller(
        [_ev("drift_alert", "lumen", severity="warning", axis="epistemic", value=0.2)],
        tags=_Tags({"lumen": EMBODIED}),
    )
    await poller._poll_loop_once()
    assert routed == ["signals"]


# --- (c) findings: once, to one channel ----------------------------------------


@pytest.mark.asyncio
async def test_dismissed_watcher_resolution_is_receipted_not_posted():
    event = {"event_id": 3, "type": "watcher_resolution_finding", "severity": "high",
             "action": "dismissed", "message": "[dismissed] P012 x.py:1",
             "agent_name": "Watcher"}
    poller, routed = _poller([event])
    await poller._poll_loop_once()
    assert routed == []
    (receipt,) = _receipts(poller)
    assert receipt["reason"] == "dismissed_resolution"


@pytest.mark.asyncio
async def test_dismissed_resolutions_can_be_reenabled(monkeypatch):
    from bridge import config

    monkeypatch.setattr(config, "SUPPRESS_DISMISSED_RESOLUTIONS", False)
    event = {"event_id": 3, "type": "watcher_resolution_finding", "severity": "high",
             "action": "dismissed", "message": "[dismissed] P012 x.py:1"}
    poller, routed = _poller([event])
    await poller._poll_loop_once()
    assert routed == ["residents"]  # resident channel only, never #alerts


@pytest.mark.asyncio
@pytest.mark.parametrize("severity,expected", [
    ("medium", ["sentinel"]), ("high", ["sentinel"]), ("critical", ["alerts"]),
])
async def test_sentinel_finding_posts_exactly_once(severity, expected):
    event = {"event_id": 5, "type": "sentinel_alarm_finding", "severity": severity,
             "message": "m", "agent_id": "sentinel"}
    poller, routed = _poller([event])
    await poller._poll_loop_once()
    assert routed == expected


# --- AgentTagResolver -------------------------------------------------------------


def _gov_returning(*results):
    gov = MagicMock()
    gov.call_tool = AsyncMock(side_effect=list(results))
    return gov


def _reply(data: dict) -> dict:
    return {"name": "agent", "success": True, "result": data}


@pytest.mark.asyncio
async def test_resolver_reads_tags_from_agent_get_and_caches():
    gov = _gov_returning(_reply({
        "success": True, "agent_id": "mcp_x", "agent_id_redacted": True,
        "tags": ["Persistent", "embodied"],
    }))
    resolver = AgentTagResolver(gov)
    assert await resolver.tags("uuid-1") == frozenset({"persistent", "embodied"})
    assert await resolver.tags("uuid-1") == frozenset({"persistent", "embodied"})
    gov.call_tool.assert_awaited_once_with(
        "agent", {"action": "get", "agent_id": "uuid-1", "lite": True},
    )


@pytest.mark.asyncio
async def test_resolver_failure_is_unknown_and_not_cached():
    gov = _gov_returning(None, _reply({"success": True, "agent_id": "uuid-1", "tags": ["ephemeral"]}))
    resolver = AgentTagResolver(gov)
    assert await resolver.tags("uuid-1") is None
    assert await resolver.tags("uuid-1") == frozenset({"ephemeral"})


@pytest.mark.asyncio
async def test_resolver_not_found_is_unknown():
    gov = _gov_returning(_reply({"success": False, "error": "Agent not found"}))
    assert await AgentTagResolver(gov).tags("uuid-1") is None


@pytest.mark.asyncio
async def test_resolver_rejects_an_answer_about_a_different_agent():
    # `get` defaults to the caller; the bridge's own record must not stand in.
    gov = _gov_returning(_reply({"success": True, "agent_id": "bridge-uuid", "tags": ["ephemeral"]}))
    assert await AgentTagResolver(gov).tags("uuid-1") is None


@pytest.mark.asyncio
async def test_resolver_expires_entries(monkeypatch):
    import bridge.agent_tags as mod

    now = [1000.0]
    monkeypatch.setattr(mod.time, "monotonic", lambda: now[0])
    gov = _gov_returning(
        _reply({"success": True, "tags": ["ephemeral"]}),
        _reply({"success": True, "tags": ["persistent"]}),
    )
    resolver = AgentTagResolver(gov, ttl=60)
    assert await resolver.tags("a") == frozenset({"ephemeral"})
    now[0] += 61
    assert await resolver.tags("a") == frozenset({"persistent"})


@pytest.mark.asyncio
async def test_resolver_timeout_is_unknown():
    import asyncio

    async def slow(*_a, **_k):
        await asyncio.sleep(1)

    gov = MagicMock()
    gov.call_tool = slow
    assert await AgentTagResolver(gov, timeout=0.01).tags("a") is None


def test_has_any_is_tristate():
    assert has_any(None, RESIDENT) is None
    assert has_any(EPHEMERAL, frozenset({"persistent"})) is False
    assert has_any(RESIDENT, frozenset({"persistent"})) is True


# --- persisted finding de-dupe ----------------------------------------------------


@pytest.mark.asyncio
async def test_finding_delivery_dedupe_survives_restart(tmp_path):
    db = str(tmp_path / "bridge.db")
    event = {"type": "sentinel_alarm_finding", "event_id": 6,
             "timestamp": "2026-10-14T08:32:47Z", "severity": "medium"}
    ch = _channel("sentinel", cid=123)
    ch.send = AsyncMock(return_value=MagicMock(id=456))

    async with BridgeCache(db) as store:
        first = FindingDeliveryCache(store=store)
        assert await first.send(ch, discord.Embed(title="f"), event) == (456, False)

    # New process: empty memory, same SQLite file.
    async with BridgeCache(db) as store:
        second = FindingDeliveryCache(store=store)
        assert await second.send(ch, discord.Embed(title="f"), event) == (456, True)
        # A different finding still delivers.
        other = {**event, "event_id": 7}
        ch.send.return_value = MagicMock(id=789)
        assert await second.send(ch, discord.Embed(title="g"), other) == (789, False)
    assert ch.send.await_count == 2


@pytest.mark.asyncio
async def test_finding_delivery_store_failure_still_delivers():
    store = MagicMock()
    store.get_finding_delivery = AsyncMock(side_effect=RuntimeError("disk"))
    store.put_finding_delivery = AsyncMock(side_effect=RuntimeError("disk"))
    ch = _channel("sentinel", cid=1)
    ch.send = AsyncMock(return_value=MagicMock(id=9))
    cache = FindingDeliveryCache(store=store)
    event = {"type": "doctor_check_finding", "event_id": 1, "timestamp": "t"}
    assert await cache.send(ch, discord.Embed(title="f"), event) == (9, False)
    # The in-memory map still coalesces within the process.
    assert await cache.send(ch, discord.Embed(title="f"), event) == (9, True)


@pytest.mark.asyncio
async def test_old_finding_deliveries_are_pruned(tmp_path, monkeypatch):
    import bridge.cache as mod

    now = [1_000_000.0]
    monkeypatch.setattr(mod.time, "time", lambda: now[0])
    async with BridgeCache(str(tmp_path / "b.db")) as store:
        await store.put_finding_delivery("old", 1)
        now[0] += BridgeCache.FINDING_DELIVERY_RETENTION_SECONDS + 1
        await store.put_finding_delivery("new", 2)
        assert await store.get_finding_delivery("old") is None
        assert await store.get_finding_delivery("new") == 2
