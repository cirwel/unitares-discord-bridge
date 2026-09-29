import json
from unittest.mock import AsyncMock

import discord
import pytest

from bridge.commands import (
    build_status_embed,
    build_agent_embed,
    build_digest_embed,
    build_health_embed,
    build_kg_search_embed,
    build_resume_embed,
    build_lumen_embed,
    _error_embed,
)
from bridge.mcp_client import (
    parse_tool_result as _parse_tool_result,
    fetch_agents as _fetch_agents,
    fetch_metrics as _fetch_metrics,
)


# ---------------------------------------------------------------------------
# build_status_embed — delegates to build_hud_embed
# ---------------------------------------------------------------------------

def test_status_embed_with_agents():
    agents = [{"id": "a1", "label": "opus"}]
    metrics = {"a1": {"E": 0.7, "I": 0.6, "S": 0.5, "V": 0.1, "verdict": "proceed"}}
    embed = build_status_embed(agents, metrics)
    assert isinstance(embed, discord.Embed)
    assert "opus" in embed.description


def test_status_embed_empty():
    embed = build_status_embed([], {})
    assert "No active agents" in embed.description


# ---------------------------------------------------------------------------
# build_agent_embed
# ---------------------------------------------------------------------------

def test_agent_embed():
    data = {
        "label": "opus_hikewa",
        "verdict": "proceed",
        "E": 0.74, "I": 0.71, "S": 0.42, "V": 0.08,
        "last_seen": "2026-02-20T10:30:00Z",
    }
    embed = build_agent_embed("abc12345-full-id", data)
    assert "opus_hikewa" in embed.title
    assert embed.colour == discord.Colour.green()
    eisv_field = next(f for f in embed.fields if f.name == "EISV")
    assert "0.74" in eisv_field.value


def test_agent_embed_pause_verdict():
    data = {"verdict": "pause", "E": 0, "I": 0, "S": 0, "V": 0}
    embed = build_agent_embed("x", data)
    assert embed.colour == discord.Colour.red()


def test_agent_embed_unknown_verdict():
    data = {"verdict": "exotic", "E": 0, "I": 0, "S": 0, "V": 0}
    embed = build_agent_embed("x", data)
    assert embed.colour == discord.Colour.greyple()


def test_agent_embed_with_trajectory():
    data = {"verdict": "guide", "E": 0, "I": 0, "S": 0, "V": 0, "trajectory": "converging"}
    embed = build_agent_embed("x", data)
    traj_field = next(f for f in embed.fields if f.name == "Trajectory")
    assert traj_field.value == "converging"


def test_agent_embed_minimal():
    embed = build_agent_embed("agent-id", {})
    assert "agent-id" in embed.title


# ---------------------------------------------------------------------------
# build_health_embed
# ---------------------------------------------------------------------------

def test_health_embed_ok():
    health = {"status": "ok", "uptime": "12h", "active_agents": 3, "database": "connected", "version": "2.5.8"}
    embed = build_health_embed(health)
    assert embed.colour == discord.Colour.green()
    assert embed.footer.text == "v2.5.8"
    status_field = next(f for f in embed.fields if f.name == "Status")
    assert status_field.value == "ok"


def test_health_embed_unhealthy():
    health = {"status": "degraded"}
    embed = build_health_embed(health)
    assert embed.colour == discord.Colour.red()


def test_health_embed_no_version():
    health = {"status": "ok"}
    embed = build_health_embed(health)
    # Should still work without version
    assert isinstance(embed, discord.Embed)


def test_health_embed_nested_fields():
    """Real /health response has nested uptime, database, connections dicts."""
    health = {
        "status": "ok",
        "version": "2.7.0",
        "uptime": {"seconds": 1921, "formatted": "32m 1s"},
        "connections": {"active": 5, "healthy": 3},
        "database": {"status": "connected", "pool_size": 3},
    }
    embed = build_health_embed(health)
    assert embed.colour == discord.Colour.green()
    uptime_field = next(f for f in embed.fields if f.name == "Uptime")
    assert uptime_field.value == "32m 1s"
    db_field = next(f for f in embed.fields if f.name == "Database")
    assert db_field.value == "connected"
    conn_field = next(f for f in embed.fields if f.name == "Connections")
    assert conn_field.value == "5"


# ---------------------------------------------------------------------------
# build_resume_embed
# ---------------------------------------------------------------------------

def test_resume_embed_success():
    result = {"success": True, "message": "Agent resumed successfully"}
    embed = build_resume_embed("agent-123", result)
    assert embed.colour == discord.Colour.green()
    assert "Resumed" in str(embed.fields)
    assert "resumed successfully" in embed.description


def test_resume_embed_failure():
    result = {"success": False, "reason": "Agent not paused"}
    embed = build_resume_embed("agent-123", result)
    assert embed.colour == discord.Colour.red()
    assert "Failed" in str(embed.fields)


def test_resume_embed_resumed_key():
    """Some responses use 'resumed' instead of 'success'."""
    result = {"resumed": True}
    embed = build_resume_embed("a1", result)
    assert embed.colour == discord.Colour.green()


# ---------------------------------------------------------------------------
# build_lumen_embed — delegates to build_sensor_embed
# ---------------------------------------------------------------------------

def test_lumen_embed():
    state = {
        "ambient_temp": 22.5, "humidity": 35, "pressure": 827,
        "light": 500, "cpu_temp": 45, "memory_percent": 62,
        "neural": {"delta": 0.5, "theta": 0.3, "alpha": 0.7, "beta": 0.6, "gamma": 0.4},
        "warmth": 0.65, "clarity": 0.72, "stability": 0.88, "presence": 0.55,
    }
    embed = build_lumen_embed(state)
    assert isinstance(embed, discord.Embed)
    assert "22.5" in embed.description


# ---------------------------------------------------------------------------
# _parse_tool_result
# ---------------------------------------------------------------------------

def test_parse_tool_result():
    result = {"result": {"content": [{"text": json.dumps({"key": "value"})}]}}
    assert _parse_tool_result(result) == {"key": "value"}


def test_parse_tool_result_empty():
    assert _parse_tool_result({}) == {}


# ---------------------------------------------------------------------------
# _fetch_agents
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_agents():
    gov = AsyncMock()
    gov.call_tool = AsyncMock(return_value={
        "result": {"content": [{"text": json.dumps([
            {"agent_id": "a1", "label": "opus"},
            {"agent_id": "a2", "name": "sonnet"},
        ])}]},
    })
    agents = await _fetch_agents(gov)
    assert len(agents) == 2
    assert {"id": "a1", "label": "opus"} in agents
    assert {"id": "a2", "label": "sonnet"} in agents


@pytest.mark.asyncio
async def test_fetch_agents_returns_empty_on_none():
    gov = AsyncMock()
    gov.call_tool = AsyncMock(return_value=None)
    agents = await _fetch_agents(gov)
    assert agents == []


@pytest.mark.asyncio
async def test_fetch_agents_sorts_by_recency_desc():
    gov = AsyncMock()
    gov.call_tool = AsyncMock(return_value={
        "result": {"content": [{"text": json.dumps([
            {"agent_id": "stale", "label": "old", "last_update": "2026-04-10T00:00:00Z"},
            {"agent_id": "fresh", "label": "new", "last_update": "2026-04-17T00:00:00Z"},
            {"agent_id": "mid", "label": "mid", "last_update": "2026-04-14T00:00:00Z"},
        ])}]},
    })
    agents = await _fetch_agents(gov)
    assert [a["id"] for a in agents] == ["fresh", "mid", "stale"]


@pytest.mark.asyncio
async def test_fetch_agents_requests_recent_window():
    gov = AsyncMock()
    gov.call_tool = AsyncMock(return_value={
        "result": {"content": [{"text": json.dumps([])}]},
    })
    await _fetch_agents(gov)
    name, args = gov.call_tool.call_args.args
    assert name == "agent"
    assert args.get("action") == "list"
    assert args.get("recent_days") == 7
    assert args.get("lite") is True


@pytest.mark.asyncio
async def test_fetch_metrics():
    gov = AsyncMock()
    gov.call_tool = AsyncMock(return_value={
        "result": {"content": [{"text": json.dumps({
            "E": 0.7, "I": 0.6, "S": 0.5, "V": 0.1, "verdict": "proceed",
        })}]},
    })
    agents = [{"id": "a1", "label": "test"}]
    metrics = await _fetch_metrics(gov, agents)
    assert "a1" in metrics
    assert metrics["a1"]["verdict"] == "proceed"


# ---------------------------------------------------------------------------
# _error_embed
# ---------------------------------------------------------------------------

def test_error_embed():
    embed = _error_embed("Something went wrong")
    assert embed.colour == discord.Colour.red()
    assert embed.title == "Error"
    assert embed.description == "Something went wrong"


# ---------------------------------------------------------------------------
# build_kg_search_embed
# ---------------------------------------------------------------------------

def test_kg_search_empty_results():
    embed = build_kg_search_embed("nothing here", [])
    assert embed.colour == discord.Colour.greyple()
    assert "No discoveries" in embed.description
    assert "nothing here" in embed.title


def test_kg_search_single_result():
    results = [{
        "by": "Vigil",
        "summary": "Governance recovered after brief outage",
        "type": "note",
        "tags": ["vigil", "recovery"],
        "created_at": "2026-04-14T10:00:00+00:00",
    }]
    embed = build_kg_search_embed("recovery", results)
    assert embed.colour == discord.Colour.blurple()
    assert "1 result" in embed.title
    field = embed.fields[0]
    assert field.name == "Vigil"
    assert "Governance recovered" in field.value
    assert "2026-04-14" in field.value
    assert "`vigil`" in field.value


def test_kg_search_truncates_long_summaries():
    long_summary = "A" * 900
    results = [{"by": "agent", "summary": long_summary, "tags": []}]
    embed = build_kg_search_embed("anything", results)
    assert embed.fields[0].value.endswith("…")
    assert len(embed.fields[0].value) <= 1024


def test_kg_search_caps_at_five_with_footer():
    results = [
        {"by": f"agent-{i}", "summary": f"finding {i}", "tags": []}
        for i in range(12)
    ]
    embed = build_kg_search_embed("many", results)
    assert len(embed.fields) == 5
    assert "12" in embed.footer.text


def test_kg_search_handles_missing_fields():
    results = [{}]
    embed = build_kg_search_embed("x", results)
    assert embed.fields[0].name == "unknown"
    assert embed.fields[0].value == "—"


def test_kg_search_plural_header_one_match():
    results = [{"by": "a", "summary": "hi", "tags": []}]
    embed = build_kg_search_embed("hi", results)
    assert "1 result" in embed.title
    assert "1 results" not in embed.title


def test_kg_search_plural_header_two_matches():
    results = [
        {"by": "a", "summary": "hi", "tags": []},
        {"by": "b", "summary": "there", "tags": []},
    ]
    embed = build_kg_search_embed("hi", results)
    assert "2 results" in embed.title


# ---------------------------------------------------------------------------
# build_digest_embed — per-class aggregation
# ---------------------------------------------------------------------------


_DIGEST_REVERSE = {
    "broadcast_events": {
        "lifecycle_paused": "BEH",
        "knowledge_confidence_clamped": "INT",
        "circuit_breaker_trip": "REC",
        "identity_drift": "CON",
    }
}
_DIGEST_META = {
    "INT": {"id": "INT", "name": "Integrity"},
    "BEH": {"id": "BEH", "name": "Behavioral Consistency"},
    "REC": {"id": "REC", "name": "Recoverability"},
    "CON": {"id": "CON", "name": "Continuity"},
}


def test_digest_empty_window():
    embed = build_digest_embed(hours=1, events=[], taxonomy_reverse=_DIGEST_REVERSE)
    assert embed.colour == discord.Colour.greyple()
    assert "No broadcaster events" in embed.description
    assert "last 1h" in embed.title


def test_digest_classifies_events_and_sorts_by_count():
    events = [
        {"type": "lifecycle_paused"},
        {"type": "lifecycle_paused"},
        {"type": "knowledge_confidence_clamped"},
        {"type": "identity_drift"},
    ]
    embed = build_digest_embed(
        hours=1,
        events=events,
        taxonomy_reverse=_DIGEST_REVERSE,
        class_meta=_DIGEST_META,
    )
    # BEH has 2, INT and CON each have 1; BEH should be listed first.
    desc = embed.description
    beh_idx = desc.find("BEH")
    int_idx = desc.find("INT")
    con_idx = desc.find("CON")
    assert beh_idx < int_idx
    assert beh_idx < con_idx
    assert "**BEH** · Behavioral Consistency: **2**" in desc
    assert "**INT** · Integrity: **1**" in desc


def test_digest_explicit_violation_class_overrides_reverse():
    # Watcher's knowledge_write events carry violation_class directly.
    events = [
        {"type": "knowledge_write", "violation_class": "ENT"},
        {"type": "knowledge_write", "violation_class": "ENT"},
    ]
    embed = build_digest_embed(
        hours=1,
        events=events,
        taxonomy_reverse=_DIGEST_REVERSE,
    )
    assert "**ENT**" in embed.description
    assert ": **2**" in embed.description


def test_digest_counts_unmapped_events_separately():
    events = [
        {"type": "knowledge_confidence_clamped"},  # maps to INT
        {"type": "some_brand_new_event_no_one_has_classified"},
    ]
    embed = build_digest_embed(
        hours=1, events=events, taxonomy_reverse=_DIGEST_REVERSE,
    )
    assert "**INT**" in embed.description
    assert "unclassified" in embed.description
    assert ": 1" in embed.description


def test_digest_colour_red_when_critical_present():
    events = [
        {"type": "lifecycle_paused", "severity": "critical"},
    ]
    embed = build_digest_embed(
        hours=1, events=events, taxonomy_reverse=_DIGEST_REVERSE,
    )
    assert embed.colour == discord.Colour.red()


def test_digest_colour_orange_when_high_but_no_critical():
    events = [
        {"type": "lifecycle_paused", "severity": "high"},
    ]
    embed = build_digest_embed(
        hours=1, events=events, taxonomy_reverse=_DIGEST_REVERSE,
    )
    assert embed.colour == discord.Colour.orange()


def test_digest_handles_none_reverse_gracefully():
    events = [{"type": "lifecycle_paused"}]
    embed = build_digest_embed(hours=1, events=events, taxonomy_reverse=None)
    # Everything falls into unclassified — no crash.
    assert "unclassified" in embed.description


# ---------------------------------------------------------------------------
# ws_events ring buffer (used by /digest)
# ---------------------------------------------------------------------------


def test_recent_events_filters_by_time():
    from bridge.ws_events import (
        record_event,
        recent_events,
        _reset_event_ring_for_tests,
    )

    _reset_event_ring_for_tests()
    record_event({"type": "lifecycle_paused", "id": "recent1"})
    record_event({"type": "identity_drift", "id": "recent2"})
    # Window of 1h should include both (just-added).
    got = recent_events(3600)
    ids = [e.get("id") for e in got]
    assert "recent1" in ids and "recent2" in ids


def test_recent_events_empty_when_window_zero():
    from bridge.ws_events import (
        record_event,
        recent_events,
        _reset_event_ring_for_tests,
    )

    _reset_event_ring_for_tests()
    record_event({"type": "x"})
    # Window of 0s → nothing qualifies (record timestamp >= now - 0 is current now,
    # and event was recorded before now, so it should be excluded by strict >=).
    # Actually record_event uses time.time(), and cutoff = time.time() - 0 = now,
    # so the previously-recorded event has ts < now by a few ns → excluded.
    import time
    time.sleep(0.001)
    assert recent_events(0) == []
