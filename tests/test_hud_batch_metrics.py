"""The HUD reads the fleet in one governance call, not 51.

Live evidence (2026-10): 2,183 seconds each held exactly 51 tools/call about
31 s apart: one agent(action="list") plus one get_governance_metrics per
agent, about 140k calls a day. agent(action="list", include_metrics=True)
already returns each row's EISV, so the per-agent calls are only a fallback.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bridge import hud as hud_module
from bridge.hud import HUDUpdater
from bridge.mcp_client import _derive_verdict, fetch_agents, fetch_metrics


def _list_row(agent_id, label, *, updates=10, metrics=None, last="2026-10-19T11:00:00+00:00"):
    return {
        "agent_id": agent_id,
        "label": label,
        "total_updates": updates,
        "last_update": last,
        "agent_id_redacted": False,
        "metrics": metrics,
    }


def _row_metrics(
    E=0.7, I=0.8, S=0.2, V=-0.05, risk=0.3, basin="boundary", verdict="safe",
    source="persisted_state",
):
    # Shape of query.py _persisted_state_metrics (source="persisted_state") or
    # _monitor_metrics (source="live_monitor" / "restored_monitor").
    return {
        "E": E, "I": I, "S": S, "V": V, "coherence": 0.5,
        "risk_score": risk, "basin": basin, "verdict": verdict,
        "source": source,
    }


_PER_AGENT_PAYLOAD = {"result": {
    "success": True, "E": {"value": 0.5}, "I": {"value": 0.6},
    "S": {"value": 0.1}, "V": {"value": 0.0}, "basin": {"value": "high"},
    "risk_score": {"value": 0.1, "status": "🟢 low"},
}}


def _gov_listing(rows, *, grouped=False):
    agents = {"active": rows, "paused": []} if grouped else rows
    payload = {"success": True, "agents": agents, "summary": {"returned": len(rows)}}
    gov = MagicMock()
    gov.consecutive_failures = 0
    gov.call_tool = AsyncMock(
        return_value={"result": {"content": [{"text": json.dumps(payload)}]}}
    )
    return gov


@pytest.mark.asyncio
async def test_with_metrics_requests_the_full_flat_listing():
    gov = _gov_listing([])
    await fetch_agents(gov, with_metrics=True)
    name, args = gov.call_tool.call_args.args
    assert name == "agent"
    assert args == {
        "action": "list", "lite": False, "grouped": False,
        "include_metrics": True, "limit": 50, "recent_days": 7,
    }


@pytest.mark.asyncio
async def test_label_only_callers_keep_the_lite_listing():
    gov = _gov_listing([])
    await fetch_agents(gov)
    assert gov.call_tool.call_args.args[1]["lite"] is True


@pytest.mark.asyncio
async def test_one_cycle_makes_one_call():
    rows = [_list_row(f"uuid-{i}", f"agent-{i}", metrics=_row_metrics()) for i in range(50)]
    gov = _gov_listing(rows)
    agents = await fetch_agents(gov, with_metrics=True)
    metrics = await fetch_metrics(gov, agents)
    assert gov.call_tool.await_count == 1
    assert len(metrics) == 50


@pytest.mark.asyncio
async def test_hud_loop_cycle_makes_one_call():
    rows = [_list_row(f"uuid-{i}", f"agent-{i}", metrics=_row_metrics()) for i in range(5)]
    gov = _gov_listing(rows)
    updater = HUDUpdater(gov, cache=MagicMock(), hud_channel=MagicMock(), interval=30)
    updater._message = MagicMock()
    updater._message.edit = AsyncMock()

    with patch.object(hud_module.asyncio, "sleep", AsyncMock(side_effect=asyncio.CancelledError)):
        with pytest.raises(asyncio.CancelledError):
            await updater._update_loop()

    assert gov.call_tool.await_count == 1
    embed = updater._message.edit.call_args.kwargs["embed"]
    assert "agent-0" in embed.description
    assert "no state" not in embed.description


@pytest.mark.asyncio
async def test_grouped_payload_does_not_crash():
    rows = [
        _list_row("uuid-a", "alpha", metrics=_row_metrics()),
        _list_row("uuid-b", "beta", metrics=_row_metrics()),
    ]
    gov = _gov_listing(rows, grouped=True)
    agents = await fetch_agents(gov, with_metrics=True)
    assert {a["id"] for a in agents} == {"uuid-a", "uuid-b"}


@pytest.mark.asyncio
@pytest.mark.parametrize("agents", [{"active": "oops"}, ["oops"]])
async def test_malformed_agents_value_does_not_crash(agents):
    gov = MagicMock()
    gov.call_tool = AsyncMock(return_value={"result": {"agents": agents}})
    assert await fetch_agents(gov, with_metrics=True) == []


@pytest.mark.asyncio
async def test_full_mode_redaction_is_counted(caplog):
    row = _list_row("Claude_Code_20261019_abcd1234", "x", metrics=_row_metrics())
    row["agent_id_redacted"] = True
    gov = _gov_listing([row])
    with caplog.at_level("WARNING"):
        agents = await fetch_agents(gov, with_metrics=True)
    assert "redacted 1/1" in caplog.text
    # The row still carries its reading, so the HUD shows it without a call.
    metrics = await fetch_metrics(gov, agents)
    assert "Claude_Code_20261019_abcd1234" in metrics
    assert gov.call_tool.await_count == 1


@pytest.mark.asyncio
async def test_row_without_metrics_falls_back_to_one_call():
    rows = [
        _list_row("uuid-a", "alpha", metrics=_row_metrics()),
        _list_row("uuid-b", "beta", metrics=None),  # monitor errored
    ]
    gov = _gov_listing(rows)
    agents = await fetch_agents(gov, with_metrics=True)
    gov.call_tool = AsyncMock(return_value=_PER_AGENT_PAYLOAD)
    metrics = await fetch_metrics(gov, agents)
    assert gov.call_tool.await_count == 1
    assert gov.call_tool.await_args.args == ("get_governance_metrics", {"agent_id": "uuid-b"})
    assert set(metrics) == {"uuid-a", "uuid-b"}


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["live_monitor", "restored_monitor", None])
async def test_loaded_monitor_row_reads_primary_eisv_per_agent(source):
    """A loaded monitor's list row carries monitor.state (the ODE vector);
    get_governance_metrics reports the primary EISV (behavioral when
    confident). The HUD must show the latter for every agent, so these rows
    are not trusted and get one call each."""
    rows = [
        _list_row("uuid-a", "alpha", metrics=_row_metrics()),
        _list_row("uuid-r", "resident", metrics=_row_metrics(E=0.95, source=source)),
    ]
    gov = _gov_listing(rows)
    agents = await fetch_agents(gov, with_metrics=True)
    gov.call_tool = AsyncMock(return_value=_PER_AGENT_PAYLOAD)
    metrics = await fetch_metrics(gov, agents)
    assert gov.call_tool.await_count == 1
    assert gov.call_tool.await_args.args == ("get_governance_metrics", {"agent_id": "uuid-r"})
    assert metrics["uuid-r"]["E"] == 0.5  # the per-agent value, not the row's 0.95
    assert metrics["uuid-a"]["E"] == 0.7


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["live_monitor", "persisted_state"])
async def test_never_observed_row_with_seed_metrics_is_no_state(source):
    """get_or_create_monitor gives a 0-update agent the genesis seed vector;
    the row then has all four values but is not a reading."""
    seed = _row_metrics(E=0.7, I=0.8, S=0.2, V=0.0, source=source)
    gov = _gov_listing([_list_row("uuid-seed", "seed", updates=0, metrics=seed)])
    agents = await fetch_agents(gov, with_metrics=True)
    gov.call_tool.reset_mock()
    metrics = await fetch_metrics(gov, agents)
    gov.call_tool.assert_not_awaited()
    assert metrics == {}


@pytest.mark.asyncio
async def test_unlabeled_uuid_row_gets_a_short_label():
    uuid = "094e2c8a-d22a-4b52-a953-a54bbea69aa4"
    gov = _gov_listing([
        _list_row(uuid, None, metrics=_row_metrics()),
        _list_row("Claude_Code_20261019_abcd1234", None, metrics=_row_metrics()),
    ])
    agents = await fetch_agents(gov, with_metrics=True)
    labels = {a["id"]: a["label"] for a in agents}
    assert labels[uuid] == "094e2c8a"
    assert labels["Claude_Code_20261019_abcd1234"] == "Claude_Code_20261019_abcd1234"


@pytest.mark.asyncio
async def test_row_with_partial_eisv_falls_back():
    partial = _row_metrics()
    partial["E"] = None
    gov = _gov_listing([_list_row("uuid-a", "alpha", metrics=partial)])
    agents = await fetch_agents(gov, with_metrics=True)
    gov.call_tool = AsyncMock(return_value=None)
    metrics = await fetch_metrics(gov, agents)
    assert gov.call_tool.await_count == 1
    assert metrics == {}


@pytest.mark.asyncio
async def test_never_observed_row_is_no_state_without_a_call():
    gov = _gov_listing([_list_row("uuid-ghost", "ghost", updates=0, metrics=None)])
    agents = await fetch_agents(gov, with_metrics=True)
    gov.call_tool.reset_mock()
    metrics = await fetch_metrics(gov, agents)
    gov.call_tool.assert_not_awaited()
    assert metrics == {}


@pytest.mark.asyncio
async def test_reserved_prefix_row_without_metrics_is_still_skipped():
    gov = _gov_listing([_list_row("mcp_20260423_deb879b6", "Chronicler", metrics=None)])
    agents = await fetch_agents(gov, with_metrics=True)
    gov.call_tool.reset_mock()
    await fetch_metrics(gov, agents)
    gov.call_tool.assert_not_awaited()


# --- verdict derivation for the list row's bare values ---

@pytest.mark.parametrize("risk, expected", [
    (0.10, "guide"),   # low risk, boundary basin
    (0.50, "guide"),   # medium
    (0.70, "pause"),   # at the high line
    (0.95, "pause"),
])
def test_float_risk_score_is_handled(risk, expected):
    assert _derive_verdict({"risk_score": risk, "basin": "boundary"}) == expected


def test_float_risk_low_with_high_basin_proceeds():
    assert _derive_verdict({"risk_score": 0.1, "basin": "high"}) == "proceed"


def test_monitor_verdict_vocabulary_is_not_taken_verbatim():
    """'safe' / 'caution' / 'high-risk' have no HUD emoji; derive instead."""
    for raw in ("safe", "caution", "high-risk"):
        assert _derive_verdict({"verdict": raw, "risk_score": 0.1, "basin": "high"}) == "proceed"


def test_bool_risk_score_is_not_a_number():
    assert _derive_verdict({"risk_score": True}) == "guide"


# --- parsing equivalence of the two payload shapes ---
#
# These tests feed the SAME hand-built numbers into a list row and a
# get_governance_metrics payload and check the bridge turns both into the
# same HUD entry. They prove only that the bridge parses both shapes alike.
# They cannot detect governance returning different values in the two calls
# (that is what the source gate in fetch_metrics and
# test_loaded_monitor_row_reads_primary_eisv_per_agent are for); a true
# value-parity check needs a governance-side contract test.

def _lite_metrics_payload(E, I, S, V, risk, basin, verdict):
    """Shape of runtime_queries.get_governance_metrics_data(lite=True)."""
    status = (
        "🟢 low" if risk < 0.45 else "🟡 medium" if risk < 0.70 else "🔴 high"
    )
    return {
        "success": True,
        "E": {"value": E, "label": "Energy"},
        "I": {"value": I, "label": "Information Integrity"},
        "S": {"value": S, "label": "Entropy"},
        "V": {"value": V, "label": "Valence"},
        "risk_score": {"value": risk, "threshold": 0.45, "status": status},
        "basin": {"value": basin, "meaning": "..."},
        "verdict": {"value": verdict, "meaning": "..."},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("risk, basin, verdict", [
    (0.20, "high", "safe"),
    (0.20, "boundary", "safe"),
    (0.55, "boundary", "caution"),
    (0.80, "boundary", "high-risk"),
    (0.30, "low", "safe"),
])
async def test_list_row_parses_like_per_agent_read(risk, basin, verdict):
    E, I, S, V = 0.71, 0.82, 0.19, -0.04
    gov = _gov_listing([_list_row(
        "uuid-a", "alpha",
        metrics=_row_metrics(E=E, I=I, S=S, V=V, risk=risk, basin=basin, verdict=verdict),
    )])
    batch = await fetch_metrics(gov, await fetch_agents(gov, with_metrics=True))

    per_agent_gov = MagicMock()
    per_agent_gov.call_tool = AsyncMock(return_value={
        "result": _lite_metrics_payload(E, I, S, V, risk, basin, verdict),
    })
    single = await fetch_metrics(per_agent_gov, [{"id": "uuid-a", "label": "alpha"}])

    assert batch == single
