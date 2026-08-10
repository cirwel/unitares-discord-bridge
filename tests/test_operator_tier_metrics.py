"""The HUD must read real EISV, and must not invent it when it can't.

Root cause of the 2026-08-10 HUD-of-seed-values incident: `list_agents`
redacts every other agent's UUID for non-operator callers and hands back a
display handle (`Claude_Code_<date>_<uuid8>`) instead. That handle is not a
valid `agent_id` — `get_governance_metrics` resolves against the UUID — so
every metrics call missed, and the server answered each miss with its ODE seed
vector under a success envelope. The HUD rendered 50 agents at a constant
E=0.70 I=0.80 S=0.20 V=0.00 and it read as a quiet fleet.

Three independent guards, one per layer:
  1. send the operator token, so UUIDs arrive un-redacted;
  2. drop a refused metrics read instead of parsing it as zeros;
  3. render "no state" rather than a placeholder vector.
"""

from unittest.mock import AsyncMock

import pytest

from bridge.hud import build_hud_embed
from bridge.mcp_client import GovernanceClient, fetch_agents, fetch_metrics


# --- layer 1: the operator header ---

def test_operator_token_sets_the_operator_header():
    client = GovernanceClient("http://gov", token="bearer", operator_token="op-secret")
    assert client._headers["X-Unitares-Operator"] == "op-secret"
    assert client._headers["Authorization"] == "Bearer bearer"
    assert client.is_operator is True


def test_operator_header_absent_without_a_token():
    client = GovernanceClient("http://gov", token="bearer")
    assert "X-Unitares-Operator" not in client._headers
    assert client.is_operator is False


def test_operator_token_is_not_sent_as_bearer():
    """Sending it as bearer auth is a no-op; only the dedicated header lifts
    redaction. This was the original mistake."""
    client = GovernanceClient("http://gov", operator_token="op-secret")
    assert client._headers.get("Authorization") != "Bearer op-secret"


# --- layer 2: a refused metrics read is dropped, not parsed ---

def _gov_returning(payload):
    client = GovernanceClient("http://gov")
    client.call_tool = AsyncMock(return_value={"result": payload})
    return client


@pytest.mark.asyncio
async def test_unknown_agent_refusal_is_not_rendered_as_zeros():
    gov = _gov_returning(
        {
            "success": False,
            "error": "Unknown agent_id 'Claude_Code_20260805_33fcecfd'",
            "error_type": "unknown_agent",
        }
    )
    metrics = await fetch_metrics(gov, [{"id": "Claude_Code_20260805_33fcecfd", "label": "x"}])
    assert metrics == {}


@pytest.mark.asyncio
async def test_real_metrics_still_parse():
    gov = _gov_returning(
        {
            "success": True,
            "E": {"value": 0.2613},
            "I": {"value": 0.8833},
            "S": {"value": 0.3417},
            "V": {"value": -0.6217},
            "basin": "low",
        }
    )
    metrics = await fetch_metrics(gov, [{"id": "69a1a4f7-a30f-4f4a-bcf9-2de8606fb819", "label": "Lumen"}])
    entry = metrics["69a1a4f7-a30f-4f4a-bcf9-2de8606fb819"]
    assert entry["E"] == pytest.approx(0.2613)
    assert entry["V"] == pytest.approx(-0.6217)


@pytest.mark.asyncio
async def test_redacted_listing_is_logged(caplog):
    """A missing operator token must be visible in the log, not silent."""
    gov = GovernanceClient("http://gov")
    gov.call_tool = AsyncMock(
        return_value={
            "result": {
                "agents": [
                    {"id": "Claude_Code_20260805_33fcecfd", "label": "a", "uuid_redacted": True},
                    {"id": "69a1a4f7-a30f-4f4a-bcf9-2de8606fb819", "label": "b"},
                ]
            }
        }
    )
    with caplog.at_level("WARNING"):
        agents = await fetch_agents(gov)
    assert len(agents) == 2
    assert "redacted 1/2" in caplog.text
    assert "GOVERNANCE_OPERATOR_TOKEN" in caplog.text


# --- layer 3: the HUD says "no state" instead of showing zeros ---

def test_missing_metrics_render_as_no_state_not_zeros():
    embed = build_hud_embed([{"id": "a1", "label": "Lumen"}], {})
    assert "no state" in embed.description
    assert "E=0.00" not in embed.description


def test_no_state_agents_are_counted_separately():
    agents = [
        {"id": "a1", "label": "has_state"},
        {"id": "a2", "label": "no_state"},
    ]
    metrics = {"a1": {"E": 0.5, "I": 0.5, "S": 0.5, "V": 0.5, "verdict": "proceed"}}
    embed = build_hud_embed(agents, metrics)
    assert "1 no state" in embed.footer.text
    assert "2 agents" in embed.footer.text


def test_no_state_agents_do_not_inflate_the_boundary_count():
    """The old fallback made every unreadable agent a 'guide' verdict, so the
    boundary count equalled the agent count and meant nothing."""
    agents = [{"id": f"a{i}", "label": f"agent{i}"} for i in range(5)]
    embed = build_hud_embed(agents, {})
    assert "0 boundary" in embed.footer.text
    assert "5 no state" in embed.footer.text
