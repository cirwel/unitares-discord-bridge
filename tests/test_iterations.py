"""Discord delivery for Lumen's bounded self-iteration attention."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
import pytest

from bridge.iterations import (
    ATTENTION_SCHEMA,
    LumenIterationPoller,
    build_iteration_embed,
    normalize_attention_item,
)


def attention_item(**overrides) -> dict:
    item = {
        "attention_id": "si-attn-" + "a" * 24,
        "proposal_id": "si-20260811-example",
        "candidate_id": "sip-" + "b" * 32,
        "stage": "canary",
        "state": "awaiting_signature",
        "priority": "high",
        "active": True,
        "summary": "Candidate awaits a distinct signed canary review.",
        "next_action": "Inspect the exact plan and sign outside Discord.",
        "required_role": "canary_reviewer",
        "occurred_at": "2026-08-11T21:30:00+00:00",
        "target_paths": ["src/anima_mcp/display/eras/geometric.py"],
        "claim_provenance": {
            "source_epistemic_status": "caller_claimed",
            "request_trust_classification": "authenticated_request_unverified_claims",
            "request_actor_authenticated": True,
            "claims_verified_by_request_provenance": False,
            "independent_verification_status": "verified",
            "effective_weight": 1.0,
            "authority_granted": False,
        },
        "acknowledgement_is_approval": False,
        "authority_granted": False,
    }
    item.update(overrides)
    return item


class FakeMessage:
    def __init__(self, message_id: int):
        self.id = message_id
        self.reactions: list[str] = []

    async def add_reaction(self, emoji: str) -> None:
        self.reactions.append(emoji)


class FakeChannel:
    def __init__(self, channel_id: int, name: str):
        self.id = channel_id
        self.name = name
        self.embeds: list[discord.Embed] = []
        self.messages: list[FakeMessage] = []

    async def send(self, *, embed: discord.Embed):
        self.embeds.append(embed)
        message = FakeMessage(self.id * 1_000 + len(self.embeds))
        self.messages.append(message)
        return message


def make_poller(projection: dict | None, *, cached: str | None = None):
    if projection is not None:
        projection.setdefault("acknowledgement_is_approval", False)
        projection.setdefault("authority_granted", False)
    anima = SimpleNamespace(
        fetch_self_iteration_attention=AsyncMock(return_value=projection)
    )
    governance = SimpleNamespace(record_bridge_event=AsyncMock(return_value=True))
    cache = SimpleNamespace(
        get_kv=AsyncMock(return_value=cached),
        set_kv=AsyncMock(),
    )
    iterations = FakeChannel(1, "lumen-iterations")
    signals = FakeChannel(2, "signals")
    alerts = FakeChannel(3, "alerts")
    poller = LumenIterationPoller(
        anima,
        governance,
        cache,
        iterations,
        signals,
        alerts,
        interval=5,
    )
    return poller, anima, governance, cache, iterations, signals, alerts


def test_attention_item_validation_rejects_authority_confusion():
    assert normalize_attention_item(attention_item()) is not None
    assert (
        normalize_attention_item(attention_item(acknowledgement_is_approval=True))
        is None
    )
    assert normalize_attention_item(attention_item(authority_granted=True)) is None
    assert normalize_attention_item(attention_item(attention_id="caller-text")) is None


def test_iteration_embed_states_acknowledgement_boundary():
    item = normalize_attention_item(attention_item())
    assert item is not None

    embed = build_iteration_embed(item)

    assert "awaiting signature" in embed.title.lower()
    assert "never approval" in embed.footer.text
    assert item["proposal_id"] in embed.fields[0].value
    provenance_field = next(
        field for field in embed.fields if field.name == "Proposal provenance"
    )
    assert "caller_claimed" in provenance_field.value
    assert "verifies claim truth: no" in provenance_field.value


@pytest.mark.asyncio
async def test_high_attention_routes_once_to_iterations_and_signals():
    projection = {"schema": ATTENTION_SCHEMA, "items": [attention_item()]}
    poller, _anima, governance, cache, iterations, signals, alerts = make_poller(
        projection
    )

    await poller._tick()
    await poller._tick()

    assert len(iterations.embeds) == 1
    assert len(signals.embeds) == 1
    assert alerts.embeds == []
    assert governance.record_bridge_event.await_count == 2
    assert cache.set_kv.await_count == 2
    payloads = [call.args[0] for call in governance.record_bridge_event.await_args_list]
    assert all(p["event_type"] == "bridge.delivery" for p in payloads)
    assert all(p["source_event_id"].startswith("si-attn-") for p in payloads)
    assert iterations.messages[0].reactions == ["✅"]


@pytest.mark.asyncio
async def test_critical_attention_also_routes_to_alerts():
    projection = {
        "schema": ATTENTION_SCHEMA,
        "items": [
            attention_item(
                priority="critical",
                state="claimed_result_indeterminate",
                required_role="operator_recovery",
            )
        ],
    }
    poller, _anima, governance, _cache, iterations, signals, alerts = make_poller(
        projection
    )

    await poller._tick()

    assert len(iterations.embeds) == 1
    assert len(signals.embeds) == 1
    assert len(alerts.embeds) == 1
    assert governance.record_bridge_event.await_count == 3


@pytest.mark.asyncio
async def test_persisted_delivery_cursor_prevents_restart_replay():
    item = attention_item(priority="low", active=False)
    delivery_id = f"{item['attention_id']}:1"
    projection = {"schema": ATTENTION_SCHEMA, "items": [item]}
    poller, _anima, governance, _cache, iterations, signals, alerts = make_poller(
        projection,
        cached=json.dumps([delivery_id]),
    )

    await poller._tick()

    assert iterations.embeds == []
    assert signals.embeds == []
    assert alerts.embeds == []
    governance.record_bridge_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_projection_outage_is_debounced_and_recovery_announced():
    poller, anima, _governance, _cache, iterations, _signals, alerts = make_poller(None)

    await poller._tick()
    assert alerts.embeds == []
    await poller._tick()
    assert len(alerts.embeds) == 1
    await poller._tick()
    assert len(alerts.embeds) == 1

    anima.fetch_self_iteration_attention.return_value = {
        "schema": ATTENTION_SCHEMA,
        "items": [],
        "acknowledgement_is_approval": False,
        "authority_granted": False,
    }
    await poller._tick()

    assert len(iterations.embeds) == 1
    assert "restored" in iterations.embeds[0].title.lower()
