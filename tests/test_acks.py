"""Tests for reaction-driven bridge.ack receipts.

The governance side matches acks to deliveries on `discord_message_id`, so the
tests that matter are: the key is present and stringified, only configured
emoji count, the bot cannot ack itself, and a receipt failure never escapes
into the event loop.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from bridge import acks
from bridge.acks import build_ack_payload, is_ack_emoji


def _raw_reaction(
    *,
    message_id: int = 111,
    channel_id: int = 222,
    guild_id: int | None = 333,
    user_id: int = 444,
    emoji: str = "✅",
) -> MagicMock:
    payload = MagicMock()
    payload.message_id = message_id
    payload.channel_id = channel_id
    payload.guild_id = guild_id
    payload.user_id = user_id
    payload.emoji = emoji
    return payload


def test_payload_carries_message_id_as_string() -> None:
    """`discord_message_id` is THE match key governance joins deliveries on."""
    payload = build_ack_payload(_raw_reaction(message_id=987654321))

    assert payload["event_type"] == "bridge.ack"
    # Governance's normalizer runs _clean_str on this field; ints would be
    # dropped, silently making every ack unmatchable.
    assert payload["discord_message_id"] == "987654321"
    assert isinstance(payload["discord_message_id"], str)


def test_payload_satisfies_governance_required_field() -> None:
    """bridge.ack is rejected unless source_event_id OR discord_message_id."""
    payload = build_ack_payload(_raw_reaction())
    assert payload.get("source_event_id") or payload.get("discord_message_id")


def test_payload_omits_guild_id_for_dm() -> None:
    payload = build_ack_payload(_raw_reaction(guild_id=None))
    assert "discord_guild_id" not in payload


def test_operator_id_is_hashed_not_raw() -> None:
    payload = build_ack_payload(_raw_reaction(user_id=555))
    assert "555" not in payload["operator_id_hash"]
    assert len(payload["operator_id_hash"]) == 64


def test_operator_hash_is_stable_and_distinct() -> None:
    same = build_ack_payload(_raw_reaction(user_id=1))["operator_id_hash"]
    again = build_ack_payload(_raw_reaction(user_id=1))["operator_id_hash"]
    other = build_ack_payload(_raw_reaction(user_id=2))["operator_id_hash"]
    assert same == again
    assert same != other


def test_channel_key_included_when_known() -> None:
    payload = build_ack_payload(_raw_reaction(), channel_key="alerts")
    assert payload["channel_key"] == "alerts"


def test_only_configured_emoji_acks(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unrelated reaction must not clear an alert from the surface."""
    monkeypatch.setattr(acks, "ACK_EMOJI", frozenset({"✅"}))
    assert is_ack_emoji("✅")
    assert not is_ack_emoji("😂")


@pytest.mark.asyncio
async def test_listener_emits_receipt_for_ack_emoji() -> None:
    bot, gov, listener = _wire()
    await listener(_raw_reaction(emoji="✅"))

    gov.record_bridge_event.assert_awaited_once()
    sent = gov.record_bridge_event.await_args.args[0]
    assert sent["event_type"] == "bridge.ack"
    assert sent["discord_message_id"] == "111"


@pytest.mark.asyncio
async def test_listener_ignores_non_ack_emoji() -> None:
    bot, gov, listener = _wire()
    await listener(_raw_reaction(emoji="😂"))
    gov.record_bridge_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_bot_cannot_ack_its_own_delivery() -> None:
    bot, gov, listener = _wire(bot_user_id=444)
    await listener(_raw_reaction(user_id=444))
    gov.record_bridge_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_receipt_failure_never_escapes() -> None:
    """Observability writes must not take down the event loop."""
    bot, gov, listener = _wire()
    gov.record_bridge_event.side_effect = RuntimeError("governance unreachable")

    await listener(_raw_reaction())  # must not raise


def _wire(bot_user_id: int = 999):
    """Register setup_acks against a fake bot and capture the listener."""
    captured: dict = {}

    def fake_event(fn):
        captured["listener"] = fn
        return fn

    bot = MagicMock()
    bot.event = fake_event
    bot.user = MagicMock()
    bot.user.id = bot_user_id
    bot.get_channel.return_value = MagicMock(name="alerts")

    gov = MagicMock()
    gov.record_bridge_event = AsyncMock(return_value=True)

    acks.setup_acks(bot, gov)
    return bot, gov, captured["listener"]
