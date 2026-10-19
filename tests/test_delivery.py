"""Bounded retry/redelivery for transient Discord connection failures.

Live evidence (2026-10-05..19): 65 posts lost to "Cannot connect to host
discord.com:443 ... nodename nor servname provided", each recorded once as
send_loop_unexpected_error and dropped; the self-iteration outage notice let
the same error escape as a traceback 96 times.
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import discord
import pytest

from bridge import config, delivery
from bridge.event_poller import EventPoller
from bridge.iterations import LumenIterationPoller
from bridge.ws_events import WSEventSubscriber


def dns_error() -> aiohttp.ClientConnectorError:
    return aiohttp.ClientConnectorError(
        MagicMock(), OSError(8, "nodename nor servname provided, or not known"),
    )


def http_error(status: int, headers: dict | None = None) -> discord.HTTPException:
    response = MagicMock()
    response.status = status
    response.reason = "x"
    response.headers = headers or {}
    return discord.HTTPException(response, "boom")


@pytest.fixture(autouse=True)
def fast_retry(monkeypatch):
    """No real sleeping: tiny backoff and redelivery delay."""
    monkeypatch.setattr(config, "SEND_ATTEMPTS", 3)
    monkeypatch.setattr(config, "SEND_RETRY_BACKOFF_SECONDS", 0)
    monkeypatch.setattr(config, "SEND_REDELIVERIES", 2)
    monkeypatch.setattr(config, "SEND_REDELIVERY_DELAY_SECONDS", 0)


# ---------------------------------------------------------------------------
# send_with_retry
# ---------------------------------------------------------------------------


def test_transient_classification():
    assert delivery.is_transient_send_error(dns_error())
    assert delivery.is_transient_send_error(asyncio.TimeoutError())
    assert delivery.is_transient_send_error(OSError("reset"))
    assert not delivery.is_transient_send_error(http_error(500))
    assert not delivery.is_transient_send_error(http_error(403))
    assert not delivery.is_transient_send_error(RuntimeError("boom"))


@pytest.mark.asyncio
async def test_retry_then_succeed():
    send = AsyncMock(side_effect=[dns_error(), dns_error(), "ok"])
    assert await delivery.send_with_retry(send) == "ok"
    assert send.await_count == 3


@pytest.mark.asyncio
async def test_retry_gives_up_after_bounded_attempts():
    send = AsyncMock(side_effect=dns_error())
    with pytest.raises(aiohttp.ClientConnectorError):
        await delivery.send_with_retry(send)
    assert send.await_count == 3


@pytest.mark.asyncio
async def test_non_transient_http_error_is_not_retried():
    send = AsyncMock(side_effect=http_error(403))
    with pytest.raises(discord.HTTPException):
        await delivery.send_with_retry(send)
    assert send.await_count == 1


# ---------------------------------------------------------------------------
# send loops (REST poller and WS subscriber share the behaviour)
# ---------------------------------------------------------------------------


def _channel(send_side_effect):
    ch = MagicMock(spec=discord.TextChannel)
    ch.id, ch.name = 7, "alerts"
    ch.send = AsyncMock(side_effect=send_side_effect)
    return ch


def _poller(gov):
    ch = _channel(None)
    return EventPoller(gov, MagicMock(), ch, ch, ch)


def _subscriber(gov):
    ch = _channel(None)
    return WSEventSubscriber("http://localhost:8767", ch, ch, ch, gov_client=gov)


def _queue(sender):
    return sender._message_queue if isinstance(sender, EventPoller) else sender._send_queue


async def _run_send_loop(sender, until, timeout=2.0):
    task = asyncio.create_task(sender._send_loop())
    try:
        for _ in range(int(timeout / 0.01)):
            if until():
                break
            await asyncio.sleep(0.01)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await asyncio.gather(*list(sender._redelivery_tasks), return_exceptions=True)


def _gov():
    gov = MagicMock()
    gov.record_bridge_event = AsyncMock(return_value=True)
    gov.consecutive_failures = 0
    return gov


def _receipts(gov):
    return [call.args[0] for call in gov.record_bridge_event.await_args_list]


EVENT = {"type": "lifecycle_silent_critical", "event_id": 1, "agent_id": "lumen"}


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_poller, _subscriber])
async def test_send_loop_retries_dns_blip_and_delivers(make):
    gov = _gov()
    sender = make(gov)
    ch = _channel([dns_error(), MagicMock(id=99)])
    await _queue(sender).put((ch, discord.Embed(title="t"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    assert ch.send.await_count == 2
    receipts = _receipts(gov)
    assert [r["event_type"] for r in receipts] == ["bridge.delivery"]
    assert receipts[0]["discord_message_id"] == "99"


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_poller, _subscriber])
async def test_send_loop_requeues_through_short_outage(make):
    # 3 attempts fail (retries spent) -> re-queued -> delivered on redelivery.
    gov = _gov()
    sender = make(gov)
    ch = _channel([dns_error(), dns_error(), dns_error(), MagicMock(id=5)])
    await _queue(sender).put((ch, discord.Embed(title="t"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    assert ch.send.await_count == 4
    assert [r["event_type"] for r in _receipts(gov)] == ["bridge.delivery"]


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_poller, _subscriber])
async def test_send_loop_records_durable_loss_after_redelivery_exhausted(make):
    gov = _gov()
    sender = make(gov)
    ch = _channel(dns_error())
    await _queue(sender).put((ch, discord.Embed(title="t"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    # 1 initial delivery + 2 redeliveries, 3 attempts each.
    assert ch.send.await_count == 9
    receipts = _receipts(gov)
    assert len(receipts) == 1
    assert receipts[0]["event_type"] == "bridge.delivery_failed"
    assert receipts[0]["reason"] == "discord_unreachable"
    assert "nodename" in receipts[0]["error"]
    assert receipts[0]["source_event_id"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_poller, _subscriber])
async def test_send_loop_non_transient_http_error_unchanged(make):
    gov = _gov()
    sender = make(gov)
    ch = _channel(http_error(403))
    await _queue(sender).put((ch, discord.Embed(title="t"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    assert ch.send.await_count == 1
    receipts = _receipts(gov)
    assert [r["reason"] for r in receipts] == ["discord_http_exception"]


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_poller, _subscriber])
async def test_send_loop_429_path_unchanged(make, monkeypatch):
    gov = _gov()
    sender = make(gov)
    ch = _channel([http_error(429, {"Retry-After": "0"}), MagicMock(id=3)])
    await _queue(sender).put((ch, discord.Embed(title="t"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 2)

    assert ch.send.await_count == 2
    assert [r["event_type"] for r in _receipts(gov)] == [
        "bridge.rate_limited", "bridge.delivery",
    ]


@pytest.mark.asyncio
async def test_failed_attempt_leaves_finding_cache_retryable():
    from bridge.cache import FindingDeliveryCache

    gov = _gov()
    sender = _poller(gov)
    sender.delivery_cache = FindingDeliveryCache()
    ch = _channel([dns_error(), MagicMock(id=11)])
    ch.id = 123
    finding = {"type": "sentinel_alarm_finding", "event_id": 6,
               "timestamp": "2026-10-17T18:12:24Z", "severity": "medium"}
    await _queue(sender).put((ch, discord.Embed(title="f"), finding))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    assert ch.send.await_count == 2
    assert [r["event_type"] for r in _receipts(gov)] == ["bridge.delivery"]


# ---------------------------------------------------------------------------
# iterations: outage notice must survive a connection error
# ---------------------------------------------------------------------------


def _iteration_poller(alerts_send):
    anima = SimpleNamespace(fetch_self_iteration_attention=AsyncMock(return_value=None))
    governance = SimpleNamespace(record_bridge_event=AsyncMock(return_value=True))
    cache = SimpleNamespace(get_kv=AsyncMock(return_value=None), set_kv=AsyncMock())
    alerts = SimpleNamespace(id=3, name="alerts", send=alerts_send)
    iterations = SimpleNamespace(id=1, name="lumen-iterations", send=AsyncMock())
    poller = LumenIterationPoller(anima, governance, cache, iterations, None, alerts)
    return poller, alerts


@pytest.mark.asyncio
async def test_unavailable_notice_survives_connection_error():
    poller, alerts = _iteration_poller(AsyncMock(side_effect=dns_error()))

    await poller._tick()
    await poller._tick()  # second failure triggers the notice; must not raise

    assert alerts.send.await_count == 3  # retried, bounded
    assert poller._unavailable_announced is False  # so the next poll tries again


@pytest.mark.asyncio
async def test_unavailable_notice_retries_dns_blip():
    poller, alerts = _iteration_poller(AsyncMock(side_effect=[dns_error(), MagicMock(id=1)]))

    await poller._tick()
    await poller._tick()

    assert alerts.send.await_count == 2
    assert poller._unavailable_announced is True
