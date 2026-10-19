"""Delivery through transient Discord connection failures.

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


def disconnected() -> aiohttp.ServerDisconnectedError:
    # Raised after the request may have been written: retry only with a nonce.
    return aiohttp.ServerDisconnectedError()


def http_error(status: int, headers: dict | None = None) -> discord.HTTPException:
    response = MagicMock()
    response.status = status
    response.reason = "x"
    response.headers = headers or {}
    return discord.HTTPException(response, "boom")


@pytest.fixture(autouse=True)
def fast_retry(monkeypatch):
    """No real sleeping."""
    monkeypatch.setattr(config, "SEND_ATTEMPTS", 3)
    monkeypatch.setattr(config, "SEND_RETRY_BACKOFF_SECONDS", 0)
    monkeypatch.setattr(config, "SEND_PROBE_MAX_SECONDS", 0)
    monkeypatch.setattr(config, "SEND_MAX_AGE_SECONDS", 900)


# ---------------------------------------------------------------------------
# classification + direct-send helper
# ---------------------------------------------------------------------------


def test_transient_classification():
    assert delivery.is_transient_send_error(dns_error())
    assert delivery.is_transient_send_error(disconnected())
    assert delivery.is_transient_send_error(asyncio.TimeoutError())
    assert delivery.is_transient_send_error(OSError("reset"))
    assert not delivery.is_transient_send_error(http_error(500))
    assert not delivery.is_transient_send_error(http_error(403))
    assert not delivery.is_transient_send_error(RuntimeError("boom"))


def test_pre_send_classification():
    assert delivery.is_pre_send_error(dns_error())
    assert not delivery.is_pre_send_error(disconnected())
    assert not delivery.is_pre_send_error(asyncio.TimeoutError())
    assert not delivery.is_pre_send_error(http_error(500))


@pytest.mark.asyncio
async def test_direct_send_retries_dns_then_succeeds():
    send = AsyncMock(side_effect=[dns_error(), dns_error(), "ok"])
    assert await delivery.send_with_retry(send) == "ok"
    assert send.await_count == 3


@pytest.mark.asyncio
async def test_direct_send_gives_up_after_bounded_attempts():
    send = AsyncMock(side_effect=dns_error())
    with pytest.raises(aiohttp.ClientConnectorError):
        await delivery.send_with_retry(send)
    assert send.await_count == 3


@pytest.mark.asyncio
async def test_direct_send_does_not_retry_after_request_may_have_landed():
    # No nonce on direct sends: a disconnect after writing could duplicate.
    send = AsyncMock(side_effect=[disconnected(), "ok"])
    with pytest.raises(aiohttp.ServerDisconnectedError):
        await delivery.send_with_retry(send)
    assert send.await_count == 1


@pytest.mark.asyncio
async def test_direct_send_http_error_is_not_retried():
    send = AsyncMock(side_effect=http_error(403))
    with pytest.raises(discord.HTTPException):
        await delivery.send_with_retry(send)
    assert send.await_count == 1


# ---------------------------------------------------------------------------
# nonce
# ---------------------------------------------------------------------------


def _ch(cid: int, name: str = "signals", side_effect=None) -> MagicMock:
    ch = MagicMock(spec=discord.TextChannel)
    ch.id, ch.name = cid, name
    ch.send = AsyncMock(side_effect=side_effect)
    return ch


def test_nonce_is_stable_per_event_and_destination():
    event = {"type": "verdict_change", "event_id": 4, "timestamp": "t", "agent_id": "a"}
    a, b = _ch(1), _ch(2)
    n1 = delivery.message_nonce(a, event)
    assert n1 == delivery.message_nonce(a, dict(event))
    assert n1 != delivery.message_nonce(b, event)
    assert n1 != delivery.message_nonce(a, {**event, "event_id": 5})
    assert len(n1) <= 25


def test_nonce_without_event_identity_is_fixed_per_message():
    item = delivery.Outgoing(_ch(1), discord.Embed(title="t"), None)
    other = delivery.Outgoing(_ch(1), discord.Embed(title="t"), None)
    assert item.nonce and len(item.nonce) <= 25
    assert item.nonce != other.nonce


# ---------------------------------------------------------------------------
# send loops (REST poller and WS subscriber share DiscordSender)
# ---------------------------------------------------------------------------


def _poller(gov, alerts=None):
    ch = _ch(7, "signals")
    return EventPoller(gov, MagicMock(), ch, ch, alerts or _ch(8, "alerts"))


def _subscriber(gov, alerts=None):
    ch = _ch(7, "signals")
    return WSEventSubscriber("http://localhost:8767", ch, ch, alerts or _ch(8, "alerts"), gov_client=gov)


def _queue(sender):
    return sender._message_queue if isinstance(sender, EventPoller) else sender._send_queue


async def _run_send_loop(sender, until, timeout=2.0):
    sender._sender.pacing = 0
    sender._sender.restart_delay = 0
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
    return task


def _gov():
    gov = MagicMock()
    gov.record_bridge_event = AsyncMock(return_value=True)
    gov.consecutive_failures = 0
    return gov


def _receipts(gov):
    return [call.args[0] for call in gov.record_bridge_event.await_args_list]


EVENT = {"type": "lifecycle_silent_critical", "event_id": 1, "agent_id": "lumen",
         "timestamp": "2026-10-17T18:06:59Z"}

SENDERS = [_poller, _subscriber]


@pytest.mark.asyncio
@pytest.mark.parametrize("make", SENDERS)
async def test_retry_reuses_the_same_nonce_so_discord_dedupes(make):
    gov = _gov()
    sender = make(gov)
    ch = _ch(9, side_effect=[disconnected(), MagicMock(id=99)])
    await _queue(sender).put((ch, discord.Embed(title="t"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    assert ch.send.await_count == 2
    nonces = [c.kwargs["nonce"] for c in ch.send.await_args_list]
    assert nonces[0] and nonces[0] == nonces[1]
    assert [r["event_type"] for r in _receipts(gov)] == ["bridge.delivery"]
    assert _receipts(gov)[0]["discord_message_id"] == "99"


@pytest.mark.asyncio
async def test_finding_cache_path_also_sends_the_nonce():
    from bridge.cache import FindingDeliveryCache

    gov = _gov()
    sender = _poller(gov)
    sender._sender.delivery_cache = FindingDeliveryCache()
    ch = _ch(123, "sentinel", side_effect=[dns_error(), MagicMock(id=11)])
    finding = {"type": "sentinel_alarm_finding", "event_id": 6,
               "timestamp": "2026-10-17T18:12:24Z", "severity": "medium"}
    await _queue(sender).put((ch, discord.Embed(title="f"), finding))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    # The failed attempt left the cache retryable; both attempts share a nonce.
    assert ch.send.await_count == 2
    assert len({c.kwargs["nonce"] for c in ch.send.await_args_list}) == 1
    assert [r["event_type"] for r in _receipts(gov)] == ["bridge.delivery"]


@pytest.mark.asyncio
@pytest.mark.parametrize("make", SENDERS)
async def test_outage_pauses_sends_and_alerts_go_first_on_recovery(make, monkeypatch):
    monkeypatch.setattr(config, "SEND_RETRY_BACKOFF_SECONDS", 0.05)
    monkeypatch.setattr(config, "SEND_PROBE_MAX_SECONDS", 0.05)
    gov = _gov()
    alerts = _ch(8, "alerts")
    sender = make(gov, alerts)
    routine = _ch(20, "activity")
    delivered: list[str] = []
    calls = {"n": 0}

    async def routine_send(*, embed, nonce):
        calls["n"] += 1
        if calls["n"] == 1:
            # The outage begins; a critical alert is raised meanwhile.
            _queue(sender).put_nowait((alerts, discord.Embed(title="CRITICAL"), None))
            raise dns_error()
        delivered.append(embed.title)
        return MagicMock(id=calls["n"])

    async def alerts_send(*, embed, nonce):
        delivered.append(embed.title)
        return MagicMock(id=1000)

    routine.send = AsyncMock(side_effect=routine_send)
    alerts.send = AsyncMock(side_effect=alerts_send)
    for title in ("routine-1", "routine-2", "routine-3"):
        await _queue(sender).put((routine, discord.Embed(title=title), None))

    await _run_send_loop(sender, lambda: len(delivered) >= 4)

    assert delivered[0] == "CRITICAL"
    assert sorted(delivered[1:]) == ["routine-1", "routine-2", "routine-3"]
    # One failed attempt, no in-loop retries of the failing message.
    assert calls["n"] == 4


@pytest.mark.asyncio
async def test_no_sends_while_breaker_is_open(monkeypatch):
    monkeypatch.setattr(config, "SEND_RETRY_BACKOFF_SECONDS", 10)
    gov = _gov()
    sender = _poller(gov)
    ch = _ch(9, side_effect=dns_error())
    for i in range(5):
        await _queue(sender).put((ch, discord.Embed(title=str(i)), None))

    await _run_send_loop(sender, lambda: False, timeout=0.2)

    assert ch.send.await_count == 1  # the probe; the rest wait out the outage
    assert sender._sender.in_outage
    assert _queue(sender).qsize() == 5  # nothing lost


@pytest.mark.asyncio
@pytest.mark.parametrize("make", SENDERS)
async def test_durable_loss_recorded_once_message_is_too_old(make, monkeypatch):
    monkeypatch.setattr(config, "SEND_MAX_AGE_SECONDS", 0)
    gov = _gov()
    sender = make(gov)
    ch = _ch(9, side_effect=dns_error())
    await _queue(sender).put((ch, discord.Embed(title="t"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    receipts = _receipts(gov)
    assert len(receipts) == 1
    assert receipts[0]["event_type"] == "bridge.delivery_failed"
    assert receipts[0]["reason"] == "discord_unreachable"
    assert "nodename" in receipts[0]["error"]
    assert receipts[0]["source_event_id"] == 1


@pytest.mark.asyncio
async def test_queue_full_during_outage_is_recorded():
    gov = _gov()
    sender = _poller(gov)
    ch = _ch(9)
    item = delivery.Outgoing(ch, discord.Embed(title="t"), EVENT)
    for i in range(_queue(sender).maxsize):
        _queue(sender).put_nowait((ch, discord.Embed(title=str(i)), None))
    await sender._sender._handle_unreachable(item, dns_error())
    (receipt,) = _receipts(gov)
    assert receipt["reason"] == "discord_unreachable"


@pytest.mark.asyncio
@pytest.mark.parametrize("make", SENDERS)
async def test_redelivered_four_tuple_item_flows_through_the_loop(make):
    # Older queue items carried a 4th field (redelivery count); the loop
    # accepts both shapes instead of dying on the unpack.
    gov = _gov()
    sender = make(gov)
    ch = _ch(9, side_effect=[MagicMock(id=1), MagicMock(id=2)])
    await _queue(sender).put((ch, discord.Embed(title="old"), EVENT, 2))
    await _queue(sender).put((ch, discord.Embed(title="new"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 2)

    assert ch.send.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("make", SENDERS)
async def test_malformed_item_does_not_kill_the_loop(make):
    gov = _gov()
    sender = make(gov)
    ch = _ch(9, side_effect=[MagicMock(id=1)])
    _queue(sender).put_nowait("garbage")
    _queue(sender).put_nowait((ch, discord.Embed(title="ok"), EVENT))

    task = await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    assert ch.send.await_count == 1
    assert task.cancelled()  # it was alive until we cancelled it


@pytest.mark.asyncio
async def test_loop_supervises_itself_after_an_escaping_error(caplog):
    # A receipt write that raises escapes per-message handling; the loop must
    # log it at error level and keep draining, not die silently.
    gov = _gov()
    gov.record_bridge_event = AsyncMock(side_effect=RuntimeError("receipt store down"))
    sender = _subscriber(gov)
    ch = _ch(9, side_effect=[MagicMock(id=1), MagicMock(id=2)])
    _queue(sender).put_nowait((ch, discord.Embed(title="a"), EVENT))
    _queue(sender).put_nowait((ch, discord.Embed(title="b"), EVENT))

    with caplog.at_level("ERROR"):
        await _run_send_loop(sender, lambda: ch.send.await_count >= 2)

    assert ch.send.await_count == 2
    assert sender._sender.loop_restarts >= 1
    assert any("send loop error" in r.message for r in caplog.records)


@pytest.mark.asyncio
@pytest.mark.parametrize("make", SENDERS)
async def test_send_loop_non_transient_http_error_unchanged(make):
    gov = _gov()
    sender = make(gov)
    ch = _ch(9, side_effect=http_error(403))
    await _queue(sender).put((ch, discord.Embed(title="t"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 1)

    assert ch.send.await_count == 1
    assert [r["reason"] for r in _receipts(gov)] == ["discord_http_exception"]


@pytest.mark.asyncio
@pytest.mark.parametrize("make", SENDERS)
async def test_send_loop_429_path_unchanged(make):
    gov = _gov()
    sender = make(gov)
    ch = _ch(9, side_effect=[http_error(429, {"Retry-After": "0"}), MagicMock(id=3)])
    await _queue(sender).put((ch, discord.Embed(title="t"), EVENT))

    await _run_send_loop(sender, lambda: gov.record_bridge_event.await_count >= 2)

    assert ch.send.await_count == 2
    assert [r["event_type"] for r in _receipts(gov)] == [
        "bridge.rate_limited", "bridge.delivery",
    ]


def test_send_queue_express_lane_preserves_item_identity():
    alerts = object()
    q = delivery.SendQueue(10, is_priority=lambda item: item[0] is alerts)
    q.put_nowait(("routine", 1))
    q.put_nowait((alerts, 2))
    q.put_nowait(("routine", 3))
    q.put_nowait((alerts, 4))
    assert [q.get_nowait()[1] for _ in range(4)] == [2, 4, 1, 3]
    assert q.empty()


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
async def test_unavailable_notice_survives_disconnect_without_retrying():
    poller, alerts = _iteration_poller(AsyncMock(side_effect=disconnected()))

    await poller._tick()
    await poller._tick()

    assert alerts.send.await_count == 1
    assert poller._unavailable_announced is False


@pytest.mark.asyncio
async def test_unavailable_notice_retries_dns_blip():
    poller, alerts = _iteration_poller(AsyncMock(side_effect=[dns_error(), MagicMock(id=1)]))

    await poller._tick()
    await poller._tick()

    assert alerts.send.await_count == 2
    assert poller._unavailable_announced is True
