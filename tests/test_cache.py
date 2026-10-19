import pytest
from bridge.cache import BridgeCache
from bridge.cache import FindingDeliveryCache
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock


@pytest.fixture
def cache(tmp_path):
    db_path = str(tmp_path / "test.db")
    return BridgeCache(db_path)


@pytest.mark.asyncio
async def test_event_cursor_default_zero(cache):
    async with cache:
        cursor = await cache.get_event_cursor()
        assert cursor == 0


@pytest.mark.asyncio
async def test_event_cursor_set_and_get(cache):
    async with cache:
        await cache.set_event_cursor(42)
        assert await cache.get_event_cursor() == 42


@pytest.mark.asyncio
async def test_event_cursor_corrupt_uuid_resets_to_zero(cache, caplog):
    # Simulates a past bug where governance emitted UUID event_ids and the
    # poller wrote one into the cursor. get_event_cursor must return 0 (not
    # raise) so the poll loop can recover.
    async with cache:
        await cache._db.execute(
            "INSERT INTO kv (key, value) VALUES ('event_cursor', ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            ("fcd718be-0243-4a26-b503-79d4a3d7bfb1",),
        )
        await cache._db.commit()
        cursor = await cache.get_event_cursor()
        assert cursor == 0


@pytest.mark.asyncio
async def test_hud_message(cache):
    async with cache:
        await cache.set_hud_message(111, 222)
        channel_id, message_id = await cache.get_hud_message()
        assert channel_id == 111
        assert message_id == 222


@pytest.mark.asyncio
async def test_sensor_message_round_trips_and_tolerates_corruption(cache):
    async with cache:
        assert await cache.get_sensor_message() is None
        await cache.set_sensor_message(333, 444)
        assert await cache.get_sensor_message() == (333, 444)
        await cache.set_kv("sensor_message", "not json")
        assert await cache.get_sensor_message() is None


def finding(**changes):
    return {"type": "sentinel_alarm_finding", "event_id": 6,
            "timestamp": "2026-10-14T08:32:47Z", "agent_id": "resident",
            "severity": "medium", "fingerprint": "lease-conflict",
            "message": "held by another writer", **changes}


def channel(channel_id=1):
    return SimpleNamespace(id=channel_id, send=AsyncMock(return_value=SimpleNamespace(id=123)))


@pytest.mark.asyncio
async def test_exact_concurrent_copies_send_once():
    cache, ch = FindingDeliveryCache(), channel()
    results = await asyncio.gather(*(cache.send(ch, None, finding()) for _ in range(3)))
    assert results == [(123, False), (123, True), (123, True)]
    assert ch.send.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"event_id": 7}, {"timestamp": "2026-10-14T09:32:47Z"},
    {"severity": "critical"}, {"message": "new evidence"},
    {"fingerprint": "different-lease"}, {"agent_id": "other-resident"},
    {"tags": ["critical"]}, {"evidence": {"assessment": "no_lease_row"}},
])
async def test_changed_findings_are_not_suppressed(change):
    cache, ch = FindingDeliveryCache(), channel()
    await cache.send(ch, None, finding())
    assert await cache.send(ch, None, finding(**change)) == (123, False)
    assert ch.send.await_count == 2


@pytest.mark.asyncio
async def test_destination_mirrors_are_preserved():
    cache, detail, alerts = FindingDeliveryCache(), channel(1), channel(2)
    await cache.send(detail, None, finding())
    assert await cache.send(alerts, None, finding()) == (123, False)
    assert detail.send.await_count == alerts.send.await_count == 1


@pytest.mark.asyncio
async def test_failed_send_does_not_prevent_other_sender_delivery():
    cache, ch = FindingDeliveryCache(), channel()
    ch.send.side_effect = [RuntimeError("transport failed"), SimpleNamespace(id=456)]
    with pytest.raises(RuntimeError):
        await cache.send(ch, None, finding())
    assert await cache.send(ch, None, finding()) == (456, False)
    assert not cache._pending


@pytest.mark.asyncio
@pytest.mark.parametrize("event", [finding(timestamp=None), finding(event_id=None),
                                         {"type": "lifecycle_silent_critical"}, None])
async def test_missing_identity_and_nonfindings_fail_open(event):
    cache, ch = FindingDeliveryCache(), channel()
    await cache.send(ch, None, event)
    assert await cache.send(ch, None, event) == (123, False)
    assert ch.send.await_count == 2


@pytest.mark.asyncio
async def test_cancelled_sender_releases_identical_waiter():
    cache, ch = FindingDeliveryCache(), channel()
    started, release = asyncio.Event(), asyncio.Event()
    async def slow_send(**kwargs):
        started.set()
        await release.wait()
        return SimpleNamespace(id=123)
    ch.send.side_effect = slow_send
    first = asyncio.create_task(cache.send(ch, None, finding()))
    await started.wait()
    second = asyncio.create_task(cache.send(ch, None, finding()))
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()
    assert await asyncio.wait_for(second, 1) == (123, False)
    assert not cache._pending


@pytest.mark.asyncio
async def test_unrelated_critical_does_not_wait_for_slow_destination():
    cache, slow, urgent = FindingDeliveryCache(), channel(1), channel(2)
    started, release = asyncio.Event(), asyncio.Event()
    async def slow_send(**kwargs):
        started.set()
        await release.wait()
        return SimpleNamespace(id=123)
    slow.send.side_effect = slow_send
    first = asyncio.create_task(cache.send(slow, None, finding()))
    await started.wait()
    try:
        assert await asyncio.wait_for(cache.send(urgent, None, finding(severity="critical")), 1) == (123, False)
    finally:
        release.set()
        await first


@pytest.mark.asyncio
async def test_dedup_memory_is_bounded_and_evicted_event_delivers():
    cache, ch = FindingDeliveryCache(max_entries=2), channel()
    for event_id in range(3):
        await cache.send(ch, None, finding(event_id=event_id))
    assert len(cache._sent) == 2
    assert await cache.send(ch, None, finding(event_id=0)) == (123, False)
