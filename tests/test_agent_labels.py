"""AgentLabelCache: one agent list per TTL, never one per event."""

import asyncio
import time

import pytest

from bridge.agent_labels import AgentLabelCache


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _cache(fetch, clock=None, **kwargs):
    return AgentLabelCache(object(), fetch=fetch, clock=clock or _Clock(), **kwargs)


async def _settle():
    """Let a background refresh that was started without waiting finish."""
    for _ in range(3):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_many_lookups_share_one_fetch_within_ttl():
    calls = []

    async def fetch(_gov):
        calls.append(1)
        return [{"id": "a1", "label": "Vigil"}, {"id": "b2", "label": "Sentinel"}]

    clock = _Clock()
    cache = _cache(fetch, clock, ttl=300)
    assert await cache.label_for("a1") == "Vigil"
    for _ in range(50):
        assert await cache.label_for("b2") == "Sentinel"
    assert await cache.label_for("unknown") is None
    assert len(calls) == 1

    clock.now += 301
    await cache.label_for("a1")
    await _settle()
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_label_equal_to_id_is_not_a_label():
    async def fetch(_gov):
        # fetch_agents falls back to the id when an agent has no label.
        return [{"id": "a1", "label": "a1"}]

    assert await _cache(fetch).label_for("a1") is None


@pytest.mark.asyncio
async def test_failed_fetch_keeps_last_labels_and_retries_after_failure_ttl():
    results = [[{"id": "a1", "label": "Vigil"}], RuntimeError("down"), []]
    calls = []

    async def fetch(_gov):
        calls.append(1)
        result = results[len(calls) - 1]
        if isinstance(result, Exception):
            raise result
        return result

    clock = _Clock()
    cache = _cache(fetch, clock, ttl=300, failure_ttl=60)
    assert await cache.label_for("a1") == "Vigil"
    clock.now += 301
    assert await cache.label_for("a1") == "Vigil"  # stale map served at once
    await _settle()
    assert len(calls) == 2
    assert await cache.label_for("a1") == "Vigil"  # error: old map kept
    clock.now += 30
    await cache.label_for("a1")
    await _settle()
    assert len(calls) == 2  # within failure TTL: no retry
    clock.now += 31
    await cache.label_for("a1")
    await _settle()
    assert len(calls) == 3
    assert await cache.label_for("a1") == "Vigil"  # empty list: old map kept


@pytest.mark.asyncio
async def test_slow_governance_does_not_stall_the_caller():
    release = asyncio.Event()

    async def fetch(_gov):
        await release.wait()
        return [{"id": "a1", "label": "Vigil"}]

    cache = _cache(fetch, wait=0.01)
    assert await cache.label_for("a1") is None  # gave up waiting
    release.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert await cache.label_for("a1") == "Vigil"
    await cache.close()


@pytest.mark.asyncio
async def test_lookups_during_a_hung_refresh_do_not_each_wait():
    # One hung refresh must cost the WebSocket reader at most one ``wait`` in
    # total, not one per event, whether or not a map is cached.
    release = asyncio.Event()
    calls = []

    async def hung(_gov):
        calls.append(1)
        await release.wait()
        return [{"id": "a1", "label": "Vigil"}]

    wait = 0.2

    # Never loaded: only the lookup that starts the first load waits.
    cold = _cache(hung, wait=wait)
    started = time.monotonic()
    for _ in range(10):
        assert await cold.label_for("a1") is None
        await asyncio.sleep(0)
    assert time.monotonic() - started < 2 * wait
    assert len(calls) == 1
    await cold.close()

    # Loaded but stale: no lookup waits; each gets the cached label.
    calls.clear()
    clock = _Clock()
    warm = _cache(hung, clock, wait=wait, ttl=300)
    release.set()
    assert await warm.label_for("a1") == "Vigil"
    release.clear()
    clock.now += 301
    started = time.monotonic()
    for _ in range(10):
        assert await warm.label_for("a1") == "Vigil"
        await asyncio.sleep(0)
    assert time.monotonic() - started < wait
    assert len(calls) == 2  # one refresh, shared
    release.set()
    await warm.close()


@pytest.mark.asyncio
async def test_default_fetch_does_not_track_governance_failures():
    seen = []

    class _Gov:
        async def call_tool(self, name, arguments, *, track_failures=True):
            seen.append(track_failures)
            return None

    cache = AgentLabelCache(_Gov(), clock=_Clock())
    assert await cache.label_for("a1") is None
    assert seen == [False]


@pytest.mark.asyncio
async def test_missing_agent_id_does_not_fetch():
    async def fetch(_gov):  # pragma: no cover - must not run
        raise AssertionError("fetched")

    assert await _cache(fetch).label_for(None) is None
    assert await _cache(fetch).label_for("") is None
