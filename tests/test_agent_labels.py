"""AgentLabelCache: one agent list per TTL, never one per event."""

import asyncio

import pytest

from bridge.agent_labels import AgentLabelCache


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _cache(fetch, clock=None, **kwargs):
    return AgentLabelCache(object(), fetch=fetch, clock=clock or _Clock(), **kwargs)


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
    assert await cache.label_for("a1") == "Vigil"  # error: old map kept
    assert len(calls) == 2
    clock.now += 30
    await cache.label_for("a1")
    assert len(calls) == 2  # within failure TTL: no retry
    clock.now += 31
    assert await cache.label_for("a1") == "Vigil"  # empty list: old map kept
    assert len(calls) == 3


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
async def test_missing_agent_id_does_not_fetch():
    async def fetch(_gov):  # pragma: no cover - must not run
        raise AssertionError("fetched")

    assert await _cache(fetch).label_for(None) is None
    assert await _cache(fetch).label_for("") is None
