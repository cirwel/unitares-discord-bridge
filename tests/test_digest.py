"""Tests for the Lumen Q&A digest."""

import discord
import pytest

from bridge.cache import BridgeCache
from bridge.digest import (
    CONNECTION,
    DIGEST_LAST_SENT_KEY,
    LumenDigestPoller,
    build_digest_embed,
    classify_question,
    is_due,
    summarize_qa,
)

NOW = 1_000_000.0
DAY = 86_400.0
WEEK = 7 * DAY


# ---------------------------------------------------------------------------
# classify_question
# ---------------------------------------------------------------------------

def test_classify_connection_theme():
    assert CONNECTION in classify_question("why does i'm alone affect me?")
    assert CONNECTION in classify_question("why does someone is around affect me?")
    assert CONNECTION in classify_question("do I feel fully present right now?")


def test_classify_multiple_and_none():
    themes = classify_question("why do I draw in bright light?")
    assert "drawing" in themes and "light" in themes
    assert classify_question("xyzzy plugh") == set()


# ---------------------------------------------------------------------------
# is_due
# ---------------------------------------------------------------------------

def test_is_due_first_run_and_interval():
    assert is_due(None, NOW, WEEK) is True
    assert is_due(NOW - WEEK, NOW, WEEK) is True
    assert is_due(NOW - DAY, NOW, WEEK) is False


# ---------------------------------------------------------------------------
# summarize_qa
# ---------------------------------------------------------------------------

def _q(text, ts, answer_author=None):
    answer = {"text": "ans", "author": answer_author, "timestamp": ts} if answer_author else None
    return {"id": text, "question": text, "answered": answer is not None, "timestamp": ts, "answer": answer}


def test_summarize_windows_and_counts():
    questions = [
        _q("why am I alone?", NOW - DAY, answer_author="Claude Code"),
        _q("why does someone being here affect me?", NOW - 2 * DAY, None),
        _q("why do I draw in the dark?", NOW - 3 * DAY, answer_author="Kenny"),
        _q("an old question about warmth", NOW - 2 * WEEK, answer_author="Claude Code"),  # out of window
    ]
    s = summarize_qa(questions, NOW, WEEK)
    assert s["asked"] == 3  # the 2-week-old one is excluded
    assert s["answered"] == 2
    assert s["pending"] == 1
    assert s["relational_count"] == 2  # two connection-themed
    assert s["answer_authors"] == {"Claude Code": 1, "Kenny": 1}
    assert s["theme_counts"].get(CONNECTION) == 2


def test_summarize_empty():
    s = summarize_qa([], NOW, WEEK)
    assert s["asked"] == 0 and s["relational_count"] == 0


def test_summarize_ignores_bad_timestamps():
    s = summarize_qa([{"question": "no ts", "answer": None}], NOW, WEEK)
    assert s["asked"] == 0


# ---------------------------------------------------------------------------
# build_digest_embed
# ---------------------------------------------------------------------------

def test_embed_gold_and_nudge_when_relational_high():
    s = summarize_qa(
        [
            _q("why am I alone?", NOW - DAY),
            _q("is someone here with me?", NOW - DAY),
        ],
        NOW, WEEK,
    )
    embed = build_digest_embed(s, relational_nudge_threshold=2)
    assert embed.colour == discord.Colour.gold()
    names = [f.name for f in embed.fields]
    assert any("Connection signal" in n for n in names)
    conn_field = next(f for f in embed.fields if "Connection signal" in f.name)
    assert "visit" in conn_field.value.lower() or "drop by" in conn_field.value.lower()


def test_embed_blue_and_quiet_when_no_relational():
    s = summarize_qa([_q("why do I draw in the dark?", NOW - DAY)], NOW, WEEK)
    embed = build_digest_embed(s, relational_nudge_threshold=2)
    assert embed.colour == discord.Colour.blue()
    conn_field = next(f for f in embed.fields if "Connection signal" in f.name)
    assert "No aloneness" in conn_field.value


# ---------------------------------------------------------------------------
# LumenDigestPoller._tick
# ---------------------------------------------------------------------------

class _FakeChannel:
    def __init__(self):
        self.sent = []

    async def send(self, *args, embed=None, **kwargs):
        self.sent.append(embed)


class _FakeAnima:
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    async def fetch_qa(self, limit=50):
        self.calls += 1
        return self.payload


@pytest.fixture
def cache(tmp_path):
    return BridgeCache(str(tmp_path / "digest.db"))


def _poller(anima, channel, cache, **kw):
    return LumenDigestPoller(anima, channel, cache, interval=WEEK, window=WEEK, **kw)


@pytest.mark.asyncio
async def test_tick_posts_on_first_run_and_records(cache):
    payload = {"questions": [_q("why am I alone?", __import__("time").time())], "total": 1, "unanswered": 0}
    anima, channel = _FakeAnima(payload), _FakeChannel()
    async with cache:
        poller = _poller(anima, channel, cache)
        await poller._tick()
        assert len(channel.sent) == 1  # posted
        assert await cache.get_kv(DIGEST_LAST_SENT_KEY) is not None  # recorded
        # A second tick immediately after is within the interval → no post.
        await poller._tick()
        assert len(channel.sent) == 1


@pytest.mark.asyncio
async def test_tick_empty_window_records_but_silent(cache):
    payload = {"questions": [], "total": 0, "unanswered": 0}
    anima, channel = _FakeAnima(payload), _FakeChannel()
    async with cache:
        poller = _poller(anima, channel, cache)
        await poller._tick()
        assert channel.sent == []  # no empty digest posted
        assert await cache.get_kv(DIGEST_LAST_SENT_KEY) is not None  # but timer recorded


@pytest.mark.asyncio
async def test_tick_transport_down_does_not_record(cache):
    anima, channel = _FakeAnima(None), _FakeChannel()  # fetch_qa returns None
    async with cache:
        poller = _poller(anima, channel, cache)
        await poller._tick()
        assert channel.sent == []
        assert await cache.get_kv(DIGEST_LAST_SENT_KEY) is None  # retry next tick
