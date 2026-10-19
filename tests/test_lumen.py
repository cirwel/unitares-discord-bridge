from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest

from bridge.lumen import LumenPoller, build_sensor_embed, build_drawing_embed


def test_sensor_embed():
    state = {
        "ambient_temp": 24.3, "humidity": 38.0, "pressure": 827.0,
        "light": 142.0, "cpu_temp": 62.0, "memory_percent": 41.0,
        "warmth": 0.7, "clarity": 0.6, "stability": 0.8, "presence": 0.5,
        "neural": {"delta": 0.8, "theta": 0.3, "alpha": 0.6, "beta": 0.5, "gamma": 0.4},
    }
    embed = build_sensor_embed(state)
    assert isinstance(embed, discord.Embed)
    assert "24.3" in embed.description
    assert "827" in embed.description
    assert "\u03b4=" in embed.description


def test_sensor_embed_missing_neural():
    state = {"ambient_temp": 20.0}
    embed = build_sensor_embed(state)
    assert isinstance(embed, discord.Embed)


def test_drawing_embed():
    drawing = {"filename": "lumen_drawing_20260223_140000.png", "era": "geometric", "manual": False}
    embed = build_drawing_embed(drawing)
    assert isinstance(embed, discord.Embed)
    assert "geometric" in embed.description.lower()


def test_drawing_embed_manual():
    drawing = {"filename": "test.png", "era": "pointillist", "manual": True}
    embed = build_drawing_embed(drawing)
    # Should indicate manual somehow
    found = False
    for field in embed.fields:
        if "yes" in str(field.value).lower():
            found = True
    assert found


# ---------------------------------------------------------------------------
# Sensor loop edit-in-place tests
# ---------------------------------------------------------------------------

SAMPLE_STATE = {
    "ambient_temp": 24.3, "humidity": 38.0, "pressure": 827.0,
    "light": 142.0, "cpu_temp": 62.0, "memory_percent": 41.0,
    "warmth": 0.7, "clarity": 0.6, "stability": 0.8, "presence": 0.5,
    "neural": {"delta": 0.8, "theta": 0.3, "alpha": 0.6, "beta": 0.5, "gamma": 0.4},
}


def _make_poller():
    anima = AsyncMock()
    art_ch = AsyncMock(spec=discord.TextChannel)
    sensor_ch = AsyncMock(spec=discord.TextChannel)
    return LumenPoller(anima, art_ch, sensor_ch, sensor_interval=0), anima, sensor_ch


@pytest.mark.asyncio
async def test_sensor_loop_posts_once_then_edits():
    """First poll sends a new message; second poll edits it."""
    poller, anima, sensor_ch = _make_poller()
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)

    sent_msg = AsyncMock(spec=discord.Message)
    sensor_ch.send = AsyncMock(return_value=sent_msg)

    # Simulate first iteration (no existing message)
    await poller._sensor_tick()
    sensor_ch.send.assert_called_once()
    assert poller._sensor_msg is sent_msg

    # Simulate second iteration (should edit, not send)
    sensor_ch.send.reset_mock()
    await poller._sensor_tick()
    sensor_ch.send.assert_not_called()
    sent_msg.edit.assert_called_once()


@pytest.mark.asyncio
async def test_sensor_loop_falls_back_on_not_found():
    """If the pinned message was deleted, post a new one."""
    poller, anima, sensor_ch = _make_poller()
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)

    stale_msg = AsyncMock(spec=discord.Message)
    stale_msg.edit = AsyncMock(side_effect=discord.NotFound(MagicMock(), "gone"))
    poller._sensor_msg = stale_msg

    new_msg = AsyncMock(spec=discord.Message)
    sensor_ch.send = AsyncMock(return_value=new_msg)

    await poller._sensor_tick()
    sensor_ch.send.assert_called_once()
    assert poller._sensor_msg is new_msg


@pytest.mark.asyncio
async def test_sensor_loop_offline_marks_card_for_repost_without_orphaning_it():
    """Offline posts a notice; the card is re-posted below it on recovery and
    the old card is deleted, not left frozen above the live one."""
    poller, anima, sensor_ch = _make_poller()
    anima.fetch_state = AsyncMock(return_value=None)

    existing_msg = AsyncMock(spec=discord.Message)
    poller._sensor_msg = existing_msg

    # Default threshold is 2: first None must not flip state.
    await poller._sensor_tick()
    assert poller._was_offline is False
    sensor_ch.send.assert_not_called()

    # Second consecutive None crosses the threshold.
    await poller._sensor_tick()
    assert poller._was_offline is True
    assert poller._card_needs_repost is True
    sensor_ch.send.assert_called_once()

    # Recovery: "Lumen Online", then a fresh card; the old card is deleted.
    new_card = AsyncMock(spec=discord.Message)
    sensor_ch.send = AsyncMock(side_effect=[AsyncMock(spec=discord.Message), new_card])
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)
    await poller._sensor_tick()

    assert sensor_ch.send.await_count == 2
    assert sensor_ch.send.call_args_list[0].kwargs["embed"].title == "Lumen Online"
    assert sensor_ch.send.call_args_list[1].kwargs["embed"].title == "Lumen Environment"
    assert poller._sensor_msg is new_card
    existing_msg.edit.assert_not_called()
    existing_msg.delete.assert_awaited_once()
    assert poller._card_needs_repost is False


@pytest.mark.asyncio
async def test_sensor_loop_single_transient_failure_does_not_announce_offline():
    """One transient None tick is debounced — no Offline embed, no _was_offline flip."""
    poller, anima, sensor_ch = _make_poller()
    # First call fails, then recovers.
    anima.fetch_state = AsyncMock(side_effect=[None, SAMPLE_STATE])

    sent_msg = AsyncMock(spec=discord.Message)
    sensor_ch.send = AsyncMock(return_value=sent_msg)

    await poller._sensor_tick()
    assert poller._was_offline is False
    # No Offline announcement.
    sensor_ch.send.assert_not_called()

    # Recovery tick: posts the regular sensor embed (first time), not "Lumen Online".
    await poller._sensor_tick()
    assert poller._was_offline is False
    sensor_ch.send.assert_called_once()
    sent_embed = sensor_ch.send.call_args.kwargs.get("embed")
    assert sent_embed is not None
    assert "Online" not in (sent_embed.title or "")
    assert "Offline" not in (sent_embed.title or "")


@pytest.mark.asyncio
async def test_sensor_loop_failure_counter_resets_on_success():
    """A success tick between failures resets the counter — threshold must be consecutive."""
    poller, anima, sensor_ch = _make_poller()
    anima.fetch_state = AsyncMock(side_effect=[None, SAMPLE_STATE, None])

    sent_msg = AsyncMock(spec=discord.Message)
    sensor_ch.send = AsyncMock(return_value=sent_msg)

    await poller._sensor_tick()  # fail (count=1)
    await poller._sensor_tick()  # success (count reset)
    await poller._sensor_tick()  # fail (count=1, not 2)

    assert poller._was_offline is False


# ---------------------------------------------------------------------------
# Outage alerts must actually notify
# ---------------------------------------------------------------------------
#
# The bridge detected the July 2026 outage correctly and posted an embed nobody
# was looking at. A channel post is a record; only a mention reaches a phone.


def _make_poller_with_mention(mention="<@&999>"):
    anima = AsyncMock()
    art_ch = AsyncMock(spec=discord.TextChannel)
    sensor_ch = AsyncMock(spec=discord.TextChannel)
    poller = LumenPoller(
        anima, art_ch, sensor_ch, sensor_interval=0, offline_mention=mention
    )
    return poller, anima, sensor_ch


async def _drive_offline(poller, anima):
    anima.fetch_state = AsyncMock(return_value=None)
    await poller._sensor_tick()
    await poller._sensor_tick()


@pytest.mark.asyncio
async def test_offline_post_carries_the_configured_mention():
    poller, anima, sensor_ch = _make_poller_with_mention()
    await _drive_offline(poller, anima)

    assert poller._was_offline is True
    kwargs = sensor_ch.send.call_args.kwargs
    assert kwargs["content"] == "<@&999>"
    assert kwargs["allowed_mentions"].everyone is False
    assert kwargs["allowed_mentions"].roles is True


@pytest.mark.asyncio
async def test_recovery_post_carries_the_mention_too():
    """Whoever was woken by the outage should not have to poll to learn it ended."""
    poller, anima, sensor_ch = _make_poller_with_mention()
    await _drive_offline(poller, anima)
    sensor_ch.send.reset_mock()

    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)
    sensor_ch.send = AsyncMock(return_value=AsyncMock(spec=discord.Message))
    await poller._sensor_tick()

    assert poller._was_offline is False
    assert sensor_ch.send.call_args_list[0].kwargs["content"] == "<@&999>"


@pytest.mark.asyncio
async def test_routine_sensor_post_is_never_mentioned():
    """Only the offline/recovery transitions ping. A status feed must stay silent."""
    poller, anima, sensor_ch = _make_poller_with_mention()
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)
    sensor_ch.send = AsyncMock(return_value=AsyncMock(spec=discord.Message))

    await poller._sensor_tick()

    assert "content" not in sensor_ch.send.call_args.kwargs


@pytest.mark.asyncio
async def test_unset_mention_keeps_todays_silent_behaviour():
    poller, anima, sensor_ch = _make_poller_with_mention(mention="")
    await _drive_offline(poller, anima)

    assert poller._was_offline is True
    assert "content" not in sensor_ch.send.call_args.kwargs
    assert "allowed_mentions" not in sensor_ch.send.call_args.kwargs


# ---------------------------------------------------------------------------
# One live sensor card across restarts and when it scrolls out of view
# ---------------------------------------------------------------------------
#
# The last three "Lumen Environment" posts matched the three bridge restarts
# exactly: the card id lived only in memory, so every restart (and every
# offline/online cycle) posted a new card and left the old one frozen.


class _History:
    def __init__(self, messages):
        self._messages = list(messages)

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for message in self._messages:
            yield message


def _message(message_id):
    msg = AsyncMock(spec=discord.Message)
    msg.id = message_id
    return msg


def _close_loop(coro, name=None):
    """Stand-in for create_logged_task: start() is tested, the loops are not."""
    coro.close()


def _make_cached_poller(cached=None, **kwargs):
    anima = AsyncMock()
    anima.fetch_gallery = AsyncMock(return_value=[])
    art_ch = AsyncMock(spec=discord.TextChannel)
    sensor_ch = AsyncMock(spec=discord.TextChannel)
    sensor_ch.id = 555
    cache = AsyncMock()
    cache.get_sensor_message = AsyncMock(return_value=cached)
    cache.set_sensor_message = AsyncMock()
    poller = LumenPoller(
        anima, art_ch, sensor_ch, sensor_interval=0, cache=cache, **kwargs
    )
    return poller, anima, sensor_ch, cache


@pytest.mark.asyncio
async def test_card_is_restored_on_start_and_edited_not_reposted():
    card = _message(42)
    poller, anima, sensor_ch, cache = _make_cached_poller(cached=(555, 42))
    sensor_ch.fetch_message = AsyncMock(return_value=card)

    with patch("bridge.lumen.create_logged_task", side_effect=_close_loop):
        await poller.start()
    sensor_ch.fetch_message.assert_awaited_once_with(42)
    assert poller._sensor_msg is card

    sensor_ch.history = MagicMock(return_value=_History([_message(101), card]))
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)
    await poller._sensor_tick()
    card.edit.assert_awaited_once()
    sensor_ch.send.assert_not_called()


@pytest.mark.asyncio
async def test_restored_card_buried_during_downtime_moves_on_the_first_tick():
    """Not card_check_every ticks later: chatter may have buried it while down."""
    card = _message(42)
    poller, anima, sensor_ch, cache = _make_cached_poller(
        cached=(555, 42), card_recent_window=3, card_check_every=6
    )
    sensor_ch.fetch_message = AsyncMock(return_value=card)
    with patch("bridge.lumen.create_logged_task", side_effect=_close_loop):
        await poller.start()

    sensor_ch.history = MagicMock(
        return_value=_History([_message(101), _message(102), _message(103)])
    )
    new_card = _message(88)
    sensor_ch.send = AsyncMock(return_value=new_card)
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)
    await poller._sensor_tick()

    sensor_ch.history.assert_called_once_with(limit=3)
    assert poller._sensor_msg is new_card
    card.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_card_restore_falls_back_to_a_new_post_when_not_found():
    poller, anima, sensor_ch, cache = _make_cached_poller(cached=(555, 42))
    sensor_ch.fetch_message = AsyncMock(
        side_effect=discord.NotFound(MagicMock(), "gone")
    )

    with patch("bridge.lumen.create_logged_task", side_effect=_close_loop):
        await poller.start()
    assert poller._sensor_msg is None

    new_card = _message(77)
    sensor_ch.send = AsyncMock(return_value=new_card)
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)
    await poller._sensor_tick()

    assert poller._sensor_msg is new_card
    cache.set_sensor_message.assert_awaited_once_with(555, 77)


@pytest.mark.asyncio
async def test_cached_card_in_another_channel_is_ignored():
    poller, _anima, sensor_ch, _cache = _make_cached_poller(cached=(999, 42))
    sensor_ch.fetch_message = AsyncMock()

    with patch("bridge.lumen.create_logged_task", side_effect=_close_loop):
        await poller.start()

    sensor_ch.fetch_message.assert_not_called()
    assert poller._sensor_msg is None


@pytest.mark.asyncio
async def test_buried_card_is_reposted_on_the_check_tick_only():
    old_card = _message(42)
    poller, anima, sensor_ch, cache = _make_cached_poller(
        card_recent_window=3, card_check_every=2
    )
    poller._sensor_msg = old_card
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)
    sensor_ch.history = MagicMock(
        return_value=_History([_message(101), _message(102), _message(103)])
    )
    new_card = _message(88)
    sensor_ch.send = AsyncMock(return_value=new_card)

    await poller._sensor_tick()  # tick 1: edit only, no history read
    sensor_ch.history.assert_not_called()
    sensor_ch.send.assert_not_called()

    await poller._sensor_tick()  # tick 2: card not in last 3 -> re-post
    sensor_ch.history.assert_called_once_with(limit=3)
    sensor_ch.send.assert_awaited_once()
    assert poller._sensor_msg is new_card
    old_card.delete.assert_awaited_once()
    cache.set_sensor_message.assert_awaited_once_with(555, 88)


@pytest.mark.asyncio
async def test_recent_card_is_left_in_place():
    card = _message(42)
    poller, anima, sensor_ch, _cache = _make_cached_poller(
        card_recent_window=3, card_check_every=1
    )
    poller._sensor_msg = card
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)
    sensor_ch.history = MagicMock(return_value=_History([_message(101), card]))

    await poller._sensor_tick()

    sensor_ch.send.assert_not_called()
    card.delete.assert_not_called()
    assert poller._sensor_msg is card


@pytest.mark.asyncio
async def test_history_read_failure_does_not_churn_the_card():
    card = _message(42)
    poller, anima, sensor_ch, _cache = _make_cached_poller(card_check_every=1)
    poller._sensor_msg = card
    anima.fetch_state = AsyncMock(return_value=SAMPLE_STATE)
    sensor_ch.history = MagicMock(side_effect=discord.HTTPException(MagicMock(), "x"))

    await poller._sensor_tick()

    sensor_ch.send.assert_not_called()
    card.delete.assert_not_called()


@pytest.mark.asyncio
async def test_debounce_still_holds_with_a_mention_configured():
    """A single transient blip must not page anyone."""
    poller, anima, sensor_ch = _make_poller_with_mention()
    anima.fetch_state = AsyncMock(return_value=None)

    await poller._sensor_tick()

    assert poller._was_offline is False
    sensor_ch.send.assert_not_called()
