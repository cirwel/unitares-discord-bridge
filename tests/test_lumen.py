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
async def test_sensor_loop_offline_clears_pinned_msg():
    """After threshold consecutive failures, offline posts a new message and clears _sensor_msg."""
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
    assert poller._sensor_msg is None
    sensor_ch.send.assert_called_once()


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


@pytest.mark.asyncio
async def test_debounce_still_holds_with_a_mention_configured():
    """A single transient blip must not page anyone."""
    poller, anima, sensor_ch = _make_poller_with_mention()
    anima.fetch_state = AsyncMock(return_value=None)

    await poller._sensor_tick()

    assert poller._was_offline is False
    sensor_ch.send.assert_not_called()
