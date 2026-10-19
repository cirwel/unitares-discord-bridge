"""The bridge must not parse other bots' text commands as its own."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from discord.ext import commands

from bridge import bot as bot_module


def _message(content: str) -> MagicMock:
    msg = MagicMock()
    msg.content = content
    return msg


def _bot_user() -> MagicMock:
    user = MagicMock()
    user.id = 4242
    return user


def test_bridge_registers_no_text_commands_beyond_builtin_help():
    # Operator commands live on bot.tree (slash). The only text command is
    # discord.py's built-in help, which stays reachable by @mention.
    assert [c.name for c in bot_module.bot.commands] == ["help"]


def test_dispatch_prefix_is_not_a_bridge_prefix():
    bot = bot_module.bot
    with patch.object(type(bot), "user", new=_bot_user()):
        prefixes = bot.command_prefix(bot, _message("!d status"))
    assert "!" not in prefixes
    assert not any("!d status".startswith(p) for p in prefixes)


def test_explicit_mention_still_addresses_the_bridge():
    bot = bot_module.bot
    with patch.object(type(bot), "user", new=_bot_user()):
        prefixes = bot.command_prefix(bot, _message("<@4242> help"))
    assert "<@4242> " in prefixes


@pytest.mark.asyncio
async def test_command_not_found_is_silenced():
    bot = bot_module.bot
    with patch.object(commands.Bot, "on_command_error", new=AsyncMock()) as default:
        await bot.on_command_error(MagicMock(), commands.CommandNotFound('Command "d" is not found'))
    default.assert_not_awaited()


@pytest.mark.asyncio
async def test_other_command_errors_keep_default_handling():
    bot = bot_module.bot
    err = commands.CommandError("real failure")
    ctx = MagicMock()
    with patch.object(commands.Bot, "on_command_error", new=AsyncMock()) as default:
        await bot.on_command_error(ctx, err)
    default.assert_awaited_once_with(ctx, err)
