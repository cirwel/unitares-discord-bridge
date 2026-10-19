"""Bridge log hygiene: survives rotation, drops httpx noise, typed and
rate-limited failure warnings.

Live evidence (2026-10): the launchd log went blind after each newsyslog
rotation until the next restart; 304,740 of 316,327 lines were httpx 200-OK
lines; every discord.py record was written twice; 299 of 311 mcp_client
warnings had an empty cause; one Lumen outage wrote 3,572 fetch_gallery
warnings.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
from unittest.mock import MagicMock, patch

import httpx
import pytest

from bridge import bot as bot_module
from bridge import mcp_client
from bridge.mcp_client import AnimaClient, GovernanceClient

from tests.test_mcp_client import (
    make_mock_response,
    mock_httpx_client,
    mock_httpx_client_error,
)


@pytest.fixture
def restore_logging():
    yield
    bot_module.configure_logging("")


def test_httpx_logger_is_warning(restore_logging):
    bot_module.configure_logging("")
    assert logging.getLogger("httpx").level == logging.WARNING


def test_without_log_path_logs_to_stderr(restore_logging):
    handler = bot_module.configure_logging("")
    assert type(handler) is logging.StreamHandler
    assert handler in logging.getLogger().handlers


def test_log_path_attaches_a_watched_file_handler(tmp_path, restore_logging):
    path = tmp_path / "logs" / "bridge.log"
    handler = bot_module.configure_logging(str(path))
    assert isinstance(handler, logging.handlers.WatchedFileHandler)
    assert handler in logging.getLogger().handlers
    assert os.path.abspath(handler.baseFilename) == str(path)


def test_reconfiguring_replaces_the_handler_instead_of_stacking(tmp_path, restore_logging):
    first = bot_module.configure_logging(str(tmp_path / "a.log"))
    second = bot_module.configure_logging(str(tmp_path / "b.log"))
    root_handlers = logging.getLogger().handlers
    assert second in root_handlers
    assert first not in root_handlers


def test_log_keeps_filling_after_rotation(tmp_path, restore_logging):
    """newsyslog renames the file; the next record must land in a new file
    at the original path, not in the archive."""
    path = tmp_path / "bridge.log"
    bot_module.configure_logging(str(path))
    test_log = logging.getLogger("bridge.rotation-test")
    test_log.warning("before rotation")
    os.rename(path, tmp_path / "bridge.log.0")
    test_log.warning("after rotation")
    for h in logging.getLogger().handlers:
        h.flush()
    assert "after rotation" in path.read_text()
    assert "after rotation" not in (tmp_path / "bridge.log.0").read_text()


def test_discord_py_gets_no_second_handler():
    with (
        patch.object(bot_module, "DISCORD_TOKEN", "x"),
        patch.object(bot_module, "setup_commands"),
        patch.object(bot_module, "setup_acks"),
        patch.object(bot_module.bot, "run") as run,
    ):
        bot_module.main()
    assert run.call_args.kwargs.get("log_handler", "missing") is None


# --- typed causes ---

@pytest.mark.asyncio
async def test_timeout_cause_is_typed_and_not_empty(caplog):
    with mock_httpx_client_error("get", httpx.ReadTimeout("")):
        client = GovernanceClient("http://gov")
        with caplog.at_level(logging.WARNING, logger="bridge.mcp_client"):
            assert await client.fetch_events() is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "ReadTimeout" in message
    assert not message.rstrip().endswith(":")


@pytest.mark.asyncio
async def test_anima_timeout_cause_is_typed(caplog):
    with mock_httpx_client_error("get", httpx.ConnectTimeout("")):
        client = AnimaClient("http://anima")
        with caplog.at_level(logging.WARNING, logger="bridge.mcp_client"):
            assert await client.fetch_gallery() is None
    assert "ConnectTimeout" in caplog.text


# --- rate limiting ---

def _levels(caplog, level):
    return [r for r in caplog.records if r.levelno == level and r.name == "bridge.mcp_client"]


@pytest.mark.asyncio
async def test_repeated_governance_failures_warn_once(caplog):
    n = mcp_client.FAILURE_LOG_EVERY - 1
    client = GovernanceClient("http://gov")
    with mock_httpx_client_error("get", httpx.ConnectError("refused")):
        with caplog.at_level(logging.DEBUG, logger="bridge.mcp_client"):
            for _ in range(n):
                await client.fetch_events()
    assert len(_levels(caplog, logging.WARNING)) == 1
    # The alert counter is unchanged: it still counts every failure.
    assert client.consecutive_failures == n


@pytest.mark.asyncio
async def test_governance_streak_warns_again_every_nth(caplog):
    every = mcp_client.FAILURE_LOG_EVERY
    client = GovernanceClient("http://gov")
    with mock_httpx_client_error("get", httpx.ConnectError("refused")):
        with caplog.at_level(logging.WARNING, logger="bridge.mcp_client"):
            for _ in range(every * 2):
                await client.fetch_events()
    # Failures 1, N and 2N.
    assert len(_levels(caplog, logging.WARNING)) == 3


@pytest.mark.asyncio
async def test_governance_recovery_logs_one_info(caplog):
    client = GovernanceClient("http://gov")
    with mock_httpx_client_error("get", httpx.ConnectError("refused")):
        for _ in range(3):
            await client.fetch_events()
    with mock_httpx_client("get", make_mock_response(json_data=[])):
        with caplog.at_level(logging.INFO, logger="bridge.mcp_client"):
            await client.fetch_events()
            await client.fetch_events()
    recovered = [r for r in _levels(caplog, logging.INFO) if "recovered" in r.getMessage()]
    assert len(recovered) == 1
    assert "3" in recovered[0].getMessage()
    assert client.consecutive_failures == 0


@pytest.mark.asyncio
async def test_repeated_anima_failures_warn_once_then_recover(caplog):
    n = mcp_client.FAILURE_LOG_EVERY - 1
    client = AnimaClient("http://anima")
    with mock_httpx_client_error("get", httpx.ConnectError("refused")):
        with caplog.at_level(logging.DEBUG, logger="bridge.mcp_client"):
            for _ in range(n):
                await client.fetch_gallery()
    assert len(_levels(caplog, logging.WARNING)) == 1

    caplog.clear()
    resp = make_mock_response(json_data={"drawings": []})
    with mock_httpx_client("get", resp):
        with caplog.at_level(logging.INFO, logger="bridge.mcp_client"):
            assert await client.fetch_gallery() == []
    assert len([r for r in _levels(caplog, logging.INFO) if "recovered" in r.getMessage()]) == 1


@pytest.mark.asyncio
async def test_anima_streaks_are_per_method(caplog):
    """A gallery outage must not hide the first state failure."""
    client = AnimaClient("http://anima")
    with mock_httpx_client_error("get", httpx.ConnectError("refused")):
        with caplog.at_level(logging.WARNING, logger="bridge.mcp_client"):
            await client.fetch_gallery()
            await client.fetch_gallery()
            await client.fetch_state()
    assert len(_levels(caplog, logging.WARNING)) == 2
    assert client.is_online is False


# --- review follow-ups ---

def test_cause_uses_the_message_and_reprs_only_an_empty_one():
    assert mcp_client._cause(httpx.ConnectError("refused")) == "ConnectError: refused"
    assert mcp_client._cause(httpx.ReadTimeout("")) == "ReadTimeout: ReadTimeout('')"


@pytest.mark.asyncio
async def test_drawing_failure_warning_names_the_file(caplog):
    with mock_httpx_client_error("get", httpx.ReadTimeout("")):
        client = AnimaClient("http://anima")
        with caplog.at_level(logging.WARNING, logger="bridge.mcp_client"):
            assert await client.fetch_drawing_image("art_042.png") is None
    warnings = _levels(caplog, logging.WARNING)
    assert len(warnings) == 1
    assert "art_042.png" in warnings[0].getMessage()


def test_log_path_expands_home_and_is_absolute(monkeypatch):
    from bridge import config
    monkeypatch.setenv("HOME", "/Users/someone")
    assert config._log_path("~/Library/Logs/bridge.log") == "/Users/someone/Library/Logs/bridge.log"
    assert os.path.isabs(config._log_path("logs/bridge.log"))
    assert config._log_path("  ") == ""
