"""Tests for env-driven bridge configuration."""

import importlib

from bridge import config


def _reload_with(monkeypatch, **env) -> object:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return importlib.reload(config)


def test_channel_key_normalises_to_a_legal_channel_name():
    assert config._channel_key("  Sentinel ") == "sentinel"
    assert config._channel_key("Field Doctor") == "field-doctor"
    assert config._channel_key("") == ""


def test_resident_finding_channels_default_to_sentinel_and_doctor(monkeypatch):
    monkeypatch.delenv("BRIDGE_RESIDENT_FINDING_CHANNELS", raising=False)
    try:
        reloaded = importlib.reload(config)
        assert reloaded.RESIDENT_FINDING_CHANNELS == ("sentinel", "doctor")
    finally:
        importlib.reload(config)


def test_resident_finding_channels_parse_and_dedupe(monkeypatch):
    try:
        reloaded = _reload_with(
            monkeypatch,
            BRIDGE_RESIDENT_FINDING_CHANNELS=" Sentinel , doctor,sentinel, ,vigil",
        )
        assert reloaded.RESIDENT_FINDING_CHANNELS == ("sentinel", "doctor", "vigil")
    finally:
        monkeypatch.delenv("BRIDGE_RESIDENT_FINDING_CHANNELS", raising=False)
        importlib.reload(config)


def test_resident_finding_channels_can_be_disabled(monkeypatch):
    # Empty value puts every resident back on the shared #residents feed.
    try:
        reloaded = _reload_with(monkeypatch, BRIDGE_RESIDENT_FINDING_CHANNELS="")
        assert reloaded.RESIDENT_FINDING_CHANNELS == ()
    finally:
        monkeypatch.delenv("BRIDGE_RESIDENT_FINDING_CHANNELS", raising=False)
        importlib.reload(config)


def test_self_iteration_attention_defaults_enabled(monkeypatch):
    monkeypatch.delenv("LUMEN_SELF_ITERATION_ENABLED", raising=False)
    monkeypatch.delenv("LUMEN_SELF_ITERATION_POLL_INTERVAL", raising=False)
    try:
        reloaded = importlib.reload(config)
        assert reloaded.SELF_ITERATION_ENABLED is True
        assert reloaded.SELF_ITERATION_POLL_INTERVAL == 60
    finally:
        importlib.reload(config)


def test_self_iteration_attention_can_be_configured(monkeypatch):
    try:
        reloaded = _reload_with(
            monkeypatch,
            LUMEN_SELF_ITERATION_ENABLED="false",
            LUMEN_SELF_ITERATION_POLL_INTERVAL="15",
        )
        assert reloaded.SELF_ITERATION_ENABLED is False
        assert reloaded.SELF_ITERATION_POLL_INTERVAL == 15
    finally:
        monkeypatch.delenv("LUMEN_SELF_ITERATION_ENABLED", raising=False)
        monkeypatch.delenv("LUMEN_SELF_ITERATION_POLL_INTERVAL", raising=False)
        importlib.reload(config)
