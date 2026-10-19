import re

import discord
from bridge.hud import build_hud_embed


def test_hud_embed_with_agents():
    agents = [{"id": "a1", "label": "opus_hikewa"}, {"id": "a2", "label": "sonnet_review"}]
    metrics = {
        "a1": {"E": 0.74, "I": 0.71, "S": 0.42, "V": 0.08, "verdict": "proceed"},
        "a2": {"E": 0.61, "I": 0.58, "S": 0.89, "V": 0.31, "verdict": "guide"},
    }
    embed = build_hud_embed(agents, metrics)
    assert isinstance(embed, discord.Embed)
    assert "opus_hikewa" in embed.description
    assert "sonnet_review" in embed.description


def test_hud_embed_empty():
    embed = build_hud_embed([], {})
    assert "No active agents" in embed.description


def test_hud_counts_paused():
    agents = [{"id": "a1", "label": "test"}]
    metrics = {"a1": {"E": 0.3, "I": 0.2, "S": 1.5, "V": 1.0, "verdict": "pause"}}
    embed = build_hud_embed(agents, metrics)
    assert "1 paused" in embed.footer.text


def test_hud_counts_boundary():
    agents = [
        {"id": "a1", "label": "agent_one"},
        {"id": "a2", "label": "agent_two"},
    ]
    metrics = {
        "a1": {"E": 0.5, "I": 0.5, "S": 0.5, "V": 0.5, "verdict": "guide"},
        "a2": {"E": 0.5, "I": 0.5, "S": 0.5, "V": 0.5, "verdict": "proceed"},
    }
    embed = build_hud_embed(agents, metrics)
    assert "1 boundary" in embed.footer.text
    assert "2 agents" in embed.footer.text


def test_hud_verdict_emojis():
    agents = [
        {"id": "a1", "label": "green"},
        {"id": "a2", "label": "yellow"},
        {"id": "a3", "label": "red"},
        {"id": "a4", "label": "blocked"},
    ]
    metrics = {
        "a1": {"E": 0.5, "I": 0.5, "S": 0.5, "V": 0.1, "verdict": "proceed"},
        "a2": {"E": 0.5, "I": 0.5, "S": 0.5, "V": 0.3, "verdict": "guide"},
        "a3": {"E": 0.5, "I": 0.5, "S": 0.5, "V": 0.9, "verdict": "pause"},
        "a4": {"E": 0.5, "I": 0.5, "S": 0.5, "V": 1.0, "verdict": "reject"},
    }
    embed = build_hud_embed(agents, metrics)
    # Green circle for proceed
    assert "\U0001f7e2" in embed.description
    # Yellow circle for guide
    assert "\U0001f7e1" in embed.description
    # Red circle for pause
    assert "\U0001f534" in embed.description
    # No entry for reject
    assert "\u26d4" in embed.description


def test_hud_title():
    embed = build_hud_embed([], {})
    assert embed.title == "UNITARES Governance \u2014 Live"


def test_hud_agent_without_metrics():
    """Agent present in list but missing from metrics dict."""
    agents = [{"id": "a1", "label": "orphan_agent"}]
    metrics = {}
    embed = build_hud_embed(agents, metrics)
    assert "orphan_agent" in embed.description
    assert "1 agents" in embed.footer.text


def test_hud_carries_a_real_dated_timestamp():
    """The HUD is edited in place; without a timestamp it reads as frozen."""
    for agents in ([], [{"id": "a1", "label": "opus"}]):
        embed = build_hud_embed(agents, {})
        assert embed.timestamp is not None
        assert embed.timestamp.tzinfo is not None
        assert embed.timestamp.utcoffset().total_seconds() == 0
        assert re.search(
            r"Updated \d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC$", embed.footer.text
        )
