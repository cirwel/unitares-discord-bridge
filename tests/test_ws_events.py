"""Tests for bridge.ws_events pure helpers.

The network-facing :class:`WSEventSubscriber` is not exercised here — its
value is hard to mock meaningfully without also mocking Discord. These
tests pin the classification logic so future event types don't silently
regress to "invisible in Discord".
"""

import discord

from bridge.ws_events import (
    _SEND_QUEUE_MAX,
    broadcaster_event_to_embed,
    classify_broadcaster_event,
    is_critical_broadcaster_event,
    resolve_violation_class,
    ws_url_from_http,
)


# ---------------------------------------------------------------------------
# ws_url_from_http
# ---------------------------------------------------------------------------


def test_ws_url_from_http_plaintext():
    assert ws_url_from_http("http://localhost:8767") == "ws://localhost:8767/ws/eisv"


def test_ws_url_from_http_tls():
    assert (
        ws_url_from_http("https://gov.cirwel.org")
        == "wss://gov.cirwel.org/ws/eisv"
    )


def test_ws_url_from_http_strips_trailing_slash():
    assert (
        ws_url_from_http("http://localhost:8767/")
        == "ws://localhost:8767/ws/eisv"
    )


def test_ws_url_from_http_passes_through_unknown_scheme():
    # Not an obvious http(s) URL — caller's responsibility; best-effort append.
    assert ws_url_from_http("unix:///tmp/sock") == "unix:///tmp/sock/ws/eisv"


# ---------------------------------------------------------------------------
# broadcaster_event_to_embed — eisv_update and empty are dropped
# ---------------------------------------------------------------------------


def test_eisv_update_returns_none():
    assert broadcaster_event_to_embed({"type": "eisv_update", "coherence": 0.5}) is None


def test_missing_type_returns_none():
    assert broadcaster_event_to_embed({}) is None
    assert broadcaster_event_to_embed({"type": ""}) is None


# ---------------------------------------------------------------------------
# Lifecycle events
# ---------------------------------------------------------------------------


def test_lifecycle_paused_red():
    embed = broadcaster_event_to_embed({
        "type": "lifecycle_paused",
        "agent_label": "vigil",
        "reason": "silent for 30min",
    })
    assert embed is not None
    assert "paused" in embed.title.lower()
    assert embed.colour == discord.Colour.red()
    assert "silent for 30min" in embed.description


def test_lifecycle_resumed_green():
    embed = broadcaster_event_to_embed({
        "type": "lifecycle_resumed",
        "agent_label": "sentinel",
    })
    assert embed.colour == discord.Colour.green()


def test_lifecycle_stuck_red():
    embed = broadcaster_event_to_embed({
        "type": "lifecycle_stuck_detected",
        "agent_label": "watcher",
    })
    assert embed.colour == discord.Colour.red()


def test_lifecycle_loop_orange():
    embed = broadcaster_event_to_embed({
        "type": "lifecycle_loop_detected",
        "agent_label": "vigil",
    })
    assert embed.colour == discord.Colour.orange()


def test_lifecycle_created_blurple():
    # "created" is neutral — deliberately not using red/orange/green.
    embed = broadcaster_event_to_embed({
        "type": "lifecycle_created",
        "agent_label": "new-agent",
    })
    assert embed.colour == discord.Colour.blurple()


# ---------------------------------------------------------------------------
# Identity events
# ---------------------------------------------------------------------------


def test_identity_drift_orange():
    embed = broadcaster_event_to_embed({
        "type": "identity_drift",
        "agent_label": "vigil",
        "detail": "session fingerprint mismatch",
    })
    assert embed.colour == discord.Colour.orange()
    assert "drift" in embed.title.lower()
    assert "fingerprint mismatch" in embed.description


def test_identity_assurance_change_blue():
    embed = broadcaster_event_to_embed({
        "type": "identity_assurance_change",
        "agent_label": "sentinel",
        "old_tier": 2,
        "new_tier": 3,
        "tier_name": "verified",
    })
    # No explicit warning colour for routine assurance changes.
    assert embed.colour == discord.Colour.blue()
    assert embed.description == "Tier 2 -> 3 (verified)"


# ---------------------------------------------------------------------------
# Knowledge events
# ---------------------------------------------------------------------------


def test_knowledge_write_carries_discovery_type_and_summary():
    embed = broadcaster_event_to_embed({
        "type": "knowledge_write",
        "agent_label": "sentinel",
        "discovery_type": "finding",
        "summary": "coordinated coherence drop across 3 agents",
        "tags": ["sentinel", "coordinated_coherence_drop", "high"],
    })
    assert "finding" in embed.title
    assert "coordinated coherence drop" in embed.description
    # high-severity tag → orange
    assert embed.colour == discord.Colour.orange()


def test_knowledge_write_critical_tag_red():
    embed = broadcaster_event_to_embed({
        "type": "knowledge_write",
        "tags": ["critical"],
    })
    assert embed.colour == discord.Colour.red()


def test_knowledge_write_truncates_long_summary():
    embed = broadcaster_event_to_embed({
        "type": "knowledge_write",
        "summary": "A" * 500,
    })
    assert embed.description.endswith("...")
    assert len(embed.description) <= 210


def test_knowledge_confidence_clamped_orange():
    embed = broadcaster_event_to_embed({
        "type": "knowledge_confidence_clamped",
        "agent_label": "opus",
        "summary": "overconfident claim",
    })
    assert embed.colour == discord.Colour.orange()


# ---------------------------------------------------------------------------
# Circuit breaker events
# ---------------------------------------------------------------------------


def test_circuit_breaker_trip_red():
    embed = broadcaster_event_to_embed({
        "type": "circuit_breaker_trip",
        "reason": "pool exhausted",
    })
    assert embed.colour == discord.Colour.red()
    assert "tripped" in embed.title.lower()
    assert "pool exhausted" in embed.description


def test_circuit_breaker_reset_green():
    embed = broadcaster_event_to_embed({"type": "circuit_breaker_reset"})
    assert embed.colour == discord.Colour.green()


# ---------------------------------------------------------------------------
# Unknown event types fall through to a generic renderer, not None
# ---------------------------------------------------------------------------


def test_unknown_future_type_renders_generically():
    # If someone adds a new broadcaster event class, we want it visible
    # immediately — not silently dropped.
    embed = broadcaster_event_to_embed({
        "type": "new_future_event_class",
        "agent_label": "x",
    })
    assert embed is not None
    assert "new future event class" in embed.title.lower()


# ---------------------------------------------------------------------------
# Agent field resolution
# ---------------------------------------------------------------------------


def test_agent_field_prefers_label_then_name_then_id():
    e1 = broadcaster_event_to_embed({
        "type": "lifecycle_paused",
        "agent_label": "opus",
        "agent_name": "gpt",
        "agent_id": "abc-def",
    })
    assert any(f.value == "opus (abc-def)" for f in e1.fields)

    e2 = broadcaster_event_to_embed({
        "type": "lifecycle_paused",
        "agent_name": "gpt",
        "agent_id": "abc-def",
    })
    assert any(f.value == "gpt (abc-def)" for f in e2.fields)

    e3 = broadcaster_event_to_embed({
        "type": "lifecycle_paused",
        "agent_id": "abcdef0123456789",
    })
    # Truncated agent id (first 12 chars)
    assert any(f.value == "abcdef012345" for f in e3.fields)

    e4 = broadcaster_event_to_embed({"type": "lifecycle_paused"})
    assert any(f.value == "system" for f in e4.fields)


# ---------------------------------------------------------------------------
# is_critical_broadcaster_event
# ---------------------------------------------------------------------------


def test_critical_trip():
    assert is_critical_broadcaster_event({"type": "circuit_breaker_trip"})


def test_critical_lifecycle_paused():
    assert is_critical_broadcaster_event({"type": "lifecycle_paused"})


def test_critical_lifecycle_silent_critical():
    assert is_critical_broadcaster_event({"type": "lifecycle_silent_critical"})


def test_critical_lifecycle_stuck():
    assert is_critical_broadcaster_event({"type": "lifecycle_stuck_detected"})


def test_critical_tag_elevates():
    assert is_critical_broadcaster_event({
        "type": "knowledge_write",
        "tags": ["critical"],
    })


def test_non_critical_by_default():
    assert not is_critical_broadcaster_event({"type": "knowledge_write"})
    assert not is_critical_broadcaster_event({"type": "lifecycle_resumed"})
    assert not is_critical_broadcaster_event({"type": "identity_drift"})


# ---------------------------------------------------------------------------
# resolve_violation_class — used for per-class channel routing
# ---------------------------------------------------------------------------


_SAMPLE_REVERSE = {
    "broadcast_events": {
        "identity_drift": "CON",
        "knowledge_confidence_clamped": "INT",
        "circuit_breaker_trip": "REC",
    },
    "watcher_patterns": {"P011": "INT"},
    "sentinel_findings": {"coordinated_degradation": "CON"},
}


def test_explicit_violation_class_wins():
    # Watcher writes knowledge_write events with violation_class on the
    # payload directly — use it even if the event type isn't in the reverse
    # lookup for broadcast_events.
    cls = resolve_violation_class(
        {"type": "knowledge_write", "violation_class": "ENT"},
        _SAMPLE_REVERSE,
    )
    assert cls == "ENT"


def test_resolves_via_reverse_lookup_when_no_explicit():
    cls = resolve_violation_class(
        {"type": "identity_drift"}, _SAMPLE_REVERSE
    )
    assert cls == "CON"


def test_returns_none_when_no_mapping():
    assert resolve_violation_class(
        {"type": "some_future_event"}, _SAMPLE_REVERSE
    ) is None


def test_returns_none_when_reverse_is_none():
    assert resolve_violation_class({"type": "identity_drift"}, None) is None


def test_explicit_wins_even_over_reverse_conflict():
    # Governance-declared reverse says INT, but Watcher asserts the write
    # is CON — Watcher's explicit claim wins.
    cls = resolve_violation_class(
        {"type": "knowledge_confidence_clamped", "violation_class": "CON"},
        _SAMPLE_REVERSE,
    )
    assert cls == "CON"


def test_empty_reverse_still_checks_explicit():
    cls = resolve_violation_class(
        {"type": "knowledge_write", "violation_class": "REC"},
        {},
    )
    assert cls == "REC"


# ---------------------------------------------------------------------------
# classify_broadcaster_event — routes broadcaster events to #activity vs #signals
# ---------------------------------------------------------------------------


def test_classify_broadcaster_activity_types():
    # Routine lifecycle, trust-tier bookkeeping, and knowledge writes are
    # high-volume but low-signal.
    assert classify_broadcaster_event({"type": "lifecycle_created"}) == "activity"
    assert classify_broadcaster_event({"type": "lifecycle_resumed"}) == "activity"
    assert classify_broadcaster_event({"type": "lifecycle_archived"}) == "activity"
    assert classify_broadcaster_event({"type": "identity_assurance_change"}) == "activity"
    assert classify_broadcaster_event({"type": "knowledge_write"}) == "activity"


def test_classify_broadcaster_signal_types():
    # Anything operators would want to read promptly.
    assert classify_broadcaster_event({"type": "lifecycle_paused"}) == "signals"
    assert classify_broadcaster_event({"type": "lifecycle_stuck_detected"}) == "signals"
    assert classify_broadcaster_event({"type": "lifecycle_silent_critical"}) == "signals"
    assert classify_broadcaster_event({"type": "lifecycle_loop_detected"}) == "signals"
    assert classify_broadcaster_event({"type": "identity_drift"}) == "signals"
    assert classify_broadcaster_event({"type": "knowledge_confidence_clamped"}) == "signals"
    assert classify_broadcaster_event({"type": "circuit_breaker_trip"}) == "signals"
    assert classify_broadcaster_event({"type": "circuit_breaker_reset"}) == "signals"


def test_classify_broadcaster_unknown_defaults_to_signals():
    # Unknown types stay visible rather than silently slotting into activity.
    assert classify_broadcaster_event({"type": "new_future_event_class"}) == "signals"
    assert classify_broadcaster_event({}) == "signals"


def test_send_queue_capacity_absorbs_sentinel_burst():
    # A fleet-wide Sentinel scan can fan out to 3 queue entries per critical
    # event (signals + alerts mirror + class-routed channel). The previous
    # maxsize=100 silently dropped ~170 lifecycle_silent_critical events over
    # 4 days. Keep headroom for ~300 agents firing at once.
    assert _SEND_QUEUE_MAX >= 1000


# ---------------------------------------------------------------------------
# Phase B transition embed rendering
# ---------------------------------------------------------------------------


def test_phase_b_promotable_renders_green_embed():
    """First flip to PROMOTABLE should render as a green embed with a
    distinct title operators can grep in a busy channel."""
    embed = broadcaster_event_to_embed({
        "type": "lease_plane_phase_b_transition",
        "agent_name": "Sentinel",
        "surface_kind": "dialectic",
        "promotable_now": True,
        "promotable_before": False,
        "message": "[lease-plane] dialectic: PROMOTABLE — all §6.1 criteria PASS or N/A",
    })
    assert embed is not None
    assert "PROMOTABLE" in embed.title
    assert "dialectic" in embed.title
    assert embed.colour == discord.Colour.green()


def test_phase_b_regression_renders_red_embed():
    """Surface that was promotable but regressed should be visually distinct
    from a forward transition — caller-confusion costs more than embed-code."""
    embed = broadcaster_event_to_embed({
        "type": "lease_plane_phase_b_transition",
        "surface_kind": "dialectic",
        "promotable_now": False,
        "promotable_before": True,
        "message": "REGRESSED",
    })
    assert embed is not None
    assert "regression" in embed.title.lower() or "REGRESSED" in embed.title
    assert embed.colour == discord.Colour.red()


def test_phase_b_first_observation_baseline_renders_neutral():
    """Real criterion flips that don't change the overall promotable state
    render as blurple to distinguish from PROMOTABLE/regression."""
    embed = broadcaster_event_to_embed({
        "type": "lease_plane_phase_b_transition",
        "surface_kind": "file",
        "promotable_now": False,
        "promotable_before": False,
        "message": "[lease-plane] file: §6.1.3 (type_a_conflict_signal): FAIL → PASS",
    })
    assert embed is not None
    assert "criterion change" in embed.title.lower()
    assert embed.colour == discord.Colour.blurple()


def test_phase_b_classified_as_signals():
    """The event type is signals-bucket — operators should see it without
    needing to scroll #activity."""
    assert (
        classify_broadcaster_event({"type": "lease_plane_phase_b_transition"})
        == "signals"
    )


# ---------------------------------------------------------------------------
# repeat-drift suppression (_suppress_repeat_drift)
# ---------------------------------------------------------------------------


def _bare_subscriber():
    """WSEventSubscriber without __init__ — the helper only touches
    _drift_last and config, so we skip the Discord-channel plumbing the
    module docstring warns about."""
    from bridge.ws_events import WSEventSubscriber

    sub = WSEventSubscriber.__new__(WSEventSubscriber)
    sub._drift_last = {}
    return sub


def _drift(agent="69a1a4f7", sim=0.123, etype="identity_drift"):
    return {"type": etype, "agent_id": agent, "lineage_similarity": sim}


def test_repeat_drift_first_posts_then_suppresses():
    sub = _bare_subscriber()
    assert sub._suppress_repeat_drift(_drift()) is False
    assert sub._suppress_repeat_drift(_drift()) is True
    assert sub._suppress_repeat_drift(_drift(sim=0.124)) is True  # within delta


def test_repeat_drift_posts_when_metric_moves():
    sub = _bare_subscriber()
    assert sub._suppress_repeat_drift(_drift(sim=0.12)) is False
    assert sub._suppress_repeat_drift(_drift(sim=0.55)) is False  # > delta


def test_repeat_drift_reposts_after_window(monkeypatch):
    sub = _bare_subscriber()
    assert sub._suppress_repeat_drift(_drift()) is False
    # Age the stored timestamp past the window instead of sleeping.
    (key, (ts, val)), = sub._drift_last.items()
    from bridge import config

    sub._drift_last[key] = (ts - config.DRIFT_REPEAT_WINDOW_SECONDS - 1, val)
    assert sub._suppress_repeat_drift(_drift()) is False


def test_repeat_drift_agents_tracked_independently():
    sub = _bare_subscriber()
    assert sub._suppress_repeat_drift(_drift(agent="a")) is False
    assert sub._suppress_repeat_drift(_drift(agent="b")) is False
    assert sub._suppress_repeat_drift(_drift(agent="a")) is True


def test_repeat_drift_ignores_other_event_types():
    sub = _bare_subscriber()
    assert sub._suppress_repeat_drift({"type": "lifecycle_paused", "agent_id": "a"}) is False
    assert sub._suppress_repeat_drift({"type": "lifecycle_paused", "agent_id": "a"}) is False
    assert sub._drift_last == {}


def test_repeat_drift_disabled_by_zero_window(monkeypatch):
    from bridge import config

    monkeypatch.setattr(config, "DRIFT_REPEAT_WINDOW_SECONDS", 0)
    sub = _bare_subscriber()
    assert sub._suppress_repeat_drift(_drift()) is False
    assert sub._suppress_repeat_drift(_drift()) is False
