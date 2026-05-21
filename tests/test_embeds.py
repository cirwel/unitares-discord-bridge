import discord
from bridge.embeds import classify_rest_event, event_to_embed, is_critical_event


def test_verdict_change_embed():
    event = {"event_id": 5, "type": "verdict_change", "severity": "warning",
             "message": "Verdict changed", "agent_id": "abc", "agent_name": "opus",
             "timestamp": "2026-02-23T14:32:00Z", "from": "proceed", "to": "guide"}
    embed = event_to_embed(event)
    assert isinstance(embed, discord.Embed)
    assert "Verdict Change" in embed.title
    assert embed.colour == discord.Colour.orange()


def test_agent_new_embed():
    event = {"event_id": 1, "type": "agent_new", "severity": "info",
             "message": "New agent", "agent_id": "abc", "agent_name": "test",
             "timestamp": "2026-02-23T10:00:00Z"}
    embed = event_to_embed(event)
    assert embed.colour == discord.Colour.blue()


def test_critical_severity_is_red():
    event = {"event_id": 10, "type": "risk_threshold", "severity": "critical",
             "message": "Risk above 70%", "agent_id": "abc", "agent_name": "test",
             "timestamp": "2026-02-23T10:00:00Z", "threshold": 0.7, "direction": "up", "value": 0.75}
    embed = event_to_embed(event)
    assert embed.colour == discord.Colour.red()


def test_is_critical_for_pause():
    assert is_critical_event({"type": "verdict_change", "to": "pause", "severity": "warning"})


def test_is_not_critical_for_proceed():
    assert not is_critical_event({"type": "verdict_change", "to": "proceed", "severity": "info"})


def test_is_critical_for_critical_severity():
    assert is_critical_event({"type": "risk_threshold", "severity": "critical"})


def test_sentinel_finding_embed():
    event = {
        "event_id": 42, "type": "sentinel_finding", "severity": "high",
        "message": "3 agents drifting in lockstep",
        "agent_id": "sentinel", "agent_name": "Sentinel",
        "timestamp": "2026-04-15T12:00:00+00:00",
        "violation_class": "BEH", "finding_type": "coordinated_degradation",
    }
    embed = event_to_embed(event)
    assert embed.title == "Sentinel Finding"
    assert embed.colour == discord.Colour.red()  # high → critical colour
    field_names = [f.name for f in embed.fields]
    assert "Violation" in field_names
    assert "Finding" in field_names


def test_vigil_finding_embed():
    event = {
        "event_id": 7, "type": "vigil_finding", "severity": "critical",
        "message": "Governance is down",
        "agent_id": "vigil", "agent_name": "Vigil",
        "timestamp": "2026-04-15T12:00:00+00:00",
        "finding_type": "governance_down",
    }
    embed = event_to_embed(event)
    assert embed.title == "Vigil Finding"
    assert embed.colour == discord.Colour.red()


def test_watcher_finding_embed():
    event = {
        "event_id": 11, "type": "watcher_finding", "severity": "high",
        "message": "[P011] /tmp/foo.py:42 — mutation before persistence",
        "agent_id": "watcher", "agent_name": "Watcher",
        "timestamp": "2026-04-15T12:00:00+00:00",
        "pattern": "P011", "file": "/tmp/foo.py", "line": 42,
        "violation_class": "INT",
    }
    embed = event_to_embed(event)
    assert embed.title == "Watcher Finding"
    field_names = [f.name for f in embed.fields]
    assert "Pattern" in field_names
    assert "Location" in field_names


def test_finding_high_severity_routes_to_alerts():
    # high severity = route to #alerts, not just the main feed
    assert is_critical_event({"type": "sentinel_finding", "severity": "high"})
    assert is_critical_event({"type": "watcher_finding", "severity": "critical"})
    assert not is_critical_event({"type": "sentinel_finding", "severity": "info"})
    assert not is_critical_event({"type": "watcher_finding", "severity": "medium"})


def test_classify_rest_event_activity_types():
    assert classify_rest_event({"type": "agent_new"}) == "activity"
    assert classify_rest_event({"type": "agent_idle"}) == "activity"


def test_classify_rest_event_signal_types():
    assert classify_rest_event({"type": "verdict_change"}) == "signals"
    assert classify_rest_event({"type": "risk_threshold"}) == "signals"
    assert classify_rest_event({"type": "drift_alert"}) == "signals"
    assert classify_rest_event({"type": "drift_oscillation"}) == "signals"
    assert classify_rest_event({"type": "trajectory_adjustment"}) == "signals"


def test_classify_rest_event_unknown_defaults_to_signals():
    # Unknown event types route to signals rather than activity so they
    # remain visible to operators until explicitly reclassified.
    assert classify_rest_event({"type": "new_future_event"}) == "signals"
    assert classify_rest_event({}) == "signals"


def test_drift_alert_with_null_value_does_not_crash():
    # Regression: governance can emit drift_alert with value=null when the
    # underlying metric reading is unavailable. The embed builder previously
    # raised TypeError on `f"{None:.2f}"`, which stalled the entire REST
    # event feed because the poll cursor never advanced past it.
    event = {
        "event_id": 42, "type": "drift_alert", "severity": "warning",
        "message": "drift", "agent_id": "a", "agent_name": "A",
        "axis": "I", "value": None,
    }
    embed = event_to_embed(event)
    value_field = next(f for f in embed.fields if f.name == "Value")
    assert value_field.value == "0.00"


def test_risk_threshold_with_null_value_does_not_crash():
    event = {
        "event_id": 43, "type": "risk_threshold", "severity": "warning",
        "message": "risk", "agent_id": "a", "agent_name": "A",
        "direction": "up", "value": None,
    }
    embed = event_to_embed(event)
    risk_field = next(f for f in embed.fields if f.name == "Risk")
    assert risk_field.value == "0%"


def test_drift_alert_with_string_value_does_not_crash():
    # Defense in depth: non-numeric scalars (a stringified number, an empty
    # string, a dict) should fall back to 0.0 rather than raise.
    event = {
        "event_id": 44, "type": "drift_alert", "severity": "warning",
        "message": "drift", "agent_id": "a", "agent_name": "A",
        "axis": "S", "value": "not-a-number",
    }
    embed = event_to_embed(event)
    value_field = next(f for f in embed.fields if f.name == "Value")
    assert value_field.value == "0.00"


def test_event_without_agent_name_falls_back_to_id_and_description():
    event = {
        "event_id": 28,
        "type": "coherence_drop",
        "severity": "high",
        "agent_id": "fe5975a6-23c7-4e55-9a9d-9c4bdb9b45a7",
        "description": "Coherence dropped from 0.48 to 0.36 (0.12 change)",
    }

    embed = event_to_embed(event)

    assert embed.title == "Coherence Drop"
    assert embed.description == "Coherence dropped from 0.48 to 0.36 (0.12 change)"
    assert embed.fields[0].name == "Agent"
    assert embed.fields[0].value == "fe5975a6-23c"
