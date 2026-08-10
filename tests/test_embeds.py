import discord
from bridge.embeds import (
    classify_rest_event,
    event_to_embed,
    finding_resident_key,
    is_critical_event,
    is_finding_event,
)


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


def test_facilitation_request_pages_alerts():
    """
    A dialectic waiting on a human must reach #alerts.

    It carries no `critical` severity, so before this it classified to #signals
    — roughly 600 events/hour — where it was delivered and unreadable. Six
    genuine requests over nine months went unanswered that way.
    """
    assert is_critical_event(
        {"type": "dialectic_facilitation_needed", "severity": "warning"}
    )


def test_other_dialectic_events_do_not_page():
    """Only the request for a human pages. Ordinary phase traffic does not."""
    for t in ("dialectic_opened", "dialectic_phase_changed", "dialectic_resolved"):
        assert not is_critical_event({"type": t, "severity": "info"}), t


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


def test_event_agent_field_includes_name_and_id_when_both_present():
    event = {
        "event_id": 29,
        "type": "coherence_drop",
        "severity": "high",
        "agent_id": "fe5975a6-23c7-4e55-9a9d-9c4bdb9b45a7",
        "agent_label": "opus_hikewa",
        "description": "Coherence dropped from 0.48 to 0.36 (0.12 change)",
    }

    embed = event_to_embed(event)

    assert embed.fields[0].name == "Agent"
    assert embed.fields[0].value == "opus_hikewa (fe5975a6-23c)"


def test_forced_release_evidence_field_renders():
    event = {
        "event_id": 43, "type": "sentinel_alarm_finding", "severity": "high",
        "message": "forced release: resident:/steward (lease 5d09980f-1c91-4271-ab94-723564b5a597)",
        "agent_id": "sentinel", "agent_name": "Sentinel",
        "timestamp": "2026-08-01T12:00:00+00:00",
        "evidence": {
            "kind": "forced_release", "assessment": "event_recorded",
            "release_reason": "forced", "held_x_ttl": 6.2,
            "holder_pid_null": True, "report_latency_s": 26620.0,
        },
    }
    embed = event_to_embed(event)
    check = next(f for f in embed.fields if f.name == "Event check")
    assert "held 6.2× TTL" in check.value
    assert "7.4h after the event" in check.value
    assert "not independent corroboration" in check.value


def test_no_lease_row_evidence_reads_as_integrity_fault():
    event = {
        "event_id": 44, "type": "sentinel_alarm_finding", "severity": "high",
        "message": "forced release: resident:/steward (lease 5d09980f-1c91-4271-ab94-723564b5a597)",
        "agent_id": "sentinel", "agent_name": "Sentinel",
        "timestamp": "2026-08-01T12:00:00+00:00",
        "evidence": {"kind": "forced_release", "assessment": "no_lease_row"},
    }
    embed = event_to_embed(event)
    check = next(f for f in embed.fields if f.name == "Event check")
    assert "integrity fault" in check.value


def test_events_without_evidence_get_no_check_field():
    event = {
        "event_id": 45, "type": "sentinel_finding", "severity": "high",
        "message": "fleet coherence dipped",
        "agent_id": "sentinel", "agent_name": "Sentinel",
        "timestamp": "2026-08-01T12:00:00+00:00",
    }
    embed = event_to_embed(event)
    assert all(f.name != "Event check" for f in embed.fields)


# ---------------------------------------------------------------------------
# Finding attribution — which resident authored a finding
# ---------------------------------------------------------------------------


def test_is_finding_event_covers_qualified_and_bare_types():
    assert is_finding_event({"type": "sentinel_finding"})
    assert is_finding_event({"type": "sentinel_alarm_finding"})
    assert is_finding_event({"type": "finding"})
    assert not is_finding_event({"type": "verdict_change"})
    assert not is_finding_event({"type": "finding_review"})
    assert not is_finding_event({})


def test_finding_resident_key_from_type_prefix():
    assert finding_resident_key({"type": "sentinel_finding"}) == "sentinel"
    assert finding_resident_key({"type": "doctor_finding"}) == "doctor"
    assert finding_resident_key({"type": "vigil_finding"}) == "vigil"
    # Qualified finding types still belong to the resident named first.
    assert finding_resident_key({"type": "sentinel_alarm_finding"}) == "sentinel"


def test_finding_resident_key_is_case_insensitive():
    assert finding_resident_key({"type": "Doctor_Finding"}) == "doctor"


def test_bare_finding_type_falls_back_to_identity_fields():
    # No author in the type — prefer the label over an opaque id.
    assert finding_resident_key({
        "type": "finding",
        "agent_id": "fe5975a6-23c7-4e55-9a9d-9c4bdb9b45a7",
        "agent_label": "Doctor",
    }) == "doctor"
    assert finding_resident_key({"type": "finding", "agent_id": "Sentinel"}) == "sentinel"
    assert finding_resident_key({"type": "finding"}) is None


def test_finding_resident_key_ignores_non_findings():
    # A verdict change *about* Sentinel is not a Sentinel finding.
    assert finding_resident_key({
        "type": "verdict_change", "agent_id": "sentinel",
    }) is None
