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
    assert embed.title == "Sentinel Finding · coordinated_degradation"
    assert embed.colour == discord.Colour.red()  # high → critical colour
    field_names = [f.name for f in embed.fields]
    assert "Violation" in field_names
    assert "Finding" not in field_names  # the finding type is in the title


def test_vigil_finding_embed():
    event = {
        "event_id": 7, "type": "vigil_finding", "severity": "critical",
        "message": "Governance is down",
        "agent_id": "vigil", "agent_name": "Vigil",
        "timestamp": "2026-04-15T12:00:00+00:00",
        "finding_type": "governance_down",
    }
    embed = event_to_embed(event)
    assert embed.title == "Vigil Finding · governance_down"
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
    assert embed.title == "Watcher Finding · P011"
    field_names = [f.name for f in embed.fields]
    # The message already opens "[P011] /tmp/foo.py:42", so neither repeats.
    assert "Pattern" not in field_names
    assert "Location" not in field_names
    assert "Violation" in field_names


def test_watcher_location_field_when_message_lacks_path():
    embed = event_to_embed({
        "type": "watcher_finding", "severity": "high", "message": "mutation before persistence",
        "pattern": "P011", "file": "/Users/someone/projects/unitares/src/a.py", "line": 7,
    })
    fields = {f.name: f.value for f in embed.fields}
    assert fields["Location"] == "unitares/src/a.py:7"


# ---------------------------------------------------------------------------
# Phone-readable finding embeds: titles, paths, fingerprints, resolutions and
# timestamps (payload shapes from unitares scripts/ops/doctor_findings.py,
# agents/watcher/findings.py + agent.py and agents/sentinel/agent.py).
# ---------------------------------------------------------------------------

from datetime import datetime, timezone

import pytest

from bridge.embeds import parse_event_timestamp, short_fingerprint


def _fields(embed):
    return {f.name: f.value for f in embed.fields}


def test_doctor_finding_titles_by_check_and_drops_repeated_prefix():
    embed = event_to_embed({
        "type": "doctor_check_finding", "severity": "warning", "event_id": 3,
        "message": "lumen_checkin: no check-in for 42m — last seen 09:12",
        "check": "lumen_checkin", "agent_name": "doctor",
        "fingerprint": "9f86d081884c7d65",
        "timestamp": "2026-10-19T09:54:00+00:00",
    })
    assert embed.title == "Doctor Check Finding · lumen_checkin"
    assert embed.description == "no check-in for 42m — last seen 09:12"
    assert _fields(embed)["Fingerprint"] == "`9f86d081884c7d65`"
    assert embed.footer.text == "doctor_check_finding"
    assert "Event #" not in embed.footer.text


def test_watcher_finding_shows_path_once_repo_relative_and_close_hint():
    fp = "a1b2c3d4e5f60718"
    path = "/Users/someone/projects/wt/unitares-fix/src/handlers.py"
    embed = event_to_embed({
        "type": "watcher_finding", "severity": "high", "event_id": 9,
        "message": f"[P011] {path}:42 — mutation before persistence",
        "agent_name": "Watcher", "fingerprint": fp,
        "pattern": "P011", "file": path, "line": 42, "violation_class": "INT",
    })
    assert embed.title == "Watcher Finding · P011"
    assert embed.description == (
        "[P011] unitares-fix/src/handlers.py:42 — mutation before persistence"
    )
    fields = _fields(embed)
    assert "Location" not in fields and "Pattern" not in fields
    assert fields["Fingerprint"] == f"`{fp}`"
    assert fields["Close"] == f"`--resolve {fp}` or `--dismiss {fp}`"


def test_sentinel_forced_release_finding_title_and_free_text_fingerprint():
    event_uuid = "6f1c2d3e-4a5b-4c6d-8e7f-90a1b2c3d4e5"
    embed = event_to_embed({
        "type": "sentinel_alarm_finding", "severity": "high", "event_id": 12,
        "message": "forced release of lease abc", "agent_name": "Sentinel",
        "alarm_kind": "ad_hoc",
        "fingerprint": f"forced_release:ad_hoc:{event_uuid}",
        "evidence": {"kind": "forced_release", "assessment": "no_lease_row"},
    })
    assert embed.title == "Sentinel Alarm Finding · ad_hoc"
    fields = _fields(embed)
    assert fields["Fingerprint"] == f"`…{event_uuid[-24:]}`"
    assert "Event check" in fields
    assert "Close" not in fields


@pytest.mark.parametrize("raw, shown", [
    ("9f86d081884c7d65", "9f86d081884c7d65"),
    ("x" * 24, "x" * 24),
    ("sha256:" + "ab" * 32, "ab" * 8),
    ("0123456789abcdef" * 4, "0123456789abcdef"),
    ("forced_release:conflict_batch:s1:2026-10-19T09:00:00+00:00", "…026-10-19T09:00:00+00:00"),
    ("", None),
    (None, None),
])
def test_short_fingerprint(raw, shown):
    assert short_fingerprint(raw) == shown


def test_confirmed_resolution_renders_green_with_was_severity_and_action():
    embed = event_to_embed({
        "type": "watcher_resolution_finding", "severity": "high",
        "message": "[confirmed] P011 /Users/someone/projects/unitares/src/a.py:42 — hint · fixed in #12",
        "agent_name": "Watcher", "fingerprint": "a1b2c3d4e5f60718",
        "action": "confirmed", "pattern": "P011",
        "file": "/Users/someone/projects/unitares/src/a.py", "line": 42,
        "resolution_reason": "fixed in #12",
    })
    assert embed.colour == discord.Colour.green()
    assert embed.title == "Resolved · P011"
    fields = _fields(embed)
    assert fields["Severity"] == "resolved (was high)"
    assert fields["Action"] == "confirmed: fixed in #12"
    assert "Close" not in fields
    assert "Location" not in fields
    assert "unitares/src/a.py:42" in embed.description


def test_dismissed_resolution_renders_grey():
    embed = event_to_embed({
        "type": "watcher_resolution_finding", "severity": "critical",
        "message": "[dismissed] P003 x.py:1 — hint", "action": "dismissed",
        "pattern": "P003", "agent_name": "Watcher", "fingerprint": "0011223344556677",
    })
    assert embed.colour == discord.Colour.light_grey()
    assert embed.title == "Resolved · P003"
    assert _fields(embed)["Severity"] == "resolved (was critical)"
    assert _fields(embed)["Action"] == "dismissed"


@pytest.mark.parametrize("raw, expected", [
    ("2026-10-19T09:54:00+00:00", datetime(2026, 10, 19, 9, 54, tzinfo=timezone.utc)),
    ("2026-10-19T09:54:00Z", datetime(2026, 10, 19, 9, 54, tzinfo=timezone.utc)),
    ("2026-10-19T09:54:00", datetime(2026, 10, 19, 9, 54, tzinfo=timezone.utc)),
    ("2026-10-19T03:54:00-06:00", datetime(2026, 10, 19, 9, 54, tzinfo=timezone.utc)),
    (None, None),
    ("", None),
    ("not a time", None),
    (1708700000, None),
])
def test_parse_event_timestamp(raw, expected):
    assert parse_event_timestamp(raw) == expected


@pytest.mark.parametrize("raw", [None, "garbage", "2026-10-19T09:54:00Z"])
def test_embed_timestamp_never_breaks_rendering(raw):
    embed = event_to_embed({"type": "verdict_change", "timestamp": raw, "from": "a", "to": "b"})
    assert embed.timestamp == parse_event_timestamp(raw)
    assert embed.footer.text == "verdict_change"


def test_high_and_critical_findings_route_to_alerts():
    # Findings post once: high/critical go to #alerts (unless a noisy
    # pattern-hit class), everything else stays in its resident channel.
    assert is_critical_event({"type": "sentinel_finding", "severity": "high"})
    assert is_critical_event({"type": "vigil_finding", "severity": "critical"})
    assert not is_critical_event({"type": "sentinel_finding", "severity": "info"})
    assert not is_critical_event({"type": "sentinel_finding", "severity": "medium"})


def test_resolution_findings_never_route_to_alerts():
    assert not is_critical_event(
        {"type": "watcher_resolution_finding", "severity": "critical", "action": "confirmed"}
    )


def test_finding_alert_severities_are_configurable(monkeypatch):
    from bridge import config

    monkeypatch.setattr(config, "FINDING_ALERT_SEVERITIES", frozenset({"critical"}))
    assert not is_critical_event({"type": "sentinel_finding", "severity": "high"})
    assert is_critical_event({"type": "sentinel_finding", "severity": "critical"})


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


def test_short_path_only_shortens_the_home_projects_directory():
    from bridge.embeds import short_path

    assert short_path("/Users/x/projects/unitares/src/a.py") == "unitares/src/a.py"
    assert short_path("/home/x/projects/wt/unitares-fix/a.py") == "unitares-fix/a.py"
    # A "projects" directory elsewhere keeps its location.
    assert (
        short_path("/Users/x/.claude/projects/-Users-x/memory/a.md")
        == "~/.claude/projects/-Users-x/memory/a.md"
    )
    assert short_path("/srv/projects/app/a.py") == "/srv/projects/app/a.py"


@pytest.mark.parametrize("fingerprint, rendered", [
    ("abc`def", "`` abc`def ``"),
    ("`edge`", "`` `edge` ``"),
    ("a``b", "`a\u02cb\u02cbb`"),
])
def test_fingerprint_with_backticks_keeps_a_valid_code_span(fingerprint, rendered):
    embed = event_to_embed({
        "type": "vigil_finding", "severity": "low", "fingerprint": fingerprint,
        "message": "m",
    })
    assert _fields(embed)["Fingerprint"] == rendered
