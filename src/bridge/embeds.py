"""Map governance events to Discord embeds."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Callable

import discord

from bridge import config

SEVERITY_COLOURS = {
    "info": discord.Colour.blue(),
    "low": discord.Colour.blue(),
    "medium": discord.Colour.orange(),
    "warning": discord.Colour.orange(),
    "high": discord.Colour.red(),
    "critical": discord.Colour.red(),
}

EVENT_TITLES = {
    "agent_new": "New Agent",
    "verdict_change": "Verdict Change",
    "risk_threshold": "Risk Threshold",
    "drift_alert": "Drift Alert",
    "drift_oscillation": "Drift Oscillation",
    "trajectory_adjustment": "Trajectory Adjustment",
    "agent_idle": "Agent Idle",
    "sentinel_finding": "Sentinel Finding",
    "vigil_finding": "Vigil Finding",
    "watcher_finding": "Watcher Finding",
}


def _event_description(event: dict) -> str:
    return event.get("message") or event.get("description") or ""


def _event_agent(event: dict) -> str:
    agent_id = event.get("resident_id") or event.get("agent_id")
    short_id = str(agent_id)[:12] if agent_id else ""
    name = (
        event.get("resident_name")
        or event.get("resident_label")
        or event.get("agent_name")
        or event.get("agent_label")
    )
    if name and short_id and str(name) not in (str(agent_id), short_id):
        return f"{name} ({short_id})"
    if name:
        return str(name)
    return short_id or "system"


def _safe_float(value, default: float = 0.0) -> float:
    """Coerce a value to float, returning *default* for None / non-numeric.

    Governance can emit metric events (drift_alert, risk_threshold) with an
    explicit ``"value": null`` when the underlying reading is unavailable.
    Passing that through ``f"{x:.2f}"`` raises ``TypeError`` and — until this
    helper existed — stalled the entire REST event feed because the poll
    cursor never advanced past the poison-pill event.
    """
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _humanize_seconds(s: float) -> str:
    return f"{round(s / 60)}min" if s < 5400 else f"{s / 3600:.1f}h"


def _evidence_line(ev: dict) -> str:
    """Render the server-computed event check for a forced-release finding.

    Scope honesty mirrors the governance dashboard: the lease row and the
    finding's source event are written by one lease-plane transaction, so a
    match is never phrased as independent corroboration, and only the
    missing-row state (a lease-plane integrity fault) reads as alarming.
    """
    assessment = ev.get("assessment")
    if assessment == "event_recorded":
        bits = ["lease row present", "release_reason=forced"]
        if ev.get("held_x_ttl") is not None:
            bits.append(f"held {ev['held_x_ttl']}× TTL")
        if ev.get("holder_pid_null"):
            bits.append("holder pid null")
        latency = ev.get("report_latency_s")
        if isinstance(latency, (int, float)) and latency >= 300:
            bits.append(f"reported {_humanize_seconds(latency)} after the event")
        return " · ".join(bits) + "\n-# same-transaction record — not independent corroboration"
    if assessment == "no_lease_row":
        return "⚠ no lease row for the claimed lease id — lease-plane integrity fault; escalate"
    if assessment == "lookup_mismatch":
        note = ev.get("note")
        return ("finding did not resolve to a matching lease row"
                + (f" — {note}" if note else "")
                + "\n-# almost certainly an evidence-side fault, not a wrong finding")
    return "machine check errored — no evidence either way"


def parse_event_timestamp(value: object) -> datetime | None:
    """An event's ``timestamp`` as an aware datetime, or ``None``.

    Accepts ISO 8601 with an offset or a trailing ``Z``; a naive time is read
    as UTC (governance writes UTC). Anything unparseable is ``None``, never an
    exception, so a bad timestamp cannot stop an embed from rendering.
    """
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        if text[-1] in "Zz":
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


_SHA256_PREFIX = "sha256:"
_HEX = re.compile(r"[0-9a-f]+")


def short_fingerprint(raw: object) -> str | None:
    """A finding fingerprint short enough for a phone, or ``None``.

    Up to 24 characters show in full (Watcher and Doctor use 16 hex). A digest
    shows its first 16 hex characters (``sha256:`` stripped), which is still a
    unique prefix for Watcher's ``--resolve``. Free text such as Sentinel's
    ``forced_release:ad_hoc:<uuid>`` shows its last 24 characters, where the
    distinguishing id is.
    """
    fp = str(raw or "").strip()
    if not fp:
        return None
    if len(fp) <= 24:
        return fp
    if fp.lower().startswith(_SHA256_PREFIX):
        return fp[len(_SHA256_PREFIX):][:16]
    if _HEX.fullmatch(fp.lower()):
        return fp[:16]
    return "…" + fp[-24:]


# Fields, in order, that say which check/kind/pattern a finding is about. The
# first one present goes in the title, so its field is not repeated.
_TITLE_QUALIFIER_KEYS = ("check", "alarm_kind", "finding_type", "pattern")


def _title_qualifier(event: dict) -> tuple[str, str] | tuple[None, None]:
    for key in _TITLE_QUALIFIER_KEYS:
        value = event.get(key)
        if value is not None and str(value).strip():
            return key, str(value).strip()
    return None, None


_HOME_PREFIX = re.compile(r"^/(?:Users|home)/[^/]+/")


def short_path(path: str) -> str:
    """Repo-relative form of an absolute path under ``~/projects``.

    ``/Users/x/projects/unitares/src/a.py`` -> ``unitares/src/a.py`` and
    ``/Users/x/projects/wt/unitares-fix/src/a.py`` -> ``unitares-fix/src/a.py``,
    keeping the checkout name. Other home paths become ``~/...``; anything else
    is unchanged.
    """
    for marker in ("/projects/wt/", "/projects/"):
        idx = path.find(marker)
        if idx != -1 and path[idx + len(marker):]:
            return path[idx + len(marker):]
    return _HOME_PREFIX.sub("~/", path)


def _truncate(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def event_to_embed(event: dict) -> discord.Embed:
    """Convert a governance event dict to a Discord embed."""
    severity = attention_severity(event, critical=is_critical_event(event))
    event_type = event.get("type", "unknown")
    colour = SEVERITY_COLOURS.get(severity, discord.Colour.greyple())
    base_title = EVENT_TITLES.get(event_type, event_type.replace("_", " ").title())
    description = _event_description(event)
    qualifier_key, qualifier = _title_qualifier(event)
    resolution = is_resolution_finding(event)

    if resolution:
        title = f"Resolved · {qualifier}" if qualifier else "Resolved"
        colour = (
            discord.Colour.light_grey() if is_dismissed_resolution(event)
            else discord.Colour.green()
        )
    else:
        title = f"{base_title} · {qualifier}" if qualifier else base_title

    # Doctor messages are "<check>: <message>"; the check is now the title.
    if qualifier_key == "check" and description.startswith(f"{qualifier}: "):
        description = description[len(qualifier) + 2:]

    file_path = str(event.get("file") or "")
    if file_path:
        shortened = short_path(file_path)
        if shortened != file_path:
            description = description.replace(file_path, shortened)

    embed = discord.Embed(
        title=_truncate(title, 256),
        description=description,
        colour=colour,
        timestamp=parse_event_timestamp(event.get("timestamp")),
    )
    embed.add_field(name="Agent", value=_event_agent(event), inline=True)
    if resolution:
        embed.add_field(name="Severity", value=f"resolved (was {severity})", inline=True)
        action = str(event.get("action") or "").strip()
        if action:
            reason = str(event.get("resolution_reason") or "").strip()
            embed.add_field(
                name="Action",
                value=_truncate(f"{action}: {reason}" if reason else action, 1024),
                inline=True,
            )
    else:
        embed.add_field(name="Severity", value=severity, inline=True)

    fingerprint = short_fingerprint(event.get("fingerprint"))
    if fingerprint:
        embed.add_field(name="Fingerprint", value=f"`{fingerprint}`", inline=True)

    # Type-specific fields
    if event_type == "verdict_change":
        embed.add_field(
            name="Transition",
            value=f"{event.get('from', '?')} \u2192 {event.get('to', '?')}",
            inline=False,
        )
    elif event_type == "risk_threshold":
        embed.add_field(name="Risk", value=f"{_safe_float(event.get('value')):.0%}", inline=True)
        embed.add_field(name="Direction", value=event.get("direction", "?"), inline=True)
        embed.add_field(
            name="Interpretation",
            value="Governance risk signal, not a confirmed service outage. Inspect the named agent and supporting evidence.",
            inline=False,
        )
    elif event_type == "drift_alert":
        embed.add_field(name="Axis", value=event.get("axis", "?"), inline=True)
        embed.add_field(name="Value", value=f"{_safe_float(event.get('value')):.2f}", inline=True)
    elif event_type == "sentinel_finding":
        if event.get("violation_class"):
            embed.add_field(name="Violation", value=event["violation_class"], inline=True)
        if event.get("finding_type") and qualifier_key != "finding_type":
            embed.add_field(name="Finding", value=event["finding_type"], inline=True)
    elif event_type == "vigil_finding":
        if event.get("finding_type") and qualifier_key != "finding_type":
            embed.add_field(name="Finding", value=event["finding_type"], inline=True)
    elif event_type in ("watcher_finding", "watcher_resolution_finding"):
        pattern = str(event.get("pattern") or "")
        # The pattern is in the title, and Watcher's message opens "[P011] ".
        if (pattern and qualifier_key != "pattern"
                and not description.startswith(f"[{pattern}]")):
            embed.add_field(name="Pattern", value=pattern, inline=True)
        if file_path:
            loc = short_path(file_path)
            # Watcher's message already reads "<path>:<line> — <hint>".
            if loc not in description:
                if event.get("line"):
                    loc = f"{loc}:{event['line']}"
                embed.add_field(name="Location", value=loc, inline=False)
        if event.get("violation_class"):
            embed.add_field(name="Violation", value=event["violation_class"], inline=True)
        if event_type == "watcher_finding" and fingerprint and not fingerprint.startswith("…"):
            embed.add_field(
                name="Close",
                value=f"`--resolve {fingerprint}` or `--dismiss {fingerprint}`",
                inline=False,
            )

    # Server-computed event check, attached at ingest for forced-release
    # findings (any event type — governance owns the computation).
    ev = event.get("evidence")
    if isinstance(ev, dict) and ev.get("kind") == "forced_release":
        embed.add_field(name="Event check", value=_evidence_line(ev)[:1024], inline=False)

    # The time is the embed timestamp (shown in the reader's zone); the footer
    # is the stable machine name to search for, not the poller's event counter,
    # which restarts at 1 when governance restarts.
    embed.set_footer(text=str(event_type))
    return embed


# Synthesized REST events that are routine activity — operators don't need
# to scan every onboard or idle ping. Everything else is a signal.
_ACTIVITY_REST_TYPES = frozenset({
    "agent_new",
    "agent_idle",
})


_FINDING_SUFFIX = "_finding"

# Identity fields consulted only for a bare ``finding`` type, which carries no
# author in its name. Labels first — they match the operator-facing resident
# names ("sentinel"), where ids may be opaque UUIDs.
_RESIDENT_ID_KEYS = (
    "resident_label",
    "agent_label",
    "resident_name",
    "agent_name",
    "resident_id",
    "agent_id",
)


def is_finding_event(event: dict) -> bool:
    """True for resident findings — ``sentinel_finding``, ``vigil_finding``, …"""
    t = str(event.get("type") or "").strip().lower()
    return t == "finding" or t.endswith(_FINDING_SUFFIX)


def finding_resident_key(event: dict) -> str | None:
    """Return the lowercased resident that authored a finding, else ``None``.

    Findings are named after their author, so the event-type prefix up to the
    first underscore is the resident: ``sentinel_finding`` and
    ``sentinel_alarm_finding`` both resolve to ``"sentinel"``. A bare
    ``finding`` type has no author in its name and falls back to the event's
    identity fields. Used to route a resident's findings to its own channel.
    """
    if not is_finding_event(event):
        return None
    t = str(event.get("type") or "").strip().lower()
    prefix = t[: -len(_FINDING_SUFFIX)] if t.endswith(_FINDING_SUFFIX) else ""
    if prefix:
        return prefix.split("_", 1)[0]
    for key in _RESIDENT_ID_KEYS:
        value = event.get(key)
        if value:
            return str(value).strip().lower()
    return None


def is_resolution_finding(event: dict) -> bool:
    """True for a resident's closure of an earlier finding (``*_resolution_finding``)."""
    return is_finding_event(event) and "_resolution" in str(event.get("type") or "").lower()


def is_dismissed_resolution(event: dict) -> bool:
    """A resolution recording that nothing needs doing (Watcher "[dismissed] ...")."""
    return (
        is_resolution_finding(event)
        and str(event.get("action") or "").strip().lower() == "dismissed"
    )


# deploy_drift_doctor's behind-origin message: "<surface>: N merged commit(s)
# not pulled into the live checkout ...". Its other critical conditions (live
# checkout on the wrong branch) are genuine and still page.
_DEPLOY_DRIFT_LAG = re.compile(r"merged commit\(s\) not pulled", re.IGNORECASE)

# Noisy pattern-hit finding classes that never page #alerts, whatever their
# severity: they recur as routine and are read in their resident channel.
# Explicit by type (and sub-kind) so every other high/critical finding —
# Sentinel verdict_shift / coordinated_degradation (sentinel_finding),
# forced-release alarms (sentinel_alarm_finding, alarm_kind=ad_hoc), the Lumen
# check-in doctor, Vigil service-down, lease-plane health, bridge liveness —
# keeps reaching the operator. Producers: unitares agents/watcher,
# agents/sentinel/forced_release_alarm.py, scripts/ops/deploy_drift_doctor.py.
_FINDING_ALERT_EXCLUSIONS: dict[str, Callable[[dict], bool]] = {
    # Watcher pattern hits (P0xx rules over source files): ~4/day at "high".
    "watcher_finding": lambda e: True,
    # Batched held-by-other lease conflicts; forced releases still page.
    "sentinel_alarm_finding": lambda e: str(e.get("alarm_kind") or "") == "conflict_batch",
    # Deploy mirror lag (merged commits not yet pulled into a live checkout).
    "deploy_drift_finding": lambda e: bool(_DEPLOY_DRIFT_LAG.search(str(e.get("message") or ""))),
}


def is_noisy_finding_class(event: dict) -> bool:
    """True for a finding class listed in ``_FINDING_ALERT_EXCLUSIONS``."""
    rule = _FINDING_ALERT_EXCLUSIONS.get(str(event.get("type") or "").strip().lower())
    return bool(rule and rule(event))


def finding_needs_operator(event: dict) -> bool:
    """Should this finding go to #alerts instead of its resident channel?

    Findings post once, to one channel. Severities in
    ``config.FINDING_ALERT_SEVERITIES`` (default: high, critical) reach
    #alerts unless the finding is a noisy pattern-hit class; a resolution
    never does, because it reports something already closed.
    """
    if not is_finding_event(event) or is_resolution_finding(event):
        return False
    if str(event.get("severity") or "").strip().lower() not in config.FINDING_ALERT_SEVERITIES:
        return False
    return not is_noisy_finding_class(event)


def finding_destination(
    event: dict,
    *,
    alerts_channel,
    resident_channels: dict,
    residents_channel,
):
    """The single channel a finding is posted to, or ``None`` if not a finding.

    #alerts when it needs the operator; otherwise the author's own channel
    (Sentinel, Doctor), else the shared #residents feed. ``None`` is also
    returned when neither resident channel exists, so the caller falls back to
    its ordinary activity/signals routing.
    """
    if not is_finding_event(event):
        return None
    if finding_needs_operator(event) and alerts_channel is not None:
        return alerts_channel
    key = finding_resident_key(event)
    own = resident_channels.get(key) if key else None
    return own or residents_channel


def classify_rest_event(event: dict) -> str:
    """Return ``"activity"`` for routine REST events, ``"signals"`` otherwise.

    Findings route to a resident channel and never reach this helper.
    """
    t = event.get("type") or ""
    if t in _ACTIVITY_REST_TYPES:
        return "activity"
    return "signals"


def is_critical_event(event: dict) -> bool:
    """Should this event also be posted to #alerts?

    For a finding this means "goes to #alerts" (see
    :func:`finding_needs_operator`); findings are never mirrored.
    """
    if is_finding_event(event):
        return finding_needs_operator(event)
    severity = event.get("severity")
    if severity == "critical":
        return True
    if event.get("type") == "verdict_change" and event.get("to") in ("pause", "reject"):
        return True
    # A dialectic that has asked for a human is the one event class where the
    # ONLY resolution is a person acting. It carries no `critical` severity, so
    # it used to land in #signals — ~600 events/hour — where it is delivered and
    # unreadable. That is why 6 genuine facilitation requests over nine months
    # were answered by nobody: not missed by the poller (it uses a cursor), just
    # buried. #alerts is low-volume by construction, which is the whole point.
    if event.get("type") == "dialectic_facilitation_needed":
        return True
    return False


def attention_severity(event: dict, *, critical: bool = False) -> str:
    """Make receipt/display severity agree with existing attention routing.

    No governance decision or source event changes. Some critical event types
    omit severity; recording them as info hides unacknowledged deliveries.
    """
    severity = str(event.get("severity") or "info").lower()
    if critical and severity not in {"high", "critical"}:
        return "critical"
    return severity
