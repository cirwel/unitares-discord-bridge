"""Map governance events to Discord embeds."""

from __future__ import annotations

import discord

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
    agent_id = event.get("agent_id")
    return (
        event.get("agent_name")
        or event.get("agent_label")
        or (str(agent_id)[:12] if agent_id else "system")
    )


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


def event_to_embed(event: dict) -> discord.Embed:
    """Convert a governance event dict to a Discord embed."""
    severity = event.get("severity", "info")
    event_type = event.get("type", "unknown")
    colour = SEVERITY_COLOURS.get(severity, discord.Colour.greyple())
    title = EVENT_TITLES.get(event_type, event_type.replace("_", " ").title())

    embed = discord.Embed(
        title=title,
        description=_event_description(event),
        colour=colour,
    )
    embed.add_field(name="Agent", value=_event_agent(event), inline=True)
    embed.add_field(name="Severity", value=severity, inline=True)

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
    elif event_type == "drift_alert":
        embed.add_field(name="Axis", value=event.get("axis", "?"), inline=True)
        embed.add_field(name="Value", value=f"{_safe_float(event.get('value')):.2f}", inline=True)
    elif event_type == "sentinel_finding":
        if event.get("violation_class"):
            embed.add_field(name="Violation", value=event["violation_class"], inline=True)
        if event.get("finding_type"):
            embed.add_field(name="Finding", value=event["finding_type"], inline=True)
    elif event_type == "vigil_finding":
        if event.get("finding_type"):
            embed.add_field(name="Finding", value=event["finding_type"], inline=True)
    elif event_type == "watcher_finding":
        if event.get("pattern"):
            embed.add_field(name="Pattern", value=event["pattern"], inline=True)
        if event.get("file"):
            loc = event["file"]
            if event.get("line"):
                loc = f"{loc}:{event['line']}"
            embed.add_field(name="Location", value=loc, inline=False)
        if event.get("violation_class"):
            embed.add_field(name="Violation", value=event["violation_class"], inline=True)

    embed.set_footer(text=f"Event #{event.get('event_id', '?')}")
    return embed


# Synthesized REST events that are routine activity — operators don't need
# to scan every onboard or idle ping. Everything else is a signal.
_ACTIVITY_REST_TYPES = frozenset({
    "agent_new",
    "agent_idle",
})


def classify_rest_event(event: dict) -> str:
    """Return ``"activity"`` for routine REST events, ``"signals"`` otherwise.

    Findings route to #residents and never reach this helper.
    """
    t = event.get("type") or ""
    if t in _ACTIVITY_REST_TYPES:
        return "activity"
    return "signals"


def is_critical_event(event: dict) -> bool:
    """Should this event also be posted to #alerts?"""
    severity = event.get("severity")
    if severity == "critical":
        return True
    if event.get("type") == "verdict_change" and event.get("to") in ("pause", "reject"):
        return True
    # Findings are high-signal by construction — high severity also pages alerts
    if event.get("type", "").endswith("_finding") and severity == "high":
        return True
    return False
