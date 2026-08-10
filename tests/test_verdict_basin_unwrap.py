"""basin arrives wrapped; the verdict derivation has to unwrap it.

`get_governance_metrics` returns basin as {"value": "low", "meaning": ...,
"thresholds": {...}} — the same shape `_scalar` already unwraps for E/I/S/V.
`_derive_verdict` used `str(data.get("basin", "")).lower()`, which stringified
the whole dict, so `basin == "low"` and `basin == "high"` were both
unreachable. Every agent fell through to risk_score, which for the live fleet
reads "🟡 medium", so the HUD painted all 50 agents yellow and the footer's
boundary count equalled the agent count.

Observed live 2026-08-10: Lumen at basin=low, risk 0.717, status 🔴 critical —
rendered as a yellow boundary agent, indistinguishable from a healthy one.
"""

from bridge.mcp_client import _derive_verdict


def test_wrapped_low_basin_pauses():
    """The live shape. This is the case that was silently unreachable."""
    verdict = _derive_verdict({
        "basin": {"value": "low", "meaning": "Degraded. May need recovery."},
        "risk_score": {"value": 0.717, "status": "🟡 medium"},
        "status": "🔴 critical",
    })
    assert verdict == "pause"


def test_wrapped_high_basin_proceeds():
    verdict = _derive_verdict({
        "basin": {"value": "high", "meaning": "Healthy."},
        "risk_score": {"value": 0.1, "status": "🟢 low"},
    })
    assert verdict == "proceed"


def test_bare_string_basin_still_works():
    """Older/lite responses may send basin unwrapped."""
    assert _derive_verdict({"basin": "low"}) == "pause"
    assert _derive_verdict({"basin": "high"}) == "proceed"


def test_explicit_verdict_still_wins():
    verdict = _derive_verdict({
        "verdict": "reject",
        "basin": {"value": "high"},
    })
    assert verdict == "reject"


def test_high_risk_pauses_without_a_basin():
    assert _derive_verdict({"risk_score": {"status": "🔴 high"}}) == "pause"


def test_medium_risk_guides():
    verdict = _derive_verdict({
        "basin": {"value": "moderate"},
        "risk_score": {"status": "🟡 medium"},
    })
    assert verdict == "guide"


def test_empty_response_falls_back_to_guide():
    assert _derive_verdict({}) == "guide"


def test_basin_dict_without_value_key_does_not_crash():
    assert _derive_verdict({"basin": {"meaning": "no value key"}}) == "guide"


def test_not_every_agent_lands_on_guide():
    """The regression this file exists to prevent: a derivation where every
    live response maps to the same verdict is indistinguishable from no
    derivation at all."""
    live_shapes = [
        {"basin": {"value": "low"}, "risk_score": {"status": "🟡 medium"}},
        {"basin": {"value": "high"}, "risk_score": {"status": "🟢 low"}},
        {"basin": {"value": "moderate"}, "risk_score": {"status": "🟡 medium"}},
    ]
    verdicts = {_derive_verdict(s) for s in live_shapes}
    assert len(verdicts) > 1, f"all live shapes collapsed to {verdicts}"
