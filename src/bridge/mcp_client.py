"""HTTP clients for governance-mcp and anima-mcp services."""

from __future__ import annotations

import json
import logging
import os

import httpx

log = logging.getLogger(__name__)

# Mirrors governance's RESERVED_PREFIXES (src/mcp_handlers/validators.py). An
# agent_id with one of these prefixes is rejected by get_governance_metrics with
# error_type=reserved_prefix. agent(action="list") redacts UUIDs for non-operator callers
# and can hand back a synthesized reserved-prefix human id (e.g. Chronicler's
# "mcp_<date>_<suffix>"); round-tripping that into a metrics read produces a
# guaranteed rejection every HUD cycle (~2/min). Skip such ids before firing.
_RESERVED_AGENT_ID_PREFIXES = (
    "system_", "admin_", "root_", "mcp_", "governance_", "auth_",
)


def _is_reserved_agent_id(agent_id: str) -> bool:
    return agent_id.lower().startswith(_RESERVED_AGENT_ID_PREFIXES)


# During an outage every poll fails the same way. Logging each one at WARNING
# buried the log (3,572 fetch_gallery warnings from a single Lumen outage), so
# a failure streak logs WARNING on its first failure and on every Nth after
# that, DEBUG in between, and one INFO line when it ends.
FAILURE_LOG_EVERY = 20


def _cause(exc: BaseException) -> str:
    """Typed, never-empty description of an exception.

    ``str()`` of an httpx timeout is "", which left 299 of 311 failure
    warnings with an empty cause. The repr is used only when the message is
    empty, so a normal failure reads "ConnectError: refused" rather than
    "ConnectError: ConnectError('refused')".
    """
    return f"{type(exc).__name__}: {str(exc) or repr(exc)}"


def _failure_level(streak: int) -> int:
    if streak == 1 or streak % FAILURE_LOG_EVERY == 0:
        return logging.WARNING
    return logging.DEBUG


class _FailureStreaks:
    """Per-key consecutive-failure counters that only gate log levels."""

    def __init__(self, service: str) -> None:
        self.service = service
        self._streaks: dict[str, int] = {}

    def failed(self, key: str, exc: BaseException, detail: str = "") -> None:
        """``detail`` names what this attempt was for (e.g. a filename), so
        the WARNING says which item failed even though the streak is keyed
        by operation."""
        streak = self._streaks.get(key, 0) + 1
        self._streaks[key] = streak
        what = f"{key}({detail})" if detail else key
        log.log(
            _failure_level(streak), "%s %s failed (%d in a row): %s",
            self.service, what, streak, _cause(exc),
        )

    def succeeded(self, key: str) -> None:
        streak = self._streaks.pop(key, 0)
        if streak:
            log.info(
                "%s %s recovered after %d consecutive failures",
                self.service, key, streak,
            )


class GovernanceClient:
    """Client for the governance-mcp HTTP API.

    Uses a persistent httpx.AsyncClient for connection pooling.
    Call ``open()`` before use and ``close()`` on shutdown.

    The bridge onboards a governance identity at startup (``onboard()``)
    and echoes the returned ``client_session_id`` on every tool call so
    its traffic is bound instead of anonymous. Per the identity.md v2
    ontology each bridge process-instance mints fresh with declared
    lineage: the previous instance's UUID is persisted to a sidecar file
    and passed as ``parent_agent_id`` on the next start.
    """

    def __init__(self, base_url: str, token: str = "", operator_token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.consecutive_failures = 0
        self.agent_uuid: str | None = None
        self.client_session_id: str | None = None
        self.is_operator = bool(operator_token)
        self._client: httpx.AsyncClient | None = None
        self._headers: dict[str, str] = {}
        if token:
            self._headers["Authorization"] = f"Bearer {token}"
        if operator_token:
            # Operator tier is a separate header from bearer auth, and only
            # this one lifts UUID redaction on agent(action="list"). Sending the
            # operator token as a bearer does nothing.
            self._headers["X-Unitares-Operator"] = operator_token

    async def open(self) -> None:
        """Create the persistent HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url, timeout=10, headers=self._headers,
            )

    async def close(self) -> None:
        """Close the persistent HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _note_success(self) -> None:
        if self.consecutive_failures:
            log.info(
                "governance recovered after %d consecutive failures",
                self.consecutive_failures,
            )
        self.consecutive_failures = 0

    def _note_failure(self, what: str, exc: BaseException) -> None:
        # consecutive_failures drives the "Governance MCP Unreachable" alert
        # and the HUD's connection line; it counts exactly as before. Only
        # the level of this log line depends on it.
        self.consecutive_failures += 1
        log.log(
            _failure_level(self.consecutive_failures),
            "governance %s failed (%d): %s",
            what, self.consecutive_failures, _cause(exc),
        )

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client
        # Fallback: create a one-shot client if open() wasn't called.
        # This keeps backwards compat for tests that don't call open().
        return httpx.AsyncClient(base_url=self.base_url, timeout=10, headers=self._headers)

    async def fetch_events(self, since: int = 0, limit: int = 50) -> list[dict] | None:
        """GET /api/events?since=N&limit=N. Returns None on error.

        The None-vs-[] distinction is load-bearing for the event poller's
        stale-cursor detection: an errored fetch must never read as "the
        server's feed is empty" — that misread made every transient governance
        stall reset the cursor to 0 and replay the whole feed to Discord
        (observed live 2026-07-28: the same events re-posted on every ~30-min
        stall window).
        """
        client = self._get_client()
        try:
            resp = await client.get(
                "/api/events",
                params={"since": since, "limit": limit},
            )
            resp.raise_for_status()
            self._note_success()
            data = resp.json()
            # /api/events wraps events in {"success": ..., "events": [...], "count": N}
            if isinstance(data, dict):
                return data.get("events", [])
            return data if isinstance(data, list) else []
        except Exception as exc:
            self._note_failure("fetch_events", exc)
            return None
        finally:
            if self._client is None:
                await client.aclose()

    async def fetch_taxonomy(self) -> dict | None:
        """GET /v1/taxonomy. Returns None on error.

        Response includes classes (list of class dicts) and reverse
        (surface_kind → surface_id → class_id lookup table). Used by
        ws_events for per-class channel routing.
        """
        client = self._get_client()
        try:
            resp = await client.get("/v1/taxonomy")
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success", True):
                return None
            return data
        except Exception as exc:
            log.warning("governance fetch_taxonomy failed: %s", _cause(exc))
            return None
        finally:
            if self._client is None:
                await client.aclose()

    async def fetch_health(self) -> dict | None:
        """GET /health. Returns None on error."""
        client = self._get_client()
        try:
            resp = await client.get("/health")
            resp.raise_for_status()
            self._note_success()
            return resp.json()
        except Exception as exc:
            self._note_failure("fetch_health", exc)
            return None
        finally:
            if self._client is None:
                await client.aclose()

    async def fetch_timeline(
        self, hours: int = 6, kinds: str = "checkin,lifecycle", limit: int = 2000,
    ) -> dict | None:
        """GET /v1/timeline (read-only). Returns the response dict, or None on error.

        Used by /sessions for its ``agents`` roster, which carries each agent's
        declared ``host_session`` ({harness, session_id, resumable} or null,
        portal hub H0). The roster only names agents with an event in the
        window, so ``kinds`` and ``limit`` decide who appears; ``truncated``
        in the response says when a kind hit the limit.

        An auxiliary read: it never touches ``consecutive_failures``, which
        drives the "Governance MCP Unreachable" alert for the event feed.
        """
        client = self._get_client()
        try:
            resp = await client.get(
                "/v1/timeline",
                params={"hours": hours, "kinds": kinds, "limit": limit},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict) or not data.get("success", False):
                return None
            return data
        except Exception as exc:
            log.warning("governance fetch_timeline failed: %s", _cause(exc))
            return None
        finally:
            if self._client is None:
                await client.aclose()

    async def record_bridge_event(self, payload: dict) -> bool:
        """POST a Discord delivery/attention receipt to governance.

        Receipt writes are best-effort observability. A failure here must not
        affect governance polling or Discord delivery.
        """
        client = self._get_client()
        enriched = {
            **payload,
            "bridge_id": self.agent_uuid or payload.get("bridge_id") or "discord-bridge",
        }
        try:
            resp = await client.post("/v1/bridge/events", json=enriched, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            return bool(data.get("success"))
        except Exception as exc:
            log.debug("governance bridge receipt failed: %s", exc)
            return False
        finally:
            if self._client is None:
                await client.aclose()

    async def onboard(self, identity_path: str) -> bool:
        """Mint a governance identity for this bridge process-instance.

        Reads the previous instance's UUID from ``identity_path`` (JSON
        sidecar) and declares it as ``parent_agent_id``; persists the
        newly minted UUID for the next instance. Sets ``agent_uuid`` and
        ``client_session_id`` on success so ``call_tool`` binds every
        subsequent call. Best-effort: on any failure the bridge keeps
        running unbound (its reads are pre-onboard-exempt server-side).
        """
        parent_uuid = None
        try:
            with open(identity_path) as f:
                parent_uuid = json.load(f).get("agent_uuid") or None
        except FileNotFoundError:
            pass
        except Exception as exc:
            log.warning("governance identity file unreadable (%s): %s", identity_path, exc)

        arguments: dict = {
            "force_new": True,
            "name": "discord-bridge",
            "client_hint": "discord-bridge",
            "spawn_reason": "new_session",
        }
        if parent_uuid:
            arguments["parent_agent_id"] = parent_uuid

        result = await self.call_tool("onboard", arguments)
        if result is None:
            log.warning("governance onboard failed — bridge continues unbound")
            return False
        try:
            data = parse_tool_result(result)
            uuid = data.get("uuid") if isinstance(data, dict) else None
            session_id = data.get("client_session_id") if isinstance(data, dict) else None
            if not uuid:
                log.warning("governance onboard returned no uuid: %s", data)
                return False
        except (json.JSONDecodeError, TypeError, KeyError) as exc:
            log.warning("governance onboard response unparseable: %s", exc)
            return False

        self.agent_uuid = uuid
        self.client_session_id = session_id
        try:
            os.makedirs(os.path.dirname(identity_path) or ".", exist_ok=True)
            with open(identity_path, "w") as f:
                json.dump({"agent_uuid": uuid}, f)
        except Exception as exc:
            # Identity still works for this process; only next-start lineage
            # is lost.
            log.warning("could not persist governance identity to %s: %s", identity_path, exc)
        log.info(
            "governance onboarded as %s (lineage: %s)",
            uuid, parent_uuid or "fresh",
        )
        return True

    async def call_tool(
        self, tool_name: str, arguments: dict, *, track_failures: bool = True,
    ) -> dict | None:
        """POST /v1/tools/call with {"name": ..., "arguments": ...}. Returns None on error.

        ``track_failures=False`` is for auxiliary reads (agent tag lookups):
        their outcome neither raises nor clears ``consecutive_failures``, which
        drives the "Governance MCP Unreachable" alert and must reflect the
        event feed itself.
        """
        client = self._get_client()
        if (
            self.client_session_id
            and isinstance(arguments, dict)
            and "client_session_id" not in arguments
        ):
            arguments = {**arguments, "client_session_id": self.client_session_id}
        try:
            resp = await client.post(
                "/v1/tools/call",
                json={"name": tool_name, "arguments": arguments},
                timeout=30,
            )
            resp.raise_for_status()
            if track_failures:
                self._note_success()
            return resp.json()
        except Exception as exc:
            if not track_failures:
                log.debug("governance call_tool(%s) failed (untracked): %s", tool_name, _cause(exc))
                return None
            self._note_failure(f"call_tool({tool_name})", exc)
            return None
        finally:
            if self._client is None:
                await client.aclose()


def parse_tool_result(result: dict) -> dict | list:
    """Unwrap the MCP tool result envelope.

    Supports two shapes:
    - Legacy MCP: result["result"]["content"][0]["text"] is a JSON string
    - Direct: result["result"] is the data dict/list itself
    """
    inner = result.get("result", {})
    if isinstance(inner, dict):
        content = inner.get("content")
        if content:
            text = content[0].get("text", "{}")
            return json.loads(text)
    return inner


def _scalar(value) -> float:
    """Coerce a value that may be a number or {"value": N, ...} dict to float."""
    if isinstance(value, dict):
        value = value.get("value", 0.0)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


_HUD_VERDICTS = frozenset({"proceed", "guide", "pause", "reject"})

# Mirror governance's GovernanceConfig.RISK_APPROVE_THRESHOLD (low/medium edge)
# and RISK_REVISE_THRESHOLD (medium/high edge), which get_governance_metrics
# uses to word risk_score.status.
_RISK_MEDIUM = 0.45
_RISK_HIGH = 0.70


def _derive_verdict(data: dict) -> str:
    """Derive a HUD verdict (proceed/guide/pause/reject) from metric response.

    Prefers an explicit ``verdict`` field; otherwise maps from basin + risk_score.status.
    """
    verdict = data.get("verdict")
    # Only a verdict already in the HUD's vocabulary is taken as-is. The agent
    # list's metrics rows carry the monitor's raw verdict ("safe", "caution",
    # "high-risk"), which the HUD has no emoji for; get_governance_metrics
    # wraps the same value in a dict, which never matched here either. Both
    # fall through to the basin/risk derivation below.
    if isinstance(verdict, str) and verdict in _HUD_VERDICTS:
        return verdict

    risk = data.get("risk_score", {})
    if isinstance(risk, dict):
        risk_status = str(risk.get("status") or "").lower()
    elif isinstance(risk, (int, float)) and not isinstance(risk, bool):
        # The agent list sends risk_score as a bare float. Bucket it the way
        # get_governance_metrics words its status, so both sources agree.
        if risk >= _RISK_HIGH:
            risk_status = "high"
        elif risk >= _RISK_MEDIUM:
            risk_status = "medium"
        else:
            risk_status = "low"
    else:
        risk_status = ""
    # basin arrives as {"value": "low", "meaning": ..., "thresholds": {...}},
    # the same wrapped shape _scalar already unwraps for E/I/S/V. The previous
    # `str(data.get("basin"))` stringified the whole dict, so no basin
    # comparison below could ever match and the branch was dead: every agent
    # fell through to "guide" via risk_score, and a degraded agent was painted
    # the same colour as a healthy one. Live at the time of the fix: Lumen at
    # basin=low / 🔴 critical rendered yellow.
    basin_raw = data.get("basin", "")
    if isinstance(basin_raw, dict):
        basin_raw = basin_raw.get("value", "")
    basin = str(basin_raw or "").lower()

    # Basin is checked before risk: it is the governance-level judgement, and a
    # "low" basin with only medium risk is exactly the case the old ordering
    # mislabelled.
    if basin == "low" or "high" in risk_status:
        return "pause"
    if "medium" in risk_status or "moderate" in risk_status:
        return "guide"
    if basin == "high":
        return "proceed"
    return "guide"


async def fetch_agents(
    gov_client: "GovernanceClient", *, with_metrics: bool = False,
    track_failures: bool = True,
) -> list[dict]:
    """Call agent(action="list") and return normalised list of {"id": ..., "label": ...}.

    Sorted by last activity descending (most-recent first). Test agents are
    filtered out by governance; we widen the window and re-sort locally so
    the HUD surfaces recent agents instead of "best of the best" high-update
    ones.

    ``with_metrics=True`` asks the full list for each row's metrics in the
    same call, and each returned dict also carries ``"metrics"`` (the row's
    metrics, possibly None) and ``"observed"`` (it has at least one state
    row). ``fetch_metrics`` reads the persisted-state rows among those
    instead of calling get_governance_metrics once per agent: the HUD used
    to make 51 calls every 30 s (about 140k a day), nearly all of the
    bridge's governance load. Agents with a loaded monitor still get their
    own call (see ``fetch_metrics``). Callers that only need labels keep the
    cheaper lite listing.

    ``track_failures`` is passed to :meth:`GovernanceClient.call_tool`; set it
    to ``False`` for cosmetic reads that must not move the unreachable alert.
    """
    if with_metrics:
        arguments = {
            "action": "list", "lite": False, "grouped": False,
            "include_metrics": True, "limit": 50, "recent_days": 7,
        }
    else:
        arguments = {"action": "list", "lite": True, "limit": 50, "recent_days": 7}
    result = await gov_client.call_tool(
        "agent", arguments, track_failures=track_failures,
    )
    if result is None:
        return []
    try:
        data = parse_tool_result(result)
        if isinstance(data, dict):
            items = data.get("agents", [])
        elif isinstance(data, list):
            items = data
        else:
            items = []
        if isinstance(items, dict):
            # The full list groups rows by lifecycle status unless told
            # grouped=False. Flatten rather than crash if it does.
            items = [
                row for group in items.values() if isinstance(group, list)
                for row in group
            ]
        items = sorted(
            items,
            key=lambda it: it.get("last_update") or it.get("last") or "",
            reverse=True,
        )
        agents = []
        redacted = 0
        for item in items:
            # Governance redacts UUIDs for non-operator callers
            # (KG 2026-04-20T00:57:45) and substitutes a display handle
            # like "Claude_Code_<date>_<uuid8>". The handle is NOT a valid
            # agent_id: get_governance_metrics resolves against the UUID and
            # will not match it. Round-tripping it produced the 2026-08-10
            # HUD-of-seed-values incident — 50 agents at a constant
            # E=0.70 I=0.80 S=0.20 V=0.00. Count them so a missing operator
            # token is visible in the log instead of silently degrading.
            # Lite rows flag it as uuid_redacted, full rows as agent_id_redacted.
            if item.get("uuid_redacted") or item.get("agent_id_redacted"):
                redacted += 1
            agent_id = item.get("agent_id") or item.get("id")
            if not agent_id:
                # Skip rows without a usable id — passing "" downstream
                # silently fires get_governance_metrics(agent_id="").
                continue
            # The lite listing falls back to the public handle for an
            # unlabeled agent; the full row has no handle, so with the
            # operator token an unlabeled agent's id is its full UUID.
            # Shorten that rather than print 36 characters on the HUD.
            label = item.get("label") or item.get("name") or _short_id(agent_id)
            agent = {"id": agent_id, "label": label}
            if with_metrics:
                agent["metrics"] = item.get("metrics")
                agent["observed"] = (item.get("total_updates") or 0) >= 1
            agents.append(agent)
        if redacted:
            log.warning(
                "agent(action='list') redacted %d/%d agent UUIDs — the per-agent "
                "get_governance_metrics read and /agent lookups cannot resolve "
                "these handles. Set GOVERNANCE_OPERATOR_TOKEN (sent as "
                "X-Unitares-Operator) to receive real UUIDs.",
                redacted, len(items),
            )
        return agents
    except (json.JSONDecodeError, TypeError, KeyError, AttributeError) as exc:
        log.warning("Failed to parse agent(action='list'): %s", _cause(exc))
        return []


async def fetch_metrics(
    gov_client: "GovernanceClient", agents: list[dict],
) -> dict[str, dict]:
    """Fetch EISV metrics for each agent. Returns {agent_id: {E, I, S, V, verdict}}.

    Agents from ``fetch_agents(..., with_metrics=True)`` carry their list
    row's metrics. An agent with no state row (``observed`` False) has
    nothing to read and renders as "no state", whatever the row holds: a
    loaded 0-update monitor reports its genesis seed vector, not a reading.

    A row is read without a call only when governance projected it from the
    persisted state row (``source == "persisted_state"``). That row stores the
    primary EISV get_governance_metrics reports (behavioral when its
    confidence is >= 0.3, else ODE). A row built from a loaded monitor
    (``live_monitor`` / ``restored_monitor``) carries the monitor's raw ODE
    state instead, so those agents, and any row without a full reading, get
    one get_governance_metrics call. Otherwise one HUD would mix two EISV
    sources.
    """
    metrics: dict[str, dict] = {}
    for agent in agents:
        agent_id = agent.get("id")
        if not agent_id:
            # Defense in depth: even if fetch_agents emitted an empty id
            # (shouldn't happen post-fix), don't fire metric requests for
            # "" — they return empty dicts and pollute the HUD.
            continue
        if "metrics" in agent:
            if not agent.get("observed", True):
                continue
            row = agent.get("metrics")
            if _has_eisv(row) and row.get("source") == _ROW_SOURCE_PERSISTED:
                metrics[agent_id] = _metrics_entry(row)
                continue
        if _is_reserved_agent_id(agent_id):
            # Reserved-prefix ids (e.g. a redacted "mcp_<date>_<suffix>" human
            # id from agent(action="list")) are rejected by get_governance_metrics with
            # error_type=reserved_prefix. Firing them anyway logs a guaranteed
            # failure every HUD cycle. The agent still shows in the HUD with its
            # label; it just carries no EISV — same outcome as today, minus the
            # rejected call.
            continue
        result = await gov_client.call_tool(
            "get_governance_metrics", {"agent_id": agent_id},
        )
        if result is None:
            continue
        try:
            data = parse_tool_result(result)
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict) and data.get("success") is False:
                # An unresolvable agent_id now refuses instead of answering
                # with the ODE seed vector (governance `unknown_agent`).
                # Drop it rather than rendering it: DEFAULT_METRICS in the
                # HUD is visibly zero, whereas a seed vector is not visibly
                # anything.
                log.warning(
                    "metrics unavailable for %s: %s (%s)",
                    agent_id, data.get("error"), data.get("error_type"),
                )
                continue
            metrics[agent_id] = _metrics_entry(data)
        except (json.JSONDecodeError, TypeError, KeyError, AttributeError) as exc:
            log.warning("Failed to parse metrics for %s: %s", agent_id, _cause(exc))
    return metrics


ATTENTION_SCHEMA = "anima.self_iteration.attention.v1"
ATTENTION_NO_RESPONSE = "no response from Anima"


def _attention_failure_reason(data: object) -> str | None:
    """Why a decoded attention response is unusable, or None when it is usable."""
    if not isinstance(data, dict):
        return f"unexpected response type {type(data).__name__}"
    if data.get("success") is not True:
        error = data.get("error")
        detail = f": {str(error)[:120]}" if error else ""
        return f"Anima reported failure{detail}"
    result = data.get("result")
    if not isinstance(result, dict):
        return "result missing or not an object"
    schema = result.get("schema")
    if schema != ATTENTION_SCHEMA:
        return f"schema mismatch (got {str(schema)[:80]})"
    if not isinstance(result.get("items"), list):
        return "items missing or not a list"
    if (
        result.get("acknowledgement_is_approval") is not False
        or result.get("authority_granted") is not False
    ):
        return (
            "authority flags not False "
            f"(acknowledgement_is_approval={result.get('acknowledgement_is_approval')!r}, "
            f"authority_granted={result.get('authority_granted')!r})"
        )
    return None


# query.py _persisted_state_metrics tags its projection with this source.
# The other sources (live_monitor, restored_monitor) read monitor.state, the
# ODE vector, not the primary EISV get_governance_metrics returns.
_ROW_SOURCE_PERSISTED = "persisted_state"


def _short_id(agent_id: str) -> str:
    """First 8 characters of a full UUID; any other id is returned as-is."""
    if len(agent_id) == 36 and agent_id.count("-") == 4:
        return agent_id[:8]
    return agent_id


def _has_eisv(row) -> bool:
    """A list row's metrics are a reading only if all four values are present."""
    return isinstance(row, dict) and all(row.get(k) is not None for k in "EISV")


def _metrics_entry(data: dict) -> dict:
    return {
        "E": _scalar(data.get("E", data.get("entropy", 0.0))),
        "I": _scalar(data.get("I", data.get("integration", 0.0))),
        "S": _scalar(data.get("S", data.get("stability", 0.0))),
        "V": _scalar(data.get("V", data.get("volatility", 0.0))),
        "verdict": _derive_verdict(data),
    }


class AnimaClient:
    """Client for the anima-mcp HTTP API.

    Uses a persistent httpx.AsyncClient for connection pooling.
    Call ``open()`` before use and ``close()`` on shutdown.
    """

    def __init__(self, base_url: str, token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.is_online = True
        self._client: httpx.AsyncClient | None = None
        self._headers: dict[str, str] = {}
        # Log-level gating only; is_online is unaffected.
        self._failures = _FailureStreaks("anima")
        if token:
            self._headers["Authorization"] = f"Bearer {token}"

    async def open(self) -> None:
        """Create the persistent HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url, timeout=10, headers=self._headers,
            )

    async def close(self) -> None:
        """Close the persistent HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client
        return httpx.AsyncClient(base_url=self.base_url, timeout=10, headers=self._headers)

    async def fetch_state(self) -> dict | None:
        """GET /state. Returns None on error. Sets is_online."""
        client = self._get_client()
        try:
            resp = await client.get("/state")
            resp.raise_for_status()
            self.is_online = True
            self._failures.succeeded("fetch_state")
            return resp.json()
        except Exception as exc:
            self.is_online = False
            self._failures.failed("fetch_state", exc)
            return None
        finally:
            if self._client is None:
                await client.aclose()

    async def fetch_gallery(self, limit: int = 5) -> list[dict] | None:
        """GET /gallery?limit=N. Returns list of drawing dicts, or None on error."""
        client = self._get_client()
        try:
            resp = await client.get(
                "/gallery",
                params={"limit": limit},
            )
            resp.raise_for_status()
            data = resp.json()
            self._failures.succeeded("fetch_gallery")
            # /gallery wraps drawings in {"drawings": [...], "total": N, ...}
            if isinstance(data, dict):
                return data.get("drawings", [])
            return data if isinstance(data, list) else []
        except Exception as exc:
            self._failures.failed("fetch_gallery", exc)
            return None
        finally:
            if self._client is None:
                await client.aclose()

    async def fetch_drawing_image(self, filename: str) -> bytes | None:
        """GET /gallery/{filename}. Returns raw bytes on success, None on error."""
        client = self._get_client()
        try:
            resp = await client.get(f"/gallery/{filename}", timeout=15)
            resp.raise_for_status()
            self._failures.succeeded("fetch_drawing_image")
            return resp.content
        except Exception as exc:
            self._failures.failed("fetch_drawing_image", exc, detail=filename)
            return None
        finally:
            if self._client is None:
                await client.aclose()

    async def fetch_qa(self, limit: int = 50) -> dict | None:
        """GET /qa?limit=N. Returns {questions: [...], total, unanswered}, or None.

        Each question is {id, question, answered, timestamp, answer}. The /qa
        endpoint reads the message board directly, so it is independent of the
        KnowledgeBase insight store (which has its own retrieval quirks).
        """
        client = self._get_client()
        try:
            resp = await client.get("/qa", params={"limit": limit})
            resp.raise_for_status()
            data = resp.json()
            self._failures.succeeded("fetch_qa")
            return data if isinstance(data, dict) else None
        except Exception as exc:
            self._failures.failed("fetch_qa", exc)
            return None
        finally:
            if self._client is None:
                await client.aclose()

    async def fetch_self_iteration_attention(self, limit: int = 50) -> dict | None:
        """The attention projection, or None when it is unusable."""
        result, _reason = await self.read_self_iteration_attention(limit=limit)
        return result

    async def read_self_iteration_attention(
        self, limit: int = 50
    ) -> tuple[dict | None, str | None]:
        """Read Anima's bounded, non-authoritative attention projection.

        Returns ``(projection, None)`` when the projection is usable, otherwise
        ``(None, reason)``. The reason says what actually failed, so an outage
        notice does not call a rejected response "no response". Reasons for
        transport failures start with ``ATTENTION_NO_RESPONSE``.

        The projection is fetched through the existing authenticated REST tool
        adapter. Only the read-only ``attention`` action is used; this client
        never submits signatures or invokes an approval-consuming action.
        """
        client = self._get_client()
        try:
            try:
                resp = await client.post(
                    "/v1/tools/call",
                    json={
                        "name": "self_iteration",
                        "arguments": {"action": "attention", "limit": limit},
                    },
                )
            except httpx.TransportError as exc:
                # Connection refused, DNS, timeouts: Anima never answered.
                reason = f"{ATTENTION_NO_RESPONSE} ({type(exc).__name__})"
                self._failures.failed("fetch_self_iteration_attention", exc)
                return None, reason
            if resp.status_code >= 400:
                reason = f"HTTP {resp.status_code} from Anima"
                log.warning("anima self-iteration attention: %s", reason)
                return None, reason
            try:
                data = resp.json()
            except ValueError:
                log.warning("anima self-iteration attention: response is not JSON")
                return None, "response was not JSON"
            reason = _attention_failure_reason(data)
            if reason is not None:
                log.warning("anima self-iteration attention rejected: %s", reason)
                return None, reason
            self._failures.succeeded("fetch_self_iteration_attention")
            return data["result"], None
        except Exception as exc:
            self._failures.failed("fetch_self_iteration_attention", exc)
            return None, f"unexpected error ({type(exc).__name__})"
        finally:
            if self._client is None:
                await client.aclose()
