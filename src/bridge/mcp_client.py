"""HTTP clients for governance-mcp and anima-mcp services."""

from __future__ import annotations

import json
import logging
import os

import httpx

log = logging.getLogger(__name__)

# Mirrors governance's RESERVED_PREFIXES (src/mcp_handlers/validators.py). An
# agent_id with one of these prefixes is rejected by get_governance_metrics with
# error_type=reserved_prefix. list_agents redacts UUIDs for non-operator callers
# and can hand back a synthesized reserved-prefix human id (e.g. Chronicler's
# "mcp_<date>_<suffix>"); round-tripping that into a metrics read produces a
# guaranteed rejection every HUD cycle (~2/min). Skip such ids before firing.
_RESERVED_AGENT_ID_PREFIXES = (
    "system_", "admin_", "root_", "mcp_", "governance_", "auth_",
)


def _is_reserved_agent_id(agent_id: str) -> bool:
    return agent_id.lower().startswith(_RESERVED_AGENT_ID_PREFIXES)


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

    def __init__(self, base_url: str, token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.consecutive_failures = 0
        self.agent_uuid: str | None = None
        self.client_session_id: str | None = None
        self._client: httpx.AsyncClient | None = None
        self._headers: dict[str, str] = {}
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
        # Fallback: create a one-shot client if open() wasn't called.
        # This keeps backwards compat for tests that don't call open().
        return httpx.AsyncClient(base_url=self.base_url, timeout=10, headers=self._headers)

    async def fetch_events(self, since: int = 0, limit: int = 50) -> list[dict]:
        """GET /api/events?since=N&limit=N. Returns [] on error."""
        client = self._get_client()
        try:
            resp = await client.get(
                "/api/events",
                params={"since": since, "limit": limit},
            )
            resp.raise_for_status()
            self.consecutive_failures = 0
            data = resp.json()
            # /api/events wraps events in {"success": ..., "events": [...], "count": N}
            if isinstance(data, dict):
                return data.get("events", [])
            return data if isinstance(data, list) else []
        except Exception as exc:
            self.consecutive_failures += 1
            log.warning("governance fetch_events failed (%d): %s", self.consecutive_failures, exc)
            return []
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
            log.warning("governance fetch_taxonomy failed: %s", exc)
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
            self.consecutive_failures = 0
            return resp.json()
        except Exception as exc:
            self.consecutive_failures += 1
            log.warning("governance fetch_health failed (%d): %s", self.consecutive_failures, exc)
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

    async def call_tool(self, tool_name: str, arguments: dict) -> dict | None:
        """POST /v1/tools/call with {"name": ..., "arguments": ...}. Returns None on error."""
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
            self.consecutive_failures = 0
            return resp.json()
        except Exception as exc:
            self.consecutive_failures += 1
            log.warning("governance call_tool(%s) failed (%d): %s", tool_name, self.consecutive_failures, exc)
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


def _derive_verdict(data: dict) -> str:
    """Derive a HUD verdict (proceed/guide/pause/reject) from metric response.

    Prefers an explicit ``verdict`` field; otherwise maps from basin + risk_score.status.
    """
    verdict = data.get("verdict")
    if isinstance(verdict, str) and verdict:
        return verdict

    risk = data.get("risk_score", {})
    risk_status = risk.get("status", "").lower() if isinstance(risk, dict) else ""
    basin = str(data.get("basin", "")).lower()

    if "high" in risk_status or basin == "low":
        return "pause"
    if "medium" in risk_status or "moderate" in risk_status:
        return "guide"
    if basin == "high":
        return "proceed"
    return "guide"


async def fetch_agents(gov_client: "GovernanceClient") -> list[dict]:
    """Call list_agents and return normalised list of {"id": ..., "label": ...}.

    Sorted by last activity descending (most-recent first). Test agents are
    filtered out by governance's lite mode; we widen the window and re-sort
    locally so the HUD surfaces recent agents instead of "best of the best"
    high-update ones.
    """
    result = await gov_client.call_tool(
        "list_agents",
        {"lite": True, "limit": 50, "recent_days": 7},
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
        items = sorted(
            items,
            key=lambda it: it.get("last_update") or it.get("last") or "",
            reverse=True,
        )
        agents = []
        for item in items:
            # Governance redacts UUIDs for non-operator callers
            # (KG 2026-04-20T00:57:45). Skip rows without a usable id —
            # passing "" downstream silently fires
            # get_governance_metrics(agent_id="") and produces a HUD with
            # labels but no EISV state.
            agent_id = item.get("agent_id") or item.get("id")
            if not agent_id:
                continue
            label = item.get("label") or item.get("name") or agent_id
            agents.append({"id": agent_id, "label": label})
        return agents
    except (json.JSONDecodeError, TypeError, KeyError) as exc:
        log.warning("Failed to parse list_agents: %s", exc)
        return []


async def fetch_metrics(
    gov_client: "GovernanceClient", agents: list[dict],
) -> dict[str, dict]:
    """Fetch EISV metrics for each agent. Returns {agent_id: {E, I, S, V, verdict}}."""
    metrics: dict[str, dict] = {}
    for agent in agents:
        agent_id = agent.get("id")
        if not agent_id:
            # Defense in depth: even if fetch_agents emitted an empty id
            # (shouldn't happen post-fix), don't fire metric requests for
            # "" — they return empty dicts and pollute the HUD.
            continue
        if _is_reserved_agent_id(agent_id):
            # Reserved-prefix ids (e.g. a redacted "mcp_<date>_<suffix>" human
            # id from list_agents) are rejected by get_governance_metrics with
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
            metrics[agent_id] = {
                "E": _scalar(data.get("E", data.get("entropy", 0.0))),
                "I": _scalar(data.get("I", data.get("integration", 0.0))),
                "S": _scalar(data.get("S", data.get("stability", 0.0))),
                "V": _scalar(data.get("V", data.get("volatility", 0.0))),
                "verdict": _derive_verdict(data),
            }
        except (json.JSONDecodeError, TypeError, KeyError) as exc:
            log.warning("Failed to parse metrics for %s: %s", agent_id, exc)
    return metrics


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
            return resp.json()
        except Exception as exc:
            self.is_online = False
            log.warning("anima fetch_state failed: %s", exc)
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
            # /gallery wraps drawings in {"drawings": [...], "total": N, ...}
            if isinstance(data, dict):
                return data.get("drawings", [])
            return data if isinstance(data, list) else []
        except Exception as exc:
            log.warning("anima fetch_gallery failed: %s", exc)
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
            return resp.content
        except Exception as exc:
            log.warning("anima fetch_drawing_image(%s) failed: %s", filename, exc)
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
            return data if isinstance(data, dict) else None
        except Exception as exc:
            log.warning("anima fetch_qa failed: %s", exc)
            return None
        finally:
            if self._client is None:
                await client.aclose()
