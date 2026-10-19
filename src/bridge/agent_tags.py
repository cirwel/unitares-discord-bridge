"""Capability lookup for the subject agent of a governance event.

Governance's synthesized REST events (agent_new, risk_threshold,
drift_oscillation, trajectory_adjustment) carry only ``agent_id`` and
``agent_name``. Whether the agent is a long-lived resident or an embodied one
is recorded on its governance record as tags (``persistent``, ``embodied``,
``ephemeral`` ...), so the bridge reads them with ``agent(action="get")``, a
pre-onboard fleet read, and caches the answer. Names are never consulted.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict

from bridge.mcp_client import GovernanceClient, parse_tool_result

log = logging.getLogger(__name__)


class AgentTagResolver:
    """Cached ``agent_id -> frozenset(tags)`` lookup.

    ``tags()`` returns ``None`` when the tags are unknown (lookup failed,
    agent not found, unparseable reply). Callers treat unknown as "post as
    before" so a governance hiccup can never silently hide an event.

    Cache lifetimes: an answer containing one of ``resident_tags`` lives
    ``ttl`` (stable); any other answer lives ``nonresident_ttl`` (short, so a
    newly tagged resident is not mis-filtered for long); a failed lookup is
    remembered as unknown for ``failure_ttl`` so an outage is not hammered.

    Lookups go through the client's untracked side path: they never move the
    ``consecutive_failures`` counter behind the "Governance MCP Unreachable"
    alert.
    """

    def __init__(
        self,
        gov: GovernanceClient,
        *,
        ttl: float = 3600.0,
        nonresident_ttl: float = 300.0,
        failure_ttl: float = 60.0,
        resident_tags: frozenset[str] = frozenset({"persistent", "protected", "embodied"}),
        max_entries: int = 2000,
        timeout: float = 5.0,
    ) -> None:
        self.gov = gov
        self.ttl = ttl
        self.nonresident_ttl = nonresident_ttl
        self.failure_ttl = failure_ttl
        self.resident_tags = frozenset(resident_tags)
        # Lookups run inside the REST poll loop; a slow governance read must
        # not stall the batch (or the liveness heartbeat) for call_tool's 30s.
        self.timeout = timeout
        self.max_entries = max_entries
        # agent_id -> (expires_at monotonic, tags or None for a failed lookup)
        self._cache: OrderedDict[str, tuple[float, frozenset[str] | None]] = OrderedDict()

    async def tags(self, agent_id: str | None) -> frozenset[str] | None:
        if not agent_id:
            return None
        key = str(agent_id)
        now = time.monotonic()
        hit = self._cache.get(key)
        if hit is not None and now < hit[0]:
            self._cache.move_to_end(key)
            return hit[1]
        tags = await self._fetch(key)
        if tags is None:
            life = self.failure_ttl
        elif tags & self.resident_tags:
            life = self.ttl
        else:
            life = self.nonresident_ttl
        self._cache[key] = (now + life, tags)
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_entries:
            self._cache.popitem(last=False)
        return tags

    async def _fetch(self, agent_id: str) -> frozenset[str] | None:
        try:
            result = await asyncio.wait_for(
                self.gov.call_tool(
                    "agent", {"action": "get", "agent_id": agent_id, "lite": True},
                    track_failures=False,
                ),
                timeout=self.timeout,
            )
            if result is None:
                return None
            data = parse_tool_result(result)
        except Exception as exc:  # noqa: BLE001 - lookup is best-effort
            log.debug("agent tag lookup failed for %s: %s", agent_id, exc)
            return None
        if not isinstance(data, dict) or data.get("success") is False:
            return None
        # `get` defaults to the caller when the target is missing; never let
        # the bridge's own record stand in for the event's subject. A redacted
        # id (non-operator caller) cannot be compared and is accepted.
        returned = data.get("agent_uuid") or (
            None if data.get("agent_id_redacted") else data.get("agent_id")
        )
        if returned and str(returned) != agent_id:
            log.debug("agent tag lookup for %s answered for %s; ignoring", agent_id, returned)
            return None
        tags = data.get("tags")
        if not isinstance(tags, list):
            return None
        return frozenset(str(t).strip().lower() for t in tags if str(t).strip())


def has_any(tags: frozenset[str] | None, wanted: frozenset[str]) -> bool | None:
    """True/False when tags are known, ``None`` when they are not."""
    if tags is None:
        return None
    return bool(tags & wanted)


def is_positively_nonresident(
    tags: frozenset[str] | None,
    *,
    nonresident_tags: frozenset[str],
    resident_tags: frozenset[str],
) -> bool:
    """Only a positive non-resident answer counts: unknown or empty tags do not."""
    if not tags:
        return False
    return bool(tags & nonresident_tags) and not (tags & resident_tags)
