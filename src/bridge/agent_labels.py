"""TTL cache of agent id -> label, for naming subjects in event embeds.

Broadcaster events such as ``lifecycle_paused`` carry only an ``agent_id``, so
an embed built from them names the subject by a 12-character UUID fragment.
This cache resolves the label from the same ``agent(action="list")`` read the
event poller and HUD use (:func:`bridge.mcp_client.fetch_agents`), at most once
per TTL and never once per event. A lookup that finds the cache stale starts
one background refresh and waits for it only briefly, so a slow or unreachable
governance cannot stall the WebSocket reader.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, Optional

from bridge.mcp_client import GovernanceClient, fetch_agents

log = logging.getLogger(__name__)

FetchAgents = Callable[[GovernanceClient], Awaitable[list[dict]]]


class AgentLabelCache:
    """Agent labels from one ``fetch_agents`` call per ``ttl`` seconds."""

    def __init__(
        self,
        gov_client: GovernanceClient,
        *,
        ttl: float = 300.0,
        failure_ttl: float = 60.0,
        wait: float = 2.0,
        fetch: FetchAgents = fetch_agents,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.gov = gov_client
        self.ttl = ttl
        self.failure_ttl = failure_ttl
        self.wait = wait
        self._fetch = fetch
        self._clock = clock
        self._labels: dict[str, str] = {}
        self._expires = 0.0
        self._task: Optional[asyncio.Task] = None

    def _start_refresh_if_stale(self) -> Optional[asyncio.Task]:
        if self._task is not None and not self._task.done():
            return self._task
        if self._clock() < self._expires:
            return None
        self._task = asyncio.get_running_loop().create_task(
            self._refresh(), name="agent-label-refresh",
        )
        return self._task

    async def _refresh(self) -> None:
        try:
            agents = await self._fetch(self.gov)
        except Exception as exc:  # noqa: BLE001 - labels are cosmetic
            log.warning("agent label refresh failed: %s: %r", type(exc).__name__, exc)
            agents = []
        labels = {
            str(agent["id"]): str(agent["label"])
            for agent in agents or []
            if agent.get("id") and agent.get("label")
            and str(agent["label"]) != str(agent["id"])
        }
        if labels:
            self._labels = labels
            self._expires = self._clock() + self.ttl
        else:
            # Keep the last good map; retry sooner than a full TTL.
            self._expires = self._clock() + self.failure_ttl

    async def label_for(self, agent_id: object) -> Optional[str]:
        """The agent's label, or ``None`` when it is unknown."""
        if not agent_id:
            return None
        task = self._start_refresh_if_stale()
        if task is not None and self.wait > 0:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=self.wait)
            except asyncio.TimeoutError:
                pass
        return self._labels.get(str(agent_id))

    async def close(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
