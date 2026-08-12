"""Lumen self-iteration attention poller and Discord presentation.

The bridge is a downstream observer. It reads Anima's bounded attention
projection, de-duplicates stable attention identifiers, and records delivery
receipts. Neither a Discord post nor a reaction is an approval signature.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone

import discord

from bridge import config
from bridge.cache import BridgeCache
from bridge.mcp_client import AnimaClient, GovernanceClient
from bridge.tasks import cancel_tasks, create_logged_task

log = logging.getLogger(__name__)

ATTENTION_SCHEMA = "anima.self_iteration.attention.v1"
SEEN_CACHE_KEY = "lumen_self_iteration_seen_v1"
MAX_SEEN_DELIVERIES = 2_000
_ATTENTION_ID = re.compile(r"si-attn-[0-9a-f]{24}")
_PRIORITIES = {"critical", "high", "medium", "low"}


def _text(value: object, *, limit: int) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "—"
    return raw if len(raw) <= limit else raw[: limit - 1] + "…"


def normalize_attention_item(value: object) -> dict | None:
    """Validate and bound one server-derived attention record for Discord."""
    if not isinstance(value, dict):
        return None
    attention_id = value.get("attention_id")
    priority = value.get("priority")
    proposal_id = value.get("proposal_id")
    if (
        not isinstance(attention_id, str)
        or not _ATTENTION_ID.fullmatch(attention_id)
        or priority not in _PRIORITIES
        or not isinstance(proposal_id, str)
        or not proposal_id
        or len(proposal_id) > 100
        or value.get("acknowledgement_is_approval") is not False
        or value.get("authority_granted") is not False
    ):
        return None
    candidate_id = value.get("candidate_id")
    if candidate_id is not None and (
        not isinstance(candidate_id, str) or len(candidate_id) > 100
    ):
        return None
    target_paths = value.get("target_paths")
    if not isinstance(target_paths, list):
        target_paths = []
    claim_provenance = value.get("claim_provenance")
    if not isinstance(claim_provenance, dict):
        return None
    if claim_provenance.get("authority_granted") is not False:
        return None
    return {
        "attention_id": attention_id,
        "proposal_id": proposal_id,
        "candidate_id": candidate_id,
        "stage": _text(value.get("stage"), limit=80),
        "state": _text(value.get("state"), limit=120),
        "priority": priority,
        "active": value.get("active") is True,
        "summary": _text(value.get("summary"), limit=1_500),
        "next_action": _text(value.get("next_action"), limit=1_000),
        "required_role": _text(value.get("required_role"), limit=120),
        "occurred_at": _text(value.get("occurred_at"), limit=80),
        "target_paths": [
            _text(path, limit=200)
            for path in target_paths[:5]
            if isinstance(path, str) and path.strip()
        ],
        "claim_provenance": {
            "source_epistemic_status": _text(
                claim_provenance.get("source_epistemic_status"), limit=80
            ),
            "request_trust_classification": _text(
                claim_provenance.get("request_trust_classification"), limit=120
            ),
            "request_actor_authenticated": claim_provenance.get(
                "request_actor_authenticated"
            )
            is True,
            "claims_verified_by_request_provenance": claim_provenance.get(
                "claims_verified_by_request_provenance"
            )
            is True,
            "independent_verification_status": _text(
                claim_provenance.get("independent_verification_status"), limit=80
            ),
            "effective_weight": claim_provenance.get("effective_weight"),
            "authority_granted": False,
        },
        "acknowledgement_is_approval": False,
        "authority_granted": False,
    }


def build_iteration_embed(item: dict) -> discord.Embed:
    """Render a validated attention item without mentioning or pinging roles."""
    colours = {
        "critical": discord.Colour.dark_red(),
        "high": discord.Colour.orange(),
        "medium": discord.Colour.gold(),
        "low": discord.Colour.blue(),
    }
    embed = discord.Embed(
        title=f"Lumen self-iteration · {item['state'].replace('_', ' ')}",
        description=item["summary"],
        colour=colours[item["priority"]],
        timestamp=datetime.now(timezone.utc),
    )
    references = f"Proposal `{item['proposal_id']}`"
    if item.get("candidate_id"):
        references += f"\nCandidate `{item['candidate_id']}`"
    embed.add_field(name="Reference", value=references, inline=True)
    embed.add_field(
        name="State",
        value=f"{item['stage']} · {item['priority']}",
        inline=True,
    )
    embed.add_field(
        name="Required role",
        value=item["required_role"],
        inline=True,
    )
    provenance = item["claim_provenance"]
    weight = provenance["effective_weight"]
    weight_text = (
        str(weight)
        if isinstance(weight, (int, float)) and not isinstance(weight, bool)
        else "unknown"
    )
    embed.add_field(
        name="Proposal provenance",
        value=(
            f"Source label: {provenance['source_epistemic_status']}\n"
            f"Request trust: {provenance['request_trust_classification']}\n"
            "Request provenance verifies claim truth: "
            f"{'yes' if provenance['claims_verified_by_request_provenance'] else 'no'}\n"
            "Independent verification: "
            f"{provenance['independent_verification_status']} · weight {weight_text}"
        ),
        inline=False,
    )
    embed.add_field(name="Safe next action", value=item["next_action"], inline=False)
    if item["target_paths"]:
        embed.add_field(
            name="Targets",
            value="\n".join(f"`{path}`" for path in item["target_paths"]),
            inline=False,
        )
    terminal = "active" if item["active"] else "notification only"
    embed.set_footer(
        text=(
            f"{item['attention_id']} · {terminal} · "
            "✅ acknowledges delivery only; never approval"
        )
    )
    return embed


class LumenIterationPoller:
    """Poll stable attention records and route them to Discord exactly once."""

    def __init__(
        self,
        anima_client: AnimaClient,
        governance_client: GovernanceClient,
        cache: BridgeCache,
        iterations_channel: discord.TextChannel,
        signals_channel: discord.TextChannel | None,
        alerts_channel: discord.TextChannel | None,
        interval: int = 60,
    ) -> None:
        self.anima = anima_client
        self.governance = governance_client
        self.cache = cache
        self.iterations_channel = iterations_channel
        self.signals_channel = signals_channel
        self.alerts_channel = alerts_channel
        self.interval = max(5, interval)
        self._task: asyncio.Task | None = None
        self._seen_loaded = False
        self._seen: set[str] = set()
        self._seen_order: list[str] = []
        self._consecutive_failures = 0
        self._unavailable_announced = False

    async def start(self) -> None:
        self._task = create_logged_task(self._loop(), name="lumen-self-iteration")

    async def stop(self) -> None:
        await cancel_tasks(self._task)

    async def _loop(self) -> None:
        while True:
            try:
                await self._tick()
            except Exception as exc:
                log.error("Self-iteration poll loop error: %s", exc, exc_info=exc)
            await asyncio.sleep(self.interval)

    async def _load_seen(self) -> None:
        if self._seen_loaded:
            return
        self._seen_loaded = True
        try:
            raw = await self.cache.get_kv(SEEN_CACHE_KEY)
            values = json.loads(raw) if raw else []
            if not isinstance(values, list) or not all(
                isinstance(item, str) for item in values
            ):
                raise ValueError("seen-delivery cache is not a string list")
        except Exception as exc:
            log.warning("Self-iteration seen cache unreadable; starting empty: %s", exc)
            values = []
        self._seen_order = values[-MAX_SEEN_DELIVERIES:]
        self._seen = set(self._seen_order)

    async def _remember(self, delivery_id: str) -> None:
        if delivery_id in self._seen:
            return
        self._seen.add(delivery_id)
        self._seen_order.append(delivery_id)
        if len(self._seen_order) > MAX_SEEN_DELIVERIES:
            expired = self._seen_order.pop(0)
            self._seen.discard(expired)
        try:
            await self.cache.set_kv(SEEN_CACHE_KEY, json.dumps(self._seen_order))
        except Exception as exc:
            # Memory de-duplication still protects this process. A cache outage
            # may replay after restart, but must not kill the poll loop.
            log.warning("Could not persist self-iteration delivery cursor: %s", exc)

    @staticmethod
    def _channel_key(channel: discord.TextChannel) -> str:
        identifier = getattr(channel, "id", None)
        return str(identifier or getattr(channel, "name", "unknown"))

    def _targets(self, item: dict) -> list[discord.TextChannel]:
        channels: list[discord.TextChannel | None] = [self.iterations_channel]
        if item["priority"] in {"high", "critical"}:
            channels.append(self.signals_channel)
        if item["priority"] == "critical":
            channels.append(self.alerts_channel)
        unique: list[discord.TextChannel] = []
        seen: set[str] = set()
        for channel in channels:
            if channel is None:
                continue
            key = self._channel_key(channel)
            if key not in seen:
                seen.add(key)
                unique.append(channel)
        return unique

    async def _record_delivery(
        self,
        item: dict,
        channel: discord.TextChannel,
        message: discord.Message,
    ) -> None:
        try:
            await self.governance.record_bridge_event(
                {
                    "event_type": "bridge.delivery",
                    "source_event_id": item["attention_id"],
                    "source_event_type": "lumen_self_iteration_attention",
                    "source_agent_id": "lumen",
                    "source_severity": item["priority"],
                    "channel_key": getattr(channel, "name", None),
                    "discord_channel_id": self._channel_key(channel),
                    "discord_message_id": str(getattr(message, "id", "")),
                }
            )
        except Exception as exc:
            # Delivery remains useful if governance receipt storage is down.
            log.debug("Self-iteration delivery receipt failed: %s", exc)

    async def _announce_unavailable(self) -> None:
        if self.alerts_channel is None or self._unavailable_announced:
            return
        embed = discord.Embed(
            title="Lumen self-iteration attention unavailable",
            description=(
                "The Discord bridge could not read Anima's server-derived attention "
                "projection twice in succession. No approval state was changed."
            ),
            colour=discord.Colour.dark_red(),
            timestamp=datetime.now(timezone.utc),
        )
        try:
            await self.alerts_channel.send(embed=embed)
            self._unavailable_announced = True
        except discord.HTTPException as exc:
            log.warning("Failed to announce self-iteration outage: %s", exc)

    async def _announce_recovery(self) -> None:
        if not self._unavailable_announced:
            return
        embed = discord.Embed(
            title="Lumen self-iteration attention restored",
            description="The server-derived attention projection is readable again.",
            colour=discord.Colour.green(),
            timestamp=datetime.now(timezone.utc),
        )
        try:
            await self.iterations_channel.send(embed=embed)
        except discord.HTTPException as exc:
            log.warning("Failed to announce self-iteration recovery: %s", exc)
            return
        self._unavailable_announced = False

    async def _tick(self) -> None:
        await self._load_seen()
        projection = await self.anima.fetch_self_iteration_attention(limit=50)
        if (
            projection is None
            or projection.get("schema") != ATTENTION_SCHEMA
            or projection.get("acknowledgement_is_approval") is not False
            or projection.get("authority_granted") is not False
        ):
            self._consecutive_failures += 1
            if self._consecutive_failures >= 2:
                await self._announce_unavailable()
            return

        self._consecutive_failures = 0
        await self._announce_recovery()
        for raw_item in projection.get("items", []):
            item = normalize_attention_item(raw_item)
            if item is None:
                log.warning("Dropped malformed self-iteration attention record")
                continue
            embed = build_iteration_embed(item)
            for channel in self._targets(item):
                delivery_id = f"{item['attention_id']}:{self._channel_key(channel)}"
                if delivery_id in self._seen:
                    continue
                try:
                    message = await channel.send(embed=embed)
                except discord.HTTPException as exc:
                    log.warning(
                        "Self-iteration delivery failed for %s to %s: %s",
                        item["attention_id"],
                        getattr(channel, "name", "unknown"),
                        exc,
                    )
                    continue
                emoji = next(iter(sorted(config.ACK_EMOJI)), None)
                if emoji and hasattr(message, "add_reaction"):
                    try:
                        await message.add_reaction(emoji)
                    except discord.HTTPException:
                        log.debug(
                            "Could not add self-iteration acknowledgement reaction"
                        )
                await self._record_delivery(item, channel, message)
                await self._remember(delivery_id)
