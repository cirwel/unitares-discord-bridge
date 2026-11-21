"""Hand one kind of finding to a dispatch agent.

Every finding the bridge posts is an embed in a channel. That is a record, and
nothing acts on a record: in the 30 days to 2026-11-20 every Vigil and Sentinel
finding was delivered and none was acknowledged. The operator's direction that
day was to route alerts to agents and build no queue.

So for ONE finding class, Vigil's per-PR stalled-draft-PR finding, the bridge
also posts a plain message that @-mentions a dispatch bot in a channel where
that bot starts sessions. The dispatch bot must separately list this bridge's
bot id as allowed to start sessions (``DISPATCH_TRIGGER_BOT_IDS`` in
dispatch_beam); without that the message is just a second record.

Rules this module holds to:

* **Off unless fully configured.** Channel, mention and repository owners must
  all be set. Anything missing or malformed — including a malformed non-empty
  ``BRIDGE_DISPATCH_HANDOFF_VIGIL_AGENT_UUID`` — disables the handoff.
* **One class, named here.** Not configurable: widening what can start an
  agent run is a code change with a review, not an env var.
* **Fixed wording.** The message becomes an agent's prompt. It is built from
  validated identifiers only: the repository (owner allowlisted), the PR
  number, and the author signature (a UUID and a restricted model string).
  The finding's free-text ``message`` and its ``url`` are never copied in; the
  link is rebuilt from the repository and number.
* **One mention.** Exactly one user id, the configured one. ``@everyone``,
  ``@here`` and roles are pinned off.
* **Once per finding.** The post goes through the same per-destination
  delivery cache as every finding, so the REST and WebSocket copies of one
  finding produce one handoff, across restarts.

What this establishes about finding origin depends on
``BRIDGE_DISPATCH_HANDOFF_VIGIL_AGENT_UUID``:

* **When set (recommended once Vigil's scoped findings token is deployed):**
  the bridge requires governance's server-stamped provenance fields —
  ``provenance_verified is True`` (strict boolean) AND
  ``verified_actor_agent_uuid`` matching the configured Vigil agent UUID
  (case-insensitive). That proves the finding was posted to governance with a
  credential bound to that agent UUID rather than merely self-asserted via
  ``agent_id="vigil"`` by another holder of the shared HTTP token. Findings
  lacking verified provenance or attributed to a different verified actor are
  ignored for handoff (their ordinary embed still posts).
* **When unset or empty (backward-compatible default):** the bridge does NOT
  establish that the finding really came from Vigil. Governance's
  ``/api/findings`` takes ``type``, ``finding_type`` and ``agent_id`` from
  whoever holds its shared HTTP token, so any holder of that token can start a
  diagnose-only agent run against a PR of an allowlisted owner, with the fixed
  text below. That residual trust is why the text is fixed, the owners are
  allowlisted and the request stops at a diagnosis.

The volume is bounded upstream (Vigil posts once per stall episode) and
downstream (dispatch caps starts per trusted bot per hour).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import discord

from bridge.delivery import Outgoing

HANDOFF_EVENT_TYPE = "vigil_finding"
HANDOFF_FINDING_TYPE = "stalled_draft_pr"
HANDOFF_AGENT_ID = "vigil"

# Every pattern here is used with ``fullmatch``: ``$`` also matches before a
# trailing newline, which would let one through into the message. Digits are
# spelled ``[0-9]`` because ``\d`` and ``str.isdigit`` accept non-ASCII digits.
_CHANNEL_RE = re.compile(r"[0-9]{5,25}")
_MENTION_RE = re.compile(r"<@!?([0-9]{5,25})>")
_OWNER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}")
_REPO_RE = re.compile(r"([A-Za-z0-9][A-Za-z0-9-]{0,38})/([A-Za-z0-9_.-]{1,100})")
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,63}")


@dataclass(frozen=True)
class HandoffConfig:
    """A validated handoff configuration. Build with :func:`parse_config`."""

    channel_id: int
    mention: str
    user_id: int
    owners: frozenset[str]
    vigil_agent_uuid: str | None = None


def parse_config(
    channel_id: str | None,
    mention: str | None,
    owners: str | None,
    vigil_agent_uuid: str | None = None,
) -> HandoffConfig | None:
    """Validate the handoff settings; ``None`` (handoff off) unless all are good.

    ``mention`` must be a single user mention (``<@123...>``): not a role, not
    free text. ``owners`` is a comma-separated list of GitHub owners whose
    repositories may be handed off; there is no wildcard. ``vigil_agent_uuid``
    is optional: when non-empty it must be a canonical UUID (case-insensitive,
    normalized to lowercase); a malformed non-empty value disables handoff
    fail-closed.
    """
    raw_channel = (channel_id or "").strip()
    if _CHANNEL_RE.fullmatch(raw_channel) is None:
        return None
    match = _MENTION_RE.fullmatch((mention or "").strip())
    if match is None:
        return None
    entries = [o.strip() for o in (owners or "").split(",")]
    # One bad entry, an empty one included, disables the handoff: a list that
    # is partly wrong is a misconfiguration, not a narrower allowlist.
    if any(_OWNER_RE.fullmatch(o) is None for o in entries):
        return None
    normalized_vigil_uuid: str | None = None
    if vigil_agent_uuid is not None:
        if not isinstance(vigil_agent_uuid, str):
            return None
        raw_vigil_uuid = vigil_agent_uuid.strip()
        if raw_vigil_uuid:
            candidate = raw_vigil_uuid.lower()
            if _UUID_RE.fullmatch(candidate) is None:
                return None
            normalized_vigil_uuid = candidate
    user_id = int(match.group(1))
    return HandoffConfig(
        channel_id=int(raw_channel),
        mention=f"<@{user_id}>",
        user_id=user_id,
        owners=frozenset(o.lower() for o in entries),
        vigil_agent_uuid=normalized_vigil_uuid,
    )


def _provenance_ok(event: dict, config: HandoffConfig) -> bool:
    """True when ``event`` satisfies the configured Vigil provenance gate.

    Unset ``vigil_agent_uuid`` preserves backward-compatible behavior. When
    set, governance must have stamped ``provenance_verified`` as a strict
    boolean ``True`` and ``verified_actor_agent_uuid`` matching the configured
    UUID (case-insensitive).
    """
    if not config.vigil_agent_uuid:
        return True
    if event.get("provenance_verified") is not True:
        return False
    actor = event.get("verified_actor_agent_uuid")
    return isinstance(actor, str) and actor.lower() == config.vigil_agent_uuid.lower()


def _pr_identity(event: dict, owners: frozenset[str]) -> tuple[str, int] | None:
    repo = event.get("repo")
    number = event.get("number")
    if not isinstance(repo, str) or isinstance(number, bool) or not isinstance(number, int):
        return None
    match = _REPO_RE.fullmatch(repo)
    if match is None or number <= 0 or number > 10_000_000:
        return None
    if match.group(1).lower() not in owners or ".." in match.group(2):
        return None
    return repo, number


def handoff_text(event: Any, config: HandoffConfig | None) -> str | None:
    """The handoff message for ``event``, or ``None`` if it gets none.

    ``None`` for every event that is not exactly Vigil's per-PR stalled
    draft-PR finding with a well-formed, allowlisted repository and number
    (and, when ``config.vigil_agent_uuid`` is set, server-verified actor
    provenance matching that UUID).
    """
    if config is None or not isinstance(event, dict):
        return None
    if event.get("type") != HANDOFF_EVENT_TYPE:
        return None
    if event.get("finding_type") != HANDOFF_FINDING_TYPE:
        return None
    if event.get("agent_id") != HANDOFF_AGENT_ID:
        return None
    if not _provenance_ok(event, config):
        return None
    identity = _pr_identity(event, config.owners)
    if identity is None:
        return None
    repo, number = identity

    uuid = event.get("author_agent_uuid")
    model = event.get("author_model")
    if isinstance(uuid, str) and _UUID_RE.fullmatch(uuid):
        author = f"The PR body is signed by agent {uuid}"
        if isinstance(model, str) and _MODEL_RE.fullmatch(model):
            author += f" (model {model})"
        author += "."
    else:
        author = "The PR body carries no agent signature."

    return (
        f"{config.mention} Vigil reports a stalled draft PR: {repo}#{number} has "
        "failing CI that nothing has re-run or fixed for over half a day.\n"
        f"https://github.com/{repo}/pull/{number}\n"
        f"{author}\n"
        "Please find out why its CI is failing and what would fix it. Follow that "
        "repository's ownership rules: if the PR is not yours to change, do not "
        "push to its branch; leave the diagnosis as a comment on the PR. Do not "
        "merge it or mark it ready.\n"
        "(Sent by the Discord bridge. Only the repository, PR number and signature "
        "above come from the finding; the rest is fixed text.)"
    )


def handoff_outgoing(
    event: Any, channel: Any, config: HandoffConfig | None
) -> Outgoing | None:
    """A queueable handoff post for ``event``, or ``None``.

    ``channel`` is the resolved handoff channel; ``None`` (not found, or not a
    text channel) disables the handoff whatever the config says.
    """
    if channel is None or config is None or not isinstance(event, dict):
        return None
    # Only a finding the delivery cache can deduplicate is handed off. Without
    # an event id, a timestamp and a channel id the cache sends every copy, so
    # the REST and WebSocket copies would each start an agent run.
    if (
        event.get("event_id") is None
        or not event.get("timestamp")
        or not isinstance(getattr(channel, "id", None), int)
    ):
        return None
    text = handoff_text(event, config)
    if text is None:
        return None
    return Outgoing(
        channel,
        None,
        event,
        content=text,
        allowed_mentions=discord.AllowedMentions(
            everyone=False,
            roles=False,
            replied_user=False,
            users=[discord.Object(id=config.user_id)],
        ),
    )
