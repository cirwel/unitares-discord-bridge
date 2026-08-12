"""Tests for the desired Discord channel structure."""

import pytest

from bridge.server_setup import (
    CHANNEL_STRUCTURE,
    build_channel_structure,
    ensure_server_structure,
)


def _governance(structure) -> dict:
    return structure["GOVERNANCE"]


def test_default_structure_has_no_resident_channels():
    # No keys configured → the shared #residents feed is the only findings
    # channel, exactly as before.
    structure = build_channel_structure()
    assert list(_governance(structure)) == list(CHANNEL_STRUCTURE["GOVERNANCE"])
    assert "VIOLATIONS" not in structure


def test_lumen_structure_has_self_iteration_attention_channel():
    channel = CHANNEL_STRUCTURE["LUMEN"]["lumen-iterations"]
    assert channel["type"] == "text"
    assert "self-iteration" in channel["topic"]


def test_resident_channels_are_created_next_to_residents():
    structure = build_channel_structure(resident_channels=("sentinel", "doctor"))
    names = list(_governance(structure))
    assert names.index("sentinel") == names.index("residents") + 1
    assert names.index("doctor") == names.index("sentinel") + 1
    assert _governance(structure)["sentinel"]["type"] == "text"
    assert "Sentinel findings only" in _governance(structure)["sentinel"]["topic"]
    assert "Doctor findings only" in _governance(structure)["doctor"]["topic"]


def test_resident_channel_cannot_hijack_a_structural_channel():
    # A key colliding with an existing channel would otherwise rewrite the
    # topic of a core feed and steer findings into it.
    structure = build_channel_structure(resident_channels=("alerts", "doctor"))
    assert _governance(structure)["alerts"] == CHANNEL_STRUCTURE["GOVERNANCE"]["alerts"]
    assert "doctor" in _governance(structure)


def test_duplicate_resident_keys_collapse():
    structure = build_channel_structure(resident_channels=("doctor", "doctor"))
    assert list(_governance(structure)).count("doctor") == 1


def test_module_level_structure_is_not_mutated():
    build_channel_structure(
        taxonomy={"classes": [{"id": "INT", "status": "active", "name": "Integrity"}]},
        resident_channels=("sentinel",),
    )
    assert "sentinel" not in CHANNEL_STRUCTURE["GOVERNANCE"]
    assert "VIOLATIONS" not in CHANNEL_STRUCTURE


def test_violations_category_still_built_alongside_resident_channels():
    structure = build_channel_structure(
        taxonomy={"classes": [{"id": "INT", "status": "active", "name": "Integrity"}]},
        resident_channels=("sentinel",),
    )
    assert "gov-int" in structure["VIOLATIONS"]
    assert "sentinel" in _governance(structure)


# ---------------------------------------------------------------------------
# Topic reconciliation against a fake guild
# ---------------------------------------------------------------------------


class FakeChannel:
    def __init__(self, name: str, topic: str = "", fail_edit: bool = False):
        self.name = name
        self.topic = topic
        self.edits: list[dict] = []
        self._fail_edit = fail_edit

    async def edit(self, **kwargs):
        if self._fail_edit:
            # Stand-in for discord.Forbidden — the bot lacks Manage Channels.
            raise RuntimeError("Missing Permissions")
        self.edits.append(kwargs)
        if "topic" in kwargs:
            self.topic = kwargs["topic"]


class FakeCategory:
    def __init__(self, name: str, channels=()):
        self.name = name
        self.channels = list(channels)


class FakeRole:
    def __init__(self, name: str):
        self.name = name


class FakeGuild:
    def __init__(self, categories=()):
        self.categories = list(categories)
        self.roles = [FakeRole(n) for n in ("Governance Admin", "observer", "lumen")]

    async def create_role(self, name, colour=None):
        role = FakeRole(name)
        self.roles.append(role)
        return role

    async def create_category(self, name):
        category = FakeCategory(name)
        self.categories.append(category)
        return category

    async def create_text_channel(self, name, category=None, topic=""):
        channel = FakeChannel(name, topic)
        category.channels.append(channel)
        return channel

    async def create_forum(self, name, category=None, topic=""):
        return await self.create_text_channel(name, category=category, topic=topic)


def _guild_with_residents(topic: str, **kwargs) -> tuple[FakeGuild, FakeChannel]:
    residents = FakeChannel("residents", topic, **kwargs)
    guild = FakeGuild([FakeCategory("GOVERNANCE", [residents])])
    return guild, residents


DESIRED_RESIDENTS_TOPIC = CHANNEL_STRUCTURE["GOVERNANCE"]["residents"]["topic"]


@pytest.mark.asyncio
async def test_stale_topic_is_rewritten_on_startup():
    # The pre-split wording: #residents still advertising Sentinel's findings
    # after Sentinel moved to its own channel.
    guild, residents = _guild_with_residents("Sentinel / Vigil / Watcher findings")
    await ensure_server_structure(guild)
    assert residents.topic == DESIRED_RESIDENTS_TOPIC
    assert residents.edits == [{"topic": DESIRED_RESIDENTS_TOPIC}]


@pytest.mark.asyncio
async def test_matching_topic_is_left_alone():
    # No pointless API call (and no audit-log churn) when nothing drifted.
    guild, residents = _guild_with_residents(DESIRED_RESIDENTS_TOPIC)
    await ensure_server_structure(guild)
    assert residents.edits == []


@pytest.mark.asyncio
async def test_sync_can_be_turned_off():
    guild, residents = _guild_with_residents("hand-written topic")
    await ensure_server_structure(guild, sync_topics=False)
    assert residents.topic == "hand-written topic"
    assert residents.edits == []


@pytest.mark.asyncio
async def test_failed_topic_edit_does_not_abort_startup():
    # Missing Manage Channels must cost a log line, not the whole bridge —
    # every other channel still has to be created and mapped.
    guild, _residents = _guild_with_residents("stale", fail_edit=True)
    channels = await ensure_server_structure(
        guild, resident_channels=("sentinel", "doctor"),
    )
    assert "residents" in channels
    assert "sentinel" in channels
    assert channels["governance-hud"].topic  # later channels still created


@pytest.mark.asyncio
async def test_freshly_created_channels_get_their_declared_topic():
    channels = await ensure_server_structure(
        FakeGuild(), resident_channels=("sentinel",),
    )
    assert channels["residents"].topic == DESIRED_RESIDENTS_TOPIC
    assert "Sentinel findings only" in channels["sentinel"].topic
    # Created with the right topic, so no follow-up edit.
    assert channels["residents"].edits == []
