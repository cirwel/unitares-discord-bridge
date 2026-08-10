"""Tests for the desired Discord channel structure."""

from bridge.server_setup import CHANNEL_STRUCTURE, build_channel_structure


def _governance(structure) -> dict:
    return structure["GOVERNANCE"]


def test_default_structure_has_no_resident_channels():
    # No keys configured → the shared #residents feed is the only findings
    # channel, exactly as before.
    structure = build_channel_structure()
    assert list(_governance(structure)) == list(CHANNEL_STRUCTURE["GOVERNANCE"])
    assert "VIOLATIONS" not in structure


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
