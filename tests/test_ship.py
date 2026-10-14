"""Test supported ship delivery arguments with isolated, fail-closed CLI stubs.

The removed --split-preview interface is not restored. These fixtures do not
authorize live use of the legacy helper: agents still use App/prepr delivery.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


@pytest.fixture
def ship(tmp_path):
    script = tmp_path / "fixture/scripts/dev/ship.sh"
    script.parent.mkdir(parents=True)
    shutil.copyfile(Path(__file__).resolve().parents[1] / "scripts/dev/ship.sh", script)
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    log = tmp_path / "calls.jsonl"
    stub = '''import json, os, sys
from pathlib import Path
name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["SHIP_TEST_LOG"], "a") as out:
    out.write(json.dumps([name, *args]) + "\\n")
if name == "git":
    if args == ["diff", "--cached", "--name-only"]:
        print(os.environ["SHIP_TEST_FILES"])
    elif args == ["branch", "--show-current"]:
        print(os.environ["SHIP_TEST_BRANCH"])
    elif args == ["rev-parse", "--short", "HEAD"]:
        print("123abcd")
    elif args[:2] in (["checkout", "-b"], ["commit", "-m"], ["push", "-u"]):
        pass
    else:
        sys.exit("unexpected git invocation: " + repr(args))
elif name == "gh":
    if args == ["pr", "view", "--json", "url", "--jq", ".url"]:
        sys.exit(1)
    elif args[:2] == ["pr", "create"]:
        print("https://example.invalid/pull/1")
    else:
        sys.exit("unexpected gh invocation: " + repr(args))
else:
    sys.exit("unexpected command: " + name)
'''
    for name in ("git", "gh"):
        path = stub_dir / name
        path.write_text(f"#!{sys.executable}\n" + stub)
        path.chmod(0o755)

    def run(message="test delivery", *, files="tests/example.py", branch="codex/test", flags=()):
        # No inherited credentials, Git config/hooks, or delivery overrides.
        env = {
            "PATH": str(stub_dir) + os.pathsep + os.defpath,
            "HOME": str(tmp_path),
            "UNITARES_SHIP_AGENT": "codex",
            "SHIP_TEST_LOG": str(log),
            "SHIP_TEST_FILES": files,
            "SHIP_TEST_BRANCH": branch,
        }
        result = subprocess.run(
            ["/bin/bash", str(script), *flags, message],
            env=env, capture_output=True, text=True, timeout=15,
        )
        calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return result, calls

    return run


@pytest.mark.parametrize("message", [
    "just a one-liner",
    "fix(foo): subject line\n\nParagraph one.\n\nParagraph two.",
    "subject\nbody immediately after",
])
def test_commit_message_and_draft_pr_title(ship, message):
    result, calls = ship(message)
    assert result.returncode == 0, result.stderr
    assert ["git", "commit", "-m", message] in calls
    assert ["git", "push", "-u", "origin", "codex/test"] in calls
    creates = [call for call in calls if call[:3] == ["gh", "pr", "create"]]
    assert creates == [[
        "gh", "pr", "create", "--draft", "--title", message.splitlines()[0],
        "--body", "Auto-shipped by ship.sh as a draft PR. Local work is visible; "
        "mark ready after validation/review.",
    ]]
    assert not any(call[:3] == ["gh", "pr", "merge"] for call in calls)
    assert not any(call[:2] == ["git", "checkout"] for call in calls)


@pytest.mark.parametrize("files,branch", [
    ("src/bridge/bot.py", "codex/test"),
    ("tests/example.py", ""),
    ("docs/example.md", "main"),
])
def test_default_delivery_creates_fresh_branch_when_required(ship, files, branch):
    result, calls = ship(files=files, branch=branch)
    assert result.returncode == 0, result.stderr
    checkouts = [call for call in calls if call[:3] == ["git", "checkout", "-b"]]
    assert len(checkouts) == 1
    new_branch = checkouts[0][3]
    assert new_branch.startswith("codex/auto/")
    assert ["git", "push", "-u", "origin", new_branch] in calls
    assert any(call[:4] == ["gh", "pr", "create", "--draft"] for call in calls)


def test_plan_does_not_deliver(ship):
    result, calls = ship(files="src/bridge/bot.py", flags=("--plan",))
    assert result.returncode == 0, result.stderr
    assert "delivery=draft_pr\n" in result.stdout
    assert "force_auto_branch=1\n" in result.stdout
    assert all(call[:2] in (["git", "diff"], ["git", "branch"], ["git", "rev-parse"])
               for call in calls)


@pytest.mark.parametrize("kwargs,error", [
    ({"files": ""}, "nothing staged"),
    ({"branch": "", "flags": ("--direct",)}, "detached HEAD cannot use direct delivery"),
    ({"flags": ("--split-preview",)}, "unknown option: --split-preview"),
])
def test_invalid_delivery_does_not_write(ship, kwargs, error):
    result, calls = ship(**kwargs)
    assert result.returncode == 2
    assert error in result.stderr
    assert not any(call[0] == "gh" or call[1] in {"commit", "push", "checkout"} for call in calls)
