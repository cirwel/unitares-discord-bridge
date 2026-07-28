import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from bridge.mcp_client import GovernanceClient, AnimaClient


# --- helpers ---

_SENTINEL = object()


def make_mock_response(status_code=200, json_data=_SENTINEL, content=b""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {} if json_data is _SENTINEL else json_data
    resp.content = content
    resp.raise_for_status = MagicMock()
    return resp


def mock_httpx_client(method="get", response=None):
    """Returns a patch context for httpx.AsyncClient.

    The mock client is returned directly (no context manager) since
    the MCP clients use _get_client() fallback, not `async with`.
    """
    mock_client_instance = AsyncMock()
    getattr(mock_client_instance, method).return_value = response
    mock_client_instance.aclose = AsyncMock()
    mock_client = MagicMock(return_value=mock_client_instance)
    return patch("bridge.mcp_client.httpx.AsyncClient", mock_client)


def mock_httpx_client_error(method="get", exc=None):
    """Returns a patch for httpx.AsyncClient where the given method raises."""
    mock_client_instance = AsyncMock()
    getattr(mock_client_instance, method).side_effect = exc or Exception("error")
    mock_client_instance.aclose = AsyncMock()
    mock_client = MagicMock(return_value=mock_client_instance)
    return patch("bridge.mcp_client.httpx.AsyncClient", mock_client)


# --- GovernanceClient tests ---

@pytest.mark.asyncio
async def test_governance_fetch_events():
    events = [{"id": 1, "type": "identity.created"}, {"id": 2, "type": "dialectic.started"}]
    resp = make_mock_response(json_data=events)

    with mock_httpx_client("get", resp):
        client = GovernanceClient("http://localhost:8767")
        result = await client.fetch_events(since=0, limit=50)

    assert result == events
    assert client.consecutive_failures == 0


@pytest.mark.asyncio
async def test_governance_fetch_events_wrapped():
    """The real /api/events endpoint wraps events in {success, events, count}."""
    events = [{"event_id": 1, "type": "agent_new"}, {"event_id": 2, "type": "risk_threshold"}]
    wrapped = {"success": True, "events": events, "count": 2}
    resp = make_mock_response(json_data=wrapped)

    with mock_httpx_client("get", resp):
        client = GovernanceClient("http://localhost:8767")
        result = await client.fetch_events(since=0, limit=50)

    assert result == events
    assert client.consecutive_failures == 0


@pytest.mark.asyncio
async def test_governance_fetch_events_server_down():
    """Failure must be None, not [] — the poller's stale-cursor reset relies
    on distinguishing an errored fetch from a genuinely empty feed."""
    with mock_httpx_client_error("get", Exception("Connection refused")):
        client = GovernanceClient("http://localhost:8767")
        result = await client.fetch_events()

    assert result is None
    assert client.consecutive_failures == 1


@pytest.mark.asyncio
async def test_governance_consecutive_failures_reset():
    """After a failure, a successful call resets consecutive_failures to 0."""
    with mock_httpx_client_error("get", Exception("timeout")):
        client = GovernanceClient("http://localhost:8767")
        await client.fetch_events()

    assert client.consecutive_failures == 1

    resp = make_mock_response(json_data=[])
    with mock_httpx_client("get", resp):
        result = await client.fetch_events()

    assert result == []
    assert client.consecutive_failures == 0


@pytest.mark.asyncio
async def test_governance_fetch_health():
    health = {"status": "ok", "uptime": 12345}
    resp = make_mock_response(json_data=health)

    with mock_httpx_client("get", resp):
        client = GovernanceClient("http://localhost:8767")
        result = await client.fetch_health()

    assert result == health


@pytest.mark.asyncio
async def test_governance_fetch_health_error():
    with mock_httpx_client_error("get", Exception("down")):
        client = GovernanceClient("http://localhost:8767")
        result = await client.fetch_health()

    assert result is None
    assert client.consecutive_failures == 1


@pytest.mark.asyncio
async def test_governance_call_tool():
    tool_result = {"agent_id": "abc-123", "status": "created"}
    resp = make_mock_response(json_data=tool_result)

    with mock_httpx_client("post", resp):
        client = GovernanceClient("http://localhost:8767")
        result = await client.call_tool("onboard", {"name": "test-agent"})

    assert result == tool_result
    assert client.consecutive_failures == 0


@pytest.mark.asyncio
async def test_governance_call_tool_error():
    with mock_httpx_client_error("post", Exception("500 error")):
        client = GovernanceClient("http://localhost:8767")
        result = await client.call_tool("onboard", {})

    assert result is None
    assert client.consecutive_failures == 1


# --- AnimaClient tests ---

@pytest.mark.asyncio
async def test_anima_fetch_state():
    state = {"warmth": 0.7, "clarity": 0.8, "stability": 0.9, "presence": 0.6}
    resp = make_mock_response(json_data=state)

    with mock_httpx_client("get", resp):
        client = AnimaClient("http://localhost:8766")
        result = await client.fetch_state()

    assert result == state
    assert client.is_online is True


@pytest.mark.asyncio
async def test_anima_fetch_state_offline():
    with mock_httpx_client_error("get", Exception("Connection refused")):
        client = AnimaClient("http://localhost:8766")
        result = await client.fetch_state()

    assert result is None
    assert client.is_online is False


@pytest.mark.asyncio
async def test_anima_fetch_gallery():
    drawings = [{"filename": "art_001.png", "timestamp": 1708700000}]
    gallery_response = {"drawings": drawings, "total": 1, "offset": 0, "limit": 5}
    resp = make_mock_response(json_data=gallery_response)

    with mock_httpx_client("get", resp):
        client = AnimaClient("http://localhost:8766")
        result = await client.fetch_gallery(limit=5)

    assert result == drawings


@pytest.mark.asyncio
async def test_anima_fetch_gallery_error():
    with mock_httpx_client_error("get", Exception("timeout")):
        client = AnimaClient("http://localhost:8766")
        result = await client.fetch_gallery()

    assert result is None


@pytest.mark.asyncio
async def test_anima_fetch_drawing_image():
    image_bytes = b"\x89PNG\r\n\x1a\nfake_image_data"
    resp = make_mock_response(content=image_bytes)

    with mock_httpx_client("get", resp):
        client = AnimaClient("http://localhost:8766")
        result = await client.fetch_drawing_image("art_001.png")

    assert result == image_bytes


@pytest.mark.asyncio
async def test_anima_fetch_drawing_image_error():
    with mock_httpx_client_error("get", Exception("404")):
        client = AnimaClient("http://localhost:8766")
        result = await client.fetch_drawing_image("nonexistent.png")

    assert result is None


# --- Lifecycle tests ---

@pytest.mark.asyncio
async def test_governance_open_close():
    """open() creates a persistent client, close() tears it down."""
    with patch("bridge.mcp_client.httpx.AsyncClient") as mock_cls:
        mock_instance = AsyncMock()
        mock_cls.return_value = mock_instance
        client = GovernanceClient("http://localhost:8767")

        await client.open()
        assert client._client is mock_instance

        await client.close()
        mock_instance.aclose.assert_awaited_once()
        assert client._client is None


@pytest.mark.asyncio
async def test_anima_open_close():
    """open() creates a persistent client, close() tears it down."""
    with patch("bridge.mcp_client.httpx.AsyncClient") as mock_cls:
        mock_instance = AsyncMock()
        mock_cls.return_value = mock_instance
        client = AnimaClient("http://localhost:8766")

        await client.open()
        assert client._client is mock_instance

        await client.close()
        mock_instance.aclose.assert_awaited_once()
        assert client._client is None


@pytest.mark.asyncio
async def test_fallback_client_closed_after_use():
    """When open() not called, each request creates and closes a one-shot client."""
    resp = make_mock_response(json_data=[])
    with mock_httpx_client("get", resp) as mock_cls:
        client = GovernanceClient("http://localhost:8767")
        await client.fetch_events()
        # The fallback client should have been closed
        mock_cls.return_value.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_governance_record_bridge_event_posts_receipt():
    resp = make_mock_response(json_data={"success": True})
    with mock_httpx_client("post", resp) as mock_cls:
        client = GovernanceClient("http://localhost:8767")
        client.agent_uuid = "bridge-uuid"
        result = await client.record_bridge_event({
            "event_type": "bridge.delivery",
            "source_event_id": 1,
        })

    assert result is True
    posted = mock_cls.return_value.post.await_args
    assert posted.args[0] == "/v1/bridge/events"
    assert posted.kwargs["json"]["bridge_id"] == "bridge-uuid"
    assert posted.kwargs["json"]["source_event_id"] == 1
    mock_cls.return_value.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_governance_record_bridge_event_is_best_effort():
    with mock_httpx_client_error("post", Exception("server old")):
        client = GovernanceClient("http://localhost:8767")
        result = await client.record_bridge_event({
            "event_type": "bridge.delivery",
            "source_event_id": 1,
        })

    assert result is False


# --- Token auth tests ---

@pytest.mark.asyncio
async def test_governance_token_passed_as_header():
    """When token is provided, httpx.AsyncClient gets Authorization header."""
    with patch("bridge.mcp_client.httpx.AsyncClient") as mock_cls:
        mock_instance = AsyncMock()
        mock_cls.return_value = mock_instance
        client = GovernanceClient("http://localhost:8767", token="my-secret-token")

        await client.open()
        mock_cls.assert_called_once_with(
            base_url="http://localhost:8767",
            timeout=10,
            headers={"Authorization": "Bearer my-secret-token"},
        )

        await client.close()


@pytest.mark.asyncio
async def test_anima_token_passed_as_header():
    """When token is provided, httpx.AsyncClient gets Authorization header."""
    with patch("bridge.mcp_client.httpx.AsyncClient") as mock_cls:
        mock_instance = AsyncMock()
        mock_cls.return_value = mock_instance
        client = AnimaClient("http://localhost:8766", token="anima-secret")

        await client.open()
        mock_cls.assert_called_once_with(
            base_url="http://localhost:8766",
            timeout=10,
            headers={"Authorization": "Bearer anima-secret"},
        )

        await client.close()


@pytest.mark.asyncio
async def test_no_token_no_auth_header():
    """When no token is provided, no Authorization header is set."""
    with patch("bridge.mcp_client.httpx.AsyncClient") as mock_cls:
        mock_instance = AsyncMock()
        mock_cls.return_value = mock_instance
        client = GovernanceClient("http://localhost:8767")

        await client.open()
        mock_cls.assert_called_once_with(
            base_url="http://localhost:8767",
            timeout=10,
            headers={},
        )


# --- fetch_agents: missing/redacted id handling ---
#
# Background: governance's list_agents may omit the `id` field for non-operator
# callers (KG 2026-04-20T00:57:45 redaction work). Before this fix, the
# `or item.get("id", "")` fallback would emit an empty-string id, which then
# went into get_governance_metrics(agent_id="") downstream — silently
# returning empty metrics for every redacted row and showing a HUD with
# labels but no EISV state.

@pytest.mark.asyncio
async def test_fetch_agents_skips_rows_without_id():
    """Rows with no agent_id and no id are dropped entirely, not emitted as id=''."""
    from bridge.mcp_client import fetch_agents

    gov = AsyncMock()
    gov.call_tool.return_value = {
        "result": {
            "agents": [
                {"agent_id": "uuid-aaa", "label": "alpha"},
                {"label": "beta-redacted"},  # missing id — must be dropped
                {"id": "uuid-ccc", "label": "gamma"},
                {"agent_id": "", "label": "empty-string-id"},  # also dropped
                {"agent_id": None, "label": "none-id"},  # also dropped
            ],
        },
    }

    agents = await fetch_agents(gov)

    ids = [a["id"] for a in agents]
    assert "" not in ids, "empty-string id must never reach downstream callers"
    assert None not in ids
    assert ids == ["uuid-aaa", "uuid-ccc"]


@pytest.mark.asyncio
async def test_fetch_agents_keeps_rows_with_either_key():
    """`agent_id` (full mode) and `id` (lite mode) are both honored."""
    from bridge.mcp_client import fetch_agents

    gov = AsyncMock()
    gov.call_tool.return_value = {
        "result": {
            "agents": [
                {"id": "lite-uuid", "label": "lite"},
                {"agent_id": "full-uuid", "label": "full"},
            ],
        },
    }

    agents = await fetch_agents(gov)
    assert {a["id"] for a in agents} == {"lite-uuid", "full-uuid"}


@pytest.mark.asyncio
async def test_fetch_agents_returns_empty_when_all_redacted():
    """If every row is redacted, we return [] rather than emitting empty IDs."""
    from bridge.mcp_client import fetch_agents

    gov = AsyncMock()
    gov.call_tool.return_value = {
        "result": {
            "agents": [
                {"label": "a"},
                {"label": "b"},
                {"label": "c"},
            ],
        },
    }

    agents = await fetch_agents(gov)
    assert agents == []


@pytest.mark.asyncio
async def test_fetch_metrics_does_not_query_for_empty_id():
    """Defensive: if a caller hands fetch_metrics an empty id, we skip
    rather than firing get_governance_metrics(agent_id='').
    """
    from bridge.mcp_client import fetch_metrics

    gov = AsyncMock()
    gov.call_tool.return_value = {"result": {"E": 0.5, "I": 0.5, "S": 0.5, "V": 0.0}}

    metrics = await fetch_metrics(gov, [{"id": ""}, {"id": None}, {"id": "uuid-a"}])

    # Only one real call made — for "uuid-a".
    assert gov.call_tool.await_count == 1
    args, kwargs = gov.call_tool.await_args
    assert args[1]["agent_id"] == "uuid-a"
    assert "uuid-a" in metrics
    assert "" not in metrics


@pytest.mark.asyncio
async def test_fetch_metrics_skips_reserved_prefix_ids():
    """A redacted reserved-prefix id (e.g. Chronicler's "mcp_<date>_<suffix>")
    is rejected by get_governance_metrics; skip it instead of firing a
    guaranteed reserved_prefix failure every HUD cycle.
    """
    from bridge.mcp_client import fetch_metrics

    gov = AsyncMock()
    gov.call_tool.return_value = {"result": {"E": 0.5, "I": 0.5, "S": 0.5, "V": 0.0}}

    metrics = await fetch_metrics(
        gov,
        [
            {"id": "mcp_20260423_deb879b6"},
            {"id": "governance_resident"},
            {"id": "uuid-a"},
        ],
    )

    # Only the non-reserved id is queried.
    assert gov.call_tool.await_count == 1
    args, kwargs = gov.call_tool.await_args
    assert args[1]["agent_id"] == "uuid-a"
    assert "uuid-a" in metrics
    assert "mcp_20260423_deb879b6" not in metrics
    assert "governance_resident" not in metrics


# --- onboard / identity binding tests ---

def _onboard_response(uuid="uuid-new", session_id="agent-new-1"):
    return make_mock_response(json_data={
        "name": "onboard",
        "result": {"success": True, "uuid": uuid, "client_session_id": session_id},
    })


@pytest.mark.asyncio
async def test_onboard_mints_and_persists_identity(tmp_path):
    """onboard() sets agent_uuid/client_session_id and writes the sidecar."""
    import json as _json

    identity_path = str(tmp_path / "governance_identity.json")
    resp = _onboard_response(uuid="uuid-1", session_id="sess-1")

    with mock_httpx_client("post", resp):
        gov = GovernanceClient("http://localhost:8767")
        ok = await gov.onboard(identity_path)

    assert ok is True
    assert gov.agent_uuid == "uuid-1"
    assert gov.client_session_id == "sess-1"
    with open(identity_path) as f:
        assert _json.load(f) == {"agent_uuid": "uuid-1"}


@pytest.mark.asyncio
async def test_onboard_declares_lineage_from_sidecar(tmp_path):
    """A prior instance's UUID is declared as parent_agent_id."""
    import json as _json

    identity_path = str(tmp_path / "governance_identity.json")
    with open(identity_path, "w") as f:
        _json.dump({"agent_uuid": "uuid-prior"}, f)

    resp = _onboard_response(uuid="uuid-2")
    mock_client_instance = AsyncMock()
    mock_client_instance.post.return_value = resp
    mock_client_instance.aclose = AsyncMock()
    with patch("bridge.mcp_client.httpx.AsyncClient", MagicMock(return_value=mock_client_instance)):
        gov = GovernanceClient("http://localhost:8767")
        ok = await gov.onboard(identity_path)

    assert ok is True
    sent = mock_client_instance.post.call_args.kwargs["json"]
    assert sent["name"] == "onboard"
    assert sent["arguments"]["parent_agent_id"] == "uuid-prior"
    assert sent["arguments"]["force_new"] is True
    assert sent["arguments"]["spawn_reason"] == "new_session"
    # Sidecar now carries the new uuid for the NEXT instance.
    with open(identity_path) as f:
        assert _json.load(f)["agent_uuid"] == "uuid-2"


@pytest.mark.asyncio
async def test_onboard_fresh_omits_parent(tmp_path):
    """No sidecar file → no parent_agent_id (honest fresh mint)."""
    identity_path = str(tmp_path / "governance_identity.json")
    resp = _onboard_response()
    mock_client_instance = AsyncMock()
    mock_client_instance.post.return_value = resp
    mock_client_instance.aclose = AsyncMock()
    with patch("bridge.mcp_client.httpx.AsyncClient", MagicMock(return_value=mock_client_instance)):
        gov = GovernanceClient("http://localhost:8767")
        await gov.onboard(identity_path)

    sent = mock_client_instance.post.call_args.kwargs["json"]
    assert "parent_agent_id" not in sent["arguments"]


@pytest.mark.asyncio
async def test_onboard_failure_leaves_bridge_unbound(tmp_path):
    """A failed onboard is non-fatal: no identity set, no sidecar written."""
    import os as _os

    identity_path = str(tmp_path / "governance_identity.json")
    with mock_httpx_client_error("post", Exception("connection refused")):
        gov = GovernanceClient("http://localhost:8767")
        ok = await gov.onboard(identity_path)

    assert ok is False
    assert gov.agent_uuid is None
    assert gov.client_session_id is None
    assert not _os.path.exists(identity_path)


@pytest.mark.asyncio
async def test_call_tool_echoes_client_session_id():
    """After onboarding, every call_tool carries client_session_id."""
    resp = make_mock_response(json_data={"result": {}})
    mock_client_instance = AsyncMock()
    mock_client_instance.post.return_value = resp
    mock_client_instance.aclose = AsyncMock()
    with patch("bridge.mcp_client.httpx.AsyncClient", MagicMock(return_value=mock_client_instance)):
        gov = GovernanceClient("http://localhost:8767")
        gov.client_session_id = "sess-echo"
        await gov.call_tool("list_agents", {"lite": True})

    sent = mock_client_instance.post.call_args.kwargs["json"]
    assert sent["arguments"]["client_session_id"] == "sess-echo"
    assert sent["arguments"]["lite"] is True


@pytest.mark.asyncio
async def test_call_tool_does_not_override_explicit_session_id():
    """An explicitly passed client_session_id wins over the bound one."""
    resp = make_mock_response(json_data={"result": {}})
    mock_client_instance = AsyncMock()
    mock_client_instance.post.return_value = resp
    mock_client_instance.aclose = AsyncMock()
    with patch("bridge.mcp_client.httpx.AsyncClient", MagicMock(return_value=mock_client_instance)):
        gov = GovernanceClient("http://localhost:8767")
        gov.client_session_id = "sess-bound"
        await gov.call_tool("onboard", {"client_session_id": "sess-explicit"})

    sent = mock_client_instance.post.call_args.kwargs["json"]
    assert sent["arguments"]["client_session_id"] == "sess-explicit"


@pytest.mark.asyncio
async def test_call_tool_unbound_does_not_inject():
    """Before onboarding, arguments pass through untouched."""
    resp = make_mock_response(json_data={"result": {}})
    mock_client_instance = AsyncMock()
    mock_client_instance.post.return_value = resp
    mock_client_instance.aclose = AsyncMock()
    with patch("bridge.mcp_client.httpx.AsyncClient", MagicMock(return_value=mock_client_instance)):
        gov = GovernanceClient("http://localhost:8767")
        await gov.call_tool("list_agents", {"lite": True})

    sent = mock_client_instance.post.call_args.kwargs["json"]
    assert "client_session_id" not in sent["arguments"]


@pytest.mark.asyncio
async def test_anima_fetch_qa():
    payload = {
        "questions": [
            {"id": "a1", "question": "why am I alone?", "answered": False,
             "timestamp": 1708700000, "answer": None},
        ],
        "total": 1,
        "unanswered": 1,
    }
    resp = make_mock_response(json_data=payload)

    with mock_httpx_client("get", resp):
        client = AnimaClient("http://localhost:8766")
        result = await client.fetch_qa(limit=50)

    assert result == payload


@pytest.mark.asyncio
async def test_anima_fetch_qa_error_returns_none():
    with mock_httpx_client_error("get", Exception("Connection refused")):
        client = AnimaClient("http://localhost:8766")
        result = await client.fetch_qa()

    assert result is None
