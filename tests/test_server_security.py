import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from water.server.app import FlowServer


def _make_mock_flow(flow_id="test-flow"):
    """Create a minimal mock Flow that passes FlowServer validation."""
    flow = MagicMock()
    flow.id = flow_id
    flow._registered = True
    flow._tasks = []
    flow.description = "A test flow"
    flow.metadata = {}
    flow.run = AsyncMock(return_value={"output": "ok"})
    return flow


def _make_app(**kwargs):
    """Build a TestClient for FlowServer with given kwargs."""
    flow = _make_mock_flow()
    server = FlowServer(flows=[flow], **kwargs)
    return TestClient(server.get_app())


# ── CORS tests ──────────────────────────────────────────────────────


def test_default_cors_is_restrictive():
    """Default CORS should not include wildcard origins."""
    client = _make_app()
    resp = client.options(
        "/health",
        headers={"Origin": "http://evil.com", "Access-Control-Request-Method": "GET"},
    )
    # With no allowed origins, the response must NOT echo back the origin
    assert resp.headers.get("access-control-allow-origin") is None


def test_custom_cors_origins_applied():
    """Explicitly configured origins should be reflected in CORS headers."""
    client = _make_app(allow_origins=["http://myapp.com"])
    resp = client.options(
        "/health",
        headers={
            "Origin": "http://myapp.com",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert resp.headers.get("access-control-allow-origin") == "http://myapp.com"


def test_wildcard_cors_when_explicitly_set():
    """Users can opt into wildcard CORS if they choose."""
    client = _make_app(allow_origins=["*"])
    resp = client.options(
        "/health",
        headers={"Origin": "http://anything.com", "Access-Control-Request-Method": "GET"},
    )
    assert resp.headers.get("access-control-allow-origin") == "*"


# ── API key auth tests ──────────────────────────────────────────────


def test_auth_rejects_missing_key():
    """Requests without credentials should receive 401."""
    client = _make_app(api_key="secret-key")
    resp = client.get("/flows")
    assert resp.status_code == 401
    assert resp.json() == {"detail": "Unauthorized"}


def test_auth_rejects_wrong_key():
    """Requests with an incorrect key should receive 401."""
    client = _make_app(api_key="secret-key")
    resp = client.get("/flows", headers={"Authorization": "Bearer wrong-key"})
    assert resp.status_code == 401


def test_auth_accepts_correct_bearer():
    """Correct Bearer token should be accepted."""
    client = _make_app(api_key="secret-key")
    resp = client.get("/flows", headers={"Authorization": "Bearer secret-key"})
    assert resp.status_code == 200


def test_auth_accepts_correct_x_api_key():
    """Correct X-API-Key header should be accepted."""
    client = _make_app(api_key="secret-key")
    resp = client.get("/flows", headers={"X-API-Key": "secret-key"})
    assert resp.status_code == 200


def test_no_auth_when_api_key_none():
    """When api_key is None, all requests should pass through without auth."""
    client = _make_app()  # api_key defaults to None
    resp = client.get("/flows")
    assert resp.status_code == 200


def test_health_excluded_from_auth():
    """Health check endpoint should be accessible without auth."""
    client = _make_app(api_key="secret-key")
    resp = client.get("/health")
    assert resp.status_code == 200


# ── Error sanitization tests ────────────────────────────────────────


def test_error_response_no_stack_trace():
    """Error responses must not contain exception details or stack traces."""
    flow = _make_mock_flow()
    flow.run = AsyncMock(side_effect=RuntimeError("secret DB password in traceback"))
    server = FlowServer(flows=[flow])
    client = TestClient(server.get_app())

    resp = client.post(
        "/flows/test-flow/run",
        json={"input_data": {"key": "value"}},
    )
    assert resp.status_code == 500
    body = resp.json()
    detail = str(body.get("detail", ""))
    assert "secret DB password" not in detail
    assert "traceback" not in detail.lower()
    assert "Traceback" not in detail
    assert detail == "Flow execution failed"
