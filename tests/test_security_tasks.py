"""Tests for SSRF and path traversal protections in built-in tasks."""

import os
import tempfile
from unittest.mock import patch, MagicMock

import pytest

from water.tasks.http import _validate_url, http_request
from water.tasks.notify import webhook_task
from water.tasks.io import file_read, file_write


# ---------------------------------------------------------------------------
# SSRF: blocked private IPs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ip",
    ["127.0.0.1", "10.0.0.0", "169.254.169.254", "192.168.1.1"],
)
def test_validate_url_blocks_private_ips(ip):
    url = f"http://{ip}/secret"
    with pytest.raises(ValueError, match="blocked IP address"):
        _validate_url(url)


# ---------------------------------------------------------------------------
# SSRF: blocked schemes
# ---------------------------------------------------------------------------


def test_validate_url_blocks_file_scheme():
    with pytest.raises(ValueError, match="not allowed"):
        _validate_url("file:///etc/passwd")


def test_validate_url_blocks_ftp_scheme():
    with pytest.raises(ValueError, match="not allowed"):
        _validate_url("ftp://example.com/file")


# ---------------------------------------------------------------------------
# SSRF: allowed public URLs (mock DNS resolution)
# ---------------------------------------------------------------------------


def _fake_getaddrinfo_public(host, port, *args, **kwargs):
    """Return a fake public IP for any hostname."""
    return [
        (2, 1, 6, "", ("93.184.216.34", 0)),  # example.com-like public IP
    ]


@patch("water.tasks.http.socket.getaddrinfo", side_effect=_fake_getaddrinfo_public)
def test_validate_url_allows_public_url(mock_gai):
    # Should not raise
    _validate_url("https://example.com/api")


@patch("water.tasks.http.socket.getaddrinfo", side_effect=_fake_getaddrinfo_public)
@patch("water.tasks.http.urllib.request.urlopen")
def test_http_request_calls_validate_and_succeeds(mock_urlopen, mock_gai):
    """http_request task should validate then make the request."""
    mock_resp = MagicMock()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    mock_resp.read.return_value = b'{"ok": true}'
    mock_resp.headers = {}
    mock_resp.status = 200
    mock_urlopen.return_value = mock_resp

    task = http_request(id="t1", url="https://example.com/api")
    result = task.execute({}, None)
    assert result["status_code"] == 200
    mock_gai.assert_called_once()


# ---------------------------------------------------------------------------
# SSRF: allow_private_ips=True permits internal URLs
# ---------------------------------------------------------------------------


def _fake_getaddrinfo_private(host, port, *args, **kwargs):
    return [(2, 1, 6, "", ("10.0.0.5", 0))]


@patch("water.tasks.http.socket.getaddrinfo", side_effect=_fake_getaddrinfo_private)
def test_validate_url_allows_private_when_opted_in(mock_gai):
    # Should not raise when allow_private_ips=True
    _validate_url("http://internal-service.local/health", allow_private_ips=True)


@patch("water.tasks.http.socket.getaddrinfo", side_effect=_fake_getaddrinfo_private)
def test_validate_url_blocks_private_when_not_opted_in(mock_gai):
    with pytest.raises(ValueError, match="blocked IP address"):
        _validate_url("http://internal-service.local/health", allow_private_ips=False)


# ---------------------------------------------------------------------------
# SSRF: webhook_task also validates
# ---------------------------------------------------------------------------


def test_webhook_task_blocks_private_ip():
    task = webhook_task(id="w1", url="http://127.0.0.1/hook")
    with pytest.raises(ValueError, match="blocked IP address"):
        task.execute({}, None)


# ---------------------------------------------------------------------------
# Path traversal: blocked when allowed_base_dir is set
# ---------------------------------------------------------------------------


def test_path_traversal_relative_blocked():
    with tempfile.TemporaryDirectory() as base:
        task = file_read(id="r1", allowed_base_dir=base)
        with pytest.raises(ValueError, match="Path traversal detected"):
            task.execute({"path": os.path.join(base, "../../etc/passwd")}, None)


def test_path_traversal_absolute_outside_blocked():
    with tempfile.TemporaryDirectory() as base:
        task = file_write(id="w1", allowed_base_dir=base)
        with pytest.raises(ValueError, match="Path traversal detected"):
            task.execute({"path": "/etc/passwd", "content": "bad"}, None)


# ---------------------------------------------------------------------------
# Path traversal: valid path within base dir allowed
# ---------------------------------------------------------------------------


def test_valid_path_within_base_dir():
    with tempfile.TemporaryDirectory() as base:
        # Create a file inside the base dir
        target = os.path.join(base, "data.txt")
        with open(target, "w") as f:
            f.write("hello")

        task = file_read(id="r1", allowed_base_dir=base)
        result = task.execute({"path": target}, None)
        assert result["success"] is True
        assert result["content"] == "hello"


def test_valid_write_within_base_dir():
    with tempfile.TemporaryDirectory() as base:
        target = os.path.join(base, "out.txt")

        task = file_write(id="w1", allowed_base_dir=base)
        result = task.execute({"path": target, "content": "ok"}, None)
        assert result["success"] is True
        assert open(target).read() == "ok"
