"""Tests for water/tasks/ built-in tasks — http, io, notify."""

import json
import os
import tempfile
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from water.tasks.http import http_request
from water.tasks.io import file_read, file_write
from water.tasks.notify import webhook_task


# ============================================================================
# Helpers
# ============================================================================

def _ctx():
    """Minimal mock execution context."""
    return MagicMock()


# ============================================================================
# HTTP task — water/tasks/http.py
# ============================================================================

class TestHttpRequest:

    def test_creates_task_with_correct_id(self):
        task = http_request(id="fetch", url="https://example.com")
        assert task.id == "fetch"

    def test_url_templating(self):
        task = http_request(id="t", url="https://api.example.com/users/{user_id}")
        # We'll mock urlopen to capture the request
        with patch("water.tasks.http.urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.read.return_value = b'{"ok":true}'
            mock_resp.headers = {}
            mock_resp.status = 200
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_open.return_value = mock_resp

            result = task.execute({"input_data": {"user_id": "42"}}, _ctx())
            assert result["status_code"] == 200

            # Verify the URL was templated
            call_args = mock_open.call_args
            req = call_args[0][0]
            assert "users/42" in req.full_url

    def test_no_url_returns_error(self):
        task = http_request(id="t", url="")
        result = task.execute({"input_data": {}}, _ctx())
        assert result["status_code"] == 0
        assert "error" in result

    def test_json_response_parsed(self):
        task = http_request(id="t", url="https://example.com")
        with patch("water.tasks.http.urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps({"key": "val"}).encode()
            mock_resp.headers = {"Content-Type": "application/json"}
            mock_resp.status = 200
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_open.return_value = mock_resp

            result = task.execute({"input_data": {}}, _ctx())
            assert result["json_data"] == {"key": "val"}

    def test_http_error_returns_status(self):
        import urllib.error
        task = http_request(id="t", url="https://example.com")
        with patch("water.tasks.http.urllib.request.urlopen") as mock_open:
            err = urllib.error.HTTPError(
                "https://example.com", 404, "Not Found", {}, None
            )
            # HTTPError.read() should return bytes
            err.read = lambda: b"not found"
            err.headers = {}
            mock_open.side_effect = err

            result = task.execute({"input_data": {}}, _ctx())
            assert result["status_code"] == 404

    def test_url_error_raises_connection_error(self):
        import urllib.error
        task = http_request(id="t", url="https://example.com")
        with patch("water.tasks.http.urllib.request.urlopen") as mock_open:
            mock_open.side_effect = urllib.error.URLError("DNS fail")
            with pytest.raises(ConnectionError, match="HTTP request failed"):
                task.execute({"input_data": {}}, _ctx())

    def test_header_templating(self):
        task = http_request(
            id="t",
            url="https://example.com",
            headers={"Authorization": "Bearer {token}"},
        )
        with patch("water.tasks.http.urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.read.return_value = b"{}"
            mock_resp.headers = {}
            mock_resp.status = 200
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_open.return_value = mock_resp

            task.execute({"input_data": {"token": "abc123"}}, _ctx())
            req = mock_open.call_args[0][0]
            assert req.get_header("Authorization") == "Bearer abc123"


# ============================================================================
# IO tasks — water/tasks/io.py
# ============================================================================

class TestFileRead:

    def test_read_existing_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("hello world")
            f.flush()
            path = f.name

        try:
            task = file_read(id="r", path=path)
            result = task.execute({"input_data": {}}, _ctx())
            assert result["success"] is True
            assert result["content"] == "hello world"
            assert result["path"] == path
        finally:
            os.unlink(path)

    def test_read_nonexistent_file(self):
        task = file_read(id="r", path="/tmp/does_not_exist_water_test_xyz.txt")
        result = task.execute({"input_data": {}}, _ctx())
        assert result["success"] is False
        assert "error" in result

    def test_read_with_path_templating(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("templated")
            f.flush()
            path = f.name

        try:
            # Use template that resolves to the temp file path
            task = file_read(id="r", path="{file_path}")
            result = task.execute({"input_data": {"file_path": path}}, _ctx())
            assert result["success"] is True
            assert result["content"] == "templated"
        finally:
            os.unlink(path)

    def test_read_json_parsing(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"key": "value"}, f)
            f.flush()
            path = f.name

        try:
            task = file_read(id="r", path=path, parse_json=True)
            result = task.execute({"input_data": {}}, _ctx())
            assert result["success"] is True
            assert result["json_data"] == {"key": "value"}
        finally:
            os.unlink(path)


class TestFileWrite:

    def test_write_new_file(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "out.txt")
            task = file_write(id="w", path=path, content="hello")
            result = task.execute({"input_data": {}}, _ctx())
            assert result["success"] is True
            assert os.path.exists(path)
            with open(path) as f:
                assert f.read() == "hello"

    def test_write_creates_parent_dirs(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "sub", "dir", "out.txt")
            task = file_write(id="w", path=path, content="nested")
            result = task.execute({"input_data": {}}, _ctx())
            assert result["success"] is True
            assert os.path.exists(path)

    def test_write_with_content_templating(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "tmpl.txt")
            task = file_write(id="w", path=path, content="Hello {name}!")
            result = task.execute({"input_data": {"name": "World"}}, _ctx())
            assert result["success"] is True
            assert result["content"] == "Hello World!"

    def test_write_with_path_templating(self):
        with tempfile.TemporaryDirectory() as td:
            task = file_write(id="w", path="{dir}/output.txt", content="data")
            result = task.execute({"input_data": {"dir": td}}, _ctx())
            assert result["success"] is True
            assert os.path.exists(os.path.join(td, "output.txt"))

    def test_write_from_input_data(self):
        """When path and content are empty, they come from input_data."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "from_data.txt")
            task = file_write(id="w")
            result = task.execute(
                {"input_data": {"path": path, "content": "from_data"}}, _ctx()
            )
            assert result["success"] is True
            with open(path) as f:
                assert f.read() == "from_data"


# ============================================================================
# Webhook / Notify task — water/tasks/notify.py
# ============================================================================

class TestWebhookTask:

    def test_creates_task_with_correct_id(self):
        task = webhook_task(id="wh", url="https://hooks.example.com")
        assert task.id == "wh"

    def test_no_url_returns_error(self):
        task = webhook_task(id="wh", url="")
        result = task.execute({"input_data": {}}, _ctx())
        assert result["success"] is False
        assert "error" in result

    def test_successful_post(self):
        task = webhook_task(id="wh", url="https://hooks.example.com/notify")
        with patch("water.tasks.notify.urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.status = 200
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_open.return_value = mock_resp

            result = task.execute({"input_data": {"msg": "hi"}}, _ctx())
            assert result["status_code"] == 200
            assert result["success"] is True

            # Verify it was a POST with JSON body
            req = mock_open.call_args[0][0]
            assert req.method == "POST"
            assert req.get_header("Content-type") == "application/json"

    def test_http_error_returns_failure(self):
        import urllib.error
        task = webhook_task(id="wh", url="https://hooks.example.com/fail")
        with patch("water.tasks.notify.urllib.request.urlopen") as mock_open:
            err = urllib.error.HTTPError(
                "https://hooks.example.com/fail", 500, "Server Error", {}, None
            )
            mock_open.side_effect = err
            result = task.execute({"input_data": {"msg": "hi"}}, _ctx())
            assert result["success"] is False
            assert result["status_code"] == 500

    def test_url_error_returns_failure(self):
        import urllib.error
        task = webhook_task(id="wh", url="https://hooks.example.com")
        with patch("water.tasks.notify.urllib.request.urlopen") as mock_open:
            mock_open.side_effect = urllib.error.URLError("connection refused")
            result = task.execute({"input_data": {}}, _ctx())
            assert result["success"] is False
            assert result["status_code"] == 0

    def test_url_templating(self):
        task = webhook_task(id="wh", url="https://hooks.example.com/{channel}")
        with patch("water.tasks.notify.urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.status = 200
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_open.return_value = mock_resp

            task.execute({"input_data": {"channel": "general"}}, _ctx())
            req = mock_open.call_args[0][0]
            assert "general" in req.full_url

    def test_custom_headers_merged(self):
        task = webhook_task(
            id="wh",
            url="https://hooks.example.com",
            headers={"X-Custom": "val"},
        )
        with patch("water.tasks.notify.urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.status = 200
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_open.return_value = mock_resp

            task.execute({"input_data": {}}, _ctx())
            req = mock_open.call_args[0][0]
            assert req.get_header("X-custom") == "val"
