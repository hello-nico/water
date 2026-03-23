"""
HTTP request task for Water.

Pre-built task for making HTTP requests with retry, auth, and response parsing.
"""

import ipaddress
import json
import socket
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Dict, Optional

from pydantic import BaseModel

from water.core.task import Task


def _validate_url(url: str, allow_private_ips: bool = False) -> None:
    """
    Validate a URL to prevent SSRF attacks.

    Checks that the scheme is http/https and that the resolved IP address
    is not in a private, reserved, loopback, or link-local range.

    Args:
        url: The URL to validate.
        allow_private_ips: If True, skip the private-IP check (for internal use).

    Raises:
        ValueError: If the URL is blocked.
    """
    parsed = urllib.parse.urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise ValueError(
            f"URL scheme '{parsed.scheme}' is not allowed. Only http and https are permitted."
        )

    hostname = parsed.hostname
    if not hostname:
        raise ValueError("URL has no hostname.")

    if not allow_private_ips:
        try:
            addrinfos = socket.getaddrinfo(hostname, None)
        except socket.gaierror as e:
            raise ValueError(f"Could not resolve hostname '{hostname}': {e}")

        for family, _type, _proto, _canonname, sockaddr in addrinfos:
            ip = ipaddress.ip_address(sockaddr[0])
            if ip.is_private or ip.is_reserved or ip.is_loopback or ip.is_link_local:
                raise ValueError(
                    f"URL resolves to blocked IP address {ip}. "
                    "Requests to private/reserved networks are not allowed."
                )


class HttpInput(BaseModel):
    url: str = ""
    method: str = "GET"
    headers: Dict[str, str] = {}
    body: Optional[str] = None


class HttpOutput(BaseModel):
    status_code: int = 0
    body: str = ""
    headers: Dict[str, str] = {}
    json_data: Optional[Dict[str, Any]] = None


def http_request(
    id: str,
    url: str = "",
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    body: Optional[str] = None,
    retry_count: int = 0,
    retry_delay: float = 1.0,
    timeout: Optional[float] = 30.0,
    description: Optional[str] = None,
    allow_private_ips: bool = False,
) -> Task:
    """
    Create an HTTP request task.

    The task uses template variables from input data: ``{variable}``
    placeholders in url, headers, and body are replaced with input values.

    Args:
        id: Task identifier.
        url: URL template (e.g., "https://api.example.com/users/{user_id}").
        method: HTTP method (GET, POST, PUT, DELETE, PATCH).
        headers: Default request headers.
        body: Request body template.
        retry_count: Number of retries on failure.
        retry_delay: Delay between retries in seconds.
        timeout: Request timeout in seconds.
        description: Task description.

    Returns:
        A Task instance.
    """
    default_headers = headers or {}

    def execute(params: dict, context: Any) -> dict:
        data = params.get("input_data", params)

        # Template substitution
        req_url = url.format(**data) if url else data.get("url", "")
        req_method = method
        req_headers = {k: v.format(**data) for k, v in default_headers.items()}
        req_body = body.format(**data).encode() if body else None

        if not req_url:
            return {"status_code": 0, "body": "", "headers": {}, "error": "No URL provided"}

        _validate_url(req_url, allow_private_ips=allow_private_ips)

        req = urllib.request.Request(
            req_url,
            data=req_body,
            headers=req_headers,
            method=req_method,
        )

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resp_body = resp.read().decode()
                resp_headers = dict(resp.headers)
                status = resp.status

                json_data = None
                try:
                    json_data = json.loads(resp_body)
                except (json.JSONDecodeError, ValueError):
                    pass

                return {
                    "status_code": status,
                    "body": resp_body,
                    "headers": resp_headers,
                    "json_data": json_data,
                }
        except urllib.error.HTTPError as e:
            return {
                "status_code": e.code,
                "body": e.read().decode(),
                "headers": dict(e.headers),
                "json_data": None,
            }
        except urllib.error.URLError as e:
            raise ConnectionError(f"HTTP request failed: {e}")

    return Task(
        id=id,
        description=description or f"HTTP {method} request",
        input_schema=HttpInput,
        output_schema=HttpOutput,
        execute=execute,
        retry_count=retry_count,
        retry_delay=retry_delay,
    )
