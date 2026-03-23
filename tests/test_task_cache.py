"""Tests for water/resilience/cache.py — InMemoryCache and cache_key helper."""

import time

import pytest

from water.resilience.cache import InMemoryCache, cache_key


# ============================================================================
# InMemoryCache — basic get/set
# ============================================================================

def test_set_and_get():
    c = InMemoryCache()
    c.set("k", 42)
    assert c.get("k") == 42


def test_get_miss():
    c = InMemoryCache()
    assert c.get("missing") is None


def test_has_present():
    c = InMemoryCache()
    c.set("k", "v")
    assert c.has("k") is True


def test_has_missing():
    c = InMemoryCache()
    assert c.has("absent") is False


def test_overwrite():
    c = InMemoryCache()
    c.set("k", 1)
    c.set("k", 2)
    assert c.get("k") == 2


def test_stores_none_value():
    """None is a valid cached value, distinct from a cache miss."""
    c = InMemoryCache()
    c.set("k", None)
    assert c.has("k") is True
    assert c.get("k") is None


def test_stores_complex_values():
    c = InMemoryCache()
    val = {"nested": [1, 2, {"three": 3}]}
    c.set("k", val)
    assert c.get("k") == val


# ============================================================================
# TTL expiration
# ============================================================================

def test_ttl_not_expired():
    c = InMemoryCache()
    c.set("k", "v", ttl=10.0)
    assert c.get("k") == "v"
    assert c.has("k") is True


def test_ttl_expired(monkeypatch):
    """After TTL elapses, the entry should be treated as a miss."""
    c = InMemoryCache()
    c.set("k", "v", ttl=0.5)

    # Fast-forward monotonic clock
    real_monotonic = time.monotonic
    offset = 1.0
    monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + offset)

    assert c.get("k") is None
    assert c.has("k") is False


def test_ttl_zero_expires_immediately(monkeypatch):
    c = InMemoryCache()
    c.set("k", "v", ttl=0.0)
    # Even with ttl=0 the entry exists until monotonic time advances
    real_monotonic = time.monotonic
    monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 0.001)
    assert c.get("k") is None


def test_no_ttl_never_expires(monkeypatch):
    c = InMemoryCache()
    c.set("k", "v")  # no TTL
    real_monotonic = time.monotonic
    monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 999999)
    assert c.get("k") == "v"


# ============================================================================
# Cache clear / invalidation
# ============================================================================

def test_clear_removes_all():
    c = InMemoryCache()
    c.set("a", 1)
    c.set("b", 2)
    c.clear()
    assert c.get("a") is None
    assert c.get("b") is None
    assert c.has("a") is False


def test_clear_on_empty_cache():
    c = InMemoryCache()
    c.clear()  # should not raise


# ============================================================================
# cache_key helper
# ============================================================================

def test_cache_key_deterministic():
    k1 = cache_key("task1", {"x": 1, "y": 2})
    k2 = cache_key("task1", {"x": 1, "y": 2})
    assert k1 == k2


def test_cache_key_order_independent():
    """Dict key ordering should not affect the hash (sort_keys=True)."""
    k1 = cache_key("t", {"a": 1, "b": 2})
    k2 = cache_key("t", {"b": 2, "a": 1})
    assert k1 == k2


def test_cache_key_different_task_ids():
    k1 = cache_key("task_a", {"x": 1})
    k2 = cache_key("task_b", {"x": 1})
    assert k1 != k2


def test_cache_key_different_data():
    k1 = cache_key("t", {"x": 1})
    k2 = cache_key("t", {"x": 2})
    assert k1 != k2


def test_cache_key_is_hex_string():
    k = cache_key("t", {})
    assert isinstance(k, str)
    assert len(k) == 64  # SHA-256 hex digest
    int(k, 16)  # should parse as hex without error
