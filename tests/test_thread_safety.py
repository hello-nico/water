"""Tests for thread safety of in-memory storage, cache, and event systems."""

import asyncio

import pytest

from water.middleware.events import EventEmitter, FlowEvent
from water.resilience.cache import InMemoryCache
from water.storage.base import FlowSession, InMemoryStorage


# ---------------------------------------------------------------------------
# InMemoryStorage concurrency
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_storage_saves():
    """Concurrent session saves should not lose data."""
    storage = InMemoryStorage()
    n = 100

    async def save_session(i: int) -> None:
        session = FlowSession(flow_id="flow1", input_data={"i": i}, execution_id=f"exec_{i}")
        await storage.save_session(session)

    await asyncio.gather(*(save_session(i) for i in range(n)))

    sessions = await storage.list_sessions()
    assert len(sessions) == n


@pytest.mark.asyncio
async def test_concurrent_storage_reads_and_writes():
    """Concurrent reads and writes should not raise or lose data."""
    storage = InMemoryStorage()
    n = 50

    async def writer(i: int) -> None:
        session = FlowSession(flow_id="flow1", input_data={"i": i}, execution_id=f"exec_{i}")
        await storage.save_session(session)

    async def reader() -> None:
        await storage.list_sessions()

    tasks = []
    for i in range(n):
        tasks.append(writer(i))
        tasks.append(reader())

    await asyncio.gather(*tasks)

    sessions = await storage.list_sessions()
    assert len(sessions) == n


# ---------------------------------------------------------------------------
# InMemoryCache concurrency
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_cache_reads_and_writes():
    """Concurrent cache gets and sets should not raise or lose data."""
    cache = InMemoryCache()
    n = 100

    async def writer(i: int) -> None:
        cache.set(f"key_{i}", i)

    async def reader(i: int) -> None:
        cache.get(f"key_{i}")
        cache.has(f"key_{i}")

    tasks = []
    for i in range(n):
        tasks.append(writer(i))
        tasks.append(reader(i))

    await asyncio.gather(*tasks)

    # All keys should be present
    for i in range(n):
        assert cache.has(f"key_{i}")
        assert cache.get(f"key_{i}") == i


@pytest.mark.asyncio
async def test_concurrent_cache_clear():
    """Clear during concurrent writes should not raise."""
    cache = InMemoryCache()
    n = 50

    async def writer(i: int) -> None:
        cache.set(f"key_{i}", i)

    async def clearer() -> None:
        cache.clear()

    tasks = [writer(i) for i in range(n)]
    tasks.append(clearer())

    await asyncio.gather(*tasks)
    # No assertion on final state — just ensure no exceptions


# ---------------------------------------------------------------------------
# EventEmitter concurrency
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_subscribe_unsubscribe():
    """Concurrent subscribe and unsubscribe should not corrupt the queue list."""
    emitter = EventEmitter()
    n = 50

    subscriptions = []

    async def subscribe_one() -> None:
        sub = await emitter.subscribe()
        subscriptions.append(sub)

    await asyncio.gather(*(subscribe_one() for _ in range(n)))
    assert emitter.subscriber_count == n

    async def unsubscribe_one(sub) -> None:
        await sub.close()

    await asyncio.gather(*(unsubscribe_one(s) for s in subscriptions))
    assert emitter.subscriber_count == 0


@pytest.mark.asyncio
async def test_concurrent_emit_and_subscribe():
    """Emitting while subscribing should not raise."""
    emitter = EventEmitter()
    n = 30

    async def subscriber() -> None:
        sub = await emitter.subscribe()
        await sub.close()

    async def emitter_task() -> None:
        await emitter.emit(FlowEvent("test", "flow1"))

    tasks = []
    for _ in range(n):
        tasks.append(subscriber())
        tasks.append(emitter_task())

    await asyncio.gather(*tasks)
