"""Tests for water/core/engine.py — ExecutionEngine."""

import asyncio

import pytest
from pydantic import BaseModel

from water.core import Flow, create_task
from water.core.engine import ExecutionEngine, NodeType
from water.storage import InMemoryStorage, FlowStatus


# --- Schemas ----------------------------------------------------------------

class NumIn(BaseModel):
    value: int


class NumOut(BaseModel):
    value: int


class TextIn(BaseModel):
    text: str


class TextOut(BaseModel):
    text: str


# --- Helpers ----------------------------------------------------------------

def _add_one(params, ctx):
    return {"value": params["input_data"]["value"] + 1}


def _double(params, ctx):
    return {"value": params["input_data"]["value"] * 2}


def _make_task(tid, fn, in_schema=NumIn, out_schema=NumOut, **kwargs):
    return create_task(
        id=tid, description=tid, input_schema=in_schema,
        output_schema=out_schema, execute=fn, **kwargs,
    )


# --- Sequential flow --------------------------------------------------------

@pytest.mark.asyncio
async def test_sequential_flow_two_tasks():
    """Two tasks chained with .then() should execute in order."""
    flow = Flow(id="seq2", description="seq2")
    flow.then(_make_task("a", _add_one)).then(_make_task("b", _double)).register()
    result = await flow.run({"value": 3})
    assert result["value"] == 8  # (3+1)*2


@pytest.mark.asyncio
async def test_sequential_flow_three_tasks():
    """Three sequential tasks should chain data correctly."""
    flow = Flow(id="seq3", description="seq3")
    flow.then(_make_task("a", _add_one))
    flow.then(_make_task("b", _double))
    flow.then(_make_task("c", _add_one))
    flow.register()
    result = await flow.run({"value": 0})
    assert result["value"] == 3  # (0+1)*2 + 1 = 3


# --- Parallel flow ----------------------------------------------------------

@pytest.mark.asyncio
async def test_parallel_flow():
    """Parallel node should run tasks concurrently and merge results by task id."""
    t1 = _make_task("p_add", _add_one)
    t2 = _make_task("p_dbl", _double)

    flow = Flow(id="par", description="par")
    flow.parallel([t1, t2]).register()
    result = await flow.run({"value": 5})
    assert result["p_add"]["value"] == 6
    assert result["p_dbl"]["value"] == 10


@pytest.mark.asyncio
async def test_parallel_tasks_actually_concurrent():
    """Parallel tasks should overlap in time (not run sequentially)."""
    import time

    async def slow_task(params, ctx):
        await asyncio.sleep(0.05)
        return {"value": params["input_data"]["value"]}

    t1 = _make_task("s1", slow_task)
    t2 = _make_task("s2", slow_task)

    flow = Flow(id="par_time", description="par_time")
    flow.parallel([t1, t2]).register()

    start = time.monotonic()
    await flow.run({"value": 1})
    elapsed = time.monotonic() - start
    # Sequential would be >= 0.1s; parallel should finish under 0.09s
    assert elapsed < 0.09


# --- Conditional branching --------------------------------------------------

@pytest.mark.asyncio
async def test_branch_first_match():
    """Branch should execute the first matching condition's task."""
    high = _make_task("high", lambda p, c: {"value": 100})
    low = _make_task("low", lambda p, c: {"value": 0})

    flow = Flow(id="br", description="br")
    flow.branch([
        (lambda d: d["value"] > 10, high),
        (lambda d: d["value"] <= 10, low),
    ]).register()

    result = await flow.run({"value": 20})
    assert result["value"] == 100


@pytest.mark.asyncio
async def test_branch_second_match():
    flow = Flow(id="br2", description="br2")
    high = _make_task("high", lambda p, c: {"value": 100})
    low = _make_task("low", lambda p, c: {"value": 0})

    flow.branch([
        (lambda d: d["value"] > 10, high),
        (lambda d: d["value"] <= 10, low),
    ]).register()

    result = await flow.run({"value": 5})
    assert result["value"] == 0


@pytest.mark.asyncio
async def test_branch_no_match_passes_through():
    """If no branch condition matches, data passes through unchanged."""
    never = _make_task("never", lambda p, c: {"value": -1})

    flow = Flow(id="br_no", description="br_no")
    flow.branch([
        (lambda d: False, never),
    ]).register()

    result = await flow.run({"value": 42})
    assert result["value"] == 42


# --- Error handling ---------------------------------------------------------

@pytest.mark.asyncio
async def test_task_failure_propagates():
    """An unhandled task error should propagate as an exception."""
    def boom(p, c):
        raise ValueError("kaboom")

    flow = Flow(id="err", description="err")
    flow.then(_make_task("bad", boom)).register()

    with pytest.raises(ValueError, match="kaboom"):
        await flow.run({"value": 1})


@pytest.mark.asyncio
async def test_task_failure_recorded_in_storage():
    """Storage should record FAILED status when a task raises."""
    storage = InMemoryStorage()

    def boom(p, c):
        raise RuntimeError("oops")

    flow = Flow(id="err_st", description="err_st", storage=storage)
    flow.then(_make_task("bad", boom)).register()

    with pytest.raises(RuntimeError):
        await flow.run({"value": 1})

    sessions = await storage.list_sessions()
    assert sessions[0].status == FlowStatus.FAILED
    assert "oops" in sessions[0].error


# --- Timeout ----------------------------------------------------------------

@pytest.mark.asyncio
async def test_task_timeout():
    """A task that exceeds its timeout should raise asyncio.TimeoutError."""
    async def hang(params, ctx):
        await asyncio.sleep(10)
        return {"value": 0}

    task = _make_task("hang", hang, timeout=0.05)
    flow = Flow(id="to", description="to")
    flow.then(task).register()

    with pytest.raises(asyncio.TimeoutError):
        await flow.run({"value": 1})


@pytest.mark.asyncio
async def test_sync_task_timeout():
    """Sync task timeout should also raise TimeoutError."""
    import time as _time

    def slow_sync(params, ctx):
        _time.sleep(10)
        return {"value": 0}

    task = _make_task("slow_sync", slow_sync, timeout=0.05)
    flow = Flow(id="to_sync", description="to_sync")
    flow.then(task).register()

    with pytest.raises(asyncio.TimeoutError):
        await flow.run({"value": 1})


# --- Retry ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_task_retry_succeeds_on_second_attempt():
    """A task that fails once then succeeds should produce the correct output."""
    attempts = {"count": 0}

    def flaky(params, ctx):
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise RuntimeError("transient")
        return {"value": params["input_data"]["value"] + 1}

    task = _make_task("flaky", flaky, retry_count=2, retry_delay=0.0)
    flow = Flow(id="retry", description="retry")
    flow.then(task).register()

    result = await flow.run({"value": 10})
    assert result["value"] == 11
    assert attempts["count"] == 2


@pytest.mark.asyncio
async def test_task_retry_exhausted():
    """When all retries are exhausted the error should propagate."""
    def always_fail(p, c):
        raise RuntimeError("permanent")

    task = _make_task("fail", always_fail, retry_count=1, retry_delay=0.0)
    flow = Flow(id="retry_ex", description="retry_ex")
    flow.then(task).register()

    with pytest.raises(RuntimeError, match="permanent"):
        await flow.run({"value": 1})


# --- Context passing -------------------------------------------------------

@pytest.mark.asyncio
async def test_context_passes_between_tasks():
    """Each task should be able to read the previous task's output via context."""
    recorded = {}

    def first(params, ctx):
        return {"value": 10}

    def second(params, ctx):
        prev = ctx.get_task_output("first")
        recorded["prev_value"] = prev["value"] if prev else None
        return {"value": params["input_data"]["value"] + 1}

    flow = Flow(id="ctx", description="ctx")
    flow.then(_make_task("first", first))
    flow.then(_make_task("second", second))
    flow.register()

    await flow.run({"value": 0})
    assert recorded["prev_value"] == 10


# --- Async task support -----------------------------------------------------

@pytest.mark.asyncio
async def test_async_task_execution():
    """Async execute functions should be awaited correctly."""
    async def async_add(params, ctx):
        await asyncio.sleep(0.01)
        return {"value": params["input_data"]["value"] + 5}

    flow = Flow(id="async_t", description="async_t")
    flow.then(_make_task("async_add", async_add)).register()
    result = await flow.run({"value": 2})
    assert result["value"] == 7
