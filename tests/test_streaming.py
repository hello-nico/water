"""Tests for the streaming module."""

import asyncio
import json

import pytest
from pydantic import BaseModel

from water import Flow, create_task
from water.integrations.streaming import StreamEvent, StreamManager, StreamingFlow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class In(BaseModel):
    value: int


class Out(BaseModel):
    value: int


def _make_task(task_id: str = "t1"):
    return create_task(
        id=task_id,
        description="test task",
        input_schema=In,
        output_schema=Out,
        execute=lambda p, c: {"value": p["input_data"]["value"] + 1},
    )


def _make_failing_task(task_id: str = "fail"):
    def _boom(p, c):
        raise RuntimeError("task exploded")

    return create_task(
        id=task_id,
        description="failing task",
        input_schema=In,
        output_schema=Out,
        execute=_boom,
    )


def _make_flow(task=None):
    task = task or _make_task()
    flow = Flow(id="test_flow", description="test")
    flow.then(task).register()
    return flow


# ---------------------------------------------------------------------------
# StreamEvent tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stream_event_creation():
    event = StreamEvent(
        event_type="task_start",
        flow_id="f1",
        execution_id="e1",
        task_id="t1",
        data={"key": "val"},
    )
    assert event.event_type == "task_start"
    assert event.flow_id == "f1"
    assert event.execution_id == "e1"
    assert event.task_id == "t1"
    assert event.data == {"key": "val"}
    assert event.timestamp  # non-empty


# ---------------------------------------------------------------------------
# StreamManager tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stream_manager_subscribe():
    """Subscriber receives events for its execution_id."""
    sm = StreamManager()
    queue = await sm.subscribe("exec1")

    event = StreamEvent(event_type="task_start", flow_id="f", execution_id="exec1")
    await sm.emit(event)

    received = await asyncio.wait_for(queue.get(), timeout=1.0)
    assert received.event_type == "task_start"
    assert received.execution_id == "exec1"


@pytest.mark.asyncio
async def test_stream_manager_execution_filter():
    """Subscriber only gets events for their execution."""
    sm = StreamManager()
    q1 = await sm.subscribe("exec1")
    q2 = await sm.subscribe("exec2")

    event = StreamEvent(event_type="task_start", flow_id="f", execution_id="exec1")
    await sm.emit(event)

    received = await asyncio.wait_for(q1.get(), timeout=1.0)
    assert received.execution_id == "exec1"

    assert q2.empty(), "Queue for exec2 should not have received the event"


@pytest.mark.asyncio
async def test_stream_manager_global_subscribe():
    """Global subscriber gets events for all executions."""
    sm = StreamManager()
    global_q = await sm.subscribe()  # no execution_id => global

    e1 = StreamEvent(event_type="flow_start", flow_id="f", execution_id="exec1")
    e2 = StreamEvent(event_type="flow_start", flow_id="f", execution_id="exec2")
    await sm.emit(e1)
    await sm.emit(e2)

    r1 = await asyncio.wait_for(global_q.get(), timeout=1.0)
    r2 = await asyncio.wait_for(global_q.get(), timeout=1.0)
    assert r1.execution_id == "exec1"
    assert r2.execution_id == "exec2"


@pytest.mark.asyncio
async def test_stream_manager_unsubscribe():
    """Unsubscribed queue stops receiving events."""
    sm = StreamManager()
    queue = await sm.subscribe("exec1")

    # Emit one event, should arrive
    await sm.emit(StreamEvent(event_type="task_start", flow_id="f", execution_id="exec1"))
    received = await asyncio.wait_for(queue.get(), timeout=1.0)
    assert received is not None

    # Unsubscribe
    await sm.unsubscribe(queue, "exec1")

    # Emit another event, should NOT arrive
    await sm.emit(StreamEvent(event_type="task_complete", flow_id="f", execution_id="exec1"))
    assert queue.empty(), "Unsubscribed queue should not receive events"


@pytest.mark.asyncio
async def test_stream_manager_multiple_subscribers():
    """Multiple subscribers all receive the same event."""
    sm = StreamManager()
    q1 = await sm.subscribe("exec1")
    q2 = await sm.subscribe("exec1")
    q_global = await sm.subscribe()

    event = StreamEvent(event_type="task_start", flow_id="f", execution_id="exec1")
    await sm.emit(event)

    r1 = await asyncio.wait_for(q1.get(), timeout=1.0)
    r2 = await asyncio.wait_for(q2.get(), timeout=1.0)
    r_global = await asyncio.wait_for(q_global.get(), timeout=1.0)

    assert r1.event_type == "task_start"
    assert r2.event_type == "task_start"
    assert r_global.event_type == "task_start"


# ---------------------------------------------------------------------------
# format_sse tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_format_sse():
    sm = StreamManager()
    event = StreamEvent(
        event_type="task_complete",
        flow_id="f1",
        execution_id="e1",
        task_id="t1",
        data={"result": 42},
    )
    sse = sm.format_sse(event)
    assert sse.startswith("event: task_complete\n")
    assert "data: " in sse
    assert sse.endswith("\n\n")

    # The data line should be valid JSON
    data_line = sse.split("data: ", 1)[1].rstrip("\n")
    parsed = json.loads(data_line)
    assert parsed["event_type"] == "task_complete"
    assert parsed["data"]["result"] == 42


# ---------------------------------------------------------------------------
# StreamingFlow tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_streaming_flow_emits_events():
    """StreamingFlow emits task_start and task_complete events."""
    sm = StreamManager()
    flow = _make_flow()
    sf = StreamingFlow(flow, sm)

    global_q = await sm.subscribe()

    result = await sf.run({"value": 10})
    assert result["value"] == 11

    events = []
    while not global_q.empty():
        events.append(await global_q.get())

    event_types = [e.event_type for e in events]
    assert "flow_start" in event_types
    assert "task_start" in event_types
    assert "task_complete" in event_types
    assert "flow_complete" in event_types


@pytest.mark.asyncio
async def test_streaming_flow_run_and_stream():
    """run_and_stream returns both result and collected events."""
    sm = StreamManager()
    flow = _make_flow()
    sf = StreamingFlow(flow, sm)

    result, events = await sf.run_and_stream({"value": 5})

    assert result["value"] == 6
    assert len(events) > 0

    event_types = [e.event_type for e in events]
    assert "flow_start" in event_types
    assert "flow_complete" in event_types


@pytest.mark.asyncio
async def test_streaming_flow_error_event():
    """Errors emit flow_error event."""
    sm = StreamManager()
    flow = _make_flow(task=_make_failing_task())
    sf = StreamingFlow(flow, sm)

    global_q = await sm.subscribe()

    with pytest.raises(RuntimeError, match="task exploded"):
        await sf.run({"value": 1})

    events = []
    while not global_q.empty():
        events.append(await global_q.get())

    event_types = [e.event_type for e in events]
    assert "flow_error" in event_types

    error_events = [e for e in events if e.event_type == "flow_error"]
    assert "task exploded" in error_events[0].data["error"]
