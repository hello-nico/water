import pytest
import asyncio
from pydantic import BaseModel
from water import create_task, Flow, EventEmitter, FlowEvent


class NumberInput(BaseModel):
    value: int

class NumberOutput(BaseModel):
    value: int


@pytest.mark.asyncio
async def test_event_emitter_basic():
    emitter = EventEmitter()
    sub = await emitter.subscribe()
    collected = []

    async def collect():
        async for event in sub:
            collected.append(event)

    collector = asyncio.create_task(collect())

    await emitter.emit(FlowEvent("test_event", "flow1", data={"key": "val"}))
    await emitter.close()

    await collector
    assert len(collected) == 1
    assert collected[0].event_type == "test_event"
    assert collected[0].data == {"key": "val"}

@pytest.mark.asyncio
async def test_event_emitter_multiple_subscribers():
    emitter = EventEmitter()
    sub1 = await emitter.subscribe()
    sub2 = await emitter.subscribe()

    assert emitter.subscriber_count == 2

    collected1 = []
    collected2 = []

    async def collect(sub, out):
        async for event in sub:
            out.append(event)

    t1 = asyncio.create_task(collect(sub1, collected1))
    t2 = asyncio.create_task(collect(sub2, collected2))

    await emitter.emit(FlowEvent("ev", "f"))
    await emitter.close()

    await t1
    await t2
    assert len(collected1) == 1
    assert len(collected2) == 1

@pytest.mark.asyncio
async def test_flow_events_integration():
    """Flow with event emitter produces task and flow events."""
    emitter = EventEmitter()
    collected = []

    task = create_task(
        id="t1",
        description="Add 1",
        input_schema=NumberInput,
        output_schema=NumberOutput,
        execute=lambda p, c: {"value": p["input_data"]["value"] + 1},
    )

    flow = Flow(id="event_flow", description="Test events")
    flow.events = emitter
    flow.then(task).register()

    sub = await emitter.subscribe()

    async def collect_events():
        async for event in sub:
            collected.append(event)

    collector = asyncio.create_task(collect_events())

    result = await flow.run({"value": 5})
    assert result["value"] == 6

    await collector

    event_types = [e.event_type for e in collected]
    assert "flow_start" in event_types
    assert "task_start" in event_types
    assert "task_complete" in event_types
    assert "flow_complete" in event_types

@pytest.mark.asyncio
async def test_event_subscription_get():
    emitter = EventEmitter()
    sub = await emitter.subscribe()

    await emitter.emit(FlowEvent("ev1", "f"))
    event = await sub.get(timeout=1.0)
    assert event.event_type == "ev1"

    # Timeout when no event
    event = await sub.get(timeout=0.05)
    assert event is None

    await sub.close()
    assert emitter.subscriber_count == 0

@pytest.mark.asyncio
async def test_flow_event_to_dict():
    event = FlowEvent("task_start", "flow1", task_id="t1", data={"x": 1})
    d = event.to_dict()
    assert d["event_type"] == "task_start"
    assert d["flow_id"] == "flow1"
    assert d["task_id"] == "t1"
    assert d["data"] == {"x": 1}
