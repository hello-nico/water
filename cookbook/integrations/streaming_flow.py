"""
Cookbook: Streaming / Real-time Progress with Water

Demonstrates how to use StreamManager and StreamingFlow to get real-time
progress events from a Water flow execution, including:

1. Setting up StreamManager and StreamingFlow
2. Subscribing to events
3. Using with a FastAPI SSE endpoint
4. Collecting events from a flow run
"""

import asyncio
from pydantic import BaseModel
from water.core import Flow, create_task
from water.integrations.streaming import StreamManager, StreamingFlow, add_streaming_routes


# ---------------------------------------------------------------------------
# 1. Define tasks and flow
# ---------------------------------------------------------------------------

class NumberIn(BaseModel):
    value: int


class NumberOut(BaseModel):
    value: int


double_task = create_task(
    id="double",
    description="Doubles the input value",
    input_schema=NumberIn,
    output_schema=NumberOut,
    execute=lambda p, c: {"value": p["input_data"]["value"] * 2},
)

add_one_task = create_task(
    id="add_one",
    description="Adds one to the input value",
    input_schema=NumberIn,
    output_schema=NumberOut,
    execute=lambda p, c: {"value": p["input_data"]["value"] + 1},
)

flow = Flow(id="math_flow", description="Double then add one")
flow.then(double_task).then(add_one_task).register()


# ---------------------------------------------------------------------------
# 2. Basic usage: subscribe and collect events
# ---------------------------------------------------------------------------

async def basic_streaming():
    """Run a flow and print events as they happen."""
    stream_manager = StreamManager()
    streaming_flow = StreamingFlow(flow, stream_manager)

    # Subscribe globally to see all events
    queue = await stream_manager.subscribe()

    # Run the flow
    result = await streaming_flow.run({"value": 5})
    print(f"Result: {result}")

    # Drain events
    while not queue.empty():
        event = await queue.get()
        print(f"  [{event.event_type}] task={event.task_id} data={event.data}")


# ---------------------------------------------------------------------------
# 3. Using run_and_stream for convenience
# ---------------------------------------------------------------------------

async def run_and_stream_example():
    """Use run_and_stream to get result + events in one call."""
    stream_manager = StreamManager()
    streaming_flow = StreamingFlow(flow, stream_manager)

    result, events = await streaming_flow.run_and_stream({"value": 10})
    print(f"Result: {result}")
    print(f"Collected {len(events)} events:")
    for event in events:
        print(f"  [{event.event_type}] flow={event.flow_id} task={event.task_id}")


# ---------------------------------------------------------------------------
# 4. FastAPI SSE endpoint
# ---------------------------------------------------------------------------

def create_app():
    """Create a FastAPI app with SSE streaming endpoints.

    Endpoints added:
        GET /api/stream/{execution_id}  - Stream events for one execution
        GET /api/stream                 - Stream all events

    Usage with curl:
        curl -N http://localhost:8000/api/stream
    """
    from fastapi import FastAPI

    app = FastAPI(title="Water Streaming Example")
    stream_manager = StreamManager()

    # Register SSE routes
    add_streaming_routes(app, stream_manager)

    # Example endpoint to trigger a flow
    @app.post("/run")
    async def run_flow():
        streaming_flow = StreamingFlow(flow, stream_manager)
        result = await streaming_flow.run({"value": 42})
        return {"result": result}

    return app


# ---------------------------------------------------------------------------
# Run examples
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Basic Streaming ===")
    asyncio.run(basic_streaming())

    print("\n=== Run and Stream ===")
    asyncio.run(run_and_stream_example())

    print("\n=== FastAPI App ===")
    print("To run the SSE server:")
    print("  uvicorn cookbook.streaming_flow:create_app --factory --reload")
