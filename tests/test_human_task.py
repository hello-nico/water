import pytest
import asyncio
from pydantic import BaseModel
from water import create_human_task, HumanInputManager, HumanInputRequired, Flow


class DataInput(BaseModel):
    value: int

class DataOutput(BaseModel):
    value: int
    approved: bool


@pytest.mark.asyncio
async def test_human_task_without_manager_raises():
    """Without a HumanInputManager, the task raises HumanInputRequired."""
    task = create_human_task(
        id="approval",
        description="Needs approval",
        prompt="Please approve this value",
        input_schema=DataInput,
        output_schema=DataOutput,
    )

    flow = Flow(id="no_mgr", description="No manager")
    flow.then(task).register()

    with pytest.raises(HumanInputRequired, match="Human input required"):
        await flow.run({"value": 42})


@pytest.mark.asyncio
async def test_human_task_with_manager():
    """HumanInputManager provides input that unblocks the task."""
    manager = HumanInputManager()

    task = create_human_task(
        id="approval",
        description="Needs approval",
        prompt="Approve?",
        input_schema=DataInput,
        output_schema=DataOutput,
        human_input_manager=manager,
    )

    flow = Flow(id="with_mgr", description="With manager")
    flow.then(task).register()

    async def provide_input_later():
        # Wait for the request to appear
        for _ in range(50):
            pending = await manager.get_pending()
            if pending:
                request_id = list(pending.keys())[0]
                await manager.provide_input(request_id, {"approved": True})
                return
            await asyncio.sleep(0.01)

    provider = asyncio.create_task(provide_input_later())
    result = await flow.run({"value": 42})
    await provider

    assert result["value"] == 42
    assert result["approved"] is True


@pytest.mark.asyncio
async def test_human_task_with_transform():
    """Custom transform function processes human input."""
    manager = HumanInputManager()

    def my_transform(data, human_response):
        return {"value": data["value"] * human_response["multiplier"]}

    task = create_human_task(
        id="multiply",
        description="Get multiplier",
        prompt="Enter multiplier",
        input_schema=DataInput,
        output_schema=DataInput,
        human_input_manager=manager,
        transform=my_transform,
    )

    flow = Flow(id="transform", description="Transform")
    flow.then(task).register()

    async def provide_input_later():
        for _ in range(50):
            pending = await manager.get_pending()
            if pending:
                request_id = list(pending.keys())[0]
                await manager.provide_input(request_id, {"multiplier": 3})
                return
            await asyncio.sleep(0.01)

    provider = asyncio.create_task(provide_input_later())
    result = await flow.run({"value": 10})
    await provider

    assert result["value"] == 30


@pytest.mark.asyncio
async def test_human_task_timeout():
    """Human task times out if no input is provided."""
    manager = HumanInputManager()

    task = create_human_task(
        id="slow_human",
        description="Slow human",
        prompt="Hurry up",
        input_schema=DataInput,
        output_schema=DataOutput,
        human_input_manager=manager,
        timeout=0.05,
    )

    flow = Flow(id="timeout", description="Timeout")
    flow.then(task).register()

    with pytest.raises(TimeoutError, match="timed out"):
        await flow.run({"value": 1})


@pytest.mark.asyncio
async def test_human_input_manager_provide_invalid():
    """Providing input for a non-existent request raises ValueError."""
    manager = HumanInputManager()
    with pytest.raises(ValueError, match="No pending request"):
        await manager.provide_input("nonexistent", {"data": 1})


@pytest.mark.asyncio
async def test_human_input_manager_cancel():
    """Cancelling a pending request removes it."""
    manager = HumanInputManager()
    future = await manager.create_request("req1", "test prompt")
    assert "req1" in await manager.get_pending()

    await manager.cancel("req1")
    assert "req1" not in await manager.get_pending()
    assert future.cancelled()
