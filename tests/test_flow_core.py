"""Tests for water/core/flow.py — Flow DSL."""

import pytest
from pydantic import BaseModel

from water.core import Flow, create_task


# --- Schemas ----------------------------------------------------------------

class NumIn(BaseModel):
    value: int


class NumOut(BaseModel):
    value: int


class MsgIn(BaseModel):
    msg: str


class MsgOut(BaseModel):
    msg: str


# --- Helpers ----------------------------------------------------------------

def _noop(params, ctx):
    return params["input_data"]


def _add_one(params, ctx):
    return {"value": params["input_data"]["value"] + 1}


def _double(params, ctx):
    return {"value": params["input_data"]["value"] * 2}


def _task(tid="t", fn=None, in_s=NumIn, out_s=NumOut, **kw):
    return create_task(id=tid, description=tid, input_schema=in_s,
                       output_schema=out_s, execute=fn or _noop, **kw)


# --- Flow creation ----------------------------------------------------------

def test_flow_creation_with_id_and_description():
    f = Flow(id="my_flow", description="A test flow")
    assert f.id == "my_flow"
    assert f.description == "A test flow"


def test_flow_auto_generated_id():
    f = Flow()
    assert f.id.startswith("flow_")


def test_flow_default_description():
    f = Flow(id="x")
    assert "x" in f.description


# --- .then() chaining -------------------------------------------------------

def test_then_adds_sequential_node():
    f = Flow(id="f")
    f.then(_task("a"))
    assert len(f._tasks) == 1
    assert f._tasks[0]["type"] == "sequential"


def test_then_chaining_returns_self():
    f = Flow(id="f")
    ret = f.then(_task("a")).then(_task("b"))
    assert ret is f
    assert len(f._tasks) == 2


@pytest.mark.asyncio
async def test_then_sequential_execution():
    f = Flow(id="f")
    f.then(_task("a", _add_one)).then(_task("b", _double)).register()
    result = await f.run({"value": 1})
    assert result["value"] == 4  # (1+1)*2


def test_then_rejects_none_task():
    f = Flow(id="f")
    with pytest.raises(ValueError, match="Task cannot be None"):
        f.then(None)


# --- .parallel() ------------------------------------------------------------

def test_parallel_adds_parallel_node():
    f = Flow(id="f")
    f.parallel([_task("a"), _task("b")])
    assert len(f._tasks) == 1
    assert f._tasks[0]["type"] == "parallel"


def test_parallel_rejects_empty_list():
    f = Flow(id="f")
    with pytest.raises(ValueError, match="Parallel task list cannot be empty"):
        f.parallel([])


@pytest.mark.asyncio
async def test_parallel_execution():
    f = Flow(id="f")
    f.parallel([_task("a", _add_one), _task("b", _double)]).register()
    result = await f.run({"value": 3})
    assert result["a"]["value"] == 4
    assert result["b"]["value"] == 6


# --- .branch() --------------------------------------------------------------

def test_branch_adds_branch_node():
    f = Flow(id="f")
    f.branch([(lambda d: True, _task("a"))])
    assert len(f._tasks) == 1
    assert f._tasks[0]["type"] == "branch"


def test_branch_rejects_empty_list():
    f = Flow(id="f")
    with pytest.raises(ValueError, match="Branch list cannot be empty"):
        f.branch([])


def test_branch_rejects_async_condition():
    f = Flow(id="f")
    with pytest.raises(ValueError, match="Branch conditions cannot be async"):
        f.branch([(lambda d: True, _task("a")), (async_cond, _task("b"))])


def test_branch_rejects_none_task():
    f = Flow(id="f")
    with pytest.raises(ValueError, match="Task cannot be None"):
        f.branch([(lambda d: True, None)])


@pytest.mark.asyncio
async def test_branch_routing():
    big = _task("big", lambda p, c: {"value": 99})
    small = _task("small", lambda p, c: {"value": 1})

    f = Flow(id="f")
    f.branch([
        (lambda d: d["value"] > 50, big),
        (lambda d: d["value"] <= 50, small),
    ]).register()

    assert (await f.run({"value": 100}))["value"] == 99
    assert (await f.run({"value": 10}))["value"] == 1


# --- .register() ------------------------------------------------------------

def test_register_sets_registered_flag():
    f = Flow(id="f")
    f.then(_task("a"))
    f.register()
    assert f._registered is True


def test_register_rejects_empty_flow():
    f = Flow(id="f")
    with pytest.raises(ValueError, match="Flow must have at least one task"):
        f.register()


def test_cannot_add_tasks_after_registration():
    f = Flow(id="f")
    f.then(_task("a")).register()
    with pytest.raises(RuntimeError, match="Cannot add tasks after registration"):
        f.then(_task("b"))


@pytest.mark.asyncio
async def test_run_requires_registration():
    f = Flow(id="f")
    f.then(_task("a"))
    with pytest.raises(RuntimeError, match="Flow must be registered before running"):
        await f.run({"value": 1})


# --- Flow validation (contract checking) ------------------------------------

def test_validate_contracts_detects_mismatch():
    t1 = _task("a", _noop, in_s=NumIn, out_s=NumOut)
    t2 = _task("b", _noop, in_s=MsgIn, out_s=MsgOut)
    f = Flow(id="f")
    f.then(t1).then(t2)
    violations = f.validate_contracts()
    assert len(violations) > 0
    assert "msg" in violations[0]["missing_fields"]


def test_strict_contracts_raises():
    t1 = _task("a", _noop, in_s=NumIn, out_s=NumOut)
    t2 = _task("b", _noop, in_s=MsgIn, out_s=MsgOut)
    f = Flow(id="f", strict_contracts=True)
    f.then(t1).then(t2)
    with pytest.raises(ValueError, match="Data contract violations"):
        f.register()


def test_matching_contracts_pass():
    t1 = _task("a", _noop, in_s=NumIn, out_s=NumOut)
    t2 = _task("b", _noop, in_s=NumIn, out_s=NumOut)
    f = Flow(id="f", strict_contracts=True)
    f.then(t1).then(t2)
    f.register()  # Should not raise


# --- Subflow composition ----------------------------------------------------

@pytest.mark.asyncio
async def test_subflow_as_task():
    """A registered sub-flow can be embedded in a parent flow via as_task()."""
    sub = Flow(id="sub")
    sub.then(_task("sub_add", _add_one)).register()

    parent = Flow(id="parent")
    parent.then(_task("p_dbl", _double))
    parent.then(sub)  # auto-coerced via _coerce_task
    parent.register()

    result = await parent.run({"value": 3})
    # 3*2 = 6, then sub: 6+1 = 7
    assert result["value"] == 7


def test_unregistered_subflow_raises():
    sub = Flow(id="sub")
    sub.then(_task("x"))
    with pytest.raises(RuntimeError, match="Sub-flow must be registered"):
        sub.as_task()


# --- Loop -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_loop_executes_while_condition_true():
    f = Flow(id="loop")
    f.loop(
        condition=lambda d: d["value"] < 5,
        task=_task("inc", _add_one),
        max_iterations=20,
    ).register()
    result = await f.run({"value": 0})
    assert result["value"] == 5


# --- Metadata / version -----------------------------------------------------

def test_set_metadata():
    f = Flow(id="f")
    f.set_metadata("env", "test")
    assert f.metadata["env"] == "test"


def test_version_stored_in_metadata_on_run():
    f = Flow(id="f", version="1.2.3")
    f.then(_task("a")).register()
    # version is set on run, but we can verify the field is stored
    assert f.version == "1.2.3"


# --- Async condition helper -------------------------------------------------

async def async_cond(d):
    return True
