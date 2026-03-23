"""Tests for water/storage/base.py — InMemoryStorage and SQLiteStorage."""

import os
import tempfile

import pytest

from water.storage import (
    FlowSession,
    FlowStatus,
    InMemoryStorage,
    SQLiteStorage,
    TaskRun,
)


# ============================================================================
# InMemoryStorage
# ============================================================================

@pytest.mark.asyncio
async def test_inmemory_save_get_session():
    s = InMemoryStorage()
    session = FlowSession(flow_id="f1", input_data={"a": 1}, execution_id="e1")
    await s.save_session(session)
    got = await s.get_session("e1")
    assert got is not None
    assert got.flow_id == "f1"
    assert got.input_data == {"a": 1}


@pytest.mark.asyncio
async def test_inmemory_get_nonexistent_session():
    s = InMemoryStorage()
    assert await s.get_session("nope") is None


@pytest.mark.asyncio
async def test_inmemory_list_sessions_all():
    s = InMemoryStorage()
    await s.save_session(FlowSession(flow_id="f1", input_data={}, execution_id="e1"))
    await s.save_session(FlowSession(flow_id="f2", input_data={}, execution_id="e2"))
    assert len(await s.list_sessions()) == 2


@pytest.mark.asyncio
async def test_inmemory_list_sessions_by_flow_id():
    s = InMemoryStorage()
    await s.save_session(FlowSession(flow_id="f1", input_data={}, execution_id="e1"))
    await s.save_session(FlowSession(flow_id="f2", input_data={}, execution_id="e2"))
    await s.save_session(FlowSession(flow_id="f1", input_data={}, execution_id="e3"))
    assert len(await s.list_sessions(flow_id="f1")) == 2
    assert len(await s.list_sessions(flow_id="f2")) == 1


@pytest.mark.asyncio
async def test_inmemory_list_empty():
    s = InMemoryStorage()
    assert await s.list_sessions() == []


@pytest.mark.asyncio
async def test_inmemory_save_get_task_runs():
    s = InMemoryStorage()
    tr = TaskRun(execution_id="e1", task_id="t1", node_index=0, status="done",
                 input_data={"x": 1}, output_data={"y": 2})
    await s.save_task_run(tr)
    runs = await s.get_task_runs("e1")
    assert len(runs) == 1
    assert runs[0].task_id == "t1"
    assert runs[0].output_data == {"y": 2}


@pytest.mark.asyncio
async def test_inmemory_update_existing_task_run():
    s = InMemoryStorage()
    tr = TaskRun(execution_id="e1", task_id="t1", node_index=0, status="running", id="r1")
    await s.save_task_run(tr)
    tr.status = "completed"
    tr.output_data = {"v": 42}
    await s.save_task_run(tr)
    runs = await s.get_task_runs("e1")
    assert len(runs) == 1
    assert runs[0].status == "completed"


@pytest.mark.asyncio
async def test_inmemory_get_task_runs_empty():
    s = InMemoryStorage()
    assert await s.get_task_runs("nope") == []


@pytest.mark.asyncio
async def test_inmemory_duplicate_session_save_overwrites():
    s = InMemoryStorage()
    session = FlowSession(flow_id="f1", input_data={"a": 1}, execution_id="e1",
                          status=FlowStatus.PENDING)
    await s.save_session(session)
    session.status = FlowStatus.COMPLETED
    await s.save_session(session)
    got = await s.get_session("e1")
    assert got.status == FlowStatus.COMPLETED
    assert len(await s.list_sessions()) == 1


# ============================================================================
# SQLiteStorage
# ============================================================================

@pytest.fixture
def sqlite_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.mark.asyncio
async def test_sqlite_save_get_session(sqlite_path):
    s = SQLiteStorage(db_path=sqlite_path)
    session = FlowSession(flow_id="f1", input_data={"k": "v"}, execution_id="e1")
    await s.save_session(session)
    got = await s.get_session("e1")
    assert got is not None
    assert got.flow_id == "f1"
    assert got.input_data == {"k": "v"}


@pytest.mark.asyncio
async def test_sqlite_get_nonexistent(sqlite_path):
    s = SQLiteStorage(db_path=sqlite_path)
    assert await s.get_session("nope") is None


@pytest.mark.asyncio
async def test_sqlite_list_sessions(sqlite_path):
    s = SQLiteStorage(db_path=sqlite_path)
    await s.save_session(FlowSession(flow_id="a", input_data={}, execution_id="e1"))
    await s.save_session(FlowSession(flow_id="b", input_data={}, execution_id="e2"))
    await s.save_session(FlowSession(flow_id="a", input_data={}, execution_id="e3"))
    assert len(await s.list_sessions()) == 3
    assert len(await s.list_sessions(flow_id="a")) == 2


@pytest.mark.asyncio
async def test_sqlite_list_empty(sqlite_path):
    s = SQLiteStorage(db_path=sqlite_path)
    assert await s.list_sessions() == []


@pytest.mark.asyncio
async def test_sqlite_save_get_task_runs(sqlite_path):
    s = SQLiteStorage(db_path=sqlite_path)
    # Need a session first for the FK
    await s.save_session(FlowSession(flow_id="f", input_data={}, execution_id="e1"))
    tr = TaskRun(execution_id="e1", task_id="t1", node_index=0,
                 status="ok", input_data={"a": 1}, output_data={"b": 2})
    await s.save_task_run(tr)
    runs = await s.get_task_runs("e1")
    assert len(runs) == 1
    assert runs[0].output_data == {"b": 2}


@pytest.mark.asyncio
async def test_sqlite_get_task_runs_empty(sqlite_path):
    s = SQLiteStorage(db_path=sqlite_path)
    assert await s.get_task_runs("nope") == []


@pytest.mark.asyncio
async def test_sqlite_duplicate_session_upserts(sqlite_path):
    s = SQLiteStorage(db_path=sqlite_path)
    session = FlowSession(flow_id="f", input_data={}, execution_id="e1",
                          status=FlowStatus.PENDING)
    await s.save_session(session)
    session.status = FlowStatus.COMPLETED
    session.result = {"done": True}
    await s.save_session(session)
    got = await s.get_session("e1")
    assert got.status == FlowStatus.COMPLETED
    assert got.result == {"done": True}
    assert len(await s.list_sessions()) == 1


# ============================================================================
# Serialization round-trips
# ============================================================================

def test_flow_session_round_trip():
    session = FlowSession(flow_id="f", input_data={"x": 1}, execution_id="e",
                          status=FlowStatus.RUNNING, current_node_index=3,
                          context_state={"step": 2})
    d = session.to_dict()
    restored = FlowSession.from_dict(d)
    assert restored.execution_id == "e"
    assert restored.status == FlowStatus.RUNNING
    assert restored.current_node_index == 3
    assert restored.context_state == {"step": 2}


def test_task_run_round_trip():
    tr = TaskRun(execution_id="e", task_id="t", node_index=1,
                 status="completed", input_data={"a": 1}, output_data={"b": 2})
    d = tr.to_dict()
    restored = TaskRun.from_dict(d)
    assert restored.task_id == "t"
    assert restored.status == "completed"
    assert restored.input_data == {"a": 1}
    assert restored.output_data == {"b": 2}
