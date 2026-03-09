"""Tests for queue operations."""

from __future__ import annotations

import asyncio
import time

import fakeredis
import pytest
import pytest_asyncio

from app.models import Priority, Task, TaskStatus
from app import queue as q


@pytest_asyncio.fixture(autouse=True)
async def fake_redis(monkeypatch):
    """Replace the real Redis connection with fakeredis for every test."""
    server = fakeredis.FakeServer()
    fake = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    monkeypatch.setattr(q, "_pool", fake)
    yield fake
    await fake.aclose()


# ── enqueue / get_task ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_enqueue_stores_task():
    task = Task(function="send_email", args=["a@b.com", "Hi"], priority=Priority.normal)
    task_id = await q.enqueue(task)
    data = await q.get_task(task_id)
    assert data is not None
    assert data["function"] == "send_email"
    assert data["status"] == "pending"


@pytest.mark.asyncio
async def test_enqueue_pushes_to_correct_queue():
    high = Task(function="x", priority=Priority.high)
    low = Task(function="x", priority=Priority.low)
    await q.enqueue(high)
    await q.enqueue(low)

    r = await q.get_redis()
    assert await r.llen("queue:high") == 1
    assert await r.llen("queue:low") == 1
    assert await r.llen("queue:normal") == 0


# ── dequeue ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_dequeue_priority_order():
    """High-priority tasks should be dequeued first."""
    t_low = Task(function="lo", priority=Priority.low)
    t_high = Task(function="hi", priority=Priority.high)
    await q.enqueue(t_low)
    await q.enqueue(t_high)

    first = await q.dequeue(timeout=1)
    assert first == t_high.task_id


@pytest.mark.asyncio
async def test_dequeue_moves_to_processing():
    task = Task(function="x")
    await q.enqueue(task)
    await q.dequeue(timeout=1)

    r = await q.get_redis()
    assert await r.llen(q.PROCESSING_KEY) == 1


@pytest.mark.asyncio
async def test_dequeue_returns_none_when_empty():
    result = await q.dequeue(timeout=1)
    assert result is None


# ── acknowledge ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_acknowledge_removes_from_processing():
    task = Task(function="x")
    await q.enqueue(task)
    task_id = await q.dequeue(timeout=1)
    await q.acknowledge(task_id)

    r = await q.get_redis()
    assert await r.llen(q.PROCESSING_KEY) == 0


# ── update_status ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_update_status():
    task = Task(function="x")
    await q.enqueue(task)
    await q.update_status(task.task_id, TaskStatus.running, worker_id="42")
    data = await q.get_task(task.task_id)
    assert data["status"] == "running"
    assert data["worker_id"] == "42"


# ── retry / dead letter ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_add_and_get_retries():
    await q.add_to_retry("task-abc", time.time() - 10)
    due = await q.get_due_retries(time.time())
    assert "task-abc" in due


@pytest.mark.asyncio
async def test_dead_letter_queue():
    task = Task(function="x")
    await q.enqueue(task)
    await q.dequeue(timeout=1)
    await q.add_to_dead(task.task_id)

    r = await q.get_redis()
    assert await r.llen(q.DEAD_KEY) == 1
    assert await r.llen(q.PROCESSING_KEY) == 0


# ── queue depths ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_queue_depths():
    await q.enqueue(Task(function="a", priority=Priority.high))
    await q.enqueue(Task(function="b", priority=Priority.normal))
    depths = await q.get_queue_depths()
    assert depths["high"] == 1
    assert depths["normal"] == 1
    assert depths["low"] == 0
