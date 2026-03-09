"""Tests for the worker execution path + FastAPI endpoints."""

from __future__ import annotations

import asyncio

import fakeredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app import queue as q
from app.main import app
from app.models import Priority, Task, TaskStatus
from app.worker import execute_task


@pytest_asyncio.fixture(autouse=True)
async def fake_redis(monkeypatch):
    server = fakeredis.FakeServer()
    fake = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
    monkeypatch.setattr(q, "_pool", fake)
    yield fake
    await fake.aclose()


# ── worker execute_task ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_execute_task_success():
    task = Task(function="fast_task", priority=Priority.normal)
    await q.enqueue(task)
    await q.dequeue(timeout=1)
    await execute_task(task.task_id, worker_id="test-1")

    data = await q.get_task(task.task_id)
    assert data["status"] == "done"
    assert data["worker_id"] == "test-1"


@pytest.mark.asyncio
async def test_execute_unknown_function():
    task = Task(function="no_such_func", priority=Priority.normal)
    await q.enqueue(task)
    await q.dequeue(timeout=1)
    await execute_task(task.task_id, worker_id="test-1")

    data = await q.get_task(task.task_id)
    assert data["status"] == "failed"
    assert "Unknown function" in data["error"]


# ── FastAPI endpoints ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_create_and_get_task():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/tasks", json={
            "function": "send_email",
            "args": ["u@x.com", "Hi"],
            "priority": "normal",
        })
        assert resp.status_code == 201
        task_id = resp.json()["task_id"]

        resp2 = await client.get(f"/tasks/{task_id}")
        assert resp2.status_code == 200
        assert resp2.json()["function"] == "send_email"


@pytest.mark.asyncio
async def test_list_tasks():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/tasks", json={"function": "fast_task"})
        resp = await client.get("/tasks")
        assert resp.status_code == 200
        assert len(resp.json()) >= 1


@pytest.mark.asyncio
async def test_cancel_pending_task():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/tasks", json={"function": "fast_task"})
        task_id = resp.json()["task_id"]

        resp2 = await client.delete(f"/tasks/{task_id}")
        assert resp2.status_code == 200
        assert resp2.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_queue_depths_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/queues")
        assert resp.status_code == 200
        data = resp.json()
        assert "high" in data


@pytest.mark.asyncio
async def test_404_unknown_task():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/tasks/nonexistent-id")
        assert resp.status_code == 404
