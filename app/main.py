"""Taskr — FastAPI producer API and dashboard."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.models import Task, TaskCreate, TaskStatus
from app.queue import (
    close_redis,
    delete_task_from_queue,
    enqueue,
    get_active_workers,
    get_all_tasks,
    get_queue_depths,
    get_redis,
    get_task,
    update_status,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_redis()


app = FastAPI(title="Taskr", version="1.0.0", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")




@app.get("/health")
async def health():
    try:
        r = await get_redis()
        await r.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis unreachable: {exc}")



@app.post("/tasks", status_code=201)
async def create_task(body: TaskCreate):
    task = Task(
        function=body.function,
        args=body.args,
        kwargs=body.kwargs,
        priority=body.priority,
        max_retries=body.max_retries,
    )
    task_id = await enqueue(task)
    return {"task_id": task_id, "status": "pending"}


@app.get("/tasks/{task_id}")
async def read_task(task_id: str):
    data = await get_task(task_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return data


@app.get("/tasks")
async def list_tasks(status: str | None = Query(None), limit: int = Query(100, le=500)):
    tasks = await get_all_tasks(limit=limit)
    if status:
        tasks = [t for t in tasks if t.get("status") == status]
    tasks.sort(key=lambda t: t.get("created_at", ""), reverse=True)
    return tasks


@app.delete("/tasks/{task_id}")
async def cancel_task(task_id: str):
    data = await get_task(task_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Task not found")
    if data.get("status") != TaskStatus.pending.value:
        raise HTTPException(status_code=400, detail="Only pending tasks can be cancelled")
    await delete_task_from_queue(task_id)
    await update_status(task_id, TaskStatus.failed, error="Cancelled by user")
    return {"task_id": task_id, "status": "cancelled"}




@app.get("/queues")
async def queue_depths():
    return await get_queue_depths()


@app.get("/workers")
async def list_workers():
    return await get_active_workers()



@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})
