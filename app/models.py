from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Priority(str, Enum):
    high = "high"
    normal = "normal"
    low = "low"


class TaskStatus(str, Enum):
    pending = "pending"
    running = "running"
    done = "done"
    failed = "failed"
    retrying = "retrying"
    dead = "dead"


class TaskCreate(BaseModel):
    function: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.normal
    max_retries: int = 3


class Task(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    function: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.normal
    retries: int = 0
    max_retries: int = 3
    status: TaskStatus = TaskStatus.pending
    error: str | None = None
    result: Any = None
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    started_at: str | None = None
    finished_at: str | None = None
    worker_id: str | None = None
