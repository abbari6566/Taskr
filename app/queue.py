from __future__ import annotations

import json

import redis.asyncio as redis

from app.config import settings
from app.models import Priority, Task, TaskStatus

QUEUE_KEYS = {
    Priority.high: "queue:high",
    Priority.normal: "queue:normal",
    Priority.low: "queue:low",
}
PROCESSING_KEY = "queue:processing"
RETRY_KEY = "queue:retry"
DEAD_KEY = "queue:dead"


def _task_key(task_id: str) -> str:
    return f"task:{task_id}"


_pool: redis.Redis | None = None


async def get_redis() -> redis.Redis:
    global _pool
    if _pool is None:
        _pool = redis.from_url(settings.redis_url, decode_responses=True)
    return _pool


async def close_redis() -> None:
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None


async def enqueue(task: Task) -> str:
    r = await get_redis()
    queue_key = QUEUE_KEYS[task.priority]
    task_data = task.model_dump(mode="json")
    pipe = r.pipeline()
    pipe.hset(_task_key(task.task_id), mapping={
        k: json.dumps(v) if isinstance(v, (dict, list)) else ("" if v is None else str(v))
        for k, v in task_data.items()
    })
    pipe.lpush(queue_key, task.task_id)
    await pipe.execute()
    return task.task_id


async def dequeue(timeout: float = 1.0) -> str | None:
    """Pop a task_id from the highest-priority non-empty queue.

    Uses BRPOPLPUSH for the reliable-queue pattern: the task_id is
    atomically moved into ``queue:processing`` so it survives crashes.
    Queues are checked in priority order (high → normal → low).
    """
    r = await get_redis()
    for priority in (Priority.high, Priority.normal, Priority.low):
        queue_key = QUEUE_KEYS[priority]
        result = await r.brpoplpush(queue_key, PROCESSING_KEY, timeout=int(timeout))
        if result is not None:
            return result
    return None


async def acknowledge(task_id: str) -> None:
    r = await get_redis()
    await r.lrem(PROCESSING_KEY, 1, task_id)


async def update_status(task_id: str, status: TaskStatus, **fields: object) -> None:
    r = await get_redis()
    mapping: dict[str, str] = {"status": status.value}
    for k, v in fields.items():
        if isinstance(v, (dict, list)):
            mapping[k] = json.dumps(v)
        elif v is None:
            mapping[k] = ""
        else:
            mapping[k] = str(v)
    await r.hset(_task_key(task_id), mapping=mapping)


async def get_task(task_id: str) -> dict | None:
    r = await get_redis()
    data = await r.hgetall(_task_key(task_id))
    if not data:
        return None
    for field in ("args", "kwargs", "result"):
        if field in data and data[field]:
            try:
                data[field] = json.loads(data[field])
            except (json.JSONDecodeError, TypeError):
                pass
    return data


async def get_queue_depths() -> dict[str, int]:
    r = await get_redis()
    pipe = r.pipeline()
    pipe.llen(QUEUE_KEYS[Priority.high])
    pipe.llen(QUEUE_KEYS[Priority.normal])
    pipe.llen(QUEUE_KEYS[Priority.low])
    pipe.llen(PROCESSING_KEY)
    pipe.zcard(RETRY_KEY)
    pipe.llen(DEAD_KEY)
    results = await pipe.execute()
    return {
        "high": results[0],
        "normal": results[1],
        "low": results[2],
        "processing": results[3],
        "retry": results[4],
        "dead": results[5],
    }


async def get_all_tasks(limit: int = 100) -> list[dict]:
    r = await get_redis()
    cursor: int | str = 0
    task_keys: list[str] = []
    while True:
        cursor, keys = await r.scan(cursor=int(cursor), match="task:*", count=200)
        task_keys.extend(keys)
        if cursor == 0 or len(task_keys) >= limit:
            break
    task_keys = task_keys[:limit]
    tasks: list[dict] = []
    for key in task_keys:
        data = await r.hgetall(key)
        if data:
            for field in ("args", "kwargs", "result"):
                if field in data and data[field]:
                    try:
                        data[field] = json.loads(data[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            tasks.append(data)
    return tasks


async def add_to_retry(task_id: str, retry_at: float) -> None:
    r = await get_redis()
    await r.zadd(RETRY_KEY, {task_id: retry_at})


async def get_due_retries(now: float) -> list[str]:
    r = await get_redis()
    return await r.zrangebyscore(RETRY_KEY, 0, now)


async def remove_from_retry(task_id: str) -> None:
    r = await get_redis()
    await r.zrem(RETRY_KEY, task_id)


async def add_to_dead(task_id: str) -> None:
    r = await get_redis()
    pipe = r.pipeline()
    pipe.lrem(PROCESSING_KEY, 1, task_id)
    pipe.lpush(DEAD_KEY, task_id)
    await pipe.execute()


async def get_processing_tasks() -> list[str]:
    r = await get_redis()
    return await r.lrange(PROCESSING_KEY, 0, -1)


async def remove_from_processing(task_id: str) -> None:
    r = await get_redis()
    await r.lrem(PROCESSING_KEY, 1, task_id)


async def delete_task_from_queue(task_id: str) -> bool:
    """Remove a pending task from its priority queue. Returns True if found."""
    r = await get_redis()
    for queue_key in QUEUE_KEYS.values():
        removed = await r.lrem(queue_key, 1, task_id)
        if removed:
            return True
    return False


async def get_active_workers() -> list[dict]:
    r = await get_redis()
    cursor: int | str = 0
    worker_keys: list[str] = []
    while True:
        cursor, keys = await r.scan(cursor=int(cursor), match="worker:*:heartbeat", count=100)
        worker_keys.extend(keys)
        if cursor == 0:
            break
    workers: list[dict] = []
    for key in worker_keys:
        ttl = await r.ttl(key)
        parts = key.split(":")
        workers.append({
            "worker_id": parts[1],
            "ttl": ttl,
            "status": "alive" if ttl > 0 else "stale",
        })
    return workers
