"""MiniQueue Worker — dequeue tasks, execute them, handle failures."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import redis.asyncio as redis

from app.config import settings
from app.models import TaskStatus
from app.queue import (
    acknowledge,
    add_to_dead,
    add_to_retry,
    dequeue,
    get_redis,
    get_task,
    remove_from_processing,
    update_status,
)
from app.tasks import TASK_REGISTRY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker:%(process)d] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

SHUTDOWN = False


def _handle_signal(sig, frame):
    global SHUTDOWN
    logger.info("Received %s — finishing current tasks then shutting down", signal.Signals(sig).name)
    SHUTDOWN = True


async def heartbeat_loop(worker_id: str) -> None:
    r = await get_redis()
    while not SHUTDOWN:
        await r.setex(f"worker:{worker_id}:heartbeat", settings.heartbeat_ttl, "alive")
        await asyncio.sleep(settings.heartbeat_interval)


async def execute_task(task_id: str, worker_id: str) -> None:
    task_data = await get_task(task_id)
    if task_data is None:
        logger.warning("Task %s not found, skipping", task_id)
        await acknowledge(task_id)
        return

    func_name = task_data.get("function", "")
    if func_name not in TASK_REGISTRY:
        logger.error("Unknown function '%s' for task %s", func_name, task_id)
        await update_status(
            task_id, TaskStatus.failed,
            error=f"Unknown function: {func_name}",
            finished_at=datetime.now(timezone.utc).isoformat(),
            worker_id=worker_id,
        )
        await acknowledge(task_id)
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    await update_status(task_id, TaskStatus.running, started_at=now_iso, worker_id=worker_id)
    logger.info("Running task %s (%s)", task_id, func_name)

    args = task_data.get("args", [])
    kwargs = task_data.get("kwargs", {})
    if isinstance(args, str):
        args = json.loads(args)
    if isinstance(kwargs, str):
        kwargs = json.loads(kwargs)

    try:
        fn = TASK_REGISTRY[func_name]
        result = await fn(*args, **kwargs)
        finished_iso = datetime.now(timezone.utc).isoformat()
        await update_status(
            task_id, TaskStatus.done,
            result=result,
            finished_at=finished_iso,
            worker_id=worker_id,
        )
        await acknowledge(task_id)
        logger.info("Task %s done", task_id)

    except Exception as exc:
        retries = int(task_data.get("retries", 0))
        max_retries = int(task_data.get("max_retries", settings.retry_max))
        retries += 1
        finished_iso = datetime.now(timezone.utc).isoformat()

        if retries < max_retries:
            delay = 2 ** retries
            retry_at = time.time() + delay
            await update_status(
                task_id, TaskStatus.retrying,
                retries=retries,
                error=str(exc),
                finished_at=finished_iso,
                worker_id=worker_id,
            )
            await acknowledge(task_id)
            await add_to_retry(task_id, retry_at)
            logger.warning(
                "Task %s failed (%s), retry %d/%d in %ds",
                task_id, exc, retries, max_retries, delay,
            )
        else:
            await update_status(
                task_id, TaskStatus.dead,
                retries=retries,
                error=str(exc),
                finished_at=finished_iso,
                worker_id=worker_id,
            )
            await add_to_dead(task_id)
            logger.error("Task %s exhausted retries, moved to dead letter queue", task_id)


async def worker_loop(worker_id: str, sem: asyncio.Semaphore) -> None:
    while not SHUTDOWN:
        await sem.acquire()
        task_id = await dequeue(timeout=1)
        if task_id is None:
            sem.release()
            continue
        asyncio.create_task(_run_with_sem(sem, task_id, worker_id))


async def _run_with_sem(sem: asyncio.Semaphore, task_id: str, worker_id: str) -> None:
    try:
        await execute_task(task_id, worker_id)
    finally:
        sem.release()


async def main() -> None:
    worker_id = str(os.getpid())
    logger.info("Worker %s starting (concurrency=%d)", worker_id, settings.worker_concurrency)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    sem = asyncio.Semaphore(settings.worker_concurrency)
    heartbeat = asyncio.create_task(heartbeat_loop(worker_id))

    await worker_loop(worker_id, sem)

    heartbeat.cancel()
    logger.info("Worker %s shut down", worker_id)


if __name__ == "__main__":
    asyncio.run(main())
