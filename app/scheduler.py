"""MiniQueue Scheduler — moves due retries back into their priority queues."""

from __future__ import annotations

import asyncio
import logging
import time

import redis.asyncio as redis

from app.config import settings
from app.models import TaskStatus
from app.queue import (
    QUEUE_KEYS,
    get_due_retries,
    get_redis,
    get_task,
    remove_from_retry,
    update_status,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scheduler] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Retry scheduler started (interval=%ds)", settings.scheduler_interval)
    while True:
        try:
            now = time.time()
            due = await get_due_retries(now)
            for task_id in due:
                task_data = await get_task(task_id)
                if task_data is None:
                    await remove_from_retry(task_id)
                    continue
                priority = task_data.get("priority", "normal")
                queue_key = QUEUE_KEYS.get(priority, QUEUE_KEYS["normal"])
                r = await get_redis()
                await r.lpush(queue_key, task_id)
                await remove_from_retry(task_id)
                await update_status(task_id, TaskStatus.pending)
                logger.info("Re-queued task %s → %s", task_id, queue_key)
        except Exception:
            logger.exception("Scheduler tick error")
        await asyncio.sleep(settings.scheduler_interval)


if __name__ == "__main__":
    asyncio.run(main())
