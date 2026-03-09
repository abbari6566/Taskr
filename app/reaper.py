"""MiniQueue Reaper — recovers tasks stuck in processing due to worker crashes."""

from __future__ import annotations

import asyncio
import logging
import time

from app.config import settings
from app.models import TaskStatus
from app.queue import (
    QUEUE_KEYS,
    get_processing_tasks,
    get_task,
    remove_from_processing,
    get_redis,
    update_status,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [reaper] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info(
        "Reaper started (interval=%ds, timeout=%ds)",
        settings.reaper_interval,
        settings.reaper_timeout,
    )
    while True:
        try:
            processing = await get_processing_tasks()
            now = time.time()
            for task_id in processing:
                task_data = await get_task(task_id)
                if task_data is None:
                    await remove_from_processing(task_id)
                    continue
                started_at = task_data.get("started_at", "")
                if not started_at:
                    continue
                from datetime import datetime, timezone

                try:
                    started_ts = datetime.fromisoformat(started_at).timestamp()
                except (ValueError, TypeError):
                    continue
                if now - started_ts > settings.reaper_timeout:
                    priority = task_data.get("priority", "normal")
                    queue_key = QUEUE_KEYS.get(priority, QUEUE_KEYS["normal"])
                    await remove_from_processing(task_id)
                    r = await get_redis()
                    await r.lpush(queue_key, task_id)
                    await update_status(task_id, TaskStatus.pending, started_at="", worker_id="")
                    logger.warning(
                        "Reaped stuck task %s (started %s) → %s",
                        task_id, started_at, queue_key,
                    )
        except Exception:
            logger.exception("Reaper tick error")
        await asyncio.sleep(settings.reaper_interval)


if __name__ == "__main__":
    asyncio.run(main())
