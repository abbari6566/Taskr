from __future__ import annotations

import asyncio
import random
from typing import Any, Callable, Coroutine

TASK_REGISTRY: dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {}


def task(name: str):
    def decorator(fn: Callable[..., Coroutine[Any, Any, Any]]):
        TASK_REGISTRY[name] = fn
        return fn
    return decorator


@task("send_email")
async def send_email(to: str, subject: str) -> dict:
    await asyncio.sleep(1)
    return {"sent": True, "to": to, "subject": subject}


@task("resize_image")
async def resize_image(url: str, width: int, height: int) -> dict:
    await asyncio.sleep(2)
    return {"resized": True, "url": url, "dimensions": f"{width}x{height}"}


@task("flaky_task")
async def flaky_task() -> dict:
    if random.random() < 0.7:
        raise Exception("Random failure!")
    return {"success": True}


@task("fast_task")
async def fast_task() -> dict:
    return {"done": True}
