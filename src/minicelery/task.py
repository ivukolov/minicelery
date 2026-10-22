from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any

from minicelery.message import TaskMessage

if TYPE_CHECKING:
    from minicelery.app import TaskQueue

class Task:
    def __init__(self, queue: TaskQueue, func, name, max_retries=1):
        self.queue = queue
        self.func = func
        self.name = name
        self.max_retries = max_retries

    async def run(self, *args, **kwargs) -> Any:
        if inspect.iscoroutinefunction(self.func):
            return await self.func(*args, **kwargs)
        return await asyncio.to_thread(self.func, *args, **kwargs)

    async def delay(self, *args, **kwargs):
        message = TaskMessage(task=self.name, args=list(args), kwargs=kwargs)
        await self.queue.send(message)
        return self
