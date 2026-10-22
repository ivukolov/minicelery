import inspect
from typing import Dict

from minicelery.brokers import Broker
from minicelery.message import TaskMessage
from minicelery.task import Task


class TaskQueue:
    def __init__(self, broker: Broker):
        self.tasks: Dict[str, Task] = {}
        self.broker = broker
        self.name = 'tasks'
        self._connected = False

    def task(self, max_retries=0):
        def decorator(func):
            task_name = f'{self.name}.{func.__module__}.{func.__name__}'
            task = Task(self, func, task_name, max_retries)
            self.tasks[task_name] = task
            return task
        return decorator

    async def connect(self) -> None:
        if not self._connected:
            await self.broker.connect()
            self._connected = True

    async def close(self) -> None:
        if self._connected:
            await self.broker.close()
            self._connected = False

    async def send(self, message: TaskMessage) -> None:
        await self.connect()
        await self.broker.publish(message=message)
