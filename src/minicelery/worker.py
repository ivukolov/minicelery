import os
import socket

from minicelery import TaskQueue


class Worker:

    def __init__(self, app: TaskQueue, name: str | None = None, queue: str = "default", poll_timeout: int = 10):
        self.name = name or f"{socket.gethostname()}-{os.getpid()}"
        self.app = app
        self.queue = queue
        self.poll_timeout = poll_timeout
        self._stopping = False


    def stop(self):
        self._stopping = True

    async def run(self):
        await self.app.connect()
        while not self._stopping:
            deliveries = await self.app.broker.consume(
                [self.queue], self.name, 1, self.poll_timeout
            )
            print(deliveries)

