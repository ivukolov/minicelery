import asyncio
import logging
import os
import socket

from minicelery import TaskQueue

logger = logging.getLogger()


class Worker:

    def __init__(self, app: TaskQueue, name: str | None = None, queue: str = "default", poll_timeout: int = 10):
        self.name = name or f"{socket.gethostname()}-{os.getpid()}"
        self.app = app
        self.queue = queue
        self.poll_timeout = poll_timeout
        self._stop_event = asyncio.Event()


    def stop(self):
        self._stop_event.set()

    async def run(self):
        async with self.app.broker:
            sleep_time = 1
            while not self._stop_event.is_set():
                try:
                    deliveries = await self.app.broker.consume(
                        [self.queue], self.name, 1, self.poll_timeout
                    )
                except Exception as e:
                    logger.exception('Delivers consume error', exc_info=True)
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_time)
                    except TimeoutError:
                        pass
                    sleep_time = min(sleep_time * 2, 30)
                    continue
                sleep_time = 1
                for delivery in deliveries:
                    task = self.app.tasks.get(delivery.message.task)
                    logger.debug(task)
                    args, kwargs = delivery.message.args, delivery.message.kwargs
                    if task:
                        try:
                            await task.run(*args, **kwargs)
                            await self.app.broker.ack(delivery)
                            logger.debug('ack')
                        except Exception as e:
                            logger.error(e, exc_info=True)

