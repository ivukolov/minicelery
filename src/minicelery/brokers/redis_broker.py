from typing import Self

from redis.asyncio import Redis

from minicelery.brokers import Broker
from minicelery.config import settings
from minicelery.message import TaskMessage



class RedisBroker(Broker):

    def __init__(self, host="localhost", port=6379, db=0, decode_responses=True):
        self._client = None
        self.host = host
        self.port = port
        self.db = db
        self.decode_responses = decode_responses


    async def connect(self) -> Redis:
        if self._client is None:
            self._client = Redis(host=self.host, port=self.port, db=self.db, decode_responses=self.decode_responses)
        # Check if the connection is successful
        await self._client.ping()
        return self._client

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()

    async def publish(self, message: TaskMessage) -> None:
        await self._client.xadd(settings.queue, {"data": message.dumps()})

