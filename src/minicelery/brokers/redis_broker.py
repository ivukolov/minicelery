from redis.asyncio import Redis

import asyncio

from minicelery.brokers import Broker


class RedisBroker(Broker):

    def __init__(self, host="localhost", port=6379, db=0):
        self._client = None
        self.host = host
        self.port = port
        self.db = db

    async def connect(self) -> Redis:
        if self._client is None:
            self._client = Redis(host=self.host, port=self.port, db=self.db)
        # Check if the connection is successful
        await self._client.ping()
        return self._client

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
