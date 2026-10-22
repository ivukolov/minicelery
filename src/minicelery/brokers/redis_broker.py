from typing import Self, Sequence

from redis import ResponseError
from redis.asyncio import Redis

from minicelery.brokers import Broker
from minicelery.brokers.base import Delivery
from minicelery.config import settings
from minicelery.message import TaskMessage



class RedisBroker(Broker):

    def __init__(self, host="localhost", port=6379, db=0, decode_responses=True, prefix="minicelery", group="workers"):
        self._client = None
        self.host = host
        self.port = port
        self.db = db
        self.decode_responses = decode_responses
        self._prefix = prefix
        self._group: str = group
        self._groups_ready: set[str] = set()


    @property
    def group(self) -> str:
        return self._group


    def key(self, queue: str) -> str:
        return f"{self._prefix}:queue:{queue}"

    @property
    def client(self) -> Redis:
        if self._client:
            return self._client
        else:
            raise AttributeError("RedisBroker has not been initialized")


    async def connect(self) -> Redis:
        if self._client is None:
            self._client = Redis(host=self.host, port=self.port, db=self.db, decode_responses=self.decode_responses)
        await self._client.ping()
        return self._client

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()

    async def publish(self, message: TaskMessage) -> None:
        await self._client.xadd(self.key(message.queue), {"data": message.dumps()})


    async def _create_group(self, key: str) -> None:
        if key in self._groups_ready:
            return
        try:
            await self.client.xgroup_create(key, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._groups_ready.add(key)

    async def consume(
            self,
            queues: Sequence[str],
            consumer: str,
            max_messages: int,
            timeout: float,
    ):
        """Consume messages"""
        #Make group keys
        keys = [self.key(queue) for queue in queues]
        # create groups
        for key in keys:
            await self._create_group(key)
        response = await self.client.xreadgroup(  # redis-py types this reply loosely
            self._group,
            consumer,
            streams=dict.fromkeys(keys, ">"),
            count=max_messages,
            block=max(1, int(timeout * 1000)),  # BLOCK 0 would mean "forever"
        )
        print(response)




