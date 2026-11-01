from typing import Self, Sequence

from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio import Redis
import logging

from minicelery.brokers import Broker
from minicelery.brokers.base import Delivery
from minicelery.config import settings
from minicelery.message import TaskMessage

logger = logging.getLogger(__name__)



class RedisBroker(Broker):

    def __init__(
            self,
            host: str = "localhost",
            port: int = 6379,
            db: int = 0,
            decode_responses: bool = True,
            prefix: str = "minicelery",
            group = "workers",
            socket_timeout: float = 20.0,
            delete_on_ack: bool = True,
    ):
        self._client: Redis | None = None
        self.host = host
        self.port = port
        self.db = db
        self.decode_responses = decode_responses
        self.socket_timeout = socket_timeout
        self._prefix = prefix
        self._dead_letter_prefix: str = 'dead_letter'
        self._group = group
        self._groups_ready: set[str] = set()
        self._delete_on_ack = delete_on_ack


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
            self._client = Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=self.decode_responses,
                socket_timeout=self.socket_timeout,
            )
        try:
            await self.client.ping()
        except ConnectionError as exc:
            logger.error('RedisBroker has not been connected', exc_info=True)
            raise ConnectionError("RedisBroker has not been connected")
        return self.client

    async def disconnect(self) -> None:
        if self.client:
            await self.client.aclose()

    async def publish(self, message: TaskMessage) -> None:
        await self.client.xadd(self.key(message.queue), {"data": message.dumps()})

    async def dead_letter(self, delivery, reason, error):
        await self.client.xadd(self._dead_letter_prefix, {"delivery": delivery, "reason": reason, "error": error})

    async def _create_group(self, key: str) -> None:
        if key in self._groups_ready:
            return
        try:
            await self.client.xgroup_create(key, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._groups_ready.add(key)

    async def ack(self, delivery: Delivery) -> None:
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.xack(delivery.queue, self.group, delivery.receipt)
            if self._delete_on_ack:
                pipe.xdel(delivery.queue, delivery.receipt)
            result = await pipe.execute()
            logger.debug(
                "XACK & XDEL: result=%s, id=%s", result, delivery.message.id)
            if not result[0]:
                logger.error(
                    "XACK missed: queue=%s receipt=%s, id=%s",
                    delivery.queue, delivery.receipt, delivery.message.id
                )


    async def _parse(self, fields: dict) -> TaskMessage | None:
        data = fields.get("data")
        try:
            message = TaskMessage.loads(data)
            logger.debug(f"Message: {message}")
        except (KeyError, ValidationError) as exc:
            await self.dead_letter(fields, reason='ValidationError', error=str(exc))
            #ADD processing not valid tasks
            logger.error(str(exc))
            return None
        return message

    async def consume(
            self,
            queues: Sequence[str],
            consumer: str,
            max_messages: int,
            timeout: float,
    ) -> list[Delivery]:
        """Consume messages"""
        #Make group keys
        keys = [self.key(queue) for queue in queues]
        # create groups
        for key in keys:
            await self._create_group(key)
        response = await self.client.xreadgroup(
            self._group,
            consumer,
            streams=dict.fromkeys(keys, ">"),
            count=max_messages,
            block=max(1, int(timeout * 1000)),
        )
        deliveries: list[Delivery] = []
        for stream, entries in response or []:
            key = stream
            for entry_id, fields in entries:
                message_obj = await self._parse(fields)
                if message_obj:
                    deliveries.append(Delivery(message_obj, key, entry_id))
        return deliveries
                



