import json
from typing import Self, Sequence

from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio import Redis
import logging

from redis.asyncio.client import Pipeline

from minicelery.brokers import Broker
from minicelery.brokers.base import Delivery
from minicelery.config import settings
from minicelery.errors import MalformedMessageError
from minicelery.message import TaskMessage, DeadTaskMessage, Reason, BaseMessage

logger = logging.getLogger(__name__)



class RedisBroker(Broker):

    def __init__(
            self,
            host: str = "localhost",
            port: int = 6379,
            db: int = 0,
            prefix: str = "minicelery",
            group = "workers",
            socket_timeout: float = 20.0,
            delete_on_ack: bool = True,
    ):
        self._client: Redis | None = None
        self.host = host
        self.port = port
        self.db = db
        self.socket_timeout = socket_timeout
        self._decode_responses = True
        self._prefix = prefix
        self._data_field = 'data'
        self._group = group
        self._groups_ready: set[str] = set()
        self._delete_on_ack = delete_on_ack


    @property
    def group(self) -> str:
        return self._group


    def key(self, queue: str) -> str:
        return f"{self._prefix}:queue:{queue}"

    def dead_letter_key(self, queue: str) -> str:
        return f"{self._prefix}:dead_letters:{queue}"

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
                decode_responses=self._decode_responses,
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
        await self._publish(self.client, self.key(message.queue), message.dumps())

    async def _publish(self, client, stream_name: str, message: bytes) -> None:
        await client.xadd(
            stream_name, {self._data_field: message}
        )

    async def dead_letter(self, message: DeadTaskMessage) -> None:
        dead_key = self.dead_letter_key(message.from_queue)
        source_key = self.key(message.from_queue)
        async with self.client.pipeline(transaction=True) as pipe:
            await self._publish(pipe, dead_key, message.dumps())
            ack_at = len(pipe)
            self._ack(pipe, source_key, message.receipt_id)
            result = await pipe.execute()
        self._check_ack(result[ack_at], source_key, message.receipt_id)


    async def _create_group(self, key: str) -> None:
        if key in self._groups_ready:
            return
        try:
            await self.client.xgroup_create(key, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._groups_ready.add(key)

    def _ack(self, pipe: Pipeline, stream_key, receipt) -> Pipeline:
        pipe.xack(stream_key, self.group, receipt)
        if self._delete_on_ack:
            pipe.xdel(stream_key, receipt)
        return pipe

    def _check_ack(self, acked: int, queue: str, receipt: str) -> None:
        if not acked:
            logger.error("XACK missed: queue=%s receipt=%s", queue, receipt)

    async def ack(self, delivery: Delivery) -> None:
        async with self.client.pipeline(transaction=True) as pipe:
            stream_key = self.key(delivery.queue)
            pipe = self._ack(pipe, stream_key, delivery.receipt)
            result = await pipe.execute()
            self._check_ack(result[0], stream_key, delivery.receipt)


    def _parse(self, fields: dict) -> TaskMessage:
        data = fields.get(self._data_field)
        if not data:
            raise MalformedMessageError(
                f"Field '{self._data_field}' is missing or empty, got keys: {list(fields)}"
            )
        try:
            message = TaskMessage.loads(data)
            logger.debug("Message: %s", message)
        except ValidationError as e:
            errors = e.errors(include_input=False, include_url=False, include_context=False)
            raise MalformedMessageError(f"Validation error: {errors}") from e
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
        keys = {self.key(queue): queue for queue in queues}
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
            queue = keys[stream]
            for receipt, fields in entries:
                try:
                    message_obj = self._parse(fields)
                except MalformedMessageError as exc:
                    dead_task_msg = DeadTaskMessage(
                        from_queue=queue,
                        raw_data=fields,
                        reason=Reason.parse_error,
                        error=str(exc),
                        worker=consumer,
                        receipt_id=receipt,
                    )
                    await self.dead_letter(dead_task_msg)
                else:
                    deliveries.append(Delivery(message_obj, queue, receipt))
        return deliveries
                



