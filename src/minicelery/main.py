import asyncio
import json
import uuid

from minicelery.brokers.redis_broker import RedisBroker
from minicelery.config import settings

redis_broker: RedisBroker = RedisBroker(host="localhost", port=6380)


async def test():
    client = await redis_broker.connect()
    message = {
        "id": str(uuid.uuid4()),
        "name": "add",
        "args": [2, 3],
        "kwargs": {},
    }
    await client.lpush(settings.queue,json.dumps(message))

asyncio.run(test())