import asyncio
import json
import uuid

from minicelery import TaskQueue
from minicelery.brokers import RedisBroker
from minicelery.config import settings
from minicelery.logging_config import setup_logging

setup_logging(settings)

redis_broker: RedisBroker = RedisBroker(host="localhost", port=6380)

app = TaskQueue(broker=redis_broker)

# async def test():
#     client = await redis_broker.connect()
#
#     message = {
#         "id": str(uuid.uuid4()),
#         "name": "add",
#         "args": [2, 3],
#         "kwargs": {},
#     }
#
#     await client.lpush(settings.queue,json.dumps(message))

@app.task(3)
async def test2(x=1, y=2):
    print(x)
    print(y)


async def tester():
    test = await test2.delay(1, 44)
    print(test)



asyncio.get_event_loop().run_until_complete(tester())