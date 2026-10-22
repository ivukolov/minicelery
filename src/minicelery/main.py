import asyncio
import json
import uuid

from minicelery import TaskQueue, worker
from minicelery.brokers import RedisBroker
from minicelery.config import settings
from minicelery.logging_config import setup_logging
from minicelery.worker import Worker

setup_logging(settings)

redis_broker: RedisBroker = RedisBroker(host="localhost", port=6380)

app = TaskQueue(broker=redis_broker)



@app.task(3)
async def test2(x=1, y=2):
    print(x)
    print(y)


async def make_test_data():
    test = await test2.delay(x=1, y=44)
    print(test)

async def start_worker(worker: Worker):
    await worker.run()

async def main():
    await make_test_data()
    await start_worker(Worker(app=app))



asyncio.get_event_loop().run_until_complete(main())