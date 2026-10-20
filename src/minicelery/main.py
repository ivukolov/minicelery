import asyncio

from minicelery.brokers.redis_broker import RedisBroker


redis_broker: RedisBroker = RedisBroker(host="localhost", port=6380)


asyncio.run(redis_broker.connect())