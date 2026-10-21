import inspect

from minicelery.brokers import Broker
from minicelery.task import Task


class App:
    def __init__(self, broker: Broker):
        self._registry = {}
        self.broker = broker
        self.name = 'tasks'

    def task(self, max_retries=1):
        def decorator(func):
            if not inspect.iscoroutinefunction(func):
                # TODO: Add support for synchronous functions
                raise TypeError(f"{func.__name__}: only coroutine functions are allowed")
            name = f'{self.name}.{func.__module__}.{func.__name__}'
            task = Task(self, func, name, max_retries)
            self._registry[name] = task
            return task
        return decorator
