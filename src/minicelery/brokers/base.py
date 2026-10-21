from abc import ABC
from typing import Self

from minicelery.message import TaskMessage


class Broker(ABC):
    """Broker Abstract Base Class"""


    async def connect(self) -> None:
        """open connection"""

    async def close(self) -> None:
        """Close the connection"""

    async def publish(self, message: TaskMessage) -> None:
        """Publish a message"""

    async def __aenter__(self) -> Self:
        """Connect to the broker"""
        await self.connect()
        return self

    async def __aexit__(
            self, exc_type, exc_value, traceback
    ):
        """Disconnect from the broker"""
        await self.close()

