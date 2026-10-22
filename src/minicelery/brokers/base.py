from abc import ABC
from dataclasses import dataclass
from typing import Self, Sequence

from minicelery.message import TaskMessage

@dataclass(frozen=True)
class Delivery:
    message: TaskMessage
    queue: str # Task name
    receipt: str # Task number


class Broker(ABC):
    """Broker Abstract Base Class"""


    async def connect(self) -> None:
        """open connection"""

    async def close(self) -> None:
        """Close the connection"""

    async def publish(self, message: TaskMessage) -> None:
        """Publish a message"""

    async def consume(
            self,
            queues: Sequence[str],
            consumer: str,
            max_messages: int,
            timeout: float,
    ) -> list[Delivery]:
        """Consume messages"""

    async def __aenter__(self) -> Self:
        """Connect to the broker"""
        await self.connect()
        return self

    async def __aexit__(
            self, exc_type, exc_value, traceback
    ):
        """Disconnect from the broker"""
        await self.close()

