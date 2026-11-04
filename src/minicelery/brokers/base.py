from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Self, Sequence

from minicelery.message import TaskMessage, DeadTaskMessage


@dataclass(frozen=True)
class Delivery:
    message: TaskMessage
    queue: str # Stream key
    receipt: str # ID of the entry in the stream is needed for XACK


class Broker(ABC):
    """Broker Abstract Base Class"""

    @abstractmethod
    async def connect(self) -> None:
        """open connection"""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the connection"""

    @abstractmethod
    async def publish(self, message: TaskMessage) -> None:
        """Publish a message"""

    @abstractmethod
    async def consume(
            self,
            queues: Sequence[str],
            consumer: str,
            max_messages: int,
            timeout: float,
    ) -> list[Delivery]:
        """Consume messages"""

    @abstractmethod
    async def dead_letter(self, message: DeadTaskMessage):
        """Dead letter error"""

    @abstractmethod
    async def ack(self, delivery: Delivery) -> None:
        """Acknowledge and del message"""

    async def __aenter__(self) -> Self:
        """Connect to the broker"""
        await self.connect()
        return self

    async def __aexit__(
            self, exc_type, exc_value, traceback
    ):
        """Disconnect from the broker"""
        await self.disconnect()

