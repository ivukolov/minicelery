from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Self
from enum import StrEnum, auto

from pydantic import BaseModel, Field

from minicelery.config import settings


def _new_id() -> str:
    return uuid.uuid4().hex


def _now() -> datetime:
    return datetime.now(UTC)



class Reason(StrEnum):
    parse_error = auto()


class BaseMessage(BaseModel):

    def dumps(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def loads(cls, raw: bytes | str) -> Self:
        return cls.model_validate_json(raw)



class TaskMessage(BaseMessage):
    """What travels through the broker.
    Attributes:
        id: str id of the task
        task: str name of the task
        queue: str name of the queue
        attempt: int  The number of attempts
        created_at: datetime when the task was created
        args: any additional arguments
        kwargs: any additional keyword arguments
        """

    id: str = Field(default_factory=_new_id)
    task: str
    queue: str = "default"
    attempt: int = 0
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)


class DeadLetterMessage(BaseMessage):
    from_queue: str
    raw_data: str
    reason: Reason
    error: str
    attempt: int | None = None
    worker: str
    receipt_id: str
    failed_at: datetime = Field(default_factory=_now)
