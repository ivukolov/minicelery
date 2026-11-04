from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Self
from enum import StrEnum, auto

from pydantic import BaseModel, Field

def _new_id() -> str:
    return uuid.uuid4().hex

def _now() -> datetime:
    return datetime.now(UTC)


class Reason(StrEnum):
    parse_error = auto()


class BaseMessage(BaseModel):

    def dumps(self) -> bytes:
        return self.model_dump_json().encode()


class TaskMessage(BaseMessage):
    """A task as it travels through the broker.

    Attributes:
        id: unique id of this task, used to find its result
        task: registered name of the task function
        queue: name of the queue the task is sent to
        attempt: how many times execution was already attempted (0 for the first run)
        args: positional arguments for the task function, must be JSON-serializable
        kwargs: keyword arguments for the task function, must be JSON-serializable
        created_at: when the task was created
    """

    id: str = Field(default_factory=_new_id)
    task: str
    queue: str = "default"
    attempt: int = 0
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)

    @classmethod
    def loads(cls, raw: bytes | str) -> Self:
        return cls.model_validate_json(raw)



class DeadTaskMessage(BaseMessage):
    """A record about a message that could not be processed (dead letter).

    Attributes:
        from_queue: short name of the queue the message came from (e.g. "default")
        raw_data: original stream fields as received, e.g. {"data": "<json>"}
        reason: why the message was moved here (parse error, unknown task, task failed)
        error: short error text, without the traceback or the task data
        attempt: attempt number of the task; None if unknown (e.g. unparsable message)
        worker: name of the worker that received the message
        receipt_id: id of the entry in the source stream, as given by the broker
        failed_at: when the message was moved to the dead letter stream
    """

    from_queue: str
    raw_data: dict[str, str]
    reason: Reason
    error: str
    attempt: int | None = None
    worker: str
    receipt_id: str
    failed_at: datetime = Field(default_factory=_now)

