from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from minicelery.config import settings


def _new_id() -> str:
    return uuid.uuid4().hex


def _now() -> datetime:
    return datetime.now(UTC)


class TaskMessage(BaseModel):
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
    queue: str = settings.queue
    attempt: int = 0
    created_at: datetime = Field(default_factory=_now)
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)

    def dumps(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def loads(cls, raw: bytes | str) -> TaskMessage:
        return cls.model_validate_json(raw)
