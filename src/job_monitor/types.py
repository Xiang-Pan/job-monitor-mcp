from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in (Status.DONE, Status.FAILED, Status.CANCELLED)


class Runner(str, Enum):
    SLURM = "slurm"
    LOCAL = "local"


class Job(BaseModel):
    id: str
    name: str = ""
    script: str
    args: list[str] = Field(default_factory=list)
    cwd: str = ""
    runner: Runner = Runner.LOCAL
    status: Status = Status.PENDING
    submitted_at: datetime = Field(default_factory=datetime.now)
    finished_at: datetime | None = None
    exit_code: int | None = None
    # runner-specific
    slurm_id: str | None = None
    pid: int | None = None
    stdout_path: str | None = None
    stderr_path: str | None = None
    meta: dict = Field(default_factory=dict)
