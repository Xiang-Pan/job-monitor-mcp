"""Background monitor that polls jobs and fires a callback on completion."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Awaitable, Callable

from .runners import get_runner
from .store import JobStore
from .types import Status

logger = logging.getLogger(__name__)

OnComplete = Callable[[str, str, int | None], Awaitable[None]]
# (job_id, status_str, exit_code)


class JobMonitor:
    def __init__(self, store: JobStore, poll_interval: int = 15):
        self.store = store
        self.poll_interval = poll_interval
        self._tasks: dict[str, asyncio.Task] = {}
        self._on_complete: OnComplete | None = None

    def on_complete(self, cb: OnComplete) -> None:
        self._on_complete = cb

    def watch(self, job_id: str) -> None:
        if job_id not in self._tasks:
            self._tasks[job_id] = asyncio.create_task(self._loop(job_id))

    def stop(self, job_id: str | None = None) -> None:
        if job_id:
            task = self._tasks.pop(job_id, None)
            if task:
                task.cancel()
        else:
            for t in self._tasks.values():
                t.cancel()
            self._tasks.clear()

    # ---- internal -------------------------------------------------------

    async def _loop(self, job_id: str) -> None:
        try:
            while True:
                job = self.store.get(job_id)
                if job is None or job.status.terminal:
                    break

                runner = get_runner(job.runner.value)
                info = {
                    "slurm_id": job.slurm_id,
                    "pid": job.pid,
                    "local_id": job.meta.get("local_id"),
                    "stdout_path": job.stdout_path,
                    "stderr_path": job.stderr_path,
                }

                try:
                    status_str, exit_code = await runner.poll(info)
                except Exception as exc:
                    logger.warning("poll error for %s: %s", job_id, exc)
                    await asyncio.sleep(self.poll_interval)
                    continue

                if status_str != job.status.value:
                    updates: dict = {"status": Status(status_str)}
                    if exit_code is not None:
                        updates["exit_code"] = exit_code
                    if Status(status_str).terminal:
                        updates["finished_at"] = datetime.now()
                    self.store.update(job_id, **updates)

                    if Status(status_str).terminal and self._on_complete:
                        await self._on_complete(job_id, status_str, exit_code)
                        break

                await asyncio.sleep(self.poll_interval)
        finally:
            self._tasks.pop(job_id, None)
