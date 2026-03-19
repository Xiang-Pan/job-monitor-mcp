"""Persist job state to a JSON file on disk."""
from __future__ import annotations

import json
from pathlib import Path

from .types import Job, Status


class JobStore:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path or Path.home() / ".job-monitor" / "jobs.json")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._jobs: dict[str, Job] = {}
        self._load()

    # ---- CRUD -----------------------------------------------------------

    def add(self, job: Job) -> None:
        self._jobs[job.id] = job
        self._save()

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def update(self, job_id: str, **fields) -> None:
        job = self._jobs.get(job_id)
        if job is None:
            return
        for k, v in fields.items():
            setattr(job, k, v)
        self._save()

    def remove(self, job_id: str) -> None:
        self._jobs.pop(job_id, None)
        self._save()

    def list_jobs(self, status: Status | None = None) -> list[Job]:
        jobs = list(self._jobs.values())
        if status is not None:
            jobs = [j for j in jobs if j.status == status]
        return sorted(jobs, key=lambda j: j.submitted_at, reverse=True)

    # ---- persistence ----------------------------------------------------

    def _load(self) -> None:
        if self.path.exists():
            try:
                raw = json.loads(self.path.read_text())
                self._jobs = {k: Job.model_validate(v) for k, v in raw.items()}
            except (json.JSONDecodeError, Exception):
                self._jobs = {}

    def _save(self) -> None:
        data = {k: v.model_dump(mode="json") for k, v in self._jobs.items()}
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, default=str))
        tmp.replace(self.path)
