"""Pluggable job runners: SLURM and local subprocess."""
from __future__ import annotations

import asyncio
import os
import shutil
import signal
import uuid
from abc import ABC, abstractmethod
from pathlib import Path


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class BaseRunner(ABC):
    @abstractmethod
    async def submit(
        self, script: str, args: list[str], cwd: str, **kw
    ) -> dict:
        """Start execution. Return dict with runner-specific keys."""

    @abstractmethod
    async def poll(self, info: dict) -> tuple[str, int | None]:
        """Return (status_str, exit_code_or_None)."""

    @abstractmethod
    async def cancel(self, info: dict) -> bool:
        ...

    @abstractmethod
    async def read_log(self, info: dict, stream: str, tail: int) -> str:
        ...


# ---------------------------------------------------------------------------
# SLURM
# ---------------------------------------------------------------------------

class SlurmRunner(BaseRunner):
    async def submit(self, script, args, cwd, **kw):
        sbatch_args: list[str] = kw.get("sbatch_args", [])
        name = kw.get("name", Path(script).stem)
        log_dir = Path(cwd) / ".job-monitor" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        base_flags = [
            f"--job-name={name}",
            f"--output={log_dir}/%j.out",
            f"--error={log_dir}/%j.err",
        ]

        # .sh / .sbatch → direct submission; else wrap with python
        if script.endswith((".sh", ".sbatch", ".slurm")):
            cmd = ["sbatch", *base_flags, *sbatch_args, script, *args]
        else:
            wrap = f"python {script} {' '.join(args)}"
            cmd = ["sbatch", *base_flags, *sbatch_args, f"--wrap={wrap}"]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"sbatch failed: {stderr.decode().strip()}")

        slurm_id = stdout.decode().strip().split()[-1]
        return {
            "slurm_id": slurm_id,
            "stdout_path": str(log_dir / f"{slurm_id}.out"),
            "stderr_path": str(log_dir / f"{slurm_id}.err"),
        }

    async def poll(self, info):
        slurm_id = info["slurm_id"]
        proc = await asyncio.create_subprocess_exec(
            "sacct", "-j", slurm_id,
            "--format=State", "--noheader", "--parsable2",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        lines = [l.strip() for l in stdout.decode().strip().splitlines() if l.strip()]
        state = lines[0] if lines else "UNKNOWN"
        # sacct may append "+", strip it
        state = state.rstrip("+").strip()

        mapping = {
            "PENDING": ("pending", None),
            "RUNNING": ("running", None),
            "COMPLETED": ("done", 0),
            "FAILED": ("failed", 1),
            "CANCELLED": ("cancelled", None),
            "TIMEOUT": ("failed", 1),
            "NODE_FAIL": ("failed", 1),
            "OUT_OF_MEMORY": ("failed", 1),
        }
        return mapping.get(state, ("running", None))

    async def cancel(self, info):
        proc = await asyncio.create_subprocess_exec("scancel", info["slurm_id"])
        await proc.communicate()
        return proc.returncode == 0

    async def read_log(self, info, stream="stdout", tail=50):
        path = info.get(f"{stream}_path")
        return _tail_file(path, tail)


# ---------------------------------------------------------------------------
# Local subprocess
# ---------------------------------------------------------------------------

class LocalRunner(BaseRunner):
    _procs: dict[str, asyncio.subprocess.Process] = {}

    async def submit(self, script, args, cwd, **kw):
        uid = uuid.uuid4().hex[:8]
        log_dir = Path(cwd) / ".job-monitor" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        out = log_dir / f"{uid}.out"
        err = log_dir / f"{uid}.err"

        out_f = open(out, "w")
        err_f = open(err, "w")

        # pick interpreter
        if script.endswith(".py"):
            cmd = ["python", script, *args]
        elif script.endswith(".sh"):
            cmd = ["bash", script, *args]
        else:
            cmd = [script, *args]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            stdout=out_f,
            stderr=err_f,
        )
        LocalRunner._procs[uid] = proc
        return {
            "local_id": uid,
            "pid": proc.pid,
            "stdout_path": str(out),
            "stderr_path": str(err),
        }

    async def poll(self, info):
        uid = info.get("local_id", "")
        proc = LocalRunner._procs.get(uid)
        if proc is None:
            # server restarted — check pid
            pid = info.get("pid")
            if pid:
                try:
                    os.kill(pid, 0)
                    return "running", None
                except ProcessLookupError:
                    return "done", 0
            return "done", None

        if proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                return "running", None

        code = proc.returncode
        return ("done" if code == 0 else "failed"), code

    async def cancel(self, info):
        uid = info.get("local_id", "")
        proc = LocalRunner._procs.get(uid)
        if proc:
            proc.kill()
            return True
        pid = info.get("pid")
        if pid:
            try:
                os.kill(pid, signal.SIGTERM)
                return True
            except ProcessLookupError:
                return False
        return False

    async def read_log(self, info, stream="stdout", tail=50):
        path = info.get(f"{stream}_path")
        return _tail_file(path, tail)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tail_file(path: str | None, n: int) -> str:
    if not path or not Path(path).exists():
        return "(log file not found)"
    lines = Path(path).read_text().splitlines()
    return "\n".join(lines[-n:])


def detect_runner() -> str:
    return "slurm" if shutil.which("sbatch") else "local"


def get_runner(name: str) -> BaseRunner:
    if name == "slurm":
        return SlurmRunner()
    return LocalRunner()
