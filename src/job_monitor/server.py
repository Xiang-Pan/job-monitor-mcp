"""MCP server — tools for submitting, monitoring, and waking up on completion."""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import SamplingMessage, TextContent

from .monitor import JobMonitor
from .runners import detect_runner, get_runner
from .store import JobStore
from .types import Job, Runner, Status

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

store = JobStore(os.environ.get("JOB_MONITOR_STORE"))
monitor = JobMonitor(
    store,
    poll_interval=int(os.environ.get("JOB_MONITOR_POLL", "15")),
)

mcp = FastMCP(
    "job-monitor",
    instructions=(
        "Submit, monitor, and manage compute jobs (SLURM or local). "
        "Notifies when jobs complete via MCP sampling."
    ),
)

# Keep a reference to the session so background tasks can notify.
_session = None


# ---------------------------------------------------------------------------
# Wake-up callback
# ---------------------------------------------------------------------------


async def _on_complete(job_id: str, status: str, exit_code: int | None) -> None:
    job = store.get(job_id)
    if not job:
        return

    summary = (
        f"Job '{job.name}' ({job.id}) finished.\n"
        f"  Status:    {status}\n"
        f"  Exit code: {exit_code}\n"
        f"  Script:    {job.script}\n"
        f"  Runtime:   {_duration(job.submitted_at, job.finished_at)}\n"
    )

    if _session is None:
        return

    # 1. Try sampling — this wakes the model up to act on the result.
    try:
        await _session.create_message(
            messages=[
                SamplingMessage(
                    role="user",
                    content=TextContent(type="text", text=summary),
                )
            ],
            max_tokens=1024,
        )
        return
    except Exception:
        pass

    # 2. Fallback: log notification (shows in client, doesn't prompt model).
    try:
        await _session.send_log_message(level="info", data=summary)
    except Exception:
        pass


monitor.on_complete(_on_complete)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _capture_session(ctx: Context | None) -> None:
    global _session
    if ctx and hasattr(ctx, "session"):
        _session = ctx.session


def _resume_monitors() -> None:
    """Re-watch any jobs that are still running (survives server restart)."""
    for job in store.list_jobs(Status.RUNNING):
        monitor.watch(job.id)
    for job in store.list_jobs(Status.PENDING):
        monitor.watch(job.id)


_resumed = False


def _ensure_resumed() -> None:
    global _resumed
    if not _resumed:
        _resumed = True
        _resume_monitors()


def _duration(start: datetime, end: datetime | None) -> str:
    if not end:
        return "-"
    s = int((end - start).total_seconds())
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def submit_job(
    script: str,
    args: str = "",
    name: str = "",
    cwd: str = "",
    runner: str = "auto",
    sbatch_args: str = "",
    ctx: Context = None,
) -> str:
    """Submit a script for execution and monitor it in the background.

    Args:
        script: Path to the script to run.
        args: Space-separated arguments for the script.
        name: Human-readable label (defaults to script stem).
        cwd: Working directory (defaults to server cwd).
        runner: "slurm", "local", or "auto" (auto-detects SLURM).
        sbatch_args: Extra sbatch flags, e.g. "--gres=gpu:1 --time=1:00:00".
    """
    _capture_session(ctx)
    _ensure_resumed()

    runner_type = runner if runner != "auto" else detect_runner()
    work_dir = cwd or os.getcwd()
    arg_list = args.split() if args else []
    sb_list = sbatch_args.split() if sbatch_args else []
    label = name or Path(script).stem

    impl = get_runner(runner_type)
    result = await impl.submit(
        script, arg_list, work_dir, sbatch_args=sb_list, name=label
    )

    job_id = result.get("slurm_id") or result.get("local_id", "")
    job = Job(
        id=job_id,
        name=label,
        script=script,
        args=arg_list,
        cwd=work_dir,
        runner=Runner(runner_type),
        status=Status.RUNNING,
        submitted_at=datetime.now(),
        slurm_id=result.get("slurm_id"),
        pid=result.get("pid"),
        stdout_path=result.get("stdout_path"),
        stderr_path=result.get("stderr_path"),
        meta={
            k: v
            for k, v in result.items()
            if k not in ("slurm_id", "pid", "stdout_path", "stderr_path")
        },
    )
    store.add(job)
    monitor.watch(job_id)
    return f"Submitted '{label}' -> {runner_type}:{job_id}"


@mcp.tool()
async def list_jobs(status: str = "", ctx: Context = None) -> str:
    """List tracked jobs, optionally filtered by status.

    Args:
        status: pending | running | done | failed | cancelled (empty = all).
    """
    _capture_session(ctx)
    _ensure_resumed()

    filt = Status(status) if status else None
    jobs = store.list_jobs(filt)
    if not jobs:
        return "No jobs."

    lines = []
    for j in jobs:
        dur = _duration(j.submitted_at, j.finished_at or datetime.now())
        lines.append(
            f"[{j.status.value:>9}] {j.id:<12} {j.name:<24} {dur:>8}  {j.script}"
        )
    return "\n".join(lines)


@mcp.tool()
async def job_detail(job_id: str, ctx: Context = None) -> str:
    """Get full details of a job."""
    _capture_session(ctx)
    _ensure_resumed()

    job = store.get(job_id)
    if not job:
        return f"Job {job_id} not found."

    return "\n".join([
        f"Job:       {job.name} ({job.id})",
        f"Script:    {job.script} {' '.join(job.args)}",
        f"Runner:    {job.runner.value}",
        f"Status:    {job.status.value}",
        f"Submitted: {job.submitted_at}",
        f"Finished:  {job.finished_at or '-'}",
        f"Exit code: {job.exit_code if job.exit_code is not None else '-'}",
        f"CWD:       {job.cwd}",
        f"Stdout:    {job.stdout_path or '-'}",
        f"Stderr:    {job.stderr_path or '-'}",
    ])


@mcp.tool()
async def read_logs(
    job_id: str, stream: str = "stdout", tail: int = 80, ctx: Context = None
) -> str:
    """Read stdout or stderr of a job.

    Args:
        job_id: Job ID.
        stream: "stdout" or "stderr".
        tail: Number of lines from the end.
    """
    _capture_session(ctx)
    job = store.get(job_id)
    if not job:
        return f"Job {job_id} not found."

    impl = get_runner(job.runner.value)
    info = {
        "slurm_id": job.slurm_id,
        "pid": job.pid,
        "local_id": job.meta.get("local_id"),
        "stdout_path": job.stdout_path,
        "stderr_path": job.stderr_path,
    }
    return await impl.read_log(info, stream, tail)


@mcp.tool()
async def cancel_job(job_id: str, ctx: Context = None) -> str:
    """Cancel a running job."""
    _capture_session(ctx)
    job = store.get(job_id)
    if not job:
        return f"Job {job_id} not found."
    if job.status.terminal:
        return f"Job already {job.status.value}."

    impl = get_runner(job.runner.value)
    info = {"slurm_id": job.slurm_id, "pid": job.pid, "local_id": job.meta.get("local_id")}
    ok = await impl.cancel(info)

    if ok:
        store.update(job_id, status=Status.CANCELLED, finished_at=datetime.now())
        monitor.stop(job_id)
        return f"Cancelled {job_id}."
    return f"Failed to cancel {job_id}."


@mcp.tool()
async def wait_for_job(job_id: str, timeout: int = 3600, ctx: Context = None) -> str:
    """Block until a job finishes. Fallback when sampling is not supported.

    Args:
        job_id: Job ID.
        timeout: Max wait in seconds (default 3600).
    """
    import asyncio

    _capture_session(ctx)
    job = store.get(job_id)
    if not job:
        return f"Job {job_id} not found."
    if job.status.terminal:
        return await job_detail(job_id)

    elapsed = 0
    while elapsed < timeout:
        if ctx:
            await ctx.report_progress(elapsed, timeout)
        await asyncio.sleep(10)
        elapsed += 10
        job = store.get(job_id)
        if job and job.status.terminal:
            return await job_detail(job_id)

    return f"Timeout after {timeout}s — job still {job.status.value}."


@mcp.tool()
async def cleanup(status: str = "done", ctx: Context = None) -> str:
    """Remove tracked jobs.

    Args:
        status: Remove jobs with this status. Use "all" for everything.
    """
    _capture_session(ctx)
    jobs = store.list_jobs() if status == "all" else store.list_jobs(Status(status))
    for j in jobs:
        monitor.stop(j.id)
        store.remove(j.id)
    return f"Removed {len(jobs)} job(s)."


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    mcp.run()


if __name__ == "__main__":
    main()
