"""Smoke test: submit a local job, monitor it, verify completion."""
import asyncio
import tempfile
from pathlib import Path

from job_monitor.monitor import JobMonitor
from job_monitor.runners import LocalRunner
from job_monitor.store import JobStore
from job_monitor.types import Job, Runner, Status


async def main():
    with tempfile.TemporaryDirectory() as tmp:
        store = JobStore(Path(tmp) / "jobs.json")
        monitor = JobMonitor(store, poll_interval=2)

        completed = asyncio.Event()
        results = {}

        async def on_complete(job_id, status, exit_code):
            results["job_id"] = job_id
            results["status"] = status
            results["exit_code"] = exit_code
            completed.set()

        monitor.on_complete(on_complete)

        # --- submit a trivial job ---
        runner = LocalRunner()
        info = await runner.submit(
            "python",
            ["-c", "import time; time.sleep(2); print('done')"],
            cwd=tmp,
        )
        print(f"1. Submitted: {info}")

        job = Job(
            id=info["local_id"],
            name="smoke-test",
            script="python",
            args=["-c", "..."],
            cwd=tmp,
            runner=Runner.LOCAL,
            status=Status.RUNNING,
            pid=info["pid"],
            stdout_path=info["stdout_path"],
            stderr_path=info["stderr_path"],
            meta={"local_id": info["local_id"]},
        )
        store.add(job)
        monitor.watch(job.id)
        print(f"2. Monitoring job {job.id}...")

        # --- wait for completion callback ---
        await asyncio.wait_for(completed.wait(), timeout=30)
        print(f"3. Callback fired: {results}")

        # --- verify store was updated ---
        updated = store.get(job.id)
        assert updated.status == Status.DONE, f"expected done, got {updated.status}"
        assert updated.exit_code == 0
        assert updated.finished_at is not None

        # --- read logs ---
        log = await runner.read_log(info, "stdout", 10)
        assert "done" in log
        print(f"4. Stdout: {log.strip()}")

        # --- list / cleanup ---
        jobs = store.list_jobs(Status.DONE)
        assert len(jobs) == 1
        store.remove(job.id)
        assert store.list_jobs() == []
        print("5. Store cleanup OK")

        monitor.stop()
        print("\nAll checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
