# job-monitor-mcp

MCP server that lets AI models submit, monitor, and get notified when compute jobs finish.

Works with **SLURM** clusters and **local** subprocesses. Auto-detects which runner to use.

## Install

```bash
pip install -e .
```

## Use with Claude Code

Add to your Claude Code settings (`~/.claude/settings.json`):

```json
{
  "mcpServers": {
    "job-monitor": {
      "command": "job-monitor"
    }
  }
}
```

Then ask Claude to run experiments — it can submit jobs, check status, read logs, and gets woken up when jobs finish.

## Tools

| Tool | Description |
|---|---|
| `submit_job` | Submit a script (SLURM or local). Starts background monitoring. |
| `list_jobs` | List tracked jobs, filterable by status. |
| `job_detail` | Full details of a specific job. |
| `read_logs` | Tail stdout/stderr of any job. |
| `cancel_job` | Cancel a running job. |
| `wait_for_job` | Block until a job finishes (fallback for clients without sampling). |
| `cleanup` | Remove finished jobs from tracking. |

## How the wake-up works

```
Model                    MCP Server                  SLURM / Local
  │── submit_job() ────────▶│── sbatch / python ────────▶│
  │◀── job_id ──────────────│                            │
  │                          │     (polls every 15s)      │
  │                          │◀── job finishes ───────────│
  │◀── sampling request ────│                            │
  │   "Job X done. ..."     │                            │
```

When a job finishes, the server requests **MCP sampling** (`create_message`) to prompt the model with the results. Falls back to `send_log_message` if the client doesn't support sampling.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `JOB_MONITOR_STORE` | `~/.job-monitor/jobs.json` | Path to job state file |
| `JOB_MONITOR_POLL` | `15` | Poll interval in seconds |

## Example

```
You:    Run exp_causal_geometry.py on SLURM with --gres=gpu:1
Claude: [calls submit_job] → Submitted 'exp_causal_geometry' -> slurm:769154

        ... time passes ...

MCP:    Job 'exp_causal_geometry' (769154) finished.
        Status: done, Exit code: 0, Runtime: 8m53s

Claude: The job completed successfully. Let me read the results...
        [calls read_logs]
```

## Test

```bash
python tests/test_smoke.py
```
