# orchestrator

Tiny Docker orchestrator: give it a JSON job, it runs each task in its own
**ephemeral container**, executes the task's shell steps in order, streams
every output line as NDJSON, and writes a JSON report. Built for small VMs
(~1 GiB RAM): ~2 MB binary, single-threaded async runtime, a few
MiB RSS, bounded output buffers, no `docker` CLI processes (talks to the
Engine API over `/var/run/docker.sock`).

## Quick start

```sh
cd apps/orchestrator
cargo build --release
docker build -t hostprobe/opencode-runner:latest runner   # Debian + opencode

./target/release/orchestrator run examples/hello.json            # report -> stdout, events -> stderr
ANTHROPIC_API_KEY=... ./target/release/orchestrator run examples/opencode.json \
    --events run.ndjson --report report.json
```

```
orchestrator run <JOB|-> [--events <PATH|->] [--report <PATH|->]
                         [--pull missing|always|never] [--tail-bytes N]
orchestrator validate <JOB|->
orchestrator cleanup <JOB_ID>        # remove leftover containers of a job
```

Exit codes: `0` all tasks succeeded · `1` some task failed · `2` invalid
input · `3` Docker/IO error · `130` interrupted (containers are removed).

## Job file

```jsonc
{
  "id": "demo",                    // [A-Za-z0-9_.-], default "job"
  "max_parallel": 2,               // containers alive at once, default 2
  "defaults": {                    // any container field, inherited by tasks
    "image": "hostprobe/opencode-runner:latest",
    "env": { "TZ": "UTC" },
    "env_passthrough": ["ANTHROPIC_API_KEY"],  // copied from orchestrator env
    "workdir": "/workspace",
    "network": "bridge",           // docker network mode
    "memory_mb": 384,              // hard limit, swap disabled
    "cpus": 1.0,
    "pids": 512,
    "timeout_secs": 600            // per step
  },
  "tasks": [
    {
      "id": "fizzbuzz",
      "image": "...",              // task fields override defaults; env maps merge
      "steps": [
        "echo plain string steps are fine",
        { "name": "code", "run": "opencode run 'write fizzbuzz.sh'",
          "timeout_secs": 900, "continue_on_error": false }
      ]
    }
  ]
}
```

Steps run with `/bin/sh -c` in the same container, so files persist between
steps of a task. A failing step (non-zero exit, timeout, runtime error) skips
the rest of the task unless `continue_on_error` is set. Unknown fields are
rejected. A missing `env_passthrough` variable is an input error.

## Events (NDJSON)

One JSON object per line, flushed per line:

```json
{"ts_ms":1792955030243,"event":"job_started","job":"smoke","tasks":4}
{"ts_ms":1792955030243,"event":"task_started","task":"ok","image":"debian:bookworm-slim"}
{"ts_ms":1792955030653,"event":"container_ready","task":"ok","container_id":"300f..."}
{"ts_ms":1792955030653,"event":"step_started","task":"ok","step":0,"name":"step-0"}
{"ts_ms":1792955030709,"event":"output","task":"ok","step":0,"stream":"stdout","line":"hola"}
{"ts_ms":1792955030713,"event":"step_finished","task":"ok","step":0,"status":"succeeded","exit_code":0,"duration_ms":60}
{"ts_ms":1792955031000,"event":"task_finished","task":"ok","status":"succeeded","duration_ms":756}
{"ts_ms":1792955032346,"event":"job_finished","job":"smoke","succeeded":false,"duration_ms":2102}
```

Also `warning` (`message`, optional `task`). Step `status`: `succeeded`,
`failed` (+`exit_code`), `timed_out`, `skipped`, `error` (+`error`).
Task `status`: `succeeded`, `failed`, `error` (+`error`, e.g. image pull failed).

## Report (JSON)

```json
{
  "id": "smoke", "succeeded": false, "duration_ms": 2102,
  "tasks": [{
    "id": "ok", "image": "debian:bookworm-slim", "status": "succeeded",
    "container_id": "300f...", "duration_ms": 756,
    "steps": [{
      "name": "step-0", "status": "succeeded", "exit_code": 0, "duration_ms": 60,
      "stdout_tail": "hola\n/tmp\n", "stderr_tail": "err\n",
      "stdout_bytes": 10, "stderr_bytes": 4
    }]
  }]
}
```

`*_tail` holds the last `--tail-bytes` (default 16 KiB) of each stream;
`*_bytes` is the full size. The complete output is in the events stream.

## Runner image

[`runner/Dockerfile`](runner/Dockerfile): `debian:bookworm-slim` + git, curl,
ripgrep, jq… + a pinned `opencode` (`--build-arg OPENCODE_VERSION=…`), running
as unprivileged user `agent` in `/workspace`. API keys are injected at run
time via `env_passthrough`, never baked into the image.

## Memory sizing on a 1 GiB VM

The orchestrator itself needs a few MiB. Budget the rest as
`max_parallel × memory_mb` + Docker daemon (~100 MiB) + OS. E.g.
`max_parallel: 2`, `memory_mb: 384` leaves comfortable headroom.

## Architecture

Clean Architecture, one crate per circle; see [AGENTS.md](AGENTS.md).
