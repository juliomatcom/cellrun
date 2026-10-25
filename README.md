# orchestrator

Tiny Docker orchestrator: give it a JSON job, it runs each task in its own
**ephemeral container**, executes the task's shell steps in order, streams
every output line as NDJSON, and writes a JSON report. Built for small VMs
(~1 GiB RAM): ~2 MB binary, single-threaded async runtime, a few
MiB RSS, bounded output buffers, no `docker` CLI processes (talks to the
Engine API over `/var/run/docker.sock`).

## Isolation

Every task runs in a throwaway container that is isolated from the host and
from every other task. The workload inside (e.g. opencode) has full access to
its own container: root, a writable filesystem and the public internet.

- **Own network per task:** each container is the only member of its own
  bridge network, so tasks cannot see each other.
- **No host access:** `sandbox-guard` installs host firewall rules on those
  bridges (`hp*` interfaces). Tasks cannot reach any host address, the LAN,
  cloud metadata (169.254.169.254) or other containers. Public internet still
  works.
- **Nothing shared:** no bind mounts, no volumes and no Docker socket.
  Containers and networks are deleted when the task ends.
- **Separate kernel (optional, recommended):** set `TASK_RUNTIME=runsc` to run
  tasks under [gVisor](https://gvisor.dev). Root inside the container then
  never touches the host kernel directly. Install `runsc` on the host and
  register it with Docker first (`runsc install && systemctl restart docker`).
- **Not configurable from job files:** jobs cannot change networking,
  mounts or privileges.

## Quick start (Docker Compose, recommended)

From the repo root:

```sh
cp .env.example .env    # set DOCKER_GID (stat -c %g /var/run/docker.sock) and API keys
docker compose --profile orchestrator build            # orchestrator + opencode runner images
docker compose --progress quiet run --rm -T orchestrator run /examples/hello.json \
  > report.json 2> events.ndjson
```

- Put your own job files in [`jobs/`](jobs) → available at `/jobs/<file>.json`.
- `-T` keeps stdout (report) and stderr (events) separate; `--progress quiet`
  keeps Compose's own messages out of the event stream.
- Variables listed in the job's `env_passthrough` must be set in `.env` (and
  listed under `environment:` in [`compose.yaml`](compose.yaml)); empty
  values are rejected.
- `sandbox-guard` runs first automatically and installs the firewall rules
  (idempotent). The orchestrator drives the host's Docker daemon via the
  mounted socket as a non-root user in the docker group, and sits on the
  shared `hostprobe-backend` network to reach services such as an event bus
  or database. Task containers cannot reach that network. `docker stop` / Ctrl-C removes all task containers and networks.

## Quick start (bare binary)

```sh
sudo sandbox-guard/guard.sh apply          # host firewall for task networks (once per boot)
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
orchestrator cleanup <JOB_ID>        # remove leftovers of a job (ALL runs of that id)
```

Exit codes: `0` all tasks succeeded · `1` some task failed · `2` invalid
input · `3` Docker/IO error · `130` interrupted (this run's containers and
networks are removed; concurrent runs of the same job are not touched).

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
steps of a task. A failing step (non-zero exit, runtime error) skips the rest
of the task unless `continue_on_error` is set. A **timeout always fails and
halts the task**, even with `continue_on_error`: the timed-out process cannot
be stopped without destroying the container, so later steps would race with
it. `memory_mb` and `pids` must be > 0 (Docker reads 0 as "unlimited"). Unknown fields are
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
as root in `/workspace` (full access inside; see [Isolation](#isolation)). API
keys are injected at run time via `env_passthrough`, never baked into the
image.

## Memory sizing on a 1 GiB VM

The orchestrator itself needs a few MiB (compose caps it at 64 MiB). Budget the rest as
`max_parallel × memory_mb` + Docker daemon (~100 MiB) + OS. gVisor adds
roughly 20–50 MiB per running task. E.g.
`max_parallel: 2`, `memory_mb: 384` leaves comfortable headroom.

## Concurrency limit: Docker address pools

Each running task owns one network. Docker's default address pools allow
only about 30 networks on a host (including compose networks), so more
concurrent tasks fail with `address pools are exhausted`. To raise the
ceiling, hand out small subnets in `/etc/docker/daemon.json` and restart
Docker:

```json
{ "default-address-pools": [ { "base": "10.200.0.0/16", "size": 24 } ] }
```

That allows 256 networks. Task networks stay isolated whatever their subnet.

## Architecture

Clean Architecture, one crate per circle; see [AGENTS.md](AGENTS.md).
