# orchestrator

Tiny Docker orchestrator: give it a JSON job, it runs each task in its own
**ephemeral container**, executes the task's shell steps in order, streams
every output line as NDJSON, and writes a JSON report. Built for small VMs
(~1 GiB RAM): ~2 MB binary, single-threaded async runtime, a few
MiB RSS, bounded output buffers, no `docker` CLI processes (talks to the
Engine API over `/var/run/docker.sock`).

## Isolation

Every task runs in a throwaway container that is isolated from the host and
from every other task. The workload inside (e.g. Goose) has full access to
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
docker compose --profile orchestrator build            # orchestrator + Goose runner images
docker compose --progress quiet run --rm -T orchestrator run /examples/hello.json \
  > report.json 2> events.ndjson
```

- `DOCKER_GID` is host-specific and is the usual cause of
  `docker unreachable`: on a Linux host (e.g. the Debian prod VM) use the
  output of `stat -c %g /var/run/docker.sock` run on that host; on Docker
  Desktop (macOS/Windows) use `0`. To check what containers actually see:
  `docker run --rm -v /var/run/docker.sock:/s alpine stat -c %g /s`.
- Put your own job files in [`jobs/`](jobs) → available at `/jobs/<file>.json`
  (git-ignored; temporary until jobs come from a database).
- To watch a run in the terminal instead, drop the redirects (events stream
  on stderr, the report prints on stdout at the end):

  ```sh
  docker compose --progress quiet run --rm -T orchestrator run /jobs/goose.json
  # report only, pretty-printed:
  docker compose --progress quiet run --rm -T orchestrator run /jobs/goose.json 2>/dev/null | jq
  # report on screen and saved to a file:
  docker compose --progress quiet run --rm -T orchestrator run /jobs/goose.json | tee report.json | jq
  ```
- How many tasks run at once is set on the orchestrator, not in the job:
  `MAX_PARALLEL` in `.env` (default 2) → `ORCHESTRATOR_MAX_PARALLEL`.
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
docker build -t hostprobe/goose-runner:latest runner     # Debian + Goose

./target/release/orchestrator run examples/hello.json            # report -> stdout, events -> stderr
OPENAI_API_KEY=... ./target/release/orchestrator run examples/goose.json \
    --max-parallel 1 --events run.ndjson --report report.json
```

```
orchestrator run <JOB|-> [--events <PATH|->] [--report <PATH|->]
                         [--pull missing|always|never] [--tail-bytes N]
                         [--max-parallel N]   # env ORCHESTRATOR_MAX_PARALLEL, default 2
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
  "defaults": {                    // any container field, inherited by tasks
    "image": "hostprobe/goose-runner:latest",
    "env": { "TZ": "UTC" },
    "env_passthrough": ["OPENAI_API_KEY"],  // copied from orchestrator env
    "workdir": "/workspace",
    "memory_mb": 256,              // hard RAM limit
    "swap_mb": 1024,               // swap on top of RAM (default 1024, 0 = none, max 2048)
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
        { "name": "code", "run": "goose run --no-session --quiet --provider openai --model gpt-6-luna --with-builtin developer -t 'write fizzbuzz.sh'",
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
it. `memory_mb` and `pids` must be > 0 (Docker reads 0 as "unlimited").
`swap_mb` only applies with `memory_mb`; it defaults to 1024 and is capped at
2048. Unknown fields are rejected, including `max_parallel`: parallelism is
host capacity, so it is set on the orchestrator (`--max-parallel` /
`ORCHESTRATOR_MAX_PARALLEL`), never by a job. A missing `env_passthrough`
variable is an input error.

### Examples

Smallest job (no agent, just shell):

```json
{ "id": "hello", "tasks": [{ "id": "t", "image": "debian:bookworm-slim", "steps": ["echo hi"] }] }
```

One Goose task, OpenAI:

```json
{
  "id": "fix",
  "defaults": { "image": "hostprobe/goose-runner:latest", "memory_mb": 256,
                "env_passthrough": ["OPENAI_API_KEY"], "timeout_secs": 900 },
  "tasks": [{ "id": "t", "steps": [
    "git clone --depth 1 https://github.com/you/repo . ",
    "goose run --no-session --quiet --provider openai --model gpt-6-luna --with-builtin developer -t 'Fix the failing test'"
  ]}]
}
```

Same task on an open-weight model: change the step's `--provider`/`--model`
and pass that provider's key, e.g. OpenRouter
(`"env_passthrough": ["OPENROUTER_API_KEY"]`,
`--provider openrouter --model qwen/qwen3-coder`) or a self-hosted
OpenAI-compatible server such as vLLM or llama.cpp
(`"env": {"OPENAI_HOST": "https://llm.example.com"}`, `--provider openai
--model <served-model>`). Any key a job passes through must also be listed
under `environment:` in [`compose.yaml`](compose.yaml).

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

[`runner/Dockerfile`](runner/Dockerfile) → `hostprobe/goose-runner`:
`debian:bookworm-slim` + git, curl, ripgrep, jq… + a pinned
[Goose](https://github.com/block/goose) (`--build-arg GOOSE_VERSION=…`),
running as root in `/workspace` (full access inside; see
[Isolation](#isolation)). It sets `GOOSE_MODE=auto` (tools run without
approval; nobody is there to approve) and `GOOSE_DISABLE_KEYRING=1` (keys come
from env vars). API keys are injected at run time via `env_passthrough`, never
baked into the image. Provider and model are picked per step with
`goose run --provider … --model …`: OpenAI, Anthropic, Gemini, OpenRouter,
Groq, Ollama or any OpenAI-compatible endpoint, so open-weight models work too.

### Why Goose (and not opencode)

Measured on Docker Desktop (arm64), same fizzbuzz task, same model
(`gpt-6-luna`), sampling the container's cgroup every 100 ms. "Process" is
anonymous memory (heap, cannot be reclaimed); "total" adds page cache (e.g.
the mapped binary, reclaimable).

| | opencode 1.19.4 | Goose 1.56.0 |
|---|---|---|
| Startup only, no API call (process) | ~390–420 MiB | — |
| Fizzbuzz task, peak process memory | **469 MiB** | **144 MiB** |
| Fizzbuzz task, peak total | 512 MiB (at the limit) | 157 MiB |
| Result at `memory_mb: 384` | killed (exit 137) | — |
| Result at `memory_mb: 256` (via orchestrator) | — | succeeded |

opencode is a JS app on the Bun runtime; `opencode run` boots its full agent
server before doing anything, and neither `BUN_OPTIONS=--smol` nor its
`OPENCODE_DISABLE_*` flags moved the number by more than ~10 MiB. Goose is a
native (Rust) binary, so one task fits in 256 MiB with headroom.

## Memory sizing on a 1 GiB VM

The orchestrator itself needs a few MiB (compose caps it at 64 MiB). Budget the rest as
`MAX_PARALLEL × memory_mb` + Docker daemon (~100 MiB) + OS. gVisor adds
roughly 20–50 MiB per running task. Goose tasks use `memory_mb: 256` (peak
~144 MiB on a small task, see [Why Goose](#why-goose-and-not-opencode)), so
`MAX_PARALLEL=2` uses ~512 MiB for tasks and fits a 1 GiB VM.

Each task may also use up to `swap_mb` (default 1 GiB) of swap beyond its RAM
limit. **The host must have swap for this to take effect**; on a Debian VM
without it, the limit is silently just `memory_mb`. Size the host's swap for
`MAX_PARALLEL × swap_mb`, e.g. a 2 GiB swapfile:

```sh
sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

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
