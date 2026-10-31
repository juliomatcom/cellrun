# cellrun

Tiny Docker job dispatcher: give it a JSON job, it runs each task in its own
**ephemeral container**, executes the task's shell steps in order, streams
every output line as NDJSON, and writes a JSON report. Built for small VMs
(~1 GiB RAM): one ~2 MB binary (`dispatcher`), single-threaded async runtime, a few
MiB RSS, bounded output buffers, no `docker` CLI processes (talks to the
Engine API over `/var/run/docker.sock`).

Cellrun is only the engine: no compose file, images, services or firewall.
Projects that use it build the binary into their own image and own the host
setup (see "Host firewall" and "Using cellrun from another project").

## Isolation

Every task runs in a throwaway container. The workload inside (e.g. a coding
agent) has full access to its own container: root, a writable filesystem and
the public internet.

**What cellrun does:**

- **Own network per task:** each container is the only member of its own
  bridge network (`<bridge-prefix><hex>`, `cr<hex>` by default), so tasks
  cannot see each other or other Docker networks.
- **Nothing shared:** no bind mounts, no volumes and no Docker socket.
  Containers and networks are deleted when the task ends.
- **Not configurable from job files:** jobs cannot change networking,
  mounts or privileges.
- **Separate kernel (optional, recommended):** `--task-runtime runsc` runs
  tasks under [gVisor](https://gvisor.dev), so root inside the container never
  touches the host kernel directly (install `runsc` and register it with
  Docker first: `runsc install && systemctl restart docker`).

**What cellrun does NOT do: the host firewall.** See below. Without it, a task
can reach the host itself, your LAN and cloud metadata.

## Host firewall (your responsibility)

> [!IMPORTANT]
> Cellrun does not install firewall rules and does not test them. Whoever runs
> the dispatcher must apply them **before any task starts, after every boot**
> (iptables rules are not persistent), and should test them on that host.

Required rules, for the bridge prefix the dispatcher uses (`--bridge-prefix`,
here `cr`; `cr+` matches every task bridge):

```sh
# 1. Tasks cannot reach any address of the host (bridge gateway, docker0, LAN/public IPs).
iptables -I INPUT 1 -i cr+ -j DROP
# 2. Tasks cannot reach private, link-local (cloud metadata), CGNAT, multicast ranges.
iptables -I DOCKER-USER 1 -i cr+ \
  -d 10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16,100.64.0.0/10,0.0.0.0/8,224.0.0.0/4,240.0.0.0/4 \
  -j DROP
# 3. IPv6 (task networks have it disabled; defense in depth).
ip6tables -I INPUT 1 -i cr+ -j DROP
ip6tables -I DOCKER-USER 1 -i cr+ -j DROP
```

Use the backend Docker programs (`iptables-nft` or `iptables-legacy`: the one
whose `DOCKER-USER` chain exists), insert each rule only once, and keep
public internet open (tasks need it). **Check it works:** from a task, the
host, a LAN address and `169.254.169.254` must time out, while e.g.
`https://example.com` still loads.

## Quick start

```sh
# apply the host firewall above (once per boot), then:
cargo build --release

./target/release/dispatcher run examples/hello.json            # report -> stdout, events -> stderr
API_KEY=... ./target/release/dispatcher run my-job.json \
    --max-parallel 1 --events run.ndjson --report report.json
```

```
dispatcher run <JOB|-> [--events <PATH|->] [--report <PATH|->]
                       [--pull missing|always|never] [--tail-bytes N]
                       [--max-line-bytes N]  # env DISPATCHER_MAX_LINE_BYTES, default 8192
                       [--max-parallel N]   # env DISPATCHER_MAX_PARALLEL, default 2
dispatcher validate <JOB|->
dispatcher cleanup <JOB_ID>          # remove leftovers of a job (ALL runs of that id)
dispatcher cleanup --all             # remove leftovers of every job with this label prefix

Host settings (flag / env, never from job files):
  --task-runtime  DISPATCHER_TASK_RUNTIME   OCI runtime for tasks, e.g. runsc (gVisor)
  --task-dns      DISPATCHER_TASK_DNS       public DNS for tasks (default 1.1.1.1,8.8.8.8)
  --label-prefix  DISPATCHER_LABEL_PREFIX   labels <prefix>.dispatcher/.job/.task/.run (default cellrun)
  --bridge-prefix DISPATCHER_BRIDGE_PREFIX  task bridges <prefix><hex> (default cr; 1-2 chars [a-z0-9])
```

Exit codes: `0` all tasks succeeded · `1` some task failed · `2` invalid
input · `3` Docker/IO error · `130` interrupted (this run's containers and
networks are removed; concurrent runs of the same job are not touched).

## Job file

```jsonc
{
  "id": "demo",                    // [A-Za-z0-9_.-], default "job"
  "defaults": {                    // any container field, inherited by tasks
    "image": "my-org/my-runner:latest",   // any image; cellrun ships none
    "env": { "TZ": "UTC" },
    "env_passthrough": ["API_KEY"],  // copied from the dispatcher env
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
        { "name": "build", "run": "make all",
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
host capacity, so it is set on the dispatcher (`--max-parallel` /
`DISPATCHER_MAX_PARALLEL`), never by a job. Variables listed in
`env_passthrough` are copied from the dispatcher's environment (keeps secrets
out of job files); a missing or empty one is an input error.

### Examples

Smallest job (no agent, just shell):

```json
{ "id": "hello", "tasks": [{ "id": "t", "image": "debian:bookworm-slim", "steps": ["echo hi"] }] }
```

## Events (NDJSON)

One JSON object per line, flushed per line:

```json
{"ts_ms":1792955030243,"event":"job_started","job":"smoke","tasks":4}
{"ts_ms":1792955030243,"event":"task_started","task":"ok","image":"debian:bookworm-slim"}
{"ts_ms":1792955030653,"event":"container_ready","task":"ok","container_id":"300f..."}
{"ts_ms":1792955030653,"event":"step_started","task":"ok","step":0,"name":"step-0"}
{"ts_ms":1792955030709,"event":"output","task":"ok","step":0,"stream":"stdout","line":"hola"}
{"ts_ms":1792955030713,"event":"step_finished","task":"ok","step":0,"status":"succeeded","exit_code":0,"duration_ms":60}
{"ts_ms":1792955031000,"event":"task_finished","task":"ok","status":"succeeded","duration_ms":756,"memory":{"peak_bytes":6832128,"swap_peak_bytes":0,"oom_kills":0}}
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
    "memory": { "peak_bytes": 6832128, "swap_peak_bytes": 0, "oom_kills": 0 },
    "steps": [{
      "name": "step-0", "status": "succeeded", "exit_code": 0, "duration_ms": 60,
      "stdout_tail": "hola\n/tmp\n", "stderr_tail": "err\n",
      "stdout_bytes": 10, "stderr_bytes": 4
    }]
  }]
}
```

An `output` event carries at most `--max-line-bytes` (default 8 KiB) of one
line; longer lines arrive as consecutive events of that size. Raise it when a
task prints a large single-line result (e.g. JSON) that you parse from the
events stream.

`*_tail` holds the last `--tail-bytes` (default 16 KiB) of each stream;
`*_bytes` is the full size. The complete output is in the events stream.

`memory` is recorded for every task that got a container, whatever its
outcome, and also appears on its `task_finished` event. It is read from the
container's cgroup v2 counters just before removal, so it covers all steps:

| Field | Source | Meaning |
|---|---|---|
| `peak_bytes` | `memory.peak` | peak RAM, incl. reclaimable page cache |
| `swap_peak_bytes` | `memory.swap.peak` | peak swap used beyond RAM |
| `oom_kills` | `memory.events` | processes killed at the limit (exit code 137) |

A field is `null` when the runtime does not expose it (e.g. some gVisor
setups); if the read fails entirely, `memory` is omitted and a `warning`
event explains why. Example: a task with `memory_mb: 32` that allocates
100 MiB ends with `exit_code: 137` and `"oom_kills": 1`.

## Memory sizing on a 1 GiB VM

The dispatcher itself needs a few MiB. Budget the rest as
`MAX_PARALLEL × memory_mb` + Docker daemon (~100 MiB) + OS. gVisor adds
roughly 20–50 MiB per running task. For example
`memory_mb: 256` with `MAX_PARALLEL=2` uses ~512 MiB for tasks and fits a 1 GiB VM.

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
only about 30 networks on a host (including any other networks), so more
concurrent tasks fail with `address pools are exhausted`. To raise the
ceiling, hand out small subnets in `/etc/docker/daemon.json` and restart
Docker:

```json
{ "default-address-pools": [ { "base": "10.200.0.0/16", "size": 24 } ] }
```

That allows 256 networks. Task networks stay isolated whatever their subnet.

## Architecture

One plain crate (`src/`), Clean Architecture modules `domain` ←
`application` ← `infrastructure` ← `main.rs`, checked by `tests/architecture.rs`;
see [AGENTS.md](AGENTS.md).

## Using cellrun from another project

Add this repo as a git submodule and treat it as a black box with a CLI
contract: job JSON in; events, report and exit code out (all above). Then:

1. **Build the binary** into your own image or host:
   `cargo build --release --locked --bin dispatcher` (in a multi-stage
   Dockerfile, copy `target/release/dispatcher` into e.g.
   `gcr.io/distroless/cc-debian12:nonroot`).
2. **Give it the Docker socket** (`/var/run/docker.sock`); as non-root, add
   the user to the socket's group. Task containers are siblings on the host.
3. **Apply the host firewall** (see "Host firewall") before any task runs,
   after every boot, for the same `--bridge-prefix`, and test it on that
   host. Cellrun does not do this for you.
4. **Pick your own names:** `--label-prefix` and `--bridge-prefix` (use a
   bridge prefix no other cellrun user on the host has). `cleanup` only finds
   resources carrying the label prefix in use.
