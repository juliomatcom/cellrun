# AGENTS.md — swarm

Guidance for humans and coding agents working in this repository. Swarm is a
standalone, reusable crate: it knows nothing about the projects that embed it
(see README, "Embedding"). Keep it that way: no project names, image
namespaces or network names baked in; anything environment-specific is a flag,
an env var or a compose variable with a neutral default.

## Repository layout

```
.
├── Cargo.toml / Cargo.lock  # one plain crate: lib `swarm` + bin `orchestrator`
├── src/                     # all Rust code (layers below)
├── tests/                   # integration + architecture tests
├── compose.yaml             # services; usable standalone or `include`d
├── .env.example             # compose variables
├── Dockerfile               # production image (multi-stage, distroless)
├── runner/                  # task image (Goose)
├── sandbox-guard/           # host firewall one-shot
├── examples/  jobs/         # sample jobs (baked in) / local jobs (git-ignored)
└── .github/workflows/ci.yml
```

## Architecture: Clean Architecture

Source dependencies point only inwards. Need something from the outside world
in a use case? Define a **port** in `application` and implement it in
`infrastructure`. Wire formats (JSON DTOs) live in `infrastructure` and are
mapped to/from domain types at the boundary; domain types are never
serialized directly.

| Layer | May depend on | Must never contain |
|---|---|---|
| `domain` | std only | I/O, async runtimes, serialization, frameworks |
| `application` | `domain` | concrete tech (Docker, HTTP, serde DTOs) |
| `infrastructure` | `application`, `domain` | business rules, CLI/arg parsing |
| `main.rs` | everything | business logic |

## Engineering standards

- **Resource budget:** targets a VM with ~1 GiB RAM shared by all services.
  Bound every buffer, queue and concurrency level; stream instead of
  collecting; set `mem_limit` per service.
- **Tests:** domain and application are unit tested with fakes for ports;
  adapters get integration tests (`#[ignore]`d when they need Docker).
- **Secrets:** never in source, job files, logs or reports. Pass via env.
- **Before pushing:** `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test`. Keep changes minimal; match surrounding style.
- **No breaking changes** to the wire formats, flags, env vars, exit codes or
  compose variables without updating README and tests together (embedders pin
  this repo as a submodule).

## What it is

A single small Rust binary that reads a JSON job, starts one **ephemeral
Docker container per task**, runs each task's shell steps inside it via
`docker exec`, streams every output line as an NDJSON event and finally writes
a JSON report. Containers are always force-removed (also on SIGINT/SIGTERM).

Target: a VM with ~1 GiB RAM. The orchestrator itself idles at a few MiB.

## Isolation model (the core requirement)

Each task container is fully isolated **from the host and from every other
task**, while the workload inside (e.g. Goose) has **full access** to its
own container: root, writable filesystem, public internet.

| Layer | Mechanism | Where |
|---|---|---|
| Network: other tasks | one bridge network per task (sole member), ICC off | `docker.rs` |
| Network: host + LAN + metadata | bridges named `<bridge-prefix><hex>` (default `sw`); host firewall drops `-i <prefix>+` to INPUT and to private ranges in DOCKER-USER | `sandbox-guard/guard.sh` |
| DNS | explicit public resolvers (private ranges are dropped) | `--task-dns` |
| Filesystem | no bind mounts, no volumes, no docker socket; anonymous volumes removed with the container | `docker.rs` |
| Kernel (optional) | `--task-runtime runsc` (gVisor) | `TASK_RUNTIME` in `.env` |
| Resources | memory (+ swap, default 1 GiB), CPU, pids limits | job file + defaults |
| Parallelism | containers alive at once | `--max-parallel` / `ORCHESTRATOR_MAX_PARALLEL` (never the job) |

Rules:
- **Jobs cannot weaken isolation.** `ContainerSpec` has no network, mount,
  capability or privilege fields; job input uses `deny_unknown_fields`.
  Do not add such fields.
- Do **not** restrict the workload inside the container (no cap drops, no
  read-only rootfs, no non-root user requirement): isolation is enforced at
  the boundary, not by crippling the tool.
- Run `sandbox-guard` before any task starts (compose `depends_on` does
  this). Without it, tasks can reach the host. The isolation integration
  test fails if the rules are missing.

## Source layout (one module per circle — the dependency rule is enforced by a test)

```
src/
├── domain/          1. entities & rules. No deps at all.
├── application/     2. RunJob use case + ports (ContainerRuntime, EventSink).
│                       deps: domain, futures-util
├── infrastructure/  3. adapters: DockerRuntime (bollard, unix socket),
│                       job_input (JSON -> domain), NdjsonSink, report_json.
│                       deps: application, domain, bollard, serde, tokio
├── lib.rs           exposes the three layers as the `swarm` library
└── main.rs          4. composition root: `orchestrator` binary (clap, tokio).
```

`tests/architecture.rs` scans each layer's non-test `use` declarations and
fails if an inner layer names an outer layer or forbidden tech (e.g. `serde`
in `application`). Update its `RULES` table if you add a layer.

### Where does my change go?

| Change | Layer |
|---|---|
| New invariant, new outcome/status, output buffering rules | `domain` |
| Execution flow (ordering, halting, parallelism, events emitted) | `application` |
| A new port (e.g. "upload artifacts") | trait in `application::ports`, impl in `infrastructure` |
| Docker API details, JSON field names, event/report wire format | `infrastructure` |
| CLI flags, exit codes, signal handling, choosing adapters | `main.rs` |

## Runtime model

- `RunJob::execute` runs tasks with `buffer_unordered(max_parallel)` on a
  **single-threaded** tokio runtime (I/O bound; smallest footprint). `max_parallel` is a
  `RunJob` setting (host capacity, from the CLI), not a `Job` field. Never
  `spawn` per line/chunk; never collect unbounded output.
- Per task: `start` (pull if needed, create with limits + labels, start
  `sleep infinity` under `--init`) → `exec` each step with `/bin/sh -c` →
  `memory_usage` (cgroup v2 peak RAM/swap + OOM kills, via a shell-builtin
  exec; always, failures become a warning) → `remove` (always). Steps share the container, so files persist across steps.
- A failed step halts the task (remaining steps `skipped`) unless
  `continue_on_error`. A timeout always halts and fails the task
  (`StepOutcome::halts_task`): Docker cannot kill an exec'd process, so the
  container is removed instead, which kills it.
- Output: every chunk → `LineSplitter` (max line 8 KiB) → `output` event;
  report keeps only a `TailBuffer` (default 16 KiB) per stream per step.
- Per task: create network `<bridge-prefix>-<job>-<task>-<id>` (bridge iface `<bridge-prefix><id>`) →
  create/start container on it → steps → remove container → remove network.
- Containers and networks carry labels `<label-prefix>.orchestrator=1`,
  `<label-prefix>.job=<id>`, `.task=<id>`, `.run=<per-process id>` (prefix `swarm`
  by default, `--label-prefix`). Keep it stable between runs: `cleanup` only
  finds resources carrying the prefix in use.
  On SIGINT/SIGTERM only this run's resources are removed (`remove_owned`);
  `orchestrator cleanup <job>` reaps leftovers of every run of that job.
- One network per running task: Docker's default address pools cap this at
  ~30 (see README to raise it).
- Defaults applied by the Docker adapter: `memory_swap = memory + swap` (swap defaults to
  1 GiB per task in the job parser, max 2 GiB; 0 disables it),
  `pids_limit = 512`, private IPC, `init`.

## Contracts

- **Input:** JSON job, see `README.md` and `src/infrastructure/job_input.rs`
  (`deny_unknown_fields`). Secrets only via `env_passthrough` (names of
  orchestrator env vars), never literal values in files.
- **Events:** NDJSON, one object per line, `event` discriminator, `ts_ms`.
- **Report:** JSON, see `src/infrastructure/report_json.rs`.
- **Exit codes:** 0 all ok · 1 a task failed/errored · 2 bad input ·
  3 Docker/IO error · 130 interrupted.

Changing a wire format is a breaking change: update README and tests together.

## Deployment (Docker Compose)

- `Dockerfile`: multi-stage, `rust:1-slim-bookworm` build → 
  `gcr.io/distroless/cc-debian12:nonroot` runtime (~40 MB, no shell).
  Examples are baked in at `/examples`.
- `compose.yaml` (standalone, or `include`d by a parent project), all under profile
  `orchestrator` (run-to-completion): `sandbox-guard` (one-shot, host
  network, NET_ADMIN, installs firewall rules), `orchestrator` (depends on
  the guard having completed) and build-only `goose-runner` (the task image;
  Goose replaced opencode for memory: ~144 vs ~469 MiB peak, see README).
- The orchestrator container talks to the **host** daemon through the
  mounted socket, runs as uid 65532 with `group_add: ${DOCKER_GID}`,
  read-only rootfs, no caps, `mem_limit: 64m`. It joins the shared
  `${BACKEND_NETWORK:-swarm-backend}` network to reach event bus / database
  services.
  Task containers are siblings on the host and cannot reach that network
  (verified by the isolation integration test).
- Job files: `jobs/` is mounted read-only at `/jobs`. Report → stdout, events
  → stderr (container fs is read-only; redirect on the host).
- Naming is parameterised, never hard-coded: `IMAGE_NAMESPACE`,
  `BACKEND_NETWORK`, `LABEL_PREFIX`, `BRIDGE_PREFIX` (see `.env.example`).
  `LABEL_PREFIX`/`BRIDGE_PREFIX` reach the binary as
  `ORCHESTRATOR_LABEL_PREFIX`/`ORCHESTRATOR_BRIDGE_PREFIX` and the guard as
  `BRIDGE_PREFIX`; the guard and the orchestrator must agree on the bridge
  prefix or the firewall matches nothing.
- `init: true` forwards SIGTERM (`docker stop`, `compose down`) so the
  orchestrator reaps its task containers.

## Commands

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test                                                   # unit + architecture
sudo sandbox-guard/guard.sh apply                            # host firewall (once per boot)
cargo test --test docker -- --ignored                        # needs Docker + guard
cargo build --release                                        # target/release/orchestrator
docker build -t swarm/goose-runner:latest runner              # task image

# via compose
docker compose --profile orchestrator build
docker compose --progress quiet run --rm -T orchestrator run /examples/hello.json \
  > report.json 2> events.ndjson
```

## Conventions

- Rust 2024 edition, `unsafe` forbidden, clippy clean.
- Errors: domain/application use small typed errors; `thiserror` in adapters;
  no `unwrap()` outside tests.
- Ports use native `async fn` in traits (static dispatch, no boxing).
- Tests: fakes for ports live in the test files (`tests/run_job.rs`); keep
  them deterministic (no real sleeps beyond a few ms).
