# AGENTS.md — apps/orchestrator

Extends the repo-wide [AGENTS.md](../../AGENTS.md). Read that first.

## What it is

A single small Rust binary that reads a JSON job, starts one **ephemeral
Docker container per task**, runs each task's shell steps inside it via
`docker exec`, streams every output line as an NDJSON event and finally writes
a JSON report. Containers are always force-removed (also on SIGINT/SIGTERM).

Target: a VM with ~1 GiB RAM. The orchestrator itself idles at a few MiB.

## Crates (one per circle — the dependency rule is enforced by Cargo)

```
crates/
├── domain/          1. entities & rules. No deps at all.
├── application/     2. RunJob use case + ports (ContainerRuntime, EventSink).
│                       deps: domain, futures-util
├── infrastructure/  3. adapters: DockerRuntime (bollard, unix socket),
│                       job_input (JSON -> domain), NdjsonSink, report_json.
│                       deps: application, domain, bollard, serde, tokio
└── cli/             4. composition root: `orchestrator` binary (clap, tokio).
```

`crates/cli/tests/architecture.rs` fails the build if an inner crate declares
a dependency on an outer crate or on forbidden tech (e.g. `serde` in
`application`). Update its `RULES` table if you add a crate.

### Where does my change go?

| Change | Crate |
|---|---|
| New invariant, new outcome/status, output buffering rules | `domain` |
| Execution flow (ordering, halting, parallelism, events emitted) | `application` |
| A new port (e.g. "upload artifacts") | trait in `application::ports`, impl in `infrastructure` |
| Docker API details, JSON field names, event/report wire format | `infrastructure` |
| CLI flags, exit codes, signal handling, choosing adapters | `cli` |

## Runtime model

- `RunJob::execute` runs tasks with `buffer_unordered(max_parallel)` on a
  **single-threaded** tokio runtime (I/O bound; smallest footprint). Never
  `spawn` per line/chunk; never collect unbounded output.
- Per task: `start` (pull if needed, create with limits + labels, start
  `sleep infinity` under `--init`) → `exec` each step with `/bin/sh -c` →
  `remove` (always). Steps share the container, so files persist across steps.
- A failed/timed-out step halts the task (remaining steps `skipped`) unless
  `continue_on_error`. Timed-out processes are killed when the container is
  removed.
- Output: every chunk → `LineSplitter` (max line 8 KiB) → `output` event;
  report keeps only a `TailBuffer` (default 16 KiB) per stream per step.
- Containers carry labels `hostprobe.orchestrator=1`, `hostprobe.job=<id>`,
  `hostprobe.task=<id>`; `orchestrator cleanup <job>` reaps leftovers.
- Defaults applied by the Docker adapter: `memory_swap = memory` (no swap),
  `pids_limit = 512`, `no-new-privileges`, `init`.

## Contracts

- **Input:** JSON job, see `README.md` and `crates/infrastructure/src/job_input.rs`
  (`deny_unknown_fields`). Secrets only via `env_passthrough` (names of
  orchestrator env vars), never literal values in files.
- **Events:** NDJSON, one object per line, `event` discriminator, `ts_ms`.
- **Report:** JSON, see `crates/infrastructure/src/report_json.rs`.
- **Exit codes:** 0 all ok · 1 a task failed/errored · 2 bad input ·
  3 Docker/IO error · 130 interrupted.

Changing a wire format is a breaking change: update README and tests together.

## Deployment (Docker Compose)

- `Dockerfile`: multi-stage, `rust:1-slim-bookworm` build → 
  `gcr.io/distroless/cc-debian12:nonroot` runtime (~40 MB, no shell).
  Examples are baked in at `/examples`.
- `compose.yaml` (included by the repo root): services `orchestrator` and
  build-only `opencode-runner`, both under profile `orchestrator` because the
  orchestrator is a run-to-completion CLI.
- The container talks to the **host** daemon through the mounted socket, runs
  as uid 65532 with `group_add: ${DOCKER_GID}`, read-only rootfs, no caps,
  `mem_limit: 64m`. Task containers are siblings on the host; if you ever add
  bind mounts to task containers, they must be host paths.
- Job files: `jobs/` is mounted read-only at `/jobs`. Report → stdout, events
  → stderr (container fs is read-only; redirect on the host).
- `init: true` forwards SIGTERM (`docker stop`, `compose down`) so the
  orchestrator reaps its task containers.

## Commands

```sh
cd apps/orchestrator
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test                                                   # unit + architecture
cargo test -p infrastructure --test docker -- --ignored      # needs Docker
cargo build --release                                        # target/release/orchestrator
docker build -t hostprobe/opencode-runner:latest runner      # task image

# via compose, from the repo root
docker compose --profile orchestrator build
docker compose --progress quiet run --rm -T orchestrator run /examples/hello.json \
  > report.json 2> events.ndjson
```

## Conventions

- Rust 2024 edition, `unsafe` forbidden, clippy clean.
- Errors: domain/application use small typed errors; `thiserror` in adapters;
  no `unwrap()` outside tests.
- Ports use native `async fn` in traits (static dispatch, no boxing).
- Tests: fakes for ports live in the test files (`application/tests`); keep
  them deterministic (no real sleeps beyond a few ms).
