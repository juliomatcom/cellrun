//! [`ContainerRuntime`] backed by the Docker Engine API over the local unix
//! socket (no `docker` CLI processes are spawned).
//!
//! # Isolation
//!
//! Each task container is the only member of its own bridge network, whose
//! host interface is named `<bridge_prefix><hex>` (`cr<hex>` by default), so
//! tasks cannot see each other or other Docker networks. Containers get no
//! mounts; inside, the workload has full (root) access. Keeping tasks off the
//! host, the LAN and cloud metadata takes a host firewall on
//! `<bridge_prefix>+`, which the caller applies (README, "Host firewall").
//! For a kernel boundary as well, set `runtime` to `runsc` (gVisor).

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bollard::Docker;
use bollard::container::LogOutput;
use bollard::errors::Error as DockerError;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::{ContainerCreateBody, HostConfig, NetworkCreateRequest};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
    ListNetworksOptionsBuilder, RemoveContainerOptionsBuilder,
};
use futures_util::{StreamExt, TryStreamExt};
use tokio::io::AsyncWriteExt;

use crate::application::{ContainerRuntime, ExecStatus, OutputObserver, RuntimeError};
use crate::domain::{ContainerSpec, JobId, MemoryUsage, StreamKind, TaskId};

/// Linux `IFNAMSIZ` - 1: a bridge interface name is `<prefix><13 hex>`.
const IFNAME_MAX: usize = 15;
const UNIQUE_HEX_LEN: usize = 13;

/// Label keys that mark everything the dispatcher creates, derived from
/// [`DockerConfig::label_prefix`].
struct LabelKeys {
    /// Marks every container and network created by the dispatcher.
    managed: String,
    job: String,
    task: String,
    /// Unique per dispatcher process: signal cleanup must only touch what
    /// this process created, never a concurrent run of the same job.
    run: String,
}

impl LabelKeys {
    fn new(prefix: &str) -> Self {
        Self {
            managed: format!("{prefix}.dispatcher"),
            job: format!("{prefix}.job"),
            task: format!("{prefix}.task"),
            run: format!("{prefix}.run"),
        }
    }
}

/// Keeps the container alive between `exec` calls; falls back to a loop on
/// images whose `sleep` lacks `infinity` (e.g. old busybox).
const KEEPALIVE: &str = "exec sleep infinity 2>/dev/null || while :; do sleep 3600; done";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PullPolicy {
    /// Pull only when the image is not present locally.
    #[default]
    Missing,
    Always,
    Never,
}

#[derive(Debug, Clone)]
pub struct DockerConfig {
    pub pull: PullPolicy,
    /// Applied when a task sets no `pids` limit (fork-bomb protection).
    pub default_pids_limit: Option<u64>,
    /// Upstream DNS for task containers. Public resolvers keep DNS working
    /// while private ranges are firewalled off.
    pub dns: Vec<String>,
    /// OCI runtime for task containers, e.g. `runsc` (gVisor).
    pub runtime: Option<String>,
    /// Prefix of the labels put on containers and networks
    /// (`<prefix>.dispatcher`, `.job`, `.task`, `.run`).
    pub label_prefix: String,
    /// Prefix of every task bridge interface (1-2 chars of `[a-z0-9]`, so the
    /// name fits Linux's 15-byte limit). The host firewall must match
    /// `<prefix>+`; it is also the prefix of container/network names.
    pub bridge_prefix: String,
}

impl Default for DockerConfig {
    fn default() -> Self {
        Self {
            pull: PullPolicy::Missing,
            default_pids_limit: Some(512),
            dns: vec!["1.1.1.1".to_owned(), "8.8.8.8".to_owned()],
            runtime: None,
            label_prefix: "cellrun".to_owned(),
            bridge_prefix: "cr".to_owned(),
        }
    }
}

pub struct DockerRuntime {
    docker: Docker,
    config: DockerConfig,
    /// container id -> its private network.
    networks: Mutex<HashMap<String, String>>,
    run_id: String,
    labels: LabelKeys,
}

impl DockerRuntime {
    /// Connects to `$DOCKER_HOST` (unix://) or `/var/run/docker.sock`.
    pub fn connect(config: DockerConfig) -> Result<Self, RuntimeError> {
        validate_prefixes(&config)?;
        let docker = Docker::connect_with_local_defaults().map_err(err)?;
        Ok(Self {
            docker,
            labels: LabelKeys::new(&config.label_prefix),
            config,
            networks: Mutex::new(HashMap::new()),
            run_id: unique_hex(),
        })
    }

    pub async fn ping(&self) -> Result<(), RuntimeError> {
        self.docker.ping().await.map(drop).map_err(err)
    }

    async fn ensure_image(&self, image: &str) -> Result<(), RuntimeError> {
        let present = match self.docker.inspect_image(image).await {
            Ok(_) => true,
            Err(e) if is_not_found(&e) => false,
            Err(e) => return Err(err(e)),
        };
        match (self.config.pull, present) {
            (PullPolicy::Never, false) => Err(RuntimeError::new(format!(
                "image `{image}` not present and pull policy is `never`"
            ))),
            (PullPolicy::Always, _) | (PullPolicy::Missing, false) => {
                let options = CreateImageOptionsBuilder::new()
                    .from_image(&normalize_image_ref(image))
                    .build();
                // Drain progress messages; errors surface as stream items.
                self.docker
                    .create_image(Some(options), None, None)
                    .try_for_each(|_| async { Ok(()) })
                    .await
                    .map_err(|e| RuntimeError::new(format!("pull `{image}`: {e}")))
            }
            _ => Ok(()),
        }
    }

    async fn create_network(
        &self,
        name: &str,
        bridge: &str,
        labels: &HashMap<String, String>,
    ) -> Result<(), RuntimeError> {
        let request = NetworkCreateRequest {
            name: name.to_owned(),
            driver: Some("bridge".to_owned()),
            enable_ipv6: Some(false),
            options: Some(HashMap::from([
                (
                    "com.docker.network.bridge.name".to_owned(),
                    bridge.to_owned(),
                ),
                (
                    "com.docker.network.bridge.enable_icc".to_owned(),
                    "false".to_owned(),
                ),
            ])),
            labels: Some(labels.clone()),
            ..Default::default()
        };
        self.docker
            .create_network(request)
            .await
            .map(drop)
            .map_err(|e| {
                let hint = if e.to_string().contains("address pools") {
                    " (Docker's address pools are exhausted: lower DISPATCHER_MAX_PARALLEL, run \
                     `dispatcher cleanup <job>` for leftovers, or configure smaller \
                     `default-address-pools` in daemon.json, see README)"
                } else {
                    ""
                };
                RuntimeError::new(format!("create network: {e}{hint}"))
            })
    }

    async fn remove_network(&self, name: &str) -> Result<(), RuntimeError> {
        match self.docker.remove_network(name).await {
            Ok(()) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(RuntimeError::new(format!("remove network {name}: {e}"))),
        }
    }

    async fn create_container(
        &self,
        name: &str,
        labels: HashMap<String, String>,
        spec: &ContainerSpec,
    ) -> Result<String, RuntimeError> {
        let limits = &spec.limits;
        let memory = limits.memory_bytes.map(saturating_i64);
        let host_config = HostConfig {
            network_mode: Some(name.to_owned()),
            dns: Some(self.config.dns.clone()),
            runtime: self.config.runtime.clone(),
            ipc_mode: Some("private".to_owned()),
            init: Some(true),
            memory,
            // Docker's `memory_swap` is RAM + swap; equal to `memory` => no swap.
            memory_swap: limits
                .memory_bytes
                .map(|m| saturating_i64(m.saturating_add(limits.swap_bytes.unwrap_or(0)))),
            nano_cpus: limits.nano_cpus.map(saturating_i64),
            pids_limit: limits
                .pids
                .or(self.config.default_pids_limit)
                .map(saturating_i64),
            ..Default::default()
        };
        let body = ContainerCreateBody {
            image: Some(spec.image.clone()),
            entrypoint: Some(vec!["/bin/sh".to_owned(), "-c".to_owned()]),
            cmd: Some(vec![KEEPALIVE.to_owned()]),
            env: Some(spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect()),
            working_dir: spec.workdir.clone(),
            labels: Some(labels),
            host_config: Some(host_config),
            ..Default::default()
        };
        let options = CreateContainerOptionsBuilder::new().name(name).build();

        let id = self
            .docker
            .create_container(Some(options), body)
            .await
            .map_err(err)?
            .id;
        if let Err(e) = self.docker.start_container(&id, None).await {
            let _ = self.remove_container(&id).await;
            return Err(err(e));
        }
        Ok(id)
    }

    async fn remove_container(&self, container_id: &str) -> Result<(), RuntimeError> {
        let options = RemoveContainerOptionsBuilder::default()
            .force(true)
            .v(true)
            .build();
        match self
            .docker
            .remove_container(container_id, Some(options))
            .await
        {
            Ok(()) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            // 409: removal already in progress.
            Err(DockerError::DockerResponseServerError {
                status_code: 409, ..
            }) => Ok(()),
            Err(e) => Err(err(e)),
        }
    }
}

impl ContainerRuntime for DockerRuntime {
    async fn start(
        &self,
        job: &JobId,
        task: &TaskId,
        spec: &ContainerSpec,
    ) -> Result<String, RuntimeError> {
        self.ensure_image(&spec.image).await?;

        let labels = HashMap::from([
            (self.labels.managed.clone(), "1".to_owned()),
            (self.labels.job.clone(), job.to_string()),
            (self.labels.task.clone(), task.to_string()),
            (self.labels.run.clone(), self.run_id.clone()),
        ]);
        let (name, bridge) = unique_names(&self.config.bridge_prefix, job, task);
        self.create_network(&name, &bridge, &labels).await?;
        match self.create_container(&name, labels, spec).await {
            Ok(id) => {
                lock(&self.networks).insert(id.clone(), name);
                Ok(id)
            }
            Err(e) => {
                let _ = self.remove_network(&name).await;
                Err(e)
            }
        }
    }

    async fn exec(
        &self,
        container_id: &str,
        command: &str,
        input: Option<&[u8]>,
        timeout: Duration,
        observer: &mut dyn OutputObserver,
    ) -> Result<ExecStatus, RuntimeError> {
        let exec = self
            .docker
            .create_exec(
                container_id,
                CreateExecOptions {
                    cmd: Some(vec!["/bin/sh", "-c", command]),
                    attach_stdin: input.map(|_| true),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                },
            )
            .await
            .map_err(err)?;

        let run = async {
            let StartExecResults::Attached {
                mut output,
                input: mut stdin,
            } = self.docker.start_exec(&exec.id, None).await.map_err(err)?
            else {
                return Err(RuntimeError::new("exec started detached"));
            };
            // Write and read concurrently: a large input would otherwise
            // deadlock against a process that fills its output pipe first.
            let write = async {
                if let Some(bytes) = input {
                    // A process that exits without reading all of its stdin
                    // closes the pipe; that is not an error of the step.
                    let _ = stdin.write_all(bytes).await;
                    let _ = stdin.shutdown().await;
                }
            };
            let read = async {
                while let Some(frame) = output.next().await {
                    match frame.map_err(err)? {
                        LogOutput::StdOut { message } | LogOutput::Console { message } => {
                            observer.on_output(StreamKind::Stdout, &message)
                        }
                        LogOutput::StdErr { message } => {
                            observer.on_output(StreamKind::Stderr, &message)
                        }
                        LogOutput::StdIn { .. } => {}
                    }
                }
                Ok(())
            };
            let ((), result) = tokio::join!(write, read);
            result
        };
        // On timeout the process keeps running inside the container; the task
        // halts and the whole container is removed, which kills it.
        match tokio::time::timeout(timeout, run).await {
            Err(_) => return Ok(ExecStatus::TimedOut),
            Ok(result) => result?,
        }

        // The stream closes when the process exits, but the daemon may not
        // have recorded the exit code yet.
        for _ in 0..50 {
            let inspect = self.docker.inspect_exec(&exec.id).await.map_err(err)?;
            if inspect.running != Some(true) {
                if let Some(code) = inspect.exit_code {
                    return Ok(ExecStatus::Exited(code));
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Err(RuntimeError::new(
            "exec finished but exit code is unavailable",
        ))
    }

    async fn memory_usage(&self, container_id: &str) -> Result<MemoryUsage, RuntimeError> {
        let mut out = Collect::default();
        match self
            .exec(
                container_id,
                READ_MEMORY,
                None,
                Duration::from_secs(5),
                &mut out,
            )
            .await?
        {
            ExecStatus::Exited(0) => Ok(parse_memory(&String::from_utf8_lossy(&out.0))),
            other => Err(RuntimeError::new(format!(
                "memory probe ended with {other:?}"
            ))),
        }
    }

    async fn remove(&self, container_id: &str) -> Result<(), RuntimeError> {
        self.remove_container(container_id).await?;
        let network = lock(&self.networks).remove(container_id);
        match network {
            Some(name) => self.remove_network(&name).await,
            None => Ok(()),
        }
    }

    async fn remove_job(&self, job: &JobId) -> Result<usize, RuntimeError> {
        self.remove_labelled(format!("{}={job}", self.labels.job))
            .await
    }

    async fn remove_owned(&self) -> Result<usize, RuntimeError> {
        self.remove_labelled(format!("{}={}", self.labels.run, self.run_id))
            .await
    }
}

impl DockerRuntime {
    /// Removes every container and network carrying this dispatcher's label
    /// prefix, whatever job or run created them (e.g. after a caller crashed
    /// and lost track of its job ids).
    pub async fn remove_all(&self) -> Result<usize, RuntimeError> {
        self.remove_labelled(format!("{}=1", self.labels.managed))
            .await
    }

    /// Removes managed containers, then networks, matching `label`.
    async fn remove_labelled(&self, label: String) -> Result<usize, RuntimeError> {
        let filters = HashMap::from([("label", vec![format!("{}=1", self.labels.managed), label])]);
        let options = ListContainersOptionsBuilder::default()
            .all(true)
            .filters(&filters)
            .build();
        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .map_err(err)?;
        // Best effort: keep going past failures, report the first one.
        let mut first_err = None;
        let mut removed = 0;
        for id in containers.into_iter().filter_map(|c| c.id) {
            match self.remove(&id).await {
                Ok(()) => removed += 1,
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        // Networks left by a crashed run (their container map is gone).
        let options = ListNetworksOptionsBuilder::new().filters(&filters).build();
        let networks = self
            .docker
            .list_networks(Some(options))
            .await
            .map_err(err)?;
        for name in networks.into_iter().filter_map(|n| n.name) {
            if let Err(e) = self.remove_network(&name).await {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(removed), Err)
    }
}

/// Prints the container's cgroup v2 memory counters as `key value` lines.
/// Shell builtins only (no `cat`/`grep`), so any image with `/bin/sh` works;
/// missing files (cgroup v1, gVisor) just yield fewer lines.
const READ_MEMORY: &str = "cd /sys/fs/cgroup 2>/dev/null || exit 0; \
    for f in memory.peak memory.swap.peak; do \
      [ -r $f ] && read v < $f && echo \"$f $v\"; \
    done; \
    [ -r memory.events ] && while read k v; do \
      [ \"$k\" = oom_kill ] && echo \"oom_kill $v\"; \
    done < memory.events; \
    exit 0";

fn parse_memory(text: &str) -> MemoryUsage {
    let mut usage = MemoryUsage::default();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(' ') else {
            continue;
        };
        let value = value.trim().parse().ok();
        match key {
            "memory.peak" => usage.peak_bytes = value,
            "memory.swap.peak" => usage.swap_peak_bytes = value,
            "oom_kill" => usage.oom_kills = value,
            _ => {}
        }
    }
    usage
}

/// Buffers a short command's stdout (the memory probe prints < 100 bytes).
#[derive(Default)]
struct Collect(Vec<u8>);

impl OutputObserver for Collect {
    fn on_output(&mut self, stream: StreamKind, chunk: &[u8]) {
        if stream == StreamKind::Stdout && self.0.len() < 4096 {
            self.0.extend_from_slice(chunk);
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn err(e: DockerError) -> RuntimeError {
    RuntimeError::new(e.to_string())
}

fn is_not_found(e: &DockerError) -> bool {
    matches!(
        e,
        DockerError::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

fn saturating_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Without a tag the Engine API pulls *every* tag of a repository.
fn normalize_image_ref(image: &str) -> String {
    let last_segment = image.rsplit('/').next().unwrap_or(image);
    if image.contains('@') || last_segment.contains(':') {
        image.to_owned()
    } else {
        format!("{image}:latest")
    }
}

/// Unique names for a task's container + network (`<prefix>-<job>-<task>-<id>`,
/// readable in `docker ps`) and its bridge interface (`<prefix><id>`, which
/// must fit Linux's 15-byte interface name limit).
fn unique_names(prefix: &str, job: &JobId, task: &TaskId) -> (String, String) {
    let id = unique_hex();
    (
        format!("{prefix}-{job}-{task}-{id}"),
        format!("{prefix}{id}"),
    )
}

fn validate_prefixes(config: &DockerConfig) -> Result<(), RuntimeError> {
    let bridge = &config.bridge_prefix;
    let ok = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    if bridge.is_empty() || bridge.len() + UNIQUE_HEX_LEN > IFNAME_MAX || !bridge.chars().all(ok) {
        return Err(RuntimeError::new(format!(
            "bridge prefix `{bridge}` must be 1-{} chars of [a-z0-9]",
            IFNAME_MAX - UNIQUE_HEX_LEN
        )));
    }
    let label = &config.label_prefix;
    if label.is_empty() || !label.chars().all(|c| ok(c) || matches!(c, '-' | '.' | '_')) {
        return Err(RuntimeError::new(format!(
            "label prefix `{label}` must be non-empty and use only [a-z0-9._-]"
        )));
    }
    Ok(())
}

/// 13 hex chars (52 bits) mixing time, pid and a process-wide counter.
fn unique_hex() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    // 52 bits of time-ish entropy mixed with pid and a counter -> 13 hex chars.
    let id =
        (nanos ^ (u64::from(std::process::id()) << 32) ^ seq.rotate_left(40)) & ((1 << 52) - 1);
    format!("{id:013x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cgroup_memory_counters() {
        let usage = parse_memory("memory.peak 157286400\nmemory.swap.peak 0\noom_kill 1\n");
        assert_eq!(
            usage,
            MemoryUsage {
                peak_bytes: Some(157_286_400),
                swap_peak_bytes: Some(0),
                oom_kills: Some(1),
            }
        );
        // cgroup v1 / gVisor: nothing readable.
        assert_eq!(parse_memory(""), MemoryUsage::default());
    }

    #[test]
    fn image_refs_get_explicit_tag() {
        assert_eq!(normalize_image_ref("debian"), "debian:latest");
        assert_eq!(normalize_image_ref("debian:bookworm"), "debian:bookworm");
        assert_eq!(
            normalize_image_ref("localhost:5000/team/app"),
            "localhost:5000/team/app:latest"
        );
        assert_eq!(normalize_image_ref("ghcr.io/a/b:1.2"), "ghcr.io/a/b:1.2");
        assert_eq!(normalize_image_ref("a/b@sha256:abc"), "a/b@sha256:abc");
    }

    #[test]
    fn names_are_unique_and_bridge_fits_ifname_limit() {
        let job = JobId::new("j").unwrap();
        let task = TaskId::new("t").unwrap();
        let (a, bridge_a) = unique_names("hp", &job, &task);
        let (b, bridge_b) = unique_names("hp", &job, &task);
        assert_ne!(a, b);
        assert_ne!(bridge_a, bridge_b);
        assert!(a.starts_with("hp-j-t-"));
        assert!(a.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'));
        assert!(bridge_a.starts_with("hp"));
        assert_eq!(bridge_a.len(), 15, "Linux IFNAMSIZ - 1");
    }

    #[test]
    fn prefixes_are_validated() {
        let with = |label: &str, bridge: &str| DockerConfig {
            label_prefix: label.to_owned(),
            bridge_prefix: bridge.to_owned(),
            ..DockerConfig::default()
        };
        assert!(validate_prefixes(&DockerConfig::default()).is_ok());
        assert!(validate_prefixes(&with("acme.app", "hp")).is_ok());
        assert!(validate_prefixes(&with("cellrun", "")).is_err());
        assert!(
            validate_prefixes(&with("cellrun", "abc")).is_err(),
            "too long for IFNAMSIZ"
        );
        assert!(validate_prefixes(&with("cellrun", "H")).is_err());
        assert!(validate_prefixes(&with("", "cr")).is_err());
    }
}
