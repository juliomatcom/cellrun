//! [`ContainerRuntime`] backed by the Docker Engine API over the local unix
//! socket (no `docker` CLI processes are spawned).
//!
//! # Isolation
//!
//! Each task container is the only member of its own bridge network, whose
//! host interface is named `hp<hex>`. The host firewall installed by
//! `sandbox-guard` drops everything from `hp+` to the host itself and to
//! private ranges, so a container reaches the public internet and nothing
//! else: not the host, not the LAN, not other tasks. Containers get no
//! mounts; inside, the workload has full (root) access. For a kernel
//! boundary as well, set `runtime` to `runsc` (gVisor).

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

use application::{ContainerRuntime, ExecStatus, OutputObserver, RuntimeError};
use domain::{ContainerSpec, JobId, StreamKind, TaskId};

/// Prefix of every task bridge interface; `sandbox-guard` firewalls `hp+`.
pub const BRIDGE_PREFIX: &str = "hp";

/// Label marking every container and network created by the orchestrator.
pub const LABEL_MANAGED: &str = "hostprobe.orchestrator";
pub const LABEL_JOB: &str = "hostprobe.job";
pub const LABEL_TASK: &str = "hostprobe.task";
/// Unique per orchestrator process: signal cleanup must only touch what this
/// process created, never a concurrent run of the same job.
pub const LABEL_RUN: &str = "hostprobe.run";

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
}

impl Default for DockerConfig {
    fn default() -> Self {
        Self {
            pull: PullPolicy::Missing,
            default_pids_limit: Some(512),
            dns: vec!["1.1.1.1".to_owned(), "8.8.8.8".to_owned()],
            runtime: None,
        }
    }
}

pub struct DockerRuntime {
    docker: Docker,
    config: DockerConfig,
    /// container id -> its private network.
    networks: Mutex<HashMap<String, String>>,
    run_id: String,
}

impl DockerRuntime {
    /// Connects to `$DOCKER_HOST` (unix://) or `/var/run/docker.sock`.
    pub fn connect(config: DockerConfig) -> Result<Self, RuntimeError> {
        let docker = Docker::connect_with_local_defaults().map_err(err)?;
        Ok(Self {
            docker,
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
                    " (Docker's address pools are exhausted: lower ORCHESTRATOR_MAX_PARALLEL, run \
                     `orchestrator cleanup <job>` for leftovers, or configure smaller \
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
            (LABEL_MANAGED.to_owned(), "1".to_owned()),
            (LABEL_JOB.to_owned(), job.to_string()),
            (LABEL_TASK.to_owned(), task.to_string()),
            (LABEL_RUN.to_owned(), self.run_id.clone()),
        ]);
        let (name, bridge) = unique_names(job, task);
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
        timeout: Duration,
        observer: &mut dyn OutputObserver,
    ) -> Result<ExecStatus, RuntimeError> {
        let exec = self
            .docker
            .create_exec(
                container_id,
                CreateExecOptions {
                    cmd: Some(vec!["/bin/sh", "-c", command]),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                },
            )
            .await
            .map_err(err)?;

        let run = async {
            let StartExecResults::Attached { mut output, .. } =
                self.docker.start_exec(&exec.id, None).await.map_err(err)?
            else {
                return Err(RuntimeError::new("exec started detached"));
            };
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

    async fn remove(&self, container_id: &str) -> Result<(), RuntimeError> {
        self.remove_container(container_id).await?;
        let network = lock(&self.networks).remove(container_id);
        match network {
            Some(name) => self.remove_network(&name).await,
            None => Ok(()),
        }
    }

    async fn remove_job(&self, job: &JobId) -> Result<usize, RuntimeError> {
        self.remove_labelled(format!("{LABEL_JOB}={job}")).await
    }

    async fn remove_owned(&self) -> Result<usize, RuntimeError> {
        self.remove_labelled(format!("{LABEL_RUN}={}", self.run_id))
            .await
    }
}

impl DockerRuntime {
    /// Removes managed containers, then networks, matching `label`.
    async fn remove_labelled(&self, label: String) -> Result<usize, RuntimeError> {
        let filters = HashMap::from([("label", vec![format!("{LABEL_MANAGED}=1"), label])]);
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

/// Unique names for a task's container + network (`hp-<job>-<task>-<id>`,
/// readable in `docker ps`) and its bridge interface (`hp<id>`, which must
/// fit Linux's 15-byte interface name limit).
fn unique_names(job: &JobId, task: &TaskId) -> (String, String) {
    let id = unique_hex();
    (
        format!("hp-{job}-{task}-{id}"),
        format!("{BRIDGE_PREFIX}{id}"),
    )
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
        let (a, bridge_a) = unique_names(&job, &task);
        let (b, bridge_b) = unique_names(&job, &task);
        assert_ne!(a, b);
        assert_ne!(bridge_a, bridge_b);
        assert!(a.starts_with("hp-j-t-"));
        assert!(a.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'));
        assert!(bridge_a.starts_with(BRIDGE_PREFIX));
        assert_eq!(bridge_a.len(), 15, "Linux IFNAMSIZ - 1");
    }
}
