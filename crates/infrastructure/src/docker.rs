//! [`ContainerRuntime`] backed by the Docker Engine API over the local unix
//! socket (no `docker` CLI processes are spawned).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bollard::Docker;
use bollard::container::LogOutput;
use bollard::errors::Error as DockerError;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
    RemoveContainerOptionsBuilder,
};
use futures_util::{StreamExt, TryStreamExt};

use application::{ContainerRuntime, ExecStatus, OutputObserver, RuntimeError};
use domain::{ContainerSpec, JobId, StreamKind, TaskId};

/// Label marking every container created by the orchestrator.
pub const LABEL_MANAGED: &str = "hostprobe.orchestrator";
pub const LABEL_JOB: &str = "hostprobe.job";
pub const LABEL_TASK: &str = "hostprobe.task";

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
}

impl Default for DockerConfig {
    fn default() -> Self {
        Self {
            pull: PullPolicy::Missing,
            default_pids_limit: Some(512),
        }
    }
}

pub struct DockerRuntime {
    docker: Docker,
    config: DockerConfig,
}

impl DockerRuntime {
    /// Connects to `$DOCKER_HOST` (unix://) or `/var/run/docker.sock`.
    pub fn connect(config: DockerConfig) -> Result<Self, RuntimeError> {
        let docker = Docker::connect_with_local_defaults().map_err(err)?;
        Ok(Self { docker, config })
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
}

impl ContainerRuntime for DockerRuntime {
    async fn start(
        &self,
        job: &JobId,
        task: &TaskId,
        spec: &ContainerSpec,
    ) -> Result<String, RuntimeError> {
        self.ensure_image(&spec.image).await?;

        let limits = &spec.limits;
        let memory = limits.memory_bytes.map(saturating_i64);
        let host_config = HostConfig {
            init: Some(true),
            memory,
            // Equal to `memory` => no swap; a swapping container would starve the VM.
            memory_swap: memory,
            nano_cpus: limits.nano_cpus.map(saturating_i64),
            pids_limit: limits
                .pids
                .or(self.config.default_pids_limit)
                .map(saturating_i64),
            network_mode: spec.network.clone(),
            security_opt: Some(vec!["no-new-privileges".to_owned()]),
            ..Default::default()
        };
        let labels = HashMap::from([
            (LABEL_MANAGED.to_owned(), "1".to_owned()),
            (LABEL_JOB.to_owned(), job.to_string()),
            (LABEL_TASK.to_owned(), task.to_string()),
        ]);
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
        let name = container_name(job, task);
        let options = CreateContainerOptionsBuilder::new().name(&name).build();

        let id = self
            .docker
            .create_container(Some(options), body)
            .await
            .map_err(err)?
            .id;
        if let Err(e) = self.docker.start_container(&id, None).await {
            let _ = self.remove(&id).await;
            return Err(err(e));
        }
        Ok(id)
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

    async fn remove_job(&self, job: &JobId) -> Result<usize, RuntimeError> {
        let filters = HashMap::from([(
            "label",
            vec![format!("{LABEL_MANAGED}=1"), format!("{LABEL_JOB}={job}")],
        )]);
        let options = ListContainersOptionsBuilder::default()
            .all(true)
            .filters(&filters)
            .build();
        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .map_err(err)?;
        let mut removed = 0;
        for id in containers.into_iter().filter_map(|c| c.id) {
            self.remove(&id).await?;
            removed += 1;
        }
        Ok(removed)
    }
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

/// `hp-<job>-<task>-<unique>`: readable in `docker ps`, unique across runs.
fn container_name(job: &JobId, task: &TaskId) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("hp-{job}-{task}-{:x}{:x}{seq:x}", std::process::id(), nanos)
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
    fn container_names_are_unique_and_docker_safe() {
        let job = JobId::new("j").unwrap();
        let task = TaskId::new("t").unwrap();
        let a = container_name(&job, &task);
        let b = container_name(&job, &task);
        assert_ne!(a, b);
        assert!(a.starts_with("hp-j-t-"));
        assert!(a.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'));
    }
}
