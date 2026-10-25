//! Integration tests against a real Docker daemon.
//!
//! Ignored by default; run with:
//! `cargo test -p infrastructure --test docker -- --ignored`
//! Override the image with `ORCHESTRATOR_TEST_IMAGE` (default `debian:bookworm-slim`).

use std::time::Duration;

use application::{ContainerRuntime, ExecStatus, OutputObserver};
use domain::{ContainerSpec, JobId, StreamKind, TaskId};
use infrastructure::{DockerConfig, DockerRuntime};

#[derive(Default)]
struct Collect {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl OutputObserver for Collect {
    fn on_output(&mut self, stream: StreamKind, chunk: &[u8]) {
        match stream {
            StreamKind::Stdout => self.stdout.extend_from_slice(chunk),
            StreamKind::Stderr => self.stderr.extend_from_slice(chunk),
        }
    }
}

fn image() -> String {
    std::env::var("ORCHESTRATOR_TEST_IMAGE").unwrap_or_else(|_| "debian:bookworm-slim".into())
}

#[tokio::test]
#[ignore = "requires a Docker daemon"]
async fn container_lifecycle() {
    let runtime = DockerRuntime::connect(DockerConfig::default()).unwrap();
    let job = JobId::new("it-lifecycle").unwrap();
    let task = TaskId::new("t").unwrap();
    let mut spec = ContainerSpec::new(image()).unwrap();
    spec.env = vec![("FOO".into(), "bar".into())];
    spec.workdir = Some("/tmp".into());
    spec.limits.memory_bytes = Some(64 * 1024 * 1024);

    let id = runtime.start(&job, &task, &spec).await.unwrap();

    let mut out = Collect::default();
    let status = runtime
        .exec(
            &id,
            "echo $FOO; pwd; echo oops >&2; exit 4",
            Duration::from_secs(10),
            &mut out,
        )
        .await
        .unwrap();
    assert_eq!(status, ExecStatus::Exited(4));
    assert_eq!(String::from_utf8_lossy(&out.stdout), "bar\n/tmp\n");
    assert_eq!(String::from_utf8_lossy(&out.stderr), "oops\n");

    // State persists between steps of the same task.
    let mut out = Collect::default();
    runtime
        .exec(&id, "touch marker", Duration::from_secs(10), &mut out)
        .await
        .unwrap();
    let status = runtime
        .exec(&id, "test -f marker", Duration::from_secs(10), &mut out)
        .await
        .unwrap();
    assert_eq!(status, ExecStatus::Exited(0));

    let status = runtime
        .exec(&id, "sleep 5", Duration::from_millis(300), &mut out)
        .await
        .unwrap();
    assert_eq!(status, ExecStatus::TimedOut);

    runtime.remove(&id).await.unwrap();
    runtime.remove(&id).await.expect("remove is idempotent");
}

#[tokio::test]
#[ignore = "requires a Docker daemon"]
async fn remove_job_reaps_labelled_containers() {
    let runtime = DockerRuntime::connect(DockerConfig::default()).unwrap();
    let job = JobId::new("it-reap").unwrap();
    let spec = ContainerSpec::new(image()).unwrap();
    for t in ["a", "b"] {
        runtime
            .start(&job, &TaskId::new(t).unwrap(), &spec)
            .await
            .unwrap();
    }
    assert_eq!(runtime.remove_job(&job).await.unwrap(), 2);
    assert_eq!(runtime.remove_job(&job).await.unwrap(), 0);
}
