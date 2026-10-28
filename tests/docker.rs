//! Integration tests against a real Docker daemon.
//!
//! Ignored by default; run with:
//! `cargo test -p infrastructure --test docker -- --ignored`
//! Override the image with `DISPATCHER_TEST_IMAGE` (default `debian:bookworm-slim`).

use std::time::Duration;

use cellrun::application::{ContainerRuntime, ExecStatus, OutputObserver};
use cellrun::domain::{ContainerSpec, JobId, StreamKind, TaskId};
use cellrun::infrastructure::{DockerConfig, DockerRuntime};

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
    std::env::var("DISPATCHER_TEST_IMAGE").unwrap_or_else(|_| "debian:bookworm-slim".into())
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

// -------------------------------------------------------------- isolation --
//
// What cellrun itself guarantees: tasks cannot reach each other or other Docker
// networks, and have full access inside. Blocking the host, the LAN and cloud
// metadata is the host firewall's job, which the user of cellrun owns (README,
// "Host firewall"); test that where the firewall is applied.

use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig, NetworkCreateRequest};
use bollard::query_parameters::{CreateContainerOptionsBuilder, RemoveContainerOptionsBuilder};

const BACKEND_NET: &str = "it-isolation-backend";
const BACKEND_SVC: &str = "it-isolation-backend-svc";

/// Stands in for another app's service on its own Docker network (e.g. a
/// database). Returns its IP.
async fn start_backend_service(docker: &Docker) -> String {
    remove_backend_service(docker).await;
    docker
        .create_network(NetworkCreateRequest {
            name: BACKEND_NET.into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let body = ContainerCreateBody {
        image: Some(image()),
        cmd: Some(vec!["sleep".into(), "300".into()]),
        host_config: Some(HostConfig {
            network_mode: Some(BACKEND_NET.into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let options = CreateContainerOptionsBuilder::new()
        .name(BACKEND_SVC)
        .build();
    docker.create_container(Some(options), body).await.unwrap();
    docker.start_container(BACKEND_SVC, None).await.unwrap();
    let info = docker.inspect_container(BACKEND_SVC, None).await.unwrap();
    info.network_settings
        .and_then(|n| n.networks)
        .and_then(|mut n| n.remove(BACKEND_NET))
        .and_then(|e| e.ip_address)
        .unwrap()
}

async fn remove_backend_service(docker: &Docker) {
    let force = RemoveContainerOptionsBuilder::default().force(true).build();
    let _ = docker.remove_container(BACKEND_SVC, Some(force)).await;
    let _ = docker.remove_network(BACKEND_NET).await;
}

async fn sh(runtime: &DockerRuntime, id: &str, script: &str) -> String {
    let mut out = Collect::default();
    runtime
        .exec(id, script, Duration::from_secs(60), &mut out)
        .await
        .unwrap();
    String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr)
}

/// For each `ip:port` prints `OPEN`, `REACHABLE` (connection refused: the
/// host answered, just no listener) or `BLOCKED` (silently dropped).
fn probe(targets: &[String]) -> String {
    targets
        .iter()
        .map(|t| {
            let (host, port) = t.rsplit_once(':').unwrap();
            format!(
                "r=$(timeout 3 bash -c 'echo > /dev/tcp/{host}/{port}' 2>&1) && echo '{t} OPEN' || \
                 {{ case \"$r\" in *refused*) echo '{t} REACHABLE';; *) echo '{t} BLOCKED';; esac; }}\n"
            )
        })
        .collect()
}

fn ip_of(runtime_out: &str) -> String {
    runtime_out.split_whitespace().next().unwrap().to_owned()
}

#[tokio::test]
#[ignore = "requires a Docker daemon"]
async fn task_containers_are_isolated_from_each_other() {
    let runtime = DockerRuntime::connect(DockerConfig::default()).unwrap();
    let job = JobId::new("it-isolation").unwrap();
    let spec = ContainerSpec::new(image()).unwrap();
    runtime.remove_job(&job).await.unwrap(); // leftovers of a failed run
    let docker = Docker::connect_with_local_defaults().unwrap();
    let backend_ip = start_backend_service(&docker).await;
    let a = runtime
        .start(&job, &TaskId::new("a").unwrap(), &spec)
        .await
        .unwrap();
    let b = runtime
        .start(&job, &TaskId::new("b").unwrap(), &spec)
        .await
        .unwrap();

    let b_ip = ip_of(&sh(&runtime, &b, "hostname -i").await);
    let targets = [
        format!("{b_ip}:9"),       // the other task
        format!("{backend_ip}:9"), // a service on another Docker network
    ];

    let report = sh(&runtime, &a, &probe(&targets)).await;
    for t in &targets {
        assert!(
            report.contains(&format!("{t} BLOCKED")),
            "{t} is reachable from a task:\n{report}"
        );
    }

    // Full access inside: root, writable system dirs, no host mounts.
    assert_eq!(sh(&runtime, &a, "id -u").await.trim(), "0");
    assert_eq!(
        sh(&runtime, &a, "touch /etc/probe && echo ok").await.trim(),
        "ok"
    );
    let mounts = sh(
        &runtime,
        &a,
        "grep -cE 'docker.sock|/home|/root ' /proc/mounts || true",
    )
    .await;
    assert_eq!(mounts.trim(), "0", "unexpected mounts");

    assert_eq!(runtime.remove_job(&job).await.unwrap(), 2);
    remove_backend_service(&docker).await;
}

#[tokio::test]
#[ignore = "requires a Docker daemon"]
async fn remove_owned_spares_concurrent_runs_of_the_same_job() {
    let job = JobId::new("it-owned").unwrap();
    let task = TaskId::new("t").unwrap();
    let spec = ContainerSpec::new(image()).unwrap();
    let run_a = DockerRuntime::connect(DockerConfig::default()).unwrap();
    let run_b = DockerRuntime::connect(DockerConfig::default()).unwrap();
    run_a.remove_job(&job).await.unwrap();
    run_a.start(&job, &task, &spec).await.unwrap();
    let b = run_b.start(&job, &task, &spec).await.unwrap();

    assert_eq!(run_a.remove_owned().await.unwrap(), 1);
    // Run B's container is still alive and usable.
    let mut out = Collect::default();
    let status = run_b
        .exec(&b, "true", Duration::from_secs(10), &mut out)
        .await
        .unwrap();
    assert_eq!(status, ExecStatus::Exited(0));
    assert_eq!(run_b.remove_owned().await.unwrap(), 1);
}
