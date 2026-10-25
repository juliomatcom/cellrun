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

// -------------------------------------------------------------- isolation --
//
// Requires the host firewall: `sudo apps/orchestrator/sandbox-guard/guard.sh apply`.

use std::net::UdpSocket;

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
#[ignore = "requires a Docker daemon and sandbox-guard rules"]
async fn task_containers_are_isolated_from_host_and_each_other() {
    let runtime = DockerRuntime::connect(DockerConfig::default()).unwrap();
    let job = JobId::new("it-isolation").unwrap();
    let spec = ContainerSpec::new(image()).unwrap();
    runtime.remove_job(&job).await.unwrap(); // leftovers of a failed run
    let a = runtime
        .start(&job, &TaskId::new("a").unwrap(), &spec)
        .await
        .unwrap();
    let b = runtime
        .start(&job, &TaskId::new("b").unwrap(), &spec)
        .await
        .unwrap();

    let a_ip = ip_of(&sh(&runtime, &a, "hostname -i").await);
    let b_ip = ip_of(&sh(&runtime, &b, "hostname -i").await);
    let gateway = format!("{}.1", a_ip.rsplit_once('.').unwrap().0);
    let mut targets = vec![
        format!("{gateway}:9"),          // host, via the task's own bridge
        "172.17.0.1:9".to_owned(),       // host, via docker0
        format!("{b_ip}:9"),             // the other task
        "169.254.169.254:80".to_owned(), // cloud metadata
    ];
    // The host's primary address (no packet is sent by `connect`).
    if let Ok(s) = UdpSocket::bind("0.0.0.0:0") {
        if s.connect("1.1.1.1:80").is_ok() {
            targets.push(format!("{}:9", s.local_addr().unwrap().ip()));
        }
    }

    let report = sh(&runtime, &a, &probe(&targets)).await;
    for t in &targets {
        assert!(
            report.contains(&format!("{t} BLOCKED")),
            "{t} is reachable from a task (is sandbox-guard applied?):\n{report}"
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
}
