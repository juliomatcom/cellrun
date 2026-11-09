//! Live test of the `sbx` adapter. Needs `sbx` installed and signed in, a
//! reachable default template image and network access.
//!
//! `cargo test --test sbx -- --ignored`

use std::time::Duration;

use cellrun::application::{ContainerRuntime, ExecStatus, OutputObserver};
use cellrun::domain::{ContainerSpec, JobId, StreamKind, TaskId};
use cellrun::infrastructure::{SbxConfig, SbxRuntime};

const IMAGE: &str = "docker.io/sbx/shell:latest";

#[derive(Default)]
struct Capture(String);

impl OutputObserver for Capture {
    fn on_output(&mut self, _: StreamKind, chunk: &[u8]) {
        self.0.push_str(&String::from_utf8_lossy(chunk));
    }
}

#[tokio::test]
#[ignore = "needs sbx"]
async fn runs_steps_in_a_microvm_and_cleans_up() {
    let runtime = SbxRuntime::new(SbxConfig::default()).unwrap();
    runtime.ping().await.unwrap();
    let (job, task) = (JobId::new("it_job").unwrap(), TaskId::new("t1").unwrap());
    let mut spec = ContainerSpec::new(IMAGE).unwrap();
    spec.env = vec![("GREETING".to_owned(), "hello".to_owned())];

    let id = runtime.start(&job, &task, &spec).await.unwrap();
    let run = |command: &'static str, input: Option<&'static [u8]>| {
        let (runtime, id) = (&runtime, id.clone());
        async move {
            let mut out = Capture::default();
            let status = runtime
                .exec(&id, command, input, None, Duration::from_secs(60), &mut out)
                .await
                .unwrap();
            (status, out.0)
        }
    };

    let (status, out) = run("echo $GREETING; uname -s", None).await;
    assert_eq!(status, ExecStatus::Exited(0));
    assert_eq!(out.trim(), "hello\nLinux");
    let (status, out) = run("tr a-z A-Z", Some(b"abc")).await;
    assert_eq!((status, out.as_str()), (ExecStatus::Exited(0), "ABC"));
    let (status, _) = run("exit 7", None).await;
    assert_eq!(status, ExecStatus::Exited(7));

    assert!(runtime.remove_job(&job).await.unwrap() >= 1);
    // Already gone: removing again is not an error.
    runtime.remove(&id).await.unwrap();
}

/// The promises of VM isolation, checked from inside a VM: a secret's real value is injected for its host only and
/// never exists in the VM, and the host's own network is out of reach.
#[tokio::test]
#[ignore = "needs sbx and the internet"]
async fn secrets_stay_on_the_host_and_the_host_network_is_denied() {
    use cellrun::domain::SecretSpec;

    const REAL: &str = "CANARY-REAL-VALUE-4f9a";
    let runtime = SbxRuntime::new(SbxConfig::default()).unwrap();
    let (job, task) = (JobId::new("it_secret").unwrap(), TaskId::new("t1").unwrap());
    let mut spec = ContainerSpec::new(IMAGE).unwrap();
    spec.secrets = vec![SecretSpec {
        env: "CANARY".to_owned(),
        value: REAL.to_owned(),
        hosts: vec!["httpbin.org".to_owned()],
    }];
    let id = runtime.start(&job, &task, &spec).await.unwrap();
    let run = |command: &'static str| {
        let (runtime, id) = (&runtime, id.clone());
        async move {
            let mut out = Capture::default();
            runtime
                .exec(&id, command, None, None, Duration::from_secs(90), &mut out)
                .await
                .unwrap();
            out.0
        }
    };

    // The VM holds a placeholder, not the value.
    let env = run("echo $CANARY").await;
    assert!(
        !env.contains(REAL) && env.trim().starts_with("ph-"),
        "{env}"
    );
    // Not anywhere on disk or in any process's environment either (root can read them all).
    // The pattern comes from a file outside the scanned paths: `sudo` puts its command line in its own environment.
    let scan = run("printf CANARY-REAL-VALUE-4f9a > /dev/shm/p; \
         sudo grep -rlF -f /dev/shm/p /proc/*/environ /etc /home /tmp /var 2>/dev/null; echo done")
    .await;
    assert_eq!(scan.trim(), "done", "the real value is in the VM: {scan}");
    // Sent to its host, the proxy swaps the value in; to any other host it stays the placeholder.
    let bound = run("curl -s -m 30 -H \"X-Probe: $CANARY\" https://httpbin.org/headers").await;
    assert!(bound.contains(REAL), "not injected for its host: {bound}");
    let other = run("curl -s -m 30 -H \"X-Probe: $CANARY\" https://postman-echo.com/headers").await;
    assert!(!other.contains(REAL), "leaked to another host: {other}");

    // The host's own network: metadata, "this host", LAN, loopback names.
    for target in [
        "http://169.254.169.254/",
        "http://0.0.0.0:22/",
        "http://10.0.0.1/",
        "http://192.168.1.1/",
        "http://[::ffff:10.1.2.3]/",
        "http://[::ffff:169.254.169.254]/",
        "http://[::1]:4001/",
        "http://host.docker.internal/",
        "http://localhost:4001/",
    ] {
        let code = run(Box::leak(
            format!("curl -s -m 8 -o /dev/null -w '%{{http_code}}' {target}; true")
                .into_boxed_str(),
        ))
        .await;
        assert!(
            matches!(code.trim(), "403" | "000"),
            "{target} answered {code}"
        );
    }
    runtime.remove(&id).await.unwrap();
}
