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
