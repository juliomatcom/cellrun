//! `Routed` sends each task to the runtime of its isolation level and keeps
//! later calls on the same one.

use std::sync::Mutex;
use std::time::Duration;

use cellrun::application::{
    ContainerRuntime, ExecStatus, InputReceiver, OutputObserver, Routed, RuntimeError,
};
use cellrun::domain::{ContainerSpec, Isolation, JobId, MemoryUsage, TaskId};

/// Records the calls it gets; ids carry its name so routing is visible.
struct Fake {
    name: &'static str,
    calls: Mutex<Vec<String>>,
}

impl Fake {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn log(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl ContainerRuntime for &Fake {
    async fn start(
        &self,
        _: &JobId,
        task: &TaskId,
        _: &ContainerSpec,
    ) -> Result<String, RuntimeError> {
        self.log(format!("start {task}"));
        Ok(format!("{}-id", self.name))
    }

    async fn exec(
        &self,
        id: &str,
        command: &str,
        _: Option<&[u8]>,
        _: Option<InputReceiver>,
        _: Duration,
        _: &mut dyn OutputObserver,
    ) -> Result<ExecStatus, RuntimeError> {
        self.log(format!("exec {id} {command}"));
        Ok(ExecStatus::Exited(0))
    }

    async fn memory_usage(&self, id: &str) -> Result<MemoryUsage, RuntimeError> {
        self.log(format!("memory {id}"));
        Ok(MemoryUsage::default())
    }

    async fn remove(&self, id: &str) -> Result<(), RuntimeError> {
        self.log(format!("remove {id}"));
        Ok(())
    }

    async fn remove_job(&self, _: &JobId) -> Result<usize, RuntimeError> {
        Ok(2)
    }

    async fn remove_owned(&self) -> Result<usize, RuntimeError> {
        Ok(1)
    }
}

struct Sink;

impl OutputObserver for Sink {
    fn on_output(&mut self, _: cellrun::domain::StreamKind, _: &[u8]) {}
}

fn spec(isolation: Isolation) -> ContainerSpec {
    let mut spec = ContainerSpec::new("img").unwrap();
    spec.isolation = isolation;
    spec
}

#[tokio::test]
async fn each_task_runs_on_the_runtime_of_its_isolation_level() {
    let (docker, vm) = (Fake::new("docker"), Fake::new("vm"));
    let routed = Routed::new(Some(&docker), Some(&vm));
    let (job, task) = (JobId::new("j").unwrap(), TaskId::new("t").unwrap());

    let c = routed
        .start(&job, &task, &spec(Isolation::Container))
        .await
        .unwrap();
    let v = routed
        .start(&job, &task, &spec(Isolation::Vm))
        .await
        .unwrap();
    assert_eq!(c, "docker-id");
    assert_ne!(
        v, "vm-id",
        "VM ids are tagged so later calls find their runtime"
    );

    for id in [&c, &v] {
        routed
            .exec(id, "ls", None, None, Duration::from_secs(1), &mut Sink)
            .await
            .unwrap();
        routed.memory_usage(id).await.unwrap();
        routed.remove(id).await.unwrap();
    }
    assert_eq!(
        docker.calls(),
        [
            "start t",
            "exec docker-id ls",
            "memory docker-id",
            "remove docker-id"
        ]
    );
    assert_eq!(
        vm.calls(),
        ["start t", "exec vm-id ls", "memory vm-id", "remove vm-id"],
        "the runtime never sees the routing tag"
    );
}

#[tokio::test]
async fn a_level_without_a_runtime_refuses_its_tasks() {
    let docker = Fake::new("docker");
    let routed: Routed<&Fake, &Fake> = Routed::new(Some(&docker), None);
    let (job, task) = (JobId::new("j").unwrap(), TaskId::new("t").unwrap());
    let err = routed
        .start(&job, &task, &spec(Isolation::Vm))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("vm isolation is not available"),
        "{err}"
    );
}

#[tokio::test]
async fn cleanup_covers_every_runtime() {
    let (docker, vm) = (Fake::new("docker"), Fake::new("vm"));
    let routed = Routed::new(Some(&docker), Some(&vm));
    assert_eq!(
        routed.remove_job(&JobId::new("j").unwrap()).await.unwrap(),
        4
    );
    assert_eq!(routed.remove_owned().await.unwrap(), 2);
}
