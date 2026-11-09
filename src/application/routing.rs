//! Picks the runtime for each task from its isolation level.
//!
//! Both runtimes implement the same port, so the use case stays unaware:
//! `start` routes on [`Isolation`] and tags the id it returns, every later
//! call routes on that tag.

use std::time::Duration;

use crate::application::control::InputReceiver;
use crate::application::ports::{ContainerRuntime, ExecStatus, OutputObserver, RuntimeError};
use crate::domain::{ContainerSpec, Isolation, JobId, MemoryUsage, TaskId};

/// Ids of the VM runtime carry this prefix; container ids stay untouched.
const VM_TAG: &str = "vm:";

/// A runtime per isolation level. A level without a runtime refuses its tasks
/// with an error (the host has no such backend).
pub struct Routed<C, V> {
    container: Option<C>,
    vm: Option<V>,
}

impl<C: ContainerRuntime, V: ContainerRuntime> Routed<C, V> {
    pub fn new(container: Option<C>, vm: Option<V>) -> Self {
        Self { container, vm }
    }

    fn container(&self) -> Result<&C, RuntimeError> {
        self.container
            .as_ref()
            .ok_or_else(|| RuntimeError::new("container isolation is not available on this host"))
    }

    fn vm(&self) -> Result<&V, RuntimeError> {
        self.vm
            .as_ref()
            .ok_or_else(|| RuntimeError::new("vm isolation is not available on this host"))
    }
}

/// Sums the counts of both runtimes; a failure of one does not skip the other.
fn merge(results: [Option<Result<usize, RuntimeError>>; 2]) -> Result<usize, RuntimeError> {
    let (mut total, mut first_error) = (0, None);
    for result in results.into_iter().flatten() {
        match result {
            Ok(n) => total += n,
            Err(e) => first_error = first_error.or(Some(e)),
        }
    }
    first_error.map_or(Ok(total), Err)
}

impl<C: ContainerRuntime, V: ContainerRuntime> ContainerRuntime for Routed<C, V> {
    async fn start(
        &self,
        job: &JobId,
        task: &TaskId,
        spec: &ContainerSpec,
    ) -> Result<String, RuntimeError> {
        match spec.isolation {
            Isolation::Container => self.container()?.start(job, task, spec).await,
            Isolation::Vm => {
                let id = self.vm()?.start(job, task, spec).await?;
                Ok(format!("{VM_TAG}{id}"))
            }
        }
    }

    async fn exec(
        &self,
        container_id: &str,
        command: &str,
        input: Option<&[u8]>,
        stream: Option<InputReceiver>,
        timeout: Duration,
        observer: &mut dyn OutputObserver,
    ) -> Result<ExecStatus, RuntimeError> {
        match container_id.strip_prefix(VM_TAG) {
            Some(id) => {
                self.vm()?
                    .exec(id, command, input, stream, timeout, observer)
                    .await
            }
            None => {
                self.container()?
                    .exec(container_id, command, input, stream, timeout, observer)
                    .await
            }
        }
    }

    async fn memory_usage(&self, container_id: &str) -> Result<MemoryUsage, RuntimeError> {
        match container_id.strip_prefix(VM_TAG) {
            Some(id) => self.vm()?.memory_usage(id).await,
            None => self.container()?.memory_usage(container_id).await,
        }
    }

    async fn remove(&self, container_id: &str) -> Result<(), RuntimeError> {
        match container_id.strip_prefix(VM_TAG) {
            Some(id) => self.vm()?.remove(id).await,
            None => self.container()?.remove(container_id).await,
        }
    }

    async fn remove_job(&self, job: &JobId) -> Result<usize, RuntimeError> {
        let container = match &self.container {
            Some(c) => Some(c.remove_job(job).await),
            None => None,
        };
        let vm = match &self.vm {
            Some(v) => Some(v.remove_job(job).await),
            None => None,
        };
        merge([container, vm])
    }

    async fn remove_owned(&self) -> Result<usize, RuntimeError> {
        let container = match &self.container {
            Some(c) => Some(c.remove_owned().await),
            None => None,
        };
        let vm = match &self.vm {
            Some(v) => Some(v.remove_owned().await),
            None => None,
        };
        merge([container, vm])
    }
}
