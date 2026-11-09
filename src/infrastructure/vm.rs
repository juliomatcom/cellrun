//! The microVM backends behind `isolation: "vm"`.
//!
//! The job format and the use case only know "vm"; which VMM runs it is the
//! host's choice (`--vm-backend`). A new backend (e.g. Firecracker) is one
//! more adapter implementing [`ContainerRuntime`] plus one variant here, with
//! static dispatch (the port uses native `async fn`, so no trait objects).

use std::time::Duration;

use crate::application::control::InputReceiver;
use crate::application::{ContainerRuntime, ExecStatus, OutputObserver, RuntimeError};
use crate::domain::{ContainerSpec, JobId, MemoryUsage, TaskId};

use super::sbx::SbxRuntime;

pub enum VmRuntime {
    /// Docker Sandboxes (`sbx`).
    Sbx(SbxRuntime),
}

impl VmRuntime {
    /// Fails unless the backend is installed and answers.
    pub async fn ping(&self) -> Result<(), RuntimeError> {
        match self {
            Self::Sbx(rt) => rt.ping().await,
        }
    }

    /// Removes every VM this backend made for this dispatcher's prefix,
    /// whatever job or run created it.
    pub async fn remove_all(&self) -> Result<usize, RuntimeError> {
        match self {
            Self::Sbx(rt) => rt.remove_all().await,
        }
    }
}

impl ContainerRuntime for VmRuntime {
    async fn start(
        &self,
        job: &JobId,
        task: &TaskId,
        spec: &ContainerSpec,
    ) -> Result<String, RuntimeError> {
        match self {
            Self::Sbx(rt) => rt.start(job, task, spec).await,
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
        match self {
            Self::Sbx(rt) => {
                rt.exec(container_id, command, input, stream, timeout, observer)
                    .await
            }
        }
    }

    async fn memory_usage(&self, container_id: &str) -> Result<MemoryUsage, RuntimeError> {
        match self {
            Self::Sbx(rt) => rt.memory_usage(container_id).await,
        }
    }

    async fn remove(&self, container_id: &str) -> Result<(), RuntimeError> {
        match self {
            Self::Sbx(rt) => rt.remove(container_id).await,
        }
    }

    async fn remove_job(&self, job: &JobId) -> Result<usize, RuntimeError> {
        match self {
            Self::Sbx(rt) => rt.remove_job(job).await,
        }
    }

    async fn remove_owned(&self) -> Result<usize, RuntimeError> {
        match self {
            Self::Sbx(rt) => rt.remove_owned().await,
        }
    }
}
