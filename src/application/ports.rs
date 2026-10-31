use std::fmt;
use std::future::Future;
use std::time::Duration;

use crate::domain::{ContainerSpec, Event, JobId, MemoryUsage, StreamKind, TaskId};

/// Error reported by a [`ContainerRuntime`] implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError(pub String);

impl RuntimeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RuntimeError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecStatus {
    Exited(i64),
    TimedOut,
}

/// Receives raw output chunks while a command runs.
pub trait OutputObserver: Send {
    fn on_output(&mut self, stream: StreamKind, chunk: &[u8]);
}

/// Driven port: something able to run ephemeral containers.
pub trait ContainerRuntime: Send + Sync {
    /// Creates and starts a long-lived container for `task`; returns its id.
    ///
    /// Isolation contract: every container gets its own network and no
    /// mounts, so it shares nothing with the host or with other tasks.
    /// Inside, the workload has full access (root, internet).
    fn start(
        &self,
        job: &JobId,
        task: &TaskId,
        spec: &ContainerSpec,
    ) -> impl Future<Output = Result<String, RuntimeError>> + Send;

    /// Runs `command` through a shell inside the container, streaming output
    /// to `observer`. Must return [`ExecStatus::TimedOut`] after `timeout`.
    /// When `input` is `Some`, those bytes are piped to the command's stdin
    /// and stdin is then closed (EOF); writing counts against `timeout`.
    fn exec(
        &self,
        container_id: &str,
        command: &str,
        input: Option<&[u8]>,
        timeout: Duration,
        observer: &mut dyn OutputObserver,
    ) -> impl Future<Output = Result<ExecStatus, RuntimeError>> + Send;

    /// Peak memory the container has used so far (all its processes).
    fn memory_usage(
        &self,
        container_id: &str,
    ) -> impl Future<Output = Result<MemoryUsage, RuntimeError>> + Send;

    /// Force-removes the container and its network. Must be idempotent.
    fn remove(&self, container_id: &str) -> impl Future<Output = Result<(), RuntimeError>> + Send;

    /// Removes every container and network belonging to `job`
    /// (crash/signal cleanup).
    /// Returns how many were removed.
    fn remove_job(&self, job: &JobId) -> impl Future<Output = Result<usize, RuntimeError>> + Send;

    /// Removes every container (and network) created by *this* runtime
    /// instance, leaving concurrent runs of the same job alone (signal
    /// cleanup). Returns how many containers were removed.
    fn remove_owned(&self) -> impl Future<Output = Result<usize, RuntimeError>> + Send;
}

/// Driven port: where progress events and logs go.
pub trait EventSink: Send + Sync {
    fn emit(&self, event: &Event<'_>);
}

impl<T: EventSink + ?Sized> EventSink for &T {
    fn emit(&self, event: &Event<'_>) {
        (**self).emit(event);
    }
}

impl<T: ContainerRuntime + ?Sized> ContainerRuntime for &T {
    fn start(
        &self,
        job: &JobId,
        task: &TaskId,
        spec: &ContainerSpec,
    ) -> impl Future<Output = Result<String, RuntimeError>> + Send {
        (**self).start(job, task, spec)
    }

    fn exec(
        &self,
        container_id: &str,
        command: &str,
        input: Option<&[u8]>,
        timeout: Duration,
        observer: &mut dyn OutputObserver,
    ) -> impl Future<Output = Result<ExecStatus, RuntimeError>> + Send {
        (**self).exec(container_id, command, input, timeout, observer)
    }

    fn memory_usage(
        &self,
        container_id: &str,
    ) -> impl Future<Output = Result<MemoryUsage, RuntimeError>> + Send {
        (**self).memory_usage(container_id)
    }

    fn remove(&self, container_id: &str) -> impl Future<Output = Result<(), RuntimeError>> + Send {
        (**self).remove(container_id)
    }

    fn remove_job(&self, job: &JobId) -> impl Future<Output = Result<usize, RuntimeError>> + Send {
        (**self).remove_job(job)
    }

    fn remove_owned(&self) -> impl Future<Output = Result<usize, RuntimeError>> + Send {
        (**self).remove_owned()
    }
}
