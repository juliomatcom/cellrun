use std::time::Duration;

use crate::job::{JobId, TaskId};
use crate::report::{MemoryUsage, StepOutcome, TaskStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Stdout,
    Stderr,
}

/// Something observable that happened while running a job. Events borrow
/// their data so emitting one never allocates.
#[derive(Debug, Clone, PartialEq)]
pub enum Event<'a> {
    JobStarted {
        job: &'a JobId,
        tasks: usize,
    },
    TaskStarted {
        task: &'a TaskId,
        image: &'a str,
    },
    ContainerReady {
        task: &'a TaskId,
        container_id: &'a str,
    },
    StepStarted {
        task: &'a TaskId,
        index: usize,
        name: &'a str,
    },
    StepOutput {
        task: &'a TaskId,
        index: usize,
        stream: StreamKind,
        line: &'a str,
    },
    StepFinished {
        task: &'a TaskId,
        index: usize,
        outcome: &'a StepOutcome,
        duration: Duration,
    },
    TaskFinished {
        task: &'a TaskId,
        status: TaskStatus,
        duration: Duration,
        error: Option<&'a str>,
        memory: Option<&'a MemoryUsage>,
    },
    JobFinished {
        job: &'a JobId,
        succeeded: bool,
        duration: Duration,
    },
    Warning {
        task: Option<&'a TaskId>,
        message: &'a str,
    },
}
