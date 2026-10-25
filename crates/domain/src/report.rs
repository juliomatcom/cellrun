use std::time::Duration;

use crate::job::{JobId, TaskId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepOutcome {
    Succeeded,
    Failed {
        exit_code: i64,
    },
    TimedOut,
    /// Not executed because an earlier step failed.
    Skipped,
    /// The runtime could not execute the step at all.
    Error {
        message: String,
    },
}

impl StepOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepReport {
    pub name: String,
    pub outcome: StepOutcome,
    pub duration: Duration,
    pub stdout_tail: String,
    pub stderr_tail: String,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
}

impl StepReport {
    pub fn skipped(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            outcome: StepOutcome::Skipped,
            duration: Duration::ZERO,
            stdout_tail: String::new(),
            stderr_tail: String::new(),
            stdout_bytes: 0,
            stderr_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Succeeded,
    /// At least one step failed or timed out.
    Failed,
    /// Infrastructure problem (image pull, container create, ...).
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskReport {
    pub id: TaskId,
    pub image: String,
    pub container_id: Option<String>,
    pub status: TaskStatus,
    pub error: Option<String>,
    pub duration: Duration,
    pub steps: Vec<StepReport>,
}

impl TaskReport {
    /// Derives the status from step outcomes; any non-ignored failure fails the
    /// task. `ignored` marks steps whose failure is tolerated.
    pub fn status_from_steps(steps: &[StepReport], ignored: impl Fn(usize) -> bool) -> TaskStatus {
        let failed = steps.iter().enumerate().any(|(i, s)| {
            !matches!(s.outcome, StepOutcome::Succeeded | StepOutcome::Skipped) && !ignored(i)
        });
        if failed {
            TaskStatus::Failed
        } else {
            TaskStatus::Succeeded
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobReport {
    pub id: JobId,
    pub duration: Duration,
    pub tasks: Vec<TaskReport>,
}

impl JobReport {
    pub fn succeeded(&self) -> bool {
        self.tasks.iter().all(|t| t.status == TaskStatus::Succeeded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(outcome: StepOutcome) -> StepReport {
        StepReport {
            outcome,
            ..StepReport::skipped("s")
        }
    }

    #[test]
    fn status_fails_on_any_unignored_failure() {
        let steps = [
            step(StepOutcome::Succeeded),
            step(StepOutcome::Failed { exit_code: 1 }),
        ];
        assert_eq!(
            TaskReport::status_from_steps(&steps, |_| false),
            TaskStatus::Failed
        );
        assert_eq!(
            TaskReport::status_from_steps(&steps, |i| i == 1),
            TaskStatus::Succeeded
        );
    }

    #[test]
    fn skipped_steps_do_not_fail_the_task_by_themselves() {
        let steps = [step(StepOutcome::Succeeded), step(StepOutcome::Skipped)];
        assert_eq!(
            TaskReport::status_from_steps(&steps, |_| false),
            TaskStatus::Succeeded
        );
    }

    #[test]
    fn timeouts_and_errors_fail_the_task() {
        for outcome in [
            StepOutcome::TimedOut,
            StepOutcome::Error {
                message: "x".into(),
            },
        ] {
            assert_eq!(
                TaskReport::status_from_steps(&[step(outcome)], |_| false),
                TaskStatus::Failed
            );
        }
    }
}
