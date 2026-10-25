use std::borrow::Cow;
use std::time::Instant;

use futures_util::stream::{self, StreamExt};

use domain::{
    Event, Job, JobId, JobReport, LineSplitter, StepOutcome, StepReport, StreamKind, TailBuffer,
    Task, TaskId, TaskReport, TaskStatus,
};

use crate::ports::{ContainerRuntime, EventSink, ExecStatus, OutputObserver};

/// Memory caps for captured output, per step and stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputLimits {
    /// Bytes of output kept in the final report (the tail).
    pub tail_bytes: usize,
    /// Longest line emitted as a single event; longer lines are split.
    pub max_line_bytes: usize,
}

impl Default for OutputLimits {
    fn default() -> Self {
        Self {
            tail_bytes: 16 * 1024,
            max_line_bytes: 8 * 1024,
        }
    }
}

/// Use case: run every task of a job in its own ephemeral container.
pub struct RunJob<R, S> {
    runtime: R,
    sink: S,
    limits: OutputLimits,
}

impl<R: ContainerRuntime, S: EventSink> RunJob<R, S> {
    pub fn new(runtime: R, sink: S, limits: OutputLimits) -> Self {
        Self {
            runtime,
            sink,
            limits,
        }
    }

    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    /// Runs the job with at most `job.max_parallel` containers alive at once.
    /// Infrastructure failures are captured in the report, never returned.
    pub async fn execute(&self, job: &Job) -> JobReport {
        let started = Instant::now();
        self.sink.emit(&Event::JobStarted {
            job: &job.id,
            tasks: job.tasks.len(),
        });

        // Unordered so a finished task frees its slot immediately, even if an
        // earlier task is still running; the report is re-sorted afterwards.
        let mut indexed = stream::iter(job.tasks.iter().enumerate())
            .map(|(i, task)| async move { (i, self.run_task(&job.id, task).await) })
            .buffer_unordered(job.max_parallel)
            .collect::<Vec<_>>()
            .await;
        indexed.sort_unstable_by_key(|(i, _)| *i);
        let tasks = indexed.into_iter().map(|(_, report)| report).collect();

        let report = JobReport {
            id: job.id.clone(),
            duration: started.elapsed(),
            tasks,
        };
        self.sink.emit(&Event::JobFinished {
            job: &job.id,
            succeeded: report.succeeded(),
            duration: report.duration,
        });
        report
    }

    async fn run_task(&self, job: &JobId, task: &Task) -> TaskReport {
        let started = Instant::now();
        let image = task.container.image.as_str();
        self.sink.emit(&Event::TaskStarted {
            task: &task.id,
            image,
        });

        let container_id = match self.runtime.start(job, &task.id, &task.container).await {
            Ok(id) => id,
            Err(err) => {
                let message = format!("failed to start container: {err}");
                return self.finish_task(
                    task,
                    None,
                    TaskStatus::Error,
                    Some(message),
                    vec![],
                    started,
                );
            }
        };
        self.sink.emit(&Event::ContainerReady {
            task: &task.id,
            container_id: &container_id,
        });

        let mut steps = Vec::with_capacity(task.steps.len());
        let mut halted = false;
        for (index, step) in task.steps.iter().enumerate() {
            let report = if halted {
                StepReport::skipped(&step.name)
            } else {
                self.sink.emit(&Event::StepStarted {
                    task: &task.id,
                    index,
                    name: &step.name,
                });
                let step_started = Instant::now();
                let mut capture = StepCapture::new(&self.sink, &task.id, index, self.limits);
                let outcome = match self
                    .runtime
                    .exec(&container_id, &step.run, step.timeout, &mut capture)
                    .await
                {
                    Ok(ExecStatus::Exited(0)) => StepOutcome::Succeeded,
                    Ok(ExecStatus::Exited(exit_code)) => StepOutcome::Failed { exit_code },
                    Ok(ExecStatus::TimedOut) => StepOutcome::TimedOut,
                    Err(err) => StepOutcome::Error {
                        message: err.to_string(),
                    },
                };
                capture.into_report(&step.name, outcome, step_started.elapsed())
            };
            halted |= report.outcome.halts_task(step.continue_on_error);
            self.sink.emit(&Event::StepFinished {
                task: &task.id,
                index,
                outcome: &report.outcome,
                duration: report.duration,
            });
            steps.push(report);
        }

        if let Err(err) = self.runtime.remove(&container_id).await {
            let message = format!("failed to remove container {container_id}: {err}");
            self.sink.emit(&Event::Warning {
                task: Some(&task.id),
                message: &message,
            });
        }

        let status = TaskReport::status_from_steps(&steps, |i| task.steps[i].continue_on_error);
        self.finish_task(task, Some(container_id), status, None, steps, started)
    }

    fn finish_task(
        &self,
        task: &Task,
        container_id: Option<String>,
        status: TaskStatus,
        error: Option<String>,
        steps: Vec<StepReport>,
        started: Instant,
    ) -> TaskReport {
        let duration = started.elapsed();
        self.sink.emit(&Event::TaskFinished {
            task: &task.id,
            status,
            duration,
            error: error.as_deref(),
        });
        TaskReport {
            id: task.id.clone(),
            image: task.container.image.clone(),
            container_id,
            status,
            error,
            duration,
            steps,
        }
    }
}

/// Splits a step's output into line events and keeps a bounded tail of each
/// stream for the report.
struct StepCapture<'a, S> {
    sink: &'a S,
    task: &'a TaskId,
    index: usize,
    stdout: (LineSplitter, TailBuffer),
    stderr: (LineSplitter, TailBuffer),
}

impl<'a, S: EventSink> StepCapture<'a, S> {
    fn new(sink: &'a S, task: &'a TaskId, index: usize, limits: OutputLimits) -> Self {
        let pair = || {
            (
                LineSplitter::new(limits.max_line_bytes),
                TailBuffer::new(limits.tail_bytes),
            )
        };
        Self {
            sink,
            task,
            index,
            stdout: pair(),
            stderr: pair(),
        }
    }

    fn into_report(
        mut self,
        name: &str,
        outcome: StepOutcome,
        duration: std::time::Duration,
    ) -> StepReport {
        let (sink, task, index) = (self.sink, self.task, self.index);
        for (stream, (splitter, _)) in [
            (StreamKind::Stdout, &mut self.stdout),
            (StreamKind::Stderr, &mut self.stderr),
        ] {
            splitter.finish(|line| emit_line(sink, task, index, stream, line));
        }
        StepReport {
            name: name.to_owned(),
            outcome,
            duration,
            stdout_tail: self.stdout.1.to_string_lossy(),
            stderr_tail: self.stderr.1.to_string_lossy(),
            stdout_bytes: self.stdout.1.total_bytes(),
            stderr_bytes: self.stderr.1.total_bytes(),
        }
    }
}

impl<S: EventSink> OutputObserver for StepCapture<'_, S> {
    fn on_output(&mut self, stream: StreamKind, chunk: &[u8]) {
        let (splitter, tail) = match stream {
            StreamKind::Stdout => &mut self.stdout,
            StreamKind::Stderr => &mut self.stderr,
        };
        tail.push(chunk);
        let (sink, task, index) = (self.sink, self.task, self.index);
        splitter.push(chunk, |line| emit_line(sink, task, index, stream, line));
    }
}

fn emit_line<S: EventSink>(sink: &S, task: &TaskId, index: usize, stream: StreamKind, line: &[u8]) {
    let line: Cow<'_, str> = String::from_utf8_lossy(line);
    sink.emit(&Event::StepOutput {
        task,
        index,
        stream,
        line: &line,
    });
}
