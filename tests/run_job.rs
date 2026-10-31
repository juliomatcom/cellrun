use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cellrun::application::{
    ContainerRuntime, EventSink, ExecStatus, OutputLimits, OutputObserver, RunJob, RuntimeError,
};
use cellrun::domain::{
    ContainerSpec, Event, Job, JobId, MemoryUsage, Step, StepOutcome, StreamKind, Task, TaskId,
    TaskStatus,
};

/// What the fake runtime does for a given command.
#[derive(Clone)]
struct Script {
    stdout: &'static [&'static str],
    stderr: &'static [&'static str],
    result: Result<ExecStatus, &'static str>,
    delay: Duration,
}

impl Script {
    fn ok(stdout: &'static [&'static str]) -> Self {
        Self {
            stdout,
            stderr: &[],
            result: Ok(ExecStatus::Exited(0)),
            delay: Duration::ZERO,
        }
    }
}

#[derive(Default)]
struct FakeRuntime {
    scripts: HashMap<&'static str, Script>,
    fail_start: bool,
    delay: Duration,
    calls: Mutex<Vec<String>>,
    live: AtomicUsize,
    peak: AtomicUsize,
}

impl FakeRuntime {
    fn with(scripts: &[(&'static str, Script)]) -> Self {
        Self {
            scripts: scripts.iter().cloned().collect(),
            ..Self::default()
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl ContainerRuntime for FakeRuntime {
    async fn start(
        &self,
        _: &JobId,
        task: &TaskId,
        _: &ContainerSpec,
    ) -> Result<String, RuntimeError> {
        self.calls.lock().unwrap().push(format!("start {task}"));
        if self.fail_start {
            return Err(RuntimeError::new("no such image"));
        }
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
        Ok(format!("c-{task}"))
    }

    async fn exec(
        &self,
        container_id: &str,
        command: &str,
        _: Option<&[u8]>,
        _: Duration,
        observer: &mut dyn OutputObserver,
    ) -> Result<ExecStatus, RuntimeError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("exec {container_id} {command}"));
        let script = self
            .scripts
            .get(command)
            .cloned()
            .unwrap_or(Script::ok(&[]));
        tokio::time::sleep(self.delay + script.delay).await;
        for chunk in script.stdout {
            observer.on_output(StreamKind::Stdout, chunk.as_bytes());
        }
        for chunk in script.stderr {
            observer.on_output(StreamKind::Stderr, chunk.as_bytes());
        }
        script.result.map_err(RuntimeError::new)
    }

    async fn memory_usage(&self, container_id: &str) -> Result<MemoryUsage, RuntimeError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("memory {container_id}"));
        Ok(MemoryUsage {
            peak_bytes: Some(1 << 20),
            ..MemoryUsage::default()
        })
    }

    async fn remove(&self, container_id: &str) -> Result<(), RuntimeError> {
        self.live.fetch_sub(1, Ordering::SeqCst);
        self.calls
            .lock()
            .unwrap()
            .push(format!("remove {container_id}"));
        Ok(())
    }

    async fn remove_job(&self, _: &JobId) -> Result<usize, RuntimeError> {
        Ok(0)
    }

    async fn remove_owned(&self) -> Result<usize, RuntimeError> {
        Ok(0)
    }
}

#[derive(Default)]
struct RecordingSink(Mutex<Vec<String>>);

impl EventSink for RecordingSink {
    fn emit(&self, event: &Event<'_>) {
        let line = match event {
            Event::StepOutput {
                task,
                index,
                stream,
                line,
            } => {
                format!("out {task}#{index} {stream:?} {line}")
            }
            Event::StepFinished {
                task,
                index,
                outcome,
                ..
            } => {
                format!("step {task}#{index} {outcome:?}")
            }
            Event::TaskFinished { task, status, .. } => format!("task {task} {status:?}"),
            Event::Warning { message, .. } => format!("warn {message}"),
            _ => return,
        };
        self.0.lock().unwrap().push(line);
    }
}

fn step(run: &str) -> Step {
    Step::new(run, run, Duration::from_secs(5)).unwrap()
}

fn task(id: &str, steps: Vec<Step>) -> Task {
    Task::new(
        TaskId::new(id).unwrap(),
        ContainerSpec::new("img").unwrap(),
        steps,
    )
    .unwrap()
}

fn job(tasks: Vec<Task>) -> Job {
    Job::new(JobId::new("job").unwrap(), tasks).unwrap()
}

fn parallel(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

#[tokio::test]
async fn runs_steps_in_order_streams_lines_and_removes_container() {
    let runtime = FakeRuntime::with(&[("a", Script::ok(&["hel", "lo\nwor", "ld"]))]);
    let sink = RecordingSink::default();
    let use_case = RunJob::new(&runtime, &sink, OutputLimits::default(), parallel(1));

    let report = use_case
        .execute(&job(vec![task("t", vec![step("a"), step("b")])]))
        .await;

    assert!(report.succeeded());
    let t = &report.tasks[0];
    assert_eq!(t.container_id.as_deref(), Some("c-t"));
    assert_eq!(t.memory.and_then(|m| m.peak_bytes), Some(1 << 20));
    assert_eq!(t.steps[0].stdout_tail, "hello\nworld");
    assert_eq!(t.steps[0].stdout_bytes, 11);
    assert_eq!(
        runtime.calls(),
        [
            "start t",
            "exec c-t a",
            "exec c-t b",
            "memory c-t",
            "remove c-t"
        ]
    );
    let events = sink.0.lock().unwrap();
    assert_eq!(events[0], "out t#0 Stdout hello");
    assert_eq!(events[1], "out t#0 Stdout world");
    assert_eq!(events.last().unwrap(), "task t Succeeded");
}

#[tokio::test]
async fn failure_skips_remaining_steps_but_still_cleans_up() {
    let failing = Script {
        stderr: &["boom\n"],
        result: Ok(ExecStatus::Exited(2)),
        ..Script::ok(&[])
    };
    let runtime = FakeRuntime::with(&[("bad", failing)]);
    let sink = RecordingSink::default();
    let use_case = RunJob::new(&runtime, &sink, OutputLimits::default(), parallel(1));

    let report = use_case
        .execute(&job(vec![task("t", vec![step("bad"), step("never")])]))
        .await;

    let t = &report.tasks[0];
    assert_eq!(t.status, TaskStatus::Failed);
    assert_eq!(t.steps[0].outcome, StepOutcome::Failed { exit_code: 2 });
    assert_eq!(t.steps[0].stderr_tail, "boom\n");
    assert_eq!(t.steps[1].outcome, StepOutcome::Skipped);
    assert_eq!(
        runtime.calls(),
        ["start t", "exec c-t bad", "memory c-t", "remove c-t"]
    );
    assert!(!report.succeeded());
}

#[tokio::test]
async fn continue_on_error_keeps_going_and_does_not_fail_task() {
    let failing = Script {
        result: Ok(ExecStatus::Exited(1)),
        ..Script::ok(&[])
    };
    let runtime = FakeRuntime::with(&[("flaky", failing)]);
    let sink = RecordingSink::default();
    let mut tolerated = step("flaky");
    tolerated.continue_on_error = true;

    let report = RunJob::new(&runtime, &sink, OutputLimits::default(), parallel(1))
        .execute(&job(vec![task("t", vec![tolerated, step("next")])]))
        .await;

    assert_eq!(report.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(report.tasks[0].steps[1].outcome, StepOutcome::Succeeded);
}

#[tokio::test]
async fn timeout_halts_task_even_with_continue_on_error() {
    let timeout = Script {
        result: Ok(ExecStatus::TimedOut),
        ..Script::ok(&[])
    };
    let runtime = FakeRuntime::with(&[("slow", timeout)]);
    let sink = RecordingSink::default();
    let mut tolerated = step("slow");
    tolerated.continue_on_error = true;

    let report = RunJob::new(&runtime, &sink, OutputLimits::default(), parallel(1))
        .execute(&job(vec![task("t", vec![tolerated, step("next")])]))
        .await;

    assert_eq!(report.tasks[0].status, TaskStatus::Failed);
    assert_eq!(report.tasks[0].steps[1].outcome, StepOutcome::Skipped);
    assert_eq!(
        runtime.calls(),
        ["start t", "exec c-t slow", "memory c-t", "remove c-t"]
    );
}

#[tokio::test]
async fn timeouts_and_runtime_errors_are_reported_per_step() {
    let timeout = Script {
        result: Ok(ExecStatus::TimedOut),
        ..Script::ok(&[])
    };
    let broken = Script {
        result: Err("socket closed"),
        ..Script::ok(&[])
    };
    let runtime = FakeRuntime::with(&[("slow", timeout), ("broken", broken)]);
    let sink = RecordingSink::default();
    let use_case = RunJob::new(&runtime, &sink, OutputLimits::default(), parallel(2));

    let report = use_case
        .execute(&job(vec![
            task("a", vec![step("slow")]),
            task("b", vec![step("broken")]),
        ]))
        .await;

    assert_eq!(report.tasks[0].steps[0].outcome, StepOutcome::TimedOut);
    assert_eq!(
        report.tasks[1].steps[0].outcome,
        StepOutcome::Error {
            message: "socket closed".into()
        }
    );
}

#[tokio::test]
async fn start_failure_marks_task_as_error_without_exec() {
    let runtime = FakeRuntime {
        fail_start: true,
        ..FakeRuntime::default()
    };
    let sink = RecordingSink::default();

    let report = RunJob::new(&runtime, &sink, OutputLimits::default(), parallel(1))
        .execute(&job(vec![task("t", vec![step("a")])]))
        .await;

    let t = &report.tasks[0];
    assert_eq!(t.status, TaskStatus::Error);
    assert!(t.error.as_deref().unwrap().contains("no such image"));
    assert!(t.steps.is_empty());
    assert_eq!(runtime.calls(), ["start t"]);
}

#[tokio::test]
async fn never_exceeds_max_parallel_containers() {
    let runtime = FakeRuntime {
        delay: Duration::from_millis(20),
        ..FakeRuntime::default()
    };
    let sink = RecordingSink::default();
    let tasks = (0..6)
        .map(|i| task(&format!("t{i}"), vec![step("x")]))
        .collect();

    let report = RunJob::new(&runtime, &sink, OutputLimits::default(), parallel(2))
        .execute(&job(tasks))
        .await;

    assert!(report.succeeded());
    assert_eq!(runtime.peak.load(Ordering::SeqCst), 2);
    // Report order matches input order regardless of completion order.
    let ids: Vec<_> = report.tasks.iter().map(|t| t.id.to_string()).collect();
    assert_eq!(ids, ["t0", "t1", "t2", "t3", "t4", "t5"]);
}

#[tokio::test]
async fn finished_task_frees_its_slot_while_earlier_task_still_runs() {
    let slow = Script {
        delay: Duration::from_millis(100),
        ..Script::ok(&[])
    };
    let runtime = FakeRuntime::with(&[("slow", slow)]);
    let sink = RecordingSink::default();
    let tasks = vec![
        task("a", vec![step("slow")]),
        task("b", vec![step("fast")]),
        task("c", vec![step("fast")]),
    ];

    RunJob::new(&runtime, &sink, OutputLimits::default(), parallel(2))
        .execute(&job(tasks))
        .await;

    let calls = runtime.calls();
    let pos = |call: &str| calls.iter().position(|c| c == call).unwrap();
    assert!(
        pos("start c") < pos("remove c-a"),
        "head-of-line blocking: {calls:?}"
    );
}

#[tokio::test]
async fn report_output_is_bounded_by_limits() {
    let runtime = FakeRuntime::with(&[("loud", Script::ok(&["0123456789\n", "abcdefghij\n"]))]);
    let sink = RecordingSink::default();
    let limits = OutputLimits {
        tail_bytes: 4,
        max_line_bytes: 64,
    };

    let report = RunJob::new(&runtime, &sink, limits, parallel(1))
        .execute(&job(vec![task("t", vec![step("loud")])]))
        .await;

    let s = &report.tasks[0].steps[0];
    assert_eq!(s.stdout_tail, "hij\n");
    assert_eq!(s.stdout_bytes, 22);
}

#[tokio::test]
async fn lines_longer_than_max_line_bytes_are_split() {
    let runtime = FakeRuntime::with(&[("long", Script::ok(&["0123456789\n"]))]);
    let sink = RecordingSink::default();
    let limits = OutputLimits {
        tail_bytes: 64,
        max_line_bytes: 4,
    };

    RunJob::new(&runtime, &sink, limits, parallel(1))
        .execute(&job(vec![task("t", vec![step("long")])]))
        .await;

    let lines: Vec<String> = sink
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|l| l.starts_with("out "))
        .cloned()
        .collect();
    assert_eq!(
        lines,
        [
            "out t#0 Stdout 0123",
            "out t#0 Stdout 4567",
            "out t#0 Stdout 89"
        ]
    );
}
