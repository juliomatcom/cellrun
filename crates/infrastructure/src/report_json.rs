//! Final job report as JSON (what the orchestrator "returns").

use std::io::Write;

use serde::Serialize;

use domain::{JobReport, MemoryUsage, StepOutcome, StepReport, TaskReport, TaskStatus};

#[derive(Serialize)]
struct JobDto<'a> {
    id: &'a str,
    succeeded: bool,
    duration_ms: u128,
    tasks: Vec<TaskDto<'a>>,
}

#[derive(Serialize)]
struct TaskDto<'a> {
    id: &'a str,
    image: &'a str,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    container_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
    duration_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory: Option<MemoryDto>,
    steps: Vec<StepDto<'a>>,
}

/// Bytes, like `stdout_bytes`; a field is `null` when unknown.
#[derive(Serialize)]
pub(crate) struct MemoryDto {
    pub peak_bytes: Option<u64>,
    pub swap_peak_bytes: Option<u64>,
    pub oom_kills: Option<u64>,
}

impl From<&MemoryUsage> for MemoryDto {
    fn from(m: &MemoryUsage) -> Self {
        Self {
            peak_bytes: m.peak_bytes,
            swap_peak_bytes: m.swap_peak_bytes,
            oom_kills: m.oom_kills,
        }
    }
}

#[derive(Serialize)]
struct StepDto<'a> {
    name: &'a str,
    #[serde(flatten)]
    outcome: OutcomeDto<'a>,
    duration_ms: u128,
    stdout_tail: &'a str,
    stderr_tail: &'a str,
    stdout_bytes: u64,
    stderr_bytes: u64,
}

#[derive(Serialize)]
pub(crate) struct OutcomeDto<'a> {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<&'a str>,
}

impl<'a> From<&'a StepOutcome> for OutcomeDto<'a> {
    fn from(outcome: &'a StepOutcome) -> Self {
        let (status, exit_code, error) = match outcome {
            StepOutcome::Succeeded => ("succeeded", Some(0), None),
            StepOutcome::Failed { exit_code } => ("failed", Some(*exit_code), None),
            StepOutcome::TimedOut => ("timed_out", None, None),
            StepOutcome::Skipped => ("skipped", None, None),
            StepOutcome::Error { message } => ("error", None, Some(message.as_str())),
        };
        Self {
            status,
            exit_code,
            error,
        }
    }
}

pub(crate) fn task_status(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Succeeded => "succeeded",
        TaskStatus::Failed => "failed",
        TaskStatus::Error => "error",
    }
}

impl<'a> From<&'a StepReport> for StepDto<'a> {
    fn from(s: &'a StepReport) -> Self {
        Self {
            name: &s.name,
            outcome: (&s.outcome).into(),
            duration_ms: s.duration.as_millis(),
            stdout_tail: &s.stdout_tail,
            stderr_tail: &s.stderr_tail,
            stdout_bytes: s.stdout_bytes,
            stderr_bytes: s.stderr_bytes,
        }
    }
}

impl<'a> From<&'a TaskReport> for TaskDto<'a> {
    fn from(t: &'a TaskReport) -> Self {
        Self {
            id: t.id.as_str(),
            image: &t.image,
            status: task_status(t.status),
            container_id: t.container_id.as_deref(),
            error: t.error.as_deref(),
            duration_ms: t.duration.as_millis(),
            memory: t.memory.as_ref().map(Into::into),
            steps: t.steps.iter().map(Into::into).collect(),
        }
    }
}

/// Writes the report as pretty JSON followed by a newline.
pub fn write_report(report: &JobReport, mut out: impl Write) -> std::io::Result<()> {
    let dto = JobDto {
        id: report.id.as_str(),
        succeeded: report.succeeded(),
        duration_ms: report.duration.as_millis(),
        tasks: report.tasks.iter().map(Into::into).collect(),
    };
    serde_json::to_writer_pretty(&mut out, &dto)?;
    out.write_all(b"\n")?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use domain::{JobId, TaskId};

    use super::*;

    #[test]
    fn serializes_report() {
        let report = JobReport {
            id: JobId::new("j").unwrap(),
            duration: Duration::from_millis(1500),
            tasks: vec![TaskReport {
                id: TaskId::new("t").unwrap(),
                image: "debian".into(),
                container_id: Some("abc".into()),
                status: TaskStatus::Failed,
                error: None,
                duration: Duration::from_millis(1200),
                memory: Some(MemoryUsage {
                    peak_bytes: Some(150 << 20),
                    swap_peak_bytes: Some(0),
                    oom_kills: None,
                }),
                steps: vec![
                    StepReport {
                        outcome: StepOutcome::Failed { exit_code: 3 },
                        stdout_tail: "out".into(),
                        stdout_bytes: 3,
                        ..StepReport::skipped("build")
                    },
                    StepReport::skipped("test"),
                ],
            }],
        };
        let mut buf = Vec::new();
        write_report(&report, &mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();

        assert_eq!(v["succeeded"], false);
        assert_eq!(v["duration_ms"], 1500);
        let task = &v["tasks"][0];
        assert_eq!(task["status"], "failed");
        assert_eq!(task["container_id"], "abc");
        assert!(task.get("error").is_none());
        assert_eq!(task["memory"]["peak_bytes"], 150 << 20);
        assert_eq!(task["memory"]["swap_peak_bytes"], 0);
        assert!(task["memory"]["oom_kills"].is_null());
        assert_eq!(task["steps"][0]["status"], "failed");
        assert_eq!(task["steps"][0]["exit_code"], 3);
        assert_eq!(task["steps"][0]["stdout_tail"], "out");
        assert_eq!(task["steps"][1]["status"], "skipped");
        assert!(task["steps"][1].get("exit_code").is_none());
    }
}
