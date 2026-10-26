//! Streams events as newline-delimited JSON, one object per line, flushed per
//! line so a log collector (or `tail -f`) sees output as it happens.

use std::io::{LineWriter, Write};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use application::EventSink;
use domain::{Event, StreamKind};

use crate::report_json::{MemoryDto, OutcomeDto, task_status};

pub struct NdjsonSink<W: Write> {
    out: Mutex<LineWriter<W>>,
}

impl<W: Write> NdjsonSink<W> {
    pub fn new(out: W) -> Self {
        Self {
            out: Mutex::new(LineWriter::new(out)),
        }
    }

    #[cfg(test)]
    fn into_inner(self) -> W {
        let writer = self.out.into_inner().unwrap_or_else(|e| e.into_inner());
        writer.into_inner().ok().expect("flush")
    }
}

#[derive(Serialize)]
struct Envelope<'a> {
    ts_ms: u128,
    #[serde(flatten)]
    event: EventDto<'a>,
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum EventDto<'a> {
    JobStarted {
        job: &'a str,
        tasks: usize,
    },
    TaskStarted {
        task: &'a str,
        image: &'a str,
    },
    ContainerReady {
        task: &'a str,
        container_id: &'a str,
    },
    StepStarted {
        task: &'a str,
        step: usize,
        name: &'a str,
    },
    Output {
        task: &'a str,
        step: usize,
        stream: &'static str,
        line: &'a str,
    },
    StepFinished {
        task: &'a str,
        step: usize,
        #[serde(flatten)]
        outcome: OutcomeDto<'a>,
        duration_ms: u128,
    },
    TaskFinished {
        task: &'a str,
        status: &'static str,
        duration_ms: u128,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        memory: Option<MemoryDto>,
    },
    JobFinished {
        job: &'a str,
        succeeded: bool,
        duration_ms: u128,
    },
    Warning {
        #[serde(skip_serializing_if = "Option::is_none")]
        task: Option<&'a str>,
        message: &'a str,
    },
}

impl<'a> From<&'a Event<'a>> for EventDto<'a> {
    fn from(event: &'a Event<'a>) -> Self {
        match *event {
            Event::JobStarted { job, tasks } => Self::JobStarted {
                job: job.as_str(),
                tasks,
            },
            Event::TaskStarted { task, image } => Self::TaskStarted {
                task: task.as_str(),
                image,
            },
            Event::ContainerReady { task, container_id } => Self::ContainerReady {
                task: task.as_str(),
                container_id,
            },
            Event::StepStarted { task, index, name } => Self::StepStarted {
                task: task.as_str(),
                step: index,
                name,
            },
            Event::StepOutput {
                task,
                index,
                stream,
                line,
            } => Self::Output {
                task: task.as_str(),
                step: index,
                stream: match stream {
                    StreamKind::Stdout => "stdout",
                    StreamKind::Stderr => "stderr",
                },
                line,
            },
            Event::StepFinished {
                task,
                index,
                outcome,
                duration,
            } => Self::StepFinished {
                task: task.as_str(),
                step: index,
                outcome: outcome.into(),
                duration_ms: duration.as_millis(),
            },
            Event::TaskFinished {
                task,
                status,
                duration,
                error,
                memory,
            } => Self::TaskFinished {
                task: task.as_str(),
                status: task_status(status),
                duration_ms: duration.as_millis(),
                error,
                memory: memory.map(Into::into),
            },
            Event::JobFinished {
                job,
                succeeded,
                duration,
            } => Self::JobFinished {
                job: job.as_str(),
                succeeded,
                duration_ms: duration.as_millis(),
            },
            Event::Warning { task, message } => Self::Warning {
                task: task.map(|t| t.as_str()),
                message,
            },
        }
    }
}

impl<W: Write + Send> EventSink for NdjsonSink<W> {
    fn emit(&self, event: &Event<'_>) {
        let envelope = Envelope {
            ts_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or_default(),
            event: event.into(),
        };
        let mut out = self.out.lock().unwrap_or_else(|e| e.into_inner());
        // A broken log pipe must never abort running containers; drop the line.
        if serde_json::to_writer(&mut *out, &envelope).is_ok() {
            let _ = out.write_all(b"\n");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use domain::{StepOutcome, TaskId};

    use super::*;

    #[test]
    fn writes_one_json_object_per_line() {
        let sink = NdjsonSink::new(Vec::new());
        let task = TaskId::new("t").unwrap();
        sink.emit(&Event::StepOutput {
            task: &task,
            index: 1,
            stream: StreamKind::Stderr,
            line: "multi \"quoted\"\nline",
        });
        sink.emit(&Event::StepFinished {
            task: &task,
            index: 1,
            outcome: &StepOutcome::Failed { exit_code: 7 },
            duration: Duration::from_millis(42),
        });

        let text = String::from_utf8(sink.into_inner()).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["event"], "output");
        assert_eq!(lines[0]["stream"], "stderr");
        assert_eq!(lines[0]["line"], "multi \"quoted\"\nline");
        assert!(lines[0]["ts_ms"].as_u64().unwrap() > 0);
        assert_eq!(lines[1]["event"], "step_finished");
        assert_eq!(lines[1]["status"], "failed");
        assert_eq!(lines[1]["exit_code"], 7);
        assert_eq!(lines[1]["duration_ms"], 42);
    }
}
