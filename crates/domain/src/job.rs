use std::collections::HashSet;
use std::time::Duration;

use crate::error::ValidationError;

/// Ids end up in container names and labels, so they are restricted to a
/// charset Docker accepts.
fn validate_id(raw: &str) -> Result<(), ValidationError> {
    let valid = !raw.is_empty()
        && raw.len() <= 64
        && raw.as_bytes()[0].is_ascii_alphanumeric()
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(ValidationError::InvalidId(raw.to_owned()))
    }
}

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(raw: impl Into<String>) -> Result<Self, ValidationError> {
                let raw = raw.into();
                validate_id(&raw)?;
                Ok(Self(raw))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_type!(JobId);
id_type!(TaskId);

/// Hard caps applied to a container. `None` means "runtime default".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResourceLimits {
    pub memory_bytes: Option<u64>,
    /// CPU quota in billionths of a CPU (Docker's `NanoCpus`).
    pub nano_cpus: Option<u64>,
    pub pids: Option<u64>,
}

/// What container a task runs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerSpec {
    pub image: String,
    /// Fully resolved `KEY=VALUE` pairs. Secrets live here only in memory and
    /// are never written into reports or events.
    pub env: Vec<(String, String)>,
    pub workdir: Option<String>,
    pub network: Option<String>,
    pub limits: ResourceLimits,
}

impl ContainerSpec {
    pub fn new(image: impl Into<String>) -> Result<Self, ValidationError> {
        let image = image.into();
        if image.trim().is_empty() {
            return Err(ValidationError::EmptyField("image"));
        }
        Ok(Self {
            image,
            env: Vec::new(),
            workdir: None,
            network: None,
            limits: ResourceLimits::default(),
        })
    }
}

/// A single shell instruction executed inside the task's container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub name: String,
    pub run: String,
    pub timeout: Duration,
    /// When `true`, a failure of this step does not stop the task.
    pub continue_on_error: bool,
}

impl Step {
    pub fn new(
        name: impl Into<String>,
        run: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, ValidationError> {
        let run = run.into();
        if run.trim().is_empty() {
            return Err(ValidationError::EmptyField("run"));
        }
        if timeout.is_zero() {
            return Err(ValidationError::ZeroValue("timeout"));
        }
        Ok(Self {
            name: name.into(),
            run,
            timeout,
            continue_on_error: false,
        })
    }
}

/// A sequence of steps sharing one ephemeral container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: TaskId,
    pub container: ContainerSpec,
    pub steps: Vec<Step>,
}

impl Task {
    pub fn new(
        id: TaskId,
        container: ContainerSpec,
        steps: Vec<Step>,
    ) -> Result<Self, ValidationError> {
        if steps.is_empty() {
            return Err(ValidationError::NoSteps(id.to_string()));
        }
        Ok(Self {
            id,
            container,
            steps,
        })
    }
}

/// A batch of independent tasks, run with bounded parallelism.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    pub max_parallel: usize,
    pub tasks: Vec<Task>,
}

impl Job {
    pub fn new(id: JobId, max_parallel: usize, tasks: Vec<Task>) -> Result<Self, ValidationError> {
        if tasks.is_empty() {
            return Err(ValidationError::NoTasks);
        }
        if max_parallel == 0 {
            return Err(ValidationError::ZeroValue("max_parallel"));
        }
        let mut seen = HashSet::with_capacity(tasks.len());
        for task in &tasks {
            if !seen.insert(task.id.as_str()) {
                return Err(ValidationError::DuplicateTaskId(task.id.to_string()));
            }
        }
        Ok(Self {
            id,
            max_parallel,
            tasks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step() -> Step {
        Step::new("s", "echo hi", Duration::from_secs(1)).unwrap()
    }

    fn task(id: &str) -> Task {
        Task::new(
            TaskId::new(id).unwrap(),
            ContainerSpec::new("debian").unwrap(),
            vec![step()],
        )
        .unwrap()
    }

    #[test]
    fn ids_accept_docker_safe_names() {
        assert!(TaskId::new("build-1.a_b").is_ok());
    }

    #[test]
    fn ids_reject_unsafe_names() {
        for bad in ["", "-lead", "has space", "x/y", &"a".repeat(65)] {
            assert!(TaskId::new(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn step_rejects_blank_command_and_zero_timeout() {
        assert_eq!(
            Step::new("s", "  ", Duration::from_secs(1)),
            Err(ValidationError::EmptyField("run"))
        );
        assert_eq!(
            Step::new("s", "ls", Duration::ZERO),
            Err(ValidationError::ZeroValue("timeout"))
        );
    }

    #[test]
    fn container_requires_image() {
        assert_eq!(
            ContainerSpec::new(""),
            Err(ValidationError::EmptyField("image"))
        );
    }

    #[test]
    fn task_requires_steps() {
        let err = Task::new(
            TaskId::new("t").unwrap(),
            ContainerSpec::new("debian").unwrap(),
            vec![],
        );
        assert_eq!(err, Err(ValidationError::NoSteps("t".into())));
    }

    #[test]
    fn job_rejects_duplicates_empty_and_zero_parallelism() {
        let id = || JobId::new("j").unwrap();
        assert_eq!(Job::new(id(), 1, vec![]), Err(ValidationError::NoTasks));
        assert_eq!(
            Job::new(id(), 0, vec![task("a")]),
            Err(ValidationError::ZeroValue("max_parallel"))
        );
        assert_eq!(
            Job::new(id(), 1, vec![task("a"), task("a")]),
            Err(ValidationError::DuplicateTaskId("a".into()))
        );
        assert!(Job::new(id(), 2, vec![task("a"), task("b")]).is_ok());
    }
}
