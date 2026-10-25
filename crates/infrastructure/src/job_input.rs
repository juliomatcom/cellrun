//! JSON job definition (the orchestrator's temporary input format).
//!
//! ```json
//! {
//!   "id": "demo",
//!   "max_parallel": 2,
//!   "defaults": { "image": "hostprobe/opencode-runner:latest",
//!                 "env_passthrough": ["ANTHROPIC_API_KEY"], "memory_mb": 512 },
//!   "tasks": [
//!     { "id": "hello", "steps": ["echo hi", { "run": "opencode run 'fix bug'", "timeout_secs": 900 }] }
//!   ]
//! }
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;

use domain::{ContainerSpec, Job, JobId, ResourceLimits, Step, Task, TaskId, ValidationError};

const DEFAULT_MAX_PARALLEL: usize = 2;
const DEFAULT_TIMEOUT_SECS: u64 = 600;

#[derive(Debug, thiserror::Error)]
pub enum InputError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{context}: {source}")]
    Invalid {
        context: String,
        source: ValidationError,
    },
    #[error("task `{task}`: no image (set `image` on the task or in `defaults`)")]
    MissingImage { task: String },
    #[error("task `{task}`: env var `{var}` listed in env_passthrough is not set")]
    MissingEnv { task: String, var: String },
    #[error("task `{task}`: {message}")]
    Limit { task: String, message: &'static str },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobDto {
    #[serde(default = "default_job_id")]
    id: String,
    #[serde(default = "default_max_parallel")]
    max_parallel: usize,
    #[serde(default)]
    defaults: ContainerDto,
    tasks: Vec<TaskDto>,
}

/// Container settings; used both for `defaults` and per task (task wins).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContainerDto {
    image: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    /// Names of variables copied from the orchestrator's own environment.
    /// Keeps secrets (API keys) out of the job file.
    #[serde(default)]
    env_passthrough: Vec<String>,
    workdir: Option<String>,
    network: Option<String>,
    memory_mb: Option<u64>,
    cpus: Option<f64>,
    pids: Option<u64>,
    timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskDto {
    id: String,
    #[serde(flatten)]
    container: ContainerDto,
    steps: Vec<StepDto>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StepDto {
    Command(String),
    Full {
        name: Option<String>,
        run: String,
        timeout_secs: Option<u64>,
        #[serde(default)]
        continue_on_error: bool,
    },
}

fn default_job_id() -> String {
    "job".to_owned()
}

fn default_max_parallel() -> usize {
    DEFAULT_MAX_PARALLEL
}

/// Parses and validates a job. `lookup_env` resolves `env_passthrough`
/// names (injected so tests don't touch the process environment).
pub fn parse_job(
    json: &[u8],
    lookup_env: impl Fn(&str) -> Option<String>,
) -> Result<Job, InputError> {
    let dto: JobDto = serde_json::from_slice(json)?;
    let invalid = |context: String| move |source| InputError::Invalid { context, source };

    let job_id = JobId::new(dto.id).map_err(invalid("job id".into()))?;
    let tasks = dto
        .tasks
        .into_iter()
        .map(|t| to_task(t, &dto.defaults, &lookup_env))
        .collect::<Result<Vec<_>, _>>()?;
    Job::new(job_id, dto.max_parallel, tasks).map_err(invalid("job".into()))
}

fn to_task(
    dto: TaskDto,
    defaults: &ContainerDto,
    lookup_env: &impl Fn(&str) -> Option<String>,
) -> Result<Task, InputError> {
    let id = TaskId::new(dto.id.clone()).map_err(|source| InputError::Invalid {
        context: "task id".into(),
        source,
    })?;
    let invalid = |source| InputError::Invalid {
        context: format!("task `{id}`"),
        source,
    };
    let c = dto.container;

    let image =
        c.image
            .or_else(|| defaults.image.clone())
            .ok_or_else(|| InputError::MissingImage {
                task: id.to_string(),
            })?;
    let mut spec = ContainerSpec::new(image).map_err(invalid)?;

    let mut env = defaults.env.clone();
    for name in defaults.env_passthrough.iter().chain(&c.env_passthrough) {
        let value = lookup_env(name).ok_or_else(|| InputError::MissingEnv {
            task: id.to_string(),
            var: name.clone(),
        })?;
        env.insert(name.clone(), value);
    }
    env.extend(c.env);
    spec.env = env.into_iter().collect();
    spec.workdir = c.workdir.or_else(|| defaults.workdir.clone());
    spec.network = c.network.or_else(|| defaults.network.clone());

    let limit_err = |message| InputError::Limit {
        task: id.to_string(),
        message,
    };
    let cpus = c.cpus.or(defaults.cpus);
    if cpus.is_some_and(|v| !(v.is_finite() && v > 0.0)) {
        return Err(limit_err("`cpus` must be a positive number"));
    }
    spec.limits = ResourceLimits {
        memory_bytes: c
            .memory_mb
            .or(defaults.memory_mb)
            .map(|mb| {
                mb.checked_mul(1024 * 1024)
                    .ok_or_else(|| limit_err("`memory_mb` too large"))
            })
            .transpose()?,
        nano_cpus: cpus.map(|v| (v * 1e9) as u64),
        pids: c.pids.or(defaults.pids),
    };

    let task_timeout = c.timeout_secs.or(defaults.timeout_secs);
    let steps = dto
        .steps
        .into_iter()
        .enumerate()
        .map(|(i, step)| {
            let (name, run, timeout, continue_on_error) = match step {
                StepDto::Command(run) => (None, run, None, false),
                StepDto::Full {
                    name,
                    run,
                    timeout_secs,
                    continue_on_error,
                } => (name, run, timeout_secs, continue_on_error),
            };
            let secs = timeout.or(task_timeout).unwrap_or(DEFAULT_TIMEOUT_SECS);
            let name = name.unwrap_or_else(|| format!("step-{i}"));
            let mut step = Step::new(name, run, Duration::from_secs(secs)).map_err(invalid)?;
            step.continue_on_error = continue_on_error;
            Ok(step)
        })
        .collect::<Result<Vec<_>, InputError>>()?;

    Task::new(id.clone(), spec, steps).map_err(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(name: &str) -> Option<String> {
        (name == "API_KEY").then(|| "secret".to_owned())
    }

    #[test]
    fn parses_minimal_job_with_defaults() {
        let job = parse_job(
            br#"{"tasks":[{"id":"t","image":"debian","steps":["echo hi"]}]}"#,
            env,
        )
        .unwrap();
        assert_eq!(job.id.as_str(), "job");
        assert_eq!(job.max_parallel, DEFAULT_MAX_PARALLEL);
        let step = &job.tasks[0].steps[0];
        assert_eq!(step.name, "step-0");
        assert_eq!(step.run, "echo hi");
        assert_eq!(step.timeout, Duration::from_secs(DEFAULT_TIMEOUT_SECS));
    }

    #[test]
    fn task_overrides_defaults_and_merges_env() {
        let json = br#"{
            "id": "demo", "max_parallel": 3,
            "defaults": {"image": "base", "env": {"A": "1", "B": "1"},
                         "env_passthrough": ["API_KEY"], "memory_mb": 256,
                         "cpus": 0.5, "timeout_secs": 30, "workdir": "/w"},
            "tasks": [{
                "id": "t", "image": "custom", "env": {"B": "2"}, "pids": 64,
                "steps": [{"name": "build", "run": "make", "timeout_secs": 5,
                           "continue_on_error": true}, "ls"]
            }]
        }"#;
        let job = parse_job(json, env).unwrap();
        let task = &job.tasks[0];
        assert_eq!(task.container.image, "custom");
        assert_eq!(
            task.container.env,
            [("A", "1"), ("API_KEY", "secret"), ("B", "2")]
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
        );
        assert_eq!(task.container.workdir.as_deref(), Some("/w"));
        assert_eq!(task.container.limits.memory_bytes, Some(256 * 1024 * 1024));
        assert_eq!(task.container.limits.nano_cpus, Some(500_000_000));
        assert_eq!(task.container.limits.pids, Some(64));
        assert_eq!(task.steps[0].timeout, Duration::from_secs(5));
        assert!(task.steps[0].continue_on_error);
        assert_eq!(task.steps[1].timeout, Duration::from_secs(30));
    }

    #[test]
    fn bundled_examples_are_valid() {
        let any_env = |_: &str| Some("x".to_owned());
        for example in [
            &include_bytes!("../../../examples/hello.json")[..],
            &include_bytes!("../../../examples/opencode.json")[..],
        ] {
            parse_job(example, any_env).unwrap();
        }
    }

    #[test]
    fn rejects_missing_passthrough_env() {
        let json =
            br#"{"tasks":[{"id":"t","image":"x","env_passthrough":["NOPE"],"steps":["ls"]}]}"#;
        assert!(
            matches!(parse_job(json, env), Err(InputError::MissingEnv { var, .. }) if var == "NOPE")
        );
    }

    #[test]
    fn rejects_missing_image() {
        let json = br#"{"tasks":[{"id":"t","steps":["ls"]}]}"#;
        assert!(matches!(
            parse_job(json, env),
            Err(InputError::MissingImage { .. })
        ));
    }

    #[test]
    fn rejects_unknown_fields_and_bad_values() {
        for json in [
            r#"{"tasks":[{"id":"t","image":"x","steps":["ls"],"typo":1}]}"#,
            r#"{"tasks":[{"id":"bad id","image":"x","steps":["ls"]}]}"#,
            r#"{"tasks":[{"id":"t","image":"x","steps":[]}]}"#,
            r#"{"tasks":[{"id":"t","image":"x","cpus":-1,"steps":["ls"]}]}"#,
            r#"{"max_parallel":0,"tasks":[{"id":"t","image":"x","steps":["ls"]}]}"#,
        ] {
            assert!(
                parse_job(json.as_bytes(), env).is_err(),
                "should reject {json}"
            );
        }
    }
}
