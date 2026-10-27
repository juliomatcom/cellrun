//! JSON job definition (the orchestrator's temporary input format).
//!
//! ```json
//! {
//!   "id": "demo",
//!   "defaults": { "image": "my-org/runner:latest",
//!                 "env_passthrough": ["API_KEY"], "memory_mb": 256 },
//!   "tasks": [
//!     { "id": "hello", "steps": ["echo hi", { "run": "make test", "timeout_secs": 900 }] }
//!   ]
//! }
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;

use crate::domain::{
    ContainerSpec, Job, JobId, ResourceLimits, Step, Task, TaskId, ValidationError,
};

const DEFAULT_TIMEOUT_SECS: u64 = 600;
/// Swap granted on top of `memory_mb` when the job does not say otherwise.
const DEFAULT_SWAP_MB: u64 = 1024;
/// Per-task swap ceiling: enough headroom for spikes without letting one
/// task fill the host's swap.
const MAX_SWAP_MB: u64 = 2048;

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
    #[error("task `{task}`: env var `{var}` listed in env_passthrough is not set or empty")]
    MissingEnv { task: String, var: String },
    #[error("task `{task}`: {message}")]
    Limit { task: String, message: &'static str },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobDto {
    #[serde(default = "default_job_id")]
    id: String,
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
    memory_mb: Option<u64>,
    /// Swap on top of `memory_mb`; omitted means `DEFAULT_SWAP_MB`, 0 means none.
    swap_mb: Option<u64>,
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
    Job::new(job_id, tasks).map_err(invalid("job".into()))
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

    for key in defaults.env.keys().chain(c.env.keys()) {
        if key.is_empty() || key.contains(['=', '\0']) {
            return Err(InputError::Limit {
                task: id.to_string(),
                message: "env names must be non-empty and contain no `=` or NUL",
            });
        }
    }
    let mut env = defaults.env.clone();
    for name in defaults.env_passthrough.iter().chain(&c.env_passthrough) {
        // Empty counts as missing: a blank API key should fail fast, not
        // later inside the container.
        let value =
            lookup_env(name)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| InputError::MissingEnv {
                    task: id.to_string(),
                    var: name.clone(),
                })?;
        env.insert(name.clone(), value);
    }
    env.extend(c.env);
    spec.env = env.into_iter().collect();
    spec.workdir = c.workdir.or_else(|| defaults.workdir.clone());

    let limit_err = |message| InputError::Limit {
        task: id.to_string(),
        message,
    };
    // Docker reads 0 as "unlimited": reject it rather than silently lifting
    // the limit.
    let memory_mb = c.memory_mb.or(defaults.memory_mb);
    let pids = c.pids.or(defaults.pids);
    if memory_mb == Some(0) || pids == Some(0) {
        return Err(limit_err(
            "`memory_mb` and `pids` must be greater than zero",
        ));
    }
    // Swap only extends a memory limit; without one Docker ignores it.
    let swap_mb = c.swap_mb.or(defaults.swap_mb);
    if swap_mb.is_some() && memory_mb.is_none() {
        return Err(limit_err("`swap_mb` requires `memory_mb`"));
    }
    if swap_mb.is_some_and(|mb| mb > MAX_SWAP_MB) {
        return Err(limit_err("`swap_mb` must be at most 2048"));
    }
    let cpus = c.cpus.or(defaults.cpus);
    if cpus.is_some_and(|v| !(v.is_finite() && v > 0.0)) {
        return Err(limit_err("`cpus` must be a positive number"));
    }
    spec.limits = ResourceLimits {
        memory_bytes: memory_mb
            .map(|mb| {
                mb.checked_mul(1024 * 1024)
                    .ok_or_else(|| limit_err("`memory_mb` too large"))
            })
            .transpose()?,
        swap_bytes: memory_mb
            .map(|_| swap_mb.unwrap_or(DEFAULT_SWAP_MB))
            .filter(|&mb| mb > 0)
            .map(|mb| mb * 1024 * 1024),
        nano_cpus: cpus.map(|v| (v * 1e9) as u64),
        pids,
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
        match name {
            "API_KEY" => Some("secret".to_owned()),
            "BLANK" => Some(String::new()),
            _ => None,
        }
    }

    #[test]
    fn parses_minimal_job_with_defaults() {
        let job = parse_job(
            br#"{"tasks":[{"id":"t","image":"debian","steps":["echo hi"]}]}"#,
            env,
        )
        .unwrap();
        assert_eq!(job.id.as_str(), "job");
        let step = &job.tasks[0].steps[0];
        assert_eq!(step.name, "step-0");
        assert_eq!(step.run, "echo hi");
        assert_eq!(step.timeout, Duration::from_secs(DEFAULT_TIMEOUT_SECS));
    }

    #[test]
    fn swap_defaults_to_1gib_with_a_memory_limit() {
        let swap = |json: &str| {
            parse_job(json.as_bytes(), env).unwrap().tasks[0]
                .container
                .limits
                .swap_bytes
        };
        assert_eq!(
            swap(r#"{"tasks":[{"id":"t","image":"x","memory_mb":64,"steps":["ls"]}]}"#),
            Some(DEFAULT_SWAP_MB * 1024 * 1024)
        );
        assert_eq!(
            swap(r#"{"tasks":[{"id":"t","image":"x","memory_mb":64,"swap_mb":0,"steps":["ls"]}]}"#),
            None
        );
        assert_eq!(
            swap(r#"{"tasks":[{"id":"t","image":"x","steps":["ls"]}]}"#),
            None
        );
    }

    #[test]
    fn task_overrides_defaults_and_merges_env() {
        let json = br#"{
            "id": "demo",
            "defaults": {"image": "base", "env": {"A": "1", "B": "1"},
                         "env_passthrough": ["API_KEY"], "memory_mb": 256, "swap_mb": 1024,
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
        assert_eq!(task.container.limits.swap_bytes, Some(1024 * 1024 * 1024));
        assert_eq!(task.container.limits.nano_cpus, Some(500_000_000));
        assert_eq!(task.container.limits.pids, Some(64));
        assert_eq!(task.steps[0].timeout, Duration::from_secs(5));
        assert!(task.steps[0].continue_on_error);
        assert_eq!(task.steps[1].timeout, Duration::from_secs(30));
    }

    #[test]
    fn bundled_examples_are_valid() {
        let any_env = |_: &str| Some("x".to_owned());
        for example in [&include_bytes!("../../examples/hello.json")[..]] {
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
        let json =
            br#"{"tasks":[{"id":"t","image":"x","env_passthrough":["BLANK"],"steps":["ls"]}]}"#;
        assert!(
            matches!(parse_job(json, env), Err(InputError::MissingEnv { var, .. }) if var == "BLANK")
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
            // Parallelism is host capacity, set on the orchestrator.
            r#"{"max_parallel":2,"tasks":[{"id":"t","image":"x","steps":["ls"]}]}"#,
            // Isolation is not configurable from a job.
            r#"{"tasks":[{"id":"t","image":"x","network":"host","steps":["ls"]}]}"#,
            // 0 would mean "unlimited" to Docker.
            r#"{"tasks":[{"id":"t","image":"x","memory_mb":0,"steps":["ls"]}]}"#,
            r#"{"defaults":{"pids":0},"tasks":[{"id":"t","image":"x","steps":["ls"]}]}"#,
            r#"{"tasks":[{"id":"t","image":"x","swap_mb":512,"steps":["ls"]}]}"#,
            r#"{"tasks":[{"id":"t","image":"x","memory_mb":64,"swap_mb":4096,"steps":["ls"]}]}"#,
            r#"{"tasks":[{"id":"t","image":"x","env":{"A=B":"1"},"steps":["ls"]}]}"#,
            r#"{"tasks":[{"id":"t","image":"x","env":{"":"1"},"steps":["ls"]}]}"#,
        ] {
            assert!(
                parse_job(json.as_bytes(), env).is_err(),
                "should reject {json}"
            );
        }
    }
}
