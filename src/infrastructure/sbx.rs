//! Docker Sandboxes (`sbx`) adapter: runs a task in a microVM with its own
//! kernel instead of a container on the host's kernel.
//!
//! One sandbox per task, created from the task's `image` (a template known to
//! `sbx`) with the `shell` agent and no workspace mount. Steps run with
//! `sbx exec`, which mirrors `docker exec` (stdin, exit status). Network goes
//! through sbx's host-side proxy (policy: the host's, plus `deny_network`
//! here); secrets are stored per sandbox with `sbx secret set-custom`, so the
//! workload only ever holds a placeholder and the proxy swaps in the real
//! value on requests to the bound hosts (headers only, Basic auth included).
//!
//! Names: `<prefix>.<run>.<job>.<task>.<id>`. `.` separates the fields (ids
//! have it replaced by `-`), which lets cleanup find a job's or a run's
//! sandboxes from `sbx ls` alone: sbx has no labels.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::application::control::InputReceiver;
use crate::application::{ContainerRuntime, ExecStatus, OutputObserver, RuntimeError};
use crate::domain::{ContainerSpec, JobId, MemoryUsage, StreamKind, TaskId};

use super::docker::{PullPolicy, lock, unique_hex};

/// What a VM must never reach, always applied on top of the host's `sbx` policy (deny wins over allow): private,
/// link-local (cloud metadata), loopback, "this host" (`0.0.0.0` connects to the host's own loopback), CGNAT,
/// benchmarking, multicast and IPv6 counterparts (NAT64 too), plus the names that reach the host: sbx checks
/// addresses of direct connections but only hostnames of connections through an upstream proxy. No `::ffff:0:0/96`:
/// sbx sees every IPv4 address in that form, so the rule would block the whole IPv4 internet (the live test checks
/// that mapped private addresses are still denied).
pub const DEFAULT_DENY_NETWORK: &[&str] = &[
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "169.254.0.0/16",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "0.0.0.0/8",
    "192.0.0.0/24",
    "198.18.0.0/15",
    "224.0.0.0/4",
    "fc00::/7",
    "fe80::/10",
    "ff00::/8",
    "::/128",
    "::1/128",
    "64:ff9b::/96",
    "localhost",
    "host.docker.internal",
    "gateway.docker.internal",
];

/// `sbx` rejects anything under this.
const MIN_MEMORY_MB: u64 = 512;
/// Longest stderr kept in an error message.
const ERROR_TEXT_MAX: usize = 400;
/// Longest a management command other than `create` may take (ls, rm, secret): the api kills cleanup after 30 s.
const SHORT_TIMEOUT: Duration = Duration::from_secs(20);
/// Cap on output of the short management commands (create, rm, ls).
const COMMAND_OUTPUT_MAX: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct SbxConfig {
    /// The `sbx` executable.
    pub bin: String,
    pub pull: PullPolicy,
    /// Prefix of sandbox names (`[a-z0-9-]`, no `.`).
    pub name_prefix: String,
    /// Extra per-sandbox egress deny rules (e.g. the host's own public IP), added to [`DEFAULT_DENY_NETWORK`],
    /// which always applies.
    pub deny_network: Vec<String>,
    /// Longest `sbx create` (it may pull and boot a VM).
    pub create_timeout: Duration,
}

impl Default for SbxConfig {
    fn default() -> Self {
        Self {
            bin: "sbx".to_owned(),
            pull: PullPolicy::Missing,
            name_prefix: "cellrun".to_owned(),
            deny_network: Vec::new(),
            create_timeout: Duration::from_secs(600),
        }
    }
}

/// What the adapter remembers about a running sandbox (memory only).
struct Sandbox {
    /// Plain env of the task; values go to `sbx exec` through the child's
    /// environment, never on the command line.
    env: Vec<(String, String)>,
    /// `NAME=placeholder` for each injected secret (not secret).
    placeholders: Vec<(String, String)>,
    workdir: Option<String>,
}

pub struct SbxRuntime {
    config: SbxConfig,
    run_id: String,
    sandboxes: Mutex<HashMap<String, Sandbox>>,
}

/// Variables of the clean base that a task's env must not replace.
fn is_base_var(name: &str) -> bool {
    matches!(name, "HOME" | "PATH" | "USER" | "LANG" | "TMPDIR")
        || name.starts_with("XDG_")
        || name.starts_with("SBX_")
        || name.starts_with("DOCKER_SANDBOXES_")
}

/// A bare `sbx` command with a clean environment: the dispatcher's own env holds every real key (the api passes
/// them for the secrets), and `sbx` would hand any variable it is asked for by name to a sandbox. Only what the
/// CLI needs to find its daemon and login passes: HOME and the XDG dirs, PATH, and sbx's own settings.
fn sbx_command(bin: &str) -> Command {
    let mut command = Command::new(bin);
    command.env_clear();
    for (key, value) in std::env::vars_os() {
        let name = key.to_string_lossy();
        let wanted = matches!(
            name.as_ref(),
            "HOME" | "PATH" | "USER" | "LANG" | "TMPDIR" | "XDG_RUNTIME_DIR"
        ) || name.starts_with("XDG_")
            || name.starts_with("SBX_")
            || name.starts_with("DOCKER_SANDBOXES_");
        if wanted {
            command.env(key, value);
        }
    }
    command
}

impl SbxRuntime {
    pub fn new(config: SbxConfig) -> Result<Self, RuntimeError> {
        let prefix = &config.name_prefix;
        let ok = prefix
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
            && prefix
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !ok || prefix.len() > 16 {
            return Err(RuntimeError::new(format!(
                "sandbox name prefix `{prefix}` must be 1-16 chars of [a-z0-9-], starting with a letter or digit"
            )));
        }
        Ok(Self {
            config,
            run_id: unique_hex()[..8].to_owned(),
            sandboxes: Mutex::new(HashMap::new()),
        })
    }

    /// Fails unless `sbx` is installed, signed in and its daemon answers.
    pub async fn ping(&self) -> Result<(), RuntimeError> {
        self.sandbox_names().await.map(|_| ())
    }

    /// Removes every sandbox carrying this prefix, whatever job or run made it.
    pub async fn remove_all(&self) -> Result<usize, RuntimeError> {
        self.remove_matching(|_| true).await
    }

    async fn sandbox_names(&self) -> Result<Vec<String>, RuntimeError> {
        let out = self.run(&["ls", "--quiet"], None, SHORT_TIMEOUT).await?;
        Ok(out.lines().map(|l| l.trim().to_owned()).collect())
    }

    async fn remove_matching(
        &self,
        keep: impl Fn(&NameParts<'_>) -> bool,
    ) -> Result<usize, RuntimeError> {
        let names: Vec<String> = self
            .sandbox_names()
            .await?
            .into_iter()
            .filter(|n| {
                NameParts::parse(n).is_some_and(|p| p.prefix == self.config.name_prefix && keep(&p))
            })
            .collect();
        if names.is_empty() {
            return Ok(0);
        }
        let mut args = vec!["rm", "--force"];
        args.extend(names.iter().map(String::as_str));
        self.run(&args, None, SHORT_TIMEOUT).await?;
        let mut sandboxes = lock(&self.sandboxes);
        for name in &names {
            sandboxes.remove(name);
        }
        Ok(names.len())
    }

    /// Runs a short `sbx` command to completion and returns its stdout.
    /// `stdin` is written then closed; error text is stderr, trimmed.
    async fn run(
        &self,
        args: &[&str],
        stdin: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<String, RuntimeError> {
        let mut command = sbx_command(&self.config.bin);
        command
            .args(args)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|e| RuntimeError::new(format!("cannot run `{}`: {e}", self.config.bin)))?;
        if let (Some(bytes), Some(mut pipe)) = (stdin, child.stdin.take()) {
            // A command that rejects its input first closes the pipe; its exit
            // status below carries the real error.
            let _ = pipe.write_all(bytes).await;
            let _ = pipe.shutdown().await;
        }
        let output = tokio::time::timeout(timeout, child.wait_with_output())
            .await
            .map_err(|_| RuntimeError::new(format!("`sbx {}` timed out", args[0])))?
            .map_err(|e| RuntimeError::new(e.to_string()))?;
        if output.status.success() {
            let mut out = output.stdout;
            out.truncate(COMMAND_OUTPUT_MAX);
            return Ok(String::from_utf8_lossy(&out).into_owned());
        }
        Err(RuntimeError::new(format!(
            "`sbx {}` failed: {}",
            args[0],
            error_text(&output.stderr)
        )))
    }

    /// Best-effort removal of one sandbox and its secrets (idempotent).
    async fn destroy(
        &self,
        name: &str,
        placeholders: &[(String, String)],
    ) -> Result<(), RuntimeError> {
        let result = match self
            .run(&["rm", "--force", name], None, SHORT_TIMEOUT)
            .await
        {
            Err(e) if !e.0.contains("not found") => Err(e),
            _ => Ok(()),
        };
        for (_, placeholder) in placeholders {
            let _ = self
                .run(
                    &["secret", "rm", "--placeholder", placeholder, "--force"],
                    None,
                    SHORT_TIMEOUT,
                )
                .await;
        }
        result
    }
}

/// Fields of a sandbox name made by [`sandbox_name`].
struct NameParts<'a> {
    prefix: &'a str,
    run: &'a str,
    job: &'a str,
}

impl<'a> NameParts<'a> {
    fn parse(name: &'a str) -> Option<Self> {
        let mut parts = name.split('.');
        let (prefix, run, job, _task, _id) = (
            parts.next()?,
            parts.next()?,
            parts.next()?,
            parts.next()?,
            parts.next()?,
        );
        parts.next().is_none().then_some(Self { prefix, run, job })
    }
}

/// sbx refuses names over 63 characters.
const NAME_MAX: usize = 63;
/// Length of the run id and the unique id in a name (`unique_hex` is 13 chars, the run id takes 8).
const RUN_ID_LEN: usize = 8;
const UNIQUE_ID_LEN: usize = 13;

/// How many characters the job and task parts of a name may take: what is left of [`NAME_MAX`] after the prefix,
/// the run id, the unique id and the four dots, split two to one. Both parts are cut to fit, and the job part
/// depends only on the prefix, so `remove_job` can compute it again.
fn part_budgets(prefix: &str) -> (usize, usize) {
    let left = NAME_MAX.saturating_sub(prefix.len() + RUN_ID_LEN + UNIQUE_ID_LEN + 4);
    let job = left * 2 / 3;
    (job, left - job)
}

/// One dot-separated part of a name: `_` and `.` become `-`, cut to `max`, and sbx wants every part to start and end
/// with a letter or digit (ids start with one; a cut or the id itself may leave a trailing `-`).
fn sanitize(id: &str, max: usize) -> String {
    let part: String = id.replace(['_', '.'], "-").chars().take(max).collect();
    part.trim_end_matches(|c: char| !c.is_ascii_alphanumeric())
        .to_owned()
}

fn sandbox_name(prefix: &str, run: &str, job: &JobId, task: &TaskId, id: &str) -> String {
    let (job_max, task_max) = part_budgets(prefix);
    format!(
        "{prefix}.{run}.{}.{}.{id}",
        sanitize(job.as_str(), job_max),
        sanitize(task.as_str(), task_max)
    )
}

fn error_text(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let text = text.trim();
    let mut end = text.len().min(ERROR_TEXT_MAX);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

/// `sbx create` arguments (the secrets are set afterwards, per sandbox).
fn create_args(config: &SbxConfig, name: &str, spec: &ContainerSpec) -> Vec<String> {
    let pull = match config.pull {
        PullPolicy::Missing => "missing",
        PullPolicy::Always => "always",
        PullPolicy::Never => "never",
    };
    let mut args: Vec<String> = [
        "create",
        "--no-input",
        "--no-progress",
        "--pull",
        pull,
        "--name",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    args.push(name.to_owned());
    if let Some(nano) = spec.limits.nano_cpus {
        args.push("--cpus".to_owned());
        args.push(nano.div_ceil(1_000_000_000).max(1).to_string());
    }
    if let Some(bytes) = spec.limits.memory_bytes {
        args.push("--memory".to_owned());
        args.push(format!(
            "{}m",
            bytes.div_ceil(1024 * 1024).max(MIN_MEMORY_MB)
        ));
    }
    let defaults = DEFAULT_DENY_NETWORK.iter().map(|r| (*r).to_owned());
    let mut seen = std::collections::HashSet::new();
    for rule in defaults.chain(config.deny_network.iter().cloned()) {
        if seen.insert(rule.clone()) {
            args.push("--deny-network".to_owned());
            args.push(rule);
        }
    }
    args.extend(["-t".to_owned(), spec.image.clone(), "shell".to_owned()]);
    args
}

/// `sbx secret set-custom` arguments; the value goes on stdin.
fn secret_args(name: &str, env: &str, placeholder: &str, hosts: &[String]) -> Vec<String> {
    let mut args: Vec<String> = [
        "secret",
        "set-custom",
        "--no-input",
        "--sandbox",
        name,
        "--env",
        env,
        "--placeholder",
        placeholder,
        "--value-stdin",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    for host in hosts {
        args.push("--host".to_owned());
        args.push(host.clone());
    }
    args
}

/// `sbx exec` arguments. Plain env goes by name only (`-e KEY`): the value is
/// in the child's environment, so it never shows up in `ps`.
fn exec_args(name: &str, sandbox: &Sandbox, interactive: bool, command: &str) -> Vec<String> {
    let mut args = vec!["exec".to_owned()];
    if interactive {
        args.push("-i".to_owned());
    }
    for (key, _) in &sandbox.env {
        args.push("-e".to_owned());
        args.push(key.clone());
    }
    for (key, placeholder) in &sandbox.placeholders {
        args.push("-e".to_owned());
        args.push(format!("{key}={placeholder}"));
    }
    if let Some(dir) = &sandbox.workdir {
        args.push("-w".to_owned());
        args.push(dir.clone());
    }
    args.extend([
        name.to_owned(),
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        command.to_owned(),
    ]);
    args
}

impl ContainerRuntime for SbxRuntime {
    async fn start(
        &self,
        job: &JobId,
        task: &TaskId,
        spec: &ContainerSpec,
    ) -> Result<String, RuntimeError> {
        let id = unique_hex();
        let name = sandbox_name(&self.config.name_prefix, &self.run_id, job, task, &id);
        let args = create_args(&self.config, &name, spec);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        if let Err(e) = self.run(&args, None, self.config.create_timeout).await {
            // The daemon may still finish a VM whose client timed out or died: never leave it behind.
            let _ = self.destroy(&name, &[]).await;
            return Err(e);
        }

        let mut placeholders = Vec::with_capacity(spec.secrets.len());
        for secret in &spec.secrets {
            let placeholder = format!("ph-{id}-{}", secret.env.to_ascii_lowercase());
            let args = secret_args(&name, &secret.env, &placeholder, &secret.hosts);
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            placeholders.push((secret.env.clone(), placeholder));
            if let Err(e) = self
                .run(&args, Some(secret.value.as_bytes()), SHORT_TIMEOUT)
                .await
            {
                // Never leave a sandbox (or a stored secret) behind.
                let _ = self.destroy(&name, &placeholders).await;
                return Err(e);
            }
        }
        lock(&self.sandboxes).insert(
            name.clone(),
            Sandbox {
                env: spec.env.clone(),
                placeholders,
                workdir: spec.workdir.clone(),
            },
        );
        Ok(name)
    }

    async fn exec(
        &self,
        container_id: &str,
        command: &str,
        input: Option<&[u8]>,
        mut stream: Option<InputReceiver>,
        timeout: Duration,
        observer: &mut dyn OutputObserver,
    ) -> Result<ExecStatus, RuntimeError> {
        let (args, env) = {
            let sandboxes = lock(&self.sandboxes);
            let sandbox = sandboxes
                .get(container_id)
                .ok_or_else(|| RuntimeError::new(format!("unknown sandbox `{container_id}`")))?;
            let interactive = input.is_some() || stream.is_some();
            (
                exec_args(container_id, sandbox, interactive, command),
                sandbox.env.clone(),
            )
        };
        let interactive = input.is_some() || stream.is_some();
        // The task's env values reach `sbx exec` by name (`-e KEY`), through the client's own environment, on top of
        // the clean base, which wins for the variables sbx itself reads.
        let mut command = sbx_command(&self.config.bin);
        for (key, value) in env {
            if std::env::var_os(&key).is_none() || !is_base_var(&key) {
                command.env(key, value);
            }
        }
        let mut child = command
            .args(&args)
            .stdin(if interactive {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // On timeout the future is dropped and the client killed; the VM
            // is removed right after, which ends whatever still runs in it.
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| RuntimeError::new(format!("cannot run `{}`: {e}", self.config.bin)))?;
        let mut stdin = child.stdin.take();
        let (Some(mut stdout), Some(mut stderr)) = (child.stdout.take(), child.stderr.take())
        else {
            return Err(RuntimeError::new("sbx exec has no output pipes"));
        };

        let run = async {
            // Write and read concurrently: a large input would otherwise
            // deadlock against a process that fills its output pipe first.
            let write = async {
                let Some(mut pipe) = stdin.take() else { return };
                if let Some(bytes) = input {
                    if pipe.write_all(bytes).await.is_err() {
                        return;
                    }
                }
                if let Some(chunks) = stream.as_mut() {
                    while let Some(chunk) = chunks.next().await {
                        if pipe.write_all(&chunk).await.is_err() || pipe.flush().await.is_err() {
                            return;
                        }
                    }
                }
                let _ = pipe.shutdown().await;
            };
            let read = async {
                let (mut out_buf, mut err_buf) = ([0u8; 8192], [0u8; 8192]);
                let (mut out_open, mut err_open) = (true, true);
                while out_open || err_open {
                    tokio::select! {
                        n = stdout.read(&mut out_buf), if out_open => match n {
                            Ok(n) if n > 0 => observer.on_output(StreamKind::Stdout, &out_buf[..n]),
                            _ => out_open = false,
                        },
                        n = stderr.read(&mut err_buf), if err_open => match n {
                            Ok(n) if n > 0 => observer.on_output(StreamKind::Stderr, &err_buf[..n]),
                            _ => err_open = false,
                        },
                    }
                }
            };
            // A streamed stdin may stay open after the process exits, so the
            // step ends with the output, never with the writer.
            tokio::pin!(write, read);
            let mut writing = true;
            loop {
                tokio::select! {
                    () = &mut read => break,
                    () = &mut write, if writing => writing = false,
                }
            }
            child.wait().await
        };
        match tokio::time::timeout(timeout, run).await {
            Err(_) => Ok(ExecStatus::TimedOut),
            Ok(Err(e)) => Err(RuntimeError::new(format!("sbx exec failed: {e}"))),
            Ok(Ok(status)) => Ok(ExecStatus::Exited(status.code().map_or(-1, i64::from))),
        }
    }

    /// The guest exposes no per-task cgroup counters, so there is nothing
    /// to report (the VM's own memory is the limit, set at creation).
    async fn memory_usage(&self, _container_id: &str) -> Result<MemoryUsage, RuntimeError> {
        Ok(MemoryUsage {
            peak_bytes: None,
            swap_peak_bytes: None,
            oom_kills: None,
        })
    }

    async fn remove(&self, container_id: &str) -> Result<(), RuntimeError> {
        let placeholders = lock(&self.sandboxes)
            .remove(container_id)
            .map(|s| s.placeholders)
            .unwrap_or_default();
        self.destroy(container_id, &placeholders).await
    }

    async fn remove_job(&self, job: &JobId) -> Result<usize, RuntimeError> {
        let wanted = sanitize(job.as_str(), part_budgets(&self.config.name_prefix).0);
        self.remove_matching(|p| p.job == wanted).await
    }

    async fn remove_owned(&self) -> Result<usize, RuntimeError> {
        self.remove_matching(|p| p.run == self.run_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::SecretSpec;

    fn ids() -> (JobId, TaskId) {
        (JobId::new("my_job.1").unwrap(), TaskId::new("t-1").unwrap())
    }

    #[test]
    fn names_round_trip_and_replace_the_separator() {
        let (job, task) = ids();
        let name = sandbox_name("cellrun", "ab12", &job, &task, "0123");
        assert_eq!(name, "cellrun.ab12.my-job-1.t-1.0123");
        let parts = NameParts::parse(&name).unwrap();
        assert_eq!(
            (parts.prefix, parts.run, parts.job),
            ("cellrun", "ab12", "my-job-1")
        );
        assert!(NameParts::parse("other-sandbox").is_none());
        assert!(NameParts::parse("a.b.c.d.e.f").is_none());
    }

    #[test]
    fn names_fit_sbxs_limit_with_the_api_sized_ids() {
        // What the api sends: `agthy-<uuid>` jobs, `agent` tasks, the `agthy` prefix.
        let job = JobId::new("agthy-c6ef6cab-f138-4bf0-bb03-9dd28f4cd0b6").unwrap();
        let task = TaskId::new("agent").unwrap();
        let name = sandbox_name("agthy", "628b8a02", &job, &task, "629b8be6611b0");
        assert!(name.len() <= NAME_MAX, "{} chars: {name}", name.len());
        let parts = NameParts::parse(&name).unwrap();
        // remove_job finds it again from the job id alone.
        assert_eq!(parts.job, sanitize(job.as_str(), part_budgets("agthy").0));
        // A cut (or the id) that ends in a dash must not end a part: sbx wants letters or digits at both ends.
        let cut = JobId::new("agthy-c6ef6cab-f138-4bf0-bb03-9dd28f4cd0b6").unwrap();
        for part in sandbox_name(
            "cellrun",
            "628b8a02",
            &cut,
            &TaskId::new("a-").unwrap(),
            "1",
        )
        .split('.')
        {
            assert!(
                part.ends_with(|c: char| c.is_ascii_alphanumeric()),
                "{part}"
            );
        }
        // The longest prefix and the longest ids still fit.
        let long = TaskId::new("t".repeat(64)).unwrap();
        let long_job = JobId::new("j".repeat(64)).unwrap();
        let prefix = "p".repeat(16);
        assert!(
            sandbox_name(&prefix, "628b8a02", &long_job, &long, "629b8be6611b0").len() <= NAME_MAX
        );
    }

    #[test]
    fn create_args_carry_limits_deny_rules_and_the_template() {
        let mut spec = ContainerSpec::new("agthy/runner:engineer").unwrap();
        spec.limits.nano_cpus = Some(1_500_000_000);
        spec.limits.memory_bytes = Some(256 * 1024 * 1024);
        let config = SbxConfig {
            deny_network: vec!["203.0.113.7".to_owned(), "10.0.0.0/8".to_owned()],
            ..SbxConfig::default()
        };
        let args = create_args(&config, "n", &spec).join(" ");
        assert!(args.contains("--cpus 2"), "{args}");
        assert!(
            args.contains("--memory 512m"),
            "below sbx's minimum: {args}"
        );
        // The defaults always apply, the host's extra rules come on top, nothing twice.
        for rule in [
            "10.0.0.0/8",
            "0.0.0.0/8",
            "localhost",
            "host.docker.internal",
            "203.0.113.7",
        ] {
            assert!(
                args.contains(&format!("--deny-network {rule}")),
                "{rule}: {args}"
            );
        }
        assert_eq!(
            args.matches("--deny-network 10.0.0.0/8").count(),
            1,
            "{args}"
        );
        assert!(args.ends_with("-t agthy/runner:engineer shell"), "{args}");
        assert!(!args.contains("--env"), "env never goes on create");
    }

    #[test]
    fn exec_args_keep_secrets_and_env_values_off_the_command_line() {
        let sandbox = Sandbox {
            env: vec![("LOG_LEVEL".to_owned(), "real-value".to_owned())],
            placeholders: vec![("API_KEY".to_owned(), "ph-1-api_key".to_owned())],
            workdir: Some("/work".to_owned()),
        };
        let args = exec_args("n", &sandbox, true, "echo hi").join(" ");
        assert_eq!(
            args,
            "exec -i -e LOG_LEVEL -e API_KEY=ph-1-api_key -w /work n /bin/sh -c echo hi"
        );
        assert!(!args.contains("real-value"));
    }

    #[test]
    fn secret_args_send_the_value_on_stdin_only() {
        let secret = SecretSpec {
            env: "API_KEY".to_owned(),
            value: "s3cret".to_owned(),
            hosts: vec!["a.example.com".to_owned(), "b.example.com".to_owned()],
        };
        let args = secret_args("n", &secret.env, "ph", &secret.hosts).join(" ");
        assert!(args.contains("--value-stdin"));
        assert!(args.contains("--host a.example.com --host b.example.com"));
        assert!(!args.contains("s3cret"));
    }

    #[test]
    fn prefix_is_validated() {
        let bad = |p: &str| {
            SbxRuntime::new(SbxConfig {
                name_prefix: p.to_owned(),
                ..SbxConfig::default()
            })
            .is_err()
        };
        assert!(bad(""));
        assert!(bad("has.dot"));
        assert!(bad("-lead"));
        assert!(bad("UPPER"));
        assert!(!bad("cellrun"));
    }

    #[test]
    fn error_text_is_bounded() {
        assert_eq!(error_text(b"  boom \n"), "boom");
        assert_eq!(error_text("é".repeat(1000).as_bytes()).len(), 400);
    }
}
