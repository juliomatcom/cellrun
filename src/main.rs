//! `dispatcher` binary: the composition root. The only place where concrete
//! adapters are chosen and plugged into use cases.

use std::fs::File;
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};

use cellrun::application::{
    ContainerRuntime, ControlRouter, EventSink, OutputLimits, Routed, RunJob,
};
use cellrun::domain::{Event, Isolation, Job, JobId};
use cellrun::infrastructure::{
    ControlReader, DEFAULT_DENY_NETWORK, DockerConfig, DockerRuntime, NdjsonSink, PullPolicy,
    SbxConfig, SbxRuntime, VmRuntime, open_control, parse_job, write_report,
};

/// What the use case runs on: Docker for container tasks, the VM backend for
/// VM tasks.
type Runtime = Routed<DockerRuntime, VmRuntime>;

const EXIT_TASK_FAILED: u8 = 1;
const EXIT_BAD_INPUT: u8 = 2;
const EXIT_RUNTIME: u8 = 3;
const EXIT_INTERRUPTED: u8 = 130;

/// Dispatches ephemeral Docker containers that run shell steps and reports
/// their output.
#[derive(Parser)]
#[command(name = "dispatcher", version)]
struct Cli {
    #[command(flatten)]
    task: TaskArgs,
    #[command(subcommand)]
    command: Command,
}

/// Host-level settings for task containers (never taken from job files).
#[derive(clap::Args)]
struct TaskArgs {
    /// OCI runtime for task containers, e.g. `runsc` (gVisor) for a
    /// separate kernel between the workload and the host.
    #[arg(long, global = true, env = "DISPATCHER_TASK_RUNTIME")]
    task_runtime: Option<String>,
    /// DNS servers for task containers (must be public: private ranges are
    /// firewalled off by the host firewall, see README).
    #[arg(
        long,
        global = true,
        env = "DISPATCHER_TASK_DNS",
        value_delimiter = ',',
        default_value = "1.1.1.1,8.8.8.8"
    )]
    task_dns: Vec<String>,
    /// `resolv.conf` options for task containers (`use-vc`: DNS over TCP,
    /// for tasks that close UDP; Go programs read it there, not from the env).
    #[arg(
        long,
        global = true,
        env = "DISPATCHER_TASK_DNS_OPTIONS",
        value_delimiter = ','
    )]
    task_dns_options: Vec<String>,
    /// Prefix of the labels on containers and networks (`<prefix>.job`, ...).
    /// Keep it stable: `cleanup` only finds resources labelled with it.
    #[arg(
        long,
        global = true,
        env = "DISPATCHER_LABEL_PREFIX",
        default_value = "cellrun"
    )]
    label_prefix: String,
    /// Prefix of task bridge interfaces (1-2 chars of [a-z0-9]). The host
    /// firewall (the caller's job, see README) must match the same prefix.
    #[arg(
        long,
        global = true,
        env = "DISPATCHER_BRIDGE_PREFIX",
        default_value = "cr"
    )]
    bridge_prefix: String,
    /// Backend that runs tasks with `isolation: "vm"`. Only needed when a job
    /// has such tasks.
    #[arg(
        long,
        global = true,
        env = "DISPATCHER_VM_BACKEND",
        value_enum,
        default_value_t = VmBackend::Sbx
    )]
    vm_backend: VmBackend,
    /// The `sbx` executable (Docker Sandboxes), for `--vm-backend sbx`.
    #[arg(long, global = true, env = "DISPATCHER_SBX_BIN", default_value = "sbx")]
    sbx_bin: String,
    /// Egress deny rules added to every VM task, on top of the backend's own
    /// policy (so the VM cannot reach the host's LAN or the cloud metadata
    /// server). Empty: private, link-local, loopback and CGNAT ranges;
    /// `none`: no extra rules.
    #[arg(
        long,
        global = true,
        env = "DISPATCHER_VM_DENY_NETWORK",
        value_delimiter = ','
    )]
    vm_deny_network: Vec<String>,
}

impl TaskArgs {
    fn vm_runtime(
        &self,
        pull: PullPolicy,
    ) -> Result<VmRuntime, cellrun::application::RuntimeError> {
        match self.vm_backend {
            VmBackend::Sbx => SbxRuntime::new(self.sbx_config(pull)).map(VmRuntime::Sbx),
        }
    }

    fn sbx_config(&self, pull: PullPolicy) -> SbxConfig {
        let deny_network = match self.vm_deny_network.as_slice() {
            [] => DEFAULT_DENY_NETWORK
                .iter()
                .map(|r| (*r).to_owned())
                .collect(),
            [only] if only == "none" => Vec::new(),
            rules => rules.iter().filter(|r| !r.is_empty()).cloned().collect(),
        };
        SbxConfig {
            bin: self.sbx_bin.clone(),
            pull,
            // Same prefix as the Docker labels, so one setting names both.
            name_prefix: self.label_prefix.clone(),
            deny_network,
            ..SbxConfig::default()
        }
    }

    fn docker_config(&self, pull: PullPolicy) -> DockerConfig {
        DockerConfig {
            pull,
            dns: self.task_dns.clone(),
            dns_options: self
                .task_dns_options
                .iter()
                .filter(|o| !o.is_empty())
                .cloned()
                .collect(),
            runtime: self.task_runtime.clone().filter(|r| !r.is_empty()),
            label_prefix: self.label_prefix.clone(),
            bridge_prefix: self.bridge_prefix.clone(),
            ..DockerConfig::default()
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Run a job. Progress events go to --events (NDJSON), the final report
    /// to --report (JSON).
    Run {
        /// Job file (JSON), or `-` for stdin.
        job: PathBuf,
        /// Where to stream NDJSON events: a file path or `-` for stderr.
        #[arg(long, default_value = "-")]
        events: PathBuf,
        /// Where to write the JSON report: a file path or `-` for stdout.
        #[arg(long, default_value = "-")]
        report: PathBuf,
        /// When to pull images.
        #[arg(long, value_enum, default_value_t = Pull::Missing)]
        pull: Pull,
        /// Bytes of stdout/stderr kept per step in the report.
        #[arg(long, default_value_t = 16 * 1024)]
        tail_bytes: usize,
        /// Longest output line emitted as one event; longer lines are split
        /// into pieces of this size. Raise it when a task prints large
        /// single-line results (e.g. JSON) that the caller must parse whole.
        #[arg(long, env = "DISPATCHER_MAX_LINE_BYTES", default_value = "8192")]
        max_line_bytes: NonZeroUsize,
        /// Task containers alive at once. Host capacity, so it is set here
        /// and never by the job: budget `max_parallel × memory_mb` of RAM.
        #[arg(long, env = "DISPATCHER_MAX_PARALLEL", default_value = "2")]
        max_parallel: NonZeroUsize,
        /// Control input for `stdin_stream` steps: NDJSON lines
        /// `{"task":"<id>","line":"<text>"}` read from this pipe/FIFO (e.g.
        /// `/dev/fd/3`), or from stdin with `-` (then the job must be a
        /// file). Closing it sends EOF to every streamed stdin.
        #[arg(long, value_name = "PATH")]
        control: Option<PathBuf>,
    },
    /// Validate a job file without running anything.
    Validate {
        /// Job file (JSON), or `-` for stdin.
        job: PathBuf,
    },
    /// Remove leftover containers of a job (e.g. after a crash).
    Cleanup {
        /// Job id.
        #[arg(required_unless_present = "all")]
        job_id: Option<String>,
        /// Remove leftovers of every job carrying the label prefix instead.
        #[arg(long, conflicts_with = "job_id")]
        all: bool,
    },
}

/// MicroVM backends for `isolation: "vm"`.
#[derive(Clone, Copy, ValueEnum)]
enum VmBackend {
    /// Docker Sandboxes (`sbx`).
    Sbx,
}

#[derive(Clone, Copy, ValueEnum)]
enum Pull {
    Missing,
    Always,
    Never,
}

impl From<Pull> for PullPolicy {
    fn from(p: Pull) -> Self {
        match p {
            Pull::Missing => Self::Missing,
            Pull::Always => Self::Always,
            Pull::Never => Self::Never,
        }
    }
}

// Single-threaded runtime: the work is I/O bound and this keeps the memory
// footprint minimal on small VMs.
fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail(EXIT_RUNTIME, format_args!("cannot start runtime: {e}")),
    };
    let code = runtime.block_on(run_cli());
    // Do not wait for a blocking read of `--control -` that is still parked
    // on a stdin the caller keeps open after the job ended.
    runtime.shutdown_background();
    code
}

async fn run_cli() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Run {
            job,
            events,
            report,
            pull,
            tail_bytes,
            max_line_bytes,
            max_parallel,
            control,
        } => {
            let limits = OutputLimits {
                tail_bytes,
                max_line_bytes: max_line_bytes.get(),
            };
            let paths = RunPaths {
                job: &job,
                events: &events,
                report: &report,
                control: control.as_deref(),
            };
            run(paths, &cli.task, pull.into(), limits, max_parallel).await
        }
        Command::Validate { job } => match load_job(&job) {
            Ok(job) => {
                println!("ok: job `{}` with {} task(s)", job.id, job.tasks.len());
                ExitCode::SUCCESS
            }
            Err(code) => code,
        },
        Command::Cleanup { job_id, all } => match job_id {
            Some(job_id) if !all => cleanup(&job_id, &cli.task).await,
            _ => cleanup_all(&cli.task).await,
        },
    }
}

/// The files a `run` reads and writes.
#[derive(Clone, Copy)]
struct RunPaths<'a> {
    job: &'a Path,
    events: &'a Path,
    report: &'a Path,
    control: Option<&'a Path>,
}

async fn run(
    paths: RunPaths<'_>,
    task: &TaskArgs,
    pull: PullPolicy,
    limits: OutputLimits,
    max_parallel: NonZeroUsize,
) -> ExitCode {
    let RunPaths {
        job,
        events,
        report,
        control,
    } = paths;
    if control == Some(Path::new("-")) && job == Path::new("-") {
        return fail(
            EXIT_BAD_INPUT,
            "`--control -` reads stdin, so the job must be a file path, not `-`",
        );
    }
    let job = match load_job(job) {
        Ok(job) => job,
        Err(code) => return code,
    };
    let control = match control.map(open_control).transpose() {
        Ok(reader) => reader,
        Err(e) => {
            return fail(
                EXIT_BAD_INPUT,
                format_args!("cannot open control input: {e}"),
            );
        }
    };
    let uses = |level: Isolation| job.tasks.iter().any(|t| t.container.isolation == level);
    let runtime = match connect(task, pull, uses(Isolation::Container), uses(Isolation::Vm)).await {
        Ok(rt) => rt,
        Err(code) => return code,
    };
    let sink: NdjsonSink<Box<dyn Write + Send>> = match open_output(events, io::stderr) {
        Ok(out) => NdjsonSink::new(out),
        Err(e) => {
            return fail(
                EXIT_BAD_INPUT,
                format_args!("cannot open events output: {e}"),
            );
        }
    };
    let router = ControlRouter::new();
    let mut use_case = RunJob::new(&runtime, &sink, limits, max_parallel);
    if control.is_some() {
        use_case = use_case.with_control(router.clone());
    }

    // The control pump never completes: it is a select branch, so it is
    // dropped (and nothing keeps the process alive) once the job is done.
    let pump = async {
        match control {
            Some(reader) => match pump_control(reader, &router, &sink).await {},
            None => std::future::pending().await,
        }
    };
    let result = tokio::select! {
        report = use_case.execute(&job) => report,
        never = pump => match never {},
        () = shutdown_signal() => {
            // Dropping `execute` abandons in-flight work; reap its containers.
            sink.emit(&Event::Warning { task: None, message: "interrupted, removing containers" });
            // Only this process's containers: a concurrent run of the same job
            // is left alone. The second sweep catches a create request that was
            // already in flight when `execute` was dropped.
            for delay in [0, 500] {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                if let Err(e) = runtime.remove_owned().await {
                    eprintln!("cleanup failed: {e}; run `dispatcher cleanup {}`", job.id);
                }
            }
            return ExitCode::from(EXIT_INTERRUPTED);
        }
    };

    let written = open_output(report, io::stdout).and_then(|out| write_report(&result, out));
    if let Err(e) = written {
        return fail(EXIT_RUNTIME, format_args!("cannot write report: {e}"));
    }
    if result.succeeded() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(EXIT_TASK_FAILED)
    }
}

/// Routes control lines to their tasks until the input closes, then sends EOF
/// to every streamed stdin and idles (the job decides when the run ends).
async fn pump_control<R, S>(
    mut reader: ControlReader<R>,
    router: &ControlRouter,
    sink: &S,
) -> std::convert::Infallible
where
    R: tokio::io::AsyncBufRead + Unpin,
    S: EventSink,
{
    loop {
        match reader.next_message().await {
            Ok(Some(message)) => router.handle(sink, message),
            Ok(None) => break,
            Err(e) => {
                sink.emit(&Event::Warning {
                    task: None,
                    message: &format!("control input failed: {e}"),
                });
                break;
            }
        }
    }
    router.close_all();
    std::future::pending().await
}

async fn cleanup(job_id: &str, task: &TaskArgs) -> ExitCode {
    let job_id = match JobId::new(job_id) {
        Ok(id) => id,
        Err(e) => return fail(EXIT_BAD_INPUT, e),
    };
    let (docker, sbx) = match reachable(task).await {
        Ok(pair) => pair,
        Err(code) => return code,
    };
    match Routed::new(docker, sbx).remove_job(&job_id).await {
        Ok(n) => {
            println!("removed {n} container(s)");
            ExitCode::SUCCESS
        }
        Err(e) => fail(EXIT_RUNTIME, e),
    }
}

async fn cleanup_all(task: &TaskArgs) -> ExitCode {
    let (docker, sbx) = match reachable(task).await {
        Ok(pair) => pair,
        Err(code) => return code,
    };
    let mut total = 0;
    if let Some(docker) = &docker {
        match docker.remove_all().await {
            Ok(n) => total += n,
            Err(e) => return fail(EXIT_RUNTIME, e),
        }
    }
    if let Some(sbx) = &sbx {
        match sbx.remove_all().await {
            Ok(n) => total += n,
            Err(e) => return fail(EXIT_RUNTIME, e),
        }
    }
    println!("removed {total} container(s)");
    ExitCode::SUCCESS
}

/// The runtimes that answer on this host, for cleanup: each is optional (a
/// host with only Docker, or only `sbx`), but at least one must be there.
async fn reachable(
    task: &TaskArgs,
) -> Result<(Option<DockerRuntime>, Option<VmRuntime>), ExitCode> {
    let docker = connect_docker(task, PullPolicy::Never).await;
    let sbx = match task.vm_runtime(PullPolicy::Never) {
        Ok(rt) => rt.ping().await.map(|()| rt),
        Err(e) => Err(e),
    };
    match (docker, sbx) {
        (Err(d), Err(_)) => Err(fail(EXIT_RUNTIME, format_args!("docker unreachable: {d}"))),
        (docker, sbx) => Ok((docker.ok(), sbx.ok())),
    }
}

fn load_job(path: &Path) -> Result<Job, ExitCode> {
    let mut raw = Vec::new();
    let read = if path == Path::new("-") {
        io::stdin().read_to_end(&mut raw)
    } else {
        File::open(path).and_then(|mut f| f.read_to_end(&mut raw))
    };
    if let Err(e) = read {
        return Err(fail(
            EXIT_BAD_INPUT,
            format_args!("cannot read {}: {e}", path.display()),
        ));
    }
    parse_job(&raw, |name| std::env::var(name).ok())
        .map_err(|e| fail(EXIT_BAD_INPUT, format_args!("invalid job: {e}")))
}

async fn connect_docker(
    task: &TaskArgs,
    pull: PullPolicy,
) -> Result<DockerRuntime, cellrun::application::RuntimeError> {
    let runtime = DockerRuntime::connect(task.docker_config(pull))?;
    runtime.ping().await?;
    Ok(runtime)
}

/// Connects only the runtimes the job needs, so a host without Docker can run
/// VM-only jobs (and the other way round).
async fn connect(
    task: &TaskArgs,
    pull: PullPolicy,
    container: bool,
    vm: bool,
) -> Result<Runtime, ExitCode> {
    let docker = if container {
        Some(
            connect_docker(task, pull)
                .await
                .map_err(|e| fail(EXIT_RUNTIME, format_args!("docker unreachable: {e}")))?,
        )
    } else {
        None
    };
    let sbx = if vm {
        let runtime = task.vm_runtime(pull).map_err(|e| fail(EXIT_RUNTIME, e))?;
        runtime
            .ping()
            .await
            .map_err(|e| fail(EXIT_RUNTIME, format_args!("vm backend unavailable: {e}")))?;
        Some(runtime)
    } else {
        None
    };
    Ok(Routed::new(docker, sbx))
}

fn open_output<W: Write + Send + 'static>(
    path: &Path,
    default: fn() -> W,
) -> io::Result<Box<dyn Write + Send>> {
    if path == Path::new("-") {
        Ok(Box::new(default()))
    } else {
        Ok(Box::new(io::BufWriter::new(File::create(path)?)))
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = term.recv() => {},
            },
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn fail(code: u8, message: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: {message}");
    ExitCode::from(code)
}
