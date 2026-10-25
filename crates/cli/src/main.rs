//! `orchestrator` binary: the composition root. The only place where concrete
//! adapters are chosen and plugged into use cases.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};

use application::{ContainerRuntime, EventSink, OutputLimits, RunJob};
use domain::{Event, Job, JobId};
use infrastructure::{
    DockerConfig, DockerRuntime, NdjsonSink, PullPolicy, parse_job, write_report,
};

const EXIT_TASK_FAILED: u8 = 1;
const EXIT_BAD_INPUT: u8 = 2;
const EXIT_RUNTIME: u8 = 3;
const EXIT_INTERRUPTED: u8 = 130;

/// Dispatches ephemeral Docker containers that run shell steps and reports
/// their output.
#[derive(Parser)]
#[command(name = "orchestrator", version)]
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
    #[arg(long, global = true, env = "ORCHESTRATOR_TASK_RUNTIME")]
    task_runtime: Option<String>,
    /// DNS servers for task containers (must be public: private ranges are
    /// firewalled off by sandbox-guard).
    #[arg(
        long,
        global = true,
        env = "ORCHESTRATOR_TASK_DNS",
        value_delimiter = ',',
        default_value = "1.1.1.1,8.8.8.8"
    )]
    task_dns: Vec<String>,
}

impl TaskArgs {
    fn docker_config(&self, pull: PullPolicy) -> DockerConfig {
        DockerConfig {
            pull,
            dns: self.task_dns.clone(),
            runtime: self.task_runtime.clone().filter(|r| !r.is_empty()),
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
    },
    /// Validate a job file without running anything.
    Validate {
        /// Job file (JSON), or `-` for stdin.
        job: PathBuf,
    },
    /// Remove leftover containers of a job (e.g. after a crash).
    Cleanup {
        /// Job id.
        job_id: String,
    },
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
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Run {
            job,
            events,
            report,
            pull,
            tail_bytes,
        } => {
            let config = cli.task.docker_config(pull.into());
            run(&job, &events, &report, config, tail_bytes).await
        }
        Command::Validate { job } => match load_job(&job) {
            Ok(job) => {
                println!("ok: job `{}` with {} task(s)", job.id, job.tasks.len());
                ExitCode::SUCCESS
            }
            Err(code) => code,
        },
        Command::Cleanup { job_id } => {
            cleanup(&job_id, cli.task.docker_config(PullPolicy::Never)).await
        }
    }
}

async fn run(
    job: &Path,
    events: &Path,
    report: &Path,
    config: DockerConfig,
    tail_bytes: usize,
) -> ExitCode {
    let job = match load_job(job) {
        Ok(job) => job,
        Err(code) => return code,
    };
    let runtime = match connect(config).await {
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
    let limits = OutputLimits {
        tail_bytes,
        ..OutputLimits::default()
    };
    let use_case = RunJob::new(&runtime, &sink, limits);

    let result = tokio::select! {
        report = use_case.execute(&job) => report,
        () = shutdown_signal() => {
            // Dropping `execute` abandons in-flight work; reap its containers.
            sink.emit(&Event::Warning { task: None, message: "interrupted, removing containers" });
            // Only this process's containers: a concurrent run of the same job
            // is left alone. The second sweep catches a create request that was
            // already in flight when `execute` was dropped.
            for delay in [0, 500] {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                if let Err(e) = runtime.remove_owned().await {
                    eprintln!("cleanup failed: {e}; run `orchestrator cleanup {}`", job.id);
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

async fn cleanup(job_id: &str, config: DockerConfig) -> ExitCode {
    let job_id = match JobId::new(job_id) {
        Ok(id) => id,
        Err(e) => return fail(EXIT_BAD_INPUT, e),
    };
    let runtime = match connect(config).await {
        Ok(rt) => rt,
        Err(code) => return code,
    };
    match runtime.remove_job(&job_id).await {
        Ok(n) => {
            println!("removed {n} container(s)");
            ExitCode::SUCCESS
        }
        Err(e) => fail(EXIT_RUNTIME, e),
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

async fn connect(config: DockerConfig) -> Result<DockerRuntime, ExitCode> {
    let runtime = DockerRuntime::connect(config).map_err(|e| fail(EXIT_RUNTIME, e))?;
    runtime
        .ping()
        .await
        .map_err(|e| fail(EXIT_RUNTIME, format_args!("docker unreachable: {e}")))?;
    Ok(runtime)
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
