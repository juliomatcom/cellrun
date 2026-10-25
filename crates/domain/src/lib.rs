//! Domain layer (innermost circle).
//!
//! Holds the core model of the orchestrator: what a job is, how its tasks and
//! steps are shaped, and what a result looks like. It has no knowledge of
//! Docker, JSON, async runtimes or the filesystem.

pub mod error;
pub mod event;
pub mod job;
pub mod output;
pub mod report;

pub use error::ValidationError;
pub use event::{Event, StreamKind};
pub use job::{ContainerSpec, Job, JobId, ResourceLimits, Step, Task, TaskId};
pub use output::{LineSplitter, TailBuffer};
pub use report::{JobReport, StepOutcome, StepReport, TaskReport, TaskStatus};
