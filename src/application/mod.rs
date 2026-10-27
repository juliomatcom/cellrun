//! Application layer: orchestrates the domain through abstract ports.
//!
//! Adapters (Docker, JSON, files...) implement the traits in [`ports`]; this
//! crate never knows which concrete implementation it is talking to.

pub mod ports;
pub mod run_job;

pub use ports::{ContainerRuntime, EventSink, ExecStatus, OutputObserver, RuntimeError};
pub use run_job::{OutputLimits, RunJob};
