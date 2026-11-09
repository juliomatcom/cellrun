//! Infrastructure layer: concrete adapters for the ports declared in
//! `application`, plus (de)serialization of the external JSON formats.
//!
//! Wire formats (DTOs) live here, never in `domain`.

pub mod control_input;
pub mod docker;
pub mod job_input;
pub mod ndjson_sink;
pub mod report_json;
pub mod sbx;
pub mod vm;

pub use control_input::{BoxedInput, ControlReader, open_control};
pub use docker::{DockerConfig, DockerRuntime, PullPolicy};
pub use job_input::{InputError, parse_job};
pub use ndjson_sink::NdjsonSink;
pub use report_json::write_report;
pub use sbx::{DEFAULT_DENY_NETWORK, SbxConfig, SbxRuntime};
pub use vm::VmRuntime;
