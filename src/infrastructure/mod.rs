//! Infrastructure layer: concrete adapters for the ports declared in
//! `application`, plus (de)serialization of the external JSON formats.
//!
//! Wire formats (DTOs) live here, never in `domain`.

pub mod docker;
pub mod job_input;
pub mod ndjson_sink;
pub mod report_json;

pub use docker::{DockerConfig, DockerRuntime, PullPolicy};
pub use job_input::{InputError, parse_job};
pub use ndjson_sink::NdjsonSink;
pub use report_json::write_report;
