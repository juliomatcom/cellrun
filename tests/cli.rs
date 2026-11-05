//! CLI usage errors that must fail before Docker is touched.

use std::process::{Command, Stdio};

#[test]
fn control_on_stdin_requires_a_job_file() {
    let out = Command::new(env!("CARGO_BIN_EXE_dispatcher"))
        .args(["run", "-", "--control", "-"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("job must be a file path"), "{stderr}");
}
