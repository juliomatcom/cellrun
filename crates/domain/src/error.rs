use std::fmt;

/// Raised when a job definition violates a domain invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    EmptyField(&'static str),
    InvalidId(String),
    DuplicateTaskId(String),
    NoTasks,
    NoSteps(String),
    ZeroValue(&'static str),
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField(field) => write!(f, "`{field}` must not be empty"),
            Self::InvalidId(id) => write!(
                f,
                "invalid id `{id}`: use 1-64 chars of [A-Za-z0-9_.-], starting with alphanumeric"
            ),
            Self::DuplicateTaskId(id) => write!(f, "duplicate task id `{id}`"),
            Self::NoTasks => write!(f, "job must contain at least one task"),
            Self::NoSteps(id) => write!(f, "task `{id}` must contain at least one step"),
            Self::ZeroValue(field) => write!(f, "`{field}` must be greater than zero"),
        }
    }
}

impl std::error::Error for ValidationError {}
