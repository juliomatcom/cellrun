//! Swarm: runs jobs of shell steps in ephemeral, isolated Docker containers.
//!
//! Clean Architecture, dependencies point inwards only:
//! [`domain`] ← [`application`] ← [`infrastructure`] ← the `dispatcher`
//! binary (composition root). `tests/architecture.rs` enforces this.

pub mod application;
pub mod domain;
pub mod infrastructure;
