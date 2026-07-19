//! Runtime foundations for the `PostgreSQL` instance agent.

pub mod config;
pub mod coordination;
pub mod domain;
pub mod http;
pub mod postgres;
pub mod postgres_generation;
pub(crate) mod postgres_recovery;
pub mod telemetry;
pub mod writable;
