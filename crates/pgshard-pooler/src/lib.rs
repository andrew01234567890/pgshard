//! Pooler runtime control-plane foundations.
//!
//! The crate publishes process health and catalog usability separately from
//! overall application readiness. The control-only executable remains
//! application-unready because it does not yet provide a `PostgreSQL` listener,
//! authentication, backend pooling, or query execution.

pub mod config;
pub mod http;
pub mod runtime;
pub mod state;
