//! Pooler runtime control-plane foundations.
//!
//! The crate publishes process health separately from fail-closed catalog
//! readiness. It does not yet provide a `PostgreSQL` listener, authentication,
//! backend pooling, or query execution.

pub mod http;
pub mod state;
