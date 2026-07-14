//! Pooler runtime control-plane foundations.
//!
//! The crate publishes process health and catalog usability separately from
//! overall application readiness. The executable remains application-unready:
//! its bounded `PostgreSQL` handshake listener rejects sessions because
//! authentication, backend pooling, and query execution do not exist yet.

pub mod config;
mod frontend;
pub mod http;
pub mod runtime;
mod server;
pub mod state;
