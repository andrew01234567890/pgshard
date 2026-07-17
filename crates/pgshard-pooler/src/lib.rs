//! Fail-closed pooler runtime foundations.
//!
//! The crate publishes process health and catalog usability separately from
//! overall application readiness. An explicitly configured compatibility mode
//! relays raw `PostgreSQL` sessions to one shard-zero writer while the catalog
//! is ready. It is not a connection pool or SQL router; absent that explicit
//! target, the bounded frontend rejects regular sessions.

pub mod config;
mod frontend;
pub mod http;
pub mod runtime;
mod server;
pub mod state;
