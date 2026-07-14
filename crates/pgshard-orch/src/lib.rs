//! Conservative orchestration primitives for pgshard.
//!
//! This crate records operation identity and lease ownership. It deliberately
//! does not select `PostgreSQL` promotion candidates or claim failover safety.

pub mod config;
pub mod domain;
pub mod http;
pub mod slot_catalog;
pub mod slot_observer;
pub mod standby_slots;
pub mod telemetry;
