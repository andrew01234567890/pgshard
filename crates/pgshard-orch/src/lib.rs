//! Conservative orchestration primitives for pgshard.
//!
//! This crate records operation identity and lease ownership. It deliberately
//! does not select `PostgreSQL` promotion candidates or claim failover safety.

pub mod config;
pub mod domain;
pub mod http;
mod postgres_connection;
pub mod slot_catalog;
pub mod slot_mutator;
pub mod slot_observer;
pub mod slot_probe_catalog;
pub mod standby_slots;
pub mod telemetry;

fn parse_lsn(value: &str) -> Option<pgshard_types::PgLsn> {
    let (high, low) = value.split_once('/')?;
    if high.is_empty() || high.len() > 8 || low.is_empty() || low.len() > 8 {
        return None;
    }
    let high = u32::from_str_radix(high, 16).ok()?;
    let low = u32::from_str_radix(low, 16).ok()?;
    Some(pgshard_types::PgLsn(
        (u64::from(high) << 32) | u64::from(low),
    ))
}
