//! Validated snapshots of the authoritative `shardschema` catalog.
//!
//! Readers use an immutable, checksummed snapshot. [`CatalogCache`] serializes
//! rare installs while [`arc_swap`] keeps request-path reads lock free. A
//! A `PostgreSQL` notification is only a wake-up hint; it never supplies catalog
//! data and can be lost without affecting correctness because callers poll.

mod cache;
mod loader;
mod model;

pub use cache::{
    CacheError, CatalogCache, CatalogNotification, InstallOutcome, NotificationError,
    RefreshDecision, RequestEpochError,
};
pub use loader::{
    CatalogReader, LoadError, MAX_LOGICAL_DATABASES, MAX_REGISTERED_TABLES_PER_DATABASE,
    MAX_ROUTING_RANGES_PER_DATABASE, MAX_TOTAL_REGISTERED_TABLES, MAX_TOTAL_ROUTING_RANGES,
};
pub use model::{
    CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, DatabaseId, IdentifierError,
    RegisteredTable, RoutingHashConfig, ShardKeyType, ShardRoute, SnapshotError, TableName,
};

/// Dedicated catalog database hosted on stable shard 0000 in Milestone 1.
pub const SHARDSCHEMA_DATABASE: &str = "shardschema";

/// Transactional `PostgreSQL` notification channel used only as a refresh hint.
pub const NOTIFY_CHANNEL: &str = "pgshard_catalog_changed";

/// Idempotent `PostgreSQL` 18 catalog migration applied inside `shardschema`.
pub const MIGRATION_SQL: &str = include_str!("../migrations/0001_shardschema.sql");
