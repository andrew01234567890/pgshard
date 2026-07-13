//! Read-only pooler state derived from catalog supervision.

use std::sync::Arc;
use std::time::Duration;

use pgshard_catalog::{CatalogFailureKind, CatalogSupervisorSnapshot, CatalogSupervisorStatus};
use serde::{Serialize, Serializer};

type CatalogSnapshotSource = dyn Fn() -> PoolerCatalogSnapshot + Send + Sync;

/// Externally reportable catalog state for one pooler process.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PoolerCatalogSnapshot {
    /// Dedicated catalog connection lifecycle phase.
    pub phase: &'static str,
    /// Whether a catalog socket is currently owned by the refresh driver.
    pub connection_up: bool,
    /// Whether new work may use the cached catalog.
    pub ready: bool,
    /// Exact bounded reason for the readiness decision.
    pub readiness_reason: &'static str,
    /// Latest authoritative catalog epoch, encoded as a decimal string in JSON.
    #[serde(serialize_with = "serialize_optional_u64_decimal")]
    pub catalog_epoch: Option<u64>,
    /// Monotonic age of the last authoritative load, encoded as decimal milliseconds.
    #[serde(
        rename = "cache_age_milliseconds",
        serialize_with = "serialize_optional_duration_milliseconds"
    )]
    pub cache_age: Option<Duration>,
    /// Failures since the last authoritative load, encoded as a decimal string.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub consecutive_failures: u64,
    /// Failures during this supervisor's life, encoded as a decimal string.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub total_failures: u64,
    /// All connection attempts, encoded as a decimal string.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub connect_attempts: u64,
    /// Connections that completed an initial load, encoded as a decimal string.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub successful_connections: u64,
    /// Credential-safe bounded class of the latest unresolved failure.
    pub last_failure: Option<&'static str>,
}

impl From<CatalogSupervisorSnapshot> for PoolerCatalogSnapshot {
    fn from(snapshot: CatalogSupervisorSnapshot) -> Self {
        Self {
            phase: snapshot.phase().as_str(),
            connection_up: snapshot.phase().connection_up(),
            ready: snapshot.ready(),
            readiness_reason: snapshot.readiness_reason().as_str(),
            catalog_epoch: snapshot.catalog_epoch().map(|epoch| epoch.0),
            cache_age: snapshot.cache_age(),
            consecutive_failures: snapshot.consecutive_failures(),
            total_failures: snapshot.total_failures(),
            connect_attempts: snapshot.connect_attempts(),
            successful_connections: snapshot.successful_connections(),
            last_failure: snapshot.last_failure().map(CatalogFailureKind::as_str),
        }
    }
}

/// Complete low-frequency status response for one pooler process.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PoolerSnapshot {
    /// Whether this process may accept new application work.
    pub ready: bool,
    /// Build version of this process.
    pub version: &'static str,
    /// Exact source identity of this process.
    pub git_sha: &'static str,
    /// Current catalog connection and cache state.
    pub catalog: PoolerCatalogSnapshot,
}

/// Compact response used by Kubernetes readiness probes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PoolerReadiness {
    /// Whether this process may accept new application work.
    pub ready: bool,
    /// Exact bounded reason for the readiness decision.
    pub reason: &'static str,
}

/// Cloneable state shared by pooler HTTP handlers.
#[derive(Clone)]
pub struct PoolerState {
    catalog_snapshot: Arc<CatalogSnapshotSource>,
}

impl PoolerState {
    /// Creates state backed by the live catalog supervisor.
    #[must_use]
    pub fn new(catalog: CatalogSupervisorStatus) -> Self {
        Self {
            catalog_snapshot: Arc::new(move || catalog.snapshot().into()),
        }
    }

    /// Returns one coherent point-in-time status response.
    #[must_use]
    pub fn snapshot(&self) -> PoolerSnapshot {
        let catalog = (self.catalog_snapshot)();
        PoolerSnapshot {
            ready: catalog.ready,
            version: pgshard_version::VERSION,
            git_sha: pgshard_version::GIT_SHA,
            catalog,
        }
    }

    /// Returns the current fail-closed readiness decision.
    #[must_use]
    pub fn readiness(&self) -> PoolerReadiness {
        let catalog = (self.catalog_snapshot)();
        PoolerReadiness {
            ready: catalog.ready,
            reason: catalog.readiness_reason,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_catalog(catalog: PoolerCatalogSnapshot) -> Self {
        Self {
            catalog_snapshot: Arc::new(move || catalog.clone()),
        }
    }
}

// Serde's `serialize_with` callback ABI passes the field by reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_u64_decimal<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

// Serde's `serialize_with` callback ABI passes `&Option<T>`.
#[allow(clippy::ref_option)]
fn serialize_optional_u64_decimal<S>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

// Serde's `serialize_with` callback ABI passes `&Option<T>`.
#[allow(clippy::ref_option)]
fn serialize_optional_duration_milliseconds<S>(
    value: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.as_millis().to_string()),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgshard_catalog::{CatalogCache, CatalogSupervisor, CatalogSupervisorConfig};

    #[test]
    fn tracks_live_fail_closed_catalog_status() {
        let supervisor = CatalogSupervisor::new(
            Arc::new(CatalogCache::new()),
            CatalogSupervisorConfig::default(),
        );
        let state = PoolerState::new(supervisor.status());

        let starting = state.snapshot();
        assert!(!starting.ready);
        assert_eq!(starting.catalog.phase, "starting");
        assert_eq!(starting.catalog.readiness_reason, "uninitialized");
        assert!(!starting.catalog.connection_up);

        drop(supervisor);
        let stopped = state.snapshot();
        assert!(!stopped.ready);
        assert_eq!(stopped.catalog.phase, "stopped");
        assert_eq!(stopped.catalog.readiness_reason, "stopped");
    }

    #[test]
    fn status_json_preserves_exact_integer_values() {
        let state = PoolerState::from_catalog(PoolerCatalogSnapshot {
            phase: "backoff",
            connection_up: false,
            ready: true,
            readiness_reason: "serving_stale",
            catalog_epoch: Some(u64::MAX),
            cache_age: Some(Duration::from_millis(1_234)),
            consecutive_failures: u64::MAX,
            total_failures: u64::MAX - 1,
            connect_attempts: u64::MAX - 2,
            successful_connections: u64::MAX - 3,
            last_failure: Some("connection"),
        });

        let value = serde_json::to_value(state.snapshot()).expect("serialize pooler status");
        let catalog = &value["catalog"];
        assert_eq!(catalog["catalog_epoch"], u64::MAX.to_string());
        assert_eq!(catalog["cache_age_milliseconds"], "1234");
        assert_eq!(catalog["consecutive_failures"], u64::MAX.to_string());
        assert_eq!(catalog["total_failures"], (u64::MAX - 1).to_string());
        assert_eq!(catalog["connect_attempts"], (u64::MAX - 2).to_string());
        assert_eq!(
            catalog["successful_connections"],
            (u64::MAX - 3).to_string()
        );
        assert_eq!(state.readiness().reason, "serving_stale");
    }
}
