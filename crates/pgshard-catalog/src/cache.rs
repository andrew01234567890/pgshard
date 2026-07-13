//! Monotonic, lock-free catalog snapshot publication.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use arc_swap::ArcSwap;
use pgshard_types::CatalogEpoch;
use thiserror::Error;
use tokio::time::Instant;

use crate::{CatalogSnapshot, ClusterId, DatabaseId, RoutingHashConfig, SnapshotError};

#[derive(Debug)]
struct CacheState {
    current: Option<Arc<CatalogSnapshot>>,
    snapshots: BTreeMap<u64, Arc<CatalogSnapshot>>,
    minimum_epoch: u64,
}

#[derive(Debug)]
pub(crate) enum InstallBeforeError {
    Cache(CacheError),
    DeadlineElapsed,
}

/// Process-local cache of immutable, checksummed catalog snapshots.
///
/// Reads take one atomic snapshot and do not acquire a mutex. Installs and
/// fences are rare control-plane operations and share a mutex so a snapshot
/// older than an accepted fence can never be published afterward.
pub struct CatalogCache {
    state: ArcSwap<CacheState>,
    write_lock: Mutex<()>,
}

impl Default for CatalogCache {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogCache {
    /// Creates an empty cache with no epoch fence.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: ArcSwap::from_pointee(CacheState {
                current: None,
                snapshots: BTreeMap::new(),
                minimum_epoch: 0,
            }),
            write_lock: Mutex::new(()),
        }
    }

    /// Returns the current snapshot when it is new enough for new planning.
    ///
    /// This request-path operation is lock free. A returned snapshot and its
    /// minimum epoch come from the same atomically published cache state.
    ///
    /// # Errors
    ///
    /// Returns [`RequestEpochError::Uninitialized`] before the first install,
    /// or [`RequestEpochError::SnapshotFenced`] if a refresh is required to
    /// reach the accepted minimum epoch.
    pub fn current_for_planning(&self) -> Result<Arc<CatalogSnapshot>, RequestEpochError> {
        let state = self.state.load();
        let snapshot = state
            .current
            .as_ref()
            .ok_or(RequestEpochError::Uninitialized)?;
        let actual = snapshot.catalog_epoch().0;
        if actual < state.minimum_epoch {
            return Err(RequestEpochError::SnapshotFenced {
                actual,
                minimum: state.minimum_epoch,
            });
        }
        Ok(Arc::clone(snapshot))
    }

    /// Validates an epoch carried by a routed request and returns the snapshot
    /// against which execution must be checked.
    ///
    /// An older installed request epoch remains usable until an explicit fence
    /// advances beyond it. A future or unknown request fails closed until this
    /// process refreshes.
    /// Callers must repeat this check at the execution boundary, not only when
    /// a request first enters a queue.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty cache, a fenced request, a future request,
    /// or a current snapshot that has itself fallen behind the fence.
    pub fn validate_request_epoch(
        &self,
        requested: CatalogEpoch,
    ) -> Result<Arc<CatalogSnapshot>, RequestEpochError> {
        let state = self.state.load();
        if requested.0 < state.minimum_epoch {
            return Err(RequestEpochError::RequestFenced {
                requested: requested.0,
                minimum: state.minimum_epoch,
            });
        }
        let snapshot = state
            .current
            .as_ref()
            .ok_or(RequestEpochError::Uninitialized)?;
        let current = snapshot.catalog_epoch().0;
        if current < state.minimum_epoch {
            return Err(RequestEpochError::SnapshotFenced {
                actual: current,
                minimum: state.minimum_epoch,
            });
        }
        if requested.0 > current {
            return Err(RequestEpochError::Future {
                requested: requested.0,
                current,
            });
        }
        state
            .snapshots
            .get(&requested.0)
            .cloned()
            .ok_or(RequestEpochError::Unavailable {
                requested: requested.0,
            })
    }

    /// Installs a strictly monotonic, internally checksummed snapshot.
    ///
    /// Cluster identity, routing hash configuration, and each surviving
    /// database identity are immutable. Per-database routing, schema, and
    /// authorization epochs may only advance. Replaying the exact current
    /// snapshot is idempotent.
    ///
    /// # Errors
    ///
    /// Rejects corrupt, stale, colliding, identity-changing, or subepoch-
    /// regressing snapshots. A snapshot below the accepted fence also fails.
    pub fn install(&self, candidate: CatalogSnapshot) -> Result<InstallOutcome, CacheError> {
        candidate.verify_checksum()?;
        let candidate = Arc::new(candidate);
        let guard = self
            .write_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.install_locked(candidate, guard, None) {
            Ok(outcome) => Ok(outcome),
            Err(InstallBeforeError::Cache(error)) => Err(error),
            Err(InstallBeforeError::DeadlineElapsed) => {
                unreachable!("an untimed cache install has no deadline")
            }
        }
    }

    pub(crate) async fn install_before(
        &self,
        candidate: CatalogSnapshot,
        deadline: Instant,
    ) -> Result<InstallOutcome, InstallBeforeError> {
        let verification = candidate.verify_checksum();
        ensure_install_deadline(Some(deadline))?;
        verification.map_err(|error| InstallBeforeError::Cache(CacheError::Snapshot(error)))?;
        let candidate = Arc::new(candidate);
        let guard = self.write_lock_before(deadline).await?;
        self.install_locked(candidate, guard, Some(deadline))
    }

    async fn write_lock_before(
        &self,
        deadline: Instant,
    ) -> Result<MutexGuard<'_, ()>, InstallBeforeError> {
        loop {
            ensure_install_deadline(Some(deadline))?;
            match self.write_lock.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(TryLockError::Poisoned(error)) => return Ok(error.into_inner()),
                Err(TryLockError::WouldBlock) => {}
            }
            tokio::task::yield_now().await;
        }
    }

    fn install_locked(
        &self,
        candidate: Arc<CatalogSnapshot>,
        _guard: MutexGuard<'_, ()>,
        deadline: Option<Instant>,
    ) -> Result<InstallOutcome, InstallBeforeError> {
        ensure_install_deadline(deadline)?;
        let state = self.state.load_full();
        let epoch = candidate.catalog_epoch().0;
        if epoch < state.minimum_epoch {
            return reject_install(
                deadline,
                CacheError::BelowFence {
                    candidate: epoch,
                    minimum: state.minimum_epoch,
                },
            );
        }

        if let Some(current) = &state.current {
            if let Err(error) = validate_successor(current, &candidate) {
                return reject_install(deadline, error);
            }
            let current_epoch = current.catalog_epoch().0;
            if epoch == current_epoch {
                if candidate.checksum() == current.checksum() {
                    ensure_install_deadline(deadline)?;
                    return Ok(InstallOutcome::AlreadyCurrent);
                }
                return reject_install(
                    deadline,
                    CacheError::EpochCollision {
                        epoch,
                        current_checksum: current.checksum(),
                        candidate_checksum: candidate.checksum(),
                    },
                );
            }
        }

        let mut snapshots = state.snapshots.clone();
        snapshots.insert(epoch, Arc::clone(&candidate));
        ensure_install_deadline(deadline)?;
        self.state.store(Arc::new(CacheState {
            current: Some(candidate),
            snapshots,
            minimum_epoch: state.minimum_epoch,
        }));
        Ok(InstallOutcome::Installed)
    }

    /// Monotonically rejects request and snapshot epochs below `minimum`.
    ///
    /// The fence and snapshot are published as one atomic cache state. A fence
    /// can therefore make the current snapshot temporarily unusable until a
    /// refresh reaches the minimum.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::ZeroFence`] when `minimum` is zero.
    pub fn fence_before(&self, minimum: CatalogEpoch) -> Result<bool, CacheError> {
        if minimum.0 == 0 {
            return Err(CacheError::ZeroFence);
        }
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = self.state.load_full();
        if minimum.0 <= state.minimum_epoch {
            return Ok(false);
        }
        let mut snapshots = state.snapshots.clone();
        snapshots.retain(|epoch, _| *epoch >= minimum.0);
        self.state.store(Arc::new(CacheState {
            current: state.current.clone(),
            snapshots,
            minimum_epoch: minimum.0,
        }));
        Ok(true)
    }

    /// Decides whether a `PostgreSQL` notification should wake the refresh loop.
    ///
    /// Notifications are hints only. Correctness still requires startup reads
    /// and periodic polling from one repeatable-read catalog transaction.
    #[must_use]
    pub fn refresh_decision(&self, notification: CatalogNotification) -> RefreshDecision {
        let state = self.state.load();
        if notification.epoch.0 < state.minimum_epoch {
            return RefreshDecision::IgnoreBelowFence;
        }
        match &state.current {
            Some(current) if notification.epoch.0 <= current.catalog_epoch().0 => {
                RefreshDecision::IgnoreKnown
            }
            _ => RefreshDecision::Refresh,
        }
    }
}

fn ensure_install_deadline(deadline: Option<Instant>) -> Result<(), InstallBeforeError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(InstallBeforeError::DeadlineElapsed);
    }
    Ok(())
}

fn reject_install<T>(
    deadline: Option<Instant>,
    error: CacheError,
) -> Result<T, InstallBeforeError> {
    ensure_install_deadline(deadline)?;
    Err(InstallBeforeError::Cache(error))
}

fn validate_successor(
    current: &CatalogSnapshot,
    candidate: &CatalogSnapshot,
) -> Result<(), CacheError> {
    if candidate.cluster_id() != current.cluster_id() {
        return Err(CacheError::ClusterChanged {
            current: current.cluster_id(),
            candidate: candidate.cluster_id(),
        });
    }
    if candidate.routing_hash() != current.routing_hash() {
        return Err(CacheError::RoutingHashChanged {
            current: current.routing_hash(),
            candidate: candidate.routing_hash(),
        });
    }
    let current_epoch = current.catalog_epoch().0;
    let candidate_epoch = candidate.catalog_epoch().0;
    if candidate_epoch < current_epoch {
        return Err(CacheError::EpochRegression {
            current: current_epoch,
            candidate: candidate_epoch,
        });
    }
    for previous in current.databases() {
        let Some(next) = candidate.database(previous.id()) else {
            continue;
        };
        if previous.name() != next.name() {
            return Err(CacheError::DatabaseIdentityChanged {
                database_id: previous.id(),
                current_name: previous.name().to_owned(),
                candidate_name: next.name().to_owned(),
            });
        }
        for (kind, old, new) in [
            (
                "routing",
                previous.epochs().routing().0,
                next.epochs().routing().0,
            ),
            ("schema", previous.epochs().schema(), next.epochs().schema()),
            (
                "authorization",
                previous.epochs().authorization(),
                next.epochs().authorization(),
            ),
        ] {
            if new < old {
                return Err(CacheError::DatabaseEpochRegression {
                    database_id: previous.id(),
                    kind,
                    current: old,
                    candidate: new,
                });
            }
        }
    }
    for next in candidate.databases() {
        if let Some(previous) = current.database_by_name(next.name())
            && previous.id() != next.id()
        {
            return Err(CacheError::DatabaseNameRebound {
                database_name: next.name().to_owned(),
                current_id: previous.id(),
                candidate_id: next.id(),
            });
        }
    }
    Ok(())
}

/// Result of publishing a catalog snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallOutcome {
    /// A newer snapshot became current.
    Installed,
    /// The exact snapshot was already current.
    AlreadyCurrent,
}

/// Parsed commit-time `PostgreSQL` catalog notification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogNotification {
    epoch: CatalogEpoch,
}

impl CatalogNotification {
    /// Parses the canonical positive decimal catalog epoch payload.
    ///
    /// # Errors
    ///
    /// Rejects empty, signed, padded, whitespace-containing, non-decimal, and
    /// zero payloads.
    pub fn parse(payload: &str) -> Result<Self, NotificationError> {
        if payload.is_empty()
            || !payload.bytes().all(|byte| byte.is_ascii_digit())
            || (payload.len() > 1 && payload.starts_with('0'))
        {
            return Err(NotificationError::NonCanonical);
        }
        let epoch = payload
            .parse::<u64>()
            .map_err(|_| NotificationError::OutOfRange)?;
        if epoch == 0 {
            return Err(NotificationError::Zero);
        }
        Ok(Self {
            epoch: CatalogEpoch(epoch),
        })
    }

    /// Returns the hinted committed catalog epoch.
    #[must_use]
    pub const fn epoch(self) -> CatalogEpoch {
        self.epoch
    }
}

/// Action for a refresh loop after receiving a notification hint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshDecision {
    /// Read a fresh snapshot from `PostgreSQL`.
    Refresh,
    /// Ignore a duplicate or stale notification already represented locally.
    IgnoreKnown,
    /// Ignore a notification that cannot satisfy the accepted minimum epoch.
    IgnoreBelowFence,
}

/// Catalog cache publication failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CacheError {
    /// The candidate snapshot failed its integrity check.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// A zero epoch cannot form a useful fence.
    #[error("catalog epoch fence must be positive")]
    ZeroFence,
    /// The candidate is older than the accepted minimum epoch.
    #[error("candidate catalog epoch {candidate} is below fence {minimum}")]
    BelowFence {
        /// Candidate epoch.
        candidate: u64,
        /// Accepted minimum epoch.
        minimum: u64,
    },
    /// Cluster identity changed after initialization.
    #[error("cluster identity changed from {current} to {candidate}")]
    ClusterChanged {
        /// Current identity.
        current: ClusterId,
        /// Rejected identity.
        candidate: ClusterId,
    },
    /// Cluster routing-hash contract changed after initialization.
    #[error("routing hash configuration changed")]
    RoutingHashChanged {
        /// Current hash contract.
        current: RoutingHashConfig,
        /// Rejected hash contract.
        candidate: RoutingHashConfig,
    },
    /// The global catalog epoch moved backward.
    #[error("catalog epoch regressed from {current} to {candidate}")]
    EpochRegression {
        /// Current epoch.
        current: u64,
        /// Rejected epoch.
        candidate: u64,
    },
    /// Two different snapshots claim one global catalog epoch.
    #[error("catalog epoch {epoch} has conflicting checksums")]
    EpochCollision {
        /// Colliding epoch.
        epoch: u64,
        /// Current canonical checksum.
        current_checksum: [u8; 32],
        /// Rejected canonical checksum.
        candidate_checksum: [u8; 32],
    },
    /// A stable database ID was rebound to another name.
    #[error("database {database_id} changed identity from {current_name:?} to {candidate_name:?}")]
    DatabaseIdentityChanged {
        /// Stable database identity.
        database_id: DatabaseId,
        /// Current exact name.
        current_name: String,
        /// Rejected exact name.
        candidate_name: String,
    },
    /// An immutable database name was rebound to another stable ID.
    #[error("database name {database_name:?} changed identity from {current_id} to {candidate_id}")]
    DatabaseNameRebound {
        /// Immutable exact database name.
        database_name: String,
        /// Current stable identity.
        current_id: DatabaseId,
        /// Rejected stable identity.
        candidate_id: DatabaseId,
    },
    /// A surviving database subepoch moved backward.
    #[error("database {database_id} {kind} epoch regressed from {current} to {candidate}")]
    DatabaseEpochRegression {
        /// Stable database identity.
        database_id: DatabaseId,
        /// Epoch component.
        kind: &'static str,
        /// Current value.
        current: u64,
        /// Rejected value.
        candidate: u64,
    },
}

/// Request/catalog epoch mismatch.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RequestEpochError {
    /// No snapshot has been installed.
    #[error("catalog cache is uninitialized")]
    Uninitialized,
    /// The request was planned against a retired epoch.
    #[error("request catalog epoch {requested} is below fence {minimum}")]
    RequestFenced {
        /// Request epoch.
        requested: u64,
        /// Accepted minimum epoch.
        minimum: u64,
    },
    /// The current process has not yet observed the request's epoch.
    #[error("request catalog epoch {requested} is ahead of current epoch {current}")]
    Future {
        /// Request epoch.
        requested: u64,
        /// Current cache epoch.
        current: u64,
    },
    /// The requested non-fenced epoch was never installed in this process.
    #[error("catalog epoch {requested} is not available in this process")]
    Unavailable {
        /// Missing request epoch.
        requested: u64,
    },
    /// The current snapshot is older than an accepted fence.
    #[error("current catalog epoch {actual} is below fence {minimum}")]
    SnapshotFenced {
        /// Current snapshot epoch.
        actual: u64,
        /// Accepted minimum epoch.
        minimum: u64,
    },
}

/// Invalid `PostgreSQL` notification payload.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NotificationError {
    /// Payload is not the canonical unsigned decimal representation.
    #[error("catalog notification must be canonical unsigned decimal")]
    NonCanonical,
    /// Payload exceeds the supported epoch range.
    #[error("catalog notification epoch is out of range")]
    OutOfRange,
    /// Epoch zero is reserved.
    #[error("catalog notification epoch must be positive")]
    Zero,
}

#[cfg(test)]
mod tests {
    use std::thread;

    use pgshard_types::{KEYSPACE_END, KeyRange, RoutingEpoch, ShardId};
    use uuid::Uuid;

    use super::*;
    use crate::{DatabaseCatalog, DatabaseEpochs, ShardRoute};

    fn snapshot(cluster: u128, epoch: u64, database_epoch: u64) -> CatalogSnapshot {
        snapshot_with_database(cluster, epoch, 2, "app", database_epoch)
    }

    fn snapshot_with_database(
        cluster: u128,
        epoch: u64,
        database_id: u128,
        database_name: &str,
        database_epoch: u64,
    ) -> CatalogSnapshot {
        let database = DatabaseCatalog::new(
            DatabaseId::new(Uuid::from_u128(database_id)).expect("database ID"),
            database_name,
            DatabaseEpochs::new(database_epoch, database_epoch, database_epoch)
                .expect("database epochs"),
            vec![ShardRoute::new(
                ShardId(0),
                KeyRange::new(0, KEYSPACE_END).expect("full key range"),
            )],
            vec![],
        )
        .expect("database");
        CatalogSnapshot::new(
            ClusterId::new(Uuid::from_u128(cluster)).expect("cluster ID"),
            epoch,
            RoutingHashConfig::new(1, 42).expect("hash configuration"),
            vec![database],
        )
        .expect("snapshot")
    }

    #[test]
    fn installs_monotonically_and_replay_is_idempotent() {
        let cache = CatalogCache::new();
        assert_eq!(
            cache.install(snapshot(1, 1, 1)).expect("first install"),
            InstallOutcome::Installed
        );
        assert_eq!(
            cache.install(snapshot(1, 1, 1)).expect("replay"),
            InstallOutcome::AlreadyCurrent
        );
        assert_eq!(
            cache.install(snapshot(1, 2, 2)).expect("advance"),
            InstallOutcome::Installed
        );
        assert_eq!(
            cache
                .current_for_planning()
                .expect("current")
                .catalog_epoch(),
            CatalogEpoch(2)
        );
        assert!(matches!(
            cache.install(snapshot(1, 1, 1)),
            Err(CacheError::EpochRegression { .. })
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timed_install_never_publishes_after_lock_deadline() {
        let cache = Arc::new(CatalogCache::new());
        cache.install(snapshot(1, 1, 1)).expect("initial install");
        let lock_cache = Arc::clone(&cache);
        let (locked_sender, locked_receiver) = std::sync::mpsc::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let holder = thread::spawn(move || {
            let _guard = lock_cache
                .write_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locked_sender.send(()).expect("report held cache lock");
            let _ = release_receiver.recv();
        });
        locked_receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("cache lock held within one second");

        let result = cache
            .install_before(
                snapshot(1, 2, 2),
                Instant::now() + std::time::Duration::from_millis(50),
            )
            .await;
        assert!(matches!(result, Err(InstallBeforeError::DeadlineElapsed)));
        assert_eq!(
            cache
                .current_for_planning()
                .expect("last validated snapshot")
                .catalog_epoch(),
            CatalogEpoch(1)
        );

        release_sender.send(()).expect("release cache lock");
        holder.join().expect("cache lock holder");
    }

    #[test]
    fn rejects_identity_hash_collision_and_subepoch_regression() {
        let cache = CatalogCache::new();
        cache.install(snapshot(1, 2, 2)).expect("initial");
        assert!(matches!(
            cache.install(snapshot(3, 3, 3)),
            Err(CacheError::ClusterChanged { .. })
        ));

        let collision = CatalogSnapshot::new(
            ClusterId::new(Uuid::from_u128(1)).expect("cluster"),
            2,
            RoutingHashConfig::new(1, 42).expect("hash"),
            vec![],
        )
        .expect("collision");
        assert!(matches!(
            cache.install(collision),
            Err(CacheError::EpochCollision { .. })
        ));
        assert!(matches!(
            cache.install(snapshot(1, 3, 1)),
            Err(CacheError::DatabaseEpochRegression { .. })
        ));
        assert!(matches!(
            cache.install(snapshot_with_database(1, 3, 4, "app", 3)),
            Err(CacheError::DatabaseNameRebound { .. })
        ));

        let changed_hash = CatalogSnapshot::new(
            ClusterId::new(Uuid::from_u128(1)).expect("cluster"),
            3,
            RoutingHashConfig::new(1, 99).expect("hash"),
            vec![],
        )
        .expect("changed hash");
        assert!(matches!(
            cache.install(changed_hash),
            Err(CacheError::RoutingHashChanged { .. })
        ));
    }

    #[test]
    fn fences_requests_and_snapshot_publication_atomically() {
        let cache = CatalogCache::new();
        cache.install(snapshot(1, 2, 2)).expect("initial");
        assert!(cache.fence_before(CatalogEpoch(3)).expect("fence"));
        assert!(matches!(
            cache.current_for_planning(),
            Err(RequestEpochError::SnapshotFenced { .. })
        ));
        assert!(matches!(
            cache.validate_request_epoch(CatalogEpoch(2)),
            Err(RequestEpochError::RequestFenced { .. })
        ));
        assert!(matches!(
            cache.install(snapshot(1, 2, 2)),
            Err(CacheError::BelowFence { .. })
        ));
        cache.install(snapshot(1, 3, 3)).expect("refresh");
        assert!(cache.validate_request_epoch(CatalogEpoch(3)).is_ok());
        assert!(!cache.fence_before(CatalogEpoch(2)).expect("stale fence"));
        assert!(matches!(
            cache.validate_request_epoch(CatalogEpoch(4)),
            Err(RequestEpochError::Future { .. })
        ));
    }

    #[test]
    fn retains_exact_request_snapshot_until_fenced() {
        let cache = CatalogCache::new();
        cache.install(snapshot(1, 2, 2)).expect("epoch two");
        cache.install(snapshot(1, 3, 3)).expect("epoch three");
        let planned = cache
            .validate_request_epoch(CatalogEpoch(2))
            .expect("retained epoch");
        assert_eq!(planned.catalog_epoch(), CatalogEpoch(2));
        assert_eq!(
            cache
                .current_for_planning()
                .expect("current")
                .catalog_epoch(),
            CatalogEpoch(3)
        );
        assert!(matches!(
            cache.validate_request_epoch(CatalogEpoch(1)),
            Err(RequestEpochError::Unavailable { requested: 1 })
        ));
        cache.fence_before(CatalogEpoch(3)).expect("fence");
        assert!(matches!(
            cache.validate_request_epoch(CatalogEpoch(2)),
            Err(RequestEpochError::RequestFenced { .. })
        ));
    }

    #[test]
    fn parses_only_canonical_notification_epochs() {
        assert_eq!(
            CatalogNotification::parse("42")
                .expect("notification")
                .epoch(),
            CatalogEpoch(42)
        );
        for invalid in ["", "0", "00", "01", "+1", "-1", " 1", "1\n", "x"] {
            assert!(CatalogNotification::parse(invalid).is_err(), "{invalid:?}");
        }
        assert_eq!(
            CatalogNotification::parse("18446744073709551616"),
            Err(NotificationError::OutOfRange)
        );
    }

    #[test]
    fn refreshes_only_for_unseen_usable_notifications() {
        let cache = CatalogCache::new();
        assert_eq!(
            cache.refresh_decision(CatalogNotification::parse("1").expect("notification")),
            RefreshDecision::Refresh
        );
        cache.install(snapshot(1, 2, 2)).expect("initial");
        assert_eq!(
            cache.refresh_decision(CatalogNotification::parse("2").expect("notification")),
            RefreshDecision::IgnoreKnown
        );
        assert_eq!(
            cache.refresh_decision(CatalogNotification::parse("3").expect("notification")),
            RefreshDecision::Refresh
        );
        cache.fence_before(CatalogEpoch(4)).expect("fence");
        assert_eq!(
            cache.refresh_decision(CatalogNotification::parse("3").expect("notification")),
            RefreshDecision::IgnoreBelowFence
        );
    }

    #[test]
    fn concurrent_reads_observe_whole_snapshots() {
        let cache = Arc::new(CatalogCache::new());
        cache.install(snapshot(1, 1, 1)).expect("initial");
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for _ in 0..2_000 {
                        let value = cache.current_for_planning().expect("current");
                        assert!((1..=100).contains(&value.catalog_epoch().0));
                        value.verify_checksum().expect("whole snapshot");
                    }
                })
            })
            .collect();
        for epoch in 2..=100 {
            cache
                .install(snapshot(1, epoch, epoch))
                .expect("monotonic install");
        }
        for reader in readers {
            reader.join().expect("reader");
        }
    }

    #[test]
    fn public_epoch_types_are_used_without_reinterpretation() {
        let epochs = DatabaseEpochs::new(7, 8, 9).expect("epochs");
        assert_eq!(epochs.routing(), RoutingEpoch(7));
    }
}
