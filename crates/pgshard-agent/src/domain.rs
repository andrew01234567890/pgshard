//! Fail-closed `PostgreSQL` identity, observation, and fencing state.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pgshard_types::{PgLsn, ShardId};
use serde::{Serialize, Serializer};
use thiserror::Error;

/// Maximum age of a role/fence observation that can authorize readiness.
pub const POSTGRES_OBSERVATION_MAX_AGE_MS: u64 = 5_000;

/// Stable identity assigned by the operator.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AgentIdentity {
    /// Cluster containing this instance.
    pub cluster_id: String,
    /// Shard containing this instance.
    pub shard_id: ShardId,
    /// Stable `PostgreSQL` instance identity.
    pub instance_id: String,
}

/// Observed `PostgreSQL` role.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresRole {
    /// The role has not been established safely.
    #[default]
    Unknown,
    /// `PostgreSQL` is accepting writes for the current fencing term.
    Primary,
    /// `PostgreSQL` is replaying WAL from a primary.
    Replica,
}

/// Locally supervised postmaster process state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresProcessState {
    /// Process supervision was not requested.
    #[default]
    Disabled,
    /// Required `PostgreSQL` 18 files passed structural offline preflight.
    Validated,
    /// The postmaster is starting without a network listener.
    StartingQuarantined,
    /// The postmaster process exists without a TCP listener; SQL readiness is not implied.
    RunningQuarantined,
    /// A bounded postmaster shutdown is in progress.
    Stopping,
    /// Target-side fencing completed; crash recovery is required before reuse.
    Fenced,
    /// Startup, supervision, or shutdown failed terminally.
    Failed,
}

/// Last locally verified `PostgreSQL` state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PostgresObservation {
    /// Role established by `PostgreSQL` inspection.
    pub role: PostgresRole,
    /// Current `PostgreSQL` timeline.
    pub timeline: u32,
    /// Durable fencing epoch observed inside `PostgreSQL`.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub fencing_epoch: u64,
    /// Local Unix time when all observation fields were read coherently.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub observed_at_unix_ms: u64,
    /// Last locally flushed WAL position, when applicable.
    #[serde(serialize_with = "serialize_optional_lsn")]
    pub flush_lsn: Option<PgLsn>,
    /// Last replayed WAL position, when applicable.
    #[serde(serialize_with = "serialize_optional_lsn")]
    pub replay_lsn: Option<PgLsn>,
}

/// Lease proving this instance belongs to the current fencing term.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FencingLease {
    /// Instance authorized by the lease.
    pub owner_instance: String,
    /// Strictly positive fencing epoch.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub epoch: u64,
    /// Lease expiration as Unix time in milliseconds for status reporting.
    /// Live authority is bounded independently by a local monotonic deadline.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub valid_until_unix_ms: u64,
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
fn serialize_optional_lsn<S>(value: &Option<PgLsn>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(PgLsn(lsn)) => serializer.serialize_some(&lsn.to_string()),
        None => serializer.serialize_none(),
    }
}

/// Externally reportable agent state.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct AgentSnapshot {
    /// Operator-assigned identity, absent until established.
    pub identity: Option<AgentIdentity>,
    /// Last `PostgreSQL` observation.
    pub postgres: Option<PostgresObservation>,
    /// Current local postmaster process state.
    pub postgres_process: PostgresProcessState,
    /// Current fencing lease.
    pub lease: Option<FencingLease>,
}

/// Thread-safe state shared by reconciliation and HTTP handlers.
#[derive(Clone, Debug, Default)]
pub struct AgentState {
    inner: Arc<RwLock<AgentInner>>,
    last_checked_unix_ms: Arc<AtomicU64>,
    highest_lease_epoch: Arc<AtomicU64>,
    max_lease_ttl_ms: u64,
}

#[derive(Debug, Default)]
struct AgentInner {
    snapshot: AgentSnapshot,
    lease_deadline: Option<LeaseDeadline>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LeaseDeadline {
    epoch: u64,
    expires_at: Instant,
}

impl AgentState {
    /// Creates state with the operator-assigned identity but no assumed lease or
    /// `PostgreSQL` role.
    /// # Errors
    ///
    /// Returns [`LeaseInstallError::InvalidMaximumLeaseTtl`] for a zero or
    /// unbounded policy.
    pub fn with_identity(
        identity: AgentIdentity,
        max_lease_ttl_ms: u64,
    ) -> Result<Self, LeaseInstallError> {
        if !(1..=300_000).contains(&max_lease_ttl_ms) {
            return Err(LeaseInstallError::InvalidMaximumLeaseTtl(max_lease_ttl_ms));
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(AgentInner {
                snapshot: AgentSnapshot {
                    identity: Some(identity),
                    ..AgentSnapshot::default()
                },
                lease_deadline: None,
            })),
            last_checked_unix_ms: Arc::new(AtomicU64::new(0)),
            highest_lease_epoch: Arc::new(AtomicU64::new(0)),
            max_lease_ttl_ms,
        })
    }

    /// Returns a consistent state snapshot.
    #[must_use]
    pub fn snapshot(&self) -> AgentSnapshot {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
            .clone()
    }

    /// Replaces the locally verified `PostgreSQL` observation.
    pub fn set_postgres(&self, observation: PostgresObservation) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
            .postgres = Some(observation);
    }

    /// Replaces the locally supervised postmaster process state.
    pub fn set_postgres_process(&self, process: PostgresProcessState) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
            .postgres_process = process;
    }

    /// Installs or renews a lease already authenticated by the orchestrator
    /// client. Epochs never move backwards, and clearing a lease revokes that
    /// term so a delayed copy cannot reauthorize the instance.
    ///
    /// # Errors
    ///
    /// Returns an error for the wrong instance, a reserved or stale epoch, or a
    /// renewal that shortens the existing monotonic authorization. Wall-clock
    /// expiry is status-only and is clamped when the system clock moves
    /// backwards.
    pub fn install_lease(
        &self,
        lease: FencingLease,
        now_unix_ms: u64,
    ) -> Result<LeaseInstallOutcome, LeaseInstallError> {
        let now = Instant::now();
        self.install_lease_at(lease, now_unix_ms, now, now)
    }

    /// Installs authority whose monotonic validity window began at
    /// `valid_from`. Callers performing a remote compare-and-swap must capture
    /// that instant before dispatch and pass the later `observed_at` instant so
    /// response latency consumes rather than extends the lease.
    pub(crate) fn install_lease_at(
        &self,
        mut lease: FencingLease,
        valid_from_unix_ms: u64,
        valid_from: Instant,
        observed_at: Instant,
    ) -> Result<LeaseInstallOutcome, LeaseInstallError> {
        let Some(ttl_ms) = lease.valid_until_unix_ms.checked_sub(valid_from_unix_ms) else {
            return Err(LeaseInstallError::Expired);
        };
        if ttl_ms == 0 {
            return Err(LeaseInstallError::Expired);
        }
        if ttl_ms > self.max_lease_ttl_ms {
            return Err(LeaseInstallError::LeaseTtlExceeded {
                requested_ms: ttl_ms,
                maximum_ms: self.max_lease_ttl_ms,
            });
        }
        let expires_at = valid_from
            .checked_add(Duration::from_millis(ttl_ms))
            .ok_or(LeaseInstallError::DeadlineOverflow)?;
        if expires_at <= observed_at {
            return Err(LeaseInstallError::Expired);
        }

        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let identity = inner
            .snapshot
            .identity
            .as_ref()
            .ok_or(LeaseInstallError::IdentityMissing)?;
        if identity.instance_id != lease.owner_instance {
            return Err(LeaseInstallError::OwnerMismatch {
                expected: identity.instance_id.clone(),
                requested: lease.owner_instance,
            });
        }
        if lease.epoch == 0 || lease.epoch == u64::MAX {
            return Err(LeaseInstallError::ReservedEpoch(lease.epoch));
        }

        let highest = self.highest_lease_epoch.load(Ordering::Acquire);
        if lease.epoch < highest || (lease.epoch == highest && inner.snapshot.lease.is_none()) {
            return Err(LeaseInstallError::StaleEpoch {
                requested: lease.epoch,
                minimum: highest.saturating_add(1),
            });
        }
        if let Some(current) = inner.snapshot.lease.as_ref()
            && lease.epoch == current.epoch
        {
            let current_deadline = inner
                .lease_deadline
                .filter(|deadline| deadline.epoch == current.epoch)
                .ok_or(LeaseInstallError::DeadlineMissing)?;
            if expires_at < current_deadline.expires_at {
                return Err(LeaseInstallError::RegressiveDeadline);
            }
            lease.valid_until_unix_ms = lease.valid_until_unix_ms.max(current.valid_until_unix_ms);
            if lease.valid_until_unix_ms == current.valid_until_unix_ms
                && expires_at == current_deadline.expires_at
            {
                drop(inner);
                return Ok(LeaseInstallOutcome::Existing);
            }
            inner.snapshot.lease = Some(lease.clone());
            inner.lease_deadline = Some(LeaseDeadline {
                epoch: lease.epoch,
                expires_at,
            });
            drop(inner);
            return Ok(LeaseInstallOutcome::Renewed);
        }

        self.highest_lease_epoch
            .store(lease.epoch, Ordering::Release);
        inner.snapshot.lease = Some(lease.clone());
        inner.lease_deadline = Some(LeaseDeadline {
            epoch: lease.epoch,
            expires_at,
        });
        drop(inner);
        Ok(LeaseInstallOutcome::Installed)
    }

    /// Removes shared observable Lease evidence immediately.
    ///
    /// The composed writable supervisor revokes its separate attempt-private
    /// process authority before calling this method.
    pub fn clear_lease(&self) {
        {
            let mut inner = self
                .inner
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inner.snapshot.lease = None;
            inner.lease_deadline = None;
        }
    }

    /// Evaluates readiness against the current wall and monotonic clocks.
    #[must_use]
    pub fn readiness(&self) -> Readiness {
        self.readiness_at(unix_time_ms(), Instant::now())
    }

    /// Evaluates deterministic readiness at supplied wall and monotonic times.
    #[must_use]
    pub fn readiness_at(&self, now_unix_ms: u64, now: Instant) -> Readiness {
        let previous = self
            .last_checked_unix_ms
            .fetch_max(now_unix_ms, Ordering::AcqRel);
        let inner = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        evaluate_readiness(
            &inner.snapshot,
            inner.lease_deadline,
            previous.max(now_unix_ms),
            now,
        )
    }

    #[cfg(test)]
    pub(crate) fn lease_deadline(&self) -> Option<Instant> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lease_deadline
            .map(|deadline| deadline.expires_at)
    }
}

/// Result of installing authenticated lease state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseInstallOutcome {
    /// A strictly newer fencing term was installed.
    Installed,
    /// The identical lease was already installed.
    Existing,
    /// The current term received a later expiration.
    Renewed,
}

/// Rejected lease state that could otherwise weaken fencing.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LeaseInstallError {
    /// The authority was constructed with an unsafe lease policy.
    #[error("maximum lease TTL {0} ms must be between 1 and 300000 ms")]
    InvalidMaximumLeaseTtl(u64),
    /// No stable instance identity is available for owner validation.
    #[error("agent identity is missing")]
    IdentityMissing,
    /// The lease belongs to another instance.
    #[error("lease owner {requested:?} does not match this instance {expected:?}")]
    OwnerMismatch {
        /// Configured instance identity.
        expected: String,
        /// Requested owner.
        requested: String,
    },
    /// Zero and the maximum epoch cannot safely authorize a term.
    #[error("fencing epoch {0} is reserved")]
    ReservedEpoch(u64),
    /// The lease is already expired at the authenticated receive time.
    #[error("fencing lease is expired")]
    Expired,
    /// Lease lifetime exceeds the configured safety policy.
    #[error("requested lease TTL {requested_ms} ms exceeds maximum {maximum_ms} ms")]
    LeaseTtlExceeded {
        /// Requested duration.
        requested_ms: u64,
        /// Configured maximum.
        maximum_ms: u64,
    },
    /// A delayed lease attempted to restore an old or explicitly cleared term.
    #[error("stale fencing epoch {requested}; next epoch must be at least {minimum}")]
    StaleEpoch {
        /// Rejected epoch.
        requested: u64,
        /// Minimum safe next term.
        minimum: u64,
    },
    /// A monotonic lease deadline could not be represented.
    #[error("fencing lease monotonic deadline overflowed")]
    DeadlineOverflow,
    /// Reported lease state exists without its matching monotonic authority.
    #[error("fencing lease has no matching monotonic deadline")]
    DeadlineMissing,
    /// A renewal cannot move the monotonic deadline backwards.
    #[error("lease renewal cannot shorten its monotonic deadline")]
    RegressiveDeadline,
}

/// Machine-readable reason for accepting or rejecting traffic.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessReason {
    /// Every required observation is present and current.
    Ready,
    /// No operator identity has been established.
    IdentityMissing,
    /// No fencing lease is installed.
    LeaseMissing,
    /// The local postmaster is intentionally unavailable to routed traffic.
    PostgresQuarantined,
    /// The local postmaster was target-fenced and requires restart recovery.
    PostgresFenced,
    /// Local postmaster validation, startup, supervision, or shutdown failed.
    PostgresFailed,
    /// The lease belongs to another instance.
    LeaseOwnerMismatch,
    /// Epoch zero can never authorize an instance.
    LeaseEpochInvalid,
    /// Reported lease state has no matching local monotonic authority.
    LeaseDeadlineMissing,
    /// The installed lease is no longer valid.
    LeaseExpired,
    /// `PostgreSQL` has not been inspected successfully.
    PostgresUnobserved,
    /// The observation timestamp is absent or lies ahead of the safe local time.
    PostgresObservationTimeInvalid,
    /// The last coherent `PostgreSQL` observation is too old for routing.
    PostgresObservationStale,
    /// `PostgreSQL` role is unknown.
    PostgresRoleUnknown,
    /// `PostgreSQL` did not report a valid timeline.
    TimelineInvalid,
    /// The lease and durable `PostgreSQL` fence describe different terms.
    FencingEpochMismatch,
    /// The required WAL location for the observed role is absent.
    LsnMissing,
}

/// Readiness response returned by the agent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct Readiness {
    /// Whether the agent is safe to include in its current service role.
    pub ready: bool,
    /// Exact reason for the decision.
    pub reason: ReadinessReason,
}

fn evaluate_readiness(
    snapshot: &AgentSnapshot,
    lease_deadline: Option<LeaseDeadline>,
    now_unix_ms: u64,
    now: Instant,
) -> Readiness {
    let reason = match (
        &snapshot.identity,
        snapshot.postgres_process,
        &snapshot.lease,
        lease_deadline,
        &snapshot.postgres,
    ) {
        (_, PostgresProcessState::Failed, _, _, _) => ReadinessReason::PostgresFailed,
        (_, PostgresProcessState::Fenced, _, _, _) => ReadinessReason::PostgresFenced,
        (
            Some(_),
            PostgresProcessState::Validated
            | PostgresProcessState::StartingQuarantined
            | PostgresProcessState::RunningQuarantined
            | PostgresProcessState::Stopping,
            _,
            _,
            _,
        ) => ReadinessReason::PostgresQuarantined,
        (None, _, _, _, _) => ReadinessReason::IdentityMissing,
        (Some(_), _, None, _, _) => ReadinessReason::LeaseMissing,
        (Some(identity), _, Some(lease), _, _) if identity.instance_id != lease.owner_instance => {
            ReadinessReason::LeaseOwnerMismatch
        }
        (_, _, Some(lease), _, _) if lease.epoch == 0 => ReadinessReason::LeaseEpochInvalid,
        (_, _, Some(_), None, _) => ReadinessReason::LeaseDeadlineMissing,
        (_, _, Some(lease), Some(deadline), _) if lease.epoch != deadline.epoch => {
            ReadinessReason::LeaseDeadlineMissing
        }
        (_, _, Some(_), Some(deadline), _) if deadline.expires_at <= now => {
            ReadinessReason::LeaseExpired
        }
        (_, _, _, _, None) => ReadinessReason::PostgresUnobserved,
        (_, _, _, _, Some(postgres))
            if postgres.observed_at_unix_ms == 0 || postgres.observed_at_unix_ms > now_unix_ms =>
        {
            ReadinessReason::PostgresObservationTimeInvalid
        }
        (_, _, _, _, Some(postgres))
            if now_unix_ms - postgres.observed_at_unix_ms > POSTGRES_OBSERVATION_MAX_AGE_MS =>
        {
            ReadinessReason::PostgresObservationStale
        }
        (_, _, _, _, Some(postgres)) if postgres.role == PostgresRole::Unknown => {
            ReadinessReason::PostgresRoleUnknown
        }
        (_, _, _, _, Some(postgres)) if postgres.timeline == 0 => ReadinessReason::TimelineInvalid,
        (_, _, Some(lease), _, Some(postgres)) if postgres.fencing_epoch != lease.epoch => {
            ReadinessReason::FencingEpochMismatch
        }
        (_, _, _, _, Some(postgres))
            if (postgres.role == PostgresRole::Primary && postgres.flush_lsn.is_none())
                || (postgres.role == PostgresRole::Replica && postgres.replay_lsn.is_none()) =>
        {
            ReadinessReason::LsnMissing
        }
        _ => ReadinessReason::Ready,
    };
    Readiness {
        ready: reason == ReadinessReason::Ready,
        reason,
    }
}

fn unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> AgentIdentity {
        AgentIdentity {
            cluster_id: "cluster-1".to_owned(),
            shard_id: ShardId(2),
            instance_id: "instance-1".to_owned(),
        }
    }

    fn primary() -> PostgresObservation {
        PostgresObservation {
            role: PostgresRole::Primary,
            timeline: 4,
            fencing_epoch: 3,
            observed_at_unix_ms: 100,
            flush_lsn: Some(PgLsn(100)),
            replay_lsn: None,
        }
    }

    fn state() -> AgentState {
        AgentState::with_identity(identity(), 10_000).expect("valid lease policy")
    }

    fn install(
        state: &AgentState,
        lease: FencingLease,
        valid_from_unix_ms: u64,
        valid_from: Instant,
    ) -> Result<LeaseInstallOutcome, LeaseInstallError> {
        state.install_lease_at(lease, valid_from_unix_ms, valid_from, valid_from)
    }

    #[test]
    fn readiness_fails_closed_without_identity_or_lease() {
        let now = Instant::now();
        assert_eq!(
            AgentState::default().readiness_at(100, now).reason,
            ReadinessReason::IdentityMissing
        );
        assert_eq!(
            state().readiness_at(100, now).reason,
            ReadinessReason::LeaseMissing
        );
    }

    #[test]
    fn readiness_rejects_wrong_owner_and_expired_lease() {
        let state = state();
        let now = Instant::now();
        state.set_postgres(primary());
        assert!(matches!(
            install(
                &state,
                FencingLease {
                    owner_instance: "someone-else".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 200,
                },
                100,
                now,
            ),
            Err(LeaseInstallError::OwnerMismatch { .. })
        ));
        assert_eq!(
            state.readiness_at(100, now).reason,
            ReadinessReason::LeaseMissing
        );

        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: 3,
                valid_until_unix_ms: 100,
            },
            99,
            now,
        )
        .expect("install expired fixture");
        assert_eq!(
            state
                .readiness_at(100, now + Duration::from_millis(1))
                .reason,
            ReadinessReason::LeaseExpired
        );
    }

    #[test]
    fn readiness_requires_role_specific_lsn() {
        let state = state();
        let now = Instant::now();
        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: 3,
                valid_until_unix_ms: 200,
            },
            100,
            now,
        )
        .expect("install lease");
        state.set_postgres(PostgresObservation {
            role: PostgresRole::Replica,
            timeline: 4,
            fencing_epoch: 3,
            observed_at_unix_ms: 100,
            flush_lsn: Some(PgLsn(100)),
            replay_lsn: None,
        });
        assert_eq!(
            state.readiness_at(100, now).reason,
            ReadinessReason::LsnMissing
        );
    }

    #[test]
    fn readiness_accepts_current_matching_fence() {
        let state = state();
        let now = Instant::now();
        state.set_postgres(primary());
        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: 3,
                valid_until_unix_ms: 200,
            },
            100,
            now,
        )
        .expect("install lease");
        assert_eq!(
            state.readiness_at(100, now),
            Readiness {
                ready: true,
                reason: ReadinessReason::Ready,
            }
        );
    }

    #[test]
    fn quarantine_process_state_overrides_valid_authority() {
        let state = state();
        let now = Instant::now();
        state.set_postgres(primary());
        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: 3,
                valid_until_unix_ms: 200,
            },
            100,
            now,
        )
        .expect("install matching lease");
        state.set_postgres_process(PostgresProcessState::RunningQuarantined);

        assert_eq!(
            state.readiness_at(100, now).reason,
            ReadinessReason::PostgresQuarantined
        );
        state.set_postgres_process(PostgresProcessState::Disabled);
        assert_eq!(state.readiness_at(100, now).reason, ReadinessReason::Ready);

        state.set_postgres_process(PostgresProcessState::Failed);
        assert_eq!(
            state.readiness_at(100, now).reason,
            ReadinessReason::PostgresFailed
        );

        state.set_postgres_process(PostgresProcessState::Fenced);
        assert_eq!(
            state.readiness_at(100, now).reason,
            ReadinessReason::PostgresFenced
        );
    }

    #[test]
    fn readiness_rejects_invalid_and_stale_observation_time() {
        let state = state();
        let now = Instant::now();
        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: 3,
                valid_until_unix_ms: 10_000,
            },
            100,
            now,
        )
        .expect("install lease");
        let mut observation = primary();
        observation.observed_at_unix_ms = 0;
        state.set_postgres(observation);
        assert_eq!(
            state.readiness_at(100, now).reason,
            ReadinessReason::PostgresObservationTimeInvalid
        );

        state.set_postgres(primary());
        assert_eq!(
            state
                .readiness_at(100 + POSTGRES_OBSERVATION_MAX_AGE_MS + 1, now)
                .reason,
            ReadinessReason::PostgresObservationStale
        );
    }

    #[test]
    fn readiness_rejects_a_stale_postgres_fence() {
        let state = state();
        let now = Instant::now();
        state.set_postgres(primary());
        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: 4,
                valid_until_unix_ms: 300,
            },
            100,
            now,
        )
        .expect("install lease");
        assert_eq!(
            state.readiness_at(100, now).reason,
            ReadinessReason::FencingEpochMismatch
        );
    }

    #[test]
    fn clock_rollback_cannot_revive_an_expired_lease() {
        let state = state();
        let now = Instant::now();
        state.set_postgres(primary());
        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: 3,
                valid_until_unix_ms: 200,
            },
            100,
            now,
        )
        .expect("install lease");
        assert!(
            state
                .readiness_at(199, now + Duration::from_millis(99))
                .ready
        );
        assert_eq!(
            state
                .readiness_at(200, now + Duration::from_millis(100))
                .reason,
            ReadinessReason::LeaseExpired
        );
        assert_eq!(
            state
                .readiness_at(150, now + Duration::from_millis(100))
                .reason,
            ReadinessReason::LeaseExpired
        );
    }

    #[test]
    fn wall_clock_jump_cannot_expire_live_monotonic_authority() {
        let state = state();
        let now = Instant::now();
        state.set_postgres(primary());
        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: 3,
                valid_until_unix_ms: 200,
            },
            100,
            now,
        )
        .expect("install lease");

        assert_eq!(
            state.readiness_at(300, now + Duration::from_millis(50)),
            Readiness {
                ready: true,
                reason: ReadinessReason::Ready,
            }
        );
    }

    #[test]
    fn delayed_install_consumes_authority_window() {
        let state = state();
        let valid_from = Instant::now();
        let lease = FencingLease {
            owner_instance: "instance-1".to_owned(),
            epoch: 3,
            valid_until_unix_ms: 200,
        };
        assert_eq!(
            state.install_lease_at(
                lease.clone(),
                100,
                valid_from,
                valid_from + Duration::from_millis(99),
            ),
            Ok(LeaseInstallOutcome::Installed)
        );
        assert_eq!(
            state.lease_deadline(),
            Some(valid_from + Duration::from_millis(100))
        );
        assert_eq!(
            state.install_lease_at(
                FencingLease { epoch: 4, ..lease },
                100,
                valid_from,
                valid_from + Duration::from_millis(100),
            ),
            Err(LeaseInstallError::Expired)
        );
    }

    #[test]
    fn renewal_cannot_regress_monotonic_deadline() {
        let state = state();
        let now = Instant::now();
        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: 3,
                valid_until_unix_ms: 300,
            },
            100,
            now,
        )
        .expect("install lease");

        assert_eq!(
            install(
                &state,
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 301,
                },
                200,
                now,
            ),
            Err(LeaseInstallError::RegressiveDeadline)
        );
    }

    #[test]
    fn renewal_clamps_status_expiry_when_wall_clock_moves_backwards() {
        let state = state();
        let initial_valid_from = Instant::now();
        assert_eq!(
            state.install_lease_at(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 7_000,
                },
                1_000,
                initial_valid_from,
                initial_valid_from,
            ),
            Ok(LeaseInstallOutcome::Installed)
        );

        let later_valid_from = initial_valid_from + Duration::from_secs(1);
        assert_eq!(
            state.install_lease_at(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 6_500,
                },
                500,
                later_valid_from,
                later_valid_from,
            ),
            Ok(LeaseInstallOutcome::Renewed)
        );
        assert_eq!(
            state
                .snapshot()
                .lease
                .map(|lease| lease.valid_until_unix_ms),
            Some(7_000)
        );
        assert_eq!(
            state.lease_deadline(),
            Some(initial_valid_from + Duration::from_secs(7))
        );
    }

    #[test]
    fn status_json_uses_exact_decimal_strings() {
        let state = state();
        let now = Instant::now();
        state.set_postgres(primary());
        install(
            &state,
            FencingLease {
                owner_instance: "instance-1".to_owned(),
                epoch: u64::MAX - 1,
                valid_until_unix_ms: 200,
            },
            100,
            now,
        )
        .expect("install large exact epoch");
        let json = serde_json::to_value(state.snapshot()).expect("serialize status");
        assert_eq!(json["lease"]["epoch"], (u64::MAX - 1).to_string());
        assert_eq!(json["lease"]["valid_until_unix_ms"], "200");
        assert_eq!(json["postgres"]["flush_lsn"], "100");
        assert_eq!(json["postgres"]["observed_at_unix_ms"], "100");
    }

    #[test]
    fn lease_terms_are_monotonic_and_clear_revokes_the_term() {
        let state = state();
        let now = Instant::now();
        let lease = FencingLease {
            owner_instance: "instance-1".to_owned(),
            epoch: 7,
            valid_until_unix_ms: 200,
        };
        assert_eq!(
            install(&state, lease.clone(), 100, now),
            Ok(LeaseInstallOutcome::Installed)
        );
        assert_eq!(
            install(&state, lease.clone(), 100, now),
            Ok(LeaseInstallOutcome::Existing)
        );
        state.clear_lease();
        assert!(matches!(
            install(&state, lease, 100, now),
            Err(LeaseInstallError::StaleEpoch { .. })
        ));
        assert_eq!(
            install(
                &state,
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 8,
                    valid_until_unix_ms: 300,
                },
                200,
                now,
            ),
            Ok(LeaseInstallOutcome::Installed)
        );
    }

    #[test]
    fn lease_policy_rejects_expired_overlong_and_invalid_limits() {
        assert!(matches!(
            AgentState::with_identity(identity(), 0),
            Err(LeaseInstallError::InvalidMaximumLeaseTtl(0))
        ));
        let state = AgentState::with_identity(identity(), 100).expect("lease policy");
        let lease = |valid_until_unix_ms| FencingLease {
            owner_instance: "instance-1".to_owned(),
            epoch: 3,
            valid_until_unix_ms,
        };
        let now = Instant::now();
        assert_eq!(
            install(&state, lease(100), 100, now),
            Err(LeaseInstallError::Expired)
        );
        assert_eq!(
            install(&state, lease(201), 100, now),
            Err(LeaseInstallError::LeaseTtlExceeded {
                requested_ms: 101,
                maximum_ms: 100,
            })
        );
        assert!(matches!(
            install(&state, lease(u64::MAX), 100, now),
            Err(LeaseInstallError::LeaseTtlExceeded { .. })
        ));
        assert!(state.snapshot().lease.is_none());
    }
}
