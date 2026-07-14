//! Fail-closed `PostgreSQL` identity, observation, and fencing state.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// Lease expiration as Unix time in milliseconds.
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
    inner: Arc<RwLock<AgentSnapshot>>,
    last_checked_unix_ms: Arc<AtomicU64>,
    highest_lease_epoch: Arc<AtomicU64>,
    max_lease_ttl_ms: u64,
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
            inner: Arc::new(RwLock::new(AgentSnapshot {
                identity: Some(identity),
                ..AgentSnapshot::default()
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
            .clone()
    }

    /// Replaces the locally verified `PostgreSQL` observation.
    pub fn set_postgres(&self, observation: PostgresObservation) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .postgres = Some(observation);
    }

    /// Replaces the locally supervised postmaster process state.
    pub fn set_postgres_process(&self, process: PostgresProcessState) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .postgres_process = process;
    }

    /// Installs or renews a lease already authenticated by the orchestrator
    /// client. Epochs never move backwards, and clearing a lease revokes that
    /// term so a delayed copy cannot reauthorize the instance.
    ///
    /// # Errors
    ///
    /// Returns an error for the wrong instance, a reserved or stale epoch, or a
    /// renewal that shortens the existing authorization.
    pub fn install_lease(
        &self,
        lease: FencingLease,
        now_unix_ms: u64,
    ) -> Result<LeaseInstallOutcome, LeaseInstallError> {
        let mut snapshot = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let identity = snapshot
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
        let Some(ttl_ms) = lease.valid_until_unix_ms.checked_sub(now_unix_ms) else {
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

        let highest = self.highest_lease_epoch.load(Ordering::Acquire);
        if lease.epoch < highest || (lease.epoch == highest && snapshot.lease.is_none()) {
            return Err(LeaseInstallError::StaleEpoch {
                requested: lease.epoch,
                minimum: highest.saturating_add(1),
            });
        }
        if let Some(current) = snapshot.lease.as_ref()
            && lease.epoch == current.epoch
        {
            if lease.valid_until_unix_ms < current.valid_until_unix_ms {
                return Err(LeaseInstallError::RegressiveExpiry);
            }
            if lease.valid_until_unix_ms == current.valid_until_unix_ms {
                return Ok(LeaseInstallOutcome::Existing);
            }
            snapshot.lease = Some(lease);
            return Ok(LeaseInstallOutcome::Renewed);
        }

        self.highest_lease_epoch
            .store(lease.epoch, Ordering::Release);
        snapshot.lease = Some(lease);
        Ok(LeaseInstallOutcome::Installed)
    }

    /// Removes local authorization immediately.
    pub fn clear_lease(&self) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lease = None;
    }

    /// Evaluates readiness against the current wall clock.
    #[must_use]
    pub fn readiness(&self) -> Readiness {
        self.readiness_at(unix_time_ms())
    }

    /// Evaluates deterministic readiness at a supplied Unix timestamp.
    #[must_use]
    pub fn readiness_at(&self, now_unix_ms: u64) -> Readiness {
        let previous = self
            .last_checked_unix_ms
            .fetch_max(now_unix_ms, Ordering::AcqRel);
        evaluate_readiness(&self.snapshot(), previous.max(now_unix_ms))
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
    /// A renewal cannot reduce its current expiration.
    #[error("lease renewal cannot shorten its expiration")]
    RegressiveExpiry,
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
    /// Local postmaster validation, startup, supervision, or shutdown failed.
    PostgresFailed,
    /// The lease belongs to another instance.
    LeaseOwnerMismatch,
    /// Epoch zero can never authorize an instance.
    LeaseEpochInvalid,
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

fn evaluate_readiness(snapshot: &AgentSnapshot, now_unix_ms: u64) -> Readiness {
    let reason = match (
        &snapshot.identity,
        snapshot.postgres_process,
        &snapshot.lease,
        &snapshot.postgres,
    ) {
        (_, PostgresProcessState::Failed, _, _) => ReadinessReason::PostgresFailed,
        (
            Some(_),
            PostgresProcessState::Validated
            | PostgresProcessState::StartingQuarantined
            | PostgresProcessState::RunningQuarantined
            | PostgresProcessState::Stopping,
            _,
            _,
        ) => ReadinessReason::PostgresQuarantined,
        (None, _, _, _) => ReadinessReason::IdentityMissing,
        (Some(_), _, None, _) => ReadinessReason::LeaseMissing,
        (Some(identity), _, Some(lease), _) if identity.instance_id != lease.owner_instance => {
            ReadinessReason::LeaseOwnerMismatch
        }
        (_, _, Some(lease), _) if lease.epoch == 0 => ReadinessReason::LeaseEpochInvalid,
        (_, _, Some(lease), _) if lease.valid_until_unix_ms <= now_unix_ms => {
            ReadinessReason::LeaseExpired
        }
        (_, _, _, None) => ReadinessReason::PostgresUnobserved,
        (_, _, _, Some(postgres))
            if postgres.observed_at_unix_ms == 0 || postgres.observed_at_unix_ms > now_unix_ms =>
        {
            ReadinessReason::PostgresObservationTimeInvalid
        }
        (_, _, _, Some(postgres))
            if now_unix_ms - postgres.observed_at_unix_ms > POSTGRES_OBSERVATION_MAX_AGE_MS =>
        {
            ReadinessReason::PostgresObservationStale
        }
        (_, _, _, Some(postgres)) if postgres.role == PostgresRole::Unknown => {
            ReadinessReason::PostgresRoleUnknown
        }
        (_, _, _, Some(postgres)) if postgres.timeline == 0 => ReadinessReason::TimelineInvalid,
        (_, _, Some(lease), Some(postgres)) if postgres.fencing_epoch != lease.epoch => {
            ReadinessReason::FencingEpochMismatch
        }
        (_, _, _, Some(postgres))
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

    #[test]
    fn readiness_fails_closed_without_identity_or_lease() {
        assert_eq!(
            AgentState::default().readiness_at(100).reason,
            ReadinessReason::IdentityMissing
        );
        assert_eq!(
            state().readiness_at(100).reason,
            ReadinessReason::LeaseMissing
        );
    }

    #[test]
    fn readiness_rejects_wrong_owner_and_expired_lease() {
        let state = state();
        state.set_postgres(primary());
        assert!(matches!(
            state.install_lease(
                FencingLease {
                    owner_instance: "someone-else".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 200,
                },
                100,
            ),
            Err(LeaseInstallError::OwnerMismatch { .. })
        ));
        assert_eq!(
            state.readiness_at(100).reason,
            ReadinessReason::LeaseMissing
        );

        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 100,
                },
                99,
            )
            .expect("install expired fixture");
        assert_eq!(
            state.readiness_at(100).reason,
            ReadinessReason::LeaseExpired
        );
    }

    #[test]
    fn readiness_requires_role_specific_lsn() {
        let state = state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 200,
                },
                100,
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
        assert_eq!(state.readiness_at(100).reason, ReadinessReason::LsnMissing);
    }

    #[test]
    fn readiness_accepts_current_matching_fence() {
        let state = state();
        state.set_postgres(primary());
        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 200,
                },
                100,
            )
            .expect("install lease");
        assert_eq!(
            state.readiness_at(100),
            Readiness {
                ready: true,
                reason: ReadinessReason::Ready,
            }
        );
    }

    #[test]
    fn quarantine_process_state_overrides_valid_authority() {
        let state = state();
        state.set_postgres(primary());
        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 200,
                },
                100,
            )
            .expect("install matching lease");
        state.set_postgres_process(PostgresProcessState::RunningQuarantined);

        assert_eq!(
            state.readiness_at(100).reason,
            ReadinessReason::PostgresQuarantined
        );
        state.set_postgres_process(PostgresProcessState::Disabled);
        assert_eq!(state.readiness_at(100).reason, ReadinessReason::Ready);

        state.set_postgres_process(PostgresProcessState::Failed);
        assert_eq!(
            state.readiness_at(100).reason,
            ReadinessReason::PostgresFailed
        );
    }

    #[test]
    fn readiness_rejects_invalid_and_stale_observation_time() {
        let state = state();
        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 10_000,
                },
                100,
            )
            .expect("install lease");
        let mut observation = primary();
        observation.observed_at_unix_ms = 0;
        state.set_postgres(observation);
        assert_eq!(
            state.readiness_at(100).reason,
            ReadinessReason::PostgresObservationTimeInvalid
        );

        state.set_postgres(primary());
        assert_eq!(
            state
                .readiness_at(100 + POSTGRES_OBSERVATION_MAX_AGE_MS + 1)
                .reason,
            ReadinessReason::PostgresObservationStale
        );
    }

    #[test]
    fn readiness_rejects_a_stale_postgres_fence() {
        let state = state();
        state.set_postgres(primary());
        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 4,
                    valid_until_unix_ms: 300,
                },
                100,
            )
            .expect("install lease");
        assert_eq!(
            state.readiness_at(100).reason,
            ReadinessReason::FencingEpochMismatch
        );
    }

    #[test]
    fn clock_rollback_cannot_revive_an_expired_lease() {
        let state = state();
        state.set_postgres(primary());
        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 200,
                },
                100,
            )
            .expect("install lease");
        assert!(state.readiness_at(199).ready);
        assert_eq!(
            state.readiness_at(200).reason,
            ReadinessReason::LeaseExpired
        );
        assert_eq!(
            state.readiness_at(150).reason,
            ReadinessReason::LeaseExpired
        );
    }

    #[test]
    fn status_json_uses_exact_decimal_strings() {
        let state = state();
        state.set_postgres(primary());
        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: u64::MAX - 1,
                    valid_until_unix_ms: 200,
                },
                100,
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
        let lease = FencingLease {
            owner_instance: "instance-1".to_owned(),
            epoch: 7,
            valid_until_unix_ms: 200,
        };
        assert_eq!(
            state.install_lease(lease.clone(), 100),
            Ok(LeaseInstallOutcome::Installed)
        );
        assert_eq!(
            state.install_lease(lease.clone(), 100),
            Ok(LeaseInstallOutcome::Existing)
        );
        state.clear_lease();
        assert!(matches!(
            state.install_lease(lease, 100),
            Err(LeaseInstallError::StaleEpoch { .. })
        ));
        assert_eq!(
            state.install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 8,
                    valid_until_unix_ms: 300,
                },
                200,
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
        assert_eq!(
            state.install_lease(lease(100), 100),
            Err(LeaseInstallError::Expired)
        );
        assert_eq!(
            state.install_lease(lease(201), 100),
            Err(LeaseInstallError::LeaseTtlExceeded {
                requested_ms: 101,
                maximum_ms: 100,
            })
        );
        assert!(matches!(
            state.install_lease(lease(u64::MAX), 100),
            Err(LeaseInstallError::LeaseTtlExceeded { .. })
        ));
        assert!(state.snapshot().lease.is_none());
    }
}
