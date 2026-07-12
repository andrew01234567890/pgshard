//! Fail-closed `PostgreSQL` identity, observation, and fencing state.

use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use pgshard_types::{PgLsn, ShardId};
use serde::Serialize;

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

/// Last locally verified `PostgreSQL` state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PostgresObservation {
    /// Role established by `PostgreSQL` inspection.
    pub role: PostgresRole,
    /// Current `PostgreSQL` timeline.
    pub timeline: u32,
    /// Last locally flushed WAL position, when applicable.
    pub flush_lsn: Option<PgLsn>,
    /// Last replayed WAL position, when applicable.
    pub replay_lsn: Option<PgLsn>,
}

/// Lease proving this instance belongs to the current fencing term.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FencingLease {
    /// Instance authorized by the lease.
    pub owner_instance: String,
    /// Strictly positive fencing epoch.
    pub epoch: u64,
    /// Lease expiration as Unix time in milliseconds.
    pub valid_until_unix_ms: u64,
}

/// Externally reportable agent state.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct AgentSnapshot {
    /// Operator-assigned identity, absent until established.
    pub identity: Option<AgentIdentity>,
    /// Last `PostgreSQL` observation.
    pub postgres: Option<PostgresObservation>,
    /// Current fencing lease.
    pub lease: Option<FencingLease>,
}

/// Thread-safe state shared by reconciliation and HTTP handlers.
#[derive(Clone, Debug, Default)]
pub struct AgentState {
    inner: Arc<RwLock<AgentSnapshot>>,
}

impl AgentState {
    /// Creates state with the operator-assigned identity but no assumed lease or
    /// `PostgreSQL` role.
    #[must_use]
    pub fn with_identity(identity: AgentIdentity) -> Self {
        Self {
            inner: Arc::new(RwLock::new(AgentSnapshot {
                identity: Some(identity),
                ..AgentSnapshot::default()
            })),
        }
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

    /// Installs a lease obtained from the orchestrator.
    pub fn install_lease(&self, lease: FencingLease) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lease = Some(lease);
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
        evaluate_readiness(&self.snapshot(), now_unix_ms)
    }
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
    /// The lease belongs to another instance.
    LeaseOwnerMismatch,
    /// Epoch zero can never authorize an instance.
    LeaseEpochInvalid,
    /// The installed lease is no longer valid.
    LeaseExpired,
    /// `PostgreSQL` has not been inspected successfully.
    PostgresUnobserved,
    /// `PostgreSQL` role is unknown.
    PostgresRoleUnknown,
    /// `PostgreSQL` did not report a valid timeline.
    TimelineInvalid,
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
    let reason = match (&snapshot.identity, &snapshot.lease, &snapshot.postgres) {
        (None, _, _) => ReadinessReason::IdentityMissing,
        (Some(_), None, _) => ReadinessReason::LeaseMissing,
        (Some(identity), Some(lease), _) if identity.instance_id != lease.owner_instance => {
            ReadinessReason::LeaseOwnerMismatch
        }
        (_, Some(lease), _) if lease.epoch == 0 => ReadinessReason::LeaseEpochInvalid,
        (_, Some(lease), _) if lease.valid_until_unix_ms <= now_unix_ms => {
            ReadinessReason::LeaseExpired
        }
        (_, _, None) => ReadinessReason::PostgresUnobserved,
        (_, _, Some(postgres)) if postgres.role == PostgresRole::Unknown => {
            ReadinessReason::PostgresRoleUnknown
        }
        (_, _, Some(postgres)) if postgres.timeline == 0 => ReadinessReason::TimelineInvalid,
        (_, _, Some(postgres))
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
            flush_lsn: Some(PgLsn(100)),
            replay_lsn: None,
        }
    }

    #[test]
    fn readiness_fails_closed_without_identity_or_lease() {
        assert_eq!(
            AgentState::default().readiness_at(100).reason,
            ReadinessReason::IdentityMissing
        );
        assert_eq!(
            AgentState::with_identity(identity())
                .readiness_at(100)
                .reason,
            ReadinessReason::LeaseMissing
        );
    }

    #[test]
    fn readiness_rejects_wrong_owner_and_expired_lease() {
        let state = AgentState::with_identity(identity());
        state.set_postgres(primary());
        state.install_lease(FencingLease {
            owner_instance: "someone-else".to_owned(),
            epoch: 3,
            valid_until_unix_ms: 200,
        });
        assert_eq!(
            state.readiness_at(100).reason,
            ReadinessReason::LeaseOwnerMismatch
        );

        state.install_lease(FencingLease {
            owner_instance: "instance-1".to_owned(),
            epoch: 3,
            valid_until_unix_ms: 100,
        });
        assert_eq!(
            state.readiness_at(100).reason,
            ReadinessReason::LeaseExpired
        );
    }

    #[test]
    fn readiness_requires_role_specific_lsn() {
        let state = AgentState::with_identity(identity());
        state.install_lease(FencingLease {
            owner_instance: "instance-1".to_owned(),
            epoch: 3,
            valid_until_unix_ms: 200,
        });
        state.set_postgres(PostgresObservation {
            role: PostgresRole::Replica,
            timeline: 4,
            flush_lsn: Some(PgLsn(100)),
            replay_lsn: None,
        });
        assert_eq!(state.readiness_at(100).reason, ReadinessReason::LsnMissing);
    }

    #[test]
    fn readiness_accepts_current_matching_fence() {
        let state = AgentState::with_identity(identity());
        state.set_postgres(primary());
        state.install_lease(FencingLease {
            owner_instance: "instance-1".to_owned(),
            epoch: 3,
            valid_until_unix_ms: 200,
        });
        assert_eq!(
            state.readiness_at(100),
            Readiness {
                ready: true,
                reason: ReadinessReason::Ready,
            }
        );
    }
}
