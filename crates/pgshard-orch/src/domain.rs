//! In-memory operation identity and conservative per-shard lease ownership.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use pgshard_types::ShardId;
use serde::{Serialize, Serializer};
use thiserror::Error;

/// Stable identity assigned to one orchestrator process.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OrchestratorIdentity {
    /// Cluster controlled by the process.
    pub cluster_id: String,
    /// Unique orchestrator replica identity.
    pub orchestrator_id: String,
}

/// Caller-supplied idempotency key for a durable operation.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct OperationId(pub String);

/// Operation classes known by the foundation state machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    /// Planned primary movement.
    Switchover,
    /// Recovery workflow; candidate safety is not implemented here.
    Failover,
    /// Coordinated backup.
    Backup,
    /// Empty-target restore.
    Restore,
    /// Managed schema operation.
    Ddl,
    /// Online shard topology change.
    Reshard,
    /// Role or grant reconciliation.
    Authorization,
}

/// Immutable identity of one requested operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OperationSpec {
    /// Idempotency key.
    pub id: OperationId,
    /// Shard owned while the operation runs.
    pub shard_id: ShardId,
    /// Operation class.
    pub kind: OperationKind,
}

/// Current operation progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationPhase {
    /// Registered but not executing.
    Pending,
    /// Executing under a lease.
    Running,
    /// Completed successfully.
    Succeeded,
    /// Completed unsuccessfully.
    Failed,
}

/// In-memory representation of an operation record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OperationRecord {
    /// Immutable request identity.
    pub spec: OperationSpec,
    /// Current phase.
    pub phase: OperationPhase,
}

/// Result of idempotent operation registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationOutcome {
    /// The operation ID was created.
    Created,
    /// The same operation was already registered.
    Existing,
}

/// Lease request for one registered operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseRequest {
    /// Target shard.
    pub shard_id: ShardId,
    /// Orchestrator attempting ownership.
    pub owner_id: String,
    /// New fencing epoch, strictly greater than every previous epoch.
    pub epoch: u64,
    /// Operation requiring ownership.
    pub operation_id: OperationId,
    /// Requested expiration timestamp in Unix milliseconds.
    pub expires_at_unix_ms: u64,
}

/// Current lease for one shard.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ShardLease {
    /// Owned shard.
    pub shard_id: ShardId,
    /// Owning orchestrator.
    pub owner_id: String,
    /// Monotonically increasing fencing epoch.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub epoch: u64,
    /// Operation whose execution is fenced by this lease.
    pub operation_id: OperationId,
    /// Expiration timestamp in Unix milliseconds.
    pub expires_at_unix_ms: u64,
}

// Serde's `serialize_with` callback ABI passes the field by reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_u64_decimal<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

/// Result of a conservative lease acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseOutcome {
    /// A new lease was recorded.
    Acquired,
    /// The identical request already owns the shard.
    Existing,
}

/// Externally reportable orchestrator status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OrchSnapshot {
    /// Stable identity, absent until configuration is established.
    pub identity: Option<OrchestratorIdentity>,
    /// Registered operation count.
    pub operation_count: usize,
    /// Current lease records, including expired records until replaced.
    pub leases: Vec<ShardLease>,
    /// Explicit limitation of this foundation.
    pub failover_automation_enabled: bool,
    /// Whether operation and lease records survive process restart.
    pub persistence_enabled: bool,
}

/// Machine-readable reason the orchestrator is not yet safe to serve control
/// operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrchReadinessReason {
    /// No stable operator-assigned identity exists.
    IdentityMissing,
    /// Operations and fencing epochs are currently in memory only.
    PersistenceUnavailable,
}

/// Fail-closed orchestrator readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OrchReadiness {
    /// Always false until durable state and recovery are wired.
    pub ready: bool,
    /// Exact reason control operations are unavailable.
    pub reason: OrchReadinessReason,
}

#[derive(Debug, Default)]
struct OrchInner {
    identity: Option<OrchestratorIdentity>,
    operations: HashMap<OperationId, OperationRecord>,
    leases: HashMap<ShardId, ShardLease>,
    last_epochs: HashMap<ShardId, u64>,
}

/// Thread-safe registry for operation IDs and shard leases.
#[derive(Clone, Debug, Default)]
pub struct OrchState {
    inner: Arc<Mutex<OrchInner>>,
}

impl OrchState {
    /// Creates state with a configured process identity.
    #[must_use]
    pub fn with_identity(identity: OrchestratorIdentity) -> Self {
        Self {
            inner: Arc::new(Mutex::new(OrchInner {
                identity: Some(identity),
                ..OrchInner::default()
            })),
        }
    }

    /// Returns whether the runtime can safely serve control operations.
    #[must_use]
    pub fn readiness(&self) -> OrchReadiness {
        let has_identity = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .identity
            .is_some();
        OrchReadiness {
            ready: false,
            reason: if has_identity {
                OrchReadinessReason::PersistenceUnavailable
            } else {
                OrchReadinessReason::IdentityMissing
            },
        }
    }

    /// Returns a consistent reportable snapshot.
    #[must_use]
    pub fn snapshot(&self) -> OrchSnapshot {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut leases: Vec<_> = inner.leases.values().cloned().collect();
        leases.sort_by_key(|lease| lease.shard_id);
        OrchSnapshot {
            identity: inner.identity.clone(),
            operation_count: inner.operations.len(),
            leases,
            failover_automation_enabled: false,
            persistence_enabled: false,
        }
    }

    /// Registers an operation idempotently for this process lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation ID is invalid or has already been
    /// assigned to different immutable input.
    pub fn register_operation(
        &self,
        spec: OperationSpec,
    ) -> Result<RegistrationOutcome, OrchError> {
        validate_operation_id(&spec.id)?;
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = inner.operations.get(&spec.id) {
            if existing.spec == spec {
                return Ok(RegistrationOutcome::Existing);
            }
            return Err(OrchError::OperationConflict(spec.id));
        }
        inner.operations.insert(
            spec.id.clone(),
            OperationRecord {
                spec,
                phase: OperationPhase::Pending,
            },
        );
        Ok(RegistrationOutcome::Created)
    }

    /// Acquires a shard lease for a previously registered operation.
    ///
    /// Expiration alone never promotes a `PostgreSQL` candidate. The caller must
    /// prove failover safety separately before requesting a higher epoch.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown/mismatched operations, active ownership,
    /// invalid epochs, or already-expired requests.
    pub fn acquire_lease(
        &self,
        request: LeaseRequest,
        now_unix_ms: u64,
    ) -> Result<LeaseOutcome, OrchError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let operation = inner
            .operations
            .get(&request.operation_id)
            .ok_or_else(|| OrchError::UnknownOperation(request.operation_id.clone()))?;
        if operation.spec.shard_id != request.shard_id {
            return Err(OrchError::OperationShardMismatch {
                operation_id: request.operation_id,
                expected: operation.spec.shard_id,
                requested: request.shard_id,
            });
        }
        if request.owner_id.is_empty() || request.expires_at_unix_ms <= now_unix_ms {
            return Err(OrchError::InvalidLeaseRequest);
        }
        if let Some(existing) = inner.leases.get(&request.shard_id) {
            let identical = existing.owner_id == request.owner_id
                && existing.epoch == request.epoch
                && existing.operation_id == request.operation_id
                && existing.expires_at_unix_ms == request.expires_at_unix_ms;
            if existing.expires_at_unix_ms > now_unix_ms {
                return if identical {
                    Ok(LeaseOutcome::Existing)
                } else {
                    Err(OrchError::LeaseHeld {
                        shard_id: request.shard_id,
                        epoch: existing.epoch,
                    })
                };
            }
        }
        let last_epoch = inner
            .last_epochs
            .get(&request.shard_id)
            .copied()
            .unwrap_or(0)
            .max(
                inner
                    .leases
                    .get(&request.shard_id)
                    .map_or(0, |lease| lease.epoch),
            );
        if request.epoch <= last_epoch {
            return Err(OrchError::StaleEpoch {
                requested: request.epoch,
                minimum: last_epoch.saturating_add(1),
            });
        }
        let lease = ShardLease {
            shard_id: request.shard_id,
            owner_id: request.owner_id,
            epoch: request.epoch,
            operation_id: request.operation_id.clone(),
            expires_at_unix_ms: request.expires_at_unix_ms,
        };
        inner.last_epochs.insert(request.shard_id, request.epoch);
        inner.leases.insert(request.shard_id, lease);
        if let Some(operation) = inner.operations.get_mut(&request.operation_id) {
            operation.phase = OperationPhase::Running;
        }
        Ok(LeaseOutcome::Acquired)
    }
}

fn validate_operation_id(operation_id: &OperationId) -> Result<(), OrchError> {
    if operation_id.0.is_empty()
        || operation_id.0.len() > 128
        || !operation_id
            .0
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(OrchError::InvalidOperationId(operation_id.clone()));
    }
    Ok(())
}

/// Operation registry or lease ownership failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum OrchError {
    /// The operation ID is unsafe or empty.
    #[error("invalid operation ID {0:?}")]
    InvalidOperationId(OperationId),
    /// The same operation ID was reused with different immutable input.
    #[error("operation ID {0:?} is already assigned to different input")]
    OperationConflict(OperationId),
    /// The lease refers to an unregistered operation.
    #[error("unknown operation ID {0:?}")]
    UnknownOperation(OperationId),
    /// The operation was registered for another shard.
    #[error("operation {operation_id:?} belongs to shard {expected:?}, not {requested:?}")]
    OperationShardMismatch {
        /// Operation ID.
        operation_id: OperationId,
        /// Registered shard.
        expected: ShardId,
        /// Requested shard.
        requested: ShardId,
    },
    /// Lease owner is empty or expiration is not in the future.
    #[error("lease owner must be non-empty and expiration must be in the future")]
    InvalidLeaseRequest,
    /// A different live lease already owns the shard.
    #[error("shard {shard_id:?} is held at epoch {epoch}")]
    LeaseHeld {
        /// Owned shard.
        shard_id: ShardId,
        /// Current epoch.
        epoch: u64,
    },
    /// Fencing epochs must increase on every ownership transition.
    #[error("stale fencing epoch {requested}; next epoch must be at least {minimum}")]
    StaleEpoch {
        /// Rejected epoch.
        requested: u64,
        /// Minimum acceptable epoch.
        minimum: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> OrchestratorIdentity {
        OrchestratorIdentity {
            cluster_id: "cluster-1".to_owned(),
            orchestrator_id: "orch-0".to_owned(),
        }
    }

    fn operation(id: &str, shard: u32, kind: OperationKind) -> OperationSpec {
        OperationSpec {
            id: OperationId(id.to_owned()),
            shard_id: ShardId(shard),
            kind,
        }
    }

    fn lease(id: &str, owner: &str, epoch: u64, expires: u64) -> LeaseRequest {
        LeaseRequest {
            shard_id: ShardId(1),
            owner_id: owner.to_owned(),
            epoch,
            operation_id: OperationId(id.to_owned()),
            expires_at_unix_ms: expires,
        }
    }

    #[test]
    fn readiness_requires_identity() {
        assert_eq!(
            OrchState::default().readiness().reason,
            OrchReadinessReason::IdentityMissing
        );
        assert_eq!(
            OrchState::with_identity(identity()).readiness(),
            OrchReadiness {
                ready: false,
                reason: OrchReadinessReason::PersistenceUnavailable,
            }
        );
    }

    #[test]
    fn operation_registration_is_idempotent_but_not_ambiguous() {
        let state = OrchState::with_identity(identity());
        let spec = operation("op-1", 1, OperationKind::Backup);
        assert_eq!(
            state.register_operation(spec.clone()).expect("created"),
            RegistrationOutcome::Created
        );
        assert_eq!(
            state.register_operation(spec).expect("same operation"),
            RegistrationOutcome::Existing
        );
        assert!(matches!(
            state.register_operation(operation("op-1", 1, OperationKind::Restore)),
            Err(OrchError::OperationConflict(_))
        ));
    }

    #[test]
    fn lease_replay_is_idempotent_and_competitor_is_rejected() {
        let state = OrchState::with_identity(identity());
        state
            .register_operation(operation("op-1", 1, OperationKind::Backup))
            .expect("register");
        let request = lease("op-1", "orch-0", 1, 200);
        assert_eq!(
            state.acquire_lease(request.clone(), 100).expect("acquire"),
            LeaseOutcome::Acquired
        );
        assert_eq!(
            state.acquire_lease(request, 100).expect("idempotent"),
            LeaseOutcome::Existing
        );
        assert!(matches!(
            state.acquire_lease(lease("op-1", "orch-1", 2, 200), 100),
            Err(OrchError::LeaseHeld { .. })
        ));
    }

    #[test]
    fn expired_lease_requires_strictly_higher_epoch() {
        let state = OrchState::with_identity(identity());
        state
            .register_operation(operation("op-1", 1, OperationKind::Failover))
            .expect("register");
        state
            .acquire_lease(lease("op-1", "orch-0", 4, 200), 100)
            .expect("acquire");
        assert!(matches!(
            state.acquire_lease(lease("op-1", "orch-1", 4, 300), 200),
            Err(OrchError::StaleEpoch {
                requested: 4,
                minimum: 5
            })
        ));
        assert_eq!(
            state
                .acquire_lease(lease("op-1", "orch-1", 5, 300), 200)
                .expect("new term"),
            LeaseOutcome::Acquired
        );
    }

    #[test]
    fn unknown_operation_cannot_acquire_lease() {
        let state = OrchState::with_identity(identity());
        assert!(matches!(
            state.acquire_lease(lease("missing", "orch-0", 1, 200), 100),
            Err(OrchError::UnknownOperation(_))
        ));
    }

    #[test]
    fn status_json_preserves_fencing_epoch_exactly() {
        let state = OrchState::with_identity(identity());
        state
            .register_operation(operation("op-1", 1, OperationKind::Backup))
            .expect("register");
        state
            .acquire_lease(lease("op-1", "orch-0", u64::MAX, 200), 100)
            .expect("acquire");
        let json = serde_json::to_value(state.snapshot()).expect("serialize status");
        assert_eq!(json["leases"][0]["epoch"], u64::MAX.to_string());
    }
}
