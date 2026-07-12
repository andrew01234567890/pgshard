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
    /// Cluster-global namespace containing the operation.
    pub cluster_id: String,
    /// Shard owned while the operation runs.
    pub shard_id: ShardId,
    /// Operation class.
    pub kind: OperationKind,
    /// Exact immutable operation-specific request bytes.
    pub payload: Vec<u8>,
    /// Catalog epoch required when execution begins.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub required_catalog_epoch: u64,
    /// Fencing epoch required when execution begins.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub required_fencing_epoch: u64,
    /// Immutable caller deadline in Unix microseconds.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub deadline_unix_micros: u64,
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

/// Coherently observed execution inputs checked before an operation starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionPreconditions {
    /// Active catalog epoch observed by the executor.
    pub catalog_epoch: u64,
    /// Fencing epoch the executor is about to install.
    pub fencing_epoch: u64,
    /// Current Unix time in microseconds from the same execution decision.
    pub now_unix_micros: u64,
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
    #[serde(serialize_with = "serialize_u64_decimal")]
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
    /// The current operation received a later bounded expiration.
    Renewed,
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
    max_lease_ttl_ms: u64,
}

impl OrchState {
    /// Creates state with a configured process identity.
    /// # Errors
    ///
    /// Returns [`OrchError::InvalidMaximumLeaseTtl`] for a zero or unbounded
    /// policy.
    pub fn with_identity(
        identity: OrchestratorIdentity,
        max_lease_ttl_ms: u64,
    ) -> Result<Self, OrchError> {
        if !(1..=300_000).contains(&max_lease_ttl_ms) {
            return Err(OrchError::InvalidMaximumLeaseTtl(max_lease_ttl_ms));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(OrchInner {
                identity: Some(identity),
                ..OrchInner::default()
            })),
            max_lease_ttl_ms,
        })
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
        if spec.required_catalog_epoch == 0
            || spec.required_fencing_epoch == 0
            || spec.required_fencing_epoch == u64::MAX
            || spec.deadline_unix_micros == 0
        {
            return Err(OrchError::InvalidOperationPreconditions(spec.id));
        }
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
        let identity = inner.identity.as_ref().ok_or(OrchError::IdentityMissing)?;
        if identity.cluster_id != spec.cluster_id {
            return Err(OrchError::OperationClusterMismatch {
                operation_id: spec.id,
                expected: identity.cluster_id.clone(),
                requested: spec.cluster_id,
            });
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
        execution: ExecutionPreconditions,
    ) -> Result<LeaseOutcome, OrchError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let identity = inner.identity.as_ref().ok_or(OrchError::IdentityMissing)?;
        if request.owner_id != identity.orchestrator_id {
            return Err(OrchError::LeaseOwnerMismatch {
                expected: identity.orchestrator_id.clone(),
                requested: request.owner_id,
            });
        }
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
        let now_unix_ms =
            validate_execution(operation, &request, execution, self.max_lease_ttl_ms)?;
        if let Some(existing) = inner.leases.get(&request.shard_id).cloned()
            && existing.expires_at_unix_ms > now_unix_ms
        {
            let same_term = existing.owner_id == request.owner_id
                && existing.epoch == request.epoch
                && existing.operation_id == request.operation_id;
            if !same_term {
                return Err(OrchError::LeaseHeld {
                    shard_id: request.shard_id,
                    epoch: existing.epoch,
                });
            }
            if request.expires_at_unix_ms < existing.expires_at_unix_ms {
                return Err(OrchError::RegressiveLeaseExpiry {
                    current: existing.expires_at_unix_ms,
                    requested: request.expires_at_unix_ms,
                });
            }
            if request.expires_at_unix_ms == existing.expires_at_unix_ms {
                return Ok(LeaseOutcome::Existing);
            }
            inner.leases.insert(
                request.shard_id,
                ShardLease {
                    expires_at_unix_ms: request.expires_at_unix_ms,
                    ..existing
                },
            );
            return Ok(LeaseOutcome::Renewed);
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

fn validate_execution(
    operation: &OperationRecord,
    request: &LeaseRequest,
    execution: ExecutionPreconditions,
    max_lease_ttl_ms: u64,
) -> Result<u64, OrchError> {
    if operation.spec.required_catalog_epoch != execution.catalog_epoch {
        return Err(OrchError::CatalogEpochMismatch {
            operation_id: request.operation_id.clone(),
            required: operation.spec.required_catalog_epoch,
            observed: execution.catalog_epoch,
        });
    }
    if operation.spec.required_fencing_epoch != execution.fencing_epoch
        || request.epoch != execution.fencing_epoch
    {
        return Err(OrchError::ExecutionFencingEpochMismatch {
            operation_id: request.operation_id.clone(),
            required: operation.spec.required_fencing_epoch,
            observed: execution.fencing_epoch,
            requested: request.epoch,
        });
    }
    if execution.now_unix_micros >= operation.spec.deadline_unix_micros {
        return Err(OrchError::OperationDeadlineExceeded {
            operation_id: request.operation_id.clone(),
            deadline_unix_micros: operation.spec.deadline_unix_micros,
            now_unix_micros: execution.now_unix_micros,
        });
    }
    let deadline_unix_ms = operation.spec.deadline_unix_micros / 1_000;
    if request.expires_at_unix_ms > deadline_unix_ms {
        return Err(OrchError::LeasePastOperationDeadline {
            operation_id: request.operation_id.clone(),
            deadline_unix_micros: operation.spec.deadline_unix_micros,
            requested_expiry_unix_ms: request.expires_at_unix_ms,
        });
    }
    let now_unix_ms = execution.now_unix_micros.div_ceil(1_000);
    let Some(ttl_ms) = request.expires_at_unix_ms.checked_sub(now_unix_ms) else {
        return Err(OrchError::InvalidLeaseRequest);
    };
    if ttl_ms == 0 {
        return Err(OrchError::InvalidLeaseRequest);
    }
    if ttl_ms > max_lease_ttl_ms {
        return Err(OrchError::LeaseTtlExceeded {
            requested_ms: ttl_ms,
            maximum_ms: max_lease_ttl_ms,
        });
    }
    Ok(now_unix_ms)
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
    /// The authority was constructed with an unsafe lease policy.
    #[error("maximum lease TTL {0} ms must be between 1 and 300000 ms")]
    InvalidMaximumLeaseTtl(u64),
    /// State has no stable operator-assigned identity.
    #[error("orchestrator identity is missing")]
    IdentityMissing,
    /// The operation ID is unsafe or empty.
    #[error("invalid operation ID {0:?}")]
    InvalidOperationId(OperationId),
    /// Immutable epochs and deadline cannot express a safe execution.
    #[error("operation {0:?} has invalid execution preconditions")]
    InvalidOperationPreconditions(OperationId),
    /// The same operation ID was reused with different immutable input.
    #[error("operation ID {0:?} is already assigned to different input")]
    OperationConflict(OperationId),
    /// The operation targets a different cluster.
    #[error("operation {operation_id:?} belongs to cluster {requested:?}, not {expected:?}")]
    OperationClusterMismatch {
        /// Operation ID.
        operation_id: OperationId,
        /// Configured cluster.
        expected: String,
        /// Requested cluster.
        requested: String,
    },
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
    /// The active catalog epoch differs from the operation's immutable input.
    #[error("operation {operation_id:?} requires catalog epoch {required}, observed {observed}")]
    CatalogEpochMismatch {
        /// Operation ID.
        operation_id: OperationId,
        /// Immutable required epoch.
        required: u64,
        /// Executor observation.
        observed: u64,
    },
    /// The requested and observed fencing terms do not match the operation.
    #[error(
        "operation {operation_id:?} requires fencing epoch {required}, observed {observed}, requested {requested}"
    )]
    ExecutionFencingEpochMismatch {
        /// Operation ID.
        operation_id: OperationId,
        /// Immutable required epoch.
        required: u64,
        /// Executor observation.
        observed: u64,
        /// Lease request epoch.
        requested: u64,
    },
    /// The immutable operation deadline has passed.
    #[error(
        "operation {operation_id:?} deadline {deadline_unix_micros} has passed at {now_unix_micros}"
    )]
    OperationDeadlineExceeded {
        /// Operation ID.
        operation_id: OperationId,
        /// Immutable deadline.
        deadline_unix_micros: u64,
        /// Coherent executor time.
        now_unix_micros: u64,
    },
    /// Lease expiration is not in the future.
    #[error("lease expiration must be in the future")]
    InvalidLeaseRequest,
    /// Lease lifetime exceeds the configured safety policy.
    #[error("requested lease TTL {requested_ms} ms exceeds maximum {maximum_ms} ms")]
    LeaseTtlExceeded {
        /// Requested duration.
        requested_ms: u64,
        /// Configured maximum.
        maximum_ms: u64,
    },
    /// Lease expiration would outlive the immutable operation deadline.
    #[error(
        "operation {operation_id:?} deadline {deadline_unix_micros} is before lease expiry {requested_expiry_unix_ms} ms"
    )]
    LeasePastOperationDeadline {
        /// Operation ID.
        operation_id: OperationId,
        /// Immutable operation deadline.
        deadline_unix_micros: u64,
        /// Rejected lease expiry.
        requested_expiry_unix_ms: u64,
    },
    /// A live lease renewal cannot shorten the current expiration.
    #[error("lease renewal expiry {requested} is before current expiry {current}")]
    RegressiveLeaseExpiry {
        /// Current expiration.
        current: u64,
        /// Rejected expiration.
        requested: u64,
    },
    /// A process cannot request ownership under another orchestrator identity.
    #[error("lease owner {requested:?} does not match this orchestrator {expected:?}")]
    LeaseOwnerMismatch {
        /// Configured orchestrator identity.
        expected: String,
        /// Requested owner.
        requested: String,
    },
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

    fn operation_with_fence(
        id: &str,
        shard: u32,
        kind: OperationKind,
        fencing_epoch: u64,
    ) -> OperationSpec {
        OperationSpec {
            id: OperationId(id.to_owned()),
            cluster_id: "cluster-1".to_owned(),
            shard_id: ShardId(shard),
            kind,
            payload: vec![1, 2, 3],
            required_catalog_epoch: 7,
            required_fencing_epoch: fencing_epoch,
            deadline_unix_micros: 1_000_000,
        }
    }

    fn operation(id: &str, shard: u32, kind: OperationKind) -> OperationSpec {
        operation_with_fence(id, shard, kind, 11)
    }

    fn state() -> OrchState {
        OrchState::with_identity(identity(), 100).expect("valid lease policy")
    }

    fn execution(fencing_epoch: u64, now_unix_ms: u64) -> ExecutionPreconditions {
        ExecutionPreconditions {
            catalog_epoch: 7,
            fencing_epoch,
            now_unix_micros: now_unix_ms * 1_000,
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
            state().readiness(),
            OrchReadiness {
                ready: false,
                reason: OrchReadinessReason::PersistenceUnavailable,
            }
        );
    }

    #[test]
    fn operation_registration_is_idempotent_but_not_ambiguous() {
        let state = state();
        let spec = operation("op-1", 1, OperationKind::Backup);
        assert_eq!(
            state.register_operation(spec.clone()).expect("created"),
            RegistrationOutcome::Created
        );
        assert_eq!(
            state
                .register_operation(spec.clone())
                .expect("same operation"),
            RegistrationOutcome::Existing
        );

        let mut variants = Vec::new();
        let mut changed = spec.clone();
        changed.cluster_id = "another-cluster".to_owned();
        variants.push(changed);
        let mut changed = spec.clone();
        changed.shard_id = ShardId(2);
        variants.push(changed);
        let mut changed = spec.clone();
        changed.kind = OperationKind::Restore;
        variants.push(changed);
        let mut changed = spec.clone();
        changed.payload.push(4);
        variants.push(changed);
        let mut changed = spec.clone();
        changed.required_catalog_epoch += 1;
        variants.push(changed);
        let mut changed = spec.clone();
        changed.required_fencing_epoch += 1;
        variants.push(changed);
        let mut changed = spec;
        changed.deadline_unix_micros += 1;
        variants.push(changed);

        for changed in variants {
            assert!(matches!(
                state.register_operation(changed),
                Err(OrchError::OperationConflict(_))
            ));
        }
    }

    #[test]
    fn lease_replay_is_idempotent_and_competitor_is_rejected() {
        let state = state();
        state
            .register_operation(operation("op-1", 1, OperationKind::Backup))
            .expect("register");
        let request = lease("op-1", "orch-0", 11, 200);
        assert_eq!(
            state
                .acquire_lease(request.clone(), execution(11, 100))
                .expect("acquire"),
            LeaseOutcome::Acquired
        );
        assert_eq!(
            state
                .acquire_lease(request, execution(11, 100))
                .expect("idempotent"),
            LeaseOutcome::Existing
        );
        assert_eq!(
            state
                .acquire_lease(lease("op-1", "orch-0", 11, 250), execution(11, 150),)
                .expect("renew"),
            LeaseOutcome::Renewed
        );
        assert_eq!(state.snapshot().leases[0].expires_at_unix_ms, 250);
        assert!(matches!(
            state.acquire_lease(lease("op-1", "orch-0", 11, 240), execution(11, 150)),
            Err(OrchError::RegressiveLeaseExpiry { .. })
        ));
        assert!(matches!(
            state.acquire_lease(lease("op-1", "orch-1", 11, 250), execution(11, 150)),
            Err(OrchError::LeaseOwnerMismatch { .. })
        ));

        state
            .register_operation(operation_with_fence("op-2", 1, OperationKind::Backup, 12))
            .expect("register competitor");
        assert!(matches!(
            state.acquire_lease(lease("op-2", "orch-0", 12, 250), execution(12, 150)),
            Err(OrchError::LeaseHeld { .. })
        ));
    }

    #[test]
    fn expired_lease_requires_strictly_higher_epoch() {
        let state = state();
        state
            .register_operation(operation("op-1", 1, OperationKind::Failover))
            .expect("register");
        state
            .acquire_lease(lease("op-1", "orch-0", 11, 200), execution(11, 100))
            .expect("acquire");
        assert!(matches!(
            state.acquire_lease(lease("op-1", "orch-0", 11, 300), execution(11, 200)),
            Err(OrchError::StaleEpoch {
                requested: 11,
                minimum: 12
            })
        ));
        state
            .register_operation(operation_with_fence("op-2", 1, OperationKind::Failover, 12))
            .expect("register next term");
        assert_eq!(
            state
                .acquire_lease(lease("op-2", "orch-0", 12, 300), execution(12, 200))
                .expect("new term"),
            LeaseOutcome::Acquired
        );
    }

    #[test]
    fn unknown_operation_cannot_acquire_lease() {
        let state = state();
        assert!(matches!(
            state.acquire_lease(lease("missing", "orch-0", 1, 200), execution(1, 100)),
            Err(OrchError::UnknownOperation(_))
        ));
    }

    #[test]
    fn rejects_cross_cluster_operations_and_reserved_epoch() {
        let state = state();
        let mut foreign = operation("foreign", 1, OperationKind::Backup);
        foreign.cluster_id = "another-cluster".to_owned();
        assert!(matches!(
            state.register_operation(foreign),
            Err(OrchError::OperationClusterMismatch { .. })
        ));

        assert!(matches!(
            state.register_operation(operation_with_fence(
                "op-max",
                1,
                OperationKind::Failover,
                u64::MAX,
            )),
            Err(OrchError::InvalidOperationPreconditions(_))
        ));
    }

    #[test]
    fn status_json_preserves_fencing_epoch_exactly() {
        let state = state();
        state
            .register_operation(operation_with_fence(
                "op-1",
                1,
                OperationKind::Backup,
                u64::MAX - 1,
            ))
            .expect("register");
        state
            .acquire_lease(
                lease("op-1", "orch-0", u64::MAX - 1, 200),
                execution(u64::MAX - 1, 100),
            )
            .expect("acquire");
        let json = serde_json::to_value(state.snapshot()).expect("serialize status");
        assert_eq!(json["leases"][0]["epoch"], (u64::MAX - 1).to_string());
        assert_eq!(json["leases"][0]["expires_at_unix_ms"], "200");
    }

    #[test]
    fn execution_preconditions_fail_before_state_transition() {
        let state = state();
        state
            .register_operation(operation("op-1", 1, OperationKind::Ddl))
            .expect("register");
        let request = lease("op-1", "orch-0", 11, 200);

        let mut wrong_catalog = execution(11, 100);
        wrong_catalog.catalog_epoch = 8;
        assert!(matches!(
            state.acquire_lease(request.clone(), wrong_catalog),
            Err(OrchError::CatalogEpochMismatch { .. })
        ));
        assert!(matches!(
            state.acquire_lease(request.clone(), execution(12, 100)),
            Err(OrchError::ExecutionFencingEpochMismatch { .. })
        ));
        let mut expired = execution(11, 100);
        expired.now_unix_micros = 1_000_000;
        assert!(matches!(
            state.acquire_lease(request, expired),
            Err(OrchError::OperationDeadlineExceeded { .. })
        ));

        let inner = state
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(inner.leases.is_empty());
        assert_eq!(
            inner.operations[&OperationId("op-1".to_owned())].phase,
            OperationPhase::Pending
        );
    }

    #[test]
    fn registration_rejects_zero_execution_preconditions() {
        let state = state();
        let mut invalid = operation("invalid", 1, OperationKind::Ddl);
        invalid.required_catalog_epoch = 0;
        assert!(matches!(
            state.register_operation(invalid),
            Err(OrchError::InvalidOperationPreconditions(_))
        ));
        let mut invalid = operation("invalid", 1, OperationKind::Ddl);
        invalid.required_fencing_epoch = 0;
        assert!(matches!(
            state.register_operation(invalid),
            Err(OrchError::InvalidOperationPreconditions(_))
        ));
        let mut invalid = operation("invalid", 1, OperationKind::Ddl);
        invalid.deadline_unix_micros = 0;
        assert!(matches!(
            state.register_operation(invalid),
            Err(OrchError::InvalidOperationPreconditions(_))
        ));
    }

    #[test]
    fn lease_policy_rejects_expired_overlong_and_invalid_limits() {
        assert!(matches!(
            OrchState::with_identity(identity(), 0),
            Err(OrchError::InvalidMaximumLeaseTtl(0))
        ));
        let state = state();
        state
            .register_operation(operation("op-1", 1, OperationKind::Backup))
            .expect("register");
        assert_eq!(
            state.acquire_lease(lease("op-1", "orch-0", 11, 100), execution(11, 100)),
            Err(OrchError::InvalidLeaseRequest)
        );
        assert_eq!(
            state.acquire_lease(lease("op-1", "orch-0", 11, 201), execution(11, 100)),
            Err(OrchError::LeaseTtlExceeded {
                requested_ms: 101,
                maximum_ms: 100,
            })
        );
        assert!(matches!(
            state.acquire_lease(lease("op-1", "orch-0", 11, u64::MAX), execution(11, 100)),
            Err(OrchError::LeasePastOperationDeadline { .. })
        ));
        assert!(matches!(
            state.acquire_lease(lease("op-1", "orch-0", 11, 1_001), execution(11, 900)),
            Err(OrchError::LeasePastOperationDeadline { .. })
        ));
    }
}
