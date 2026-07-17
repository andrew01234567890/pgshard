//! In-memory operation identity and conservative per-shard lease ownership.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pgshard_types::ShardId;
use serde::{Serialize, Serializer};
use thiserror::Error;

const DEFAULT_MAX_LEASE_TTL_MS: u64 = 15_000;
const MAX_LEASE_TTL_MS: u64 = 300_000;

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
    /// Reportable expiration timestamp in Unix milliseconds. Process-local
    /// liveness is bounded independently by a monotonic deadline.
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

/// Informational result of a conservative lease acquisition.
///
/// This value describes the in-memory mutation only. It is never evidence that
/// the lease is still live when execution is dispatched; use the returned
/// [`LeaseGrant`] to revalidate at that boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseOutcome {
    /// A new lease was recorded.
    Acquired,
    /// The identical request already owns the shard.
    Existing,
    /// The current operation received a later bounded expiration.
    Renewed,
}

/// Handle to a lease term that must be revalidated immediately before dispatch.
///
/// Acquiring this handle does not authorize execution: the process can be
/// descheduled after acquisition, the term can expire, or a higher epoch can
/// replace it. Call [`Self::validate_for_execution`] with a coherent observation
/// of the execution preconditions at the dispatch boundary. The receiving
/// target must still enforce [`LeaseExecutionGuard::fencing_epoch`]; a local
/// guard cannot prove that a remote side effect completed before expiry.
#[must_use = "lease acquisition is informational until the grant is revalidated for execution"]
pub struct LeaseGrant {
    inner: Arc<Mutex<OrchInner>>,
    clock_origin: Instant,
    outcome: LeaseOutcome,
    shard_id: ShardId,
    owner_id: String,
    epoch: u64,
    operation_id: OperationId,
    expires_at_unix_ms: u64,
    monotonic_deadline: Duration,
}

impl fmt::Debug for LeaseGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseGrant")
            .field("outcome", &self.outcome)
            .field("shard_id", &self.shard_id)
            .field("epoch", &self.epoch)
            .field("operation_id", &self.operation_id)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish_non_exhaustive()
    }
}

impl LeaseGrant {
    /// Returns the informational result of the acquisition attempt.
    #[must_use]
    pub const fn outcome(&self) -> LeaseOutcome {
        self.outcome
    }

    /// Revalidates this exact term at an execution-dispatch boundary.
    ///
    /// The returned guard proves only that the local term and supplied catalog
    /// and fencing observations matched at the validation instant. A target
    /// receiving work must atomically reject stale fencing epochs.
    ///
    /// # Errors
    ///
    /// Returns an error if this handle was superseded, its monotonic deadline
    /// passed, or the execution observations no longer match the operation.
    pub fn validate_for_execution(
        &self,
        execution: ExecutionPreconditions,
    ) -> Result<LeaseExecutionGuard<'_>, OrchError> {
        self.validate_for_execution_with_clock(execution, || self.clock_origin.elapsed())
    }

    #[cfg(test)]
    fn validate_for_execution_at(
        &self,
        execution: ExecutionPreconditions,
        now_monotonic: Duration,
    ) -> Result<LeaseExecutionGuard<'_>, OrchError> {
        self.validate_for_execution_with_clock(execution, || now_monotonic)
    }

    fn validate_for_execution_with_clock<F>(
        &self,
        execution: ExecutionPreconditions,
        mut clock: F,
    ) -> Result<LeaseExecutionGuard<'_>, OrchError>
    where
        F: FnMut() -> Duration,
    {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current_lease = inner.leases.get(&self.shard_id);
        let current_deadline = inner.lease_deadlines.get(&self.shard_id);
        let exact_term = current_lease.is_some_and(|lease| {
            lease.owner_id == self.owner_id
                && lease.epoch == self.epoch
                && lease.operation_id == self.operation_id
                && lease.expires_at_unix_ms == self.expires_at_unix_ms
        }) && current_deadline == Some(&self.monotonic_deadline);
        if !exact_term {
            return Err(OrchError::LeaseGrantSuperseded {
                shard_id: self.shard_id,
                epoch: self.epoch,
            });
        }
        // Sample only after waiting for and inspecting shared state. Otherwise
        // lock contention can carry a stale timestamp past the deadline.
        if clock() >= self.monotonic_deadline {
            return Err(OrchError::LeaseGrantExpired {
                shard_id: self.shard_id,
                epoch: self.epoch,
            });
        }
        let operation = inner
            .operations
            .get(&self.operation_id)
            .ok_or_else(|| OrchError::UnknownOperation(self.operation_id.clone()))?;
        if operation.phase != OperationPhase::Running {
            return Err(OrchError::LeaseGrantSuperseded {
                shard_id: self.shard_id,
                epoch: self.epoch,
            });
        }
        validate_execution_epochs(operation, &self.operation_id, self.epoch, execution)?;
        // Epoch validation is deliberately bracketed too: descheduling during
        // those checks must not return a guard after the local deadline.
        if clock() >= self.monotonic_deadline {
            return Err(OrchError::LeaseGrantExpired {
                shard_id: self.shard_id,
                epoch: self.epoch,
            });
        }
        Ok(LeaseExecutionGuard { grant: self })
    }
}

/// Non-constructible proof of one local lease check at a dispatch boundary.
///
/// This guard is intentionally neither `Clone` nor `Copy`. It is not a promise
/// that time stops after validation; dispatch must carry its fencing epoch to a
/// target that atomically rejects stale work.
#[must_use = "dispatch must carry this guard's fencing epoch to the fenced target"]
pub struct LeaseExecutionGuard<'grant> {
    grant: &'grant LeaseGrant,
}

impl fmt::Debug for LeaseExecutionGuard<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseExecutionGuard")
            .field("shard_id", &self.grant.shard_id)
            .field("epoch", &self.grant.epoch)
            .field("operation_id", &self.grant.operation_id)
            .finish_non_exhaustive()
    }
}

impl LeaseExecutionGuard<'_> {
    /// Returns the shard whose target must enforce this guard.
    #[must_use]
    pub const fn shard_id(&self) -> ShardId {
        self.grant.shard_id
    }

    /// Returns the exact operation admitted by the local check.
    #[must_use]
    pub const fn operation_id(&self) -> &OperationId {
        &self.grant.operation_id
    }

    /// Returns the epoch the receiving target must atomically fence.
    #[must_use]
    pub const fn fencing_epoch(&self) -> u64 {
        self.grant.epoch
    }
}

/// Externally reportable orchestrator status.
#[allow(clippy::struct_excessive_bools)]
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
    /// Whether this process recently observed the exact operator-owned Lease.
    pub coordination_ready: bool,
    /// Whether this process currently holds the Kubernetes leadership Lease.
    pub leader: bool,
    /// Pinned Kubernetes Lease object incarnation.
    pub coordination_lease_uid: Option<String>,
    /// Resource version returned by the latest authoritative Lease operation.
    pub coordination_resource_version: Option<String>,
}

/// Machine-readable reason the orchestrator is not yet safe to serve control
/// operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrchReadinessReason {
    /// The process can observe the exact operator-owned Kubernetes Lease.
    Ready,
    /// No stable operator-assigned identity exists.
    IdentityMissing,
    /// No recent authoritative Kubernetes Lease observation is proven.
    CoordinationUnavailable,
}

/// Fail-closed orchestrator readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OrchReadiness {
    /// True only while the process owns a renewable coordination incarnation.
    pub ready: bool,
    /// Exact reason control operations are unavailable.
    pub reason: OrchReadinessReason,
}

#[derive(Debug, Default)]
struct OrchInner {
    identity: Option<OrchestratorIdentity>,
    operations: HashMap<OperationId, OperationRecord>,
    leases: HashMap<ShardId, ShardLease>,
    // Monotonic deadlines are process-local liveness authority. The Unix
    // expiry remains the external validation and reporting value; wall-clock
    // steps never decide whether the current process still owns the term.
    lease_deadlines: HashMap<ShardId, Duration>,
    last_epochs: HashMap<ShardId, u64>,
    coordination_ready: bool,
    leader: bool,
    coordination_lease_uid: Option<String>,
    coordination_resource_version: Option<String>,
    coordination_deadline: Option<Instant>,
}

/// Thread-safe registry for operation IDs and shard leases.
#[derive(Clone, Debug)]
pub struct OrchState {
    inner: Arc<Mutex<OrchInner>>,
    max_lease_ttl_ms: u64,
    clock_origin: Instant,
}

impl Default for OrchState {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OrchInner::default())),
            max_lease_ttl_ms: DEFAULT_MAX_LEASE_TTL_MS,
            clock_origin: Instant::now(),
        }
    }
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
        if !(1..=MAX_LEASE_TTL_MS).contains(&max_lease_ttl_ms) {
            return Err(OrchError::InvalidMaximumLeaseTtl(max_lease_ttl_ms));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(OrchInner {
                identity: Some(identity),
                ..OrchInner::default()
            })),
            max_lease_ttl_ms,
            clock_origin: Instant::now(),
        })
    }

    /// Returns whether the runtime can safely serve control operations.
    #[must_use]
    pub fn readiness(&self) -> OrchReadiness {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.identity.is_none() {
            return OrchReadiness {
                ready: false,
                reason: OrchReadinessReason::IdentityMissing,
            };
        }
        let coordination_live = inner.coordination_ready
            && inner
                .coordination_deadline
                .is_some_and(|deadline| Instant::now() < deadline);
        if !coordination_live {
            inner.coordination_ready = false;
            inner.leader = false;
            return OrchReadiness {
                ready: false,
                reason: OrchReadinessReason::CoordinationUnavailable,
            };
        }
        OrchReadiness {
            ready: true,
            reason: OrchReadinessReason::Ready,
        }
    }

    /// Installs one acknowledged Kubernetes Lease observation.
    ///
    /// The Lease UID is pinned for the process lifetime. Kubernetes resource
    /// versions are opaque and therefore recorded but never numerically ordered.
    /// A false result leaves readiness and leadership disabled.
    #[must_use]
    pub(crate) fn record_coordination_ready(
        &self,
        lease_uid: &str,
        resource_version: &str,
        leader: bool,
        deadline: Instant,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let valid = !lease_uid.is_empty()
            && lease_uid.len() <= 128
            && !resource_version.is_empty()
            && resource_version.len() <= 128
            && inner
                .coordination_lease_uid
                .as_deref()
                .is_none_or(|current| current == lease_uid)
            && Instant::now() < deadline;
        if !valid {
            inner.coordination_ready = false;
            inner.leader = false;
            return false;
        }
        inner.coordination_lease_uid = Some(lease_uid.to_owned());
        inner.coordination_resource_version = Some(resource_version.to_owned());
        inner.coordination_deadline = Some(deadline);
        inner.coordination_ready = true;
        inner.leader = leader;
        true
    }

    /// Removes readiness without discarding pinned anti-regression evidence.
    pub(crate) fn record_coordination_unavailable(&self) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.coordination_ready = false;
        inner.leader = false;
    }

    /// Removes externally visible readiness before process shutdown begins.
    pub fn begin_shutdown(&self) {
        self.record_coordination_unavailable();
    }

    /// Returns a consistent reportable snapshot.
    #[must_use]
    pub fn snapshot(&self) -> OrchSnapshot {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let coordination_ready = inner.coordination_ready
            && inner
                .coordination_deadline
                .is_some_and(|deadline| Instant::now() < deadline);
        if !coordination_ready {
            inner.coordination_ready = false;
            inner.leader = false;
        }
        let mut leases: Vec<_> = inner.leases.values().cloned().collect();
        leases.sort_by_key(|lease| lease.shard_id);
        OrchSnapshot {
            identity: inner.identity.clone(),
            operation_count: inner.operations.len(),
            leases,
            failover_automation_enabled: false,
            persistence_enabled: false,
            coordination_ready,
            leader: inner.leader,
            coordination_lease_uid: inner.coordination_lease_uid.clone(),
            coordination_resource_version: inner.coordination_resource_version.clone(),
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
    ) -> Result<LeaseGrant, OrchError> {
        self.acquire_lease_with_clock(request, execution, || trusted_clock(self.clock_origin))
    }

    #[cfg(test)]
    fn acquire_lease_at(
        &self,
        request: LeaseRequest,
        execution: ExecutionPreconditions,
        now_unix_micros: u64,
    ) -> Result<LeaseGrant, OrchError> {
        self.acquire_lease_at_clocks(
            request,
            execution,
            now_unix_micros,
            Duration::from_micros(now_unix_micros),
        )
    }

    #[cfg(test)]
    fn acquire_lease_at_clocks(
        &self,
        request: LeaseRequest,
        execution: ExecutionPreconditions,
        now_unix_micros: u64,
        now_monotonic: Duration,
    ) -> Result<LeaseGrant, OrchError> {
        self.acquire_lease_with_sample(
            request,
            execution,
            ClockSample {
                wall_before_unix_micros: now_unix_micros,
                wall_after_unix_micros: now_unix_micros,
                monotonic_before_first_wall: now_monotonic,
                monotonic_between_walls: now_monotonic,
                monotonic_after: now_monotonic,
            },
        )
    }

    #[cfg(test)]
    fn acquire_lease_with_sample(
        &self,
        request: LeaseRequest,
        execution: ExecutionPreconditions,
        sample: ClockSample,
    ) -> Result<LeaseGrant, OrchError> {
        self.acquire_lease_with_clock(request, execution, || Ok(sample))
    }

    fn acquire_lease_with_clock<F>(
        &self,
        request: LeaseRequest,
        execution: ExecutionPreconditions,
        clock: F,
    ) -> Result<LeaseGrant, OrchError>
    where
        F: FnOnce() -> Result<ClockSample, OrchError>,
    {
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
        let now = clock()?;
        validate_execution(
            operation,
            &request,
            execution,
            now.conservative_unix_micros(),
            self.max_lease_ttl_ms,
        )?;
        if let Some((existing, existing_deadline)) =
            live_lease(&inner, request.shard_id, now.monotonic_after)
        {
            let (outcome, monotonic_deadline) = renew_live_lease(
                &mut inner,
                &request,
                existing,
                existing_deadline,
                now.monotonic_after,
                self.max_lease_ttl_ms,
            )?;
            return Ok(self.lease_grant(&request, outcome, monotonic_deadline));
        }
        let monotonic_deadline = now.earliest_deadline(request.expires_at_unix_ms)?;
        validate_monotonic_ttl(
            now.monotonic_after,
            monotonic_deadline,
            self.max_lease_ttl_ms,
        )?;
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
            owner_id: request.owner_id.clone(),
            epoch: request.epoch,
            operation_id: request.operation_id.clone(),
            expires_at_unix_ms: request.expires_at_unix_ms,
        };
        inner.last_epochs.insert(request.shard_id, request.epoch);
        inner.leases.insert(request.shard_id, lease);
        inner
            .lease_deadlines
            .insert(request.shard_id, monotonic_deadline);
        if let Some(operation) = inner.operations.get_mut(&request.operation_id) {
            operation.phase = OperationPhase::Running;
        }
        Ok(self.lease_grant(&request, LeaseOutcome::Acquired, monotonic_deadline))
    }

    fn lease_grant(
        &self,
        request: &LeaseRequest,
        outcome: LeaseOutcome,
        monotonic_deadline: Duration,
    ) -> LeaseGrant {
        LeaseGrant {
            inner: Arc::clone(&self.inner),
            clock_origin: self.clock_origin,
            outcome,
            shard_id: request.shard_id,
            owner_id: request.owner_id.clone(),
            epoch: request.epoch,
            operation_id: request.operation_id.clone(),
            expires_at_unix_ms: request.expires_at_unix_ms,
            monotonic_deadline,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ClockSample {
    wall_before_unix_micros: u64,
    wall_after_unix_micros: u64,
    monotonic_before_first_wall: Duration,
    monotonic_between_walls: Duration,
    monotonic_after: Duration,
}

impl ClockSample {
    const fn conservative_unix_micros(self) -> u64 {
        if self.wall_before_unix_micros >= self.wall_after_unix_micros {
            self.wall_before_unix_micros
        } else {
            self.wall_after_unix_micros
        }
    }

    fn earliest_deadline(self, expires_at_unix_ms: u64) -> Result<Duration, OrchError> {
        let before = self
            .monotonic_before_first_wall
            .checked_add(Duration::from_millis(lease_ttl_ms(
                expires_at_unix_ms,
                self.wall_before_unix_micros,
            )?))
            .ok_or(OrchError::ClockUnavailable)?;
        let after = self
            .monotonic_between_walls
            .checked_add(Duration::from_millis(lease_ttl_ms(
                expires_at_unix_ms,
                self.wall_after_unix_micros,
            )?))
            .ok_or(OrchError::ClockUnavailable)?;
        Ok(before.min(after))
    }
}

fn renew_live_lease(
    inner: &mut OrchInner,
    request: &LeaseRequest,
    existing: ShardLease,
    existing_deadline: Duration,
    now: Duration,
    max_lease_ttl_ms: u64,
) -> Result<(LeaseOutcome, Duration), OrchError> {
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
        return Ok((LeaseOutcome::Existing, existing_deadline));
    }
    let extension_ms = request.expires_at_unix_ms - existing.expires_at_unix_ms;
    let monotonic_deadline = existing_deadline
        .checked_add(Duration::from_millis(extension_ms))
        .ok_or(OrchError::ClockUnavailable)?;
    validate_monotonic_ttl(now, monotonic_deadline, max_lease_ttl_ms)?;
    inner.leases.insert(
        request.shard_id,
        ShardLease {
            expires_at_unix_ms: request.expires_at_unix_ms,
            ..existing
        },
    );
    inner
        .lease_deadlines
        .insert(request.shard_id, monotonic_deadline);
    Ok((LeaseOutcome::Renewed, monotonic_deadline))
}

fn live_lease(
    inner: &OrchInner,
    shard_id: ShardId,
    now: Duration,
) -> Option<(ShardLease, Duration)> {
    let deadline = *inner.lease_deadlines.get(&shard_id)?;
    let lease = inner.leases.get(&shard_id)?.clone();
    (deadline > now).then_some((lease, deadline))
}

fn validate_monotonic_ttl(
    now: Duration,
    deadline: Duration,
    max_lease_ttl_ms: u64,
) -> Result<(), OrchError> {
    let Some(remaining) = deadline.checked_sub(now) else {
        return Err(OrchError::InvalidLeaseRequest);
    };
    if remaining.is_zero() {
        return Err(OrchError::InvalidLeaseRequest);
    }
    if remaining > Duration::from_millis(max_lease_ttl_ms) {
        let requested_ms = u64::try_from(remaining.as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(u64::from(remaining.subsec_nanos() % 1_000_000 != 0));
        return Err(OrchError::LeaseTtlExceeded {
            requested_ms,
            maximum_ms: max_lease_ttl_ms,
        });
    }
    Ok(())
}

fn validate_execution(
    operation: &OperationRecord,
    request: &LeaseRequest,
    execution: ExecutionPreconditions,
    now_unix_micros: u64,
    max_lease_ttl_ms: u64,
) -> Result<(), OrchError> {
    validate_execution_epochs(operation, &request.operation_id, request.epoch, execution)?;
    if now_unix_micros >= operation.spec.deadline_unix_micros {
        return Err(OrchError::OperationDeadlineExceeded {
            operation_id: request.operation_id.clone(),
            deadline_unix_micros: operation.spec.deadline_unix_micros,
            now_unix_micros,
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
    let ttl_ms = lease_ttl_ms(request.expires_at_unix_ms, now_unix_micros)?;
    if ttl_ms > max_lease_ttl_ms {
        return Err(OrchError::LeaseTtlExceeded {
            requested_ms: ttl_ms,
            maximum_ms: max_lease_ttl_ms,
        });
    }
    Ok(())
}

fn lease_ttl_ms(expires_at_unix_ms: u64, now_unix_micros: u64) -> Result<u64, OrchError> {
    let now_unix_ms = now_unix_micros.div_ceil(1_000);
    let Some(ttl_ms) = expires_at_unix_ms.checked_sub(now_unix_ms) else {
        return Err(OrchError::InvalidLeaseRequest);
    };
    if ttl_ms == 0 {
        return Err(OrchError::InvalidLeaseRequest);
    }
    Ok(ttl_ms)
}

fn validate_execution_epochs(
    operation: &OperationRecord,
    operation_id: &OperationId,
    requested_epoch: u64,
    execution: ExecutionPreconditions,
) -> Result<(), OrchError> {
    if operation.spec.required_catalog_epoch != execution.catalog_epoch {
        return Err(OrchError::CatalogEpochMismatch {
            operation_id: operation_id.clone(),
            required: operation.spec.required_catalog_epoch,
            observed: execution.catalog_epoch,
        });
    }
    if operation.spec.required_fencing_epoch != execution.fencing_epoch
        || requested_epoch != execution.fencing_epoch
    {
        return Err(OrchError::ExecutionFencingEpochMismatch {
            operation_id: operation_id.clone(),
            required: operation.spec.required_fencing_epoch,
            observed: execution.fencing_epoch,
            requested: requested_epoch,
        });
    }
    Ok(())
}

fn trusted_clock(origin: Instant) -> Result<ClockSample, OrchError> {
    // Pair each wall read with a preceding monotonic sample. Admission uses the
    // greater wall value, while deadline translation chooses the earlier of the
    // two paired candidates. A pause plus a backward wall step therefore cannot
    // add the pause to the lease term. The final monotonic sample rejects a term
    // consumed anywhere in the sampling window.
    let monotonic_before_first_wall = origin.elapsed();
    let wall_before_unix_micros = unix_clock_micros()?;
    let monotonic_between_walls = origin.elapsed();
    let wall_after_unix_micros = unix_clock_micros()?;
    let monotonic_after = origin.elapsed();
    Ok(ClockSample {
        wall_before_unix_micros,
        wall_after_unix_micros,
        monotonic_before_first_wall,
        monotonic_between_walls,
        monotonic_after,
    })
}

fn unix_clock_micros() -> Result<u64, OrchError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OrchError::ClockUnavailable)?;
    u64::try_from(elapsed.as_micros()).map_err(|_| OrchError::ClockUnavailable)
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
    /// The process clock cannot supply a representable Unix timestamp.
    #[error("orchestrator process clock cannot supply Unix microseconds")]
    ClockUnavailable,
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
    /// An acquisition handle no longer describes the exact installed term.
    #[error("lease grant for shard {shard_id:?} at epoch {epoch} was superseded")]
    LeaseGrantSuperseded {
        /// Shard named by the stale handle.
        shard_id: ShardId,
        /// Epoch named by the stale handle.
        epoch: u64,
    },
    /// A handle reached its process-local monotonic deadline before dispatch.
    #[error("lease grant for shard {shard_id:?} at epoch {epoch} expired before dispatch")]
    LeaseGrantExpired {
        /// Shard whose local term expired.
        shard_id: ShardId,
        /// Expired epoch.
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

    fn execution(fencing_epoch: u64) -> ExecutionPreconditions {
        ExecutionPreconditions {
            catalog_epoch: 7,
            fencing_epoch,
        }
    }

    fn acquire_at(
        state: &OrchState,
        request: LeaseRequest,
        fencing_epoch: u64,
        now_unix_ms: u64,
    ) -> Result<LeaseOutcome, OrchError> {
        state
            .acquire_lease_at(request, execution(fencing_epoch), now_unix_ms * 1_000)
            .map(|grant| grant.outcome())
    }

    fn acquire_at_clocks(
        state: &OrchState,
        request: LeaseRequest,
        fencing_epoch: u64,
        now_unix_ms: u64,
        now_monotonic_ms: u64,
    ) -> Result<LeaseOutcome, OrchError> {
        state
            .acquire_lease_at_clocks(
                request,
                execution(fencing_epoch),
                now_unix_ms * 1_000,
                Duration::from_millis(now_monotonic_ms),
            )
            .map(|grant| grant.outcome())
    }

    fn acquire_in_clock_window(
        state: &OrchState,
        request: LeaseRequest,
        fencing_epoch: u64,
        now_unix_ms: u64,
        monotonic_before_ms: u64,
        monotonic_after_ms: u64,
    ) -> Result<LeaseOutcome, OrchError> {
        state
            .acquire_lease_with_sample(
                request,
                execution(fencing_epoch),
                ClockSample {
                    wall_before_unix_micros: now_unix_ms * 1_000,
                    wall_after_unix_micros: now_unix_ms * 1_000,
                    monotonic_before_first_wall: Duration::from_millis(monotonic_before_ms),
                    monotonic_between_walls: Duration::from_millis(monotonic_before_ms),
                    monotonic_after: Duration::from_millis(monotonic_after_ms),
                },
            )
            .map(|grant| grant.outcome())
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
    fn readiness_requires_identity_and_live_coordination() {
        let unconfigured = OrchState::default();
        assert_eq!(
            unconfigured.readiness().reason,
            OrchReadinessReason::IdentityMissing
        );
        assert_eq!(unconfigured.max_lease_ttl_ms, DEFAULT_MAX_LEASE_TTL_MS);
        assert_eq!(
            state().readiness(),
            OrchReadiness {
                ready: false,
                reason: OrchReadinessReason::CoordinationUnavailable,
            }
        );

        let state = state();
        assert!(state.record_coordination_ready(
            "lease-uid-1",
            "11",
            true,
            Instant::now() + Duration::from_secs(1)
        ));
        assert_eq!(
            state.readiness(),
            OrchReadiness {
                ready: true,
                reason: OrchReadinessReason::Ready,
            }
        );
        assert!(state.snapshot().leader);
        state.record_coordination_unavailable();
        assert!(!state.readiness().ready);
        assert!(!state.snapshot().leader);
        assert!(!state.record_coordination_ready(
            "lease-uid-2",
            "12",
            false,
            Instant::now() + Duration::from_secs(1)
        ));
        assert!(state.record_coordination_ready(
            "lease-uid-1",
            "10",
            false,
            Instant::now() + Duration::from_secs(1)
        ));
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot.coordination_lease_uid.as_deref(),
            Some("lease-uid-1")
        );
        assert_eq!(
            snapshot.coordination_resource_version.as_deref(),
            Some("10")
        );
        assert!(!snapshot.leader);

        let expired = OrchState::with_identity(identity(), 100).expect("valid lease policy");
        assert!(!expired.record_coordination_ready("lease-uid-1", "1", true, Instant::now()));
        assert!(!expired.readiness().ready);

        let paused = OrchState::with_identity(identity(), 100).expect("valid lease policy");
        assert!(paused.record_coordination_ready(
            "lease-uid-1",
            "1",
            true,
            Instant::now() + Duration::from_millis(10)
        ));
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(
            paused.readiness().reason,
            OrchReadinessReason::CoordinationUnavailable
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
            acquire_at(&state, request.clone(), 11, 100).expect("acquire"),
            LeaseOutcome::Acquired
        );
        assert_eq!(
            acquire_at(&state, request, 11, 100).expect("idempotent"),
            LeaseOutcome::Existing
        );
        assert_eq!(
            acquire_at(&state, lease("op-1", "orch-0", 11, 250), 11, 150).expect("renew"),
            LeaseOutcome::Renewed
        );
        assert_eq!(state.snapshot().leases[0].expires_at_unix_ms, 250);
        assert!(matches!(
            acquire_at(&state, lease("op-1", "orch-0", 11, 240), 11, 150),
            Err(OrchError::RegressiveLeaseExpiry { .. })
        ));
        assert!(matches!(
            acquire_at(&state, lease("op-1", "orch-1", 11, 250), 11, 150),
            Err(OrchError::LeaseOwnerMismatch { .. })
        ));

        state
            .register_operation(operation_with_fence("op-2", 1, OperationKind::Backup, 12))
            .expect("register competitor");
        assert!(matches!(
            acquire_at(&state, lease("op-2", "orch-0", 12, 250), 12, 150),
            Err(OrchError::LeaseHeld { .. })
        ));
    }

    #[test]
    fn expired_lease_requires_strictly_higher_epoch() {
        let state = state();
        state
            .register_operation(operation("op-1", 1, OperationKind::Failover))
            .expect("register");
        acquire_at(&state, lease("op-1", "orch-0", 11, 200), 11, 100).expect("acquire");
        assert!(matches!(
            acquire_at(&state, lease("op-1", "orch-0", 11, 300), 11, 200),
            Err(OrchError::StaleEpoch {
                requested: 11,
                minimum: 12
            })
        ));
        state
            .register_operation(operation_with_fence("op-2", 1, OperationKind::Failover, 12))
            .expect("register next term");
        assert_eq!(
            acquire_at(&state, lease("op-2", "orch-0", 12, 300), 12, 200).expect("new term"),
            LeaseOutcome::Acquired
        );
    }

    #[test]
    fn unknown_operation_cannot_acquire_lease() {
        let state = state();
        assert!(matches!(
            acquire_at(&state, lease("missing", "orch-0", 1, 200), 1, 100),
            Err(OrchError::UnknownOperation(_))
        ));
    }

    #[test]
    fn public_lease_liveness_uses_the_process_clock() {
        let state = state();
        state
            .register_operation(operation("expired", 1, OperationKind::Backup))
            .expect("register expired operation");
        assert!(matches!(
            state.acquire_lease(lease("expired", "orch-0", 11, 200), execution(11),),
            Err(OrchError::OperationDeadlineExceeded { .. })
        ));
        assert!(state.snapshot().leases.is_empty());
    }

    #[test]
    fn process_clock_failure_is_fail_closed() {
        let state = state();
        state
            .register_operation(operation("clock-error", 1, OperationKind::Backup))
            .expect("register operation");
        assert!(matches!(
            state.acquire_lease_with_clock(
                lease("clock-error", "orch-0", 11, 200),
                execution(11),
                || Err(OrchError::ClockUnavailable),
            ),
            Err(OrchError::ClockUnavailable)
        ));
        assert!(state.snapshot().leases.is_empty());
    }

    #[test]
    fn sampling_delay_cannot_install_an_already_expired_lease() {
        let state = state();
        state
            .register_operation(operation("delayed", 1, OperationKind::Backup))
            .expect("register operation");

        assert_eq!(
            acquire_in_clock_window(
                &state,
                lease("delayed", "orch-0", 11, 200),
                11,
                100,
                100,
                200,
            ),
            Err(OrchError::InvalidLeaseRequest)
        );
        assert!(state.snapshot().leases.is_empty());
    }

    #[test]
    fn sampling_delay_cannot_renew_a_term_that_expired_inside_the_window() {
        let state = state();
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register first writer");
        state
            .register_operation(operation_with_fence(
                "writer-2",
                1,
                OperationKind::Failover,
                12,
            ))
            .expect("register successor");
        acquire_at_clocks(&state, lease("writer-1", "orch-0", 11, 200), 11, 100, 100)
            .expect("acquire initial lease");

        assert!(matches!(
            acquire_in_clock_window(
                &state,
                lease("writer-1", "orch-0", 11, 250),
                11,
                150,
                150,
                201,
            ),
            Err(OrchError::StaleEpoch {
                requested: 11,
                minimum: 12,
            })
        ));
        assert_eq!(state.snapshot().leases[0].expires_at_unix_ms, 200);
        assert_eq!(
            acquire_at_clocks(&state, lease("writer-2", "orch-0", 12, 301), 12, 201, 201,)
                .expect("expired term admits its successor"),
            LeaseOutcome::Acquired
        );
    }

    #[test]
    fn lease_grant_revalidation_rejects_post_sample_dispatch_delay() {
        let state = state();
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register writer");
        let grant = state
            .acquire_lease_at_clocks(
                lease("writer-1", "orch-0", 11, 200),
                execution(11),
                100_000,
                Duration::from_millis(100),
            )
            .expect("acquire grant");
        assert_eq!(grant.outcome(), LeaseOutcome::Acquired);

        let guard = grant
            .validate_for_execution_at(execution(11), Duration::from_millis(199))
            .expect("term remains live immediately before its deadline");
        assert_eq!(guard.shard_id(), ShardId(1));
        assert_eq!(guard.operation_id(), &OperationId("writer-1".to_owned()));
        assert_eq!(guard.fencing_epoch(), 11);
        assert!(matches!(
            grant.validate_for_execution_at(execution(11), Duration::from_millis(200)),
            Err(OrchError::LeaseGrantExpired {
                shard_id: ShardId(1),
                epoch: 11,
            })
        ));
    }

    #[test]
    fn dispatch_clock_is_sampled_while_state_is_locked() {
        let state = state();
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register writer");
        let grant = state
            .acquire_lease_at_clocks(
                lease("writer-1", "orch-0", 11, 200),
                execution(11),
                100_000,
                Duration::from_millis(100),
            )
            .expect("acquire grant");
        let inner = Arc::clone(&grant.inner);

        let _guard = grant
            .validate_for_execution_with_clock(execution(11), || {
                assert!(matches!(
                    inner.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ));
                Duration::from_millis(199)
            })
            .expect("clock read occurs after locking shared state");
    }

    #[test]
    fn mutex_wait_cannot_revalidate_a_term_past_its_deadline() {
        let state = state();
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register writer");
        let grant = state
            .acquire_lease_at_clocks(
                lease("writer-1", "orch-0", 11, 200),
                execution(11),
                100_000,
                Duration::from_millis(100),
            )
            .expect("acquire grant");
        let now_ms = Arc::new(std::sync::atomic::AtomicU64::new(199));
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);

        std::thread::scope(|scope| {
            let state_lock = state
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let validation_now = Arc::clone(&now_ms);
            let validation = scope.spawn(move || {
                started_tx.send(()).expect("announce validation attempt");
                grant
                    .validate_for_execution_with_clock(execution(11), || {
                        Duration::from_millis(
                            validation_now.load(std::sync::atomic::Ordering::SeqCst),
                        )
                    })
                    .map(|_| ())
            });
            started_rx.recv().expect("validation thread started");
            std::thread::sleep(Duration::from_millis(20));
            now_ms.store(200, std::sync::atomic::Ordering::SeqCst);
            drop(state_lock);

            assert!(matches!(
                validation.join().expect("validation thread joins"),
                Err(OrchError::LeaseGrantExpired { epoch: 11, .. })
            ));
        });
    }

    #[test]
    fn pause_and_backward_wall_step_cannot_extend_new_term() {
        let state = state();
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register writer");
        let grant = state
            .acquire_lease_with_sample(
                lease("writer-1", "orch-0", 11, 200),
                execution(11),
                ClockSample {
                    wall_before_unix_micros: 100_000,
                    wall_after_unix_micros: 50_000,
                    monotonic_before_first_wall: Duration::from_millis(100),
                    monotonic_between_walls: Duration::from_millis(150),
                    monotonic_after: Duration::from_millis(150),
                },
            )
            .expect("use the earliest paired deadline");

        assert!(
            grant
                .validate_for_execution_at(execution(11), Duration::from_millis(199))
                .is_ok()
        );
        assert!(matches!(
            grant.validate_for_execution_at(execution(11), Duration::from_millis(200)),
            Err(OrchError::LeaseGrantExpired { epoch: 11, .. })
        ));
    }

    #[test]
    fn renewed_or_replaced_term_supersedes_older_grants() {
        let state = OrchState::with_identity(identity(), 1_000).expect("valid lease policy");
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register first writer");
        state
            .register_operation(operation_with_fence(
                "writer-2",
                1,
                OperationKind::Failover,
                12,
            ))
            .expect("register successor");
        let original = state
            .acquire_lease_at_clocks(
                lease("writer-1", "orch-0", 11, 200),
                execution(11),
                100_000,
                Duration::from_millis(100),
            )
            .expect("acquire original term");
        let renewed = state
            .acquire_lease_at_clocks(
                lease("writer-1", "orch-0", 11, 250),
                execution(11),
                150_000,
                Duration::from_millis(150),
            )
            .expect("renew term");
        assert_eq!(renewed.outcome(), LeaseOutcome::Renewed);
        assert!(matches!(
            original.validate_for_execution_at(execution(11), Duration::from_millis(150)),
            Err(OrchError::LeaseGrantSuperseded { epoch: 11, .. })
        ));
        assert!(
            renewed
                .validate_for_execution_at(execution(11), Duration::from_millis(199))
                .is_ok()
        );

        let successor = state
            .acquire_lease_at_clocks(
                lease("writer-2", "orch-0", 12, 350),
                execution(12),
                250_000,
                Duration::from_millis(250),
            )
            .expect("replace expired term");
        assert_eq!(successor.outcome(), LeaseOutcome::Acquired);
        assert!(matches!(
            renewed.validate_for_execution_at(execution(11), Duration::from_millis(250)),
            Err(OrchError::LeaseGrantSuperseded { epoch: 11, .. })
        ));
    }

    #[test]
    fn lease_grant_rechecks_execution_epochs_at_dispatch() {
        let state = state();
        state
            .register_operation(operation("writer-1", 1, OperationKind::Ddl))
            .expect("register operation");
        let grant = state
            .acquire_lease_at_clocks(
                lease("writer-1", "orch-0", 11, 200),
                execution(11),
                100_000,
                Duration::from_millis(100),
            )
            .expect("acquire grant");
        let mut wrong_catalog = execution(11);
        wrong_catalog.catalog_epoch = 8;
        assert!(matches!(
            grant.validate_for_execution_at(wrong_catalog, Duration::from_millis(101)),
            Err(OrchError::CatalogEpochMismatch { .. })
        ));
        assert!(matches!(
            grant.validate_for_execution_at(execution(12), Duration::from_millis(101)),
            Err(OrchError::ExecutionFencingEpochMismatch { .. })
        ));
    }

    #[test]
    fn concurrent_dual_writers_cannot_both_acquire_one_shard() {
        let state = state();
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register first writer");
        state
            .register_operation(operation_with_fence(
                "writer-2",
                1,
                OperationKind::Failover,
                12,
            ))
            .expect("register second writer");
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let results = std::thread::scope(|scope| {
            let first_state = state.clone();
            let first_barrier = Arc::clone(&barrier);
            let first = scope.spawn(move || {
                first_barrier.wait();
                acquire_at(&first_state, lease("writer-1", "orch-0", 11, 200), 11, 100)
            });
            let second_state = state.clone();
            let second = scope.spawn(move || {
                barrier.wait();
                acquire_at(&second_state, lease("writer-2", "orch-0", 12, 200), 12, 100)
            });
            [
                first.join().expect("first writer thread"),
                second.join().expect("second writer thread"),
            ]
        });
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Ok(LeaseOutcome::Acquired)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(OrchError::LeaseHeld { .. })))
                .count(),
            1
        );
        assert_eq!(state.snapshot().leases.len(), 1);
    }

    #[test]
    fn a_forward_wall_step_during_renewal_cannot_cut_a_live_lease_short() {
        let state = state();
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register first writer");
        state
            .register_operation(operation_with_fence(
                "writer-2",
                1,
                OperationKind::Failover,
                12,
            ))
            .expect("register successor");
        acquire_at_clocks(&state, lease("writer-1", "orch-0", 11, 200), 11, 100, 100)
            .expect("acquire initial lease");
        assert_eq!(
            acquire_at_clocks(&state, lease("writer-1", "orch-0", 11, 201), 11, 190, 150,)
                .expect("renew after a forward wall-clock step"),
            LeaseOutcome::Renewed
        );

        assert!(matches!(
            acquire_at_clocks(&state, lease("writer-2", "orch-0", 12, 400), 12, 300, 200,),
            Err(OrchError::LeaseHeld { epoch: 11, .. })
        ));
        assert_eq!(state.snapshot().leases[0].epoch, 11);
    }

    #[test]
    fn a_backward_wall_step_during_renewal_cannot_extend_a_live_lease() {
        let state = OrchState::with_identity(identity(), 1_000).expect("valid lease policy");
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register first writer");
        state
            .register_operation(operation_with_fence(
                "writer-2",
                1,
                OperationKind::Failover,
                12,
            ))
            .expect("register successor");
        acquire_at_clocks(&state, lease("writer-1", "orch-0", 11, 600), 11, 500, 100)
            .expect("acquire initial lease");
        assert_eq!(
            acquire_at_clocks(&state, lease("writer-1", "orch-0", 11, 601), 11, 400, 150,)
                .expect("renew after a backward wall-clock step"),
            LeaseOutcome::Renewed
        );

        assert_eq!(
            acquire_at_clocks(&state, lease("writer-2", "orch-0", 12, 601), 12, 401, 202,)
                .expect("monotonic expiry admits the higher epoch"),
            LeaseOutcome::Acquired
        );
        assert_eq!(state.snapshot().leases[0].epoch, 12);
    }

    #[test]
    fn renewal_cannot_move_the_monotonic_deadline_beyond_policy() {
        let state = state();
        state
            .register_operation(operation("writer-1", 1, OperationKind::Failover))
            .expect("register writer");
        acquire_at_clocks(&state, lease("writer-1", "orch-0", 11, 200), 11, 100, 100)
            .expect("acquire initial lease");

        assert_eq!(
            acquire_at_clocks(&state, lease("writer-1", "orch-0", 11, 400), 11, 300, 150,),
            Err(OrchError::LeaseTtlExceeded {
                requested_ms: 250,
                maximum_ms: 100,
            })
        );
        assert_eq!(state.snapshot().leases[0].expires_at_unix_ms, 200);
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
        acquire_at(
            &state,
            lease("op-1", "orch-0", u64::MAX - 1, 200),
            u64::MAX - 1,
            100,
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

        let mut wrong_catalog = execution(11);
        wrong_catalog.catalog_epoch = 8;
        assert!(matches!(
            state.acquire_lease_at(request.clone(), wrong_catalog, 100_000),
            Err(OrchError::CatalogEpochMismatch { .. })
        ));
        assert!(matches!(
            state.acquire_lease_at(request.clone(), execution(12), 100_000),
            Err(OrchError::ExecutionFencingEpochMismatch { .. })
        ));
        assert!(matches!(
            state.acquire_lease_at(request, execution(11), 1_000_000),
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
            acquire_at(&state, lease("op-1", "orch-0", 11, 100), 11, 100),
            Err(OrchError::InvalidLeaseRequest)
        );
        assert_eq!(
            acquire_at(&state, lease("op-1", "orch-0", 11, 201), 11, 100),
            Err(OrchError::LeaseTtlExceeded {
                requested_ms: 101,
                maximum_ms: 100,
            })
        );
        assert!(matches!(
            acquire_at(&state, lease("op-1", "orch-0", 11, u64::MAX), 11, 100),
            Err(OrchError::LeasePastOperationDeadline { .. })
        ));
        assert!(matches!(
            acquire_at(&state, lease("op-1", "orch-0", 11, 1_001), 11, 900),
            Err(OrchError::LeasePastOperationDeadline { .. })
        ));
    }
}
