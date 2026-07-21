//! In-memory operation identity and conservative per-shard lease ownership.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pgshard_types::ShardId;
use serde::{Serialize, Serializer};
use thiserror::Error;

use crate::agent_status::{ReplicationCorrelationSummary, ShardZeroReplicationProof};
use crate::boottime::{
    BoottimeClock, BoottimeError, SuspendAwareInstant, system_clock as system_boottime_clock,
};
use crate::catalog_candidate::BoundCandidateSet;
use crate::catalog_materialization::{
    CatalogBootstrapDispatch, CatalogMaterializationCapability,
    issue_catalog_materialization_capability as issue_catalog_capability,
    prepare_catalog_bootstrap_dispatch as prepare_catalog_dispatch,
    revalidate_catalog_bootstrap_dispatch as revalidate_catalog_dispatch,
    revalidate_catalog_materialization_capability as revalidate_catalog_capability,
};
use crate::topology::TopologyDiagnostics;

const DEFAULT_MAX_LEASE_TTL_MS: u64 = 15_000;
const MAX_LEASE_TTL_MS: u64 = 300_000;

pub(crate) trait IntoSuspendAwareDeadline {
    fn into_suspend_aware(self, now: SuspendAwareInstant) -> Option<SuspendAwareInstant>;
}

impl IntoSuspendAwareDeadline for SuspendAwareInstant {
    fn into_suspend_aware(self, _now: SuspendAwareInstant) -> Option<SuspendAwareInstant> {
        Some(self)
    }
}

#[cfg(test)]
impl IntoSuspendAwareDeadline for Instant {
    fn into_suspend_aware(self, now: SuspendAwareInstant) -> Option<SuspendAwareInstant> {
        let remaining = self.checked_duration_since(now.monotonic)?;
        now.checked_add(remaining)
    }
}

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
    /// Validated operator-published discovery topology, when configured.
    pub topology: Option<TopologyDiagnostics>,
    /// Freshness-bounded, diagnostic-only agent-status collection summary.
    pub agent_status: AgentStatusDiagnostics,
    /// Freshness-bounded, diagnostic-only catalog-candidate summary.
    pub catalog_candidates: CatalogCandidateDiagnostics,
    /// Joint, diagnostic-only correlation of live catalog bootstrap inputs.
    pub catalog_bootstrap_observation: CatalogBootstrapObservationDiagnostics,
}

/// Lifecycle of diagnostic-only remote agent-status evidence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatusPhase {
    /// The explicit runtime contract for agent observation is absent.
    #[default]
    Disabled,
    /// A new all-members collection is in progress; no older evidence remains.
    Collecting,
    /// Every expected member was validated within one live monotonic window.
    Fresh,
    /// The latest collection failed closed without publishing partial evidence.
    Unavailable,
    /// The last complete collection passed its process-local freshness deadline.
    Expired,
    /// Process shutdown cleared all agent evidence.
    ShuttingDown,
}

/// Stable failure classes for diagnostic-only collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatusFailureReason {
    /// Kubernetes identity or topology evidence was unavailable or invalid.
    IdentityUnavailable,
    /// One direct agent request or response was unavailable or invalid.
    StatusUnavailable,
    /// Kubernetes identity changed across the request bracket.
    IdentityChanged,
    /// The complete collection could not be published before its deadline.
    FreshnessExpired,
}

/// Bounded summary only; raw remote status is never retained.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AgentStatusDiagnostics {
    /// Current collection lifecycle.
    pub phase: AgentStatusPhase,
    /// Finite member count required for an atomic complete result.
    pub expected_members: usize,
    /// Complete fresh member count, or zero when no complete result is live.
    pub fresh_members: usize,
    /// Shards whose complete agent evidence was internally correlated.
    pub replication_correlated_shards: usize,
    /// Correlated shards whose source reported one exact target-fence ACK.
    pub target_fence_acknowledged_shards: usize,
    /// Correlated shards with an exact any-one remote-apply barrier witness.
    pub remote_apply_witnessed_shards: usize,
    /// Configured process-local maximum receipt age in milliseconds.
    pub maximum_age_ms: u64,
    /// Latest stable failure class, if any.
    pub failure: Option<AgentStatusFailureReason>,
    /// Explicitly states that this evidence grants no authority.
    pub diagnostic_only: bool,
}

impl Default for AgentStatusDiagnostics {
    fn default() -> Self {
        Self {
            phase: AgentStatusPhase::Disabled,
            expected_members: 0,
            fresh_members: 0,
            replication_correlated_shards: 0,
            target_fence_acknowledged_shards: 0,
            remote_apply_witnessed_shards: 0,
            maximum_age_ms: 0,
            failure: None,
            diagnostic_only: true,
        }
    }
}

/// Lifecycle of diagnostic-only catalog-candidate evidence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogCandidatePhase {
    /// The explicit multi-member runtime contract is absent.
    #[default]
    Disabled,
    /// A new complete observation is in progress; no older evidence remains.
    Collecting,
    /// Every expected immutable candidate is fresh and exactly bound.
    Fresh,
    /// The latest complete observation failed closed.
    Unavailable,
    /// The process-local freshness window elapsed.
    Expired,
    /// Process shutdown cleared all candidate evidence.
    ShuttingDown,
}

/// Stable, bounded candidate-observation failure classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogCandidateFailureReason {
    /// The exact `PgShardCluster` status subresource could not be read.
    ClusterStatusUnavailable,
    /// One exact candidate `ConfigMap` could not be read.
    CandidateUnavailable,
    /// Status or a candidate object changed across the read bracket.
    EvidenceChanged,
    /// An identity, checkpoint, metadata, payload, or digest was invalid.
    ValidationFailed,
    /// The complete observation exceeded its monotonic freshness bound.
    FreshnessExpired,
}

/// Bounded summary only; names, UIDs, digests, and payloads are never retained.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CatalogCandidateDiagnostics {
    /// Current observation lifecycle.
    pub phase: CatalogCandidatePhase,
    /// Exact candidate cardinality required for a complete publication.
    pub expected_candidates: usize,
    /// Complete fresh candidate count, or zero without live evidence.
    pub fresh_candidates: usize,
    /// Configured process-local maximum age in milliseconds.
    pub maximum_age_ms: u64,
    /// Latest stable failure class, if any.
    pub failure: Option<CatalogCandidateFailureReason>,
    /// Explicitly states that this evidence grants no authority.
    pub diagnostic_only: bool,
}

impl Default for CatalogCandidateDiagnostics {
    fn default() -> Self {
        Self {
            phase: CatalogCandidatePhase::Disabled,
            expected_candidates: 0,
            fresh_candidates: 0,
            maximum_age_ms: 0,
            failure: None,
            diagnostic_only: true,
        }
    }
}

/// Lifecycle of the joint, diagnostic-only catalog bootstrap observation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogBootstrapObservationPhase {
    /// The explicit multi-member observation contract is absent.
    #[default]
    Disabled,
    /// At least one required input is being collected without last-good use.
    Collecting,
    /// Live candidates and shard-zero replication evidence are correlated.
    Correlated,
    /// A required input is unavailable or shard-zero evidence is absent.
    Unavailable,
    /// At least one required monotonic input deadline elapsed.
    Expired,
    /// Process shutdown cleared the joint observation.
    ShuttingDown,
}

/// Stable, bounded failure classes for the joint bootstrap observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogBootstrapObservationFailureReason {
    /// Live agent-status evidence is unavailable.
    AgentStatusUnavailable,
    /// Live catalog-candidate evidence is unavailable.
    CatalogCandidatesUnavailable,
    /// One required process-local freshness deadline elapsed.
    FreshnessExpired,
    /// Every shard-zero member reported no replication evidence.
    ShardZeroReplicationEvidenceUnavailable,
}

/// Bounded joint summary; it contains no identities, generations, or LSNs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[allow(clippy::struct_excessive_bools)] // Exact independent status facts are intentional.
pub struct CatalogBootstrapObservationDiagnostics {
    /// Current joint observation lifecycle.
    pub phase: CatalogBootstrapObservationPhase,
    /// Exact shard-zero candidate cardinality expected from the live input.
    pub expected_candidates: usize,
    /// Currently live candidate count, independent of agent evidence outcome.
    pub fresh_candidates: usize,
    /// Whether complete live shard-zero replication evidence was correlated.
    pub shard_zero_replication_correlated: bool,
    /// Whether the correlated live shard-zero source acknowledged its target fence.
    pub shard_zero_target_fence_acknowledged: bool,
    /// Whether one correlated live shard-zero standby witnessed remote apply.
    pub shard_zero_remote_apply_witnessed: bool,
    /// Latest stable joint failure class, if any.
    pub failure: Option<CatalogBootstrapObservationFailureReason>,
    /// Explicitly states that this observation grants no authority.
    pub diagnostic_only: bool,
}

impl Default for CatalogBootstrapObservationDiagnostics {
    fn default() -> Self {
        Self {
            phase: CatalogBootstrapObservationPhase::Disabled,
            expected_candidates: 0,
            fresh_candidates: 0,
            shard_zero_replication_correlated: false,
            shard_zero_target_fence_acknowledged: false,
            shard_zero_remote_apply_witnessed: false,
            failure: None,
            diagnostic_only: true,
        }
    }
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
    coordination_deadline: Option<SuspendAwareInstant>,
    coordination_generation: u64,
    // Diagnostic-only identity evidence is never retained past this process-
    // local monotonic deadline and never participates in authority decisions.
    agent_identity_binding_deadline: Option<SuspendAwareInstant>,
    agent_status: AgentStatusDiagnostics,
    agent_replication_correlation: Option<ReplicationCorrelationSummary>,
    agent_shard_zero_replication_proof: Option<ShardZeroReplicationProof>,
    agent_proof_generation: u64,
    // Catalog-candidate evidence is independently diagnostic and cannot alter
    // coordination, agent status, readiness, leadership, or operation state.
    catalog_candidate_deadline: Option<SuspendAwareInstant>,
    catalog_candidate_proof: Option<BoundCandidateSet>,
    catalog_proof_generation: u64,
    catalog_candidates: CatalogCandidateDiagnostics,
    topology: Option<TopologyDiagnostics>,
}

fn advance_generation(generation: &mut u64) -> bool {
    let Some(next) = generation.checked_add(1) else {
        return false;
    };
    *generation = next;
    true
}

fn clear_agent_proof(inner: &mut OrchInner) -> bool {
    inner.agent_shard_zero_replication_proof = None;
    inner.agent_identity_binding_deadline = None;
    advance_generation(&mut inner.agent_proof_generation)
}

fn clear_catalog_proof(inner: &mut OrchInner) -> bool {
    inner.catalog_candidate_proof = None;
    inner.catalog_candidate_deadline = None;
    advance_generation(&mut inner.catalog_proof_generation)
}

fn expire_coordination_state(inner: &mut OrchInner, now: Option<SuspendAwareInstant>) -> bool {
    let coordination_ready = inner.coordination_ready
        && inner
            .coordination_deadline
            .zip(now)
            .is_some_and(|(deadline, now)| deadline.is_live_at(now));
    if !coordination_ready {
        if inner.coordination_ready || inner.leader {
            let _ = advance_generation(&mut inner.coordination_generation);
        }
        inner.coordination_ready = false;
        inner.leader = false;
        inner.coordination_deadline = None;
    }
    coordination_ready
}

fn expire_proof_state(inner: &mut OrchInner, now: Option<SuspendAwareInstant>) {
    let identity_binding_live = inner
        .agent_identity_binding_deadline
        .zip(now)
        .is_some_and(|(deadline, now)| deadline.is_live_at(now));
    if !identity_binding_live {
        inner.agent_identity_binding_deadline = None;
        if inner.agent_status.phase == AgentStatusPhase::Fresh {
            let _ = clear_agent_proof(inner);
            inner.agent_replication_correlation = None;
            inner.agent_status.phase = AgentStatusPhase::Expired;
            inner.agent_status.fresh_members = 0;
            inner.agent_status.replication_correlated_shards = 0;
            inner.agent_status.target_fence_acknowledged_shards = 0;
            inner.agent_status.remote_apply_witnessed_shards = 0;
            inner.agent_status.failure = Some(AgentStatusFailureReason::FreshnessExpired);
        }
        if let Some(topology) = &mut inner.topology
            && topology.agent_status_collection
                != crate::topology::AgentStatusCollectionState::DisabledAgentRuntimeRequired
        {
            topology.agent_status_collection =
                crate::topology::AgentStatusCollectionState::DisabledPodIdentityRequired;
        }
    }

    let catalog_candidate_live = inner
        .catalog_candidate_deadline
        .zip(now)
        .is_some_and(|(deadline, now)| deadline.is_live_at(now));
    if !catalog_candidate_live {
        inner.catalog_candidate_deadline = None;
        if inner.catalog_candidates.phase == CatalogCandidatePhase::Fresh {
            let _ = clear_catalog_proof(inner);
            inner.catalog_candidates.phase = CatalogCandidatePhase::Expired;
            inner.catalog_candidates.fresh_candidates = 0;
            inner.catalog_candidates.failure =
                Some(CatalogCandidateFailureReason::FreshnessExpired);
        }
    }
}

fn catalog_bootstrap_observation(inner: &OrchInner) -> CatalogBootstrapObservationDiagnostics {
    let expected_candidates = inner.catalog_candidates.expected_candidates;
    let fresh_candidates = inner.catalog_candidates.fresh_candidates;
    let replication = inner.agent_replication_correlation;
    let (
        phase,
        failure,
        shard_zero_replication_correlated,
        shard_zero_target_fence_acknowledged,
        shard_zero_remote_apply_witnessed,
    ) = if inner.agent_status.phase == AgentStatusPhase::ShuttingDown
        || inner.catalog_candidates.phase == CatalogCandidatePhase::ShuttingDown
    {
        (
            CatalogBootstrapObservationPhase::ShuttingDown,
            None,
            false,
            false,
            false,
        )
    } else if inner.agent_status.phase == AgentStatusPhase::Disabled
        || inner.catalog_candidates.phase == CatalogCandidatePhase::Disabled
    {
        (
            CatalogBootstrapObservationPhase::Disabled,
            None,
            false,
            false,
            false,
        )
    } else if inner.agent_status.phase == AgentStatusPhase::Unavailable {
        (
            CatalogBootstrapObservationPhase::Unavailable,
            Some(CatalogBootstrapObservationFailureReason::AgentStatusUnavailable),
            false,
            false,
            false,
        )
    } else if inner.catalog_candidates.phase == CatalogCandidatePhase::Unavailable {
        (
            CatalogBootstrapObservationPhase::Unavailable,
            Some(CatalogBootstrapObservationFailureReason::CatalogCandidatesUnavailable),
            false,
            false,
            false,
        )
    } else if inner.agent_status.phase == AgentStatusPhase::Expired
        || inner.catalog_candidates.phase == CatalogCandidatePhase::Expired
    {
        (
            CatalogBootstrapObservationPhase::Expired,
            Some(CatalogBootstrapObservationFailureReason::FreshnessExpired),
            false,
            false,
            false,
        )
    } else if inner.agent_status.phase == AgentStatusPhase::Collecting
        || inner.catalog_candidates.phase == CatalogCandidatePhase::Collecting
    {
        (
            CatalogBootstrapObservationPhase::Collecting,
            None,
            false,
            false,
            false,
        )
    } else if inner.agent_status.phase == AgentStatusPhase::Fresh
        && inner.catalog_candidates.phase == CatalogCandidatePhase::Fresh
        && replication.is_some_and(|summary| summary.shard_zero_correlated)
    {
        (
            CatalogBootstrapObservationPhase::Correlated,
            None,
            true,
            replication.is_some_and(|summary| summary.shard_zero_target_fence_acknowledged),
            replication.is_some_and(|summary| summary.shard_zero_remote_apply_witnessed),
        )
    } else {
        (
            CatalogBootstrapObservationPhase::Unavailable,
            Some(CatalogBootstrapObservationFailureReason::ShardZeroReplicationEvidenceUnavailable),
            false,
            false,
            false,
        )
    };
    CatalogBootstrapObservationDiagnostics {
        phase,
        expected_candidates,
        fresh_candidates,
        shard_zero_replication_correlated,
        shard_zero_target_fence_acknowledged,
        shard_zero_remote_apply_witnessed,
        failure,
        diagnostic_only: true,
    }
}

/// Thread-safe registry for operation IDs and shard leases.
#[derive(Clone)]
pub struct OrchState {
    inner: Arc<Mutex<OrchInner>>,
    max_lease_ttl_ms: u64,
    clock_origin: Instant,
    boottime: Arc<dyn BoottimeClock>,
}

impl fmt::Debug for OrchState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrchState")
            .field("inner", &"<redacted>")
            .field("max_lease_ttl_ms", &self.max_lease_ttl_ms)
            .finish_non_exhaustive()
    }
}

impl Default for OrchState {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OrchInner::default())),
            max_lease_ttl_ms: DEFAULT_MAX_LEASE_TTL_MS,
            clock_origin: Instant::now(),
            boottime: system_boottime_clock(),
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
        Self::with_identity_and_optional_topology(identity, max_lease_ttl_ms, None)
    }

    /// Creates state with a configured process identity and one already
    /// validated, diagnostic-only discovery topology.
    ///
    /// Topology presence does not affect readiness, leadership, or operation
    /// authority.
    ///
    /// # Errors
    ///
    /// Returns [`OrchError::InvalidMaximumLeaseTtl`] for a zero or unbounded
    /// policy.
    pub fn with_identity_and_topology(
        identity: OrchestratorIdentity,
        max_lease_ttl_ms: u64,
        topology: TopologyDiagnostics,
    ) -> Result<Self, OrchError> {
        Self::with_identity_and_optional_topology(identity, max_lease_ttl_ms, Some(topology))
    }

    fn with_identity_and_optional_topology(
        identity: OrchestratorIdentity,
        max_lease_ttl_ms: u64,
        topology: Option<TopologyDiagnostics>,
    ) -> Result<Self, OrchError> {
        Self::with_identity_optional_topology_and_clock(
            identity,
            max_lease_ttl_ms,
            topology,
            system_boottime_clock(),
        )
    }

    fn with_identity_optional_topology_and_clock(
        identity: OrchestratorIdentity,
        max_lease_ttl_ms: u64,
        topology: Option<TopologyDiagnostics>,
        boottime: Arc<dyn BoottimeClock>,
    ) -> Result<Self, OrchError> {
        if !(1..=MAX_LEASE_TTL_MS).contains(&max_lease_ttl_ms) {
            return Err(OrchError::InvalidMaximumLeaseTtl(max_lease_ttl_ms));
        }
        let agent_status = match topology.as_ref() {
            Some(topology) => AgentStatusDiagnostics {
                phase: if topology.agent_status_collection
                    == crate::topology::AgentStatusCollectionState::DisabledAgentRuntimeRequired
                {
                    AgentStatusPhase::Disabled
                } else {
                    AgentStatusPhase::Unavailable
                },
                expected_members: topology.member_count,
                ..AgentStatusDiagnostics::default()
            },
            None => AgentStatusDiagnostics::default(),
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(OrchInner {
                identity: Some(identity),
                agent_status,
                catalog_candidates: CatalogCandidateDiagnostics::default(),
                topology,
                ..OrchInner::default()
            })),
            max_lease_ttl_ms,
            clock_origin: Instant::now(),
            boottime,
        })
    }

    pub(crate) fn suspend_aware_now(&self) -> Result<SuspendAwareInstant, BoottimeError> {
        self.suspend_aware_now_with(Instant::now)
    }

    pub(crate) fn suspend_aware_now_with<F>(
        &self,
        mut monotonic_clock: F,
    ) -> Result<SuspendAwareInstant, BoottimeError>
    where
        F: FnMut() -> Instant,
    {
        // BOOTTIME is sampled last so a suspend between clock reads is visible
        // to liveness checks. Pre-I/O callers do not dispatch until both
        // samples exist, so the ordering cannot extend an in-flight window.
        let monotonic = monotonic_clock();
        let boottime = self.boottime.now()?;
        Ok(SuspendAwareInstant {
            monotonic,
            boottime,
        })
    }

    pub(crate) fn suspend_aware_deadline_after(
        &self,
        duration: Duration,
    ) -> Result<SuspendAwareInstant, BoottimeError> {
        self.suspend_aware_now()?
            .checked_add(duration)
            .ok_or(BoottimeError::InvalidTimestamp)
    }

    #[cfg(test)]
    pub(crate) fn with_identity_and_clock_for_test(
        identity: OrchestratorIdentity,
        max_lease_ttl_ms: u64,
        boottime: Arc<dyn BoottimeClock>,
    ) -> Result<Self, OrchError> {
        Self::with_identity_optional_topology_and_clock(identity, max_lease_ttl_ms, None, boottime)
    }

    #[cfg(test)]
    pub(crate) fn with_identity_and_topology_and_clock_for_test(
        identity: OrchestratorIdentity,
        max_lease_ttl_ms: u64,
        topology: TopologyDiagnostics,
        boottime: Arc<dyn BoottimeClock>,
    ) -> Result<Self, OrchError> {
        Self::with_identity_optional_topology_and_clock(
            identity,
            max_lease_ttl_ms,
            Some(topology),
            boottime,
        )
    }

    /// Returns whether the runtime can safely serve control operations.
    #[must_use]
    pub fn readiness(&self) -> OrchReadiness {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Sample only after acquiring the state mutex. Otherwise contention
        // could carry a stale timestamp across an evidence deadline.
        let coordination_live =
            expire_coordination_state(&mut inner, self.suspend_aware_now().ok());
        if inner.identity.is_none() {
            return OrchReadiness {
                ready: false,
                reason: OrchReadinessReason::IdentityMissing,
            };
        }
        if !coordination_live {
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
    pub(crate) fn record_coordination_ready<D: IntoSuspendAwareDeadline>(
        &self,
        lease_uid: &str,
        resource_version: &str,
        leader: bool,
        deadline: D,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.suspend_aware_now().ok();
        let deadline = now.and_then(|now| deadline.into_suspend_aware(now));
        let generation_advanced = advance_generation(&mut inner.coordination_generation);
        let valid = generation_advanced
            && !lease_uid.is_empty()
            && lease_uid.len() <= 128
            && !resource_version.is_empty()
            && resource_version.len() <= 128
            && inner
                .coordination_lease_uid
                .as_deref()
                .is_none_or(|current| current == lease_uid)
            && deadline
                .zip(now)
                .is_some_and(|(deadline, now)| deadline.is_live_at(now));
        if !valid {
            inner.coordination_ready = false;
            inner.leader = false;
            inner.coordination_deadline = None;
            return false;
        }
        inner.coordination_lease_uid = Some(lease_uid.to_owned());
        inner.coordination_resource_version = Some(resource_version.to_owned());
        inner.coordination_deadline = deadline;
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
        let _ = advance_generation(&mut inner.coordination_generation);
        inner.coordination_ready = false;
        inner.leader = false;
        inner.coordination_deadline = None;
    }

    /// Clears earlier evidence before a new all-members collection begins.
    pub(crate) fn record_agent_status_collecting(&self, maximum_age: Duration) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.agent_status.phase == AgentStatusPhase::ShuttingDown {
            return;
        }
        let _ = clear_agent_proof(&mut inner);
        inner.agent_identity_binding_deadline = None;
        inner.agent_replication_correlation = None;
        inner.agent_status.phase = AgentStatusPhase::Collecting;
        inner.agent_status.fresh_members = 0;
        inner.agent_status.replication_correlated_shards = 0;
        inner.agent_status.target_fence_acknowledged_shards = 0;
        inner.agent_status.remote_apply_witnessed_shards = 0;
        inner.agent_status.maximum_age_ms =
            u64::try_from(maximum_age.as_millis()).unwrap_or(u64::MAX);
        inner.agent_status.failure = None;
        if let Some(topology) = &mut inner.topology
            && topology.agent_status_collection
                != crate::topology::AgentStatusCollectionState::DisabledAgentRuntimeRequired
        {
            topology.agent_status_collection =
                crate::topology::AgentStatusCollectionState::DisabledPodIdentityRequired;
        }
    }

    /// Records a failed-closed collection without retaining partial status.
    pub(crate) fn record_agent_status_failure(&self, failure: AgentStatusFailureReason) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.agent_status.phase == AgentStatusPhase::ShuttingDown {
            return;
        }
        let _ = clear_agent_proof(&mut inner);
        inner.agent_identity_binding_deadline = None;
        inner.agent_replication_correlation = None;
        inner.agent_status.phase = AgentStatusPhase::Unavailable;
        inner.agent_status.fresh_members = 0;
        inner.agent_status.replication_correlated_shards = 0;
        inner.agent_status.target_fence_acknowledged_shards = 0;
        inner.agent_status.remote_apply_witnessed_shards = 0;
        inner.agent_status.failure = Some(failure);
        if let Some(topology) = &mut inner.topology
            && topology.agent_status_collection
                != crate::topology::AgentStatusCollectionState::DisabledAgentRuntimeRequired
        {
            topology.agent_status_collection =
                crate::topology::AgentStatusCollectionState::DisabledPodIdentityRequired;
        }
    }

    /// Atomically publishes only one complete, still-live collection summary.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn record_agent_status_fresh<D: IntoSuspendAwareDeadline>(
        &self,
        member_count: usize,
        replication_correlation: ReplicationCorrelationSummary,
        deadline: D,
    ) -> bool {
        self.record_agent_status_fresh_exact(member_count, replication_correlation, None, deadline)
    }

    /// Atomically publishes one complete summary and its optional exact,
    /// non-serializable shard-zero proof.
    #[must_use]
    pub(crate) fn record_agent_status_fresh_exact<D: IntoSuspendAwareDeadline>(
        &self,
        member_count: usize,
        replication_correlation: ReplicationCorrelationSummary,
        shard_zero_replication_proof: Option<ShardZeroReplicationProof>,
        deadline: D,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.suspend_aware_now().ok();
        let deadline = now.and_then(|now| deadline.into_suspend_aware(now));
        if inner.agent_status.phase == AgentStatusPhase::ShuttingDown {
            return false;
        }
        let generation_advanced = clear_agent_proof(&mut inner);
        let proof_shape_valid = shard_zero_replication_proof.is_none()
            || (replication_correlation.shard_zero_correlated
                && replication_correlation.shard_zero_target_fence_acknowledged
                && replication_correlation.shard_zero_remote_apply_witnessed);
        let valid = generation_advanced
            && proof_shape_valid
            && member_count > 0
            && member_count == inner.agent_status.expected_members
            && replication_correlation.correlated_shards
                <= inner
                    .topology
                    .as_ref()
                    .map_or(0, |topology| topology.shard_count)
            && replication_correlation.acknowledged_correlated_shards
                <= replication_correlation.correlated_shards
            && replication_correlation.remote_apply_witnessed_shards
                <= replication_correlation.correlated_shards
            && (!replication_correlation.shard_zero_correlated
                || replication_correlation.correlated_shards > 0)
            && (!replication_correlation.shard_zero_target_fence_acknowledged
                || (replication_correlation.shard_zero_correlated
                    && replication_correlation.acknowledged_correlated_shards > 0))
            && (!replication_correlation.shard_zero_correlated
                || replication_correlation.acknowledged_correlated_shards
                    != replication_correlation.correlated_shards
                || replication_correlation.shard_zero_target_fence_acknowledged)
            && (!replication_correlation.shard_zero_remote_apply_witnessed
                || (replication_correlation.shard_zero_correlated
                    && replication_correlation.remote_apply_witnessed_shards > 0))
            && (!replication_correlation.shard_zero_correlated
                || replication_correlation.remote_apply_witnessed_shards
                    != replication_correlation.correlated_shards
                || replication_correlation.shard_zero_remote_apply_witnessed)
            && deadline
                .zip(now)
                .is_some_and(|(deadline, now)| deadline.is_live_at(now));
        if !valid {
            inner.agent_identity_binding_deadline = None;
            inner.agent_replication_correlation = None;
            inner.agent_status.phase = AgentStatusPhase::Unavailable;
            inner.agent_status.fresh_members = 0;
            inner.agent_status.replication_correlated_shards = 0;
            inner.agent_status.target_fence_acknowledged_shards = 0;
            inner.agent_status.remote_apply_witnessed_shards = 0;
            inner.agent_status.failure = Some(AgentStatusFailureReason::FreshnessExpired);
            return false;
        }
        inner.agent_identity_binding_deadline = deadline;
        inner.agent_replication_correlation = Some(replication_correlation);
        inner.agent_shard_zero_replication_proof = shard_zero_replication_proof;
        inner.agent_status.phase = AgentStatusPhase::Fresh;
        inner.agent_status.fresh_members = member_count;
        inner.agent_status.replication_correlated_shards =
            replication_correlation.correlated_shards;
        inner.agent_status.target_fence_acknowledged_shards =
            replication_correlation.acknowledged_correlated_shards;
        inner.agent_status.remote_apply_witnessed_shards =
            replication_correlation.remote_apply_witnessed_shards;
        inner.agent_status.failure = None;
        if let Some(topology) = &mut inner.topology {
            topology.agent_status_collection =
                crate::topology::AgentStatusCollectionState::FreshDiagnosticEvidence;
        }
        true
    }

    /// Clears earlier candidate evidence before a new complete read bracket.
    pub(crate) fn record_catalog_candidates_collecting(
        &self,
        expected_candidates: usize,
        maximum_age: Duration,
    ) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.catalog_candidates.phase == CatalogCandidatePhase::ShuttingDown {
            return;
        }
        let _ = clear_catalog_proof(&mut inner);
        inner.catalog_candidate_deadline = None;
        inner.catalog_candidates.phase = CatalogCandidatePhase::Collecting;
        inner.catalog_candidates.expected_candidates = expected_candidates;
        inner.catalog_candidates.fresh_candidates = 0;
        inner.catalog_candidates.maximum_age_ms =
            u64::try_from(maximum_age.as_millis()).unwrap_or(u64::MAX);
        inner.catalog_candidates.failure = None;
    }

    /// Records one failed-closed candidate observation without last-good data.
    pub(crate) fn record_catalog_candidate_failure(&self, failure: CatalogCandidateFailureReason) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.catalog_candidates.phase == CatalogCandidatePhase::ShuttingDown {
            return;
        }
        let _ = clear_catalog_proof(&mut inner);
        inner.catalog_candidate_deadline = None;
        inner.catalog_candidates.phase = CatalogCandidatePhase::Unavailable;
        inner.catalog_candidates.fresh_candidates = 0;
        inner.catalog_candidates.failure = Some(failure);
    }

    /// Atomically publishes one complete, still-live candidate count only.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn record_catalog_candidates_fresh<D: IntoSuspendAwareDeadline>(
        &self,
        candidate_count: usize,
        deadline: D,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.suspend_aware_now().ok();
        let deadline = now.and_then(|now| deadline.into_suspend_aware(now));
        let Some(deadline) = deadline else {
            let _ = clear_catalog_proof(&mut inner);
            inner.catalog_candidates.phase = CatalogCandidatePhase::Unavailable;
            inner.catalog_candidates.fresh_candidates = 0;
            inner.catalog_candidates.failure =
                Some(CatalogCandidateFailureReason::FreshnessExpired);
            return false;
        };
        Self::record_catalog_candidates_fresh_locked(
            &mut inner,
            candidate_count,
            None,
            deadline,
            now,
        )
    }

    /// Atomically publishes one complete summary and its exact,
    /// non-serializable candidate proof graph.
    #[must_use]
    pub(crate) fn record_catalog_candidates_fresh_exact<D: IntoSuspendAwareDeadline>(
        &self,
        proof: BoundCandidateSet,
        deadline: D,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let candidate_count = proof.candidate_count();
        let now = self.suspend_aware_now().ok();
        let deadline = now.and_then(|now| deadline.into_suspend_aware(now));
        let Some(deadline) = deadline else {
            let _ = clear_catalog_proof(&mut inner);
            inner.catalog_candidates.phase = CatalogCandidatePhase::Unavailable;
            inner.catalog_candidates.fresh_candidates = 0;
            inner.catalog_candidates.failure =
                Some(CatalogCandidateFailureReason::FreshnessExpired);
            return false;
        };
        Self::record_catalog_candidates_fresh_locked(
            &mut inner,
            candidate_count,
            Some(proof),
            deadline,
            now,
        )
    }

    fn record_catalog_candidates_fresh_locked(
        inner: &mut OrchInner,
        candidate_count: usize,
        proof: Option<BoundCandidateSet>,
        deadline: SuspendAwareInstant,
        now: Option<SuspendAwareInstant>,
    ) -> bool {
        if inner.catalog_candidates.phase == CatalogCandidatePhase::ShuttingDown {
            return false;
        }
        let generation_advanced = clear_catalog_proof(inner);
        let valid = generation_advanced
            && matches!(candidate_count, 3 | 5)
            && candidate_count == inner.catalog_candidates.expected_candidates
            && now.is_some_and(|now| deadline.is_live_at(now));
        if !valid {
            inner.catalog_candidate_deadline = None;
            inner.catalog_candidates.phase = CatalogCandidatePhase::Unavailable;
            inner.catalog_candidates.fresh_candidates = 0;
            inner.catalog_candidates.failure =
                Some(CatalogCandidateFailureReason::FreshnessExpired);
            return false;
        }
        inner.catalog_candidate_deadline = Some(deadline);
        inner.catalog_candidate_proof = proof;
        inner.catalog_candidates.phase = CatalogCandidatePhase::Fresh;
        inner.catalog_candidates.fresh_candidates = candidate_count;
        inner.catalog_candidates.failure = None;
        true
    }

    /// Issues a move-only token only while exact independent proof graphs and
    /// leadership overlap. There is intentionally no runtime consumer yet.
    #[allow(dead_code)]
    pub(crate) fn catalog_materialization_capability(
        &self,
    ) -> Option<CatalogMaterializationCapability> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.suspend_aware_now().ok();
        Self::catalog_materialization_capability_locked(&mut inner, now)
    }

    #[cfg(test)]
    fn catalog_materialization_capability_at(
        &self,
        now: Instant,
    ) -> Option<CatalogMaterializationCapability> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self
            .boottime
            .now()
            .ok()
            .map(|boottime| SuspendAwareInstant {
                monotonic: now,
                boottime,
            });
        Self::catalog_materialization_capability_locked(&mut inner, now)
    }

    fn catalog_materialization_capability_locked(
        inner: &mut OrchInner,
        now: Option<SuspendAwareInstant>,
    ) -> Option<CatalogMaterializationCapability> {
        let _ = expire_coordination_state(inner, now);
        expire_proof_state(inner, now);
        let now = now?;
        let identity = inner.identity.as_ref()?;
        let topology = inner.topology.as_ref()?;
        let replication = inner.agent_shard_zero_replication_proof.as_ref()?;
        let candidates = inner.catalog_candidate_proof.as_ref()?;
        issue_catalog_capability(
            replication,
            candidates,
            &identity.cluster_id,
            &topology.cluster_object_uid,
            inner.coordination_ready,
            inner.leader,
            inner.coordination_generation,
            inner.agent_proof_generation,
            inner.catalog_proof_generation,
            inner.coordination_lease_uid.as_deref()?,
            inner.coordination_resource_version.as_deref()?,
            inner.coordination_deadline?,
            inner.agent_identity_binding_deadline?,
            inner.catalog_candidate_deadline?,
            now,
        )
    }

    /// Revalidates every monotonic generation, exact coordination observation,
    /// freshness deadline, and proof cross-binding without extending the token.
    #[allow(dead_code)]
    pub(crate) fn revalidate_catalog_materialization_capability(
        &self,
        capability: &CatalogMaterializationCapability,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.suspend_aware_now().ok();
        Self::revalidate_catalog_materialization_capability_locked(&mut inner, capability, now)
    }

    fn revalidate_catalog_materialization_capability_locked(
        inner: &mut OrchInner,
        capability: &CatalogMaterializationCapability,
        now: Option<SuspendAwareInstant>,
    ) -> bool {
        let _ = expire_coordination_state(inner, now);
        expire_proof_state(inner, now);
        let Some(now) = now else {
            return false;
        };
        let Some(identity) = inner.identity.as_ref() else {
            return false;
        };
        let Some(topology) = inner.topology.as_ref() else {
            return false;
        };
        let Some(replication) = inner.agent_shard_zero_replication_proof.as_ref() else {
            return false;
        };
        let Some(candidates) = inner.catalog_candidate_proof.as_ref() else {
            return false;
        };
        let Some(coordination_lease_uid) = inner.coordination_lease_uid.as_deref() else {
            return false;
        };
        let Some(coordination_resource_version) = inner.coordination_resource_version.as_deref()
        else {
            return false;
        };
        let (Some(coordination_deadline), Some(agent_deadline), Some(candidate_deadline)) = (
            inner.coordination_deadline,
            inner.agent_identity_binding_deadline,
            inner.catalog_candidate_deadline,
        ) else {
            return false;
        };
        revalidate_catalog_capability(
            capability,
            replication,
            candidates,
            &identity.cluster_id,
            &topology.cluster_object_uid,
            inner.coordination_ready,
            inner.leader,
            inner.coordination_generation,
            inner.agent_proof_generation,
            inner.catalog_proof_generation,
            coordination_lease_uid,
            coordination_resource_version,
            coordination_deadline,
            agent_deadline,
            candidate_deadline,
            now,
        )
    }

    /// Consumes a live capability and seals its exact target and material
    /// inputs into one opaque, non-I/O dispatch envelope.
    #[allow(dead_code)]
    pub(crate) fn catalog_bootstrap_dispatch(
        &self,
        capability: CatalogMaterializationCapability,
    ) -> Option<CatalogBootstrapDispatch> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.suspend_aware_now().ok();
        Self::catalog_bootstrap_dispatch_locked(&mut inner, capability, now)
    }

    #[cfg(test)]
    fn catalog_bootstrap_dispatch_at(
        &self,
        capability: CatalogMaterializationCapability,
        now: Instant,
    ) -> Option<CatalogBootstrapDispatch> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self
            .boottime
            .now()
            .ok()
            .map(|boottime| SuspendAwareInstant {
                monotonic: now,
                boottime,
            });
        Self::catalog_bootstrap_dispatch_locked(&mut inner, capability, now)
    }

    fn catalog_bootstrap_dispatch_locked(
        inner: &mut OrchInner,
        capability: CatalogMaterializationCapability,
        now: Option<SuspendAwareInstant>,
    ) -> Option<CatalogBootstrapDispatch> {
        let _ = expire_coordination_state(inner, now);
        expire_proof_state(inner, now);
        let now = now?;
        let identity = inner.identity.as_ref()?;
        let topology = inner.topology.as_ref()?;
        let replication = inner.agent_shard_zero_replication_proof.as_ref()?;
        let candidates = inner.catalog_candidate_proof.as_ref()?;
        prepare_catalog_dispatch(
            capability,
            replication,
            candidates,
            &identity.cluster_id,
            &topology.cluster_object_uid,
            inner.coordination_ready,
            inner.leader,
            inner.coordination_generation,
            inner.agent_proof_generation,
            inner.catalog_proof_generation,
            inner.coordination_lease_uid.as_deref()?,
            inner.coordination_resource_version.as_deref()?,
            inner.coordination_deadline?,
            inner.agent_identity_binding_deadline?,
            inner.catalog_candidate_deadline?,
            now,
        )
    }

    /// Revalidates a sealed dispatch without extending its original deadline
    /// or performing any external I/O.
    #[allow(dead_code)]
    pub(crate) fn revalidate_catalog_bootstrap_dispatch(
        &self,
        dispatch: &CatalogBootstrapDispatch,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.suspend_aware_now().ok();
        Self::revalidate_catalog_bootstrap_dispatch_locked(&mut inner, dispatch, now)
    }

    #[cfg(test)]
    fn revalidate_catalog_bootstrap_dispatch_at(
        &self,
        dispatch: &CatalogBootstrapDispatch,
        now: Instant,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self
            .boottime
            .now()
            .ok()
            .map(|boottime| SuspendAwareInstant {
                monotonic: now,
                boottime,
            });
        Self::revalidate_catalog_bootstrap_dispatch_locked(&mut inner, dispatch, now)
    }

    fn revalidate_catalog_bootstrap_dispatch_locked(
        inner: &mut OrchInner,
        dispatch: &CatalogBootstrapDispatch,
        now: Option<SuspendAwareInstant>,
    ) -> bool {
        let _ = expire_coordination_state(inner, now);
        expire_proof_state(inner, now);
        let Some(now) = now else {
            return false;
        };
        let Some(identity) = inner.identity.as_ref() else {
            return false;
        };
        let Some(topology) = inner.topology.as_ref() else {
            return false;
        };
        let Some(replication) = inner.agent_shard_zero_replication_proof.as_ref() else {
            return false;
        };
        let Some(candidates) = inner.catalog_candidate_proof.as_ref() else {
            return false;
        };
        let Some(coordination_lease_uid) = inner.coordination_lease_uid.as_deref() else {
            return false;
        };
        let Some(coordination_resource_version) = inner.coordination_resource_version.as_deref()
        else {
            return false;
        };
        let (Some(coordination_deadline), Some(agent_deadline), Some(candidate_deadline)) = (
            inner.coordination_deadline,
            inner.agent_identity_binding_deadline,
            inner.catalog_candidate_deadline,
        ) else {
            return false;
        };
        revalidate_catalog_dispatch(
            dispatch,
            replication,
            candidates,
            &identity.cluster_id,
            &topology.cluster_object_uid,
            inner.coordination_ready,
            inner.leader,
            inner.coordination_generation,
            inner.agent_proof_generation,
            inner.catalog_proof_generation,
            coordination_lease_uid,
            coordination_resource_version,
            coordination_deadline,
            agent_deadline,
            candidate_deadline,
            now,
        )
    }

    /// Makes shutdown terminal for candidate diagnostics without coupling it to
    /// the independent coordination or agent-status state machines.
    pub(crate) fn record_catalog_candidate_shutdown(&self) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = clear_catalog_proof(&mut inner);
        inner.catalog_candidate_deadline = None;
        inner.catalog_candidates.phase = CatalogCandidatePhase::ShuttingDown;
        inner.catalog_candidates.fresh_candidates = 0;
        inner.catalog_candidates.failure = None;
    }

    /// Removes externally visible readiness before process shutdown begins.
    pub fn begin_shutdown(&self) {
        self.record_coordination_unavailable();
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = clear_agent_proof(&mut inner);
        let _ = clear_catalog_proof(&mut inner);
        inner.agent_identity_binding_deadline = None;
        inner.agent_replication_correlation = None;
        inner.agent_status.phase = AgentStatusPhase::ShuttingDown;
        inner.agent_status.fresh_members = 0;
        inner.agent_status.replication_correlated_shards = 0;
        inner.agent_status.target_fence_acknowledged_shards = 0;
        inner.agent_status.remote_apply_witnessed_shards = 0;
        inner.agent_status.failure = None;
        inner.catalog_candidate_deadline = None;
        inner.catalog_candidates.phase = CatalogCandidatePhase::ShuttingDown;
        inner.catalog_candidates.fresh_candidates = 0;
        inner.catalog_candidates.failure = None;
        if let Some(topology) = &mut inner.topology
            && topology.agent_status_collection
                != crate::topology::AgentStatusCollectionState::DisabledAgentRuntimeRequired
        {
            topology.agent_status_collection =
                crate::topology::AgentStatusCollectionState::DisabledPodIdentityRequired;
        }
    }

    /// Returns a consistent reportable snapshot.
    #[must_use]
    pub fn snapshot(&self) -> OrchSnapshot {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self.suspend_aware_now().ok();
        Self::snapshot_locked(&mut inner, now)
    }

    #[cfg(test)]
    fn snapshot_at(&self, now: Instant) -> OrchSnapshot {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = self
            .boottime
            .now()
            .ok()
            .map(|boottime| SuspendAwareInstant {
                monotonic: now,
                boottime,
            });
        Self::snapshot_locked(&mut inner, now)
    }

    fn snapshot_locked(inner: &mut OrchInner, now: Option<SuspendAwareInstant>) -> OrchSnapshot {
        let coordination_ready = expire_coordination_state(inner, now);
        expire_proof_state(inner, now);
        let catalog_bootstrap_observation = catalog_bootstrap_observation(inner);
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
            topology: inner.topology.clone(),
            agent_status: inner.agent_status.clone(),
            catalog_candidates: inner.catalog_candidates.clone(),
            catalog_bootstrap_observation,
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_at_for_test(&self, now: Instant) -> OrchSnapshot {
        self.snapshot_at(now)
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
    use std::sync::Weak;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::agent_status::{
        RemoteApplyWitnessProof, ReplicationProofMemberIdentity, ShardZeroSourceReplicationProof,
        ShardZeroStandbyReplicationProof, TargetFenceAcknowledgementProof,
        WritableLeaseProofIdentity,
    };
    use crate::boottime::{BoottimeInstant, FakeBoottimeClock};
    use crate::catalog_candidate::{
        AnnotationIdentity, BootstrapReference, CandidateDocumentV1, CandidateFingerprint,
        CatalogAccessReference, ClusterFingerprint, ConfigurationReference, ContentReference,
        DiscoveryMember, DiscoveryTopology, MaterialReference, MaterializationBundle,
        NameReference, ObjectReference, PodTemplateReference, PolicyReference,
    };

    struct LockAssertingBoottimeClock {
        clock: FakeBoottimeClock,
        state: Mutex<Option<Weak<Mutex<OrchInner>>>>,
        enforce: AtomicBool,
    }

    impl LockAssertingBoottimeClock {
        fn new(now: BoottimeInstant) -> Self {
            Self {
                clock: FakeBoottimeClock::new(now),
                state: Mutex::new(None),
                enforce: AtomicBool::new(false),
            }
        }

        fn require_state_lock(&self, state: &OrchState) {
            *self.state.lock().expect("clock state") = Some(Arc::downgrade(&state.inner));
            self.enforce.store(true, Ordering::Release);
        }
    }

    impl BoottimeClock for LockAssertingBoottimeClock {
        fn now(&self) -> Result<BoottimeInstant, BoottimeError> {
            if self.enforce.load(Ordering::Acquire) {
                let state = self
                    .state
                    .lock()
                    .expect("clock state")
                    .as_ref()
                    .and_then(Weak::upgrade)
                    .expect("bound state");
                assert!(matches!(
                    state.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ));
            }
            self.clock.now()
        }
    }

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

    fn bootstrap_observation_state() -> OrchState {
        OrchState::with_identity_and_topology(
            identity(),
            100,
            TopologyDiagnostics {
                schema_version: crate::topology::TOPOLOGY_SCHEMA_VERSION.to_owned(),
                cluster_object_uid: "cluster-uid".to_owned(),
                shard_count: 2,
                member_count: 6,
                agent_status_collection:
                    crate::topology::AgentStatusCollectionState::DisabledPodIdentityRequired,
            },
        )
        .expect("bootstrap observation state")
    }

    fn bootstrap_observation_state_with_clock(clock: Arc<dyn BoottimeClock>) -> OrchState {
        OrchState::with_identity_and_topology_and_clock_for_test(
            identity(),
            100,
            TopologyDiagnostics {
                schema_version: crate::topology::TOPOLOGY_SCHEMA_VERSION.to_owned(),
                cluster_object_uid: "cluster-uid".to_owned(),
                shard_count: 2,
                member_count: 6,
                agent_status_collection:
                    crate::topology::AgentStatusCollectionState::DisabledPodIdentityRequired,
            },
            clock,
        )
        .expect("bootstrap observation state")
    }

    fn publish_bootstrap_inputs(
        state: &OrchState,
        replication: ReplicationCorrelationSummary,
        agent_deadline: Instant,
        candidate_deadline: Instant,
    ) {
        state.record_agent_status_collecting(Duration::from_secs(5));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(5));
        assert!(state.record_agent_status_fresh(6, replication, agent_deadline));
        assert!(state.record_catalog_candidates_fresh(3, candidate_deadline));
    }

    fn proof_member(ordinal: u32) -> ReplicationProofMemberIdentity {
        ReplicationProofMemberIdentity {
            cluster_id: "cluster-1".to_owned(),
            cluster_uid: "cluster-uid".to_owned(),
            shard_id: 0,
            member_ordinal: ordinal,
            instance_id: if ordinal == 0 {
                "cluster-1-shard-0000-0".to_owned()
            } else {
                format!("cluster-1-shard-0000-m{ordinal:04}-0")
            },
            pod_uid: format!("pod-uid-{ordinal}"),
            postmaster_pid: 100 + ordinal,
            boot_id: format!("00000000-0000-0000-0000-{ordinal:012}"),
        }
    }

    fn exact_replication_proof() -> ShardZeroReplicationProof {
        let source = proof_member(0);
        let standbys = [1_u32, 2]
            .into_iter()
            .map(|ordinal| ShardZeroStandbyReplicationProof {
                member: proof_member(ordinal),
                source_instance_id: source.instance_id.clone(),
                member_slot_name: format!("pgshard_member_{ordinal:04}"),
                system_identifier: 42,
                timeline: 3,
                canonical_generation_identity: "generation-7".to_owned(),
                generation_barrier_lsn: 100,
                receive_lsn: 120 + u64::from(ordinal),
                replay_lsn: 110 + u64::from(ordinal),
            })
            .collect::<Vec<_>>();
        ShardZeroReplicationProof {
            writable_lease: WritableLeaseProofIdentity {
                name: "cluster-1-shard-0000-writable".to_owned(),
                uid: "writable-lease-uid".to_owned(),
            },
            source: ShardZeroSourceReplicationProof {
                member: source,
                system_identifier: 42,
                timeline: 3,
                canonical_generation_identity: "generation-7".to_owned(),
                generation_barrier_lsn: 100,
                target_fence_acknowledgement: TargetFenceAcknowledgementProof {
                    observed_at_unix_ms: 1,
                    canonical_generation_identity: "generation-7".to_owned(),
                    deadline_boottime_ns: 5_000_000_000,
                    remaining_validity_at_ack_ms: 5_000,
                    remaining_validity_at_report_ms: 5_000,
                    boot_id: "00000000-0000-0000-0000-000000000000".to_owned(),
                    postmaster_pid: 100,
                    control_backend_pid: 200,
                },
            },
            remote_apply_witness: RemoteApplyWitnessProof {
                member: proof_member(1),
                member_slot_name: "pgshard_member_0001".to_owned(),
                system_identifier: 42,
                timeline: 3,
                canonical_generation_identity: "generation-7".to_owned(),
                generation_barrier_lsn: 100,
                receive_lsn: 121,
                replay_lsn: 111,
            },
            standbys,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn exact_candidate_proof() -> BoundCandidateSet {
        let writable_lease = ObjectReference {
            name: "cluster-1-shard-0000-writable".to_owned(),
            uid: "writable-lease-uid".to_owned(),
        };
        let replication_credential = MaterialReference {
            name: "cluster-1-replication".to_owned(),
            uid: "replication-uid".to_owned(),
            material_sha256: "1".repeat(64),
        };
        let catalog_access = CatalogAccessReference {
            name: "cluster-1-catalog-access".to_owned(),
            uid: "catalog-access-uid".to_owned(),
            client_sha256: "2".repeat(64),
            server_sha256: "3".repeat(64),
        };
        let operation_writer_access = MaterialReference {
            name: format!("cluster-1-writer-{}", "a".repeat(32)),
            uid: "writer-uid".to_owned(),
            material_sha256: "4".repeat(64),
        };
        let discovery_topology = DiscoveryTopology {
            config_map: NameReference {
                name: "cluster-1-database-topology".to_owned(),
            },
            members: (0..3)
                .map(|ordinal| DiscoveryMember {
                    ordinal,
                    instance_id: proof_member(ordinal).instance_id,
                    dns_name: format!("member-{ordinal}.database.svc"),
                    postgresql_port: 5_432,
                    agent_http_port: 9_180,
                    physical_slot: format!("pgshard_member_{ordinal:04}"),
                })
                .collect(),
            sha256: "5".repeat(64),
        };
        let shard_zero_bootstraps = (0..3)
            .map(|ordinal| BootstrapReference {
                secret: ObjectReference {
                    name: format!("bootstrap-{ordinal}"),
                    uid: format!("bootstrap-secret-uid-{ordinal}"),
                },
                pvc: ObjectReference {
                    name: format!("data-{ordinal}"),
                    uid: format!("pvc-uid-{ordinal}"),
                },
            })
            .collect::<Vec<_>>();
        let postgresql_configuration = ConfigurationReference {
            name: format!("cluster-1-postgresql-config-{}", "7".repeat(64)),
            uid: "postgresql-configuration-uid".to_owned(),
            data_sha256: "7".repeat(64),
        };
        let materialization_bundles = (0..3)
            .map(|_| MaterializationBundle {
                postgresql_configuration: postgresql_configuration.clone(),
                shardschema_migration: ContentReference {
                    sha256: "8".repeat(64),
                },
                database_genesis: ContentReference {
                    sha256: "9".repeat(64),
                },
                database_topology_preflight: ContentReference {
                    sha256: "a".repeat(64),
                },
                catalog_access: catalog_access.clone(),
                operation_writer_access: operation_writer_access.clone(),
                serving_hba: PolicyReference {
                    version: "pgshard.catalog-serving-hba.v1".to_owned(),
                    sha256: "b".repeat(64),
                },
                target_pod_template: PodTemplateReference {
                    stateful_set_name: "cluster-1-postgresql-0".to_owned(),
                    postgresql_runtime: "agent-quarantine".to_owned(),
                    bootstrap_hba_mode: "replication-bootstrap-primary".to_owned(),
                    configuration_annotation: AnnotationIdentity {
                        key: "pgshard.io/config-hash".to_owned(),
                        value: "7".repeat(64),
                    },
                    shardschema_migration_annotation: AnnotationIdentity {
                        key: "pgshard.io/shardschema-migration-sha256".to_owned(),
                        value: "8".repeat(64),
                    },
                    sha256: "c".repeat(64),
                },
                sha256: "d".repeat(64),
            })
            .collect::<Vec<_>>();
        let candidates = (0..3)
            .map(|ordinal| CandidateFingerprint {
                name: format!("cluster-1-catalog-candidate-{ordinal}"),
                uid: format!("candidate-uid-{ordinal}"),
                resource_version: format!("candidate-rv-{ordinal}"),
                payload_sha256: format!("{ordinal}").repeat(64),
                document: CandidateDocumentV1 {
                    schema_version: "pgshard.catalog-bootstrap-candidate.v1".to_owned(),
                    cluster_object_uid: "cluster-uid".to_owned(),
                    shard: 0,
                    member: ordinal,
                    instance_id: proof_member(ordinal).instance_id,
                    discovery_topology: discovery_topology.clone(),
                    bootstrap: shard_zero_bootstraps[ordinal as usize].clone(),
                    writable_lease: writable_lease.clone(),
                    replication_credential: replication_credential.clone(),
                    catalog_access: catalog_access.clone(),
                    materialization_bundle: materialization_bundles[ordinal as usize].clone(),
                },
            })
            .collect();
        BoundCandidateSet {
            cluster: ClusterFingerprint {
                uid: "cluster-uid".to_owned(),
                resource_version: "cluster-rv".to_owned(),
                generation: 7,
                status_sha256: "6".repeat(64),
            },
            candidates,
            shard_zero_bootstraps,
            writable_lease,
            replication_credential,
            catalog_access,
            operation_writer_access,
            materialization_bundles,
        }
    }

    fn publish_exact_bootstrap_authority(state: &OrchState, deadline: SuspendAwareInstant) {
        let summary = ReplicationCorrelationSummary {
            correlated_shards: 1,
            shard_zero_correlated: true,
            acknowledged_correlated_shards: 1,
            shard_zero_target_fence_acknowledged: true,
            remote_apply_witnessed_shards: 1,
            shard_zero_remote_apply_witnessed: true,
        };
        state.record_agent_status_collecting(Duration::from_secs(2));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(2));
        assert!(state.record_agent_status_fresh_exact(
            6,
            summary,
            Some(exact_replication_proof()),
            deadline,
        ));
        assert!(state.record_catalog_candidates_fresh_exact(exact_candidate_proof(), deadline));
        assert!(state.record_coordination_ready(
            "coordination-uid",
            "coordination-rv-1",
            true,
            deadline,
        ));
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
    fn topology_diagnostics_never_change_readiness() {
        let topology = TopologyDiagnostics {
            schema_version: crate::topology::TOPOLOGY_SCHEMA_VERSION.to_owned(),
            cluster_object_uid: "cluster-uid".to_owned(),
            shard_count: 2,
            member_count: 6,
            agent_status_collection:
                crate::topology::AgentStatusCollectionState::DisabledPodIdentityRequired,
        };
        let state = OrchState::with_identity_and_topology(identity(), 1_000, topology.clone())
            .expect("valid state");

        assert_eq!(
            state.readiness(),
            OrchReadiness {
                ready: false,
                reason: OrchReadinessReason::CoordinationUnavailable,
            }
        );
        assert_eq!(state.snapshot().topology, Some(topology));
        assert!(state.snapshot().leases.is_empty());
    }

    #[test]
    fn exact_catalog_capability_requires_overlap_and_revalidates_generations() {
        let state = bootstrap_observation_state();
        let now = Instant::now();
        let summary = ReplicationCorrelationSummary {
            correlated_shards: 1,
            shard_zero_correlated: true,
            acknowledged_correlated_shards: 1,
            shard_zero_target_fence_acknowledged: true,
            remote_apply_witnessed_shards: 1,
            shard_zero_remote_apply_witnessed: true,
        };
        state.record_agent_status_collecting(Duration::from_secs(5));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(5));
        assert!(state.record_agent_status_fresh_exact(
            6,
            summary,
            Some(exact_replication_proof()),
            now + Duration::from_secs(5),
        ));
        assert!(state.record_catalog_candidates_fresh_exact(
            exact_candidate_proof(),
            now + Duration::from_secs(5),
        ));
        assert!(state.catalog_materialization_capability().is_none());

        assert!(state.record_coordination_ready(
            "coordination-uid",
            "coordination-rv-1",
            true,
            now + Duration::from_secs(5),
        ));
        let first = state
            .catalog_materialization_capability()
            .expect("exact live proof overlap");
        assert!(state.revalidate_catalog_materialization_capability(&first));

        // Replacing either exact proof advances its private generation and
        // irrevocably invalidates already issued tokens.
        assert!(state.record_agent_status_fresh_exact(
            6,
            summary,
            Some(exact_replication_proof()),
            now + Duration::from_secs(5),
        ));
        assert!(!state.revalidate_catalog_materialization_capability(&first));
        let second = state
            .catalog_materialization_capability()
            .expect("replacement exact proof");

        state.record_catalog_candidates_collecting(3, Duration::from_secs(5));
        assert!(!state.revalidate_catalog_materialization_capability(&second));
        assert!(state.catalog_materialization_capability().is_none());

        let mut mismatched = exact_candidate_proof();
        mismatched.writable_lease.uid = "replacement-lease-uid".to_owned();
        assert!(
            state.record_catalog_candidates_fresh_exact(mismatched, now + Duration::from_secs(5),)
        );
        assert!(state.catalog_materialization_capability().is_none());

        assert!(state.record_catalog_candidates_fresh_exact(
            exact_candidate_proof(),
            now + Duration::from_secs(5),
        ));
        let third = state
            .catalog_materialization_capability()
            .expect("restored cross-binding");
        let dispatch = state
            .catalog_bootstrap_dispatch(third)
            .expect("sealed exact dispatch");
        assert!(state.revalidate_catalog_bootstrap_dispatch(&dispatch));
        let debug = format!("{state:?}");
        for private_value in ["pod-uid-0", "generation-7", "writer-uid"] {
            assert!(!debug.contains(private_value));
        }
        assert!(debug.contains("<redacted>"));
        state.record_coordination_unavailable();
        assert!(!state.revalidate_catalog_bootstrap_dispatch(&dispatch));
        assert!(state.catalog_materialization_capability().is_none());

        // Public snapshots remain bounded summaries and never expose the
        // retained identity graphs or writer material reference.
        let encoded = serde_json::to_string(&state.snapshot()).expect("serialize snapshot");
        for private_value in ["pod-uid-0", "generation-7", "writer-uid", &"4".repeat(64)] {
            assert!(!encoded.contains(private_value));
        }
    }

    #[test]
    fn dispatch_revalidation_itself_expires_and_clears_exact_proofs() {
        let state = bootstrap_observation_state();
        let now = Instant::now();
        let deadline = now + Duration::from_secs(2);
        let summary = ReplicationCorrelationSummary {
            correlated_shards: 1,
            shard_zero_correlated: true,
            acknowledged_correlated_shards: 1,
            shard_zero_target_fence_acknowledged: true,
            remote_apply_witnessed_shards: 1,
            shard_zero_remote_apply_witnessed: true,
        };
        state.record_agent_status_collecting(Duration::from_secs(5));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(5));
        assert!(state.record_agent_status_fresh_exact(
            6,
            summary,
            Some(exact_replication_proof()),
            deadline,
        ));
        assert!(state.record_catalog_candidates_fresh_exact(exact_candidate_proof(), deadline,));
        assert!(state.record_coordination_ready(
            "coordination-uid",
            "coordination-rv-1",
            true,
            deadline,
        ));
        let capability = state
            .catalog_materialization_capability_at(now)
            .expect("live exact capability");
        let dispatch = state
            .catalog_bootstrap_dispatch_at(capability, now)
            .expect("live exact dispatch");
        let (agent_generation, catalog_generation) = {
            let inner = state.inner.lock().expect("state");
            (inner.agent_proof_generation, inner.catalog_proof_generation)
        };

        assert!(!state.revalidate_catalog_bootstrap_dispatch_at(&dispatch, deadline,));
        let inner = state.inner.lock().expect("state");
        assert!(inner.agent_shard_zero_replication_proof.is_none());
        assert!(inner.catalog_candidate_proof.is_none());
        assert!(inner.agent_proof_generation > agent_generation);
        assert!(inner.catalog_proof_generation > catalog_generation);
        assert_eq!(inner.agent_status.phase, AgentStatusPhase::Expired);
        assert_eq!(
            inner.catalog_candidates.phase,
            CatalogCandidatePhase::Expired
        );
    }

    #[test]
    fn capability_and_proofs_expire_across_host_suspend() {
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");
        let summary = ReplicationCorrelationSummary {
            correlated_shards: 1,
            shard_zero_correlated: true,
            acknowledged_correlated_shards: 1,
            shard_zero_target_fence_acknowledged: true,
            remote_apply_witnessed_shards: 1,
            shard_zero_remote_apply_witnessed: true,
        };
        state.record_agent_status_collecting(Duration::from_secs(2));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(2));
        assert!(state.record_agent_status_fresh_exact(
            6,
            summary,
            Some(exact_replication_proof()),
            deadline,
        ));
        assert!(state.record_catalog_candidates_fresh_exact(exact_candidate_proof(), deadline,));
        assert!(state.record_coordination_ready(
            "coordination-uid",
            "coordination-rv-1",
            true,
            deadline,
        ));
        let capability = state
            .catalog_materialization_capability()
            .expect("live exact capability");
        let generations = {
            let inner = state.inner.lock().expect("state");
            (
                inner.coordination_generation,
                inner.agent_proof_generation,
                inner.catalog_proof_generation,
            )
        };

        // Simulate suspension: CLOCK_MONOTONIC remains unchanged while
        // CLOCK_BOOTTIME advances beyond every evidence deadline.
        clock
            .advance(Duration::from_secs(3))
            .expect("advance clock");
        assert!(!state.revalidate_catalog_materialization_capability(&capability));
        assert_eq!(
            state.readiness(),
            OrchReadiness {
                ready: false,
                reason: OrchReadinessReason::CoordinationUnavailable,
            }
        );
        let inner = state.inner.lock().expect("state");
        assert!(inner.agent_shard_zero_replication_proof.is_none());
        assert!(inner.catalog_candidate_proof.is_none());
        assert!(inner.coordination_generation > generations.0);
        assert!(inner.agent_proof_generation > generations.1);
        assert!(inner.catalog_proof_generation > generations.2);
        assert_eq!(inner.agent_status.phase, AgentStatusPhase::Expired);
        assert_eq!(
            inner.catalog_candidates.phase,
            CatalogCandidatePhase::Expired
        );
    }

    #[test]
    fn authority_clock_failure_clears_all_capability_evidence() {
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");
        let summary = ReplicationCorrelationSummary {
            correlated_shards: 1,
            shard_zero_correlated: true,
            acknowledged_correlated_shards: 1,
            shard_zero_target_fence_acknowledged: true,
            remote_apply_witnessed_shards: 1,
            shard_zero_remote_apply_witnessed: true,
        };
        state.record_agent_status_collecting(Duration::from_secs(2));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(2));
        assert!(state.record_agent_status_fresh_exact(
            6,
            summary,
            Some(exact_replication_proof()),
            deadline,
        ));
        assert!(state.record_catalog_candidates_fresh_exact(exact_candidate_proof(), deadline,));
        assert!(state.record_coordination_ready(
            "coordination-uid",
            "coordination-rv-1",
            true,
            deadline,
        ));
        let capability = state
            .catalog_materialization_capability()
            .expect("live exact capability");

        clock.fail();
        assert!(!state.revalidate_catalog_materialization_capability(&capability));
        assert!(state.catalog_materialization_capability().is_none());
        assert!(!state.snapshot().coordination_ready);
        let inner = state.inner.lock().expect("state");
        assert!(inner.agent_shard_zero_replication_proof.is_none());
        assert!(inner.catalog_candidate_proof.is_none());
        assert_eq!(inner.agent_status.phase, AgentStatusPhase::Expired);
        assert_eq!(
            inner.catalog_candidates.phase,
            CatalogCandidatePhase::Expired
        );
    }

    #[test]
    fn capability_revalidation_samples_time_after_mutex_contention() {
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");
        let summary = ReplicationCorrelationSummary {
            correlated_shards: 1,
            shard_zero_correlated: true,
            acknowledged_correlated_shards: 1,
            shard_zero_target_fence_acknowledged: true,
            remote_apply_witnessed_shards: 1,
            shard_zero_remote_apply_witnessed: true,
        };
        state.record_agent_status_collecting(Duration::from_secs(2));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(2));
        assert!(state.record_agent_status_fresh_exact(
            6,
            summary,
            Some(exact_replication_proof()),
            deadline,
        ));
        assert!(state.record_catalog_candidates_fresh_exact(exact_candidate_proof(), deadline,));
        assert!(state.record_coordination_ready(
            "coordination-uid",
            "coordination-rv-1",
            true,
            deadline,
        ));
        let capability = state
            .catalog_materialization_capability()
            .expect("live exact capability");

        let guard = state.inner.lock().expect("hold state mutex");
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let worker = std::thread::spawn({
            let state = state.clone();
            let barrier = barrier.clone();
            move || {
                barrier.wait();
                state.revalidate_catalog_materialization_capability(&capability)
            }
        });
        barrier.wait();
        // Give the worker time to reach the held mutex. A pre-lock clock
        // sample would occur during this interval and incorrectly survive.
        std::thread::sleep(Duration::from_millis(10));
        clock
            .advance(Duration::from_secs(3))
            .expect("suspend across mutex wait");
        drop(guard);

        assert!(!worker.join().expect("revalidation thread"));
        assert!(!state.snapshot().coordination_ready);
    }

    #[test]
    fn dispatch_preparation_samples_clock_while_state_is_locked() {
        let clock = Arc::new(LockAssertingBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");
        publish_exact_bootstrap_authority(&state, deadline);
        let capability = state
            .catalog_materialization_capability()
            .expect("live exact capability");

        clock.require_state_lock(&state);
        assert!(state.catalog_bootstrap_dispatch(capability).is_some());
    }

    #[test]
    fn dispatch_revalidation_samples_clock_while_state_is_locked() {
        let clock = Arc::new(LockAssertingBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");
        publish_exact_bootstrap_authority(&state, deadline);
        let capability = state
            .catalog_materialization_capability()
            .expect("live exact capability");
        let dispatch = state
            .catalog_bootstrap_dispatch(capability)
            .expect("live exact dispatch");

        clock.require_state_lock(&state);
        assert!(state.revalidate_catalog_bootstrap_dispatch(&dispatch));
    }

    #[test]
    fn disabled_agent_runtime_baseline_survives_expiry_and_shutdown_paths() {
        let topology = TopologyDiagnostics {
            schema_version: crate::topology::TOPOLOGY_SCHEMA_VERSION.to_owned(),
            cluster_object_uid: "cluster-uid".to_owned(),
            shard_count: 1,
            member_count: 1,
            agent_status_collection:
                crate::topology::AgentStatusCollectionState::DisabledAgentRuntimeRequired,
        };
        let state = OrchState::with_identity_and_topology(identity(), 1_000, topology.clone())
            .expect("valid state");
        state.record_agent_status_failure(AgentStatusFailureReason::IdentityUnavailable);
        assert_eq!(state.snapshot().topology, Some(topology.clone()));
        state.begin_shutdown();
        assert_eq!(state.snapshot().topology, Some(topology));
    }

    #[test]
    fn requested_shutdown_is_terminal_for_all_agent_status_writes() {
        let topology = TopologyDiagnostics {
            schema_version: crate::topology::TOPOLOGY_SCHEMA_VERSION.to_owned(),
            cluster_object_uid: "cluster-uid".to_owned(),
            shard_count: 1,
            member_count: 3,
            agent_status_collection:
                crate::topology::AgentStatusCollectionState::DisabledPodIdentityRequired,
        };
        let state = OrchState::with_identity_and_topology(identity(), 1_000, topology)
            .expect("valid state");
        state.record_agent_status_collecting(Duration::from_secs(5));
        state.begin_shutdown();

        state.record_agent_status_collecting(Duration::from_secs(5));
        state.record_agent_status_failure(AgentStatusFailureReason::StatusUnavailable);
        assert!(!state.record_agent_status_fresh(
            3,
            ReplicationCorrelationSummary::default(),
            Instant::now() + Duration::from_secs(5),
        ));
        let snapshot = state.snapshot_at(Instant::now() + Duration::from_secs(10));

        assert_eq!(snapshot.agent_status.phase, AgentStatusPhase::ShuttingDown);
        assert_eq!(snapshot.agent_status.fresh_members, 0);
        assert_eq!(snapshot.agent_status.failure, None);
        assert_eq!(
            snapshot.topology.expect("topology").agent_status_collection,
            crate::topology::AgentStatusCollectionState::DisabledPodIdentityRequired,
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn joint_bootstrap_observation_requires_shard_zero_without_changing_authority() {
        let state = bootstrap_observation_state();
        let now = Instant::now();
        assert!(state.record_coordination_ready(
            "coordination-uid",
            "coordination-rv",
            true,
            now + Duration::from_mins(1),
        ));
        let ready = state.readiness();

        publish_bootstrap_inputs(
            &state,
            ReplicationCorrelationSummary::default(),
            now + Duration::from_secs(5),
            now + Duration::from_secs(5),
        );
        let absent = state.snapshot();
        assert_eq!(absent.agent_status.replication_correlated_shards, 0);
        assert_eq!(absent.agent_status.target_fence_acknowledged_shards, 0);
        assert_eq!(absent.agent_status.remote_apply_witnessed_shards, 0);
        assert_eq!(
            absent.catalog_bootstrap_observation.phase,
            CatalogBootstrapObservationPhase::Unavailable
        );
        assert_eq!(absent.catalog_bootstrap_observation.expected_candidates, 3);
        assert_eq!(absent.catalog_bootstrap_observation.fresh_candidates, 3);
        assert!(
            !absent
                .catalog_bootstrap_observation
                .shard_zero_replication_correlated
        );
        assert!(
            !absent
                .catalog_bootstrap_observation
                .shard_zero_target_fence_acknowledged
        );
        assert!(
            !absent
                .catalog_bootstrap_observation
                .shard_zero_remote_apply_witnessed
        );
        assert_eq!(
            absent.catalog_bootstrap_observation.failure,
            Some(CatalogBootstrapObservationFailureReason::ShardZeroReplicationEvidenceUnavailable)
        );
        assert!(absent.catalog_bootstrap_observation.diagnostic_only);
        assert_eq!(state.readiness(), ready);
        assert!(state.snapshot().leader);

        state.record_agent_status_collecting(Duration::from_secs(5));
        let collecting = state.snapshot();
        assert_eq!(
            collecting.catalog_bootstrap_observation.phase,
            CatalogBootstrapObservationPhase::Collecting
        );
        assert_eq!(collecting.catalog_bootstrap_observation.fresh_candidates, 3);
        assert_eq!(collecting.agent_status.replication_correlated_shards, 0);
        assert_eq!(collecting.agent_status.target_fence_acknowledged_shards, 0);
        assert_eq!(collecting.agent_status.remote_apply_witnessed_shards, 0);

        assert!(state.record_agent_status_fresh(
            6,
            ReplicationCorrelationSummary {
                correlated_shards: 2,
                shard_zero_correlated: true,
                acknowledged_correlated_shards: 2,
                shard_zero_target_fence_acknowledged: true,
                remote_apply_witnessed_shards: 2,
                shard_zero_remote_apply_witnessed: true,
            },
            now + Duration::from_secs(5),
        ));
        let correlated = state.snapshot();
        assert_eq!(correlated.agent_status.replication_correlated_shards, 2);
        assert_eq!(correlated.agent_status.target_fence_acknowledged_shards, 2);
        assert_eq!(correlated.agent_status.remote_apply_witnessed_shards, 2);
        assert_eq!(
            correlated.catalog_bootstrap_observation.phase,
            CatalogBootstrapObservationPhase::Correlated
        );
        assert_eq!(correlated.catalog_bootstrap_observation.fresh_candidates, 3);
        assert!(
            correlated
                .catalog_bootstrap_observation
                .shard_zero_replication_correlated
        );
        assert!(
            correlated
                .catalog_bootstrap_observation
                .shard_zero_target_fence_acknowledged
        );
        assert!(
            correlated
                .catalog_bootstrap_observation
                .shard_zero_remote_apply_witnessed
        );
        assert_eq!(correlated.catalog_bootstrap_observation.failure, None);
        let serialized = serde_json::to_string(&correlated).expect("serialize bounded diagnostics");
        for raw_ack_field in [
            "target_fence_acknowledgement",
            "observed_at_unix_ms",
            "generation_identity",
            "deadline_boottime_ns",
            "remaining_validity_at_ack_ms",
            "boot_id",
            "postmaster_pid",
            "control_backend_pid",
            "member_slot_name",
            "slot_active",
            "slot_walsender_match",
            "stream_state",
            "sync_state",
            "flush_lsn",
            "replay_lsn",
            "receive_lsn",
            "generation_barrier_lsn",
            "system_identifier",
            "timeline",
        ] {
            assert!(!serialized.contains(raw_ack_field));
        }
        for raw_value in ["pgshard_member_0001", "generation-7"] {
            assert!(!serialized.contains(raw_value));
        }
        assert_eq!(state.readiness(), ready);

        state.record_catalog_candidates_collecting(3, Duration::from_secs(5));
        let collecting = state.snapshot();
        assert_eq!(
            collecting.catalog_bootstrap_observation.phase,
            CatalogBootstrapObservationPhase::Collecting
        );
        assert_eq!(collecting.catalog_bootstrap_observation.fresh_candidates, 0);
        assert!(
            !collecting
                .catalog_bootstrap_observation
                .shard_zero_replication_correlated
        );
        assert_eq!(collecting.agent_status.replication_correlated_shards, 2);
        assert_eq!(collecting.agent_status.target_fence_acknowledged_shards, 2);
        assert_eq!(collecting.agent_status.remote_apply_witnessed_shards, 2);
        assert!(
            !collecting
                .catalog_bootstrap_observation
                .shard_zero_target_fence_acknowledged
        );
        assert!(
            !collecting
                .catalog_bootstrap_observation
                .shard_zero_remote_apply_witnessed
        );

        assert!(state.record_catalog_candidates_fresh(3, now + Duration::from_secs(5)));
        state.record_catalog_candidate_failure(CatalogCandidateFailureReason::CandidateUnavailable);
        let candidate_failure = state.snapshot();
        assert_eq!(
            candidate_failure.catalog_bootstrap_observation.phase,
            CatalogBootstrapObservationPhase::Unavailable
        );
        assert_eq!(
            candidate_failure.catalog_bootstrap_observation.failure,
            Some(CatalogBootstrapObservationFailureReason::CatalogCandidatesUnavailable)
        );
        assert_eq!(
            candidate_failure
                .catalog_bootstrap_observation
                .fresh_candidates,
            0
        );
        assert!(
            !candidate_failure
                .catalog_bootstrap_observation
                .shard_zero_replication_correlated
        );
        assert!(
            !candidate_failure
                .catalog_bootstrap_observation
                .shard_zero_target_fence_acknowledged
        );
        assert!(
            !candidate_failure
                .catalog_bootstrap_observation
                .shard_zero_remote_apply_witnessed
        );
        assert_eq!(
            candidate_failure
                .agent_status
                .target_fence_acknowledged_shards,
            2
        );
        assert_eq!(
            candidate_failure.agent_status.remote_apply_witnessed_shards,
            2
        );
        assert_eq!(state.readiness(), ready);

        state.record_catalog_candidates_collecting(3, Duration::from_secs(5));
        assert!(state.record_catalog_candidates_fresh(3, now + Duration::from_secs(5)));
        state.record_agent_status_failure(AgentStatusFailureReason::StatusUnavailable);
        let agent_failure = state.snapshot();
        assert_eq!(
            agent_failure.catalog_bootstrap_observation.failure,
            Some(CatalogBootstrapObservationFailureReason::AgentStatusUnavailable)
        );
        assert_eq!(
            agent_failure.catalog_bootstrap_observation.fresh_candidates,
            3
        );
        assert_eq!(agent_failure.agent_status.replication_correlated_shards, 0);
        assert_eq!(
            agent_failure.agent_status.target_fence_acknowledged_shards,
            0
        );
        assert_eq!(agent_failure.agent_status.remote_apply_witnessed_shards, 0);
        assert!(
            !agent_failure
                .catalog_bootstrap_observation
                .shard_zero_target_fence_acknowledged
        );
        assert!(
            !agent_failure
                .catalog_bootstrap_observation
                .shard_zero_remote_apply_witnessed
        );
        assert_eq!(state.readiness(), ready);

        state.begin_shutdown();
        let shutdown = state.snapshot();
        assert_eq!(
            shutdown.catalog_bootstrap_observation.phase,
            CatalogBootstrapObservationPhase::ShuttingDown
        );
        assert!(
            !shutdown
                .catalog_bootstrap_observation
                .shard_zero_replication_correlated
        );
        assert!(
            !shutdown
                .catalog_bootstrap_observation
                .shard_zero_target_fence_acknowledged
        );
        assert!(
            !shutdown
                .catalog_bootstrap_observation
                .shard_zero_remote_apply_witnessed
        );
        assert_eq!(shutdown.agent_status.target_fence_acknowledged_shards, 0);
        assert_eq!(shutdown.agent_status.remote_apply_witnessed_shards, 0);
        assert_eq!(shutdown.catalog_bootstrap_observation.failure, None);
    }

    #[test]
    fn either_input_deadline_expires_the_joint_observation_without_last_good() {
        let now = Instant::now();
        let agent_expires = bootstrap_observation_state();
        publish_bootstrap_inputs(
            &agent_expires,
            ReplicationCorrelationSummary {
                correlated_shards: 1,
                shard_zero_correlated: true,
                acknowledged_correlated_shards: 1,
                shard_zero_target_fence_acknowledged: true,
                remote_apply_witnessed_shards: 1,
                shard_zero_remote_apply_witnessed: true,
            },
            now + Duration::from_secs(1),
            now + Duration::from_secs(2),
        );
        let expired = agent_expires.snapshot_at_for_test(now + Duration::from_secs(1));
        assert_eq!(
            expired.catalog_bootstrap_observation.phase,
            CatalogBootstrapObservationPhase::Expired
        );
        assert_eq!(expired.catalog_bootstrap_observation.fresh_candidates, 3);
        assert!(
            !expired
                .catalog_bootstrap_observation
                .shard_zero_replication_correlated
        );
        assert_eq!(expired.agent_status.replication_correlated_shards, 0);
        assert_eq!(expired.agent_status.target_fence_acknowledged_shards, 0);
        assert_eq!(expired.agent_status.remote_apply_witnessed_shards, 0);
        assert!(
            !expired
                .catalog_bootstrap_observation
                .shard_zero_target_fence_acknowledged
        );
        assert!(
            !expired
                .catalog_bootstrap_observation
                .shard_zero_remote_apply_witnessed
        );

        let candidate_expires = bootstrap_observation_state();
        publish_bootstrap_inputs(
            &candidate_expires,
            ReplicationCorrelationSummary {
                correlated_shards: 1,
                shard_zero_correlated: true,
                acknowledged_correlated_shards: 1,
                shard_zero_target_fence_acknowledged: true,
                remote_apply_witnessed_shards: 1,
                shard_zero_remote_apply_witnessed: true,
            },
            now + Duration::from_secs(2),
            now + Duration::from_secs(1),
        );
        let expired = candidate_expires.snapshot_at_for_test(now + Duration::from_secs(1));
        assert_eq!(
            expired.catalog_bootstrap_observation.phase,
            CatalogBootstrapObservationPhase::Expired
        );
        assert_eq!(expired.catalog_bootstrap_observation.fresh_candidates, 0);
        assert!(
            !expired
                .catalog_bootstrap_observation
                .shard_zero_replication_correlated
        );
        assert_eq!(expired.agent_status.replication_correlated_shards, 1);
        assert_eq!(expired.agent_status.target_fence_acknowledged_shards, 1);
        assert_eq!(expired.agent_status.remote_apply_witnessed_shards, 1);
        assert!(
            !expired
                .catalog_bootstrap_observation
                .shard_zero_target_fence_acknowledged
        );
        assert!(
            !expired
                .catalog_bootstrap_observation
                .shard_zero_remote_apply_witnessed
        );
    }

    #[test]
    fn rejects_impossible_bounded_correlation_summaries() {
        for impossible in [
            ReplicationCorrelationSummary {
                correlated_shards: 1,
                shard_zero_correlated: true,
                acknowledged_correlated_shards: 2,
                shard_zero_target_fence_acknowledged: true,
                remote_apply_witnessed_shards: 0,
                shard_zero_remote_apply_witnessed: false,
            },
            ReplicationCorrelationSummary {
                correlated_shards: 1,
                shard_zero_correlated: false,
                acknowledged_correlated_shards: 1,
                shard_zero_target_fence_acknowledged: true,
                remote_apply_witnessed_shards: 0,
                shard_zero_remote_apply_witnessed: false,
            },
            ReplicationCorrelationSummary {
                correlated_shards: 1,
                shard_zero_correlated: true,
                acknowledged_correlated_shards: 0,
                shard_zero_target_fence_acknowledged: false,
                remote_apply_witnessed_shards: 2,
                shard_zero_remote_apply_witnessed: true,
            },
            ReplicationCorrelationSummary {
                correlated_shards: 1,
                shard_zero_correlated: false,
                acknowledged_correlated_shards: 0,
                shard_zero_target_fence_acknowledged: false,
                remote_apply_witnessed_shards: 1,
                shard_zero_remote_apply_witnessed: true,
            },
            ReplicationCorrelationSummary {
                correlated_shards: 1,
                shard_zero_correlated: true,
                acknowledged_correlated_shards: 1,
                shard_zero_target_fence_acknowledged: false,
                remote_apply_witnessed_shards: 0,
                shard_zero_remote_apply_witnessed: false,
            },
            ReplicationCorrelationSummary {
                correlated_shards: 1,
                shard_zero_correlated: true,
                acknowledged_correlated_shards: 0,
                shard_zero_target_fence_acknowledged: false,
                remote_apply_witnessed_shards: 1,
                shard_zero_remote_apply_witnessed: false,
            },
        ] {
            let state = bootstrap_observation_state();
            state.record_agent_status_collecting(Duration::from_secs(5));
            assert!(!state.record_agent_status_fresh(
                6,
                impossible,
                Instant::now() + Duration::from_secs(5),
            ));
            let snapshot = state.snapshot();
            assert_eq!(snapshot.agent_status.replication_correlated_shards, 0);
            assert_eq!(snapshot.agent_status.target_fence_acknowledged_shards, 0);
            assert_eq!(snapshot.agent_status.remote_apply_witnessed_shards, 0);
        }
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

    #[test]
    fn coordination_and_diagnostics_expire_across_host_suspend() {
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");

        assert!(state.record_coordination_ready("lease-uid", "1", true, deadline));
        state.record_agent_status_collecting(Duration::from_secs(2));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(2));
        assert!(state.record_agent_status_fresh(
            6,
            ReplicationCorrelationSummary::default(),
            deadline,
        ));
        assert!(state.record_catalog_candidates_fresh(3, deadline));

        clock
            .advance(Duration::from_secs(3))
            .expect("advance through suspend");
        let snapshot = state.snapshot();
        assert!(!snapshot.coordination_ready);
        assert!(!snapshot.leader);
        assert_eq!(snapshot.agent_status.phase, AgentStatusPhase::Expired);
        assert_eq!(
            snapshot.catalog_candidates.phase,
            CatalogCandidatePhase::Expired
        );
    }

    #[test]
    fn authority_clock_failure_clears_coordination_and_diagnostics() {
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");

        assert!(state.record_coordination_ready("lease-uid", "1", true, deadline));
        state.record_agent_status_collecting(Duration::from_secs(2));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(2));
        assert!(state.record_agent_status_fresh(
            6,
            ReplicationCorrelationSummary::default(),
            deadline,
        ));
        assert!(state.record_catalog_candidates_fresh(3, deadline));

        clock.fail();
        let snapshot = state.snapshot();
        assert!(!snapshot.coordination_ready);
        assert!(!snapshot.leader);
        assert_eq!(snapshot.agent_status.phase, AgentStatusPhase::Expired);
        assert_eq!(
            snapshot.catalog_candidates.phase,
            CatalogCandidatePhase::Expired
        );
        assert_eq!(
            state.readiness().reason,
            OrchReadinessReason::CoordinationUnavailable
        );
    }

    #[test]
    fn record_methods_sample_suspend_aware_time_while_state_is_locked() {
        let clock = Arc::new(LockAssertingBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");

        state.record_agent_status_collecting(Duration::from_secs(2));
        state.record_catalog_candidates_collecting(3, Duration::from_secs(2));
        clock.require_state_lock(&state);
        assert!(state.record_coordination_ready("lease-uid", "1", true, deadline));
        assert!(state.record_agent_status_fresh(
            6,
            ReplicationCorrelationSummary::default(),
            deadline,
        ));
        assert!(state.record_catalog_candidates_fresh(3, deadline));
    }

    #[test]
    fn readiness_samples_suspend_aware_time_while_state_is_locked() {
        let clock = Arc::new(LockAssertingBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");
        assert!(state.record_coordination_ready("lease-uid", "1", true, deadline));

        clock.require_state_lock(&state);
        assert!(state.readiness().ready);
    }

    #[test]
    fn snapshot_samples_suspend_aware_time_while_state_is_locked() {
        let clock = Arc::new(LockAssertingBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = bootstrap_observation_state_with_clock(clock.clone());
        let deadline = state
            .suspend_aware_deadline_after(Duration::from_secs(2))
            .expect("deadline");
        assert!(state.record_coordination_ready("lease-uid", "1", true, deadline));

        clock.require_state_lock(&state);
        let snapshot = state.snapshot();
        assert!(snapshot.coordination_ready);
        assert!(snapshot.leader);
    }
}
