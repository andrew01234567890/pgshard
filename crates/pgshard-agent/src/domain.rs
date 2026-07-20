//! Fail-closed `PostgreSQL` identity, observation, and fencing state.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pgshard_types::writable_generation::DurableWritableGeneration;
use pgshard_types::{PgLsn, ShardId};
use serde::{Serialize, Serializer};
use thiserror::Error;

use crate::boottime::{BoottimeClock, BoottimeError, BoottimeInstant, system_clock};

/// Maximum age of a role/fence observation that can authorize readiness.
pub const POSTGRES_OBSERVATION_MAX_AGE_MS: u64 = 5_000;

/// Maximum age of one coherent physical-replication observation that may be
/// considered by the future initial-serving gate.
pub const REPLICATION_EVIDENCE_MAX_AGE_MS: u64 = 5_000;

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
    /// The postmaster is starting as a replication-only physical-clone source.
    StartingReplicationBootstrap,
    /// The postmaster accepts only authenticated physical-replication traffic.
    RunningReplicationBootstrap,
    /// The postmaster is starting as a non-serving physical-replication standby.
    StartingReplicationStandby,
    /// The postmaster is replaying as a non-serving physical-replication standby.
    RunningReplicationStandby,
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
    /// Live authority is bounded independently by a local suspend-aware deadline.
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

// Serde's `serialize_with` callback ABI passes the field by reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_lsn<S>(value: &PgLsn, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.0.to_string())
}

/// Exact generation durability configured for the bootstrap source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum GenerationDurabilityEvidence {
    /// The generation is durable only on the source.
    Local,
    /// The generation must be replayed by any one canonical managed standby.
    RemoteApplyAnyOne {
        /// Exact ordered member application-name candidates.
        candidates: Vec<String>,
    },
}

/// `pg_stat_replication` states accepted from `PostgreSQL`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationStreamState {
    /// Walsender startup handshake is in progress.
    Startup,
    /// The standby is catching up to the source.
    Catchup,
    /// WAL is streaming continuously.
    Streaming,
    /// A base-backup walsender is active.
    Backup,
    /// Walsender shutdown is in progress.
    Stopping,
}

/// `pg_stat_replication.sync_state` values accepted from `PostgreSQL`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationSyncState {
    /// The standby is not a synchronous candidate.
    Async,
    /// The standby is a spare priority-based synchronous candidate.
    Potential,
    /// The standby is the selected priority-based synchronous standby.
    Sync,
    /// The standby participates in quorum-based synchronous replication.
    Quorum,
}

/// One configured source-side member slot and its live walsender progress.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SourceReplicationCandidateEvidence {
    /// Canonical member application and physical-slot identity.
    pub member_slot_name: String,
    /// Whether the permanent physical slot exists and is active.
    pub slot_active: bool,
    /// Whether the active slot PID and the uniquely named walsender agree.
    pub slot_walsender_match: bool,
    /// Current walsender state, absent while disconnected.
    pub stream_state: Option<ReplicationStreamState>,
    /// Current synchronous-selection state, absent while disconnected.
    pub sync_state: Option<ReplicationSyncState>,
    /// Last WAL position reported flushed by this standby.
    #[serde(serialize_with = "serialize_optional_lsn")]
    pub flush_lsn: Option<PgLsn>,
    /// Last WAL position reported replayed by this standby.
    #[serde(serialize_with = "serialize_optional_lsn")]
    pub replay_lsn: Option<PgLsn>,
}

/// Coherent evidence sampled from a replication-bootstrap source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SourceReplicationEvidence {
    /// Local Unix time after the complete bounded observation succeeded.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub observed_at_unix_ms: u64,
    /// `PostgreSQL` physical-cluster identifier.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub system_identifier: u64,
    /// Current `PostgreSQL` timeline.
    pub timeline: u32,
    /// Exact recovery state; a bootstrap source must report false.
    pub in_recovery: bool,
    /// Exact canonical writable-generation row observed under relation locks.
    pub generation_identity: String,
    /// Source flush position used as the exact candidate replay barrier.
    #[serde(serialize_with = "serialize_lsn")]
    pub generation_barrier_lsn: PgLsn,
    /// Exact configured generation durability and candidate set.
    pub durability: GenerationDurabilityEvidence,
    /// One entry for every configured canonical candidate, in configured order.
    pub candidates: Vec<SourceReplicationCandidateEvidence>,
}

/// Coherent evidence sampled from a replication standby.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StandbyReplicationEvidence {
    /// Local Unix time after the complete bounded observation succeeded.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub observed_at_unix_ms: u64,
    /// `PostgreSQL` physical-cluster identifier.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub system_identifier: u64,
    /// Current `PostgreSQL` recovery timeline.
    pub timeline: u32,
    /// Exact recovery state; a physical standby must report true.
    pub in_recovery: bool,
    /// Exact canonical writable-generation row replayed locally.
    pub generation_identity: String,
    /// Configured canonical member application and physical-slot identity.
    pub member_slot_name: String,
    /// Last WAL position received from the source.
    #[serde(serialize_with = "serialize_lsn")]
    pub receive_lsn: PgLsn,
    /// Last WAL position replayed locally.
    #[serde(serialize_with = "serialize_lsn")]
    pub replay_lsn: PgLsn,
}

/// Non-authoritative physical-replication evidence exposed in agent status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum ReplicationEvidence {
    /// Evidence from the writable-Lease-fenced clone source.
    Source(SourceReplicationEvidence),
    /// Evidence from a TCP-closed physical standby.
    Standby(StandbyReplicationEvidence),
}

/// Exact, non-secret runtime configuration identity published in agent status.
///
/// This record is untrusted, non-authoritative diagnostic evidence. Publishing
/// a source record does not authorize SQL traffic or change the postmaster's
/// network policy. No serving or activation decision may consume it until a
/// collector binds each response to the controller-observed Pod namespace,
/// name, UID, queried endpoint, and expected source Lease namespace/name/UID.
/// That binding is necessary but not sufficient: any future decision also
/// needs live controller-observed Pod, endpoint, and Lease evidence, the exact
/// candidate checkpoint and durable admission term, and an explicit fencing
/// and routing policy compatible with unmodified `PostgreSQL`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ActivationConfigEvidence {
    /// Stable agent identity configured for this process.
    pub identity: AgentIdentity,
    /// Exact Kubernetes cluster-object incarnation.
    pub cluster_uid: String,
    /// Exact Kubernetes Pod incarnation running this process.
    pub pod_uid: String,
    /// Role-specific non-serving `PostgreSQL` configuration.
    #[serde(flatten)]
    pub postgres: ActivationPostgresConfig,
}

/// Role-specific configuration identity for non-serving activation evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum ActivationPostgresConfig {
    /// Writable-Lease-fenced physical-clone source configuration.
    Source {
        /// Namespace containing the exact writable-term Lease.
        lease_namespace: String,
        /// Exact writable-term Lease name.
        lease_name: String,
        /// Exact writable-term Lease incarnation.
        lease_uid: String,
        /// Exact configured generation durability and candidate set.
        durability: GenerationDurabilityEvidence,
        /// Minimum remaining target-fence validity required by supervision.
        #[serde(serialize_with = "serialize_u64_decimal")]
        target_fence_required_margin_ms: u64,
    },
    /// TCP-closed physical standby configuration.
    Standby {
        /// Exact configured source DNS identity.
        primary_host: String,
        /// Exact configured source `PostgreSQL` port.
        primary_port: u16,
        /// Canonical member application and physical-slot identity.
        member_slot_name: String,
    },
}

/// Exact target-fence acknowledgement retained for the current postmaster.
///
/// The control-backend PID identifies the retained `PostgreSQL` session that
/// owns the extension's statement-admission fence. The postmaster PID binds
/// that ACK to the locally pidfd-supervised process incarnation. This ACK does
/// not prove interruption of already-running backends, physical walsenders, or
/// auxiliary WAL writers; complete fencing still requires the agent to stop
/// and reap the postmaster process tree within its Lease safety margin. This
/// ACK does not prove that process-tree absence or promotion safety has been
/// established. These values are diagnostic and never grant authority by
/// themselves.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TargetFenceAcknowledgement {
    /// Local Unix time immediately after the exact target ACK was validated.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub observed_at_unix_ms: u64,
    /// Exact canonical writable-generation bytes acknowledged by `PostgreSQL`.
    pub generation_identity: String,
    /// Exact local `CLOCK_BOOTTIME` deadline acknowledged by `PostgreSQL`.
    /// This raw value is diagnostic only and is never compared across hosts.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub deadline_boottime_ns: u64,
    /// Source-local remaining validity sampled immediately after the ACK.
    ///
    /// This is a diagnostic upper bound at the source's ACK instant. A remote
    /// observer must not age it using wall time, retain it as last-known-good,
    /// or use it for authorization. Any future collector must establish its
    /// own monotonic request/receipt freshness bound.
    #[serde(serialize_with = "serialize_u64_decimal")]
    pub remaining_validity_at_ack_ms: u64,
    /// Linux boot incarnation containing the supervised postmaster and ACK.
    pub boot_id: String,
    /// PID of the locally pidfd-supervised postmaster.
    pub postmaster_pid: u32,
    /// PID of the retained `PostgreSQL` target-fence control backend.
    pub control_backend_pid: u32,
}

/// PID identity of the currently pidfd-supervised postmaster.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PostgresProcessIdentity {
    /// Linux PID captured from the spawned child before supervision begins.
    pub postmaster_pid: u32,
    /// Canonical Linux boot ID in which the pidfd was opened.
    pub boot_id: String,
}

/// Pure result of evaluating whether replication evidence could support a
/// future initial-serving transition.
///
/// This value is diagnostic only. It does not change readiness, role, Lease
/// authority, or serving state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialServingEligibility {
    /// One remote-apply standby coherently witnessed the source barrier.
    Eligible,
    /// The evidence is coherent, but the configured generation durability is
    /// explicitly local-only and therefore an HA downgrade.
    AsynchronousDurabilityDowngrade,
    /// Required source or standby evidence is absent.
    EvidenceMissing,
    /// An evidence timestamp is zero or in the future.
    EvidenceTimeInvalid,
    /// One required observation is older than the bounded freshness window.
    EvidenceStale,
    /// A canonical generation could not be reconstructed exactly.
    GenerationInvalid,
    /// Physical-cluster or timeline identity is invalid or inconsistent.
    PhysicalIdentityMismatch,
    /// Source or standby recovery state contradicts its supervised role.
    RecoveryStateMismatch,
    /// The source evidence has a malformed or incomplete candidate set.
    CandidateSetInvalid,
    /// No coherent synchronous standby has replayed the sampled source barrier.
    SynchronousWitnessMissing,
}

/// Externally reportable, non-authoritative agent state.
///
/// HTTP status is self-reported diagnostic evidence. A future collector must
/// preserve controller-observed Pod and endpoint provenance; this snapshot by
/// itself must never authorize serving, activation, promotion, or routing.
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
    /// Fresh non-authoritative replication/generation evidence, when observed.
    pub replication_evidence: Option<ReplicationEvidence>,
    /// Exact non-serving runtime configuration identity, when configured.
    pub activation_config: Option<ActivationConfigEvidence>,
    /// Latest exact target-fence ACK for the current supervised source.
    pub target_fence_acknowledgement: Option<TargetFenceAcknowledgement>,
    /// PID identity of the current locally supervised postmaster.
    pub postgres_process_identity: Option<PostgresProcessIdentity>,
}

/// Thread-safe state shared by reconciliation and HTTP handlers.
#[derive(Clone, Debug)]
pub struct AgentState {
    inner: Arc<RwLock<AgentInner>>,
    last_checked_unix_ms: Arc<AtomicU64>,
    highest_lease_epoch: Arc<AtomicU64>,
    max_lease_ttl_ms: u64,
    boottime: Arc<dyn BoottimeClock>,
}

#[derive(Debug, Default)]
struct AgentInner {
    snapshot: AgentSnapshot,
    lease_deadline: Option<LeaseDeadline>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LeaseDeadline {
    epoch: u64,
    expires_at: BoottimeInstant,
}

impl Default for AgentState {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            last_checked_unix_ms: Arc::default(),
            highest_lease_epoch: Arc::default(),
            max_lease_ttl_ms: 0,
            boottime: system_clock(),
        }
    }
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
        Self::with_identity_and_clock(identity, max_lease_ttl_ms, system_clock())
    }

    fn with_identity_and_clock(
        identity: AgentIdentity,
        max_lease_ttl_ms: u64,
        boottime: Arc<dyn BoottimeClock>,
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
            boottime,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_test_clock(
        identity: AgentIdentity,
        max_lease_ttl_ms: u64,
        boottime: Arc<dyn BoottimeClock>,
    ) -> Result<Self, LeaseInstallError> {
        Self::with_identity_and_clock(identity, max_lease_ttl_ms, boottime)
    }

    pub(crate) fn boottime_clock(&self) -> Arc<dyn BoottimeClock> {
        Arc::clone(&self.boottime)
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
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.snapshot.postgres_process = process;
        if !matches!(
            process,
            PostgresProcessState::StartingReplicationBootstrap
                | PostgresProcessState::RunningReplicationBootstrap
                | PostgresProcessState::StartingReplicationStandby
                | PostgresProcessState::RunningReplicationStandby
        ) {
            inner.snapshot.replication_evidence = None;
        }
        if !matches!(
            process,
            PostgresProcessState::StartingReplicationBootstrap
                | PostgresProcessState::RunningReplicationBootstrap
        ) {
            inner.snapshot.target_fence_acknowledgement = None;
        }
        if !matches!(
            process,
            PostgresProcessState::StartingQuarantined
                | PostgresProcessState::RunningQuarantined
                | PostgresProcessState::StartingReplicationBootstrap
                | PostgresProcessState::RunningReplicationBootstrap
                | PostgresProcessState::StartingReplicationStandby
                | PostgresProcessState::RunningReplicationStandby
        ) {
            inner.snapshot.postgres_process_identity = None;
        }
    }

    /// Records the exact spawned postmaster PID while the process is in a
    /// locally supervised lifecycle.
    pub(crate) fn set_postgres_process_identity(&self, postmaster_pid: u32, boot_id: String) {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let process_running = matches!(
            inner.snapshot.postgres_process,
            PostgresProcessState::StartingQuarantined
                | PostgresProcessState::RunningQuarantined
                | PostgresProcessState::StartingReplicationBootstrap
                | PostgresProcessState::RunningReplicationBootstrap
                | PostgresProcessState::StartingReplicationStandby
                | PostgresProcessState::RunningReplicationStandby
        );
        inner.snapshot.postgres_process_identity =
            (process_running && postmaster_pid != 0 && canonical_linux_boot_id(&boot_id))
                .then_some(PostgresProcessIdentity {
                    postmaster_pid,
                    boot_id,
                });
    }

    /// Publishes the exact non-serving runtime configuration identity.
    ///
    /// Configuration is established once before the HTTP listener starts and
    /// remains status-only for the process lifetime.
    pub fn set_activation_config(&self, config: ActivationConfigEvidence) {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.snapshot.identity.as_ref() == Some(&config.identity) {
            inner.snapshot.activation_config = Some(config);
        }
    }

    /// Publishes an exact target-fence ACK only while the corresponding source
    /// postmaster is in a non-serving supervised lifecycle.
    pub(crate) fn set_target_fence_acknowledgement(
        &self,
        acknowledgement: TargetFenceAcknowledgement,
    ) {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let source_configured = matches!(
            inner
                .snapshot
                .activation_config
                .as_ref()
                .map(|config| &config.postgres),
            Some(ActivationPostgresConfig::Source { .. })
        );
        let source_running = matches!(
            inner.snapshot.postgres_process,
            PostgresProcessState::StartingReplicationBootstrap
                | PostgresProcessState::RunningReplicationBootstrap
        );
        inner.snapshot.target_fence_acknowledgement =
            (source_configured && source_running).then_some(acknowledgement);
    }

    /// Clears target-fence evidence after authority or process loss.
    pub(crate) fn clear_target_fence_acknowledgement(&self) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
            .target_fence_acknowledgement = None;
    }

    /// Replaces the last coherent physical-replication evidence atomically.
    pub fn set_replication_evidence(&self, evidence: ReplicationEvidence) {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let process_matches = matches!(
            (&evidence, inner.snapshot.postgres_process),
            (
                ReplicationEvidence::Source(_),
                PostgresProcessState::StartingReplicationBootstrap
                    | PostgresProcessState::RunningReplicationBootstrap
            ) | (
                ReplicationEvidence::Standby(_),
                PostgresProcessState::StartingReplicationStandby
                    | PostgresProcessState::RunningReplicationStandby
            )
        );
        inner.snapshot.replication_evidence = process_matches.then_some(evidence);
    }

    /// Clears replication evidence immediately after any failed SQL sample or
    /// process lifecycle transition.
    pub fn clear_replication_evidence(&self) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
            .replication_evidence = None;
    }

    /// Installs or renews a lease already authenticated by the orchestrator
    /// client. Epochs never move backwards, and clearing a lease revokes that
    /// term so a delayed copy cannot reauthorize the instance.
    ///
    /// # Errors
    ///
    /// Returns an error for the wrong instance, a reserved or stale epoch, or a
    /// renewal that shortens the existing boot-time authorization. Wall-clock
    /// expiry is status-only and is clamped when the system clock moves
    /// backwards.
    pub fn install_lease(
        &self,
        lease: FencingLease,
        now_unix_ms: u64,
    ) -> Result<LeaseInstallOutcome, LeaseInstallError> {
        let now = self.boottime.now()?;
        self.install_lease_at(lease, now_unix_ms, now, now)
    }

    /// Installs authority whose suspend-aware validity window began at
    /// `valid_from`. Callers performing a remote compare-and-swap must capture
    /// that instant before dispatch and pass the later `observed_at` instant so
    /// response latency consumes rather than extends the lease.
    pub(crate) fn install_lease_at(
        &self,
        mut lease: FencingLease,
        valid_from_unix_ms: u64,
        valid_from: BoottimeInstant,
        observed_at: BoottimeInstant,
    ) -> Result<LeaseInstallOutcome, LeaseInstallError> {
        if observed_at < valid_from {
            return Err(BoottimeError::RegressiveObservation.into());
        }
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
            inner.snapshot.target_fence_acknowledgement = None;
            inner.lease_deadline = None;
        }
    }

    /// Evaluates readiness against the current reporting wall clock and
    /// suspend-aware authority clock.
    #[must_use]
    pub fn readiness(&self) -> Readiness {
        let now_unix_ms = unix_time_ms();
        match self.boottime.now() {
            Ok(now) => self.readiness_at(now_unix_ms, now),
            Err(_) => Readiness {
                ready: false,
                reason: ReadinessReason::AuthorityClockUnavailable,
            },
        }
    }

    /// Evaluates deterministic readiness at supplied reporting wall and boot
    /// times.
    #[must_use]
    pub fn readiness_at(&self, now_unix_ms: u64, now: BoottimeInstant) -> Readiness {
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
    pub(crate) fn lease_deadline(&self) -> Option<BoottimeInstant> {
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
    /// A suspend-aware lease deadline could not be represented.
    #[error("fencing lease boot-time deadline overflowed")]
    DeadlineOverflow,
    /// Reported lease state exists without its matching boot-time authority.
    #[error("fencing lease has no matching boot-time deadline")]
    DeadlineMissing,
    /// A renewal cannot move the suspend-aware deadline backwards.
    #[error("lease renewal cannot shorten its boot-time deadline")]
    RegressiveDeadline,
    /// The suspend-aware local authority clock could not be read.
    #[error("suspend-aware authority clock is unavailable: {0}")]
    AuthorityClock(#[from] BoottimeError),
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
    /// The local postmaster is a non-serving physical-replication bootstrap source.
    PostgresReplicationBootstrap,
    /// The local postmaster is a non-serving physical-replication standby.
    PostgresReplicationStandby,
    /// The local postmaster was target-fenced and requires restart recovery.
    PostgresFenced,
    /// Local postmaster validation, startup, supervision, or shutdown failed.
    PostgresFailed,
    /// The lease belongs to another instance.
    LeaseOwnerMismatch,
    /// Epoch zero can never authorize an instance.
    LeaseEpochInvalid,
    /// Reported lease state has no matching local suspend-aware authority.
    LeaseDeadlineMissing,
    /// The suspend-aware local authority clock could not be read.
    AuthorityClockUnavailable,
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
    now: BoottimeInstant,
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
        (
            Some(_),
            PostgresProcessState::StartingReplicationBootstrap
            | PostgresProcessState::RunningReplicationBootstrap,
            _,
            _,
            _,
        ) => ReadinessReason::PostgresReplicationBootstrap,
        (
            Some(_),
            PostgresProcessState::StartingReplicationStandby
            | PostgresProcessState::RunningReplicationStandby,
            _,
            _,
            _,
        ) => ReadinessReason::PostgresReplicationStandby,
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

/// Classifies collected source and standby evidence without changing any
/// agent state.
///
/// The classifier is deliberately stricter than current readiness: it is a
/// building block for a future explicit serving transition, not permission to
/// serve. Local-only generation durability is surfaced as a distinct downgrade
/// rather than being confused with either HA eligibility or missing evidence.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn classify_initial_serving_eligibility(
    source: Option<&SourceReplicationEvidence>,
    standbys: &[StandbyReplicationEvidence],
    now_unix_ms: u64,
) -> InitialServingEligibility {
    let Some(source) = source else {
        return InitialServingEligibility::EvidenceMissing;
    };
    if !evidence_time_valid(source.observed_at_unix_ms, now_unix_ms) {
        return InitialServingEligibility::EvidenceTimeInvalid;
    }
    if evidence_stale(source.observed_at_unix_ms, now_unix_ms) {
        return InitialServingEligibility::EvidenceStale;
    }
    if source.system_identifier == 0 || source.timeline == 0 || source.generation_barrier_lsn.0 == 0
    {
        return InitialServingEligibility::PhysicalIdentityMismatch;
    }
    if source.in_recovery {
        return InitialServingEligibility::RecoveryStateMismatch;
    }
    if !canonical_generation_identity(&source.generation_identity) {
        return InitialServingEligibility::GenerationInvalid;
    }

    let GenerationDurabilityEvidence::RemoteApplyAnyOne { candidates } = &source.durability else {
        return InitialServingEligibility::AsynchronousDurabilityDowngrade;
    };
    if !canonical_candidate_set(candidates)
        || source.candidates.len() != candidates.len()
        || source
            .candidates
            .iter()
            .zip(candidates)
            .any(|(observed, configured)| observed.member_slot_name != *configured)
    {
        return InitialServingEligibility::CandidateSetInvalid;
    }

    // `ANY 1` is existential: a malformed, stale, or otherwise unusable
    // observation for one configured candidate must not poison a distinct
    // candidate that supplies an exact witness. Accumulate deterministic
    // diagnostics while considering every source-qualified candidate.
    let mut evidence_time_invalid = false;
    let mut stale_evidence_seen = false;
    let mut physical_identity_mismatch = false;
    let mut recovery_state_mismatch = false;
    let mut generation_invalid = false;
    let mut evidence_missing = false;
    for candidate in &source.candidates {
        if !candidate.slot_active
            || !candidate.slot_walsender_match
            || candidate.stream_state != Some(ReplicationStreamState::Streaming)
            || !matches!(
                candidate.sync_state,
                Some(ReplicationSyncState::Sync | ReplicationSyncState::Quorum)
            )
            || candidate
                .flush_lsn
                .is_none_or(|lsn| lsn.0 < source.generation_barrier_lsn.0)
            || candidate
                .replay_lsn
                .is_none_or(|lsn| lsn.0 < source.generation_barrier_lsn.0)
        {
            continue;
        }

        let matching: Vec<_> = standbys
            .iter()
            .filter(|standby| standby.member_slot_name == candidate.member_slot_name)
            .collect();
        if matching.len() != 1 {
            evidence_missing = true;
            continue;
        }
        let standby = matching[0];
        if !evidence_time_valid(standby.observed_at_unix_ms, now_unix_ms) {
            evidence_time_invalid = true;
            continue;
        }
        if evidence_stale(standby.observed_at_unix_ms, now_unix_ms) {
            stale_evidence_seen = true;
            continue;
        }
        if standby.system_identifier != source.system_identifier
            || standby.timeline != source.timeline
            || standby.system_identifier == 0
            || standby.timeline == 0
        {
            physical_identity_mismatch = true;
            continue;
        }
        if !standby.in_recovery {
            recovery_state_mismatch = true;
            continue;
        }
        if standby.generation_identity != source.generation_identity
            || !canonical_generation_identity(&standby.generation_identity)
        {
            generation_invalid = true;
            continue;
        }
        if standby.receive_lsn.0 < source.generation_barrier_lsn.0
            || standby.replay_lsn.0 < source.generation_barrier_lsn.0
        {
            continue;
        }
        return InitialServingEligibility::Eligible;
    }

    if evidence_time_invalid {
        InitialServingEligibility::EvidenceTimeInvalid
    } else if stale_evidence_seen {
        InitialServingEligibility::EvidenceStale
    } else if physical_identity_mismatch {
        InitialServingEligibility::PhysicalIdentityMismatch
    } else if recovery_state_mismatch {
        InitialServingEligibility::RecoveryStateMismatch
    } else if generation_invalid {
        InitialServingEligibility::GenerationInvalid
    } else if evidence_missing {
        InitialServingEligibility::EvidenceMissing
    } else {
        InitialServingEligibility::SynchronousWitnessMissing
    }
}

pub(crate) fn canonical_linux_boot_id(boot_id: &str) -> bool {
    boot_id.len() == 36
        && boot_id.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
            }
        })
}

fn evidence_time_valid(observed_at_unix_ms: u64, now_unix_ms: u64) -> bool {
    observed_at_unix_ms != 0 && observed_at_unix_ms <= now_unix_ms
}

fn evidence_stale(observed_at_unix_ms: u64, now_unix_ms: u64) -> bool {
    now_unix_ms - observed_at_unix_ms > REPLICATION_EVIDENCE_MAX_AGE_MS
}

fn canonical_generation_identity(value: &str) -> bool {
    DurableWritableGeneration::parse_canonical(value.as_bytes())
        .is_some_and(|generation| generation.canonical_bytes() == value.as_bytes())
}

fn canonical_candidate_set(candidates: &[String]) -> bool {
    matches!(candidates.len(), 2 | 4)
        && candidates
            .iter()
            .enumerate()
            .all(|(index, candidate)| *candidate == format!("pgshard_member_{:04}", index + 1))
}

pub(crate) fn unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_boottime_now() -> BoottimeInstant {
        system_clock().now().expect("read CLOCK_BOOTTIME")
    }

    fn advance(at: BoottimeInstant, duration: Duration) -> BoottimeInstant {
        at.checked_add(duration).expect("test boot time fits")
    }

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

    fn generation_identity() -> String {
        String::from_utf8(
            DurableWritableGeneration::new(
                "cluster-1".to_owned(),
                "cluster-uid".to_owned(),
                ShardId(2),
                "database".to_owned(),
                "writable".to_owned(),
                "lease-uid".to_owned(),
                "instance-1/pod/0123456789abcdef01234567".to_owned(),
                7,
            )
            .expect("valid generation")
            .canonical_bytes(),
        )
        .expect("canonical generation is UTF-8")
    }

    fn source_evidence() -> SourceReplicationEvidence {
        let candidates = vec![
            "pgshard_member_0001".to_owned(),
            "pgshard_member_0002".to_owned(),
        ];
        SourceReplicationEvidence {
            observed_at_unix_ms: 10_000,
            system_identifier: 42,
            timeline: 3,
            in_recovery: false,
            generation_identity: generation_identity(),
            generation_barrier_lsn: PgLsn(100),
            durability: GenerationDurabilityEvidence::RemoteApplyAnyOne {
                candidates: candidates.clone(),
            },
            candidates: candidates
                .into_iter()
                .enumerate()
                .map(
                    |(index, member_slot_name)| SourceReplicationCandidateEvidence {
                        member_slot_name,
                        slot_active: index == 0,
                        slot_walsender_match: index == 0,
                        stream_state: (index == 0).then_some(ReplicationStreamState::Streaming),
                        sync_state: (index == 0).then_some(ReplicationSyncState::Quorum),
                        flush_lsn: (index == 0).then_some(PgLsn(100)),
                        replay_lsn: (index == 0).then_some(PgLsn(100)),
                    },
                )
                .collect(),
        }
    }

    fn standby_evidence() -> StandbyReplicationEvidence {
        StandbyReplicationEvidence {
            observed_at_unix_ms: 10_000,
            system_identifier: 42,
            timeline: 3,
            in_recovery: true,
            generation_identity: generation_identity(),
            member_slot_name: "pgshard_member_0001".to_owned(),
            receive_lsn: PgLsn(100),
            replay_lsn: PgLsn(100),
        }
    }

    fn qualify_second_source_candidate(source: &mut SourceReplicationEvidence) {
        let candidate = &mut source.candidates[1];
        candidate.slot_active = true;
        candidate.slot_walsender_match = true;
        candidate.stream_state = Some(ReplicationStreamState::Streaming);
        candidate.sync_state = Some(ReplicationSyncState::Quorum);
        candidate.flush_lsn = Some(source.generation_barrier_lsn);
        candidate.replay_lsn = Some(source.generation_barrier_lsn);
    }

    fn standby_evidence_for(member_slot_name: &str) -> StandbyReplicationEvidence {
        StandbyReplicationEvidence {
            member_slot_name: member_slot_name.to_owned(),
            ..standby_evidence()
        }
    }

    fn source_activation_snapshot() -> AgentSnapshot {
        let identity = identity();
        AgentSnapshot {
            identity: Some(identity.clone()),
            postgres_process: PostgresProcessState::RunningReplicationBootstrap,
            lease: Some(FencingLease {
                owner_instance: identity.instance_id.clone(),
                epoch: 7,
                valid_until_unix_ms: 100_000,
            }),
            replication_evidence: Some(ReplicationEvidence::Source(source_evidence())),
            activation_config: Some(ActivationConfigEvidence {
                identity,
                cluster_uid: "cluster-uid".to_owned(),
                pod_uid: "pod".to_owned(),
                postgres: ActivationPostgresConfig::Source {
                    lease_namespace: "database".to_owned(),
                    lease_name: "writable".to_owned(),
                    lease_uid: "lease-uid".to_owned(),
                    durability: GenerationDurabilityEvidence::RemoteApplyAnyOne {
                        candidates: vec![
                            "pgshard_member_0001".to_owned(),
                            "pgshard_member_0002".to_owned(),
                        ],
                    },
                    target_fence_required_margin_ms: 3_500,
                },
            }),
            target_fence_acknowledgement: Some(TargetFenceAcknowledgement {
                observed_at_unix_ms: 10_000,
                generation_identity: generation_identity(),
                deadline_boottime_ns: 100_000,
                remaining_validity_at_ack_ms: 60_000,
                boot_id: "11111111-2222-3333-8444-555555555555".to_owned(),
                postmaster_pid: 100,
                control_backend_pid: 101,
            }),
            postgres_process_identity: Some(PostgresProcessIdentity {
                postmaster_pid: 100,
                boot_id: "11111111-2222-3333-8444-555555555555".to_owned(),
            }),
            ..AgentSnapshot::default()
        }
    }

    fn install(
        state: &AgentState,
        lease: FencingLease,
        valid_from_unix_ms: u64,
        valid_from: BoottimeInstant,
    ) -> Result<LeaseInstallOutcome, LeaseInstallError> {
        state.install_lease_at(lease, valid_from_unix_ms, valid_from, valid_from)
    }

    #[test]
    fn replication_evidence_is_atomic_status_only_state() {
        let state = state();
        state.set_postgres_process(PostgresProcessState::StartingReplicationBootstrap);
        let readiness_before_evidence = state.readiness();
        let evidence = ReplicationEvidence::Source(source_evidence());
        state.set_replication_evidence(evidence.clone());
        assert_eq!(state.snapshot().replication_evidence, Some(evidence));
        let json = serde_json::to_value(state.snapshot()).expect("serialize evidence status");
        assert_eq!(json["replication_evidence"]["role"], "source");
        assert_eq!(json["replication_evidence"]["system_identifier"], "42");
        assert_eq!(
            json["replication_evidence"]["generation_barrier_lsn"],
            "100"
        );
        assert_eq!(
            state.readiness(),
            readiness_before_evidence,
            "evidence must never change readiness"
        );
        state.clear_replication_evidence();
        assert!(state.snapshot().replication_evidence.is_none());

        state.set_replication_evidence(ReplicationEvidence::Source(source_evidence()));
        state.set_postgres_process(PostgresProcessState::Stopping);
        assert!(state.snapshot().replication_evidence.is_none());
    }

    #[test]
    fn process_and_target_ack_evidence_are_atomic_status_only_state() {
        let state = state();
        state.set_activation_config(
            source_activation_snapshot()
                .activation_config
                .expect("source activation config"),
        );
        state.set_postgres_process(PostgresProcessState::StartingReplicationBootstrap);
        let boot_id = "11111111-2222-3333-8444-555555555555".to_owned();
        state.set_postgres_process_identity(100, boot_id.clone());
        let readiness_before_ack = state.readiness();
        state.set_target_fence_acknowledgement(TargetFenceAcknowledgement {
            observed_at_unix_ms: 10_000,
            generation_identity: generation_identity(),
            deadline_boottime_ns: 100_000,
            remaining_validity_at_ack_ms: 60_000,
            boot_id: boot_id.clone(),
            postmaster_pid: 100,
            control_backend_pid: 101,
        });
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot.postgres_process_identity,
            Some(PostgresProcessIdentity {
                postmaster_pid: 100,
                boot_id,
            })
        );
        assert_eq!(
            snapshot
                .target_fence_acknowledgement
                .as_ref()
                .expect("target ACK")
                .control_backend_pid,
            101
        );
        let json = serde_json::to_value(snapshot).expect("serialize activation status");
        assert_eq!(json["activation_config"]["role"], "source");
        assert_eq!(
            json["target_fence_acknowledgement"]["deadline_boottime_ns"],
            "100000"
        );
        assert_eq!(
            json["target_fence_acknowledgement"]["remaining_validity_at_ack_ms"],
            "60000"
        );
        assert_eq!(
            state.readiness(),
            readiness_before_ack,
            "activation evidence must never change readiness"
        );

        state.set_postgres_process(PostgresProcessState::Stopping);
        let snapshot = state.snapshot();
        assert!(snapshot.target_fence_acknowledgement.is_none());
        assert!(snapshot.postgres_process_identity.is_none());
        assert!(snapshot.activation_config.is_some());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn initial_serving_classifier_is_pure_fail_closed_and_exhaustive() {
        let now = 10_000;
        let source = source_evidence();
        let standby = standby_evidence();
        assert_eq!(
            classify_initial_serving_eligibility(None, &[], now),
            InitialServingEligibility::EvidenceMissing
        );
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                std::slice::from_ref(&standby),
                now
            ),
            InitialServingEligibility::Eligible
        );

        let mut changed = source.clone();
        changed.observed_at_unix_ms = 0;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&changed),
                std::slice::from_ref(&standby),
                now
            ),
            InitialServingEligibility::EvidenceTimeInvalid
        );
        changed.observed_at_unix_ms = now + 1;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&changed),
                std::slice::from_ref(&standby),
                now
            ),
            InitialServingEligibility::EvidenceTimeInvalid
        );
        changed.observed_at_unix_ms = now - REPLICATION_EVIDENCE_MAX_AGE_MS - 1;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&changed),
                std::slice::from_ref(&standby),
                now
            ),
            InitialServingEligibility::EvidenceStale
        );

        changed = source.clone();
        changed.system_identifier = 0;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&changed),
                std::slice::from_ref(&standby),
                now
            ),
            InitialServingEligibility::PhysicalIdentityMismatch
        );
        changed = source.clone();
        changed.in_recovery = true;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&changed),
                std::slice::from_ref(&standby),
                now
            ),
            InitialServingEligibility::RecoveryStateMismatch
        );
        changed = source.clone();
        changed.generation_identity.push('x');
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&changed),
                std::slice::from_ref(&standby),
                now
            ),
            InitialServingEligibility::GenerationInvalid
        );
        changed = source.clone();
        changed.durability = GenerationDurabilityEvidence::Local;
        assert_eq!(
            classify_initial_serving_eligibility(Some(&changed), &[], now),
            InitialServingEligibility::AsynchronousDurabilityDowngrade
        );

        changed = source.clone();
        changed.candidates.pop();
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&changed),
                std::slice::from_ref(&standby),
                now
            ),
            InitialServingEligibility::CandidateSetInvalid
        );
        changed = source.clone();
        changed.candidates[0].replay_lsn = Some(PgLsn(99));
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&changed),
                std::slice::from_ref(&standby),
                now
            ),
            InitialServingEligibility::SynchronousWitnessMissing
        );
        assert_eq!(
            classify_initial_serving_eligibility(Some(&source), &[], now),
            InitialServingEligibility::EvidenceMissing
        );

        let mut changed_standby = standby.clone();
        changed_standby.observed_at_unix_ms = 0;
        assert_eq!(
            classify_initial_serving_eligibility(Some(&source), &[changed_standby], now),
            InitialServingEligibility::EvidenceTimeInvalid
        );
        changed_standby = standby.clone();
        changed_standby.observed_at_unix_ms = now - REPLICATION_EVIDENCE_MAX_AGE_MS - 1;
        assert_eq!(
            classify_initial_serving_eligibility(Some(&source), &[changed_standby], now),
            InitialServingEligibility::EvidenceStale
        );
        changed_standby = standby.clone();
        changed_standby.system_identifier += 1;
        assert_eq!(
            classify_initial_serving_eligibility(Some(&source), &[changed_standby], now),
            InitialServingEligibility::PhysicalIdentityMismatch
        );
        changed_standby = standby.clone();
        changed_standby.in_recovery = false;
        assert_eq!(
            classify_initial_serving_eligibility(Some(&source), &[changed_standby], now),
            InitialServingEligibility::RecoveryStateMismatch
        );
        changed_standby = standby.clone();
        changed_standby.generation_identity = generation_identity().replace("term=7", "term=8");
        assert_eq!(
            classify_initial_serving_eligibility(Some(&source), &[changed_standby], now),
            InitialServingEligibility::GenerationInvalid
        );
        changed_standby = standby;
        changed_standby.replay_lsn = PgLsn(99);
        assert_eq!(
            classify_initial_serving_eligibility(Some(&source), &[changed_standby], now),
            InitialServingEligibility::SynchronousWitnessMissing
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn any_one_classifier_uses_a_valid_second_candidate_despite_bad_first_candidate() {
        let now = 10_000;
        let mut source = source_evidence();
        qualify_second_source_candidate(&mut source);
        let valid_second = standby_evidence_for("pgshard_member_0002");

        let mut first = standby_evidence();
        first.observed_at_unix_ms = 0;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[first, valid_second.clone()],
                now
            ),
            InitialServingEligibility::Eligible
        );
        let mut first = standby_evidence();
        first.observed_at_unix_ms = now + 1;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[first, valid_second.clone()],
                now
            ),
            InitialServingEligibility::Eligible
        );
        let mut first = standby_evidence();
        first.observed_at_unix_ms = now - REPLICATION_EVIDENCE_MAX_AGE_MS - 1;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[first, valid_second.clone()],
                now
            ),
            InitialServingEligibility::Eligible
        );

        let mut first = standby_evidence();
        first.system_identifier += 1;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[first, valid_second.clone()],
                now
            ),
            InitialServingEligibility::Eligible
        );
        let mut first = standby_evidence();
        first.system_identifier = 0;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[first, valid_second.clone()],
                now
            ),
            InitialServingEligibility::Eligible
        );
        let mut first = standby_evidence();
        first.in_recovery = false;
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[first, valid_second.clone()],
                now
            ),
            InitialServingEligibility::Eligible
        );

        let mut first = standby_evidence();
        first.generation_identity.push('x');
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[first, valid_second.clone()],
                now
            ),
            InitialServingEligibility::Eligible
        );
        let mut first = standby_evidence();
        first.generation_identity = generation_identity().replace("term=7", "term=8");
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[first, valid_second.clone()],
                now
            ),
            InitialServingEligibility::Eligible
        );
        let mut first = standby_evidence();
        first.replay_lsn = PgLsn(source.generation_barrier_lsn.0 - 1);
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[first, valid_second.clone()],
                now
            ),
            InitialServingEligibility::Eligible
        );

        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                std::slice::from_ref(&valid_second),
                now
            ),
            InitialServingEligibility::Eligible
        );
        assert_eq!(
            classify_initial_serving_eligibility(
                Some(&source),
                &[standby_evidence(), standby_evidence(), valid_second,],
                now
            ),
            InitialServingEligibility::Eligible
        );
    }

    #[test]
    fn readiness_fails_closed_without_identity_or_lease() {
        let now = test_boottime_now();
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
        let now = test_boottime_now();
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
                .readiness_at(100, advance(now, Duration::from_millis(1)))
                .reason,
            ReadinessReason::LeaseExpired
        );
    }

    #[test]
    fn readiness_requires_role_specific_lsn() {
        let state = state();
        let now = test_boottime_now();
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
        let now = test_boottime_now();
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
        let now = test_boottime_now();
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
        state.set_postgres_process(PostgresProcessState::RunningReplicationBootstrap);
        assert_eq!(
            state.readiness_at(100, now).reason,
            ReadinessReason::PostgresReplicationBootstrap
        );
        state.set_postgres_process(PostgresProcessState::StartingReplicationStandby);
        assert_eq!(
            state.readiness_at(100, now).reason,
            ReadinessReason::PostgresReplicationStandby
        );
        state.set_postgres_process(PostgresProcessState::RunningReplicationStandby);
        assert_eq!(
            state.readiness_at(100, now),
            Readiness {
                ready: false,
                reason: ReadinessReason::PostgresReplicationStandby,
            }
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
        let now = test_boottime_now();
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
        let now = test_boottime_now();
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
    fn wall_clock_rollback_cannot_revive_an_expired_lease() {
        let state = state();
        let now = test_boottime_now();
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
                .readiness_at(199, advance(now, Duration::from_millis(99)))
                .ready
        );
        assert_eq!(
            state
                .readiness_at(200, advance(now, Duration::from_millis(100)))
                .reason,
            ReadinessReason::LeaseExpired
        );
        assert_eq!(
            state
                .readiness_at(150, advance(now, Duration::from_millis(100)))
                .reason,
            ReadinessReason::LeaseExpired
        );
    }

    #[test]
    fn wall_clock_jump_cannot_expire_live_boottime_authority() {
        let state = state();
        let now = test_boottime_now();
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
            state.readiness_at(300, advance(now, Duration::from_millis(50))),
            Readiness {
                ready: true,
                reason: ReadinessReason::Ready,
            }
        );
    }

    #[test]
    fn suspend_like_boottime_jump_expires_authority_without_using_realtime() {
        let initial = BoottimeInstant::from_nanos_for_test(1_000_000_000);
        let clock = Arc::new(crate::boottime::FakeBoottimeClock::new(initial));
        let state = AgentState::with_test_clock(identity(), 10_000, clock.clone())
            .expect("valid fake-clock state");
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
            .expect("install fake-clock authority");

        assert!(
            state
                .readiness_at(100, clock.now().expect("fake clock"))
                .ready
        );
        // This models a host suspend/resume: wall time is deliberately held
        // constant while CLOCK_BOOTTIME advances past the authority deadline.
        clock
            .advance(Duration::from_millis(101))
            .expect("advance fake boot clock");
        assert_eq!(
            state.readiness_at(100, clock.now().expect("fake clock")),
            Readiness {
                ready: false,
                reason: ReadinessReason::LeaseExpired,
            }
        );
    }

    #[test]
    fn authority_clock_failure_and_deadline_overflow_fail_closed() {
        let initial = BoottimeInstant::from_nanos_for_test(1_000_000_000);
        let clock = Arc::new(crate::boottime::FakeBoottimeClock::new(initial));
        let failed_clock_state = AgentState::with_test_clock(identity(), 10_000, clock.clone())
            .expect("valid fake-clock state");
        clock.fail();
        assert!(matches!(
            failed_clock_state.install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 200,
                },
                100,
            ),
            Err(LeaseInstallError::AuthorityClock(_))
        ));
        assert_eq!(
            failed_clock_state.readiness().reason,
            ReadinessReason::AuthorityClockUnavailable
        );

        let state = state();
        let near_limit = BoottimeInstant::from_nanos_for_test(u64::MAX - 1);
        assert_eq!(
            state.install_lease_at(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 102,
                },
                100,
                near_limit,
                near_limit,
            ),
            Err(LeaseInstallError::DeadlineOverflow)
        );
        assert!(state.snapshot().lease.is_none());
    }

    #[test]
    fn delayed_install_consumes_authority_window() {
        let state = state();
        let valid_from = test_boottime_now();
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
                advance(valid_from, Duration::from_millis(99)),
            ),
            Ok(LeaseInstallOutcome::Installed)
        );
        assert_eq!(
            state.lease_deadline(),
            Some(advance(valid_from, Duration::from_millis(100)))
        );
        assert_eq!(
            state.install_lease_at(
                FencingLease { epoch: 4, ..lease },
                100,
                valid_from,
                advance(valid_from, Duration::from_millis(100)),
            ),
            Err(LeaseInstallError::Expired)
        );
    }

    #[test]
    fn regressive_boottime_observation_cannot_install_authority() {
        let state = state();
        let valid_from = BoottimeInstant::from_nanos_for_test(2_000_000_000);
        let observed_at = BoottimeInstant::from_nanos_for_test(1_999_999_999);

        assert_eq!(
            state.install_lease_at(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 3,
                    valid_until_unix_ms: 200,
                },
                100,
                valid_from,
                observed_at,
            ),
            Err(LeaseInstallError::AuthorityClock(
                BoottimeError::RegressiveObservation,
            ))
        );
        assert!(state.snapshot().lease.is_none());
        assert!(state.lease_deadline().is_none());
    }

    #[test]
    fn renewal_cannot_regress_boottime_deadline() {
        let state = state();
        let now = test_boottime_now();
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
        let initial_valid_from = test_boottime_now();
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

        let later_valid_from = advance(initial_valid_from, Duration::from_secs(1));
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
            Some(advance(initial_valid_from, Duration::from_secs(7)))
        );
    }

    #[test]
    fn status_json_uses_exact_decimal_strings() {
        let state = state();
        let now = test_boottime_now();
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
        let now = test_boottime_now();
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
        let now = test_boottime_now();
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
