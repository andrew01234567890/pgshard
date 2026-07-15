//! Fail-closed validation for attaching a managed decoder to a standby.
//!
//! This module validates one bounded observation set supplied by a future
//! orchestrator probe. It does not query `PostgreSQL`, create or drop slots,
//! select a candidate, mutate catalog state, or authorize a live replication
//! session. PID and start-LSN inputs remain caller reports until a future
//! connection-owning runtime binds them to actual protocol bytes.

use std::{num::NonZeroU32, time::Duration};

use pgshard_types::{CatalogEpoch, PgLsn};
use thiserror::Error;
use uuid::Uuid;

/// Shortest accepted age bound for physical-slot feedback observations.
pub const MIN_FEEDBACK_AGE_LIMIT: Duration = Duration::from_secs(2);
/// Longest accepted age bound for physical-slot feedback observations.
pub const MAX_FEEDBACK_AGE_LIMIT: Duration = Duration::from_mins(5);
/// Minimum scheduling margin between feedback reports and their health bound.
pub const MIN_FEEDBACK_REPORTING_MARGIN: Duration = Duration::from_secs(1);
/// Shortest accepted age bound for a successful slot-synchronization cycle.
pub const MIN_SLOT_SYNC_AGE_LIMIT: Duration = Duration::from_secs(1);
/// Longest accepted age bound for a successful slot-synchronization cycle.
pub const MAX_SLOT_SYNC_AGE_LIMIT: Duration = Duration::from_mins(5);
/// Longest accepted age bound for a multi-server preflight observation set.
pub const MAX_OBSERVATION_AGE_LIMIT: Duration = Duration::from_secs(30);

/// A `PostgreSQL` replication-slot name validated against server restrictions.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReplicationSlotName(String);

impl ReplicationSlotName {
    /// Creates a lowercase `PostgreSQL` replication-slot name.
    ///
    /// # Errors
    ///
    /// Returns [`SlotNameError`] unless the name contains 1-63 lowercase
    /// ASCII letters, digits, or underscores.
    pub fn new(value: impl Into<String>) -> Result<Self, SlotNameError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 63
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(SlotNameError);
        }
        Ok(Self(value))
    }

    /// Returns the exact server-side slot name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An invalid replication-slot name.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("replication slot name must contain 1-63 lowercase ASCII letters, digits, or underscores")]
pub struct SlotNameError;

/// Non-nil catalog allocation generation for one managed logical slot name.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SlotGeneration(Uuid);

impl SlotGeneration {
    /// Creates a non-nil slot generation.
    ///
    /// # Errors
    ///
    /// Rejects the nil UUID because it cannot identify a catalog allocation.
    pub fn new(value: Uuid) -> Result<Self, SlotGenerationError> {
        if value.is_nil() {
            return Err(SlotGenerationError);
        }
        Ok(Self(value))
    }

    /// Returns the exact catalog generation UUID.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// An invalid managed-slot generation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("managed slot generation must be non-nil")]
pub struct SlotGenerationError;

/// Immutable identity that prevents a decoder from crossing a restore or fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicationSourceIdentity {
    system_identifier: u64,
    timeline: u32,
    database_oid: u32,
    restore_incarnation: Uuid,
    catalog_epoch: CatalogEpoch,
}

impl ReplicationSourceIdentity {
    /// Creates a complete nonzero source identity.
    ///
    /// # Errors
    ///
    /// Returns [`SourceIdentityError`] when any numeric identity is zero or
    /// the restore incarnation is nil.
    pub fn new(
        system_identifier: u64,
        timeline: u32,
        database_oid: u32,
        restore_incarnation: Uuid,
        catalog_epoch: CatalogEpoch,
    ) -> Result<Self, SourceIdentityError> {
        if system_identifier == 0 {
            return Err(SourceIdentityError::SystemIdentifier);
        }
        if timeline == 0 {
            return Err(SourceIdentityError::Timeline);
        }
        if database_oid == 0 {
            return Err(SourceIdentityError::DatabaseOid);
        }
        if restore_incarnation.is_nil() {
            return Err(SourceIdentityError::RestoreIncarnation);
        }
        if catalog_epoch.0 == 0 {
            return Err(SourceIdentityError::CatalogEpoch);
        }
        Ok(Self {
            system_identifier,
            timeline,
            database_oid,
            restore_incarnation,
            catalog_epoch,
        })
    }

    /// Returns the `PostgreSQL` cluster system identifier.
    #[must_use]
    pub const fn system_identifier(self) -> u64 {
        self.system_identifier
    }

    /// Returns the observed `PostgreSQL` timeline.
    #[must_use]
    pub const fn timeline(self) -> u32 {
        self.timeline
    }

    /// Returns the exact logical database OID.
    #[must_use]
    pub const fn database_oid(self) -> u32 {
        self.database_oid
    }

    /// Returns the shard restore-incarnation UUID.
    #[must_use]
    pub const fn restore_incarnation(self) -> Uuid {
        self.restore_incarnation
    }

    /// Returns the catalog epoch that authorized this source.
    #[must_use]
    pub const fn catalog_epoch(self) -> CatalogEpoch {
        self.catalog_epoch
    }

    fn same_server_as(self, other: Self) -> bool {
        self.system_identifier == other.system_identifier
            && self.timeline == other.timeline
            && self.restore_incarnation == other.restore_incarnation
            && self.catalog_epoch == other.catalog_epoch
    }
}

/// An incomplete replication-source identity.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SourceIdentityError {
    /// `PostgreSQL` system identifier is zero.
    #[error("PostgreSQL system identifier must be nonzero")]
    SystemIdentifier,
    /// Timeline is zero.
    #[error("PostgreSQL timeline must be nonzero")]
    Timeline,
    /// Database OID is zero.
    #[error("PostgreSQL database OID must be nonzero")]
    DatabaseOid,
    /// Restore incarnation is nil.
    #[error("restore incarnation must be non-nil")]
    RestoreIncarnation,
    /// Catalog epoch is zero.
    #[error("catalog epoch must be nonzero")]
    CatalogEpoch,
}

/// `PostgreSQL`'s observed `wal_status` for a replication slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotWalRetention {
    /// Required WAL is retained within `max_wal_size`.
    Reserved,
    /// Required WAL is retained beyond `max_wal_size`.
    Extended,
    /// Required WAL is no longer guaranteed to remain retained.
    Unreserved,
    /// Required WAL has already been removed.
    Lost,
}

impl SlotWalRetention {
    const fn is_retained(self) -> bool {
        matches!(self, Self::Reserved | Self::Extended)
    }
}

/// Primary-side observation of the standby's physical slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalSlotObservation {
    /// Exact physical slot name.
    pub name: ReplicationSlotName,
    /// Upstream walsender PID currently owning the slot, if any.
    pub active_pid: Option<NonZeroU32>,
    /// `PostgreSQL`'s current WAL-retention state.
    pub wal_retention: Option<SlotWalRetention>,
    /// `PostgreSQL` invalidation reason, if present.
    pub invalidation: Option<SlotInvalidation>,
    /// Whether upstream `catalog_xmin` covers the standby decoder horizon.
    pub protects_catalog_horizon: bool,
    /// Age of the last observed feedback sample, if any.
    pub feedback_age: Option<Duration>,
}

/// `PostgreSQL` reason for invalidating a replication slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotInvalidation {
    /// Required WAL was removed.
    WalRemoved,
    /// Required catalog rows were removed.
    RowsRemoved,
    /// `wal_level` became insufficient.
    WalLevelInsufficient,
    /// The slot exceeded `idle_replication_slot_timeout`.
    IdleTimeout,
}

/// Binary state of a required `PostgreSQL` setting or slot property.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettingState {
    /// Setting is enabled.
    Enabled,
    /// Setting is disabled.
    Disabled,
}

/// Effective WAL level required for logical decoding on a standby.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalWalLevel {
    /// `wal_level = logical` is effective.
    Logical,
    /// The effective level cannot support logical decoding.
    Insufficient,
}

/// Whether the primary gates logical failover-slot progress on this standby.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailoverSlotSynchronization {
    /// The physical slot is present in `synchronized_standby_slots`.
    GatedOnPhysicalSlot,
    /// The physical slot is absent from `synchronized_standby_slots`.
    NotGated,
}

/// Whether a member is a standby or a writable server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryState {
    /// `pg_is_in_recovery()` is true.
    Standby,
    /// `pg_is_in_recovery()` is false.
    Writable,
}

/// Current WAL-receiver state relevant to decoder eligibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalReceiverState {
    /// Receiver is streaming from the expected primary.
    Streaming,
    /// Receiver is stopped, starting, or connected to another source.
    NotStreaming,
}

/// One successful slot-sync cycle tied to its worker connection generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotSyncSuccessObservation {
    /// Worker connection generation that performed the cycle.
    pub connection_generation: Uuid,
    /// Age since policy-relevant anchor state was observed synchronized.
    pub age: Duration,
}

/// Live `PostgreSQL` 18 slot-sync worker evidence from the direct primary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotSyncWorkerObservation {
    /// Non-nil generation for this exact upstream worker connection.
    pub connection_generation: Uuid,
    /// Source identity correlated with this exact worker connection.
    pub upstream_source_identity: ReplicationSourceIdentity,
    /// Most recent successful cycle on this connection, if any.
    pub last_success: Option<SlotSyncSuccessObservation>,
}

/// Logical replication output plugin observed for a slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalSlotPlugin {
    /// Exact built-in `pgoutput` plugin.
    PgOutput,
    /// Any other or missing output plugin.
    Other,
}

/// Physical role encoded by a logical slot's `failover` and `synced` flags.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalSlotKind {
    /// `failover = true` and `synced = false` on the original primary.
    FailoverAnchor,
    /// `failover = true` and `synced = true` on a standby or promoted primary.
    SynchronizedFailoverAnchor,
    /// `failover = false` and `synced = false` for standby-local decoding.
    StandbyLocalDecoder,
    /// Any unsafe or role-incompatible flag combination.
    Other,
}

/// Persistence evidence for one logical slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotPersistence {
    /// Mutation history or stronger evidence proves the slot is persistent.
    Persistent,
    /// `PostgreSQL` reports the slot as temporary and therefore not durable.
    NonPersistent,
    /// A non-temporary public-view row may be persistent or in `RS_EPHEMERAL`
    /// state; `temporary = false` and inactivity cannot distinguish them.
    Unproven,
}

/// Whether a backend currently owns a slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotActivity {
    /// No backend owns the slot.
    Inactive,
    /// Exact backend PID currently owning the slot.
    Active(NonZeroU32),
}

/// Catalog ownership classification for a replication slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotOwnership {
    /// Exact generation matches the reported active catalog allocation.
    ///
    /// This current-state observation is not proof of uninterrupted mutation
    /// authority or retired-generation history.
    Managed(SlotGeneration),
    /// Slot is user-owned, unknown, or otherwise unproven.
    Unknown,
}

/// Coherent observation of one logical replication slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSlotObservation {
    /// Exact cluster-wide slot name.
    pub name: ReplicationSlotName,
    /// Database that owns the slot.
    pub database_oid: u32,
    /// Observed output plugin.
    pub plugin: LogicalSlotPlugin,
    /// Observed `failover`/`synced` role.
    pub kind: LogicalSlotKind,
    /// Observed persistence.
    pub persistence: SlotPersistence,
    /// Whether the slot decodes prepared transactions.
    pub two_phase: SettingState,
    /// Exact LSN at which prepared-transaction decoding was enabled.
    pub two_phase_at: Option<PgLsn>,
    /// Observed backend ownership.
    pub activity: SlotActivity,
    /// `shardschema` ownership proof.
    pub ownership: SlotOwnership,
    /// `PostgreSQL` invalidation reason, if present.
    pub invalidation: Option<SlotInvalidation>,
    /// `PostgreSQL` WAL availability for the slot.
    pub wal_retention: Option<SlotWalRetention>,
    /// Durable confirmed-flush LSN, represented as `PostgreSQL`'s 64-bit WAL
    /// offset, if the slot has a usable consistent point.
    pub confirmed_flush_lsn: Option<PgLsn>,
}

/// One conservative replay floor coherently bound to its observed source lineage.
///
/// The fields are deliberately opaque. Only crate-owned source correlation may
/// construct production evidence. In particular, a bare
/// `pg_last_wal_replay_lsn()` result cannot create this proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceBoundReplayFloor {
    source_identity: ReplicationSourceIdentity,
    lsn: PgLsn,
}

impl SourceBoundReplayFloor {
    // The path's fields and constructor are private to the correlator, so no
    // other crate module can supply arbitrary source or LSN evidence here.
    pub(crate) const fn from_correlated_path(
        path: &crate::slot_observer::CorrelatedStandbyReplicationPath,
    ) -> Self {
        Self {
            source_identity: path.source_identity(),
            lsn: path.standby_replay_floor_lsn(),
        }
    }

    /// Returns the catalog-selected source identity whose observable
    /// `PostgreSQL` components were correlated with the replay floor.
    #[must_use]
    pub const fn source_identity(self) -> ReplicationSourceIdentity {
        self.source_identity
    }

    /// Returns the conservative replay floor after its lineage has been bound.
    #[must_use]
    pub const fn lsn(self) -> PgLsn {
        self.lsn
    }

    #[cfg(test)]
    const fn for_test(source_identity: ReplicationSourceIdentity, lsn: PgLsn) -> Self {
        Self {
            source_identity,
            lsn,
        }
    }
}

/// One bounded standby and upstream observation set.
///
/// `PostgreSQL` cannot provide a transaction spanning two servers. The future
/// probe must therefore collect these values within its freshness bound, then
/// recheck every invariant after exclusive slot acquisition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StandbyDecoderObservation {
    /// Age of the oldest value when this observation set is evaluated.
    pub oldest_observation_age: Duration,
    /// Stable ordinal of the observed member.
    pub member_ordinal: u16,
    /// Observed recovery role.
    pub recovery: RecoveryState,
    /// Identity observed from this exact source connection.
    pub source_identity: ReplicationSourceIdentity,
    /// Recovery state observed on the WAL receiver's direct upstream.
    pub upstream_recovery: RecoveryState,
    /// Identity observed from the direct upstream connection.
    pub upstream_source_identity: ReplicationSourceIdentity,
    /// Effective WAL level on the direct upstream.
    pub upstream_wal_level: LogicalWalLevel,
    /// Effective `hot_standby_feedback` setting.
    pub hot_standby_feedback: SettingState,
    /// Effective `wal_receiver_status_interval` setting.
    pub wal_receiver_status_interval: Duration,
    /// Effective `sync_replication_slots` setting.
    pub sync_replication_slots: SettingState,
    /// Live database connection and success evidence for the slot-sync worker.
    pub slot_sync_worker: Option<SlotSyncWorkerObservation>,
    /// Effective WAL level on the candidate.
    pub wal_level: LogicalWalLevel,
    /// Conservative replay floor bound to its coherently observed source lineage.
    pub replay_floor: Option<SourceBoundReplayFloor>,
    /// WAL receiver state for the expected primary.
    pub wal_receiver: WalReceiverState,
    /// Live `pg_stat_wal_receiver.slot_name`, if observed.
    pub wal_receiver_slot_name: Option<ReplicationSlotName>,
    /// Primary-side walsender PID serving the managed member, if observed.
    pub upstream_walsender_pid: Option<NonZeroU32>,
    /// Primary-side walsender `application_name`, if observed.
    pub upstream_walsender_application_name: Option<ReplicationSlotName>,
    /// Effective `primary_slot_name`, if configured.
    pub primary_slot_name: Option<ReplicationSlotName>,
    /// Primary-side physical-slot observation.
    pub upstream_physical_slot: Option<PhysicalSlotObservation>,
    /// Primary-side failover-slot synchronization policy for this standby.
    pub failover_slot_synchronization: FailoverSlotSynchronization,
    /// Failover anchor observed on the writable primary.
    pub upstream_failover_anchor: Option<LogicalSlotObservation>,
    /// Synchronized failover-anchor copy on this standby.
    pub synchronized_anchor: Option<LogicalSlotObservation>,
    /// Independent non-failover decoder slot created on this standby.
    pub local_decoder: Option<LogicalSlotObservation>,
}

/// Bounded freshness requirements for one multi-server preflight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StandbyDecoderEvidenceLimits {
    observation: Duration,
    feedback: Duration,
    slot_sync: Duration,
}

impl StandbyDecoderEvidenceLimits {
    /// Creates safe freshness bounds.
    ///
    /// # Errors
    ///
    /// Rejects a zero or overlong multi-server observation bound and a
    /// physical-feedback or slot-sync-success bound outside the supported range.
    pub fn new(
        maximum_observation_age: Duration,
        maximum_feedback_age: Duration,
        maximum_slot_sync_age: Duration,
    ) -> Result<Self, StandbyDecoderEvidenceLimitError> {
        if maximum_observation_age.is_zero() || maximum_observation_age > MAX_OBSERVATION_AGE_LIMIT
        {
            return Err(StandbyDecoderEvidenceLimitError::ObservationAge(
                maximum_observation_age,
            ));
        }
        if !(MIN_FEEDBACK_AGE_LIMIT..=MAX_FEEDBACK_AGE_LIMIT).contains(&maximum_feedback_age) {
            return Err(StandbyDecoderEvidenceLimitError::FeedbackAge(
                maximum_feedback_age,
            ));
        }
        if !(MIN_SLOT_SYNC_AGE_LIMIT..=MAX_SLOT_SYNC_AGE_LIMIT).contains(&maximum_slot_sync_age) {
            return Err(StandbyDecoderEvidenceLimitError::SlotSyncAge(
                maximum_slot_sync_age,
            ));
        }
        Ok(Self {
            observation: maximum_observation_age,
            feedback: maximum_feedback_age,
            slot_sync: maximum_slot_sync_age,
        })
    }

    /// Returns the maximum age of a complete multi-server observation set.
    #[must_use]
    pub const fn maximum_observation_age(self) -> Duration {
        self.observation
    }

    /// Returns the maximum accepted age of physical feedback evidence.
    #[must_use]
    pub const fn maximum_feedback_age(self) -> Duration {
        self.feedback
    }

    /// Returns the maximum accepted age of slot-sync success evidence.
    #[must_use]
    pub const fn maximum_slot_sync_age(self) -> Duration {
        self.slot_sync
    }
}

/// Unsafe evidence-age configuration.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum StandbyDecoderEvidenceLimitError {
    /// A multi-server observation set must expire within thirty seconds.
    #[error(
        "maximum observation age {0:?} must be positive and at most {MAX_OBSERVATION_AGE_LIMIT:?}"
    )]
    ObservationAge(Duration),
    /// Physical-slot feedback health must be bounded from two seconds to five minutes.
    #[error(
        "maximum feedback age {0:?} must be between {MIN_FEEDBACK_AGE_LIMIT:?} and {MAX_FEEDBACK_AGE_LIMIT:?}"
    )]
    FeedbackAge(Duration),
    /// Slot synchronization must prove a recent successful cycle.
    #[error(
        "maximum slot-sync age {0:?} must be between {MIN_SLOT_SYNC_AGE_LIMIT:?} and {MAX_SLOT_SYNC_AGE_LIMIT:?}"
    )]
    SlotSyncAge(Duration),
}

/// Catalog-bound activation boundaries for always-enabled two-phase decoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagedTwoPhasePolicy {
    /// Boundary shared by the primary anchor and its synchronized copy.
    pub failover_anchor_at: PgLsn,
    /// Boundary for the independent standby-local decoder.
    pub local_decoder_at: PgLsn,
}

impl ManagedTwoPhasePolicy {
    const fn expected_for(self, role: ManagedSlotRole) -> PgLsn {
        match role {
            ManagedSlotRole::PrimaryFailoverAnchor | ManagedSlotRole::SynchronizedAnchor => {
                self.failover_anchor_at
            }
            ManagedSlotRole::StandbyDecoder => self.local_decoder_at,
        }
    }
}

/// Exact name and catalog allocation generation for one managed logical slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedSlotTarget {
    name: ReplicationSlotName,
    generation: SlotGeneration,
}

impl ManagedSlotTarget {
    /// Binds an exact slot name to the full generation encoded in its suffix.
    ///
    /// # Errors
    ///
    /// Rejects a name that does not encode the supplied generation exactly.
    pub fn new(
        name: ReplicationSlotName,
        generation: SlotGeneration,
    ) -> Result<Self, ManagedSlotTargetError> {
        let suffix = generation.as_uuid().simple().to_string();
        if !name.as_str().ends_with(&suffix) {
            return Err(ManagedSlotTargetError);
        }
        Ok(Self { name, generation })
    }

    /// Returns the exact server-side slot name.
    #[must_use]
    pub fn name(&self) -> &ReplicationSlotName {
        &self.name
    }

    /// Returns the catalog-provided allocation generation.
    #[must_use]
    pub const fn generation(&self) -> SlotGeneration {
        self.generation
    }
}

/// A managed slot name that does not encode its full catalog generation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("managed logical slot name must end with its full generation UUID")]
pub struct ManagedSlotTargetError;

/// Exact catalog target that binds a member to its managed slot namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StandbyDecoderTarget {
    member_ordinal: u16,
    physical_slot: ReplicationSlotName,
    failover_anchor: ManagedSlotTarget,
    local_decoder: ManagedSlotTarget,
}

impl StandbyDecoderTarget {
    /// Creates a target with distinct physical, anchor, and decoder slot names.
    ///
    /// # Errors
    ///
    /// Rejects reuse within `PostgreSQL`'s cluster-wide slot namespace.
    pub fn new(
        member_ordinal: u16,
        physical_slot: ReplicationSlotName,
        failover_anchor: ManagedSlotTarget,
        local_decoder: ManagedSlotTarget,
    ) -> Result<Self, StandbyDecoderTargetError> {
        if physical_slot == failover_anchor.name
            || physical_slot == local_decoder.name
            || failover_anchor.name == local_decoder.name
        {
            return Err(StandbyDecoderTargetError::SlotNameCollision);
        }
        if failover_anchor.generation == local_decoder.generation {
            return Err(StandbyDecoderTargetError::SlotGenerationCollision);
        }
        Ok(Self {
            member_ordinal,
            physical_slot,
            failover_anchor,
            local_decoder,
        })
    }
}

/// Invalid catalog target for a standby decoder.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum StandbyDecoderTargetError {
    /// `PostgreSQL`'s cluster-wide replication-slot namespace was reused.
    #[error("physical, failover-anchor, and standby-decoder slot names must be distinct")]
    SlotNameCollision,
    /// One catalog generation was reused for two logical slots.
    #[error("failover-anchor and standby-decoder generations must be distinct")]
    SlotGenerationCollision,
}

/// Catalog-derived requirements for one standby decoder attachment attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StandbyDecoderPolicy {
    expected_source: ReplicationSourceIdentity,
    target: StandbyDecoderTarget,
    two_phase: ManagedTwoPhasePolicy,
    durable_checkpoint_lsn: PgLsn,
    evidence_limits: StandbyDecoderEvidenceLimits,
}

impl StandbyDecoderPolicy {
    /// Creates bounded attachment requirements for an already validated target.
    ///
    /// # Errors
    ///
    /// Rejects a zero checkpoint or unsafe prepared-decoding boundary.
    pub fn new(
        expected_source: ReplicationSourceIdentity,
        target: StandbyDecoderTarget,
        two_phase: ManagedTwoPhasePolicy,
        durable_checkpoint_lsn: PgLsn,
        evidence_limits: StandbyDecoderEvidenceLimits,
    ) -> Result<Self, StandbyDecoderPolicyError> {
        if durable_checkpoint_lsn.0 == 0 {
            return Err(StandbyDecoderPolicyError::ZeroCheckpoint);
        }
        if two_phase.failover_anchor_at.0 == 0
            || two_phase.local_decoder_at.0 == 0
            || lsn_follows_on_same_source(two_phase.failover_anchor_at, durable_checkpoint_lsn)
            || lsn_follows_on_same_source(two_phase.local_decoder_at, durable_checkpoint_lsn)
        {
            return Err(StandbyDecoderPolicyError::UnsafeTwoPhaseBoundary);
        }
        Ok(Self {
            expected_source,
            target,
            two_phase,
            durable_checkpoint_lsn,
            evidence_limits,
        })
    }

    /// Returns the exact source identity authorized by the catalog snapshot.
    #[must_use]
    pub const fn expected_source(&self) -> ReplicationSourceIdentity {
        self.expected_source
    }

    /// Returns the selected standby member ordinal.
    #[must_use]
    pub const fn member_ordinal(&self) -> u16 {
        self.target.member_ordinal
    }

    /// Returns the physical slot protecting this standby's WAL and catalog horizon.
    #[must_use]
    pub fn physical_slot(&self) -> &ReplicationSlotName {
        &self.target.physical_slot
    }

    /// Returns the cluster-scoped failover-anchor allocation.
    #[must_use]
    pub fn failover_anchor(&self) -> &ManagedSlotTarget {
        &self.target.failover_anchor
    }

    /// Returns the selected member's independent standby-local decoder allocation.
    #[must_use]
    pub fn local_decoder(&self) -> &ManagedSlotTarget {
        &self.target.local_decoder
    }

    /// Returns the catalog-bound prepared-transaction activation boundaries.
    #[must_use]
    pub const fn two_phase_policy(&self) -> ManagedTwoPhasePolicy {
        self.two_phase
    }

    /// Returns the durable checkpoint from which attachment must resume.
    #[must_use]
    pub const fn durable_checkpoint_lsn(&self) -> PgLsn {
        self.durable_checkpoint_lsn
    }

    /// Returns the catalog-selected evidence freshness limits.
    #[must_use]
    pub const fn evidence_limits(&self) -> StandbyDecoderEvidenceLimits {
        self.evidence_limits
    }
}

/// Invalid catalog-derived attachment policy.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum StandbyDecoderPolicyError {
    /// A zero checkpoint cannot prove a resume point.
    #[error("durable decoder checkpoint LSN must be nonzero")]
    ZeroCheckpoint,
    /// A prepared-decoding activation boundary is absent or follows the checkpoint.
    #[error(
        "two-phase activation boundaries must be nonzero and not follow the durable checkpoint"
    )]
    UnsafeTwoPhaseBoundary,
}

/// Managed logical-slot role being validated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedSlotRole {
    /// Primary-side failover anchor whose progress is synchronized downstream.
    PrimaryFailoverAnchor,
    /// Synchronized copy of the primary's failover anchor.
    SynchronizedAnchor,
    /// Independent non-failover slot used for hot-standby decoding.
    StandbyDecoder,
}

/// Why a managed logical slot cannot be used.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedSlotProblem {
    /// Expected slot is absent.
    Missing,
    /// Slot name does not match the catalog record.
    NameMismatch,
    /// Slot belongs to another database.
    DatabaseMismatch,
    /// Slot generation does not match the active catalog allocation.
    GenerationMismatch {
        /// Catalog-authorized generation.
        expected: SlotGeneration,
        /// Observed generation, or `None` for an unowned slot.
        observed: Option<SlotGeneration>,
    },
    /// Slot uses an output plugin other than `pgoutput`.
    WrongPlugin,
    /// Slot is not persistent.
    NotPersistent,
    /// A consumer-capable slot is already active during the pre-attachment check.
    Active,
    /// The reportedly acquired local decoder has no active backend.
    Inactive,
    /// Observed ownership differs from the caller-reported backend PID.
    ActiveBackendMismatch {
        /// Caller-reported replication backend PID.
        expected: NonZeroU32,
        /// Observed active backend PID.
        observed: NonZeroU32,
    },
    /// Slot has been invalidated by `PostgreSQL`.
    Invalidated(SlotInvalidation),
    /// Slot `failover` or `synced` flags do not match its role.
    WrongFlags,
    /// Slot's prepared-transaction decoding mode differs from policy.
    TwoPhaseMismatch,
    /// Current prepared-decoding activation boundary differs from the catalog.
    TwoPhaseBoundaryMismatch {
        /// Expected catalog boundary.
        expected: PgLsn,
        /// Boundary exposed by `PostgreSQL`.
        observed: Option<PgLsn>,
    },
    /// Slot progress has not reached its prepared-decoding activation boundary.
    TwoPhaseBoundaryAhead {
        /// Catalog-bound prepared-decoding activation boundary.
        two_phase_at: PgLsn,
        /// Slot confirmed-flush LSN.
        confirmed_flush_lsn: PgLsn,
    },
    /// Slot has no observable WAL-retention state.
    WalRetentionMissing,
    /// Required WAL is not guaranteed to remain retained.
    WalNotRetained(SlotWalRetention),
    /// Slot lacks a confirmed consistent point.
    ProgressMissing,
    /// Slot starts after the durable consumer checkpoint and would create a gap.
    ProgressAhead {
        /// Slot confirmed-flush LSN.
        confirmed_flush_lsn: PgLsn,
        /// Catalog checkpoint from which the consumer must resume.
        durable_checkpoint_lsn: PgLsn,
    },
}

/// Why the upstream physical slot cannot protect standby decoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalSlotProblem {
    /// Expected slot is absent on the primary.
    Missing,
    /// Observed name differs from `primary_slot_name` and the catalog.
    NameMismatch,
    /// No WAL receiver owns the upstream slot.
    Inactive,
    /// Slot has no observable WAL-retention state.
    WalRetentionMissing,
    /// Required WAL is not guaranteed to remain retained.
    WalNotRetained(SlotWalRetention),
    /// Slot has been invalidated by `PostgreSQL`.
    Invalidated(SlotInvalidation),
    /// Upstream catalog horizon does not cover the standby decoder.
    CatalogHorizonUnprotected,
    /// No feedback sample has been observed.
    FeedbackMissing,
    /// Feedback is older than policy permits.
    FeedbackStale {
        /// Observed feedback age.
        observed: Duration,
        /// Maximum accepted feedback age.
        maximum: Duration,
    },
}

/// A deterministic fail-closed standby attachment rejection.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StandbyDecoderIneligible {
    /// The oldest value in the multi-server observation set expired.
    #[error("standby decoder preflight observation is stale")]
    ObservationStale {
        /// Age of the oldest observed value.
        observed: Duration,
        /// Maximum accepted observation age.
        maximum: Duration,
    },
    /// The member is not currently a hot standby.
    #[error("candidate is not in PostgreSQL recovery")]
    NotInRecovery,
    /// The observation belongs to a different replica member.
    #[error("candidate member ordinal does not match the catalog")]
    MemberOrdinalMismatch {
        /// Catalog-authorized member ordinal.
        expected: u16,
        /// Observed member ordinal.
        observed: u16,
    },
    /// System, timeline, database, restore, or catalog identity changed.
    #[error("replication source identity does not match the catalog")]
    SourceIdentityMismatch,
    /// The writable upstream is not the exact catalog-authorized source.
    #[error("upstream replication source identity does not match the catalog")]
    UpstreamSourceIdentityMismatch,
    /// Candidate is cascading from another standby rather than the primary.
    #[error("candidate upstream is not the writable primary")]
    UpstreamNotWritable,
    /// Direct upstream cannot emit the WAL required for logical decoding.
    #[error("upstream wal_level is insufficient for logical decoding")]
    UpstreamWalLevelInsufficient,
    /// Standby feedback is disabled.
    #[error("hot_standby_feedback is disabled")]
    HotStandbyFeedbackDisabled,
    /// Periodic hot-standby feedback is disabled or slower than policy permits.
    #[error("wal_receiver_status_interval is zero or exceeds the margin-adjusted feedback bound")]
    FeedbackReportingIntervalUnsafe {
        /// Effective reporting interval.
        observed: Duration,
        /// Maximum accepted reporting interval.
        maximum: Duration,
    },
    /// `PostgreSQL` synchronized-slot worker is disabled.
    #[error("sync_replication_slots is disabled")]
    SlotSynchronizationDisabled,
    /// No live `PostgreSQL` slot-sync worker connection was observed.
    #[error("slot-sync worker is not connected to the direct primary")]
    SlotSynchronizationConnectionMissing,
    /// Slot-sync worker connection generation is nil.
    #[error("slot-sync worker connection generation is invalid")]
    SlotSynchronizationConnectionGenerationInvalid,
    /// Slot-sync worker connected to a different primary server generation.
    #[error("slot-sync worker source does not match the direct primary")]
    SlotSynchronizationSourceMismatch,
    /// No successful cycle was observed on the current worker connection.
    #[error("slot-sync worker has no successful cycle on its current connection")]
    SlotSynchronizationSuccessMissing,
    /// Successful cycle belongs to another worker connection generation.
    #[error("slot-sync success belongs to another worker connection")]
    SlotSynchronizationSuccessConnectionMismatch,
    /// The last synchronized anchor observation is too old.
    #[error("slot-sync worker has no recent synchronized anchor observation")]
    SlotSynchronizationStale {
        /// Age of the last successful cycle.
        observed: Duration,
        /// Maximum accepted success age.
        maximum: Duration,
    },
    /// Effective WAL level cannot support logical decoding.
    #[error("wal_level is insufficient for logical decoding")]
    WalLevelInsufficient,
    /// Candidate replay floor is unavailable.
    #[error("standby replay floor is unavailable")]
    ReplayFloorMissing,
    /// Replay floor was observed on another source lineage.
    #[error("standby replay floor source does not match the candidate source")]
    ReplayFloorSourceIdentityMismatch,
    /// Candidate's replay floor does not cover the durable checkpoint.
    #[error("standby replay floor is behind the durable checkpoint")]
    ReplayFloorBehind {
        /// Conservative replay floor.
        observed: PgLsn,
        /// Durable consumer checkpoint.
        required: PgLsn,
    },
    /// WAL receiver is not streaming.
    #[error("standby WAL receiver is not streaming")]
    WalReceiverNotStreaming,
    /// Live WAL receiver slot differs from the managed physical slot.
    #[error("pg_stat_wal_receiver.slot_name does not match the managed physical slot")]
    WalReceiverSlotNameMismatch,
    /// Primary-side walsender row is unavailable.
    #[error("upstream walsender PID is unavailable")]
    WalSenderMissing,
    /// Primary-side walsender application name cannot be tied to the member.
    #[error("upstream walsender application_name does not match the managed physical slot")]
    WalSenderApplicationNameMismatch,
    /// Primary walsender and physical slot refer to different backends.
    #[error("upstream walsender PID does not own the managed physical slot")]
    WalSenderPhysicalSlotMismatch,
    /// Standby `primary_slot_name` differs from the catalog.
    #[error("primary_slot_name does not match the managed physical slot")]
    PrimarySlotNameMismatch,
    /// The primary does not gate failover-slot progress on this standby.
    #[error("physical slot is absent from synchronized_standby_slots")]
    FailoverSlotNotSynchronized,
    /// Primary-side physical-slot proof failed.
    #[error("upstream physical slot is ineligible: {0:?}")]
    PhysicalSlot(PhysicalSlotProblem),
    /// A synchronized copy cannot safely be ahead of its writable source.
    #[error("synchronized anchor progress is ahead of the primary anchor")]
    SynchronizedAnchorAhead {
        /// Standby synchronized-copy progress.
        synchronized: PgLsn,
        /// Writable-primary anchor progress.
        primary: PgLsn,
    },
    /// Managed logical-slot proof failed.
    #[error("{role:?} slot is ineligible: {problem:?}")]
    ManagedSlot {
        /// Slot role under validation.
        role: ManagedSlotRole,
        /// Exact failed invariant.
        problem: ManagedSlotProblem,
    },
    /// Reported `START_REPLICATION` LSN differs from the durable checkpoint.
    #[error("reported replication start LSN does not match the durable checkpoint")]
    ReportedStartMismatch {
        /// Caller-reported replication start.
        observed: PgLsn,
        /// Durable consumer checkpoint.
        expected: PgLsn,
    },
}

/// Non-authorizing proof that a standby passed one pre-attachment observation.
///
/// This value can become stale immediately. It must be consumed by
/// [`validate_quarantined_standby_decoder_attachment`] after the caller reports
/// that `START_REPLICATION` acquired the slot. Neither value proves what a
/// socket actually sent or which backend owns it.
#[derive(Debug, Eq, PartialEq)]
pub struct StandbyDecoderPreflight {
    policy: StandbyDecoderPolicy,
}

impl StandbyDecoderPreflight {
    /// Returns the validated standby member ordinal.
    #[must_use]
    pub const fn member_ordinal(&self) -> u16 {
        self.policy.target.member_ordinal
    }

    /// Returns the exact identity validated for attachment.
    #[must_use]
    pub const fn source_identity(&self) -> ReplicationSourceIdentity {
        self.policy.expected_source
    }

    /// Returns the upstream physical slot protecting feedback and WAL.
    #[must_use]
    pub fn physical_slot(&self) -> &ReplicationSlotName {
        &self.policy.target.physical_slot
    }

    /// Returns the primary and synchronized promotion anchor that was validated but not consumed.
    #[must_use]
    pub fn failover_anchor(&self) -> &ReplicationSlotName {
        &self.policy.target.failover_anchor.name
    }

    /// Returns the independent standby-local slot that may be attached.
    #[must_use]
    pub fn local_decoder(&self) -> &ReplicationSlotName {
        &self.policy.target.local_decoder.name
    }

    /// Returns the durable checkpoint against which slot progress was checked.
    #[must_use]
    pub const fn durable_checkpoint_lsn(&self) -> PgLsn {
        self.policy.durable_checkpoint_lsn
    }
}

/// Non-authorizing report from a claimed post-acquisition observation.
///
/// This report records the claimed backend PID, exact source, catalog epoch,
/// exact catalog-provided slot generations, and durable checkpoint that passed
/// the pure checks. It is not a session capability: a future connection-owning
/// runtime must bind the actual `BackendKeyData` PID and encoded
/// `START_REPLICATION` command to one linear session state before releasing
/// quarantined bytes or sending feedback.
#[derive(Debug, Eq, PartialEq)]
pub struct StandbyDecoderPostAcquisitionReport {
    policy: StandbyDecoderPolicy,
    reported_backend_pid: NonZeroU32,
}

impl StandbyDecoderPostAcquisitionReport {
    /// Returns the backend PID supplied to the pure check.
    #[must_use]
    pub const fn reported_backend_pid(&self) -> NonZeroU32 {
        self.reported_backend_pid
    }

    /// Returns the exact source identity that passed the check.
    #[must_use]
    pub const fn source_identity(&self) -> ReplicationSourceIdentity {
        self.policy.expected_source
    }

    /// Returns the standby-local slot generation that passed the check.
    #[must_use]
    pub const fn local_decoder_generation(&self) -> SlotGeneration {
        self.policy.target.local_decoder.generation
    }

    /// Returns the durable checkpoint used by the check.
    #[must_use]
    pub const fn durable_checkpoint_lsn(&self) -> PgLsn {
        self.policy.durable_checkpoint_lsn
    }
}

/// Validates one pre-attachment observation without changing any state.
///
/// The primary failover anchor and its synchronized copy must both be
/// persistent and safe for promotion. The synchronized copy may be transiently
/// active in `PostgreSQL`'s slot-sync worker; the primary anchor and independent
/// standby-local decoder must be inactive. Only the standby-local slot is
/// identified for a future attachment attempt. Callers must re-observe both
/// source identities and catalog/fencing epochs at the actual `PostgreSQL`
/// attachment boundary.
///
/// # Errors
///
/// Returns the first deterministic failed invariant. No partial proof is
/// returned.
pub fn validate_standby_decoder_attachment(
    policy: &StandbyDecoderPolicy,
    observation: &StandbyDecoderObservation,
) -> Result<StandbyDecoderPreflight, StandbyDecoderIneligible> {
    validate_standby_observation(policy, observation, LocalDecoderExpectation::Inactive)?;
    Ok(StandbyDecoderPreflight {
        policy: policy.clone(),
    })
}

/// Validates a claimed post-acquisition observation without authorizing a session.
///
/// The caller reports the PID and requested LSN it associates with a
/// quarantined `START_REPLICATION`, plus a new bounded observation after the
/// local slot became active. This function consumes the stale preflight, fully
/// rechecks every invariant, and requires `active_pid` to equal the reported
/// PID. It cannot prove that the report matches bytes sent on a socket. The
/// future stream runtime must add that connection-owned proof before treating
/// the result as authorization.
///
/// # Errors
///
/// Returns the first failed fresh invariant, reported PID, or reported-start
/// check. No report is returned on failure.
pub fn validate_quarantined_standby_decoder_attachment(
    preflight: StandbyDecoderPreflight,
    observation: &StandbyDecoderObservation,
    reported_backend_pid: NonZeroU32,
    reported_start_lsn: PgLsn,
) -> Result<StandbyDecoderPostAcquisitionReport, StandbyDecoderIneligible> {
    let policy = &preflight.policy;
    if reported_start_lsn != policy.durable_checkpoint_lsn {
        return Err(StandbyDecoderIneligible::ReportedStartMismatch {
            observed: reported_start_lsn,
            expected: policy.durable_checkpoint_lsn,
        });
    }
    validate_standby_observation(
        policy,
        observation,
        LocalDecoderExpectation::ReportedActive(reported_backend_pid),
    )?;
    Ok(StandbyDecoderPostAcquisitionReport {
        policy: preflight.policy,
        reported_backend_pid,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalDecoderExpectation {
    Inactive,
    ReportedActive(NonZeroU32),
}

fn validate_standby_observation(
    policy: &StandbyDecoderPolicy,
    observation: &StandbyDecoderObservation,
    local_decoder: LocalDecoderExpectation,
) -> Result<(), StandbyDecoderIneligible> {
    validate_standby_identity_and_configuration(policy, observation)?;
    if observation.wal_level != LogicalWalLevel::Logical {
        return Err(StandbyDecoderIneligible::WalLevelInsufficient);
    }
    let replay_floor = observation
        .replay_floor
        .ok_or(StandbyDecoderIneligible::ReplayFloorMissing)?;
    if replay_floor.source_identity() != observation.source_identity {
        return Err(StandbyDecoderIneligible::ReplayFloorSourceIdentityMismatch);
    }
    let replay_floor_lsn = replay_floor.lsn();
    if lsn_follows_on_same_source(policy.durable_checkpoint_lsn, replay_floor_lsn) {
        return Err(StandbyDecoderIneligible::ReplayFloorBehind {
            observed: replay_floor_lsn,
            required: policy.durable_checkpoint_lsn,
        });
    }
    if observation.wal_receiver != WalReceiverState::Streaming {
        return Err(StandbyDecoderIneligible::WalReceiverNotStreaming);
    }
    if observation.wal_receiver_slot_name.as_ref() != Some(&policy.target.physical_slot) {
        return Err(StandbyDecoderIneligible::WalReceiverSlotNameMismatch);
    }
    let upstream_walsender_pid = observation
        .upstream_walsender_pid
        .ok_or(StandbyDecoderIneligible::WalSenderMissing)?;
    if observation.upstream_walsender_application_name.as_ref()
        != Some(&policy.target.physical_slot)
    {
        return Err(StandbyDecoderIneligible::WalSenderApplicationNameMismatch);
    }
    if observation.primary_slot_name.as_ref() != Some(&policy.target.physical_slot) {
        return Err(StandbyDecoderIneligible::PrimarySlotNameMismatch);
    }
    if observation.failover_slot_synchronization != FailoverSlotSynchronization::GatedOnPhysicalSlot
    {
        return Err(StandbyDecoderIneligible::FailoverSlotNotSynchronized);
    }
    let physical_slot_pid =
        validate_physical_slot(policy, observation.upstream_physical_slot.as_ref())?;
    if upstream_walsender_pid != physical_slot_pid {
        return Err(StandbyDecoderIneligible::WalSenderPhysicalSlotMismatch);
    }
    let primary_anchor_lsn = validate_logical_slot(
        ManagedSlotRole::PrimaryFailoverAnchor,
        policy,
        observation.upstream_failover_anchor.as_ref(),
        local_decoder,
    )?;
    let synchronized_anchor_lsn = validate_logical_slot(
        ManagedSlotRole::SynchronizedAnchor,
        policy,
        observation.synchronized_anchor.as_ref(),
        local_decoder,
    )?;
    if lsn_follows_on_same_source(synchronized_anchor_lsn, primary_anchor_lsn) {
        return Err(StandbyDecoderIneligible::SynchronizedAnchorAhead {
            synchronized: synchronized_anchor_lsn,
            primary: primary_anchor_lsn,
        });
    }
    validate_logical_slot(
        ManagedSlotRole::StandbyDecoder,
        policy,
        observation.local_decoder.as_ref(),
        local_decoder,
    )?;
    Ok(())
}

fn validate_standby_identity_and_configuration(
    policy: &StandbyDecoderPolicy,
    observation: &StandbyDecoderObservation,
) -> Result<(), StandbyDecoderIneligible> {
    if observation.oldest_observation_age > policy.evidence_limits.observation {
        return Err(StandbyDecoderIneligible::ObservationStale {
            observed: observation.oldest_observation_age,
            maximum: policy.evidence_limits.observation,
        });
    }
    if observation.recovery != RecoveryState::Standby {
        return Err(StandbyDecoderIneligible::NotInRecovery);
    }
    if observation.member_ordinal != policy.target.member_ordinal {
        return Err(StandbyDecoderIneligible::MemberOrdinalMismatch {
            expected: policy.target.member_ordinal,
            observed: observation.member_ordinal,
        });
    }
    if observation.source_identity != policy.expected_source {
        return Err(StandbyDecoderIneligible::SourceIdentityMismatch);
    }
    if observation.upstream_recovery != RecoveryState::Writable {
        return Err(StandbyDecoderIneligible::UpstreamNotWritable);
    }
    if observation.upstream_source_identity != policy.expected_source {
        return Err(StandbyDecoderIneligible::UpstreamSourceIdentityMismatch);
    }
    if observation.upstream_wal_level != LogicalWalLevel::Logical {
        return Err(StandbyDecoderIneligible::UpstreamWalLevelInsufficient);
    }
    if observation.hot_standby_feedback != SettingState::Enabled {
        return Err(StandbyDecoderIneligible::HotStandbyFeedbackDisabled);
    }
    let maximum_reporting_interval = policy
        .evidence_limits
        .feedback
        .saturating_sub(MIN_FEEDBACK_REPORTING_MARGIN);
    if observation.wal_receiver_status_interval.is_zero()
        || observation.wal_receiver_status_interval > maximum_reporting_interval
    {
        return Err(StandbyDecoderIneligible::FeedbackReportingIntervalUnsafe {
            observed: observation.wal_receiver_status_interval,
            maximum: maximum_reporting_interval,
        });
    }
    if observation.sync_replication_slots != SettingState::Enabled {
        return Err(StandbyDecoderIneligible::SlotSynchronizationDisabled);
    }
    let slot_sync_worker = observation
        .slot_sync_worker
        .ok_or(StandbyDecoderIneligible::SlotSynchronizationConnectionMissing)?;
    if slot_sync_worker.connection_generation.is_nil() {
        return Err(StandbyDecoderIneligible::SlotSynchronizationConnectionGenerationInvalid);
    }
    if !slot_sync_worker
        .upstream_source_identity
        .same_server_as(policy.expected_source)
    {
        return Err(StandbyDecoderIneligible::SlotSynchronizationSourceMismatch);
    }
    let last_success = slot_sync_worker
        .last_success
        .ok_or(StandbyDecoderIneligible::SlotSynchronizationSuccessMissing)?;
    if last_success.connection_generation != slot_sync_worker.connection_generation {
        return Err(StandbyDecoderIneligible::SlotSynchronizationSuccessConnectionMismatch);
    }
    if last_success.age > policy.evidence_limits.slot_sync {
        return Err(StandbyDecoderIneligible::SlotSynchronizationStale {
            observed: last_success.age,
            maximum: policy.evidence_limits.slot_sync,
        });
    }
    Ok(())
}

fn validate_physical_slot(
    policy: &StandbyDecoderPolicy,
    slot: Option<&PhysicalSlotObservation>,
) -> Result<NonZeroU32, StandbyDecoderIneligible> {
    let slot = slot.ok_or(StandbyDecoderIneligible::PhysicalSlot(
        PhysicalSlotProblem::Missing,
    ))?;
    if slot.name != policy.target.physical_slot {
        return Err(StandbyDecoderIneligible::PhysicalSlot(
            PhysicalSlotProblem::NameMismatch,
        ));
    }
    let active_pid = slot
        .active_pid
        .ok_or(StandbyDecoderIneligible::PhysicalSlot(
            PhysicalSlotProblem::Inactive,
        ))?;
    if let Some(invalidation) = slot.invalidation {
        return Err(StandbyDecoderIneligible::PhysicalSlot(
            PhysicalSlotProblem::Invalidated(invalidation),
        ));
    }
    let wal_retention = slot
        .wal_retention
        .ok_or(StandbyDecoderIneligible::PhysicalSlot(
            PhysicalSlotProblem::WalRetentionMissing,
        ))?;
    if !wal_retention.is_retained() {
        return Err(StandbyDecoderIneligible::PhysicalSlot(
            PhysicalSlotProblem::WalNotRetained(wal_retention),
        ));
    }
    if !slot.protects_catalog_horizon {
        return Err(StandbyDecoderIneligible::PhysicalSlot(
            PhysicalSlotProblem::CatalogHorizonUnprotected,
        ));
    }
    let age = slot
        .feedback_age
        .ok_or(StandbyDecoderIneligible::PhysicalSlot(
            PhysicalSlotProblem::FeedbackMissing,
        ))?;
    if age > policy.evidence_limits.feedback {
        return Err(StandbyDecoderIneligible::PhysicalSlot(
            PhysicalSlotProblem::FeedbackStale {
                observed: age,
                maximum: policy.evidence_limits.feedback,
            },
        ));
    }
    Ok(active_pid)
}

fn validate_logical_slot(
    role: ManagedSlotRole,
    policy: &StandbyDecoderPolicy,
    slot: Option<&LogicalSlotObservation>,
    local_decoder: LocalDecoderExpectation,
) -> Result<PgLsn, StandbyDecoderIneligible> {
    let reject = |problem| StandbyDecoderIneligible::ManagedSlot { role, problem };
    let slot = slot.ok_or_else(|| reject(ManagedSlotProblem::Missing))?;
    let expected_target = match role {
        ManagedSlotRole::PrimaryFailoverAnchor | ManagedSlotRole::SynchronizedAnchor => {
            &policy.target.failover_anchor
        }
        ManagedSlotRole::StandbyDecoder => &policy.target.local_decoder,
    };
    if slot.name != expected_target.name {
        return Err(reject(ManagedSlotProblem::NameMismatch));
    }
    if slot.database_oid != policy.expected_source.database_oid() {
        return Err(reject(ManagedSlotProblem::DatabaseMismatch));
    }
    let observed_generation = match slot.ownership {
        SlotOwnership::Managed(generation) => Some(generation),
        SlotOwnership::Unknown => None,
    };
    if observed_generation != Some(expected_target.generation) {
        return Err(reject(ManagedSlotProblem::GenerationMismatch {
            expected: expected_target.generation,
            observed: observed_generation,
        }));
    }
    if slot.plugin != LogicalSlotPlugin::PgOutput {
        return Err(reject(ManagedSlotProblem::WrongPlugin));
    }
    if slot.persistence != SlotPersistence::Persistent {
        return Err(reject(ManagedSlotProblem::NotPersistent));
    }
    let expected_two_phase_at = policy.two_phase.expected_for(role);
    if slot.two_phase != SettingState::Enabled {
        return Err(reject(ManagedSlotProblem::TwoPhaseMismatch));
    }
    if slot.two_phase_at != Some(expected_two_phase_at) {
        return Err(reject(ManagedSlotProblem::TwoPhaseBoundaryMismatch {
            expected: expected_two_phase_at,
            observed: slot.two_phase_at,
        }));
    }
    validate_slot_activity(role, local_decoder, slot.activity).map_err(reject)?;
    if let Some(invalidation) = slot.invalidation {
        return Err(reject(ManagedSlotProblem::Invalidated(invalidation)));
    }
    let flags_match = match role {
        ManagedSlotRole::PrimaryFailoverAnchor => matches!(
            slot.kind,
            LogicalSlotKind::FailoverAnchor | LogicalSlotKind::SynchronizedFailoverAnchor
        ),
        ManagedSlotRole::SynchronizedAnchor => {
            slot.kind == LogicalSlotKind::SynchronizedFailoverAnchor
        }
        ManagedSlotRole::StandbyDecoder => slot.kind == LogicalSlotKind::StandbyLocalDecoder,
    };
    if !flags_match {
        return Err(reject(ManagedSlotProblem::WrongFlags));
    }
    let wal_retention = slot
        .wal_retention
        .ok_or_else(|| reject(ManagedSlotProblem::WalRetentionMissing))?;
    if !wal_retention.is_retained() {
        return Err(reject(ManagedSlotProblem::WalNotRetained(wal_retention)));
    }
    let confirmed_flush_lsn = slot
        .confirmed_flush_lsn
        .filter(|lsn| lsn.0 != 0)
        .ok_or_else(|| reject(ManagedSlotProblem::ProgressMissing))?;
    if lsn_follows_on_same_source(expected_two_phase_at, confirmed_flush_lsn) {
        return Err(reject(ManagedSlotProblem::TwoPhaseBoundaryAhead {
            two_phase_at: expected_two_phase_at,
            confirmed_flush_lsn,
        }));
    }
    if lsn_follows_on_same_source(confirmed_flush_lsn, policy.durable_checkpoint_lsn) {
        return Err(reject(ManagedSlotProblem::ProgressAhead {
            confirmed_flush_lsn,
            durable_checkpoint_lsn: policy.durable_checkpoint_lsn,
        }));
    }
    Ok(confirmed_flush_lsn)
}

fn validate_slot_activity(
    role: ManagedSlotRole,
    local_decoder: LocalDecoderExpectation,
    activity: SlotActivity,
) -> Result<(), ManagedSlotProblem> {
    match role {
        ManagedSlotRole::SynchronizedAnchor => Ok(()),
        ManagedSlotRole::PrimaryFailoverAnchor => match activity {
            SlotActivity::Inactive => Ok(()),
            SlotActivity::Active(_) => Err(ManagedSlotProblem::Active),
        },
        ManagedSlotRole::StandbyDecoder => match (local_decoder, activity) {
            (LocalDecoderExpectation::Inactive, SlotActivity::Active(_)) => {
                Err(ManagedSlotProblem::Active)
            }
            (LocalDecoderExpectation::ReportedActive(_), SlotActivity::Inactive) => {
                Err(ManagedSlotProblem::Inactive)
            }
            (LocalDecoderExpectation::ReportedActive(expected), SlotActivity::Active(observed))
                if expected != observed =>
            {
                Err(ManagedSlotProblem::ActiveBackendMismatch { expected, observed })
            }
            (LocalDecoderExpectation::Inactive, SlotActivity::Inactive)
            | (LocalDecoderExpectation::ReportedActive(_), SlotActivity::Active(_)) => Ok(()),
        },
    }
}

const fn lsn_follows_on_same_source(left: PgLsn, right: PgLsn) -> bool {
    left.0 > right.0
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHECKPOINT: PgLsn = PgLsn(0x20_0000_0040);
    const BEFORE_CHECKPOINT: PgLsn = PgLsn(CHECKPOINT.0 - 1);
    const TWO_BEFORE_CHECKPOINT: PgLsn = PgLsn(CHECKPOINT.0 - 2);
    const THREE_BEFORE_CHECKPOINT: PgLsn = PgLsn(CHECKPOINT.0 - 3);
    const AFTER_CHECKPOINT: PgLsn = PgLsn(CHECKPOINT.0 + 1);

    fn slot(name: &str) -> ReplicationSlotName {
        ReplicationSlotName::new(name).expect("valid test slot name")
    }

    fn source(timeline: u32) -> ReplicationSourceIdentity {
        source_for_database(timeline, 16_384)
    }

    fn source_for_database(timeline: u32, database_oid: u32) -> ReplicationSourceIdentity {
        ReplicationSourceIdentity::new(
            7_219_834_723_984_723,
            timeline,
            database_oid,
            Uuid::from_u128(0x1234),
            CatalogEpoch(42),
        )
        .expect("valid test source")
    }

    fn replay_floor(
        source_identity: ReplicationSourceIdentity,
        lsn: PgLsn,
    ) -> SourceBoundReplayFloor {
        SourceBoundReplayFloor::for_test(source_identity, lsn)
    }

    fn generation(value: u128) -> SlotGeneration {
        SlotGeneration::new(Uuid::from_u128(value)).expect("valid test slot generation")
    }

    fn managed_target(prefix: &str, value: u128) -> ManagedSlotTarget {
        let generation = generation(value);
        let name = slot(&format!("{prefix}_{}", generation.as_uuid().simple()));
        ManagedSlotTarget::new(name, generation).expect("generation-encoded test slot")
    }

    fn anchor_target() -> ManagedSlotTarget {
        managed_target("pgshard_anchor", 0xa1)
    }

    fn local_target() -> ManagedSlotTarget {
        managed_target("pgshard_local", 0xb1)
    }

    fn pid(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("nonzero test PID")
    }

    fn evidence_limits() -> StandbyDecoderEvidenceLimits {
        StandbyDecoderEvidenceLimits::new(
            Duration::from_secs(2),
            Duration::from_secs(3),
            Duration::from_secs(3),
        )
        .expect("valid evidence limits")
    }

    fn policy() -> StandbyDecoderPolicy {
        policy_with_two_phase(ManagedTwoPhasePolicy {
            failover_anchor_at: TWO_BEFORE_CHECKPOINT,
            local_decoder_at: TWO_BEFORE_CHECKPOINT,
        })
    }

    fn policy_with_two_phase(two_phase: ManagedTwoPhasePolicy) -> StandbyDecoderPolicy {
        StandbyDecoderPolicy::new(
            source(7),
            StandbyDecoderTarget::new(
                1,
                slot("pgshard_member_0001"),
                anchor_target(),
                local_target(),
            )
            .expect("valid test target"),
            two_phase,
            CHECKPOINT,
            evidence_limits(),
        )
        .expect("valid test policy")
    }

    fn logical_slot(
        name: ReplicationSlotName,
        failover: bool,
        synchronized: bool,
    ) -> LogicalSlotObservation {
        LogicalSlotObservation {
            name,
            database_oid: 16_384,
            plugin: LogicalSlotPlugin::PgOutput,
            kind: match (failover, synchronized) {
                (true, false) => LogicalSlotKind::FailoverAnchor,
                (true, true) => LogicalSlotKind::SynchronizedFailoverAnchor,
                (false, false) => LogicalSlotKind::StandbyLocalDecoder,
                _ => LogicalSlotKind::Other,
            },
            persistence: SlotPersistence::Persistent,
            two_phase: SettingState::Enabled,
            two_phase_at: Some(TWO_BEFORE_CHECKPOINT),
            activity: SlotActivity::Inactive,
            ownership: SlotOwnership::Managed(if failover {
                generation(0xa1)
            } else {
                generation(0xb1)
            }),
            invalidation: None,
            wal_retention: Some(SlotWalRetention::Reserved),
            confirmed_flush_lsn: Some(BEFORE_CHECKPOINT),
        }
    }

    fn observation() -> StandbyDecoderObservation {
        StandbyDecoderObservation {
            oldest_observation_age: Duration::from_millis(10),
            member_ordinal: 1,
            recovery: RecoveryState::Standby,
            source_identity: source(7),
            upstream_recovery: RecoveryState::Writable,
            upstream_source_identity: source(7),
            upstream_wal_level: LogicalWalLevel::Logical,
            hot_standby_feedback: SettingState::Enabled,
            wal_receiver_status_interval: Duration::from_secs(1),
            sync_replication_slots: SettingState::Enabled,
            slot_sync_worker: Some(SlotSyncWorkerObservation {
                connection_generation: Uuid::from_u128(0xc1),
                upstream_source_identity: source_for_database(7, 5),
                last_success: Some(SlotSyncSuccessObservation {
                    connection_generation: Uuid::from_u128(0xc1),
                    age: Duration::from_secs(1),
                }),
            }),
            wal_level: LogicalWalLevel::Logical,
            replay_floor: Some(replay_floor(source(7), CHECKPOINT)),
            wal_receiver: WalReceiverState::Streaming,
            wal_receiver_slot_name: Some(slot("pgshard_member_0001")),
            upstream_walsender_pid: Some(pid(701)),
            upstream_walsender_application_name: Some(slot("pgshard_member_0001")),
            primary_slot_name: Some(slot("pgshard_member_0001")),
            upstream_physical_slot: Some(PhysicalSlotObservation {
                name: slot("pgshard_member_0001"),
                active_pid: Some(pid(701)),
                wal_retention: Some(SlotWalRetention::Extended),
                invalidation: None,
                protects_catalog_horizon: true,
                feedback_age: Some(Duration::from_secs(1)),
            }),
            failover_slot_synchronization: FailoverSlotSynchronization::GatedOnPhysicalSlot,
            upstream_failover_anchor: Some(logical_slot(
                anchor_target().name().clone(),
                true,
                false,
            )),
            synchronized_anchor: Some(logical_slot(anchor_target().name().clone(), true, true)),
            local_decoder: Some(logical_slot(local_target().name().clone(), false, false)),
        }
    }

    fn managed_slot_mut(
        observation: &mut StandbyDecoderObservation,
        role: ManagedSlotRole,
    ) -> &mut LogicalSlotObservation {
        match role {
            ManagedSlotRole::PrimaryFailoverAnchor => observation
                .upstream_failover_anchor
                .as_mut()
                .expect("primary failover anchor"),
            ManagedSlotRole::SynchronizedAnchor => observation
                .synchronized_anchor
                .as_mut()
                .expect("synchronized anchor"),
            ManagedSlotRole::StandbyDecoder => observation
                .local_decoder
                .as_mut()
                .expect("standby-local decoder"),
        }
    }

    fn acquired_observation(backend_pid: NonZeroU32) -> StandbyDecoderObservation {
        let mut observed = observation();
        observed
            .local_decoder
            .as_mut()
            .expect("standby-local decoder")
            .activity = SlotActivity::Active(backend_pid);
        observed
    }

    fn post_check(
        preflight: StandbyDecoderPreflight,
        observed: &StandbyDecoderObservation,
        backend_pid: NonZeroU32,
        start_lsn: PgLsn,
    ) -> Result<StandbyDecoderPostAcquisitionReport, StandbyDecoderIneligible> {
        validate_quarantined_standby_decoder_attachment(preflight, observed, backend_pid, start_lsn)
    }

    #[test]
    fn accepts_distinct_anchor_and_standby_local_decoder() {
        let proof = validate_standby_decoder_attachment(&policy(), &observation())
            .expect("coherent standby should be eligible");
        assert_eq!(proof.member_ordinal(), 1);
        assert_eq!(proof.source_identity(), source(7));
        assert_eq!(proof.physical_slot().as_str(), "pgshard_member_0001");
        assert_eq!(proof.failover_anchor(), anchor_target().name());
        assert_eq!(proof.local_decoder(), local_target().name());
        assert_eq!(proof.durable_checkpoint_lsn(), CHECKPOINT);
    }

    #[test]
    fn reports_only_a_fresh_quarantined_attachment_with_the_claimed_backend() {
        let backend_pid = pid(900);
        let preflight = validate_standby_decoder_attachment(&policy(), &observation())
            .expect("coherent preflight");
        let report = post_check(
            preflight,
            &acquired_observation(backend_pid),
            backend_pid,
            CHECKPOINT,
        )
        .expect("fresh acquired slot should pass the pure check");

        assert_eq!(report.reported_backend_pid(), backend_pid);
        assert_eq!(report.source_identity(), source(7));
        assert_eq!(report.local_decoder_generation(), generation(0xb1));
        assert_eq!(report.durable_checkpoint_lsn(), CHECKPOINT);
    }

    #[test]
    fn post_acquisition_report_rechecks_start_progress_mode_source_and_backend() {
        let backend_pid = pid(900);
        let preflight = validate_standby_decoder_attachment(&policy(), &observation())
            .expect("coherent preflight");
        assert_eq!(
            post_check(
                preflight,
                &acquired_observation(backend_pid),
                backend_pid,
                AFTER_CHECKPOINT,
            ),
            Err(StandbyDecoderIneligible::ReportedStartMismatch {
                observed: AFTER_CHECKPOINT,
                expected: CHECKPOINT,
            })
        );

        let preflight = validate_standby_decoder_attachment(&policy(), &observation())
            .expect("coherent preflight");
        let mut acquired = acquired_observation(backend_pid);
        acquired
            .local_decoder
            .as_mut()
            .expect("local decoder")
            .confirmed_flush_lsn = Some(AFTER_CHECKPOINT);
        assert_eq!(
            post_check(preflight, &acquired, backend_pid, CHECKPOINT),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::ProgressAhead {
                    confirmed_flush_lsn: AFTER_CHECKPOINT,
                    durable_checkpoint_lsn: CHECKPOINT,
                },
            })
        );

        let preflight = validate_standby_decoder_attachment(&policy(), &observation())
            .expect("coherent preflight");
        let mut acquired = acquired_observation(backend_pid);
        acquired
            .local_decoder
            .as_mut()
            .expect("local decoder")
            .two_phase = SettingState::Disabled;
        assert_eq!(
            post_check(preflight, &acquired, backend_pid, CHECKPOINT),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::TwoPhaseMismatch,
            })
        );

        let preflight = validate_standby_decoder_attachment(&policy(), &observation())
            .expect("coherent preflight");
        let mut acquired = acquired_observation(backend_pid);
        acquired.source_identity = source(8);
        assert_eq!(
            post_check(preflight, &acquired, backend_pid, CHECKPOINT),
            Err(StandbyDecoderIneligible::SourceIdentityMismatch)
        );

        let preflight = validate_standby_decoder_attachment(&policy(), &observation())
            .expect("coherent preflight");
        assert_eq!(
            post_check(
                preflight,
                &acquired_observation(pid(901)),
                backend_pid,
                CHECKPOINT,
            ),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::ActiveBackendMismatch {
                    expected: backend_pid,
                    observed: pid(901),
                },
            })
        );

        let preflight = validate_standby_decoder_attachment(&policy(), &observation())
            .expect("coherent preflight");
        assert_eq!(
            post_check(preflight, &observation(), backend_pid, CHECKPOINT),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::Inactive,
            })
        );
    }

    #[test]
    fn accepts_feedback_reporting_interval_below_policy_bound() {
        let mut candidate = observation();
        candidate.wal_receiver_status_interval = Duration::from_secs(2);

        validate_standby_decoder_attachment(&policy(), &candidate)
            .expect("reporting interval with freshness margin should be eligible");
    }

    #[test]
    fn requires_fresh_standby_feedback_slot_sync_and_streaming() {
        type MutateObservation = fn(&mut StandbyDecoderObservation);
        let cases: [(MutateObservation, StandbyDecoderIneligible); 8] = [
            (
                |candidate: &mut StandbyDecoderObservation| {
                    candidate.oldest_observation_age = Duration::from_secs(3);
                },
                StandbyDecoderIneligible::ObservationStale {
                    observed: Duration::from_secs(3),
                    maximum: Duration::from_secs(2),
                },
            ),
            (
                |candidate: &mut StandbyDecoderObservation| {
                    candidate.recovery = RecoveryState::Writable;
                },
                StandbyDecoderIneligible::NotInRecovery,
            ),
            (
                |candidate: &mut StandbyDecoderObservation| {
                    candidate.hot_standby_feedback = SettingState::Disabled;
                },
                StandbyDecoderIneligible::HotStandbyFeedbackDisabled,
            ),
            (
                |candidate: &mut StandbyDecoderObservation| {
                    candidate.wal_receiver_status_interval = Duration::ZERO;
                },
                StandbyDecoderIneligible::FeedbackReportingIntervalUnsafe {
                    observed: Duration::ZERO,
                    maximum: Duration::from_secs(2),
                },
            ),
            (
                |candidate: &mut StandbyDecoderObservation| {
                    candidate.wal_receiver_status_interval = Duration::from_secs(3);
                },
                StandbyDecoderIneligible::FeedbackReportingIntervalUnsafe {
                    observed: Duration::from_secs(3),
                    maximum: Duration::from_secs(2),
                },
            ),
            (
                |candidate: &mut StandbyDecoderObservation| {
                    candidate.sync_replication_slots = SettingState::Disabled;
                },
                StandbyDecoderIneligible::SlotSynchronizationDisabled,
            ),
            (
                |candidate: &mut StandbyDecoderObservation| {
                    candidate.wal_receiver = WalReceiverState::NotStreaming;
                },
                StandbyDecoderIneligible::WalReceiverNotStreaming,
            ),
            (
                |candidate: &mut StandbyDecoderObservation| {
                    candidate.wal_level = LogicalWalLevel::Insufficient;
                },
                StandbyDecoderIneligible::WalLevelInsufficient,
            ),
        ];
        for (mutate, expected) in cases {
            let mut candidate = observation();
            mutate(&mut candidate);
            assert_eq!(
                validate_standby_decoder_attachment(&policy(), &candidate),
                Err(expected)
            );
        }
    }

    #[test]
    fn rejects_source_forks_and_receiver_identity_mismatch() {
        let mut candidate = observation();
        candidate.member_ordinal = 2;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::MemberOrdinalMismatch {
                expected: 1,
                observed: 2,
            })
        );

        let mut candidate = observation();
        candidate.source_identity = source(8);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::SourceIdentityMismatch)
        );

        let mut candidate = observation();
        candidate.upstream_source_identity = source(8);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::UpstreamSourceIdentityMismatch)
        );

        let mut candidate = observation();
        candidate.upstream_recovery = RecoveryState::Standby;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::UpstreamNotWritable)
        );

        let mut candidate = observation();
        candidate.upstream_wal_level = LogicalWalLevel::Insufficient;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::UpstreamWalLevelInsufficient)
        );

        let mut candidate = observation();
        candidate.wal_receiver_slot_name = Some(slot("wrong_member"));
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::WalReceiverSlotNameMismatch)
        );

        let mut candidate = observation();
        candidate.upstream_walsender_pid = None;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::WalSenderMissing)
        );

        let mut candidate = observation();
        candidate.upstream_walsender_application_name = Some(slot("wrong_member"));
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::WalSenderApplicationNameMismatch)
        );

        let mut candidate = observation();
        candidate
            .upstream_physical_slot
            .as_mut()
            .expect("physical slot")
            .active_pid = Some(pid(702));
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::WalSenderPhysicalSlotMismatch)
        );

        let mut candidate = observation();
        candidate.primary_slot_name = Some(slot("wrong_member"));
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::PrimarySlotNameMismatch)
        );
    }

    #[test]
    fn requires_live_source_bound_slot_sync_connection_and_recent_cycle() {
        let mut candidate = observation();
        candidate.slot_sync_worker = None;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::SlotSynchronizationConnectionMissing)
        );

        let mut candidate = observation();
        candidate
            .slot_sync_worker
            .as_mut()
            .expect("slot-sync worker")
            .connection_generation = Uuid::nil();
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::SlotSynchronizationConnectionGenerationInvalid)
        );

        let mut candidate = observation();
        candidate
            .slot_sync_worker
            .as_mut()
            .expect("slot-sync worker")
            .upstream_source_identity = source_for_database(8, 5);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::SlotSynchronizationSourceMismatch)
        );

        let mut candidate = observation();
        candidate
            .slot_sync_worker
            .as_mut()
            .expect("slot-sync worker")
            .last_success = None;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::SlotSynchronizationSuccessMissing)
        );

        let mut candidate = observation();
        candidate
            .slot_sync_worker
            .as_mut()
            .expect("slot-sync worker")
            .last_success
            .as_mut()
            .expect("slot-sync success")
            .connection_generation = Uuid::from_u128(0xc2);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::SlotSynchronizationSuccessConnectionMismatch)
        );

        let mut candidate = observation();
        candidate
            .slot_sync_worker
            .as_mut()
            .expect("slot-sync worker")
            .last_success
            .as_mut()
            .expect("slot-sync success")
            .age = Duration::from_secs(4);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::SlotSynchronizationStale {
                observed: Duration::from_secs(4),
                maximum: Duration::from_secs(3),
            })
        );
    }

    #[test]
    fn requires_replay_through_checkpoint_and_primary_sync_gating() {
        let mut candidate = observation();
        candidate.replay_floor = None;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ReplayFloorMissing)
        );

        let mut candidate = observation();
        candidate.replay_floor = Some(replay_floor(source(8), CHECKPOINT));
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ReplayFloorSourceIdentityMismatch)
        );

        let mut candidate = observation();
        candidate.replay_floor = Some(replay_floor(source(7), BEFORE_CHECKPOINT));
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ReplayFloorBehind {
                observed: BEFORE_CHECKPOINT,
                required: CHECKPOINT,
            })
        );

        let mut candidate = observation();
        candidate.failover_slot_synchronization = FailoverSlotSynchronization::NotGated;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::FailoverSlotNotSynchronized)
        );
    }

    #[test]
    fn requires_recent_upstream_feedback_and_retained_wal() {
        let mut candidate = observation();
        candidate
            .upstream_physical_slot
            .as_mut()
            .expect("physical slot")
            .feedback_age = Some(Duration::from_secs(4));
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::PhysicalSlot(
                PhysicalSlotProblem::FeedbackStale {
                    observed: Duration::from_secs(4),
                    maximum: Duration::from_secs(3),
                }
            ))
        );

        let mut candidate = observation();
        candidate
            .upstream_physical_slot
            .as_mut()
            .expect("physical slot")
            .wal_retention = Some(SlotWalRetention::Lost);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::PhysicalSlot(
                PhysicalSlotProblem::WalNotRetained(SlotWalRetention::Lost)
            ))
        );

        let mut candidate = observation();
        candidate
            .upstream_physical_slot
            .as_mut()
            .expect("physical slot")
            .protects_catalog_horizon = false;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::PhysicalSlot(
                PhysicalSlotProblem::CatalogHorizonUnprotected
            ))
        );
    }

    #[test]
    fn rejects_incomplete_or_invalid_upstream_physical_slot_proof() {
        type MutateObservation = fn(&mut StandbyDecoderObservation);
        let cases: [(MutateObservation, PhysicalSlotProblem); 6] = [
            (
                |candidate| candidate.upstream_physical_slot = None,
                PhysicalSlotProblem::Missing,
            ),
            (
                |candidate| {
                    candidate
                        .upstream_physical_slot
                        .as_mut()
                        .expect("physical slot")
                        .name = slot("wrong_physical");
                },
                PhysicalSlotProblem::NameMismatch,
            ),
            (
                |candidate| {
                    candidate
                        .upstream_physical_slot
                        .as_mut()
                        .expect("physical slot")
                        .active_pid = None;
                },
                PhysicalSlotProblem::Inactive,
            ),
            (
                |candidate| {
                    candidate
                        .upstream_physical_slot
                        .as_mut()
                        .expect("physical slot")
                        .wal_retention = None;
                },
                PhysicalSlotProblem::WalRetentionMissing,
            ),
            (
                |candidate| {
                    candidate
                        .upstream_physical_slot
                        .as_mut()
                        .expect("physical slot")
                        .invalidation = Some(SlotInvalidation::IdleTimeout);
                },
                PhysicalSlotProblem::Invalidated(SlotInvalidation::IdleTimeout),
            ),
            (
                |candidate| {
                    candidate
                        .upstream_physical_slot
                        .as_mut()
                        .expect("physical slot")
                        .feedback_age = None;
                },
                PhysicalSlotProblem::FeedbackMissing,
            ),
        ];

        for (mutate, problem) in cases {
            let mut candidate = observation();
            mutate(&mut candidate);
            assert_eq!(
                validate_standby_decoder_attachment(&policy(), &candidate),
                Err(StandbyDecoderIneligible::PhysicalSlot(problem))
            );
        }
    }

    #[test]
    fn synchronized_anchor_is_never_accepted_as_local_decoder() {
        let mut candidate = observation();
        let local = candidate.local_decoder.as_mut().expect("local decoder");
        local.kind = LogicalSlotKind::SynchronizedFailoverAnchor;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::WrongFlags,
            })
        );
    }

    #[test]
    fn primary_and_synchronized_anchor_flags_are_role_aware() {
        let mut candidate = observation();
        candidate.upstream_failover_anchor = None;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::PrimaryFailoverAnchor,
                problem: ManagedSlotProblem::Missing,
            })
        );

        let mut candidate = observation();
        candidate
            .upstream_failover_anchor
            .as_mut()
            .expect("primary anchor")
            .kind = LogicalSlotKind::StandbyLocalDecoder;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::PrimaryFailoverAnchor,
                problem: ManagedSlotProblem::WrongFlags,
            })
        );

        let mut promoted_primary = observation();
        promoted_primary
            .upstream_failover_anchor
            .as_mut()
            .expect("primary anchor")
            .kind = LogicalSlotKind::SynchronizedFailoverAnchor;
        validate_standby_decoder_attachment(&policy(), &promoted_primary)
            .expect("synced has no rejecting meaning on a promoted primary");

        let mut candidate = observation();
        candidate
            .upstream_failover_anchor
            .as_mut()
            .expect("primary anchor")
            .confirmed_flush_lsn = Some(AFTER_CHECKPOINT);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::PrimaryFailoverAnchor,
                problem: ManagedSlotProblem::ProgressAhead {
                    confirmed_flush_lsn: AFTER_CHECKPOINT,
                    durable_checkpoint_lsn: CHECKPOINT,
                },
            })
        );

        let mut candidate = observation();
        candidate
            .upstream_failover_anchor
            .as_mut()
            .expect("primary anchor")
            .confirmed_flush_lsn = Some(TWO_BEFORE_CHECKPOINT);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::SynchronizedAnchorAhead {
                synchronized: BEFORE_CHECKPOINT,
                primary: TWO_BEFORE_CHECKPOINT,
            })
        );

        let mut candidate = observation();
        candidate.synchronized_anchor = None;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::SynchronizedAnchor,
                problem: ManagedSlotProblem::Missing,
            })
        );

        let mut candidate = observation();
        candidate
            .synchronized_anchor
            .as_mut()
            .expect("synchronized anchor")
            .kind = LogicalSlotKind::FailoverAnchor;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::SynchronizedAnchor,
                problem: ManagedSlotProblem::WrongFlags,
            })
        );
    }

    #[test]
    fn allows_sync_worker_ownership_but_rejects_active_consumer_slots() {
        let mut candidate = observation();
        candidate
            .synchronized_anchor
            .as_mut()
            .expect("anchor")
            .activity = SlotActivity::Active(pid(801));
        validate_standby_decoder_attachment(&policy(), &candidate)
            .expect("slot-sync worker ownership does not consume the anchor");

        let mut candidate = observation();
        candidate
            .upstream_failover_anchor
            .as_mut()
            .expect("primary anchor")
            .activity = SlotActivity::Active(pid(802));
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::PrimaryFailoverAnchor,
                problem: ManagedSlotProblem::Active,
            })
        );

        let mut candidate = observation();
        candidate
            .local_decoder
            .as_mut()
            .expect("local decoder")
            .activity = SlotActivity::Active(pid(803));
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::Active,
            })
        );
    }

    #[test]
    fn requires_exact_catalog_bound_current_two_phase_activation_for_every_slot() {
        for role in [
            ManagedSlotRole::PrimaryFailoverAnchor,
            ManagedSlotRole::SynchronizedAnchor,
            ManagedSlotRole::StandbyDecoder,
        ] {
            let mut candidate = observation();
            managed_slot_mut(&mut candidate, role).two_phase_at = None;
            assert_eq!(
                validate_standby_decoder_attachment(&policy(), &candidate),
                Err(StandbyDecoderIneligible::ManagedSlot {
                    role,
                    problem: ManagedSlotProblem::TwoPhaseBoundaryMismatch {
                        expected: TWO_BEFORE_CHECKPOINT,
                        observed: None,
                    },
                })
            );

            let mut candidate = observation();
            managed_slot_mut(&mut candidate, role).two_phase_at = Some(THREE_BEFORE_CHECKPOINT);
            assert_eq!(
                validate_standby_decoder_attachment(&policy(), &candidate),
                Err(StandbyDecoderIneligible::ManagedSlot {
                    role,
                    problem: ManagedSlotProblem::TwoPhaseBoundaryMismatch {
                        expected: TWO_BEFORE_CHECKPOINT,
                        observed: Some(THREE_BEFORE_CHECKPOINT),
                    },
                })
            );

            let mut candidate = observation();
            managed_slot_mut(&mut candidate, role).confirmed_flush_lsn =
                Some(THREE_BEFORE_CHECKPOINT);
            assert_eq!(
                validate_standby_decoder_attachment(&policy(), &candidate),
                Err(StandbyDecoderIneligible::ManagedSlot {
                    role,
                    problem: ManagedSlotProblem::TwoPhaseBoundaryAhead {
                        two_phase_at: TWO_BEFORE_CHECKPOINT,
                        confirmed_flush_lsn: THREE_BEFORE_CHECKPOINT,
                    },
                })
            );
        }
    }

    #[test]
    fn requires_always_enabled_two_phase_and_exact_slot_generation() {
        for role in [
            ManagedSlotRole::PrimaryFailoverAnchor,
            ManagedSlotRole::SynchronizedAnchor,
            ManagedSlotRole::StandbyDecoder,
        ] {
            let mut candidate = observation();
            managed_slot_mut(&mut candidate, role).two_phase = SettingState::Disabled;
            assert_eq!(
                validate_standby_decoder_attachment(&policy(), &candidate),
                Err(StandbyDecoderIneligible::ManagedSlot {
                    role,
                    problem: ManagedSlotProblem::TwoPhaseMismatch,
                })
            );

            let expected = if role == ManagedSlotRole::StandbyDecoder {
                generation(0xb1)
            } else {
                generation(0xa1)
            };
            let mut candidate = observation();
            managed_slot_mut(&mut candidate, role).ownership =
                SlotOwnership::Managed(generation(0xff));
            assert_eq!(
                validate_standby_decoder_attachment(&policy(), &candidate),
                Err(StandbyDecoderIneligible::ManagedSlot {
                    role,
                    problem: ManagedSlotProblem::GenerationMismatch {
                        expected,
                        observed: Some(generation(0xff)),
                    },
                })
            );
        }
    }

    #[test]
    fn rejects_unknown_invalid_unretained_or_gap_creating_slots() {
        let mut candidate = observation();
        candidate
            .local_decoder
            .as_mut()
            .expect("local decoder")
            .ownership = SlotOwnership::Unknown;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::GenerationMismatch {
                    expected: generation(0xb1),
                    observed: None,
                },
            })
        );

        let mut candidate = observation();
        candidate
            .local_decoder
            .as_mut()
            .expect("local decoder")
            .invalidation = Some(SlotInvalidation::RowsRemoved);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::Invalidated(SlotInvalidation::RowsRemoved),
            })
        );

        let mut candidate = observation();
        candidate
            .local_decoder
            .as_mut()
            .expect("local decoder")
            .wal_retention = Some(SlotWalRetention::Unreserved);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::WalNotRetained(SlotWalRetention::Unreserved),
            })
        );

        let mut candidate = observation();
        candidate
            .local_decoder
            .as_mut()
            .expect("local decoder")
            .two_phase = SettingState::Disabled;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::TwoPhaseMismatch,
            })
        );

        let mut candidate = observation();
        candidate
            .local_decoder
            .as_mut()
            .expect("local decoder")
            .confirmed_flush_lsn = Some(AFTER_CHECKPOINT);
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::ProgressAhead {
                    confirmed_flush_lsn: AFTER_CHECKPOINT,
                    durable_checkpoint_lsn: CHECKPOINT,
                },
            })
        );
    }

    #[test]
    fn rejects_incomplete_or_mismatched_local_slot_proof() {
        type MutateSlot = fn(&mut LogicalSlotObservation);

        let mut candidate = observation();
        candidate.local_decoder = None;
        assert_eq!(
            validate_standby_decoder_attachment(&policy(), &candidate),
            Err(StandbyDecoderIneligible::ManagedSlot {
                role: ManagedSlotRole::StandbyDecoder,
                problem: ManagedSlotProblem::Missing,
            })
        );

        let cases: [(MutateSlot, ManagedSlotProblem); 7] = [
            (
                |slot| slot.name = self::slot("wrong_local"),
                ManagedSlotProblem::NameMismatch,
            ),
            (
                |slot| slot.database_oid += 1,
                ManagedSlotProblem::DatabaseMismatch,
            ),
            (
                |slot| slot.plugin = LogicalSlotPlugin::Other,
                ManagedSlotProblem::WrongPlugin,
            ),
            (
                |slot| slot.persistence = SlotPersistence::NonPersistent,
                ManagedSlotProblem::NotPersistent,
            ),
            (
                |slot| slot.persistence = SlotPersistence::Unproven,
                ManagedSlotProblem::NotPersistent,
            ),
            (
                |slot| slot.wal_retention = None,
                ManagedSlotProblem::WalRetentionMissing,
            ),
            (
                |slot| slot.confirmed_flush_lsn = None,
                ManagedSlotProblem::ProgressMissing,
            ),
        ];

        for (mutate, problem) in cases {
            let mut candidate = observation();
            mutate(candidate.local_decoder.as_mut().expect("local decoder"));
            assert_eq!(
                validate_standby_decoder_attachment(&policy(), &candidate),
                Err(StandbyDecoderIneligible::ManagedSlot {
                    role: ManagedSlotRole::StandbyDecoder,
                    problem,
                })
            );
        }
    }

    #[test]
    fn validates_replication_slot_names() {
        assert_eq!(slot("a").as_str(), "a");
        assert_eq!(
            slot(&"x".repeat(63)).as_str(),
            "x".repeat(63),
            "PostgreSQL accepts NAMEDATALEN minus one bytes"
        );
        for name in ["", "Upper", "has-dash", &"x".repeat(64)] {
            assert_eq!(ReplicationSlotName::new(name), Err(SlotNameError));
        }
    }

    #[test]
    fn validates_non_nil_catalog_slot_generation() {
        assert_eq!(SlotGeneration::new(Uuid::nil()), Err(SlotGenerationError));
        assert_eq!(generation(0xa1).as_uuid(), Uuid::from_u128(0xa1));
    }

    #[test]
    fn validates_complete_source_identity() {
        let source_cases = [
            (
                (0, 1, 1, Uuid::from_u128(1), CatalogEpoch(1)),
                SourceIdentityError::SystemIdentifier,
            ),
            (
                (1, 0, 1, Uuid::from_u128(1), CatalogEpoch(1)),
                SourceIdentityError::Timeline,
            ),
            (
                (1, 1, 0, Uuid::from_u128(1), CatalogEpoch(1)),
                SourceIdentityError::DatabaseOid,
            ),
            (
                (1, 1, 1, Uuid::nil(), CatalogEpoch(1)),
                SourceIdentityError::RestoreIncarnation,
            ),
            (
                (1, 1, 1, Uuid::from_u128(1), CatalogEpoch(0)),
                SourceIdentityError::CatalogEpoch,
            ),
        ];
        for ((system_identifier, timeline, database_oid, incarnation, epoch), expected) in
            source_cases
        {
            assert_eq!(
                ReplicationSourceIdentity::new(
                    system_identifier,
                    timeline,
                    database_oid,
                    incarnation,
                    epoch,
                ),
                Err(expected)
            );
        }

        let valid_source = source(7);
        assert_eq!(valid_source.system_identifier(), 7_219_834_723_984_723);
        assert_eq!(valid_source.timeline(), 7);
        assert_eq!(valid_source.database_oid(), 16_384);
        assert_eq!(valid_source.restore_incarnation(), Uuid::from_u128(0x1234));
        assert_eq!(valid_source.catalog_epoch(), CatalogEpoch(42));
    }

    #[test]
    fn validates_policy_names_and_checkpoint() {
        assert_eq!(
            StandbyDecoderPolicy::new(
                source(7),
                StandbyDecoderTarget::new(1, slot("physical"), anchor_target(), local_target())
                    .expect("valid target"),
                ManagedTwoPhasePolicy {
                    failover_anchor_at: BEFORE_CHECKPOINT,
                    local_decoder_at: BEFORE_CHECKPOINT,
                },
                PgLsn(0),
                evidence_limits(),
            ),
            Err(StandbyDecoderPolicyError::ZeroCheckpoint)
        );
        let anchor = anchor_target();
        let colliding_physical_name = anchor.name().clone();
        assert_eq!(
            StandbyDecoderTarget::new(1, colliding_physical_name, anchor, local_target()),
            Err(StandbyDecoderTargetError::SlotNameCollision)
        );
        assert_eq!(
            StandbyDecoderTarget::new(
                1,
                slot("physical"),
                managed_target("anchor", 0xa1),
                managed_target("local", 0xa1),
            ),
            Err(StandbyDecoderTargetError::SlotGenerationCollision)
        );
        assert_eq!(
            ManagedSlotTarget::new(slot("missing_generation"), generation(0xa1)),
            Err(ManagedSlotTargetError)
        );
        for boundary in [PgLsn(0), AFTER_CHECKPOINT] {
            assert_eq!(
                StandbyDecoderPolicy::new(
                    source(7),
                    StandbyDecoderTarget::new(
                        1,
                        slot("physical"),
                        anchor_target(),
                        local_target(),
                    )
                    .expect("valid target"),
                    ManagedTwoPhasePolicy {
                        failover_anchor_at: boundary,
                        local_decoder_at: BEFORE_CHECKPOINT,
                    },
                    CHECKPOINT,
                    evidence_limits(),
                ),
                Err(StandbyDecoderPolicyError::UnsafeTwoPhaseBoundary)
            );
        }
    }

    #[test]
    fn validates_evidence_age_bounds() {
        for maximum_observation_age in [
            Duration::ZERO,
            MAX_OBSERVATION_AGE_LIMIT + Duration::from_millis(1),
        ] {
            assert_eq!(
                StandbyDecoderEvidenceLimits::new(
                    maximum_observation_age,
                    Duration::from_secs(3),
                    Duration::from_secs(3),
                ),
                Err(StandbyDecoderEvidenceLimitError::ObservationAge(
                    maximum_observation_age
                ))
            );
        }
        assert_eq!(
            StandbyDecoderEvidenceLimits::new(
                Duration::from_secs(2),
                Duration::from_millis(999),
                Duration::from_secs(3),
            ),
            Err(StandbyDecoderEvidenceLimitError::FeedbackAge(
                Duration::from_millis(999)
            ))
        );
        assert_eq!(
            StandbyDecoderEvidenceLimits::new(
                Duration::from_secs(2),
                MAX_FEEDBACK_AGE_LIMIT + Duration::from_millis(1),
                Duration::from_secs(3),
            ),
            Err(StandbyDecoderEvidenceLimitError::FeedbackAge(
                MAX_FEEDBACK_AGE_LIMIT + Duration::from_millis(1)
            ))
        );
        for maximum_observation_age in [Duration::from_nanos(1), MAX_OBSERVATION_AGE_LIMIT] {
            StandbyDecoderEvidenceLimits::new(
                maximum_observation_age,
                Duration::from_secs(3),
                Duration::from_secs(3),
            )
            .expect("inclusive observation-age boundary");
        }
        for maximum_feedback_age in [MIN_FEEDBACK_AGE_LIMIT, MAX_FEEDBACK_AGE_LIMIT] {
            StandbyDecoderEvidenceLimits::new(
                Duration::from_secs(2),
                maximum_feedback_age,
                Duration::from_secs(3),
            )
            .expect("inclusive feedback-age boundary");
        }
        for maximum_slot_sync_age in [
            Duration::from_millis(999),
            MAX_SLOT_SYNC_AGE_LIMIT + Duration::from_millis(1),
        ] {
            assert_eq!(
                StandbyDecoderEvidenceLimits::new(
                    Duration::from_secs(2),
                    Duration::from_secs(3),
                    maximum_slot_sync_age,
                ),
                Err(StandbyDecoderEvidenceLimitError::SlotSyncAge(
                    maximum_slot_sync_age
                ))
            );
        }
        for maximum_slot_sync_age in [MIN_SLOT_SYNC_AGE_LIMIT, MAX_SLOT_SYNC_AGE_LIMIT] {
            StandbyDecoderEvidenceLimits::new(
                Duration::from_secs(2),
                Duration::from_secs(3),
                maximum_slot_sync_age,
            )
            .expect("inclusive slot-sync-age boundary");
        }
    }
}
