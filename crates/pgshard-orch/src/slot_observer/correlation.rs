//! Source-bound correlation for separately sampled standby replication paths.
//!
//! This module turns the local `PostgreSQL` 18 observations into a bounded proof
//! that a standby receiver and a primary physical-slot walsender reported
//! compatible endpoints for one catalog-selected path. It does not prove that
//! those endpoints were connected to each other and deliberately does not
//! manufacture exact replay-lineage, feedback freshness, catalog-horizon
//! coverage, lifecycle ownership, or slot-sync success.

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use pgshard_types::PgLsn;
use thiserror::Error;
use tokio::time::Instant;

use super::{
    LocalLogicalSlotObservationBatch, LocalPostgresBackendIdentity, LocalPostgresTransactionId,
    LocalPrimaryReplicationObservationBatch, LocalWalReceiverActivity, LocalWalSenderActivity,
};
use crate::standby_slots::{
    FailoverSlotSynchronization, LogicalWalLevel, MIN_FEEDBACK_REPORTING_MARGIN, RecoveryState,
    ReplicationSlotName, ReplicationSourceIdentity, SettingState, SlotActivity, SlotInvalidation,
    SlotPersistence, SlotWalRetention, StandbyDecoderPolicy,
};

/// Bounded correlation of one standby receiver with its primary-side path.
///
/// The proof binds catalog-selected source identity components, database,
/// physical-slot name, receiver state, primary slot ownership, and walsender
/// PID/application state across a local monotonic observation window. It can
/// become stale immediately and is not decoder-attachment authorization.
///
/// The matching endpoint reports do not prove network adjacency or rule out a
/// cascading upstream. A connection-owning runtime must establish that direct
/// relationship separately.
///
/// In particular, `PostgreSQL`'s SQL API exposes a replay LSN without its
/// atomically sampled replay timeline, so this proof does not compare or carry
/// that raw LSN. The peer reply timestamp is only a change token, the raw 32-bit
/// `catalog_xmin` has no cross-sample ordering semantics, and a non-temporary
/// physical slot still lacks mutation-history attestation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelatedStandbyReplicationPath {
    source_identity: ReplicationSourceIdentity,
    member_ordinal: u16,
    oldest_observation_age: Duration,
    physical_slot: ReplicationSlotName,
    wal_receiver_pid: NonZeroU32,
    walsender_identity: LocalPostgresBackendIdentity,
    physical_slot_persistence: SlotPersistence,
    physical_catalog_xmin: LocalPostgresTransactionId,
    physical_restart_lsn: PgLsn,
    physical_wal_retention: SlotWalRetention,
    peer_reply_epoch_micros: NonZeroU64,
}

impl CorrelatedStandbyReplicationPath {
    /// Returns the catalog source whose observable `PostgreSQL` identity matched.
    #[must_use]
    pub const fn source_identity(&self) -> ReplicationSourceIdentity {
        self.source_identity
    }

    /// Returns the catalog-selected standby member ordinal.
    #[must_use]
    pub const fn member_ordinal(&self) -> u16 {
        self.member_ordinal
    }

    /// Returns the age of the oldest local sample at correlation time.
    #[must_use]
    pub const fn oldest_observation_age(&self) -> Duration {
        self.oldest_observation_age
    }

    /// Returns the exact physical slot reported at both ends of the path.
    #[must_use]
    pub const fn physical_slot(&self) -> &ReplicationSlotName {
        &self.physical_slot
    }

    /// Returns the standby-local WAL receiver PID.
    #[must_use]
    pub const fn wal_receiver_pid(&self) -> NonZeroU32 {
        self.wal_receiver_pid
    }

    /// Returns the primary-local walsender PID plus backend generation.
    #[must_use]
    pub const fn walsender_identity(&self) -> LocalPostgresBackendIdentity {
        self.walsender_identity
    }

    /// Returns conservative physical-slot persistence evidence.
    #[must_use]
    pub const fn physical_slot_persistence(&self) -> SlotPersistence {
        self.physical_slot_persistence
    }

    /// Returns the raw, unordered primary-side catalog horizon.
    #[must_use]
    pub const fn physical_catalog_xmin(&self) -> LocalPostgresTransactionId {
        self.physical_catalog_xmin
    }

    /// Returns the primary physical slot's retained WAL position.
    #[must_use]
    pub const fn physical_restart_lsn(&self) -> PgLsn {
        self.physical_restart_lsn
    }

    /// Returns the primary physical slot's retained-WAL classification.
    #[must_use]
    pub const fn physical_wal_retention(&self) -> SlotWalRetention {
        self.physical_wal_retention
    }

    /// Returns the peer-wall-clock reply value as an equality/change token.
    #[must_use]
    pub const fn peer_reply_epoch_micros(&self) -> NonZeroU64 {
        self.peer_reply_epoch_micros
    }
}

/// Correlates compatible separately sampled standby and primary endpoint state.
///
/// The freshness boundary is evaluated against the monotonic clock captured
/// inside this call; callers cannot backdate the decision.
///
/// # Errors
///
/// Rejects stale or inconsistent sample windows; a source, database, role, or
/// configuration mismatch; an unready receiver; or an unsafe/incomplete
/// primary physical-slot and walsender path.
pub fn correlate_standby_replication_path(
    policy: &StandbyDecoderPolicy,
    standby: &LocalLogicalSlotObservationBatch,
    primary: &LocalPrimaryReplicationObservationBatch,
) -> Result<CorrelatedStandbyReplicationPath, StandbyReplicationPathCorrelationError> {
    correlate_standby_replication_path_at(policy, standby, primary, Instant::now())
}

fn correlate_standby_replication_path_at(
    policy: &StandbyDecoderPolicy,
    standby: &LocalLogicalSlotObservationBatch,
    primary: &LocalPrimaryReplicationObservationBatch,
    evaluated_at: Instant,
) -> Result<CorrelatedStandbyReplicationPath, StandbyReplicationPathCorrelationError> {
    let oldest_observation_age = observation_age(policy, standby, primary, evaluated_at)?;
    validate_sources_and_roles(policy, standby, primary)?;
    let wal_receiver_pid = validate_standby_path(policy, standby)?;
    let primary_path = validate_primary_path(policy, primary)?;

    Ok(CorrelatedStandbyReplicationPath {
        source_identity: policy.expected_source(),
        member_ordinal: policy.member_ordinal(),
        oldest_observation_age,
        physical_slot: policy.physical_slot().clone(),
        wal_receiver_pid,
        walsender_identity: primary_path.walsender_identity,
        physical_slot_persistence: primary_path.persistence,
        physical_catalog_xmin: primary_path.catalog_xmin,
        physical_restart_lsn: primary_path.restart_lsn,
        physical_wal_retention: primary_path.wal_retention,
        peer_reply_epoch_micros: primary_path.peer_reply_epoch_micros,
    })
}

fn validate_sources_and_roles(
    policy: &StandbyDecoderPolicy,
    standby: &LocalLogicalSlotObservationBatch,
    primary: &LocalPrimaryReplicationObservationBatch,
) -> Result<(), StandbyReplicationPathCorrelationError> {
    let expected = policy.expected_source();
    let prerequisites = standby.prerequisites();
    if standby.database_name() != primary.database_name() {
        return Err(StandbyReplicationPathCorrelationError::DatabaseNameMismatch);
    }
    if !matches_source(
        expected,
        prerequisites.system_identifier(),
        prerequisites.checkpoint_timeline(),
        standby.database_oid(),
    ) {
        return Err(StandbyReplicationPathCorrelationError::StandbySourceMismatch);
    }
    if !matches_source(
        expected,
        primary.system_identifier(),
        primary.checkpoint_timeline(),
        primary.database_oid(),
    ) {
        return Err(StandbyReplicationPathCorrelationError::PrimarySourceMismatch);
    }
    if prerequisites.recovery() != RecoveryState::Standby {
        return Err(StandbyReplicationPathCorrelationError::StandbyNotInRecovery);
    }
    if primary.recovery() != RecoveryState::Writable {
        return Err(StandbyReplicationPathCorrelationError::PrimaryNotWritable);
    }
    if primary.current_timeline() != Some(expected.timeline()) {
        return Err(StandbyReplicationPathCorrelationError::PrimaryCurrentTimelineMismatch);
    }
    if prerequisites.wal_level() != LogicalWalLevel::Logical {
        return Err(StandbyReplicationPathCorrelationError::StandbyWalLevelInsufficient);
    }
    if primary.wal_level() != LogicalWalLevel::Logical {
        return Err(StandbyReplicationPathCorrelationError::PrimaryWalLevelInsufficient);
    }
    Ok(())
}

fn validate_standby_path(
    policy: &StandbyDecoderPolicy,
    standby: &LocalLogicalSlotObservationBatch,
) -> Result<NonZeroU32, StandbyReplicationPathCorrelationError> {
    let prerequisites = standby.prerequisites();
    if prerequisites.hot_standby_feedback() != SettingState::Enabled {
        return Err(StandbyReplicationPathCorrelationError::HotStandbyFeedbackDisabled);
    }
    let maximum_feedback_interval = policy
        .evidence_limits()
        .maximum_feedback_age()
        .saturating_sub(MIN_FEEDBACK_REPORTING_MARGIN);
    let feedback_interval = prerequisites.wal_receiver_status_interval();
    if feedback_interval.is_zero() || feedback_interval > maximum_feedback_interval {
        return Err(
            StandbyReplicationPathCorrelationError::FeedbackReportingIntervalUnsafe {
                observed: feedback_interval,
                maximum: maximum_feedback_interval,
            },
        );
    }
    if prerequisites.sync_replication_slots() != SettingState::Enabled {
        return Err(StandbyReplicationPathCorrelationError::SlotSynchronizationDisabled);
    }
    if prerequisites.primary_slot_name() != Some(policy.physical_slot()) {
        return Err(StandbyReplicationPathCorrelationError::PrimarySlotNameMismatch);
    }
    let wal_receiver_pid = prerequisites
        .wal_receiver_pid()
        .ok_or(StandbyReplicationPathCorrelationError::WalReceiverMissing)?;
    if prerequisites.wal_receiver_activity() != LocalWalReceiverActivity::Streaming {
        return Err(StandbyReplicationPathCorrelationError::WalReceiverNotStreaming);
    }
    if prerequisites.wal_receiver_slot_name() != Some(policy.physical_slot()) {
        return Err(StandbyReplicationPathCorrelationError::WalReceiverSlotNameMismatch);
    }
    if prerequisites.wal_receiver_received_timeline() != Some(policy.expected_source().timeline()) {
        return Err(StandbyReplicationPathCorrelationError::WalReceiverTimelineMismatch);
    }
    Ok(wal_receiver_pid)
}

struct PrimaryPathEvidence {
    walsender_identity: LocalPostgresBackendIdentity,
    persistence: SlotPersistence,
    catalog_xmin: LocalPostgresTransactionId,
    restart_lsn: PgLsn,
    wal_retention: SlotWalRetention,
    peer_reply_epoch_micros: NonZeroU64,
}

fn validate_primary_path(
    policy: &StandbyDecoderPolicy,
    primary: &LocalPrimaryReplicationObservationBatch,
) -> Result<PrimaryPathEvidence, StandbyReplicationPathCorrelationError> {
    if primary.failover_slot_synchronization() != FailoverSlotSynchronization::GatedOnPhysicalSlot {
        return Err(StandbyReplicationPathCorrelationError::FailoverSlotNotGated);
    }

    let physical_slot = primary
        .physical_slot()
        .ok_or(StandbyReplicationPathCorrelationError::PhysicalSlotMissing)?;
    if physical_slot.name() != policy.physical_slot() {
        return Err(StandbyReplicationPathCorrelationError::PhysicalSlotNameMismatch);
    }
    if physical_slot.persistence() == SlotPersistence::NonPersistent {
        return Err(StandbyReplicationPathCorrelationError::PhysicalSlotTemporary);
    }
    let physical_slot_pid = match physical_slot.activity() {
        SlotActivity::Active(pid) => pid,
        SlotActivity::Inactive => {
            return Err(StandbyReplicationPathCorrelationError::PhysicalSlotInactive);
        }
    };
    if let Some(reason) = physical_slot.invalidation() {
        return Err(StandbyReplicationPathCorrelationError::PhysicalSlotInvalidated(reason));
    }
    let physical_catalog_xmin = physical_slot
        .catalog_xmin()
        .ok_or(StandbyReplicationPathCorrelationError::PhysicalCatalogXminMissing)?;
    let physical_restart_lsn = physical_slot
        .restart_lsn()
        .ok_or(StandbyReplicationPathCorrelationError::PhysicalRestartLsnMissing)?;
    let physical_wal_retention = physical_slot
        .wal_retention()
        .ok_or(StandbyReplicationPathCorrelationError::PhysicalWalRetentionMissing)?;
    if !matches!(
        physical_wal_retention,
        SlotWalRetention::Reserved | SlotWalRetention::Extended
    ) {
        return Err(
            StandbyReplicationPathCorrelationError::PhysicalWalNotRetained(physical_wal_retention),
        );
    }

    let wal_sender = primary
        .wal_sender()
        .ok_or(StandbyReplicationPathCorrelationError::WalSenderMissing)?;
    if wal_sender.identity().pid() != physical_slot_pid {
        return Err(StandbyReplicationPathCorrelationError::WalSenderSlotPidMismatch);
    }
    if wal_sender.application_name() != policy.physical_slot() {
        return Err(StandbyReplicationPathCorrelationError::WalSenderApplicationNameMismatch);
    }
    if wal_sender.activity() != LocalWalSenderActivity::Streaming {
        return Err(StandbyReplicationPathCorrelationError::WalSenderNotStreaming);
    }
    let peer_reply_epoch_micros = wal_sender
        .reply_epoch_micros()
        .ok_or(StandbyReplicationPathCorrelationError::WalSenderReplyMissing)?;

    Ok(PrimaryPathEvidence {
        walsender_identity: wal_sender.identity(),
        persistence: physical_slot.persistence(),
        catalog_xmin: physical_catalog_xmin,
        restart_lsn: physical_restart_lsn,
        wal_retention: physical_wal_retention,
        peer_reply_epoch_micros,
    })
}

fn observation_age(
    policy: &StandbyDecoderPolicy,
    standby: &LocalLogicalSlotObservationBatch,
    primary: &LocalPrimaryReplicationObservationBatch,
    evaluated_at: Instant,
) -> Result<Duration, StandbyReplicationPathCorrelationError> {
    let standby_started = standby.prerequisite_collection_started_at();
    let standby_prerequisites_finished = standby.prerequisite_collection_finished_at();
    let standby_slots_started = standby.slot_collection_started_at();
    let standby_finished = standby.slot_collection_finished_at();
    let primary_started = primary.collection_started_at();
    let primary_finished = primary.collection_finished_at();
    if standby_started > standby_prerequisites_finished
        || standby_prerequisites_finished > standby_slots_started
        || standby_slots_started > standby_finished
        || primary_started > primary_finished
    {
        return Err(StandbyReplicationPathCorrelationError::ObservationWindowInconsistent);
    }
    let started = standby_started.min(primary_started);
    let finished = standby_finished.max(primary_finished);
    if evaluated_at < finished {
        return Err(StandbyReplicationPathCorrelationError::ObservationWindowInconsistent);
    }
    let observed = evaluated_at.duration_since(started);
    let maximum = policy.evidence_limits().maximum_observation_age();
    if observed > maximum {
        return Err(StandbyReplicationPathCorrelationError::ObservationStale { observed, maximum });
    }
    Ok(observed)
}

fn matches_source(
    expected: ReplicationSourceIdentity,
    system_identifier: u64,
    timeline: u32,
    database_oid: u32,
) -> bool {
    system_identifier == expected.system_identifier()
        && timeline == expected.timeline()
        && database_oid == expected.database_oid()
}

/// A fail-closed rejection while correlating one primary/standby path.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum StandbyReplicationPathCorrelationError {
    /// Collection intervals were reversed or evaluated before they completed.
    #[error("replication-path observation window is internally inconsistent")]
    ObservationWindowInconsistent,
    /// The complete sample set exceeded its catalog-selected age bound.
    #[error("replication-path observation age {observed:?} exceeds {maximum:?}")]
    ObservationStale {
        /// Age of the oldest sampled value.
        observed: Duration,
        /// Maximum accepted age.
        maximum: Duration,
    },
    /// Primary and standby connections used different database names.
    #[error("primary and standby observation database names differ")]
    DatabaseNameMismatch,
    /// Standby physical source components differ from the catalog fence.
    #[error("standby PostgreSQL source identity does not match the catalog fence")]
    StandbySourceMismatch,
    /// Primary physical source components differ from the catalog fence.
    #[error("primary PostgreSQL source identity does not match the catalog fence")]
    PrimarySourceMismatch,
    /// Candidate server is not in recovery.
    #[error("candidate server is not a standby")]
    StandbyNotInRecovery,
    /// Upstream server is not writable.
    #[error("upstream server is not writable")]
    PrimaryNotWritable,
    /// Writable upstream is inserting WAL on another or no timeline.
    #[error("primary current WAL insertion timeline does not match the catalog fence")]
    PrimaryCurrentTimelineMismatch,
    /// Candidate WAL level is insufficient for logical decoding.
    #[error("standby wal_level is insufficient for logical decoding")]
    StandbyWalLevelInsufficient,
    /// Primary WAL level is insufficient for logical decoding.
    #[error("primary wal_level is insufficient for logical decoding")]
    PrimaryWalLevelInsufficient,
    /// Standby feedback is disabled.
    #[error("hot_standby_feedback is disabled")]
    HotStandbyFeedbackDisabled,
    /// Feedback reporting is disabled or outside the policy scheduling margin.
    #[error("wal_receiver_status_interval is zero or exceeds the safe bound")]
    FeedbackReportingIntervalUnsafe {
        /// Observed interval.
        observed: Duration,
        /// Maximum accepted interval after the scheduling margin.
        maximum: Duration,
    },
    /// Continuous failover-slot synchronization is disabled.
    #[error("sync_replication_slots is disabled")]
    SlotSynchronizationDisabled,
    /// Standby configuration names another or no physical slot.
    #[error("primary_slot_name does not match the catalog-selected physical slot")]
    PrimarySlotNameMismatch,
    /// No displayable WAL receiver exists.
    #[error("standby WAL receiver is absent")]
    WalReceiverMissing,
    /// WAL receiver is not streaming.
    #[error("standby WAL receiver is not streaming")]
    WalReceiverNotStreaming,
    /// WAL receiver reports another or no physical slot.
    #[error("standby WAL receiver slot does not match the catalog-selected physical slot")]
    WalReceiverSlotNameMismatch,
    /// Receiver has not received WAL on the catalog-selected current timeline.
    #[error("standby WAL receiver timeline does not match the catalog fence")]
    WalReceiverTimelineMismatch,
    /// Primary does not gate failover-slot progress on this physical slot.
    #[error("physical slot is absent from synchronized_standby_slots")]
    FailoverSlotNotGated,
    /// Requested physical slot is absent on the primary.
    #[error("primary physical slot is absent")]
    PhysicalSlotMissing,
    /// Primary physical slot row has another name.
    #[error("primary physical slot name does not match the catalog target")]
    PhysicalSlotNameMismatch,
    /// Primary physical slot is known to be temporary.
    #[error("primary physical slot is temporary")]
    PhysicalSlotTemporary,
    /// No backend owns the primary physical slot.
    #[error("primary physical slot is inactive")]
    PhysicalSlotInactive,
    /// `PostgreSQL` invalidated the primary physical slot.
    #[error("primary physical slot is invalidated: {0:?}")]
    PhysicalSlotInvalidated(SlotInvalidation),
    /// Primary physical slot has no catalog horizon.
    #[error("primary physical slot catalog_xmin is absent")]
    PhysicalCatalogXminMissing,
    /// Primary physical slot has no retained WAL position.
    #[error("primary physical slot restart_lsn is absent")]
    PhysicalRestartLsnMissing,
    /// `PostgreSQL` omitted the physical slot's WAL-retention state.
    #[error("primary physical slot WAL-retention state is absent")]
    PhysicalWalRetentionMissing,
    /// Required physical-slot WAL is no longer retained.
    #[error("primary physical slot WAL is not retained: {0:?}")]
    PhysicalWalNotRetained(SlotWalRetention),
    /// No walsender owns the physical slot PID.
    #[error("primary physical-slot walsender is absent")]
    WalSenderMissing,
    /// Walsender and physical slot report different primary-local PIDs.
    #[error("primary walsender PID does not own the physical slot")]
    WalSenderSlotPidMismatch,
    /// Walsender application name does not match the catalog member slot.
    #[error("primary walsender application_name does not match the physical slot")]
    WalSenderApplicationNameMismatch,
    /// Walsender is not in streaming state.
    #[error("primary walsender is not streaming")]
    WalSenderNotStreaming,
    /// `PostgreSQL` has not reported any peer reply on this walsender.
    #[error("primary walsender has no peer reply token")]
    WalSenderReplyMissing,
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use pgshard_types::CatalogEpoch;
    use uuid::Uuid;

    use super::super::{
        LocalPhysicalReplicationSlotObservation, LocalStandbyPrerequisiteObservation,
        LocalWalSenderObservation, LogicalSlotSnapshotEntry,
    };
    use super::*;
    use crate::standby_slots::{
        ManagedSlotTarget, ManagedTwoPhasePolicy, SlotGeneration, StandbyDecoderEvidenceLimits,
        StandbyDecoderTarget,
    };

    const SYSTEM_IDENTIFIER: u64 = 7_219_834_723_984_723;
    const TIMELINE: u32 = 7;
    const DATABASE_OID: u32 = 16_384;
    const CHECKPOINT: PgLsn = PgLsn(0x3000);
    const RESTART: PgLsn = PgLsn(0x2000);

    #[derive(Clone)]
    struct Fixture {
        policy: StandbyDecoderPolicy,
        standby: LocalLogicalSlotObservationBatch,
        primary: LocalPrimaryReplicationObservationBatch,
        evaluated_at: Instant,
    }

    fn nonzero_u32(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("nonzero test value")
    }

    fn nonzero_u64(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).expect("nonzero test value")
    }

    fn slot(name: &str) -> ReplicationSlotName {
        ReplicationSlotName::new(name).expect("valid test slot name")
    }

    fn managed_target(prefix: &str, generation: u128) -> ManagedSlotTarget {
        let generation =
            SlotGeneration::new(Uuid::from_u128(generation)).expect("valid test slot generation");
        ManagedSlotTarget::new(
            slot(&format!("{prefix}_{}", generation.as_uuid().simple())),
            generation,
        )
        .expect("generation-encoded test slot")
    }

    fn policy() -> StandbyDecoderPolicy {
        let source = ReplicationSourceIdentity::new(
            SYSTEM_IDENTIFIER,
            TIMELINE,
            DATABASE_OID,
            Uuid::from_u128(0x1234),
            CatalogEpoch(42),
        )
        .expect("valid source identity");
        let target = StandbyDecoderTarget::new(
            1,
            slot("pgshard_member_0001"),
            managed_target("pgshard_anchor", 0xa1),
            managed_target("pgshard_local", 0xb1),
        )
        .expect("valid standby target");
        let limits = StandbyDecoderEvidenceLimits::new(
            Duration::from_secs(2),
            Duration::from_secs(3),
            Duration::from_secs(3),
        )
        .expect("valid evidence limits");
        StandbyDecoderPolicy::new(
            source,
            target,
            ManagedTwoPhasePolicy {
                failover_anchor_at: PgLsn(CHECKPOINT.0 - 2),
                local_decoder_at: PgLsn(CHECKPOINT.0 - 2),
            },
            CHECKPOINT,
            limits,
        )
        .expect("valid decoder policy")
    }

    fn standby_batch(
        policy: &StandbyDecoderPolicy,
        base: Instant,
    ) -> LocalLogicalSlotObservationBatch {
        let physical_slot = policy.physical_slot().clone();
        LocalLogicalSlotObservationBatch {
            database_name: "shardschema".to_owned(),
            database_oid: DATABASE_OID,
            prerequisite_collection_started_at: base,
            prerequisite_collection_finished_at: base + Duration::from_millis(1),
            prerequisites: LocalStandbyPrerequisiteObservation {
                system_identifier: SYSTEM_IDENTIFIER,
                checkpoint_timeline: TIMELINE,
                recovery: RecoveryState::Standby,
                wal_level: LogicalWalLevel::Logical,
                hot_standby_feedback: SettingState::Enabled,
                wal_receiver_status_interval: Duration::from_secs(1),
                sync_replication_slots: SettingState::Enabled,
                primary_slot_name: Some(physical_slot.clone()),
                replay_lsn: None,
                wal_receiver_pid: Some(nonzero_u32(401)),
                wal_receiver_activity: LocalWalReceiverActivity::Streaming,
                wal_receiver_slot_name: Some(physical_slot),
                wal_receiver_received_timeline: Some(TIMELINE),
                slot_sync_worker: None,
            },
            slot_collection_started_at: base + Duration::from_millis(2),
            slot_collection_finished_at: base + Duration::from_millis(3),
            entries: vec![LogicalSlotSnapshotEntry {
                target: policy.local_decoder().clone(),
                observation: None,
            }],
        }
    }

    fn primary_batch(
        policy: &StandbyDecoderPolicy,
        base: Instant,
    ) -> LocalPrimaryReplicationObservationBatch {
        let sender_pid = nonzero_u32(501);
        LocalPrimaryReplicationObservationBatch {
            database_name: "shardschema".to_owned(),
            database_oid: DATABASE_OID,
            collection_started_at: base + Duration::from_millis(4),
            collection_finished_at: base + Duration::from_millis(5),
            system_identifier: SYSTEM_IDENTIFIER,
            checkpoint_timeline: TIMELINE,
            current_timeline: Some(TIMELINE),
            recovery: RecoveryState::Writable,
            wal_level: LogicalWalLevel::Logical,
            failover_slot_synchronization: FailoverSlotSynchronization::GatedOnPhysicalSlot,
            physical_slot: Some(LocalPhysicalReplicationSlotObservation {
                name: policy.physical_slot().clone(),
                persistence: SlotPersistence::Unproven,
                activity: SlotActivity::Active(sender_pid),
                catalog_xmin: Some(LocalPostgresTransactionId(nonzero_u32(755))),
                restart_lsn: Some(RESTART),
                wal_retention: Some(SlotWalRetention::Reserved),
                invalidation: None,
            }),
            wal_sender: Some(LocalWalSenderObservation {
                identity: LocalPostgresBackendIdentity {
                    pid: sender_pid,
                    start_epoch_micros: nonzero_u64(1_700_000_000_000_000),
                },
                application_name: policy.physical_slot().clone(),
                activity: LocalWalSenderActivity::Streaming,
                reply_epoch_micros: Some(nonzero_u64(1_700_000_001_000_000)),
            }),
        }
    }

    fn fixture() -> Fixture {
        let policy = policy();
        let base = Instant::now();
        Fixture {
            standby: standby_batch(&policy, base),
            primary: primary_batch(&policy, base),
            policy,
            evaluated_at: base + Duration::from_millis(6),
        }
    }

    fn reject(mutate: impl FnOnce(&mut Fixture), expected: StandbyReplicationPathCorrelationError) {
        let mut fixture = fixture();
        mutate(&mut fixture);
        assert_eq!(
            correlate_standby_replication_path_at(
                &fixture.policy,
                &fixture.standby,
                &fixture.primary,
                fixture.evaluated_at,
            ),
            Err(expected)
        );
    }

    fn prerequisites(fixture: &mut Fixture) -> &mut LocalStandbyPrerequisiteObservation {
        &mut fixture.standby.prerequisites
    }

    fn physical_slot(fixture: &mut Fixture) -> &mut LocalPhysicalReplicationSlotObservation {
        fixture
            .primary
            .physical_slot
            .as_mut()
            .expect("fixture physical slot")
    }

    fn wal_sender(fixture: &mut Fixture) -> &mut LocalWalSenderObservation {
        fixture
            .primary
            .wal_sender
            .as_mut()
            .expect("fixture walsender")
    }

    #[test]
    fn correlates_only_the_bounded_raw_replication_path() {
        let fixture = fixture();
        let proof = correlate_standby_replication_path_at(
            &fixture.policy,
            &fixture.standby,
            &fixture.primary,
            fixture.evaluated_at,
        )
        .expect("coherent fixture must correlate");

        assert_eq!(proof.source_identity(), fixture.policy.expected_source());
        assert_eq!(proof.member_ordinal(), 1);
        assert_eq!(proof.oldest_observation_age(), Duration::from_millis(6));
        assert_eq!(proof.physical_slot(), fixture.policy.physical_slot());
        assert_eq!(proof.wal_receiver_pid(), nonzero_u32(401));
        assert_eq!(proof.walsender_identity().pid(), nonzero_u32(501));
        assert_eq!(
            proof.walsender_identity().start_epoch_micros(),
            nonzero_u64(1_700_000_000_000_000)
        );
        assert_eq!(proof.physical_slot_persistence(), SlotPersistence::Unproven);
        assert_eq!(proof.physical_catalog_xmin().get(), nonzero_u32(755));
        assert_eq!(proof.physical_restart_lsn(), RESTART);
        assert_eq!(proof.physical_wal_retention(), SlotWalRetention::Reserved);
        assert_eq!(
            proof.peer_reply_epoch_micros(),
            nonzero_u64(1_700_000_001_000_000)
        );
    }

    #[test]
    fn raw_sql_replay_position_never_participates_in_correlation() {
        for raw_replay_lsn in [None, Some(PgLsn(CHECKPOINT.0 - 1))] {
            let mut fixture = fixture();
            prerequisites(&mut fixture).replay_lsn = raw_replay_lsn;

            correlate_standby_replication_path_at(
                &fixture.policy,
                &fixture.standby,
                &fixture.primary,
                fixture.evaluated_at,
            )
            .expect("raw replay SQL evidence must remain non-authorizing");
        }
    }

    #[test]
    fn rejects_inconsistent_and_stale_observation_windows() {
        reject(
            |fixture| {
                fixture.standby.prerequisite_collection_finished_at =
                    fixture.standby.prerequisite_collection_started_at - Duration::from_millis(1);
            },
            StandbyReplicationPathCorrelationError::ObservationWindowInconsistent,
        );
        reject(
            |fixture| {
                fixture.evaluated_at = fixture.primary.collection_started_at;
            },
            StandbyReplicationPathCorrelationError::ObservationWindowInconsistent,
        );
        reject(
            |fixture| {
                fixture.evaluated_at =
                    fixture.standby.prerequisite_collection_started_at + Duration::from_secs(3);
            },
            StandbyReplicationPathCorrelationError::ObservationStale {
                observed: Duration::from_secs(3),
                maximum: Duration::from_secs(2),
            },
        );
    }

    #[test]
    fn public_api_cannot_backdate_stale_observations() {
        let policy = policy();
        let base = Instant::now()
            .checked_sub(Duration::from_secs(3))
            .expect("monotonic clock has three seconds of history");
        let standby = standby_batch(&policy, base);
        let primary = primary_batch(&policy, base);

        assert!(matches!(
            correlate_standby_replication_path(&policy, &standby, &primary),
            Err(StandbyReplicationPathCorrelationError::ObservationStale {
                observed,
                maximum,
            }) if observed >= Duration::from_secs(3) && maximum == Duration::from_secs(2)
        ));
    }

    #[test]
    fn rejects_database_source_role_and_configuration_mismatches() {
        reject(
            |fixture| fixture.primary.database_name = "postgres".to_owned(),
            StandbyReplicationPathCorrelationError::DatabaseNameMismatch,
        );
        reject(
            |fixture| prerequisites(fixture).system_identifier += 1,
            StandbyReplicationPathCorrelationError::StandbySourceMismatch,
        );
        reject(
            |fixture| prerequisites(fixture).checkpoint_timeline += 1,
            StandbyReplicationPathCorrelationError::StandbySourceMismatch,
        );
        reject(
            |fixture| fixture.primary.checkpoint_timeline += 1,
            StandbyReplicationPathCorrelationError::PrimarySourceMismatch,
        );
        reject(
            |fixture| prerequisites(fixture).recovery = RecoveryState::Writable,
            StandbyReplicationPathCorrelationError::StandbyNotInRecovery,
        );
        reject(
            |fixture| fixture.primary.recovery = RecoveryState::Standby,
            StandbyReplicationPathCorrelationError::PrimaryNotWritable,
        );
        reject(
            |fixture| fixture.primary.current_timeline = Some(TIMELINE + 1),
            StandbyReplicationPathCorrelationError::PrimaryCurrentTimelineMismatch,
        );
        reject(
            |fixture| prerequisites(fixture).wal_level = LogicalWalLevel::Insufficient,
            StandbyReplicationPathCorrelationError::StandbyWalLevelInsufficient,
        );
        reject(
            |fixture| fixture.primary.wal_level = LogicalWalLevel::Insufficient,
            StandbyReplicationPathCorrelationError::PrimaryWalLevelInsufficient,
        );
        reject(
            |fixture| prerequisites(fixture).hot_standby_feedback = SettingState::Disabled,
            StandbyReplicationPathCorrelationError::HotStandbyFeedbackDisabled,
        );
        reject(
            |fixture| {
                prerequisites(fixture).wal_receiver_status_interval = Duration::ZERO;
            },
            StandbyReplicationPathCorrelationError::FeedbackReportingIntervalUnsafe {
                observed: Duration::ZERO,
                maximum: Duration::from_secs(2),
            },
        );
        reject(
            |fixture| prerequisites(fixture).sync_replication_slots = SettingState::Disabled,
            StandbyReplicationPathCorrelationError::SlotSynchronizationDisabled,
        );
        reject(
            |fixture| prerequisites(fixture).primary_slot_name = None,
            StandbyReplicationPathCorrelationError::PrimarySlotNameMismatch,
        );
    }

    #[test]
    fn rejects_receiver_path_mismatches() {
        reject(
            |fixture| prerequisites(fixture).wal_receiver_pid = None,
            StandbyReplicationPathCorrelationError::WalReceiverMissing,
        );
        reject(
            |fixture| {
                prerequisites(fixture).wal_receiver_activity = LocalWalReceiverActivity::Stopped;
            },
            StandbyReplicationPathCorrelationError::WalReceiverNotStreaming,
        );
        reject(
            |fixture| prerequisites(fixture).wal_receiver_slot_name = None,
            StandbyReplicationPathCorrelationError::WalReceiverSlotNameMismatch,
        );
        reject(
            |fixture| prerequisites(fixture).wal_receiver_received_timeline = None,
            StandbyReplicationPathCorrelationError::WalReceiverTimelineMismatch,
        );
        reject(
            |fixture| {
                prerequisites(fixture).wal_receiver_received_timeline = Some(TIMELINE + 1);
            },
            StandbyReplicationPathCorrelationError::WalReceiverTimelineMismatch,
        );
        reject(
            |fixture| {
                fixture.primary.failover_slot_synchronization =
                    FailoverSlotSynchronization::NotGated;
            },
            StandbyReplicationPathCorrelationError::FailoverSlotNotGated,
        );
    }

    #[test]
    fn rejects_unusable_primary_physical_slots() {
        reject(
            |fixture| fixture.primary.physical_slot = None,
            StandbyReplicationPathCorrelationError::PhysicalSlotMissing,
        );
        reject(
            |fixture| physical_slot(fixture).name = slot("another_physical_slot"),
            StandbyReplicationPathCorrelationError::PhysicalSlotNameMismatch,
        );
        reject(
            |fixture| physical_slot(fixture).persistence = SlotPersistence::NonPersistent,
            StandbyReplicationPathCorrelationError::PhysicalSlotTemporary,
        );
        reject(
            |fixture| physical_slot(fixture).activity = SlotActivity::Inactive,
            StandbyReplicationPathCorrelationError::PhysicalSlotInactive,
        );
        reject(
            |fixture| physical_slot(fixture).invalidation = Some(SlotInvalidation::WalRemoved),
            StandbyReplicationPathCorrelationError::PhysicalSlotInvalidated(
                SlotInvalidation::WalRemoved,
            ),
        );
        reject(
            |fixture| physical_slot(fixture).catalog_xmin = None,
            StandbyReplicationPathCorrelationError::PhysicalCatalogXminMissing,
        );
        reject(
            |fixture| physical_slot(fixture).restart_lsn = None,
            StandbyReplicationPathCorrelationError::PhysicalRestartLsnMissing,
        );
        reject(
            |fixture| physical_slot(fixture).wal_retention = None,
            StandbyReplicationPathCorrelationError::PhysicalWalRetentionMissing,
        );
        reject(
            |fixture| {
                physical_slot(fixture).wal_retention = Some(SlotWalRetention::Lost);
            },
            StandbyReplicationPathCorrelationError::PhysicalWalNotRetained(SlotWalRetention::Lost),
        );
    }

    #[test]
    fn rejects_incoherent_primary_walsenders() {
        reject(
            |fixture| fixture.primary.wal_sender = None,
            StandbyReplicationPathCorrelationError::WalSenderMissing,
        );
        reject(
            |fixture| wal_sender(fixture).identity.pid = nonzero_u32(999),
            StandbyReplicationPathCorrelationError::WalSenderSlotPidMismatch,
        );
        reject(
            |fixture| wal_sender(fixture).application_name = slot("another_application"),
            StandbyReplicationPathCorrelationError::WalSenderApplicationNameMismatch,
        );
        reject(
            |fixture| wal_sender(fixture).activity = LocalWalSenderActivity::Catchup,
            StandbyReplicationPathCorrelationError::WalSenderNotStreaming,
        );
        reject(
            |fixture| wal_sender(fixture).reply_epoch_micros = None,
            StandbyReplicationPathCorrelationError::WalSenderReplyMissing,
        );
    }
}
