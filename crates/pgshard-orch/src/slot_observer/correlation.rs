//! Source-bound correlation for separately sampled standby replication paths.
//!
//! This module turns the local `PostgreSQL` 18 observations into a bounded proof
//! that a standby receiver and a primary physical-slot walsender reported
//! compatible endpoints for one catalog-selected path. It does not prove that
//! those endpoints were connected to each other and deliberately does not
//! manufacture exact replay-lineage, feedback freshness, catalog-horizon
//! coverage, lifecycle ownership, or source-bound recent slot-sync success.

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use pgshard_types::PgLsn;
use thiserror::Error;
use tokio::time::Instant;

use super::{
    LocalLogicalSlotObservationBatch, LocalPostgresBackendIdentity, LocalPostgresTransactionId,
    LocalPrimaryReplicationObservationBatch, LocalSlotSyncWorkerActivity, LocalWalReceiverActivity,
    LocalWalSenderActivity,
};
use crate::standby_slots::{
    FailoverSlotSynchronization, LogicalSlotKind, LogicalSlotObservation, LogicalSlotPlugin,
    LogicalWalLevel, MIN_FEEDBACK_REPORTING_MARGIN, RecoveryState, ReplicationSlotName,
    ReplicationSourceIdentity, SettingState, SlotActivity, SlotInvalidation, SlotPersistence,
    SlotWalRetention, SourceBoundReplayFloor, StandbyDecoderPolicy,
};

/// Bounded correlation of one standby receiver with its primary-side path.
///
/// The proof binds catalog-selected source identity components, database,
/// physical-slot name, receiver state, primary slot activity, and walsender
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
    standby_replay_floor_lsn: PgLsn,
    physical_slot: ReplicationSlotName,
    wal_receiver_pid: NonZeroU32,
    slot_sync_worker_identity: LocalPostgresBackendIdentity,
    walsender_identity: LocalPostgresBackendIdentity,
    physical_slot_persistence: SlotPersistence,
    physical_catalog_xmin: LocalPostgresTransactionId,
    physical_restart_lsn: PgLsn,
    physical_wal_retention: SlotWalRetention,
    peer_reply_epoch_micros: NonZeroU64,
    failover_anchor: ReplicationSlotName,
    primary_failover_anchor_confirmed_lsn: PgLsn,
    synchronized_failover_anchor_confirmed_lsn: PgLsn,
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

    /// Returns the source-bound lower bound on standby replay.
    ///
    /// This is the standby control-file checkpoint record, not the unpaired
    /// live replay LSN. It may have been inherited from a base backup.
    #[must_use]
    pub const fn standby_replay_floor_lsn(&self) -> PgLsn {
        self.standby_replay_floor_lsn
    }

    /// Returns an opaque source-bound replay floor for later attachment gates.
    ///
    /// The value comes from a coherent control-file checkpoint pair already
    /// matched to the catalog source, never from the raw SQL replay getter.
    #[must_use]
    pub const fn source_bound_replay_floor(&self) -> SourceBoundReplayFloor {
        SourceBoundReplayFloor::from_correlated_path(self)
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

    /// Returns the stable local slot-sync worker generation around the slot snapshot.
    ///
    /// This identity proves only that the two local worker samples surrounded
    /// the slot query without an observed restart. It does not identify the
    /// worker's upstream connection or date its last successful cycle.
    #[must_use]
    pub const fn slot_sync_worker_identity(&self) -> LocalPostgresBackendIdentity {
        self.slot_sync_worker_identity
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

    /// Returns the catalog-selected failover-anchor slot name observed at both ends.
    ///
    /// The name encodes the requested generation, but the observations do not
    /// attest lifecycle ownership or mutation history.
    #[must_use]
    pub const fn failover_anchor(&self) -> &ReplicationSlotName {
        &self.failover_anchor
    }

    /// Returns the primary failover anchor's conservative confirmed-flush LSN.
    ///
    /// This separately sampled value is comparison evidence, not attachment
    /// authority or proof of a recent slot-sync cycle.
    #[must_use]
    pub const fn primary_failover_anchor_confirmed_lsn(&self) -> PgLsn {
        self.primary_failover_anchor_confirmed_lsn
    }

    /// Returns the synchronized standby copy's conservative confirmed-flush LSN.
    ///
    /// This separately sampled value is comparison evidence, not attachment
    /// authority or proof of direct upstream adjacency.
    #[must_use]
    pub const fn synchronized_failover_anchor_confirmed_lsn(&self) -> PgLsn {
        self.synchronized_failover_anchor_confirmed_lsn
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
/// primary physical-slot, walsender, or failover-anchor path.
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
    let standby_path = validate_standby_path(policy, standby)?;
    let primary_path = validate_primary_path(policy, primary)?;
    let failover_anchor = validate_failover_anchors(policy, standby, primary)?;

    Ok(CorrelatedStandbyReplicationPath {
        source_identity: policy.expected_source(),
        member_ordinal: policy.member_ordinal(),
        oldest_observation_age,
        standby_replay_floor_lsn: standby_path.replay_floor_lsn,
        physical_slot: policy.physical_slot().clone(),
        wal_receiver_pid: standby_path.wal_receiver_pid,
        slot_sync_worker_identity: standby_path.slot_sync_worker_identity,
        walsender_identity: primary_path.walsender_identity,
        physical_slot_persistence: primary_path.persistence,
        physical_catalog_xmin: primary_path.catalog_xmin,
        physical_restart_lsn: primary_path.restart_lsn,
        physical_wal_retention: primary_path.wal_retention,
        peer_reply_epoch_micros: primary_path.peer_reply_epoch_micros,
        failover_anchor: policy.failover_anchor().name().clone(),
        primary_failover_anchor_confirmed_lsn: failover_anchor.primary_confirmed_lsn,
        synchronized_failover_anchor_confirmed_lsn: failover_anchor.synchronized_confirmed_lsn,
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
) -> Result<StandbyPathEvidence, StandbyReplicationPathCorrelationError> {
    let prerequisites = standby.prerequisites();
    let standby_replay_floor_lsn = prerequisites.checkpoint_lsn();
    if standby_replay_floor_lsn.0 < policy.durable_checkpoint_lsn().0 {
        return Err(
            StandbyReplicationPathCorrelationError::StandbyReplayFloorBehind {
                observed: standby_replay_floor_lsn,
                required: policy.durable_checkpoint_lsn(),
            },
        );
    }
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
    let slot_sync_worker = prerequisites
        .slot_sync_worker()
        .ok_or(StandbyReplicationPathCorrelationError::SlotSyncWorkerMissingBeforeSnapshot)?;
    let post_slot_sync_worker = standby
        .post_slot_sync_worker()
        .ok_or(StandbyReplicationPathCorrelationError::SlotSyncWorkerMissingAfterSnapshot)?;
    if slot_sync_worker.identity() != post_slot_sync_worker.identity() {
        return Err(StandbyReplicationPathCorrelationError::SlotSyncWorkerChangedDuringSnapshot);
    }
    if post_slot_sync_worker.activity() != LocalSlotSyncWorkerActivity::WaitingAfterCycle {
        return Err(
            StandbyReplicationPathCorrelationError::SlotSyncWorkerNotWaitingAfterSnapshot {
                observed: post_slot_sync_worker.activity(),
            },
        );
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
    Ok(StandbyPathEvidence {
        replay_floor_lsn: standby_replay_floor_lsn,
        wal_receiver_pid,
        slot_sync_worker_identity: slot_sync_worker.identity(),
    })
}

struct StandbyPathEvidence {
    replay_floor_lsn: PgLsn,
    wal_receiver_pid: NonZeroU32,
    slot_sync_worker_identity: LocalPostgresBackendIdentity,
}

struct PrimaryPathEvidence {
    walsender_identity: LocalPostgresBackendIdentity,
    persistence: SlotPersistence,
    catalog_xmin: LocalPostgresTransactionId,
    restart_lsn: PgLsn,
    wal_retention: SlotWalRetention,
    peer_reply_epoch_micros: NonZeroU64,
}

struct FailoverAnchorEvidence {
    primary_confirmed_lsn: PgLsn,
    synchronized_confirmed_lsn: PgLsn,
}

fn validate_failover_anchors(
    policy: &StandbyDecoderPolicy,
    standby: &LocalLogicalSlotObservationBatch,
    primary: &LocalPrimaryReplicationObservationBatch,
) -> Result<FailoverAnchorEvidence, StandbyReplicationPathCorrelationError> {
    let primary_confirmed_lsn = validate_failover_anchor(
        policy,
        ObservedFailoverAnchorSide::Primary,
        primary.failover_anchor(),
    )?;
    let synchronized_anchor = standby
        .entries()
        .iter()
        .find(|entry| entry.target() == policy.failover_anchor())
        .and_then(|entry| entry.observation());
    let synchronized_confirmed_lsn = validate_failover_anchor(
        policy,
        ObservedFailoverAnchorSide::SynchronizedStandby,
        synchronized_anchor,
    )?;
    if synchronized_confirmed_lsn.0 > primary_confirmed_lsn.0 {
        return Err(
            StandbyReplicationPathCorrelationError::SynchronizedAnchorAhead {
                synchronized: synchronized_confirmed_lsn,
                primary: primary_confirmed_lsn,
            },
        );
    }
    Ok(FailoverAnchorEvidence {
        primary_confirmed_lsn,
        synchronized_confirmed_lsn,
    })
}

fn validate_failover_anchor(
    policy: &StandbyDecoderPolicy,
    side: ObservedFailoverAnchorSide,
    slot: Option<&LogicalSlotObservation>,
) -> Result<PgLsn, StandbyReplicationPathCorrelationError> {
    let reject = |problem| StandbyReplicationPathCorrelationError::FailoverAnchor { side, problem };
    let slot = slot.ok_or_else(|| reject(ObservedFailoverAnchorProblem::Missing))?;
    if slot.name != *policy.failover_anchor().name() {
        return Err(reject(ObservedFailoverAnchorProblem::NameMismatch));
    }
    if slot.database_oid != policy.expected_source().database_oid() {
        return Err(reject(ObservedFailoverAnchorProblem::DatabaseMismatch));
    }
    if slot.plugin != LogicalSlotPlugin::PgOutput {
        return Err(reject(ObservedFailoverAnchorProblem::WrongPlugin));
    }
    if slot.persistence == SlotPersistence::NonPersistent {
        return Err(reject(ObservedFailoverAnchorProblem::Temporary));
    }
    let flags_match = match side {
        ObservedFailoverAnchorSide::Primary => matches!(
            slot.kind,
            LogicalSlotKind::FailoverAnchor | LogicalSlotKind::SynchronizedFailoverAnchor
        ),
        ObservedFailoverAnchorSide::SynchronizedStandby => {
            slot.kind == LogicalSlotKind::SynchronizedFailoverAnchor
        }
    };
    if !flags_match {
        return Err(reject(ObservedFailoverAnchorProblem::WrongFlags));
    }
    if slot.two_phase != SettingState::Enabled {
        return Err(reject(ObservedFailoverAnchorProblem::TwoPhaseDisabled));
    }
    let expected_two_phase_at = policy.two_phase_policy().failover_anchor_at;
    if slot.two_phase_at != Some(expected_two_phase_at) {
        return Err(reject(
            ObservedFailoverAnchorProblem::TwoPhaseBoundaryMismatch {
                expected: expected_two_phase_at,
                observed: slot.two_phase_at,
            },
        ));
    }
    if side == ObservedFailoverAnchorSide::Primary
        && !matches!(slot.activity, SlotActivity::Inactive)
    {
        return Err(reject(ObservedFailoverAnchorProblem::Active));
    }
    if let Some(reason) = slot.invalidation {
        return Err(reject(ObservedFailoverAnchorProblem::Invalidated(reason)));
    }
    let wal_retention = slot
        .wal_retention
        .ok_or_else(|| reject(ObservedFailoverAnchorProblem::WalRetentionMissing))?;
    if !matches!(
        wal_retention,
        SlotWalRetention::Reserved | SlotWalRetention::Extended
    ) {
        return Err(reject(ObservedFailoverAnchorProblem::WalNotRetained(
            wal_retention,
        )));
    }
    let confirmed_flush_lsn = slot
        .confirmed_flush_lsn
        .filter(|lsn| lsn.0 != 0)
        .ok_or_else(|| reject(ObservedFailoverAnchorProblem::ProgressMissing))?;
    if expected_two_phase_at.0 > confirmed_flush_lsn.0 {
        return Err(reject(
            ObservedFailoverAnchorProblem::TwoPhaseBoundaryAhead {
                two_phase_at: expected_two_phase_at,
                confirmed_flush_lsn,
            },
        ));
    }
    if confirmed_flush_lsn.0 > policy.durable_checkpoint_lsn().0 {
        return Err(reject(ObservedFailoverAnchorProblem::ProgressAhead {
            confirmed_flush_lsn,
            durable_checkpoint_lsn: policy.durable_checkpoint_lsn(),
        }));
    }
    Ok(confirmed_flush_lsn)
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
    let standby_slots_finished = standby.slot_collection_finished_at();
    let standby_post_worker_started = standby.post_worker_collection_started_at();
    let standby_finished = standby.post_worker_collection_finished_at();
    let primary_started = primary.collection_started_at();
    let primary_finished = primary.collection_finished_at();
    if standby_started > standby_prerequisites_finished
        || standby_prerequisites_finished > standby_slots_started
        || standby_slots_started > standby_slots_finished
        || standby_slots_finished > standby_post_worker_started
        || standby_post_worker_started > standby_finished
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

/// Endpoint whose failover-anchor row failed conservative correlation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservedFailoverAnchorSide {
    /// Failover-enabled slot sampled on the writable primary.
    ///
    /// `PostgreSQL` can retain `synced = true` as synchronized-origin metadata
    /// after promoting a standby, while its hot-standby restrictions no longer
    /// apply once the server is writable.
    Primary,
    /// Continuously synchronized promotion copy sampled on the standby.
    SynchronizedStandby,
}

/// Unsafe or incomplete state in one observed failover-anchor row.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ObservedFailoverAnchorProblem {
    /// Exact catalog-selected slot was absent.
    #[error("slot is absent")]
    Missing,
    /// Row name differs from the catalog-selected target.
    #[error("slot name does not match the catalog target")]
    NameMismatch,
    /// Slot belongs to another database.
    #[error("slot database does not match the catalog source")]
    DatabaseMismatch,
    /// Slot does not use the built-in `pgoutput` plugin.
    #[error("slot does not use pgoutput")]
    WrongPlugin,
    /// Slot is known to be temporary.
    #[error("slot is temporary")]
    Temporary,
    /// `failover` or `synced` flags do not match the endpoint role.
    #[error("slot failover or synchronized flags do not match its endpoint")]
    WrongFlags,
    /// Prepared-transaction decoding is disabled.
    #[error("slot does not decode prepared transactions")]
    TwoPhaseDisabled,
    /// Prepared-decoding activation boundary differs from the catalog fence.
    #[error("slot prepared-decoding boundary does not match the catalog fence")]
    TwoPhaseBoundaryMismatch {
        /// Catalog-selected activation boundary.
        expected: PgLsn,
        /// Boundary exposed by `PostgreSQL`.
        observed: Option<PgLsn>,
    },
    /// Primary anchor is already owned by a decoder backend.
    #[error("primary slot is active")]
    Active,
    /// `PostgreSQL` invalidated the slot.
    #[error("slot is invalidated: {0:?}")]
    Invalidated(SlotInvalidation),
    /// `PostgreSQL` omitted the slot's WAL-retention classification.
    #[error("slot WAL-retention state is absent")]
    WalRetentionMissing,
    /// Required slot WAL is no longer guaranteed to be retained.
    #[error("slot WAL is not retained: {0:?}")]
    WalNotRetained(SlotWalRetention),
    /// Slot has no nonzero confirmed consistent point.
    #[error("slot confirmed-flush progress is absent")]
    ProgressMissing,
    /// Slot has not reached the prepared-decoding activation boundary.
    #[error("slot confirmed-flush progress precedes its prepared-decoding boundary")]
    TwoPhaseBoundaryAhead {
        /// Catalog-selected prepared-decoding boundary.
        two_phase_at: PgLsn,
        /// Slot confirmed-flush progress.
        confirmed_flush_lsn: PgLsn,
    },
    /// Slot has advanced beyond the catalog's durable consumer checkpoint.
    #[error("slot confirmed-flush progress is ahead of the durable checkpoint")]
    ProgressAhead {
        /// Slot confirmed-flush progress.
        confirmed_flush_lsn: PgLsn,
        /// Catalog's durable consumer checkpoint.
        durable_checkpoint_lsn: PgLsn,
    },
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
    /// No slot-sync worker was visible before the logical-slot snapshot.
    #[error("standby slot-sync worker is absent before the logical-slot snapshot")]
    SlotSyncWorkerMissingBeforeSnapshot,
    /// No slot-sync worker was visible after the logical-slot snapshot.
    #[error("standby slot-sync worker is absent after the logical-slot snapshot")]
    SlotSyncWorkerMissingAfterSnapshot,
    /// The local slot-sync worker generation changed across the snapshot.
    #[error("standby slot-sync worker changed during the logical-slot snapshot")]
    SlotSyncWorkerChangedDuringSnapshot,
    /// The post-snapshot worker sample did not expose its completed-cycle wait.
    #[error("standby slot-sync worker is not waiting after a completed cycle")]
    SlotSyncWorkerNotWaitingAfterSnapshot {
        /// Raw activity observed after the logical-slot query.
        observed: LocalSlotSyncWorkerActivity,
    },
    /// Standby's conservative control-file replay floor precedes the checkpoint.
    #[error("standby control-file replay floor {observed:?} is behind checkpoint {required:?}")]
    StandbyReplayFloorBehind {
        /// Standby's control-file checkpoint record.
        observed: PgLsn,
        /// Durable consumer checkpoint required by the catalog.
        required: PgLsn,
    },
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
    /// One primary or synchronized failover-anchor row is unusable.
    #[error("{side:?} failover anchor is unusable: {problem}")]
    FailoverAnchor {
        /// Endpoint that exposed the problem.
        side: ObservedFailoverAnchorSide,
        /// Unsafe or incomplete row state.
        problem: ObservedFailoverAnchorProblem,
    },
    /// Separately sampled synchronized progress cannot lead its primary source.
    #[error(
        "synchronized failover-anchor progress {synchronized:?} is ahead of primary progress {primary:?}"
    )]
    SynchronizedAnchorAhead {
        /// Synchronized standby copy's confirmed-flush LSN.
        synchronized: PgLsn,
        /// Original primary anchor's confirmed-flush LSN.
        primary: PgLsn,
    },
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use pgshard_types::CatalogEpoch;
    use uuid::Uuid;

    use super::super::{
        LocalPhysicalReplicationSlotObservation, LocalSlotSyncWorkerObservation,
        LocalStandbyPrerequisiteObservation, LocalWalSenderObservation, LogicalSlotSnapshotEntry,
    };
    use super::*;
    use crate::standby_slots::{
        ManagedSlotTarget, ManagedTwoPhasePolicy, SlotGeneration, SlotOwnership,
        StandbyDecoderEvidenceLimits, StandbyDecoderTarget,
    };

    const SYSTEM_IDENTIFIER: u64 = 7_219_834_723_984_723;
    const TIMELINE: u32 = 7;
    const DATABASE_OID: u32 = 16_384;
    const CHECKPOINT: PgLsn = PgLsn(0x3000);
    const STANDBY_REPLAY_FLOOR: PgLsn = PgLsn(0x4000);
    const RESTART: PgLsn = PgLsn(0x2000);
    const ANCHOR_PROGRESS: PgLsn = PgLsn(CHECKPOINT.0 - 1);

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

    fn slot_sync_worker(
        pid: u32,
        start_epoch_micros: u64,
        activity: LocalSlotSyncWorkerActivity,
    ) -> LocalSlotSyncWorkerObservation {
        LocalSlotSyncWorkerObservation {
            identity: LocalPostgresBackendIdentity {
                pid: nonzero_u32(pid),
                start_epoch_micros: nonzero_u64(start_epoch_micros),
            },
            activity,
        }
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
        let worker = slot_sync_worker(
            301,
            1_700_000_000_123_456,
            LocalSlotSyncWorkerActivity::Running,
        );
        LocalLogicalSlotObservationBatch {
            database_name: "shardschema".to_owned(),
            database_oid: DATABASE_OID,
            prerequisite_collection_started_at: base,
            prerequisite_collection_finished_at: base + Duration::from_millis(1),
            prerequisites: LocalStandbyPrerequisiteObservation {
                system_identifier: SYSTEM_IDENTIFIER,
                checkpoint_lsn: STANDBY_REPLAY_FLOOR,
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
                slot_sync_worker: Some(worker),
            },
            slot_collection_started_at: base + Duration::from_millis(2),
            slot_collection_finished_at: base + Duration::from_millis(3),
            post_worker_collection_started_at: base + Duration::from_millis(4),
            post_worker_collection_finished_at: base + Duration::from_millis(5),
            post_slot_sync_worker: Some(LocalSlotSyncWorkerObservation {
                activity: LocalSlotSyncWorkerActivity::WaitingAfterCycle,
                ..worker
            }),
            entries: vec![
                LogicalSlotSnapshotEntry {
                    target: policy.local_decoder().clone(),
                    observation: None,
                },
                LogicalSlotSnapshotEntry {
                    target: policy.failover_anchor().clone(),
                    observation: Some(anchor_observation(
                        policy,
                        LogicalSlotKind::SynchronizedFailoverAnchor,
                    )),
                },
            ],
        }
    }

    fn anchor_observation(
        policy: &StandbyDecoderPolicy,
        kind: LogicalSlotKind,
    ) -> LogicalSlotObservation {
        LogicalSlotObservation {
            name: policy.failover_anchor().name().clone(),
            database_oid: DATABASE_OID,
            plugin: LogicalSlotPlugin::PgOutput,
            kind,
            persistence: SlotPersistence::Unproven,
            two_phase: SettingState::Enabled,
            two_phase_at: Some(policy.two_phase_policy().failover_anchor_at),
            activity: SlotActivity::Inactive,
            ownership: SlotOwnership::Unknown,
            invalidation: None,
            wal_retention: Some(SlotWalRetention::Reserved),
            confirmed_flush_lsn: Some(ANCHOR_PROGRESS),
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
            collection_started_at: base + Duration::from_millis(6),
            collection_finished_at: base + Duration::from_millis(7),
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
            failover_anchor: Some(anchor_observation(policy, LogicalSlotKind::FailoverAnchor)),
        }
    }

    fn fixture() -> Fixture {
        let policy = policy();
        let base = Instant::now();
        Fixture {
            standby: standby_batch(&policy, base),
            primary: primary_batch(&policy, base),
            policy,
            evaluated_at: base + Duration::from_millis(8),
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

    fn anchor_error(
        side: ObservedFailoverAnchorSide,
        problem: ObservedFailoverAnchorProblem,
    ) -> StandbyReplicationPathCorrelationError {
        StandbyReplicationPathCorrelationError::FailoverAnchor { side, problem }
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

    fn primary_anchor(fixture: &mut Fixture) -> &mut LogicalSlotObservation {
        fixture
            .primary
            .failover_anchor
            .as_mut()
            .expect("fixture primary anchor")
    }

    fn synchronized_anchor(fixture: &mut Fixture) -> &mut LogicalSlotObservation {
        fixture
            .standby
            .entries
            .iter_mut()
            .find(|entry| entry.target == *fixture.policy.failover_anchor())
            .and_then(|entry| entry.observation.as_mut())
            .expect("fixture synchronized anchor")
    }

    fn anchor(
        fixture: &mut Fixture,
        side: ObservedFailoverAnchorSide,
    ) -> &mut LogicalSlotObservation {
        match side {
            ObservedFailoverAnchorSide::Primary => primary_anchor(fixture),
            ObservedFailoverAnchorSide::SynchronizedStandby => synchronized_anchor(fixture),
        }
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
        assert_eq!(proof.oldest_observation_age(), Duration::from_millis(8));
        assert_eq!(proof.standby_replay_floor_lsn(), STANDBY_REPLAY_FLOOR);
        assert_eq!(
            proof.source_bound_replay_floor().source_identity(),
            fixture.policy.expected_source()
        );
        assert_eq!(
            proof.source_bound_replay_floor().lsn(),
            STANDBY_REPLAY_FLOOR
        );
        assert_eq!(proof.physical_slot(), fixture.policy.physical_slot());
        assert_eq!(proof.wal_receiver_pid(), nonzero_u32(401));
        assert_eq!(proof.slot_sync_worker_identity().pid(), nonzero_u32(301));
        assert_eq!(
            proof.slot_sync_worker_identity().start_epoch_micros(),
            nonzero_u64(1_700_000_000_123_456)
        );
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
        assert_eq!(
            proof.failover_anchor(),
            fixture.policy.failover_anchor().name()
        );
        assert_eq!(
            proof.primary_failover_anchor_confirmed_lsn(),
            ANCHOR_PROGRESS
        );
        assert_eq!(
            proof.synchronized_failover_anchor_confirmed_lsn(),
            ANCHOR_PROGRESS
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
    fn standby_replay_floor_must_cover_the_durable_checkpoint() {
        reject(
            |fixture| prerequisites(fixture).checkpoint_lsn = PgLsn(CHECKPOINT.0 - 1),
            StandbyReplicationPathCorrelationError::StandbyReplayFloorBehind {
                observed: PgLsn(CHECKPOINT.0 - 1),
                required: CHECKPOINT,
            },
        );

        let mut fixture = fixture();
        prerequisites(&mut fixture).checkpoint_lsn = CHECKPOINT;
        correlate_standby_replication_path_at(
            &fixture.policy,
            &fixture.standby,
            &fixture.primary,
            fixture.evaluated_at,
        )
        .expect("replay floor exactly at the checkpoint must be eligible");
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
                fixture.standby.post_worker_collection_started_at =
                    fixture.standby.slot_collection_finished_at - Duration::from_millis(1);
            },
            StandbyReplicationPathCorrelationError::ObservationWindowInconsistent,
        );
        reject(
            |fixture| {
                fixture.standby.post_worker_collection_finished_at =
                    fixture.standby.post_worker_collection_started_at - Duration::from_millis(1);
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
    fn rejects_incomplete_or_changed_slot_sync_worker_windows() {
        reject(
            |fixture| prerequisites(fixture).slot_sync_worker = None,
            StandbyReplicationPathCorrelationError::SlotSyncWorkerMissingBeforeSnapshot,
        );
        reject(
            |fixture| fixture.standby.post_slot_sync_worker = None,
            StandbyReplicationPathCorrelationError::SlotSyncWorkerMissingAfterSnapshot,
        );
        for (pid, start_epoch_micros) in
            [(302, 1_700_000_000_123_456), (301, 1_700_000_000_123_457)]
        {
            reject(
                |fixture| {
                    fixture.standby.post_slot_sync_worker = Some(slot_sync_worker(
                        pid,
                        start_epoch_micros,
                        LocalSlotSyncWorkerActivity::WaitingAfterCycle,
                    ));
                },
                StandbyReplicationPathCorrelationError::SlotSyncWorkerChangedDuringSnapshot,
            );
        }
        for activity in [
            LocalSlotSyncWorkerActivity::Running,
            LocalSlotSyncWorkerActivity::OtherWait,
        ] {
            reject(
                |fixture| {
                    fixture
                        .standby
                        .post_slot_sync_worker
                        .as_mut()
                        .expect("fixture post-slot worker")
                        .activity = activity;
                },
                StandbyReplicationPathCorrelationError::SlotSyncWorkerNotWaitingAfterSnapshot {
                    observed: activity,
                },
            );
        }
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

    #[test]
    fn rejects_missing_or_misidentified_failover_anchors() {
        reject(
            |fixture| fixture.primary.failover_anchor = None,
            anchor_error(
                ObservedFailoverAnchorSide::Primary,
                ObservedFailoverAnchorProblem::Missing,
            ),
        );
        reject(
            |fixture| {
                fixture.standby.entries.pop();
            },
            anchor_error(
                ObservedFailoverAnchorSide::SynchronizedStandby,
                ObservedFailoverAnchorProblem::Missing,
            ),
        );
        for side in [
            ObservedFailoverAnchorSide::Primary,
            ObservedFailoverAnchorSide::SynchronizedStandby,
        ] {
            reject(
                |fixture| anchor(fixture, side).name = slot("another_anchor"),
                anchor_error(side, ObservedFailoverAnchorProblem::NameMismatch),
            );
            reject(
                |fixture| anchor(fixture, side).database_oid += 1,
                anchor_error(side, ObservedFailoverAnchorProblem::DatabaseMismatch),
            );
            reject(
                |fixture| anchor(fixture, side).plugin = LogicalSlotPlugin::Other,
                anchor_error(side, ObservedFailoverAnchorProblem::WrongPlugin),
            );
            reject(
                |fixture| anchor(fixture, side).persistence = SlotPersistence::NonPersistent,
                anchor_error(side, ObservedFailoverAnchorProblem::Temporary),
            );
        }
        reject(
            |fixture| {
                primary_anchor(fixture).kind = LogicalSlotKind::StandbyLocalDecoder;
            },
            anchor_error(
                ObservedFailoverAnchorSide::Primary,
                ObservedFailoverAnchorProblem::WrongFlags,
            ),
        );
        reject(
            |fixture| {
                synchronized_anchor(fixture).kind = LogicalSlotKind::FailoverAnchor;
            },
            anchor_error(
                ObservedFailoverAnchorSide::SynchronizedStandby,
                ObservedFailoverAnchorProblem::WrongFlags,
            ),
        );
    }

    #[test]
    fn accepts_a_synchronized_anchor_left_by_primary_promotion() {
        let mut fixture = fixture();
        primary_anchor(&mut fixture).kind = LogicalSlotKind::SynchronizedFailoverAnchor;

        correlate_standby_replication_path_at(
            &fixture.policy,
            &fixture.standby,
            &fixture.primary,
            fixture.evaluated_at,
        )
        .expect("a promoted primary retains origin metadata without standby restrictions");
    }

    #[test]
    fn rejects_unsafe_failover_anchor_state_and_progress() {
        for side in [
            ObservedFailoverAnchorSide::Primary,
            ObservedFailoverAnchorSide::SynchronizedStandby,
        ] {
            reject(
                |fixture| anchor(fixture, side).two_phase = SettingState::Disabled,
                anchor_error(side, ObservedFailoverAnchorProblem::TwoPhaseDisabled),
            );
            reject(
                |fixture| anchor(fixture, side).two_phase_at = None,
                anchor_error(
                    side,
                    ObservedFailoverAnchorProblem::TwoPhaseBoundaryMismatch {
                        expected: PgLsn(CHECKPOINT.0 - 2),
                        observed: None,
                    },
                ),
            );
            reject(
                |fixture| {
                    anchor(fixture, side).invalidation = Some(SlotInvalidation::WalRemoved);
                },
                anchor_error(
                    side,
                    ObservedFailoverAnchorProblem::Invalidated(SlotInvalidation::WalRemoved),
                ),
            );
            reject(
                |fixture| anchor(fixture, side).wal_retention = None,
                anchor_error(side, ObservedFailoverAnchorProblem::WalRetentionMissing),
            );
            reject(
                |fixture| {
                    anchor(fixture, side).wal_retention = Some(SlotWalRetention::Unreserved);
                },
                anchor_error(
                    side,
                    ObservedFailoverAnchorProblem::WalNotRetained(SlotWalRetention::Unreserved),
                ),
            );
            reject(
                |fixture| anchor(fixture, side).confirmed_flush_lsn = Some(PgLsn(0)),
                anchor_error(side, ObservedFailoverAnchorProblem::ProgressMissing),
            );
            reject(
                |fixture| {
                    anchor(fixture, side).confirmed_flush_lsn = Some(PgLsn(CHECKPOINT.0 - 3));
                },
                anchor_error(
                    side,
                    ObservedFailoverAnchorProblem::TwoPhaseBoundaryAhead {
                        two_phase_at: PgLsn(CHECKPOINT.0 - 2),
                        confirmed_flush_lsn: PgLsn(CHECKPOINT.0 - 3),
                    },
                ),
            );
            reject(
                |fixture| {
                    anchor(fixture, side).confirmed_flush_lsn = Some(PgLsn(CHECKPOINT.0 + 1));
                },
                anchor_error(
                    side,
                    ObservedFailoverAnchorProblem::ProgressAhead {
                        confirmed_flush_lsn: PgLsn(CHECKPOINT.0 + 1),
                        durable_checkpoint_lsn: CHECKPOINT,
                    },
                ),
            );
        }
        reject(
            |fixture| primary_anchor(fixture).activity = SlotActivity::Active(nonzero_u32(601)),
            anchor_error(
                ObservedFailoverAnchorSide::Primary,
                ObservedFailoverAnchorProblem::Active,
            ),
        );
    }

    #[test]
    fn synchronized_anchor_may_be_active_but_cannot_lead_primary() {
        let mut fixture = fixture();
        synchronized_anchor(&mut fixture).activity = SlotActivity::Active(nonzero_u32(701));
        correlate_standby_replication_path_at(
            &fixture.policy,
            &fixture.standby,
            &fixture.primary,
            fixture.evaluated_at,
        )
        .expect("slot-sync worker may transiently own the synchronized copy");

        reject(
            |fixture| {
                primary_anchor(fixture).confirmed_flush_lsn = Some(PgLsn(CHECKPOINT.0 - 2));
            },
            StandbyReplicationPathCorrelationError::SynchronizedAnchorAhead {
                synchronized: ANCHOR_PROGRESS,
                primary: PgLsn(CHECKPOINT.0 - 2),
            },
        );
    }
}
