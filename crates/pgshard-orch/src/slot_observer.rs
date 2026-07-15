//! Read-only observation of local `PostgreSQL` 18 logical replication slots
//! and standby-decoder prerequisites.
//!
//! The observer consumes a dedicated database client and its connection driver,
//! then returns one bounded, non-atomic observation batch for an exact, small
//! target set. It never creates, advances,
//! acquires, or drops a slot. Slot names and catalog generations are useful
//! correlation keys, but they do not prove who created a server-side slot, so
//! every observation remains [`SlotOwnership::Unknown`] until a future
//! mutation-history attestor supplies stronger evidence.

mod correlation;

pub use correlation::{
    CorrelatedStandbyReplicationPath, ObservedFailoverAnchorProblem, ObservedFailoverAnchorSide,
    StandbyReplicationPathCorrelationError, correlate_standby_replication_path,
};

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use pgshard_catalog::CatalogOperationTimeout;
use pgshard_types::PgLsn;
use thiserror::Error;
#[cfg(test)]
use tokio::time::timeout;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinError,
    time::{Instant, timeout_at},
};
use tokio_postgres::{Client, Connection, Row};

#[cfg(test)]
use crate::postgres_connection::CONNECTION_CLEANUP_TIMEOUT;
use crate::postgres_connection::{ConnectionTask, ConnectionTaskError};
use crate::standby_slots::{
    FailoverSlotSynchronization, LogicalSlotKind, LogicalSlotObservation, LogicalSlotPlugin,
    LogicalWalLevel, ManagedSlotTarget, RecoveryState, ReplicationSlotName, SettingState,
    SlotActivity, SlotInvalidation, SlotNameError, SlotOwnership, SlotPersistence,
    SlotWalRetention, StandbyDecoderPolicy,
};

const MIN_POSTGRES_VERSION_NUM: i32 = 180_000;
const MAX_OBSERVATION_TARGETS: usize = 3;
const MAX_SYNCHRONIZED_STANDBY_SLOTS_BYTES: i32 = 4096;
const SERVER_STATEMENT_TIMEOUT_HEADROOM: Duration = Duration::from_millis(25);
const PIN_SEARCH_PATH_SQL: &str = "SELECT pg_catalog.set_config('search_path', '', false)";
const SET_STATEMENT_TIMEOUT_SQL: &str =
    "SELECT pg_catalog.set_config('statement_timeout', $1, false)";
const REQUIREMENTS_SQL: &str = "\
    SELECT pg_catalog.current_setting('server_version_num')::pg_catalog.int4, \
           pg_catalog.current_database(), \
           (SELECT oid::pg_catalog.int8 FROM pg_catalog.pg_database \
             WHERE datname OPERATOR(pg_catalog.=) pg_catalog.current_database()), \
           pg_catalog.getdatabaseencoding()";
const OBSERVE_PREREQUISITES_SQL: &str = "\
    SELECT control.system_identifier::pg_catalog.int8, \
           checkpoint_control.timeline_id, \
           pg_catalog.pg_is_in_recovery(), \
           pg_catalog.current_setting('wal_level'), \
           pg_catalog.current_setting('hot_standby_feedback')::pg_catalog.bool, \
           (SELECT setting::pg_catalog.int8 FROM pg_catalog.pg_settings \
             WHERE name OPERATOR(pg_catalog.=) 'wal_receiver_status_interval'), \
           (SELECT unit::pg_catalog.text FROM pg_catalog.pg_settings \
             WHERE name OPERATOR(pg_catalog.=) 'wal_receiver_status_interval'), \
           pg_catalog.current_setting('sync_replication_slots')::pg_catalog.bool, \
           NULLIF(pg_catalog.current_setting('primary_slot_name'), ''), \
           pg_catalog.pg_last_wal_replay_lsn()::pg_catalog.text AS replay_lsn, \
           receiver.pid::pg_catalog.int4, \
           receiver.status::pg_catalog.text, \
           receiver.slot_name::pg_catalog.text, \
           NULLIF(receiver.received_tli, 0)::pg_catalog.int4, \
           pg_catalog.pg_has_role( \
               current_user, 'pg_read_all_stats', 'USAGE' \
           ) AS has_read_all_stats, \
           slotsync.pid::pg_catalog.int4 AS slotsync_pid, \
           pg_catalog.floor( \
               pg_catalog.date_part('epoch', slotsync.backend_start) * 1000000 \
           )::pg_catalog.int8 AS slotsync_backend_start_epoch_micros, \
           slotsync.wait_event_type::pg_catalog.text AS slotsync_wait_event_type, \
           slotsync.wait_event::pg_catalog.text AS slotsync_wait_event, \
           checkpoint_control.checkpoint_lsn::pg_catalog.text AS checkpoint_lsn \
      FROM pg_catalog.pg_control_system() AS control \
     CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint_control \
      LEFT JOIN pg_catalog.pg_stat_get_wal_receiver() AS receiver \
        ON receiver.pid IS NOT NULL \
      LEFT JOIN LATERAL ( \
            SELECT activity.pid, activity.backend_start, \
                   activity.wait_event_type, activity.wait_event \
              FROM pg_catalog.pg_stat_get_activity(NULL) AS activity \
             WHERE activity.backend_type OPERATOR(pg_catalog.=) 'slotsync worker' \
             LIMIT 2 \
      ) AS slotsync ON true";
const OBSERVE_SLOTS_SQL: &str = "\
    SELECT slot_name::pg_catalog.text AS slot_name, \
           plugin::pg_catalog.text AS plugin, slot_type, \
           datoid::pg_catalog.int8 AS database_oid, temporary, active, \
           active_pid::pg_catalog.int8 AS active_pid, wal_status, two_phase, \
           two_phase_at::pg_catalog.text AS two_phase_at, invalidation_reason, \
           failover, synced, confirmed_flush_lsn::pg_catalog.text AS confirmed_flush_lsn \
      FROM pg_catalog.pg_replication_slots \
     WHERE slot_name::pg_catalog.text OPERATOR(pg_catalog.=) \
           ANY($1::pg_catalog.text[]) \
     ORDER BY slot_name";
const OBSERVE_SLOT_SYNC_WORKER_SQL: &str = "\
    SELECT pg_catalog.pg_has_role( \
               current_user, 'pg_read_all_stats', 'USAGE' \
           ) AS has_read_all_stats, \
           slotsync.pid::pg_catalog.int4 AS slotsync_pid, \
           pg_catalog.floor( \
               pg_catalog.date_part('epoch', slotsync.backend_start) * 1000000 \
           )::pg_catalog.int8 AS slotsync_backend_start_epoch_micros, \
           slotsync.wait_event_type::pg_catalog.text AS slotsync_wait_event_type, \
           slotsync.wait_event::pg_catalog.text AS slotsync_wait_event \
      FROM (VALUES (true)) AS singleton(only_row) \
      LEFT JOIN LATERAL ( \
            SELECT activity.pid, activity.backend_start, \
                   activity.wait_event_type, activity.wait_event \
              FROM pg_catalog.pg_stat_get_activity(NULL) AS activity \
             WHERE activity.backend_type OPERATOR(pg_catalog.=) 'slotsync worker' \
             LIMIT 2 \
      ) AS slotsync ON singleton.only_row";
const OBSERVE_PRIMARY_REPLICATION_SQL: &str = "\
    SELECT control.system_identifier::pg_catalog.int8, \
           checkpoint_control.timeline_id, \
           pg_catalog.pg_is_in_recovery(), \
           CASE WHEN NOT pg_catalog.pg_is_in_recovery() \
                THEN pg_catalog.substring( \
                         pg_catalog.pg_walfile_name( \
                             pg_catalog.pg_current_wal_lsn()), 1, 8) \
           END AS current_timeline_hex, \
           pg_catalog.current_setting('wal_level'), \
           pg_catalog.pg_has_role( \
               current_user, 'pg_read_all_stats', 'USAGE' \
           ) AS has_read_all_stats, \
           sync_policy.octets, sync_policy.value, \
           physical.slot_name::pg_catalog.text, \
           physical.plugin::pg_catalog.text, physical.slot_type, \
           physical.datoid::pg_catalog.int8 AS database_oid, \
           physical.temporary, physical.active, \
           physical.active_pid::pg_catalog.int8, \
           physical.catalog_xmin::pg_catalog.text, \
           physical.restart_lsn::pg_catalog.text, physical.wal_status, \
           physical.invalidation_reason, \
           sender.pid::pg_catalog.int4 AS sender_pid, \
           sender.application_name::pg_catalog.text AS sender_application_name, \
           pg_catalog.floor( \
               pg_catalog.date_part('epoch', sender.backend_start) * 1000000 \
           )::pg_catalog.int8 AS sender_backend_start_epoch_micros, \
           sender.state::pg_catalog.text AS sender_state, \
           pg_catalog.floor( \
               pg_catalog.date_part('epoch', sender.reply_time) * 1000000 \
           )::pg_catalog.int8 AS sender_reply_epoch_micros, \
           anchor.slot_name::pg_catalog.text AS anchor_slot_name, \
           anchor.plugin::pg_catalog.text AS anchor_plugin, \
           anchor.slot_type::pg_catalog.text AS anchor_slot_type, \
           anchor.datoid::pg_catalog.int8 AS anchor_database_oid, \
           anchor.temporary AS anchor_temporary, \
           anchor.active AS anchor_active, \
           anchor.active_pid::pg_catalog.int8 AS anchor_active_pid, \
           anchor.wal_status::pg_catalog.text AS anchor_wal_status, \
           anchor.two_phase AS anchor_two_phase, \
           anchor.two_phase_at::pg_catalog.text AS anchor_two_phase_at, \
           anchor.invalidation_reason::pg_catalog.text AS anchor_invalidation_reason, \
           anchor.failover AS anchor_failover, \
           anchor.synced AS anchor_synced, \
           anchor.confirmed_flush_lsn::pg_catalog.text AS anchor_confirmed_flush_lsn \
      FROM pg_catalog.pg_control_system() AS control \
     CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint_control \
     CROSS JOIN LATERAL ( \
           SELECT pg_catalog.octet_length(setting)::pg_catalog.int8 AS octets, \
                  CASE WHEN pg_catalog.octet_length(setting) \
                                  OPERATOR(pg_catalog.<=) $3::pg_catalog.int4 \
                       THEN setting END AS value \
             FROM ( \
                   SELECT pg_catalog.current_setting( \
                              'synchronized_standby_slots') AS setting \
             ) AS raw_policy \
     ) AS sync_policy \
      LEFT JOIN LATERAL ( \
            SELECT slot_name, plugin, slot_type, datoid, temporary, active, \
                   active_pid, catalog_xmin, restart_lsn, wal_status, \
                   invalidation_reason \
              FROM pg_catalog.pg_replication_slots \
             WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name \
             LIMIT 2 \
      ) AS physical ON true \
      LEFT JOIN LATERAL ( \
            SELECT activity.pid, activity.application_name, \
                   activity.backend_start, walsender.state, walsender.reply_time \
              FROM pg_catalog.pg_stat_get_activity( \
                       NULL::pg_catalog.int4) AS activity \
              JOIN pg_catalog.pg_stat_get_wal_senders() AS walsender \
                ON walsender.pid OPERATOR(pg_catalog.=) activity.pid \
             WHERE activity.pid OPERATOR(pg_catalog.=) physical.active_pid \
             LIMIT 2 \
      ) AS sender ON true \
      LEFT JOIN LATERAL ( \
            SELECT slot_name, plugin, slot_type, datoid, temporary, active, \
                   active_pid, wal_status, two_phase, two_phase_at, \
                   invalidation_reason, failover, synced, confirmed_flush_lsn \
              FROM pg_catalog.pg_replication_slots \
             WHERE slot_name OPERATOR(pg_catalog.=) $2::pg_catalog.name \
             LIMIT 2 \
      ) AS anchor ON true";

/// Validated, bounded set of managed slot targets to observe on one server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSlotObservationRequest {
    targets: Vec<ManagedSlotTarget>,
}

impl LogicalSlotObservationRequest {
    /// Creates an exact target set for one local `PostgreSQL` observation.
    ///
    /// # Errors
    ///
    /// Rejects an empty or overlong request, duplicate server-side names, and
    /// reuse of one catalog generation for multiple names.
    pub fn new(targets: Vec<ManagedSlotTarget>) -> Result<Self, SlotObservationRequestError> {
        if targets.is_empty() {
            return Err(SlotObservationRequestError::Empty);
        }
        if targets.len() > MAX_OBSERVATION_TARGETS {
            return Err(SlotObservationRequestError::TooMany {
                received: targets.len(),
                maximum: MAX_OBSERVATION_TARGETS,
            });
        }
        for (index, target) in targets.iter().enumerate() {
            for previous in &targets[..index] {
                if target.name() == previous.name() {
                    return Err(SlotObservationRequestError::DuplicateName(
                        target.name().as_str().to_owned(),
                    ));
                }
                if target.generation() == previous.generation() {
                    return Err(SlotObservationRequestError::DuplicateGeneration);
                }
            }
        }
        Ok(Self { targets })
    }

    /// Returns the exact targets in caller-supplied order.
    #[must_use]
    pub fn targets(&self) -> &[ManagedSlotTarget] {
        &self.targets
    }
}

/// Invalid local logical-slot observation request.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SlotObservationRequestError {
    /// At least one exact target is required.
    #[error("logical slot observation requires at least one target")]
    Empty,
    /// The request exceeded the fixed local snapshot bound.
    #[error("logical slot observation requested {received} targets; maximum is {maximum}")]
    TooMany {
        /// Number of targets supplied by the caller.
        received: usize,
        /// Hard per-snapshot bound.
        maximum: usize,
    },
    /// One server-side name appeared more than once.
    #[error("logical slot observation contains duplicate target name {0:?}")]
    DuplicateName(String),
    /// One never-reused catalog generation was assigned to multiple names.
    #[error("logical slot observation reuses a catalog generation")]
    DuplicateGeneration,
}

/// Exact primary-side physical path and failover anchor to observe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryReplicationObservationRequest {
    physical_slot: ReplicationSlotName,
    failover_anchor: ManagedSlotTarget,
}

impl PrimaryReplicationObservationRequest {
    /// Creates a request for distinct physical and logical slot names.
    ///
    /// # Errors
    ///
    /// Rejects reuse of `PostgreSQL`'s cluster-wide slot namespace.
    pub fn new(
        physical_slot: ReplicationSlotName,
        failover_anchor: ManagedSlotTarget,
    ) -> Result<Self, PrimaryReplicationObservationRequestError> {
        if physical_slot == *failover_anchor.name() {
            return Err(PrimaryReplicationObservationRequestError::SlotNameCollision);
        }
        Ok(Self {
            physical_slot,
            failover_anchor,
        })
    }

    /// Derives both exact primary-side targets from a catalog-fenced policy.
    #[must_use]
    pub fn from_policy(policy: &StandbyDecoderPolicy) -> Self {
        Self {
            physical_slot: policy.physical_slot().clone(),
            failover_anchor: policy.failover_anchor().clone(),
        }
    }

    /// Returns the exact physical slot expected to own the walsender.
    #[must_use]
    pub const fn physical_slot(&self) -> &ReplicationSlotName {
        &self.physical_slot
    }

    /// Returns the primary failover anchor expected to synchronize downstream.
    #[must_use]
    pub const fn failover_anchor(&self) -> &ManagedSlotTarget {
        &self.failover_anchor
    }
}

/// Invalid primary replication observation target set.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PrimaryReplicationObservationRequestError {
    /// `PostgreSQL`'s cluster-wide replication-slot namespace was reused.
    #[error("primary physical slot and failover anchor names must be distinct")]
    SlotNameCollision,
}

/// One requested target and its optional server-side observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSlotSnapshotEntry {
    target: ManagedSlotTarget,
    observation: Option<LogicalSlotObservation>,
}

/// Raw local WAL-receiver activity, before any upstream identity correlation.
///
/// `Streaming` here means only that `PostgreSQL`'s local receiver reports that
/// state. It does not prove that the receiver is connected to the catalog's
/// expected primary; the later multi-server observer must establish that
/// separately before constructing an eligibility
/// [`WalReceiverState`](crate::standby_slots::WalReceiverState).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalWalReceiverActivity {
    /// No displayable local WAL receiver exists.
    Absent,
    /// `PostgreSQL` reports the receiver stopped.
    Stopped,
    /// `PostgreSQL` reports the receiver starting.
    Starting,
    /// `PostgreSQL` reports the receiver actively streaming from an uncorrelated source.
    Streaming,
    /// `PostgreSQL` reports the receiver waiting for more WAL.
    Waiting,
    /// `PostgreSQL` reports the receiver restarting its connection.
    Restarting,
    /// `PostgreSQL` reports the receiver stopping.
    Stopping,
}

/// Stable identity of one local `PostgreSQL` backend process.
///
/// The start timestamp comes from the server's wall clock and is used only for
/// equality across observations. It is never treated as a freshness clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalPostgresBackendIdentity {
    pid: NonZeroU32,
    start_epoch_micros: NonZeroU64,
}

/// Raw `PostgreSQL` transaction ID without cross-server ordering semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalPostgresTransactionId(NonZeroU32);

impl LocalPostgresTransactionId {
    /// Returns the server's 32-bit transaction ID.
    ///
    /// This value cannot be ordered across wraparound or across independently
    /// sampled servers without additional epoch evidence.
    #[must_use]
    pub const fn get(self) -> NonZeroU32 {
        self.0
    }
}

/// Raw activity state of the physical walsender owning a managed slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalWalSenderActivity {
    /// Walsender is starting up.
    Startup,
    /// Walsender is sending retained WAL to catch the standby up.
    Catchup,
    /// Walsender reports active streaming.
    Streaming,
    /// Walsender is serving a base backup.
    Backup,
    /// Walsender is stopping.
    Stopping,
}

/// One local primary-side physical slot observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalPhysicalReplicationSlotObservation {
    name: ReplicationSlotName,
    persistence: SlotPersistence,
    activity: SlotActivity,
    catalog_xmin: Option<LocalPostgresTransactionId>,
    restart_lsn: Option<PgLsn>,
    wal_retention: Option<SlotWalRetention>,
    invalidation: Option<SlotInvalidation>,
}

impl LocalPhysicalReplicationSlotObservation {
    /// Returns the exact server-side physical slot name.
    #[must_use]
    pub const fn name(&self) -> &ReplicationSlotName {
        &self.name
    }

    /// Returns conservative persistence evidence from `PostgreSQL`'s public view.
    #[must_use]
    pub const fn persistence(&self) -> SlotPersistence {
        self.persistence
    }

    /// Returns whether a backend currently owns the slot.
    #[must_use]
    pub const fn activity(&self) -> SlotActivity {
        self.activity
    }

    /// Returns the raw catalog horizon carried by hot-standby feedback.
    #[must_use]
    pub const fn catalog_xmin(&self) -> Option<LocalPostgresTransactionId> {
        self.catalog_xmin
    }

    /// Returns the oldest retained WAL position reported for the slot.
    #[must_use]
    pub const fn restart_lsn(&self) -> Option<PgLsn> {
        self.restart_lsn
    }

    /// Returns `PostgreSQL`'s current WAL-retention classification.
    #[must_use]
    pub const fn wal_retention(&self) -> Option<SlotWalRetention> {
        self.wal_retention
    }

    /// Returns `PostgreSQL`'s invalidation reason, if any.
    #[must_use]
    pub const fn invalidation(&self) -> Option<SlotInvalidation> {
        self.invalidation
    }
}

/// One primary-side walsender joined to a physical slot's active PID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalWalSenderObservation {
    identity: LocalPostgresBackendIdentity,
    application_name: ReplicationSlotName,
    activity: LocalWalSenderActivity,
    reply_epoch_micros: Option<NonZeroU64>,
}

impl LocalWalSenderObservation {
    /// Returns the primary-local backend identity for equality checks.
    #[must_use]
    pub const fn identity(&self) -> LocalPostgresBackendIdentity {
        self.identity
    }

    /// Returns the bounded, replication-slot-shaped application name.
    #[must_use]
    pub const fn application_name(&self) -> &ReplicationSlotName {
        &self.application_name
    }

    /// Returns the raw physical walsender activity.
    #[must_use]
    pub const fn activity(&self) -> LocalWalSenderActivity {
        self.activity
    }

    /// Returns the timestamp the standby embedded in its latest reply.
    ///
    /// The value comes from the peer's wall clock and is an equality/change
    /// token only. It is not a primary receive time or monotonic age proof and
    /// does not distinguish a status reply from hot-standby feedback.
    #[must_use]
    pub const fn reply_epoch_micros(&self) -> Option<NonZeroU64> {
        self.reply_epoch_micros
    }
}

impl LocalPostgresBackendIdentity {
    pub(crate) const fn from_parts(pid: NonZeroU32, start_epoch_micros: NonZeroU64) -> Self {
        Self {
            pid,
            start_epoch_micros,
        }
    }

    /// Returns the local process identifier.
    #[must_use]
    pub const fn pid(self) -> NonZeroU32 {
        self.pid
    }

    /// Returns the positive server-wall-clock start value in Unix microseconds.
    #[must_use]
    pub const fn start_epoch_micros(self) -> NonZeroU64 {
        self.start_epoch_micros
    }
}

/// Raw local state of `PostgreSQL` 18's continuous slot-sync worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalSlotSyncWorkerActivity {
    /// The worker is executing or is not currently waiting on a named event.
    Running,
    /// The worker completed a cycle and is in `ReplicationSlotsyncMain` wait.
    WaitingAfterCycle,
    /// The worker is blocked on another coherent `PostgreSQL` wait event.
    OtherWait,
}

/// One local slot-sync worker observation before upstream correlation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalSlotSyncWorkerObservation {
    identity: LocalPostgresBackendIdentity,
    activity: LocalSlotSyncWorkerActivity,
}

impl LocalSlotSyncWorkerObservation {
    /// Returns the PID plus backend-start identity of this worker generation.
    #[must_use]
    pub const fn identity(self) -> LocalPostgresBackendIdentity {
        self.identity
    }

    /// Returns the raw local worker activity.
    #[must_use]
    pub const fn activity(self) -> LocalSlotSyncWorkerActivity {
        self.activity
    }
}

/// One local server's `PostgreSQL` 18 state needed by standby-first decoding.
///
/// This is observation only. In particular, an enabled slot-sync setting is
/// not evidence that the background worker completed a cycle, and the latest
/// checkpoint position and timeline can conservatively lag recovery until
/// `PostgreSQL` records a later restartpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalStandbyPrerequisiteObservation {
    system_identifier: u64,
    checkpoint_lsn: PgLsn,
    checkpoint_timeline: u32,
    recovery: RecoveryState,
    wal_level: LogicalWalLevel,
    hot_standby_feedback: SettingState,
    wal_receiver_status_interval: Duration,
    sync_replication_slots: SettingState,
    primary_slot_name: Option<ReplicationSlotName>,
    replay_lsn: Option<PgLsn>,
    wal_receiver_pid: Option<NonZeroU32>,
    wal_receiver_activity: LocalWalReceiverActivity,
    wal_receiver_slot_name: Option<ReplicationSlotName>,
    wal_receiver_received_timeline: Option<u32>,
    slot_sync_worker: Option<LocalSlotSyncWorkerObservation>,
}

impl LocalStandbyPrerequisiteObservation {
    /// Returns the unsigned `PostgreSQL` cluster system identifier.
    #[must_use]
    pub const fn system_identifier(&self) -> u64 {
        self.system_identifier
    }

    /// Returns the control-file checkpoint WAL record.
    ///
    /// This value and `checkpoint_timeline` come from one CRC-checked
    /// `pg_control` snapshot. On a queryable standby it is a conservative
    /// replay floor. It may be inherited from the base backup; later advances
    /// are recorded after the restartpoint flush phase, when `PostgreSQL` installs
    /// the safe checkpoint pair.
    #[must_use]
    pub const fn checkpoint_lsn(&self) -> PgLsn {
        self.checkpoint_lsn
    }

    /// Returns the timeline stored in `PostgreSQL`'s latest control-file checkpoint.
    #[must_use]
    pub const fn checkpoint_timeline(&self) -> u32 {
        self.checkpoint_timeline
    }

    /// Returns whether the observed server is in recovery.
    #[must_use]
    pub const fn recovery(&self) -> RecoveryState {
        self.recovery
    }

    /// Returns the effective logical-decoding WAL level.
    #[must_use]
    pub const fn wal_level(&self) -> LogicalWalLevel {
        self.wal_level
    }

    /// Returns the effective hot-standby-feedback setting.
    #[must_use]
    pub const fn hot_standby_feedback(&self) -> SettingState {
        self.hot_standby_feedback
    }

    /// Returns `PostgreSQL`'s effective WAL-receiver feedback interval.
    #[must_use]
    pub const fn wal_receiver_status_interval(&self) -> Duration {
        self.wal_receiver_status_interval
    }

    /// Returns whether continuous logical failover-slot synchronization is enabled.
    #[must_use]
    pub const fn sync_replication_slots(&self) -> SettingState {
        self.sync_replication_slots
    }

    /// Returns the configured physical upstream slot, if any.
    #[must_use]
    pub const fn primary_slot_name(&self) -> Option<&ReplicationSlotName> {
        self.primary_slot_name.as_ref()
    }

    /// Returns the raw last replayed WAL location, if `PostgreSQL` exposes one.
    ///
    /// SQL does not return its replay timeline atomically, so this value cannot
    /// construct a `SourceBoundReplayFloor` or authorize LSN ordering.
    #[must_use]
    pub const fn replay_lsn(&self) -> Option<PgLsn> {
        self.replay_lsn
    }

    /// Returns the displayable local WAL receiver's process identifier, if any.
    #[must_use]
    pub const fn wal_receiver_pid(&self) -> Option<NonZeroU32> {
        self.wal_receiver_pid
    }

    /// Returns raw local receiver activity without claiming upstream identity.
    #[must_use]
    pub const fn wal_receiver_activity(&self) -> LocalWalReceiverActivity {
        self.wal_receiver_activity
    }

    /// Returns the live WAL receiver's physical slot, if reported.
    #[must_use]
    pub const fn wal_receiver_slot_name(&self) -> Option<&ReplicationSlotName> {
        self.wal_receiver_slot_name.as_ref()
    }

    /// Returns the live receiver's last received timeline, if available.
    ///
    /// Neither this value nor the control-file checkpoint timeline binds the
    /// raw replay LSN to a lineage. The coherent checkpoint LSN and timeline
    /// instead supply a separate conservative replay floor after source
    /// correlation.
    #[must_use]
    pub const fn wal_receiver_received_timeline(&self) -> Option<u32> {
        self.wal_receiver_received_timeline
    }

    /// Returns the local continuous slot-sync worker, if one is observable.
    ///
    /// A waiting worker proves only that one local cycle returned. Upstream
    /// connection identity and exact synchronized slot state still require a
    /// later multi-server correlation step.
    #[must_use]
    pub const fn slot_sync_worker(&self) -> Option<LocalSlotSyncWorkerObservation> {
        self.slot_sync_worker
    }
}

impl LogicalSlotSnapshotEntry {
    /// Returns the exact catalog target requested by the caller.
    #[must_use]
    pub const fn target(&self) -> &ManagedSlotTarget {
        &self.target
    }

    /// Returns the local server row, or `None` when the slot was absent.
    #[must_use]
    pub const fn observation(&self) -> Option<&LogicalSlotObservation> {
        self.observation.as_ref()
    }
}

/// One bounded, non-atomic local `PostgreSQL` logical-slot observation batch.
///
/// `pg_replication_slots` copies different slots while holding their individual
/// mutexes, not under one cross-slot lock. Separate monotonic intervals bracket
/// the prerequisite, slot, and post-slot worker queries. Matching worker
/// identities prove only that one local process generation surrounded the slot
/// query; entries are not a point-in-time snapshot. A future
/// mutating reconciler must collect a fresh batch after exclusive acquisition
/// and recheck every invariant before authorizing use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalLogicalSlotObservationBatch {
    database_name: String,
    database_oid: u32,
    prerequisite_collection_started_at: Instant,
    prerequisite_collection_finished_at: Instant,
    prerequisites: LocalStandbyPrerequisiteObservation,
    slot_collection_started_at: Instant,
    slot_collection_finished_at: Instant,
    post_worker_collection_started_at: Instant,
    post_worker_collection_finished_at: Instant,
    post_slot_sync_worker: Option<LocalSlotSyncWorkerObservation>,
    entries: Vec<LogicalSlotSnapshotEntry>,
}

/// One bounded, non-authorizing primary-side replication sample.
///
/// `PostgreSQL` does not expose a monotonic timestamp for hot-standby feedback,
/// and this query is not atomic with a standby-side sample. The returned data
/// can prove local PID joins, configuration membership, and raw failover-anchor
/// state, but a later
/// multi-server correlator must establish source identity, freshness, catalog
/// horizon coverage, and lifecycle ownership before decoder attachment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalPrimaryReplicationObservationBatch {
    database_name: String,
    database_oid: u32,
    collection_started_at: Instant,
    collection_finished_at: Instant,
    system_identifier: u64,
    checkpoint_timeline: u32,
    current_timeline: Option<u32>,
    recovery: RecoveryState,
    wal_level: LogicalWalLevel,
    failover_slot_synchronization: FailoverSlotSynchronization,
    physical_slot: Option<LocalPhysicalReplicationSlotObservation>,
    wal_sender: Option<LocalWalSenderObservation>,
    failover_anchor: Option<LogicalSlotObservation>,
}

impl LocalPrimaryReplicationObservationBatch {
    /// Returns the database used for this primary-local sample.
    #[must_use]
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Returns that database's live `PostgreSQL` OID.
    #[must_use]
    pub const fn database_oid(&self) -> u32 {
        self.database_oid
    }

    /// Returns the local monotonic instant immediately before collection.
    #[must_use]
    pub const fn collection_started_at(&self) -> Instant {
        self.collection_started_at
    }

    /// Returns the local monotonic instant immediately after collection.
    #[must_use]
    pub const fn collection_finished_at(&self) -> Instant {
        self.collection_finished_at
    }

    /// Returns the unsigned `PostgreSQL` cluster system identifier.
    #[must_use]
    pub const fn system_identifier(&self) -> u64 {
        self.system_identifier
    }

    /// Returns the timeline stored in `PostgreSQL`'s latest control checkpoint.
    #[must_use]
    pub const fn checkpoint_timeline(&self) -> u32 {
        self.checkpoint_timeline
    }

    /// Returns the writable server's current WAL insertion timeline.
    ///
    /// This is `None` only when the observed server is in recovery. A writable
    /// observation fails closed instead of returning without this value.
    #[must_use]
    pub const fn current_timeline(&self) -> Option<u32> {
        self.current_timeline
    }

    /// Returns whether the sampled upstream is writable or in recovery.
    #[must_use]
    pub const fn recovery(&self) -> RecoveryState {
        self.recovery
    }

    /// Returns the upstream's effective WAL level.
    #[must_use]
    pub const fn wal_level(&self) -> LogicalWalLevel {
        self.wal_level
    }

    /// Returns whether the bounded plain configured list contains the slot.
    #[must_use]
    pub const fn failover_slot_synchronization(&self) -> FailoverSlotSynchronization {
        self.failover_slot_synchronization
    }

    /// Returns the exact primary-local physical slot row, if present.
    #[must_use]
    pub const fn physical_slot(&self) -> Option<&LocalPhysicalReplicationSlotObservation> {
        self.physical_slot.as_ref()
    }

    /// Returns a walsender joined to the slot's active PID, if present.
    #[must_use]
    pub const fn wal_sender(&self) -> Option<&LocalWalSenderObservation> {
        self.wal_sender.as_ref()
    }

    /// Returns the exact primary failover-anchor row, if present.
    ///
    /// The generation remains catalog-selected rather than server-attested.
    #[must_use]
    pub const fn failover_anchor(&self) -> Option<&LogicalSlotObservation> {
        self.failover_anchor.as_ref()
    }
}

impl LocalLogicalSlotObservationBatch {
    /// Returns the database on the consumed observation connection.
    #[must_use]
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Returns that database's live `PostgreSQL` OID.
    #[must_use]
    pub const fn database_oid(&self) -> u32 {
        self.database_oid
    }

    /// Returns the local monotonic instant before prerequisite collection.
    #[must_use]
    pub const fn prerequisite_collection_started_at(&self) -> Instant {
        self.prerequisite_collection_started_at
    }

    /// Returns the local monotonic instant after prerequisite collection.
    #[must_use]
    pub const fn prerequisite_collection_finished_at(&self) -> Instant {
        self.prerequisite_collection_finished_at
    }

    /// Returns the local standby-decoder prerequisites observed before the slots.
    #[must_use]
    pub const fn prerequisites(&self) -> &LocalStandbyPrerequisiteObservation {
        &self.prerequisites
    }

    /// Returns the local monotonic instant immediately before the slot query.
    #[must_use]
    pub const fn slot_collection_started_at(&self) -> Instant {
        self.slot_collection_started_at
    }

    /// Returns the local monotonic instant immediately after the slot query.
    #[must_use]
    pub const fn slot_collection_finished_at(&self) -> Instant {
        self.slot_collection_finished_at
    }

    /// Returns the local monotonic instant before the post-slot worker query.
    #[must_use]
    pub const fn post_worker_collection_started_at(&self) -> Instant {
        self.post_worker_collection_started_at
    }

    /// Returns the local monotonic instant after the post-slot worker query.
    #[must_use]
    pub const fn post_worker_collection_finished_at(&self) -> Instant {
        self.post_worker_collection_finished_at
    }

    /// Returns the slot-sync worker observed immediately after the slot query.
    ///
    /// When either side reports a worker, the observer accepts the batch only
    /// if both samples carry the same PID and backend-start identity. The
    /// server-wall-clock start value is an equality token, not a freshness
    /// timestamp.
    #[must_use]
    pub const fn post_slot_sync_worker(&self) -> Option<LocalSlotSyncWorkerObservation> {
        self.post_slot_sync_worker
    }

    /// Returns entries in exact request order, including missing slots.
    #[must_use]
    pub fn entries(&self) -> &[LogicalSlotSnapshotEntry] {
        &self.entries
    }
}

/// Observes an exact local slot set using a consumed, dedicated connection.
///
/// The matching client and connection driver are consumed together. An elapsed
/// absolute deadline or any observation failure aborts and boundedly reaps the
/// driver, so neither a client nor a pending socket can be reused in unknown
/// protocol state. Connection establishment is outside this function and must
/// be bounded separately by its caller.
///
/// # Errors
///
/// Returns an error on timeout, SQL or typed-row failure, `PostgreSQL` older than
/// 18, non-UTF8 encoding, missing statistics privilege, malformed built-in
/// state, a slot-sync worker generation change, a physical slot occupying a
/// requested logical name, or a result outside the exact request set.
pub async fn observe_local_logical_slots<S, T>(
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    request: &LogicalSlotObservationRequest,
) -> Result<LocalLogicalSlotObservationBatch, LocalSlotObservationError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let duration = operation_timeout.get();
    let deadline = Instant::now() + duration;
    let connection_task = ConnectionTask::new(tokio::spawn(connection));
    match timeout_at(deadline, observe_before(client, request, deadline)).await {
        Ok(Ok(batch)) => connection_task.finish(batch).await.map_err(Into::into),
        Ok(Err(error)) => {
            connection_task.abort_and_wait().await;
            Err(error)
        }
        Err(_) => {
            connection_task.abort_and_wait().await;
            Err(LocalSlotObservationError::OperationTimeout { duration })
        }
    }
}

/// Observes one primary-local physical slot and its exact active walsender.
///
/// The matching client and connection driver are consumed together under one
/// absolute deadline. The query pins built-in names, bounds the configured
/// synchronized-slot policy before returning it, and joins the physical slot's
/// `active_pid` directly to `pg_stat_get_wal_senders()`. It never creates,
/// advances, acquires, or drops a slot.
///
/// # Errors
///
/// Returns an error on timeout, connection failure, `PostgreSQL` older than 18,
/// non-UTF8 encoding, missing effective statistics privilege, unsupported or
/// overlong synchronized-slot policy, or internally inconsistent server state.
pub async fn observe_local_primary_replication<S, T>(
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    request: &PrimaryReplicationObservationRequest,
) -> Result<LocalPrimaryReplicationObservationBatch, LocalSlotObservationError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let duration = operation_timeout.get();
    let deadline = Instant::now() + duration;
    let connection_task = ConnectionTask::new(tokio::spawn(connection));
    match timeout_at(deadline, observe_primary_before(client, request, deadline)).await {
        Ok(Ok(batch)) => connection_task.finish(batch).await.map_err(Into::into),
        Ok(Err(error)) => {
            connection_task.abort_and_wait().await;
            Err(error)
        }
        Err(_) => {
            connection_task.abort_and_wait().await;
            Err(LocalSlotObservationError::OperationTimeout { duration })
        }
    }
}

async fn observe_primary_before(
    client: Client,
    request: &PrimaryReplicationObservationRequest,
    deadline: Instant,
) -> Result<LocalPrimaryReplicationObservationBatch, LocalSlotObservationError> {
    client.batch_execute("DISCARD ALL").await?;
    client.query_one(PIN_SEARCH_PATH_SQL, &[]).await?;
    set_statement_timeout(&client, deadline).await?;
    let requirements = client.query_one(REQUIREMENTS_SQL, &[]).await?;
    let version: i32 = requirements.try_get(0)?;
    let database_name: String = requirements.try_get(1)?;
    let database_oid = positive_u32(requirements.try_get::<_, i64>(2)?, "database_oid")?;
    let encoding: String = requirements.try_get(3)?;
    if version < MIN_POSTGRES_VERSION_NUM {
        return Err(LocalSlotObservationError::UnsupportedPostgresVersion(
            version,
        ));
    }
    if encoding != "UTF8" {
        return Err(LocalSlotObservationError::WrongEncoding(encoding));
    }

    set_statement_timeout(&client, deadline).await?;
    let collection_started_at = Instant::now();
    let row = client
        .query_one(
            OBSERVE_PRIMARY_REPLICATION_SQL,
            &[
                &request.physical_slot().as_str(),
                &request.failover_anchor().name().as_str(),
                &MAX_SYNCHRONIZED_STANDBY_SLOTS_BYTES,
            ],
        )
        .await?;
    let collection_finished_at = Instant::now();

    let system_identifier = parse_system_identifier(row.try_get(0)?)?;
    let checkpoint_timeline = parse_timeline_id(row.try_get(1)?)?;
    let recovery = if row.try_get(2)? {
        RecoveryState::Standby
    } else {
        RecoveryState::Writable
    };
    let current_timeline = parse_primary_current_timeline(row.try_get(3)?, recovery)?;
    let wal_level = match row.try_get::<_, String>(4)?.as_str() {
        "logical" => LogicalWalLevel::Logical,
        _ => LogicalWalLevel::Insufficient,
    };
    if !row.try_get::<_, bool>(5)? {
        return Err(LocalSlotObservationError::StatisticsPrivilegeRequired);
    }
    let failover_slot_synchronization =
        parse_synchronized_slot_policy(row.try_get(6)?, row.try_get(7)?, request.physical_slot())?;
    let physical_slot = parse_physical_slot(&row, request.physical_slot())?;
    let wal_sender = parse_wal_sender(&row, physical_slot.as_ref())?;
    let failover_anchor = parse_primary_failover_anchor(&row, request.failover_anchor())?;

    Ok(LocalPrimaryReplicationObservationBatch {
        database_name,
        database_oid,
        collection_started_at,
        collection_finished_at,
        system_identifier,
        checkpoint_timeline,
        current_timeline,
        recovery,
        wal_level,
        failover_slot_synchronization,
        physical_slot,
        wal_sender,
        failover_anchor,
    })
}

async fn observe_before(
    client: Client,
    request: &LogicalSlotObservationRequest,
    deadline: Instant,
) -> Result<LocalLogicalSlotObservationBatch, LocalSlotObservationError> {
    client.batch_execute("DISCARD ALL").await?;
    client.query_one(PIN_SEARCH_PATH_SQL, &[]).await?;
    set_statement_timeout(&client, deadline).await?;
    let prerequisite_collection_started_at = Instant::now();
    let requirements = client.query_one(REQUIREMENTS_SQL, &[]).await?;
    let version: i32 = requirements.try_get(0)?;
    let database_name: String = requirements.try_get(1)?;
    let database_oid = positive_u32(requirements.try_get::<_, i64>(2)?, "database_oid")?;
    let encoding: String = requirements.try_get(3)?;
    if version < MIN_POSTGRES_VERSION_NUM {
        return Err(LocalSlotObservationError::UnsupportedPostgresVersion(
            version,
        ));
    }
    if encoding != "UTF8" {
        return Err(LocalSlotObservationError::WrongEncoding(encoding));
    }
    set_statement_timeout(&client, deadline).await?;
    let prerequisite_row = client.query_one(OBSERVE_PREREQUISITES_SQL, &[]).await?;
    let prerequisite_collection_finished_at = Instant::now();
    let prerequisites = parse_prerequisites(&prerequisite_row)?;

    let names: Vec<String> = request
        .targets
        .iter()
        .map(|target| target.name().as_str().to_owned())
        .collect();
    set_statement_timeout(&client, deadline).await?;
    let slot_collection_started_at = Instant::now();
    let rows = client.query(OBSERVE_SLOTS_SQL, &[&names]).await?;
    let slot_collection_finished_at = Instant::now();
    set_statement_timeout(&client, deadline).await?;
    let post_worker_collection_started_at = Instant::now();
    let post_worker_row = client.query_one(OBSERVE_SLOT_SYNC_WORKER_SQL, &[]).await?;
    let post_worker_collection_finished_at = Instant::now();
    let post_slot_sync_worker = parse_slot_sync_worker_row(&post_worker_row)?;
    validate_slot_sync_worker_window(prerequisites.slot_sync_worker(), post_slot_sync_worker)?;
    let mut observations = vec![None; request.targets.len()];
    for row in rows {
        let name: String = row.try_get("slot_name")?;
        let Some(index) = request
            .targets
            .iter()
            .position(|target| target.name().as_str() == name)
        else {
            return Err(LocalSlotObservationError::UnexpectedSlot(name));
        };
        if observations[index].is_some() {
            return Err(LocalSlotObservationError::DuplicateSlot(name));
        }
        observations[index] = Some(parse_logical_slot(&row)?);
    }

    let entries = request
        .targets
        .iter()
        .cloned()
        .zip(observations)
        .map(|(target, observation)| LogicalSlotSnapshotEntry {
            target,
            observation,
        })
        .collect();
    Ok(LocalLogicalSlotObservationBatch {
        database_name,
        database_oid,
        prerequisite_collection_started_at,
        prerequisite_collection_finished_at,
        prerequisites,
        slot_collection_started_at,
        slot_collection_finished_at,
        post_worker_collection_started_at,
        post_worker_collection_finished_at,
        post_slot_sync_worker,
        entries,
    })
}

fn parse_synchronized_slot_policy(
    octets: i64,
    value: Option<String>,
    target: &ReplicationSlotName,
) -> Result<FailoverSlotSynchronization, LocalSlotObservationError> {
    let maximum = i64::from(MAX_SYNCHRONIZED_STANDBY_SLOTS_BYTES);
    if octets < 0 {
        return Err(LocalSlotObservationError::InvalidNonnegativeInteger {
            field: "synchronized_standby_slots_octets",
            value: octets,
        });
    }
    if octets > maximum {
        return Err(LocalSlotObservationError::SynchronizedStandbySlotsTooLong {
            observed: octets,
            maximum,
        });
    }
    let value = value.ok_or(LocalSlotObservationError::InconsistentSynchronizedStandbySlots)?;
    if i64::try_from(value.len()).ok() != Some(octets) {
        return Err(LocalSlotObservationError::InconsistentSynchronizedStandbySlots);
    }
    if value.is_empty() {
        return Ok(FailoverSlotSynchronization::NotGated);
    }

    let mut parsed = Vec::new();
    for raw_name in value.split(',') {
        let name = ReplicationSlotName::new(raw_name)
            .map_err(|_| LocalSlotObservationError::UnsupportedSynchronizedStandbySlotsList)?;
        if parsed.contains(&name) {
            return Err(LocalSlotObservationError::UnsupportedSynchronizedStandbySlotsList);
        }
        parsed.push(name);
    }
    if parsed.contains(target) {
        Ok(FailoverSlotSynchronization::GatedOnPhysicalSlot)
    } else {
        Ok(FailoverSlotSynchronization::NotGated)
    }
}

fn parse_physical_slot(
    row: &Row,
    target: &ReplicationSlotName,
) -> Result<Option<LocalPhysicalReplicationSlotObservation>, LocalSlotObservationError> {
    let name: Option<String> = row.try_get(8)?;
    let plugin: Option<String> = row.try_get(9)?;
    let slot_type: Option<String> = row.try_get(10)?;
    let database_oid: Option<i64> = row.try_get(11)?;
    let temporary: Option<bool> = row.try_get(12)?;
    let active: Option<bool> = row.try_get(13)?;
    let active_pid: Option<i64> = row.try_get(14)?;
    let catalog_xmin: Option<String> = row.try_get(15)?;
    let restart_lsn: Option<String> = row.try_get(16)?;
    let wal_status: Option<String> = row.try_get(17)?;
    let invalidation_reason: Option<String> = row.try_get(18)?;

    let Some(name) = name else {
        if plugin.is_some()
            || slot_type.is_some()
            || database_oid.is_some()
            || temporary.is_some()
            || active.is_some()
            || active_pid.is_some()
            || catalog_xmin.is_some()
            || restart_lsn.is_some()
            || wal_status.is_some()
            || invalidation_reason.is_some()
        {
            return Err(LocalSlotObservationError::InconsistentPhysicalSlot);
        }
        return Ok(None);
    };
    let parsed_name = ReplicationSlotName::new(name.clone())?;
    if parsed_name != *target {
        return Err(LocalSlotObservationError::UnexpectedSlot(name));
    }
    if slot_type.as_deref() != Some("physical") || plugin.is_some() || database_oid.is_some() {
        return Err(LocalSlotObservationError::PhysicalSlotNameCollision(name));
    }
    let temporary = temporary.ok_or(LocalSlotObservationError::InconsistentPhysicalSlot)?;
    let active = active.ok_or(LocalSlotObservationError::InconsistentPhysicalSlot)?;
    let activity = parse_slot_activity(&name, active, active_pid)?;

    Ok(Some(LocalPhysicalReplicationSlotObservation {
        name: parsed_name,
        persistence: classify_persistence(temporary),
        activity,
        catalog_xmin: catalog_xmin
            .map(|value| parse_transaction_id(&value))
            .transpose()?,
        restart_lsn: restart_lsn
            .map(|value| {
                parse_lsn(&value).ok_or(LocalSlotObservationError::InvalidLsn {
                    field: "physical_restart_lsn",
                    value,
                })
            })
            .transpose()?,
        wal_retention: parse_wal_retention(wal_status)?,
        invalidation: parse_invalidation(invalidation_reason)?,
    }))
}

fn parse_wal_sender(
    row: &Row,
    physical_slot: Option<&LocalPhysicalReplicationSlotObservation>,
) -> Result<Option<LocalWalSenderObservation>, LocalSlotObservationError> {
    let pid: Option<i32> = row.try_get(19)?;
    let application_name: Option<String> = row.try_get(20)?;
    let backend_start_epoch_micros: Option<i64> = row.try_get(21)?;
    let state: Option<String> = row.try_get(22)?;
    let reply_epoch_micros: Option<i64> = row.try_get(23)?;
    if pid.is_none()
        && application_name.is_none()
        && backend_start_epoch_micros.is_none()
        && state.is_none()
        && reply_epoch_micros.is_none()
    {
        return Ok(None);
    }
    let (Some(pid), Some(application_name), Some(start), Some(state)) =
        (pid, application_name, backend_start_epoch_micros, state)
    else {
        return Err(LocalSlotObservationError::InconsistentWalSender);
    };
    let pid = u32::try_from(pid)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(LocalSlotObservationError::InvalidWalSenderPid(pid))?;
    let start_epoch_micros = u64::try_from(start)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(LocalSlotObservationError::InvalidWalSenderStart(start))?;
    let activity = parse_wal_sender_activity(state)?;
    let application_name = ReplicationSlotName::new(application_name)
        .map_err(|_| LocalSlotObservationError::InvalidWalSenderApplicationName)?;
    let reply_epoch_micros = reply_epoch_micros
        .map(|value| {
            u64::try_from(value)
                .ok()
                .and_then(NonZeroU64::new)
                .ok_or(LocalSlotObservationError::InvalidWalSenderReply(value))
        })
        .transpose()?;
    let Some(physical_slot) = physical_slot else {
        return Err(LocalSlotObservationError::InconsistentWalSender);
    };
    if physical_slot.activity() != SlotActivity::Active(pid) {
        return Err(LocalSlotObservationError::InconsistentWalSender);
    }

    Ok(Some(LocalWalSenderObservation {
        identity: LocalPostgresBackendIdentity {
            pid,
            start_epoch_micros,
        },
        application_name,
        activity,
        reply_epoch_micros,
    }))
}

fn parse_wal_sender_activity(
    state: String,
) -> Result<LocalWalSenderActivity, LocalSlotObservationError> {
    match state.as_str() {
        "startup" => Ok(LocalWalSenderActivity::Startup),
        "catchup" => Ok(LocalWalSenderActivity::Catchup),
        "streaming" => Ok(LocalWalSenderActivity::Streaming),
        "backup" => Ok(LocalWalSenderActivity::Backup),
        "stopping" => Ok(LocalWalSenderActivity::Stopping),
        _ => Err(LocalSlotObservationError::UnsupportedWalSenderState(state)),
    }
}

fn parse_transaction_id(
    value: &str,
) -> Result<LocalPostgresTransactionId, LocalSlotObservationError> {
    value
        .parse::<u32>()
        .ok()
        .and_then(NonZeroU32::new)
        .map(LocalPostgresTransactionId)
        .ok_or(LocalSlotObservationError::InvalidTransactionId)
}

fn parse_prerequisites(
    row: &Row,
) -> Result<LocalStandbyPrerequisiteObservation, LocalSlotObservationError> {
    let system_identifier = parse_system_identifier(row.try_get(0)?)?;
    let checkpoint_lsn = required_nonzero_lsn(row, "checkpoint_lsn")?;
    let checkpoint_timeline = parse_timeline_id(row.try_get(1)?)?;
    let recovery = if row.try_get(2)? {
        RecoveryState::Standby
    } else {
        RecoveryState::Writable
    };
    let wal_level = match row.try_get::<_, String>(3)?.as_str() {
        "logical" => LogicalWalLevel::Logical,
        _ => LogicalWalLevel::Insufficient,
    };
    let hot_standby_feedback = setting_state(row.try_get(4)?);
    let wal_receiver_status_interval =
        parse_wal_receiver_interval(row.try_get(5)?, &row.try_get::<_, String>(6)?)?;
    let sync_replication_slots = setting_state(row.try_get(7)?);
    let primary_slot_name = optional_slot_name(row.try_get(8)?)?;
    let replay_lsn = optional_lsn(row, "replay_lsn")?;
    let receiver_pid = row.try_get(10)?;
    let receiver_status: Option<String> = row.try_get(11)?;
    let receiver_slot_name = row.try_get(12)?;
    let receiver_timeline = row.try_get(13)?;
    let ParsedWalReceiver {
        pid: wal_receiver_pid,
        activity: wal_receiver_activity,
        slot_name: wal_receiver_slot_name,
        received_timeline: wal_receiver_received_timeline,
    } = parse_wal_receiver(
        receiver_pid,
        receiver_status.as_deref(),
        receiver_slot_name,
        receiver_timeline,
    )?;
    let slot_sync_worker = parse_slot_sync_worker_row(row)?;

    Ok(LocalStandbyPrerequisiteObservation {
        system_identifier,
        checkpoint_lsn,
        checkpoint_timeline,
        recovery,
        wal_level,
        hot_standby_feedback,
        wal_receiver_status_interval,
        sync_replication_slots,
        primary_slot_name,
        replay_lsn,
        wal_receiver_pid,
        wal_receiver_activity,
        wal_receiver_slot_name,
        wal_receiver_received_timeline,
        slot_sync_worker,
    })
}

fn parse_slot_sync_worker_row(
    row: &Row,
) -> Result<Option<LocalSlotSyncWorkerObservation>, LocalSlotObservationError> {
    if !row.try_get::<_, bool>("has_read_all_stats")? {
        return Err(LocalSlotObservationError::StatisticsPrivilegeRequired);
    }
    parse_slot_sync_worker(
        row.try_get("slotsync_pid")?,
        row.try_get("slotsync_backend_start_epoch_micros")?,
        row.try_get::<_, Option<String>>("slotsync_wait_event_type")?
            .as_deref(),
        row.try_get::<_, Option<String>>("slotsync_wait_event")?
            .as_deref(),
    )
}

fn parse_system_identifier(value: i64) -> Result<u64, LocalSlotObservationError> {
    // PostgreSQL stores this as uint64 but exposes the control-file field as
    // int8. Preserve the bit pattern so clusters initialized after the signed
    // boundary do not acquire a different identity.
    let value = value.cast_unsigned();
    if value == 0 {
        return Err(LocalSlotObservationError::InvalidSystemIdentifier);
    }
    Ok(value)
}

fn parse_timeline_id(value: i32) -> Result<u32, LocalSlotObservationError> {
    // PostgreSQL stores TimeLineID as uint32 but exposes the control-file field
    // as int4. Preserve the signed SQL value's complete bit pattern.
    let value = value.cast_unsigned();
    if value == 0 {
        return Err(LocalSlotObservationError::InvalidTimelineId);
    }
    Ok(value)
}

fn parse_primary_current_timeline(
    value: Option<String>,
    recovery: RecoveryState,
) -> Result<Option<u32>, LocalSlotObservationError> {
    match (recovery, value) {
        (RecoveryState::Standby, None) => Ok(None),
        (RecoveryState::Writable, Some(value))
            if value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            let Some(timeline) = u32::from_str_radix(&value, 16)
                .ok()
                .and_then(NonZeroU32::new)
            else {
                return Err(LocalSlotObservationError::InvalidPrimaryCurrentTimeline(
                    Some(value),
                ));
            };
            Ok(Some(timeline.get()))
        }
        (_, value) => Err(LocalSlotObservationError::InvalidPrimaryCurrentTimeline(
            value,
        )),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedWalReceiver {
    pid: Option<NonZeroU32>,
    activity: LocalWalReceiverActivity,
    slot_name: Option<ReplicationSlotName>,
    received_timeline: Option<u32>,
}

fn parse_wal_receiver(
    pid: Option<i32>,
    status: Option<&str>,
    slot_name: Option<String>,
    received_timeline: Option<i32>,
) -> Result<ParsedWalReceiver, LocalSlotObservationError> {
    let Some(pid) = pid else {
        if status.is_some() || slot_name.is_some() || received_timeline.is_some() {
            return Err(LocalSlotObservationError::InconsistentWalReceiver);
        }
        return Ok(ParsedWalReceiver {
            pid: None,
            activity: LocalWalReceiverActivity::Absent,
            slot_name: None,
            received_timeline: None,
        });
    };
    let pid = u32::try_from(pid)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(LocalSlotObservationError::InvalidWalReceiverPid(pid))?;
    let Some(status) = status else {
        // PostgreSQL deliberately leaves the PID visible while redacting every
        // other field from roles without pg_read_all_stats. An observer must
        // not turn that live-but-unobservable receiver into an absent one.
        return Err(LocalSlotObservationError::WalReceiverDetailsUnavailable { pid });
    };
    let activity = match status {
        "stopped" => LocalWalReceiverActivity::Stopped,
        "starting" => LocalWalReceiverActivity::Starting,
        "streaming" => LocalWalReceiverActivity::Streaming,
        "waiting" => LocalWalReceiverActivity::Waiting,
        "restarting" => LocalWalReceiverActivity::Restarting,
        "stopping" => LocalWalReceiverActivity::Stopping,
        _ => {
            return Err(LocalSlotObservationError::UnsupportedWalReceiverStatus(
                status.to_owned(),
            ));
        }
    };
    let received_timeline = received_timeline.map(parse_timeline_id).transpose()?;
    Ok(ParsedWalReceiver {
        pid: Some(pid),
        activity,
        slot_name: optional_slot_name(slot_name)?,
        received_timeline,
    })
}

fn parse_slot_sync_worker(
    pid: Option<i32>,
    backend_start_epoch_micros: Option<i64>,
    wait_event_type: Option<&str>,
    wait_event: Option<&str>,
) -> Result<Option<LocalSlotSyncWorkerObservation>, LocalSlotObservationError> {
    let Some(pid) = pid else {
        if backend_start_epoch_micros.is_some() || wait_event_type.is_some() || wait_event.is_some()
        {
            return Err(LocalSlotObservationError::InconsistentSlotSyncWorker);
        }
        return Ok(None);
    };
    let pid = u32::try_from(pid)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(LocalSlotObservationError::InvalidSlotSyncWorkerPid(pid))?;
    let start_value =
        backend_start_epoch_micros.ok_or(LocalSlotObservationError::InconsistentSlotSyncWorker)?;
    let start_epoch_micros = u64::try_from(start_value)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(LocalSlotObservationError::InvalidSlotSyncWorkerStart(
            start_value,
        ))?;
    let activity = match (wait_event_type, wait_event) {
        (None, None) => LocalSlotSyncWorkerActivity::Running,
        (Some("Activity"), Some("ReplicationSlotsyncMain")) => {
            LocalSlotSyncWorkerActivity::WaitingAfterCycle
        }
        (Some(_), Some(_)) => LocalSlotSyncWorkerActivity::OtherWait,
        _ => return Err(LocalSlotObservationError::InconsistentSlotSyncWorker),
    };
    Ok(Some(LocalSlotSyncWorkerObservation {
        identity: LocalPostgresBackendIdentity {
            pid,
            start_epoch_micros,
        },
        activity,
    }))
}

fn validate_slot_sync_worker_window(
    before: Option<LocalSlotSyncWorkerObservation>,
    after: Option<LocalSlotSyncWorkerObservation>,
) -> Result<(), LocalSlotObservationError> {
    if before.map(LocalSlotSyncWorkerObservation::identity)
        == after.map(LocalSlotSyncWorkerObservation::identity)
    {
        Ok(())
    } else {
        Err(LocalSlotObservationError::SlotSyncWorkerChanged)
    }
}

const fn setting_state(enabled: bool) -> SettingState {
    if enabled {
        SettingState::Enabled
    } else {
        SettingState::Disabled
    }
}

fn parse_wal_receiver_interval(
    value: i64,
    unit: &str,
) -> Result<Duration, LocalSlotObservationError> {
    if unit != "s" {
        return Err(LocalSlotObservationError::UnsupportedSettingUnit {
            setting: "wal_receiver_status_interval",
            unit: unit.to_owned(),
        });
    }
    let seconds =
        u64::try_from(value).map_err(|_| LocalSlotObservationError::InvalidNonnegativeInteger {
            field: "wal_receiver_status_interval",
            value,
        })?;
    Ok(Duration::from_secs(seconds))
}

fn optional_slot_name(
    value: Option<String>,
) -> Result<Option<ReplicationSlotName>, LocalSlotObservationError> {
    value
        .map(ReplicationSlotName::new)
        .transpose()
        .map_err(Into::into)
}

async fn set_statement_timeout(
    client: &Client,
    deadline: Instant,
) -> Result<(), tokio_postgres::Error> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let timeout = remaining
        .saturating_sub(SERVER_STATEMENT_TIMEOUT_HEADROOM)
        .max(Duration::from_millis(1));
    let milliseconds = u64::try_from(timeout.as_millis())
        .expect("bounded slot observation timeout fits PostgreSQL milliseconds");
    let setting = format!("{milliseconds}ms");
    client
        .query_one(SET_STATEMENT_TIMEOUT_SQL, &[&setting])
        .await?;
    Ok(())
}

// These booleans are the exact PostgreSQL view columns. Keeping the raw row
// together preserves validation order before it is converted to closed enums.
#[allow(clippy::struct_excessive_bools)]
struct LogicalSlotFields {
    name_text: String,
    plugin: Option<String>,
    slot_type: String,
    database_oid: Option<i64>,
    temporary: bool,
    active: bool,
    active_pid: Option<i64>,
    wal_status: Option<String>,
    two_phase: bool,
    two_phase_at: Option<String>,
    invalidation_reason: Option<String>,
    failover: bool,
    synced: bool,
    confirmed_flush_lsn: Option<String>,
}

pub(crate) fn parse_logical_slot(
    row: &Row,
) -> Result<LogicalSlotObservation, LocalSlotObservationError> {
    parse_logical_slot_fields(LogicalSlotFields {
        name_text: row.try_get("slot_name")?,
        plugin: row.try_get("plugin")?,
        slot_type: row.try_get("slot_type")?,
        database_oid: row.try_get("database_oid")?,
        temporary: row.try_get("temporary")?,
        active: row.try_get("active")?,
        active_pid: row.try_get("active_pid")?,
        wal_status: row.try_get("wal_status")?,
        two_phase: row.try_get("two_phase")?,
        two_phase_at: row.try_get("two_phase_at")?,
        invalidation_reason: row.try_get("invalidation_reason")?,
        failover: row.try_get("failover")?,
        synced: row.try_get("synced")?,
        confirmed_flush_lsn: row.try_get("confirmed_flush_lsn")?,
    })
}

fn parse_primary_failover_anchor(
    row: &Row,
    target: &ManagedSlotTarget,
) -> Result<Option<LogicalSlotObservation>, LocalSlotObservationError> {
    let name_text: Option<String> = row.try_get(24)?;
    let plugin: Option<String> = row.try_get(25)?;
    let slot_type: Option<String> = row.try_get(26)?;
    let database_oid: Option<i64> = row.try_get(27)?;
    let temporary: Option<bool> = row.try_get(28)?;
    let active: Option<bool> = row.try_get(29)?;
    let active_pid: Option<i64> = row.try_get(30)?;
    let wal_status: Option<String> = row.try_get(31)?;
    let two_phase: Option<bool> = row.try_get(32)?;
    let two_phase_at: Option<String> = row.try_get(33)?;
    let invalidation_reason: Option<String> = row.try_get(34)?;
    let failover: Option<bool> = row.try_get(35)?;
    let synced: Option<bool> = row.try_get(36)?;
    let confirmed_flush_lsn: Option<String> = row.try_get(37)?;
    let Some(name_text) = name_text else {
        if plugin.is_some()
            || slot_type.is_some()
            || database_oid.is_some()
            || temporary.is_some()
            || active.is_some()
            || active_pid.is_some()
            || wal_status.is_some()
            || two_phase.is_some()
            || two_phase_at.is_some()
            || invalidation_reason.is_some()
            || failover.is_some()
            || synced.is_some()
            || confirmed_flush_lsn.is_some()
        {
            return Err(LocalSlotObservationError::InconsistentPrimaryFailoverAnchor);
        }
        return Ok(None);
    };
    if name_text != target.name().as_str() {
        return Err(LocalSlotObservationError::UnexpectedSlot(name_text));
    }
    let fields = LogicalSlotFields {
        name_text,
        plugin,
        slot_type: slot_type.ok_or(LocalSlotObservationError::InconsistentPrimaryFailoverAnchor)?,
        database_oid,
        temporary: temporary.ok_or(LocalSlotObservationError::InconsistentPrimaryFailoverAnchor)?,
        active: active.ok_or(LocalSlotObservationError::InconsistentPrimaryFailoverAnchor)?,
        active_pid,
        wal_status,
        two_phase: two_phase.ok_or(LocalSlotObservationError::InconsistentPrimaryFailoverAnchor)?,
        two_phase_at,
        invalidation_reason,
        failover: failover.ok_or(LocalSlotObservationError::InconsistentPrimaryFailoverAnchor)?,
        synced: synced.ok_or(LocalSlotObservationError::InconsistentPrimaryFailoverAnchor)?,
        confirmed_flush_lsn,
    };
    parse_logical_slot_fields(fields).map(Some)
}

fn parse_logical_slot_fields(
    fields: LogicalSlotFields,
) -> Result<LogicalSlotObservation, LocalSlotObservationError> {
    let LogicalSlotFields {
        name_text,
        plugin,
        slot_type,
        database_oid,
        temporary,
        active,
        active_pid,
        wal_status,
        two_phase,
        two_phase_at,
        invalidation_reason,
        failover,
        synced,
        confirmed_flush_lsn,
    } = fields;
    let name = ReplicationSlotName::new(name_text.clone())?;
    if slot_type != "logical" {
        return Err(LocalSlotObservationError::NonLogicalTarget(name_text));
    }
    let database_oid = database_oid
        .ok_or_else(|| LocalSlotObservationError::MissingDatabaseOid(name_text.clone()))?;
    let database_oid = positive_u32(database_oid, "database_oid")?;
    let plugin = match plugin.as_deref() {
        Some("pgoutput") => LogicalSlotPlugin::PgOutput,
        _ => LogicalSlotPlugin::Other,
    };
    let kind = match (failover, synced) {
        (true, false) => LogicalSlotKind::FailoverAnchor,
        (true, true) => LogicalSlotKind::SynchronizedFailoverAnchor,
        (false, false) => LogicalSlotKind::StandbyLocalDecoder,
        (false, true) => LogicalSlotKind::Other,
    };
    let two_phase = if two_phase {
        SettingState::Enabled
    } else {
        SettingState::Disabled
    };
    let activity = parse_slot_activity(&name_text, active, active_pid)?;
    let persistence = classify_persistence(temporary);
    Ok(LogicalSlotObservation {
        name,
        database_oid,
        plugin,
        kind,
        persistence,
        two_phase,
        two_phase_at: parse_optional_lsn_value(two_phase_at, "two_phase_at")?,
        activity,
        ownership: SlotOwnership::Unknown,
        invalidation: parse_invalidation(invalidation_reason)?,
        wal_retention: parse_wal_retention(wal_status)?,
        confirmed_flush_lsn: parse_optional_lsn_value(confirmed_flush_lsn, "confirmed_flush_lsn")?,
    })
}

fn parse_slot_activity(
    name: &str,
    active: bool,
    active_pid: Option<i64>,
) -> Result<SlotActivity, LocalSlotObservationError> {
    Ok(match (active, active_pid) {
        (false, None) => SlotActivity::Inactive,
        (true, Some(pid)) => {
            let pid = u32::try_from(pid)
                .ok()
                .and_then(NonZeroU32::new)
                .ok_or(LocalSlotObservationError::InvalidActivePid(pid))?;
            SlotActivity::Active(pid)
        }
        _ => {
            return Err(LocalSlotObservationError::InconsistentActivity(
                name.to_owned(),
            ));
        }
    })
}

const fn classify_persistence(temporary: bool) -> SlotPersistence {
    if temporary {
        SlotPersistence::NonPersistent
    } else {
        SlotPersistence::Unproven
    }
}

fn parse_wal_retention(
    value: Option<String>,
) -> Result<Option<SlotWalRetention>, LocalSlotObservationError> {
    value
        .map(|value| match value.as_str() {
            "reserved" => Ok(SlotWalRetention::Reserved),
            "extended" => Ok(SlotWalRetention::Extended),
            "unreserved" => Ok(SlotWalRetention::Unreserved),
            "lost" => Ok(SlotWalRetention::Lost),
            _ => Err(LocalSlotObservationError::UnsupportedWalStatus(value)),
        })
        .transpose()
}

fn parse_invalidation(
    value: Option<String>,
) -> Result<Option<SlotInvalidation>, LocalSlotObservationError> {
    value
        .map(|value| match value.as_str() {
            "wal_removed" => Ok(SlotInvalidation::WalRemoved),
            "rows_removed" => Ok(SlotInvalidation::RowsRemoved),
            "wal_level_insufficient" => Ok(SlotInvalidation::WalLevelInsufficient),
            "idle_timeout" => Ok(SlotInvalidation::IdleTimeout),
            _ => Err(LocalSlotObservationError::UnsupportedInvalidationReason(
                value,
            )),
        })
        .transpose()
}

fn optional_lsn(
    row: &Row,
    field: &'static str,
) -> Result<Option<PgLsn>, LocalSlotObservationError> {
    parse_optional_lsn_value(row.try_get(field)?, field)
}

fn parse_optional_lsn_value(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<PgLsn>, LocalSlotObservationError> {
    value
        .map(|value| {
            parse_lsn(&value).ok_or(LocalSlotObservationError::InvalidLsn { field, value })
        })
        .transpose()
}

fn required_nonzero_lsn(
    row: &Row,
    field: &'static str,
) -> Result<PgLsn, LocalSlotObservationError> {
    let value = row.try_get::<_, String>(field)?;
    parse_lsn(&value)
        .filter(|lsn| lsn.0 != 0)
        .ok_or(LocalSlotObservationError::InvalidLsn { field, value })
}

fn positive_u32(value: i64, field: &'static str) -> Result<u32, LocalSlotObservationError> {
    u32::try_from(value)
        .ok()
        .and_then(NonZeroU32::new)
        .map(NonZeroU32::get)
        .ok_or(LocalSlotObservationError::InvalidPositiveInteger { field, value })
}

pub(crate) fn parse_lsn(value: &str) -> Option<PgLsn> {
    let (high, low) = value.split_once('/')?;
    if high.is_empty() || high.len() > 8 || low.is_empty() || low.len() > 8 {
        return None;
    }
    let high = u32::from_str_radix(high, 16).ok()?;
    let low = u32::from_str_radix(low, 16).ok()?;
    Some(PgLsn((u64::from(high) << 32) | u64::from(low)))
}

/// Fail-closed local `PostgreSQL` logical-slot observation error.
#[derive(Debug, Error)]
pub enum LocalSlotObservationError {
    /// `PostgreSQL` query or typed-row failure.
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    /// The absolute observation deadline elapsed and the consumed client was dropped.
    #[error("local logical slot observation exceeded its terminal deadline {duration:?}")]
    OperationTimeout {
        /// Validated deadline applied to the full observation.
        duration: Duration,
    },
    /// The connection driver failed after the observation client was dropped.
    #[error("local logical slot observation connection failed: {0}")]
    Connection(#[source] tokio_postgres::Error),
    /// The task driving the dedicated connection panicked or was cancelled.
    #[error("local logical slot observation connection task failed: {0}")]
    ConnectionTask(#[source] JoinError),
    /// The dedicated connection did not close within its fixed cleanup bound.
    #[error("local logical slot observation connection cleanup exceeded {duration:?}")]
    ConnectionCleanupTimeout {
        /// Fixed local connection cleanup bound.
        duration: Duration,
    },
    /// Server is older than the minimum supported release.
    #[error(
        "logical slot observation requires PostgreSQL 18 or newer; observed server_version_num {0}"
    )]
    UnsupportedPostgresVersion(i32),
    /// The observed database is not UTF8.
    #[error("logical slot observation requires UTF8; observed {0:?}")]
    WrongEncoding(String),
    /// A positive `PostgreSQL` numeric identity did not fit the Rust model.
    #[error(
        "PostgreSQL observation field {field} must be a positive 32-bit integer; observed {value}"
    )]
    InvalidPositiveInteger {
        /// Rejected field.
        field: &'static str,
        /// Rejected `PostgreSQL` value.
        value: i64,
    },
    /// `PostgreSQL` reported a zero cluster system identifier.
    #[error("PostgreSQL system identifier must be nonzero")]
    InvalidSystemIdentifier,
    /// `PostgreSQL` reported a zero timeline identifier.
    #[error("PostgreSQL timeline identifier must be nonzero")]
    InvalidTimelineId,
    /// A writable primary did not expose one exact nonzero current timeline.
    #[error("PostgreSQL primary current timeline is unavailable or invalid: {0:?}")]
    InvalidPrimaryCurrentTimeline(Option<String>),
    /// A live WAL receiver's backend PID was zero or outside the supported range.
    #[error("PostgreSQL WAL receiver PID is invalid: {0}")]
    InvalidWalReceiverPid(i32),
    /// A live WAL receiver exists, but `PostgreSQL` redacted its details.
    #[error(
        "PostgreSQL WAL receiver {pid} details are unavailable; the observer role requires pg_read_all_stats"
    )]
    WalReceiverDetailsUnavailable {
        /// Visible PID of the receiver whose remaining fields were redacted.
        pid: NonZeroU32,
    },
    /// The observer role cannot distinguish redacted auxiliary-process rows.
    #[error("local slot observation requires effective pg_read_all_stats privileges")]
    StatisticsPrivilegeRequired,
    /// The primary's synchronized-slot policy exceeded the observation bound.
    #[error("synchronized_standby_slots length {observed} exceeds the observation bound {maximum}")]
    SynchronizedStandbySlotsTooLong {
        /// Server-reported policy length in bytes.
        observed: i64,
        /// Maximum policy length accepted by the observer.
        maximum: i64,
    },
    /// The bounded synchronized-slot value disagreed with its reported length.
    #[error("PostgreSQL returned inconsistent synchronized_standby_slots details")]
    InconsistentSynchronizedStandbySlots,
    /// The synchronized-slot value is not a plain unique replication-slot list.
    #[error("synchronized_standby_slots must be a plain unique replication-slot-name list")]
    UnsupportedSynchronizedStandbySlotsList,
    /// `PostgreSQL` returned receiver details without a receiver PID.
    #[error("PostgreSQL WAL receiver details are inconsistent with its PID")]
    InconsistentWalReceiver,
    /// `PostgreSQL` returned a receiver status outside the `PostgreSQL` 18 closed set.
    #[error("unsupported PostgreSQL 18 WAL receiver status {0:?}")]
    UnsupportedWalReceiverStatus(String),
    /// A local slot-sync worker PID was zero or outside the supported range.
    #[error("PostgreSQL slot-sync worker PID is invalid: {0}")]
    InvalidSlotSyncWorkerPid(i32),
    /// A slot-sync worker backend-start timestamp was not a positive Unix value.
    #[error("PostgreSQL slot-sync worker backend start is invalid: {0}")]
    InvalidSlotSyncWorkerStart(i64),
    /// `PostgreSQL` returned a partial slot-sync worker identity or wait event.
    #[error("PostgreSQL slot-sync worker details are internally inconsistent")]
    InconsistentSlotSyncWorker,
    /// The slot-sync worker started, exited, or restarted across the slot query.
    #[error("PostgreSQL slot-sync worker changed during logical-slot observation")]
    SlotSyncWorkerChanged,
    /// The requested physical slot name is occupied by another slot kind.
    #[error("requested physical replication slot name {0:?} is occupied by a non-physical slot")]
    PhysicalSlotNameCollision(String),
    /// `PostgreSQL` returned a partial or impossible physical-slot row.
    #[error("PostgreSQL physical replication slot details are internally inconsistent")]
    InconsistentPhysicalSlot,
    /// `PostgreSQL` returned a partial primary failover-anchor row.
    #[error("PostgreSQL primary failover-anchor details are internally inconsistent")]
    InconsistentPrimaryFailoverAnchor,
    /// A physical-slot catalog horizon was not a valid nonzero transaction ID.
    #[error("PostgreSQL physical slot catalog_xmin is invalid")]
    InvalidTransactionId,
    /// A physical walsender PID was zero or outside the supported range.
    #[error("PostgreSQL physical walsender PID is invalid: {0}")]
    InvalidWalSenderPid(i32),
    /// A physical walsender backend-start timestamp was not a positive Unix value.
    #[error("PostgreSQL physical walsender backend start is invalid: {0}")]
    InvalidWalSenderStart(i64),
    /// A physical walsender reply timestamp was not a positive Unix value.
    #[error("PostgreSQL physical walsender reply timestamp is invalid: {0}")]
    InvalidWalSenderReply(i64),
    /// A physical walsender application name violates the replication-slot-name contract.
    #[error("PostgreSQL physical walsender application_name is not replication-slot-shaped")]
    InvalidWalSenderApplicationName,
    /// `PostgreSQL` returned a walsender state outside its version 18 closed set.
    #[error("unsupported PostgreSQL 18 physical walsender state {0:?}")]
    UnsupportedWalSenderState(String),
    /// `PostgreSQL` returned a partial or PID-inconsistent walsender row.
    #[error("PostgreSQL physical walsender details are internally inconsistent")]
    InconsistentWalSender,
    /// A nonnegative `PostgreSQL` setting did not fit the Rust model.
    #[error("PostgreSQL observation field {field} must be a nonnegative integer; observed {value}")]
    InvalidNonnegativeInteger {
        /// Rejected field.
        field: &'static str,
        /// Rejected `PostgreSQL` value.
        value: i64,
    },
    /// `PostgreSQL` 18 exposed an unexpected canonical setting unit.
    #[error("PostgreSQL setting {setting} has unsupported unit {unit:?}")]
    UnsupportedSettingUnit {
        /// Setting whose unit was rejected.
        setting: &'static str,
        /// Unit returned by `pg_settings`.
        unit: String,
    },
    /// An expected logical slot had no owning database OID.
    #[error("requested logical slot {0:?} has no database OID")]
    MissingDatabaseOid(String),
    /// A physical slot occupied an expected logical slot name.
    #[error("requested logical slot name {0:?} is occupied by a non-logical slot")]
    NonLogicalTarget(String),
    /// `PostgreSQL` returned a row outside the bound target set.
    #[error("PostgreSQL returned unexpected logical slot {0:?}")]
    UnexpectedSlot(String),
    /// `PostgreSQL` returned one cluster-wide slot more than once.
    #[error("PostgreSQL returned duplicate logical slot {0:?}")]
    DuplicateSlot(String),
    /// The active flag and backend PID were not coherent.
    #[error("logical slot {0:?} has inconsistent active state and backend PID")]
    InconsistentActivity(String),
    /// An active backend PID was zero or outside the supported range.
    #[error("logical slot active backend PID is invalid: {0}")]
    InvalidActivePid(i64),
    /// `PostgreSQL` returned a WAL state outside the `PostgreSQL` 18 closed set.
    #[error("unsupported PostgreSQL 18 replication slot WAL status {0:?}")]
    UnsupportedWalStatus(String),
    /// `PostgreSQL` returned an invalidation reason outside the `PostgreSQL` 18 closed set.
    #[error("unsupported PostgreSQL 18 replication slot invalidation reason {0:?}")]
    UnsupportedInvalidationReason(String),
    /// `PostgreSQL` returned a malformed LSN.
    #[error("PostgreSQL field {field} contains invalid LSN {value:?}")]
    InvalidLsn {
        /// Rejected field.
        field: &'static str,
        /// Rejected value.
        value: String,
    },
    /// `PostgreSQL` returned a slot name outside the bounded identifier grammar.
    #[error(transparent)]
    SlotName(#[from] SlotNameError),
}

impl From<ConnectionTaskError> for LocalSlotObservationError {
    fn from(error: ConnectionTaskError) -> Self {
        match error {
            ConnectionTaskError::Connection(source) => Self::Connection(source),
            ConnectionTaskError::Task(source) => Self::ConnectionTask(source),
            ConnectionTaskError::CleanupTimeout { duration } => {
                Self::ConnectionCleanupTimeout { duration }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use tokio::sync::oneshot;
    use uuid::Uuid;

    use super::*;
    use crate::standby_slots::{ManagedSlotTargetError, SlotGeneration};

    fn target(prefix: &str, generation: u128) -> ManagedSlotTarget {
        let generation = SlotGeneration::new(Uuid::from_u128(generation)).expect("generation");
        ManagedSlotTarget::new(
            ReplicationSlotName::new(format!("{prefix}_{}", generation.as_uuid().simple()))
                .expect("slot name"),
            generation,
        )
        .expect("target")
    }

    #[test]
    fn request_is_small_distinct_and_ordered() {
        let first = target("anchor", 1);
        let second = target("decoder", 2);
        let request = LogicalSlotObservationRequest::new(vec![first.clone(), second.clone()])
            .expect("valid request");
        assert_eq!(request.targets(), &[first.clone(), second.clone()]);
        assert_eq!(
            LogicalSlotObservationRequest::new(Vec::new()),
            Err(SlotObservationRequestError::Empty)
        );
        assert!(matches!(
            LogicalSlotObservationRequest::new(vec![first.clone(), first]),
            Err(SlotObservationRequestError::DuplicateName(_))
        ));
        let reused_generation = ManagedSlotTarget::new(
            ReplicationSlotName::new(format!("other_{}", second.generation().as_uuid().simple()))
                .expect("slot name"),
            second.generation(),
        );
        assert!(reused_generation.is_ok());
        let reused_generation = reused_generation.expect("alternate name");
        assert_eq!(
            LogicalSlotObservationRequest::new(vec![second, reused_generation]),
            Err(SlotObservationRequestError::DuplicateGeneration)
        );
        assert!(matches!(
            LogicalSlotObservationRequest::new(vec![
                target("one", 11),
                target("two", 12),
                target("three", 13),
                target("four", 14),
            ]),
            Err(SlotObservationRequestError::TooMany {
                received: 4,
                maximum: MAX_OBSERVATION_TARGETS
            })
        ));
    }

    #[test]
    fn primary_policy_membership_is_exact_bounded_and_plain() {
        let physical_slot = ReplicationSlotName::new("pgshard_member_0001").expect("slot");
        let failover_anchor = target("anchor", 1);
        let request = PrimaryReplicationObservationRequest::new(
            physical_slot.clone(),
            failover_anchor.clone(),
        )
        .expect("distinct primary observation targets");
        assert_eq!(request.physical_slot(), &physical_slot);
        assert_eq!(request.failover_anchor(), &failover_anchor);
        assert_eq!(
            PrimaryReplicationObservationRequest::new(
                failover_anchor.name().clone(),
                failover_anchor,
            ),
            Err(PrimaryReplicationObservationRequestError::SlotNameCollision)
        );
        let configured = "pgshard_member_0000,pgshard_member_0001";
        assert_eq!(
            parse_synchronized_slot_policy(
                i64::try_from(configured.len()).expect("length"),
                Some(configured.to_owned()),
                &physical_slot,
            )
            .expect("plain unique policy"),
            FailoverSlotSynchronization::GatedOnPhysicalSlot
        );
        assert_eq!(
            parse_synchronized_slot_policy(0, Some(String::new()), &physical_slot)
                .expect("empty policy"),
            FailoverSlotSynchronization::NotGated
        );
        assert_eq!(
            parse_synchronized_slot_policy(
                19,
                Some("pgshard_member_0002".to_owned()),
                &physical_slot,
            )
            .expect("other member"),
            FailoverSlotSynchronization::NotGated
        );
        for unsupported in [
            " pgshard_member_0001",
            "pgshard_member_0001 ",
            "\"pgshard_member_0001\"",
            "pgshard_member_0001,,pgshard_member_0002",
            "pgshard_member_0001,pgshard_member_0001",
        ] {
            assert!(matches!(
                parse_synchronized_slot_policy(
                    i64::try_from(unsupported.len()).expect("length"),
                    Some(unsupported.to_owned()),
                    &physical_slot,
                ),
                Err(LocalSlotObservationError::UnsupportedSynchronizedStandbySlotsList)
            ));
        }
        assert!(matches!(
            parse_synchronized_slot_policy(
                i64::from(MAX_SYNCHRONIZED_STANDBY_SLOTS_BYTES) + 1,
                None,
                &physical_slot,
            ),
            Err(LocalSlotObservationError::SynchronizedStandbySlotsTooLong { .. })
        ));
        assert!(matches!(
            parse_synchronized_slot_policy(1, Some(String::new()), &physical_slot),
            Err(LocalSlotObservationError::InconsistentSynchronizedStandbySlots)
        ));
    }

    #[test]
    fn primary_raw_identifiers_are_nonzero_and_unordered() {
        assert_eq!(
            parse_primary_current_timeline(Some("00000001".to_owned()), RecoveryState::Writable,)
                .expect("current timeline"),
            Some(1)
        );
        assert_eq!(
            parse_primary_current_timeline(Some("FFFFFFFF".to_owned()), RecoveryState::Writable,)
                .expect("maximum current timeline"),
            Some(u32::MAX)
        );
        assert_eq!(
            parse_primary_current_timeline(None, RecoveryState::Standby)
                .expect("standby has no insertion timeline"),
            None
        );
        for invalid in [None, Some("00000000"), Some("0000001"), Some("0000000g")] {
            assert!(matches!(
                parse_primary_current_timeline(invalid.map(str::to_owned), RecoveryState::Writable,),
                Err(LocalSlotObservationError::InvalidPrimaryCurrentTimeline(_))
            ));
        }
        assert!(matches!(
            parse_primary_current_timeline(Some("00000001".to_owned()), RecoveryState::Standby,),
            Err(LocalSlotObservationError::InvalidPrimaryCurrentTimeline(_))
        ));
        assert_eq!(
            parse_transaction_id("4294967295")
                .expect("maximum xid")
                .get()
                .get(),
            u32::MAX
        );
        for invalid in ["", "0", "-1", "4294967296", "future"] {
            assert!(matches!(
                parse_transaction_id(invalid),
                Err(LocalSlotObservationError::InvalidTransactionId)
            ));
        }
        let pid = NonZeroU32::new(42).expect("PID");
        assert_eq!(
            parse_slot_activity("member", true, Some(42)).expect("active"),
            SlotActivity::Active(pid)
        );
        assert_eq!(
            parse_slot_activity("member", false, None).expect("inactive"),
            SlotActivity::Inactive
        );
        for inconsistent in [(true, None), (false, Some(42))] {
            assert!(matches!(
                parse_slot_activity("member", inconsistent.0, inconsistent.1),
                Err(LocalSlotObservationError::InconsistentActivity(_))
            ));
        }
        for (state, expected) in [
            ("startup", LocalWalSenderActivity::Startup),
            ("catchup", LocalWalSenderActivity::Catchup),
            ("streaming", LocalWalSenderActivity::Streaming),
            ("backup", LocalWalSenderActivity::Backup),
            ("stopping", LocalWalSenderActivity::Stopping),
        ] {
            assert_eq!(
                parse_wal_sender_activity(state.to_owned()).expect("known state"),
                expected
            );
        }
        assert!(matches!(
            parse_wal_sender_activity("future".to_owned()),
            Err(LocalSlotObservationError::UnsupportedWalSenderState(state))
                if state == "future"
        ));
    }

    #[test]
    fn parses_postgres_lsn_and_closed_slot_states() {
        assert_eq!(parse_lsn("1/2"), Some(PgLsn(0x1_0000_0002)));
        assert_eq!(parse_lsn("FFFFFFFF/FFFFFFFF"), Some(PgLsn(u64::MAX)));
        for invalid in ["", "0", "/0", "0/", "0/000000000", "g/0"] {
            assert_eq!(parse_lsn(invalid), None);
        }
        assert_eq!(
            parse_wal_retention(Some("reserved".to_owned())).expect("known"),
            Some(SlotWalRetention::Reserved)
        );
        assert!(matches!(
            parse_wal_retention(Some("future".to_owned())),
            Err(LocalSlotObservationError::UnsupportedWalStatus(_))
        ));
        assert_eq!(
            parse_invalidation(Some("rows_removed".to_owned())).expect("known"),
            Some(SlotInvalidation::RowsRemoved)
        );
        assert!(matches!(
            parse_invalidation(Some("future".to_owned())),
            Err(LocalSlotObservationError::UnsupportedInvalidationReason(_))
        ));

        assert_eq!(classify_persistence(false), SlotPersistence::Unproven);
        assert_eq!(classify_persistence(true), SlotPersistence::NonPersistent);
    }

    #[test]
    fn parses_unsigned_system_identity_and_exact_feedback_unit() {
        assert_eq!(parse_system_identifier(1).expect("identity"), 1);
        assert_eq!(
            parse_system_identifier(i64::MIN).expect("unsigned identity"),
            1_u64 << 63
        );
        assert_eq!(
            parse_system_identifier(-1).expect("maximum unsigned identity"),
            u64::MAX
        );
        assert!(matches!(
            parse_system_identifier(0),
            Err(LocalSlotObservationError::InvalidSystemIdentifier)
        ));
        assert_eq!(parse_timeline_id(1).expect("timeline"), 1);
        assert_eq!(
            parse_timeline_id(i32::MIN).expect("high-bit timeline"),
            1_u32 << 31
        );
        assert_eq!(parse_timeline_id(-1).expect("maximum timeline"), u32::MAX);
        assert!(matches!(
            parse_timeline_id(0),
            Err(LocalSlotObservationError::InvalidTimelineId)
        ));
        assert_eq!(
            parse_wal_receiver_interval(0, "s").expect("disabled interval"),
            Duration::ZERO
        );
        assert_eq!(
            parse_wal_receiver_interval(10, "s").expect("feedback interval"),
            Duration::from_secs(10)
        );
        assert!(matches!(
            parse_wal_receiver_interval(-1, "s"),
            Err(LocalSlotObservationError::InvalidNonnegativeInteger { .. })
        ));
        assert!(matches!(
            parse_wal_receiver_interval(10, "ms"),
            Err(LocalSlotObservationError::UnsupportedSettingUnit { .. })
        ));
    }

    #[test]
    fn parses_closed_receiver_activity_and_fails_on_redaction() {
        assert_eq!(
            parse_wal_receiver(None, None, None, None).expect("absent receiver"),
            ParsedWalReceiver {
                pid: None,
                activity: LocalWalReceiverActivity::Absent,
                slot_name: None,
                received_timeline: None,
            }
        );
        for (status, expected) in [
            ("stopped", LocalWalReceiverActivity::Stopped),
            ("starting", LocalWalReceiverActivity::Starting),
            ("streaming", LocalWalReceiverActivity::Streaming),
            ("waiting", LocalWalReceiverActivity::Waiting),
            ("restarting", LocalWalReceiverActivity::Restarting),
            ("stopping", LocalWalReceiverActivity::Stopping),
        ] {
            let receiver = parse_wal_receiver(
                Some(42),
                Some(status),
                Some("pgshard_member_0001".to_owned()),
                Some(-1),
            )
            .expect("known receiver state");
            assert_eq!(receiver.pid.map(NonZeroU32::get), Some(42));
            assert_eq!(receiver.activity, expected);
            assert_eq!(
                receiver.slot_name.as_ref().map(ReplicationSlotName::as_str),
                Some("pgshard_member_0001")
            );
            assert_eq!(receiver.received_timeline, Some(u32::MAX));
        }
        assert!(matches!(
            parse_wal_receiver(Some(42), None, None, Some(7)),
            Err(LocalSlotObservationError::WalReceiverDetailsUnavailable { pid })
                if pid.get() == 42
        ));
        assert!(matches!(
            parse_wal_receiver(Some(0), Some("streaming"), None, Some(7)),
            Err(LocalSlotObservationError::InvalidWalReceiverPid(0))
        ));
        assert!(matches!(
            parse_wal_receiver(None, Some("streaming"), None, None),
            Err(LocalSlotObservationError::InconsistentWalReceiver)
        ));
        assert!(matches!(
            parse_wal_receiver(None, None, Some("pgshard_member_0001".to_owned()), None,),
            Err(LocalSlotObservationError::InconsistentWalReceiver)
        ));
        assert!(matches!(
            parse_wal_receiver(None, None, None, Some(7)),
            Err(LocalSlotObservationError::InconsistentWalReceiver)
        ));
        assert!(matches!(
            parse_wal_receiver(Some(42), Some("future"), None, Some(7)),
            Err(LocalSlotObservationError::UnsupportedWalReceiverStatus(status))
                if status == "future"
        ));
    }

    #[test]
    fn parses_slot_sync_worker_identity_and_cycle_boundary() {
        assert_eq!(
            parse_slot_sync_worker(None, None, None, None).expect("absent worker"),
            None
        );
        let worker = parse_slot_sync_worker(
            Some(42),
            Some(1_700_000_000_123_456),
            Some("Activity"),
            Some("ReplicationSlotsyncMain"),
        )
        .expect("waiting worker")
        .expect("worker row");
        assert_eq!(worker.identity().pid().get(), 42);
        assert_eq!(
            worker.identity().start_epoch_micros().get(),
            1_700_000_000_123_456
        );
        assert_eq!(
            worker.activity(),
            LocalSlotSyncWorkerActivity::WaitingAfterCycle
        );

        for (wait_type, wait_event, expected) in [
            (None, None, LocalSlotSyncWorkerActivity::Running),
            (
                Some("Client"),
                Some("ClientRead"),
                LocalSlotSyncWorkerActivity::OtherWait,
            ),
        ] {
            assert_eq!(
                parse_slot_sync_worker(
                    Some(43),
                    Some(1_700_000_000_123_457),
                    wait_type,
                    wait_event,
                )
                .expect("coherent worker")
                .expect("worker row")
                .activity(),
                expected
            );
        }

        assert!(matches!(
            parse_slot_sync_worker(Some(0), Some(1), None, None),
            Err(LocalSlotObservationError::InvalidSlotSyncWorkerPid(0))
        ));
        assert!(matches!(
            parse_slot_sync_worker(Some(42), None, None, None),
            Err(LocalSlotObservationError::InconsistentSlotSyncWorker)
        ));
        assert!(matches!(
            parse_slot_sync_worker(Some(42), Some(0), None, None),
            Err(LocalSlotObservationError::InvalidSlotSyncWorkerStart(0))
        ));
        for inconsistent in [
            (None, Some(1), None, None),
            (Some(42), Some(1), Some("Activity"), None),
            (Some(42), Some(1), None, Some("ReplicationSlotsyncMain")),
        ] {
            assert!(matches!(
                parse_slot_sync_worker(
                    inconsistent.0,
                    inconsistent.1,
                    inconsistent.2,
                    inconsistent.3,
                ),
                Err(LocalSlotObservationError::InconsistentSlotSyncWorker)
            ));
        }
    }

    #[test]
    fn slot_snapshot_requires_one_stable_worker_generation() {
        let waiting = parse_slot_sync_worker(
            Some(42),
            Some(1_700_000_000_123_456),
            Some("Activity"),
            Some("ReplicationSlotsyncMain"),
        )
        .expect("waiting worker");
        let running = parse_slot_sync_worker(Some(42), Some(1_700_000_000_123_456), None, None)
            .expect("same running worker");
        let same_pid_restarted = parse_slot_sync_worker(
            Some(42),
            Some(1_700_000_000_123_457),
            Some("Activity"),
            Some("ReplicationSlotsyncMain"),
        )
        .expect("replacement worker");
        let pid_reused_start = parse_slot_sync_worker(
            Some(43),
            Some(1_700_000_000_123_456),
            Some("Activity"),
            Some("ReplicationSlotsyncMain"),
        )
        .expect("replacement worker with reused start token");

        assert!(validate_slot_sync_worker_window(None, None).is_ok());
        assert!(validate_slot_sync_worker_window(running, waiting).is_ok());
        for changed in [
            (None, waiting),
            (waiting, None),
            (waiting, same_pid_restarted),
            (waiting, pid_reused_start),
        ] {
            assert!(matches!(
                validate_slot_sync_worker_window(changed.0, changed.1),
                Err(LocalSlotObservationError::SlotSyncWorkerChanged)
            ));
        }
    }

    #[test]
    fn alternate_target_names_still_require_the_full_generation() {
        let generation = SlotGeneration::new(Uuid::from_u128(99)).expect("generation");
        assert!(matches!(
            ManagedSlotTarget::new(
                ReplicationSlotName::new("missing_generation").expect("slot name"),
                generation
            ),
            Err(ManagedSlotTargetError)
        ));
    }

    #[tokio::test]
    async fn aborting_connection_task_drops_the_pending_driver() {
        struct DropSignal(Option<oneshot::Sender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (started_sender, started_receiver) = oneshot::channel();
        let (dropped_sender, dropped_receiver) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(dropped_sender));
            let _ = started_sender.send(());
            pending::<Result<(), tokio_postgres::Error>>().await
        });
        started_receiver.await.expect("driver started");

        ConnectionTask::new(handle).abort_and_wait().await;

        timeout(CONNECTION_CLEANUP_TIMEOUT, dropped_receiver)
            .await
            .expect("aborted driver dropped before the cleanup bound")
            .expect("driver retained its drop signal");
    }

    #[tokio::test]
    async fn cancelling_graceful_finish_aborts_the_pending_driver() {
        struct DropSignal(Option<oneshot::Sender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (started_sender, started_receiver) = oneshot::channel();
        let (dropped_sender, dropped_receiver) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(dropped_sender));
            let _ = started_sender.send(());
            pending::<Result<(), tokio_postgres::Error>>().await
        });
        started_receiver.await.expect("driver started");
        let mut finish = Box::pin(ConnectionTask::new(handle).finish(()));

        assert!(
            timeout(Duration::from_millis(1), finish.as_mut())
                .await
                .is_err(),
            "pending driver unexpectedly completed graceful finish"
        );
        drop(finish);

        timeout(CONNECTION_CLEANUP_TIMEOUT, dropped_receiver)
            .await
            .expect("cancelled finish aborted the driver before the cleanup bound")
            .expect("driver retained its drop signal");
    }
}
