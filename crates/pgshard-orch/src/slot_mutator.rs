//! Bounded creation and deletion of catalog-named `PostgreSQL` 18 logical slots.
//!
//! Each operation consumes a dedicated client and connection driver. A failed
//! create or drop after dispatch is deliberately reported as outcome-unknown:
//! `PostgreSQL` can persist a slot before the SQL response reaches the caller,
//! and slot changes are not rolled back with the surrounding transaction. The
//! caller must observe the exact generation-qualified target before deciding
//! what reconciliation is safe; blind retries are never implied by this API.
//! A separate writable connection to the canonical `shardschema` database
//! serializes every managed mutation on the target name, then revalidates the
//! exact durable generation, lifecycle, restore incarnation, source lineage,
//! role and catalog epoch after acquiring that fence. The mutation connection
//! may target another database, but it must match the catalog allocation.
//! Cancelling a mutation future discards its typed result; callers must then
//! conservatively reconcile the target as outcome-unknown.

use std::{
    fmt,
    future::Future,
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use pgshard_catalog::{CatalogOperationTimeout, SHARDSCHEMA_DATABASE};
use pgshard_types::{CatalogEpoch, PgLsn};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinError,
    time::{Instant, sleep, timeout_at},
};
use tokio_postgres::{Client, Connection, IsolationLevel, Statement, Transaction, error::SqlState};
use uuid::Uuid;

use crate::{
    postgres_connection::{ConnectionTask, ConnectionTaskError},
    slot_observer::{
        CorrelatedStandbyReplicationPath, LocalPostgresBackendIdentity, LocalSlotObservationError,
        parse_logical_slot, parse_lsn,
    },
    standby_slots::{
        LogicalSlotKind, LogicalSlotObservation, LogicalSlotPlugin, ManagedSlotTarget,
        RecoveryState, ReplicationSlotName, ReplicationSourceIdentity, SettingState, SlotActivity,
        SlotGeneration, SlotInvalidation, SlotOwnership, SlotPersistence, SlotWalRetention,
    },
};

const MIN_POSTGRES_VERSION_NUM: i32 = 180_000;
const MAX_ADVISORY_LOCK_ROWS: usize = 16;
const SERVER_STATEMENT_TIMEOUT_HEADROOM: Duration = Duration::from_millis(25);
const SERVER_TRANSACTION_TIMEOUT_GRACE: Duration = Duration::from_millis(101);
const TARGET_FENCE_RETRY_INTERVAL: Duration = Duration::from_millis(10);
const PIN_SEARCH_PATH_SQL: &str = "SELECT pg_catalog.set_config('search_path', '', false)";
const SET_STATEMENT_TIMEOUT_SQL: &str =
    "SELECT pg_catalog.set_config('statement_timeout', $1, false)";
const RESET_STATEMENT_TIMEOUT_SQL: &str =
    "SELECT pg_catalog.set_config('statement_timeout', '0', false)";
const SET_LOCAL_CATALOG_RETIREMENT_TIMEOUTS_SQL: &str = "\
    SELECT pg_catalog.set_config('statement_timeout', $1, true), \
           pg_catalog.set_config('transaction_timeout', $2, true)";
const DISABLE_LOCAL_STATEMENT_TIMEOUT_SQL: &str = "SET LOCAL statement_timeout = 0";
const CATALOG_FENCE_REQUIREMENTS_SQL: &str = "\
    SELECT pg_catalog.current_setting('server_version_num')::pg_catalog.int4, \
           pg_catalog.current_database()::pg_catalog.text, \
           pg_catalog.getdatabaseencoding()::pg_catalog.text, \
           pg_catalog.pg_is_in_recovery(), \
           pg_catalog.pg_backend_pid()::pg_catalog.int4";
const CATALOG_SLOT_AUTHORIZATION_SQL: &str = "\
    WITH candidates AS ( \
        SELECT 'probe'::pg_catalog.text AS allocation_kind, probes.state, \
               probes.probe_generation::pg_catalog.text AS slot_generation, \
               probes.slot_name::pg_catalog.text AS slot_name, \
               'primary-anchor'::pg_catalog.text AS slot_role, \
               probes.database_name::pg_catalog.text AS database_name, \
               probes.system_identifier::pg_catalog.text AS system_identifier, \
               probes.database_oid, probes.source_timeline, \
               probes.restore_incarnation::pg_catalog.text AS restore_incarnation, \
               probes.creation_receipt_id::pg_catalog.text AS creation_receipt_id, \
               probes.cleanup_receipt_id::pg_catalog.text AS cleanup_receipt_id, \
               pgshard_catalog.managed_slot_creation_attempt_state( \
                   probes.probe_generation, probes.slot_name::pg_catalog.text, \
                   $3::pg_catalog.text::pg_catalog.uuid \
               ) AS creation_attempt_state, \
               restores.state::pg_catalog.text AS restore_state, \
               shards.state::pg_catalog.text AS shard_state, \
               NULL::pg_catalog.text AS attachment_state, \
               NULL::pg_catalog.text AS consumer_shard_state, \
               NULL::pg_catalog.text AS consumer_state, \
               NULL::pg_catalog.text AS logical_database_state \
          FROM pgshard_catalog.slot_sync_probes AS probes \
          JOIN pgshard_catalog.shard_restore_incarnations AS restores \
            ON restores.restore_incarnation = probes.restore_incarnation \
           AND restores.shard_id = probes.shard_id \
          JOIN pgshard_catalog.shards AS shards \
            ON shards.shard_id = probes.shard_id \
         WHERE probes.probe_generation = $1::pg_catalog.text::pg_catalog.uuid \
           AND probes.slot_name OPERATOR(pg_catalog.=) $2::pg_catalog.name \
        UNION ALL \
        SELECT 'consumer'::pg_catalog.text, slots.state, \
               slots.slot_generation::pg_catalog.text, \
               slots.slot_name::pg_catalog.text, slots.slot_role, \
               attachments.database_name::pg_catalog.text, \
               attachments.system_identifier::pg_catalog.text, \
               attachments.database_oid, attachments.selected_source_timeline, \
               attachments.restore_incarnation::pg_catalog.text, \
               NULL::pg_catalog.text, NULL::pg_catalog.text, \
               pgshard_catalog.managed_slot_creation_attempt_state( \
                   slots.slot_generation, slots.slot_name::pg_catalog.text, \
                   $3::pg_catalog.text::pg_catalog.uuid \
               ), \
               restores.state::pg_catalog.text, shards.state::pg_catalog.text, \
               attachments.state::pg_catalog.text, \
               consumer_shards.state::pg_catalog.text, \
               consumers.state::pg_catalog.text, databases.state::pg_catalog.text \
          FROM pgshard_catalog.managed_replication_slots AS slots \
          JOIN pgshard_catalog.logical_consumer_attachments AS attachments \
            ON attachments.attachment_generation = slots.attachment_generation \
           AND attachments.consumer_id = slots.consumer_id \
           AND attachments.logical_database_id = slots.logical_database_id \
           AND attachments.shard_id = slots.shard_id \
          JOIN pgshard_catalog.shard_restore_incarnations AS restores \
            ON restores.restore_incarnation = attachments.restore_incarnation \
           AND restores.shard_id = attachments.shard_id \
          JOIN pgshard_catalog.shards AS shards \
            ON shards.shard_id = attachments.shard_id \
          JOIN pgshard_catalog.logical_consumer_shards AS consumer_shards \
            ON consumer_shards.consumer_id = slots.consumer_id \
           AND consumer_shards.logical_database_id = slots.logical_database_id \
           AND consumer_shards.shard_id = slots.shard_id \
          JOIN pgshard_catalog.logical_consumers AS consumers \
            ON consumers.consumer_id = slots.consumer_id \
           AND consumers.logical_database_id = slots.logical_database_id \
          JOIN pgshard_catalog.logical_databases AS databases \
            ON databases.logical_database_id = slots.logical_database_id \
         WHERE slots.slot_generation = $1::pg_catalog.text::pg_catalog.uuid \
           AND slots.slot_name OPERATOR(pg_catalog.=) $2::pg_catalog.name \
    ) \
    SELECT candidates.*, state.catalog_epoch \
      FROM candidates \
      CROSS JOIN pgshard_catalog.cluster_state AS state \
     WHERE state.singleton \
     LIMIT 2";
const BASIC_REQUIREMENTS_SQL: &str = "\
    SELECT pg_catalog.current_setting('server_version_num')::pg_catalog.int4, \
           pg_catalog.current_database(), \
           (SELECT oid::pg_catalog.int8 FROM pg_catalog.pg_database \
             WHERE datname OPERATOR(pg_catalog.=) pg_catalog.current_database()), \
           pg_catalog.getdatabaseencoding(), \
           pg_catalog.pg_backend_pid()::pg_catalog.int4, \
           (SELECT oid::pg_catalog.int8 FROM pg_catalog.pg_roles \
             WHERE rolname OPERATOR(pg_catalog.=) SESSION_USER), \
           (SELECT oid::pg_catalog.int8 FROM pg_catalog.pg_roles \
             WHERE rolname OPERATOR(pg_catalog.=) CURRENT_USER), \
           ARRAY( \
               SELECT pg_catalog.format( \
                          '%s:%s:%s:%s:%s', locks.classid, locks.objid, \
                          locks.objsubid, locks.mode, locks.granted) \
                 FROM pg_catalog.pg_locks AS locks \
                WHERE locks.pid OPERATOR(pg_catalog.=) pg_catalog.pg_backend_pid() \
                  AND locks.locktype OPERATOR(pg_catalog.=) 'advisory' \
                ORDER BY locks.classid, locks.objid, locks.objsubid, locks.mode, \
                         locks.granted \
                LIMIT $1::pg_catalog.int8 \
           )::pg_catalog.text[]";
const SOURCE_REQUIREMENTS_SQL: &str = "\
    SELECT control.system_identifier::pg_catalog.int8, \
           checkpoint.timeline_id, \
           pg_catalog.pg_is_in_recovery(), \
           CASE WHEN NOT pg_catalog.pg_is_in_recovery() \
                THEN pg_catalog.substring( \
                         pg_catalog.pg_walfile_name( \
                             pg_catalog.pg_current_wal_lsn()), 1, 8) \
           END AS current_timeline_hex, \
           pg_catalog.current_setting('wal_level'), \
           pg_catalog.current_setting('hot_standby_feedback')::pg_catalog.bool, \
           (SELECT setting::pg_catalog.int8 FROM pg_catalog.pg_settings \
             WHERE name OPERATOR(pg_catalog.=) 'wal_receiver_status_interval'), \
           (SELECT unit::pg_catalog.text FROM pg_catalog.pg_settings \
             WHERE name OPERATOR(pg_catalog.=) 'wal_receiver_status_interval'), \
           pg_catalog.current_setting('sync_replication_slots')::pg_catalog.bool, \
           NULLIF(pg_catalog.current_setting('primary_slot_name'), ''), \
           receiver.pid::pg_catalog.int4, receiver.status::pg_catalog.text, \
           receiver.slot_name::pg_catalog.text, \
           NULLIF(receiver.received_tli, 0)::pg_catalog.int4, \
           slotsync.pid::pg_catalog.int4, \
           pg_catalog.floor( \
               pg_catalog.date_part('epoch', slotsync.backend_start) * 1000000 \
           )::pg_catalog.int8, \
           pg_catalog.pg_backend_pid()::pg_catalog.int4, \
           (SELECT oid::pg_catalog.int8 FROM pg_catalog.pg_roles \
             WHERE rolname OPERATOR(pg_catalog.=) SESSION_USER), \
           (SELECT oid::pg_catalog.int8 FROM pg_catalog.pg_roles \
             WHERE rolname OPERATOR(pg_catalog.=) CURRENT_USER), \
           ARRAY( \
               SELECT pg_catalog.format( \
                          '%s:%s:%s:%s:%s', locks.classid, locks.objid, \
                          locks.objsubid, locks.mode, locks.granted) \
                 FROM pg_catalog.pg_locks AS locks \
                WHERE locks.pid OPERATOR(pg_catalog.=) pg_catalog.pg_backend_pid() \
                  AND locks.locktype OPERATOR(pg_catalog.=) 'advisory' \
                ORDER BY locks.classid, locks.objid, locks.objsubid, locks.mode, \
                         locks.granted \
                LIMIT $1::pg_catalog.int8 \
           )::pg_catalog.text[] \
      FROM pg_catalog.pg_control_system() AS control \
     CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint \
      LEFT JOIN pg_catalog.pg_stat_get_wal_receiver() AS receiver \
        ON receiver.pid IS NOT NULL \
      LEFT JOIN LATERAL ( \
            SELECT activity.pid, activity.backend_start \
              FROM pg_catalog.pg_stat_get_activity(NULL) AS activity \
             WHERE activity.backend_type OPERATOR(pg_catalog.=) 'slotsync worker' \
             LIMIT 2 \
      ) AS slotsync ON true";
const SELECT_SLOT_SQL: &str = "\
    SELECT slot_name::pg_catalog.text AS slot_name, \
           plugin::pg_catalog.text AS plugin, slot_type, \
           datoid::pg_catalog.int8 AS database_oid, temporary, active, \
           active_pid::pg_catalog.int8 AS active_pid, wal_status, two_phase, \
           two_phase_at::pg_catalog.text AS two_phase_at, invalidation_reason, \
           failover, synced, confirmed_flush_lsn::pg_catalog.text AS confirmed_flush_lsn \
      FROM pg_catalog.pg_replication_slots \
     WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name";
const CREATE_SLOT_SQL: &str = "\
    SELECT slot_name::pg_catalog.text, lsn::pg_catalog.text \
      FROM pg_catalog.pg_create_logical_replication_slot( \
               $1::pg_catalog.name, 'pgoutput'::pg_catalog.name, false, true, \
               $2::pg_catalog.bool)";
const DROP_SLOT_SQL: &str = "SELECT pg_catalog.pg_drop_replication_slot($1::pg_catalog.name)";
const BEGIN_CATALOG_CREATION_ATTEMPT_SQL: &str = "\
    SELECT pgshard_catalog.begin_managed_slot_creation_attempt( \
        $1::pg_catalog.text::pg_catalog.uuid, $2::pg_catalog.text, \
        $3::pg_catalog.text, $4::pg_catalog.text::pg_catalog.numeric, \
        $5::pg_catalog.int8, $6::pg_catalog.int8, \
        $7::pg_catalog.text::pg_catalog.uuid, $8::pg_catalog.int8, \
        $9::pg_catalog.text::pg_catalog.uuid \
    )::pg_catalog.text";
const ABANDON_CATALOG_CREATION_ATTEMPT_SQL: &str = "\
    SELECT pgshard_catalog.abandon_managed_slot_creation_attempt( \
        $1::pg_catalog.text::pg_catalog.uuid, $2::pg_catalog.text, \
        $3::pg_catalog.text::pg_catalog.uuid \
    )";
const COMPLETE_CATALOG_CONSUMER_RETIREMENT_SQL: &str = "\
    SELECT pgshard_catalog.complete_managed_replication_slot_retirement( \
        $1::pg_catalog.text::pg_catalog.uuid, $2::pg_catalog.text, \
        $3::pg_catalog.text::pg_catalog.uuid, $4::pg_catalog.text::pg_catalog.uuid \
    )";
const ACTIVATE_CATALOG_CONSUMER_SLOT_SQL: &str = "\
    SELECT pgshard_catalog.activate_managed_replication_slot( \
        $1::pg_catalog.text::pg_catalog.uuid, \
        $2::pg_catalog.text::pg_catalog.uuid, \
        $3::pg_catalog.text::pg_catalog.pg_lsn, \
        $3::pg_catalog.text::pg_catalog.pg_lsn \
    )";
pub(crate) const ACQUIRE_TARGET_FENCE_SQL: &str = "\
    SELECT acquired_fence_id::pg_catalog.text, acquired_backend_pid \
      FROM pgshard_catalog.acquire_managed_slot_target_fence($1::pg_catalog.text)";
const RELEASE_TARGET_FENCE_SQL: &str = "\
    SELECT pgshard_catalog.release_managed_slot_target_fence( \
        $1::pg_catalog.text, $2::pg_catalog.text::pg_catalog.uuid \
    )";
const RELEASE_CURRENT_TARGET_FENCE_SQL: &str = "\
    SELECT pgshard_catalog.release_managed_slot_target_fence( \
        $1::pg_catalog.text, NULL::pg_catalog.uuid \
    )";
const VERIFY_TARGET_FENCE_SQL: &str = "\
    SELECT pgshard_catalog.verify_managed_slot_target_fence( \
        $1::pg_catalog.text, $2::pg_catalog.text::pg_catalog.uuid \
    )";

/// Managed logical-slot shape this primitive may create.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedLogicalSlotRole {
    /// Writable-primary slot continuously synchronized to promotion candidates.
    PrimaryFailoverAnchor,
    /// Independent non-failover slot decoded locally on a hot standby.
    StandbyLocalDecoder,
}

impl ManagedLogicalSlotRole {
    const fn catalog_label(self) -> &'static str {
        match self {
            Self::PrimaryFailoverAnchor => "primary-anchor",
            Self::StandbyLocalDecoder => "standby-decoder",
        }
    }

    const fn recovery(self) -> RecoveryState {
        match self {
            Self::PrimaryFailoverAnchor => RecoveryState::Writable,
            Self::StandbyLocalDecoder => RecoveryState::Standby,
        }
    }

    const fn failover(self) -> bool {
        matches!(self, Self::PrimaryFailoverAnchor)
    }

    const fn slot_kind(self) -> LogicalSlotKind {
        match self {
            Self::PrimaryFailoverAnchor => LogicalSlotKind::FailoverAnchor,
            Self::StandbyLocalDecoder => LogicalSlotKind::StandbyLocalDecoder,
        }
    }
}

impl fmt::Display for ManagedLogicalSlotRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PrimaryFailoverAnchor => "primary failover anchor",
            Self::StandbyLocalDecoder => "standby-local decoder",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StandbyMutationPath {
    physical_slot: ReplicationSlotName,
    expected_wal_receiver_pid: NonZeroU32,
    expected_slot_sync_worker: LocalPostgresBackendIdentity,
    maximum_feedback_reporting_interval: Duration,
    valid_until: Instant,
}

impl StandbyMutationPath {
    fn from_correlated(path: CorrelatedStandbyReplicationPath) -> Self {
        let mutation_path = Self {
            physical_slot: path.physical_slot().clone(),
            expected_wal_receiver_pid: path.wal_receiver_pid(),
            expected_slot_sync_worker: path.slot_sync_worker_identity(),
            maximum_feedback_reporting_interval: path.maximum_feedback_reporting_interval(),
            valid_until: path.valid_until(),
        };
        drop(path);
        mutation_path
    }
}

/// Exact catalog allocation and source expected for one local slot creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedLogicalSlotCreateRequest {
    target: ManagedSlotTarget,
    source: ReplicationSourceIdentity,
    role: ManagedLogicalSlotRole,
    standby_path: Option<StandbyMutationPath>,
}

impl ManagedLogicalSlotCreateRequest {
    /// Binds a primary failover anchor to one catalog-selected writable source.
    #[must_use]
    pub const fn primary_failover_anchor(
        target: ManagedSlotTarget,
        source: ReplicationSourceIdentity,
    ) -> Self {
        Self {
            target,
            source,
            role: ManagedLogicalSlotRole::PrimaryFailoverAnchor,
            standby_path: None,
        }
    }

    /// Consumes a fresh, unforgeable correlated path for its exact local decoder.
    ///
    /// The mutator rechecks the proof's receiver, timeline, feedback, physical
    /// slot and slot-sync-worker generation immediately before and after slot
    /// creation. A proof that expires before dispatch is rejected without a
    /// mutation; expiry after dispatch makes the outcome unknown.
    #[must_use]
    pub fn standby_local_decoder(path: CorrelatedStandbyReplicationPath) -> Self {
        let target = path.local_decoder().clone();
        let source = path.source_identity();
        Self {
            target,
            source,
            role: ManagedLogicalSlotRole::StandbyLocalDecoder,
            standby_path: Some(StandbyMutationPath::from_correlated(path)),
        }
    }

    /// Returns the exact generation-qualified slot target.
    #[must_use]
    pub const fn target(&self) -> &ManagedSlotTarget {
        &self.target
    }

    /// Returns the catalog-selected source identity.
    #[must_use]
    pub const fn source(&self) -> ReplicationSourceIdentity {
        self.source
    }

    /// Returns the local logical-slot role to create.
    #[must_use]
    pub const fn role(&self) -> ManagedLogicalSlotRole {
        self.role
    }
}

/// Point-in-time proof returned only after this primitive created and verified a slot.
///
/// The receipt is intentionally not serializable and has no public constructor.
/// It can authorize bounded cleanup in the same process, but it is not a
/// durable mutation ledger, a live-slot lease, or proof against later external
/// deletion and recreation. `shardschema` persists the matching pre-dispatch
/// creation attempt, while crash reconciliation must also observe the exact
/// physical target because post-dispatch outcomes can remain unknown.
#[derive(Debug, Eq, PartialEq)]
pub struct ManagedLogicalSlotReceipt {
    receipt_id: ManagedLogicalSlotReceiptId,
    target: ManagedSlotTarget,
    source: ReplicationSourceIdentity,
    role: ManagedLogicalSlotRole,
    database_name: String,
    creation_lsn: PgLsn,
    observation: LogicalSlotObservation,
    effective_role_oid: u32,
    advisory_lock_count: usize,
}

impl ManagedLogicalSlotReceipt {
    /// Returns the opaque identity of this exact successful creation attempt.
    #[must_use]
    pub const fn receipt_id(&self) -> ManagedLogicalSlotReceiptId {
        self.receipt_id
    }

    /// Returns the exact generation-qualified server target.
    #[must_use]
    pub const fn target(&self) -> &ManagedSlotTarget {
        &self.target
    }

    /// Returns the source identity checked immediately before creation.
    #[must_use]
    pub const fn source(&self) -> ReplicationSourceIdentity {
        self.source
    }

    /// Returns the managed slot role that was created.
    #[must_use]
    pub const fn role(&self) -> ManagedLogicalSlotRole {
        self.role
    }

    /// Returns the database name observed on the mutation connection.
    #[must_use]
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Returns `PostgreSQL`'s durable consistent point from the create response.
    #[must_use]
    pub const fn creation_lsn(&self) -> PgLsn {
        self.creation_lsn
    }

    /// Returns the exact post-create slot observation.
    ///
    /// Persistence and generation ownership are proven only at this bounded
    /// point by the successful mutation path represented by this receipt.
    #[must_use]
    pub const fn observation(&self) -> &LogicalSlotObservation {
        &self.observation
    }

    /// Returns the effective database-role OID preserved across the mutation.
    #[must_use]
    pub const fn effective_role_oid(&self) -> u32 {
        self.effective_role_oid
    }

    /// Returns the exact number of caller advisory-lock rows preserved.
    #[must_use]
    pub const fn advisory_lock_count(&self) -> usize {
        self.advisory_lock_count
    }
}

/// Opaque identity for one exact successful managed-slot creation attempt.
///
/// The value has no public constructor. It distinguishes a later recreation
/// even when `PostgreSQL` returns the same slot name and creation LSN.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ManagedLogicalSlotReceiptId(Uuid);

impl fmt::Debug for ManagedLogicalSlotReceiptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ManagedLogicalSlotReceiptId(<redacted>)")
    }
}

impl ManagedLogicalSlotReceiptId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Reconstructs a non-nil ID loaded from the trusted catalog boundary.
    pub(crate) fn from_uuid(value: Uuid) -> Option<Self> {
        (!value.is_nil()).then_some(Self(value))
    }

    /// Returns the UUID representation persisted in `shardschema`.
    pub(crate) const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Known result of a receipt-authorized drop attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedLogicalSlotDropOutcome {
    /// The exact slot was present, safely matched the receipt, and was removed.
    Dropped,
    /// The exact generation-qualified name was already absent before dispatch.
    AlreadyAbsent,
}

/// Point-in-time proof that an exact receipt-authorized slot was absent.
///
/// This value has no public constructor and is returned only after the drop
/// path verifies the exact source, role, session fence, and slot shape. It is
/// carried inside [`ManagedLogicalSlotDropFence`] while it can close a durable
/// catalog lifecycle. After that connection-bound fence is released, this
/// receipt is historical evidence only and cannot authorize catalog retirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedLogicalSlotDropReceipt {
    receipt_id: ManagedLogicalSlotReceiptId,
    target: ManagedSlotTarget,
    source: ReplicationSourceIdentity,
    role: ManagedLogicalSlotRole,
    database_name: String,
    outcome: ManagedLogicalSlotDropOutcome,
}

impl ManagedLogicalSlotDropReceipt {
    /// Returns the exact successful creation attempt proven absent.
    #[must_use]
    pub const fn receipt_id(&self) -> ManagedLogicalSlotReceiptId {
        self.receipt_id
    }

    /// Returns the exact generation-qualified target proven absent.
    #[must_use]
    pub const fn target(&self) -> &ManagedSlotTarget {
        &self.target
    }

    /// Returns the source lineage verified by the drop preflight.
    #[must_use]
    pub const fn source(&self) -> ReplicationSourceIdentity {
        self.source
    }

    /// Returns the managed role whose exact shape was checked.
    #[must_use]
    pub const fn role(&self) -> ManagedLogicalSlotRole {
        self.role
    }

    /// Returns the database name verified by the drop preflight.
    #[must_use]
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Returns whether the exact slot was dropped or already absent.
    #[must_use]
    pub const fn outcome(&self) -> ManagedLogicalSlotDropOutcome {
        self.outcome
    }
}

/// Connection-bound absence proof that still excludes managed same-name creation.
///
/// The canonical `shardschema` session holds pgshard's hidden per-target fence
/// from before the final slot observation until this value is released or
/// dropped. Every creation and deletion performed through this module uses that
/// registry. Direct SQL issued by a bypassing actor is outside that coordination
/// boundary.
///
/// A catalog lifecycle must borrow this value through its COMMIT and verify the
/// same backend afterward before treating the absence proof as durable.
pub struct ManagedLogicalSlotDropFence {
    receipt: ManagedLogicalSlotDropReceipt,
    client: Client,
    connection_task: ConnectionTask,
    backend_pid: NonZeroU32,
    fence_id: Uuid,
}

impl fmt::Debug for ManagedLogicalSlotDropFence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedLogicalSlotDropFence")
            .field("receipt", &self.receipt)
            .field("backend_pid", &self.backend_pid)
            .finish_non_exhaustive()
    }
}

impl ManagedLogicalSlotDropFence {
    fn new(
        receipt: ManagedLogicalSlotDropReceipt,
        client: Client,
        connection_task: ConnectionTask,
        backend_pid: NonZeroU32,
        fence_id: Uuid,
    ) -> Self {
        Self {
            receipt,
            client,
            connection_task,
            backend_pid,
            fence_id,
        }
    }

    /// Returns the exact point-in-time absence receipt protected by this fence.
    #[must_use]
    pub fn receipt(&self) -> &ManagedLogicalSlotDropReceipt {
        &self.receipt
    }

    /// Returns the canonical `shardschema` backend that currently owns the fence.
    ///
    /// This identifier is diagnostic only. Possessing it does not grant fence
    /// authority; callers must retain this value and pass its live session
    /// verification before making catalog retirement durable.
    #[must_use]
    pub const fn catalog_fence_backend_pid(&self) -> NonZeroU32 {
        self.backend_pid
    }

    /// Releases the hidden target fence after its caller no longer needs it.
    pub async fn release(self) -> ManagedLogicalSlotDropReceipt {
        let Self {
            receipt,
            client,
            connection_task,
            backend_pid: _,
            fence_id,
        } = self;
        let _ = client
            .query_one(
                RELEASE_TARGET_FENCE_SQL,
                &[&receipt.target().name().as_str(), &fence_id.to_string()],
            )
            .await;
        drop(client);
        connection_task.abort_and_wait().await;
        receipt
    }

    pub(crate) async fn verify_held_until(
        &self,
        deadline: Instant,
        duration: Duration,
    ) -> Result<(), ManagedLogicalSlotTargetFenceError> {
        timeout_at(deadline, set_statement_timeout(&self.client, deadline))
            .await
            .map_err(|_| ManagedLogicalSlotTargetFenceError::Timeout { duration })??;
        let row = timeout_at(
            deadline,
            self.client.query_one(
                VERIFY_TARGET_FENCE_SQL,
                &[
                    &self.receipt.target().name().as_str(),
                    &self.fence_id.to_string(),
                ],
            ),
        )
        .await
        .map_err(|_| ManagedLogicalSlotTargetFenceError::Timeout { duration })??;
        let backend_pid = positive_nonzero_u32(
            row.try_get(0)
                .map_err(ManagedLogicalSlotTargetFenceError::Postgres)?,
            "target_fence_backend_pid",
        )
        .map_err(|_| ManagedLogicalSlotTargetFenceError::BackendChanged)?;
        if backend_pid != self.backend_pid {
            return Err(ManagedLogicalSlotTargetFenceError::BackendChanged);
        }
        Ok(())
    }

    pub(crate) fn catalog_client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    pub(crate) const fn fence_id(&self) -> Uuid {
        self.fence_id
    }
}

/// Failure to prove that the same canonical `shardschema` session still holds a target fence.
#[derive(Debug, Error)]
pub enum ManagedLogicalSlotTargetFenceError {
    /// Fence liveness could not be checked before the operation deadline.
    #[error("managed logical-slot target-fence verification exceeded {duration:?}")]
    Timeout {
        /// Whole-operation deadline supplied by the catalog lifecycle.
        duration: Duration,
    },
    /// The canonical `shardschema` session failed while its fence should have remained held.
    #[error("managed logical-slot target-fence verification failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    /// The live connection no longer identifies the backend that acquired the fence.
    #[error("managed logical-slot target fence moved to another PostgreSQL backend")]
    BackendChanged,
}

/// Failure while binding a successful physical creation receipt to a consumer slot.
#[derive(Debug, Error)]
pub enum ManagedLogicalSlotCatalogActivationError {
    /// The deadline elapsed before `COMMIT` and the transaction was rolled back.
    #[error("managed logical-slot catalog activation for {target:?} exceeded {duration:?}")]
    OperationTimeout {
        /// Exact generation-qualified target whose transaction was rolled back.
        target: ManagedSlotTarget,
        /// Whole-operation timeout supplied by the caller.
        duration: Duration,
    },
    /// The bounded operation elapsed after dispatch may have begun.
    #[error(
        "managed logical-slot catalog activation outcome for {target:?} is unknown after {duration:?}"
    )]
    OutcomeUnknownDeadline {
        /// Exact generation-qualified target that must be reconciled.
        target: ManagedSlotTarget,
        /// Whole-operation timeout supplied by the caller.
        duration: Duration,
    },
    /// `COMMIT` may have succeeded before its response was lost.
    #[error("managed logical-slot catalog activation outcome for {target:?} is unknown: {source}")]
    OutcomeUnknownPostgres {
        /// Exact generation-qualified target that must be reconciled.
        target: ManagedSlotTarget,
        /// Failure observed at the commit boundary.
        #[source]
        source: tokio_postgres::Error,
    },
    /// A pre-commit operation failed and the catalog transaction was rolled back.
    #[error("managed logical-slot catalog activation was rejected: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    /// The transaction could not be explicitly rolled back after a known failure.
    #[error("managed logical-slot catalog activation rollback failed: {0}")]
    RollbackFailed(#[source] tokio_postgres::Error),
}

impl ManagedLogicalSlotCatalogActivationError {
    /// Returns whether the durable activation outcome needs exact catalog reconciliation.
    #[must_use]
    pub const fn outcome_is_unknown(&self) -> bool {
        matches!(
            self,
            Self::OutcomeUnknownDeadline { .. } | Self::OutcomeUnknownPostgres { .. }
        )
    }
}

/// Failure while making a consumer-slot absence proof durable in `shardschema`.
#[derive(Debug, Error)]
pub enum ManagedLogicalSlotCatalogRetirementError {
    /// The deadline elapsed before `COMMIT` and the transaction was rolled back.
    #[error("managed logical-slot catalog retirement for {target:?} exceeded {duration:?}")]
    OperationTimeout {
        /// Exact generation-qualified target whose transaction was rolled back.
        target: ManagedSlotTarget,
        /// Whole-operation timeout supplied by the caller.
        duration: Duration,
    },
    /// The bounded catalog operation elapsed before a known result was returned.
    #[error(
        "managed logical-slot catalog retirement outcome for {target:?} is unknown after {duration:?}"
    )]
    OutcomeUnknownDeadline {
        /// Exact generation-qualified target that must be reconciled.
        target: ManagedSlotTarget,
        /// Whole-operation timeout supplied by the caller.
        duration: Duration,
    },
    /// `COMMIT` may have succeeded before its response was lost.
    #[error("managed logical-slot catalog retirement outcome for {target:?} is unknown: {source}")]
    OutcomeUnknownPostgres {
        /// Exact generation-qualified target that must be reconciled.
        target: ManagedSlotTarget,
        /// Failure observed at the commit boundary.
        #[source]
        source: tokio_postgres::Error,
    },
    /// A pre-commit operation failed and the catalog transaction was rolled back.
    #[error("managed logical-slot catalog retirement was rejected: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    /// The transaction could not be explicitly rolled back after a known failure.
    #[error("managed logical-slot catalog retirement rollback failed: {0}")]
    RollbackFailed(#[source] tokio_postgres::Error),
    /// The canonical target fence was no longer provable across retirement.
    #[error(
        "managed logical-slot target fence for {target:?} was lost across catalog retirement: {source}"
    )]
    TargetFenceLost {
        /// Exact generation-qualified target whose absence must be reconciled.
        target: ManagedSlotTarget,
        /// Canonical `shardschema` session liveness failure.
        #[source]
        source: ManagedLogicalSlotTargetFenceError,
    },
}

impl ManagedLogicalSlotCatalogRetirementError {
    /// Returns true when the durable catalog outcome must be reloaded.
    #[must_use]
    pub const fn outcome_is_unknown(&self) -> bool {
        matches!(
            self,
            Self::OutcomeUnknownDeadline { .. }
                | Self::OutcomeUnknownPostgres { .. }
                | Self::TargetFenceLost { .. }
        )
    }
}

/// A receipt-authorized drop failure with explicit retry authority.
#[derive(Debug, Error)]
pub enum ManagedLogicalSlotDropError {
    /// No drop was dispatched, so the unchanged receipt is returned to its caller.
    #[error("managed logical-slot drop failed before dispatch: {source}")]
    BeforeDispatch {
        /// Sole process-local authority for another bounded cleanup attempt.
        receipt: Box<ManagedLogicalSlotReceipt>,
        /// Exact fail-closed preflight failure.
        #[source]
        source: LocalSlotMutationError,
    },
    /// A drop may already have taken effect; retrying with the old receipt is unsafe.
    #[error("managed logical-slot drop outcome is unknown: {0}")]
    OutcomeUnknown(#[source] LocalSlotMutationError),
}

impl ManagedLogicalSlotDropError {
    /// Returns the receipt only when no drop was dispatched.
    #[must_use]
    pub fn into_retry_receipt(self) -> Option<(ManagedLogicalSlotReceipt, LocalSlotMutationError)> {
        match self {
            Self::BeforeDispatch { receipt, source } => Some((*receipt, source)),
            Self::OutcomeUnknown(_) => None,
        }
    }

    /// Returns true only when `PostgreSQL` may already have applied the drop.
    #[must_use]
    pub const fn outcome_is_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown(_))
    }
}

/// Persistent operation whose response can be lost after the effect occurs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalSlotMutationOperation {
    /// Create a persistent logical slot.
    Create,
    /// Drop a persistent logical slot.
    Drop,
}

impl fmt::Display for LocalSlotMutationOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Create => "create",
            Self::Drop => "drop",
        })
    }
}

/// Exact reason a visible slot is unsafe for this mutation path.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ManagedLogicalSlotShapeProblem {
    /// The server row disappeared before postflight observation.
    #[error("slot is absent")]
    Missing,
    /// The result escaped the exact target predicate.
    #[error("slot name differs from the generation-qualified target")]
    WrongName,
    /// The slot belongs to another database.
    #[error("slot database OID is {observed}, expected {expected}")]
    WrongDatabase {
        /// Catalog-selected database OID.
        expected: u32,
        /// Server-reported slot database OID.
        observed: u32,
    },
    /// The slot does not use built-in `pgoutput`.
    #[error("slot does not use pgoutput")]
    WrongPlugin,
    /// The slot flags do not encode the requested role.
    #[error("slot role is {observed:?}, expected {expected}")]
    WrongRole {
        /// Role authorized for this exact catalog allocation.
        expected: ManagedLogicalSlotRole,
        /// Role encoded by `PostgreSQL`'s `failover` and `synced` flags.
        observed: LogicalSlotKind,
    },
    /// `PostgreSQL` reports a temporary slot.
    #[error("slot is temporary")]
    Temporary,
    /// Another backend owns the slot.
    #[error("slot is active")]
    Active,
    /// Prepared-transaction decoding is not enabled.
    #[error("slot does not enable two-phase decoding")]
    TwoPhaseDisabled,
    /// The immutable prepared-decoding boundary differs from creation.
    #[error("slot two-phase boundary is {observed:?}, expected {expected:?}")]
    WrongTwoPhaseBoundary {
        /// Immutable boundary returned by the controlled create.
        expected: PgLsn,
        /// Current `PostgreSQL` boundary, if present.
        observed: Option<PgLsn>,
    },
    /// A newly created slot was already invalidated.
    #[error("new slot is invalidated: {0:?}")]
    Invalidated(SlotInvalidation),
    /// A newly created slot did not retain its required WAL.
    #[error("new slot does not report retained WAL")]
    WalNotRetained,
    /// The slot has no usable confirmed-flush position.
    #[error("slot has no confirmed-flush LSN")]
    MissingConfirmedFlushLsn,
    /// Post-create progress differed from `PostgreSQL`'s create response.
    #[error("slot confirmed-flush LSN is {observed:?}, expected {expected:?}")]
    WrongInitialConfirmedFlushLsn {
        /// Consistent point returned by the create call.
        expected: PgLsn,
        /// Confirmed point visible in the postflight row.
        observed: PgLsn,
    },
    /// A previously created slot now reports progress before its creation point.
    #[error("slot confirmed-flush LSN {observed:?} precedes creation LSN {minimum:?}")]
    ConfirmedFlushLsnRegressed {
        /// Earliest possible progress from the controlled create.
        minimum: PgLsn,
        /// Current server-reported confirmed point.
        observed: PgLsn,
    },
}

/// Cause retained when `PostgreSQL` may already have applied a mutation.
#[derive(Debug, Error)]
pub enum LocalSlotMutationUnknownCause {
    /// The absolute client deadline elapsed after mutation dispatch began.
    #[error("the absolute operation deadline {duration:?} elapsed")]
    Deadline {
        /// Validated deadline applied to the complete operation.
        duration: Duration,
    },
    /// `PostgreSQL` or the protocol failed after mutation dispatch began.
    #[error("PostgreSQL returned an error after mutation dispatch: {0}")]
    Postgres(#[source] tokio_postgres::Error),
    /// The dedicated connection failed after mutation dispatch began.
    #[error("the dedicated PostgreSQL connection failed after mutation dispatch: {0}")]
    Connection(#[source] tokio_postgres::Error),
    /// The driver task failed after mutation dispatch began.
    #[error("the dedicated PostgreSQL connection task failed after mutation dispatch: {0}")]
    ConnectionTask(#[source] JoinError),
    /// The driver could not be reaped within the fixed local bound.
    #[error("the dedicated PostgreSQL connection cleanup exceeded {duration:?}")]
    ConnectionCleanupTimeout {
        /// Fixed local driver-cleanup bound.
        duration: Duration,
    },
    /// The create response named something other than the exact request target.
    #[error("PostgreSQL create response returned unexpected slot name {0:?}")]
    UnexpectedCreatedSlot(String),
    /// The create response did not contain one valid nonzero LSN.
    #[error("PostgreSQL create response returned invalid LSN {0:?}")]
    InvalidCreationLsn(String),
    /// The exact postflight slot row could not be interpreted safely.
    #[error("PostgreSQL returned an unsafe postflight slot row: {0}")]
    PostflightObservation(#[source] LocalSlotObservationError),
    /// The postflight row did not match the requested persistent slot.
    #[error("postflight slot validation failed: {0}")]
    PostflightShape(ManagedLogicalSlotShapeProblem),
    /// Endpoint source, role, or standby settings changed after dispatch.
    #[error("postflight source validation failed: {0}")]
    PostflightSource(String),
    /// A successful drop response was followed by a still-present exact name.
    #[error("the exact slot name remained present after the drop response")]
    SlotStillPresent,
}

/// Fail-closed local logical-slot mutation error.
#[derive(Debug, Error)]
pub enum LocalSlotMutationError {
    /// A `PostgreSQL` failure occurred before mutation dispatch.
    #[error(
        "PostgreSQL {operation} preflight for managed slot {target:?} failed before dispatch: {source}"
    )]
    PreflightPostgres {
        /// Mutation that was not yet dispatched.
        operation: LocalSlotMutationOperation,
        /// Exact generation-qualified target.
        target: ManagedSlotTarget,
        /// `PostgreSQL` or protocol failure.
        #[source]
        source: tokio_postgres::Error,
    },
    /// The absolute deadline elapsed before mutation dispatch.
    #[error(
        "PostgreSQL {operation} preflight for managed slot {target:?} exceeded {duration:?}; no mutation was dispatched"
    )]
    PreflightTimeout {
        /// Mutation that was not yet dispatched.
        operation: LocalSlotMutationOperation,
        /// Exact generation-qualified target.
        target: ManagedSlotTarget,
        /// Validated absolute client deadline.
        duration: Duration,
    },
    /// Server is older than the minimum supported release.
    #[error(
        "managed slot mutation requires PostgreSQL 18 or newer; observed server_version_num {0}"
    )]
    UnsupportedPostgresVersion(i32),
    /// The connected database is not UTF8.
    #[error("managed slot mutation requires UTF8; observed {0:?}")]
    WrongEncoding(String),
    /// The serialization connection did not target the authoritative catalog.
    #[error(
        "managed slot mutation target fencing requires the writable shardschema database; observed {0:?}"
    )]
    WrongCatalogFenceDatabase(String),
    /// A standby catalog connection can return stale authorization state.
    #[error("managed slot mutation target fencing requires writable shardschema")]
    CatalogFenceInRecovery,
    /// The catalog-fence backend changed while acquiring the target lock.
    #[error("managed slot mutation catalog-fence backend changed")]
    CatalogFenceBackendChanged,
    /// A pre-dispatch failure could not durably abandon its creation barrier.
    #[error(
        "managed slot target {target:?} retains an unresolved catalog creation attempt after {original}; cleanup failed: {source}"
    )]
    CatalogCreationAttemptUnresolved {
        /// Exact generation-qualified target that must be reconciled.
        target: ManagedSlotTarget,
        /// Pre-dispatch failure that required durable attempt cleanup.
        original: Box<LocalSlotMutationError>,
        /// Catalog failure while releasing the fence or abandoning the attempt.
        #[source]
        source: tokio_postgres::Error,
    },
    /// Bounded cleanup could not prove that the pre-dispatch barrier was abandoned.
    #[error(
        "managed slot target {target:?} creation-attempt cleanup after {original} exceeded {duration:?}; reconcile the durable attempt before retrying"
    )]
    CatalogCreationAttemptCleanupTimeout {
        /// Exact generation-qualified target that must be reconciled.
        target: ManagedSlotTarget,
        /// Pre-dispatch failure that required durable attempt cleanup.
        original: Box<LocalSlotMutationError>,
        /// Cleanup bound.
        duration: Duration,
    },
    /// The generation already has a durable create whose outcome must be reconciled.
    #[error(
        "managed slot target {0:?} already has an unresolved catalog creation attempt; reconcile it before retrying"
    )]
    CatalogCreationAttemptPending(ManagedSlotTarget),
    /// No permanent catalog allocation authorizes the requested target.
    #[error("managed slot target {0:?} has no exact shardschema allocation")]
    CatalogAuthorizationMissing(ManagedSlotTarget),
    /// More than one permanent allocation claimed one supposedly unique target.
    #[error("managed slot target {0:?} has duplicate shardschema allocations")]
    DuplicateCatalogAuthorization(ManagedSlotTarget),
    /// A catalog authorization row could not be represented safely.
    #[error("managed slot shardschema authorization field {0} is invalid")]
    InvalidCatalogAuthorizationField(&'static str),
    /// The durable allocation no longer matches the requested target or source.
    #[error("managed slot target {0:?} no longer matches its shardschema allocation")]
    CatalogAuthorizationIdentityChanged(ManagedSlotTarget),
    /// The durable lifecycle no longer permits this local mutation.
    #[error(
        "managed slot target {target:?} is in catalog state {state:?}, which does not authorize {operation}"
    )]
    CatalogAuthorizationStateChanged {
        /// Persistent mutation that was not dispatched.
        operation: LocalSlotMutationOperation,
        /// Exact target whose lifecycle changed.
        target: ManagedSlotTarget,
        /// Current durable lifecycle label.
        state: String,
    },
    /// The cleanup capability does not match the durable probe retirement.
    #[error("managed slot target {0:?} cleanup receipt no longer matches shardschema")]
    CatalogCleanupReceiptChanged(ManagedSlotTarget),
    /// The request was built from an older catalog snapshot.
    #[error(
        "managed slot target {target:?} expected catalog epoch {expected:?}, observed {observed:?}"
    )]
    StaleCatalogAuthorization {
        /// Exact target that must be reloaded.
        target: ManagedSlotTarget,
        /// Epoch carried by the request.
        expected: pgshard_types::CatalogEpoch,
        /// Current authoritative epoch after target-fence acquisition.
        observed: pgshard_types::CatalogEpoch,
    },
    /// The target connection used another logical database than its allocation.
    #[error(
        "managed slot mutation connected to database {observed:?}, expected catalog database {expected:?}"
    )]
    CatalogDatabaseMismatch {
        /// Database name stored in the durable allocation.
        expected: String,
        /// Database name observed on the mutation connection.
        observed: String,
    },
    /// Backend, session role, effective role, or advisory-lock fence changed.
    #[error("managed slot mutation session identity or advisory-lock fence changed")]
    SessionFenceChanged,
    /// The caller held more advisory locks than this primitive can compare safely.
    #[error("managed slot mutation supports at most {maximum} caller advisory-lock rows")]
    TooManyAdvisoryLocks {
        /// Hard upper bound on caller advisory-lock rows.
        maximum: usize,
    },
    /// `PostgreSQL` returned a malformed nonzero identity component.
    #[error("PostgreSQL returned invalid managed-slot source field {0}")]
    InvalidSourceField(&'static str),
    /// Observable server identity differs from the catalog-selected source.
    #[error(
        "managed-slot source mismatch: expected system {expected_system_identifier}, timeline {expected_timeline}, database OID {expected_database_oid}; observed system {observed_system_identifier}, timeline {observed_timeline}, database OID {observed_database_oid}"
    )]
    SourceMismatch {
        /// Catalog-selected cluster system identifier.
        expected_system_identifier: u64,
        /// Catalog-selected timeline.
        expected_timeline: u32,
        /// Catalog-selected database OID.
        expected_database_oid: u32,
        /// Connected cluster system identifier.
        observed_system_identifier: u64,
        /// Connected endpoint timeline.
        observed_timeline: u32,
        /// Connected database OID.
        observed_database_oid: u32,
    },
    /// The endpoint recovery state is incompatible with the requested role.
    #[error("managed {role} requires recovery state {expected:?}; observed {observed:?}")]
    WrongRecoveryState {
        /// Requested managed logical-slot role.
        role: ManagedLogicalSlotRole,
        /// Required `PostgreSQL` recovery state.
        expected: RecoveryState,
        /// Observed `PostgreSQL` recovery state.
        observed: RecoveryState,
    },
    /// Logical decoding is not enabled on the endpoint.
    #[error("managed {0} requires wal_level=logical")]
    InsufficientWalLevel(ManagedLogicalSlotRole),
    /// Standby decoding cannot safely protect its catalog horizon.
    #[error("managed standby-local decoding requires hot_standby_feedback=on")]
    HotStandbyFeedbackDisabled,
    /// Continuous failover-slot synchronization is not enabled.
    #[error("managed standby-local decoding requires sync_replication_slots=on")]
    SlotSynchronizationDisabled,
    /// The correlated multi-server proof expired before creation dispatch.
    #[error("managed standby-local decoder requires a fresh correlated replication path")]
    StandbyPathExpired,
    /// The live receiver is absent or not streaming from the correlated path.
    #[error("managed standby-local decoder requires its correlated WAL receiver to be streaming")]
    WalReceiverNotStreaming,
    /// The live receiver uses another physical slot.
    #[error("managed standby-local decoder receiver does not use the correlated physical slot")]
    PrimarySlotNameMismatch,
    /// The live receiver moved to another backend generation before mutation.
    #[error("managed standby-local decoder WAL receiver changed after path correlation")]
    WalReceiverChanged,
    /// The live receiver is already receiving another source timeline.
    #[error("managed standby-local decoder receiver timeline is {observed}, expected {expected}")]
    WalReceiverTimelineMismatch {
        /// Catalog and correlated-primary timeline.
        expected: u32,
        /// Receiver's current source timeline.
        observed: u32,
    },
    /// Feedback reporting is disabled or exceeds the correlated safety limit.
    #[error("managed standby-local decoder feedback interval {observed:?} exceeds {maximum:?}")]
    FeedbackReportingIntervalUnsafe {
        /// Current effective receiver reporting interval.
        observed: Duration,
        /// Maximum interval carried by the correlated proof.
        maximum: Duration,
    },
    /// The correlated continuous slot-sync worker is no longer observable.
    #[error("managed standby-local decoder slot-sync worker is unavailable")]
    SlotSyncWorkerMissing,
    /// The continuous slot-sync worker restarted after path correlation.
    #[error("managed standby-local decoder slot-sync worker changed after path correlation")]
    SlotSyncWorkerChanged,
    /// Create preflight found any exact cluster-wide name collision.
    #[error("managed slot create target {0:?} is already occupied")]
    TargetOccupied(ManagedSlotTarget),
    /// Receipt-authorized drop found a changed or unsafe target shape.
    #[error("managed slot drop target {target:?} is unsafe: {problem}")]
    UnsafeDropTarget {
        /// Exact generation-qualified target.
        target: ManagedSlotTarget,
        /// Shape mismatch that prevented deletion.
        problem: ManagedLogicalSlotShapeProblem,
    },
    /// Receipt-authorized drop could not safely interpret the exact row.
    #[error("managed slot drop target {target:?} returned an unsafe row: {source}")]
    UnsafeDropObservation {
        /// Exact generation-qualified target.
        target: ManagedSlotTarget,
        /// Fail-closed `PostgreSQL` 18 row parsing failure.
        #[source]
        source: LocalSlotObservationError,
    },
    /// `PostgreSQL` may already have applied the create or drop.
    #[error(
        "PostgreSQL {operation} outcome for managed slot {target:?} is unknown; observe and reconcile the exact target before retrying: {source}"
    )]
    OutcomeUnknown {
        /// Mutation that may already have taken effect.
        operation: LocalSlotMutationOperation,
        /// Exact generation-qualified target that must be reconciled.
        target: ManagedSlotTarget,
        /// Failure observed after dispatch began.
        #[source]
        source: LocalSlotMutationUnknownCause,
    },
}

impl LocalSlotMutationError {
    /// Returns true only when `PostgreSQL` may already have applied the mutation.
    #[must_use]
    pub const fn outcome_is_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MutationIdentity {
    target: ManagedSlotTarget,
    source: ReplicationSourceIdentity,
    role: ManagedLogicalSlotRole,
    standby_path: Option<StandbyMutationPath>,
}

impl From<ManagedLogicalSlotCreateRequest> for MutationIdentity {
    fn from(request: ManagedLogicalSlotCreateRequest) -> Self {
        Self {
            target: request.target,
            source: request.source,
            role: request.role,
            standby_path: request.standby_path,
        }
    }
}

struct MutationContext {
    operation: LocalSlotMutationOperation,
    identity: MutationIdentity,
    duration: Duration,
    deadline: Instant,
}

impl MutationContext {
    fn new(
        operation: LocalSlotMutationOperation,
        identity: MutationIdentity,
        timeout: CatalogOperationTimeout,
    ) -> Self {
        let duration = timeout.get();
        Self {
            operation,
            identity,
            duration,
            deadline: Instant::now() + duration,
        }
    }

    fn preflight_postgres(&self, source: tokio_postgres::Error) -> LocalSlotMutationError {
        LocalSlotMutationError::PreflightPostgres {
            operation: self.operation,
            target: self.identity.target.clone(),
            source,
        }
    }

    fn preflight_timeout(&self) -> LocalSlotMutationError {
        LocalSlotMutationError::PreflightTimeout {
            operation: self.operation,
            target: self.identity.target.clone(),
            duration: self.duration,
        }
    }

    fn preflight_deadline(&self) -> Instant {
        self.identity
            .standby_path
            .as_ref()
            .map_or(self.deadline, |path| self.deadline.min(path.valid_until))
    }

    fn preflight_deadline_error(&self) -> LocalSlotMutationError {
        if self
            .identity
            .standby_path
            .as_ref()
            .is_some_and(|path| path.valid_until <= self.deadline)
        {
            LocalSlotMutationError::StandbyPathExpired
        } else {
            self.preflight_timeout()
        }
    }

    fn ensure_dispatch_deadline(&self) -> Result<(), LocalSlotMutationError> {
        if Instant::now() >= self.preflight_deadline() {
            Err(self.preflight_deadline_error())
        } else {
            Ok(())
        }
    }

    fn unknown(&self, source: LocalSlotMutationUnknownCause) -> LocalSlotMutationError {
        LocalSlotMutationError::OutcomeUnknown {
            operation: self.operation,
            target: self.identity.target.clone(),
            source,
        }
    }
}

async fn bounded_preflight<F, V>(
    context: &MutationContext,
    operation: F,
) -> Result<V, LocalSlotMutationError>
where
    F: Future<Output = Result<V, LocalSlotMutationError>>,
{
    match timeout_at(context.preflight_deadline(), operation).await {
        Ok(result) => result,
        Err(_) => Err(context.preflight_deadline_error()),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MutationSessionIdentity {
    backend_pid: NonZeroU32,
    session_role_oid: u32,
    effective_role_oid: u32,
    advisory_locks: Vec<String>,
}

struct PreparedServer {
    database_name: String,
    session: MutationSessionIdentity,
    caller_advisory_lock_count: usize,
}

struct PreparedCatalogFence {
    backend_pid: NonZeroU32,
    fence_id: Uuid,
    database_name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogAllocationKind {
    Probe,
    Consumer,
}

struct CatalogAuthorization {
    kind: CatalogAllocationKind,
    state: String,
    target: ManagedSlotTarget,
    role: ManagedLogicalSlotRole,
    database_name: String,
    source: ReplicationSourceIdentity,
    creation_receipt_id: Option<ManagedLogicalSlotReceiptId>,
    cleanup_receipt_id: Option<ManagedLogicalSlotReceiptId>,
    creation_attempt_state: Option<String>,
    restore_state: String,
    shard_state: String,
    attachment_state: Option<String>,
    consumer_shard_state: Option<String>,
    consumer_state: Option<String>,
    logical_database_state: Option<String>,
}

struct PreparedCreate {
    server: PreparedServer,
    statement: Statement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogCreationAttemptStart {
    Persisted,
    TargetBusy,
}

enum PreparedDrop {
    Absent {
        server: PreparedServer,
    },
    Present {
        server: PreparedServer,
        statement: Statement,
    },
}

struct DropSessions {
    catalog_client: Client,
    catalog_connection_task: ConnectionTask,
    mutation_client: Client,
    mutation_connection_task: ConnectionTask,
}

struct ObservedSource {
    system_identifier: u64,
    timeline: u32,
    database_oid: u32,
    recovery: RecoveryState,
    wal_level: String,
    hot_standby_feedback: bool,
    wal_receiver_status_interval: Duration,
    sync_replication_slots: bool,
    primary_slot_name: Option<ReplicationSlotName>,
    wal_receiver_pid: Option<NonZeroU32>,
    wal_receiver_streaming: bool,
    wal_receiver_slot_name: Option<ReplicationSlotName>,
    wal_receiver_received_timeline: Option<u32>,
    slot_sync_worker: Option<LocalPostgresBackendIdentity>,
}

struct ObservedWalReceiver {
    pid: Option<NonZeroU32>,
    streaming: bool,
    slot_name: Option<ReplicationSlotName>,
    received_timeline: Option<u32>,
}

/// Creates one persistent `pgoutput` slot and verifies its exact postflight row.
///
/// The target must be absent, the source must match the connected `PostgreSQL`
/// 18 endpoint, and the endpoint must have the requested primary or standby
/// role. The first connection must target the authoritative writable
/// `shardschema` database. It holds the hidden catalog-backed target fence and
/// revalidates the durable allocation after any retry; the second connection
/// performs the slot mutation against the allocation's exact database. A
/// standby-local request can only be constructed by consuming a fresh
/// [`CorrelatedStandbyReplicationPath`]. Its live receiver timeline, physical
/// slot, feedback interval, feedback setting and slot-sync-worker generation
/// are rechecked around dispatch. The caller's effective role and advisory
/// locks are preserved and verified rather than reset.
///
/// # Errors
///
/// Returns a preflight error when no mutation was dispatched. Every error once
/// dispatch begins is [`LocalSlotMutationError::OutcomeUnknown`], including a
/// timeout or lost connection. Such an error must be reconciled by observation
/// and must not be retried blindly. Cancelling this future has the same
/// reconciliation requirement because the caller cannot know whether dispatch
/// had already occurred.
pub async fn create_managed_logical_slot<CS, CT, S, T>(
    catalog_client: Client,
    catalog_connection: Connection<CS, CT>,
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    request: ManagedLogicalSlotCreateRequest,
) -> Result<ManagedLogicalSlotReceipt, LocalSlotMutationError>
where
    CS: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    CT: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let context = MutationContext::new(
        LocalSlotMutationOperation::Create,
        request.into(),
        operation_timeout,
    );
    // Persist capability identity before the target endpoint can observe a
    // mutation. The durable pending attempt survives loss of this process or
    // its advisory-lock backend and prevents the allocation from retiring
    // beneath an in-flight create.
    let receipt_id = ManagedLogicalSlotReceiptId::new();
    let catalog_connection_task = ConnectionTask::new(tokio::spawn(catalog_connection));
    let connection_task = ConnectionTask::new(tokio::spawn(connection));
    let catalog_fence = match bounded_preflight(
        &context,
        prepare_catalog_create_fence(&catalog_client, &context, receipt_id),
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(client);
            connection_task.abort_and_wait().await;
            let error = if matches!(
                error,
                LocalSlotMutationError::CatalogCreationAttemptPending(_)
            ) {
                error
            } else {
                abandon_after_known_create_failure(&catalog_client, &context, receipt_id, error)
                    .await
            };
            drop(catalog_client);
            catalog_connection_task.abort_and_wait().await;
            return Err(error);
        }
    };
    let prepared = match bounded_preflight(&context, prepare_create(&client, &context)).await {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(client);
            connection_task.abort_and_wait().await;
            let error =
                abandon_after_known_create_failure(&catalog_client, &context, receipt_id, error)
                    .await;
            drop(catalog_client);
            catalog_connection_task.abort_and_wait().await;
            return Err(error);
        }
    };
    if prepared.server.database_name != catalog_fence.database_name {
        let error = LocalSlotMutationError::CatalogDatabaseMismatch {
            expected: catalog_fence.database_name,
            observed: prepared.server.database_name,
        };
        drop(client);
        connection_task.abort_and_wait().await;
        let error =
            abandon_after_known_create_failure(&catalog_client, &context, receipt_id, error).await;
        drop(catalog_client);
        catalog_connection_task.abort_and_wait().await;
        return Err(error);
    }
    let result = timeout_at(
        context.deadline,
        create_at_dispatch_boundary(&client, &context, prepared, receipt_id),
    )
    .await;
    let result = finish_mutation(client, connection_task, &context, result).await;
    let result = match result {
        Err(error) if !error.outcome_is_unknown() => {
            Err(
                abandon_after_known_create_failure(&catalog_client, &context, receipt_id, error)
                    .await,
            )
        }
        result => result,
    };
    drop(catalog_client);
    catalog_connection_task.abort_and_wait().await;
    result
}

/// Drops only the inactive exact slot represented by a successful create receipt.
///
/// An absent target is an idempotent known result. Any changed plugin,
/// database, role, prepared-decoding boundary, activity, or progress fails
/// before dispatch. A failed or timed-out drop after dispatch has an unknown
/// persistent outcome and must be observed before reconciliation. A known
/// absence returns the canonical `shardschema` connection-bound target fence;
/// callers must retain it through any catalog COMMIT that makes the absence
/// durable. The second connection performs deletion against the allocation's
/// exact database.
///
/// # Errors
///
/// A fail-closed preflight error returns the unchanged receipt through
/// [`ManagedLogicalSlotDropError::BeforeDispatch`]. An error after dispatch is
/// explicitly outcome-unknown and carries no reusable receipt. Cancelling this
/// future likewise requires exact-target reconciliation.
pub async fn drop_managed_logical_slot<CS, CT, S, T>(
    catalog_client: Client,
    catalog_connection: Connection<CS, CT>,
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    receipt: ManagedLogicalSlotReceipt,
) -> Result<ManagedLogicalSlotDropFence, ManagedLogicalSlotDropError>
where
    CS: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    CT: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let creation_lsn = receipt.creation_lsn;
    let identity = MutationIdentity {
        target: receipt.target.clone(),
        source: receipt.source,
        role: receipt.role,
        standby_path: None,
    };
    let context = MutationContext::new(
        LocalSlotMutationOperation::Drop,
        identity,
        operation_timeout,
    );
    let catalog_connection_task = ConnectionTask::new(tokio::spawn(catalog_connection));
    let connection_task = ConnectionTask::new(tokio::spawn(connection));
    let catalog_fence = match bounded_preflight(
        &context,
        prepare_catalog_fence(&catalog_client, &context, Some(receipt.receipt_id)),
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(client);
            connection_task.abort_and_wait().await;
            drop(catalog_client);
            catalog_connection_task.abort_and_wait().await;
            return Err(ManagedLogicalSlotDropError::BeforeDispatch {
                receipt: Box::new(receipt),
                source: error,
            });
        }
    };
    let prepared =
        match bounded_preflight(&context, prepare_drop(&client, &context, creation_lsn)).await {
            Ok(present) => present,
            Err(error) => {
                drop(client);
                connection_task.abort_and_wait().await;
                drop(catalog_client);
                catalog_connection_task.abort_and_wait().await;
                return Err(ManagedLogicalSlotDropError::BeforeDispatch {
                    receipt: Box::new(receipt),
                    source: error,
                });
            }
        };
    finish_prepared_drop(
        DropSessions {
            catalog_client,
            catalog_connection_task,
            mutation_client: client,
            mutation_connection_task: connection_task,
        },
        &context,
        catalog_fence,
        prepared,
        receipt,
    )
    .await
}

/// Atomically binds an exact physical creation receipt to its consumer-slot allocation.
///
/// The receipt capability is supplied by the caller and is never reloaded from
/// `shardschema`. A failed `COMMIT` response is outcome-unknown and must be
/// reconciled with the same receipt before any retry.
///
/// # Errors
///
/// Returns a known rejection when the receipt, allocation, or activation
/// boundary does not match. Deadline and `COMMIT` response loss are reported as
/// outcome-unknown.
pub async fn activate_managed_consumer_slot<S, T>(
    mut client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    receipt: &ManagedLogicalSlotReceipt,
) -> Result<(), ManagedLogicalSlotCatalogActivationError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let duration = operation_timeout.get();
    let deadline = Instant::now() + duration;
    let target = receipt.target().clone();
    let connection_task = ConnectionTask::new(tokio::spawn(connection));
    let result = timeout_at(
        deadline,
        execute_consumer_catalog_activation(&mut client, deadline, duration, receipt),
    )
    .await
    .unwrap_or_else(|_| {
        Err(ManagedLogicalSlotCatalogActivationError::OutcomeUnknownDeadline { target, duration })
    });
    drop(client);
    connection_task.abort_and_wait().await;
    result
}

async fn execute_consumer_catalog_activation(
    client: &mut Client,
    deadline: Instant,
    duration: Duration,
    receipt: &ManagedLogicalSlotReceipt,
) -> Result<(), ManagedLogicalSlotCatalogActivationError> {
    client.batch_execute("DISCARD ALL").await?;
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    if let Err(source) = set_catalog_retirement_timeouts(&transaction, deadline).await {
        return rollback_consumer_catalog_activation(transaction, source.into()).await;
    }
    let generation = receipt.target().generation().as_uuid().to_string();
    let receipt_id = receipt.receipt_id().as_uuid().to_string();
    let creation_lsn = receipt.creation_lsn();
    let consistent_point = format!(
        "{:X}/{:X}",
        creation_lsn.0 >> 32,
        creation_lsn.0 & u64::from(u32::MAX)
    );
    if let Err(source) = transaction
        .query_one(
            ACTIVATE_CATALOG_CONSUMER_SLOT_SQL,
            &[&generation, &receipt_id, &consistent_point],
        )
        .await
    {
        return rollback_consumer_catalog_activation(transaction, source.into()).await;
    }
    if let Err(source) = transaction
        .batch_execute(DISABLE_LOCAL_STATEMENT_TIMEOUT_SQL)
        .await
    {
        return rollback_consumer_catalog_activation(transaction, source.into()).await;
    }
    if Instant::now() >= deadline {
        return rollback_consumer_catalog_activation(
            transaction,
            ManagedLogicalSlotCatalogActivationError::OperationTimeout {
                target: receipt.target().clone(),
                duration,
            },
        )
        .await;
    }
    transaction.commit().await.map_err(|source| {
        ManagedLogicalSlotCatalogActivationError::OutcomeUnknownPostgres {
            target: receipt.target().clone(),
            source,
        }
    })
}

async fn rollback_consumer_catalog_activation<T>(
    transaction: Transaction<'_>,
    error: ManagedLogicalSlotCatalogActivationError,
) -> Result<T, ManagedLogicalSlotCatalogActivationError> {
    transaction
        .rollback()
        .await
        .map_err(ManagedLogicalSlotCatalogActivationError::RollbackFailed)?;
    Err(error)
}

/// Commits consumer-slot retirement while the exact physical absence remains fenced.
///
/// The hidden creation receipt and generation-qualified target are taken from
/// `absence`; callers cannot substitute catalog authority. The transaction is
/// executed by the same canonical `shardschema` backend that has held the hidden
/// target fence since the final physical observation. Its opaque fence ID is
/// checked on both sides of `COMMIT` and remains owned by `absence` afterward.
///
/// # Errors
///
/// A database rejection before `COMMIT` is returned as a known rolled-back
/// result. A deadline, failed `COMMIT` response, or lost fence is explicitly
/// outcome-unknown and requires loading the exact durable generation before
/// any further reconciliation.
pub async fn complete_managed_consumer_slot_retirement(
    operation_timeout: CatalogOperationTimeout,
    absence: &mut ManagedLogicalSlotDropFence,
) -> Result<(), ManagedLogicalSlotCatalogRetirementError> {
    let duration = operation_timeout.get();
    let deadline = Instant::now() + duration;
    let target = absence.receipt().target().clone();
    let creation_receipt_id = absence.receipt().receipt_id().as_uuid().to_string();
    let fence_id = absence.fence_id().to_string();

    absence
        .verify_held_until(deadline, duration)
        .await
        .map_err(
            |source| ManagedLogicalSlotCatalogRetirementError::TargetFenceLost {
                target: target.clone(),
                source,
            },
        )?;
    let result = timeout_at(
        deadline,
        execute_consumer_catalog_retirement(
            absence.catalog_client_mut(),
            deadline,
            duration,
            &target,
            &creation_receipt_id,
            &fence_id,
        ),
    )
    .await
    .unwrap_or_else(|_| {
        Err(
            ManagedLogicalSlotCatalogRetirementError::OutcomeUnknownDeadline {
                target: target.clone(),
                duration,
            },
        )
    });
    let fence_result = absence.verify_held_until(deadline, duration).await;
    match (result, fence_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (_, Err(source)) => {
            Err(ManagedLogicalSlotCatalogRetirementError::TargetFenceLost { target, source })
        }
    }
}

async fn execute_consumer_catalog_retirement(
    client: &mut Client,
    deadline: Instant,
    duration: Duration,
    target: &ManagedSlotTarget,
    creation_receipt_id: &str,
    fence_id: &str,
) -> Result<(), ManagedLogicalSlotCatalogRetirementError> {
    let slot_generation = target.generation().as_uuid().to_string();
    let slot_name = target.name().as_str();
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await
        .map_err(ManagedLogicalSlotCatalogRetirementError::Postgres)?;
    if let Err(source) = set_catalog_retirement_timeouts(&transaction, deadline).await {
        return rollback_consumer_catalog_retirement(transaction, source.into()).await;
    }
    if let Err(source) = transaction
        .query_one(
            COMPLETE_CATALOG_CONSUMER_RETIREMENT_SQL,
            &[
                &slot_generation.as_str(),
                &slot_name,
                &creation_receipt_id,
                &fence_id,
            ],
        )
        .await
    {
        return rollback_consumer_catalog_retirement(transaction, source.into()).await;
    }
    if let Err(source) = transaction
        .batch_execute(DISABLE_LOCAL_STATEMENT_TIMEOUT_SQL)
        .await
    {
        return rollback_consumer_catalog_retirement(transaction, source.into()).await;
    }
    if Instant::now() >= deadline {
        return rollback_consumer_catalog_retirement(
            transaction,
            ManagedLogicalSlotCatalogRetirementError::OperationTimeout {
                target: target.clone(),
                duration,
            },
        )
        .await;
    }
    transaction.commit().await.map_err(|source| {
        ManagedLogicalSlotCatalogRetirementError::OutcomeUnknownPostgres {
            target: target.clone(),
            source,
        }
    })
}

async fn rollback_consumer_catalog_retirement<T>(
    transaction: Transaction<'_>,
    error: ManagedLogicalSlotCatalogRetirementError,
) -> Result<T, ManagedLogicalSlotCatalogRetirementError> {
    transaction
        .rollback()
        .await
        .map_err(ManagedLogicalSlotCatalogRetirementError::RollbackFailed)?;
    Err(error)
}

async fn set_catalog_retirement_timeouts(
    transaction: &Transaction<'_>,
    deadline: Instant,
) -> Result<(), tokio_postgres::Error> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let statement = remaining
        .saturating_sub(SERVER_STATEMENT_TIMEOUT_HEADROOM)
        .max(Duration::from_millis(1));
    let transaction_timeout = remaining.saturating_add(SERVER_TRANSACTION_TIMEOUT_GRACE);
    let statement = postgres_milliseconds(statement);
    let transaction_timeout = postgres_milliseconds(transaction_timeout);
    transaction
        .query_one(
            SET_LOCAL_CATALOG_RETIREMENT_TIMEOUTS_SQL,
            &[&statement, &transaction_timeout],
        )
        .await?;
    Ok(())
}

async fn finish_prepared_drop(
    sessions: DropSessions,
    context: &MutationContext,
    catalog_fence: PreparedCatalogFence,
    prepared: PreparedDrop,
    receipt: ManagedLogicalSlotReceipt,
) -> Result<ManagedLogicalSlotDropFence, ManagedLogicalSlotDropError> {
    let DropSessions {
        catalog_client,
        catalog_connection_task,
        mutation_client: client,
        mutation_connection_task: connection_task,
    } = sessions;
    let mutation_database = prepared_drop_database(&prepared);
    if mutation_database != catalog_fence.database_name {
        let error = LocalSlotMutationError::CatalogDatabaseMismatch {
            expected: catalog_fence.database_name,
            observed: mutation_database.to_owned(),
        };
        drop(client);
        connection_task.abort_and_wait().await;
        drop(catalog_client);
        catalog_connection_task.abort_and_wait().await;
        return Err(ManagedLogicalSlotDropError::BeforeDispatch {
            receipt: Box::new(receipt),
            source: error,
        });
    }
    match prepared {
        PreparedDrop::Absent { .. } => {
            drop(client);
            connection_task.abort_and_wait().await;
            Ok(ManagedLogicalSlotDropFence::new(
                drop_receipt(receipt, ManagedLogicalSlotDropOutcome::AlreadyAbsent),
                catalog_client,
                catalog_connection_task,
                catalog_fence.backend_pid,
                catalog_fence.fence_id,
            ))
        }
        PreparedDrop::Present { server, statement } => {
            let result = timeout_at(
                context.deadline,
                drop_at_dispatch_boundary(&client, context, &server, &statement),
            )
            .await;
            match result {
                Ok(Ok(outcome)) => {
                    drop(client);
                    connection_task.abort_and_wait().await;
                    Ok(ManagedLogicalSlotDropFence::new(
                        drop_receipt(receipt, outcome),
                        catalog_client,
                        catalog_connection_task,
                        catalog_fence.backend_pid,
                        catalog_fence.fence_id,
                    ))
                }
                Ok(Err(source)) => {
                    drop(client);
                    connection_task.abort_and_wait().await;
                    drop(catalog_client);
                    catalog_connection_task.abort_and_wait().await;
                    classify_drop_failure(receipt, source)
                }
                Err(_) => {
                    drop(client);
                    connection_task.abort_and_wait().await;
                    drop(catalog_client);
                    catalog_connection_task.abort_and_wait().await;
                    Err(ManagedLogicalSlotDropError::OutcomeUnknown(
                        context.unknown(LocalSlotMutationUnknownCause::Deadline {
                            duration: context.duration,
                        }),
                    ))
                }
            }
        }
    }
}

fn prepared_drop_database(prepared: &PreparedDrop) -> &str {
    match prepared {
        PreparedDrop::Absent { server } | PreparedDrop::Present { server, .. } => {
            &server.database_name
        }
    }
}

fn classify_drop_failure(
    receipt: ManagedLogicalSlotReceipt,
    source: LocalSlotMutationError,
) -> Result<ManagedLogicalSlotDropFence, ManagedLogicalSlotDropError> {
    if source.outcome_is_unknown() {
        Err(ManagedLogicalSlotDropError::OutcomeUnknown(source))
    } else {
        Err(ManagedLogicalSlotDropError::BeforeDispatch {
            receipt: Box::new(receipt),
            source,
        })
    }
}

fn drop_receipt(
    receipt: ManagedLogicalSlotReceipt,
    outcome: ManagedLogicalSlotDropOutcome,
) -> ManagedLogicalSlotDropReceipt {
    ManagedLogicalSlotDropReceipt {
        receipt_id: receipt.receipt_id,
        target: receipt.target,
        source: receipt.source,
        role: receipt.role,
        database_name: receipt.database_name,
        outcome,
    }
}

async fn finish_mutation<V>(
    client: Client,
    connection_task: ConnectionTask,
    context: &MutationContext,
    result: Result<Result<V, LocalSlotMutationError>, tokio::time::error::Elapsed>,
) -> Result<V, LocalSlotMutationError> {
    drop(client);
    match result {
        Ok(Ok(receipt)) => connection_task
            .finish(receipt)
            .await
            .map_err(|error| context.unknown(error.into())),
        Ok(Err(error)) => {
            connection_task.abort_and_wait().await;
            Err(error)
        }
        Err(_) => {
            connection_task.abort_and_wait().await;
            Err(context.unknown(LocalSlotMutationUnknownCause::Deadline {
                duration: context.duration,
            }))
        }
    }
}

async fn prepare_catalog_fence(
    client: &Client,
    context: &MutationContext,
    cleanup_receipt_id: Option<ManagedLogicalSlotReceiptId>,
) -> Result<PreparedCatalogFence, LocalSlotMutationError> {
    client
        .batch_execute("DISCARD ALL")
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    set_statement_timeout(client, context.preflight_deadline())
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let before = client
        .query_one(CATALOG_FENCE_REQUIREMENTS_SQL, &[])
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let backend_pid = validate_catalog_fence_requirements(&before, context)?;

    let fence_id = acquire_catalog_target_fence(client, context, backend_pid).await?;

    set_statement_timeout(client, context.preflight_deadline())
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let after = client
        .query_one(CATALOG_FENCE_REQUIREMENTS_SQL, &[])
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let after_backend_pid = validate_catalog_fence_requirements(&after, context)?;
    if after_backend_pid != backend_pid {
        return Err(LocalSlotMutationError::CatalogFenceBackendChanged);
    }

    set_statement_timeout(client, context.preflight_deadline())
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let generation = context.identity.target.generation().as_uuid().to_string();
    let cleanup_receipt_id = cleanup_receipt_id
        .expect("drop catalog preflight always carries the exact cleanup receipt");
    let cleanup_receipt_text = cleanup_receipt_id.as_uuid().to_string();
    let rows = client
        .query(
            CATALOG_SLOT_AUTHORIZATION_SQL,
            &[
                &generation,
                &context.identity.target.name().as_str(),
                &cleanup_receipt_text,
            ],
        )
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let authorization = match rows.as_slice() {
        [] => {
            return Err(LocalSlotMutationError::CatalogAuthorizationMissing(
                context.identity.target.clone(),
            ));
        }
        [row] => parse_catalog_authorization(row)?,
        _ => {
            return Err(LocalSlotMutationError::DuplicateCatalogAuthorization(
                context.identity.target.clone(),
            ));
        }
    };
    validate_catalog_authorization(&authorization, context, None, Some(cleanup_receipt_id))?;
    client
        .query_one(RESET_STATEMENT_TIMEOUT_SQL, &[])
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    Ok(PreparedCatalogFence {
        backend_pid,
        fence_id,
        database_name: authorization.database_name,
    })
}

async fn prepare_catalog_create_fence(
    client: &Client,
    context: &MutationContext,
    creation_attempt_id: ManagedLogicalSlotReceiptId,
) -> Result<PreparedCatalogFence, LocalSlotMutationError> {
    client
        .batch_execute("DISCARD ALL")
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    set_statement_timeout(client, context.preflight_deadline())
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let before = client
        .query_one(CATALOG_FENCE_REQUIREMENTS_SQL, &[])
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let backend_pid = validate_catalog_fence_requirements(&before, context)?;

    let attempt_start =
        begin_catalog_creation_attempt(client, context, creation_attempt_id).await?;

    async {
        let fence_id = acquire_catalog_target_fence(client, context, backend_pid).await?;

        if attempt_start == CatalogCreationAttemptStart::TargetBusy {
            set_statement_timeout(client, context.preflight_deadline())
                .await
                .map_err(|source| context.preflight_postgres(source))?;
            let retry =
                begin_catalog_creation_attempt(client, context, creation_attempt_id).await?;
            if retry != CatalogCreationAttemptStart::Persisted {
                return Err(LocalSlotMutationError::CatalogFenceBackendChanged);
            }
        }

        set_statement_timeout(client, context.preflight_deadline())
            .await
            .map_err(|source| context.preflight_postgres(source))?;
        let after = client
            .query_one(CATALOG_FENCE_REQUIREMENTS_SQL, &[])
            .await
            .map_err(|source| context.preflight_postgres(source))?;
        let after_backend_pid = validate_catalog_fence_requirements(&after, context)?;
        if after_backend_pid != backend_pid {
            return Err(LocalSlotMutationError::CatalogFenceBackendChanged);
        }

        set_statement_timeout(client, context.preflight_deadline())
            .await
            .map_err(|source| context.preflight_postgres(source))?;
        let generation = context.identity.target.generation().as_uuid().to_string();
        let creation_attempt_text = creation_attempt_id.as_uuid().to_string();
        let rows = client
            .query(
                CATALOG_SLOT_AUTHORIZATION_SQL,
                &[
                    &generation,
                    &context.identity.target.name().as_str(),
                    &creation_attempt_text,
                ],
            )
            .await
            .map_err(|source| context.preflight_postgres(source))?;
        let authorization = match rows.as_slice() {
            [] => {
                return Err(LocalSlotMutationError::CatalogAuthorizationMissing(
                    context.identity.target.clone(),
                ));
            }
            [row] => parse_catalog_authorization(row)?,
            _ => {
                return Err(LocalSlotMutationError::DuplicateCatalogAuthorization(
                    context.identity.target.clone(),
                ));
            }
        };
        validate_catalog_authorization(&authorization, context, Some(creation_attempt_id), None)?;
        client
            .query_one(RESET_STATEMENT_TIMEOUT_SQL, &[])
            .await
            .map_err(|source| context.preflight_postgres(source))?;
        Ok(PreparedCatalogFence {
            backend_pid,
            fence_id,
            database_name: authorization.database_name,
        })
    }
    .await
}

async fn begin_catalog_creation_attempt(
    client: &Client,
    context: &MutationContext,
    creation_attempt_id: ManagedLogicalSlotReceiptId,
) -> Result<CatalogCreationAttemptStart, LocalSlotMutationError> {
    let source = context.identity.source;
    let database_oid = i64::from(source.database_oid());
    let timeline = i64::from(source.timeline());
    let catalog_epoch = i64::try_from(source.catalog_epoch().0)
        .map_err(|_| LocalSlotMutationError::InvalidCatalogAuthorizationField("catalog_epoch"))?;
    let row = match client
        .query_one(
            BEGIN_CATALOG_CREATION_ATTEMPT_SQL,
            &[
                &context.identity.target.generation().as_uuid().to_string(),
                &context.identity.target.name().as_str(),
                &context.identity.role.catalog_label(),
                &source.system_identifier().to_string(),
                &database_oid,
                &timeline,
                &source.restore_incarnation().to_string(),
                &catalog_epoch,
                &creation_attempt_id.as_uuid().to_string(),
            ],
        )
        .await
    {
        Ok(row) => row,
        Err(source)
            if source
                .as_db_error()
                .is_some_and(|error| error.code() == &SqlState::LOCK_NOT_AVAILABLE) =>
        {
            return Ok(CatalogCreationAttemptStart::TargetBusy);
        }
        Err(source)
            if source.as_db_error().is_some_and(|error| {
                error.code() == &SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE
                    && error.message() == "managed slot already has an unresolved creation attempt"
            }) =>
        {
            return Err(LocalSlotMutationError::CatalogCreationAttemptPending(
                context.identity.target.clone(),
            ));
        }
        Err(source) => return Err(context.preflight_postgres(source)),
    };
    let allocation_kind: String = row
        .try_get(0)
        .map_err(|source| context.preflight_postgres(source))?;
    if !matches!(allocation_kind.as_str(), "probe" | "consumer") {
        return Err(LocalSlotMutationError::InvalidCatalogAuthorizationField(
            "allocation_kind",
        ));
    }
    Ok(CatalogCreationAttemptStart::Persisted)
}

async fn abandon_after_known_create_failure(
    client: &Client,
    context: &MutationContext,
    creation_attempt_id: ManagedLogicalSlotReceiptId,
    original: LocalSlotMutationError,
) -> LocalSlotMutationError {
    let cleanup_deadline = Instant::now() + context.duration;
    let cleanup = async {
        set_statement_timeout(client, cleanup_deadline).await?;
        let _: bool = client
            .query_one(
                RELEASE_CURRENT_TARGET_FENCE_SQL,
                &[&context.identity.target.name().as_str()],
            )
            .await?
            .try_get(0)?;
        set_statement_timeout(client, cleanup_deadline).await?;
        client
            .query_one(
                ABANDON_CATALOG_CREATION_ATTEMPT_SQL,
                &[
                    &context.identity.target.generation().as_uuid().to_string(),
                    &context.identity.target.name().as_str(),
                    &creation_attempt_id.as_uuid().to_string(),
                ],
            )
            .await?;
        Ok::<(), tokio_postgres::Error>(())
    };
    match timeout_at(cleanup_deadline, cleanup).await {
        Ok(Ok(())) => original,
        Ok(Err(source)) => LocalSlotMutationError::CatalogCreationAttemptUnresolved {
            target: context.identity.target.clone(),
            original: Box::new(original),
            source,
        },
        Err(_) => LocalSlotMutationError::CatalogCreationAttemptCleanupTimeout {
            target: context.identity.target.clone(),
            original: Box::new(original),
            duration: context.duration,
        },
    }
}

fn validate_catalog_fence_requirements(
    row: &tokio_postgres::Row,
    context: &MutationContext,
) -> Result<NonZeroU32, LocalSlotMutationError> {
    let version: i32 = row
        .try_get(0)
        .map_err(|source| context.preflight_postgres(source))?;
    if version < MIN_POSTGRES_VERSION_NUM {
        return Err(LocalSlotMutationError::UnsupportedPostgresVersion(version));
    }
    let database: String = row
        .try_get(1)
        .map_err(|source| context.preflight_postgres(source))?;
    if database != SHARDSCHEMA_DATABASE {
        return Err(LocalSlotMutationError::WrongCatalogFenceDatabase(database));
    }
    let encoding: String = row
        .try_get(2)
        .map_err(|source| context.preflight_postgres(source))?;
    if encoding != "UTF8" {
        return Err(LocalSlotMutationError::WrongEncoding(encoding));
    }
    let recovery: bool = row
        .try_get(3)
        .map_err(|source| context.preflight_postgres(source))?;
    if recovery {
        return Err(LocalSlotMutationError::CatalogFenceInRecovery);
    }
    positive_nonzero_u32(
        row.try_get(4)
            .map_err(|source| context.preflight_postgres(source))?,
        "catalog_fence_backend_pid",
    )
}

async fn acquire_catalog_target_fence(
    client: &Client,
    context: &MutationContext,
    expected_backend_pid: NonZeroU32,
) -> Result<Uuid, LocalSlotMutationError> {
    loop {
        set_statement_timeout(client, context.preflight_deadline())
            .await
            .map_err(|source| context.preflight_postgres(source))?;
        match client
            .query_one(
                ACQUIRE_TARGET_FENCE_SQL,
                &[&context.identity.target.name().as_str()],
            )
            .await
        {
            Ok(row) => return validate_acquired_catalog_fence(&row, expected_backend_pid),
            Err(source)
                if source
                    .as_db_error()
                    .is_some_and(|error| error.code() == &SqlState::LOCK_NOT_AVAILABLE) =>
            {
                sleep(TARGET_FENCE_RETRY_INTERVAL).await;
            }
            Err(source) => return Err(context.preflight_postgres(source)),
        }
    }
}

fn validate_acquired_catalog_fence(
    row: &tokio_postgres::Row,
    expected_backend_pid: NonZeroU32,
) -> Result<Uuid, LocalSlotMutationError> {
    let fence_text: String = row
        .try_get(0)
        .map_err(|_| LocalSlotMutationError::InvalidCatalogAuthorizationField("target_fence_id"))?;
    let fence_id = Uuid::parse_str(&fence_text)
        .ok()
        .filter(|value| !value.is_nil())
        .ok_or(LocalSlotMutationError::InvalidCatalogAuthorizationField(
            "target_fence_id",
        ))?;
    let backend_pid = positive_nonzero_u32(
        row.try_get(1).map_err(|_| {
            LocalSlotMutationError::InvalidCatalogAuthorizationField("target_fence_backend_pid")
        })?,
        "target_fence_backend_pid",
    )
    .map_err(|_| {
        LocalSlotMutationError::InvalidCatalogAuthorizationField("target_fence_backend_pid")
    })?;
    if backend_pid != expected_backend_pid {
        return Err(LocalSlotMutationError::CatalogFenceBackendChanged);
    }
    Ok(fence_id)
}

fn parse_catalog_authorization(
    row: &tokio_postgres::Row,
) -> Result<CatalogAuthorization, LocalSlotMutationError> {
    let field = |name| LocalSlotMutationError::InvalidCatalogAuthorizationField(name);
    let kind = match row
        .try_get::<_, String>("allocation_kind")
        .map_err(|_| field("allocation_kind"))?
        .as_str()
    {
        "probe" => CatalogAllocationKind::Probe,
        "consumer" => CatalogAllocationKind::Consumer,
        _ => return Err(field("allocation_kind")),
    };
    let generation_text: String = row
        .try_get("slot_generation")
        .map_err(|_| field("slot_generation"))?;
    let generation = Uuid::parse_str(&generation_text)
        .ok()
        .and_then(|value| SlotGeneration::new(value).ok())
        .ok_or_else(|| field("slot_generation"))?;
    let slot_name: String = row.try_get("slot_name").map_err(|_| field("slot_name"))?;
    let target = ManagedSlotTarget::new(
        ReplicationSlotName::new(slot_name).map_err(|_| field("slot_name"))?,
        generation,
    )
    .map_err(|_| field("slot_name"))?;
    let role = match row
        .try_get::<_, String>("slot_role")
        .map_err(|_| field("slot_role"))?
        .as_str()
    {
        "primary-anchor" => ManagedLogicalSlotRole::PrimaryFailoverAnchor,
        "standby-decoder" => ManagedLogicalSlotRole::StandbyLocalDecoder,
        _ => return Err(field("slot_role")),
    };
    let system_identifier = row
        .try_get::<_, String>("system_identifier")
        .map_err(|_| field("system_identifier"))?
        .parse::<u64>()
        .map_err(|_| field("system_identifier"))?;
    let database_oid = u32::try_from(
        row.try_get::<_, i64>("database_oid")
            .map_err(|_| field("database_oid"))?,
    )
    .map_err(|_| field("database_oid"))?;
    let timeline = u32::try_from(
        row.try_get::<_, i64>("source_timeline")
            .map_err(|_| field("source_timeline"))?,
    )
    .map_err(|_| field("source_timeline"))?;
    let restore_incarnation = Uuid::parse_str(
        &row.try_get::<_, String>("restore_incarnation")
            .map_err(|_| field("restore_incarnation"))?,
    )
    .map_err(|_| field("restore_incarnation"))?;
    let catalog_epoch = u64::try_from(
        row.try_get::<_, i64>("catalog_epoch")
            .map_err(|_| field("catalog_epoch"))?,
    )
    .map_err(|_| field("catalog_epoch"))?;
    let source = ReplicationSourceIdentity::new(
        system_identifier,
        timeline,
        database_oid,
        restore_incarnation,
        CatalogEpoch(catalog_epoch),
    )
    .map_err(|_| field("source_identity"))?;
    Ok(CatalogAuthorization {
        kind,
        state: row.try_get("state").map_err(|_| field("state"))?,
        target,
        role,
        database_name: row
            .try_get("database_name")
            .map_err(|_| field("database_name"))?,
        source,
        creation_receipt_id: parse_catalog_receipt_id(row, "creation_receipt_id")?,
        cleanup_receipt_id: parse_catalog_receipt_id(row, "cleanup_receipt_id")?,
        creation_attempt_state: row
            .try_get("creation_attempt_state")
            .map_err(|_| field("creation_attempt_state"))?,
        restore_state: row
            .try_get("restore_state")
            .map_err(|_| field("restore_state"))?,
        shard_state: row
            .try_get("shard_state")
            .map_err(|_| field("shard_state"))?,
        attachment_state: row
            .try_get("attachment_state")
            .map_err(|_| field("attachment_state"))?,
        consumer_shard_state: row
            .try_get("consumer_shard_state")
            .map_err(|_| field("consumer_shard_state"))?,
        consumer_state: row
            .try_get("consumer_state")
            .map_err(|_| field("consumer_state"))?,
        logical_database_state: row
            .try_get("logical_database_state")
            .map_err(|_| field("logical_database_state"))?,
    })
}

fn parse_catalog_receipt_id(
    row: &tokio_postgres::Row,
    name: &'static str,
) -> Result<Option<ManagedLogicalSlotReceiptId>, LocalSlotMutationError> {
    let value: Option<String> = row
        .try_get(name)
        .map_err(|_| LocalSlotMutationError::InvalidCatalogAuthorizationField(name))?;
    value
        .map(|value| {
            Uuid::parse_str(&value)
                .ok()
                .and_then(ManagedLogicalSlotReceiptId::from_uuid)
                .ok_or(LocalSlotMutationError::InvalidCatalogAuthorizationField(
                    name,
                ))
        })
        .transpose()
}

fn validate_catalog_authorization(
    authorization: &CatalogAuthorization,
    context: &MutationContext,
    creation_attempt_id: Option<ManagedLogicalSlotReceiptId>,
    cleanup_receipt_id: Option<ManagedLogicalSlotReceiptId>,
) -> Result<(), LocalSlotMutationError> {
    validate_catalog_authorization_identity_and_parent_state(authorization, context)?;
    match context.operation {
        LocalSlotMutationOperation::Create => {
            validate_catalog_create_authorization(authorization, context, creation_attempt_id)
        }
        LocalSlotMutationOperation::Drop => {
            validate_catalog_drop_authorization(authorization, context, cleanup_receipt_id)
        }
    }
}

fn validate_catalog_authorization_identity_and_parent_state(
    authorization: &CatalogAuthorization,
    context: &MutationContext,
) -> Result<(), LocalSlotMutationError> {
    let expected = context.identity.source;
    let observed = authorization.source;
    let exact_identity = authorization.target == context.identity.target
        && authorization.role == context.identity.role
        && observed.system_identifier() == expected.system_identifier()
        && observed.timeline() == expected.timeline()
        && observed.database_oid() == expected.database_oid()
        && observed.restore_incarnation() == expected.restore_incarnation();
    if !exact_identity {
        return Err(LocalSlotMutationError::CatalogAuthorizationIdentityChanged(
            context.identity.target.clone(),
        ));
    }
    let shard_is_eligible = match context.operation {
        LocalSlotMutationOperation::Create => matches!(
            authorization.shard_state.as_str(),
            "provisioning" | "active"
        ),
        LocalSlotMutationOperation::Drop => matches!(
            authorization.shard_state.as_str(),
            "provisioning" | "active" | "draining"
        ),
    };
    if authorization.restore_state != "active" || !shard_is_eligible {
        return Err(LocalSlotMutationError::CatalogAuthorizationStateChanged {
            operation: context.operation,
            target: context.identity.target.clone(),
            state: format!(
                "{} restore/{} shard/{} slot",
                authorization.restore_state, authorization.shard_state, authorization.state
            ),
        });
    }
    Ok(())
}

fn validate_catalog_create_authorization(
    authorization: &CatalogAuthorization,
    context: &MutationContext,
    creation_attempt_id: Option<ManagedLogicalSlotReceiptId>,
) -> Result<(), LocalSlotMutationError> {
    creation_attempt_id.expect("create always carries a durable attempt receipt");
    let lifecycle_ready = authorization.state == "allocated"
        && authorization.creation_attempt_state.as_deref() == Some("pending")
        && match authorization.kind {
            CatalogAllocationKind::Probe => {
                authorization.creation_receipt_id.is_none()
                    && authorization.cleanup_receipt_id.is_none()
            }
            CatalogAllocationKind::Consumer => {
                authorization.attachment_state.as_deref() == Some("staged")
                    && matches!(
                        authorization.consumer_shard_state.as_deref(),
                        Some("provisioning" | "fenced")
                    )
                    && authorization.consumer_state.as_deref() == Some("active")
                    && authorization.logical_database_state.as_deref() == Some("active")
            }
        };
    if !lifecycle_ready {
        return Err(LocalSlotMutationError::CatalogAuthorizationStateChanged {
            operation: context.operation,
            target: context.identity.target.clone(),
            state: authorization.state.clone(),
        });
    }
    let expected = context.identity.source;
    let observed = authorization.source;
    if observed.catalog_epoch() != expected.catalog_epoch() {
        return Err(LocalSlotMutationError::StaleCatalogAuthorization {
            target: context.identity.target.clone(),
            expected: expected.catalog_epoch(),
            observed: observed.catalog_epoch(),
        });
    }
    Ok(())
}

fn validate_catalog_drop_authorization(
    authorization: &CatalogAuthorization,
    context: &MutationContext,
    cleanup_receipt_id: Option<ManagedLogicalSlotReceiptId>,
) -> Result<(), LocalSlotMutationError> {
    let receipt_id = cleanup_receipt_id.expect("drop always carries a cleanup receipt");
    let lifecycle_ready = match authorization.kind {
        CatalogAllocationKind::Probe => {
            authorization.state == "retiring"
                && authorization.cleanup_receipt_id == Some(receipt_id)
                && authorization
                    .creation_attempt_state
                    .as_deref()
                    .is_none_or(|state| state == "activated")
        }
        CatalogAllocationKind::Consumer => match authorization.state.as_str() {
            "allocated" => {
                authorization.attachment_state.as_deref() == Some("staged")
                    && authorization.creation_attempt_state.as_deref() == Some("pending")
            }
            "active" => {
                (authorization.attachment_state.as_deref() == Some("staged")
                    || (authorization.attachment_state.as_deref() == Some("retiring")
                        && authorization.consumer_shard_state.as_deref() == Some("fenced")))
                    && authorization.creation_attempt_state.as_deref() == Some("activated")
            }
            "retiring" => {
                authorization.attachment_state.as_deref() == Some("retiring")
                    && authorization.consumer_shard_state.as_deref() == Some("fenced")
                    && authorization.creation_attempt_state.as_deref() == Some("activated")
            }
            _ => false,
        },
    };
    if !lifecycle_ready {
        if authorization.kind == CatalogAllocationKind::Probe && authorization.state == "retiring" {
            return Err(LocalSlotMutationError::CatalogCleanupReceiptChanged(
                context.identity.target.clone(),
            ));
        }
        return Err(LocalSlotMutationError::CatalogAuthorizationStateChanged {
            operation: context.operation,
            target: context.identity.target.clone(),
            state: authorization.state.clone(),
        });
    }
    Ok(())
}

async fn prepare_create(
    client: &Client,
    context: &MutationContext,
) -> Result<PreparedCreate, LocalSlotMutationError> {
    let server = prepare_server(client, context, MAX_ADVISORY_LOCK_ROWS).await?;
    set_statement_timeout(client, context.deadline)
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let statement = prepare_mutation_statement(context, client.prepare(CREATE_SLOT_SQL)).await?;
    set_statement_timeout(client, context.deadline)
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    if client
        .query_opt(
            "SELECT slot_name::pg_catalog.text \
               FROM pg_catalog.pg_replication_slots \
              WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name",
            &[&context.identity.target.name().as_str()],
        )
        .await
        .map_err(|source| context.preflight_postgres(source))?
        .is_some()
    {
        return Err(LocalSlotMutationError::TargetOccupied(
            context.identity.target.clone(),
        ));
    }
    set_statement_timeout(client, context.deadline)
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    Ok(PreparedCreate { server, statement })
}

async fn prepare_mutation_statement<F, V>(
    context: &MutationContext,
    operation: F,
) -> Result<V, LocalSlotMutationError>
where
    F: Future<Output = Result<V, tokio_postgres::Error>>,
{
    match timeout_at(context.preflight_deadline(), operation).await {
        Ok(Ok(statement)) => Ok(statement),
        Ok(Err(source)) => Err(context.preflight_postgres(source)),
        Err(_) => Err(context.preflight_deadline_error()),
    }
}

async fn prepare_drop(
    client: &Client,
    context: &MutationContext,
    creation_lsn: PgLsn,
) -> Result<PreparedDrop, LocalSlotMutationError> {
    let server = prepare_server(client, context, MAX_ADVISORY_LOCK_ROWS).await?;
    set_statement_timeout(client, context.deadline)
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let statement = prepare_mutation_statement(context, client.prepare(DROP_SLOT_SQL)).await?;
    set_statement_timeout(client, context.deadline)
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let row = client
        .query_opt(SELECT_SLOT_SQL, &[&context.identity.target.name().as_str()])
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let Some(row) = row else {
        return Ok(PreparedDrop::Absent { server });
    };
    let observation = parse_logical_slot(&row).map_err(|source| {
        LocalSlotMutationError::UnsafeDropObservation {
            target: context.identity.target.clone(),
            source,
        }
    })?;
    validate_slot_shape(&observation, &context.identity, creation_lsn, false).map_err(
        |problem| LocalSlotMutationError::UnsafeDropTarget {
            target: context.identity.target.clone(),
            problem,
        },
    )?;
    set_statement_timeout(client, context.deadline)
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    Ok(PreparedDrop::Present { server, statement })
}

async fn prepare_server(
    client: &Client,
    context: &MutationContext,
    maximum_advisory_locks: usize,
) -> Result<PreparedServer, LocalSlotMutationError> {
    client
        .query_one(PIN_SEARCH_PATH_SQL, &[])
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    set_statement_timeout(client, context.deadline)
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let advisory_lock_query_limit = i64::try_from(maximum_advisory_locks.saturating_add(1))
        .expect("small advisory-lock bound fits PostgreSQL bigint");
    let basic = client
        .query_one(BASIC_REQUIREMENTS_SQL, &[&advisory_lock_query_limit])
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let version: i32 = basic
        .try_get(0)
        .map_err(|source| context.preflight_postgres(source))?;
    if version < MIN_POSTGRES_VERSION_NUM {
        return Err(LocalSlotMutationError::UnsupportedPostgresVersion(version));
    }
    let database_name: String = basic
        .try_get(1)
        .map_err(|source| context.preflight_postgres(source))?;
    let database_oid = positive_u32(
        basic
            .try_get(2)
            .map_err(|source| context.preflight_postgres(source))?,
        "database_oid",
    )?;
    let encoding: String = basic
        .try_get(3)
        .map_err(|source| context.preflight_postgres(source))?;
    if encoding != "UTF8" {
        return Err(LocalSlotMutationError::WrongEncoding(encoding));
    }
    let session = parse_mutation_session(&basic, 4, maximum_advisory_locks, context)?;
    set_statement_timeout(client, context.deadline)
        .await
        .map_err(|source| context.preflight_postgres(source))?;
    let source = client
        .query_one(SOURCE_REQUIREMENTS_SQL, &[&advisory_lock_query_limit])
        .await
        .map_err(|error| context.preflight_postgres(error))?;
    validate_source_row(
        &source,
        database_oid,
        &session,
        maximum_advisory_locks,
        context,
    )?;
    let caller_advisory_lock_count = session.advisory_locks.len();
    Ok(PreparedServer {
        database_name,
        session,
        caller_advisory_lock_count,
    })
}

async fn create_at_dispatch_boundary(
    client: &Client,
    context: &MutationContext,
    prepared: PreparedCreate,
    receipt_id: ManagedLogicalSlotReceiptId,
) -> Result<ManagedLogicalSlotReceipt, LocalSlotMutationError> {
    dispatch_mutation_before_deadline(context, async {
        create_after_dispatch(client, context, prepared, receipt_id)
            .await
            .map_err(|source| context.unknown(source))
    })
    .await
}

async fn dispatch_mutation_before_deadline<F, V>(
    context: &MutationContext,
    dispatch: F,
) -> Result<V, LocalSlotMutationError>
where
    F: Future<Output = Result<V, LocalSlotMutationError>>,
{
    context.ensure_dispatch_deadline()?;
    dispatch.await
}

async fn create_after_dispatch(
    client: &Client,
    context: &MutationContext,
    prepared: PreparedCreate,
    receipt_id: ManagedLogicalSlotReceiptId,
) -> Result<ManagedLogicalSlotReceipt, LocalSlotMutationUnknownCause> {
    let row = client
        .query_one(
            &prepared.statement,
            &[
                &context.identity.target.name().as_str(),
                &context.identity.role.failover(),
            ],
        )
        .await?;
    let returned_name: String = row.try_get(0)?;
    if returned_name != context.identity.target.name().as_str() {
        return Err(LocalSlotMutationUnknownCause::UnexpectedCreatedSlot(
            returned_name,
        ));
    }
    let returned_lsn: String = row.try_get(1)?;
    let creation_lsn = parse_lsn(&returned_lsn)
        .filter(|lsn| lsn.0 != 0)
        .ok_or_else(|| LocalSlotMutationUnknownCause::InvalidCreationLsn(returned_lsn.clone()))?;
    revalidate_server_after_dispatch(client, context, &prepared.server).await?;
    set_statement_timeout(client, context.deadline).await?;
    let postflight = client
        .query_opt(SELECT_SLOT_SQL, &[&context.identity.target.name().as_str()])
        .await?
        .ok_or_else(|| {
            LocalSlotMutationUnknownCause::PostflightShape(ManagedLogicalSlotShapeProblem::Missing)
        })?;
    let observation = parse_logical_slot(&postflight)
        .map_err(LocalSlotMutationUnknownCause::PostflightObservation)?;
    let observation = validate_slot_shape(&observation, &context.identity, creation_lsn, true)
        .map_err(LocalSlotMutationUnknownCause::PostflightShape)?;
    Ok(ManagedLogicalSlotReceipt {
        receipt_id,
        target: context.identity.target.clone(),
        source: context.identity.source,
        role: context.identity.role,
        database_name: prepared.server.database_name,
        creation_lsn,
        observation,
        effective_role_oid: prepared.server.session.effective_role_oid,
        advisory_lock_count: prepared.server.caller_advisory_lock_count,
    })
}

async fn drop_after_dispatch(
    client: &Client,
    context: &MutationContext,
    prepared: &PreparedServer,
    statement: &Statement,
) -> Result<ManagedLogicalSlotDropOutcome, LocalSlotMutationUnknownCause> {
    client
        .query_one(statement, &[&context.identity.target.name().as_str()])
        .await?;
    revalidate_server_after_dispatch(client, context, prepared).await?;
    set_statement_timeout(client, context.deadline).await?;
    if client
        .query_opt(
            "SELECT slot_name::pg_catalog.text \
               FROM pg_catalog.pg_replication_slots \
              WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name",
            &[&context.identity.target.name().as_str()],
        )
        .await?
        .is_some()
    {
        return Err(LocalSlotMutationUnknownCause::SlotStillPresent);
    }
    Ok(ManagedLogicalSlotDropOutcome::Dropped)
}

async fn drop_at_dispatch_boundary(
    client: &Client,
    context: &MutationContext,
    prepared: &PreparedServer,
    statement: &Statement,
) -> Result<ManagedLogicalSlotDropOutcome, LocalSlotMutationError> {
    dispatch_mutation_before_deadline(context, async {
        drop_after_dispatch(client, context, prepared, statement)
            .await
            .map_err(|source| context.unknown(source))
    })
    .await
}

async fn revalidate_server_after_dispatch(
    client: &Client,
    context: &MutationContext,
    prepared: &PreparedServer,
) -> Result<(), LocalSlotMutationUnknownCause> {
    set_statement_timeout(client, context.deadline).await?;
    let advisory_lock_query_limit = i64::try_from(MAX_ADVISORY_LOCK_ROWS.saturating_add(1))
        .expect("small advisory-lock bound fits PostgreSQL bigint");
    let row = client
        .query_one(SOURCE_REQUIREMENTS_SQL, &[&advisory_lock_query_limit])
        .await?;
    validate_source_row(
        &row,
        context.identity.source.database_oid(),
        &prepared.session,
        MAX_ADVISORY_LOCK_ROWS,
        context,
    )
    .map_err(postflight_source_error)
}

fn postflight_source_error(error: LocalSlotMutationError) -> LocalSlotMutationUnknownCause {
    match error {
        LocalSlotMutationError::PreflightPostgres { source, .. } => {
            LocalSlotMutationUnknownCause::Postgres(source)
        }
        other => LocalSlotMutationUnknownCause::PostflightSource(other.to_string()),
    }
}

fn validate_source_row(
    row: &tokio_postgres::Row,
    database_oid: u32,
    expected_session: &MutationSessionIdentity,
    maximum_advisory_locks: usize,
    context: &MutationContext,
) -> Result<(), LocalSlotMutationError> {
    let system_identifier = row
        .try_get::<_, i64>(0)
        .map_err(|source| context.preflight_postgres(source))?
        .cast_unsigned();
    if system_identifier == 0 {
        return Err(LocalSlotMutationError::InvalidSourceField(
            "system_identifier",
        ));
    }
    let checkpoint_timeline = row
        .try_get::<_, i32>(1)
        .map_err(|source| context.preflight_postgres(source))?
        .cast_unsigned();
    if checkpoint_timeline == 0 {
        return Err(LocalSlotMutationError::InvalidSourceField(
            "checkpoint_timeline",
        ));
    }
    let recovery = if row
        .try_get(2)
        .map_err(|source| context.preflight_postgres(source))?
    {
        RecoveryState::Standby
    } else {
        RecoveryState::Writable
    };
    let current_timeline: Option<String> = row
        .try_get(3)
        .map_err(|source| context.preflight_postgres(source))?;
    let observed_timeline =
        parse_endpoint_timeline(recovery, checkpoint_timeline, current_timeline)?;
    let wal_level: String = row
        .try_get(4)
        .map_err(|source| context.preflight_postgres(source))?;
    let hot_standby_feedback: bool = row
        .try_get(5)
        .map_err(|source| context.preflight_postgres(source))?;
    let wal_receiver_status_interval = parse_feedback_interval(row, context)?;
    let sync_replication_slots: bool = row
        .try_get(8)
        .map_err(|source| context.preflight_postgres(source))?;
    let primary_slot_name = optional_slot_name(
        row.try_get(9)
            .map_err(|source| context.preflight_postgres(source))?,
        "primary_slot_name",
    )?;
    let receiver = parse_observed_wal_receiver(row, context)?;
    let slot_sync_worker = parse_observed_slot_sync_worker(row, context)?;
    let session = parse_mutation_session(row, 16, maximum_advisory_locks, context)?;
    if &session != expected_session {
        return Err(LocalSlotMutationError::SessionFenceChanged);
    }
    validate_source_identity(
        &ObservedSource {
            system_identifier,
            timeline: observed_timeline,
            database_oid,
            recovery,
            wal_level,
            hot_standby_feedback,
            wal_receiver_status_interval,
            sync_replication_slots,
            primary_slot_name,
            wal_receiver_pid: receiver.pid,
            wal_receiver_streaming: receiver.streaming,
            wal_receiver_slot_name: receiver.slot_name,
            wal_receiver_received_timeline: receiver.received_timeline,
            slot_sync_worker,
        },
        context,
    )
}

fn parse_feedback_interval(
    row: &tokio_postgres::Row,
    context: &MutationContext,
) -> Result<Duration, LocalSlotMutationError> {
    let setting: i64 = row
        .try_get(6)
        .map_err(|source| context.preflight_postgres(source))?;
    let unit: String = row
        .try_get(7)
        .map_err(|source| context.preflight_postgres(source))?;
    let seconds = u64::try_from(setting)
        .map_err(|_| LocalSlotMutationError::InvalidSourceField("wal_receiver_status_interval"))?;
    if unit != "s" {
        return Err(LocalSlotMutationError::InvalidSourceField(
            "wal_receiver_status_interval_unit",
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn parse_observed_wal_receiver(
    row: &tokio_postgres::Row,
    context: &MutationContext,
) -> Result<ObservedWalReceiver, LocalSlotMutationError> {
    let pid = optional_nonzero_u32(
        row.try_get(10)
            .map_err(|source| context.preflight_postgres(source))?,
        "wal_receiver_pid",
    )?;
    let status: Option<String> = row
        .try_get(11)
        .map_err(|source| context.preflight_postgres(source))?;
    let slot_name = optional_slot_name(
        row.try_get(12)
            .map_err(|source| context.preflight_postgres(source))?,
        "wal_receiver_slot_name",
    )?;
    let received_timeline = optional_timeline_id(
        row.try_get(13)
            .map_err(|source| context.preflight_postgres(source))?,
        "wal_receiver_received_timeline",
    )?;
    if pid.is_none() && (status.is_some() || slot_name.is_some() || received_timeline.is_some()) {
        return Err(LocalSlotMutationError::InvalidSourceField("wal_receiver"));
    }
    Ok(ObservedWalReceiver {
        pid,
        streaming: status.as_deref() == Some("streaming"),
        slot_name,
        received_timeline,
    })
}

fn parse_observed_slot_sync_worker(
    row: &tokio_postgres::Row,
    context: &MutationContext,
) -> Result<Option<LocalPostgresBackendIdentity>, LocalSlotMutationError> {
    let pid = optional_nonzero_u32(
        row.try_get(14)
            .map_err(|source| context.preflight_postgres(source))?,
        "slot_sync_worker_pid",
    )?;
    let start = optional_nonzero_u64(
        row.try_get(15)
            .map_err(|source| context.preflight_postgres(source))?,
        "slot_sync_worker_start",
    )?;
    match (pid, start) {
        (Some(pid), Some(start)) => Ok(Some(LocalPostgresBackendIdentity::from_parts(pid, start))),
        (None, None) => Ok(None),
        _ => Err(LocalSlotMutationError::InvalidSourceField(
            "slot_sync_worker",
        )),
    }
}

fn parse_mutation_session(
    row: &tokio_postgres::Row,
    first_column: usize,
    maximum_advisory_locks: usize,
    context: &MutationContext,
) -> Result<MutationSessionIdentity, LocalSlotMutationError> {
    let advisory_locks = bounded_advisory_locks(
        row.try_get(first_column + 3)
            .map_err(|source| context.preflight_postgres(source))?,
        maximum_advisory_locks,
    )?;
    Ok(MutationSessionIdentity {
        backend_pid: positive_nonzero_u32(
            row.try_get(first_column)
                .map_err(|source| context.preflight_postgres(source))?,
            "backend_pid",
        )?,
        session_role_oid: positive_u32(
            row.try_get(first_column + 1)
                .map_err(|source| context.preflight_postgres(source))?,
            "session_role_oid",
        )?,
        effective_role_oid: positive_u32(
            row.try_get(first_column + 2)
                .map_err(|source| context.preflight_postgres(source))?,
            "effective_role_oid",
        )?,
        advisory_locks,
    })
}

fn bounded_advisory_locks(
    advisory_locks: Vec<String>,
    maximum: usize,
) -> Result<Vec<String>, LocalSlotMutationError> {
    if advisory_locks.len() > maximum {
        return Err(LocalSlotMutationError::TooManyAdvisoryLocks { maximum });
    }
    Ok(advisory_locks)
}

fn validate_source_identity(
    observed: &ObservedSource,
    context: &MutationContext,
) -> Result<(), LocalSlotMutationError> {
    let expected = context.identity.source;
    if observed.system_identifier != expected.system_identifier()
        || observed.timeline != expected.timeline()
        || observed.database_oid != expected.database_oid()
    {
        return Err(LocalSlotMutationError::SourceMismatch {
            expected_system_identifier: expected.system_identifier(),
            expected_timeline: expected.timeline(),
            expected_database_oid: expected.database_oid(),
            observed_system_identifier: observed.system_identifier,
            observed_timeline: observed.timeline,
            observed_database_oid: observed.database_oid,
        });
    }
    let required_recovery = context.identity.role.recovery();
    if observed.recovery != required_recovery {
        return Err(LocalSlotMutationError::WrongRecoveryState {
            role: context.identity.role,
            expected: required_recovery,
            observed: observed.recovery,
        });
    }
    if context.operation == LocalSlotMutationOperation::Create && observed.wal_level != "logical" {
        return Err(LocalSlotMutationError::InsufficientWalLevel(
            context.identity.role,
        ));
    }
    if context.operation == LocalSlotMutationOperation::Create
        && context.identity.role == ManagedLogicalSlotRole::StandbyLocalDecoder
    {
        let path = context
            .identity
            .standby_path
            .as_ref()
            .expect("standby creation requests are constructed from correlated paths");
        if observed.primary_slot_name.as_ref() != Some(&path.physical_slot)
            || observed.wal_receiver_slot_name.as_ref() != Some(&path.physical_slot)
        {
            return Err(LocalSlotMutationError::PrimarySlotNameMismatch);
        }
        if observed.wal_receiver_pid.is_none() || !observed.wal_receiver_streaming {
            return Err(LocalSlotMutationError::WalReceiverNotStreaming);
        }
        let receiver_timeline = observed
            .wal_receiver_received_timeline
            .ok_or(LocalSlotMutationError::WalReceiverNotStreaming)?;
        if receiver_timeline != expected.timeline() {
            return Err(LocalSlotMutationError::WalReceiverTimelineMismatch {
                expected: expected.timeline(),
                observed: receiver_timeline,
            });
        }
        if Instant::now() >= path.valid_until {
            return Err(LocalSlotMutationError::StandbyPathExpired);
        }
        if !observed.hot_standby_feedback {
            return Err(LocalSlotMutationError::HotStandbyFeedbackDisabled);
        }
        let maximum_feedback = path.maximum_feedback_reporting_interval;
        if observed.wal_receiver_status_interval.is_zero()
            || observed.wal_receiver_status_interval > maximum_feedback
        {
            return Err(LocalSlotMutationError::FeedbackReportingIntervalUnsafe {
                observed: observed.wal_receiver_status_interval,
                maximum: maximum_feedback,
            });
        }
        if !observed.sync_replication_slots {
            return Err(LocalSlotMutationError::SlotSynchronizationDisabled);
        }
        if observed.wal_receiver_pid != Some(path.expected_wal_receiver_pid) {
            return Err(LocalSlotMutationError::WalReceiverChanged);
        }
        let slot_sync_worker = observed
            .slot_sync_worker
            .ok_or(LocalSlotMutationError::SlotSyncWorkerMissing)?;
        if slot_sync_worker != path.expected_slot_sync_worker {
            return Err(LocalSlotMutationError::SlotSyncWorkerChanged);
        }
    }
    Ok(())
}

fn parse_endpoint_timeline(
    recovery: RecoveryState,
    checkpoint_timeline: u32,
    current_timeline: Option<String>,
) -> Result<u32, LocalSlotMutationError> {
    match (recovery, current_timeline) {
        (RecoveryState::Standby, None) => Ok(checkpoint_timeline),
        (RecoveryState::Writable, Some(value))
            if value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            u32::from_str_radix(&value, 16)
                .ok()
                .filter(|timeline| *timeline != 0)
                .ok_or(LocalSlotMutationError::InvalidSourceField(
                    "current_timeline",
                ))
        }
        _ => Err(LocalSlotMutationError::InvalidSourceField(
            "current_timeline",
        )),
    }
}

fn validate_slot_shape(
    observation: &LogicalSlotObservation,
    identity: &MutationIdentity,
    creation_lsn: PgLsn,
    initial: bool,
) -> Result<LogicalSlotObservation, ManagedLogicalSlotShapeProblem> {
    if observation.name != *identity.target.name() {
        return Err(ManagedLogicalSlotShapeProblem::WrongName);
    }
    if observation.database_oid != identity.source.database_oid() {
        return Err(ManagedLogicalSlotShapeProblem::WrongDatabase {
            expected: identity.source.database_oid(),
            observed: observation.database_oid,
        });
    }
    if observation.plugin != LogicalSlotPlugin::PgOutput {
        return Err(ManagedLogicalSlotShapeProblem::WrongPlugin);
    }
    if observation.kind != identity.role.slot_kind() {
        return Err(ManagedLogicalSlotShapeProblem::WrongRole {
            expected: identity.role,
            observed: observation.kind,
        });
    }
    if observation.persistence == SlotPersistence::NonPersistent {
        return Err(ManagedLogicalSlotShapeProblem::Temporary);
    }
    if observation.activity != SlotActivity::Inactive {
        return Err(ManagedLogicalSlotShapeProblem::Active);
    }
    if observation.two_phase != SettingState::Enabled {
        return Err(ManagedLogicalSlotShapeProblem::TwoPhaseDisabled);
    }
    if observation.two_phase_at != Some(creation_lsn) {
        return Err(ManagedLogicalSlotShapeProblem::WrongTwoPhaseBoundary {
            expected: creation_lsn,
            observed: observation.two_phase_at,
        });
    }
    let confirmed = observation
        .confirmed_flush_lsn
        .ok_or(ManagedLogicalSlotShapeProblem::MissingConfirmedFlushLsn)?;
    if initial && confirmed != creation_lsn {
        return Err(
            ManagedLogicalSlotShapeProblem::WrongInitialConfirmedFlushLsn {
                expected: creation_lsn,
                observed: confirmed,
            },
        );
    }
    if !initial && confirmed.0 < creation_lsn.0 {
        return Err(ManagedLogicalSlotShapeProblem::ConfirmedFlushLsnRegressed {
            minimum: creation_lsn,
            observed: confirmed,
        });
    }
    if initial {
        if let Some(invalidation) = observation.invalidation {
            return Err(ManagedLogicalSlotShapeProblem::Invalidated(invalidation));
        }
        if !matches!(
            observation.wal_retention,
            Some(SlotWalRetention::Reserved | SlotWalRetention::Extended)
        ) {
            return Err(ManagedLogicalSlotShapeProblem::WalNotRetained);
        }
    }
    let mut proven = observation.clone();
    proven.persistence = SlotPersistence::Persistent;
    proven.ownership = SlotOwnership::Managed(identity.target.generation());
    Ok(proven)
}

fn positive_u32(value: i64, field: &'static str) -> Result<u32, LocalSlotMutationError> {
    u32::try_from(value)
        .ok()
        .filter(|value| *value != 0)
        .ok_or(LocalSlotMutationError::InvalidSourceField(field))
}

fn positive_nonzero_u32(
    value: i32,
    field: &'static str,
) -> Result<NonZeroU32, LocalSlotMutationError> {
    u32::try_from(value)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(LocalSlotMutationError::InvalidSourceField(field))
}

fn optional_nonzero_u32(
    value: Option<i32>,
    field: &'static str,
) -> Result<Option<NonZeroU32>, LocalSlotMutationError> {
    value
        .map(|value| positive_nonzero_u32(value, field))
        .transpose()
}

fn optional_timeline_id(
    value: Option<i32>,
    field: &'static str,
) -> Result<Option<u32>, LocalSlotMutationError> {
    value
        .map(|value| {
            let value = value.cast_unsigned();
            if value == 0 {
                Err(LocalSlotMutationError::InvalidSourceField(field))
            } else {
                Ok(value)
            }
        })
        .transpose()
}

fn optional_nonzero_u64(
    value: Option<i64>,
    field: &'static str,
) -> Result<Option<NonZeroU64>, LocalSlotMutationError> {
    value
        .map(|value| {
            u64::try_from(value)
                .ok()
                .and_then(NonZeroU64::new)
                .ok_or(LocalSlotMutationError::InvalidSourceField(field))
        })
        .transpose()
}

fn optional_slot_name(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<ReplicationSlotName>, LocalSlotMutationError> {
    value
        .map(ReplicationSlotName::new)
        .transpose()
        .map_err(|_| LocalSlotMutationError::InvalidSourceField(field))
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
        .expect("bounded slot mutation timeout fits PostgreSQL milliseconds");
    client
        .query_one(SET_STATEMENT_TIMEOUT_SQL, &[&format!("{milliseconds}ms")])
        .await?;
    Ok(())
}

fn postgres_milliseconds(duration: Duration) -> String {
    let milliseconds = u64::try_from(duration.as_millis())
        .expect("bounded slot mutation timeout fits PostgreSQL milliseconds");
    format!("{milliseconds}ms")
}

impl From<tokio_postgres::Error> for LocalSlotMutationUnknownCause {
    fn from(source: tokio_postgres::Error) -> Self {
        Self::Postgres(source)
    }
}

impl From<ConnectionTaskError> for LocalSlotMutationUnknownCause {
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
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use pgshard_types::CatalogEpoch;
    use uuid::Uuid;

    use super::*;
    use crate::standby_slots::{ReplicationSlotName, SlotGeneration};

    #[test]
    fn receipt_capability_debug_output_is_redacted() {
        let secret = Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
        let receipt_id = ManagedLogicalSlotReceiptId(secret);
        let debug = format!("{receipt_id:?}");
        assert_eq!(debug, "ManagedLogicalSlotReceiptId(<redacted>)");
        assert!(!debug.contains(&secret.to_string()));
        assert!(!debug.contains(&secret.simple().to_string()));

        let identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        let mut receipt = receipt(&identity);
        receipt.receipt_id = receipt_id;
        let debug = format!("{receipt:?}");
        assert!(!debug.contains(&secret.to_string()));
        assert!(!debug.contains(&secret.simple().to_string()));
        let drop_receipt = drop_receipt(receipt, ManagedLogicalSlotDropOutcome::AlreadyAbsent);
        let debug = format!("{drop_receipt:?}");
        assert!(!debug.contains(&secret.to_string()));
        assert!(!debug.contains(&secret.simple().to_string()));
    }

    fn identity(role: ManagedLogicalSlotRole) -> MutationIdentity {
        let generation = SlotGeneration::new(Uuid::from_u128(1)).expect("generation");
        let standby_path =
            (role == ManagedLogicalSlotRole::StandbyLocalDecoder).then(|| StandbyMutationPath {
                physical_slot: ReplicationSlotName::new("pgshard_member_0001")
                    .expect("physical slot"),
                expected_wal_receiver_pid: NonZeroU32::new(10).expect("receiver pid"),
                expected_slot_sync_worker: LocalPostgresBackendIdentity::from_parts(
                    NonZeroU32::new(11).expect("worker pid"),
                    NonZeroU64::new(12).expect("worker start"),
                ),
                maximum_feedback_reporting_interval: Duration::from_secs(2),
                valid_until: Instant::now() + Duration::from_secs(30),
            });
        MutationIdentity {
            target: ManagedSlotTarget::new(
                ReplicationSlotName::new("pgshard_test_slot_00000000000000000000000000000001")
                    .expect("name"),
                generation,
            )
            .expect("target"),
            source: ReplicationSourceIdentity::new(1, 1, 1, Uuid::from_u128(2), CatalogEpoch(1))
                .expect("source"),
            role,
            standby_path,
        }
    }

    fn observation(identity: &MutationIdentity) -> LogicalSlotObservation {
        LogicalSlotObservation {
            name: identity.target.name().clone(),
            database_oid: identity.source.database_oid(),
            plugin: LogicalSlotPlugin::PgOutput,
            kind: identity.role.slot_kind(),
            persistence: SlotPersistence::Unproven,
            two_phase: SettingState::Enabled,
            two_phase_at: Some(PgLsn(10)),
            activity: SlotActivity::Inactive,
            ownership: SlotOwnership::Unknown,
            invalidation: None,
            wal_retention: Some(SlotWalRetention::Reserved),
            confirmed_flush_lsn: Some(PgLsn(10)),
        }
    }

    fn receipt(identity: &MutationIdentity) -> ManagedLogicalSlotReceipt {
        ManagedLogicalSlotReceipt {
            receipt_id: ManagedLogicalSlotReceiptId(Uuid::from_u128(3)),
            target: identity.target.clone(),
            source: identity.source,
            role: identity.role,
            database_name: "postgres".to_owned(),
            creation_lsn: PgLsn(10),
            observation: observation(identity),
            effective_role_oid: 10,
            advisory_lock_count: 0,
        }
    }

    fn observed_source(identity: &MutationIdentity) -> ObservedSource {
        ObservedSource {
            system_identifier: identity.source.system_identifier(),
            timeline: identity.source.timeline(),
            database_oid: identity.source.database_oid(),
            recovery: identity.role.recovery(),
            wal_level: "logical".to_owned(),
            hot_standby_feedback: true,
            wal_receiver_status_interval: Duration::from_secs(1),
            sync_replication_slots: true,
            primary_slot_name: identity
                .standby_path
                .as_ref()
                .map(|path| path.physical_slot.clone()),
            wal_receiver_pid: identity
                .standby_path
                .as_ref()
                .map(|path| path.expected_wal_receiver_pid),
            wal_receiver_streaming: identity.standby_path.is_some(),
            wal_receiver_slot_name: identity
                .standby_path
                .as_ref()
                .map(|path| path.physical_slot.clone()),
            wal_receiver_received_timeline: identity
                .standby_path
                .as_ref()
                .map(|_| identity.source.timeline()),
            slot_sync_worker: identity
                .standby_path
                .as_ref()
                .map(|path| path.expected_slot_sync_worker),
        }
    }

    fn context(identity: MutationIdentity) -> MutationContext {
        context_for(LocalSlotMutationOperation::Create, identity)
    }

    fn context_for(
        operation: LocalSlotMutationOperation,
        identity: MutationIdentity,
    ) -> MutationContext {
        MutationContext::new(operation, identity, CatalogOperationTimeout::default())
    }

    fn catalog_authorization(
        identity: &MutationIdentity,
        kind: CatalogAllocationKind,
    ) -> CatalogAuthorization {
        CatalogAuthorization {
            kind,
            state: "allocated".to_owned(),
            target: identity.target.clone(),
            role: identity.role,
            database_name: SHARDSCHEMA_DATABASE.to_owned(),
            source: identity.source,
            creation_receipt_id: None,
            cleanup_receipt_id: None,
            creation_attempt_state: Some("pending".to_owned()),
            restore_state: "active".to_owned(),
            shard_state: "active".to_owned(),
            attachment_state: (kind == CatalogAllocationKind::Consumer)
                .then(|| "staged".to_owned()),
            consumer_shard_state: (kind == CatalogAllocationKind::Consumer)
                .then(|| "provisioning".to_owned()),
            consumer_state: (kind == CatalogAllocationKind::Consumer).then(|| "active".to_owned()),
            logical_database_state: (kind == CatalogAllocationKind::Consumer)
                .then(|| "active".to_owned()),
        }
    }

    #[test]
    fn role_contracts_select_exact_server_shapes() {
        assert_eq!(
            ManagedLogicalSlotRole::PrimaryFailoverAnchor.recovery(),
            RecoveryState::Writable
        );
        assert!(ManagedLogicalSlotRole::PrimaryFailoverAnchor.failover());
        assert_eq!(
            ManagedLogicalSlotRole::StandbyLocalDecoder.recovery(),
            RecoveryState::Standby
        );
        assert!(!ManagedLogicalSlotRole::StandbyLocalDecoder.failover());
    }

    #[test]
    fn standby_creation_requires_feedback_and_continuous_slot_sync() {
        let identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        let context = context(identity.clone());
        let mut observed = observed_source(&identity);
        observed.hot_standby_feedback = false;
        assert!(matches!(
            validate_source_identity(&observed, &context),
            Err(LocalSlotMutationError::HotStandbyFeedbackDisabled)
        ));
        observed.hot_standby_feedback = true;
        observed.wal_receiver_status_interval = Duration::ZERO;
        assert!(matches!(
            validate_source_identity(&observed, &context),
            Err(LocalSlotMutationError::FeedbackReportingIntervalUnsafe { .. })
        ));
        observed.wal_receiver_status_interval = Duration::from_secs(1);
        observed.sync_replication_slots = false;
        assert!(matches!(
            validate_source_identity(&observed, &context),
            Err(LocalSlotMutationError::SlotSynchronizationDisabled)
        ));
    }

    #[test]
    fn standby_creation_rejects_changed_live_receiver_lineage() {
        let identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        let context = context(identity.clone());
        let mut observed = observed_source(&identity);
        observed.wal_receiver_received_timeline = Some(identity.source.timeline() + 1);
        assert!(matches!(
            validate_source_identity(&observed, &context),
            Err(LocalSlotMutationError::WalReceiverTimelineMismatch { .. })
        ));
        observed.wal_receiver_received_timeline = Some(identity.source.timeline());
        observed.wal_receiver_pid = NonZeroU32::new(99);
        assert!(matches!(
            validate_source_identity(&observed, &context),
            Err(LocalSlotMutationError::WalReceiverChanged)
        ));
    }

    #[test]
    fn create_revalidates_catalog_state_and_epoch_after_waiting_for_the_fence() {
        let identity = identity(ManagedLogicalSlotRole::PrimaryFailoverAnchor);
        let context = context(identity.clone());
        let mut authorization = catalog_authorization(&identity, CatalogAllocationKind::Probe);
        let attempt = Some(ManagedLogicalSlotReceiptId(Uuid::from_u128(3)));
        assert!(validate_catalog_authorization(&authorization, &context, attempt, None).is_ok());

        authorization.state = "retired".to_owned();
        assert!(matches!(
            validate_catalog_authorization(&authorization, &context, attempt, None),
            Err(LocalSlotMutationError::CatalogAuthorizationStateChanged {
                operation: LocalSlotMutationOperation::Create,
                ..
            })
        ));
        authorization.state = "allocated".to_owned();
        authorization.source = ReplicationSourceIdentity::new(
            identity.source.system_identifier(),
            identity.source.timeline(),
            identity.source.database_oid(),
            identity.source.restore_incarnation(),
            CatalogEpoch(identity.source.catalog_epoch().0 + 1),
        )
        .expect("newer catalog epoch");
        assert!(matches!(
            validate_catalog_authorization(&authorization, &context, attempt, None),
            Err(LocalSlotMutationError::StaleCatalogAuthorization { .. })
        ));
    }

    #[test]
    fn probe_drop_requires_the_exact_retirement_receipt() {
        let identity = identity(ManagedLogicalSlotRole::PrimaryFailoverAnchor);
        let context = context_for(LocalSlotMutationOperation::Drop, identity.clone());
        let expected_receipt = ManagedLogicalSlotReceiptId(Uuid::from_u128(3));
        let mut authorization = catalog_authorization(&identity, CatalogAllocationKind::Probe);
        authorization.state = "retiring".to_owned();
        authorization.cleanup_receipt_id = Some(expected_receipt);
        authorization.creation_attempt_state = Some("activated".to_owned());
        assert!(
            validate_catalog_authorization(&authorization, &context, None, Some(expected_receipt))
                .is_ok()
        );

        authorization.cleanup_receipt_id = Some(ManagedLogicalSlotReceiptId(Uuid::from_u128(4)));
        assert!(matches!(
            validate_catalog_authorization(&authorization, &context, None, Some(expected_receipt)),
            Err(LocalSlotMutationError::CatalogCleanupReceiptChanged(_))
        ));
    }

    #[test]
    fn consumer_drop_rejects_an_active_attachment_and_requires_fencing() {
        let identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        let context = context_for(LocalSlotMutationOperation::Drop, identity.clone());
        let receipt = ManagedLogicalSlotReceiptId(Uuid::from_u128(3));
        let mut authorization = catalog_authorization(&identity, CatalogAllocationKind::Consumer);
        authorization.state = "active".to_owned();
        authorization.creation_attempt_state = Some("activated".to_owned());

        assert!(
            validate_catalog_authorization(&authorization, &context, None, Some(receipt)).is_ok(),
            "a staged activation rollback remains eligible"
        );
        authorization.attachment_state = Some("active".to_owned());
        assert!(matches!(
            validate_catalog_authorization(&authorization, &context, None, Some(receipt)),
            Err(LocalSlotMutationError::CatalogAuthorizationStateChanged { .. })
        ));

        authorization.attachment_state = Some("retiring".to_owned());
        authorization.consumer_shard_state = Some("ready".to_owned());
        assert!(matches!(
            validate_catalog_authorization(&authorization, &context, None, Some(receipt)),
            Err(LocalSlotMutationError::CatalogAuthorizationStateChanged { .. })
        ));
        authorization.consumer_shard_state = Some("fenced".to_owned());
        assert!(
            validate_catalog_authorization(&authorization, &context, None, Some(receipt)).is_ok()
        );

        authorization.state = "retiring".to_owned();
        assert!(
            validate_catalog_authorization(&authorization, &context, None, Some(receipt)).is_ok()
        );
    }

    #[test]
    fn standby_decoder_create_requires_a_staged_attachment() {
        let identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        let context = context(identity.clone());
        let mut authorization = catalog_authorization(&identity, CatalogAllocationKind::Consumer);
        let attempt = Some(ManagedLogicalSlotReceiptId(Uuid::from_u128(3)));
        assert!(validate_catalog_authorization(&authorization, &context, attempt, None).is_ok());
        authorization.attachment_state = Some("retiring".to_owned());
        assert!(matches!(
            validate_catalog_authorization(&authorization, &context, attempt, None),
            Err(LocalSlotMutationError::CatalogAuthorizationStateChanged {
                operation: LocalSlotMutationOperation::Create,
                ..
            })
        ));
    }

    #[test]
    fn standby_drop_skips_receiver_and_creation_health() {
        let mut identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        identity.standby_path = None;
        let context = context_for(LocalSlotMutationOperation::Drop, identity.clone());
        let mut observed = observed_source(&identity);
        observed.wal_level = "replica".to_owned();
        observed.hot_standby_feedback = false;
        observed.wal_receiver_status_interval = Duration::ZERO;
        observed.sync_replication_slots = false;
        observed.primary_slot_name = None;
        observed.wal_receiver_pid = None;
        observed.wal_receiver_streaming = false;
        observed.wal_receiver_slot_name = None;
        observed.wal_receiver_received_timeline = None;
        observed.slot_sync_worker = None;
        assert!(validate_source_identity(&observed, &context).is_ok());
    }

    #[test]
    fn primary_creation_rejects_standby_role_but_ignores_standby_only_settings() {
        let identity = identity(ManagedLogicalSlotRole::PrimaryFailoverAnchor);
        let context = context(identity.clone());
        let mut observed = observed_source(&identity);
        observed.hot_standby_feedback = false;
        observed.sync_replication_slots = false;
        assert!(validate_source_identity(&observed, &context).is_ok());
        observed.recovery = RecoveryState::Standby;
        assert!(matches!(
            validate_source_identity(&observed, &context),
            Err(LocalSlotMutationError::WrongRecoveryState { .. })
        ));
    }

    #[test]
    fn receiver_timeline_preserves_postgres_unsigned_bit_pattern() {
        assert_eq!(
            optional_timeline_id(Some(i32::MIN), "timeline").expect("high-bit timeline"),
            Some(1_u32 << 31)
        );
        assert_eq!(
            optional_timeline_id(Some(-1), "timeline").expect("maximum timeline"),
            Some(u32::MAX)
        );
        assert!(matches!(
            optional_timeline_id(Some(0), "timeline"),
            Err(LocalSlotMutationError::InvalidSourceField("timeline"))
        ));
    }

    #[test]
    fn advisory_lock_snapshot_is_hard_bounded() {
        assert!(
            bounded_advisory_locks(
                vec![String::new(); MAX_ADVISORY_LOCK_ROWS],
                MAX_ADVISORY_LOCK_ROWS,
            )
            .is_ok()
        );
        assert!(matches!(
            bounded_advisory_locks(
                vec![String::new(); MAX_ADVISORY_LOCK_ROWS + 1],
                MAX_ADVISORY_LOCK_ROWS,
            ),
            Err(LocalSlotMutationError::TooManyAdvisoryLocks {
                maximum: MAX_ADVISORY_LOCK_ROWS
            })
        ));
    }

    #[tokio::test]
    async fn expiring_standby_proof_bounds_blocked_preflight() {
        let mut identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        identity
            .standby_path
            .as_mut()
            .expect("standby path")
            .valid_until = Instant::now() + Duration::from_millis(10);
        let context = context(identity);
        let error = bounded_preflight(
            &context,
            std::future::pending::<Result<(), LocalSlotMutationError>>(),
        )
        .await
        .expect_err("proof expiry must cancel blocked preflight");
        assert!(matches!(error, LocalSlotMutationError::StandbyPathExpired));
    }

    #[tokio::test]
    async fn delayed_create_statement_preparation_cannot_outlive_standby_proof() {
        let mut identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        identity
            .standby_path
            .as_mut()
            .expect("standby path")
            .valid_until = Instant::now() + Duration::from_millis(10);
        let context = context(identity);
        let delayed_preparation = async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok::<(), tokio_postgres::Error>(())
        };
        let error = prepare_mutation_statement(&context, delayed_preparation)
            .await
            .expect_err("statement preparation must finish within proof validity");
        assert!(matches!(error, LocalSlotMutationError::StandbyPathExpired));
    }

    #[tokio::test]
    async fn expired_completed_preflight_never_polls_create_dispatch() {
        let mut identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        identity
            .standby_path
            .as_mut()
            .expect("standby path")
            .valid_until = Instant::now();
        let context = context(identity);
        let dispatched = Arc::new(AtomicBool::new(false));
        let callback_state = Arc::clone(&dispatched);
        let error = dispatch_mutation_before_deadline(&context, async move {
            callback_state.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
        .expect_err("expired proof must reject completed preflight before dispatch");
        assert!(matches!(error, LocalSlotMutationError::StandbyPathExpired));
        assert!(!dispatched.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn elapsed_operation_deadline_never_polls_mutation_dispatch() {
        let mut context = context(identity(ManagedLogicalSlotRole::PrimaryFailoverAnchor));
        context.deadline = Instant::now();
        let dispatched = Arc::new(AtomicBool::new(false));
        let callback_state = Arc::clone(&dispatched);
        let error = dispatch_mutation_before_deadline(&context, async move {
            callback_state.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
        .expect_err("elapsed operation deadline must reject mutation before dispatch");
        assert!(matches!(
            error,
            LocalSlotMutationError::PreflightTimeout { .. }
        ));
        assert!(!dispatched.load(Ordering::SeqCst));
    }

    #[test]
    fn successful_create_shape_upgrades_only_bounded_ownership_evidence() {
        let identity = identity(ManagedLogicalSlotRole::StandbyLocalDecoder);
        let proven = validate_slot_shape(&observation(&identity), &identity, PgLsn(10), true)
            .expect("exact postflight shape");
        assert_eq!(proven.persistence, SlotPersistence::Persistent);
        assert_eq!(
            proven.ownership,
            SlotOwnership::Managed(identity.target.generation())
        );
    }

    #[test]
    fn drop_rejects_progress_regression_and_changed_boundary() {
        let identity = identity(ManagedLogicalSlotRole::PrimaryFailoverAnchor);
        let mut observed = observation(&identity);
        observed.confirmed_flush_lsn = Some(PgLsn(9));
        assert!(matches!(
            validate_slot_shape(&observed, &identity, PgLsn(10), false),
            Err(ManagedLogicalSlotShapeProblem::ConfirmedFlushLsnRegressed { .. })
        ));
        observed.confirmed_flush_lsn = Some(PgLsn(11));
        observed.two_phase_at = Some(PgLsn(11));
        assert!(matches!(
            validate_slot_shape(&observed, &identity, PgLsn(10), false),
            Err(ManagedLogicalSlotShapeProblem::WrongTwoPhaseBoundary { .. })
        ));
    }

    #[test]
    fn only_post_dispatch_failures_are_outcome_unknown() {
        let identity = identity(ManagedLogicalSlotRole::PrimaryFailoverAnchor);
        let context = context(identity);
        assert!(
            context
                .unknown(LocalSlotMutationUnknownCause::Deadline {
                    duration: context.duration,
                })
                .outcome_is_unknown()
        );
        assert!(!context.preflight_timeout().outcome_is_unknown());
    }

    #[test]
    fn elapsed_drop_deadline_returns_the_retry_receipt() {
        let identity = identity(ManagedLogicalSlotRole::PrimaryFailoverAnchor);
        let expected_target = identity.target.clone();
        let context = context_for(LocalSlotMutationOperation::Drop, identity.clone());
        let error = classify_drop_failure(receipt(&identity), context.preflight_timeout())
            .expect_err("a drop rejected before dispatch must fail");

        assert!(!error.outcome_is_unknown());
        let (receipt, source) = error
            .into_retry_receipt()
            .expect("a pre-dispatch deadline returns the sole retry receipt");
        assert_eq!(receipt.target(), &expected_target);
        assert!(matches!(
            source,
            LocalSlotMutationError::PreflightTimeout { .. }
        ));
    }

    #[test]
    fn successful_drop_receipt_preserves_the_verified_identity() {
        let identity = identity(ManagedLogicalSlotRole::PrimaryFailoverAnchor);
        let proof = drop_receipt(receipt(&identity), ManagedLogicalSlotDropOutcome::Dropped);

        assert_eq!(proof.target(), &identity.target);
        assert_eq!(proof.receipt_id(), receipt(&identity).receipt_id());
        assert_eq!(proof.source(), identity.source);
        assert_eq!(proof.role(), identity.role);
        assert_eq!(proof.database_name(), "postgres");
        assert_eq!(proof.outcome(), ManagedLogicalSlotDropOutcome::Dropped);
    }
}
