//! Durable `shardschema` ownership for one slot-synchronization probe.
//!
//! Probe rows are permanent lifecycle records, not evidence that a `PostgreSQL`
//! slot exists. Allocation and state transitions use one dedicated bounded
//! connection and a `REPEATABLE READ` transaction. Every write is conditional,
//! so an ambiguous commit can be reconciled by loading the exact generation and
//! retrying the same transition. Final retirement additionally requires the
//! connection-bound absence fence returned by the local slot mutator.
//! Allocation, activation and retirement-start are serialized by the catalog's
//! database-enforced transaction advisory lock. The trigger acquires it after
//! the catalog-state row, preserving one lock order for typed and direct admin
//! writes. Final retirement additionally carries the live absence fence.
//!
//! Every operation consumes a newly connected, idle catalog session and starts
//! with `DISCARD ALL`. The connection must therefore authenticate as a
//! principal with the required direct catalog privileges. Session-local
//! `SET ROLE` and session-authorization state, advisory locks, prepared
//! statements, and settings are intentionally not accepted as mutation
//! authority. `PostgreSQL` role inheritance still participates in normal ACL
//! checks and is not removed by `DISCARD ALL`.

use std::{fmt, time::Duration};

use pgshard_catalog::{CatalogOperationTimeout, SHARDSCHEMA_DATABASE};
use pgshard_types::{CatalogEpoch, PgLsn};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{Instant, timeout_at},
};
use tokio_postgres::{Client, Connection, IsolationLevel, Row, Transaction};
use uuid::Uuid;

use crate::{
    postgres_connection::ConnectionTask,
    slot_catalog::valid_resource_name,
    slot_mutator::{
        ManagedLogicalSlotDropFence, ManagedLogicalSlotDropReceipt, ManagedLogicalSlotReceipt,
        ManagedLogicalSlotReceiptId, ManagedLogicalSlotRole, ManagedLogicalSlotTargetFenceError,
    },
    standby_slots::{
        ManagedSlotTarget, ManagedSlotTargetError, ReplicationSlotName, ReplicationSourceIdentity,
        SlotGeneration, SlotGenerationError, SlotNameError, SourceIdentityError,
    },
};

const MIN_POSTGRES_VERSION_NUM: i32 = 180_000;
const SERVER_STATEMENT_TIMEOUT_HEADROOM: Duration = Duration::from_millis(25);
const SERVER_TRANSACTION_TIMEOUT_GRACE: Duration = Duration::from_millis(101);

const REQUIREMENTS_SQL: &str = "\
    SELECT pg_catalog.current_setting('server_version_num')::pg_catalog.int4, \
           pg_catalog.current_database()::pg_catalog.text, \
           pg_catalog.getdatabaseencoding()::pg_catalog.text";

const SET_SESSION_TIMEOUTS_SQL: &str = "\
    SELECT pg_catalog.set_config('statement_timeout', $1, false), \
           pg_catalog.set_config('transaction_timeout', $2, false)";
const SET_LOCAL_TIMEOUTS_SQL: &str = "\
    SELECT pg_catalog.set_config('statement_timeout', $1, true), \
           pg_catalog.set_config('transaction_timeout', $2, true)";
const DISABLE_LOCAL_STATEMENT_TIMEOUT_SQL: &str = "SET LOCAL statement_timeout = 0";

const SELECT_CATALOG_EPOCH_SQL: &str = "\
    SELECT catalog_epoch \
      FROM pgshard_catalog.cluster_state \
     WHERE singleton";

const SELECT_PROBE_SQL: &str = "\
    SELECT probe_generation::pg_catalog.text AS probe_generation, \
           shard_id::pg_catalog.text AS shard_id, \
           restore_incarnation::pg_catalog.text AS restore_incarnation, \
           system_identifier::pg_catalog.text AS system_identifier, \
           database_oid, database_name::pg_catalog.text AS database_name, \
           source_timeline, slot_name::pg_catalog.text AS slot_name, \
           consistent_point::pg_catalog.text AS consistent_point, \
           creation_receipt_id::pg_catalog.text AS creation_receipt_id, \
           cleanup_receipt_id::pg_catalog.text AS cleanup_receipt_id, state \
      FROM pgshard_catalog.slot_sync_probes \
     WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid \
     LIMIT 2";

const SELECT_LIVE_PROBE_SQL: &str = "\
    SELECT probe_generation::pg_catalog.text AS probe_generation, \
           shard_id::pg_catalog.text AS shard_id, \
           restore_incarnation::pg_catalog.text AS restore_incarnation, \
           system_identifier::pg_catalog.text AS system_identifier, \
           database_oid, database_name::pg_catalog.text AS database_name, \
           source_timeline, slot_name::pg_catalog.text AS slot_name, \
           consistent_point::pg_catalog.text AS consistent_point, \
           creation_receipt_id::pg_catalog.text AS creation_receipt_id, \
           cleanup_receipt_id::pg_catalog.text AS cleanup_receipt_id, state \
      FROM pgshard_catalog.slot_sync_probes \
     WHERE shard_id = $1::pg_catalog.text \
       AND state IN ('allocated', 'active', 'retiring') \
     ORDER BY probe_generation \
     LIMIT 2";

const INSERT_PROBE_SQL: &str = "\
    INSERT INTO pgshard_catalog.slot_sync_probes( \
        probe_generation, shard_id, restore_incarnation, system_identifier, \
        database_oid, database_name, source_timeline, slot_name \
    ) VALUES ( \
        $1::pg_catalog.text::pg_catalog.uuid, $2::pg_catalog.text, \
        $3::pg_catalog.text::pg_catalog.uuid, $4::pg_catalog.text::pg_catalog.numeric, \
        $5::pg_catalog.int8, $6::pg_catalog.text, $7::pg_catalog.int8, \
        $8::pg_catalog.text \
    )";

const ACTIVATE_PROBE_SQL: &str = "\
    UPDATE pgshard_catalog.slot_sync_probes \
       SET consistent_point = $2::pg_catalog.text::pg_catalog.pg_lsn, \
           creation_receipt_id = $3::pg_catalog.text::pg_catalog.uuid, \
           state = 'active', activated_at = pg_catalog.statement_timestamp() \
     WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid \
       AND state = 'allocated'";

const BEGIN_RETIREMENT_SQL: &str = "\
    UPDATE pgshard_catalog.slot_sync_probes \
       SET state = 'retiring', \
           cleanup_receipt_id = $2::pg_catalog.text::pg_catalog.uuid, \
           retiring_at = pg_catalog.statement_timestamp() \
     WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid \
       AND state IN ('allocated', 'active')";

const COMPLETE_RETIREMENT_SQL: &str = "\
    UPDATE pgshard_catalog.slot_sync_probes \
       SET state = 'retired', retired_at = pg_catalog.statement_timestamp() \
     WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid \
       AND state = 'retiring'";

/// Immutable source components used to reserve a probe generation.
///
/// Unlike an attachable [`ReplicationSourceIdentity`], this allocation input
/// may carry genesis catalog epoch zero. A successful insert increments the
/// catalog epoch before the returned probe can authorize a `PostgreSQL` slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotSyncProbeAllocationSource {
    system_identifier: u64,
    timeline: u32,
    database_oid: u32,
    restore_incarnation: Uuid,
    expected_catalog_epoch: CatalogEpoch,
}

impl SlotSyncProbeAllocationSource {
    /// Creates complete immutable source components for allocation.
    ///
    /// # Errors
    ///
    /// Rejects a zero server, timeline, or database identity and a nil restore
    /// incarnation. Catalog epoch zero is valid only at this allocation edge.
    pub fn new(
        system_identifier: u64,
        timeline: u32,
        database_oid: u32,
        restore_incarnation: Uuid,
        expected_catalog_epoch: CatalogEpoch,
    ) -> Result<Self, SlotSyncProbeAllocationSourceError> {
        if system_identifier == 0 {
            return Err(SlotSyncProbeAllocationSourceError::SystemIdentifier);
        }
        if timeline == 0 {
            return Err(SlotSyncProbeAllocationSourceError::Timeline);
        }
        if database_oid == 0 {
            return Err(SlotSyncProbeAllocationSourceError::DatabaseOid);
        }
        if restore_incarnation.is_nil() {
            return Err(SlotSyncProbeAllocationSourceError::RestoreIncarnation);
        }
        Ok(Self {
            system_identifier,
            timeline,
            database_oid,
            restore_incarnation,
            expected_catalog_epoch,
        })
    }

    /// Returns the `PostgreSQL` cluster system identifier.
    #[must_use]
    pub const fn system_identifier(self) -> u64 {
        self.system_identifier
    }

    /// Returns the writable source timeline.
    #[must_use]
    pub const fn timeline(self) -> u32 {
        self.timeline
    }

    /// Returns the logical database OID.
    #[must_use]
    pub const fn database_oid(self) -> u32 {
        self.database_oid
    }

    /// Returns the immutable shard restore incarnation.
    #[must_use]
    pub const fn restore_incarnation(self) -> Uuid {
        self.restore_incarnation
    }

    /// Returns the catalog epoch that must still be current for first insert.
    #[must_use]
    pub const fn expected_catalog_epoch(self) -> CatalogEpoch {
        self.expected_catalog_epoch
    }
}

/// Invalid source components for a slot-sync probe allocation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SlotSyncProbeAllocationSourceError {
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
}

/// Requested permanent identity for one per-shard slot-sync probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotSyncProbeAllocation {
    shard_id: String,
    target: ManagedSlotTarget,
    source: SlotSyncProbeAllocationSource,
    database_name: String,
}

impl SlotSyncProbeAllocation {
    /// Creates an allocation bound to one exact catalog/source generation.
    ///
    /// # Errors
    ///
    /// Rejects a non-canonical shard name or a database name outside the
    /// catalog's `PostgreSQL` identifier byte bound.
    pub fn new(
        shard_id: impl Into<String>,
        target: ManagedSlotTarget,
        source: SlotSyncProbeAllocationSource,
        database_name: impl Into<String>,
    ) -> Result<Self, SlotSyncProbeAllocationError> {
        let shard_id = shard_id.into();
        if !valid_resource_name(&shard_id) {
            return Err(SlotSyncProbeAllocationError::InvalidShardId);
        }
        let database_name = database_name.into();
        if !valid_database_name(&database_name) {
            return Err(SlotSyncProbeAllocationError::InvalidDatabaseName);
        }
        Ok(Self {
            shard_id,
            target,
            source,
            database_name,
        })
    }

    /// Returns the canonical shard resource name.
    #[must_use]
    pub fn shard_id(&self) -> &str {
        &self.shard_id
    }

    /// Returns the never-reused, generation-qualified slot target.
    #[must_use]
    pub const fn target(&self) -> &ManagedSlotTarget {
        &self.target
    }

    /// Returns the catalog-fenced source identity used for first allocation.
    #[must_use]
    pub const fn source(&self) -> SlotSyncProbeAllocationSource {
        self.source
    }

    /// Returns the exact `PostgreSQL` database containing the logical slot.
    #[must_use]
    pub fn database_name(&self) -> &str {
        &self.database_name
    }
}

/// Invalid slot-sync probe allocation input.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SlotSyncProbeAllocationError {
    /// Shard name violates the catalog resource-name domain.
    #[error("slot-sync probe shard ID must be a canonical resource name")]
    InvalidShardId,
    /// Database name violates `PostgreSQL`'s identifier byte bound.
    #[error("slot-sync probe database name must contain 1-63 UTF-8 bytes and no NUL")]
    InvalidDatabaseName,
}

/// Durable catalog lifecycle state for one probe generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotSyncProbeState {
    /// The identity is reserved but no `PostgreSQL` creation is recorded.
    Allocated,
    /// A matching local creation receipt supplied the consistent point.
    Active,
    /// Cleanup is required or in progress.
    Retiring,
    /// Exact absence was proven and the permanent tombstone is closed.
    Retired,
}

impl fmt::Display for SlotSyncProbeState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Allocated => "allocated",
            Self::Active => "active",
            Self::Retiring => "retiring",
            Self::Retired => "retired",
        })
    }
}

/// One transactionally loaded durable probe record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogSlotSyncProbe {
    shard_id: String,
    target: ManagedSlotTarget,
    source: ReplicationSourceIdentity,
    database_name: String,
    consistent_point: Option<PgLsn>,
    creation_receipt_id: Option<ManagedLogicalSlotReceiptId>,
    cleanup_receipt_id: Option<ManagedLogicalSlotReceiptId>,
    state: SlotSyncProbeState,
}

impl CatalogSlotSyncProbe {
    /// Returns the canonical shard resource name.
    #[must_use]
    pub fn shard_id(&self) -> &str {
        &self.shard_id
    }

    /// Returns the never-reused, generation-qualified server target.
    #[must_use]
    pub const fn target(&self) -> &ManagedSlotTarget {
        &self.target
    }

    /// Returns the immutable source lineage plus the loaded catalog epoch.
    #[must_use]
    pub const fn source(&self) -> ReplicationSourceIdentity {
        self.source
    }

    /// Returns the exact `PostgreSQL` database containing the logical slot.
    #[must_use]
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Returns the one-time `PostgreSQL` creation boundary, when activated.
    #[must_use]
    pub const fn consistent_point(&self) -> Option<PgLsn> {
        self.consistent_point
    }

    /// Returns the exact creation receipt persisted by activation, if any.
    #[must_use]
    pub const fn creation_receipt_id(&self) -> Option<ManagedLogicalSlotReceiptId> {
        self.creation_receipt_id
    }

    /// Returns the exact receipt selected for cleanup, if retirement began.
    #[must_use]
    pub const fn cleanup_receipt_id(&self) -> Option<ManagedLogicalSlotReceiptId> {
        self.cleanup_receipt_id
    }

    /// Returns the durable lifecycle state.
    #[must_use]
    pub const fn state(&self) -> SlotSyncProbeState {
        self.state
    }
}

/// Durable probe mutation whose commit can require exact-row reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotSyncProbeCatalogMutation {
    /// Reserve a permanent generation and name.
    Allocate,
    /// Record the verified `PostgreSQL` creation boundary.
    Activate,
    /// Fence the generation for cleanup.
    BeginRetirement,
    /// Close the permanent tombstone after exact absence proof.
    CompleteRetirement,
}

impl fmt::Display for SlotSyncProbeCatalogMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Allocate => "allocate",
            Self::Activate => "activate",
            Self::BeginRetirement => "begin_retirement",
            Self::CompleteRetirement => "complete_retirement",
        })
    }
}

/// Loads one exact permanent probe generation, including its retired tombstone.
///
/// # Errors
///
/// Fails closed on invalid catalog state, `PostgreSQL`/session requirements,
/// typed-row parsing, connection failure, or the absolute operation deadline.
pub async fn load_slot_sync_probe<S, T>(
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    target: &ManagedSlotTarget,
) -> Result<Option<CatalogSlotSyncProbe>, SlotSyncProbeCatalogError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let result = run_command(
        client,
        connection,
        operation_timeout,
        ProbeCommand::LoadExact(target),
    )
    .await?;
    match result {
        ProbeCommandResult::Optional(probe) => Ok(probe),
        ProbeCommandResult::Probe(_) => unreachable!("load command returns an optional probe"),
    }
}

/// Loads the at-most-one non-retired probe for a shard.
///
/// # Errors
///
/// Rejects a non-canonical shard name and otherwise has the same fail-closed
/// behavior as [`load_slot_sync_probe`].
pub async fn load_live_slot_sync_probe<S, T>(
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    shard_id: &str,
) -> Result<Option<CatalogSlotSyncProbe>, SlotSyncProbeCatalogError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    if !valid_resource_name(shard_id) {
        return Err(SlotSyncProbeCatalogError::InvalidShardId);
    }
    let result = run_command(
        client,
        connection,
        operation_timeout,
        ProbeCommand::LoadLive(shard_id),
    )
    .await?;
    match result {
        ProbeCommandResult::Optional(probe) => Ok(probe),
        ProbeCommandResult::Probe(_) => unreachable!("load command returns an optional probe"),
    }
}

/// Allocates or idempotently reloads one exact permanent probe identity.
///
/// A new allocation requires the caller's catalog epoch to still be current.
/// Retrying the same generation after an ambiguous commit simply reloads its
/// immutable row without issuing a no-op write or advancing the catalog epoch.
///
/// # Errors
///
/// Fails closed on stale catalog authority, a different live probe, identity
/// mismatch, transaction conflict, `PostgreSQL`/session failure, or deadline.
pub async fn allocate_slot_sync_probe<S, T>(
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    allocation: &SlotSyncProbeAllocation,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    Ok(expect_probe(
        run_command(
            client,
            connection,
            operation_timeout,
            ProbeCommand::Allocate(allocation),
        )
        .await?,
    ))
}

/// Activates an allocated row using the exact local slot-creation receipt.
///
/// # Errors
///
/// Rejects a receipt for another target, source, database, or role, stale
/// catalog authority, a changed activation boundary, or an invalid lifecycle
/// transition. Ambiguous commits remain safely retryable with the same inputs.
pub async fn activate_slot_sync_probe<S, T>(
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    probe: &CatalogSlotSyncProbe,
    receipt: &ManagedLogicalSlotReceipt,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    Ok(expect_probe(
        run_command(
            client,
            connection,
            operation_timeout,
            ProbeCommand::Activate { probe, receipt },
        )
        .await?,
    ))
}

/// Moves an allocated or active probe into cleanup for one exact creation.
///
/// # Errors
///
/// Rejects stale catalog authority, changed identity or creation receipt, a
/// missing row, or a catalog/connection failure. Repeating an already-applied
/// transition with the same receipt is a read-only idempotent success.
pub async fn begin_slot_sync_probe_retirement<S, T>(
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    probe: &CatalogSlotSyncProbe,
    receipt: &ManagedLogicalSlotReceipt,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    Ok(expect_probe(
        run_command(
            client,
            connection,
            operation_timeout,
            ProbeCommand::BeginRetirement { probe, receipt },
        )
        .await?,
    ))
}

/// Permanently retires a probe after the exact local slot is proven absent.
///
/// The drop fence is intentionally process-local and must still identify the
/// same source backend after catalog COMMIT. Recovery after a process restart
/// or external mutation still requires a future durable reconciler and fresh
/// absence proof; callers cannot manufacture retirement authority from a
/// catalog row or a released receipt alone.
///
/// # Errors
///
/// Rejects a mismatched absence receipt, lost target fence, stale catalog
/// authority, a row that is not retiring, or a catalog/connection failure.
/// Repeating an already-applied retirement under the same live fence is a
/// read-only idempotent success.
pub async fn complete_slot_sync_probe_retirement(
    operation_timeout: CatalogOperationTimeout,
    probe: &CatalogSlotSyncProbe,
    absence: &mut ManagedLogicalSlotDropFence,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError> {
    let duration = operation_timeout.get();
    let deadline = Instant::now() + duration;
    validate_absence_receipt(probe, absence.receipt())?;
    absence
        .verify_held_until(deadline, duration)
        .await
        .map_err(|source| target_fence_lost(probe, source))?;
    let result = timeout_at(
        deadline,
        execute_fenced_retirement(absence.catalog_client_mut(), deadline, duration, probe),
    )
    .await
    .unwrap_or_else(|_| {
        Err(SlotSyncProbeCatalogError::OutcomeUnknown {
            operation: SlotSyncProbeCatalogMutation::CompleteRetirement,
            target: probe.target.clone(),
            source: SlotSyncProbeCatalogUnknownCause::Deadline { duration },
        })
    });
    let fence_result = absence.verify_held_until(deadline, duration).await;
    match (result, fence_result) {
        (Ok(result), Ok(())) => Ok(expect_probe(result)),
        (Err(error), Ok(())) => Err(error),
        (_, Err(source)) => Err(target_fence_lost(probe, source)),
    }
}

async fn execute_fenced_retirement(
    client: &mut Client,
    deadline: Instant,
    duration: Duration,
    probe: &CatalogSlotSyncProbe,
) -> Result<ProbeCommandResult, SlotSyncProbeCatalogError> {
    set_session_timeouts(client, deadline).await?;
    let requirements = client.query_one(REQUIREMENTS_SQL, &[]).await?;
    validate_requirements(&requirements)?;
    let loaded = run_mutation_transaction(
        client,
        deadline,
        duration,
        ProbeMutation::CompleteRetirement(probe),
    )
    .await?;
    Ok(ProbeCommandResult::Probe(loaded))
}

fn target_fence_lost(
    probe: &CatalogSlotSyncProbe,
    source: ManagedLogicalSlotTargetFenceError,
) -> SlotSyncProbeCatalogError {
    SlotSyncProbeCatalogError::TargetFenceLost {
        target: probe.target.clone(),
        source,
    }
}

fn expect_probe(result: ProbeCommandResult) -> CatalogSlotSyncProbe {
    match result {
        ProbeCommandResult::Probe(probe) => probe,
        ProbeCommandResult::Optional(_) => unreachable!("mutation command returns a probe"),
    }
}

enum ProbeCommand<'a> {
    LoadExact(&'a ManagedSlotTarget),
    LoadLive(&'a str),
    Allocate(&'a SlotSyncProbeAllocation),
    Activate {
        probe: &'a CatalogSlotSyncProbe,
        receipt: &'a ManagedLogicalSlotReceipt,
    },
    BeginRetirement {
        probe: &'a CatalogSlotSyncProbe,
        receipt: &'a ManagedLogicalSlotReceipt,
    },
}

impl ProbeCommand<'_> {
    const fn mutation(&self) -> Option<SlotSyncProbeCatalogMutation> {
        match self {
            Self::LoadExact(_) | Self::LoadLive(_) => None,
            Self::Allocate(_) => Some(SlotSyncProbeCatalogMutation::Allocate),
            Self::Activate { .. } => Some(SlotSyncProbeCatalogMutation::Activate),
            Self::BeginRetirement { .. } => Some(SlotSyncProbeCatalogMutation::BeginRetirement),
        }
    }

    fn target(&self) -> Option<&ManagedSlotTarget> {
        match self {
            Self::LoadExact(target) => Some(target),
            Self::LoadLive(_) => None,
            Self::Allocate(allocation) => Some(&allocation.target),
            Self::Activate { probe, .. } | Self::BeginRetirement { probe, .. } => {
                Some(&probe.target)
            }
        }
    }
}

enum ProbeCommandResult {
    Optional(Option<CatalogSlotSyncProbe>),
    Probe(CatalogSlotSyncProbe),
}

async fn run_command<S, T>(
    client: Client,
    connection: Connection<S, T>,
    operation_timeout: CatalogOperationTimeout,
    command: ProbeCommand<'_>,
) -> Result<ProbeCommandResult, SlotSyncProbeCatalogError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let duration = operation_timeout.get();
    let deadline = Instant::now() + duration;
    run_command_until(client, connection, deadline, duration, command).await
}

async fn run_command_until<S, T>(
    mut client: Client,
    connection: Connection<S, T>,
    deadline: Instant,
    duration: Duration,
    command: ProbeCommand<'_>,
) -> Result<ProbeCommandResult, SlotSyncProbeCatalogError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mutation = command.mutation();
    let target = command.target().cloned();
    let connection_task = ConnectionTask::new(tokio::spawn(connection));
    let result = timeout_at(
        deadline,
        execute_before_deadline(&mut client, deadline, duration, command),
    )
    .await;
    drop(client);
    connection_task.abort_and_wait().await;

    match result {
        Ok(result) => result,
        Err(_) => match (mutation, target) {
            (Some(operation), Some(target)) => Err(SlotSyncProbeCatalogError::OutcomeUnknown {
                operation,
                target,
                source: SlotSyncProbeCatalogUnknownCause::Deadline { duration },
            }),
            _ => Err(SlotSyncProbeCatalogError::OperationTimeout { duration }),
        },
    }
}

async fn execute_before_deadline(
    client: &mut Client,
    deadline: Instant,
    duration: Duration,
    command: ProbeCommand<'_>,
) -> Result<ProbeCommandResult, SlotSyncProbeCatalogError> {
    client.batch_execute("DISCARD ALL").await?;
    set_session_timeouts(client, deadline).await?;
    let requirements = client.query_one(REQUIREMENTS_SQL, &[]).await?;
    validate_requirements(&requirements)?;

    match command {
        ProbeCommand::LoadExact(target) => {
            let probe =
                run_read_transaction(client, deadline, duration, ProbeRead::Exact(target)).await?;
            Ok(ProbeCommandResult::Optional(probe))
        }
        ProbeCommand::LoadLive(shard_id) => {
            let probe =
                run_read_transaction(client, deadline, duration, ProbeRead::Live(shard_id)).await?;
            Ok(ProbeCommandResult::Optional(probe))
        }
        ProbeCommand::Allocate(allocation) => {
            let probe = run_mutation_transaction(
                client,
                deadline,
                duration,
                ProbeMutation::Allocate(allocation),
            )
            .await?;
            Ok(ProbeCommandResult::Probe(probe))
        }
        ProbeCommand::Activate { probe, receipt } => {
            validate_creation_receipt_identity(probe, receipt)?;
            let consistent_point = receipt.creation_lsn();
            let loaded = run_mutation_transaction(
                client,
                deadline,
                duration,
                ProbeMutation::Activate {
                    probe,
                    consistent_point,
                    receipt_id: receipt.receipt_id(),
                },
            )
            .await?;
            Ok(ProbeCommandResult::Probe(loaded))
        }
        ProbeCommand::BeginRetirement { probe, receipt } => {
            validate_creation_receipt_identity(probe, receipt)?;
            let loaded = run_mutation_transaction(
                client,
                deadline,
                duration,
                ProbeMutation::BeginRetirement {
                    probe,
                    receipt_id: receipt.receipt_id(),
                    consistent_point: receipt.creation_lsn(),
                },
            )
            .await?;
            Ok(ProbeCommandResult::Probe(loaded))
        }
    }
}

enum ProbeRead<'a> {
    Exact(&'a ManagedSlotTarget),
    Live(&'a str),
}

enum ProbeMutation<'a> {
    Allocate(&'a SlotSyncProbeAllocation),
    Activate {
        probe: &'a CatalogSlotSyncProbe,
        consistent_point: PgLsn,
        receipt_id: ManagedLogicalSlotReceiptId,
    },
    BeginRetirement {
        probe: &'a CatalogSlotSyncProbe,
        receipt_id: ManagedLogicalSlotReceiptId,
        consistent_point: PgLsn,
    },
    CompleteRetirement(&'a CatalogSlotSyncProbe),
}

impl ProbeMutation<'_> {
    const fn operation(&self) -> SlotSyncProbeCatalogMutation {
        match self {
            Self::Allocate(_) => SlotSyncProbeCatalogMutation::Allocate,
            Self::Activate { .. } => SlotSyncProbeCatalogMutation::Activate,
            Self::BeginRetirement { .. } => SlotSyncProbeCatalogMutation::BeginRetirement,
            Self::CompleteRetirement(_) => SlotSyncProbeCatalogMutation::CompleteRetirement,
        }
    }

    const fn target(&self) -> &ManagedSlotTarget {
        match self {
            Self::Allocate(allocation) => &allocation.target,
            Self::Activate { probe, .. }
            | Self::BeginRetirement { probe, .. }
            | Self::CompleteRetirement(probe) => &probe.target,
        }
    }
}

async fn run_read_transaction(
    client: &mut Client,
    deadline: Instant,
    duration: Duration,
    operation: ProbeRead<'_>,
) -> Result<Option<CatalogSlotSyncProbe>, SlotSyncProbeCatalogError> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    if let Err(source) = set_local_timeouts(&transaction, deadline).await {
        return rollback_known(transaction, source.into(), duration).await;
    }
    let epoch = match load_catalog_epoch(&transaction).await {
        Ok(epoch) => epoch,
        Err(error) => return rollback_known(transaction, error, duration).await,
    };
    let result = match operation {
        ProbeRead::Exact(target) => load_matching_exact(&transaction, target, epoch).await,
        ProbeRead::Live(shard_id) => load_live(&transaction, shard_id, epoch).await,
    };
    let value = match result {
        Ok(value) => value,
        Err(error) => return rollback_known(transaction, error, duration).await,
    };
    if let Err(source) = transaction
        .batch_execute(DISABLE_LOCAL_STATEMENT_TIMEOUT_SQL)
        .await
    {
        return rollback_known(transaction, source.into(), duration).await;
    }
    transaction.commit().await?;
    Ok(value)
}

async fn load_matching_exact(
    transaction: &Transaction<'_>,
    target: &ManagedSlotTarget,
    epoch: CatalogEpoch,
) -> Result<Option<CatalogSlotSyncProbe>, SlotSyncProbeCatalogError> {
    let probe = load_exact(transaction, target.generation().as_uuid(), epoch).await?;
    if let Some(probe) = &probe
        && probe.target != *target
    {
        return Err(SlotSyncProbeCatalogError::TargetMismatch {
            requested: target.clone(),
            catalog: probe.target.clone(),
        });
    }
    Ok(probe)
}

async fn run_mutation_transaction(
    client: &mut Client,
    deadline: Instant,
    duration: Duration,
    operation: ProbeMutation<'_>,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError> {
    let operation_name = operation.operation();
    let target = operation.target().clone();
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    if let Err(source) = set_local_timeouts(&transaction, deadline).await {
        return rollback_known(transaction, source.into(), duration).await;
    }
    let epoch = match load_catalog_epoch(&transaction).await {
        Ok(epoch) => epoch,
        Err(error) => return rollback_known(transaction, error, duration).await,
    };
    let mutation_result = match operation {
        ProbeMutation::Allocate(allocation) => {
            allocate_in_transaction(&transaction, allocation, epoch).await
        }
        ProbeMutation::Activate {
            probe,
            consistent_point,
            receipt_id,
        } => {
            activate_in_transaction(&transaction, probe, consistent_point, receipt_id, epoch).await
        }
        ProbeMutation::BeginRetirement {
            probe,
            receipt_id,
            consistent_point,
        } => {
            begin_retirement_in_transaction(
                &transaction,
                probe,
                receipt_id,
                consistent_point,
                epoch,
            )
            .await
        }
        ProbeMutation::CompleteRetirement(probe) => {
            complete_retirement_in_transaction(&transaction, probe, epoch).await
        }
    };
    let value = match mutation_result {
        Ok(value) => value,
        Err(error) => return rollback_known(transaction, error, duration).await,
    };
    if let Err(source) = transaction
        .batch_execute(DISABLE_LOCAL_STATEMENT_TIMEOUT_SQL)
        .await
    {
        return rollback_known(transaction, source.into(), duration).await;
    }
    if Instant::now() >= deadline {
        return rollback_known(
            transaction,
            SlotSyncProbeCatalogError::OperationTimeout { duration },
            duration,
        )
        .await;
    }
    transaction
        .commit()
        .await
        .map_err(|source| SlotSyncProbeCatalogError::OutcomeUnknown {
            operation: operation_name,
            target,
            source: SlotSyncProbeCatalogUnknownCause::Postgres(source),
        })?;
    Ok(value)
}

async fn rollback_known<T>(
    transaction: Transaction<'_>,
    error: SlotSyncProbeCatalogError,
    duration: Duration,
) -> Result<T, SlotSyncProbeCatalogError> {
    let normalized = normalize_rolled_back_error(error, duration);
    transaction
        .rollback()
        .await
        .map_err(|source| SlotSyncProbeCatalogError::RollbackFailed { source })?;
    Err(normalized)
}

fn normalize_rolled_back_error(
    error: SlotSyncProbeCatalogError,
    duration: Duration,
) -> SlotSyncProbeCatalogError {
    let SlotSyncProbeCatalogError::Postgres(source) = error else {
        return error;
    };
    let Some(code) = source.code() else {
        return SlotSyncProbeCatalogError::Postgres(source);
    };
    match code.code() {
        "40001" | "23505" => SlotSyncProbeCatalogError::ConcurrentCatalogMutation,
        "57014" => SlotSyncProbeCatalogError::StatementTimeout { duration },
        "25P04" => SlotSyncProbeCatalogError::OperationTimeout { duration },
        _ => SlotSyncProbeCatalogError::Postgres(source),
    }
}

async fn allocate_in_transaction(
    transaction: &Transaction<'_>,
    allocation: &SlotSyncProbeAllocation,
    epoch: CatalogEpoch,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError> {
    if let Some(existing) =
        load_exact(transaction, allocation.target.generation().as_uuid(), epoch).await?
    {
        verify_allocation_identity(&existing, allocation)?;
        return Ok(existing);
    }
    require_current_epoch(allocation.source.expected_catalog_epoch(), epoch)?;
    if let Some(existing) = load_live(transaction, &allocation.shard_id, epoch).await? {
        return Err(SlotSyncProbeCatalogError::LiveProbeExists {
            shard_id: allocation.shard_id.clone(),
            target: existing.target,
        });
    }

    let generation = allocation.target.generation().as_uuid().to_string();
    let restore = allocation.source.restore_incarnation().to_string();
    let system_identifier = allocation.source.system_identifier().to_string();
    let database_oid = i64::from(allocation.source.database_oid());
    let timeline = i64::from(allocation.source.timeline());
    let changed = transaction
        .execute(
            INSERT_PROBE_SQL,
            &[
                &generation,
                &allocation.shard_id,
                &restore,
                &system_identifier,
                &database_oid,
                &allocation.database_name,
                &timeline,
                &allocation.target.name().as_str(),
            ],
        )
        .await?;
    require_single_row(changed, SlotSyncProbeCatalogMutation::Allocate)?;
    let epoch = load_catalog_epoch(transaction).await?;
    let inserted = load_exact(transaction, allocation.target.generation().as_uuid(), epoch)
        .await?
        .ok_or(SlotSyncProbeCatalogError::MutationRowMissing(
            SlotSyncProbeCatalogMutation::Allocate,
        ))?;
    verify_allocation_identity(&inserted, allocation)?;
    Ok(inserted)
}

async fn activate_in_transaction(
    transaction: &Transaction<'_>,
    expected: &CatalogSlotSyncProbe,
    consistent_point: PgLsn,
    receipt_id: ManagedLogicalSlotReceiptId,
    epoch: CatalogEpoch,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError> {
    let current = load_expected(transaction, expected, epoch).await?;
    match current.state {
        SlotSyncProbeState::Active => {
            if current.creation_receipt_id != Some(receipt_id) {
                return Err(SlotSyncProbeCatalogError::ActivationReceiptMismatch);
            }
            if current.consistent_point == Some(consistent_point) {
                return Ok(current);
            }
            return Err(SlotSyncProbeCatalogError::ActivationBoundaryChanged {
                catalog: current.consistent_point,
                receipt: consistent_point,
            });
        }
        SlotSyncProbeState::Allocated => {}
        SlotSyncProbeState::Retiring | SlotSyncProbeState::Retired => {
            return Err(SlotSyncProbeCatalogError::InvalidTransition {
                operation: SlotSyncProbeCatalogMutation::Activate,
                state: current.state,
            });
        }
    }
    require_current_epoch(expected.source.catalog_epoch(), epoch)?;
    let changed = transaction
        .execute(
            ACTIVATE_PROBE_SQL,
            &[
                &expected.target.generation().as_uuid().to_string(),
                &lsn_text(consistent_point),
                &receipt_id.as_uuid().to_string(),
            ],
        )
        .await?;
    require_single_row(changed, SlotSyncProbeCatalogMutation::Activate)?;
    load_after_mutation(
        transaction,
        expected,
        SlotSyncProbeCatalogMutation::Activate,
    )
    .await
}

async fn begin_retirement_in_transaction(
    transaction: &Transaction<'_>,
    expected: &CatalogSlotSyncProbe,
    receipt_id: ManagedLogicalSlotReceiptId,
    consistent_point: PgLsn,
    epoch: CatalogEpoch,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError> {
    let current = load_expected(transaction, expected, epoch).await?;
    match current.state {
        SlotSyncProbeState::Retiring | SlotSyncProbeState::Retired => {
            if current.cleanup_receipt_id == Some(receipt_id)
                && current
                    .creation_receipt_id
                    .is_none_or(|id| id == receipt_id)
                && current
                    .consistent_point
                    .is_none_or(|point| point == consistent_point)
            {
                return Ok(current);
            }
            return Err(SlotSyncProbeCatalogError::RetirementReceiptMismatch);
        }
        SlotSyncProbeState::Active => {
            if current.creation_receipt_id != Some(receipt_id)
                || current.consistent_point != Some(consistent_point)
            {
                return Err(SlotSyncProbeCatalogError::RetirementReceiptMismatch);
            }
        }
        SlotSyncProbeState::Allocated => {}
    }
    require_current_epoch(expected.source.catalog_epoch(), epoch)?;
    let changed = transaction
        .execute(
            BEGIN_RETIREMENT_SQL,
            &[
                &expected.target.generation().as_uuid().to_string(),
                &receipt_id.as_uuid().to_string(),
            ],
        )
        .await?;
    require_single_row(changed, SlotSyncProbeCatalogMutation::BeginRetirement)?;
    load_after_mutation(
        transaction,
        expected,
        SlotSyncProbeCatalogMutation::BeginRetirement,
    )
    .await
}

async fn complete_retirement_in_transaction(
    transaction: &Transaction<'_>,
    expected: &CatalogSlotSyncProbe,
    epoch: CatalogEpoch,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError> {
    let current = load_expected(transaction, expected, epoch).await?;
    match current.state {
        SlotSyncProbeState::Retired => return Ok(current),
        SlotSyncProbeState::Retiring => {}
        SlotSyncProbeState::Allocated | SlotSyncProbeState::Active => {
            return Err(SlotSyncProbeCatalogError::InvalidTransition {
                operation: SlotSyncProbeCatalogMutation::CompleteRetirement,
                state: current.state,
            });
        }
    }
    require_current_epoch(expected.source.catalog_epoch(), epoch)?;
    let changed = transaction
        .execute(
            COMPLETE_RETIREMENT_SQL,
            &[&expected.target.generation().as_uuid().to_string()],
        )
        .await?;
    require_single_row(changed, SlotSyncProbeCatalogMutation::CompleteRetirement)?;
    load_after_mutation(
        transaction,
        expected,
        SlotSyncProbeCatalogMutation::CompleteRetirement,
    )
    .await
}

async fn load_after_mutation(
    transaction: &Transaction<'_>,
    expected: &CatalogSlotSyncProbe,
    operation: SlotSyncProbeCatalogMutation,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError> {
    let epoch = load_catalog_epoch(transaction).await?;
    let loaded = load_exact(transaction, expected.target.generation().as_uuid(), epoch)
        .await?
        .ok_or(SlotSyncProbeCatalogError::MutationRowMissing(operation))?;
    verify_probe_identity(&loaded, expected)?;
    Ok(loaded)
}

async fn load_expected(
    transaction: &Transaction<'_>,
    expected: &CatalogSlotSyncProbe,
    epoch: CatalogEpoch,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError> {
    let loaded = load_exact(transaction, expected.target.generation().as_uuid(), epoch)
        .await?
        .ok_or_else(|| SlotSyncProbeCatalogError::ProbeNotFound(expected.target.clone()))?;
    verify_probe_identity(&loaded, expected)?;
    Ok(loaded)
}

async fn load_catalog_epoch(
    transaction: &Transaction<'_>,
) -> Result<CatalogEpoch, SlotSyncProbeCatalogError> {
    let row = transaction
        .query_opt(SELECT_CATALOG_EPOCH_SQL, &[])
        .await?
        .ok_or(SlotSyncProbeCatalogError::MissingCatalogState)?;
    let value: i64 = row.try_get(0)?;
    let value = u64::try_from(value)
        .map_err(|_| SlotSyncProbeCatalogError::InvalidCatalogField("catalog_epoch"))?;
    Ok(CatalogEpoch(value))
}

async fn load_exact(
    transaction: &Transaction<'_>,
    generation: Uuid,
    epoch: CatalogEpoch,
) -> Result<Option<CatalogSlotSyncProbe>, SlotSyncProbeCatalogError> {
    let rows = transaction
        .query(SELECT_PROBE_SQL, &[&generation.to_string()])
        .await?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => parse_probe(row, epoch).map(Some),
        _ => Err(SlotSyncProbeCatalogError::DuplicateProbeGeneration),
    }
}

async fn load_live(
    transaction: &Transaction<'_>,
    shard_id: &str,
    epoch: CatalogEpoch,
) -> Result<Option<CatalogSlotSyncProbe>, SlotSyncProbeCatalogError> {
    let rows = transaction
        .query(SELECT_LIVE_PROBE_SQL, &[&shard_id])
        .await?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => parse_probe(row, epoch).map(Some),
        _ => Err(SlotSyncProbeCatalogError::DuplicateLiveProbe(
            shard_id.to_owned(),
        )),
    }
}

fn parse_probe(
    row: &Row,
    epoch: CatalogEpoch,
) -> Result<CatalogSlotSyncProbe, SlotSyncProbeCatalogError> {
    let generation = parse_uuid(row, "probe_generation")?;
    let generation = SlotGeneration::new(generation)?;
    let target = ManagedSlotTarget::new(
        ReplicationSlotName::new(required_string(row, "slot_name")?)?,
        generation,
    )?;
    let shard_id = required_string(row, "shard_id")?;
    if !valid_resource_name(&shard_id) {
        return Err(SlotSyncProbeCatalogError::InvalidCatalogField("shard_id"));
    }
    let database_name = required_string(row, "database_name")?;
    if !valid_database_name(&database_name) {
        return Err(SlotSyncProbeCatalogError::InvalidCatalogField(
            "database_name",
        ));
    }
    let restore_incarnation = parse_uuid(row, "restore_incarnation")?;
    let system_identifier_text = required_string(row, "system_identifier")?;
    let system_identifier = system_identifier_text
        .parse::<u64>()
        .map_err(|_| SlotSyncProbeCatalogError::InvalidCatalogField("system_identifier"))?;
    let database_oid = positive_u32(row, "database_oid")?;
    let timeline = positive_u32(row, "source_timeline")?;
    let source = ReplicationSourceIdentity::new(
        system_identifier,
        timeline,
        database_oid,
        restore_incarnation,
        epoch,
    )?;
    let consistent_point = optional_string(row, "consistent_point")?
        .map(|value| {
            parse_lsn(&value).filter(|lsn| lsn.0 != 0).ok_or(
                SlotSyncProbeCatalogError::InvalidCatalogField("consistent_point"),
            )
        })
        .transpose()?;
    let creation_receipt_id = optional_receipt_id(row, "creation_receipt_id")?;
    let cleanup_receipt_id = optional_receipt_id(row, "cleanup_receipt_id")?;
    let state_text = required_string(row, "state")?;
    let state = match state_text.as_str() {
        "allocated" => SlotSyncProbeState::Allocated,
        "active" => SlotSyncProbeState::Active,
        "retiring" => SlotSyncProbeState::Retiring,
        "retired" => SlotSyncProbeState::Retired,
        _ => return Err(SlotSyncProbeCatalogError::UnsupportedState(state_text)),
    };
    let lifecycle_is_consistent = match state {
        SlotSyncProbeState::Allocated => {
            consistent_point.is_none()
                && creation_receipt_id.is_none()
                && cleanup_receipt_id.is_none()
        }
        SlotSyncProbeState::Active => {
            consistent_point.is_some()
                && creation_receipt_id.is_some()
                && cleanup_receipt_id.is_none()
        }
        SlotSyncProbeState::Retiring => {
            cleanup_receipt_id.is_some()
                && consistent_point.is_some() == creation_receipt_id.is_some()
                && creation_receipt_id
                    .zip(cleanup_receipt_id)
                    .is_none_or(|(creation, cleanup)| creation == cleanup)
        }
        SlotSyncProbeState::Retired => {
            (cleanup_receipt_id.is_some()
                && consistent_point.is_some() == creation_receipt_id.is_some()
                && creation_receipt_id
                    .zip(cleanup_receipt_id)
                    .is_none_or(|(creation, cleanup)| creation == cleanup))
                || (creation_receipt_id.is_none() && cleanup_receipt_id.is_none())
        }
    };
    if !lifecycle_is_consistent {
        return Err(SlotSyncProbeCatalogError::InconsistentLifecycleRow);
    }
    Ok(CatalogSlotSyncProbe {
        shard_id,
        target,
        source,
        database_name,
        consistent_point,
        creation_receipt_id,
        cleanup_receipt_id,
        state,
    })
}

fn validate_requirements(row: &Row) -> Result<(), SlotSyncProbeCatalogError> {
    let version: i32 = row.try_get(0)?;
    if version < MIN_POSTGRES_VERSION_NUM {
        return Err(SlotSyncProbeCatalogError::UnsupportedPostgresVersion(
            version,
        ));
    }
    let database: String = row.try_get(1)?;
    if database != SHARDSCHEMA_DATABASE {
        return Err(SlotSyncProbeCatalogError::WrongDatabase(database));
    }
    let encoding: String = row.try_get(2)?;
    if encoding != "UTF8" {
        return Err(SlotSyncProbeCatalogError::WrongEncoding(encoding));
    }
    Ok(())
}

fn validate_creation_receipt_identity(
    probe: &CatalogSlotSyncProbe,
    receipt: &ManagedLogicalSlotReceipt,
) -> Result<(), SlotSyncProbeCatalogError> {
    if receipt.target() != &probe.target
        || receipt.role() != ManagedLogicalSlotRole::PrimaryFailoverAnchor
        || receipt.database_name() != probe.database_name
        || !same_source_lineage(receipt.source(), probe.source)
        || receipt.creation_lsn().0 == 0
    {
        return Err(SlotSyncProbeCatalogError::CreationReceiptMismatch);
    }
    Ok(())
}

fn validate_absence_receipt(
    probe: &CatalogSlotSyncProbe,
    absence: &ManagedLogicalSlotDropReceipt,
) -> Result<(), SlotSyncProbeCatalogError> {
    if absence.target() != &probe.target
        || absence.role() != ManagedLogicalSlotRole::PrimaryFailoverAnchor
        || absence.database_name() != probe.database_name
        || !same_source_lineage(absence.source(), probe.source)
        || probe.cleanup_receipt_id != Some(absence.receipt_id())
    {
        return Err(SlotSyncProbeCatalogError::AbsenceReceiptMismatch);
    }
    Ok(())
}

fn verify_allocation_identity(
    probe: &CatalogSlotSyncProbe,
    allocation: &SlotSyncProbeAllocation,
) -> Result<(), SlotSyncProbeCatalogError> {
    if probe.shard_id != allocation.shard_id
        || probe.target != allocation.target
        || probe.database_name != allocation.database_name
        || !same_allocation_source_lineage(probe.source, allocation.source)
    {
        return Err(SlotSyncProbeCatalogError::AllocationIdentityMismatch);
    }
    Ok(())
}

fn verify_probe_identity(
    current: &CatalogSlotSyncProbe,
    expected: &CatalogSlotSyncProbe,
) -> Result<(), SlotSyncProbeCatalogError> {
    if current.shard_id != expected.shard_id
        || current.target != expected.target
        || current.database_name != expected.database_name
        || !same_source_lineage(current.source, expected.source)
    {
        return Err(SlotSyncProbeCatalogError::ProbeIdentityChanged);
    }
    Ok(())
}

fn same_source_lineage(left: ReplicationSourceIdentity, right: ReplicationSourceIdentity) -> bool {
    left.system_identifier() == right.system_identifier()
        && left.timeline() == right.timeline()
        && left.database_oid() == right.database_oid()
        && left.restore_incarnation() == right.restore_incarnation()
}

fn same_allocation_source_lineage(
    current: ReplicationSourceIdentity,
    allocation: SlotSyncProbeAllocationSource,
) -> bool {
    current.system_identifier() == allocation.system_identifier()
        && current.timeline() == allocation.timeline()
        && current.database_oid() == allocation.database_oid()
        && current.restore_incarnation() == allocation.restore_incarnation()
}

fn require_current_epoch(
    expected: CatalogEpoch,
    observed: CatalogEpoch,
) -> Result<(), SlotSyncProbeCatalogError> {
    if expected != observed {
        return Err(SlotSyncProbeCatalogError::StaleCatalogEpoch { expected, observed });
    }
    Ok(())
}

fn require_single_row(
    changed: u64,
    operation: SlotSyncProbeCatalogMutation,
) -> Result<(), SlotSyncProbeCatalogError> {
    if changed != 1 {
        return Err(SlotSyncProbeCatalogError::UnexpectedMutationCount { operation, changed });
    }
    Ok(())
}

async fn set_session_timeouts(
    client: &Client,
    deadline: Instant,
) -> Result<(), tokio_postgres::Error> {
    let (statement, transaction) = server_timeout_settings(deadline);
    client
        .query_one(SET_SESSION_TIMEOUTS_SQL, &[&statement, &transaction])
        .await?;
    Ok(())
}

async fn set_local_timeouts(
    transaction: &Transaction<'_>,
    deadline: Instant,
) -> Result<(), tokio_postgres::Error> {
    let (statement, transaction_timeout) = server_timeout_settings(deadline);
    transaction
        .query_one(SET_LOCAL_TIMEOUTS_SQL, &[&statement, &transaction_timeout])
        .await?;
    Ok(())
}

fn server_timeout_settings(deadline: Instant) -> (String, String) {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let transaction = remaining.saturating_add(SERVER_TRANSACTION_TIMEOUT_GRACE);
    (
        postgres_milliseconds(
            remaining
                .saturating_sub(SERVER_STATEMENT_TIMEOUT_HEADROOM)
                .max(Duration::from_millis(1)),
        ),
        postgres_milliseconds(transaction),
    )
}

fn postgres_milliseconds(duration: Duration) -> String {
    let milliseconds = u64::try_from(duration.as_millis())
        .expect("bounded catalog timeout fits PostgreSQL milliseconds");
    format!("{milliseconds}ms")
}

fn valid_database_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 63 && !value.contains('\0')
}

fn required_string(row: &Row, field: &'static str) -> Result<String, SlotSyncProbeCatalogError> {
    optional_string(row, field)?.ok_or(SlotSyncProbeCatalogError::MissingCatalogField(field))
}

fn optional_string(
    row: &Row,
    field: &'static str,
) -> Result<Option<String>, SlotSyncProbeCatalogError> {
    Ok(row.try_get(field)?)
}

fn parse_uuid(row: &Row, field: &'static str) -> Result<Uuid, SlotSyncProbeCatalogError> {
    let value = required_string(row, field)?;
    let parsed = Uuid::parse_str(&value)
        .map_err(|_| SlotSyncProbeCatalogError::InvalidCatalogField(field))?;
    if parsed.is_nil() {
        return Err(SlotSyncProbeCatalogError::InvalidCatalogField(field));
    }
    Ok(parsed)
}

fn optional_receipt_id(
    row: &Row,
    field: &'static str,
) -> Result<Option<ManagedLogicalSlotReceiptId>, SlotSyncProbeCatalogError> {
    optional_string(row, field)?
        .map(|value| {
            Uuid::parse_str(&value)
                .ok()
                .and_then(ManagedLogicalSlotReceiptId::from_uuid)
                .ok_or(SlotSyncProbeCatalogError::InvalidCatalogField(field))
        })
        .transpose()
}

fn positive_u32(row: &Row, field: &'static str) -> Result<u32, SlotSyncProbeCatalogError> {
    let value: i64 = row.try_get(field)?;
    let value =
        u32::try_from(value).map_err(|_| SlotSyncProbeCatalogError::InvalidCatalogField(field))?;
    if value == 0 {
        return Err(SlotSyncProbeCatalogError::InvalidCatalogField(field));
    }
    Ok(value)
}

fn parse_lsn(value: &str) -> Option<PgLsn> {
    let (high, low) = value.split_once('/')?;
    if high.is_empty() || high.len() > 8 || low.is_empty() || low.len() > 8 {
        return None;
    }
    let high = u32::from_str_radix(high, 16).ok()?;
    let low = u32::from_str_radix(low, 16).ok()?;
    Some(PgLsn((u64::from(high) << 32) | u64::from(low)))
}

fn lsn_text(lsn: PgLsn) -> String {
    format!("{:X}/{:X}", lsn.0 >> 32, lsn.0 & u64::from(u32::MAX))
}

/// Failure while loading or transitioning a durable slot-sync probe.
#[derive(Debug, Error)]
pub enum SlotSyncProbeCatalogError {
    /// `PostgreSQL` query or transaction failure with a known rolled-back result.
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    /// The absolute read deadline elapsed.
    #[error("slot-sync probe catalog operation exceeded {duration:?}")]
    OperationTimeout {
        /// Validated whole-operation deadline.
        duration: Duration,
    },
    /// `PostgreSQL` canceled a statement and the transaction rolled back.
    #[error("slot-sync probe catalog statement exceeded {duration:?}")]
    StatementTimeout {
        /// Validated whole-operation deadline.
        duration: Duration,
    },
    /// Concurrent catalog serialization or uniqueness race; reload and retry.
    #[error("slot-sync probe catalog mutation raced another committed catalog change")]
    ConcurrentCatalogMutation,
    /// A write COMMIT or outer mutation deadline may have taken effect.
    #[error(
        "slot-sync probe catalog {operation} outcome for {target:?} is unknown; reload the exact generation before retrying: {source}"
    )]
    OutcomeUnknown {
        /// Durable lifecycle transition attempted.
        operation: SlotSyncProbeCatalogMutation,
        /// Exact generation-qualified target to reconcile.
        target: ManagedSlotTarget,
        /// Failure observed at or beyond the ambiguous boundary.
        #[source]
        source: SlotSyncProbeCatalogUnknownCause,
    },
    /// The canonical catalog target fence was lost before the boundary was proven.
    #[error(
        "slot-sync probe target fence for {target:?} was lost across catalog retirement; reconcile the exact slot and catalog row: {source}"
    )]
    TargetFenceLost {
        /// Exact generation-qualified target whose absence is no longer fenced.
        target: ManagedSlotTarget,
        /// Canonical `shardschema` session liveness failure.
        #[source]
        source: ManagedLogicalSlotTargetFenceError,
    },
    /// Explicit rollback failed; the dedicated session is discarded.
    #[error("slot-sync probe catalog rollback failed: {source}")]
    RollbackFailed {
        /// `PostgreSQL` rollback failure.
        #[source]
        source: tokio_postgres::Error,
    },
    /// Server predates the minimum supported release.
    #[error("slot-sync probe catalog requires PostgreSQL 18 or newer; observed {0}")]
    UnsupportedPostgresVersion(i32),
    /// Connection targets a database other than `shardschema`.
    #[error("slot-sync probe catalog requires shardschema; observed {0:?}")]
    WrongDatabase(String),
    /// Catalog database is not UTF8.
    #[error("slot-sync probe catalog requires UTF8; observed {0:?}")]
    WrongEncoding(String),
    /// Cluster-state singleton is absent.
    #[error("slot-sync probe catalog cluster_state singleton is missing")]
    MissingCatalogState,
    /// Public load request used a non-canonical shard ID.
    #[error("slot-sync probe shard ID must be a canonical resource name")]
    InvalidShardId,
    /// Required catalog column was unexpectedly NULL.
    #[error("slot-sync probe catalog field {0} is missing")]
    MissingCatalogField(&'static str),
    /// Catalog column violated its closed Rust representation.
    #[error("slot-sync probe catalog field {0} is invalid")]
    InvalidCatalogField(&'static str),
    /// Catalog contained an unsupported lifecycle label.
    #[error("unsupported slot-sync probe catalog state {0:?}")]
    UnsupportedState(String),
    /// State and consistent-point fields disagree.
    #[error("slot-sync probe catalog lifecycle row is internally inconsistent")]
    InconsistentLifecycleRow,
    /// Primary-key uniqueness unexpectedly failed.
    #[error("catalog returned duplicate rows for one slot-sync probe generation")]
    DuplicateProbeGeneration,
    /// Partial live uniqueness unexpectedly failed.
    #[error("catalog returned multiple live slot-sync probes for shard {0:?}")]
    DuplicateLiveProbe(String),
    /// Exact generation exists under a different server-side name.
    #[error("requested probe target {requested:?} differs from catalog target {catalog:?}")]
    TargetMismatch {
        /// Caller target.
        requested: ManagedSlotTarget,
        /// Durable target.
        catalog: ManagedSlotTarget,
    },
    /// Same generation was previously allocated with another immutable identity.
    #[error("slot-sync probe allocation identity differs from its permanent catalog row")]
    AllocationIdentityMismatch,
    /// Loaded mutation token no longer identifies the same permanent row.
    #[error("slot-sync probe identity changed in the catalog")]
    ProbeIdentityChanged,
    /// Another generation already owns the shard's live-probe slot.
    #[error("shard {shard_id:?} already has live slot-sync probe {target:?}")]
    LiveProbeExists {
        /// Conflicting shard.
        shard_id: String,
        /// Existing generation-qualified target.
        target: ManagedSlotTarget,
    },
    /// Caller token predates a committed catalog change.
    #[error("stale slot-sync probe catalog epoch {expected:?}; current is {observed:?}")]
    StaleCatalogEpoch {
        /// Epoch carried by caller authority.
        expected: CatalogEpoch,
        /// Epoch loaded transactionally.
        observed: CatalogEpoch,
    },
    /// Requested generation is absent.
    #[error("slot-sync probe {0:?} is not present in the catalog")]
    ProbeNotFound(ManagedSlotTarget),
    /// Creation receipt does not authorize this probe.
    #[error("logical-slot creation receipt does not match the slot-sync probe")]
    CreationReceiptMismatch,
    /// An active row belongs to another successful creation attempt.
    #[error("slot-sync probe activation receipt differs from the catalog receipt")]
    ActivationReceiptMismatch,
    /// Absence receipt does not authorize this probe tombstone.
    #[error("logical-slot absence receipt does not match the slot-sync probe")]
    AbsenceReceiptMismatch,
    /// Retirement was requested with another successful creation attempt.
    #[error("logical-slot creation receipt does not match the probe cleanup receipt")]
    RetirementReceiptMismatch,
    /// An active row already recorded another one-time boundary.
    #[error(
        "slot-sync probe activation boundary changed: catalog {catalog:?}, receipt {receipt:?}"
    )]
    ActivationBoundaryChanged {
        /// Existing catalog boundary.
        catalog: Option<PgLsn>,
        /// Receipt boundary.
        receipt: PgLsn,
    },
    /// Lifecycle operation is not valid from the loaded state.
    #[error("cannot {operation} a slot-sync probe in state {state}")]
    InvalidTransition {
        /// Requested mutation.
        operation: SlotSyncProbeCatalogMutation,
        /// Current durable state.
        state: SlotSyncProbeState,
    },
    /// Conditional DML did not affect exactly one row.
    #[error("slot-sync probe {operation} changed {changed} rows instead of exactly one")]
    UnexpectedMutationCount {
        /// Requested mutation.
        operation: SlotSyncProbeCatalogMutation,
        /// `PostgreSQL` affected-row count.
        changed: u64,
    },
    /// A committed DML statement did not expose its own row.
    #[error("slot-sync probe {0} row was missing after its catalog mutation")]
    MutationRowMissing(SlotSyncProbeCatalogMutation),
    /// Source identity stored in the catalog was invalid.
    #[error(transparent)]
    SourceIdentity(#[from] SourceIdentityError),
    /// Slot generation was invalid.
    #[error(transparent)]
    SlotGeneration(#[from] SlotGenerationError),
    /// Slot name was invalid.
    #[error(transparent)]
    SlotName(#[from] SlotNameError),
    /// Slot name did not encode the complete generation.
    #[error(transparent)]
    ManagedSlotTarget(#[from] ManagedSlotTargetError),
}

impl SlotSyncProbeCatalogError {
    /// Returns true only when the catalog commit must be reloaded before retry.
    #[must_use]
    pub const fn outcome_is_unknown(&self) -> bool {
        matches!(
            self,
            Self::OutcomeUnknown { .. } | Self::TargetFenceLost { .. }
        )
    }
}

/// Cause observed at a potentially committed catalog boundary.
#[derive(Debug, Error)]
pub enum SlotSyncProbeCatalogUnknownCause {
    /// Whole-operation deadline elapsed.
    #[error("operation exceeded {duration:?}")]
    Deadline {
        /// Validated operation deadline.
        duration: Duration,
    },
    /// `PostgreSQL` connection or COMMIT response failed.
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> ManagedSlotTarget {
        let generation = SlotGeneration::new(Uuid::from_u128(1)).expect("generation");
        ManagedSlotTarget::new(
            ReplicationSlotName::new(format!("sync_probe_{}", generation.as_uuid().simple()))
                .expect("slot name"),
            generation,
        )
        .expect("managed target")
    }

    fn source(epoch: u64) -> ReplicationSourceIdentity {
        ReplicationSourceIdentity::new(1, 1, 1, Uuid::from_u128(2), CatalogEpoch(epoch))
            .expect("source")
    }

    fn allocation_source(epoch: u64) -> SlotSyncProbeAllocationSource {
        SlotSyncProbeAllocationSource::new(1, 1, 1, Uuid::from_u128(2), CatalogEpoch(epoch))
            .expect("allocation source")
    }

    #[test]
    fn allocation_rejects_invalid_catalog_identifiers() {
        assert!(matches!(
            SlotSyncProbeAllocation::new("Shard", target(), allocation_source(1), "shardschema"),
            Err(SlotSyncProbeAllocationError::InvalidShardId)
        ));
        assert!(matches!(
            SlotSyncProbeAllocation::new("shard-0000", target(), allocation_source(1), ""),
            Err(SlotSyncProbeAllocationError::InvalidDatabaseName)
        ));
        assert!(matches!(
            SlotSyncProbeAllocation::new("shard-0000", target(), allocation_source(1), "bad\0name"),
            Err(SlotSyncProbeAllocationError::InvalidDatabaseName)
        ));
        assert!(matches!(
            SlotSyncProbeAllocation::new(
                "shard-0000",
                target(),
                allocation_source(1),
                "a".repeat(64)
            ),
            Err(SlotSyncProbeAllocationError::InvalidDatabaseName)
        ));
        assert!(matches!(
            SlotSyncProbeAllocation::new(
                "shard-0000",
                target(),
                allocation_source(1),
                "é".repeat(32)
            ),
            Err(SlotSyncProbeAllocationError::InvalidDatabaseName)
        ));
        assert!(
            SlotSyncProbeAllocation::new(
                "shard-0000",
                target(),
                allocation_source(1),
                "a".repeat(63)
            )
            .is_ok()
        );
    }

    #[test]
    fn allocation_source_accepts_genesis_but_requires_complete_lineage() {
        assert_eq!(
            allocation_source(0).expected_catalog_epoch(),
            CatalogEpoch(0)
        );
        for (source, expected) in [
            (
                SlotSyncProbeAllocationSource::new(0, 1, 1, Uuid::from_u128(2), CatalogEpoch(0)),
                SlotSyncProbeAllocationSourceError::SystemIdentifier,
            ),
            (
                SlotSyncProbeAllocationSource::new(1, 0, 1, Uuid::from_u128(2), CatalogEpoch(0)),
                SlotSyncProbeAllocationSourceError::Timeline,
            ),
            (
                SlotSyncProbeAllocationSource::new(1, 1, 0, Uuid::from_u128(2), CatalogEpoch(0)),
                SlotSyncProbeAllocationSourceError::DatabaseOid,
            ),
            (
                SlotSyncProbeAllocationSource::new(1, 1, 1, Uuid::nil(), CatalogEpoch(0)),
                SlotSyncProbeAllocationSourceError::RestoreIncarnation,
            ),
        ] {
            assert_eq!(source, Err(expected));
        }
    }

    #[test]
    fn catalog_epoch_fence_requires_exact_equality() {
        assert!(require_current_epoch(CatalogEpoch(3), CatalogEpoch(3)).is_ok());
        assert!(matches!(
            require_current_epoch(CatalogEpoch(2), CatalogEpoch(3)),
            Err(SlotSyncProbeCatalogError::StaleCatalogEpoch {
                expected: CatalogEpoch(2),
                observed: CatalogEpoch(3)
            })
        ));
    }

    #[test]
    fn immutable_source_comparison_excludes_only_catalog_epoch() {
        assert!(same_source_lineage(source(1), source(2)));
        let different =
            ReplicationSourceIdentity::new(2, 1, 1, Uuid::from_u128(2), CatalogEpoch(2))
                .expect("different source");
        assert!(!same_source_lineage(source(1), different));
    }

    #[test]
    fn lsn_round_trip_preserves_full_unsigned_value() {
        for value in [1, 0x1_0000_0002, u64::MAX] {
            let lsn = PgLsn(value);
            assert_eq!(parse_lsn(&lsn_text(lsn)), Some(lsn));
        }
    }
}
