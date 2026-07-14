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

use std::{num::NonZeroU32, time::Duration};

use pgshard_catalog::CatalogOperationTimeout;
use pgshard_types::PgLsn;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::{JoinError, JoinHandle},
    time::{Instant, timeout, timeout_at},
};
use tokio_postgres::{Client, Connection, Row};

use crate::standby_slots::{
    LogicalSlotKind, LogicalSlotObservation, LogicalSlotPlugin, LogicalWalLevel, ManagedSlotTarget,
    RecoveryState, ReplicationSlotName, SettingState, SlotActivity, SlotInvalidation,
    SlotNameError, SlotOwnership, SlotPersistence, SlotWalRetention,
};

const MIN_POSTGRES_VERSION_NUM: i32 = 180_000;
const MAX_OBSERVATION_TARGETS: usize = 3;
const SERVER_STATEMENT_TIMEOUT_HEADROOM: Duration = Duration::from_millis(25);
const CONNECTION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);
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
           receiver.slot_name::pg_catalog.text \
      FROM pg_catalog.pg_control_system() AS control \
     CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint_control \
      LEFT JOIN pg_catalog.pg_stat_get_wal_receiver() AS receiver \
        ON receiver.pid IS NOT NULL";
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

/// One local server's `PostgreSQL` 18 state needed by standby-first decoding.
///
/// This is observation only. In particular, an enabled slot-sync setting is
/// not evidence that the background worker completed a cycle, and the latest
/// checkpoint timeline can conservatively lag a recovery timeline change until
/// `PostgreSQL` records a later restartpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalStandbyPrerequisiteObservation {
    system_identifier: u64,
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
}

impl LocalStandbyPrerequisiteObservation {
    /// Returns the unsigned `PostgreSQL` cluster system identifier.
    #[must_use]
    pub const fn system_identifier(&self) -> u64 {
        self.system_identifier
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

    /// Returns the last replayed WAL location, if `PostgreSQL` exposes one.
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
/// mutexes, not under one cross-slot lock. The interval conservatively brackets
/// the catalog query, but entries are not a point-in-time snapshot. A future
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
    entries: Vec<LogicalSlotSnapshotEntry>,
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

    /// Returns entries in exact request order, including missing slots.
    #[must_use]
    pub fn entries(&self) -> &[LogicalSlotSnapshotEntry] {
        &self.entries
    }
}

struct ConnectionTask {
    handle: Option<JoinHandle<Result<(), tokio_postgres::Error>>>,
}

impl ConnectionTask {
    fn new(handle: JoinHandle<Result<(), tokio_postgres::Error>>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn handle_mut(&mut self) -> &mut JoinHandle<Result<(), tokio_postgres::Error>> {
        self.handle
            .as_mut()
            .expect("slot observer connection task is consumed exactly once")
    }

    async fn abort_and_wait(mut self) {
        self.abort();
        let _ = timeout(CONNECTION_CLEANUP_TIMEOUT, self.handle_mut()).await;
        self.handle.take();
    }

    async fn finish<V>(mut self, value: V) -> Result<V, LocalSlotObservationError> {
        let result = match timeout(CONNECTION_CLEANUP_TIMEOUT, self.handle_mut()).await {
            Ok(Ok(Ok(()))) => Ok(value),
            Ok(Ok(Err(source))) => Err(LocalSlotObservationError::Connection(source)),
            Ok(Err(source)) => Err(LocalSlotObservationError::ConnectionTask(source)),
            Err(_) => {
                self.abort();
                let _ = timeout(CONNECTION_CLEANUP_TIMEOUT, self.handle_mut()).await;
                Err(LocalSlotObservationError::ConnectionCleanupTimeout {
                    duration: CONNECTION_CLEANUP_TIMEOUT,
                })
            }
        };
        self.handle.take();
        result
    }

    fn abort(&self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.abort();
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
/// 18, non-UTF8 encoding, malformed built-in state, a physical slot occupying
/// a requested logical name, or a result outside the exact request set.
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
        Ok(Ok(batch)) => connection_task.finish(batch).await,
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
        entries,
    })
}

fn parse_prerequisites(
    row: &Row,
) -> Result<LocalStandbyPrerequisiteObservation, LocalSlotObservationError> {
    let system_identifier = parse_system_identifier(row.try_get(0)?)?;
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
    let (wal_receiver_pid, wal_receiver_activity, wal_receiver_slot_name) =
        parse_wal_receiver(receiver_pid, receiver_status.as_deref(), receiver_slot_name)?;

    Ok(LocalStandbyPrerequisiteObservation {
        system_identifier,
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
    })
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

fn parse_wal_receiver(
    pid: Option<i32>,
    status: Option<&str>,
    slot_name: Option<String>,
) -> Result<
    (
        Option<NonZeroU32>,
        LocalWalReceiverActivity,
        Option<ReplicationSlotName>,
    ),
    LocalSlotObservationError,
> {
    let Some(pid) = pid else {
        if status.is_some() || slot_name.is_some() {
            return Err(LocalSlotObservationError::InconsistentWalReceiver);
        }
        return Ok((None, LocalWalReceiverActivity::Absent, None));
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
    Ok((Some(pid), activity, optional_slot_name(slot_name)?))
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

fn parse_logical_slot(row: &Row) -> Result<LogicalSlotObservation, LocalSlotObservationError> {
    let name_text: String = row.try_get("slot_name")?;
    let name = ReplicationSlotName::new(name_text.clone())?;
    let slot_type: String = row.try_get("slot_type")?;
    if slot_type != "logical" {
        return Err(LocalSlotObservationError::NonLogicalTarget(name_text));
    }
    let database_oid = row
        .try_get::<_, Option<i64>>("database_oid")?
        .ok_or_else(|| LocalSlotObservationError::MissingDatabaseOid(name_text.clone()))?;
    let database_oid = positive_u32(database_oid, "database_oid")?;
    let plugin = match row.try_get::<_, Option<String>>("plugin")?.as_deref() {
        Some("pgoutput") => LogicalSlotPlugin::PgOutput,
        _ => LogicalSlotPlugin::Other,
    };
    let failover: bool = row.try_get("failover")?;
    let synced: bool = row.try_get("synced")?;
    let kind = match (failover, synced) {
        (true, false) => LogicalSlotKind::FailoverAnchor,
        (true, true) => LogicalSlotKind::SynchronizedFailoverAnchor,
        (false, false) => LogicalSlotKind::StandbyLocalDecoder,
        (false, true) => LogicalSlotKind::Other,
    };
    let temporary: bool = row.try_get("temporary")?;
    let two_phase = if row.try_get::<_, bool>("two_phase")? {
        SettingState::Enabled
    } else {
        SettingState::Disabled
    };
    let active: bool = row.try_get("active")?;
    let active_pid: Option<i64> = row.try_get("active_pid")?;
    let activity = match (active, active_pid) {
        (false, None) => SlotActivity::Inactive,
        (true, Some(pid)) => {
            let pid = u32::try_from(pid)
                .ok()
                .and_then(NonZeroU32::new)
                .ok_or(LocalSlotObservationError::InvalidActivePid(pid))?;
            SlotActivity::Active(pid)
        }
        _ => return Err(LocalSlotObservationError::InconsistentActivity(name_text)),
    };
    let persistence = classify_persistence(temporary);

    Ok(LogicalSlotObservation {
        name,
        database_oid,
        plugin,
        kind,
        persistence,
        two_phase,
        two_phase_at: optional_lsn(row, "two_phase_at")?,
        activity,
        ownership: SlotOwnership::Unknown,
        invalidation: parse_invalidation(row.try_get("invalidation_reason")?)?,
        wal_retention: parse_wal_retention(row.try_get("wal_status")?)?,
        confirmed_flush_lsn: optional_lsn(row, "confirmed_flush_lsn")?,
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
    row.try_get::<_, Option<String>>(field)?
        .map(|value| {
            parse_lsn(&value).ok_or(LocalSlotObservationError::InvalidLsn { field, value })
        })
        .transpose()
}

fn positive_u32(value: i64, field: &'static str) -> Result<u32, LocalSlotObservationError> {
    u32::try_from(value)
        .ok()
        .and_then(NonZeroU32::new)
        .map(NonZeroU32::get)
        .ok_or(LocalSlotObservationError::InvalidPositiveInteger { field, value })
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
    /// `PostgreSQL` reported a zero checkpoint timeline identifier.
    #[error("PostgreSQL checkpoint timeline identifier must be nonzero")]
    InvalidTimelineId,
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
    /// `PostgreSQL` returned receiver details without a receiver PID.
    #[error("PostgreSQL WAL receiver details are inconsistent with its PID")]
    InconsistentWalReceiver,
    /// `PostgreSQL` returned a receiver status outside the `PostgreSQL` 18 closed set.
    #[error("unsupported PostgreSQL 18 WAL receiver status {0:?}")]
    UnsupportedWalReceiverStatus(String),
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
    #[error("logical slot field {field} contains invalid PostgreSQL LSN {value:?}")]
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
            parse_wal_receiver(None, None, None).expect("absent receiver"),
            (None, LocalWalReceiverActivity::Absent, None)
        );
        for (status, expected) in [
            ("stopped", LocalWalReceiverActivity::Stopped),
            ("starting", LocalWalReceiverActivity::Starting),
            ("streaming", LocalWalReceiverActivity::Streaming),
            ("waiting", LocalWalReceiverActivity::Waiting),
            ("restarting", LocalWalReceiverActivity::Restarting),
            ("stopping", LocalWalReceiverActivity::Stopping),
        ] {
            let (pid, activity, slot_name) = parse_wal_receiver(
                Some(42),
                Some(status),
                Some("pgshard_member_0001".to_owned()),
            )
            .expect("known receiver state");
            assert_eq!(pid.map(NonZeroU32::get), Some(42));
            assert_eq!(activity, expected);
            assert_eq!(
                slot_name.as_ref().map(ReplicationSlotName::as_str),
                Some("pgshard_member_0001")
            );
        }
        assert!(matches!(
            parse_wal_receiver(Some(42), None, None),
            Err(LocalSlotObservationError::WalReceiverDetailsUnavailable { pid })
                if pid.get() == 42
        ));
        assert!(matches!(
            parse_wal_receiver(Some(0), Some("streaming"), None),
            Err(LocalSlotObservationError::InvalidWalReceiverPid(0))
        ));
        assert!(matches!(
            parse_wal_receiver(None, Some("streaming"), None),
            Err(LocalSlotObservationError::InconsistentWalReceiver)
        ));
        assert!(matches!(
            parse_wal_receiver(None, None, Some("pgshard_member_0001".to_owned())),
            Err(LocalSlotObservationError::InconsistentWalReceiver)
        ));
        assert!(matches!(
            parse_wal_receiver(Some(42), Some("future"), None),
            Err(LocalSlotObservationError::UnsupportedWalReceiverStatus(status))
                if status == "future"
        ));
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
