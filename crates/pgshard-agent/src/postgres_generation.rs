//! WAL-backed writable-generation publication through quarantined `PostgreSQL`.
#![cfg_attr(test, allow(dead_code))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pgshard_types::PgLsn;
use pgshard_types::writable_generation::{
    DurableWritableGeneration, WritableGenerationTransition, WritableGenerationTransitionError,
    classify_writable_generation_transition,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_postgres::{Client, Config, NoTls};

use crate::domain::{
    GenerationDurabilityEvidence, ReplicationStreamState, ReplicationSyncState,
    SourceReplicationCandidateEvidence, SourceReplicationEvidence, StandbyReplicationEvidence,
};

#[cfg(test)]
use tokio::sync::oneshot;

const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(25);
const CONNECTION_OPTIONS: &str = "-c search_path=pg_catalog \
    -c session_preload_libraries= -c local_preload_libraries= \
    -c event_triggers=off -c jit=off -c default_tablespace= -c temp_tablespaces= \
    -c default_table_access_method=heap -c default_transaction_read_only=off \
    -c default_transaction_isolation=read\\ committed \
    -c row_security=off -c synchronous_commit=on -c log_statement=none \
    -c log_min_error_statement=panic -c log_parameter_max_length=0 \
    -c log_parameter_max_length_on_error=0";
const TRANSACTION_SETTINGS: &str = "\
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\
    SET LOCAL search_path = pg_catalog;\
    SET LOCAL lock_timeout = '2s';\
    SET LOCAL statement_timeout = '5s';\
    SET LOCAL transaction_timeout = '10s';\
    SET LOCAL idle_in_transaction_session_timeout = '5s';";
const LOCAL_COMMIT: &str = "SET LOCAL synchronous_commit = local";
const REMOTE_APPLY_COMMIT: &str = "SET LOCAL synchronous_commit = remote_apply";
const DISABLE_REMOTE_COMMIT_TIMEOUTS: &str = "\
    SET LOCAL statement_timeout = 0;\
    SET LOCAL transaction_timeout = 0;";
const CREATE_SCHEMA: &str = "CREATE SCHEMA pgshard_internal AUTHORIZATION postgres";
const SCHEMA_IS_SAFE: &str = "\
    SELECT n.nspowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'postgres') \
       AND n.nspacl IS NULL \
       AND NOT EXISTS (SELECT FROM pg_catalog.pg_default_acl AS d \
                       WHERE d.defaclrole = n.nspowner \
                         AND (d.defaclnamespace = 0 OR d.defaclnamespace = n.oid)) \
    FROM pg_catalog.pg_namespace AS n WHERE n.nspname = 'pgshard_internal'";
const CREATE_TABLE: &str = "\
    CREATE TABLE pgshard_internal.writable_generation (\
        singleton boolean PRIMARY KEY,\
        generation bytea NOT NULL\
    )";
const RELATION_IS_SAFE: &str = "\
    SELECT n.nspowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'postgres') \
       AND c.relowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'postgres') \
       AND n.nspacl IS NULL AND c.relacl IS NULL \
       AND c.relkind = 'r' AND c.relpersistence = 'p' AND c.reltablespace = 0 \
       AND c.relam = (SELECT oid FROM pg_catalog.pg_am WHERE amname = 'heap') \
       AND c.reloptions IS NULL AND NOT c.relispartition AND c.relreplident = 'd' \
       AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity \
       AND NOT c.relhasrules AND NOT c.relhastriggers \
       AND (SELECT pg_catalog.array_agg(\
                a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod) \
                    || ':' || a.attnotnull::text \
                ORDER BY a.attnum) \
            FROM pg_catalog.pg_attribute AS a \
            WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) \
           = ARRAY['singleton:boolean:true', 'generation:bytea:true'] \
       AND NOT EXISTS (SELECT FROM pg_catalog.pg_attrdef AS d WHERE d.adrelid = c.oid) \
       AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t \
                       WHERE t.tgrelid = c.oid AND NOT t.tgisinternal) \
       AND NOT EXISTS (SELECT FROM pg_catalog.pg_rewrite AS r WHERE r.ev_class = c.oid) \
       AND NOT EXISTS (SELECT FROM pg_catalog.pg_policy AS p WHERE p.polrelid = c.oid) \
       AND NOT EXISTS (SELECT FROM pg_catalog.pg_inherits AS i \
                       WHERE i.inhrelid = c.oid OR i.inhparent = c.oid) \
       AND (SELECT bool_and(x.convalidated) \
                    AND array_agg(x.contype::text || ':' || x.conkey::text \
                                  ORDER BY x.contype, x.conkey::text) \
                        = ARRAY['n:{1}', 'n:{2}', 'p:{1}'] \
            FROM pg_catalog.pg_constraint AS x WHERE x.conrelid = c.oid) \
       AND (SELECT count(*) = 1 AND bool_and( \
                       i.indisprimary AND i.indisunique AND NOT i.indisexclusion \
                       AND i.indimmediate AND i.indisvalid AND i.indisready \
                       AND i.indislive AND NOT i.indisclustered \
                       AND NOT i.indisreplident AND NOT i.indcheckxmin \
                       AND NOT i.indnullsnotdistinct \
                       AND i.indnatts = 1 AND i.indnkeyatts = 1 \
                       AND i.indkey[0] = 1 AND i.indcollation[0] = 0 \
                       AND i.indoption[0] = 0 \
                       AND i.indexprs IS NULL AND i.indpred IS NULL \
                       AND ic.relname = 'writable_generation_pkey' \
                       AND ic.relnamespace = n.oid AND ic.relowner = c.relowner \
                       AND ic.relkind = 'i' AND ic.relpersistence = 'p' \
                       AND ic.reltablespace = 0 AND ic.relacl IS NULL \
                       AND ic.reloptions IS NULL \
                       AND ic.relam = (SELECT oid FROM pg_catalog.pg_am \
                                      WHERE amname = 'btree') \
                       AND i.indclass[0] = ( \
                           SELECT o.oid FROM pg_catalog.pg_opclass AS o \
                           JOIN pg_catalog.pg_am AS a ON a.oid = o.opcmethod \
                           JOIN pg_catalog.pg_namespace AS x ON x.oid = o.opcnamespace \
                           WHERE o.opcname = 'bool_ops' AND o.opcdefault \
                             AND o.opcintype = pg_catalog.to_regtype('pg_catalog.bool') \
                             AND a.amname = 'btree' AND x.nspname = 'pg_catalog')) \
            FROM pg_catalog.pg_index AS i \
            JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid \
            WHERE i.indrelid = c.oid) \
    FROM pg_catalog.pg_namespace AS n \
    JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid \
    WHERE n.nspname = 'pgshard_internal' AND c.relname = 'writable_generation'";
const LOCK_GENERATION_TABLE: &str = "\
    LOCK TABLE pgshard_internal.writable_generation IN SHARE ROW EXCLUSIVE MODE";
const LOCK_DEFAULT_ACL_CATALOG: &str = "\
    LOCK TABLE pg_catalog.pg_default_acl IN SHARE ROW EXCLUSIVE MODE";
const LOCK_GENERATION_CATALOG_ROWS: &str = "\
    SELECT n.oid, c.oid, i.indexrelid, ic.oid \
    FROM pg_catalog.pg_namespace AS n \
    JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid \
    JOIN pg_catalog.pg_index AS i ON i.indrelid = c.oid \
    JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid \
    WHERE n.nspname = 'pgshard_internal' AND c.relname = 'writable_generation' \
    FOR UPDATE OF n, c, i, ic";
const SELECT_FOR_UPDATE: &str = "\
    SELECT singleton, generation FROM pgshard_internal.writable_generation FOR UPDATE";
const LOCK_GENERATION_TABLE_ACCESS_SHARE: &str = "\
    LOCK TABLE ONLY pgshard_internal.writable_generation IN ACCESS SHARE MODE";
const SELECT_GENERATION: &str = "\
    SELECT singleton, generation FROM pgshard_internal.writable_generation";
const INSERT_GENERATION: &str = "\
    INSERT INTO pgshard_internal.writable_generation (singleton, generation) \
    VALUES (true, $1)";
const UPDATE_GENERATION: &str = "\
    UPDATE pgshard_internal.writable_generation SET generation = $1 \
    WHERE singleton = true";
const CURRENT_FLUSH_LSN: &str = "SELECT pg_catalog.pg_current_wal_flush_lsn()::text";
const SYNCHRONOUS_STANDBY_OBSERVATION: &str = "\
    SELECT r.application_name, r.state, r.sync_state, \
           r.flush_lsn >= $2::text::pg_catalog.pg_lsn, \
           r.replay_lsn >= $2::text::pg_catalog.pg_lsn \
    FROM pg_catalog.pg_stat_replication AS r \
    JOIN pg_catalog.pg_replication_slots AS s ON s.active_pid = r.pid \
    WHERE r.application_name = ANY($1::text[]) \
      AND s.slot_name = r.application_name \
      AND s.slot_type = 'physical' AND NOT s.temporary AND s.active \
    ORDER BY r.application_name, r.pid";
const SOURCE_IDENTITY_OBSERVATION: &str = "\
    SELECT s.system_identifier::text, c.timeline_id::text, \
           pg_catalog.pg_is_in_recovery(), \
           pg_catalog.current_setting('synchronous_standby_names') \
    FROM pg_catalog.pg_control_system() AS s \
    CROSS JOIN pg_catalog.pg_control_checkpoint() AS c";
const SOURCE_WAL_OBSERVATION: &str = "\
    SELECT pg_catalog.pg_current_wal_flush_lsn()::text";
const SOURCE_CANDIDATE_OBSERVATION: &str = "\
    SELECT candidate.member_slot_name, s.slot_name IS NOT NULL, s.slot_type::text, \
           s.temporary, COALESCE(s.active, false), \
           COALESCE(s.active_pid = r.pid, false), r.state::text, r.sync_state::text, \
           r.flush_lsn::text, r.replay_lsn::text, \
           COALESCE(r.flush_lsn >= $2::text::pg_catalog.pg_lsn, false), \
           COALESCE(r.replay_lsn >= $2::text::pg_catalog.pg_lsn, false) \
    FROM unnest($1::text[]) WITH ORDINALITY \
         AS candidate(member_slot_name, candidate_ordinal) \
    LEFT JOIN pg_catalog.pg_replication_slots AS s \
      ON s.slot_name = candidate.member_slot_name \
    LEFT JOIN pg_catalog.pg_stat_replication AS r \
      ON r.application_name = candidate.member_slot_name \
    ORDER BY candidate.candidate_ordinal, r.pid";
const STANDBY_IDENTITY_OBSERVATION: &str = "\
    SELECT s.system_identifier::text, c.timeline_id::text, \
           pg_catalog.pg_is_in_recovery(), \
           pg_catalog.current_setting('primary_slot_name') \
    FROM pg_catalog.pg_control_system() AS s \
    CROSS JOIN pg_catalog.pg_control_checkpoint() AS c";
const STANDBY_WAL_OBSERVATION: &str = "\
    SELECT pg_catalog.pg_last_wal_receive_lsn()::text, \
           pg_catalog.pg_last_wal_replay_lsn()::text";
const EVIDENCE_TRANSACTION_SETTINGS: &str = "\
    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;\
    SET LOCAL search_path = pg_catalog;\
    SET LOCAL statement_timeout = '2s';\
    SET LOCAL transaction_timeout = '5s';\
    SET LOCAL idle_in_transaction_session_timeout = '2s';";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GenerationDurability {
    Local,
    RemoteApplyAnyOne { application_names: Vec<String> },
}

impl GenerationDurability {
    pub(crate) fn remote_apply_any_one(
        application_names: Vec<String>,
    ) -> Result<Self, PostgresGenerationError> {
        let durability = Self::RemoteApplyAnyOne { application_names };
        validate_generation_durability(&durability)?;
        Ok(durability)
    }

    const fn transaction_setting(&self) -> &'static str {
        match self {
            Self::Local => LOCAL_COMMIT,
            Self::RemoteApplyAnyOne { .. } => REMOTE_APPLY_COMMIT,
        }
    }

    pub(crate) const fn is_remote_apply(&self) -> bool {
        matches!(self, Self::RemoteApplyAnyOne { .. })
    }

    pub(crate) fn synchronous_standby_names_setting(&self) -> String {
        match self {
            Self::Local => String::new(),
            Self::RemoteApplyAnyOne { application_names } => {
                format!("ANY 1 ({})", application_names.join(", "))
            }
        }
    }

    pub(crate) fn evidence(&self) -> GenerationDurabilityEvidence {
        match self {
            Self::Local => GenerationDurabilityEvidence::Local,
            Self::RemoteApplyAnyOne { application_names } => {
                GenerationDurabilityEvidence::RemoteApplyAnyOne {
                    candidates: application_names.clone(),
                }
            }
        }
    }

    fn application_names(&self) -> &[String] {
        match self {
            Self::Local => &[],
            Self::RemoteApplyAnyOne { application_names } => application_names,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SynchronousStandbyObservation {
    application_name: String,
    state: String,
    sync_state: String,
    flush_covers_barrier: Option<bool>,
    replay_covers_barrier: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SynchronousStandbyProof {
    Pending,
    Replayed,
}

#[cfg(test)]
struct StablePublicationGate {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[cfg(test)]
static TEST_STABLE_PUBLICATION_GATE: std::sync::Mutex<Option<StablePublicationGate>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
static TEST_EVIDENCE_ACCESS_SHARE_GATE: std::sync::Mutex<Option<StablePublicationGate>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
static TEST_EVIDENCE_SNAPSHOT_GATE: std::sync::Mutex<Option<StablePublicationGate>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
static TEST_EVIDENCE_WAL_CAPTURE_GATE: std::sync::Mutex<Option<StablePublicationGate>> =
    std::sync::Mutex::new(None);

/// One retained peer-authenticated `PostgreSQL` session used for bounded
/// replication-evidence samples. Keeping this session alive prevents the
/// 250 ms monitor cadence from creating a fresh backend on every sample.
pub(crate) struct ReplicationEvidenceSession {
    connection: ConnectedPostgres,
}

/// Establishes the one session a replication-evidence monitor retains until
/// it terminates or loses its first confirmed stream.
pub(crate) async fn connect_replication_evidence(
    socket_dir: &Path,
) -> Result<ReplicationEvidenceSession, PostgresGenerationError> {
    Ok(ReplicationEvidenceSession {
        connection: connect_with_application(socket_dir, "pgshard-replication-evidence").await?,
    })
}

impl ReplicationEvidenceSession {
    /// Samples one coherent source generation barrier and every configured
    /// member slot on this retained session.
    pub(crate) async fn observe_source(
        &mut self,
        expected_generation: &DurableWritableGeneration,
        durability: &GenerationDurability,
    ) -> Result<SourceReplicationEvidence, PostgresGenerationError> {
        validate_generation_durability(durability)?;
        let observed_at_unix_ms = unix_time_ms();
        // PostgreSQL's WAL-position functions read live shared state rather
        // than MVCC state. Complete a separately bounded transaction before
        // beginning the repeatable-read generation transaction, so its fixed
        // snapshot cannot precede this WAL barrier and pair an old row with
        // newer durability.
        let wal_transaction = self.connection.client.transaction().await?;
        wal_transaction
            .batch_execute(EVIDENCE_TRANSACTION_SETTINGS)
            .await?;
        let wal_row = wal_transaction
            .query_one(SOURCE_WAL_OBSERVATION, &[])
            .await?;
        let barrier_text = wal_row.try_get::<_, String>(0)?;
        let generation_barrier_lsn = parse_pg_lsn(&barrier_text)?;
        if generation_barrier_lsn.0 == 0 {
            return Err(PostgresGenerationError::InvalidReplicationEvidence);
        }
        wal_transaction.rollback().await?;
        #[cfg(test)]
        evidence_wal_capture_checkpoint().await;

        let transaction = self.connection.client.transaction().await?;
        transaction
            .batch_execute(EVIDENCE_TRANSACTION_SETTINGS)
            .await?;
        transaction
            .batch_execute(LOCK_GENERATION_TABLE_ACCESS_SHARE)
            .await?;
        #[cfg(test)]
        evidence_access_share_checkpoint().await;
        let generation = read_generation_evidence(&transaction).await?;
        if &generation != expected_generation {
            return Err(PostgresGenerationError::GenerationEvidenceChanged);
        }

        let row = transaction
            .query_one(SOURCE_IDENTITY_OBSERVATION, &[])
            .await?;
        let system_identifier = parse_system_identifier(&row.try_get::<_, String>(0)?)?;
        let timeline = parse_timeline(&row.try_get::<_, String>(1)?)?;
        let in_recovery = row.try_get::<_, bool>(2)?;
        let runtime_synchronous_standby_names = row.try_get::<_, String>(3)?;
        if in_recovery
            || runtime_synchronous_standby_names != durability.synchronous_standby_names_setting()
        {
            return Err(PostgresGenerationError::InvalidReplicationEvidence);
        }

        let candidate_rows = transaction
            .query(
                SOURCE_CANDIDATE_OBSERVATION,
                &[&durability.application_names(), &barrier_text],
            )
            .await?;
        let candidates = parse_source_candidate_evidence(
            durability.application_names(),
            &candidate_rows,
            generation_barrier_lsn,
        )?;
        transaction.rollback().await?;

        Ok(SourceReplicationEvidence {
            observed_at_unix_ms,
            system_identifier,
            timeline,
            in_recovery,
            generation_identity: canonical_generation_text(&generation),
            generation_barrier_lsn,
            durability: durability.evidence(),
            candidates,
        })
    }

    /// Samples one coherent standby identity, replay position, and exact
    /// replayed generation on this retained session.
    pub(crate) async fn observe_standby(
        &mut self,
        expected_member_slot_name: &str,
    ) -> Result<StandbyReplicationEvidence, PostgresGenerationError> {
        if !is_canonical_managed_member_name(expected_member_slot_name) {
            return Err(PostgresGenerationError::InvalidReplicationEvidence);
        }
        let observed_at_unix_ms = unix_time_ms();
        // Capture both live receiver/replay positions in a separately bounded
        // transaction that ends before the fixed generation snapshot begins.
        // A concurrent replay may therefore make these values conservative,
        // but they are never sampled after the generation snapshot.
        let wal_transaction = self.connection.client.transaction().await?;
        wal_transaction
            .batch_execute(EVIDENCE_TRANSACTION_SETTINGS)
            .await?;
        let wal_row = wal_transaction
            .query_one(STANDBY_WAL_OBSERVATION, &[])
            .await?;
        let receive_lsn = parse_pg_lsn(&wal_row.try_get::<_, String>(0)?)?;
        let replay_lsn = parse_pg_lsn(&wal_row.try_get::<_, String>(1)?)?;
        if receive_lsn.0 == 0 || replay_lsn.0 == 0 {
            return Err(PostgresGenerationError::InvalidReplicationEvidence);
        }
        wal_transaction.rollback().await?;
        #[cfg(test)]
        evidence_wal_capture_checkpoint().await;

        let transaction = self.connection.client.transaction().await?;
        transaction
            .batch_execute(EVIDENCE_TRANSACTION_SETTINGS)
            .await?;
        transaction
            .batch_execute(LOCK_GENERATION_TABLE_ACCESS_SHARE)
            .await?;
        #[cfg(test)]
        evidence_access_share_checkpoint().await;
        let generation = read_generation_evidence(&transaction).await?;
        let row = transaction
            .query_one(STANDBY_IDENTITY_OBSERVATION, &[])
            .await?;
        let system_identifier = parse_system_identifier(&row.try_get::<_, String>(0)?)?;
        let timeline = parse_timeline(&row.try_get::<_, String>(1)?)?;
        let in_recovery = row.try_get::<_, bool>(2)?;
        let member_slot_name = row.try_get::<_, String>(3)?;
        if !in_recovery || member_slot_name != expected_member_slot_name {
            return Err(PostgresGenerationError::InvalidReplicationEvidence);
        }
        transaction.rollback().await?;

        Ok(StandbyReplicationEvidence {
            observed_at_unix_ms,
            system_identifier,
            timeline,
            in_recovery,
            generation_identity: canonical_generation_text(&generation),
            member_slot_name,
            receive_lsn,
            replay_lsn,
        })
    }
}

async fn read_generation_evidence(
    transaction: &tokio_postgres::Transaction<'_>,
) -> Result<DurableWritableGeneration, PostgresGenerationError> {
    if query_safety(transaction, SCHEMA_IS_SAFE).await? != Some(true) {
        return Err(PostgresGenerationError::UnsafeSchema);
    }
    if query_safety(transaction, RELATION_IS_SAFE).await? != Some(true) {
        return Err(PostgresGenerationError::UnsafeRelation);
    }
    #[cfg(test)]
    evidence_snapshot_checkpoint().await;
    // The explicit AccessShareLock already held through transaction end keeps
    // target shape/name DDL out. Plain SELECT does not strengthen that lock or
    // acquire a write-capable catalog/tuple lock.
    let rows = transaction.query(SELECT_GENERATION, &[]).await?;
    if query_safety(transaction, SCHEMA_IS_SAFE).await? != Some(true) {
        return Err(PostgresGenerationError::UnsafeSchema);
    }
    if query_safety(transaction, RELATION_IS_SAFE).await? != Some(true) {
        return Err(PostgresGenerationError::UnsafeRelation);
    }
    parse_locked_generation_rows(&rows)?.ok_or(PostgresGenerationError::GenerationEvidenceMissing)
}

fn parse_source_candidate_evidence(
    expected_candidates: &[String],
    rows: &[tokio_postgres::Row],
    generation_barrier_lsn: PgLsn,
) -> Result<Vec<SourceReplicationCandidateEvidence>, PostgresGenerationError> {
    if rows.len() != expected_candidates.len() {
        return Err(PostgresGenerationError::InvalidReplicationEvidence);
    }
    rows.iter()
        .zip(expected_candidates)
        .map(|(row, expected)| {
            let member_slot_name = row.try_get::<_, String>(0)?;
            let slot_exists = row.try_get::<_, bool>(1)?;
            let slot_type = row.try_get::<_, Option<String>>(2)?;
            let slot_temporary = row.try_get::<_, Option<bool>>(3)?;
            let slot_active = row.try_get::<_, bool>(4)?;
            let slot_walsender_match = row.try_get::<_, bool>(5)?;
            let stream_state = row
                .try_get::<_, Option<String>>(6)?
                .map(|value| parse_replication_stream_state(&value))
                .transpose()?;
            let sync_state = row
                .try_get::<_, Option<String>>(7)?
                .map(|value| parse_replication_sync_state(&value))
                .transpose()?;
            let flush_lsn = row
                .try_get::<_, Option<String>>(8)?
                .map(|value| parse_pg_lsn(&value))
                .transpose()?;
            let replay_lsn = row
                .try_get::<_, Option<String>>(9)?
                .map(|value| parse_pg_lsn(&value))
                .transpose()?;
            let flush_covers_generation_barrier = row.try_get::<_, bool>(10)?;
            let replay_covers_generation_barrier = row.try_get::<_, bool>(11)?;
            if member_slot_name != *expected
                || (slot_exists
                    && (slot_type.as_deref() != Some("physical") || slot_temporary != Some(false)))
                || (!slot_exists
                    && (slot_type.is_some()
                        || slot_temporary.is_some()
                        || slot_active
                        || slot_walsender_match))
                || stream_state.is_some() != sync_state.is_some()
                || flush_covers_generation_barrier
                    != flush_lsn.is_some_and(|lsn| lsn.0 >= generation_barrier_lsn.0)
                || replay_covers_generation_barrier
                    != replay_lsn.is_some_and(|lsn| lsn.0 >= generation_barrier_lsn.0)
            {
                return Err(PostgresGenerationError::InvalidReplicationEvidence);
            }
            Ok(SourceReplicationCandidateEvidence {
                member_slot_name,
                slot_active,
                slot_walsender_match,
                stream_state,
                sync_state,
                flush_lsn,
                replay_lsn,
            })
        })
        .collect()
}

fn parse_replication_stream_state(
    value: &str,
) -> Result<ReplicationStreamState, PostgresGenerationError> {
    match value {
        "startup" => Ok(ReplicationStreamState::Startup),
        "catchup" => Ok(ReplicationStreamState::Catchup),
        "streaming" => Ok(ReplicationStreamState::Streaming),
        "backup" => Ok(ReplicationStreamState::Backup),
        "stopping" => Ok(ReplicationStreamState::Stopping),
        _ => Err(PostgresGenerationError::InvalidReplicationEvidence),
    }
}

fn parse_replication_sync_state(
    value: &str,
) -> Result<ReplicationSyncState, PostgresGenerationError> {
    match value {
        "async" => Ok(ReplicationSyncState::Async),
        "potential" => Ok(ReplicationSyncState::Potential),
        "sync" => Ok(ReplicationSyncState::Sync),
        "quorum" => Ok(ReplicationSyncState::Quorum),
        _ => Err(PostgresGenerationError::InvalidReplicationEvidence),
    }
}

fn parse_system_identifier(value: &str) -> Result<u64, PostgresGenerationError> {
    parse_canonical_decimal(value)
        .filter(|value| *value > 0)
        .ok_or(PostgresGenerationError::InvalidReplicationEvidence)
}

fn parse_timeline(value: &str) -> Result<u32, PostgresGenerationError> {
    parse_canonical_decimal(value)
        .filter(|value| *value > 0)
        .ok_or(PostgresGenerationError::InvalidReplicationEvidence)
}

fn parse_canonical_decimal<T>(value: &str) -> Option<T>
where
    T: std::str::FromStr + ToString,
{
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let parsed = value.parse::<T>().ok()?;
    (parsed.to_string() == value).then_some(parsed)
}

fn parse_pg_lsn(value: &str) -> Result<PgLsn, PostgresGenerationError> {
    let (high, low) = value
        .split_once('/')
        .ok_or(PostgresGenerationError::InvalidReplicationEvidence)?;
    if high.is_empty()
        || low.is_empty()
        || high.len() > 8
        || low.len() > 8
        || !high.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !low.bytes().all(|byte| byte.is_ascii_hexdigit())
        || high.bytes().any(|byte| byte.is_ascii_lowercase())
        || low.bytes().any(|byte| byte.is_ascii_lowercase())
        || (high.len() > 1 && high.starts_with('0'))
        || (low.len() > 1 && low.starts_with('0'))
    {
        return Err(PostgresGenerationError::InvalidReplicationEvidence);
    }
    let high = u32::from_str_radix(high, 16)
        .map_err(|_| PostgresGenerationError::InvalidReplicationEvidence)?;
    let low = u32::from_str_radix(low, 16)
        .map_err(|_| PostgresGenerationError::InvalidReplicationEvidence)?;
    Ok(PgLsn((u64::from(high) << 32) | u64::from(low)))
}

fn canonical_generation_text(generation: &DurableWritableGeneration) -> String {
    String::from_utf8(generation.canonical_bytes())
        .expect("canonical writable generation is always UTF-8")
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Publishes one generation to a singleton WAL-logged row in the `postgres`
/// database over the private Unix socket.
///
/// `authority_exact` must consult the attempt-private authority channel. It is
/// checked before every connection attempt and immediately before `COMMIT`.
/// The caller supplies the outer shutdown, child-exit, and timeout race.
#[cfg(test)]
pub(crate) async fn publish_writable_generation<F>(
    socket_dir: &Path,
    requested: &DurableWritableGeneration,
    authority_exact: &F,
) -> Result<(), PostgresGenerationError>
where
    F: Fn() -> bool,
{
    publish_writable_generation_with_durability(
        socket_dir,
        requested,
        &GenerationDurability::Local,
        authority_exact,
    )
    .await
}

pub(crate) async fn publish_writable_generation_with_durability<F>(
    socket_dir: &Path,
    requested: &DurableWritableGeneration,
    durability: &GenerationDurability,
    authority_exact: &F,
) -> Result<(), PostgresGenerationError>
where
    F: Fn() -> bool,
{
    validate_generation_durability(durability)?;
    loop {
        if !authority_exact() {
            return Err(PostgresGenerationError::AuthorityChanged);
        }
        let mut connection = match connect(socket_dir).await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::debug!(reason = %error, "waiting for quarantined PostgreSQL socket");
                sleep(CONNECT_RETRY_DELAY).await;
                continue;
            }
        };
        let transaction = connection.client.transaction().await?;
        transaction.batch_execute(TRANSACTION_SETTINGS).await?;
        transaction
            .batch_execute(durability.transaction_setting())
            .await?;
        match query_safety(&transaction, SCHEMA_IS_SAFE).await? {
            Some(true) => {}
            Some(false) => return Err(PostgresGenerationError::UnsafeSchema),
            None => {
                transaction.batch_execute(CREATE_SCHEMA).await?;
                if query_safety(&transaction, SCHEMA_IS_SAFE).await? != Some(true) {
                    return Err(PostgresGenerationError::UnsafeSchema);
                }
            }
        }
        match query_safety(&transaction, RELATION_IS_SAFE).await? {
            Some(true) => {}
            Some(false) => return Err(PostgresGenerationError::UnsafeRelation),
            None => {
                transaction.batch_execute(CREATE_TABLE).await?;
                if query_safety(&transaction, RELATION_IS_SAFE).await? != Some(true) {
                    return Err(PostgresGenerationError::UnsafeRelation);
                }
            }
        }
        lock_and_validate_generation_relation(&transaction).await?;
        #[cfg(test)]
        stable_publication_checkpoint().await;
        let rows = transaction.query(SELECT_FOR_UPDATE, &[]).await?;
        let existing = parse_locked_generation_rows(&rows)?;
        let transition = classify_writable_generation_transition(existing.as_ref(), requested)?;
        let bytes = requested.canonical_bytes();
        match transition {
            WritableGenerationTransition::Initialize => {
                transaction.execute(INSERT_GENERATION, &[&bytes]).await?;
            }
            WritableGenerationTransition::Advance => {
                let updated = transaction.execute(UPDATE_GENERATION, &[&bytes]).await?;
                if updated != 1 {
                    return Err(PostgresGenerationError::SingletonChanged);
                }
            }
            WritableGenerationTransition::Replay => {}
        }

        prepare_remote_apply_commit(&transaction, durability).await?;
        // No await or state-changing operation may be inserted between this
        // exact authority observation and dispatching COMMIT.
        if !authority_exact() {
            return Err(PostgresGenerationError::AuthorityChanged);
        }
        match transaction.commit().await {
            Ok(()) => {
                return prove_publication_durability(
                    socket_dir,
                    requested,
                    durability,
                    authority_exact,
                )
                .await;
            }
            Err(commit_error) => {
                tracing::warn!(reason = %commit_error, "reconciling ambiguous writable-generation commit");
                match reconcile_ambiguous_commit(
                    socket_dir,
                    existing.as_ref(),
                    requested,
                    authority_exact,
                )
                .await?
                {
                    AmbiguousCommitOutcome::Committed => {
                        return prove_publication_durability(
                            socket_dir,
                            requested,
                            durability,
                            authority_exact,
                        )
                        .await;
                    }
                    AmbiguousCommitOutcome::Retry => {
                        sleep(CONNECT_RETRY_DELAY).await;
                    }
                }
            }
        }
    }
}

async fn prepare_remote_apply_commit(
    transaction: &tokio_postgres::Transaction<'_>,
    durability: &GenerationDurability,
) -> Result<(), tokio_postgres::Error> {
    if durability.is_remote_apply() {
        // Remote apply may legitimately wait while every managed standby is
        // cloning or unavailable. The supervisor still races this future
        // against authority loss, shutdown, and postmaster exit.
        transaction
            .batch_execute(DISABLE_REMOTE_COMMIT_TIMEOUTS)
            .await?;
    }
    Ok(())
}

pub(crate) fn validate_generation_durability(
    durability: &GenerationDurability,
) -> Result<(), PostgresGenerationError> {
    let GenerationDurability::RemoteApplyAnyOne { application_names } = durability else {
        return Ok(());
    };
    if !matches!(application_names.len(), 2 | 4) {
        return Err(PostgresGenerationError::InvalidSynchronousStandbySet);
    }
    for (index, application_name) in application_names.iter().enumerate() {
        let expected = format!("pgshard_member_{:04}", index + 1);
        if application_name != &expected || !is_canonical_managed_member_name(application_name) {
            return Err(PostgresGenerationError::InvalidSynchronousStandbySet);
        }
    }
    Ok(())
}

pub(crate) fn is_canonical_managed_member_name(name: &str) -> bool {
    let Some(ordinal) = name
        .strip_prefix("pgshard_member_")
        .and_then(|value| value.parse::<u16>().ok())
    else {
        return false;
    };
    format!("pgshard_member_{ordinal:04}") == name
}

async fn prove_publication_durability<F>(
    socket_dir: &Path,
    requested: &DurableWritableGeneration,
    durability: &GenerationDurability,
    authority_exact: &F,
) -> Result<(), PostgresGenerationError>
where
    F: Fn() -> bool,
{
    let GenerationDurability::RemoteApplyAnyOne { application_names } = durability else {
        return Ok(());
    };
    if !authority_exact() {
        return Err(PostgresGenerationError::AuthorityChanged);
    }
    let mut connection = connect(socket_dir).await?;
    let barrier = connection
        .client
        .query_one(CURRENT_FLUSH_LSN, &[])
        .await?
        .try_get::<_, String>(0)?;
    loop {
        if !authority_exact() {
            return Err(PostgresGenerationError::AuthorityChanged);
        }
        let rows = connection
            .client
            .query(
                SYNCHRONOUS_STANDBY_OBSERVATION,
                &[application_names, &barrier],
            )
            .await?;
        let observations = rows
            .iter()
            .map(parse_synchronous_standby_observation)
            .collect::<Result<Vec<_>, _>>()?;
        match classify_synchronous_standby_observations(application_names, &observations)? {
            SynchronousStandbyProof::Pending => sleep(CONNECT_RETRY_DELAY).await,
            SynchronousStandbyProof::Replayed => break,
        }
    }

    if !authority_exact() {
        return Err(PostgresGenerationError::AuthorityChanged);
    }
    let observed = read_current(&mut connection.client).await?;
    verify_synchronous_publication(observed.as_ref(), requested, authority_exact())
}

fn parse_synchronous_standby_observation(
    row: &tokio_postgres::Row,
) -> Result<SynchronousStandbyObservation, tokio_postgres::Error> {
    Ok(SynchronousStandbyObservation {
        application_name: row.try_get(0)?,
        state: row.try_get(1)?,
        sync_state: row.try_get(2)?,
        flush_covers_barrier: row.try_get(3)?,
        replay_covers_barrier: row.try_get(4)?,
    })
}

fn classify_synchronous_standby_observations(
    candidates: &[String],
    observations: &[SynchronousStandbyObservation],
) -> Result<SynchronousStandbyProof, PostgresGenerationError> {
    let candidate_set: BTreeSet<_> = candidates.iter().map(String::as_str).collect();
    let mut identity_counts = BTreeMap::new();
    for observation in observations {
        *identity_counts
            .entry(observation.application_name.as_str())
            .or_insert(0_usize) += 1;
    }
    if let Some((application_name, matches)) = identity_counts
        .into_iter()
        .find(|(_, matches)| *matches > 1)
    {
        return Err(PostgresGenerationError::AmbiguousSynchronousStandby {
            application_name: application_name.to_owned(),
            matches,
        });
    }
    Ok(
        if observations.iter().any(|observation| {
            candidate_set.contains(observation.application_name.as_str())
                && observation.state == "streaming"
                && matches!(observation.sync_state.as_str(), "sync" | "quorum")
                && observation.flush_covers_barrier == Some(true)
                && observation.replay_covers_barrier == Some(true)
        }) {
            SynchronousStandbyProof::Replayed
        } else {
            SynchronousStandbyProof::Pending
        },
    )
}

fn verify_synchronous_publication(
    observed: Option<&DurableWritableGeneration>,
    requested: &DurableWritableGeneration,
    authority_exact: bool,
) -> Result<(), PostgresGenerationError> {
    if !authority_exact {
        return Err(PostgresGenerationError::AuthorityChanged);
    }
    if observed != Some(requested) {
        return Err(PostgresGenerationError::SynchronousGenerationChanged);
    }
    Ok(())
}

async fn query_safety(
    transaction: &tokio_postgres::Transaction<'_>,
    query: &str,
) -> Result<Option<bool>, tokio_postgres::Error> {
    match transaction.query_opt(query, &[]).await? {
        Some(row) => Ok(Some(row.try_get::<_, Option<bool>>(0)?.unwrap_or(false))),
        None => Ok(None),
    }
}

async fn lock_and_validate_generation_relation(
    transaction: &tokio_postgres::Transaction<'_>,
) -> Result<(), PostgresGenerationError> {
    // Take the shared catalog lock first: ALTER DEFAULT PRIVILEGES takes
    // RowExclusive on pg_default_acl, while unrelated relation DDL does not
    // need to acquire this stronger catalog lock after locking our table. This
    // fixed order avoids introducing a table/catalog lock cycle. The bounded
    // transaction lock timeout remains the fail-closed deadlock backstop.
    transaction.batch_execute(LOCK_DEFAULT_ACL_CATALOG).await?;
    // The relation lock resolves the currently named object and excludes DDL
    // that changes its table/index shape. Catalog tuple locks additionally
    // serialize namespace and ACL/owner metadata updates that need not take a
    // conflicting relation lock. All locks remain held until commit/rollback.
    transaction.batch_execute(LOCK_GENERATION_TABLE).await?;
    let catalog_rows = transaction.query(LOCK_GENERATION_CATALOG_ROWS, &[]).await?;
    if catalog_rows.is_empty() {
        return Err(PostgresGenerationError::UnsafeRelation);
    }
    if query_safety(transaction, SCHEMA_IS_SAFE).await? != Some(true) {
        return Err(PostgresGenerationError::UnsafeSchema);
    }
    if query_safety(transaction, RELATION_IS_SAFE).await? != Some(true) {
        return Err(PostgresGenerationError::UnsafeRelation);
    }
    Ok(())
}

fn parse_locked_generation_rows(
    rows: &[tokio_postgres::Row],
) -> Result<Option<DurableWritableGeneration>, PostgresGenerationError> {
    match rows {
        [] => Ok(None),
        [row] if row.try_get::<_, bool>(0)? => {
            let bytes = row.try_get::<_, Vec<u8>>(1)?;
            Ok(Some(parse_generation(&bytes)?))
        }
        _ => Err(PostgresGenerationError::SingletonChanged),
    }
}

#[cfg(test)]
fn gate_next_stable_publication() -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let mut gate = TEST_STABLE_PUBLICATION_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        gate.replace(StablePublicationGate {
            entered: entered_tx,
            release: release_rx,
        })
        .is_none(),
        "test already has a stable-publication gate"
    );
    (entered_rx, release_tx)
}

#[cfg(test)]
fn gate_next_evidence_access_share() -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let mut gate = TEST_EVIDENCE_ACCESS_SHARE_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        gate.replace(StablePublicationGate {
            entered: entered_tx,
            release: release_rx,
        })
        .is_none(),
        "test already has an evidence AccessShare gate"
    );
    (entered_rx, release_tx)
}

#[cfg(test)]
fn gate_next_evidence_snapshot() -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let mut gate = TEST_EVIDENCE_SNAPSHOT_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        gate.replace(StablePublicationGate {
            entered: entered_tx,
            release: release_rx,
        })
        .is_none(),
        "test already has an evidence snapshot gate"
    );
    (entered_rx, release_tx)
}

#[cfg(test)]
fn gate_next_evidence_wal_capture() -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let mut gate = TEST_EVIDENCE_WAL_CAPTURE_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        gate.replace(StablePublicationGate {
            entered: entered_tx,
            release: release_rx,
        })
        .is_none(),
        "test already has an evidence WAL-capture gate"
    );
    (entered_rx, release_tx)
}

#[cfg(test)]
async fn stable_publication_checkpoint() {
    let gate = TEST_STABLE_PUBLICATION_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(gate) = gate {
        let _ = gate.entered.send(());
        let _ = gate.release.await;
    }
}

#[cfg(test)]
async fn evidence_access_share_checkpoint() {
    let gate = TEST_EVIDENCE_ACCESS_SHARE_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(gate) = gate {
        let _ = gate.entered.send(());
        let _ = gate.release.await;
    }
}

#[cfg(test)]
async fn evidence_snapshot_checkpoint() {
    let gate = TEST_EVIDENCE_SNAPSHOT_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(gate) = gate {
        let _ = gate.entered.send(());
        let _ = gate.release.await;
    }
}

#[cfg(test)]
async fn evidence_wal_capture_checkpoint() {
    let gate = TEST_EVIDENCE_WAL_CAPTURE_GATE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(gate) = gate {
        let _ = gate.entered.send(());
        let _ = gate.release.await;
    }
}

async fn reconcile_ambiguous_commit<F>(
    socket_dir: &Path,
    previous: Option<&DurableWritableGeneration>,
    requested: &DurableWritableGeneration,
    authority_exact: &F,
) -> Result<AmbiguousCommitOutcome, PostgresGenerationError>
where
    F: Fn() -> bool,
{
    loop {
        if !authority_exact() {
            return Err(PostgresGenerationError::AuthorityChanged);
        }
        let mut connection = match connect(socket_dir).await {
            Ok(connection) => connection,
            Err(error) => {
                tracing::debug!(reason = %error, "waiting to reconcile writable-generation commit");
                if !authority_exact() {
                    return Err(PostgresGenerationError::AuthorityChanged);
                }
                sleep(CONNECT_RETRY_DELAY).await;
                continue;
            }
        };
        let observed = read_current(&mut connection.client).await?;
        return classify_ambiguous_commit(
            previous,
            observed.as_ref(),
            requested,
            authority_exact(),
        );
    }
}

async fn read_current(
    client: &mut Client,
) -> Result<Option<DurableWritableGeneration>, PostgresGenerationError> {
    let transaction = client.transaction().await?;
    transaction.batch_execute(TRANSACTION_SETTINGS).await?;
    match query_safety(&transaction, SCHEMA_IS_SAFE).await? {
        None => return Ok(None),
        Some(false) => return Err(PostgresGenerationError::UnsafeSchema),
        Some(true) => {}
    }
    match query_safety(&transaction, RELATION_IS_SAFE).await? {
        None => return Ok(None),
        Some(false) => return Err(PostgresGenerationError::UnsafeRelation),
        Some(true) => {}
    }
    lock_and_validate_generation_relation(&transaction).await?;
    let rows = transaction.query(SELECT_FOR_UPDATE, &[]).await?;
    parse_locked_generation_rows(&rows)
}

fn classify_ambiguous_commit(
    previous: Option<&DurableWritableGeneration>,
    observed: Option<&DurableWritableGeneration>,
    requested: &DurableWritableGeneration,
    authority_exact: bool,
) -> Result<AmbiguousCommitOutcome, PostgresGenerationError> {
    if observed == Some(requested) {
        return Ok(AmbiguousCommitOutcome::Committed);
    }
    if observed == previous {
        return if authority_exact {
            Ok(AmbiguousCommitOutcome::Retry)
        } else {
            Err(PostgresGenerationError::AuthorityChanged)
        };
    }
    match classify_writable_generation_transition(observed, requested) {
        Ok(WritableGenerationTransition::Initialize | WritableGenerationTransition::Advance) => {
            // A value other than the exact pre-commit snapshot is never safe
            // to overwrite during ambiguity, even if its term is lower.
            Err(PostgresGenerationError::AmbiguousGenerationChanged)
        }
        Ok(WritableGenerationTransition::Replay) => Ok(AmbiguousCommitOutcome::Committed),
        Err(error) => Err(error.into()),
    }
}

fn parse_generation(bytes: &[u8]) -> Result<DurableWritableGeneration, PostgresGenerationError> {
    DurableWritableGeneration::parse_canonical(bytes)
        .ok_or(PostgresGenerationError::MalformedGeneration)
}

async fn connect(socket_dir: &Path) -> Result<ConnectedPostgres, tokio_postgres::Error> {
    connect_with_application(socket_dir, "pgshard-generation-publisher").await
}

async fn connect_with_application(
    socket_dir: &Path,
    application_name: &'static str,
) -> Result<ConnectedPostgres, tokio_postgres::Error> {
    let mut config = Config::new();
    config
        .host_path(socket_dir)
        .port(5432)
        .user("postgres")
        .dbname("postgres")
        .application_name(application_name)
        .options(CONNECTION_OPTIONS);
    let (client, connection) = config.connect(NoTls).await?;
    let driver = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(reason = %error, "PostgreSQL generation connection ended");
        }
    });
    Ok(ConnectedPostgres { client, driver })
}

struct ConnectedPostgres {
    client: Client,
    driver: JoinHandle<()>,
}

impl Drop for ConnectedPostgres {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AmbiguousCommitOutcome {
    Committed,
    Retry,
}

/// Failure to establish or reconcile the WAL-backed generation record.
#[derive(Debug, Error)]
pub enum PostgresGenerationError {
    /// Attempt-private authority no longer exactly matches the request.
    #[error("attempt-private writable authority changed during WAL publication")]
    AuthorityChanged,
    /// The durable row is not the canonical bounded generation encoding.
    #[error("PostgreSQL writable-generation row is malformed")]
    MalformedGeneration,
    /// The singleton row changed unexpectedly within a locked transaction.
    #[error("PostgreSQL writable-generation singleton changed unexpectedly")]
    SingletonChanged,
    /// The schema or table is not the expected owned WAL-logged relation.
    #[error("PostgreSQL writable-generation relation is not the expected owned WAL-logged table")]
    UnsafeRelation,
    /// The namespace is not privately owned with default privileges unchanged.
    #[error("PostgreSQL writable-generation schema has unsafe ownership or ACLs")]
    UnsafeSchema,
    /// Ambiguous reconciliation observed neither the old nor requested value.
    #[error("PostgreSQL writable-generation row changed during ambiguous commit recovery")]
    AmbiguousGenerationChanged,
    /// The remote-apply candidate set is not one supported complete topology.
    #[error(
        "PostgreSQL writable-generation synchronous candidates must be the exact sorted member 1..2 or 1..4 set"
    )]
    InvalidSynchronousStandbySet,
    /// More than one physical walsender claimed one managed standby identity.
    #[error(
        "PostgreSQL exposed {matches} walsenders for synchronous standby identity {application_name:?}"
    )]
    AmbiguousSynchronousStandby {
        /// Duplicated physical standby identity.
        application_name: String,
        /// Number of matching physical walsenders.
        matches: usize,
    },
    /// The primary generation changed before synchronous proof completed.
    #[error("PostgreSQL writable-generation row changed during synchronous durability proof")]
    SynchronousGenerationChanged,
    /// No singleton generation row is visible in the coherent SQL sample.
    #[error("PostgreSQL writable-generation evidence is missing")]
    GenerationEvidenceMissing,
    /// The observed source generation differs from exact startup authority.
    #[error("PostgreSQL writable-generation evidence changed after publication")]
    GenerationEvidenceChanged,
    /// A `PostgreSQL` identity, LSN, runtime setting, slot, or walsender value was
    /// absent, noncanonical, ambiguous, or internally inconsistent.
    #[error("PostgreSQL replication evidence is invalid or incoherent")]
    InvalidReplicationEvidence,
    /// The requested transition violates the durable fencing floor.
    #[error(transparent)]
    UnsafeTransition(#[from] WritableGenerationTransitionError),
    /// `PostgreSQL` rejected or lost a non-commit publication operation.
    #[error("PostgreSQL writable-generation publication failed: {0}")]
    Database(#[from] tokio_postgres::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct ReplicationSlotNameContract {
        member_physical_slots: Vec<MemberPhysicalSlotCase>,
    }

    #[derive(serde::Deserialize)]
    struct MemberPhysicalSlotCase {
        member_ordinal: u16,
        slot_name: String,
    }
    use pgshard_types::ShardId;

    fn generation(cluster: &str, holder: &str, term: u64) -> DurableWritableGeneration {
        DurableWritableGeneration::new(
            cluster.to_owned(),
            format!("{cluster}-uid"),
            ShardId(0),
            "database".to_owned(),
            format!("{cluster}-lease"),
            format!("{cluster}-lease-uid"),
            holder.to_owned(),
            term,
        )
        .expect("valid PostgreSQL generation fixture")
    }

    fn standby_observation(
        application_name: &str,
        state: &str,
        sync_state: &str,
        flush_covers_barrier: Option<bool>,
        replay_covers_barrier: Option<bool>,
    ) -> SynchronousStandbyObservation {
        SynchronousStandbyObservation {
            application_name: application_name.to_owned(),
            state: state.to_owned(),
            sync_state: sync_state.to_owned(),
            flush_covers_barrier,
            replay_covers_barrier,
        }
    }

    #[test]
    fn replication_evidence_scalar_parsers_accept_only_canonical_values() {
        assert_eq!(parse_system_identifier("1").expect("system ID"), 1);
        assert_eq!(
            parse_system_identifier(&u64::MAX.to_string()).expect("maximum system ID"),
            u64::MAX
        );
        assert_eq!(
            parse_timeline("4294967295").expect("maximum timeline"),
            u32::MAX
        );
        for invalid in [
            "",
            "0",
            "01",
            "+1",
            " 1",
            "1 ",
            "1.0",
            "18446744073709551616",
        ] {
            assert!(matches!(
                parse_system_identifier(invalid),
                Err(PostgresGenerationError::InvalidReplicationEvidence)
            ));
        }
        for invalid in ["", "0", "01", "4294967296"] {
            assert!(matches!(
                parse_timeline(invalid),
                Err(PostgresGenerationError::InvalidReplicationEvidence)
            ));
        }

        assert_eq!(parse_pg_lsn("0/0").expect("zero LSN"), PgLsn(0));
        assert_eq!(
            parse_pg_lsn("1/2").expect("small LSN"),
            PgLsn((1_u64 << 32) | 2)
        );
        assert_eq!(
            parse_pg_lsn("FFFFFFFF/FFFFFFFF").expect("maximum LSN"),
            PgLsn(u64::MAX)
        );
        for invalid in [
            "",
            "1",
            "1/",
            "/1",
            "1/2/3",
            "01/2",
            "1/02",
            "a/1",
            "A/b",
            "100000000/0",
            "0/100000000",
            "G/0",
            " 1/2",
        ] {
            assert!(matches!(
                parse_pg_lsn(invalid),
                Err(PostgresGenerationError::InvalidReplicationEvidence)
            ));
        }
    }

    #[test]
    fn replication_state_parsers_are_closed_over_postgres_values() {
        for (value, expected) in [
            ("startup", ReplicationStreamState::Startup),
            ("catchup", ReplicationStreamState::Catchup),
            ("streaming", ReplicationStreamState::Streaming),
            ("backup", ReplicationStreamState::Backup),
            ("stopping", ReplicationStreamState::Stopping),
        ] {
            assert_eq!(
                parse_replication_stream_state(value).expect("known stream state"),
                expected
            );
        }
        for invalid in ["", "STREAMING", "streaming ", "unknown"] {
            assert!(matches!(
                parse_replication_stream_state(invalid),
                Err(PostgresGenerationError::InvalidReplicationEvidence)
            ));
        }
        for (value, expected) in [
            ("async", ReplicationSyncState::Async),
            ("potential", ReplicationSyncState::Potential),
            ("sync", ReplicationSyncState::Sync),
            ("quorum", ReplicationSyncState::Quorum),
        ] {
            assert_eq!(
                parse_replication_sync_state(value).expect("known sync state"),
                expected
            );
        }
        for invalid in ["", "SYNC", "quorum ", "unknown"] {
            assert!(matches!(
                parse_replication_sync_state(invalid),
                Err(PostgresGenerationError::InvalidReplicationEvidence)
            ));
        }
    }

    #[test]
    fn synchronous_candidates_require_complete_sorted_managed_topology() {
        assert!(validate_generation_durability(&GenerationDurability::Local).is_ok());
        for names in [
            vec!["pgshard_member_0001", "pgshard_member_0002"],
            vec![
                "pgshard_member_0001",
                "pgshard_member_0002",
                "pgshard_member_0003",
                "pgshard_member_0004",
            ],
        ] {
            assert!(
                GenerationDurability::remote_apply_any_one(
                    names.into_iter().map(str::to_owned).collect()
                )
                .is_ok()
            );
        }
        for names in [
            vec![],
            vec!["pgshard_member_0001"],
            vec!["pgshard_member_0002", "pgshard_member_0001"],
            vec!["pgshard_member_0001", "pgshard_member_0001"],
            vec!["pgshard_member_0001", "pgshard_member_0003"],
            vec!["pgshard_member_0001", "walreceiver"],
        ] {
            assert!(matches!(
                GenerationDurability::remote_apply_any_one(
                    names.into_iter().map(str::to_owned).collect()
                ),
                Err(PostgresGenerationError::InvalidSynchronousStandbySet)
            ));
        }
        assert_eq!(
            GenerationDurability::remote_apply_any_one(vec![
                "pgshard_member_0001".to_owned(),
                "pgshard_member_0002".to_owned(),
            ])
            .expect("valid any-one topology")
            .synchronous_standby_names_setting(),
            "ANY 1 (pgshard_member_0001, pgshard_member_0002)"
        );
    }

    #[test]
    fn managed_member_names_match_shared_contract() {
        let contract: ReplicationSlotNameContract = serde_json::from_str(include_str!(
            "../../../contracts/replication-slot-names.json"
        ))
        .expect("valid shared replication-slot naming contract");
        assert!(!contract.member_physical_slots.is_empty());
        for case in contract.member_physical_slots {
            assert_eq!(
                format!("pgshard_member_{:04}", case.member_ordinal),
                case.slot_name
            );
            assert!(is_canonical_managed_member_name(&case.slot_name));
        }
    }

    #[test]
    fn synchronous_classifier_requires_one_streaming_selected_replayed_standby() {
        let candidates = vec![
            "pgshard_member_0001".to_owned(),
            "pgshard_member_0002".to_owned(),
        ];
        for sync_state in ["sync", "quorum"] {
            assert_eq!(
                classify_synchronous_standby_observations(
                    &candidates,
                    &[standby_observation(
                        "pgshard_member_0002",
                        "streaming",
                        sync_state,
                        Some(true),
                        Some(true),
                    )]
                )
                .expect("exact synchronous standby proves replay"),
                SynchronousStandbyProof::Replayed
            );
        }

        let adverse = [
            standby_observation(
                "pgshard_member_0001",
                "startup",
                "quorum",
                Some(true),
                Some(true),
            ),
            standby_observation(
                "pgshard_member_0001",
                "catchup",
                "quorum",
                Some(true),
                Some(true),
            ),
            standby_observation(
                "pgshard_member_0001",
                "streaming",
                "async",
                Some(true),
                Some(true),
            ),
            standby_observation(
                "pgshard_member_0001",
                "streaming",
                "potential",
                Some(true),
                Some(true),
            ),
            standby_observation(
                "pgshard_member_0001",
                "streaming",
                "quorum",
                None,
                Some(true),
            ),
            standby_observation(
                "pgshard_member_0001",
                "streaming",
                "quorum",
                Some(true),
                None,
            ),
            standby_observation(
                "pgshard_member_0001",
                "streaming",
                "quorum",
                Some(false),
                Some(true),
            ),
            standby_observation(
                "pgshard_member_0001",
                "streaming",
                "quorum",
                Some(true),
                Some(false),
            ),
            standby_observation(
                "pgshard_member_0003",
                "streaming",
                "quorum",
                Some(true),
                Some(true),
            ),
        ];
        for observation in adverse {
            assert_eq!(
                classify_synchronous_standby_observations(&candidates, &[observation])
                    .expect("transiently unsafe standby remains pending"),
                SynchronousStandbyProof::Pending
            );
        }
        assert_eq!(
            classify_synchronous_standby_observations(&candidates, &[])
                .expect("missing exact standby remains pending"),
            SynchronousStandbyProof::Pending
        );
    }

    #[test]
    fn synchronous_classifier_rejects_duplicate_identity() {
        let candidates = vec![
            "pgshard_member_0001".to_owned(),
            "pgshard_member_0002".to_owned(),
        ];
        let observations = [
            standby_observation(
                "pgshard_member_0001",
                "streaming",
                "quorum",
                Some(true),
                Some(true),
            ),
            standby_observation(
                "pgshard_member_0001",
                "streaming",
                "quorum",
                Some(true),
                Some(true),
            ),
        ];
        assert!(matches!(
            classify_synchronous_standby_observations(&candidates, &observations),
            Err(PostgresGenerationError::AmbiguousSynchronousStandby { matches: 2, .. })
        ));
    }

    #[test]
    fn synchronous_final_recheck_requires_exact_row_and_authority() {
        let requested = generation("cluster-1", "holder-b", 2);
        assert!(verify_synchronous_publication(Some(&requested), &requested, true).is_ok());
        assert!(matches!(
            verify_synchronous_publication(Some(&requested), &requested, false),
            Err(PostgresGenerationError::AuthorityChanged)
        ));
        for observed in [
            None,
            Some(generation("cluster-1", "holder-a", 1)),
            Some(generation("cluster-1", "holder-c", 3)),
        ] {
            assert!(matches!(
                verify_synchronous_publication(observed.as_ref(), &requested, true),
                Err(PostgresGenerationError::SynchronousGenerationChanged)
            ));
        }
    }

    #[test]
    fn ambiguous_commit_accepts_only_requested_or_exact_old_retry() {
        let old = generation("cluster-1", "holder-a", 1);
        let requested = generation("cluster-1", "holder-b", 2);
        assert_eq!(
            classify_ambiguous_commit(Some(&old), Some(&requested), &requested, false)
                .expect("requested value proves commit"),
            AmbiguousCommitOutcome::Committed
        );
        assert_eq!(
            classify_ambiguous_commit(Some(&old), Some(&old), &requested, true)
                .expect("old value may retry under exact authority"),
            AmbiguousCommitOutcome::Retry
        );
        assert!(matches!(
            classify_ambiguous_commit(Some(&old), Some(&old), &requested, false),
            Err(PostgresGenerationError::AuthorityChanged)
        ));
    }

    #[test]
    fn ambiguous_commit_fences_unknown_higher_conflicting_and_foreign_rows() {
        let old = generation("cluster-1", "holder-a", 1);
        let requested = generation("cluster-1", "holder-b", 2);
        let lower_other = generation("cluster-1", "holder-z", 1);
        assert!(matches!(
            classify_ambiguous_commit(Some(&old), Some(&lower_other), &requested, true),
            Err(PostgresGenerationError::AmbiguousGenerationChanged)
        ));
        assert!(matches!(
            classify_ambiguous_commit(
                Some(&old),
                Some(&generation("cluster-1", "holder-z", 3)),
                &requested,
                true,
            ),
            Err(PostgresGenerationError::UnsafeTransition(
                WritableGenerationTransitionError::Regression { .. }
            ))
        ));
        assert!(matches!(
            classify_ambiguous_commit(
                Some(&old),
                Some(&generation("cluster-1", "holder-z", 2)),
                &requested,
                true,
            ),
            Err(PostgresGenerationError::UnsafeTransition(
                WritableGenerationTransitionError::ConflictingHolder { .. }
            ))
        ));
        assert!(matches!(
            classify_ambiguous_commit(
                Some(&old),
                Some(&generation("cluster-2", "holder-z", 1)),
                &requested,
                true,
            ),
            Err(PostgresGenerationError::UnsafeTransition(
                WritableGenerationTransitionError::ForeignUniverse
            ))
        ));
    }

    #[test]
    fn publication_sql_is_fixed_logged_and_local() {
        assert!(TRANSACTION_SETTINGS.contains("search_path = pg_catalog"));
        assert!(TRANSACTION_SETTINGS.contains("ISOLATION LEVEL READ COMMITTED"));
        assert!(LOCAL_COMMIT.contains("synchronous_commit = local"));
        assert!(REMOTE_APPLY_COMMIT.contains("synchronous_commit = remote_apply"));
        assert!(TRANSACTION_SETTINGS.contains("lock_timeout"));
        assert!(TRANSACTION_SETTINGS.contains("statement_timeout"));
        assert!(TRANSACTION_SETTINGS.contains("transaction_timeout"));
        assert!(CREATE_TABLE.contains("CREATE TABLE"));
        assert!(!CREATE_TABLE.contains("UNLOGGED"));
        assert!(!CREATE_TABLE.contains("CHECK"));
        assert!(!CREATE_SCHEMA.contains("IF NOT EXISTS"));
        assert!(SCHEMA_IS_SAFE.contains("n.nspacl IS NULL"));
        assert!(SCHEMA_IS_SAFE.contains("pg_default_acl"));
        assert!(SELECT_FOR_UPDATE.contains("FOR UPDATE"));
        assert!(RELATION_IS_SAFE.contains("relpersistence = 'p'"));
        assert!(RELATION_IS_SAFE.contains("NOT c.relhastriggers"));
        assert!(RELATION_IS_SAFE.contains("ARRAY['n:{1}', 'n:{2}', 'p:{1}']"));
        assert!(RELATION_IS_SAFE.contains("count(*) = 1"));
        assert!(RELATION_IS_SAFE.contains("i.indexprs IS NULL"));
        assert!(RELATION_IS_SAFE.contains("i.indpred IS NULL"));
        assert!(RELATION_IS_SAFE.contains("i.indclass[0]"));
        assert!(LOCK_GENERATION_TABLE.contains("SHARE ROW EXCLUSIVE"));
        assert!(LOCK_DEFAULT_ACL_CATALOG.contains("pg_catalog.pg_default_acl"));
        assert!(LOCK_GENERATION_CATALOG_ROWS.contains("FOR UPDATE OF n, c, i, ic"));
        assert!(CURRENT_FLUSH_LSN.contains("pg_current_wal_flush_lsn"));
        assert!(SYNCHRONOUS_STANDBY_OBSERVATION.contains("pg_stat_replication"));
        assert!(SYNCHRONOUS_STANDBY_OBSERVATION.contains("pg_replication_slots"));
        assert!(SYNCHRONOUS_STANDBY_OBSERVATION.contains("slot_type = 'physical'"));
        assert!(SYNCHRONOUS_STANDBY_OBSERVATION.contains("flush_lsn"));
        assert!(SYNCHRONOUS_STANDBY_OBSERVATION.contains("replay_lsn"));
        assert!(EVIDENCE_TRANSACTION_SETTINGS.contains("REPEATABLE READ, READ ONLY"));
        assert!(LOCK_GENERATION_TABLE_ACCESS_SHARE.contains("TABLE ONLY"));
        assert!(LOCK_GENERATION_TABLE_ACCESS_SHARE.contains("ACCESS SHARE MODE"));
        assert!(!LOCK_GENERATION_TABLE_ACCESS_SHARE.contains("ROW EXCLUSIVE"));
        assert!(!EVIDENCE_TRANSACTION_SETTINGS.contains("FOR UPDATE"));
        assert!(!SELECT_GENERATION.contains("FOR UPDATE"));
        assert!(SOURCE_WAL_OBSERVATION.contains("pg_current_wal_flush_lsn"));
        assert!(!SOURCE_IDENTITY_OBSERVATION.contains("pg_current_wal_flush_lsn"));
        assert!(STANDBY_WAL_OBSERVATION.contains("pg_last_wal_receive_lsn"));
        assert!(STANDBY_WAL_OBSERVATION.contains("pg_last_wal_replay_lsn"));
        assert!(!STANDBY_IDENTITY_OBSERVATION.contains("pg_last_wal_receive_lsn"));
        assert!(!STANDBY_IDENTITY_OBSERVATION.contains("pg_last_wal_replay_lsn"));
        assert!(CONNECTION_OPTIONS.contains("event_triggers=off"));
        assert!(CONNECTION_OPTIONS.contains("log_statement=none"));
    }

    #[tokio::test]
    #[ignore = "requires disposable primary and streaming-standby PostgreSQL 18 Unix sockets"]
    #[allow(clippy::too_many_lines)]
    async fn live_postgres18_proves_any_one_synchronous_generation_replay() {
        let socket_dir = std::env::var_os("PGSHARD_AGENT_TEST_SOCKET_DIR")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_SOCKET_DIR is required");
        let standby_socket_dir = std::env::var_os("PGSHARD_AGENT_TEST_STANDBY_SOCKET_DIR")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_STANDBY_SOCKET_DIR is required");
        let standby = connect(&standby_socket_dir)
            .await
            .expect("connect to streaming standby");
        let primary = connect(&socket_dir).await.expect("inspect primary WAL");
        assert_same_control_identity(&primary.client, &standby.client).await;
        let first = generation("cluster-1", "holder-a", 1);
        assert_unsafe_schema_rejected_before_ddl(&socket_dir, &first).await;

        assert_remote_apply_blocks_on_paused_replay(
            &socket_dir,
            &primary.client,
            &standby.client,
            &first,
        )
        .await;

        publish_synchronous_generation(&socket_dir, &first)
            .await
            .expect("replay live synchronous generation");
        assert_stable_publication_blocks_ddl(
            &socket_dir,
            &first,
            "BEGIN; SET LOCAL lock_timeout = '5s'; \
             GRANT USAGE ON SCHEMA pgshard_internal TO PUBLIC; ROLLBACK",
        )
        .await;
        assert_stable_publication_blocks_ddl(
            &socket_dir,
            &first,
            "BEGIN; SET LOCAL lock_timeout = '5s'; \
             ALTER DEFAULT PRIVILEGES FOR ROLE postgres \
             IN SCHEMA pgshard_internal \
             GRANT SELECT ON TABLES TO PUBLIC; ROLLBACK",
        )
        .await;
        assert_stable_publication_blocks_ddl(
            &socket_dir,
            &first,
            "BEGIN; SET LOCAL lock_timeout = '5s'; \
             DROP TABLE pgshard_internal.writable_generation; \
             CREATE TABLE pgshard_internal.writable_generation (\
                 singleton boolean, generation bytea); \
             ROLLBACK",
        )
        .await;
        assert_eq!(
            default_acl_count(&primary.client).await,
            0,
            "blocked default-privilege change must not survive its rollback"
        );
        assert!(matches!(
            publish_writable_generation(
                &socket_dir,
                &generation("cluster-1", "stale-holder", 1),
                &|| true,
            )
            .await,
            Err(PostgresGenerationError::UnsafeTransition(
                WritableGenerationTransitionError::ConflictingHolder { term: 1 }
            ))
        ));
        let second = generation("cluster-1", "holder-b", 2);
        assert_ambiguous_reread_rejects_extra_row(&socket_dir, &first).await;
        assert_hostile_indexes_rejected_before_dml(&socket_dir, &second).await;

        publish_synchronous_generation(&socket_dir, &second)
            .await
            .expect("advance synchronously replayed generation");
        assert_replayed_generation(&standby.client, &second).await;

        assert_live_replication_evidence(&socket_dir, &standby_socket_dir, &second).await;

        let row = primary
            .client
            .query_one(
                "SELECT g.generation, c.relpersistence::text \
                 FROM pgshard_internal.writable_generation AS g \
                 JOIN pg_catalog.pg_class AS c \
                   ON c.oid = 'pgshard_internal.writable_generation'::regclass \
                 WHERE g.singleton",
                &[],
            )
            .await
            .expect("read live WAL row");
        assert_eq!(
            row.try_get::<_, Vec<u8>>(0).expect("generation bytes"),
            second.canonical_bytes()
        );
        assert_eq!(
            row.try_get::<_, String>(1).expect("relation persistence"),
            "p"
        );
        assert_evidence_wal_capture_precedes_generation_snapshot(
            &socket_dir,
            &standby_socket_dir,
            &second,
        )
        .await;
        primary
            .client
            .batch_execute("DROP SCHEMA pgshard_internal CASCADE")
            .await
            .expect("remove disposable generation schema");
    }

    #[allow(clippy::too_many_lines)]
    async fn assert_live_replication_evidence(
        source_socket: &Path,
        standby_socket: &Path,
        generation: &DurableWritableGeneration,
    ) {
        let durability = GenerationDurability::remote_apply_any_one(vec![
            "pgshard_member_0001".to_owned(),
            "pgshard_member_0002".to_owned(),
        ])
        .expect("valid live evidence durability");
        let mut source_session = connect_replication_evidence(source_socket)
            .await
            .expect("connect retained source evidence session");
        let source_backend_pid_before = source_session
            .connection
            .client
            .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
            .await
            .expect("read source evidence backend pid")
            .try_get::<_, i32>(0)
            .expect("source evidence backend pid");
        let source_backend_count_before = replication_evidence_backend_count(&source_session).await;
        let wal_insert_lsn_before = current_wal_insert_lsn(&source_session).await;
        let source_evidence = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let evidence = source_session
                    .observe_source(generation, &durability)
                    .await
                    .expect("observe exact source generation evidence");
                if source_has_synchronous_witness(&evidence) {
                    break evidence;
                }
                sleep(CONNECT_RETRY_DELAY).await;
            }
        })
        .await
        .expect("synchronous source evidence converged");
        for _ in 0..3 {
            source_session
                .observe_source(generation, &durability)
                .await
                .expect("repeat source evidence sample on retained session");
        }
        let source_backend_pid_after = source_session
            .connection
            .client
            .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
            .await
            .expect("re-read source evidence backend pid")
            .try_get::<_, i32>(0)
            .expect("source evidence backend pid");
        assert_eq!(source_backend_pid_after, source_backend_pid_before);
        assert_eq!(source_backend_count_before, 1);
        assert_eq!(replication_evidence_backend_count(&source_session).await, 1);
        assert_eq!(
            current_wal_insert_lsn(&source_session).await,
            wal_insert_lsn_before
        );
        assert_evidence_access_share_blocks_ddl(
            source_socket,
            generation,
            &durability,
            "BEGIN; ALTER TABLE pgshard_internal.writable_generation \
             ADD COLUMN concurrent_shape integer; ROLLBACK",
        )
        .await;
        assert_evidence_access_share_blocks_ddl(
            source_socket,
            generation,
            &durability,
            "BEGIN; DROP TABLE pgshard_internal.writable_generation; \
             CREATE TABLE pgshard_internal.writable_generation (\
                 singleton boolean, generation bytea); ROLLBACK",
        )
        .await;
        assert_evidence_access_share_blocks_rls_commit(source_socket, generation, &durability)
            .await;
        assert_evidence_timestamp_is_conservative_for_default_acl_commit(
            source_socket,
            generation,
            &durability,
        )
        .await;

        let mut standby_session = connect_replication_evidence(standby_socket)
            .await
            .expect("connect retained standby evidence session");
        let standby_evidence = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let evidence = standby_session
                    .observe_standby("pgshard_member_0001")
                    .await
                    .expect("observe exact standby generation evidence");
                if evidence.replay_lsn.0 >= source_evidence.generation_barrier_lsn.0 {
                    break evidence;
                }
                sleep(CONNECT_RETRY_DELAY).await;
            }
        })
        .await
        .expect("standby evidence replayed the sampled source barrier");
        assert_eq!(
            source_evidence.generation_identity,
            canonical_generation_text(generation)
        );
        assert_eq!(
            standby_evidence.generation_identity,
            source_evidence.generation_identity
        );
        assert_eq!(
            standby_evidence.system_identifier,
            source_evidence.system_identifier
        );
        assert_eq!(standby_evidence.timeline, source_evidence.timeline);
        assert!(!source_evidence.in_recovery);
        assert!(standby_evidence.in_recovery);
        assert!(source_has_synchronous_witness(&source_evidence));
        let now_unix_ms = source_evidence
            .observed_at_unix_ms
            .max(standby_evidence.observed_at_unix_ms);
        assert_eq!(
            crate::domain::classify_initial_serving_eligibility(
                Some(&source_evidence),
                &[standby_evidence],
                now_unix_ms,
            ),
            crate::domain::InitialServingEligibility::Eligible
        );
    }

    #[allow(clippy::too_many_lines)]
    async fn assert_evidence_wal_capture_precedes_generation_snapshot(
        source_socket: &Path,
        standby_socket: &Path,
        current_generation: &DurableWritableGeneration,
    ) {
        let durability = GenerationDurability::remote_apply_any_one(vec![
            "pgshard_member_0001".to_owned(),
            "pgshard_member_0002".to_owned(),
        ])
        .expect("valid temporal evidence durability");

        // A source sample blocked after its flush-position read has not begun
        // its repeatable-read transaction. Advancing the generation here must
        // therefore be observed as an exact-generation mismatch, rather than
        // returning the old row paired with WAL flushed by the new commit.
        let third = generation("cluster-1", "holder-c", 3);
        let (source_captured, release_source) = gate_next_evidence_wal_capture();
        let source_observation_socket = source_socket.to_owned();
        let source_observation_generation = current_generation.clone();
        let source_observation_durability = durability.clone();
        let source_observation = tokio::spawn(async move {
            let mut session = connect_replication_evidence(&source_observation_socket).await?;
            session
                .observe_source(
                    &source_observation_generation,
                    &source_observation_durability,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), source_captured)
            .await
            .expect("source captured WAL before its generation snapshot")
            .expect("source retained pre-snapshot WAL gate");
        publish_synchronous_generation(source_socket, &third)
            .await
            .expect("advance generation while source evidence is pre-snapshot");
        release_source
            .send(())
            .expect("release source pre-snapshot WAL gate");
        assert!(matches!(
            source_observation
                .await
                .expect("join source temporal observation"),
            Err(PostgresGenerationError::GenerationEvidenceChanged)
        ));

        // A standby sample blocked at the same boundary may see a later
        // generation after replay resumes, but its receive/replay positions
        // remain the earlier values. They must not satisfy the later source
        // barrier until a fresh complete standby sample is taken.
        let fourth = generation("cluster-1", "holder-d", 4);
        let (standby_captured, release_standby) = gate_next_evidence_wal_capture();
        let standby_observation_socket = standby_socket.to_owned();
        let standby_observation = tokio::spawn(async move {
            let mut session = connect_replication_evidence(&standby_observation_socket).await?;
            session.observe_standby("pgshard_member_0001").await
        });
        tokio::time::timeout(Duration::from_secs(5), standby_captured)
            .await
            .expect("standby captured WAL before its generation snapshot")
            .expect("standby retained pre-snapshot WAL gate");
        publish_synchronous_generation(source_socket, &fourth)
            .await
            .expect("advance generation while standby evidence is pre-snapshot");
        assert_replayed_generation(
            &connect(standby_socket)
                .await
                .expect("connect temporal standby verification")
                .client,
            &fourth,
        )
        .await;
        release_standby
            .send(())
            .expect("release standby pre-snapshot WAL gate");
        let conservative_standby = standby_observation
            .await
            .expect("join standby temporal observation")
            .expect("standby temporal observation remains coherent");
        assert_eq!(
            conservative_standby.generation_identity,
            canonical_generation_text(&fourth)
        );

        let mut source_session = connect_replication_evidence(source_socket)
            .await
            .expect("connect temporal source evidence session");
        let source_evidence = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let evidence = source_session
                    .observe_source(&fourth, &durability)
                    .await
                    .expect("observe later source generation");
                if source_has_synchronous_witness(&evidence) {
                    break evidence;
                }
                sleep(CONNECT_RETRY_DELAY).await;
            }
        })
        .await
        .expect("later source evidence converged");
        assert!(conservative_standby.receive_lsn.0 < source_evidence.generation_barrier_lsn.0);
        assert!(conservative_standby.replay_lsn.0 < source_evidence.generation_barrier_lsn.0);
        let now_unix_ms = source_evidence
            .observed_at_unix_ms
            .max(conservative_standby.observed_at_unix_ms);
        assert_eq!(
            crate::domain::classify_initial_serving_eligibility(
                Some(&source_evidence),
                &[conservative_standby],
                now_unix_ms,
            ),
            crate::domain::InitialServingEligibility::SynchronousWitnessMissing
        );

        let mut standby_session = connect_replication_evidence(standby_socket)
            .await
            .expect("connect fresh temporal standby evidence session");
        let fresh_standby = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let evidence = standby_session
                    .observe_standby("pgshard_member_0001")
                    .await
                    .expect("observe fresh standby evidence");
                if evidence.replay_lsn.0 >= source_evidence.generation_barrier_lsn.0 {
                    break evidence;
                }
                sleep(CONNECT_RETRY_DELAY).await;
            }
        })
        .await
        .expect("fresh standby evidence reached the later source barrier");
        let now_unix_ms = source_evidence
            .observed_at_unix_ms
            .max(fresh_standby.observed_at_unix_ms);
        assert_eq!(
            crate::domain::classify_initial_serving_eligibility(
                Some(&source_evidence),
                &[fresh_standby],
                now_unix_ms,
            ),
            crate::domain::InitialServingEligibility::Eligible
        );
    }

    async fn assert_evidence_access_share_blocks_ddl(
        socket_dir: &Path,
        generation: &DurableWritableGeneration,
        durability: &GenerationDurability,
        ddl: &'static str,
    ) {
        let (entered, release) = gate_next_evidence_access_share();
        let observation_socket = socket_dir.to_owned();
        let observation_generation = generation.clone();
        let observation_durability = durability.clone();
        let observation = tokio::spawn(async move {
            let mut session = connect_replication_evidence(&observation_socket).await?;
            session
                .observe_source(&observation_generation, &observation_durability)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .expect("evidence sample acquired target AccessShare")
            .expect("evidence sample retained AccessShare gate");
        let change_started_at_unix_ms = unix_time_ms();

        let attacker = connect(socket_dir).await.expect("connect evidence DDL");
        let attacker_pid = attacker
            .client
            .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
            .await
            .expect("read evidence DDL backend PID")
            .try_get::<_, i32>(0)
            .expect("evidence DDL backend PID");
        let ddl_task = tokio::spawn(async move { attacker.client.batch_execute(ddl).await });
        let observer = connect(socket_dir).await.expect("observe evidence DDL");
        wait_for_backend_lock(&observer.client, attacker_pid).await;
        assert!(
            !ddl_task.is_finished(),
            "target shape/name DDL passed the evidence AccessShare lock"
        );

        release.send(()).expect("release evidence AccessShare gate");
        let evidence = observation
            .await
            .expect("join locked evidence sample")
            .expect("locked evidence sample remains coherent");
        assert!(
            evidence.observed_at_unix_ms <= change_started_at_unix_ms,
            "evidence timestamp must precede target DDL attempt"
        );
        ddl_task
            .await
            .expect("join blocked evidence DDL")
            .expect("target DDL proceeds only after evidence sample releases");
    }

    async fn assert_evidence_access_share_blocks_rls_commit(
        socket_dir: &Path,
        generation: &DurableWritableGeneration,
        durability: &GenerationDurability,
    ) {
        let (entered, release) = gate_next_evidence_access_share();
        let observation_socket = socket_dir.to_owned();
        let observation_generation = generation.clone();
        let observation_durability = durability.clone();
        let observation = tokio::spawn(async move {
            let mut session = connect_replication_evidence(&observation_socket).await?;
            session
                .observe_source(&observation_generation, &observation_durability)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .expect("RLS evidence sample acquired target AccessShare")
            .expect("RLS evidence sample retained AccessShare gate");
        let change_started_at_unix_ms = unix_time_ms();

        let attacker = connect(socket_dir)
            .await
            .expect("connect concurrent RLS DDL");
        let attacker_pid = attacker
            .client
            .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
            .await
            .expect("read concurrent RLS backend PID")
            .try_get::<_, i32>(0)
            .expect("concurrent RLS backend PID");
        let rls = tokio::spawn(async move {
            attacker
                .client
                .batch_execute(
                    "ALTER TABLE pgshard_internal.writable_generation \
                     ENABLE ROW LEVEL SECURITY",
                )
                .await
        });
        let observer = connect(socket_dir)
            .await
            .expect("observe concurrent RLS DDL");
        wait_for_backend_lock(&observer.client, attacker_pid).await;
        assert!(!rls.is_finished(), "RLS DDL passed evidence AccessShare");

        release.send(()).expect("release RLS evidence gate");
        match observation.await.expect("join RLS evidence sample") {
            Ok(evidence) => assert!(evidence.observed_at_unix_ms <= change_started_at_unix_ms),
            Err(PostgresGenerationError::UnsafeRelation) => {}
            Err(error) => panic!("unexpected concurrent RLS evidence result: {error}"),
        }
        rls.await
            .expect("join concurrent RLS DDL")
            .expect("RLS commits after evidence AccessShare releases");

        let mut session = connect_replication_evidence(socket_dir)
            .await
            .expect("connect post-RLS evidence session");
        assert!(matches!(
            session.observe_source(generation, durability).await,
            Err(PostgresGenerationError::UnsafeRelation)
        ));
        observer
            .client
            .batch_execute(
                "ALTER TABLE pgshard_internal.writable_generation \
                 DISABLE ROW LEVEL SECURITY",
            )
            .await
            .expect("restore evidence relation RLS state");
        session
            .observe_source(generation, durability)
            .await
            .expect("evidence recovers after RLS state restoration");
    }

    async fn assert_evidence_timestamp_is_conservative_for_default_acl_commit(
        socket_dir: &Path,
        generation: &DurableWritableGeneration,
        durability: &GenerationDurability,
    ) {
        let (entered, release) = gate_next_evidence_snapshot();
        let observation_socket = socket_dir.to_owned();
        let observation_generation = generation.clone();
        let observation_durability = durability.clone();
        let observation = tokio::spawn(async move {
            let mut session = connect_replication_evidence(&observation_socket).await?;
            session
                .observe_source(&observation_generation, &observation_durability)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .expect("ACL evidence sample established repeatable-read snapshot")
            .expect("ACL evidence sample retained snapshot gate");
        let change_started_at_unix_ms = unix_time_ms();

        let attacker = connect(socket_dir)
            .await
            .expect("connect concurrent default-ACL change");
        tokio::time::timeout(
            Duration::from_secs(2),
            attacker.client.batch_execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE postgres \
                 IN SCHEMA pgshard_internal \
                 GRANT SELECT ON TABLES TO PUBLIC",
            ),
        )
        .await
        .expect("default-ACL commit remains compatible with target AccessShare")
        .expect("commit concurrent default-ACL change");
        assert_eq!(default_acl_count(&attacker.client).await, 1);

        release.send(()).expect("release ACL snapshot gate");
        let evidence = observation
            .await
            .expect("join ACL evidence sample")
            .expect("repeatable-read evidence retains its pre-ACL snapshot");
        assert!(
            evidence.observed_at_unix_ms <= change_started_at_unix_ms,
            "evidence returned across concurrent ACL commit must be timestamped before it"
        );

        let mut session = connect_replication_evidence(socket_dir)
            .await
            .expect("connect post-ACL evidence session");
        assert!(matches!(
            session.observe_source(generation, durability).await,
            Err(PostgresGenerationError::UnsafeSchema)
        ));
        attacker
            .client
            .batch_execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE postgres \
                 IN SCHEMA pgshard_internal \
                 REVOKE SELECT ON TABLES FROM PUBLIC",
            )
            .await
            .expect("restore default ACLs");
        assert_eq!(default_acl_count(&attacker.client).await, 0);
        session
            .observe_source(generation, durability)
            .await
            .expect("evidence recovers after default-ACL restoration");
    }

    async fn replication_evidence_backend_count(session: &ReplicationEvidenceSession) -> i64 {
        session
            .connection
            .client
            .query_one(
                "SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity \
                 WHERE application_name = 'pgshard-replication-evidence'",
                &[],
            )
            .await
            .expect("count replication evidence backends")
            .try_get(0)
            .expect("replication evidence backend count")
    }

    async fn current_wal_insert_lsn(session: &ReplicationEvidenceSession) -> String {
        session
            .connection
            .client
            .query_one("SELECT pg_catalog.pg_current_wal_insert_lsn()::text", &[])
            .await
            .expect("read current WAL insert LSN")
            .try_get(0)
            .expect("current WAL insert LSN")
    }

    fn source_has_synchronous_witness(evidence: &SourceReplicationEvidence) -> bool {
        evidence.candidates.iter().any(|candidate| {
            candidate.member_slot_name == "pgshard_member_0001"
                && candidate.slot_active
                && candidate.slot_walsender_match
                && candidate.stream_state == Some(ReplicationStreamState::Streaming)
                && candidate.sync_state == Some(ReplicationSyncState::Quorum)
                && candidate
                    .flush_lsn
                    .is_some_and(|lsn| lsn.0 >= evidence.generation_barrier_lsn.0)
                && candidate
                    .replay_lsn
                    .is_some_and(|lsn| lsn.0 >= evidence.generation_barrier_lsn.0)
        })
    }

    async fn assert_remote_apply_blocks_on_paused_replay(
        socket_dir: &Path,
        primary: &Client,
        standby: &Client,
        generation: &DurableWritableGeneration,
    ) {
        standby
            .batch_execute("SELECT pg_catalog.pg_wal_replay_pause()")
            .await
            .expect("pause standby replay");
        let publication_socket = socket_dir.to_owned();
        let publication_generation = generation.clone();
        let publication = tokio::spawn(async move {
            publish_synchronous_generation(&publication_socket, &publication_generation).await
        });
        wait_for_synchronous_commit(primary).await;
        assert!(
            !publication.is_finished(),
            "remote-apply generation publication passed paused standby replay"
        );
        standby
            .batch_execute("SELECT pg_catalog.pg_wal_replay_resume()")
            .await
            .expect("resume standby replay");
        tokio::time::timeout(Duration::from_secs(15), publication)
            .await
            .expect("synchronous generation publication completed after replay resumed")
            .expect("join synchronous generation publication")
            .expect("initialize synchronously replayed generation");
        assert_replayed_generation(standby, generation).await;
    }

    async fn publish_synchronous_generation(
        socket_dir: &Path,
        generation: &DurableWritableGeneration,
    ) -> Result<(), PostgresGenerationError> {
        let durability = GenerationDurability::remote_apply_any_one(vec![
            "pgshard_member_0001".to_owned(),
            "pgshard_member_0002".to_owned(),
        ])?;
        publish_writable_generation_with_durability(socket_dir, generation, &durability, &|| true)
            .await
    }

    async fn assert_unsafe_schema_rejected_before_ddl(
        socket_dir: &Path,
        generation: &DurableWritableGeneration,
    ) {
        let connection = connect(socket_dir).await.expect("prepare unsafe schema");
        connection
            .client
            .batch_execute(
                "CREATE SCHEMA pgshard_internal AUTHORIZATION postgres; \
                 GRANT USAGE ON SCHEMA pgshard_internal TO PUBLIC",
            )
            .await
            .expect("create unsafe preexisting schema");
        assert!(matches!(
            publish_writable_generation(socket_dir, generation, &|| true).await,
            Err(PostgresGenerationError::UnsafeSchema)
        ));
        let relation = connection
            .client
            .query_one(
                "SELECT pg_catalog.to_regclass(\
                     'pgshard_internal.writable_generation')::text",
                &[],
            )
            .await
            .expect("inspect rejected schema")
            .try_get::<_, Option<String>>(0)
            .expect("optional rejected relation");
        assert!(
            relation.is_none(),
            "unsafe schema was mutated before rejection"
        );
        connection
            .client
            .batch_execute("DROP SCHEMA pgshard_internal")
            .await
            .expect("remove unsafe schema fixture");
    }

    async fn assert_ambiguous_reread_rejects_extra_row(
        socket_dir: &Path,
        generation: &DurableWritableGeneration,
    ) {
        let attacker = connect(socket_dir)
            .await
            .expect("prepare singleton-cardinality attack");
        attacker
            .client
            .execute(
                "INSERT INTO pgshard_internal.writable_generation \
                 (singleton, generation) VALUES (false, $1)",
                &[&generation.canonical_bytes()],
            )
            .await
            .expect("insert false singleton row");
        let mut rereader = connect(socket_dir)
            .await
            .expect("connect ambiguous rereader");
        assert!(matches!(
            read_current(&mut rereader.client).await,
            Err(PostgresGenerationError::SingletonChanged)
        ));
        attacker
            .client
            .execute(
                "DELETE FROM pgshard_internal.writable_generation WHERE NOT singleton",
                &[],
            )
            .await
            .expect("remove false singleton row");
    }

    async fn assert_hostile_indexes_rejected_before_dml(
        socket_dir: &Path,
        requested: &DurableWritableGeneration,
    ) {
        let attacker = connect(socket_dir)
            .await
            .expect("prepare hostile index fixtures");
        attacker
            .client
            .batch_execute(
                "CREATE SEQUENCE pgshard_internal.attacker_calls; \
                 CREATE FUNCTION pgshard_internal.attacker_index_expression(boolean) \
                 RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT \
                 SET search_path = pg_catalog AS $$ \
                 BEGIN \
                   PERFORM pg_catalog.nextval(\
                     'pgshard_internal.attacker_calls'::pg_catalog.regclass); \
                   RETURN $1; \
                 END $$; \
                 CREATE INDEX writable_generation_attacker_expression \
                 ON pgshard_internal.writable_generation \
                 ((pgshard_internal.attacker_index_expression(singleton)))",
            )
            .await
            .expect("create hostile expression index");
        let calls_before = sequence_state(&attacker.client).await;
        assert!(matches!(
            publish_writable_generation(socket_dir, requested, &|| true).await,
            Err(PostgresGenerationError::UnsafeRelation)
        ));
        assert_eq!(
            sequence_state(&attacker.client).await,
            calls_before,
            "relation rejection must precede attacker expression execution"
        );
        attacker
            .client
            .batch_execute(
                "DROP INDEX pgshard_internal.writable_generation_attacker_expression; \
                 DROP FUNCTION pgshard_internal.attacker_index_expression(boolean); \
                 DROP SEQUENCE pgshard_internal.attacker_calls; \
                 CREATE INDEX writable_generation_attacker_extra \
                 ON pgshard_internal.writable_generation (generation)",
            )
            .await
            .expect("replace expression index with extra plain index");
        assert!(matches!(
            publish_writable_generation(socket_dir, requested, &|| true).await,
            Err(PostgresGenerationError::UnsafeRelation)
        ));
        attacker
            .client
            .batch_execute("DROP INDEX pgshard_internal.writable_generation_attacker_extra")
            .await
            .expect("remove extra plain index");
    }

    async fn control_identity(client: &Client) -> (String, String) {
        let row = client
            .query_one(
                "SELECT s.system_identifier::text, c.timeline_id::text \
                 FROM pg_catalog.pg_control_system() AS s, \
                      pg_catalog.pg_control_checkpoint() AS c",
                &[],
            )
            .await
            .expect("read PostgreSQL control identity");
        (
            row.try_get(0).expect("system identifier"),
            row.try_get(1).expect("checkpoint timeline"),
        )
    }

    async fn assert_same_control_identity(primary: &Client, standby: &Client) {
        assert_eq!(
            control_identity(primary).await,
            control_identity(standby).await,
            "standby must be the same physical PostgreSQL system and timeline"
        );
    }

    async fn default_acl_count(client: &Client) -> i64 {
        client
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_default_acl AS d \
                 WHERE d.defaclrole = (\
                     SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'postgres')",
                &[],
            )
            .await
            .expect("read PostgreSQL default ACL count")
            .try_get(0)
            .expect("PostgreSQL default ACL count")
    }

    async fn wait_for_synchronous_commit(primary: &Client) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting = primary
                    .query_one(
                        "SELECT count(*) = 1 FROM pg_catalog.pg_stat_activity \
                         WHERE application_name = 'pgshard-generation-publisher' \
                           AND wait_event_type = 'IPC' AND wait_event = 'SyncRep'",
                        &[],
                    )
                    .await
                    .expect("observe remote-apply commit wait")
                    .try_get::<_, bool>(0)
                    .expect("remote-apply wait count");
                if waiting {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("generation publisher reached PostgreSQL synchronous-replication wait");
    }

    async fn assert_stable_publication_blocks_ddl(
        socket_dir: &Path,
        generation: &DurableWritableGeneration,
        ddl: &'static str,
    ) {
        let (entered, release) = gate_next_stable_publication();
        let publication_socket = socket_dir.to_owned();
        let publication_generation = generation.clone();
        let publication = tokio::spawn(async move {
            publish_writable_generation(&publication_socket, &publication_generation, &|| true)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .expect("publisher reached stable relation lock")
            .expect("publisher retained stable relation gate");

        let attacker = connect(socket_dir).await.expect("connect concurrent DDL");
        let attacker_pid = attacker
            .client
            .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
            .await
            .expect("read concurrent DDL backend PID")
            .try_get::<_, i32>(0)
            .expect("concurrent DDL backend PID");
        let ddl_task = tokio::spawn(async move { attacker.client.batch_execute(ddl).await });
        let observer = connect(socket_dir).await.expect("observe concurrent DDL");
        wait_for_backend_lock(&observer.client, attacker_pid).await;
        assert!(
            !ddl_task.is_finished(),
            "concurrent DDL passed the publisher's stable lock"
        );

        release.send(()).expect("release stable publication");
        publication
            .await
            .expect("join stable publication")
            .expect("stable publication commits before DDL");
        ddl_task
            .await
            .expect("join concurrent DDL")
            .expect("concurrent DDL proceeds only after publication commit");
    }

    async fn wait_for_backend_lock(observer: &Client, backend_pid: i32) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting = observer
                    .query_one(
                        "SELECT COALESCE((\
                             SELECT wait_event_type = 'Lock' \
                             FROM pg_catalog.pg_stat_activity WHERE pid = $1), false)",
                        &[&backend_pid],
                    )
                    .await
                    .expect("inspect concurrent DDL wait")
                    .try_get::<_, bool>(0)
                    .expect("concurrent DDL lock wait");
                if waiting {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("concurrent DDL reached the publisher lock");
    }

    async fn sequence_state(client: &Client) -> (i64, bool) {
        let row = client
            .query_one(
                "SELECT last_value, is_called \
                 FROM pgshard_internal.attacker_calls",
                &[],
            )
            .await
            .expect("read attacker sequence state");
        (
            row.try_get(0).expect("attacker sequence value"),
            row.try_get(1).expect("attacker sequence called state"),
        )
    }

    async fn assert_replayed_generation(standby: &Client, expected: &DurableWritableGeneration) {
        let bytes = standby
            .query_one(
                "SELECT generation FROM pgshard_internal.writable_generation \
                 WHERE singleton",
                &[],
            )
            .await
            .expect("read replicated generation")
            .try_get::<_, Vec<u8>>(0)
            .expect("replicated generation bytes");
        assert_eq!(bytes, expected.canonical_bytes());
    }
}
