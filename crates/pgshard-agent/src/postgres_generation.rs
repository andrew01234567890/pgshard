//! WAL-backed writable-generation publication through quarantined `PostgreSQL`.
#![cfg_attr(test, allow(dead_code))]

use std::path::Path;
use std::time::Duration;

use pgshard_types::writable_generation::{
    DurableWritableGeneration, WritableGenerationTransition, WritableGenerationTransitionError,
    classify_writable_generation_transition,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_postgres::{Client, Config, NoTls};

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
    SET LOCAL idle_in_transaction_session_timeout = '5s';\
    SET LOCAL synchronous_commit = on;";
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
const INSERT_GENERATION: &str = "\
    INSERT INTO pgshard_internal.writable_generation (singleton, generation) \
    VALUES (true, $1)";
const UPDATE_GENERATION: &str = "\
    UPDATE pgshard_internal.writable_generation SET generation = $1 \
    WHERE singleton = true";

#[cfg(test)]
struct StablePublicationGate {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[cfg(test)]
static TEST_STABLE_PUBLICATION_GATE: std::sync::Mutex<Option<StablePublicationGate>> =
    std::sync::Mutex::new(None);

/// Publishes one generation to a singleton WAL-logged row in the `postgres`
/// database over the private Unix socket.
///
/// `authority_exact` must consult the attempt-private authority channel. It is
/// checked before every connection attempt and immediately before `COMMIT`.
/// The caller supplies the outer shutdown, child-exit, and timeout race.
pub(crate) async fn publish_writable_generation<F>(
    socket_dir: &Path,
    requested: &DurableWritableGeneration,
    authority_exact: &F,
) -> Result<(), PostgresGenerationError>
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
                tracing::debug!(reason = %error, "waiting for quarantined PostgreSQL socket");
                sleep(CONNECT_RETRY_DELAY).await;
                continue;
            }
        };
        let transaction = connection.client.transaction().await?;
        transaction.batch_execute(TRANSACTION_SETTINGS).await?;
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

        // No await or state-changing operation may be inserted between this
        // exact authority observation and dispatching COMMIT.
        if !authority_exact() {
            return Err(PostgresGenerationError::AuthorityChanged);
        }
        match transaction.commit().await {
            Ok(()) => return Ok(()),
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
                    AmbiguousCommitOutcome::Committed => return Ok(()),
                    AmbiguousCommitOutcome::Retry => {
                        sleep(CONNECT_RETRY_DELAY).await;
                    }
                }
            }
        }
    }
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
    // The relation lock resolves the currently named object and excludes
    // DDL that changes its table/index shape. Catalog tuple locks additionally
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
    let mut config = Config::new();
    config
        .host_path(socket_dir)
        .port(5432)
        .user("postgres")
        .dbname("postgres")
        .application_name("pgshard-generation-publisher")
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
        assert!(TRANSACTION_SETTINGS.contains("synchronous_commit = on"));
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
        assert!(LOCK_GENERATION_CATALOG_ROWS.contains("FOR UPDATE OF n, c, i, ic"));
        assert!(CONNECTION_OPTIONS.contains("event_triggers=off"));
        assert!(CONNECTION_OPTIONS.contains("log_statement=none"));
    }

    #[tokio::test]
    #[ignore = "requires disposable primary and streaming-standby PostgreSQL 18 Unix sockets"]
    async fn live_postgres18_replicates_exact_generation_across_flush_barriers() {
        let socket_dir = std::env::var_os("PGSHARD_AGENT_TEST_SOCKET_DIR")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_SOCKET_DIR is required");
        let standby_socket_dir = std::env::var_os("PGSHARD_AGENT_TEST_STANDBY_SOCKET_DIR")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_STANDBY_SOCKET_DIR is required");
        let standby = connect(&standby_socket_dir)
            .await
            .expect("connect to streaming standby");
        let first = generation("cluster-1", "holder-a", 1);
        assert_unsafe_schema_rejected_before_ddl(&socket_dir, &first).await;

        publish_writable_generation(&socket_dir, &first, &|| true)
            .await
            .expect("initialize live generation");
        publish_writable_generation(&socket_dir, &first, &|| true)
            .await
            .expect("replay live generation");
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
             DROP TABLE pgshard_internal.writable_generation; \
             CREATE TABLE pgshard_internal.writable_generation (\
                 singleton boolean, generation bytea); \
             ROLLBACK",
        )
        .await;
        let primary = connect(&socket_dir).await.expect("inspect primary WAL");
        assert_eq!(
            control_identity(&primary.client).await,
            control_identity(&standby.client).await,
            "standby must be the same physical PostgreSQL system and timeline"
        );
        let first_barrier = current_flush_lsn(&primary.client).await;
        wait_for_replayed_generation(&standby.client, &first_barrier, &first).await;
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

        publish_writable_generation(&socket_dir, &second, &|| true)
            .await
            .expect("advance live generation");
        let second_barrier = current_flush_lsn(&primary.client).await;
        wait_for_replayed_generation(&standby.client, &second_barrier, &second).await;

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
        primary
            .client
            .batch_execute("DROP SCHEMA pgshard_internal CASCADE")
            .await
            .expect("remove disposable generation schema");
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

    async fn current_flush_lsn(client: &Client) -> String {
        client
            .query_one("SELECT pg_catalog.pg_current_wal_flush_lsn()::text", &[])
            .await
            .expect("read primary WAL flush barrier")
            .try_get(0)
            .expect("primary flush LSN")
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

    async fn wait_for_replayed_generation(
        standby: &Client,
        barrier: &str,
        expected: &DurableWritableGeneration,
    ) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let replayed = standby
                    .query_one(
                        "SELECT COALESCE(\
                             pg_catalog.pg_last_wal_replay_lsn() >= $1::text::pg_lsn, false)",
                        &[&barrier],
                    )
                    .await
                    .expect("read standby replay position")
                    .try_get::<_, bool>(0)
                    .expect("standby replay comparison");
                if replayed {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("standby replay reached primary flush barrier");
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
