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

const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(25);
const CONNECTION_OPTIONS: &str = "-c search_path=pg_catalog \
    -c session_preload_libraries= -c local_preload_libraries= \
    -c event_triggers=off -c jit=off -c default_tablespace= -c temp_tablespaces= \
    -c default_table_access_method=heap -c default_transaction_read_only=off \
    -c row_security=off -c synchronous_commit=on -c log_statement=none \
    -c log_min_error_statement=panic -c log_parameter_max_length=0 \
    -c log_parameter_max_length_on_error=0";
const TRANSACTION_SETTINGS: &str = "\
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
    FROM pg_catalog.pg_namespace AS n \
    JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid \
    WHERE n.nspname = 'pgshard_internal' AND c.relname = 'writable_generation'";
const LOCK_GENERATION_TABLE: &str = "\
    LOCK TABLE pgshard_internal.writable_generation IN SHARE ROW EXCLUSIVE MODE";
const SELECT_FOR_UPDATE: &str = "\
    SELECT singleton, generation FROM pgshard_internal.writable_generation FOR UPDATE";
const SELECT_CURRENT: &str = "\
    SELECT generation FROM pgshard_internal.writable_generation \
    WHERE singleton = true";
const INSERT_GENERATION: &str = "\
    INSERT INTO pgshard_internal.writable_generation (singleton, generation) \
    VALUES (true, $1)";
const UPDATE_GENERATION: &str = "\
    UPDATE pgshard_internal.writable_generation SET generation = $1 \
    WHERE singleton = true";

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
        if query_safety(&transaction, SCHEMA_IS_SAFE).await? != Some(true) {
            // Recheck after relation setup so a catalog-level race cannot make
            // a previously safe namespace authorize publication.
            return Err(PostgresGenerationError::UnsafeSchema);
        }
        if query_safety(&transaction, RELATION_IS_SAFE).await? != Some(true) {
            return Err(PostgresGenerationError::UnsafeRelation);
        }
        transaction.batch_execute(LOCK_GENERATION_TABLE).await?;
        let rows = transaction.query(SELECT_FOR_UPDATE, &[]).await?;
        let existing = match rows.as_slice() {
            [] => None,
            [row] if row.try_get::<_, bool>(0)? => {
                let bytes = row.try_get::<_, Vec<u8>>(1)?;
                Some(parse_generation(&bytes)?)
            }
            _ => return Err(PostgresGenerationError::SingletonChanged),
        };
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
    transaction.batch_execute(LOCK_GENERATION_TABLE).await?;
    if query_safety(&transaction, SCHEMA_IS_SAFE).await? != Some(true) {
        return Err(PostgresGenerationError::UnsafeSchema);
    }
    if query_safety(&transaction, RELATION_IS_SAFE).await? != Some(true) {
        return Err(PostgresGenerationError::UnsafeRelation);
    }
    match transaction.query_opt(SELECT_CURRENT, &[]).await {
        Ok(Some(row)) => {
            let bytes = row.try_get::<_, Vec<u8>>(0)?;
            Ok(Some(parse_generation(&bytes)?))
        }
        Ok(None) => Ok(None),
        Err(error) => Err(error.into()),
    }
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
        assert!(LOCK_GENERATION_TABLE.contains("SHARE ROW EXCLUSIVE"));
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
        let unsafe_schema = connect(&socket_dir).await.expect("prepare unsafe schema");
        unsafe_schema
            .client
            .batch_execute(
                "CREATE SCHEMA pgshard_internal AUTHORIZATION postgres; \
                 GRANT USAGE ON SCHEMA pgshard_internal TO PUBLIC",
            )
            .await
            .expect("create unsafe preexisting schema");
        assert!(matches!(
            publish_writable_generation(&socket_dir, &first, &|| true).await,
            Err(PostgresGenerationError::UnsafeSchema)
        ));
        let relation = unsafe_schema
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
        unsafe_schema
            .client
            .batch_execute("DROP SCHEMA pgshard_internal")
            .await
            .expect("remove unsafe schema fixture");
        drop(unsafe_schema);

        publish_writable_generation(&socket_dir, &first, &|| true)
            .await
            .expect("initialize live generation");
        publish_writable_generation(&socket_dir, &first, &|| true)
            .await
            .expect("replay live generation");
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
