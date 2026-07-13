//! Live `PostgreSQL` 18 contract tests for the shard schema catalog.
//!
//! Run explicitly with a superuser URL whose database name is `shardschema`:
//! `PGSHARD_TEST_DATABASE_URL=... cargo test -p pgshard-catalog --test postgres18 -- --ignored`

use std::error::Error;
use std::future::{pending, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use tokio_postgres::{AsyncMessage, Client, Error as PgError, IsolationLevel, NoTls};
use uuid::Uuid;

use pgshard_catalog::{
    CatalogCache, CatalogConnectionPhase, CatalogPollInterval, CatalogReader,
    CatalogReadinessReason, CatalogRefreshError, CatalogSupervisor, CatalogSupervisorConfig,
    CatalogSupervisorSnapshot, CatalogSupervisorStatus, DatabaseId, InstallOutcome, LoadError,
    run_catalog_refresh,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Fixture {
    logical_database_id: String,
    nonce: u128,
    shard_id: String,
}

struct RoutingFixture {
    catalog_epoch: i64,
    valid_epoch: i64,
}

struct CatalogListener {
    _client: Client,
    receiver: Receiver<String>,
    task: JoinHandle<Result<(), PgError>>,
}

struct CatalogDriver {
    cache: Arc<CatalogCache>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    task: JoinHandle<Result<(), CatalogRefreshError>>,
}

impl CatalogDriver {
    async fn start(database_url: &str, poll_interval: CatalogPollInterval) -> TestResult<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
        let cache = Arc::new(CatalogCache::new());
        let driver_cache = Arc::clone(&cache);
        let (shutdown, shutdown_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run_catalog_refresh(
            client,
            connection,
            driver_cache,
            poll_interval,
            async move {
                let _ = shutdown_receiver.await;
            },
        ));
        Ok(Self {
            cache,
            shutdown,
            task,
        })
    }

    async fn shutdown(self) -> TestResult {
        self.shutdown
            .send(())
            .map_err(|()| "catalog refresh driver dropped its shutdown receiver")?;
        tokio::time::timeout(Duration::from_secs(5), self.task).await???;
        Ok(())
    }
}

fn assert_sqlstate(error: &PgError, expected: &str) {
    let actual = error
        .as_db_error()
        .map(|database_error| database_error.code().code());
    assert_eq!(
        actual,
        Some(expected),
        "unexpected PostgreSQL error: {error}"
    );
}

async fn catalog_epoch(client: &Client) -> Result<i64, PgError> {
    Ok(client
        .query_one(
            "SELECT catalog_epoch FROM pgshard_catalog.cluster_state WHERE singleton",
            &[],
        )
        .await?
        .get(0))
}

async fn stage_epoch(client: &Client, logical_database_id: &str) -> Result<i64, PgError> {
    Ok(client
        .query_one(
            "INSERT INTO pgshard_catalog.routing_epochs(logical_database_id) \
             VALUES ($1::text::uuid) RETURNING routing_epoch",
            &[&logical_database_id],
        )
        .await?
        .get(0))
}

fn notification_epoch(payload: &str) -> Option<i64> {
    let epoch = pgshard_catalog::CatalogNotification::parse(payload)
        .ok()?
        .epoch()
        .0;
    i64::try_from(epoch).ok()
}

fn wait_for_epoch(receiver: &Receiver<String>, expected: i64) -> Result<(), String> {
    for _ in 0..100 {
        match receiver.try_recv() {
            Ok(payload) if notification_epoch(&payload) == Some(expected) => return Ok(()),
            Ok(_) | Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(20)),
            Err(TryRecvError::Disconnected) => return Err("LISTEN connection disconnected".into()),
        }
    }
    Err(format!(
        "did not receive catalog epoch {expected} within two seconds"
    ))
}

fn wait_for_payload(receiver: &Receiver<String>, expected: &str) -> Result<(), String> {
    for _ in 0..100 {
        match receiver.try_recv() {
            Ok(payload) if payload == expected => return Ok(()),
            Ok(_) | Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(20)),
            Err(TryRecvError::Disconnected) => return Err("LISTEN connection disconnected".into()),
        }
    }
    Err(format!(
        "did not receive catalog payload {expected:?} within two seconds"
    ))
}

fn assert_no_notification(receiver: &Receiver<String>, context: &str) {
    std::thread::sleep(Duration::from_millis(100));
    match receiver.try_recv() {
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => panic!("{context}: LISTEN connection disconnected"),
        Ok(payload) => panic!("{context}: received payload {payload:?}"),
    }
}

async fn assert_installation_contract(client: &Client) -> TestResult {
    client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;
    let epoch_after_first_migration = catalog_epoch(client).await?;
    client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;
    assert_eq!(
        catalog_epoch(client).await?,
        epoch_after_first_migration,
        "reapplying the migration must not mutate catalog state"
    );

    let database_name: String = client
        .query_one("SELECT current_database()", &[])
        .await?
        .get(0);
    assert_eq!(database_name, "shardschema");
    let server_version: i32 = client
        .query_one("SELECT current_setting('server_version_num')::integer", &[])
        .await?
        .get(0);
    assert!(server_version >= 180_000);

    let unsafe_column_count: i64 = client
        .query_one(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_schema = 'pgshard_catalog' \
               AND lower(column_name) ~ '(password|credential|secret|dsn)'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(unsafe_column_count, 0);

    let error = client
        .execute(
            "UPDATE pgshard_catalog.cluster_configuration SET hash_seed = hash_seed + 1",
            &[],
        )
        .await
        .expect_err("hash configuration must be immutable");
    assert_sqlstate(&error, "55000");

    let overlong_name = format!("x{}", "a".repeat(63));
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.logical_databases(database_name) VALUES ($1::text)",
            &[&overlong_name],
        )
        .await
        .expect_err("64-byte identifiers must be rejected");
    assert_sqlstate(&error, "23514");
    Ok(())
}

async fn create_fixture(client: &Client) -> TestResult<Fixture> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let logical_name = format!("db_{nonce}");
    let logical_database_id = client
        .query_one(
            "INSERT INTO pgshard_catalog.logical_databases(database_name) \
             VALUES ($1::text) RETURNING logical_database_id::text",
            &[&logical_name],
        )
        .await?
        .get(0);
    let shard_number: i64 = client
        .query_one(
            "SELECT coalesce(max(shard_number), 0) + 1 FROM pgshard_catalog.shards",
            &[],
        )
        .await?
        .get(0);
    let shard_id = format!("shard-{nonce}");
    client
        .execute(
            "INSERT INTO pgshard_catalog.shards(shard_id, shard_number) \
             VALUES ($1::text, $2)",
            &[&shard_id, &shard_number],
        )
        .await?;
    Ok(Fixture {
        logical_database_id,
        nonce,
        shard_id,
    })
}

async fn assert_identity_history_contract(client: &Client, fixture: &Fixture) -> TestResult {
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_databases SET database_name = database_name || '_moved' \
             WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id],
        )
        .await
        .expect_err("logical database names must not be rebound");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "DELETE FROM pgshard_catalog.logical_databases \
             WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id],
        )
        .await
        .expect_err("logical database tombstones must be permanent");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.shards SET shard_number = shard_number + 1000000 \
             WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await
        .expect_err("shard numbers must not be rebound");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "DELETE FROM pgshard_catalog.shards WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await
        .expect_err("shard identities must be permanent");
    assert_sqlstate(&error, "55000");
    Ok(())
}

async fn assert_admin_privilege_contract(client: &mut Client) -> TestResult {
    let login_roles: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_roles \
             WHERE rolname IN ('pgshard_catalog_reader', 'pgshard_catalog_admin') \
               AND rolcanlogin",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(login_roles, 0, "catalog group roles must remain NOLOGIN");

    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let transaction = client.transaction().await?;
    transaction
        .batch_execute("SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.logical_databases(database_name) VALUES ($1::text)",
            &[&format!("admin_test_{nonce}")],
        )
        .await?;
    let error = transaction
        .execute(
            "UPDATE pgshard_catalog.cluster_configuration SET hash_seed = hash_seed + 1",
            &[],
        )
        .await
        .expect_err("catalog admin must not mutate immutable configuration directly");
    assert_sqlstate(&error, "42501");
    transaction.rollback().await?;
    Ok(())
}

async fn assert_admin_write_path(client: &mut Client) -> TestResult {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let transaction = client.transaction().await?;
    transaction
        .batch_execute("SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
    let logical_database_id: String = transaction
        .query_one(
            "INSERT INTO pgshard_catalog.logical_databases(database_name) \
             VALUES ($1::text) RETURNING logical_database_id::text",
            &[&format!("admin_route_{nonce}")],
        )
        .await?
        .get(0);
    let shard_number: i64 = transaction
        .query_one(
            "SELECT coalesce(max(shard_number), 0) + 1 FROM pgshard_catalog.shards",
            &[],
        )
        .await?
        .get(0);
    let shard_id = format!("admin-{nonce}");
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.shards(shard_id, shard_number) \
             VALUES ($1::text, $2)",
            &[&shard_id, &shard_number],
        )
        .await?;
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.registered_tables( \
                 logical_database_id, schema_name, table_name, shard_key_column, shard_key_type \
             ) VALUES ($1::text::uuid, 'public', 'events', 'tenant_id', 'bigint')",
            &[&logical_database_id],
        )
        .await?;
    let routing_epoch: i64 = transaction
        .query_one(
            "INSERT INTO pgshard_catalog.routing_epochs(logical_database_id) \
             VALUES ($1::text::uuid) RETURNING routing_epoch",
            &[&logical_database_id],
        )
        .await?
        .get(0);
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges \
             (routing_epoch, range_start, range_end, shard_id) \
             VALUES ($1, 0, 18446744073709551616, $2::text)",
            &[&routing_epoch, &shard_id],
        )
        .await?;
    let observed_catalog_epoch: i64 = transaction
        .query_one(
            "SELECT catalog_epoch FROM pgshard_catalog.cluster_state WHERE singleton",
            &[],
        )
        .await?
        .get(0);
    transaction
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &logical_database_id,
                &routing_epoch,
                &Option::<i64>::None,
                &observed_catalog_epoch,
            ],
        )
        .await?;
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.operation_tombstones( \
                 operation_kind, operation_id, request_fingerprint, outcome_code \
             ) VALUES ($1::text, gen_random_uuid(), decode(repeat('00', 32), 'hex'), 'ok')",
            &[&format!("admin_{nonce}")],
        )
        .await?;
    transaction.rollback().await?;
    Ok(())
}

async fn assert_catalog_reader_rejects_existing_transaction(
    client: &Client,
    database_url: &str,
) -> TestResult {
    let (probe_client, probe_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let probe_connection_task = tokio::spawn(probe_connection);
    let database_name = format!(
        "reader_probe_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    probe_client.batch_execute("BEGIN").await?;
    probe_client
        .execute(
            "INSERT INTO pgshard_catalog.logical_databases(database_name) VALUES ($1::text)",
            &[&database_name],
        )
        .await?;

    let error = match CatalogReader::subscribe(probe_client, &CatalogCache::new()).await {
        Err(LoadError::Postgres(error)) => error,
        Err(error) => return Err(format!("unexpected catalog reader error: {error}").into()),
        Ok(_) => return Err("catalog reader accepted a manually opened transaction".into()),
    };
    assert_sqlstate(&error, "25001");
    tokio::time::timeout(Duration::from_secs(5), probe_connection_task).await???;

    let committed: i64 = client
        .query_one(
            "SELECT count(*) FROM pgshard_catalog.logical_databases WHERE database_name = $1",
            &[&database_name],
        )
        .await?
        .get(0);
    assert_eq!(committed, 0, "reader startup committed caller work");
    Ok(())
}

async fn assert_initial_catalog_reader_contract(client: &Client, database_url: &str) -> TestResult {
    let (reader_client, reader_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let reader_connection_task = tokio::spawn(reader_connection);
    let cache = CatalogCache::new();
    let (mut reader, outcome) = CatalogReader::subscribe(reader_client, &cache).await?;
    assert_eq!(outcome, InstallOutcome::Installed);
    assert_eq!(
        i64::try_from(cache.current_for_planning()?.catalog_epoch().0)?,
        catalog_epoch(client).await?
    );
    assert_eq!(
        reader.refresh(&cache).await?,
        InstallOutcome::AlreadyCurrent
    );
    drop(reader);
    reader_connection_task.abort();
    Ok(())
}

async fn assert_registered_table_contract(client: &Client, fixture: &Fixture) -> TestResult {
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.registered_tables( \
                 logical_database_id, schema_name, table_name, shard_key_column, \
                 shard_key_type, shard_key_encoding, shard_key_collation \
             ) VALUES ($1::text::uuid, 'public', 'bad_text_key', 'tenant_id', 'text', 'UTF8', 'en_US')",
            &[&fixture.logical_database_id],
        )
        .await
        .expect_err("locale-dependent text routing must be rejected");
    assert_sqlstate(&error, "23514");
    client
        .execute(
            "INSERT INTO pgshard_catalog.registered_tables( \
                 logical_database_id, schema_name, table_name, shard_key_column, \
                 shard_key_type, shard_key_encoding, shard_key_collation \
             ) VALUES ($1::text::uuid, 'public', 'events', 'tenant_id', 'text', 'UTF8', 'C')",
            &[&fixture.logical_database_id],
        )
        .await?;
    Ok(())
}

async fn assert_tombstone_contract(client: &Client, fixture: &Fixture) -> TestResult {
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.operation_tombstones( \
                 operation_kind, operation_id, request_fingerprint, outcome_code \
             ) VALUES ('test', gen_random_uuid(), decode('00', 'hex'), 'ok')",
            &[],
        )
        .await
        .expect_err("operation fingerprints must be fixed-size");
    assert_sqlstate(&error, "23514");

    let operation_kind = format!("test_{}", fixture.nonce);
    client
        .execute(
            "INSERT INTO pgshard_catalog.operation_tombstones( \
                 operation_kind, operation_id, request_fingerprint, outcome_code \
             ) VALUES ($1::text, gen_random_uuid(), decode(repeat('00', 32), 'hex'), 'ok')",
            &[&operation_kind],
        )
        .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.operation_tombstones \
             SET outcome_code = 'changed' WHERE operation_kind = $1::text",
            &[&operation_kind],
        )
        .await
        .expect_err("operation tombstones must be permanent");
    assert_sqlstate(&error, "55000");
    Ok(())
}

async fn assert_invalid_routing_contracts(
    client: &Client,
    fixture: &Fixture,
) -> TestResult<RoutingFixture> {
    let expected_none: Option<i64> = None;
    let gap_epoch = stage_epoch(client, &fixture.logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges \
             (routing_epoch, range_start, range_end, shard_id) VALUES \
             ($1, 0, 10, 'shard-0000'), ($1, 11, 18446744073709551616, $2::text)",
            &[&gap_epoch, &fixture.shard_id],
        )
        .await?;
    let current_catalog_epoch = catalog_epoch(client).await?;
    let error = client
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &fixture.logical_database_id,
                &gap_epoch,
                &expected_none,
                &current_catalog_epoch,
            ],
        )
        .await
        .expect_err("a gap must prevent activation");
    assert_sqlstate(&error, "22023");

    let overlap_epoch = stage_epoch(client, &fixture.logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges \
             (routing_epoch, range_start, range_end, shard_id) VALUES \
             ($1, 0, 11, 'shard-0000'), ($1, 10, 18446744073709551616, $2::text)",
            &[&overlap_epoch, &fixture.shard_id],
        )
        .await?;
    let error = client
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &fixture.logical_database_id,
                &overlap_epoch,
                &expected_none,
                &current_catalog_epoch,
            ],
        )
        .await
        .expect_err("an overlap must prevent activation");
    assert_sqlstate(&error, "22023");

    let valid_epoch = stage_epoch(client, &fixture.logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges \
             (routing_epoch, range_start, range_end, shard_id) VALUES \
             ($1, 0, 9223372036854775808, 'shard-0000'), \
             ($1, 9223372036854775808, 18446744073709551616, $2::text)",
            &[&valid_epoch, &fixture.shard_id],
        )
        .await?;
    assert_cas_failures(
        client,
        &fixture.logical_database_id,
        valid_epoch,
        current_catalog_epoch,
    )
    .await?;
    Ok(RoutingFixture {
        catalog_epoch: current_catalog_epoch,
        valid_epoch,
    })
}

async fn assert_cas_failures(
    client: &Client,
    logical_database_id: &str,
    valid_epoch: i64,
    current_catalog_epoch: i64,
) -> TestResult {
    let expected_none: Option<i64> = None;
    let error = client
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &logical_database_id,
                &valid_epoch,
                &expected_none,
                &(current_catalog_epoch + 1),
            ],
        )
        .await
        .expect_err("a stale catalog epoch must fail closed");
    assert_sqlstate(&error, "40001");
    let error = client
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &logical_database_id,
                &valid_epoch,
                &Some(valid_epoch + 1_000_000),
                &current_catalog_epoch,
            ],
        )
        .await
        .expect_err("a stale active routing epoch must fail closed");
    assert_sqlstate(&error, "40001");
    Ok(())
}

async fn connect_listener(database_url: &str) -> TestResult<CatalogListener> {
    let (client, mut connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let (sender, receiver) = mpsc::channel();
    let task = tokio::spawn(async move {
        while let Some(message) = poll_fn(|context| connection.poll_message(context)).await {
            match message {
                Ok(AsyncMessage::Notification(notification)) => {
                    if sender.send(notification.payload().to_owned()).is_err() {
                        return Ok(());
                    }
                }
                Ok(_) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    });
    client
        .batch_execute(&format!("LISTEN {}", pgshard_catalog::NOTIFY_CHANNEL))
        .await?;
    Ok(CatalogListener {
        _client: client,
        receiver,
        task,
    })
}

async fn commit_valid_activation(
    client: &mut Client,
    listener: &CatalogListener,
    fixture: &Fixture,
    routing: &RoutingFixture,
) -> TestResult<i64> {
    let transaction = client.transaction().await?;
    let activated_catalog_epoch = transaction
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &fixture.logical_database_id,
                &routing.valid_epoch,
                &Option::<i64>::None,
                &routing.catalog_epoch,
            ],
        )
        .await?
        .get(0);
    assert_no_notification(
        &listener.receiver,
        "NOTIFY must not become visible before commit",
    );
    transaction.commit().await?;
    wait_for_epoch(&listener.receiver, activated_catalog_epoch)?;

    let active_epoch: i64 = client
        .query_one(
            "SELECT routing_epoch FROM pgshard_catalog.active_routing_epochs \
             WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id],
        )
        .await?
        .get(0);
    assert_eq!(active_epoch, routing.valid_epoch);
    let error = client
        .execute(
            "UPDATE pgshard_catalog.routing_ranges SET range_end = range_end - 1 \
             WHERE routing_epoch = $1 AND range_start = 0",
            &[&routing.valid_epoch],
        )
        .await
        .expect_err("activated routing ranges must be immutable");
    assert_sqlstate(&error, "55000");

    let error = client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'retired' WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await
        .expect_err("a shard in active routing cannot be retired");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_databases SET state = 'retired' \
             WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id],
        )
        .await
        .expect_err("a logical database with active routing cannot be retired");
    assert_sqlstate(&error, "55000");
    Ok(activated_catalog_epoch)
}

async fn assert_loader_contract(
    client: &mut Client,
    database_url: &str,
    listener: &CatalogListener,
    fixture: &Fixture,
    routing_epoch: i64,
) -> TestResult<i64> {
    let initial_epoch = catalog_epoch(client).await?;
    let notification_driver = CatalogDriver::start(
        database_url,
        CatalogPollInterval::new(Duration::from_secs(30))?,
    )
    .await?;
    wait_for_cache_epoch(&notification_driver.cache, initial_epoch).await?;
    let snapshot = notification_driver.cache.current_for_planning()?;
    let database_id = DatabaseId::new(Uuid::parse_str(&fixture.logical_database_id)?)?;
    let database = snapshot
        .database(database_id)
        .ok_or("loaded snapshot omitted active logical database")?;
    assert_eq!(database.epochs().routing().0, u64::try_from(routing_epoch)?);
    assert_eq!(database.routes().len(), 2);
    assert!(
        database
            .table(&pgshard_catalog::TableName::new("public", "events")?)
            .is_some()
    );
    let database_name = format!("loader_notify_{}", fixture.nonce);
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_databases(database_name) VALUES ($1::text)",
            &[&database_name],
        )
        .await?;
    let notified_epoch = catalog_epoch(client).await?;
    wait_for_epoch(&listener.receiver, notified_epoch)?;
    wait_for_cache_epoch(&notification_driver.cache, notified_epoch).await?;

    client
        .execute(
            "SELECT pg_catalog.pg_notify($1, $2)",
            &[&pgshard_catalog::NOTIFY_CHANNEL, &"invalid"],
        )
        .await?;
    wait_for_payload(&listener.receiver, "invalid")?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !notification_driver.task.is_finished(),
        "invalid notification terminated the refresh driver"
    );
    notification_driver.shutdown().await?;

    let polling_driver = CatalogDriver::start(
        database_url,
        CatalogPollInterval::new(Duration::from_secs(1))?,
    )
    .await?;
    wait_for_cache_epoch(&polling_driver.cache, notified_epoch).await?;

    let transaction = client.transaction().await?;
    transaction
        .batch_execute("SET LOCAL session_replication_role = replica")
        .await?;
    let missed_epoch: i64 = transaction
        .query_one(
            "UPDATE pgshard_catalog.cluster_state \
             SET catalog_epoch = catalog_epoch + 1 \
             WHERE singleton RETURNING catalog_epoch",
            &[],
        )
        .await?
        .get(0);
    transaction.commit().await?;
    assert_no_notification(
        &listener.receiver,
        "simulated missed catalog notification was unexpectedly delivered",
    );
    wait_for_cache_epoch(&polling_driver.cache, missed_epoch).await?;
    polling_driver.shutdown().await?;
    assert_catalog_driver_connection_loss(client, database_url, missed_epoch).await?;
    assert_catalog_supervisor_reconnects_with_bounded_grace(client, database_url, missed_epoch)
        .await?;
    Ok(missed_epoch)
}

async fn wait_for_cache_epoch(cache: &CatalogCache, expected: i64) -> TestResult {
    let expected = u64::try_from(expected)?;
    for _ in 0..250 {
        if cache
            .current_for_planning()
            .is_ok_and(|snapshot| snapshot.catalog_epoch().0 == expected)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(format!("catalog cache did not reach epoch {expected} within five seconds").into())
}

async fn assert_shutdown_interrupts_initial_load(
    client: &mut Client,
    database_url: &str,
) -> TestResult {
    let (observer, observer_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let observer_task = tokio::spawn(observer_connection);
    let application_name = format!(
        "pgshard_stop_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let mut driver_config: tokio_postgres::Config = database_url.parse()?;
    driver_config.application_name(&application_name);
    let (driver_client, driver_connection) = driver_config.connect(NoTls).await?;
    let cache = Arc::new(CatalogCache::new());
    let driver_cache = Arc::clone(&cache);
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();

    let blocker = client.transaction().await?;
    blocker
        .batch_execute("LOCK TABLE pgshard_catalog.cluster_configuration IN ACCESS EXCLUSIVE MODE")
        .await?;
    let driver_task = tokio::spawn(run_catalog_refresh(
        driver_client,
        driver_connection,
        driver_cache,
        CatalogPollInterval::new(Duration::from_secs(30))?,
        async move {
            let _ = shutdown_receiver.await;
        },
    ));
    let driver_pid = wait_for_application_backend(&observer, &application_name)
        .await?
        .ok_or("catalog driver backend did not connect within two seconds")?;
    if !wait_for_backend_lock(&observer, driver_pid).await? {
        return Err("catalog driver initial load did not block on the fixture lock".into());
    }

    shutdown_sender
        .send(())
        .map_err(|()| "blocked catalog driver dropped its shutdown receiver")?;
    tokio::time::timeout(Duration::from_secs(5), driver_task).await???;
    assert!(
        cache.current_for_planning().is_err(),
        "blocked initial load published a catalog snapshot"
    );
    blocker.rollback().await?;
    assert!(
        wait_for_backend_exit(&observer, driver_pid).await?,
        "catalog driver backend survived shutdown"
    );

    observer_task.abort();
    Ok(())
}

async fn assert_catalog_driver_connection_loss(
    client: &Client,
    database_url: &str,
    expected_epoch: i64,
) -> TestResult {
    let application_name = format!(
        "pgshard_catalog_loss_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let mut driver_config: tokio_postgres::Config = database_url.parse()?;
    driver_config.application_name(&application_name);
    let (driver_client, driver_connection) = driver_config.connect(NoTls).await?;
    let cache = Arc::new(CatalogCache::new());
    let driver_cache = Arc::clone(&cache);
    let driver_task = tokio::spawn(run_catalog_refresh(
        driver_client,
        driver_connection,
        driver_cache,
        CatalogPollInterval::new(Duration::from_secs(1))?,
        pending(),
    ));
    wait_for_cache_epoch(&cache, expected_epoch).await?;

    let driver_pid: i32 = client
        .query_one(
            "SELECT pid FROM pg_catalog.pg_stat_activity \
             WHERE datname = current_database() AND application_name = $1",
            &[&application_name],
        )
        .await?
        .get(0);

    let terminated: bool = client
        .query_one("SELECT pg_terminate_backend($1)", &[&driver_pid])
        .await?
        .get(0);
    assert!(terminated, "catalog driver backend was not terminated");
    let result = tokio::time::timeout(Duration::from_secs(5), driver_task).await??;
    assert!(
        result.is_err(),
        "catalog driver silently survived connection loss"
    );
    Ok(())
}

async fn wait_for_supervisor_status(
    status: &CatalogSupervisorStatus,
    description: &str,
    predicate: impl Fn(CatalogSupervisorSnapshot) -> bool,
) -> TestResult<CatalogSupervisorSnapshot> {
    for _ in 0..250 {
        let snapshot = status.snapshot();
        if predicate(snapshot) {
            return Ok(snapshot);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(format!("catalog supervisor did not become {description} within five seconds").into())
}

async fn catalog_backend_pid(client: &Client, application_name: &str) -> TestResult<i32> {
    Ok(client
        .query_one(
            "SELECT pid FROM pg_catalog.pg_stat_activity \
             WHERE datname = current_database() AND application_name = $1",
            &[&application_name],
        )
        .await?
        .get(0))
}

async fn terminate_catalog_backend(client: &Client, application_name: &str) -> TestResult {
    let pid = catalog_backend_pid(client, application_name).await?;
    let terminated: bool = client
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await?
        .get(0);
    if !terminated {
        return Err(format!("catalog supervisor backend {pid} was not terminated").into());
    }
    Ok(())
}

async fn assert_catalog_supervisor_reconnects_with_bounded_grace(
    client: &Client,
    database_url: &str,
    expected_epoch: i64,
) -> TestResult {
    let application_name = format!(
        "pgshard_catalog_supervisor_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let mut connection_config: tokio_postgres::Config = database_url.parse()?;
    connection_config.application_name(&application_name);
    let mut unavailable_config = tokio_postgres::Config::new();
    unavailable_config
        .host("127.0.0.1")
        .port(1)
        .user("pgshard_unavailable")
        .dbname("shardschema")
        .connect_timeout(Duration::from_millis(100));
    let config = CatalogSupervisorConfig::new(
        CatalogPollInterval::new(Duration::from_secs(1))?,
        Duration::from_secs(2),
        Duration::from_millis(10),
        Duration::from_millis(20),
    )?;
    let cache = Arc::new(CatalogCache::new());
    let supervisor = CatalogSupervisor::new(cache, config);
    let status = supervisor.status();
    let attempts = Arc::new(AtomicUsize::new(0));
    let reconnect_gate = Arc::new(tokio::sync::Notify::new());
    let connector_attempts = Arc::clone(&attempts);
    let connector_gate = Arc::clone(&reconnect_gate);
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(supervisor.run(
        move || {
            let attempt = connector_attempts.fetch_add(1, Ordering::SeqCst);
            let reconnect_gate = Arc::clone(&connector_gate);
            let connection_config = if attempt == 0 {
                unavailable_config.clone()
            } else {
                connection_config.clone()
            };
            async move {
                if attempt > 1 {
                    reconnect_gate.notified().await;
                }
                connection_config.connect(NoTls).await
            }
        },
        async move {
            let _ = shutdown_receiver.await;
        },
    ));

    assert_catalog_supervisor_lifecycle(
        client,
        &application_name,
        expected_epoch,
        &status,
        &reconnect_gate,
        shutdown_sender,
        task,
    )
    .await
}

async fn assert_catalog_supervisor_lifecycle(
    client: &Client,
    application_name: &str,
    expected_epoch: i64,
    status: &CatalogSupervisorStatus,
    reconnect_gate: &tokio::sync::Notify,
    shutdown_sender: tokio::sync::oneshot::Sender<()>,
    task: JoinHandle<()>,
) -> TestResult {
    let initial = wait_for_supervisor_status(status, "initially ready", |snapshot| {
        snapshot.ready() && snapshot.phase() == CatalogConnectionPhase::Connected
    })
    .await?;
    assert_eq!(initial.total_failures(), 1);
    assert_eq!(initial.consecutive_failures(), 0);
    assert_eq!(
        initial.catalog_epoch().map(|epoch| epoch.0),
        Some(u64::try_from(expected_epoch)?)
    );

    terminate_catalog_backend(client, application_name).await?;
    let reconnecting = wait_for_supervisor_status(status, "reconnecting", |snapshot| {
        snapshot.connect_attempts() >= 3 && snapshot.phase() == CatalogConnectionPhase::Connecting
    })
    .await?;
    assert!(
        reconnecting.ready(),
        "a recent validated cache should survive a brief catalog outage"
    );
    assert_eq!(
        reconnecting.readiness_reason(),
        CatalogReadinessReason::ServingStale
    );

    let expired = wait_for_supervisor_status(status, "stale", |snapshot| {
        snapshot.readiness_reason() == CatalogReadinessReason::Stale
    })
    .await?;
    assert!(!expired.ready());
    assert!(
        expired
            .cache_age()
            .is_some_and(|age| age >= Duration::from_secs(2)),
        "stale cache age did not reach its configured deadline"
    );

    reconnect_gate.notify_one();
    let recovered = wait_for_supervisor_status(status, "recovered", |snapshot| {
        snapshot.ready()
            && snapshot.phase() == CatalogConnectionPhase::Connected
            && snapshot.successful_connections() >= 2
    })
    .await?;
    assert_eq!(recovered.consecutive_failures(), 0);
    assert_eq!(recovered.last_failure(), None);

    terminate_catalog_backend(client, application_name).await?;
    wait_for_supervisor_status(status, "blocked in a second reconnect", |snapshot| {
        snapshot.connect_attempts() >= 4 && snapshot.phase() == CatalogConnectionPhase::Connecting
    })
    .await?;
    shutdown_sender
        .send(())
        .map_err(|()| "catalog supervisor dropped its shutdown receiver")?;
    tokio::time::timeout(Duration::from_secs(5), task).await??;
    assert_eq!(
        status.snapshot().readiness_reason(),
        CatalogReadinessReason::Stopped
    );
    Ok(())
}

async fn assert_rollback_contract(
    client: &mut Client,
    listener: &CatalogListener,
    fixture: &Fixture,
    routing: &RoutingFixture,
    activated_catalog_epoch: i64,
) -> TestResult {
    let rollback_epoch = stage_epoch(client, &fixture.logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges \
             (routing_epoch, range_start, range_end, shard_id) \
             VALUES ($1, 0, 18446744073709551616, 'shard-0000')",
            &[&rollback_epoch],
        )
        .await?;
    let transaction = client.transaction().await?;
    transaction
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &fixture.logical_database_id,
                &rollback_epoch,
                &Some(routing.valid_epoch),
                &activated_catalog_epoch,
            ],
        )
        .await?;
    assert_no_notification(&listener.receiver, "uncommitted activation notified");
    transaction.rollback().await?;
    assert_no_notification(&listener.receiver, "rolled-back activation notified");

    let rows = client
        .query(
            "SELECT routing_epoch, state FROM pgshard_catalog.routing_epochs \
             WHERE routing_epoch IN ($1, $2) ORDER BY routing_epoch",
            &[&routing.valid_epoch, &rollback_epoch],
        )
        .await?;
    assert_eq!(rows[0].get::<_, i64>(0), routing.valid_epoch);
    assert_eq!(rows[0].get::<_, &str>(1), "active");
    assert_eq!(rows[1].get::<_, i64>(0), rollback_epoch);
    assert_eq!(rows[1].get::<_, &str>(1), "staged");
    Ok(())
}

async fn stage_full_epoch(client: &Client, fixture: &Fixture) -> Result<i64, PgError> {
    let routing_epoch = stage_epoch(client, &fixture.logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges \
             (routing_epoch, range_start, range_end, shard_id) \
             VALUES ($1, 0, 18446744073709551616, $2::text)",
            &[&routing_epoch, &fixture.shard_id],
        )
        .await?;
    Ok(routing_epoch)
}

async fn assert_routing_epoch_cannot_regress(
    client: &Client,
    listener: &CatalogListener,
    fixture: &Fixture,
    initial_active_epoch: i64,
) -> TestResult {
    let older_epoch = stage_full_epoch(client, fixture).await?;
    let newer_epoch = stage_full_epoch(client, fixture).await?;
    assert!(newer_epoch > older_epoch && older_epoch > initial_active_epoch);

    let before_activation = catalog_epoch(client).await?;
    let newer_catalog_epoch: i64 = client
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &fixture.logical_database_id,
                &newer_epoch,
                &Some(initial_active_epoch),
                &before_activation,
            ],
        )
        .await?
        .get(0);
    wait_for_epoch(&listener.receiver, newer_catalog_epoch)?;

    let error = client
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &fixture.logical_database_id,
                &older_epoch,
                &Some(newer_epoch),
                &newer_catalog_epoch,
            ],
        )
        .await
        .expect_err("an older staged routing epoch must never replace a newer active epoch");
    assert_sqlstate(&error, "40001");
    assert_no_notification(
        &listener.receiver,
        "rejected routing regression emitted a notification",
    );
    assert_eq!(catalog_epoch(client).await?, newer_catalog_epoch);
    let active_epoch: i64 = client
        .query_one(
            "SELECT routing_epoch FROM pgshard_catalog.active_routing_epochs \
             WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id],
        )
        .await?
        .get(0);
    assert_eq!(active_epoch, newer_epoch);
    let states = client
        .query(
            "SELECT routing_epoch, state FROM pgshard_catalog.routing_epochs \
             WHERE routing_epoch IN ($1, $2) ORDER BY routing_epoch",
            &[&older_epoch, &newer_epoch],
        )
        .await?;
    assert_eq!(states[0].get::<_, i64>(0), older_epoch);
    assert_eq!(states[0].get::<_, &str>(1), "staged");
    assert_eq!(states[1].get::<_, i64>(0), newer_epoch);
    assert_eq!(states[1].get::<_, &str>(1), "active");
    Ok(())
}

async fn run_repeatable_read_activation(
    mut client: Client,
    logical_database_id: String,
    target_epoch: i64,
    active_epoch: i64,
    expected_catalog_epoch: i64,
) -> Result<Option<PgError>, PgError> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let result = transaction
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &logical_database_id,
                &target_epoch,
                &Some(active_epoch),
                &expected_catalog_epoch,
            ],
        )
        .await;
    match result {
        Ok(_) => {
            transaction.commit().await?;
            Ok(None)
        }
        Err(error) => {
            transaction.rollback().await?;
            Ok(Some(error))
        }
    }
}

async fn wait_for_backend_lock(client: &Client, backend_pid: i32) -> Result<bool, PgError> {
    for _ in 0..100 {
        let waiting = client
            .query_opt(
                "SELECT coalesce(wait_event_type = 'Lock', false) \
                 FROM pg_catalog.pg_stat_activity WHERE pid = $1",
                &[&backend_pid],
            )
            .await?
            .is_some_and(|row| row.get(0));
        if waiting {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(false)
}

async fn wait_for_application_backend(
    client: &Client,
    application_name: &str,
) -> Result<Option<i32>, PgError> {
    for _ in 0..100 {
        let backend = client
            .query_opt(
                "SELECT pid FROM pg_catalog.pg_stat_activity \
                 WHERE datname = current_database() AND application_name = $1",
                &[&application_name],
            )
            .await?;
        if let Some(backend) = backend {
            return Ok(Some(backend.get(0)));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(None)
}

async fn wait_for_backend_exit(client: &Client, backend_pid: i32) -> Result<bool, PgError> {
    for _ in 0..250 {
        if client
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = $1",
                &[&backend_pid],
            )
            .await?
            .is_none()
        {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(false)
}

async fn assert_serialization_fence_state(
    client: &Client,
    fixture: &Fixture,
    target_epoch: i64,
    active_epoch: i64,
    expected_catalog_epoch: i64,
) -> TestResult {
    let staged = client
        .query_one(
            "SELECT epochs.state, epochs.range_revision, ranges.range_end::text \
             FROM pgshard_catalog.routing_epochs AS epochs \
             JOIN pgshard_catalog.routing_ranges AS ranges USING (routing_epoch) \
             WHERE epochs.routing_epoch = $1 AND ranges.range_start = 0",
            &[&target_epoch],
        )
        .await?;
    assert_eq!(staged.get::<_, &str>(0), "staged");
    assert!(staged.get::<_, i64>(1) > 0);
    assert_eq!(staged.get::<_, &str>(2), "18446744073709551615");
    let still_active: i64 = client
        .query_one(
            "SELECT routing_epoch FROM pgshard_catalog.active_routing_epochs \
             WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id],
        )
        .await?
        .get(0);
    assert_eq!(still_active, active_epoch);
    assert_eq!(catalog_epoch(client).await?, expected_catalog_epoch);
    Ok(())
}

async fn assert_repeatable_read_activation_fences_concurrent_range_mutation(
    client: &Client,
    database_url: &str,
    listener: &CatalogListener,
    fixture: &Fixture,
) -> TestResult {
    let active_epoch: i64 = client
        .query_one(
            "SELECT routing_epoch FROM pgshard_catalog.active_routing_epochs \
             WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id],
        )
        .await?
        .get(0);
    let target_epoch = stage_full_epoch(client, fixture).await?;
    let expected_catalog_epoch = catalog_epoch(client).await?;

    let (mut mutator_client, mutator_connection) =
        tokio_postgres::connect(database_url, NoTls).await?;
    let mutator_connection_task = tokio::spawn(mutator_connection);
    let mutator = mutator_client.transaction().await?;
    mutator
        .execute(
            "UPDATE pgshard_catalog.routing_ranges \
             SET range_end = 18446744073709551615 \
             WHERE routing_epoch = $1 AND range_start = 0",
            &[&target_epoch],
        )
        .await?;

    let (activation_client, activation_connection) =
        tokio_postgres::connect(database_url, NoTls).await?;
    let activation_connection_task = tokio::spawn(activation_connection);
    let activation_pid: i32 = activation_client
        .query_one("SELECT pg_backend_pid()", &[])
        .await?
        .get(0);
    let activation = tokio::spawn(run_repeatable_read_activation(
        activation_client,
        fixture.logical_database_id.clone(),
        target_epoch,
        active_epoch,
        expected_catalog_epoch,
    ));

    if !wait_for_backend_lock(client, activation_pid).await? {
        activation.abort();
        mutator.rollback().await?;
        mutator_connection_task.abort();
        activation_connection_task.abort();
        return Err("activation did not wait on the concurrently versioned parent epoch".into());
    }

    mutator.commit().await?;
    let activation_error = tokio::time::timeout(Duration::from_secs(5), activation)
        .await???
        .expect("stale REPEATABLE READ activation must fail");
    assert_sqlstate(&activation_error, "40001");
    assert_no_notification(
        &listener.receiver,
        "serialization failure emitted a catalog notification",
    );
    assert_serialization_fence_state(
        client,
        fixture,
        target_epoch,
        active_epoch,
        expected_catalog_epoch,
    )
    .await?;

    mutator_connection_task.abort();
    activation_connection_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing to a PostgreSQL 18 shardschema database"]
async fn migration_and_activation_contract() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);

    assert_installation_contract(&client).await?;
    assert_shutdown_interrupts_initial_load(&mut client, &database_url).await?;
    assert_initial_catalog_reader_contract(&client, &database_url).await?;
    assert_catalog_reader_rejects_existing_transaction(&client, &database_url).await?;
    assert_admin_privilege_contract(&mut client).await?;
    assert_admin_write_path(&mut client).await?;
    let fixture = create_fixture(&client).await?;
    assert_identity_history_contract(&client, &fixture).await?;
    assert_registered_table_contract(&client, &fixture).await?;
    assert_tombstone_contract(&client, &fixture).await?;
    let routing = assert_invalid_routing_contracts(&client, &fixture).await?;
    let listener = connect_listener(&database_url).await?;
    commit_valid_activation(&mut client, &listener, &fixture, &routing).await?;
    let activated_epoch = assert_loader_contract(
        &mut client,
        &database_url,
        &listener,
        &fixture,
        routing.valid_epoch,
    )
    .await?;
    assert_rollback_contract(&mut client, &listener, &fixture, &routing, activated_epoch).await?;
    assert_routing_epoch_cannot_regress(&client, &listener, &fixture, routing.valid_epoch).await?;
    assert_repeatable_read_activation_fences_concurrent_range_mutation(
        &client,
        &database_url,
        &listener,
        &fixture,
    )
    .await?;

    listener.task.abort();
    connection_task.abort();
    Ok(())
}
