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
    CatalogCache, CatalogConnectionPhase, CatalogFailureKind, CatalogOperation,
    CatalogOperationTimeout, CatalogPollInterval, CatalogReader, CatalogReadinessReason,
    CatalogRefreshError, CatalogSupervisor, CatalogSupervisorConfig, CatalogSupervisorSnapshot,
    CatalogSupervisorStatus, DatabaseId, InstallOutcome, LoadError, run_catalog_refresh,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct Fixture {
    logical_database_id: String,
    nonce: u128,
    restore_incarnation: String,
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

struct ReconnectGate {
    enabled: tokio::sync::watch::Sender<bool>,
}

impl ReconnectGate {
    fn new() -> Self {
        let (enabled, _) = tokio::sync::watch::channel(false);
        Self { enabled }
    }

    async fn wait(&self) {
        let mut enabled = self.enabled.subscribe();
        while !*enabled.borrow_and_update() {
            enabled
                .changed()
                .await
                .expect("reconnect gate retains its sender");
        }
    }

    fn enable(&self) {
        self.enabled.send_replace(true);
    }

    fn disable(&self) {
        self.enabled.send_replace(false);
    }
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
            CatalogOperationTimeout::default(),
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

fn assert_database_message(error: &PgError, expected: &str) {
    let actual = error
        .as_db_error()
        .map(tokio_postgres::error::DbError::message);
    assert_eq!(
        actual,
        Some(expected),
        "unexpected PostgreSQL error: {error}"
    );
}

async fn advance_checkpoint(
    client: &Client,
    checkpoint_generation: &str,
    expected_ownership_fence: i64,
    expected_checkpoint_ordinal: i64,
    checkpoint_lsn: &str,
    checkpoint_ordinal: i64,
    snapshot_required: bool,
) -> Result<i64, PgError> {
    Ok(client
        .query_one(
            "SELECT pgshard_catalog.advance_logical_consumer_checkpoint( \
                 $1::text::uuid, $2, $3, $4::text::pg_lsn, $5, $6 \
             )",
            &[
                &checkpoint_generation,
                &expected_ownership_fence,
                &expected_checkpoint_ordinal,
                &checkpoint_lsn,
                &checkpoint_ordinal,
                &snapshot_required,
            ],
        )
        .await?
        .get(0))
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

    let unsafe_security_definers: i64 = client
        .query_one(
            "SELECT count(*) \
             FROM pg_catalog.pg_proc AS procedures \
             JOIN pg_catalog.pg_namespace AS namespaces \
               ON namespaces.oid = procedures.pronamespace \
             WHERE namespaces.nspname = 'pgshard_catalog' \
               AND procedures.prosecdef \
               AND NOT coalesce( \
                   procedures.proconfig @> \
                       ARRAY['search_path=pg_catalog, pgshard_catalog, pg_temp'], \
                   false \
               )",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        unsafe_security_definers, 0,
        "security-definer functions must place pg_temp last"
    );

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
    let restore_incarnation: String = client
        .query_one(
            "SELECT restore_incarnation::text \
             FROM pgshard_catalog.shard_restore_incarnations \
             WHERE shard_id = $1::text AND state = 'active'",
            &[&shard_id],
        )
        .await?
        .get(0);
    Ok(Fixture {
        logical_database_id,
        nonce,
        restore_incarnation,
        shard_id,
    })
}

async fn insert_slot_sync_probe(
    client: &Client,
    fixture: &Fixture,
    probe_generation: Uuid,
    restore_incarnation: &str,
    slot_name: &str,
) -> Result<u64, PgError> {
    client
        .execute(
            "INSERT INTO pgshard_catalog.slot_sync_probes( \
                 probe_generation, shard_id, restore_incarnation, system_identifier, \
                 database_oid, database_name, source_timeline, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text, $3::text::uuid, 18446744073709551615, \
                 4294967295, 'postgres', 4294967295, $4::text \
             )",
            &[
                &probe_generation.to_string(),
                &fixture.shard_id,
                &restore_incarnation,
                &slot_name,
            ],
        )
        .await
}

struct SlotSyncProbeFixture {
    generation: Uuid,
    replacement_generation: Uuid,
    replacement_name: String,
}

async fn allocate_slot_sync_probe_fixture(
    client: &Client,
    fixture: &Fixture,
) -> TestResult<SlotSyncProbeFixture> {
    let probe_generation = fixture_uuid(fixture.nonce, 90);
    let probe_name = format!("sync_probe_{}", probe_generation.simple());
    let wrong_name = format!("sync_probe_{}", fixture_uuid(fixture.nonce, 91).simple());
    let error = insert_slot_sync_probe(
        client,
        fixture,
        probe_generation,
        &fixture.restore_incarnation,
        &wrong_name,
    )
    .await
    .expect_err("a slot-sync probe name must encode its complete generation");
    assert_sqlstate(&error, "23514");

    let unknown_restore = fixture_uuid(fixture.nonce, 92).to_string();
    let error = insert_slot_sync_probe(
        client,
        fixture,
        probe_generation,
        &unknown_restore,
        &probe_name,
    )
    .await
    .expect_err("a probe cannot invent its shard restore lineage");
    assert_sqlstate(&error, "55000");

    insert_slot_sync_probe(
        client,
        fixture,
        probe_generation,
        &fixture.restore_incarnation,
        &probe_name,
    )
    .await?;

    let replacement_generation = fixture_uuid(fixture.nonce, 93);
    let replacement_name = format!("sync_probe_{}", replacement_generation.simple());
    let error = insert_slot_sync_probe(
        client,
        fixture,
        replacement_generation,
        &fixture.restore_incarnation,
        &replacement_name,
    )
    .await
    .expect_err("one shard cannot have two live slot-sync probes");
    assert_sqlstate(&error, "23505");

    Ok(SlotSyncProbeFixture {
        generation: probe_generation,
        replacement_generation,
        replacement_name,
    })
}

async fn assert_slot_sync_probe_activation(
    client: &Client,
    fixture: &Fixture,
    probe: &SlotSyncProbeFixture,
) -> TestResult {
    let error = client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes SET state = 'active' \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await
        .expect_err("probe activation requires its PostgreSQL consistent point");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes SET source_timeline = 1 \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await
        .expect_err("probe source identity must remain immutable");
    assert_sqlstate(&error, "55000");

    client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'active', consistent_point = '0/10', \
                 activated_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes SET consistent_point = '0/20' \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await
        .expect_err("an active probe cannot rewrite its creation boundary");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retired', retiring_at = statement_timestamp(), \
                 retired_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await
        .expect_err("an active probe must enter cleanup before retirement");
    assert_sqlstate(&error, "55000");

    let error = client
        .execute(
            "UPDATE pgshard_catalog.shard_restore_incarnations \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE restore_incarnation = $1::text::uuid",
            &[&fixture.restore_incarnation],
        )
        .await
        .expect_err("a restore cannot retire while its probe may exist");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "restore incarnation retains a non-retired slot-sync probe",
    );
    client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'draining' WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'retired' WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await
        .expect_err("a shard cannot retire while its probe may exist");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        &format!(
            "shard {} still has a non-retired slot-sync probe",
            fixture.shard_id
        ),
    );
    client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'active' WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await?;

    Ok(())
}

async fn assert_slot_sync_probe_retirement(
    client: &Client,
    fixture: &Fixture,
    probe: &SlotSyncProbeFixture,
) -> TestResult {
    client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retiring', retiring_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await?;
    let error = insert_slot_sync_probe(
        client,
        fixture,
        probe.replacement_generation,
        &fixture.restore_incarnation,
        &probe.replacement_name,
    )
    .await
    .expect_err("cleanup must finish before a replacement probe is allocated");
    assert_sqlstate(&error, "23505");
    client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await?;
    let error = client
        .execute(
            "DELETE FROM pgshard_catalog.slot_sync_probes \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await
        .expect_err("probe generation tombstones must be permanent");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET retired_at = retired_at + interval '1 second' \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await
        .expect_err("retired probe history must be immutable");
    assert_sqlstate(&error, "55000");

    insert_slot_sync_probe(
        client,
        fixture,
        probe.replacement_generation,
        &fixture.restore_incarnation,
        &probe.replacement_name,
    )
    .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retiring', retiring_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.replacement_generation.to_string()],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.replacement_generation.to_string()],
        )
        .await?;
    Ok(())
}

async fn assert_slot_sync_probe_contract(client: &Client, fixture: &Fixture) -> TestResult {
    let probe = allocate_slot_sync_probe_fixture(client, fixture).await?;
    assert_slot_sync_probe_activation(client, fixture, &probe).await?;
    assert_slot_sync_probe_retirement(client, fixture, &probe).await
}

async fn assert_migration_does_not_resurrect_retired_restore(
    client: &Client,
    nonce: u128,
) -> TestResult {
    let shard_id = format!("replay-{nonce}");
    let shard_number: i64 = client
        .query_one(
            "SELECT coalesce(max(shard_number), 0) + 1 FROM pgshard_catalog.shards",
            &[],
        )
        .await?
        .get(0);
    client
        .execute(
            "INSERT INTO pgshard_catalog.shards(shard_id, shard_number) \
             VALUES ($1::text, $2)",
            &[&shard_id, &shard_number],
        )
        .await?;
    let restore_incarnation: String = client
        .query_one(
            "SELECT restore_incarnation::text \
             FROM pgshard_catalog.shard_restore_incarnations \
             WHERE shard_id = $1::text AND state = 'active'",
            &[&shard_id],
        )
        .await?
        .get(0);
    client
        .execute(
            "UPDATE pgshard_catalog.shard_restore_incarnations \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE restore_incarnation = $1::text::uuid",
            &[&restore_incarnation],
        )
        .await?;
    let epoch_before_replay = catalog_epoch(client).await?;
    client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;
    assert_eq!(
        catalog_epoch(client).await?,
        epoch_before_replay,
        "migration replay mutated catalog state after restore retirement"
    );
    let row = client
        .query_one(
            "SELECT \
                 count(*) FILTER (WHERE state = 'active'), \
                 count(*) FILTER ( \
                     WHERE restore_incarnation = $2::text::uuid AND state = 'retired' \
                 ) \
             FROM pgshard_catalog.shard_restore_incarnations \
             WHERE shard_id = $1::text",
            &[&shard_id, &restore_incarnation],
        )
        .await?;
    let active_count: i64 = row.get(0);
    let retired_count: i64 = row.get(1);
    assert_eq!(
        active_count, 0,
        "migration replay resurrected retired WAL history"
    );
    assert_eq!(retired_count, 1, "migration replay rewrote restore history");
    Ok(())
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
    let restore_incarnation: String = transaction
        .query_one(
            "SELECT restore_incarnation::text \
             FROM pgshard_catalog.shard_restore_incarnations \
             WHERE shard_id = $1::text AND state = 'active'",
            &[&shard_id],
        )
        .await?
        .get(0);
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.registered_tables( \
                 logical_database_id, schema_name, table_name, shard_key_column, shard_key_type \
             ) VALUES ($1::text::uuid, 'public', 'events', 'tenant_id', 'bigint')",
            &[&logical_database_id],
        )
        .await?;
    assert_admin_slot_sync_probe_write_path(&transaction, nonce, &shard_id, &restore_incarnation)
        .await?;
    assert_admin_consumer_write_path(
        &transaction,
        nonce,
        &logical_database_id,
        &shard_id,
        &restore_incarnation,
    )
    .await?;
    assert_admin_routing_write_path(&transaction, nonce, &logical_database_id, &shard_id).await?;
    transaction.rollback().await?;
    Ok(())
}

async fn assert_admin_routing_write_path(
    transaction: &tokio_postgres::Transaction<'_>,
    nonce: u128,
    logical_database_id: &str,
    shard_id: &str,
) -> TestResult {
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
    Ok(())
}

async fn assert_admin_slot_sync_probe_write_path(
    transaction: &tokio_postgres::Transaction<'_>,
    nonce: u128,
    shard_id: &str,
    restore_incarnation: &str,
) -> TestResult {
    let probe_generation = fixture_uuid(nonce, 100);
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.slot_sync_probes( \
                 probe_generation, shard_id, restore_incarnation, system_identifier, \
                 database_oid, database_name, source_timeline, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text, $3::text::uuid, 1, 1, \
                 'admin_database', 1, $4::text \
             )",
            &[
                &probe_generation.to_string(),
                &shard_id,
                &restore_incarnation,
                &format!("sync_probe_{}", probe_generation.simple()),
            ],
        )
        .await?;
    transaction
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'active', consistent_point = '0/1', \
                 activated_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe_generation.to_string()],
        )
        .await?;
    transaction
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retiring', retiring_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe_generation.to_string()],
        )
        .await?;
    transaction
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe_generation.to_string()],
        )
        .await?;
    Ok(())
}

async fn assert_admin_consumer_write_path(
    transaction: &tokio_postgres::Transaction<'_>,
    nonce: u128,
    logical_database_id: &str,
    shard_id: &str,
    restore_incarnation: &str,
) -> TestResult {
    let checkpoint_generation = fixture_uuid(nonce, 101).to_string();
    let consumer_id: String = transaction
        .query_one(
            "INSERT INTO pgshard_catalog.logical_consumers( \
                 logical_database_id, consumer_name, purpose \
             ) VALUES ($1::text::uuid, $2::text, 'change-stream') \
             RETURNING consumer_id::text",
            &[&logical_database_id, &format!("admin-stream-{nonce}")],
        )
        .await?
        .get(0);
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_shards( \
                 consumer_id, logical_database_id, shard_id \
             ) VALUES ($1::text::uuid, $2::text::uuid, $3::text)",
            &[&consumer_id, &logical_database_id, &shard_id],
        )
        .await?;
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_checkpoints( \
                 checkpoint_generation, consumer_id, logical_database_id, shard_id, \
                 restore_incarnation, system_identifier, database_oid, source_timeline \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text, \
                 $5::text::uuid, 1, 1, 1 \
             )",
            &[
                &checkpoint_generation,
                &consumer_id,
                &logical_database_id,
                &shard_id,
                &restore_incarnation,
            ],
        )
        .await?;
    let attachment_generation = fixture_uuid(nonce, 102).to_string();
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_attachments( \
                 attachment_generation, consumer_id, logical_database_id, shard_id, \
                 restore_incarnation, system_identifier, database_oid, database_name, \
                 selected_source_member_ordinal, selected_source_role, selected_source_timeline \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text, \
                 $5::text::uuid, 1, 1, 'admin_database', 0, 'primary-anchor', 1 \
             )",
            &[
                &attachment_generation,
                &consumer_id,
                &logical_database_id,
                &shard_id,
                &restore_incarnation,
            ],
        )
        .await?;
    let slot_generation = fixture_uuid(nonce, 104);
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.managed_replication_slots( \
                 slot_generation, attachment_generation, consumer_id, logical_database_id, \
                 shard_id, slot_role, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                 $5::text, 'primary-anchor', $6::text \
             )",
            &[
                &slot_generation.to_string(),
                &attachment_generation,
                &consumer_id,
                &logical_database_id,
                &shard_id,
                &format!("anchor_{}", slot_generation.simple()),
            ],
        )
        .await?;
    assert_admin_checkpoint_write_path(
        transaction,
        &checkpoint_generation,
        &attachment_generation,
        slot_generation,
    )
    .await
}

async fn assert_admin_checkpoint_write_path(
    transaction: &tokio_postgres::Transaction<'_>,
    checkpoint_generation: &str,
    attachment_generation: &str,
    slot_generation: Uuid,
) -> TestResult {
    transaction
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'active', consistent_point = '0/1', two_phase_at = '0/1', \
                 activated_at = statement_timestamp() \
             WHERE slot_generation = $1::text::uuid",
            &[&slot_generation.to_string()],
        )
        .await?;
    transaction
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'active', activated_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&attachment_generation],
        )
        .await?;
    transaction
        .batch_execute("SAVEPOINT direct_checkpoint_progress")
        .await?;
    let error = transaction
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
             SET checkpoint_lsn = '0/1', checkpoint_ordinal = 1 \
             WHERE checkpoint_generation = $1::text::uuid",
            &[&checkpoint_generation],
        )
        .await
        .expect_err("catalog admin must use the ownership-fenced checkpoint CAS");
    assert_sqlstate(&error, "42501");
    transaction
        .batch_execute(
            "ROLLBACK TO SAVEPOINT direct_checkpoint_progress; \
             RELEASE SAVEPOINT direct_checkpoint_progress",
        )
        .await?;
    let checkpoint_ordinal: i64 = transaction
        .query_one(
            "SELECT pgshard_catalog.advance_logical_consumer_checkpoint( \
                 $1::text::uuid, 1, 0, '0/1', 1, false \
             )",
            &[&checkpoint_generation],
        )
        .await?
        .get(0);
    assert_eq!(checkpoint_ordinal, 1);
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

fn fixture_uuid(nonce: u128, discriminator: u128) -> Uuid {
    Uuid::from_u128(nonce.wrapping_add(discriminator) | (1_u128 << 127))
}

struct ConsumerRegistryFixture {
    consumer_id: String,
    checkpoint_generation: String,
    attachment_generation: String,
    selected_member_ordinal: i32,
    anchor_generation: Uuid,
    decoder_generation: Uuid,
    decoder_name: String,
}

#[derive(Clone, Copy)]
enum SelectedSource {
    Primary(i32),
    Standby(i32),
}

impl SelectedSource {
    const fn member_ordinal(self) -> i32 {
        match self {
            Self::Primary(member_ordinal) | Self::Standby(member_ordinal) => member_ordinal,
        }
    }

    const fn role(self) -> &'static str {
        match self {
            Self::Primary(_) => "primary-anchor",
            Self::Standby(_) => "standby-decoder",
        }
    }
}

async fn assert_checkpoint_progress_requires_active_source(
    client: &Client,
    checkpoint_generation: &str,
    context: &str,
) -> TestResult {
    let error = advance_checkpoint(client, checkpoint_generation, 1, 0, "0/10", 1, true)
        .await
        .expect_err(context);
    assert_sqlstate(&error, "55000");
    Ok(())
}

async fn create_initial_consumer_checkpoint(
    client: &Client,
    shard: &Fixture,
    consumer_id: &str,
) -> TestResult<String> {
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_checkpoints( \
                 checkpoint_generation, consumer_id, logical_database_id, shard_id, \
                 restore_incarnation, system_identifier, database_oid, source_timeline, \
                 checkpoint_lsn, checkpoint_ordinal, snapshot_required \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text, \
                 $5::text::uuid, 18446744073709551615, 4294967295, 1, \
                 '0/10', 1, false \
             )",
            &[
                &fixture_uuid(shard.nonce, 12).to_string(),
                &consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &shard.restore_incarnation,
            ],
        )
        .await
        .expect_err("a new checkpoint generation cannot bypass its snapshot boundary");
    assert_sqlstate(&error, "55000");
    let checkpoint_generation = fixture_uuid(shard.nonce, 1).to_string();
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_checkpoints( \
                 checkpoint_generation, consumer_id, logical_database_id, shard_id, \
                 restore_incarnation, system_identifier, database_oid, source_timeline \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text, \
                 $5::text::uuid, 18446744073709551615, 4294967295, 1 \
             )",
            &[
                &checkpoint_generation,
                &consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &shard.restore_incarnation,
            ],
        )
        .await?;
    assert_checkpoint_progress_requires_active_source(
        client,
        &checkpoint_generation,
        "checkpoint progress requires an active source attachment",
    )
    .await?;
    Ok(checkpoint_generation)
}

async fn create_consumer_registry_fixture(
    client: &Client,
    shard: &Fixture,
) -> TestResult<ConsumerRegistryFixture> {
    let consumer_id: String = client
        .query_one(
            "INSERT INTO pgshard_catalog.logical_consumers( \
                 logical_database_id, consumer_name, purpose \
             ) VALUES ($1::text::uuid, $2::text, 'change-stream') \
             RETURNING consumer_id::text",
            &[
                &shard.logical_database_id,
                &format!("stream-{}", shard.nonce),
            ],
        )
        .await?
        .get(0);
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_shards( \
                 consumer_id, logical_database_id, shard_id \
             ) VALUES ($1::text::uuid, $2::text::uuid, $3::text)",
            &[&consumer_id, &shard.logical_database_id, &shard.shard_id],
        )
        .await?;
    let checkpoint_generation =
        create_initial_consumer_checkpoint(client, shard, &consumer_id).await?;
    let attachment_generation = fixture_uuid(shard.nonce, 2).to_string();
    let selected_member_ordinal = 1;
    let error = insert_consumer_attachment(
        client,
        shard,
        &consumer_id,
        &fixture_uuid(shard.nonce, 3).to_string(),
        &fixture_uuid(shard.nonce, 10).to_string(),
        SelectedSource::Standby(selected_member_ordinal),
        1,
    )
    .await
    .expect_err("an attachment cannot invent a shard restore incarnation");
    assert_sqlstate(&error, "55000");
    insert_consumer_attachment(
        client,
        shard,
        &consumer_id,
        &attachment_generation,
        &shard.restore_incarnation,
        SelectedSource::Standby(selected_member_ordinal),
        1,
    )
    .await?;
    let anchor_generation = fixture_uuid(shard.nonce, 4);
    let decoder_generation = fixture_uuid(shard.nonce, 5);
    Ok(ConsumerRegistryFixture {
        consumer_id,
        checkpoint_generation,
        attachment_generation,
        selected_member_ordinal,
        anchor_generation,
        decoder_generation,
        decoder_name: format!("decoder_{}", decoder_generation.simple()),
    })
}

async fn insert_consumer_attachment(
    client: &Client,
    shard: &Fixture,
    consumer_id: &str,
    attachment_generation: &str,
    restore_incarnation: &str,
    selected_source: SelectedSource,
    timeline: i64,
) -> Result<u64, PgError> {
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_attachments( \
                 attachment_generation, consumer_id, logical_database_id, shard_id, \
                 restore_incarnation, system_identifier, database_oid, database_name, \
                 selected_source_member_ordinal, selected_source_role, selected_source_timeline \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text, \
                 $5::text::uuid, 18446744073709551615, 4294967295, $6::text, \
                 $7, $8::text, $9 \
             )",
            &[
                &attachment_generation,
                &consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &restore_incarnation,
                &format!("database_{}", shard.nonce),
                &selected_source.member_ordinal(),
                &selected_source.role(),
                &timeline,
            ],
        )
        .await
}

async fn assert_invalid_managed_slot_allocations(
    client: &Client,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let wrong_name = format!("decoder_{}", fixture_uuid(shard.nonce, 6).simple());
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.managed_replication_slots( \
                 slot_generation, attachment_generation, consumer_id, logical_database_id, \
                 shard_id, slot_role, member_ordinal, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                 $5::text, 'standby-decoder', $6, $7::text \
             )",
            &[
                &consumer.decoder_generation.to_string(),
                &consumer.attachment_generation,
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &consumer.selected_member_ordinal,
                &wrong_name,
            ],
        )
        .await
        .expect_err("a managed slot name must encode its complete catalog generation");
    assert_sqlstate(&error, "23514");

    let invalid_anchor_generation = fixture_uuid(shard.nonce, 13);
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.managed_replication_slots( \
                 slot_generation, attachment_generation, consumer_id, logical_database_id, \
                 shard_id, slot_role, member_ordinal, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                 $5::text, 'primary-anchor', $6, $7::text \
             )",
            &[
                &invalid_anchor_generation.to_string(),
                &consumer.attachment_generation,
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &consumer.selected_member_ordinal,
                &format!("anchor_{}", invalid_anchor_generation.simple()),
            ],
        )
        .await
        .expect_err("a failover anchor is cluster-scoped rather than member-bound");
    assert_sqlstate(&error, "23514");

    let invalid_decoder_generation = fixture_uuid(shard.nonce, 14);
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.managed_replication_slots( \
                 slot_generation, attachment_generation, consumer_id, logical_database_id, \
                 shard_id, slot_role, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                 $5::text, 'standby-decoder', $6::text \
             )",
            &[
                &invalid_decoder_generation.to_string(),
                &consumer.attachment_generation,
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &format!("decoder_{}", invalid_decoder_generation.simple()),
            ],
        )
        .await
        .expect_err("a standby decoder must identify its member");
    assert_sqlstate(&error, "23514");

    let retired_probe_generation = fixture_uuid(shard.nonce, 90);
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.managed_replication_slots( \
                 slot_generation, attachment_generation, consumer_id, logical_database_id, \
                 shard_id, slot_role, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                 $5::text, 'primary-anchor', $6::text \
             )",
            &[
                &retired_probe_generation.to_string(),
                &consumer.attachment_generation,
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &format!("sync_probe_{}", retired_probe_generation.simple()),
            ],
        )
        .await
        .expect_err("a retired probe generation cannot be reused by a consumer slot");
    assert_sqlstate(&error, "55000");
    Ok(())
}

async fn allocate_managed_slots(
    client: &Client,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let anchor_name = format!("anchor_{}", consumer.anchor_generation.simple());
    for (generation, role, member, name) in [
        (
            consumer.anchor_generation.to_string(),
            "primary-anchor",
            None,
            anchor_name.as_str(),
        ),
        (
            consumer.decoder_generation.to_string(),
            "standby-decoder",
            Some(consumer.selected_member_ordinal),
            consumer.decoder_name.as_str(),
        ),
    ] {
        client
            .execute(
                "INSERT INTO pgshard_catalog.managed_replication_slots( \
                     slot_generation, attachment_generation, consumer_id, logical_database_id, \
                     shard_id, slot_role, member_ordinal, slot_name \
                 ) VALUES ( \
                     $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                     $5::text, $6::text, $7, $8::text \
                 )",
                &[
                    &generation,
                    &consumer.attachment_generation,
                    &consumer.consumer_id,
                    &shard.logical_database_id,
                    &shard.shard_id,
                    &role,
                    &member,
                    &name,
                ],
            )
            .await?;
    }
    Ok(())
}

struct ManagedSlotAllocation {
    shard_id: String,
    logical_database_id: String,
    consumer_id: String,
    attachment_generation: String,
    generation: Uuid,
}

enum StaleCatalogMutation {
    AllocateManagedSlot(Box<ManagedSlotAllocation>),
    RetireRestore(String),
    RetireShard(String),
}

impl StaleCatalogMutation {
    async fn execute(&self, transaction: &tokio_postgres::Transaction<'_>) -> Result<(), PgError> {
        match self {
            Self::AllocateManagedSlot(allocation) => {
                transaction
                    .execute(
                        "INSERT INTO pgshard_catalog.managed_replication_slots( \
                             slot_generation, attachment_generation, consumer_id, \
                             logical_database_id, shard_id, slot_role, slot_name \
                         ) VALUES ( \
                             $1::text::uuid, $2::text::uuid, $3::text::uuid, \
                             $4::text::uuid, $5::text, 'primary-anchor', $6::text \
                         )",
                        &[
                            &allocation.generation.to_string(),
                            &allocation.attachment_generation,
                            &allocation.consumer_id,
                            &allocation.logical_database_id,
                            &allocation.shard_id,
                            &format!("anchor_{}", allocation.generation.simple()),
                        ],
                    )
                    .await?;
            }
            Self::RetireRestore(restore_incarnation) => {
                transaction
                    .execute(
                        "UPDATE pgshard_catalog.shard_restore_incarnations \
                         SET state = 'retired', retired_at = statement_timestamp() \
                         WHERE restore_incarnation = $1::text::uuid",
                        &[restore_incarnation],
                    )
                    .await?;
            }
            Self::RetireShard(shard_id) => {
                transaction
                    .execute(
                        "UPDATE pgshard_catalog.shards SET state = 'draining' \
                         WHERE shard_id = $1::text",
                        &[shard_id],
                    )
                    .await?;
                transaction
                    .execute(
                        "UPDATE pgshard_catalog.shards SET state = 'retired' \
                         WHERE shard_id = $1::text",
                        &[shard_id],
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

async fn run_repeatable_read_catalog_mutation(
    mut client: Client,
    snapshot_ready: tokio::sync::oneshot::Sender<()>,
    start: tokio::sync::oneshot::Receiver<()>,
    mutation: StaleCatalogMutation,
) -> Result<Option<PgError>, PgError> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    transaction
        .batch_execute("SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
    transaction
        .query_one(
            "SELECT catalog_epoch FROM pgshard_catalog.cluster_state WHERE singleton",
            &[],
        )
        .await?;
    let _ = snapshot_ready.send(());
    if start.await.is_err() {
        transaction.rollback().await?;
        return Ok(None);
    }

    let result = mutation.execute(&transaction).await;
    match result {
        Ok(()) => {
            transaction.commit().await?;
            Ok(None)
        }
        Err(error) => {
            transaction.rollback().await?;
            Ok(Some(error))
        }
    }
}

struct RepeatableReadRace {
    pid: i32,
    snapshot_ready: Option<tokio::sync::oneshot::Receiver<()>>,
    start: Option<tokio::sync::oneshot::Sender<()>>,
    mutation_task: Option<JoinHandle<Result<Option<PgError>, PgError>>>,
    connection_task: Option<JoinHandle<Result<(), PgError>>>,
}

impl RepeatableReadRace {
    async fn spawn(database_url: &str, mutation: StaleCatalogMutation) -> TestResult<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
        let connection_task = tokio::spawn(connection);
        let pid = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        let (snapshot_ready_sender, snapshot_ready) = tokio::sync::oneshot::channel();
        let (start, start_receiver) = tokio::sync::oneshot::channel();
        let mutation_task = tokio::spawn(run_repeatable_read_catalog_mutation(
            client,
            snapshot_ready_sender,
            start_receiver,
            mutation,
        ));
        Ok(Self {
            pid,
            snapshot_ready: Some(snapshot_ready),
            start: Some(start),
            mutation_task: Some(mutation_task),
            connection_task: Some(connection_task),
        })
    }

    async fn wait_for_snapshot(&mut self) -> TestResult {
        self.snapshot_ready
            .take()
            .ok_or("repeatable-read snapshot was already observed")?
            .await?;
        Ok(())
    }

    fn start(&mut self) -> TestResult {
        self.start
            .take()
            .ok_or("repeatable-read mutation was already started")?
            .send(())
            .map_err(|()| "repeatable-read mutation ended before start".into())
    }

    async fn finish(mut self) -> TestResult<PgError> {
        let mutation_task = self
            .mutation_task
            .take()
            .ok_or("repeatable-read mutation task was already consumed")?;
        let error = tokio::time::timeout(Duration::from_secs(5), mutation_task)
            .await???
            .ok_or("stale repeatable-read catalog mutation committed")?;
        let connection_task = self
            .connection_task
            .take()
            .ok_or("repeatable-read connection task was already consumed")?;
        tokio::time::timeout(Duration::from_secs(5), connection_task).await???;
        Ok(error)
    }
}

impl Drop for RepeatableReadRace {
    fn drop(&mut self) {
        if let Some(task) = &self.mutation_task {
            task.abort();
        }
        if let Some(task) = &self.connection_task {
            task.abort();
        }
    }
}

async fn assert_repeatable_read_probe_lifecycle_races_are_fenced(
    observer: &Client,
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let generation = fixture_uuid(shard.nonce, 94);
    let probe_name = format!("sync_probe_{}", generation.simple());
    let probe_shard = create_fixture(observer).await?;
    let (mut probe_client, probe_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let probe_connection_task = tokio::spawn(probe_connection);
    let probe_transaction = probe_client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    probe_transaction
        .batch_execute("SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
    probe_transaction
        .query_one(
            "SELECT catalog_epoch FROM pgshard_catalog.cluster_state WHERE singleton",
            &[],
        )
        .await?;

    let mut races = vec![
        RepeatableReadRace::spawn(
            database_url,
            StaleCatalogMutation::AllocateManagedSlot(Box::new(ManagedSlotAllocation {
                shard_id: shard.shard_id.clone(),
                logical_database_id: shard.logical_database_id.clone(),
                consumer_id: consumer.consumer_id.clone(),
                attachment_generation: consumer.attachment_generation.clone(),
                generation,
            })),
        )
        .await?,
        RepeatableReadRace::spawn(
            database_url,
            StaleCatalogMutation::RetireRestore(probe_shard.restore_incarnation.clone()),
        )
        .await?,
        RepeatableReadRace::spawn(
            database_url,
            StaleCatalogMutation::RetireShard(probe_shard.shard_id.clone()),
        )
        .await?,
    ];
    for race in &mut races {
        race.wait_for_snapshot().await?;
    }

    probe_transaction
        .execute(
            "INSERT INTO pgshard_catalog.slot_sync_probes( \
                 probe_generation, shard_id, restore_incarnation, system_identifier, \
                 database_oid, database_name, source_timeline, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text, $3::text::uuid, 18446744073709551615, \
                 4294967295, 'postgres', 4294967295, $4::text \
             )",
            &[
                &generation.to_string(),
                &probe_shard.shard_id,
                &probe_shard.restore_incarnation,
                &probe_name,
            ],
        )
        .await?;
    for race in &mut races {
        if race.start().is_err() {
            probe_transaction.rollback().await?;
            probe_connection_task.abort();
            return Err("repeatable-read race task ended before mutation".into());
        }
    }
    for race in &races {
        if !wait_for_backend_lock(observer, race.pid).await? {
            probe_transaction.rollback().await?;
            probe_connection_task.abort();
            return Err("catalog mutation did not wait on the versioned epoch gate".into());
        }
    }

    probe_transaction.commit().await?;
    for race in races {
        let error = race.finish().await?;
        assert_sqlstate(&error, "40001");
        assert_database_message(
            &error,
            "could not serialize access due to concurrent update",
        );
    }
    retire_racing_probe(observer, generation).await?;
    assert_racing_probe_parents_remain_active(observer, &probe_shard).await?;

    drop(probe_client);
    tokio::time::timeout(Duration::from_secs(5), probe_connection_task).await???;
    Ok(())
}

async fn assert_racing_probe_parents_remain_active(
    client: &Client,
    probe_shard: &Fixture,
) -> TestResult {
    let parent_states = client
        .query_one(
            "SELECT shards.state, incarnations.state \
             FROM pgshard_catalog.shards AS shards \
             JOIN pgshard_catalog.shard_restore_incarnations AS incarnations USING (shard_id) \
             WHERE shards.shard_id = $1::text",
            &[&probe_shard.shard_id],
        )
        .await?;
    assert_eq!(parent_states.get::<_, &str>(0), "active");
    assert_eq!(parent_states.get::<_, &str>(1), "active");
    Ok(())
}

async fn retire_racing_probe(client: &Client, generation: Uuid) -> TestResult {
    client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retiring', retiring_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await?;
    let managed_rows: i64 = client
        .query_one(
            "SELECT count(*) FROM pgshard_catalog.managed_replication_slots \
             WHERE slot_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await?
        .get(0);
    assert_eq!(managed_rows, 0);
    Ok(())
}

async fn assert_managed_slot_allocation(
    client: &Client,
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    assert_invalid_managed_slot_allocations(client, shard, consumer).await?;
    assert_repeatable_read_probe_lifecycle_races_are_fenced(client, database_url, shard, consumer)
        .await?;
    allocate_managed_slots(client, shard, consumer).await?;
    let anchor_name = format!("anchor_{}", consumer.anchor_generation.simple());
    let error = insert_slot_sync_probe(
        client,
        shard,
        consumer.anchor_generation,
        &shard.restore_incarnation,
        &anchor_name,
    )
    .await
    .expect_err("a consumer slot generation cannot be reused by a probe");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'retired', consistent_point = '0/10', two_phase_at = '0/10', \
                 activated_at = statement_timestamp(), retired_at = statement_timestamp() \
             WHERE slot_generation = $1::text::uuid",
            &[&consumer.anchor_generation.to_string()],
        )
        .await
        .expect_err("abandoning an allocated slot cannot fabricate activation history");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'active', activated_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&consumer.attachment_generation],
        )
        .await
        .expect_err("an attachment cannot activate before both managed slots");
    assert_sqlstate(&error, "55000");
    Ok(())
}

async fn activate_consumer_registry_fixture(
    client: &Client,
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'active', consistent_point = '0/30', two_phase_at = '0/30', \
                 activated_at = statement_timestamp() \
             WHERE slot_generation = $1::text::uuid",
            &[&consumer.anchor_generation.to_string()],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'active', consistent_point = '0/20', two_phase_at = '0/40', \
                 activated_at = statement_timestamp() \
             WHERE slot_generation = $1::text::uuid",
            &[&consumer.decoder_generation.to_string()],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'active', activated_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&consumer.attachment_generation],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards SET state = 'fenced' \
             WHERE consumer_id = $1::text::uuid \
               AND logical_database_id = $2::text::uuid AND shard_id = $3::text",
            &[
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
            ],
        )
        .await?;
    assert_snapshot_activation_boundaries(client, database_url, shard, consumer).await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards SET state = 'ready' \
             WHERE consumer_id = $1::text::uuid \
               AND logical_database_id = $2::text::uuid AND shard_id = $3::text",
            &[
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
            ],
        )
        .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.shard_restore_incarnations \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE restore_incarnation = $1::text::uuid",
            &[&shard.restore_incarnation],
        )
        .await
        .expect_err("an active consumer must fence before restore-incarnation rotation");
    assert_sqlstate(&error, "55000");
    Ok(())
}

#[derive(Clone, Copy)]
struct SnapshotBoundaryCase {
    context: &'static str,
    anchor_consistent: &'static str,
    anchor_two_phase: &'static str,
    decoder_consistent: &'static str,
    decoder_two_phase: &'static str,
}

struct SnapshotBoundaryFixture {
    consumer_id: String,
    checkpoint_generation: String,
    attachment_generation: String,
    anchor_generation: Uuid,
    decoder_generation: Uuid,
}

async fn create_snapshot_boundary_fixture(
    transaction: &tokio_postgres::Transaction<'_>,
    shard: &Fixture,
    index: u128,
) -> TestResult<SnapshotBoundaryFixture> {
    let consumer_id: String = transaction
        .query_one(
            "INSERT INTO pgshard_catalog.logical_consumers( \
                 logical_database_id, consumer_name, purpose \
             ) VALUES ($1::text::uuid, $2::text, 'change-stream') \
             RETURNING consumer_id::text",
            &[
                &shard.logical_database_id,
                &format!("boundary-{index}-{}", shard.nonce),
            ],
        )
        .await?
        .get(0);
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_shards( \
                 consumer_id, logical_database_id, shard_id \
             ) VALUES ($1::text::uuid, $2::text::uuid, $3::text)",
            &[&consumer_id, &shard.logical_database_id, &shard.shard_id],
        )
        .await?;
    let discriminator = 1_000 + index * 10;
    let checkpoint_generation = fixture_uuid(shard.nonce, discriminator + 1).to_string();
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_checkpoints( \
                 checkpoint_generation, consumer_id, logical_database_id, shard_id, \
                 restore_incarnation, system_identifier, database_oid, source_timeline \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text, \
                 $5::text::uuid, 18446744073709551615, 4294967295, 1 \
             )",
            &[
                &checkpoint_generation,
                &consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &shard.restore_incarnation,
            ],
        )
        .await?;
    let attachment_generation = fixture_uuid(shard.nonce, discriminator + 2).to_string();
    transaction
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_attachments( \
                 attachment_generation, consumer_id, logical_database_id, shard_id, \
                 restore_incarnation, system_identifier, database_oid, database_name, \
                 selected_source_member_ordinal, selected_source_role, \
                 selected_source_timeline \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text, \
                 $5::text::uuid, 18446744073709551615, 4294967295, $6::text, \
                 1, 'standby-decoder', 1 \
             )",
            &[
                &attachment_generation,
                &consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &shard.restore_incarnation,
                &format!("boundary_database_{index}"),
            ],
        )
        .await?;
    let fixture = SnapshotBoundaryFixture {
        consumer_id,
        checkpoint_generation,
        attachment_generation,
        anchor_generation: fixture_uuid(shard.nonce, discriminator + 3),
        decoder_generation: fixture_uuid(shard.nonce, discriminator + 4),
    };
    allocate_snapshot_boundary_slots(transaction, shard, &fixture).await?;
    Ok(fixture)
}

async fn allocate_snapshot_boundary_slots(
    transaction: &tokio_postgres::Transaction<'_>,
    shard: &Fixture,
    fixture: &SnapshotBoundaryFixture,
) -> TestResult {
    for (generation, role, member_ordinal) in [
        (fixture.anchor_generation, "primary-anchor", None),
        (fixture.decoder_generation, "standby-decoder", Some(1_i32)),
    ] {
        transaction
            .execute(
                "INSERT INTO pgshard_catalog.managed_replication_slots( \
                     slot_generation, attachment_generation, consumer_id, \
                     logical_database_id, shard_id, slot_role, member_ordinal, slot_name \
                 ) VALUES ( \
                     $1::text::uuid, $2::text::uuid, $3::text::uuid, \
                     $4::text::uuid, $5::text, $6::text, $7, $8::text \
                 )",
                &[
                    &generation.to_string(),
                    &fixture.attachment_generation,
                    &fixture.consumer_id,
                    &shard.logical_database_id,
                    &shard.shard_id,
                    &role,
                    &member_ordinal,
                    &format!("boundary_{}", generation.simple()),
                ],
            )
            .await?;
    }
    Ok(())
}

async fn activate_snapshot_boundary_fixture(
    transaction: &tokio_postgres::Transaction<'_>,
    fixture: &SnapshotBoundaryFixture,
    boundary: SnapshotBoundaryCase,
) -> TestResult {
    for (generation, consistent_point, two_phase_at) in [
        (
            fixture.anchor_generation,
            boundary.anchor_consistent,
            boundary.anchor_two_phase,
        ),
        (
            fixture.decoder_generation,
            boundary.decoder_consistent,
            boundary.decoder_two_phase,
        ),
    ] {
        transaction
            .execute(
                "UPDATE pgshard_catalog.managed_replication_slots \
                 SET state = 'active', consistent_point = $2::text::pg_lsn, \
                     two_phase_at = $3::text::pg_lsn, \
                     activated_at = statement_timestamp() \
                 WHERE slot_generation = $1::text::uuid",
                &[&generation.to_string(), &consistent_point, &two_phase_at],
            )
            .await?;
    }
    transaction
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'active', activated_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&fixture.attachment_generation],
        )
        .await?;
    Ok(())
}

async fn assert_snapshot_boundary_case(
    client: &mut Client,
    shard: &Fixture,
    index: u128,
    boundary: SnapshotBoundaryCase,
) -> TestResult {
    let transaction = client.transaction().await?;
    let fixture = create_snapshot_boundary_fixture(&transaction, shard, index).await?;
    activate_snapshot_boundary_fixture(&transaction, &fixture, boundary).await?;
    let Err(error) = transaction
        .query_one(
            "SELECT pgshard_catalog.advance_logical_consumer_checkpoint( \
                 $1::text::uuid, 1, 0, '0/18', 1, false \
             )",
            &[&fixture.checkpoint_generation],
        )
        .await
    else {
        panic!("{} was not enforced", boundary.context);
    };
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "snapshot completion is behind a managed slot activation boundary",
    );
    transaction.rollback().await?;
    Ok(())
}

async fn assert_isolated_snapshot_boundaries(database_url: &str, shard: &Fixture) -> TestResult {
    let (mut client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let boundaries = [
        SnapshotBoundaryCase {
            context: "primary-anchor consistent point",
            anchor_consistent: "0/20",
            anchor_two_phase: "0/10",
            decoder_consistent: "0/10",
            decoder_two_phase: "0/10",
        },
        SnapshotBoundaryCase {
            context: "primary-anchor two-phase boundary",
            anchor_consistent: "0/10",
            anchor_two_phase: "0/20",
            decoder_consistent: "0/10",
            decoder_two_phase: "0/10",
        },
        SnapshotBoundaryCase {
            context: "standby-decoder consistent point",
            anchor_consistent: "0/10",
            anchor_two_phase: "0/10",
            decoder_consistent: "0/20",
            decoder_two_phase: "0/10",
        },
        SnapshotBoundaryCase {
            context: "standby-decoder two-phase boundary",
            anchor_consistent: "0/10",
            anchor_two_phase: "0/10",
            decoder_consistent: "0/10",
            decoder_two_phase: "0/20",
        },
    ];
    let result: TestResult = async {
        for (index, boundary) in boundaries.into_iter().enumerate() {
            assert_snapshot_boundary_case(&mut client, shard, index as u128, boundary).await?;
        }
        Ok(())
    }
    .await;
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), connection_task).await???;
    result
}

async fn assert_snapshot_activation_boundaries(
    client: &Client,
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards SET state = 'ready' \
             WHERE consumer_id = $1::text::uuid \
               AND logical_database_id = $2::text::uuid AND shard_id = $3::text",
            &[
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
            ],
        )
        .await
        .expect_err("a snapshot-required checkpoint cannot become ready");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
             SET checkpoint_lsn = '0/10' \
             WHERE checkpoint_generation = $1::text::uuid",
            &[&consumer.checkpoint_generation],
        )
        .await
        .expect_err("checkpoint LSN changes must advance the checkpoint ordinal");
    assert_sqlstate(&error, "55000");
    assert_isolated_snapshot_boundaries(database_url, shard).await?;
    assert_eq!(
        advance_checkpoint(
            client,
            &consumer.checkpoint_generation,
            1,
            0,
            "0/40",
            1,
            false,
        )
        .await?,
        1
    );
    Ok(())
}

async fn assert_consumer_registry_history_guards(
    client: &Client,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let error = advance_checkpoint(
        client,
        &consumer.checkpoint_generation,
        1,
        1,
        "0/10",
        2,
        false,
    )
    .await
    .expect_err("durable checkpoint LSN must not regress");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots SET two_phase_at = '0/30' \
             WHERE slot_generation = $1::text::uuid",
            &[&consumer.decoder_generation.to_string()],
        )
        .await
        .expect_err("an activated two-phase boundary must be immutable");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "DELETE FROM pgshard_catalog.managed_replication_slots \
             WHERE slot_generation = $1::text::uuid",
            &[&consumer.decoder_generation.to_string()],
        )
        .await
        .expect_err("managed slot generations must be permanent");
    assert_sqlstate(&error, "55000");

    let replacement = fixture_uuid(shard.nonce, 7).to_string();
    insert_consumer_attachment(
        client,
        shard,
        &consumer.consumer_id,
        &replacement,
        &shard.restore_incarnation,
        SelectedSource::Standby(consumer.selected_member_ordinal),
        2,
    )
    .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'retired', activated_at = statement_timestamp(), \
                 retired_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&replacement],
        )
        .await
        .expect_err("abandoning a staged attachment cannot fabricate activation history");
    assert_sqlstate(&error, "55000");
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&replacement],
        )
        .await?;
    Ok(())
}

async fn assert_retired_source_guards(
    client: &Client,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let error = client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET retired_at = retired_at + interval '1 second' \
             WHERE slot_generation = $1::text::uuid",
            &[&consumer.decoder_generation.to_string()],
        )
        .await
        .expect_err("retired managed slot tombstones must be immutable");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET retired_at = retired_at + interval '1 second' \
             WHERE attachment_generation = $1::text::uuid",
            &[&consumer.attachment_generation],
        )
        .await
        .expect_err("retired source attachment tombstones must be immutable");
    assert_sqlstate(&error, "55000");
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
             SET checkpoint_lsn = '0/50', checkpoint_ordinal = 2 \
             WHERE checkpoint_generation = $1::text::uuid",
            &[&consumer.checkpoint_generation],
        )
        .await
        .expect_err("checkpoint progress cannot outlive its active source attachment");
    assert_sqlstate(&error, "55000");
    Ok(())
}

async fn observe_backend_blocked_by(
    observer: &Client,
    waiting_pid: i32,
    holding_pid: i32,
) -> Result<bool, PgError> {
    for _ in 0..200 {
        let blocked: bool = observer
            .query_one(
                "SELECT EXISTS ( \
                     SELECT FROM pg_catalog.pg_stat_activity \
                     WHERE pid = $1 AND wait_event_type = 'Lock' \
                       AND $2 = ANY(pg_catalog.pg_blocking_pids(pid)) \
                 )",
                &[&waiting_pid, &holding_pid],
            )
            .await?
            .get(0);
        if blocked {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(false)
}

async fn assert_fence_wins_checkpoint_race(
    observer: &Client,
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let (mut fencer, fencer_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let fencer_connection_task = tokio::spawn(fencer_connection);
    let (advancer, advancer_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let advancer_connection_task = tokio::spawn(advancer_connection);
    let fencer_pid: i32 = fencer
        .query_one("SELECT pg_backend_pid()", &[])
        .await?
        .get(0);
    let advancer_pid: i32 = advancer
        .query_one("SELECT pg_backend_pid()", &[])
        .await?
        .get(0);
    advancer
        .batch_execute("SET ROLE pgshard_catalog_admin")
        .await?;

    let transaction = fencer.transaction().await?;
    transaction
        .batch_execute("SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
    transaction
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards \
             SET state = 'fenced', ownership_fence = ownership_fence + 1 \
             WHERE consumer_id = $1::text::uuid \
               AND logical_database_id = $2::text::uuid AND shard_id = $3::text",
            &[
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
            ],
        )
        .await?;

    let checkpoint_generation = consumer.checkpoint_generation.clone();
    let advance_task = tokio::spawn(async move {
        advance_checkpoint(&advancer, &checkpoint_generation, 1, 1, "0/50", 2, false).await
    });
    if !observe_backend_blocked_by(observer, advancer_pid, fencer_pid).await? {
        transaction.rollback().await?;
        advance_task.abort();
        let _ = advance_task.await;
        fencer_connection_task.abort();
        advancer_connection_task.abort();
        return Err("checkpoint CAS did not block behind the in-flight ownership fence".into());
    }

    transaction.commit().await?;
    let error = advance_task
        .await?
        .expect_err("a stale owner advanced a checkpoint after its fence committed");
    assert_sqlstate(&error, "40001");
    assert_database_message(
        &error,
        "logical consumer ownership fence compare-and-swap failed: expected 1, observed 2",
    );
    drop(fencer);
    fencer_connection_task.abort();
    advancer_connection_task.abort();
    Ok(())
}

async fn assert_restore_waits_for_retiring_attachment(
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let (mut client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let transaction = client.transaction().await?;
    transaction
        .batch_execute("SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
    transaction
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE checkpoint_generation = $1::text::uuid",
            &[&consumer.checkpoint_generation],
        )
        .await?;
    let error = transaction
        .execute(
            "UPDATE pgshard_catalog.shard_restore_incarnations \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE restore_incarnation = $1::text::uuid",
            &[&shard.restore_incarnation],
        )
        .await
        .expect_err("restore retirement skipped a retiring source attachment");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "restore incarnation retains non-retired logical consumer attachment",
    );
    transaction.rollback().await?;
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), connection_task).await???;
    Ok(())
}

async fn fence_and_retire_consumer_attachment(
    client: &Client,
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards SET state = 'fenced' \
             WHERE consumer_id = $1::text::uuid \
               AND logical_database_id = $2::text::uuid AND shard_id = $3::text",
            &[
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
            ],
        )
        .await
        .expect_err("fencing a ready owner must advance its ownership fence");
    assert_sqlstate(&error, "55000");
    assert_fence_wins_checkpoint_race(client, database_url, shard, consumer).await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments SET state = 'retiring' \
             WHERE attachment_generation = $1::text::uuid",
            &[&consumer.attachment_generation],
        )
        .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&consumer.attachment_generation],
        )
        .await
        .expect_err("an attachment cannot retire while it retains live slots");
    assert_sqlstate(&error, "55000");
    assert_restore_waits_for_retiring_attachment(database_url, shard, consumer).await?;
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots SET state = 'retiring' \
             WHERE attachment_generation = $1::text::uuid AND state = 'active'",
            &[&consumer.attachment_generation],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid AND state = 'retiring'",
            &[&consumer.attachment_generation],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&consumer.attachment_generation],
        )
        .await?;
    assert_retired_source_guards(client, consumer).await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.shard_restore_incarnations \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE restore_incarnation = $1::text::uuid",
            &[&shard.restore_incarnation],
        )
        .await
        .expect_err("a restore incarnation cannot retire with a current checkpoint");
    assert_sqlstate(&error, "55000");
    Ok(())
}

async fn assert_primary_fallback_attachment(
    client: &Client,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let attachment_generation = fixture_uuid(shard.nonce, 20).to_string();
    insert_consumer_attachment(
        client,
        shard,
        &consumer.consumer_id,
        &attachment_generation,
        &shard.restore_incarnation,
        SelectedSource::Primary(0),
        1,
    )
    .await?;
    assert_retired_slot_identity_cannot_be_reused(client, shard, consumer, &attachment_generation)
        .await?;

    let slot_generation = fixture_uuid(shard.nonce, 21);
    client
        .execute(
            "INSERT INTO pgshard_catalog.managed_replication_slots( \
                 slot_generation, attachment_generation, consumer_id, logical_database_id, \
                 shard_id, slot_role, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                 $5::text, 'primary-anchor', $6::text \
             )",
            &[
                &slot_generation.to_string(),
                &attachment_generation,
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &format!("anchor_{}", slot_generation.simple()),
            ],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'active', consistent_point = '0/20', two_phase_at = '0/20', \
                 activated_at = statement_timestamp() \
             WHERE slot_generation = $1::text::uuid",
            &[&slot_generation.to_string()],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'active', activated_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&attachment_generation],
        )
        .await?;

    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments SET state = 'retiring' \
             WHERE attachment_generation = $1::text::uuid",
            &[&attachment_generation],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots SET state = 'retiring' \
             WHERE slot_generation = $1::text::uuid",
            &[&slot_generation.to_string()],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE slot_generation = $1::text::uuid",
            &[&slot_generation.to_string()],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&attachment_generation],
        )
        .await?;
    Ok(())
}

async fn assert_retired_slot_identity_cannot_be_reused(
    client: &Client,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
    attachment_generation: &str,
) -> TestResult {
    let new_generation = fixture_uuid(shard.nonce, 22);
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.managed_replication_slots( \
                 slot_generation, attachment_generation, consumer_id, logical_database_id, \
                 shard_id, slot_role, member_ordinal, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                 $5::text, 'standby-decoder', $6, $7::text \
             )",
            &[
                &new_generation.to_string(),
                &attachment_generation,
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &consumer.selected_member_ordinal,
                &consumer.decoder_name,
            ],
        )
        .await
        .expect_err("a new generation cannot reuse a retired slot name");
    assert_sqlstate(&error, "23514");

    let replacement_name = format!("replacement_{}", consumer.decoder_generation.simple());
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.managed_replication_slots( \
                 slot_generation, attachment_generation, consumer_id, logical_database_id, \
                 shard_id, slot_role, member_ordinal, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                 $5::text, 'standby-decoder', $6, $7::text \
             )",
            &[
                &consumer.decoder_generation.to_string(),
                &attachment_generation,
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &consumer.selected_member_ordinal,
                &replacement_name,
            ],
        )
        .await
        .expect_err("a retired generation cannot be rebound to a replacement name");
    assert_sqlstate(&error, "23505");
    Ok(())
}

async fn assert_mismatched_source_requires_snapshot(
    client: &Client,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let attachment_generation = fixture_uuid(shard.nonce, 30).to_string();
    insert_consumer_attachment(
        client,
        shard,
        &consumer.consumer_id,
        &attachment_generation,
        &shard.restore_incarnation,
        SelectedSource::Primary(0),
        2,
    )
    .await?;
    let slot_generation = fixture_uuid(shard.nonce, 31);
    client
        .execute(
            "INSERT INTO pgshard_catalog.managed_replication_slots( \
                 slot_generation, attachment_generation, consumer_id, logical_database_id, \
                 shard_id, slot_role, slot_name \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, \
                 $5::text, 'primary-anchor', $6::text \
             )",
            &[
                &slot_generation.to_string(),
                &attachment_generation,
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
                &format!("anchor_{}", slot_generation.simple()),
            ],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'active', consistent_point = '0/20', two_phase_at = '0/20', \
                 activated_at = statement_timestamp() \
             WHERE slot_generation = $1::text::uuid",
            &[&slot_generation.to_string()],
        )
        .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'active', activated_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&attachment_generation],
        )
        .await
        .expect_err("a checkpoint cannot resume on a different source timeline");
    assert_sqlstate(&error, "55000");
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE slot_generation = $1::text::uuid",
            &[&slot_generation.to_string()],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid",
            &[&attachment_generation],
        )
        .await?;
    Ok(())
}

async fn retire_consumer_registry_fixture(
    client: &Client,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
             SET checkpoint_ordinal = checkpoint_ordinal + 1, state = 'retired', \
                 retired_at = statement_timestamp() \
             WHERE checkpoint_generation = $1::text::uuid",
            &[&consumer.checkpoint_generation],
        )
        .await
        .expect_err("checkpoint retirement cannot advance progress in the same statement");
    assert_sqlstate(&error, "55000");
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE checkpoint_generation = $1::text::uuid",
            &[&consumer.checkpoint_generation],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards SET state = 'retired' \
             WHERE consumer_id = $1::text::uuid \
               AND logical_database_id = $2::text::uuid AND shard_id = $3::text",
            &[
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
            ],
        )
        .await?;
    for state in ["draining", "retired"] {
        client
            .execute(
                "UPDATE pgshard_catalog.logical_consumers SET state = $2::text \
                 WHERE consumer_id = $1::text::uuid",
                &[&consumer.consumer_id, &state],
            )
            .await?;
    }
    let error = client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
             SET checkpoint_ordinal = checkpoint_ordinal + 1 \
             WHERE checkpoint_generation = $1::text::uuid",
            &[&consumer.checkpoint_generation],
        )
        .await
        .expect_err("retired checkpoint generations must remain immutable tombstones");
    assert_sqlstate(&error, "55000");

    client
        .execute(
            "UPDATE pgshard_catalog.shard_restore_incarnations \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE restore_incarnation = $1::text::uuid",
            &[&shard.restore_incarnation],
        )
        .await?;
    let replacement_restore = fixture_uuid(shard.nonce, 11).to_string();
    client
        .execute(
            "INSERT INTO pgshard_catalog.shard_restore_incarnations( \
                 restore_incarnation, shard_id \
             ) VALUES ($1::text::uuid, $2::text)",
            &[&replacement_restore, &shard.shard_id],
        )
        .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.shard_restore_incarnations SET state = 'active' \
             WHERE restore_incarnation = $1::text::uuid",
            &[&shard.restore_incarnation],
        )
        .await
        .expect_err("retired restore incarnations must remain permanent tombstones");
    assert_sqlstate(&error, "55000");
    Ok(())
}

async fn assert_consumer_requires_active_database(client: &Client, nonce: u128) -> TestResult {
    let logical_database_id: String = client
        .query_one(
            "INSERT INTO pgshard_catalog.logical_databases(database_name) \
             VALUES ($1::text) RETURNING logical_database_id::text",
            &[&format!("consumer_lifecycle_{nonce}")],
        )
        .await?
        .get(0);
    client
        .execute(
            "UPDATE pgshard_catalog.logical_databases SET state = 'draining' \
             WHERE logical_database_id = $1::text::uuid",
            &[&logical_database_id],
        )
        .await?;
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumers( \
                 logical_database_id, consumer_name, purpose \
             ) VALUES ($1::text::uuid, $2::text, 'change-stream')",
            &[&logical_database_id, &format!("draining-{nonce}")],
        )
        .await
        .expect_err("a draining database cannot gain a logical consumer");
    assert_sqlstate(&error, "55000");
    client
        .execute(
            "UPDATE pgshard_catalog.logical_databases SET state = 'retired' \
             WHERE logical_database_id = $1::text::uuid",
            &[&logical_database_id],
        )
        .await?;
    let error = client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumers( \
                 logical_database_id, consumer_name, purpose \
             ) VALUES ($1::text::uuid, $2::text, 'change-stream')",
            &[&logical_database_id, &format!("retired-{nonce}")],
        )
        .await
        .expect_err("a retired database cannot gain a logical consumer");
    assert_sqlstate(&error, "55000");
    Ok(())
}

async fn assert_logical_consumer_registry_contract(
    client: &Client,
    database_url: &str,
    fixture: &Fixture,
) -> TestResult {
    let consumer = create_consumer_registry_fixture(client, fixture).await?;
    assert_managed_slot_allocation(client, database_url, fixture, &consumer).await?;
    activate_consumer_registry_fixture(client, database_url, fixture, &consumer).await?;
    assert_consumer_registry_history_guards(client, fixture, &consumer).await?;
    fence_and_retire_consumer_attachment(client, database_url, fixture, &consumer).await?;
    assert_mismatched_source_requires_snapshot(client, fixture, &consumer).await?;
    assert_primary_fallback_attachment(client, fixture, &consumer).await?;
    retire_consumer_registry_fixture(client, fixture, &consumer).await
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
        CatalogOperationTimeout::default(),
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

async fn assert_operation_timeout_aborts_blocked_refresh(
    client: &mut Client,
    database_url: &str,
) -> TestResult {
    let (observer, observer_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let observer_task = tokio::spawn(observer_connection);
    let expected_epoch = catalog_epoch(&observer).await?;
    let application_name = format!(
        "pgshard_timeout_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let mut driver_config: tokio_postgres::Config = database_url.parse()?;
    driver_config.application_name(&application_name);
    let (driver_client, driver_connection) = driver_config.connect(NoTls).await?;
    let cache = Arc::new(CatalogCache::new());
    let driver_cache = Arc::clone(&cache);
    let operation_timeout = CatalogOperationTimeout::new(Duration::from_secs(1))?;
    let driver_task = tokio::spawn(run_catalog_refresh(
        driver_client,
        driver_connection,
        driver_cache,
        CatalogPollInterval::new(Duration::from_secs(30))?,
        operation_timeout,
        pending(),
    ));
    wait_for_cache_epoch(&cache, expected_epoch).await?;

    let driver_pid = catalog_backend_pid(&observer, &application_name).await?;
    let blocker = client.transaction().await?;
    blocker
        .batch_execute("LOCK TABLE pgshard_catalog.cluster_configuration IN ACCESS EXCLUSIVE MODE")
        .await?;
    let future_epoch = expected_epoch
        .checked_add(1)
        .ok_or("catalog epoch cannot represent a future notification")?;
    observer
        .execute(
            "SELECT pg_catalog.pg_notify($1, $2)",
            &[&pgshard_catalog::NOTIFY_CHANNEL, &future_epoch.to_string()],
        )
        .await?;
    if !wait_for_backend_lock(&observer, driver_pid).await? {
        return Err("catalog refresh did not block on the fixture lock".into());
    }

    let result = tokio::time::timeout(Duration::from_secs(5), driver_task).await??;
    let error = result.expect_err("blocked catalog refresh must exceed its deadline");
    assert!(matches!(
        error,
        CatalogRefreshError::OperationTimeout {
            operation: CatalogOperation::Refresh,
            timeout,
        } if timeout == operation_timeout.get()
    ));
    assert_eq!(
        cache.current_for_planning()?.catalog_epoch().0,
        u64::try_from(expected_epoch)?,
        "timed-out refresh replaced the last validated catalog"
    );
    assert!(
        wait_for_backend_exit(&observer, driver_pid).await?,
        "catalog backend survived its operation timeout"
    );
    blocker.rollback().await?;

    observer_task.abort();
    Ok(())
}

async fn assert_operation_timeout_aborts_blocked_initial_load(
    client: &mut Client,
    database_url: &str,
) -> TestResult {
    let (observer, observer_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let observer_task = tokio::spawn(observer_connection);
    let application_name = format!(
        "pgshard_initial_timeout_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let mut driver_config: tokio_postgres::Config = database_url.parse()?;
    driver_config.application_name(&application_name);
    let (driver_client, driver_connection) = driver_config.connect(NoTls).await?;
    let cache = Arc::new(CatalogCache::new());
    let driver_cache = Arc::clone(&cache);
    let operation_timeout = CatalogOperationTimeout::new(Duration::from_secs(1))?;
    let blocker = client.transaction().await?;
    blocker
        .batch_execute("LOCK TABLE pgshard_catalog.cluster_configuration IN ACCESS EXCLUSIVE MODE")
        .await?;
    let driver_task = tokio::spawn(run_catalog_refresh(
        driver_client,
        driver_connection,
        driver_cache,
        CatalogPollInterval::new(Duration::from_secs(30))?,
        operation_timeout,
        pending(),
    ));
    let driver_pid = wait_for_application_backend(&observer, &application_name)
        .await?
        .ok_or("catalog driver backend did not connect within two seconds")?;
    if !wait_for_backend_lock(&observer, driver_pid).await? {
        return Err("catalog driver initial load did not block on the fixture lock".into());
    }

    let result = tokio::time::timeout(Duration::from_secs(5), driver_task).await??;
    let error = result.expect_err("blocked catalog initial load must exceed its deadline");
    assert!(matches!(
        error,
        CatalogRefreshError::OperationTimeout {
            operation: CatalogOperation::InitialLoad,
            timeout,
        } if timeout == operation_timeout.get()
    ));
    assert!(
        cache.current_for_planning().is_err(),
        "timed-out initial load published a catalog snapshot"
    );
    assert!(
        wait_for_backend_exit(&observer, driver_pid).await?,
        "catalog backend survived its initial-load timeout"
    );
    blocker.rollback().await?;

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
        CatalogOperationTimeout::default(),
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

fn catalog_supervisor_test_config() -> Result<CatalogSupervisorConfig, Box<dyn Error>> {
    Ok(CatalogSupervisorConfig::new(
        CatalogPollInterval::new(Duration::from_secs(1))?,
        Duration::from_secs(2),
        Duration::from_millis(10),
        Duration::from_millis(20),
    )?
    .with_timeouts(
        Duration::from_millis(100),
        CatalogOperationTimeout::new(Duration::from_secs(1))?,
    )?)
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
    let config = catalog_supervisor_test_config()?;
    let cache = Arc::new(CatalogCache::new());
    let supervisor = CatalogSupervisor::new(cache, config);
    let status = supervisor.status();
    let attempts = Arc::new(AtomicUsize::new(0));
    let reconnect_gate = Arc::new(ReconnectGate::new());
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
                    reconnect_gate.wait().await;
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
    .await?;
    assert_catalog_supervisor_abort_cleanup(client, database_url).await
}

async fn assert_catalog_supervisor_lifecycle(
    client: &Client,
    application_name: &str,
    expected_epoch: i64,
    status: &CatalogSupervisorStatus,
    reconnect_gate: &ReconnectGate,
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
        snapshot.connect_attempts() >= 4
            && snapshot.phase() == CatalogConnectionPhase::Connecting
            && snapshot.last_failure() == Some(CatalogFailureKind::ConnectTimeout)
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
    assert_eq!(
        reconnecting.last_failure(),
        Some(CatalogFailureKind::ConnectTimeout),
        "a blocked connector must be retried after its own deadline"
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

    reconnect_gate.enable();
    let recovered = wait_for_supervisor_status(status, "recovered", |snapshot| {
        snapshot.ready()
            && snapshot.phase() == CatalogConnectionPhase::Connected
            && snapshot.successful_connections() >= 2
    })
    .await?;
    assert_eq!(recovered.consecutive_failures(), 0);
    assert_eq!(recovered.last_failure(), None);

    reconnect_gate.disable();
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

async fn assert_catalog_supervisor_abort_cleanup(
    client: &Client,
    database_url: &str,
) -> TestResult {
    let application_name = format!(
        "pgshard_catalog_abort_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let mut connection_config: tokio_postgres::Config = database_url.parse()?;
    connection_config.application_name(&application_name);
    let supervisor = CatalogSupervisor::new(
        Arc::new(CatalogCache::new()),
        catalog_supervisor_test_config()?,
    );
    let status = supervisor.status();
    let task = tokio::spawn(supervisor.run(
        move || {
            let connection_config = connection_config.clone();
            async move { connection_config.connect(NoTls).await }
        },
        pending(),
    ));
    wait_for_supervisor_status(&status, "ready before cancellation", |snapshot| {
        snapshot.ready() && snapshot.phase() == CatalogConnectionPhase::Connected
    })
    .await?;
    let backend_pid = catalog_backend_pid(client, &application_name).await?;

    task.abort();
    let cancellation = task
        .await
        .expect_err("aborted catalog supervisor task must not complete normally");
    assert!(cancellation.is_cancelled());
    let stopped = status.snapshot();
    assert!(!stopped.ready());
    assert_eq!(stopped.phase(), CatalogConnectionPhase::Stopped);
    assert_eq!(stopped.readiness_reason(), CatalogReadinessReason::Stopped);
    assert!(!stopped.phase().connection_up());
    assert!(
        wait_for_backend_exit(client, backend_pid).await?,
        "catalog backend survived supervisor task cancellation"
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

#[tokio::test]
async fn reconnect_gate_preserves_current_state_across_subscribers() {
    let gate = ReconnectGate::new();
    gate.enable();
    gate.disable();

    assert!(
        tokio::time::timeout(Duration::from_millis(50), gate.wait())
            .await
            .is_err(),
        "an earlier enabled period bypassed the disabled gate"
    );

    gate.enable();
    tokio::time::timeout(Duration::from_millis(50), gate.wait())
        .await
        .expect("an enabled reconnect gate should release immediately");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing to a PostgreSQL 18 shardschema database"]
async fn migration_and_activation_contract() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);

    assert_installation_contract(&client).await?;
    assert_shutdown_interrupts_initial_load(&mut client, &database_url).await?;
    assert_operation_timeout_aborts_blocked_initial_load(&mut client, &database_url).await?;
    assert_operation_timeout_aborts_blocked_refresh(&mut client, &database_url).await?;
    assert_initial_catalog_reader_contract(&client, &database_url).await?;
    assert_catalog_reader_rejects_existing_transaction(&client, &database_url).await?;
    assert_admin_privilege_contract(&mut client).await?;
    assert_admin_write_path(&mut client).await?;
    let fixture = create_fixture(&client).await?;
    assert_migration_does_not_resurrect_retired_restore(&client, fixture.nonce).await?;
    assert_slot_sync_probe_contract(&client, &fixture).await?;
    assert_identity_history_contract(&client, &fixture).await?;
    assert_registered_table_contract(&client, &fixture).await?;
    assert_tombstone_contract(&client, &fixture).await?;
    assert_consumer_requires_active_database(&client, fixture.nonce).await?;
    assert_logical_consumer_registry_contract(&client, &database_url, &fixture).await?;
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
