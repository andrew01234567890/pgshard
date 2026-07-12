//! Live `PostgreSQL` 18 contract tests for the shard schema catalog.
//!
//! Run explicitly with a superuser URL whose database name is `shardschema`:
//! `PGSHARD_TEST_DATABASE_URL=... cargo test -p pgshard-catalog --test postgres18 -- --ignored`

use std::error::Error;
use std::future::poll_fn;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use tokio_postgres::{AsyncMessage, Client, Error as PgError, NoTls};

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

fn assert_no_notification(receiver: &Receiver<String>, context: &str) {
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        matches!(receiver.try_recv(), Err(TryRecvError::Empty)),
        "{context}"
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing to a PostgreSQL 18 shardschema database"]
async fn migration_and_activation_contract() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);

    assert_installation_contract(&client).await?;
    assert_admin_privilege_contract(&mut client).await?;
    assert_admin_write_path(&mut client).await?;
    let fixture = create_fixture(&client).await?;
    assert_identity_history_contract(&client, &fixture).await?;
    assert_registered_table_contract(&client, &fixture).await?;
    assert_tombstone_contract(&client, &fixture).await?;
    let routing = assert_invalid_routing_contracts(&client, &fixture).await?;
    let listener = connect_listener(&database_url).await?;
    let activated_epoch =
        commit_valid_activation(&mut client, &listener, &fixture, &routing).await?;
    assert_rollback_contract(&mut client, &listener, &fixture, &routing, activated_epoch).await?;

    listener.task.abort();
    connection_task.abort();
    Ok(())
}
