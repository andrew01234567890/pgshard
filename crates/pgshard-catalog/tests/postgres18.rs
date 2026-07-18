//! Live `PostgreSQL` 18 contract tests for the shard schema catalog.
//!
//! Run explicitly with a superuser URL for a disposable database whose name is
//! `shardschema`; the test recreates and removes `pgshard_catalog`:
//! `PGSHARD_TEST_DATABASE_URL=... cargo test -p pgshard-catalog --test postgres18 -- --ignored`

use std::error::Error;
use std::future::{pending, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use tokio_postgres::{
    AsyncMessage, Client, Error as PgError, GenericClient, IsolationLevel, NoTls,
};
use uuid::Uuid;

use pgshard_catalog::{
    CatalogCache, CatalogConnectionPhase, CatalogFailureKind, CatalogOperation,
    CatalogOperationTimeout, CatalogPollInterval, CatalogReader, CatalogReadinessReason,
    CatalogRefreshError, CatalogSupervisor, CatalogSupervisorConfig, CatalogSupervisorSnapshot,
    CatalogSupervisorStatus, DatabaseId, DatabaseShardId, InstallOutcome, LoadError,
    run_catalog_refresh,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

// Exact migration bytes from tag v0.49.0, SHA-256
// a0f23cc211c37d4dc70a93efa222c4d7fa594ca25d22f37d404399b88a5378a6.
const V0_49_0_MIGRATION_SQL: &str = include_str!("fixtures/v0_49_0_shardschema.sql");

struct Fixture {
    database_shard_id: String,
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
        "unexpected PostgreSQL error: {error}, detail: {:?}",
        error
            .as_db_error()
            .and_then(tokio_postgres::error::DbError::detail)
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

const PRE_RECEIPT_PROBE_SCHEMA_SQL: &str = r"
            BEGIN;
            CREATE SCHEMA pgshard_catalog;
            CREATE DOMAIN pgshard_catalog.sql_identifier AS text
                CHECK (octet_length(VALUE) BETWEEN 1 AND 63);
            CREATE DOMAIN pgshard_catalog.resource_name AS text
                CHECK (
                    VALUE ~ '^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$'
                    AND octet_length(VALUE) BETWEEN 1 AND 63
                );
            CREATE DOMAIN pgshard_catalog.replication_slot_name AS text
                CHECK (
                    VALUE ~ '^[a-z0-9_]+$'
                    AND octet_length(VALUE) BETWEEN 1 AND 63
                );
            CREATE TABLE pgshard_catalog.shards (
                shard_id pgshard_catalog.resource_name PRIMARY KEY,
                shard_number bigint NOT NULL UNIQUE
                    CHECK (shard_number BETWEEN 0 AND 4294967295),
                state text NOT NULL DEFAULT 'active'
                    CHECK (state IN ('provisioning', 'active', 'draining', 'retired')),
                created_at timestamptz NOT NULL DEFAULT statement_timestamp()
            );
            CREATE TABLE pgshard_catalog.shard_restore_incarnations (
                restore_incarnation uuid PRIMARY KEY
                    CHECK (restore_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid),
                shard_id pgshard_catalog.resource_name NOT NULL
                    REFERENCES pgshard_catalog.shards(shard_id) ON DELETE RESTRICT,
                state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'retired')),
                installed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
                retired_at timestamptz,
                UNIQUE (restore_incarnation, shard_id),
                CHECK ((state = 'active') = (retired_at IS NULL))
            );
            CREATE TABLE pgshard_catalog.slot_sync_probes (
                probe_generation uuid PRIMARY KEY
                    CHECK (probe_generation <> '00000000-0000-0000-0000-000000000000'::uuid),
                shard_id pgshard_catalog.resource_name NOT NULL
                    REFERENCES pgshard_catalog.shards(shard_id) ON DELETE RESTRICT,
                restore_incarnation uuid NOT NULL
                    CHECK (restore_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid),
                system_identifier numeric(20, 0) NOT NULL
                    CHECK (system_identifier BETWEEN 1 AND 18446744073709551615),
                database_oid bigint NOT NULL CHECK (database_oid BETWEEN 1 AND 4294967295),
                database_name pgshard_catalog.sql_identifier NOT NULL,
                source_timeline bigint NOT NULL CHECK (source_timeline BETWEEN 1 AND 4294967295),
                slot_name pgshard_catalog.replication_slot_name NOT NULL,
                consistent_point pg_lsn,
                state text NOT NULL DEFAULT 'allocated'
                    CHECK (state IN ('allocated', 'active', 'retiring', 'retired')),
                created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
                activated_at timestamptz,
                retiring_at timestamptz,
                retired_at timestamptz,
                UNIQUE (shard_id, slot_name),
                FOREIGN KEY (restore_incarnation, shard_id)
                    REFERENCES pgshard_catalog.shard_restore_incarnations(
                        restore_incarnation,
                        shard_id
                    ) ON DELETE RESTRICT,
                CHECK (right(slot_name::text, 32) = replace(probe_generation::text, '-', '')),
                CHECK (consistent_point IS NULL OR consistent_point > '0/0'),
                CHECK (
                    (
                        state = 'allocated'
                        AND consistent_point IS NULL
                        AND activated_at IS NULL
                        AND retiring_at IS NULL
                        AND retired_at IS NULL
                    ) OR (
                        state = 'active'
                        AND consistent_point IS NOT NULL
                        AND activated_at IS NOT NULL
                        AND retiring_at IS NULL
                        AND retired_at IS NULL
                    ) OR (
                        state = 'retiring'
                        AND retiring_at IS NOT NULL
                        AND retired_at IS NULL
                        AND (
                            (consistent_point IS NULL AND activated_at IS NULL)
                            OR (consistent_point IS NOT NULL AND activated_at IS NOT NULL)
                        )
                    ) OR (
                        state = 'retired'
                        AND retiring_at IS NOT NULL
                        AND retired_at IS NOT NULL
                        AND (
                            (consistent_point IS NULL AND activated_at IS NULL)
                            OR (consistent_point IS NOT NULL AND activated_at IS NOT NULL)
                        )
                    )
                )
            );
            CREATE UNIQUE INDEX slot_sync_probes_one_live_per_shard
                ON pgshard_catalog.slot_sync_probes(shard_id)
                WHERE state IN ('allocated', 'active', 'retiring');
            CREATE VIEW pgshard_catalog.managed_replication_slots AS
            SELECT NULL::uuid AS slot_generation WHERE false;
            CREATE FUNCTION pgshard_catalog.protect_slot_sync_probe()
            RETURNS trigger
            LANGUAGE plpgsql
            SECURITY DEFINER
            SET search_path = pg_catalog, pgshard_catalog, pg_temp
            AS $function$
            DECLARE
                restore_state text;
                shard_state text;
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '55000',
                        MESSAGE = 'slot-sync probe generations are permanent';
                END IF;

                SELECT incarnations.state, shards.state
                  INTO restore_state, shard_state
                  FROM pgshard_catalog.shard_restore_incarnations AS incarnations
                  JOIN pgshard_catalog.shards AS shards
                    ON shards.shard_id = incarnations.shard_id
                 WHERE incarnations.restore_incarnation = NEW.restore_incarnation
                   AND incarnations.shard_id = NEW.shard_id
                 FOR KEY SHARE OF incarnations, shards;

                IF TG_OP = 'INSERT' THEN
                    IF NEW.state <> 'allocated'
                       OR NEW.consistent_point IS NOT NULL
                       OR NEW.activated_at IS NOT NULL
                       OR NEW.retiring_at IS NOT NULL
                       OR NEW.retired_at IS NOT NULL THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '55000',
                            MESSAGE = 'a slot-sync probe must start allocated';
                    END IF;
                    IF restore_state IS DISTINCT FROM 'active'
                       OR shard_state IS NULL
                       OR shard_state NOT IN ('provisioning', 'active') THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '55000',
                            MESSAGE = 'slot-sync probes require an active shard restore';
                    END IF;
                    IF EXISTS (
                        SELECT
                          FROM pgshard_catalog.managed_replication_slots AS slots
                         WHERE slots.slot_generation = NEW.probe_generation
                    ) THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '55000',
                            MESSAGE = 'replication-slot generations cannot be reused across managed roles';
                    END IF;
                    RETURN NEW;
                END IF;

                IF NEW.probe_generation IS DISTINCT FROM OLD.probe_generation
                   OR NEW.shard_id IS DISTINCT FROM OLD.shard_id
                   OR NEW.restore_incarnation IS DISTINCT FROM OLD.restore_incarnation
                   OR NEW.system_identifier IS DISTINCT FROM OLD.system_identifier
                   OR NEW.database_oid IS DISTINCT FROM OLD.database_oid
                   OR NEW.database_name IS DISTINCT FROM OLD.database_name
                   OR NEW.source_timeline IS DISTINCT FROM OLD.source_timeline
                   OR NEW.slot_name IS DISTINCT FROM OLD.slot_name
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '55000',
                        MESSAGE = 'slot-sync probe allocation identity is immutable';
                END IF;

                IF OLD.state = 'retired' THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '55000',
                        MESSAGE = 'retired slot-sync probes are immutable';
                END IF;

                IF NEW.state = OLD.state THEN
                    IF NEW.consistent_point IS DISTINCT FROM OLD.consistent_point
                       OR NEW.activated_at IS DISTINCT FROM OLD.activated_at
                       OR NEW.retiring_at IS DISTINCT FROM OLD.retiring_at
                       OR NEW.retired_at IS DISTINCT FROM OLD.retired_at THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '55000',
                            MESSAGE = 'slot-sync probe lifecycle history is immutable';
                    END IF;
                    RETURN NEW;
                END IF;

                IF OLD.state = 'allocated' AND NEW.state = 'active' THEN
                    IF restore_state IS DISTINCT FROM 'active'
                       OR shard_state IS NULL
                       OR shard_state NOT IN ('provisioning', 'active')
                       OR NEW.consistent_point IS NULL
                       OR NEW.activated_at IS NULL
                       OR NEW.retiring_at IS NOT NULL
                       OR NEW.retired_at IS NOT NULL THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '55000',
                            MESSAGE = 'slot-sync probe activation is incomplete or misplaced';
                    END IF;
                ELSIF OLD.state IN ('allocated', 'active') AND NEW.state = 'retiring' THEN
                    IF NEW.consistent_point IS DISTINCT FROM OLD.consistent_point
                       OR NEW.activated_at IS DISTINCT FROM OLD.activated_at
                       OR NEW.retiring_at IS NULL
                       OR NEW.retired_at IS NOT NULL THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '55000',
                            MESSAGE = 'slot-sync probe retirement must preserve activation history';
                    END IF;
                ELSIF OLD.state = 'retiring' AND NEW.state = 'retired' THEN
                    IF NEW.consistent_point IS DISTINCT FROM OLD.consistent_point
                       OR NEW.activated_at IS DISTINCT FROM OLD.activated_at
                       OR NEW.retiring_at IS DISTINCT FROM OLD.retiring_at
                       OR NEW.retired_at IS NULL THEN
                        RAISE EXCEPTION USING
                            ERRCODE = '55000',
                            MESSAGE = 'slot-sync probe retirement is incomplete';
                    END IF;
                ELSE
                    RAISE EXCEPTION USING
                        ERRCODE = '55000',
                        MESSAGE = 'invalid slot-sync probe transition';
                END IF;

                RETURN NEW;
            END
            $function$;
            CREATE TRIGGER slot_sync_probes_protect_history
            BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.slot_sync_probes
            FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_slot_sync_probe();
            INSERT INTO pgshard_catalog.shards(shard_id, shard_number)
            VALUES ('legacy-shard', 4000000000);
            INSERT INTO pgshard_catalog.shards(shard_id, shard_number)
            VALUES ('legacy-shard-active', 4000000001);
            INSERT INTO pgshard_catalog.shard_restore_incarnations(
                restore_incarnation,
                shard_id
            ) VALUES (
                '10000000-0000-0000-0000-000000000001',
                'legacy-shard'
            );
            INSERT INTO pgshard_catalog.shard_restore_incarnations(
                restore_incarnation,
                shard_id
            ) VALUES (
                '10000000-0000-0000-0000-000000000002',
                'legacy-shard-active'
            );
            INSERT INTO pgshard_catalog.slot_sync_probes(
                probe_generation,
                shard_id,
                restore_incarnation,
                system_identifier,
                database_oid,
                database_name,
                source_timeline,
                slot_name
            ) VALUES (
                '20000000-0000-0000-0000-000000000001',
                'legacy-shard',
                '10000000-0000-0000-0000-000000000001',
                1,
                1,
                'shardschema',
                1,
                'legacy_probe_20000000000000000000000000000001'
            );
            INSERT INTO pgshard_catalog.slot_sync_probes(
                probe_generation,
                shard_id,
                restore_incarnation,
                system_identifier,
                database_oid,
                database_name,
                source_timeline,
                slot_name
            ) VALUES (
                '20000000-0000-0000-0000-000000000002',
                'legacy-shard-active',
                '10000000-0000-0000-0000-000000000002',
                1,
                1,
                'shardschema',
                1,
                'legacy_probe_20000000000000000000000000000002'
            );
            UPDATE pgshard_catalog.slot_sync_probes
               SET state = 'active',
                   consistent_point = '0/10',
                   activated_at = statement_timestamp()
             WHERE probe_generation = '20000000-0000-0000-0000-000000000002';
            DROP VIEW pgshard_catalog.managed_replication_slots;
            COMMIT;
            ";

async fn install_pre_receipt_probe_schema(client: &Client) -> TestResult {
    client
        .batch_execute("DROP SCHEMA IF EXISTS pgshard_catalog CASCADE")
        .await?;
    drop_catalog_roles(client).await?;
    client
        .batch_execute(
            "CREATE ROLE pgshard_catalog_reader NOLOGIN; \
             CREATE ROLE pgshard_catalog_admin NOLOGIN; \
             GRANT pgshard_catalog_reader TO pgshard_catalog_admin",
        )
        .await?;
    client.batch_execute(PRE_RECEIPT_PROBE_SCHEMA_SQL).await?;
    Ok(())
}

async fn assert_pre_receipt_probe_upgrade(client: &Client) -> TestResult {
    install_pre_receipt_probe_schema(client).await?;
    let blocked = client
        .batch_execute(pgshard_catalog::MIGRATION_SQL)
        .await
        .expect_err("a receiptless active probe must block the forward migration");
    assert_eq!(
        blocked.code().map(tokio_postgres::error::SqlState::code),
        Some("55000"),
        "unexpected pre-receipt rejection: {:?}",
        blocked
            .as_db_error()
            .map(tokio_postgres::error::DbError::detail)
    );
    assert_eq!(
        blocked
            .as_db_error()
            .map(tokio_postgres::error::DbError::message),
        Some("receiptless live slot-sync probes block catalog upgrade")
    );
    client.batch_execute("ROLLBACK").await?;

    client
        .batch_execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
                SET state = 'retiring', \
                    retiring_at = statement_timestamp() \
              WHERE state = 'active'",
        )
        .await?;
    let retiring_generations = client
        .query(
            "SELECT probe_generation::text \
               FROM pgshard_catalog.slot_sync_probes \
              WHERE state = 'retiring'",
            &[],
        )
        .await?;
    for row in retiring_generations {
        let generation = Uuid::parse_str(&row.try_get::<_, String>(0)?)?;
        client
            .execute(
                "UPDATE pgshard_catalog.slot_sync_probes \
                    SET state = 'retired', retired_at = statement_timestamp() \
                  WHERE probe_generation = $1::text::uuid AND state = 'retiring'",
                &[&generation.to_string()],
            )
            .await?;
    }
    client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;

    let rows = client
        .query(
            "SELECT state, creation_receipt_id::text, cleanup_receipt_id::text \
               FROM pgshard_catalog.slot_sync_probes \
              WHERE shard_id IN ('legacy-shard', 'legacy-shard-active') \
              ORDER BY state",
            &[],
        )
        .await?;
    assert_eq!(rows.len(), 2);
    for row in rows {
        let state: String = row.try_get(0)?;
        assert!(matches!(state.as_str(), "allocated" | "retired"));
        assert_eq!(row.try_get::<_, Option<String>>(1)?, None);
        assert_eq!(row.try_get::<_, Option<String>>(2)?, None);
    }
    let validated_constraints: i64 = client
        .query_one(
            "SELECT count(*) \
               FROM pg_catalog.pg_constraint \
              WHERE conrelid = 'pgshard_catalog.slot_sync_probes'::regclass \
                AND conname IN ( \
                    'slot_sync_probes_receipt_ids_nonzero', \
                    'slot_sync_probes_receipt_lifecycle' \
                ) \
                AND convalidated",
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(validated_constraints, 2);
    Ok(())
}

async fn assert_pre_creation_attempt_consumer_upgrade(client: &Client) -> TestResult {
    let fixture = create_fixture(client).await?;
    let consumer = create_consumer_registry_fixture(client, &fixture).await?;
    allocate_managed_slots(client, &fixture, &consumer).await?;
    begin_managed_slot_creation_attempt(
        client,
        consumer.anchor_generation,
        consumer.anchor_receipt_id,
    )
    .await?;
    begin_managed_slot_creation_attempt(
        client,
        consumer.decoder_generation,
        consumer.decoder_receipt_id,
    )
    .await?;
    activate_managed_replication_slot(
        client,
        consumer.anchor_generation,
        consumer.anchor_receipt_id,
        "0/30",
        "0/30",
    )
    .await?;
    activate_managed_replication_slot(
        client,
        consumer.decoder_generation,
        consumer.decoder_receipt_id,
        "0/20",
        "0/40",
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
            "UPDATE pgshard_catalog.logical_consumer_shards \
                SET state = 'fenced' \
              WHERE consumer_id = $1::text::uuid \
                AND logical_database_id = $2::text::uuid \
                AND shard_id = $3::text",
            &[
                &consumer.consumer_id,
                &fixture.logical_database_id,
                &fixture.shard_id,
            ],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
                SET state = 'retiring' \
              WHERE attachment_generation = $1::text::uuid",
            &[&consumer.attachment_generation],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
                SET state = 'retiring' \
              WHERE slot_generation = $1::text::uuid",
            &[&consumer.decoder_generation.to_string()],
        )
        .await?;
    client
        .batch_execute("DROP TABLE pgshard_catalog.managed_slot_creation_attempts CASCADE")
        .await?;

    let blocked = client
        .batch_execute(pgshard_catalog::MIGRATION_SQL)
        .await
        .expect_err("receiptless active and retiring consumer slots must block the upgrade");
    assert_sqlstate(&blocked, "55000");
    assert_database_message(
        &blocked,
        "receiptless live managed replication slots block catalog upgrade",
    );
    client.batch_execute("ROLLBACK").await?;

    client
        .batch_execute("DROP SCHEMA pgshard_catalog CASCADE")
        .await?;
    drop_catalog_roles(client).await?;
    client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;
    Ok(())
}

async fn drop_catalog_roles(client: &Client) -> TestResult {
    client
        .batch_execute(
            "DO $cleanup$ \
             BEGIN \
                 IF pg_catalog.to_regrole('pgshard_catalog_reader') IS NOT NULL \
                    AND pg_catalog.to_regrole('pgshard_catalog_admin') IS NOT NULL THEN \
                     REVOKE pgshard_catalog_reader FROM pgshard_catalog_admin; \
                 END IF; \
                 IF pg_catalog.to_regrole('pgshard_catalog_owner') IS NOT NULL THEN \
                     REVOKE pg_read_all_stats FROM pgshard_catalog_owner; \
                     DROP OWNED BY pgshard_catalog_owner; \
                 END IF; \
             END \
             $cleanup$; \
             DROP ROLE IF EXISTS pgshard_catalog_admin; \
             DROP ROLE IF EXISTS pgshard_catalog_reader; \
             DROP ROLE IF EXISTS pgshard_catalog_owner",
        )
        .await?;
    Ok(())
}

async fn assert_squatted_catalog_role_is_rejected(client: &Client) -> TestResult {
    let squatter = format!("pgshard_role_squatter_{}", Uuid::new_v4().simple());
    client
        .batch_execute("DROP SCHEMA IF EXISTS pgshard_catalog CASCADE")
        .await?;
    drop_catalog_roles(client).await?;
    client
        .batch_execute(&format!(
            "CREATE ROLE {squatter} NOLOGIN CREATEROLE; \
             SET ROLE {squatter}; \
             CREATE ROLE pgshard_catalog_reader NOLOGIN; \
             RESET ROLE"
        ))
        .await?;

    let migration = client.batch_execute(pgshard_catalog::MIGRATION_SQL).await;
    let rollback_result = client.batch_execute("ROLLBACK").await;
    let reset_result = client.batch_execute("RESET ROLE").await;
    let drop_reader_result = client
        .batch_execute("DROP ROLE IF EXISTS pgshard_catalog_reader")
        .await;
    let drop_squatter_result = client
        .batch_execute(&format!("DROP ROLE IF EXISTS {squatter}"))
        .await;

    rollback_result?;
    reset_result?;
    drop_reader_result?;
    drop_squatter_result?;
    let error = migration.expect_err("a fixed role must not predate catalog bootstrap");
    assert_sqlstate(&error, "42501");
    assert_database_message(
        &error,
        "pgshard catalog roles exist before catalog bootstrap",
    );
    Ok(())
}

async fn assert_installation_contract(client: &Client, database_url: &str) -> TestResult {
    assert_pre_receipt_probe_upgrade(client)
        .await
        .map_err(|error| format!("pre-receipt probe upgrade: {error}"))?;
    assert_pre_creation_attempt_consumer_upgrade(client)
        .await
        .map_err(|error| format!("pre-creation-attempt consumer upgrade: {error}"))?;
    let epoch_after_first_migration = catalog_epoch(client).await?;
    client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;
    assert_eq!(
        catalog_epoch(client).await?,
        epoch_after_first_migration,
        "reapplying the migration must not mutate catalog state"
    );
    assert_database_genesis_contract(client).await?;
    assert_concurrent_database_genesis_contract(client, database_url).await?;
    assert_legacy_catalog_owner_upgrade(client, database_url)
        .await
        .map_err(|error| format!("legacy catalog-owner upgrade: {error:?}"))?;
    assert_catalog_role_bootstrap_rejections(client)
        .await
        .map_err(|error| format!("catalog role bootstrap rejection: {error}"))?;
    assert_migration_pins_search_path(client)
        .await
        .map_err(|error| format!("migration search-path pin: {error}"))?;

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

async fn assert_database_genesis_contract(client: &Client) -> TestResult {
    client
        .batch_execute("BEGIN; SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
    let result: TestResult = async {
        let fixture = install_database_genesis_fixture(client).await?;
        assert_database_genesis_rejections(client, &fixture).await?;
        Ok(())
    }
    .await;
    let rollback = client.batch_execute("ROLLBACK").await;
    result?;
    rollback?;
    Ok(())
}

struct DatabaseGenesisFixture {
    nonce: u128,
    first_name: String,
    next_cell: i64,
    catalog_epoch: i64,
}

async fn install_database_genesis_fixture(client: &Client) -> TestResult<DatabaseGenesisFixture> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let first_name = format!("genesis_a_{nonce}");
    let second_name = format!("genesis_b_{nonce}");
    let next_cell: i64 = client
        .query_one(
            "SELECT coalesce(max(shard_number), 0) + 1 FROM pgshard_catalog.shards",
            &[],
        )
        .await?
        .get(0);
    let next_shard = format!("genesis-{nonce}");
    client
        .execute(
            "INSERT INTO pgshard_catalog.shards(shard_id, shard_number) VALUES ($1::text, $2)",
            &[&next_shard, &next_cell],
        )
        .await?;

    let shared_cells = vec![0_i64, next_cell];
    let installed = client
        .query_one(
            "SELECT logical_database_id::text, routing_epoch, installed \
               FROM pgshard_catalog.install_database_genesis( \
                   $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
               )",
            &[&first_name, &shared_cells],
        )
        .await?;
    let database_id: String = installed.get(0);
    let routing_epoch: i64 = installed.get(1);
    assert!(installed.get::<_, bool>(2));
    let epoch_after_install = catalog_epoch(client).await?;
    let retried = client
        .query_one(
            "SELECT logical_database_id::text, routing_epoch, installed \
               FROM pgshard_catalog.install_database_genesis( \
                   $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
               )",
            &[&first_name, &shared_cells],
        )
        .await?;
    assert_eq!(retried.get::<_, String>(0), database_id);
    assert_eq!(retried.get::<_, i64>(1), routing_epoch);
    assert!(!retried.get::<_, bool>(2));
    assert_eq!(catalog_epoch(client).await?, epoch_after_install);

    let dedicated_cells = vec![next_cell];
    client
        .query_one(
            "SELECT installed FROM pgshard_catalog.install_database_genesis( \
                 $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
             )",
            &[&second_name, &dedicated_cells],
        )
        .await?;
    let catalog_epoch = catalog_epoch(client).await?;
    let placements: Vec<(String, i64)> = client
        .query(
            "SELECT databases.database_name::text, shards.shard_number \
               FROM pgshard_catalog.logical_databases AS databases \
               JOIN pgshard_catalog.active_routing_epochs AS active \
                 ON active.logical_database_id = databases.logical_database_id \
               JOIN pgshard_catalog.routing_ranges AS ranges \
                 ON ranges.logical_database_id = active.logical_database_id \
                AND ranges.routing_epoch = active.routing_epoch \
               JOIN pgshard_catalog.database_shard_placements AS placements \
                 ON placements.logical_database_id = ranges.logical_database_id \
                AND placements.database_shard_id = ranges.database_shard_id \
                AND placements.state = 'active' \
               JOIN pgshard_catalog.shards AS shards \
                 ON shards.shard_id = placements.shard_id \
              WHERE databases.database_name::text IN ($1, $2) \
              ORDER BY databases.database_name, ranges.range_start",
            &[&first_name, &second_name],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        placements,
        vec![
            (first_name.clone(), 0),
            (first_name.clone(), next_cell),
            (second_name, next_cell),
        ]
    );
    Ok(DatabaseGenesisFixture {
        nonce,
        first_name,
        next_cell,
        catalog_epoch,
    })
}

async fn assert_database_genesis_rejections(
    client: &Client,
    fixture: &DatabaseGenesisFixture,
) -> TestResult {
    client
        .batch_execute("SAVEPOINT conflicting_genesis")
        .await?;
    let conflicting_cells = vec![fixture.next_cell, 0_i64];
    let error = client
        .query_one(
            "SELECT * FROM pgshard_catalog.install_database_genesis( \
                 $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
             )",
            &[&fixture.first_name, &conflicting_cells],
        )
        .await
        .expect_err("a conflicting database topology must fail closed");
    assert_sqlstate(&error, "22023");
    assert_database_message(
        &error,
        "logical database genesis topology does not match active routing",
    );
    client
        .batch_execute("ROLLBACK TO SAVEPOINT conflicting_genesis")
        .await?;
    assert_eq!(catalog_epoch(client).await?, fixture.catalog_epoch);

    client.batch_execute("SAVEPOINT duplicate_cells").await?;
    let duplicate_cells = vec![0_i64, 0_i64];
    let duplicate_name = format!("genesis_duplicate_{}", fixture.nonce);
    let error = client
        .query_one(
            "SELECT * FROM pgshard_catalog.install_database_genesis( \
                 $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
             )",
            &[&duplicate_name, &duplicate_cells],
        )
        .await
        .expect_err("duplicate cell placement must fail closed");
    assert_sqlstate(&error, "22023");
    assert_database_message(
        &error,
        "logical database genesis contains a duplicate cell ordinal",
    );
    client
        .batch_execute("ROLLBACK TO SAVEPOINT duplicate_cells")
        .await?;

    client.batch_execute("SAVEPOINT unavailable_cell").await?;
    let unavailable_name = format!("genesis_unavailable_{}", fixture.nonce);
    let unavailable_cells = vec![fixture.next_cell + 1];
    let error = client
        .query_one(
            "SELECT * FROM pgshard_catalog.install_database_genesis( \
                 $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
             )",
            &[&unavailable_name, &unavailable_cells],
        )
        .await
        .expect_err("unavailable cell placement must fail closed");
    assert_sqlstate(&error, "22023");
    assert_database_message(
        &error,
        "logical database genesis references an unavailable cell",
    );
    client
        .batch_execute("ROLLBACK TO SAVEPOINT unavailable_cell")
        .await?;
    Ok(())
}

#[derive(Debug)]
struct GenesisOutcome {
    database_id: String,
    routing_epoch: i64,
    installed: bool,
}

async fn race_database_genesis(
    database_url: &str,
    database_name: &str,
    left_cells: Vec<i64>,
    right_cells: Vec<i64>,
) -> TestResult<(
    Result<GenesisOutcome, PgError>,
    Result<GenesisOutcome, PgError>,
)> {
    let (left_client, left_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let (right_client, right_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let (observer, observer_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connections = [
        tokio::spawn(left_connection),
        tokio::spawn(right_connection),
        tokio::spawn(observer_connection),
    ];

    left_client.batch_execute("BEGIN").await?;
    let left = left_client
        .query_one(
            "SELECT logical_database_id::text, routing_epoch, installed \
               FROM pgshard_catalog.install_database_genesis( \
                   $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
               )",
            &[&database_name, &left_cells],
        )
        .await
        .map(|row| GenesisOutcome {
            database_id: row.get(0),
            routing_epoch: row.get(1),
            installed: row.get(2),
        })?;
    let right_pid: i32 = right_client
        .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
        .await?
        .get(0);
    let right_name = database_name.to_owned();
    let right = tokio::spawn(async move {
        right_client
            .query_one(
                "SELECT logical_database_id::text, routing_epoch, installed \
                   FROM pgshard_catalog.install_database_genesis( \
                       $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
                   )",
                &[&right_name, &right_cells],
            )
            .await
            .map(|row| GenesisOutcome {
                database_id: row.get(0),
                routing_epoch: row.get(1),
                installed: row.get(2),
            })
    });

    if !wait_for_backend_lock(&observer, right_pid).await? {
        left_client.batch_execute("ROLLBACK").await?;
        right.abort();
        for connection in connections {
            connection.abort();
        }
        return Err("concurrent genesis call did not wait for the catalog transaction lock".into());
    }

    left_client.batch_execute("COMMIT").await?;
    let right = right.await?;
    for connection in connections {
        connection.abort();
    }
    Ok((Ok(left), right))
}

async fn assert_concurrent_database_genesis_contract(
    client: &Client,
    database_url: &str,
) -> TestResult {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let identical_name = format!("genesis_same_{nonce}");
    let (left, right) =
        race_database_genesis(database_url, &identical_name, vec![0_i64], vec![0_i64]).await?;
    let mut identical = [left?, right?];
    identical.sort_by_key(|outcome| outcome.installed);
    assert!(!identical[0].installed);
    assert!(identical[1].installed);
    assert_eq!(identical[0].database_id, identical[1].database_id);
    assert_eq!(identical[0].routing_epoch, identical[1].routing_epoch);

    let next_cell: i64 = client
        .query_one(
            "SELECT coalesce(max(shard_number), 0) + 1 FROM pgshard_catalog.shards",
            &[],
        )
        .await?
        .get(0);
    client
        .execute(
            "INSERT INTO pgshard_catalog.shards(shard_id, shard_number) VALUES ($1::text, $2)",
            &[&format!("genesis-race-{nonce}"), &next_cell],
        )
        .await?;
    let conflicting_name = format!("genesis_conflict_{nonce}");
    let sequence_before: i64 = client
        .query_one(
            "SELECT last_value FROM pgshard_catalog.routing_epochs_routing_epoch_seq",
            &[],
        )
        .await?
        .get(0);
    let (left, right) = race_database_genesis(
        database_url,
        &conflicting_name,
        vec![0_i64, next_cell],
        vec![next_cell, 0_i64],
    )
    .await?;
    let winner = match (left, right) {
        (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => {
            assert!(winner.installed);
            assert_sqlstate(&loser, "22023");
            assert_database_message(
                &loser,
                "logical database genesis topology does not match active routing",
            );
            winner
        }
        (left, right) => {
            return Err(format!("conflicting genesis race outcomes: {left:?}, {right:?}").into());
        }
    };
    let sequence_after: i64 = client
        .query_one(
            "SELECT last_value FROM pgshard_catalog.routing_epochs_routing_epoch_seq",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        sequence_after,
        sequence_before + 1,
        "conflicting loser consumed a routing identity"
    );
    let durable: (i64, i64) = client
        .query_one(
            "SELECT pg_catalog.count(*), min(active.routing_epoch) \
               FROM pgshard_catalog.logical_databases AS databases \
               JOIN pgshard_catalog.active_routing_epochs AS active \
                 ON active.logical_database_id = databases.logical_database_id \
              WHERE databases.database_name = $1::text",
            &[&conflicting_name],
        )
        .await
        .map(|row| (row.get(0), row.get(1)))?;
    assert_eq!(durable, (1, winner.routing_epoch));
    assert_genesis_stage_activation_lock_order(client, database_url).await
}

async fn assert_genesis_stage_activation_lock_order(
    client: &Client,
    database_url: &str,
) -> TestResult {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let database_name = format!("genesis_lock_order_{nonce}");
    let installed = client
        .query_one(
            "SELECT logical_database_id::text, routing_epoch, installed \
               FROM pgshard_catalog.install_database_genesis( \
                   $1::text::pgshard_catalog.sql_identifier, ARRAY[0]::bigint[] \
               )",
            &[&database_name],
        )
        .await?;
    assert!(installed.get::<_, bool>(2));
    let database_id = installed.get::<_, String>(0);
    let active_routing_epoch = installed.get::<_, i64>(1);
    let expected_catalog_epoch = catalog_epoch(client).await?;

    let (staging_client, staging_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let (genesis_client, genesis_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let (observer, observer_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connections = [
        tokio::spawn(staging_connection),
        tokio::spawn(genesis_connection),
        tokio::spawn(observer_connection),
    ];

    staging_client.batch_execute("BEGIN").await?;
    let staging_pid: i32 = staging_client
        .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
        .await?
        .get(0);
    let staged_routing_epoch: i64 = staging_client
        .query_one(
            "INSERT INTO pgshard_catalog.routing_epochs(logical_database_id) \
             VALUES ($1::text::uuid) RETURNING routing_epoch",
            &[&database_id],
        )
        .await?
        .get(0);
    staging_client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges( \
                 logical_database_id, routing_epoch, range_start, range_end, database_shard_id \
             ) SELECT logical_database_id, $1, range_start, range_end, database_shard_id \
                  FROM pgshard_catalog.routing_ranges \
                 WHERE routing_epoch = $2",
            &[&staged_routing_epoch, &active_routing_epoch],
        )
        .await?;

    genesis_client.batch_execute("BEGIN").await?;
    let replay = tokio::time::timeout(
        Duration::from_secs(5),
        genesis_client.query_one(
            "SELECT logical_database_id::text, routing_epoch, installed \
               FROM pgshard_catalog.install_database_genesis( \
                   $1::text::pgshard_catalog.sql_identifier, ARRAY[0]::bigint[] \
               )",
            &[&database_name],
        ),
    )
    .await
    .map_err(|_| "database genesis blocked behind staged-epoch foreign-key lock")??;
    assert_eq!(replay.get::<_, String>(0), database_id);
    assert_eq!(replay.get::<_, i64>(1), active_routing_epoch);
    assert!(!replay.get::<_, bool>(2));

    let activation_database_id = database_id.clone();
    let activation = tokio::spawn(async move {
        let resulting_catalog_epoch: i64 = staging_client
            .query_one(
                "SELECT pgshard_catalog.activate_routing_epoch( \
                     $1::text::uuid, $2, $3, $4 \
                 )",
                &[
                    &activation_database_id,
                    &staged_routing_epoch,
                    &active_routing_epoch,
                    &expected_catalog_epoch,
                ],
            )
            .await?
            .get(0);
        staging_client.batch_execute("COMMIT").await?;
        Ok::<i64, PgError>(resulting_catalog_epoch)
    });

    if !wait_for_backend_lock(&observer, staging_pid).await? {
        genesis_client.batch_execute("ROLLBACK").await?;
        activation.abort();
        for connection in connections {
            connection.abort();
        }
        return Err("staged activation did not wait for genesis catalog lock".into());
    }

    genesis_client.batch_execute("COMMIT").await?;
    let resulting_catalog_epoch = tokio::time::timeout(Duration::from_secs(5), activation)
        .await
        .map_err(|_| "staged activation remained blocked after genesis commit")???;
    assert!(resulting_catalog_epoch >= staged_routing_epoch);
    for connection in connections {
        connection.abort();
    }
    Ok(())
}

async fn assert_legacy_catalog_owner_upgrade(client: &Client, database_url: &str) -> TestResult {
    client
        .batch_execute("DROP SCHEMA pgshard_catalog CASCADE")
        .await?;
    drop_catalog_roles(client).await?;
    assert_non_superuser_v049_owner_is_rejected(client).await?;
    assert_bootstrap_superuser_v049_owner_upgrade(client).await?;
    client
        .batch_execute("DROP SCHEMA pgshard_catalog CASCADE")
        .await?;
    drop_catalog_roles(client).await?;
    assert_distinct_superuser_v049_owner_upgrade(client, database_url).await
}

async fn assert_distinct_superuser_v049_owner_upgrade(
    client: &Client,
    database_url: &str,
) -> TestResult {
    let legacy_owner = format!("pgshard_legacy_owner_{}", Uuid::new_v4().simple());
    let runtime_reader = format!("pgshard_runtime_reader_{}", Uuid::new_v4().simple());
    let runtime_admin = format!("pgshard_runtime_admin_{}", Uuid::new_v4().simple());
    let legacy_grantee = format!("pgshard_legacy_grantee_{}", Uuid::new_v4().simple());
    let fixture_roles = [
        &runtime_reader,
        &runtime_admin,
        &legacy_grantee,
        &legacy_owner,
    ];
    let role_setup = client
        .batch_execute(&format!(
            "CREATE ROLE {legacy_owner} NOLOGIN SUPERUSER; \
             CREATE ROLE {runtime_reader} NOLOGIN; \
             CREATE ROLE {runtime_admin} NOLOGIN; \
             CREATE ROLE {legacy_grantee} NOLOGIN; \
             GRANT CREATE ON DATABASE shardschema TO {legacy_owner}; \
             SET ROLE {legacy_owner}"
        ))
        .await;
    if let Err(error) = role_setup {
        let cleanup = cleanup_legacy_upgrade_fixture(client, &fixture_roles, true).await;
        cleanup?;
        return Err(error.into());
    }
    let fixture_install: TestResult = async {
        client.batch_execute(V0_49_0_MIGRATION_SQL).await?;
        client.batch_execute("RESET ROLE").await?;
        Ok(())
    }
    .await;
    if let Err(error) = fixture_install {
        let cleanup = cleanup_legacy_upgrade_fixture(client, &fixture_roles, true).await;
        cleanup?;
        return Err(error);
    }

    let mut upgrade_committed = false;
    let upgrade_result: TestResult = async {
        assert_external_catalog_trigger_rejections(client, database_url).await?;
        client
            .batch_execute(&format!(
                "GRANT pgshard_catalog_reader TO {legacy_owner} WITH ADMIN OPTION; \
                 GRANT pgshard_catalog_admin TO {legacy_owner} WITH ADMIN OPTION; \
                 GRANT pgshard_catalog_reader TO {runtime_reader} \
                     WITH ADMIN FALSE, INHERIT FALSE, SET TRUE; \
                 GRANT pgshard_catalog_reader TO {runtime_reader} \
                     WITH ADMIN FALSE, INHERIT TRUE, SET FALSE \
                     GRANTED BY {legacy_owner}; \
                 GRANT pgshard_catalog_admin TO {runtime_admin} \
                     WITH ADMIN FALSE, INHERIT FALSE, SET TRUE \
                     GRANTED BY {legacy_owner}; \
                 SET ROLE {legacy_owner}; \
                 CREATE TYPE pgshard_catalog.legacy_composite AS (value integer); \
                 CREATE PROCEDURE pgshard_catalog.legacy_procedure() \
                     LANGUAGE plpgsql \
                     SECURITY DEFINER \
                     SET search_path = pg_catalog, pgshard_catalog, pg_temp \
                     AS 'BEGIN NULL; END'; \
                 GRANT ALL PRIVILEGES ON SCHEMA pgshard_catalog \
                     TO pgshard_catalog_reader WITH GRANT OPTION; \
                 GRANT UPDATE, TRUNCATE ON pgshard_catalog.cluster_state \
                     TO pgshard_catalog_reader WITH GRANT OPTION; \
                 GRANT UPDATE (catalog_epoch) ON pgshard_catalog.cluster_state \
                     TO pgshard_catalog_admin WITH GRANT OPTION; \
                 GRANT EXECUTE ON FUNCTION pgshard_catalog.notify_catalog_state() \
                     TO pgshard_catalog_reader WITH GRANT OPTION; \
                 GRANT EXECUTE ON PROCEDURE pgshard_catalog.legacy_procedure() \
                     TO pgshard_catalog_reader WITH GRANT OPTION; \
                 GRANT USAGE ON TYPE pgshard_catalog.legacy_composite \
                     TO pgshard_catalog_admin WITH GRANT OPTION; \
                 GRANT USAGE ON SCHEMA pgshard_catalog TO {legacy_grantee}; \
                 GRANT SELECT (singleton) ON pgshard_catalog.cluster_state \
                     TO {legacy_grantee}; \
                 RESET ROLE"
            ))
            .await?;
        let epoch_before_upgrade = catalog_epoch(client).await?;
        client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;
        upgrade_committed = true;
        if catalog_epoch(client).await? != epoch_before_upgrade + 1 {
            return Err("owner takeover did not publish the database-shard conversion".into());
        }
        client
            .batch_execute(&format!("ALTER ROLE {legacy_owner} NOSUPERUSER"))
            .await?;
        assert_catalog_owned_by_dedicated_role(client).await?;
        assert_legacy_catalog_access_removed(client, &legacy_owner, &legacy_grantee).await?;
        assert_legacy_memberships_rehomed(client, &legacy_owner, &runtime_reader, &runtime_admin)
            .await?;
        assert_legacy_fixed_role_acls_removed(client).await?;
        Ok(())
    }
    .await;
    let cleanup_result =
        cleanup_legacy_upgrade_fixture(client, &fixture_roles, !upgrade_committed).await;
    upgrade_result?;
    cleanup_result?;
    Ok(())
}

async fn assert_external_catalog_trigger_rejections(
    client: &Client,
    database_url: &str,
) -> TestResult {
    assert_database_event_trigger_is_rejected(client).await?;
    assert_catalog_rewrite_rule_is_rejected(client).await?;
    assert_altered_identity_sequence_is_rejected(client).await?;
    assert_rewound_identity_sequences_are_rejected(client).await?;
    assert_orphaned_restore_lineage_is_rejected(client).await?;
    assert_external_inherited_relation_is_rejected(client).await?;
    assert_catalog_relation_with_external_parent_is_rejected(client).await?;
    assert_pinned_builtin_check_is_rejected(client).await?;
    assert_external_check_function_is_rejected(client).await?;
    assert_disabled_internal_trigger_is_rejected(client).await?;
    assert_same_identity_altered_trigger_is_rejected(client).await?;
    assert_external_executable_trigger_is_rejected(client).await?;
    assert_external_reference_trigger_is_rejected(client).await?;
    assert_concurrent_external_trigger_is_rejected(client, database_url).await
}

async fn assert_external_inherited_relation_is_rejected(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "CREATE SCHEMA pgshard_hostile_inheritance; \
         CREATE TABLE pgshard_hostile_inheritance.registered_tables_child () \
             INHERITS (pgshard_catalog.registered_tables)",
        "pre-existing pgshard_catalog contains external inherited relations",
        &[],
    )
    .await
}

async fn assert_catalog_relation_with_external_parent_is_rejected(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "CREATE SCHEMA pgshard_hostile_parent; \
         CREATE TABLE pgshard_hostile_parent.registered_tables_parent (); \
         ALTER TABLE pgshard_catalog.registered_tables \
             INHERIT pgshard_hostile_parent.registered_tables_parent",
        "pre-existing pgshard_catalog contains external inherited relations",
        &[],
    )
    .await
}

async fn assert_pinned_builtin_check_is_rejected(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "ALTER TABLE pgshard_catalog.shards \
             ADD CONSTRAINT hostile_pinned_builtin_check \
             CHECK (pg_catalog.set_config('search_path', 'pg_catalog', false) = 'pg_catalog') \
             NOT VALID",
        "pre-existing pgshard_catalog contains noncanonical executable relation metadata",
        &[],
    )
    .await
}

async fn assert_external_check_function_is_rejected(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "CREATE SCHEMA pgshard_hostile_metadata; \
         CREATE FUNCTION pgshard_hostile_metadata.execute_as_owner(text) \
             RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER \
             AS 'BEGIN RAISE EXCEPTION ''hostile relation metadata executed''; END'; \
         ALTER TABLE pgshard_catalog.shards \
             ADD CONSTRAINT hostile_external_check \
             CHECK (pgshard_hostile_metadata.execute_as_owner(shard_id::text)) \
             NOT VALID",
        "pre-existing pgshard_catalog contains noncanonical executable relation metadata",
        &[],
    )
    .await
}

async fn assert_migration_pins_search_path(client: &Client) -> TestResult {
    let schema = format!("pgshard_hostile_path_{}", Uuid::new_v4().simple());
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             CREATE FUNCTION {schema}.current_setting(text) RETURNS text \
                 LANGUAGE plpgsql AS \
                 'BEGIN RAISE EXCEPTION ''hostile search_path routine executed''; END'; \
             SET search_path = {schema}, pg_catalog"
        ))
        .await?;

    let migration = client.batch_execute(pgshard_catalog::MIGRATION_SQL).await;
    if migration.is_err() {
        let _ = client.batch_execute("ROLLBACK").await;
    }
    let cleanup = client
        .batch_execute(&format!("RESET search_path; DROP SCHEMA {schema} CASCADE"))
        .await;
    migration?;
    cleanup?;
    Ok(())
}

async fn assert_database_event_trigger_is_rejected(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "CREATE FUNCTION public.pgshard_rejected_event_trigger() \
             RETURNS event_trigger LANGUAGE plpgsql AS \
             'BEGIN NULL; END'; \
         CREATE EVENT TRIGGER pgshard_rejected_event_trigger \
             ON ddl_command_start \
             EXECUTE FUNCTION public.pgshard_rejected_event_trigger()",
        "pre-existing shardschema contains an unsupported event trigger",
        &[],
    )
    .await
}

async fn assert_catalog_rewrite_rule_is_rejected(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "CREATE RULE pgshard_rejected_rule AS \
             ON INSERT TO pgshard_catalog.shards DO INSTEAD NOTHING",
        "pre-existing pgshard_catalog contains an unsupported rewrite rule",
        &[],
    )
    .await
}

async fn assert_altered_identity_sequence_is_rejected(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "ALTER SEQUENCE pgshard_catalog.routing_epochs_routing_epoch_seq \
             INCREMENT BY 2 CYCLE",
        "pre-existing pgshard_catalog contains an unsupported identity sequence",
        &[],
    )
    .await
}

async fn assert_rewound_identity_sequences_are_rejected(client: &Client) -> TestResult {
    let routing_database = Uuid::new_v4();
    let routing_result = assert_catalog_migration_rejection(
        client,
        &format!(
            "INSERT INTO pgshard_catalog.logical_databases( \
                 logical_database_id, database_name \
             ) VALUES ('{routing_database}', 'rewound_routing_{}'); \
             INSERT INTO pgshard_catalog.routing_epochs(logical_database_id) \
             VALUES ('{routing_database}'); \
             SELECT pg_catalog.setval( \
                 'pgshard_catalog.routing_epochs_routing_epoch_seq', \
                 (SELECT pg_catalog.max(routing_epoch) \
                    FROM pgshard_catalog.routing_epochs), \
                 false \
             )",
            routing_database.simple()
        ),
        "pre-existing pgshard_catalog contains unsafe identity sequence progress",
        &[],
    )
    .await;
    let routing_repair = client
        .batch_execute(
            "SELECT pg_catalog.setval( \
                 'pgshard_catalog.routing_epochs_routing_epoch_seq', \
                 GREATEST( \
                     COALESCE( \
                         (SELECT pg_catalog.max(routing_epoch) \
                            FROM pgshard_catalog.routing_epochs), \
                         0 \
                     ) + 1, \
                     1 \
                 ), \
                 false \
             )",
        )
        .await;
    routing_result?;
    routing_repair?;

    let table_database = Uuid::new_v4();
    let table_result = assert_catalog_migration_rejection(
        client,
        &format!(
            "INSERT INTO pgshard_catalog.logical_databases( \
                 logical_database_id, database_name \
             ) VALUES ('{table_database}', 'rewound_table_{}'); \
             INSERT INTO pgshard_catalog.registered_tables( \
                 logical_database_id, schema_name, table_name, \
                 shard_key_column, shard_key_type \
             ) VALUES ( \
                 '{table_database}', 'public', 'rewound_table', 'id', 'bigint' \
             ); \
             SELECT pg_catalog.setval( \
                 'pgshard_catalog.registered_tables_registered_table_id_seq', \
                 (SELECT pg_catalog.max(registered_table_id) \
                    FROM pgshard_catalog.registered_tables), \
                 false \
             )",
            table_database.simple()
        ),
        "pre-existing pgshard_catalog contains unsafe identity sequence progress",
        &[],
    )
    .await;
    let table_repair = client
        .batch_execute(
            "SELECT pg_catalog.setval( \
                 'pgshard_catalog.registered_tables_registered_table_id_seq', \
                 GREATEST( \
                     COALESCE( \
                         (SELECT pg_catalog.max(registered_table_id) \
                            FROM pgshard_catalog.registered_tables), \
                         0 \
                     ) + 1, \
                     1 \
                 ), \
                 false \
             )",
        )
        .await;
    table_result?;
    table_repair?;
    assert_exhausted_identity_sequences_are_rejected(client).await
}

async fn assert_exhausted_identity_sequences_are_rejected(client: &Client) -> TestResult {
    for (sequence_name, relation_name, column_name) in [
        (
            "routing_epochs_routing_epoch_seq",
            "routing_epochs",
            "routing_epoch",
        ),
        (
            "registered_tables_registered_table_id_seq",
            "registered_tables",
            "registered_table_id",
        ),
    ] {
        let exhausted_result = assert_catalog_migration_rejection(
            client,
            &format!(
                "SELECT pg_catalog.setval( \
                     'pgshard_catalog.{sequence_name}', \
                     (SELECT sequences.seqmax \
                        FROM pg_catalog.pg_sequence AS sequences \
                       WHERE sequences.seqrelid = \
                             'pgshard_catalog.{sequence_name}'::pg_catalog.regclass), \
                     true \
                 )"
            ),
            "pre-existing pgshard_catalog contains unsafe identity sequence progress",
            &[],
        )
        .await;
        let exhausted_repair = client
            .batch_execute(&format!(
                "SELECT pg_catalog.setval( \
                     'pgshard_catalog.{sequence_name}', \
                     GREATEST( \
                         COALESCE( \
                             (SELECT pg_catalog.max({column_name}) \
                                FROM pgshard_catalog.{relation_name}), \
                             0 \
                         ) + 1, \
                         1 \
                     ), \
                     false \
                 )"
            ))
            .await;
        exhausted_result?;
        exhausted_repair?;
    }
    Ok(())
}

async fn assert_orphaned_restore_lineage_is_rejected(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "ALTER TABLE pgshard_catalog.shard_restore_incarnations \
             DISABLE TRIGGER ALL; \
         INSERT INTO pgshard_catalog.shard_restore_incarnations( \
             restore_incarnation, shard_id, state \
         ) VALUES ( \
             '30000000-0000-0000-0000-000000000001', \
             'orphaned-restore-shard', \
             'active' \
         ); \
         ALTER TABLE pgshard_catalog.shard_restore_incarnations \
             ENABLE TRIGGER ALL",
        "pre-existing pgshard_catalog contains invalid restore lineage",
        &[],
    )
    .await
}

async fn assert_disabled_internal_trigger_is_rejected(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "DO $pgshard_disable_internal_trigger$ \
         DECLARE \
             internal_trigger name; \
         BEGIN \
             SELECT triggers.tgname \
               INTO STRICT internal_trigger \
               FROM pg_catalog.pg_trigger AS triggers \
               JOIN pg_catalog.pg_class AS relations ON relations.oid = triggers.tgrelid \
              WHERE relations.oid = 'pgshard_catalog.routing_ranges'::pg_catalog.regclass \
                AND triggers.tgisinternal \
              ORDER BY triggers.oid \
              LIMIT 1; \
             EXECUTE pg_catalog.format( \
                 'ALTER TABLE pgshard_catalog.routing_ranges DISABLE TRIGGER %I', \
                 internal_trigger \
             ); \
         END \
         $pgshard_disable_internal_trigger$",
        "pre-existing pgshard_catalog contains an unsupported attached trigger",
        &[],
    )
    .await
}

async fn assert_same_identity_altered_trigger_is_rejected(client: &Client) -> TestResult {
    let role = format!("pgshard_trigger_replace_role_{}", Uuid::new_v4().simple());
    assert_catalog_migration_rejection(
        client,
        &format!(
            "CREATE ROLE {role} NOLOGIN; \
             GRANT USAGE ON SCHEMA pgshard_catalog TO {role}; \
             GRANT TRIGGER ON pgshard_catalog.cluster_state TO {role}; \
             GRANT EXECUTE ON FUNCTION pgshard_catalog.notify_catalog_state() TO {role}; \
             SET ROLE {role}; \
             CREATE OR REPLACE TRIGGER cluster_state_notify \
                 AFTER UPDATE ON pgshard_catalog.cluster_state \
                 FOR EACH ROW WHEN (false) \
                 EXECUTE FUNCTION pgshard_catalog.notify_catalog_state('unexpected'); \
             RESET ROLE"
        ),
        "pre-existing pgshard_catalog contains an unsupported attached trigger",
        &[&role],
    )
    .await
}

async fn assert_external_executable_trigger_is_rejected(client: &Client) -> TestResult {
    let role = format!("pgshard_trigger_role_{}", Uuid::new_v4().simple());
    let schema = format!("pgshard_trigger_schema_{}", Uuid::new_v4().simple());
    assert_catalog_migration_rejection(
        client,
        &format!(
            "CREATE ROLE {role} NOLOGIN; \
             CREATE SCHEMA {schema} AUTHORIZATION {role}; \
             GRANT USAGE ON SCHEMA pgshard_catalog TO {role}; \
             GRANT TRIGGER ON pgshard_catalog.cluster_state TO {role}; \
             SET ROLE {role}; \
             CREATE FUNCTION {schema}.observe_catalog_write() RETURNS trigger \
                 LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'; \
             CREATE TRIGGER external_catalog_write \
                 BEFORE UPDATE ON pgshard_catalog.cluster_state \
                 FOR EACH ROW EXECUTE FUNCTION {schema}.observe_catalog_write(); \
             RESET ROLE"
        ),
        "pre-existing pgshard_catalog contains an unsupported attached trigger",
        &[&role],
    )
    .await
}

async fn assert_external_reference_trigger_is_rejected(client: &Client) -> TestResult {
    let role = format!("pgshard_reference_role_{}", Uuid::new_v4().simple());
    let schema = format!("pgshard_reference_schema_{}", Uuid::new_v4().simple());
    assert_catalog_migration_rejection(
        client,
        &format!(
            "CREATE ROLE {role} NOLOGIN; \
             CREATE SCHEMA {schema} AUTHORIZATION {role}; \
             GRANT USAGE ON SCHEMA pgshard_catalog TO {role}; \
             GRANT REFERENCES (singleton) \
                 ON pgshard_catalog.cluster_state TO {role}; \
             SET ROLE {role}; \
             CREATE TABLE {schema}.catalog_reference ( \
                 singleton boolean PRIMARY KEY \
                     REFERENCES pgshard_catalog.cluster_state(singleton) \
             ); \
             RESET ROLE"
        ),
        "pre-existing pgshard_catalog contains an unsupported attached trigger",
        &[&role],
    )
    .await
}

async fn assert_concurrent_external_trigger_is_rejected(
    client: &Client,
    database_url: &str,
) -> TestResult {
    let role = format!("pgshard_trigger_race_role_{}", Uuid::new_v4().simple());
    let schema = format!("pgshard_trigger_race_schema_{}", Uuid::new_v4().simple());
    client
        .batch_execute(&format!(
            "CREATE ROLE {role} NOLOGIN; \
             CREATE SCHEMA {schema} AUTHORIZATION {role}; \
             GRANT USAGE ON SCHEMA pgshard_catalog TO {role}; \
             GRANT TRIGGER ON pgshard_catalog.cluster_state TO {role}; \
             SET ROLE {role}; \
             CREATE FUNCTION {schema}.observe_catalog_write() RETURNS trigger \
                 LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'; \
             RESET ROLE"
        ))
        .await?;

    let concurrent_result =
        run_concurrent_external_trigger_rejection(database_url, &role, &schema).await;

    let cleanup_result = client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; \
             DROP OWNED BY {role}; \
             DROP ROLE IF EXISTS {role}"
        ))
        .await;
    concurrent_result?;
    cleanup_result?;
    Ok(())
}

async fn run_concurrent_external_trigger_rejection(
    database_url: &str,
    role: &str,
    schema: &str,
) -> TestResult {
    let (attacker, attacker_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let attacker_connection_task = tokio::spawn(attacker_connection);
    let (migration_client, migration_connection) =
        tokio_postgres::connect(database_url, NoTls).await?;
    let migration_connection_task = tokio::spawn(migration_connection);

    let test_result: TestResult = async {
        begin_concurrent_external_trigger(&attacker, &migration_client, role, schema).await?;
        let lock_result = run_bounded_concurrent_migration(
            &migration_client,
            Duration::from_secs(2),
            "catalog migration waited for a concurrent catalog relation lock",
        )
        .await?;
        let rollback_result = rollback_concurrent_migration_connection(
            &migration_client,
            "migration before lock rejection retry",
        )
        .await;
        let lock_error = match lock_result {
            Ok(()) => {
                rollback_result?;
                return Err(
                    "catalog migration accepted an uncommitted attached-trigger transaction".into(),
                );
            }
            Err(error) => error,
        };
        rollback_result?;
        let lock_sqlstate = lock_error
            .as_db_error()
            .map(|database_error| database_error.code().code());
        if lock_sqlstate != Some("55P03") {
            return Err(format!("unexpected concurrent-lock rejection: {lock_error}").into());
        }

        attacker.batch_execute("COMMIT").await?;
        let migration_result = run_bounded_concurrent_migration(
            &migration_client,
            Duration::from_secs(10),
            "catalog migration retry did not finish after catalog traffic quiesced",
        )
        .await?;
        let rollback_result = rollback_concurrent_migration_connection(
            &migration_client,
            "migration after committed-trigger retry",
        )
        .await;
        let migration_error = match migration_result {
            Ok(()) => {
                rollback_result?;
                return Err("migration accepted a concurrently committed trigger".into());
            }
            Err(error) => error,
        };
        rollback_result?;
        let rejection_sqlstate = migration_error
            .as_db_error()
            .map(|database_error| database_error.code().code());
        let rejection_message = migration_error
            .as_db_error()
            .map(tokio_postgres::error::DbError::message);
        if rejection_sqlstate != Some("42501")
            || rejection_message
                != Some("pre-existing pgshard_catalog contains an unsupported attached trigger")
        {
            return Err(
                format!("unexpected concurrent-trigger rejection: {migration_error}").into(),
            );
        }
        let default_isolation: String = migration_client
            .query_one(
                "SELECT pg_catalog.current_setting('default_transaction_isolation')",
                &[],
            )
            .await?
            .get(0);
        if default_isolation != "repeatable read" {
            return Err("migration changed the session's default isolation".into());
        }
        Ok(())
    }
    .await;

    let cleanup_result = cleanup_concurrent_migration_connections(
        attacker,
        migration_client,
        attacker_connection_task,
        migration_connection_task,
    )
    .await;
    test_result?;
    cleanup_result
}

async fn begin_concurrent_external_trigger(
    attacker: &Client,
    migration_client: &Client,
    role: &str,
    schema: &str,
) -> TestResult {
    migration_client
        .batch_execute(
            "SET SESSION CHARACTERISTICS AS TRANSACTION \
             ISOLATION LEVEL REPEATABLE READ",
        )
        .await?;
    attacker
        .batch_execute(&format!(
            "BEGIN; \
             SET LOCAL ROLE {role}; \
             CREATE TRIGGER concurrent_catalog_write \
                 BEFORE UPDATE ON pgshard_catalog.cluster_state \
                 FOR EACH ROW EXECUTE FUNCTION {schema}.observe_catalog_write()"
        ))
        .await?;
    Ok(())
}

async fn cleanup_concurrent_migration_connections(
    attacker: Client,
    migration_client: Client,
    attacker_connection_task: JoinHandle<Result<(), PgError>>,
    migration_connection_task: JoinHandle<Result<(), PgError>>,
) -> TestResult {
    let attacker_cleanup = rollback_concurrent_migration_connection(&attacker, "attacker").await;
    let migration_cleanup =
        rollback_concurrent_migration_connection(&migration_client, "migration").await;
    drop(attacker);
    drop(migration_client);
    attacker_connection_task.abort();
    migration_connection_task.abort();
    let _ = attacker_connection_task.await;
    let _ = migration_connection_task.await;
    attacker_cleanup?;
    migration_cleanup
}

async fn run_bounded_concurrent_migration(
    client: &Client,
    query_timeout: Duration,
    timeout_message: &str,
) -> TestResult<Result<(), PgError>> {
    let result = tokio::time::timeout(
        query_timeout,
        client.batch_execute(pgshard_catalog::MIGRATION_SQL),
    )
    .await;
    let Ok(result) = result else {
        let cancellation = tokio::time::timeout(
            Duration::from_secs(5),
            client.cancel_token().cancel_query(NoTls),
        )
        .await
        .map_err(|_| format!("{timeout_message}; query cancellation timed out"))?;
        cancellation.map_err(|error| format!("{timeout_message}; cancellation failed: {error}"))?;
        return Err(timeout_message.to_owned().into());
    };
    Ok(result)
}

async fn rollback_concurrent_migration_connection(
    client: &Client,
    connection_name: &str,
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), client.batch_execute("ROLLBACK"))
        .await
        .map_err(|_| format!("{connection_name} connection rollback timed out"))??;
    Ok(())
}

struct V049DatabaseShardFixture<'a> {
    bootstrap_role: &'a str,
    runtime_reader: &'a str,
    runtime_admin: &'a str,
    database_name: &'a str,
    first_active_shard_id: &'a str,
    second_active_shard_id: &'a str,
    historical_shard_id: &'a str,
}

async fn stage_ambiguous_v049_epoch(
    client: &Client,
    logical_database_id: &str,
    shard_id: &str,
) -> TestResult<i64> {
    let routing_epoch = stage_epoch(client, logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges( \
                 routing_epoch, range_start, range_end, shard_id \
             ) VALUES \
                 ($1, 0, 9223372036854775808, $2::text), \
                 ($1, 9223372036854775808, 18446744073709551616, $2::text)",
            &[&routing_epoch, &shard_id],
        )
        .await?;
    Ok(routing_epoch)
}

async fn install_v049_database_shard_fixture(
    client: &Client,
    fixture: &V049DatabaseShardFixture<'_>,
) -> TestResult<i64> {
    let bootstrap_role = fixture.bootstrap_role;
    let runtime_reader = fixture.runtime_reader;
    let runtime_admin = fixture.runtime_admin;
    let database_name = fixture.database_name;
    let first_active_shard_id = fixture.first_active_shard_id;
    let second_active_shard_id = fixture.second_active_shard_id;
    let historical_shard_id = fixture.historical_shard_id;
    client
        .batch_execute(&format!(
            "CREATE ROLE {runtime_reader} NOLOGIN; \
             CREATE ROLE {runtime_admin} NOLOGIN; \
             SET ROLE {bootstrap_role}"
        ))
        .await?;
    client.batch_execute(V0_49_0_MIGRATION_SQL).await?;
    let logical_database_id: String = client
        .query_one(
            "INSERT INTO pgshard_catalog.logical_databases(database_name) \
             VALUES ($1::text) RETURNING logical_database_id::text",
            &[&database_name],
        )
        .await?
        .get(0);
    client
        .execute(
            "INSERT INTO pgshard_catalog.shards(shard_id, shard_number) \
             VALUES ($1::text, 2), ($2::text, 1)",
            &[&first_active_shard_id, &second_active_shard_id],
        )
        .await?;
    let historical_epoch = stage_epoch(client, &logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges( \
                 routing_epoch, range_start, range_end, shard_id \
             ) VALUES ($1, 0, 18446744073709551616, $2::text)",
            &[&historical_epoch, &historical_shard_id],
        )
        .await?;
    let observed_catalog_epoch = catalog_epoch(client).await?;
    client
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch( \
                 $1::text::uuid, $2, NULL, $3 \
             )",
            &[
                &logical_database_id,
                &historical_epoch,
                &observed_catalog_epoch,
            ],
        )
        .await?;
    let routing_epoch = stage_epoch(client, &logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges( \
                 routing_epoch, range_start, range_end, shard_id \
             ) VALUES \
                 ($1, 0, 9223372036854775808, $2::text), \
                 ($1, 9223372036854775808, 18446744073709551616, $3::text)",
            &[
                &routing_epoch,
                &first_active_shard_id,
                &second_active_shard_id,
            ],
        )
        .await?;
    let observed_catalog_epoch = catalog_epoch(client).await?;
    client
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch( \
                 $1::text::uuid, $2, $3, $4 \
             )",
            &[
                &logical_database_id,
                &routing_epoch,
                &historical_epoch,
                &observed_catalog_epoch,
            ],
        )
        .await?;
    let ambiguous_epoch =
        stage_ambiguous_v049_epoch(client, &logical_database_id, first_active_shard_id).await?;
    client
        .batch_execute(&format!(
            "GRANT pgshard_catalog_reader TO {runtime_reader} \
                 WITH ADMIN FALSE, INHERIT TRUE, SET FALSE; \
             GRANT pgshard_catalog_admin TO {runtime_admin} \
                 WITH ADMIN FALSE, INHERIT FALSE, SET TRUE; \
             RESET ROLE"
        ))
        .await?;
    Ok(ambiguous_epoch)
}

async fn assert_v049_converted_route_order(client: &Client, database_name: &str) -> TestResult {
    let converted_routes = client
        .query(
            "SELECT database_shards.shard_ordinal, \
                    database_shards.database_shard_id::text, \
                    placements.placement_generation, shards.shard_number, \
                    ranges.range_start::text, ranges.range_end::text \
               FROM pgshard_catalog.logical_databases AS databases \
               JOIN pgshard_catalog.routing_epochs AS epochs \
                 ON epochs.logical_database_id = databases.logical_database_id \
               JOIN pgshard_catalog.routing_ranges AS ranges \
                 ON ranges.logical_database_id = epochs.logical_database_id \
                AND ranges.routing_epoch = epochs.routing_epoch \
               JOIN pgshard_catalog.database_shards AS database_shards \
                 ON database_shards.logical_database_id = ranges.logical_database_id \
                AND database_shards.database_shard_id = ranges.database_shard_id \
               JOIN pgshard_catalog.database_shard_placements AS placements \
                 ON placements.logical_database_id = database_shards.logical_database_id \
                AND placements.database_shard_id = database_shards.database_shard_id \
                AND placements.state = 'active' \
               JOIN pgshard_catalog.shards AS shards \
                 ON shards.shard_id = placements.shard_id \
              WHERE databases.database_name = $1::text \
                AND epochs.state = 'active' \
              ORDER BY ranges.range_start",
            &[&database_name],
        )
        .await?;
    assert_eq!(converted_routes.len(), 2);
    let first_identity: String = converted_routes[0].get(1);
    let second_identity: String = converted_routes[1].get(1);
    assert_ne!(first_identity, second_identity);
    assert_ne!(first_identity, Uuid::nil().to_string());
    assert_ne!(second_identity, Uuid::nil().to_string());
    let expected_routes = [
        (0, 1, 2, "0", "9223372036854775808"),
        (1, 1, 1, "9223372036854775808", "18446744073709551616"),
    ];
    for (route, expected) in converted_routes.iter().zip(expected_routes) {
        assert_eq!(route.get::<_, i64>(0), expected.0);
        assert_eq!(route.get::<_, i64>(2), expected.1);
        assert_eq!(route.get::<_, i64>(3), expected.2);
        assert_eq!(route.get::<_, &str>(4), expected.3);
        assert_eq!(route.get::<_, &str>(5), expected.4);
    }
    Ok(())
}

async fn assert_v049_historical_target(
    client: &Client,
    database_name: &str,
    historical_shard_id: &str,
) -> TestResult {
    let historical_states: (String, String) = client
        .query_one(
            "SELECT database_shards.state, placements.state \
               FROM pgshard_catalog.logical_databases AS databases \
               JOIN pgshard_catalog.database_shards AS database_shards \
                 USING (logical_database_id) \
               JOIN pgshard_catalog.database_shard_placements AS placements \
                 USING (logical_database_id, database_shard_id) \
              WHERE databases.database_name = $1::text \
                AND placements.shard_id = $2::text",
            &[&database_name, &historical_shard_id],
        )
        .await
        .map(|row| (row.get(0), row.get(1)))?;
    assert_eq!(historical_states, ("retired".into(), "superseded".into()));
    Ok(())
}

async fn assert_v049_genesis_replay(client: &Client, database_name: &str) -> TestResult {
    let genesis_replay = client
        .query_one(
            "SELECT logical_database_id::text, routing_epoch, installed \
               FROM pgshard_catalog.install_database_genesis( \
                   $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
               )",
            &[&database_name, &vec![2_i64, 1_i64]],
        )
        .await?;
    assert!(!genesis_replay.get::<_, bool>(2));
    Ok(())
}

async fn assert_v049_database_shard_conversion(
    client: &Client,
    database_name: &str,
    historical_shard_id: &str,
    epoch_after_upgrade: i64,
    runtime_reader: &str,
    runtime_admin: &str,
) -> TestResult {
    assert_v049_converted_route_order(client, database_name).await?;
    assert_v049_historical_target(client, database_name, historical_shard_id).await?;
    let legacy_target_columns: i64 = client
        .query_one(
            "SELECT count(*) \
               FROM pg_catalog.pg_attribute AS attributes \
              WHERE attributes.attrelid = \
                        'pgshard_catalog.routing_ranges'::regclass \
                AND attributes.attname = 'shard_id' \
                AND attributes.attnum > 0 \
                AND NOT attributes.attisdropped",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(legacy_target_columns, 0);
    assert_v049_genesis_replay(client, database_name).await?;
    client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;
    if catalog_epoch(client).await? != epoch_after_upgrade {
        return Err("database-shard conversion replay mutated catalog state".into());
    }
    for state in ["draining", "retired"] {
        client
            .execute(
                "UPDATE pgshard_catalog.shards SET state = $2::text \
                 WHERE shard_id = $1::text",
                &[&historical_shard_id, &state],
            )
            .await?;
    }
    assert_catalog_owned_by_dedicated_role(client).await?;
    assert_bootstrap_memberships_preserved(client, runtime_reader, runtime_admin).await?;
    Ok(())
}

async fn assert_v049_unavailable_live_target_rejected(
    client: &Client,
    shard_id: &str,
) -> TestResult {
    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let lineage_retire_result = client
        .execute(
            "UPDATE pgshard_catalog.shard_restore_incarnations \
                SET state = 'retired', retired_at = statement_timestamp() \
              WHERE shard_id = $1::text AND state = 'active'",
            &[&shard_id],
        )
        .await;
    let retire_result = client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'retired' WHERE shard_id = $1::text",
            &[&shard_id],
        )
        .await;
    let reset_result = client
        .batch_execute("SET session_replication_role = origin")
        .await;
    lineage_retire_result?;
    retire_result?;
    reset_result?;

    let epoch_before_rejection = catalog_epoch(client).await?;
    let error = client
        .batch_execute(pgshard_catalog::MIGRATION_SQL)
        .await
        .expect_err("legacy live routing to a retired physical shard was upgraded");
    client.batch_execute("ROLLBACK").await?;
    assert_database_message(
        &error,
        "legacy live routing references an unavailable physical shard",
    );
    assert_sqlstate(&error, "55000");
    if catalog_epoch(client).await? != epoch_before_rejection {
        return Err("rejected unavailable-target conversion mutated catalog state".into());
    }

    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let activate_result = client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'active' WHERE shard_id = $1::text",
            &[&shard_id],
        )
        .await;
    let lineage_activate_result = client
        .execute(
            "UPDATE pgshard_catalog.shard_restore_incarnations \
                SET state = 'active', retired_at = NULL \
              WHERE shard_id = $1::text AND state = 'retired'",
            &[&shard_id],
        )
        .await;
    let reset_result = client
        .batch_execute("SET session_replication_role = origin")
        .await;
    activate_result?;
    lineage_activate_result?;
    reset_result?;
    Ok(())
}

async fn assert_v049_unavailable_staged_target_rejected(
    client: &Client,
    database_name: &str,
) -> TestResult {
    let staged_only_shard_id = format!("legacy-staged-{}", Uuid::new_v4().simple());
    let logical_database_id: String = client
        .query_one(
            "SELECT logical_database_id::text FROM pgshard_catalog.logical_databases \
              WHERE database_name = $1::text",
            &[&database_name],
        )
        .await?
        .get(0);
    let staged_only_shard_number: i64 = client
        .query_one(
            "SELECT pg_catalog.max(shard_number) + 1 FROM pgshard_catalog.shards",
            &[],
        )
        .await?
        .get(0);
    client
        .execute(
            "INSERT INTO pgshard_catalog.shards(shard_id, shard_number) VALUES ($1::text, $2)",
            &[&staged_only_shard_id, &staged_only_shard_number],
        )
        .await?;
    let staged_only_epoch = stage_epoch(client, &logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges( \
                 routing_epoch, range_start, range_end, shard_id \
             ) VALUES ($1, 0, 18446744073709551616, $2::text)",
            &[&staged_only_epoch, &staged_only_shard_id],
        )
        .await?;
    assert_v049_unavailable_live_target_rejected(client, &staged_only_shard_id).await?;
    client
        .execute(
            "DELETE FROM pgshard_catalog.routing_ranges WHERE routing_epoch = $1",
            &[&staged_only_epoch],
        )
        .await?;
    client
        .execute(
            "DELETE FROM pgshard_catalog.routing_epochs WHERE routing_epoch = $1",
            &[&staged_only_epoch],
        )
        .await?;
    Ok(())
}

async fn assert_bootstrap_superuser_v049_owner_upgrade(client: &Client) -> TestResult {
    let runtime_reader = format!("pgshard_bootstrap_reader_{}", Uuid::new_v4().simple());
    let runtime_admin = format!("pgshard_bootstrap_admin_{}", Uuid::new_v4().simple());
    let database_name = format!("legacy_route_{}", Uuid::new_v4().simple());
    let first_active_shard_id = format!("legacy-cell-a-{}", Uuid::new_v4().simple());
    let second_active_shard_id = format!("legacy-cell-b-{}", Uuid::new_v4().simple());
    let historical_shard_id = "shard-0000".to_owned();
    let fixture_roles = [&runtime_reader, &runtime_admin];
    let bootstrap_role: String = client
        .query_one(
            "SELECT pg_catalog.quote_ident(rolname) \
               FROM pg_catalog.pg_roles \
              WHERE oid = 10 AND rolsuper",
            &[],
        )
        .await?
        .get(0);

    let fixture = V049DatabaseShardFixture {
        bootstrap_role: &bootstrap_role,
        runtime_reader: &runtime_reader,
        runtime_admin: &runtime_admin,
        database_name: &database_name,
        first_active_shard_id: &first_active_shard_id,
        second_active_shard_id: &second_active_shard_id,
        historical_shard_id: &historical_shard_id,
    };
    let ambiguous_epoch = match install_v049_database_shard_fixture(client, &fixture).await {
        Ok(epoch) => epoch,
        Err(error) => {
            cleanup_legacy_upgrade_fixture(client, &fixture_roles, true).await?;
            return Err(error);
        }
    };

    let mut upgrade_committed = false;
    let upgrade_result: TestResult = async {
        assert_bootstrap_memberships_preserved(client, &runtime_reader, &runtime_admin).await?;
        assert_v049_unavailable_live_target_rejected(client, &first_active_shard_id).await?;
        assert_v049_unavailable_staged_target_rejected(client, &database_name).await?;
        let epoch_before_rejection = catalog_epoch(client).await?;
        let error = client
            .batch_execute(pgshard_catalog::MIGRATION_SQL)
            .await
            .expect_err("ambiguous live physical-target reuse was upgraded");
        client.batch_execute("ROLLBACK").await?;
        assert_sqlstate(&error, "55000");
        assert_database_message(
            &error,
            "ambiguous legacy live routing reuses a physical target within one epoch",
        );
        if catalog_epoch(client).await? != epoch_before_rejection {
            return Err("rejected database-shard conversion mutated catalog state".into());
        }
        client
            .execute(
                "DELETE FROM pgshard_catalog.routing_ranges WHERE routing_epoch = $1",
                &[&ambiguous_epoch],
            )
            .await?;
        client
            .execute(
                "DELETE FROM pgshard_catalog.routing_epochs WHERE routing_epoch = $1",
                &[&ambiguous_epoch],
            )
            .await?;
        let epoch_before_upgrade = catalog_epoch(client).await?;
        client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;
        upgrade_committed = true;
        let epoch_after_upgrade = catalog_epoch(client).await?;
        if epoch_after_upgrade != epoch_before_upgrade + 1 {
            return Err(
                "bootstrap-owner takeover did not publish the database-shard conversion".into(),
            );
        }
        assert_v049_database_shard_conversion(
            client,
            &database_name,
            &historical_shard_id,
            epoch_after_upgrade,
            &runtime_reader,
            &runtime_admin,
        )
        .await
    }
    .await;
    let cleanup_result =
        cleanup_legacy_upgrade_fixture(client, &fixture_roles, !upgrade_committed).await;
    upgrade_result?;
    cleanup_result?;
    Ok(())
}

async fn assert_bootstrap_memberships_preserved(
    client: &Client,
    runtime_reader: &str,
    runtime_admin: &str,
) -> TestResult {
    let rows = client
        .query(
            "SELECT granted.rolname, members.rolname, memberships.grantor, \
                    memberships.admin_option, memberships.inherit_option, \
                    memberships.set_option \
               FROM pg_catalog.pg_auth_members AS memberships \
               JOIN pg_catalog.pg_roles AS granted \
                 ON granted.oid = memberships.roleid \
               JOIN pg_catalog.pg_roles AS members \
                 ON members.oid = memberships.member \
              WHERE granted.rolname IN ( \
                        'pgshard_catalog_reader', 'pgshard_catalog_admin' \
                    ) \
                AND members.rolname = ANY($1::text[]) \
              ORDER BY granted.rolname, members.rolname",
            &[&vec![runtime_reader, runtime_admin]],
        )
        .await?;
    if rows.len() != 2 {
        return Err("bootstrap-owned runtime memberships were not preserved".into());
    }
    let admin_row = &rows[0];
    let admin_matches = admin_row.get::<_, &str>(0) == "pgshard_catalog_admin"
        && admin_row.get::<_, &str>(1) == runtime_admin
        && admin_row.get::<_, u32>(2) == 10
        && !admin_row.get::<_, bool>(3)
        && !admin_row.get::<_, bool>(4)
        && admin_row.get::<_, bool>(5);
    if !admin_matches {
        return Err("bootstrap-owned administrator membership changed".into());
    }
    let reader_row = &rows[1];
    let reader_matches = reader_row.get::<_, &str>(0) == "pgshard_catalog_reader"
        && reader_row.get::<_, &str>(1) == runtime_reader
        && reader_row.get::<_, u32>(2) == 10
        && !reader_row.get::<_, bool>(3)
        && reader_row.get::<_, bool>(4)
        && !reader_row.get::<_, bool>(5);
    if !reader_matches {
        return Err("bootstrap-owned reader membership changed".into());
    }
    Ok(())
}

async fn assert_non_superuser_v049_owner_is_rejected(client: &Client) -> TestResult {
    let legacy_owner = format!("pgshard_untrusted_owner_{}", Uuid::new_v4().simple());
    let fixture_roles = [&legacy_owner];
    let role_setup = client
        .batch_execute(&format!(
            "CREATE ROLE {legacy_owner} NOLOGIN CREATEROLE; \
             GRANT CREATE ON DATABASE shardschema TO {legacy_owner}; \
             SET ROLE {legacy_owner}"
        ))
        .await;
    if let Err(error) = role_setup {
        let cleanup = cleanup_legacy_upgrade_fixture(client, &fixture_roles, true).await;
        cleanup?;
        return Err(error.into());
    }
    let fixture_install: TestResult = async {
        client.batch_execute(V0_49_0_MIGRATION_SQL).await?;
        client.batch_execute("RESET ROLE").await?;
        Ok(())
    }
    .await;
    if let Err(error) = fixture_install {
        let cleanup = cleanup_legacy_upgrade_fixture(client, &fixture_roles, true).await;
        cleanup?;
        return Err(error);
    }

    let rejection_result: TestResult = async {
        let released_memberships: i64 = client
            .query_one(
                "SELECT pg_catalog.count(*) \
                   FROM pg_catalog.pg_auth_members AS memberships \
                   JOIN pg_catalog.pg_roles AS granted \
                     ON granted.oid = memberships.roleid \
                   JOIN pg_catalog.pg_roles AS members \
                     ON members.oid = memberships.member \
                   JOIN pg_catalog.pg_roles AS grantors \
                     ON grantors.oid = memberships.grantor \
                  WHERE ( \
                        members.rolname = $1 \
                        AND granted.rolname IN ( \
                            'pgshard_catalog_reader', 'pgshard_catalog_admin' \
                        ) \
                        AND memberships.admin_option \
                    ) OR ( \
                        granted.rolname = 'pgshard_catalog_reader' \
                        AND members.rolname = 'pgshard_catalog_admin' \
                        AND grantors.rolname = $1 \
                    )",
                &[&legacy_owner],
            )
            .await?
            .get(0);
        if released_memberships != 3 {
            return Err("v0.49 role grant chain changed".into());
        }

        let migration = client.batch_execute(pgshard_catalog::MIGRATION_SQL).await;
        let rollback_result = client.batch_execute("ROLLBACK").await;
        let error = match migration {
            Ok(()) => {
                rollback_result?;
                return Err("an untrusted released owner was promoted".into());
            }
            Err(error) => error,
        };
        let actual_sqlstate = error
            .as_db_error()
            .map(|database_error| database_error.code().code());
        let actual_message = error
            .as_db_error()
            .map(tokio_postgres::error::DbError::message);
        rollback_result?;
        if actual_sqlstate != Some("42501")
            || actual_message
                != Some("pre-existing pgshard_catalog schema owner must be a superuser")
        {
            return Err(format!("unexpected untrusted-owner rejection: {error}").into());
        }
        Ok(())
    }
    .await;
    let cleanup_result = cleanup_legacy_upgrade_fixture(client, &fixture_roles, true).await;
    rejection_result?;
    cleanup_result?;
    Ok(())
}

async fn cleanup_legacy_upgrade_fixture(
    client: &Client,
    fixture_roles: &[&String],
    remove_catalog: bool,
) -> TestResult {
    let mut failures = Vec::new();
    if let Err(error) = client
        .batch_execute("ROLLBACK; RESET ROLE; RESET SESSION AUTHORIZATION")
        .await
    {
        failures.push(format!("reset legacy fixture session: {error}"));
    }
    if remove_catalog
        && let Err(error) = client
            .batch_execute("DROP SCHEMA IF EXISTS pgshard_catalog CASCADE")
            .await
    {
        failures.push(format!("drop legacy fixture schema: {error}"));
    }
    for role_name in fixture_roles {
        if let Err(error) = client
            .batch_execute(&format!("DROP OWNED BY {role_name}"))
            .await
        {
            failures.push(format!("drop objects owned by {role_name}: {error}"));
        }
    }
    if remove_catalog && let Err(error) = drop_catalog_roles(client).await {
        failures.push(format!("drop legacy fixture catalog roles: {error}"));
    }
    for role_name in fixture_roles {
        if let Err(error) = client
            .batch_execute(&format!("DROP ROLE IF EXISTS {role_name}"))
            .await
        {
            failures.push(format!("drop legacy fixture role {role_name}: {error}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

async fn assert_catalog_owned_by_dedicated_role(client: &Client) -> TestResult {
    let mismatched_owners: i64 = client
        .query_one(
            "WITH catalog_schema AS ( \
                 SELECT oid FROM pg_catalog.pg_namespace \
                  WHERE nspname = 'pgshard_catalog' \
             ), object_owners AS ( \
                 SELECT relowner AS owner FROM pg_catalog.pg_class, catalog_schema \
                  WHERE relnamespace = catalog_schema.oid \
                 UNION ALL \
                 SELECT proowner FROM pg_catalog.pg_proc, catalog_schema \
                  WHERE pronamespace = catalog_schema.oid \
                 UNION ALL \
                 SELECT typowner FROM pg_catalog.pg_type, catalog_schema \
                  WHERE typnamespace = catalog_schema.oid \
                 UNION ALL \
                 SELECT collowner FROM pg_catalog.pg_collation, catalog_schema \
                  WHERE collnamespace = catalog_schema.oid \
             ) \
             SELECT count(*) \
               FROM object_owners, pg_catalog.pg_roles AS owners \
              WHERE owners.oid = object_owners.owner \
                AND owners.rolname <> 'pgshard_catalog_owner'",
            &[],
        )
        .await?
        .get(0);
    if mismatched_owners != 0 {
        return Err("catalog objects did not transfer to the dedicated owner".into());
    }
    let schema_owner: String = client
        .query_one(
            "SELECT owners.rolname \
               FROM pg_catalog.pg_namespace AS namespaces \
               JOIN pg_catalog.pg_roles AS owners ON owners.oid = namespaces.nspowner \
              WHERE namespaces.nspname = 'pgshard_catalog'",
            &[],
        )
        .await?
        .get(0);
    if schema_owner != "pgshard_catalog_owner" {
        return Err(format!("catalog schema retained owner {schema_owner}").into());
    }
    Ok(())
}

async fn assert_legacy_catalog_access_removed(
    client: &Client,
    legacy_owner: &str,
    legacy_grantee: &str,
) -> TestResult {
    let legacy_memberships: i64 = client
        .query_one(
            "SELECT count(*) \
               FROM pg_catalog.pg_auth_members AS memberships \
               JOIN pg_catalog.pg_roles AS members ON members.oid = memberships.member \
               JOIN pg_catalog.pg_roles AS granted ON granted.oid = memberships.roleid \
              WHERE members.rolname = $1 \
                AND granted.rolname IN ( \
                    'pgshard_catalog_reader', 'pgshard_catalog_admin' \
                )",
            &[&legacy_owner],
        )
        .await?
        .get(0);
    if legacy_memberships != 0 {
        return Err("legacy owner retained a fixed catalog role membership".into());
    }
    let legacy_access: bool = client
        .query_one(
            "SELECT pg_catalog.has_schema_privilege($1, 'pgshard_catalog', 'USAGE') \
                 OR pg_catalog.has_schema_privilege($2, 'pgshard_catalog', 'USAGE') \
                 OR pg_catalog.has_column_privilege( \
                     $2, 'pgshard_catalog.cluster_state', 'singleton', 'SELECT' \
                 )",
            &[&legacy_owner, &legacy_grantee],
        )
        .await?
        .get(0);
    if legacy_access {
        return Err("legacy principal retained catalog access".into());
    }
    let owner_can_see_backend_generation: bool = client
        .query_one(
            "SELECT pgshard_catalog.managed_slot_backend_identity_live( \
                 pg_catalog.pg_backend_pid(), \
                 (SELECT backend_start FROM pg_catalog.pg_stat_activity \
                   WHERE pid = pg_catalog.pg_backend_pid()), \
                 pg_catalog.pg_postmaster_start_time() \
             )",
            &[],
        )
        .await?
        .get(0);
    if !owner_can_see_backend_generation {
        return Err("catalog owner cannot validate exact backend generations".into());
    }
    Ok(())
}

async fn assert_legacy_memberships_rehomed(
    client: &Client,
    legacy_owner: &str,
    runtime_reader: &str,
    runtime_admin: &str,
) -> TestResult {
    let rows = client
        .query(
            "SELECT granted.rolname, members.rolname, grantors.rolname, \
                    memberships.admin_option, memberships.inherit_option, \
                    memberships.set_option \
               FROM pg_catalog.pg_auth_members AS memberships \
               JOIN pg_catalog.pg_roles AS granted \
                 ON granted.oid = memberships.roleid \
               JOIN pg_catalog.pg_roles AS members \
                 ON members.oid = memberships.member \
               JOIN pg_catalog.pg_roles AS grantors \
                 ON grantors.oid = memberships.grantor \
              WHERE granted.rolname IN ( \
                        'pgshard_catalog_reader', 'pgshard_catalog_admin' \
                    ) \
                AND members.rolname = ANY($1::text[]) \
              ORDER BY granted.rolname, members.rolname",
            &[&vec![legacy_owner, runtime_reader, runtime_admin]],
        )
        .await?;
    if rows.len() != 2 {
        return Err("legacy creator memberships were not removed".into());
    }
    let admin_row = &rows[0];
    let admin_matches = admin_row.get::<_, &str>(0) == "pgshard_catalog_admin"
        && admin_row.get::<_, &str>(1) == runtime_admin
        && admin_row.get::<_, &str>(2) != legacy_owner
        && !admin_row.get::<_, bool>(3)
        && !admin_row.get::<_, bool>(4)
        && admin_row.get::<_, bool>(5);
    if !admin_matches {
        return Err("runtime administrator membership was not re-homed exactly".into());
    }
    let reader_row = &rows[1];
    let reader_matches = reader_row.get::<_, &str>(0) == "pgshard_catalog_reader"
        && reader_row.get::<_, &str>(1) == runtime_reader
        && reader_row.get::<_, &str>(2) != legacy_owner
        && !reader_row.get::<_, bool>(3)
        && reader_row.get::<_, bool>(4)
        && reader_row.get::<_, bool>(5);
    if !reader_matches {
        return Err("runtime reader membership was not re-homed exactly".into());
    }
    Ok(())
}

async fn assert_legacy_fixed_role_acls_removed(client: &Client) -> TestResult {
    let unsafe_access: bool = client
        .query_one(
            "SELECT pg_catalog.has_schema_privilege( \
                        'pgshard_catalog_reader', 'pgshard_catalog', 'CREATE' \
                    ) \
                 OR pg_catalog.has_table_privilege( \
                        'pgshard_catalog_reader', \
                        'pgshard_catalog.cluster_state', \
                        'UPDATE' \
                    ) \
                 OR pg_catalog.has_table_privilege( \
                        'pgshard_catalog_reader', \
                        'pgshard_catalog.cluster_state', \
                        'TRUNCATE' \
                    ) \
                 OR pg_catalog.has_column_privilege( \
                        'pgshard_catalog_admin', \
                        'pgshard_catalog.cluster_state', \
                        'catalog_epoch', \
                        'UPDATE' \
                    ) \
                 OR pg_catalog.has_function_privilege( \
                        'pgshard_catalog_reader', \
                        'pgshard_catalog.notify_catalog_state()', \
                        'EXECUTE' \
                    ) \
                 OR pg_catalog.has_function_privilege( \
                        'pgshard_catalog_reader', \
                        'pgshard_catalog.legacy_procedure()', \
                        'EXECUTE' \
                    ) \
                 OR EXISTS ( \
                        SELECT \
                          FROM pg_catalog.pg_type AS types \
                          CROSS JOIN LATERAL pg_catalog.aclexplode(types.typacl) AS acl \
                         WHERE types.oid = \
                                   'pgshard_catalog.legacy_composite'::pg_catalog.regtype \
                           AND acl.grantee = \
                                   'pgshard_catalog_admin'::pg_catalog.regrole \
                    )",
            &[],
        )
        .await?
        .get(0);
    if unsafe_access {
        return Err("legacy fixed-role ACLs survived takeover".into());
    }
    let composite_owner: String = client
        .query_one(
            "SELECT owners.rolname \
               FROM pg_catalog.pg_type AS types \
               JOIN pg_catalog.pg_roles AS owners ON owners.oid = types.typowner \
              WHERE types.oid = \
                        'pgshard_catalog.legacy_composite'::pg_catalog.regtype",
            &[],
        )
        .await?
        .get(0);
    if composite_owner != "pgshard_catalog_owner" {
        return Err("standalone composite type retained its legacy owner".into());
    }
    Ok(())
}

async fn assert_catalog_role_bootstrap_rejections(client: &Client) -> TestResult {
    assert_catalog_migration_rejection(
        client,
        "ALTER ROLE pgshard_catalog_reader LOGIN",
        "pre-existing pgshard_catalog_reader role has unsafe attributes",
        &[],
    )
    .await?;

    assert_unsupported_schema_object_is_rejected(client).await?;

    let delegable_member = format!("pgshard_delegable_{}", Uuid::new_v4().simple());
    assert_catalog_migration_rejection(
        client,
        &format!(
            "CREATE ROLE {delegable_member} NOLOGIN; \
             GRANT pgshard_catalog_reader TO {delegable_member} WITH ADMIN OPTION"
        ),
        "pre-existing pgshard catalog role has a delegable membership",
        &[&delegable_member],
    )
    .await?;

    assert_catalog_inheritance_rejections(client).await?;

    let owner_member = format!("pgshard_owner_member_{}", Uuid::new_v4().simple());
    assert_catalog_migration_rejection(
        client,
        &format!(
            "CREATE ROLE {owner_member} NOLOGIN; \
             GRANT pgshard_catalog_owner TO {owner_member}"
        ),
        "pre-existing pgshard_catalog_owner role has a member",
        &[&owner_member],
    )
    .await?;

    assert_catalog_migration_rejection(
        client,
        "ALTER DEFAULT PRIVILEGES FOR ROLE pgshard_catalog_owner \
             IN SCHEMA pgshard_catalog \
             REVOKE SELECT ON TABLES FROM pgshard_catalog_reader; \
         REASSIGN OWNED BY pgshard_catalog_owner TO pgshard_catalog_admin",
        "pre-existing pgshard_catalog schema has an unsafe fixed-role owner",
        &[],
    )
    .await?;

    assert_catalog_migration_rejection(
        client,
        "ALTER DEFAULT PRIVILEGES FOR ROLE pgshard_catalog_owner \
             IN SCHEMA pgshard_catalog \
             REVOKE SELECT ON TABLES FROM pgshard_catalog_reader; \
         REASSIGN OWNED BY pgshard_catalog_owner TO postgres; \
         REVOKE pgshard_catalog_reader FROM pgshard_catalog_admin; \
         DROP OWNED BY pgshard_catalog_admin; \
         DROP ROLE pgshard_catalog_admin",
        "legacy pgshard_catalog schema requires both released fixed roles",
        &[],
    )
    .await?;

    let mixed_owner = format!("pgshard_mixed_owner_{}", Uuid::new_v4().simple());
    let mixed_table = format!("owner_mismatch_{}", Uuid::new_v4().simple());
    assert_catalog_migration_rejection(
        client,
        &format!(
            "CREATE ROLE {mixed_owner} NOLOGIN; \
             CREATE TABLE pgshard_catalog.{mixed_table}(value integer); \
             ALTER TABLE pgshard_catalog.{mixed_table} OWNER TO {mixed_owner}"
        ),
        "pre-existing pgshard_catalog objects must share the schema owner",
        &[&mixed_owner],
    )
    .await?;

    let default_grantee = format!("pgshard_default_grantee_{}", Uuid::new_v4().simple());
    assert_catalog_migration_rejection(
        client,
        &format!(
            "CREATE ROLE {default_grantee} NOLOGIN; \
             ALTER DEFAULT PRIVILEGES FOR ROLE pgshard_catalog_owner \
                 IN SCHEMA pgshard_catalog \
                 GRANT INSERT ON TABLES TO {default_grantee}"
        ),
        "pre-existing pgshard_catalog default privileges do not match the released boundary",
        &[&default_grantee],
    )
    .await?;
    Ok(())
}

async fn assert_unsupported_schema_object_is_rejected(client: &Client) -> TestResult {
    let unsupported_routine = format!("legacy_operator_eq_{}", Uuid::new_v4().simple());
    assert_catalog_migration_rejection(
        client,
        &format!(
            "SET ROLE pgshard_catalog_owner; \
             CREATE FUNCTION pgshard_catalog.{unsupported_routine}( \
                 left_value integer, right_value integer \
             ) RETURNS boolean \
                 LANGUAGE SQL IMMUTABLE \
                 AS 'SELECT left_value = right_value'; \
             CREATE OPERATOR pgshard_catalog.=== ( \
                 LEFTARG = integer, \
                 RIGHTARG = integer, \
                 FUNCTION = pgshard_catalog.{unsupported_routine} \
             ); \
             RESET ROLE"
        ),
        "pre-existing pgshard_catalog contains an unsupported schema object",
        &[],
    )
    .await
}

async fn assert_catalog_inheritance_rejections(client: &Client) -> TestResult {
    for (setup_sql, case_name) in [
        (
            "GRANT pg_read_all_stats TO pgshard_catalog_reader",
            "reader",
        ),
        (
            "GRANT pg_read_all_stats TO pgshard_catalog_admin",
            "administrator",
        ),
        ("GRANT pg_signal_backend TO pgshard_catalog_owner", "owner"),
    ] {
        assert_catalog_migration_rejection(
            client,
            setup_sql,
            "pre-existing pgshard catalog role inherits an unexpected role",
            &[],
        )
        .await
        .map_err(|error| format!("{case_name} inheritance rejection: {error}"))?;
    }
    Ok(())
}

async fn assert_catalog_migration_rejection(
    client: &Client,
    setup_sql: &str,
    expected_message: &str,
    extra_roles: &[&String],
) -> TestResult {
    let epoch_before = catalog_epoch(client).await?;
    if let Err(error) = client.batch_execute(&format!("BEGIN; {setup_sql}")).await {
        let rollback = client.batch_execute("ROLLBACK").await;
        rollback?;
        return Err(error.into());
    }
    let migration = client.batch_execute(pgshard_catalog::MIGRATION_SQL).await;
    let rollback = client.batch_execute("ROLLBACK").await;
    match migration {
        Ok(()) => {
            restore_catalog_after_unexpected_acceptance(client, extra_roles).await?;
            return Err(format!("migration accepted rejected state: {expected_message}").into());
        }
        Err(error) => {
            let actual_sqlstate = error
                .as_db_error()
                .map(|database_error| database_error.code().code().to_owned());
            let actual_message = error
                .as_db_error()
                .map(|database_error| database_error.message().to_owned());
            rollback?;
            if actual_sqlstate.as_deref() != Some("42501") {
                return Err(
                    format!("unexpected rejection SQLSTATE {actual_sqlstate:?}: {error}").into(),
                );
            }
            if actual_message.as_deref() != Some(expected_message) {
                return Err(format!(
                    "unexpected rejection message {actual_message:?}, expected {expected_message}"
                )
                .into());
            }
        }
    }
    if catalog_epoch(client).await? != epoch_before {
        return Err("rejected migration changed the catalog epoch".into());
    }
    for role_name in extra_roles {
        let role_survived: bool = client
            .query_one(
                "SELECT pg_catalog.to_regrole($1::text) IS NOT NULL",
                &[role_name],
            )
            .await?
            .get(0);
        if role_survived {
            return Err(format!("rejected migration retained role {role_name}").into());
        }
    }
    Ok(())
}

async fn restore_catalog_after_unexpected_acceptance(
    client: &Client,
    extra_roles: &[&String],
) -> TestResult {
    client.batch_execute("ROLLBACK; RESET ROLE").await?;
    client
        .batch_execute("DROP SCHEMA IF EXISTS pgshard_catalog CASCADE")
        .await?;
    for role_name in extra_roles {
        client
            .batch_execute(&format!("DROP OWNED BY {role_name}"))
            .await?;
    }
    drop_catalog_roles(client).await?;
    for role_name in extra_roles {
        client
            .batch_execute(&format!("DROP ROLE IF EXISTS {role_name}"))
            .await?;
    }
    client.batch_execute(pgshard_catalog::MIGRATION_SQL).await?;
    Ok(())
}

async fn assert_restricted_catalog_migration_is_rejected(client: &Client) -> TestResult {
    let role_name = format!("pgshard_migration_test_{}", Uuid::new_v4().simple());
    client
        .batch_execute(&format!(
            "CREATE ROLE {role_name} NOLOGIN CREATEROLE; SET ROLE {role_name}"
        ))
        .await?;
    let migration = client.batch_execute(pgshard_catalog::MIGRATION_SQL).await;
    let rollback_result = client.batch_execute("ROLLBACK").await;
    let reset_result = client.batch_execute("RESET ROLE").await;
    let drop_result = client
        .batch_execute(&format!("DROP ROLE IF EXISTS {role_name}"))
        .await;

    let error = migration.expect_err("catalog bootstrap must reject a non-superuser owner");
    assert_sqlstate(&error, "42501");
    assert_database_message(
        &error,
        "pgshard catalog migration requires a superuser bootstrap principal",
    );
    rollback_result?;
    reset_result?;
    drop_result?;
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
    let database_shard_id: String = client
        .query_one(
            "INSERT INTO pgshard_catalog.database_shards( \
                 logical_database_id, shard_ordinal, state, activated_at \
             ) VALUES ($1::text::uuid, 0, 'active', statement_timestamp()) \
             RETURNING database_shard_id::text",
            &[&logical_database_id],
        )
        .await?
        .get(0);
    client
        .execute(
            "INSERT INTO pgshard_catalog.database_shard_placements( \
                 logical_database_id, database_shard_id, placement_generation, \
                 shard_id, state, activated_at \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, 1, $3::text, \
                 'active', statement_timestamp() \
             )",
            &[&logical_database_id, &database_shard_id, &shard_id],
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
        database_shard_id,
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

async fn complete_slot_sync_probe_retirement<C>(
    client: &C,
    probe_generation: Uuid,
) -> Result<u64, PgError>
where
    C: GenericClient + Sync,
{
    let cleanup_receipt_id: String = client
        .query_one(
            "SELECT cleanup_receipt_id::text FROM pgshard_catalog.slot_sync_probes \
             WHERE probe_generation = $1::text::uuid",
            &[&probe_generation.to_string()],
        )
        .await?
        .get(0);
    complete_slot_sync_probe_retirement_with_receipt(
        client,
        probe_generation,
        Uuid::parse_str(&cleanup_receipt_id).expect("catalog receipt is a UUID"),
    )
    .await
}

async fn complete_slot_sync_probe_retirement_with_receipt<C>(
    client: &C,
    probe_generation: Uuid,
    cleanup_receipt_id: Uuid,
) -> Result<u64, PgError>
where
    C: GenericClient + Sync,
{
    let slot_name: String = client
        .query_one(
            "SELECT slot_name::text FROM pgshard_catalog.slot_sync_probes \
             WHERE probe_generation = $1::text::uuid",
            &[&probe_generation.to_string()],
        )
        .await?
        .get(0);
    let fence_id: String = client
        .query_one(
            "SELECT acquired_fence_id::text \
               FROM pgshard_catalog.acquire_managed_slot_target_fence($1::text)",
            &[&slot_name],
        )
        .await?
        .get(0);
    let retirement = client
        .query_one(
            "SELECT pgshard_catalog.complete_slot_sync_probe_retirement( \
                        $1::text::uuid, $2::text, $3::text::uuid, $4::text::uuid \
                    )",
            &[
                &probe_generation.to_string(),
                &slot_name,
                &cleanup_receipt_id.to_string(),
                &fence_id,
            ],
        )
        .await;
    let released: bool = client
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::text, $2::text::uuid \
                    )",
            &[&slot_name, &fence_id],
        )
        .await?
        .get(0);
    assert!(released, "test fixture must release its target fence");
    retirement?;
    Ok(1)
}

async fn begin_managed_slot_creation_attempt<C>(
    client: &C,
    slot_generation: Uuid,
    creation_receipt_id: Uuid,
) -> Result<(), PgError>
where
    C: GenericClient + Sync,
{
    client
        .query_one(
            "WITH candidates AS ( \
                 SELECT probes.probe_generation AS slot_generation, \
                        probes.slot_name::text AS slot_name, \
                        'primary-anchor'::text AS slot_role, \
                        probes.system_identifier, probes.database_oid, \
                        probes.source_timeline, probes.restore_incarnation \
                   FROM pgshard_catalog.slot_sync_probes AS probes \
                  WHERE probes.probe_generation = $1::text::uuid \
                 UNION ALL \
                 SELECT slots.slot_generation, slots.slot_name::text, slots.slot_role, \
                        attachments.system_identifier, attachments.database_oid, \
                        attachments.selected_source_timeline, attachments.restore_incarnation \
                   FROM pgshard_catalog.managed_replication_slots AS slots \
                   JOIN pgshard_catalog.logical_consumer_attachments AS attachments \
                     ON attachments.attachment_generation = slots.attachment_generation \
                  WHERE slots.slot_generation = $1::text::uuid \
             ) \
             SELECT pgshard_catalog.begin_managed_slot_creation_attempt( \
                        candidates.slot_generation, candidates.slot_name, \
                        candidates.slot_role, candidates.system_identifier, \
                        candidates.database_oid, candidates.source_timeline, \
                        candidates.restore_incarnation, state.catalog_epoch, \
                        $2::text::uuid \
                    ) \
               FROM candidates \
               CROSS JOIN pgshard_catalog.cluster_state AS state \
              WHERE state.singleton",
            &[
                &slot_generation.to_string(),
                &creation_receipt_id.to_string(),
            ],
        )
        .await?;
    Ok(())
}

async fn activate_managed_replication_slot<C>(
    client: &C,
    slot_generation: Uuid,
    creation_receipt_id: Uuid,
    consistent_point: &str,
    two_phase_at: &str,
) -> Result<(), PgError>
where
    C: GenericClient + Sync,
{
    client
        .query_one(
            "SELECT pgshard_catalog.activate_managed_replication_slot( \
                        $1::text::uuid, $2::text::uuid, \
                        $3::text::pg_lsn, $4::text::pg_lsn \
                    )",
            &[
                &slot_generation.to_string(),
                &creation_receipt_id.to_string(),
                &consistent_point,
                &two_phase_at,
            ],
        )
        .await?;
    Ok(())
}

async fn complete_managed_replication_slot_retirement<C>(
    client: &C,
    slot_generation: Uuid,
    creation_receipt_id: Uuid,
) -> Result<(), PgError>
where
    C: GenericClient + Sync,
{
    let slot_name: String = client
        .query_one(
            "SELECT slot_name::text \
               FROM pgshard_catalog.managed_replication_slots \
              WHERE slot_generation = $1::text::uuid",
            &[&slot_generation.to_string()],
        )
        .await?
        .get(0);
    let fence_id: String = client
        .query_one(
            "SELECT acquired_fence_id::text \
               FROM pgshard_catalog.acquire_managed_slot_target_fence($1::text)",
            &[&slot_name],
        )
        .await?
        .get(0);
    let retirement = client
        .query_one(
            "SELECT pgshard_catalog.complete_managed_replication_slot_retirement( \
                        $1::text::uuid, $2::text, $3::text::uuid, $4::text::uuid \
                    )",
            &[
                &slot_generation.to_string(),
                &slot_name,
                &creation_receipt_id.to_string(),
                &fence_id,
            ],
        )
        .await;
    let released: bool = client
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::text, $2::text::uuid \
                    )",
            &[&slot_name, &fence_id],
        )
        .await?
        .get(0);
    assert!(released, "test fixture must release its target fence");
    retirement?;
    Ok(())
}

struct SlotSyncProbeFixture {
    generation: Uuid,
    receipt_id: Uuid,
    replacement_generation: Uuid,
    replacement_name: String,
}

async fn assert_active_probe_receipt_immutable(
    client: &Client,
    probe: &SlotSyncProbeFixture,
) -> TestResult {
    let error = client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET creation_receipt_id = gen_random_uuid() \
             WHERE probe_generation = $1::text::uuid",
            &[&probe.generation.to_string()],
        )
        .await
        .expect_err("an active probe cannot rewrite its creation receipt identity");
    assert_sqlstate(&error, "55000");
    Ok(())
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
        receipt_id: fixture_uuid(fixture.nonce, 94),
        replacement_generation,
        replacement_name,
    })
}

async fn assert_slot_sync_probe_activation(
    client: &Client,
    fixture: &Fixture,
    probe: &SlotSyncProbeFixture,
) -> TestResult {
    client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'draining' WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await?;
    let error = begin_managed_slot_creation_attempt(client, probe.generation, probe.receipt_id)
        .await
        .expect_err("a draining shard cannot begin a managed slot creation");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "managed slot allocation is not eligible for creation",
    );
    client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'active' WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await?;
    begin_managed_slot_creation_attempt(client, probe.generation, probe.receipt_id).await?;
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
        .query_one(
            "SELECT pgshard_catalog.activate_slot_sync_probe( \
                        $1::text::uuid, $2::text::uuid, '0/10'::pg_lsn \
                    )",
            &[&probe.generation.to_string(), &probe.receipt_id.to_string()],
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
    assert_active_probe_receipt_immutable(client, probe).await?;
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

    assert_probe_blocks_parent_retirement(client, fixture).await
}

async fn assert_probe_blocks_parent_retirement(client: &Client, fixture: &Fixture) -> TestResult {
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
    let error = client
        .query_one(
            "SELECT pgshard_catalog.begin_slot_sync_probe_retirement( \
                        $1::text::uuid, gen_random_uuid(), '0/10'::pg_lsn \
                    )",
            &[&probe.generation.to_string()],
        )
        .await
        .expect_err("active cleanup must use its exact creation receipt identity");
    assert_sqlstate(&error, "55000");
    client
        .query_one(
            "SELECT pgshard_catalog.begin_slot_sync_probe_retirement( \
                        $1::text::uuid, $2::text::uuid, '0/10'::pg_lsn \
                    )",
            &[&probe.generation.to_string(), &probe.receipt_id.to_string()],
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
    complete_slot_sync_probe_retirement(client, probe.generation).await?;
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
        .query_one(
            "SELECT pgshard_catalog.begin_slot_sync_probe_retirement( \
                        $1::text::uuid, gen_random_uuid(), '0/1'::pg_lsn \
                    )",
            &[&probe.replacement_generation.to_string()],
        )
        .await?;
    complete_slot_sync_probe_retirement(client, probe.replacement_generation).await?;
    Ok(())
}

async fn assert_slot_sync_probe_contract(client: &Client, fixture: &Fixture) -> TestResult {
    let probe = allocate_slot_sync_probe_fixture(client, fixture).await?;
    assert_slot_sync_probe_activation(client, fixture, &probe).await?;
    assert_slot_sync_probe_retirement(client, fixture, &probe).await
}

async fn assert_migration_rejects_active_shard_without_restore(
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
    let replay = client
        .batch_execute(pgshard_catalog::MIGRATION_SQL)
        .await
        .expect_err("an active shard without an active restore must block migration replay");
    client.batch_execute("ROLLBACK").await?;
    assert_sqlstate(&replay, "42501");
    assert_database_message(
        &replay,
        "pre-existing pgshard_catalog contains invalid restore lineage",
    );
    assert_eq!(
        catalog_epoch(client).await?,
        epoch_before_replay,
        "rejected migration replay mutated catalog state after restore retirement"
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
        "rejected migration replay resurrected retired WAL history"
    );
    assert_eq!(
        retired_count, 1,
        "rejected migration replay rewrote restore history"
    );
    client
        .execute(
            "INSERT INTO pgshard_catalog.shard_restore_incarnations( \
                 restore_incarnation, shard_id \
             ) VALUES (gen_random_uuid(), $1::text)",
            &[&shard_id],
        )
        .await?;
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
    assert_catalog_role_boundary(client).await?;

    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let transaction = client.transaction().await?;
    transaction
        .batch_execute("SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
    transaction
        .batch_execute("SAVEPOINT hidden_creation_attempt_ledger")
        .await?;
    let error = transaction
        .query_one(
            "SELECT count(*) FROM pgshard_catalog.managed_slot_creation_attempts",
            &[],
        )
        .await
        .expect_err("catalog roles must not read managed-slot capability receipts");
    assert_sqlstate(&error, "42501");
    transaction
        .batch_execute("ROLLBACK TO SAVEPOINT hidden_creation_attempt_ledger")
        .await?;
    transaction
        .batch_execute("SAVEPOINT hidden_probe_receipts")
        .await?;
    let error = transaction
        .query_one(
            "SELECT creation_receipt_id, cleanup_receipt_id \
               FROM pgshard_catalog.slot_sync_probes LIMIT 1",
            &[],
        )
        .await
        .expect_err("catalog roles must not read slot-sync probe receipt capabilities");
    assert_sqlstate(&error, "42501");
    transaction
        .batch_execute("ROLLBACK TO SAVEPOINT hidden_probe_receipts")
        .await?;
    assert_hidden_target_registry(&transaction).await?;
    let unknown_receipt_state: Option<String> = transaction
        .query_one(
            "SELECT pgshard_catalog.managed_slot_creation_attempt_state( \
                        $1::text::uuid, 'unknown_slot', $2::text::uuid \
                    )",
            &[&Uuid::new_v4().to_string(), &Uuid::new_v4().to_string()],
        )
        .await?
        .get(0);
    assert_eq!(unknown_receipt_state, None);
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

async fn assert_catalog_role_boundary(client: &Client) -> TestResult {
    let login_roles: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_roles \
              WHERE rolname IN ( \
                  'pgshard_catalog_owner', \
                  'pgshard_catalog_reader', \
                  'pgshard_catalog_admin' \
              ) AND rolcanlogin",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(login_roles, 0, "catalog group roles must remain NOLOGIN");
    let owner_members: i64 = client
        .query_one(
            "SELECT count(*) \
               FROM pg_catalog.pg_auth_members AS memberships \
               JOIN pg_catalog.pg_roles AS granted ON granted.oid = memberships.roleid \
              WHERE granted.rolname = 'pgshard_catalog_owner'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(owner_members, 0, "catalog owner must not be assumable");
    Ok(())
}

async fn assert_hidden_target_registry(
    transaction: &tokio_postgres::Transaction<'_>,
) -> TestResult {
    transaction
        .batch_execute("SAVEPOINT hidden_target_fences")
        .await?;
    let error = transaction
        .query_one(
            "SELECT fence_id FROM pgshard_catalog.managed_slot_target_fences LIMIT 1",
            &[],
        )
        .await
        .expect_err("catalog roles must not read target-fence capabilities");
    assert_sqlstate(&error, "42501");
    transaction
        .batch_execute("ROLLBACK TO SAVEPOINT hidden_target_fences")
        .await?;
    transaction
        .batch_execute("SAVEPOINT raw_probe_lifecycle")
        .await?;
    let error = transaction
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes SET state = state WHERE false",
            &[],
        )
        .await
        .expect_err("catalog admin must use receipt-authorized probe lifecycle functions");
    assert_sqlstate(&error, "42501");
    transaction
        .batch_execute("ROLLBACK TO SAVEPOINT raw_probe_lifecycle")
        .await?;
    Ok(())
}

async fn assert_catalog_reader_cannot_seize_target_fence(database_url: &str) -> TestResult {
    let target_name = format!("reader_fence_{}", Uuid::new_v4().simple());
    let (reader, reader_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let reader_task = tokio::spawn(reader_connection);
    reader
        .batch_execute("SET ROLE pgshard_catalog_reader")
        .await?;
    let denied = reader
        .query_one(
            "SELECT * FROM pgshard_catalog.acquire_managed_slot_target_fence($1::text)",
            &[&target_name],
        )
        .await
        .expect_err("catalog readers must not acquire managed target fences");
    assert_sqlstate(&denied, "42501");

    let (controller, controller_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let controller_task = tokio::spawn(controller_connection);
    controller
        .batch_execute("SET ROLE pgshard_catalog_admin")
        .await?;
    let fence_id: String = controller
        .query_one(
            "SELECT acquired_fence_id::text \
               FROM pgshard_catalog.acquire_managed_slot_target_fence($1::text)",
            &[&target_name],
        )
        .await?
        .get(0);
    let released: bool = controller
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::text, $2::text::uuid \
                    )",
            &[&target_name, &fence_id],
        )
        .await?
        .get(0);
    assert!(released, "controller must release its hidden random fence");
    drop(controller);
    tokio::time::timeout(Duration::from_secs(5), controller_task).await???;

    drop(reader);
    tokio::time::timeout(Duration::from_secs(5), reader_task).await???;
    Ok(())
}

async fn assert_stale_target_fence_backend_generation_is_reclaimed(
    database_url: &str,
) -> TestResult {
    let target_name = format!("stale_fence_{}", Uuid::new_v4().simple());
    let (holder, holder_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let holder_task = tokio::spawn(holder_connection);
    holder
        .batch_execute("SET ROLE pgshard_catalog_admin")
        .await?;
    let stale_fence_id = acquire_target_fence(&holder, &target_name).await?;
    drop(holder);
    tokio::time::timeout(Duration::from_secs(5), holder_task).await???;

    let (controller, controller_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let controller_task = tokio::spawn(controller_connection);
    let controller_pid: i32 = controller
        .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
        .await?
        .get(0);
    controller
        .execute(
            "UPDATE pgshard_catalog.managed_slot_target_fences \
                SET owner_pid = $2 \
              WHERE target_name::text = $1::text",
            &[&target_name, &controller_pid],
        )
        .await?;
    controller
        .batch_execute("SET ROLE pgshard_catalog_admin")
        .await?;
    let replacement_fence_id = acquire_target_fence(&controller, &target_name).await?;
    assert_ne!(replacement_fence_id, stale_fence_id);
    let released: bool = controller
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::text, $2::text::uuid \
                    )",
            &[&target_name, &replacement_fence_id],
        )
        .await?
        .get(0);
    assert!(released);
    drop(controller);
    tokio::time::timeout(Duration::from_secs(5), controller_task).await???;
    Ok(())
}

async fn assert_admin_write_path(client: &mut Client) -> TestResult {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let transaction = client.transaction().await?;
    transaction
        .batch_execute("SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
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
    let genesis = transaction
        .query_one(
            "SELECT logical_database_id::text, routing_epoch \
               FROM pgshard_catalog.install_database_genesis( \
                   $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
               )",
            &[&format!("admin_route_{nonce}"), &vec![shard_number]],
        )
        .await?;
    let logical_database_id: String = genesis.get(0);
    let active_routing_epoch: i64 = genesis.get(1);
    let database_shard_id: String = transaction
        .query_one(
            "SELECT database_shard_id::text \
               FROM pgshard_catalog.database_shards \
              WHERE logical_database_id = $1::text::uuid \
                AND shard_ordinal = 0",
            &[&logical_database_id],
        )
        .await?
        .get(0);
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
    assert_admin_routing_write_path(
        &transaction,
        nonce,
        &logical_database_id,
        &database_shard_id,
        active_routing_epoch,
    )
    .await?;
    transaction.rollback().await?;
    Ok(())
}

async fn assert_admin_routing_write_path(
    transaction: &tokio_postgres::Transaction<'_>,
    nonce: u128,
    logical_database_id: &str,
    database_shard_id: &str,
    active_routing_epoch: i64,
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
             (logical_database_id, routing_epoch, range_start, range_end, database_shard_id) \
             VALUES ($1::text::uuid, $2, 0, 18446744073709551616, $3::text::uuid)",
            &[&logical_database_id, &routing_epoch, &database_shard_id],
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
                &Some(active_routing_epoch),
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
    let creation_receipt_id = fixture_uuid(nonce, 105);
    begin_managed_slot_creation_attempt(transaction, probe_generation, creation_receipt_id).await?;
    transaction
        .query_one(
            "SELECT pgshard_catalog.activate_slot_sync_probe( \
                        $1::text::uuid, $2::text::uuid, '0/1'::pg_lsn \
                    )",
            &[
                &probe_generation.to_string(),
                &creation_receipt_id.to_string(),
            ],
        )
        .await?;
    transaction
        .query_one(
            "SELECT pgshard_catalog.begin_slot_sync_probe_retirement( \
                        $1::text::uuid, $2::text::uuid, '0/1'::pg_lsn \
                    )",
            &[
                &probe_generation.to_string(),
                &creation_receipt_id.to_string(),
            ],
        )
        .await?;
    complete_slot_sync_probe_retirement_with_receipt(
        transaction,
        probe_generation,
        creation_receipt_id,
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
    let receipt_id = Uuid::new_v4();
    begin_managed_slot_creation_attempt(transaction, slot_generation, receipt_id).await?;
    activate_managed_replication_slot(transaction, slot_generation, receipt_id, "0/1", "0/1")
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

async fn assert_malformed_active_routing_fails_closed(
    client: &Client,
    database_url: &str,
    fixture: &Fixture,
    active_routing_epoch: i64,
) -> TestResult {
    let staged_routing_epoch = stage_epoch(client, &fixture.logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges( \
                 logical_database_id, routing_epoch, range_start, range_end, database_shard_id \
             ) SELECT logical_database_id, $1, range_start, range_end, database_shard_id \
                  FROM pgshard_catalog.routing_ranges \
                 WHERE routing_epoch = $2",
            &[&staged_routing_epoch, &active_routing_epoch],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.active_routing_epochs \
                SET routing_epoch = $1 \
              WHERE logical_database_id = $2::text::uuid",
            &[&staged_routing_epoch, &fixture.logical_database_id],
        )
        .await?;

    let database_name: String = client
        .query_one(
            "SELECT database_name::text \
               FROM pgshard_catalog.logical_databases \
              WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id],
        )
        .await?
        .get(0);
    let cell_ordinals: Vec<i64> = client
        .query(
            "SELECT DISTINCT shards.shard_number \
               FROM pgshard_catalog.routing_ranges AS ranges \
               JOIN pgshard_catalog.database_shard_placements AS placements \
                 ON placements.logical_database_id = ranges.logical_database_id \
                AND placements.database_shard_id = ranges.database_shard_id \
                AND placements.state = 'active' \
               JOIN pgshard_catalog.shards AS shards ON shards.shard_id = placements.shard_id \
              WHERE ranges.routing_epoch = $1 \
              ORDER BY shards.shard_number",
            &[&staged_routing_epoch],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let error = client
        .query_one(
            "SELECT * FROM pgshard_catalog.install_database_genesis( \
                 $1::text::pgshard_catalog.sql_identifier, $2::bigint[] \
             )",
            &[&database_name, &cell_ordinals],
        )
        .await
        .expect_err("genesis retry must reject a pointer to staged routing");
    assert_database_message(
        &error,
        "logical database genesis does not reference exactly one owned active routing epoch",
    );
    assert_sqlstate(&error, "55000");

    let (reader_client, reader_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let reader_connection_task = tokio::spawn(reader_connection);
    let expected_database_id = DatabaseId::new(Uuid::parse_str(&fixture.logical_database_id)?)?;
    match CatalogReader::subscribe(reader_client, &CatalogCache::new()).await {
        Err(LoadError::InvalidActiveRoutingEpoch { database_id }) => {
            assert_eq!(database_id, expected_database_id);
        }
        Err(error) => return Err(format!("unexpected malformed routing error: {error}").into()),
        Ok(_) => return Err("catalog reader published staged routing as active".into()),
    }
    reader_connection_task.abort();

    client
        .execute(
            "UPDATE pgshard_catalog.active_routing_epochs \
                SET routing_epoch = $1 \
              WHERE logical_database_id = $2::text::uuid",
            &[&active_routing_epoch, &fixture.logical_database_id],
        )
        .await?;
    client
        .execute(
            "DELETE FROM pgshard_catalog.routing_ranges WHERE routing_epoch = $1",
            &[&staged_routing_epoch],
        )
        .await?;
    client
        .execute(
            "DELETE FROM pgshard_catalog.routing_epochs WHERE routing_epoch = $1",
            &[&staged_routing_epoch],
        )
        .await?;

    assert_unavailable_routing_shard_fails_closed(client, database_url, fixture).await
}

async fn assert_unavailable_routing_shard_fails_closed(
    client: &Client,
    database_url: &str,
    fixture: &Fixture,
) -> TestResult {
    let unavailable_shard_id = format!("retired-route-{}", fixture.nonce);
    let unavailable_shard_number: i64 = client
        .query_one(
            "SELECT pg_catalog.max(shard_number) + 1 FROM pgshard_catalog.shards",
            &[],
        )
        .await?
        .get(0);
    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let shard_insert_result = client
        .execute(
            "INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state) \
             VALUES ($1::text, $2, 'retired')",
            &[&unavailable_shard_id, &unavailable_shard_number],
        )
        .await;
    let placement_update_result = client
        .execute(
            "UPDATE pgshard_catalog.database_shard_placements \
                SET shard_id = $3::text \
              WHERE logical_database_id = $1::text::uuid \
                AND database_shard_id = $2::text::uuid \
                AND state = 'active'",
            &[
                &fixture.logical_database_id,
                &fixture.database_shard_id,
                &unavailable_shard_id,
            ],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    shard_insert_result?;
    placement_update_result?;

    let cache = CatalogCache::new();
    let (reader_client, reader_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let reader_connection_task = tokio::spawn(reader_connection);
    let expected_database_id = DatabaseId::new(Uuid::parse_str(&fixture.logical_database_id)?)?;
    match CatalogReader::subscribe(reader_client, &cache).await {
        Err(LoadError::InvalidRoutingShardState {
            database_id,
            shard_number,
            state,
        }) => {
            assert_eq!(database_id, expected_database_id);
            assert_eq!(i64::from(shard_number), unavailable_shard_number);
            assert_eq!(state, "retired");
        }
        Err(error) => return Err(format!("unexpected unavailable route error: {error}").into()),
        Ok(_) => return Err("catalog reader hid an active route to a retired shard".into()),
    }
    assert!(
        cache.current_for_planning().is_err(),
        "malformed unavailable route was published"
    );
    reader_connection_task.abort();

    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let restore_result = client
        .execute(
            "UPDATE pgshard_catalog.database_shard_placements \
                SET shard_id = $3::text \
              WHERE logical_database_id = $1::text::uuid \
                AND database_shard_id = $2::text::uuid \
                AND state = 'active'",
            &[
                &fixture.logical_database_id,
                &fixture.database_shard_id,
                &fixture.shard_id,
            ],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    restore_result?;

    assert_orphaned_routing_shard_fails_closed(client, database_url, fixture).await
}

async fn assert_orphaned_routing_shard_fails_closed(
    client: &Client,
    database_url: &str,
    fixture: &Fixture,
) -> TestResult {
    let orphaned_shard_id = format!("orphan-route-{}", fixture.nonce);
    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let update_result = client
        .execute(
            "UPDATE pgshard_catalog.database_shard_placements \
                SET shard_id = $3::text \
              WHERE logical_database_id = $1::text::uuid \
                AND database_shard_id = $2::text::uuid \
                AND state = 'active'",
            &[
                &fixture.logical_database_id,
                &fixture.database_shard_id,
                &orphaned_shard_id,
            ],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    update_result?;

    let cache = CatalogCache::new();
    let (reader_client, reader_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let reader_connection_task = tokio::spawn(reader_connection);
    let expected_database_id = DatabaseId::new(Uuid::parse_str(&fixture.logical_database_id)?)?;
    match CatalogReader::subscribe(reader_client, &cache).await {
        Err(LoadError::MissingRoutingShard {
            database_id,
            shard_id,
        }) => {
            assert_eq!(database_id, expected_database_id);
            assert_eq!(shard_id, orphaned_shard_id);
        }
        Err(error) => return Err(format!("unexpected orphaned route error: {error}").into()),
        Ok(_) => return Err("catalog reader hid an active route to a missing shard".into()),
    }
    assert!(
        cache.current_for_planning().is_err(),
        "malformed orphaned route was published"
    );
    reader_connection_task.abort();

    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let restore_result = client
        .execute(
            "UPDATE pgshard_catalog.database_shard_placements \
                SET shard_id = $3::text \
              WHERE logical_database_id = $1::text::uuid \
                AND database_shard_id = $2::text::uuid \
                AND state = 'active'",
            &[
                &fixture.logical_database_id,
                &fixture.database_shard_id,
                &fixture.shard_id,
            ],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    restore_result?;
    assert_database_shard_layers_fail_closed(client, database_url, fixture).await
}

async fn assert_database_shard_layers_fail_closed(
    client: &Client,
    database_url: &str,
    fixture: &Fixture,
) -> TestResult {
    assert_missing_database_shard_fails_closed(client, database_url, fixture).await?;
    assert_retired_database_shard_fails_closed(client, database_url, fixture).await?;
    assert_missing_active_placement_fails_closed(client, database_url, fixture).await
}

async fn assert_missing_database_shard_fails_closed(
    client: &Client,
    database_url: &str,
    fixture: &Fixture,
) -> TestResult {
    let missing_database_shard_id = Uuid::new_v4().to_string();
    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let corrupt_result = client
        .execute(
            "UPDATE pgshard_catalog.routing_ranges \
                SET database_shard_id = $2::text::uuid \
              WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id, &missing_database_shard_id],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    corrupt_result?;

    let cache = CatalogCache::new();
    let (reader_client, reader_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let reader_connection_task = tokio::spawn(reader_connection);
    let expected_database_id = DatabaseId::new(Uuid::parse_str(&fixture.logical_database_id)?)?;
    let expected_database_shard_id =
        DatabaseShardId::new(Uuid::parse_str(&missing_database_shard_id)?)?;
    match CatalogReader::subscribe(reader_client, &cache).await {
        Err(LoadError::MissingRoutingDatabaseShard {
            database_id,
            database_shard_id,
        }) => {
            assert_eq!(database_id, expected_database_id);
            assert_eq!(database_shard_id, expected_database_shard_id);
        }
        Err(error) => {
            return Err(format!("unexpected missing database-shard error: {error}").into());
        }
        Ok(_) => return Err("catalog reader hid a missing database-shard identity".into()),
    }
    assert!(cache.current_for_planning().is_err());
    reader_connection_task.abort();

    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let restore_result = client
        .execute(
            "UPDATE pgshard_catalog.routing_ranges \
                SET database_shard_id = $2::text::uuid \
              WHERE logical_database_id = $1::text::uuid",
            &[&fixture.logical_database_id, &fixture.database_shard_id],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    restore_result?;
    Ok(())
}

async fn assert_retired_database_shard_fails_closed(
    client: &Client,
    database_url: &str,
    fixture: &Fixture,
) -> TestResult {
    let expected_database_id = DatabaseId::new(Uuid::parse_str(&fixture.logical_database_id)?)?;
    let expected_database_shard_id =
        DatabaseShardId::new(Uuid::parse_str(&fixture.database_shard_id)?)?;
    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let retire_result = client
        .execute(
            "UPDATE pgshard_catalog.database_shards \
                SET state = 'retired', draining_at = statement_timestamp(), \
                    retired_at = statement_timestamp() \
              WHERE logical_database_id = $1::text::uuid \
                AND database_shard_id = $2::text::uuid",
            &[&fixture.logical_database_id, &fixture.database_shard_id],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    retire_result?;

    let cache = CatalogCache::new();
    let (reader_client, reader_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let reader_connection_task = tokio::spawn(reader_connection);
    match CatalogReader::subscribe(reader_client, &cache).await {
        Err(LoadError::InvalidRoutingDatabaseShardState {
            database_id,
            database_shard_id,
            state,
        }) => {
            assert_eq!(database_id, expected_database_id);
            assert_eq!(database_shard_id, expected_database_shard_id);
            assert_eq!(state, "retired");
        }
        Err(error) => {
            return Err(format!("unexpected retired database-shard error: {error}").into());
        }
        Ok(_) => return Err("catalog reader hid a retired database-shard identity".into()),
    }
    assert!(cache.current_for_planning().is_err());
    reader_connection_task.abort();

    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let activate_result = client
        .execute(
            "UPDATE pgshard_catalog.database_shards \
                SET state = 'active', draining_at = NULL, retired_at = NULL \
              WHERE logical_database_id = $1::text::uuid \
                AND database_shard_id = $2::text::uuid",
            &[&fixture.logical_database_id, &fixture.database_shard_id],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    activate_result?;
    Ok(())
}

async fn assert_missing_active_placement_fails_closed(
    client: &Client,
    database_url: &str,
    fixture: &Fixture,
) -> TestResult {
    let expected_database_id = DatabaseId::new(Uuid::parse_str(&fixture.logical_database_id)?)?;
    let expected_database_shard_id =
        DatabaseShardId::new(Uuid::parse_str(&fixture.database_shard_id)?)?;
    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let supersede_result = client
        .execute(
            "UPDATE pgshard_catalog.database_shard_placements \
                SET state = 'superseded', superseded_at = statement_timestamp() \
              WHERE logical_database_id = $1::text::uuid \
                AND database_shard_id = $2::text::uuid \
                AND state = 'active'",
            &[&fixture.logical_database_id, &fixture.database_shard_id],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    supersede_result?;

    let cache = CatalogCache::new();
    let (reader_client, reader_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let reader_connection_task = tokio::spawn(reader_connection);
    match CatalogReader::subscribe(reader_client, &cache).await {
        Err(LoadError::InvalidActivePlacementCount {
            database_id,
            database_shard_id,
            count,
        }) => {
            assert_eq!(database_id, expected_database_id);
            assert_eq!(database_shard_id, expected_database_shard_id);
            assert_eq!(count, 0);
        }
        Err(error) => {
            return Err(format!("unexpected missing active-placement error: {error}").into());
        }
        Ok(_) => return Err("catalog reader hid a missing active placement".into()),
    }
    assert!(cache.current_for_planning().is_err());
    reader_connection_task.abort();

    client
        .batch_execute("SET session_replication_role = replica")
        .await?;
    let restore_result = client
        .execute(
            "UPDATE pgshard_catalog.database_shard_placements \
                SET state = 'active', superseded_at = NULL \
              WHERE logical_database_id = $1::text::uuid \
                AND database_shard_id = $2::text::uuid",
            &[&fixture.logical_database_id, &fixture.database_shard_id],
        )
        .await;
    client
        .batch_execute("SET session_replication_role = origin")
        .await?;
    restore_result?;
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
    anchor_receipt_id: Uuid,
    decoder_generation: Uuid,
    decoder_receipt_id: Uuid,
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
        anchor_receipt_id: fixture_uuid(shard.nonce, 40),
        decoder_generation,
        decoder_receipt_id: fixture_uuid(shard.nonce, 41),
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

async fn assert_pending_consumer_creation_fences(
    observer: &Client,
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let generation = consumer.anchor_generation;
    let slot_name = format!("anchor_{}", generation.simple());
    let receipt_id = Uuid::new_v4();

    assert_old_snapshot_cannot_miss_pending_attempt(database_url, observer, generation, receipt_id)
        .await?;
    assert_pending_attempt_respects_target_fence(
        observer,
        database_url,
        shard,
        consumer,
        generation,
        &slot_name,
    )
    .await?;
    assert_direct_pending_consumer_lifecycle_rejected(observer, generation).await?;
    assert_pending_attempt_blocks_parent_changes(observer, shard, consumer).await?;

    for _ in 0..2 {
        observer
            .query_one(
                "SELECT pgshard_catalog.abandon_managed_slot_creation_attempt( \
                            $1::text::uuid, $2::text, $3::text::uuid \
                        )",
                &[&generation.to_string(), &slot_name, &receipt_id.to_string()],
            )
            .await?;
    }
    let attempt_state: String = observer
        .query_one(
            "SELECT state FROM pgshard_catalog.managed_slot_creation_attempts \
              WHERE creation_receipt_id = $1::text::uuid",
            &[&receipt_id.to_string()],
        )
        .await?
        .get(0);
    assert_eq!(attempt_state, "abandoned");
    Ok(())
}

async fn assert_old_snapshot_cannot_miss_pending_attempt(
    database_url: &str,
    observer: &Client,
    generation: Uuid,
    receipt_id: Uuid,
) -> TestResult {
    let (mut stale_client, stale_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let stale_connection_task = tokio::spawn(stale_connection);
    let stale_transaction = stale_client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    stale_transaction
        .batch_execute("SET LOCAL ROLE pgshard_catalog_admin")
        .await?;
    stale_transaction
        .query_one(
            "SELECT catalog_epoch FROM pgshard_catalog.cluster_state WHERE singleton",
            &[],
        )
        .await?;

    begin_managed_slot_creation_attempt(observer, generation, receipt_id).await?;
    let stale_error = stale_transaction
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots SET state = state \
             WHERE slot_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await
        .expect_err("an older snapshot cannot miss a newly pending creation attempt");
    assert_sqlstate(&stale_error, "40001");
    stale_transaction.rollback().await?;
    drop(stale_client);
    tokio::time::timeout(Duration::from_secs(5), stale_connection_task).await???;
    Ok(())
}

async fn assert_pending_attempt_respects_target_fence(
    observer: &Client,
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
    generation: Uuid,
    slot_name: &str,
) -> TestResult {
    let (holder, holder_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let holder_connection = tokio::spawn(holder_connection);
    let fence_id = acquire_target_fence(&holder, slot_name).await?;

    let absent_receipt = Uuid::new_v4();
    observer
        .query_one(
            "SELECT pgshard_catalog.abandon_managed_slot_creation_attempt( \
                        $1::text::uuid, $2::text, $3::text::uuid \
                    )",
            &[
                &generation.to_string(),
                &slot_name,
                &absent_receipt.to_string(),
            ],
        )
        .await?;

    assert_target_fence_blocks_catalog_writes(observer, shard, consumer, generation).await?;
    release_target_fence(holder, holder_connection, slot_name, &fence_id).await?;
    Ok(())
}

async fn assert_direct_pending_consumer_lifecycle_rejected(
    observer: &Client,
    generation: Uuid,
) -> TestResult {
    let activation_error = observer
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
                SET state = 'active', consistent_point = '0/10', two_phase_at = '0/10', \
                    activated_at = statement_timestamp() \
              WHERE slot_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await
        .expect_err("direct DML cannot consume a pending consumer creation attempt");
    assert_sqlstate(&activation_error, "55000");
    assert_database_message(
        &activation_error,
        "managed slot activation requires its receipt-authorized creation attempt",
    );
    let retirement_error = observer
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
                SET state = 'retired', retired_at = statement_timestamp() \
              WHERE slot_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await
        .expect_err("direct DML cannot retire an unresolved consumer creation attempt");
    assert_sqlstate(&retirement_error, "55000");
    assert_database_message(
        &retirement_error,
        "managed slot retirement requires receipt-authorized absence reconciliation",
    );
    Ok(())
}

async fn assert_target_fence_blocks_catalog_writes(
    observer: &Client,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
    generation: Uuid,
) -> TestResult {
    let lifecycle_error = observer
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
                SET state = 'retired', retired_at = statement_timestamp() \
              WHERE slot_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await
        .expect_err("a direct managed-slot lifecycle write cannot bypass the target fence");
    assert_sqlstate(&lifecycle_error, "55P03");
    assert_database_message(&lifecycle_error, "managed slot target fence is busy");
    let ownership_fence_error = observer
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards SET state = 'fenced' \
              WHERE consumer_id = $1::text::uuid \
                AND logical_database_id = $2::text::uuid \
                AND shard_id = $3::text",
            &[
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
            ],
        )
        .await
        .expect_err("consumer ownership cannot wait while retaining catalog state");
    assert_sqlstate(&ownership_fence_error, "55P03");
    assert_database_message(&ownership_fence_error, "managed slot target fence is busy");
    let state_while_fenced: String = observer
        .query_one(
            "SELECT state FROM pgshard_catalog.managed_replication_slots \
              WHERE slot_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await?
        .get(0);
    assert_eq!(state_while_fenced, "allocated");
    Ok(())
}

async fn assert_target_registry_lock_is_fail_fast(
    observer: &Client,
    database_url: &str,
) -> TestResult {
    let target_name = format!("lock_order_{}", Uuid::new_v4().simple());
    let (holder, contender) = connect_lock_test_pair(observer, database_url).await?;

    let mut contender_force_terminate = false;
    let probe_result: TestResult<_> = async {
        holder.client().batch_execute("BEGIN").await?;
        acquire_target_fence(holder.client(), &target_name).await?;
        contender
            .client()
            .batch_execute(
                "BEGIN; \
                 SELECT 1 FROM pgshard_catalog.cluster_state \
                  WHERE singleton FOR UPDATE",
            )
            .await?;
        let lock_outcome = tokio::time::timeout(
            Duration::from_secs(1),
            contender.client().query_one(
                "SELECT pgshard_catalog.lock_managed_slot_target($1::text)",
                &[&target_name],
            ),
        )
        .await;
        contender_force_terminate = lock_outcome.is_err();
        Ok(lock_outcome)
    }
    .await;
    let contender_cleanup = contender.cleanup(observer, contender_force_terminate).await;
    let holder_cleanup = holder.cleanup(observer, false).await;
    contender_cleanup?;
    holder_cleanup?;
    let lock_outcome = probe_result?;
    let error = lock_outcome
        .map_err(|_| "target registry lock waited while cluster state was retained")?
        .expect_err("a retained target row must fail fast");
    assert_sqlstate(&error, "55P03");
    assert_database_message(&error, "managed slot target fence is busy");

    let rows: i64 = observer
        .query_one(
            "SELECT pg_catalog.count(*) \
               FROM pgshard_catalog.managed_slot_target_fences \
              WHERE target_name::pg_catalog.text = $1::pg_catalog.text",
            &[&target_name],
        )
        .await?
        .get(0);
    assert_eq!(rows, 0, "rolled-back first acquisition left a registry row");

    assert_cross_target_registry_lock_remains_independent(observer, database_url).await?;
    Ok(())
}

async fn assert_cross_target_registry_lock_remains_independent(
    observer: &Client,
    database_url: &str,
) -> TestResult {
    let existing_target = format!("lock_existing_{}", Uuid::new_v4().simple());
    let missing_target = format!("lock_missing_{}", Uuid::new_v4().simple());
    let (lifecycle, creator) = connect_lock_test_pair(observer, database_url).await?;
    let lifecycle_pid = lifecycle.backend_pid;
    let creator_pid = creator.backend_pid;
    let (probe_result, lifecycle_force_terminate, creator_force_terminate) =
        run_cross_target_registry_lock_probe(
            observer,
            lifecycle.client(),
            creator.client(),
            lifecycle_pid,
            creator_pid,
            &existing_target,
            &missing_target,
        )
        .await;
    let lifecycle_cleanup = lifecycle.cleanup(observer, lifecycle_force_terminate).await;
    let creator_cleanup = creator.cleanup(observer, creator_force_terminate).await;
    let registry_cleanup = observer
        .execute(
            "DELETE FROM pgshard_catalog.managed_slot_target_fences \
              WHERE target_name::text = ANY($1::text[])",
            &[&vec![existing_target, missing_target]],
        )
        .await;
    lifecycle_cleanup?;
    creator_cleanup?;
    registry_cleanup?;
    probe_result
}

async fn run_cross_target_registry_lock_probe(
    observer: &Client,
    lifecycle: &Client,
    creator: &Client,
    lifecycle_pid: i32,
    creator_pid: i32,
    existing_target: &str,
    missing_target: &str,
) -> (TestResult, bool, bool) {
    let mut lifecycle_force_terminate = false;
    let mut creator_force_terminate = false;
    let probe_result: TestResult = async {
        acquire_target_fence(lifecycle, existing_target).await?;
        lifecycle
            .batch_execute(
                "BEGIN; \
                 SELECT 1 FROM pgshard_catalog.cluster_state \
                  WHERE singleton FOR UPDATE",
            )
            .await?;
        creator.batch_execute("BEGIN").await?;
        creator
            .query_one(
                "SELECT pgshard_catalog.lock_managed_slot_target($1::text)",
                &[&missing_target],
            )
            .await?;

        let creator_cluster_state = creator.query_one(
            "SELECT 1 FROM pgshard_catalog.cluster_state \
              WHERE singleton FOR UPDATE",
            &[],
        );
        tokio::pin!(creator_cluster_state);
        let wait_observed = tokio::select! {
            result = &mut creator_cluster_state => {
                result?;
                return Err("different-target creator bypassed retained cluster state".into());
            }
            result = observe_backend_blocked_by(observer, creator_pid, lifecycle_pid) => result?,
        };
        if !wait_observed {
            creator_force_terminate = true;
            return Err("different-target creator did not form the reverse lock waiter".into());
        }

        let existing_target_outcome = tokio::time::timeout(
            Duration::from_secs(1),
            lifecycle.query_one(
                "SELECT pgshard_catalog.lock_managed_slot_target($1::text)",
                &[&existing_target],
            ),
        )
        .await;
        let existing_target_row = if let Ok(result) = existing_target_outcome {
            result?
        } else {
            lifecycle_force_terminate = true;
            return Err(
                "existing-target registry lock waited behind a different-target insertion".into(),
            );
        };
        drop(existing_target_row);

        lifecycle.batch_execute("ROLLBACK").await?;
        if let Ok(result) =
            tokio::time::timeout(Duration::from_secs(2), &mut creator_cluster_state).await
        {
            result?;
        } else {
            creator_force_terminate = true;
            return Err("different-target creator remained blocked after rollback".into());
        }
        Ok(())
    }
    .await;
    (
        probe_result,
        lifecycle_force_terminate,
        creator_force_terminate,
    )
}

struct LockTestConnection {
    client: Option<Client>,
    connection_task: Option<JoinHandle<Result<(), PgError>>>,
    backend_pid: i32,
}

impl LockTestConnection {
    async fn connect(database_url: &str) -> TestResult<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
        let mut connection_task = tokio::spawn(connection);
        let backend_pid = match client
            .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
            .await
        {
            Ok(row) => row.get(0),
            Err(error) => {
                drop(client);
                if tokio::time::timeout(Duration::from_secs(5), &mut connection_task)
                    .await
                    .is_err()
                {
                    connection_task.abort();
                    let _ = connection_task.await;
                }
                return Err(error.into());
            }
        };
        Ok(Self {
            client: Some(client),
            connection_task: Some(connection_task),
            backend_pid,
        })
    }

    fn client(&self) -> &Client {
        self.client.as_ref().expect("lock-test client is live")
    }

    async fn cleanup(mut self, observer: &Client, force_terminate: bool) -> TestResult {
        let mut failures = Vec::new();
        let mut forced = force_terminate;
        let client = self.client.take().expect("lock-test client is live");
        if !forced {
            match tokio::time::timeout(Duration::from_secs(2), client.batch_execute("ROLLBACK"))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failures.push(format!(
                        "rollback failed for backend {}: {error}",
                        self.backend_pid
                    ));
                    forced = true;
                }
                Err(_) => {
                    failures.push(format!(
                        "rollback timed out for backend {}",
                        self.backend_pid
                    ));
                    forced = true;
                }
            }
        }
        if forced {
            match observer
                .query_one(
                    "SELECT pg_catalog.pg_terminate_backend($1)",
                    &[&self.backend_pid],
                )
                .await
            {
                Ok(row) if row.get::<_, bool>(0) => {}
                Ok(_) => failures.push(format!("backend {} refused termination", self.backend_pid)),
                Err(error) => {
                    failures.push(format!("terminate backend {}: {error}", self.backend_pid));
                }
            }
        }
        drop(client);

        let mut connection_task = self
            .connection_task
            .take()
            .expect("lock-test connection task is live");
        match tokio::time::timeout(Duration::from_secs(5), &mut connection_task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(_))) if forced => {}
            Ok(Ok(Err(error))) => failures.push(format!(
                "connection driver failed for backend {}: {error}",
                self.backend_pid
            )),
            Ok(Err(error)) => failures.push(format!(
                "connection task failed for backend {}: {error}",
                self.backend_pid
            )),
            Err(_) => {
                connection_task.abort();
                let _ = connection_task.await;
                failures.push(format!(
                    "connection driver did not stop for backend {}",
                    self.backend_pid
                ));
            }
        }
        match wait_for_backend_exit(observer, self.backend_pid).await {
            Ok(true) => {}
            Ok(false) => failures.push(format!(
                "backend {} remained live after test cleanup",
                self.backend_pid
            )),
            Err(error) => failures.push(format!(
                "observe backend {} exit: {error}",
                self.backend_pid
            )),
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; ").into())
        }
    }
}

async fn connect_lock_test_pair(
    observer: &Client,
    database_url: &str,
) -> TestResult<(LockTestConnection, LockTestConnection)> {
    let first = LockTestConnection::connect(database_url).await?;
    match LockTestConnection::connect(database_url).await {
        Ok(second) => Ok((first, second)),
        Err(connect_error) => {
            let cleanup = first.cleanup(observer, false).await;
            if let Err(cleanup_error) = cleanup {
                return Err(format!(
                    "second lock-test connection failed: {connect_error}; \
                     first connection cleanup failed: {cleanup_error}"
                )
                .into());
            }
            Err(connect_error)
        }
    }
}

async fn release_target_fence(
    holder: Client,
    holder_connection: tokio::task::JoinHandle<Result<(), PgError>>,
    slot_name: &str,
    fence_id: &str,
) -> TestResult {
    let released: bool = holder
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::text, $2::text::uuid \
                    )",
            &[&slot_name, &fence_id],
        )
        .await?
        .get(0);
    assert!(
        released,
        "the simulated create target fence must release once"
    );
    drop(holder);
    tokio::time::timeout(Duration::from_secs(5), holder_connection).await???;
    Ok(())
}

async fn acquire_target_fence(client: &Client, slot_name: &str) -> TestResult<String> {
    Ok(client
        .query_one(
            "SELECT acquired_fence_id::text \
               FROM pgshard_catalog.acquire_managed_slot_target_fence($1::text)",
            &[&slot_name],
        )
        .await?
        .get(0))
}

async fn assert_retired_probe_history_does_not_expand_parent_fences(
    observer: &Client,
    database_url: &str,
) -> TestResult {
    const HISTORY_ROWS: usize = 64;
    let fixture = create_fixture(observer).await?;
    let mut first_retired_name = None;
    for index in 0..HISTORY_ROWS {
        let generation = Uuid::new_v4();
        let slot_name = format!("history_{index}_{}", generation.simple());
        insert_slot_sync_probe(
            observer,
            &fixture,
            generation,
            &fixture.restore_incarnation,
            &slot_name,
        )
        .await?;
        observer
            .execute(
                "UPDATE pgshard_catalog.slot_sync_probes \
                    SET state = 'retiring', cleanup_receipt_id = gen_random_uuid(), \
                        retiring_at = statement_timestamp() \
                  WHERE probe_generation = $1::text::uuid",
                &[&generation.to_string()],
            )
            .await?;
        complete_slot_sync_probe_retirement(observer, generation).await?;
        first_retired_name.get_or_insert(slot_name);
    }

    let retired_name = first_retired_name.expect("retired history is non-empty");
    let (holder, holder_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let holder_connection = tokio::spawn(holder_connection);
    let fence_id = acquire_target_fence(&holder, &retired_name).await?;
    observer
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'draining' WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await?;
    observer
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'active' WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await?;
    release_target_fence(holder, holder_connection, &retired_name, &fence_id).await?;

    let live_generation = Uuid::new_v4();
    let live_name = format!("history_live_{}", live_generation.simple());
    insert_slot_sync_probe(
        observer,
        &fixture,
        live_generation,
        &fixture.restore_incarnation,
        &live_name,
    )
    .await?;
    let (holder, holder_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let holder_connection = tokio::spawn(holder_connection);
    let fence_id = acquire_target_fence(&holder, &live_name).await?;
    let error = observer
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'draining' WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await
        .expect_err("a live probe target must still fence its parent lifecycle");
    assert_sqlstate(&error, "55P03");
    release_target_fence(holder, holder_connection, &live_name, &fence_id).await?;

    observer
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
                SET state = 'retiring', cleanup_receipt_id = gen_random_uuid(), \
                    retiring_at = statement_timestamp() \
              WHERE probe_generation = $1::text::uuid",
            &[&live_generation.to_string()],
        )
        .await?;
    complete_slot_sync_probe_retirement(observer, live_generation).await?;
    Ok(())
}

async fn assert_pending_attempt_blocks_parent_changes(
    observer: &Client,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    let ownership_error = observer
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards SET state = 'fenced' \
              WHERE consumer_id = $1::text::uuid \
                AND logical_database_id = $2::text::uuid \
                AND shard_id = $3::text",
            &[
                &consumer.consumer_id,
                &shard.logical_database_id,
                &shard.shard_id,
            ],
        )
        .await
        .expect_err("pending slot creation must fence consumer ownership changes");
    assert_sqlstate(&ownership_error, "55000");
    assert_database_message(
        &ownership_error,
        "consumer ownership fencing is blocked by a pending managed slot creation",
    );
    let attachment_error = observer
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
                SET state = 'active', activated_at = statement_timestamp() \
              WHERE attachment_generation = $1::text::uuid",
            &[&consumer.attachment_generation],
        )
        .await
        .expect_err("pending slot creation must fence source attachment changes");
    assert_sqlstate(&attachment_error, "55000");
    assert_database_message(
        &attachment_error,
        "source attachment lifecycle is blocked by a pending managed slot creation",
    );
    Ok(())
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
             SET state = 'retiring', cleanup_receipt_id = gen_random_uuid(), \
                 retiring_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await?;
    let slot_name: String = client
        .query_one(
            "SELECT slot_name::text FROM pgshard_catalog.slot_sync_probes \
              WHERE probe_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await?
        .get(0);
    client.batch_execute("BEGIN").await?;
    client
        .query_one(
            "SELECT pg_catalog.pg_advisory_xact_lock( \
                        pg_catalog.hashtextextended($1::text, 1346851656::bigint) \
                    )",
            &[&slot_name],
        )
        .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE probe_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await
        .expect_err("final probe retirement cannot run without the live target fence");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "slot-sync probe final retirement requires its live target fence",
    );
    client.batch_execute("ROLLBACK").await?;

    assert_released_probe_fence_cannot_be_replaced(client, generation, &slot_name).await?;
    complete_slot_sync_probe_retirement(client, generation).await?;
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

async fn assert_released_probe_fence_cannot_be_replaced(
    client: &Client,
    generation: Uuid,
    slot_name: &str,
) -> TestResult {
    let cleanup_receipt_id: String = client
        .query_one(
            "SELECT cleanup_receipt_id::text \
               FROM pgshard_catalog.slot_sync_probes \
              WHERE probe_generation = $1::text::uuid",
            &[&generation.to_string()],
        )
        .await?
        .get(0);
    let released_fence_id = acquire_target_fence(client, slot_name).await?;
    let released: bool = client
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::text, $2::text::uuid \
                    )",
            &[&slot_name, &released_fence_id],
        )
        .await?
        .get(0);
    assert!(released);
    client.batch_execute("BEGIN").await?;
    client
        .query_one(
            "SELECT pg_catalog.pg_advisory_xact_lock( \
                        pg_catalog.hashtextextended($1::text, 1346851656::bigint) \
                    )",
            &[&released_fence_id],
        )
        .await?;
    let error = client
        .query_one(
            "SELECT pgshard_catalog.complete_slot_sync_probe_retirement( \
                        $1::text::uuid, $2::text, $3::text::uuid, $4::text::uuid \
                    )",
            &[
                &generation.to_string(),
                &slot_name,
                &cleanup_receipt_id,
                &released_fence_id,
            ],
        )
        .await
        .expect_err("a transaction advisory lock cannot replace a released probe fence");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "slot-sync probe final retirement requires its exact live target fence",
    );
    client.batch_execute("ROLLBACK").await?;
    Ok(())
}

async fn assert_managed_slot_allocation(
    client: &Client,
    database_url: &str,
    shard: &Fixture,
    consumer: &ConsumerRegistryFixture,
) -> TestResult {
    assert_invalid_managed_slot_allocations(client, shard, consumer).await?;
    assert_retired_probe_history_does_not_expand_parent_fences(client, database_url).await?;
    assert_repeatable_read_probe_lifecycle_races_are_fenced(client, database_url, shard, consumer)
        .await?;
    allocate_managed_slots(client, shard, consumer).await?;
    assert_pending_consumer_creation_fences(client, database_url, shard, consumer).await?;
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
    begin_managed_slot_creation_attempt(
        client,
        consumer.anchor_generation,
        consumer.anchor_receipt_id,
    )
    .await?;
    begin_managed_slot_creation_attempt(
        client,
        consumer.decoder_generation,
        consumer.decoder_receipt_id,
    )
    .await?;
    activate_managed_replication_slot(
        client,
        consumer.anchor_generation,
        consumer.anchor_receipt_id,
        "0/30",
        "0/30",
    )
    .await?;
    activate_managed_replication_slot(
        client,
        consumer.decoder_generation,
        consumer.decoder_receipt_id,
        "0/20",
        "0/40",
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
        let receipt_id = Uuid::new_v4();
        begin_managed_slot_creation_attempt(transaction, generation, receipt_id).await?;
        activate_managed_replication_slot(
            transaction,
            generation,
            receipt_id,
            consistent_point,
            two_phase_at,
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
    let error = client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
             SET state = 'retired', retired_at = statement_timestamp() \
             WHERE attachment_generation = $1::text::uuid AND state = 'retiring'",
            &[&consumer.attachment_generation],
        )
        .await
        .expect_err("direct DML cannot consume activated retirement capabilities");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "managed slot retirement requires receipt-authorized absence reconciliation",
    );
    complete_managed_replication_slot_retirement(
        client,
        consumer.anchor_generation,
        consumer.anchor_receipt_id,
    )
    .await?;
    complete_managed_replication_slot_retirement(
        client,
        consumer.decoder_generation,
        consumer.decoder_receipt_id,
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
    let receipt_id = Uuid::new_v4();
    begin_managed_slot_creation_attempt(client, slot_generation, receipt_id).await?;
    activate_managed_replication_slot(client, slot_generation, receipt_id, "0/20", "0/20").await?;
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
    let slot_name = format!("anchor_{}", slot_generation.simple());
    assert_consumer_retirement_requires_hidden_fences(
        client,
        slot_generation,
        receipt_id,
        &slot_name,
    )
    .await?;
    complete_managed_replication_slot_retirement(client, slot_generation, receipt_id).await?;
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

async fn assert_consumer_retirement_requires_hidden_fences(
    client: &Client,
    slot_generation: Uuid,
    receipt_id: Uuid,
    slot_name: &str,
) -> TestResult {
    client.batch_execute("BEGIN").await?;
    client
        .query_one(
            "SELECT pg_catalog.pg_advisory_xact_lock( \
                        pg_catalog.hashtextextended($1::text, 1346851656::bigint) \
                    )",
            &[&slot_name],
        )
        .await?;
    let counterfeit_fence_id = Uuid::new_v4();
    let error = client
        .query_one(
            "SELECT pgshard_catalog.complete_managed_replication_slot_retirement( \
                        $1::text::uuid, $2::text, $3::text::uuid, $4::text::uuid \
                    )",
            &[
                &slot_generation.to_string(),
                &slot_name,
                &receipt_id.to_string(),
                &counterfeit_fence_id.to_string(),
            ],
        )
        .await
        .expect_err("consumer retirement cannot outlive its absence fence");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "managed slot final retirement requires its live target fence",
    );
    client.batch_execute("ROLLBACK").await?;

    let released_fence_id = acquire_target_fence(client, slot_name).await?;
    let released: bool = client
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::text, $2::text::uuid \
                    )",
            &[&slot_name, &released_fence_id],
        )
        .await?
        .get(0);
    assert!(released);
    client.batch_execute("BEGIN").await?;
    client
        .query_one(
            "SELECT pg_catalog.pg_advisory_xact_lock( \
                        pg_catalog.hashtextextended($1::text, 1346851656::bigint) \
                    )",
            &[&released_fence_id],
        )
        .await?;
    let error = client
        .query_one(
            "SELECT pgshard_catalog.complete_managed_replication_slot_retirement( \
                        $1::text::uuid, $2::text, $3::text::uuid, $4::text::uuid \
                    )",
            &[
                &slot_generation.to_string(),
                &slot_name,
                &receipt_id.to_string(),
                &released_fence_id,
            ],
        )
        .await
        .expect_err("a transaction advisory lock cannot replace a released consumer fence");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "managed slot final retirement requires its live target fence",
    );
    client.batch_execute("ROLLBACK").await?;

    let error =
        complete_managed_replication_slot_retirement(client, slot_generation, Uuid::new_v4())
            .await
            .expect_err("consumer retirement requires its exact hidden receipt");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "managed slot retirement requires its exact creation attempt",
    );
    complete_managed_replication_slot_retirement(client, slot_generation, receipt_id).await?;
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
    let receipt_id = Uuid::new_v4();
    begin_managed_slot_creation_attempt(client, slot_generation, receipt_id).await?;
    activate_managed_replication_slot(client, slot_generation, receipt_id, "0/20", "0/20").await?;
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
    complete_managed_replication_slot_retirement(client, slot_generation, receipt_id).await?;
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

async fn assert_duplicate_physical_placement_rejected(
    client: &Client,
    fixture: &Fixture,
) -> TestResult {
    let duplicate_database_shard_id: String = client
        .query_one(
            "INSERT INTO pgshard_catalog.database_shards( \
                 logical_database_id, shard_ordinal, state, activated_at \
             ) VALUES ($1::text::uuid, 1, 'active', statement_timestamp()) \
             RETURNING database_shard_id::text",
            &[&fixture.logical_database_id],
        )
        .await?
        .get(0);
    client
        .execute(
            "INSERT INTO pgshard_catalog.database_shard_placements( \
                 logical_database_id, database_shard_id, placement_generation, \
                 shard_id, state, activated_at \
             ) VALUES ($1::text::uuid, $2::text::uuid, 1, $3::text, \
                       'active', statement_timestamp())",
            &[
                &fixture.logical_database_id,
                &duplicate_database_shard_id,
                &fixture.shard_id,
            ],
        )
        .await?;
    let duplicate_physical_epoch = stage_epoch(client, &fixture.logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges( \
                 logical_database_id, routing_epoch, range_start, range_end, database_shard_id \
             ) VALUES \
                 ($1::text::uuid, $2, 0, 9223372036854775808, $3::text::uuid), \
                 ($1::text::uuid, $2, 9223372036854775808, 18446744073709551616, $4::text::uuid)",
            &[
                &fixture.logical_database_id,
                &duplicate_physical_epoch,
                &fixture.database_shard_id,
                &duplicate_database_shard_id,
            ],
        )
        .await?;
    let expected_none: Option<i64> = None;
    let current_catalog_epoch = catalog_epoch(client).await?;
    let error = client
        .query_one(
            "SELECT pgshard_catalog.activate_routing_epoch($1::text::uuid, $2, $3, $4)",
            &[
                &fixture.logical_database_id,
                &duplicate_physical_epoch,
                &expected_none,
                &current_catalog_epoch,
            ],
        )
        .await
        .expect_err("two database shards on one physical shard were activated");
    assert_sqlstate(&error, "22023");
    assert_database_message(
        &error,
        "routing epoch maps multiple database shards to one physical shard",
    );
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
             (logical_database_id, routing_epoch, range_start, range_end, database_shard_id) \
             VALUES \
             ($2::text::uuid, $1, 0, 10, $3::text::uuid), \
             ($2::text::uuid, $1, 11, 18446744073709551616, $3::text::uuid)",
            &[
                &gap_epoch,
                &fixture.logical_database_id,
                &fixture.database_shard_id,
            ],
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
             (logical_database_id, routing_epoch, range_start, range_end, database_shard_id) \
             VALUES \
             ($2::text::uuid, $1, 0, 11, $3::text::uuid), \
             ($2::text::uuid, $1, 10, 18446744073709551616, $3::text::uuid)",
            &[
                &overlap_epoch,
                &fixture.logical_database_id,
                &fixture.database_shard_id,
            ],
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

    assert_duplicate_physical_placement_rejected(client, fixture).await?;
    let current_catalog_epoch = catalog_epoch(client).await?;

    let valid_epoch = stage_epoch(client, &fixture.logical_database_id).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.routing_ranges \
             (logical_database_id, routing_epoch, range_start, range_end, database_shard_id) \
             VALUES \
             ($2::text::uuid, $1, 0, 9223372036854775808, $3::text::uuid), \
             ($2::text::uuid, $1, 9223372036854775808, 18446744073709551616, \
              $3::text::uuid)",
            &[
                &valid_epoch,
                &fixture.logical_database_id,
                &fixture.database_shard_id,
            ],
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
             (logical_database_id, routing_epoch, range_start, range_end, database_shard_id) \
             VALUES ($2::text::uuid, $1, 0, 18446744073709551616, $3::text::uuid)",
            &[
                &rollback_epoch,
                &fixture.logical_database_id,
                &fixture.database_shard_id,
            ],
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
             (logical_database_id, routing_epoch, range_start, range_end, database_shard_id) \
             VALUES ($2::text::uuid, $1, 0, 18446744073709551616, $3::text::uuid)",
            &[
                &routing_epoch,
                &fixture.logical_database_id,
                &fixture.database_shard_id,
            ],
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

async fn stage_database_shard_replacement(
    client: &Client,
    fixture: &Fixture,
) -> Result<(String, i64, String), Box<dyn Error>> {
    let replacement_shard_number: i64 = client
        .query_one(
            "SELECT coalesce(max(shard_number), 0) + 1 FROM pgshard_catalog.shards",
            &[],
        )
        .await?
        .get(0);
    let replacement_shard_id = format!("move-{}", fixture.nonce);
    client
        .execute(
            "INSERT INTO pgshard_catalog.shards(shard_id, shard_number) \
             VALUES ($1::text, $2)",
            &[&replacement_shard_id, &replacement_shard_number],
        )
        .await?;
    let replacement_placement_id: String = client
        .query_one(
            "INSERT INTO pgshard_catalog.database_shard_placements( \
                 logical_database_id, database_shard_id, placement_generation, shard_id \
             ) VALUES ($1::text::uuid, $2::text::uuid, 2, $3::text) \
             RETURNING placement_id::text",
            &[
                &fixture.logical_database_id,
                &fixture.database_shard_id,
                &replacement_shard_id,
            ],
        )
        .await?
        .get(0);

    let error = client
        .execute(
            "DELETE FROM pgshard_catalog.database_shard_placements \
              WHERE placement_id = $1::text::uuid",
            &[&replacement_placement_id],
        )
        .await
        .expect_err("a staged placement identity was deleted and became reusable");
    assert_sqlstate(&error, "55000");
    assert_database_message(&error, "database-shard placement identities are permanent");

    let error = client
        .execute(
            "UPDATE pgshard_catalog.database_shard_placements \
                SET state = 'active', activated_at = statement_timestamp() \
              WHERE placement_id = $1::text::uuid",
            &[&replacement_placement_id],
        )
        .await
        .expect_err("a replacement cannot activate before the old placement is superseded");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "database-shard placement transitions require an atomic target-fenced cutover",
    );
    Ok((
        replacement_shard_id,
        replacement_shard_number,
        replacement_placement_id,
    ))
}

async fn assert_rejected_placement_rows_unchanged(
    client: &Client,
    fixture: &Fixture,
    replacement_shard_id: &str,
    replacement_shard_number: i64,
) -> TestResult<i64> {
    let placements: Vec<(i64, String, String, i64)> = client
        .query(
            "SELECT placements.placement_generation, placements.state, \
                    placements.shard_id::text, shards.shard_number \
               FROM pgshard_catalog.database_shard_placements AS placements \
               JOIN pgshard_catalog.shards AS shards \
                 ON shards.shard_id = placements.shard_id \
              WHERE placements.logical_database_id = $1::text::uuid \
                AND placements.database_shard_id = $2::text::uuid \
               ORDER BY placements.placement_generation",
            &[&fixture.logical_database_id, &fixture.database_shard_id],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect();
    let serving_shard_number = placements[0].3;
    assert_eq!(
        placements,
        vec![
            (
                1,
                "active".into(),
                fixture.shard_id.clone(),
                serving_shard_number,
            ),
            (
                2,
                "staged".into(),
                replacement_shard_id.to_owned(),
                replacement_shard_number,
            ),
        ]
    );
    Ok(serving_shard_number)
}

async fn assert_database_shard_placement_contract(
    client: &mut Client,
    database_url: &str,
    fixture: &Fixture,
) -> TestResult {
    let (replacement_shard_id, replacement_shard_number, replacement_placement_id) =
        stage_database_shard_replacement(client, fixture).await?;

    let error = client
        .execute(
            "UPDATE pgshard_catalog.database_shard_placements \
                SET state = 'superseded', superseded_at = statement_timestamp() \
              WHERE logical_database_id = $1::text::uuid \
                AND database_shard_id = $2::text::uuid \
                AND state = 'active'",
            &[&fixture.logical_database_id, &fixture.database_shard_id],
        )
        .await
        .expect_err("an unfenced cutover superseded the serving placement");
    assert_sqlstate(&error, "55000");
    assert_database_message(
        &error,
        "database-shard placement transitions require an atomic target-fenced cutover",
    );

    let serving_shard_number = assert_rejected_placement_rows_unchanged(
        client,
        fixture,
        &replacement_shard_id,
        replacement_shard_number,
    )
    .await?;

    let unchanged_epoch = catalog_epoch(client).await?;
    let driver = CatalogDriver::start(
        database_url,
        CatalogPollInterval::new(Duration::from_secs(30))?,
    )
    .await?;
    wait_for_cache_epoch(&driver.cache, unchanged_epoch).await?;
    let snapshot = driver.cache.current_for_planning()?;
    let database_id = DatabaseId::new(Uuid::parse_str(&fixture.logical_database_id)?)?;
    let database = snapshot
        .database(database_id)
        .ok_or("moved database disappeared from the catalog snapshot")?;
    assert!(
        database.routes().iter().all(|route| {
            route.database_shard_id().to_string() == fixture.database_shard_id
                && i64::from(route.shard_id().0) == serving_shard_number
        }),
        "a rejected placement cutover changed the serving route"
    );
    driver.shutdown().await?;

    client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'draining' \
             WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await?;
    let error = client
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'retired' \
              WHERE shard_id = $1::text",
            &[&fixture.shard_id],
        )
        .await
        .expect_err("an active placement allowed physical shard retirement");
    assert_sqlstate(&error, "55000");
    let staged_state: String = client
        .query_one(
            "SELECT state FROM pgshard_catalog.database_shard_placements \
              WHERE placement_id = $1::text::uuid",
            &[&replacement_placement_id],
        )
        .await?
        .get(0);
    assert_eq!(staged_state, "staged");
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

async fn run_migration_and_activation_contract(
    client: &mut Client,
    database_url: &str,
) -> TestResult {
    assert_squatted_catalog_role_is_rejected(client).await?;
    assert_restricted_catalog_migration_is_rejected(client).await?;
    assert_installation_contract(client, database_url).await?;
    assert_shutdown_interrupts_initial_load(client, database_url).await?;
    assert_operation_timeout_aborts_blocked_initial_load(client, database_url).await?;
    assert_operation_timeout_aborts_blocked_refresh(client, database_url).await?;
    assert_initial_catalog_reader_contract(client, database_url).await?;
    assert_catalog_reader_rejects_existing_transaction(client, database_url).await?;
    assert_admin_privilege_contract(client).await?;
    assert_catalog_reader_cannot_seize_target_fence(database_url).await?;
    assert_stale_target_fence_backend_generation_is_reclaimed(database_url).await?;
    assert_target_registry_lock_is_fail_fast(client, database_url).await?;
    assert_admin_write_path(client).await?;
    let fixture = create_fixture(client).await?;
    assert_migration_rejects_active_shard_without_restore(client, fixture.nonce).await?;
    assert_slot_sync_probe_contract(client, &fixture).await?;
    assert_identity_history_contract(client, &fixture).await?;
    assert_registered_table_contract(client, &fixture).await?;
    assert_tombstone_contract(client, &fixture).await?;
    assert_consumer_requires_active_database(client, fixture.nonce).await?;
    assert_logical_consumer_registry_contract(client, database_url, &fixture).await?;
    let routing = assert_invalid_routing_contracts(client, &fixture).await?;
    let listener = connect_listener(database_url).await?;
    commit_valid_activation(client, &listener, &fixture, &routing).await?;
    let activated_epoch = assert_loader_contract(
        client,
        database_url,
        &listener,
        &fixture,
        routing.valid_epoch,
    )
    .await?;
    assert_malformed_active_routing_fails_closed(
        client,
        database_url,
        &fixture,
        routing.valid_epoch,
    )
    .await?;
    assert_rollback_contract(client, &listener, &fixture, &routing, activated_epoch).await?;
    assert_routing_epoch_cannot_regress(client, &listener, &fixture, routing.valid_epoch).await?;
    assert_repeatable_read_activation_fences_concurrent_range_mutation(
        client,
        database_url,
        &listener,
        &fixture,
    )
    .await?;
    assert_database_shard_placement_contract(client, database_url, &fixture).await?;

    listener.task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing to a disposable PostgreSQL 18 shardschema database"]
async fn migration_and_activation_contract() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);

    let result = run_migration_and_activation_contract(&mut client, &database_url).await;
    let rollback_result = client.batch_execute("ROLLBACK").await;
    let cleanup_result = client
        .batch_execute("DROP SCHEMA IF EXISTS pgshard_catalog CASCADE")
        .await;
    let role_cleanup_result = drop_catalog_roles(&client).await;
    connection_task.abort();
    result?;
    rollback_result?;
    cleanup_result?;
    role_cleanup_result?;
    Ok(())
}
