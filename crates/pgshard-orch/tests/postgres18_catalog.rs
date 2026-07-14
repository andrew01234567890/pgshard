//! Live `PostgreSQL` 18 coverage for catalog-to-standby-policy loading.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pgshard_catalog::{CatalogOperationTimeout, MIGRATION_SQL};
use pgshard_orch::slot_catalog::{
    LogicalConsumerPurpose, LogicalConsumerShardKey, SlotCatalogLoadError, SlotCatalogReader,
};
use pgshard_orch::standby_slots::StandbyDecoderEvidenceLimits;
use pgshard_types::PgLsn;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Fixture {
    consumer_id: Uuid,
    logical_database_id: Uuid,
    checkpoint_generation: Uuid,
    attachment_generation: Uuid,
    restore_incarnation: Uuid,
    anchor_generation: Uuid,
    decoder_generation: Uuid,
    anchor_name: String,
    decoder_name: String,
}

struct CatalogIds {
    consumer: String,
    logical_database: String,
    restore_incarnation: String,
}

async fn random_uuid(client: &Client) -> TestResult<Uuid> {
    let text: String = client
        .query_one("SELECT gen_random_uuid()::text", &[])
        .await?
        .get(0);
    Ok(Uuid::parse_str(&text)?)
}

async fn create_consumer(client: &Client, suffix: u128) -> TestResult<CatalogIds> {
    let logical_database: String = client
        .query_one(
            "INSERT INTO pgshard_catalog.logical_databases(database_name) \
             VALUES ($1::text) RETURNING logical_database_id::text",
            &[&format!("slot_loader_{suffix}")],
        )
        .await?
        .get(0);
    let restore_incarnation: String = client
        .query_one(
            "SELECT restore_incarnation::text \
               FROM pgshard_catalog.shard_restore_incarnations \
              WHERE shard_id = 'shard-0000' AND state = 'active'",
            &[],
        )
        .await?
        .get(0);
    let consumer: String = client
        .query_one(
            "INSERT INTO pgshard_catalog.logical_consumers( \
                 logical_database_id, consumer_name, purpose \
             ) VALUES ($1::text::uuid, $2::text, 'reshard-materializer') \
             RETURNING consumer_id::text",
            &[&logical_database, &format!("loader-{suffix}")],
        )
        .await?
        .get(0);
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_shards( \
                 consumer_id, logical_database_id, shard_id \
             ) VALUES ($1::text::uuid, $2::text::uuid, 'shard-0000')",
            &[&consumer, &logical_database],
        )
        .await?;
    Ok(CatalogIds {
        consumer,
        logical_database,
        restore_incarnation,
    })
}

async fn create_source_records(client: &Client, ids: &CatalogIds) -> TestResult<(Uuid, Uuid)> {
    let checkpoint_generation = random_uuid(client).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_checkpoints( \
                 checkpoint_generation, consumer_id, logical_database_id, shard_id, \
                 restore_incarnation, system_identifier, database_oid, source_timeline \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, 'shard-0000', \
                 $4::text::uuid, 7219834723984723, 16384, 7 \
             )",
            &[
                &checkpoint_generation.to_string(),
                &ids.consumer,
                &ids.logical_database,
                &ids.restore_incarnation,
            ],
        )
        .await?;

    let attachment_generation = random_uuid(client).await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_attachments( \
                 attachment_generation, consumer_id, logical_database_id, shard_id, \
                 restore_incarnation, system_identifier, database_oid, database_name, \
                 selected_source_member_ordinal, selected_source_role, \
                 selected_source_timeline \
             ) VALUES ( \
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, 'shard-0000', \
                 $4::text::uuid, 7219834723984723, 16384, 'application', \
                 1, 'standby-decoder', 7 \
             )",
            &[
                &attachment_generation.to_string(),
                &ids.consumer,
                &ids.logical_database,
                &ids.restore_incarnation,
            ],
        )
        .await?;
    Ok((checkpoint_generation, attachment_generation))
}

async fn allocate_active_slots(
    client: &Client,
    ids: &CatalogIds,
    attachment_generation: Uuid,
) -> TestResult<(Uuid, Uuid, String, String)> {
    let anchor_generation = random_uuid(client).await?;
    let decoder_generation = random_uuid(client).await?;
    let anchor_name = format!("anchor_{}", anchor_generation.simple());
    let decoder_name = format!("decoder_{}", decoder_generation.simple());
    for (generation, role, member_ordinal, name) in [
        (
            anchor_generation,
            "primary-anchor",
            None,
            anchor_name.as_str(),
        ),
        (
            decoder_generation,
            "standby-decoder",
            Some(1_i32),
            decoder_name.as_str(),
        ),
    ] {
        client
            .execute(
                "INSERT INTO pgshard_catalog.managed_replication_slots( \
                     slot_generation, attachment_generation, consumer_id, \
                     logical_database_id, shard_id, slot_role, member_ordinal, slot_name \
                 ) VALUES ( \
                     $1::text::uuid, $2::text::uuid, $3::text::uuid, \
                     $4::text::uuid, 'shard-0000', $5::text, $6, $7::text \
                 )",
                &[
                    &generation.to_string(),
                    &attachment_generation.to_string(),
                    &ids.consumer,
                    &ids.logical_database,
                    &role,
                    &member_ordinal,
                    &name,
                ],
            )
            .await?;
        client
            .execute(
                "UPDATE pgshard_catalog.managed_replication_slots \
                    SET state = 'active', consistent_point = '0/10', \
                        two_phase_at = '0/10', activated_at = statement_timestamp() \
                  WHERE slot_generation = $1::text::uuid",
                &[&generation.to_string()],
            )
            .await?;
    }
    Ok((
        anchor_generation,
        decoder_generation,
        anchor_name,
        decoder_name,
    ))
}

async fn activate_attachment_and_owner(
    client: &Client,
    ids: &CatalogIds,
    checkpoint_generation: Uuid,
    attachment_generation: Uuid,
) -> TestResult {
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
                SET state = 'active', activated_at = statement_timestamp() \
              WHERE attachment_generation = $1::text::uuid",
            &[&attachment_generation.to_string()],
        )
        .await?;
    client
        .query_one(
            "SELECT pgshard_catalog.advance_logical_consumer_checkpoint( \
                 $1::text::uuid, 1, 0, '0/20', 1, false \
             )",
            &[&checkpoint_generation.to_string()],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards SET state = 'fenced' \
              WHERE consumer_id = $1::text::uuid \
                AND logical_database_id = $2::text::uuid \
                AND shard_id = 'shard-0000'",
            &[&ids.consumer, &ids.logical_database],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards SET state = 'ready' \
              WHERE consumer_id = $1::text::uuid \
                AND logical_database_id = $2::text::uuid \
                AND shard_id = 'shard-0000'",
            &[&ids.consumer, &ids.logical_database],
        )
        .await?;
    Ok(())
}

async fn create_fixture(client: &Client) -> TestResult<Fixture> {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let ids = create_consumer(client, suffix).await?;
    let (checkpoint_generation, attachment_generation) =
        create_source_records(client, &ids).await?;
    let (anchor_generation, decoder_generation, anchor_name, decoder_name) =
        allocate_active_slots(client, &ids, attachment_generation).await?;
    activate_attachment_and_owner(client, &ids, checkpoint_generation, attachment_generation)
        .await?;

    Ok(Fixture {
        consumer_id: Uuid::parse_str(&ids.consumer)?,
        logical_database_id: Uuid::parse_str(&ids.logical_database)?,
        checkpoint_generation,
        attachment_generation,
        restore_incarnation: Uuid::parse_str(&ids.restore_incarnation)?,
        anchor_generation,
        decoder_generation,
        anchor_name,
        decoder_name,
    })
}

fn limits() -> StandbyDecoderEvidenceLimits {
    StandbyDecoderEvidenceLimits::new(
        Duration::from_secs(2),
        Duration::from_secs(3),
        Duration::from_secs(3),
    )
    .expect("valid live-test evidence limits")
}

async fn catalog_identity(client: &Client) -> TestResult<(Uuid, u64)> {
    let row = client
        .query_one(
            "SELECT configuration.cluster_id::text, state.catalog_epoch \
               FROM pgshard_catalog.cluster_configuration AS configuration \
               CROSS JOIN pgshard_catalog.cluster_state AS state \
              WHERE configuration.singleton AND state.singleton",
            &[],
        )
        .await?;
    let cluster_id = Uuid::parse_str(&row.try_get::<_, String>(0)?)?;
    let catalog_epoch = u64::try_from(row.try_get::<_, i64>(1)?)?;
    Ok((cluster_id, catalog_epoch))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing at PostgreSQL 18 shardschema"]
async fn loads_only_the_ready_exact_member_policy() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let (admin, admin_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let admin_task = tokio::spawn(admin_connection);
    admin.batch_execute(MIGRATION_SQL).await?;
    let fixture = create_fixture(&admin).await?;
    let (expected_cluster_id, expected_catalog_epoch) = catalog_identity(&admin).await?;

    let (reader_client, reader_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let reader_task = tokio::spawn(reader_connection);
    let mut reader =
        SlotCatalogReader::new(reader_client, CatalogOperationTimeout::default()).await?;
    let key = LogicalConsumerShardKey::new(
        fixture.consumer_id,
        fixture.logical_database_id,
        "shard-0000",
    )?;
    let loaded = reader
        .load_standby_policy(&key, 1, limits())
        .await?
        .expect("ready selected standby policy");

    assert_eq!(loaded.cluster_id(), expected_cluster_id);
    assert_eq!(loaded.key(), &key);
    assert_eq!(
        loaded.purpose(),
        LogicalConsumerPurpose::ReshardMaterializer
    );
    assert_eq!(loaded.ownership_fence(), 1);
    assert_eq!(
        loaded.checkpoint_generation(),
        fixture.checkpoint_generation
    );
    assert_eq!(loaded.checkpoint_ordinal(), 1);
    assert_eq!(
        loaded.attachment_generation(),
        fixture.attachment_generation
    );
    assert_eq!(loaded.database_name(), "application");

    let policy = loaded.policy();
    assert_eq!(policy.member_ordinal(), 1);
    assert_eq!(policy.physical_slot().as_str(), "pgshard_member_0001");
    assert_eq!(
        policy.failover_anchor().name().as_str(),
        fixture.anchor_name
    );
    assert_eq!(
        policy.failover_anchor().generation().as_uuid(),
        fixture.anchor_generation
    );
    assert_eq!(policy.local_decoder().name().as_str(), fixture.decoder_name);
    assert_eq!(
        policy.local_decoder().generation().as_uuid(),
        fixture.decoder_generation
    );
    assert_eq!(policy.durable_checkpoint_lsn(), PgLsn(0x20));
    assert_eq!(policy.two_phase_policy().failover_anchor_at, PgLsn(0x10));
    assert_eq!(policy.two_phase_policy().local_decoder_at, PgLsn(0x10));
    let source = policy.expected_source();
    assert_eq!(source.catalog_epoch().0, expected_catalog_epoch);
    assert_eq!(source.system_identifier(), 7_219_834_723_984_723);
    assert_eq!(source.database_oid(), 16_384);
    assert_eq!(source.timeline(), 7);
    assert_eq!(source.restore_incarnation(), fixture.restore_incarnation);

    assert!(
        reader
            .load_standby_policy(&key, 2, limits())
            .await?
            .is_none(),
        "another member must not inherit this decoder allocation"
    );

    admin
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards \
                SET state = 'fenced', ownership_fence = ownership_fence + 1 \
              WHERE consumer_id = $1::text::uuid \
                AND logical_database_id = $2::text::uuid \
                AND shard_id = 'shard-0000'",
            &[
                &fixture.consumer_id.to_string(),
                &fixture.logical_database_id.to_string(),
            ],
        )
        .await?;
    assert!(
        reader
            .load_standby_policy(&key, 1, limits())
            .await?
            .is_none(),
        "a fenced owner must disappear from attachable policy reads"
    );

    drop(reader);
    drop(admin);
    tokio::time::timeout(Duration::from_secs(5), reader_task).await???;
    tokio::time::timeout(Duration::from_secs(5), admin_task).await???;
    Ok(())
}

async fn assert_snapshot_and_seed_checkpoint_rejected(
    admin: &Client,
    reader: &mut SlotCatalogReader,
    fixture: &Fixture,
    key: &LogicalConsumerShardKey,
) -> TestResult {
    admin
        .batch_execute(
            "ALTER TABLE pgshard_catalog.logical_consumer_checkpoints DISABLE TRIGGER USER",
        )
        .await?;
    admin
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
                SET snapshot_required = true \
              WHERE checkpoint_generation = $1::text::uuid",
            &[&fixture.checkpoint_generation.to_string()],
        )
        .await?;
    let snapshot_error = reader
        .load_standby_policy(key, 1, limits())
        .await
        .expect_err("snapshot-required ready policy must fail closed");
    admin
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
                SET snapshot_required = false, checkpoint_ordinal = 0 \
              WHERE checkpoint_generation = $1::text::uuid",
            &[&fixture.checkpoint_generation.to_string()],
        )
        .await?;
    let ordinal_error = reader
        .load_standby_policy(key, 1, limits())
        .await
        .expect_err("zero-ordinal ready policy must fail closed");
    admin
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_checkpoints \
                SET checkpoint_ordinal = 1 \
              WHERE checkpoint_generation = $1::text::uuid",
            &[&fixture.checkpoint_generation.to_string()],
        )
        .await?;
    admin
        .batch_execute(
            "ALTER TABLE pgshard_catalog.logical_consumer_checkpoints ENABLE TRIGGER USER",
        )
        .await?;
    assert!(matches!(
        snapshot_error,
        SlotCatalogLoadError::SnapshotRequired
    ));
    assert!(matches!(
        ordinal_error,
        SlotCatalogLoadError::ZeroCheckpointOrdinal
    ));
    Ok(())
}

async fn assert_missing_attachment_rejected(
    admin: &Client,
    reader: &mut SlotCatalogReader,
    fixture: &Fixture,
    key: &LogicalConsumerShardKey,
) -> TestResult {
    admin
        .batch_execute(
            "ALTER TABLE pgshard_catalog.logical_consumer_attachments DISABLE TRIGGER USER",
        )
        .await?;
    admin
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
                SET state = 'retiring' \
              WHERE attachment_generation = $1::text::uuid",
            &[&fixture.attachment_generation.to_string()],
        )
        .await?;
    let error = reader
        .load_standby_policy(key, 1, limits())
        .await
        .expect_err("ready policy without an active attachment must fail closed");
    admin
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments SET state = 'active' \
              WHERE attachment_generation = $1::text::uuid",
            &[&fixture.attachment_generation.to_string()],
        )
        .await?;
    admin
        .batch_execute(
            "ALTER TABLE pgshard_catalog.logical_consumer_attachments ENABLE TRIGGER USER",
        )
        .await?;
    assert!(matches!(
        error,
        SlotCatalogLoadError::IncompleteReadyPolicy("active source attachment")
    ));
    Ok(())
}

async fn assert_missing_slots_rejected(
    admin: &Client,
    reader: &mut SlotCatalogReader,
    fixture: &Fixture,
    key: &LogicalConsumerShardKey,
) -> TestResult {
    admin
        .batch_execute("ALTER TABLE pgshard_catalog.managed_replication_slots DISABLE TRIGGER USER")
        .await?;
    for (generation, missing_component) in [
        (fixture.anchor_generation, "active primary anchor"),
        (fixture.decoder_generation, "active standby decoder"),
    ] {
        admin
            .execute(
                "UPDATE pgshard_catalog.managed_replication_slots SET state = 'retiring' \
                  WHERE slot_generation = $1::text::uuid",
                &[&generation.to_string()],
            )
            .await?;
        let error = reader
            .load_standby_policy(key, 1, limits())
            .await
            .expect_err("ready policy with a missing active slot must fail closed");
        admin
            .execute(
                "UPDATE pgshard_catalog.managed_replication_slots SET state = 'active' \
                  WHERE slot_generation = $1::text::uuid",
                &[&generation.to_string()],
            )
            .await?;
        assert!(matches!(
            error,
            SlotCatalogLoadError::IncompleteReadyPolicy(component)
                if component == missing_component
        ));
    }
    admin
        .batch_execute("ALTER TABLE pgshard_catalog.managed_replication_slots ENABLE TRIGGER USER")
        .await?;
    Ok(())
}

async fn assert_missing_singleton_rejected(
    admin: &Client,
    reader: &mut SlotCatalogReader,
    key: &LogicalConsumerShardKey,
) -> TestResult {
    let catalog_epoch: i64 = admin
        .query_one(
            "DELETE FROM pgshard_catalog.cluster_state WHERE singleton \
             RETURNING catalog_epoch",
            &[],
        )
        .await?
        .try_get(0)?;
    let error = reader
        .load_standby_policy(key, 1, limits())
        .await
        .expect_err("missing cluster state must fail closed");
    admin
        .execute(
            "INSERT INTO pgshard_catalog.cluster_state(singleton, catalog_epoch) \
             VALUES (true, $1)",
            &[&catalog_epoch],
        )
        .await?;
    assert!(matches!(
        error,
        SlotCatalogLoadError::MissingSingleton("cluster_state")
    ));
    assert!(
        reader
            .load_standby_policy(key, 1, limits())
            .await?
            .is_some(),
        "restored exact policy must remain loadable"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing at PostgreSQL 18 shardschema"]
async fn rejects_incomplete_or_snapshot_required_ready_policy() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let (admin, admin_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let admin_task = tokio::spawn(admin_connection);
    admin.batch_execute(MIGRATION_SQL).await?;
    let fixture = create_fixture(&admin).await?;
    let key = LogicalConsumerShardKey::new(
        fixture.consumer_id,
        fixture.logical_database_id,
        "shard-0000",
    )?;

    let (reader_client, reader_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let reader_task = tokio::spawn(reader_connection);
    let mut reader =
        SlotCatalogReader::new(reader_client, CatalogOperationTimeout::default()).await?;

    assert_snapshot_and_seed_checkpoint_rejected(&admin, &mut reader, &fixture, &key).await?;
    assert_missing_attachment_rejected(&admin, &mut reader, &fixture, &key).await?;
    assert_missing_slots_rejected(&admin, &mut reader, &fixture, &key).await?;
    assert_missing_singleton_rejected(&admin, &mut reader, &key).await?;

    drop(reader);
    drop(admin);
    tokio::time::timeout(Duration::from_secs(5), reader_task).await???;
    tokio::time::timeout(Duration::from_secs(5), admin_task).await???;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing at PostgreSQL 18 shardschema"]
async fn blocked_policy_read_times_out_and_the_connection_retries() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let (mut admin, admin_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let admin_task = tokio::spawn(admin_connection);
    admin.batch_execute(MIGRATION_SQL).await?;
    let fixture = create_fixture(&admin).await?;
    let key = LogicalConsumerShardKey::new(
        fixture.consumer_id,
        fixture.logical_database_id,
        "shard-0000",
    )?;

    let operation_timeout = CatalogOperationTimeout::new(Duration::from_secs(1))?;
    let (reader_client, reader_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let reader_task = tokio::spawn(reader_connection);
    let reader_pid: i32 = reader_client
        .query_one("SELECT pg_backend_pid()", &[])
        .await?
        .try_get(0)?;
    let mut reader = SlotCatalogReader::new(reader_client, operation_timeout).await?;

    let (delay_client, delay_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let delay_task = tokio::spawn(delay_connection);
    delay_client
        .batch_execute(
            "BEGIN; \
             LOCK TABLE pgshard_catalog.cluster_configuration IN ACCESS EXCLUSIVE MODE",
        )
        .await?;
    let release_delay = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        delay_client.batch_execute("ROLLBACK").await
    });

    let blocker = admin.transaction().await?;
    blocker
        .batch_execute("LOCK TABLE pgshard_catalog.logical_consumers IN ACCESS EXCLUSIVE MODE")
        .await?;
    let started = Instant::now();
    let error = reader
        .load_standby_policy(&key, 1, limits())
        .await
        .expect_err("blocked catalog read must time out");
    let elapsed = started.elapsed();
    assert!(matches!(
        error,
        SlotCatalogLoadError::StatementTimeout { timeout }
            if timeout == operation_timeout.get()
    ));
    assert!(
        elapsed >= Duration::from_millis(700) && elapsed < Duration::from_secs(2),
        "blocked read elapsed {elapsed:?}"
    );
    tokio::time::timeout(Duration::from_secs(2), release_delay).await???;
    tokio::time::timeout(Duration::from_secs(5), delay_task).await???;

    let cleanup = blocker
        .query_one(
            "SELECT state, xact_start IS NULL \
               FROM pg_catalog.pg_stat_activity WHERE pid = $1",
            &[&reader_pid],
        )
        .await?;
    let state: String = cleanup.try_get(0)?;
    let no_transaction: bool = cleanup.try_get(1)?;
    assert_eq!(state, "idle");
    assert!(no_transaction, "reader backend retained a transaction");
    blocker.rollback().await?;

    assert!(
        reader
            .load_standby_policy(&key, 1, limits())
            .await?
            .is_some(),
        "the same dedicated connection must retry after server cancellation"
    );

    drop(reader);
    drop(admin);
    tokio::time::timeout(Duration::from_secs(5), reader_task).await???;
    tokio::time::timeout(Duration::from_secs(5), admin_task).await???;
    Ok(())
}
