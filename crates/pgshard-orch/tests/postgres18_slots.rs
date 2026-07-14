//! Live `PostgreSQL` 18 coverage for bounded local logical-slot observation.

use std::{
    error::Error,
    io,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use pgshard_catalog::CatalogOperationTimeout;
use pgshard_orch::{
    slot_observer::{
        LocalLogicalSlotObservationBatch, LocalSlotObservationError, LogicalSlotObservationRequest,
        observe_local_logical_slots,
    },
    standby_slots::{
        LogicalSlotKind, LogicalSlotPlugin, ManagedSlotTarget, ReplicationSlotName, SettingState,
        SlotActivity, SlotGeneration, SlotOwnership, SlotPersistence, SlotWalRetention,
    },
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tokio_postgres::{Client, Config, Connection, NoTls, error::SqlState};
use uuid::Uuid;

const CREATE_LOGICAL_SLOT_SQL: &str = "\
    SELECT slot_name::pg_catalog.text, lsn::pg_catalog.text \
      FROM pg_catalog.pg_create_logical_replication_slot( \
               $1::pg_catalog.name, 'pgoutput'::pg_catalog.name, \
               $2::pg_catalog.bool, $3::pg_catalog.bool, $4::pg_catalog.bool)";
const CONNECTION_EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const CLEANUP_RETRY_INTERVAL: Duration = Duration::from_millis(20);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

type TestError = Box<dyn Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

fn combine_fixture_results(
    fixture: TestResult,
    cleanup: TestResult,
    cleanup_connection: TestResult,
) -> TestResult {
    let mut errors = Vec::new();
    for (phase, result) in [
        ("fixture", fixture),
        ("fixture cleanup", cleanup),
        ("cleanup connection", cleanup_connection),
    ] {
        if let Err(error) = result {
            errors.push((phase, error));
        }
    }
    if errors.len() == 1 {
        return Err(errors.pop().expect("one collected error").1);
    }
    if errors.is_empty() {
        return Ok(());
    }
    let detail = errors
        .into_iter()
        .map(|(phase, error)| format!("{phase}: {error}"))
        .collect::<Vec<_>>()
        .join("; ");
    Err(io::Error::other(format!("multiple live-test failures: {detail}")).into())
}

fn target(prefix: &str) -> TestResult<ManagedSlotTarget> {
    let elapsed = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    let sequence = u128::from(NEXT_GENERATION.fetch_add(1, Ordering::Relaxed));
    let pid = u128::from(std::process::id());
    let generation =
        SlotGeneration::new(Uuid::from_u128(elapsed.as_nanos() ^ (pid << 64) ^ sequence))?;
    Ok(ManagedSlotTarget::new(
        ReplicationSlotName::new(format!("{prefix}_{}", generation.as_uuid().simple()))?,
        generation,
    )?)
}

async fn create_logical_slot(
    client: &Client,
    target: &ManagedSlotTarget,
    temporary: bool,
    two_phase: bool,
    failover: bool,
) -> Result<(), tokio_postgres::Error> {
    client
        .query_one(
            CREATE_LOGICAL_SLOT_SQL,
            &[&target.name().as_str(), &temporary, &two_phase, &failover],
        )
        .await?;
    Ok(())
}

async fn drop_slot(client: &Client, target: &ManagedSlotTarget) -> TestResult {
    client
        .query_one(
            "SELECT pg_catalog.pg_drop_replication_slot($1::pg_catalog.name)",
            &[&target.name().as_str()],
        )
        .await?;
    Ok(())
}

async fn observe(
    database_url: &str,
    request: &LogicalSlotObservationRequest,
) -> TestResult<LocalLogicalSlotObservationBatch> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    Ok(observe_local_logical_slots(
        client,
        connection,
        CatalogOperationTimeout::default(),
        request,
    )
    .await?)
}

async fn observe_with_config(
    config: &Config,
    request: &LogicalSlotObservationRequest,
) -> TestResult<LocalLogicalSlotObservationBatch> {
    let (client, connection) = config.connect(NoTls).await?;
    Ok(observe_local_logical_slots(
        client,
        connection,
        CatalogOperationTimeout::default(),
        request,
    )
    .await?)
}

async fn assert_hostile_path_is_effective(config: &Config) -> TestResult {
    let (client, connection) = config.connect(NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let observed: String = client
        .query_one("SELECT current_database()::pg_catalog.text", &[])
        .await?
        .try_get(0)?;
    assert_eq!(observed, "hostile");
    drop(client);
    finish_connection(connection_task).await
}

async fn finish_connection(task: JoinHandle<Result<(), tokio_postgres::Error>>) -> TestResult {
    timeout(CONNECTION_EXIT_TIMEOUT, task).await???;
    Ok(())
}

async fn cleanup_slot(client: &Client, target: &ManagedSlotTarget) -> TestResult {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let row = client
            .query_opt(
                "SELECT active FROM pg_catalog.pg_replication_slots \
                  WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name",
                &[&target.name().as_str()],
            )
            .await?;
        let Some(row) = row else {
            return Ok(());
        };
        if !row.try_get::<_, bool>(0)? {
            return drop_slot(client, target).await;
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "test slot {:?} remained active during cleanup",
                target.name().as_str()
            ))
            .into());
        }
        sleep(CLEANUP_RETRY_INTERVAL).await;
    }
}

async fn cleanup_observation_fixture(
    client: &Client,
    targets: &[ManagedSlotTarget],
    hostile_schema: &str,
) -> TestResult {
    let mut first_error = None;
    for target in targets {
        if let Err(error) = cleanup_slot(client, target).await
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    let drop_schema_result = client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {hostile_schema} CASCADE"))
        .await;
    if let Some(error) = first_error {
        return Err(error);
    }
    drop_schema_result?;
    Ok(())
}

async fn run_observation_fixture(
    database_url: String,
    anchor: ManagedSlotTarget,
    decoder: ManagedSlotTarget,
    temporary: ManagedSlotTarget,
    hostile_schema: String,
) -> TestResult {
    let (setup, setup_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let setup_task = tokio::spawn(setup_connection);
    let result = run_observation_assertions(
        &database_url,
        &setup,
        &anchor,
        &decoder,
        &temporary,
        &hostile_schema,
    )
    .await;
    drop(setup);
    let connection_result = finish_connection(setup_task).await;
    result?;
    connection_result
}

async fn run_observation_assertions(
    database_url: &str,
    setup: &Client,
    anchor: &ManagedSlotTarget,
    decoder: &ManagedSlotTarget,
    temporary: &ManagedSlotTarget,
    hostile_schema: &str,
) -> TestResult {
    setup
        .batch_execute(&format!(
            "CREATE SCHEMA {hostile_schema}; \
             CREATE FUNCTION {hostile_schema}.current_database() \
             RETURNS pg_catalog.name LANGUAGE SQL IMMUTABLE \
             AS 'SELECT ''hostile''::pg_catalog.name'; \
             CREATE FUNCTION {hostile_schema}.current_setting(pg_catalog.text) \
             RETURNS pg_catalog.text LANGUAGE SQL IMMUTABLE \
             AS 'SELECT ''0''::pg_catalog.text'"
        ))
        .await?;
    create_logical_slot(setup, anchor, false, true, true).await?;
    create_logical_slot(setup, decoder, false, true, false).await?;
    create_logical_slot(setup, temporary, true, false, false).await?;

    let request = LogicalSlotObservationRequest::new(vec![
        decoder.clone(),
        anchor.clone(),
        temporary.clone(),
    ])?;
    let mut hostile_config: Config = database_url.parse()?;
    hostile_config.options(format!("-csearch_path={hostile_schema},pg_catalog"));
    assert_hostile_path_is_effective(&hostile_config).await?;
    let batch = observe_with_config(&hostile_config, &request).await?;
    let expected_database_oid: i64 = setup
        .query_one(
            "SELECT oid::pg_catalog.int8 FROM pg_catalog.pg_database \
              WHERE datname OPERATOR(pg_catalog.=) pg_catalog.current_database()",
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(batch.database_name(), "shardschema");
    assert_eq!(u32::try_from(expected_database_oid)?, batch.database_oid());
    assert!(batch.slot_collection_started_at() <= batch.slot_collection_finished_at());
    assert_observed_slot_states(&batch, anchor, decoder, temporary);

    drop_slot(setup, decoder).await?;
    let missing_request =
        LogicalSlotObservationRequest::new(vec![anchor.clone(), decoder.clone()])?;
    let missing = observe(database_url, &missing_request).await?;
    assert!(missing.entries()[0].observation().is_some());
    assert!(missing.entries()[1].observation().is_none());

    drop_slot(setup, anchor).await?;
    setup
        .query_one(
            "SELECT slot_name::pg_catalog.text \
               FROM pg_catalog.pg_create_physical_replication_slot( \
                    $1::pg_catalog.name, false, false)",
            &[&anchor.name().as_str()],
        )
        .await?;
    let physical_request = LogicalSlotObservationRequest::new(vec![anchor.clone()])?;
    let (physical_client, physical_connection) =
        tokio_postgres::connect(database_url, NoTls).await?;
    let error = observe_local_logical_slots(
        physical_client,
        physical_connection,
        CatalogOperationTimeout::default(),
        &physical_request,
    )
    .await
    .expect_err("physical slot name collision must fail closed");
    assert!(matches!(
        error,
        LocalSlotObservationError::NonLogicalTarget(name) if name == anchor.name().as_str()
    ));
    Ok(())
}

fn assert_observed_slot_states(
    batch: &LocalLogicalSlotObservationBatch,
    anchor: &ManagedSlotTarget,
    decoder: &ManagedSlotTarget,
    temporary: &ManagedSlotTarget,
) {
    assert_eq!(batch.entries().len(), 3);

    let decoder_observation = batch.entries()[0].observation().expect("decoder row");
    assert_eq!(batch.entries()[0].target(), decoder);
    assert_eq!(decoder_observation.name, *decoder.name());
    assert_eq!(decoder_observation.plugin, LogicalSlotPlugin::PgOutput);
    assert_eq!(
        decoder_observation.kind,
        LogicalSlotKind::StandbyLocalDecoder
    );
    assert_eq!(decoder_observation.persistence, SlotPersistence::Unproven);
    assert_eq!(decoder_observation.two_phase, SettingState::Enabled);
    assert!(decoder_observation.two_phase_at.is_some());
    assert_eq!(decoder_observation.activity, SlotActivity::Inactive);
    assert_eq!(decoder_observation.ownership, SlotOwnership::Unknown);
    assert_eq!(decoder_observation.invalidation, None);
    assert!(matches!(
        decoder_observation.wal_retention,
        Some(SlotWalRetention::Reserved | SlotWalRetention::Extended)
    ));
    assert!(decoder_observation.confirmed_flush_lsn.is_some());

    let anchor_observation = batch.entries()[1].observation().expect("anchor row");
    assert_eq!(batch.entries()[1].target(), anchor);
    assert_eq!(anchor_observation.kind, LogicalSlotKind::FailoverAnchor);
    assert_eq!(anchor_observation.persistence, SlotPersistence::Unproven);
    assert_eq!(anchor_observation.two_phase, SettingState::Enabled);
    assert_eq!(anchor_observation.ownership, SlotOwnership::Unknown);

    let temporary_observation = batch.entries()[2].observation().expect("temporary row");
    assert_eq!(batch.entries()[2].target(), temporary);
    assert_eq!(
        temporary_observation.kind,
        LogicalSlotKind::StandbyLocalDecoder
    );
    assert_eq!(
        temporary_observation.persistence,
        SlotPersistence::NonPersistent
    );
    assert_eq!(temporary_observation.two_phase, SettingState::Disabled);
    assert_eq!(temporary_observation.two_phase_at, None);
    assert_eq!(temporary_observation.ownership, SlotOwnership::Unknown);
}

#[tokio::test]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing at PostgreSQL 18 with wal_level=logical"]
async fn observes_exact_slot_states_with_pinned_search_path_and_final_cleanup() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let (cleanup, cleanup_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let cleanup_task = tokio::spawn(cleanup_connection);
    let anchor = target("pgshard_test_anchor")?;
    let decoder = target("pgshard_test_decoder")?;
    let temporary = target("pgshard_test_temp")?;
    let hostile_schema = format!("hostile_{}", anchor.generation().as_uuid().simple());
    let targets = vec![anchor.clone(), decoder.clone(), temporary.clone()];

    let fixture_result = match tokio::spawn(run_observation_fixture(
        database_url,
        anchor,
        decoder,
        temporary,
        hostile_schema.clone(),
    ))
    .await
    {
        Ok(result) => result,
        Err(error) => Err(error.into()),
    };
    let cleanup_result = cleanup_observation_fixture(&cleanup, &targets, &hostile_schema).await;
    drop(cleanup);
    let cleanup_connection_result = finish_connection(cleanup_task).await;

    combine_fixture_results(fixture_result, cleanup_result, cleanup_connection_result)
}

async fn backend_exists(client: &Client, backend_pid: i32) -> TestResult<bool> {
    Ok(client
        .query_one(
            "SELECT EXISTS ( \
                        SELECT FROM pg_catalog.pg_stat_get_activity( \
                                        NULL::pg_catalog.int4) AS activity \
                         WHERE pid OPERATOR(pg_catalog.=) $1)",
            &[&backend_pid],
        )
        .await?
        .try_get(0)?)
}

async fn backend_pid_for_application(client: &Client, application_name: &str) -> TestResult<i32> {
    client
        .query_opt(
            "SELECT pid \
               FROM pg_catalog.pg_stat_get_activity(NULL::pg_catalog.int4) \
              WHERE application_name OPERATOR(pg_catalog.=) $1::pg_catalog.text",
            &[&application_name],
        )
        .await?
        .ok_or_else(|| -> TestError {
            Box::new(io::Error::other(format!(
                "observer backend with application name {application_name:?} was absent"
            )))
        })?
        .try_get(0)
        .map_err(Into::into)
}

async fn wait_for_backend_exit(client: &Client, backend_pid: i32) -> TestResult {
    let deadline = Instant::now() + CONNECTION_EXIT_TIMEOUT;
    while backend_exists(client, backend_pid).await? {
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed-out observer backend {backend_pid} remained connected"
            ))
            .into());
        }
        sleep(CLEANUP_RETRY_INTERVAL).await;
    }
    Ok(())
}

async fn assert_locked_observation_is_terminal<S, T>(
    monitor: &Client,
    observer_client: Client,
    observer_connection: Connection<S, T>,
    request: &LogicalSlotObservationRequest,
    backend_pid: i32,
) -> TestResult
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let operation_timeout = CatalogOperationTimeout::new(Duration::from_millis(100))?;
    let started = Instant::now();
    let error = observe_local_logical_slots(
        observer_client,
        observer_connection,
        operation_timeout,
        request,
    )
    .await
    .expect_err("catalog lock must terminate the bounded observation");
    if started.elapsed() >= Duration::from_secs(2) {
        return Err(io::Error::other("locked slot observation exceeded its client bound").into());
    }
    let terminal_timeout = matches!(error, LocalSlotObservationError::OperationTimeout { .. })
        || matches!(
            error,
            LocalSlotObservationError::Postgres(ref source)
                if source.code() == Some(&SqlState::QUERY_CANCELED)
        );
    if !terminal_timeout {
        return Err(io::Error::other(format!(
            "locked slot observation returned an unexpected error: {error}"
        ))
        .into());
    }
    wait_for_backend_exit(monitor, backend_pid).await
}

#[tokio::test]
#[ignore = "requires PGSHARD_TEST_DATABASE_URL pointing at PostgreSQL 18"]
async fn timeout_aborts_the_connection_and_backend_while_the_blocker_remains() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let (blocker, blocker_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let blocker_task = tokio::spawn(blocker_connection);
    let (monitor, monitor_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let monitor_task = tokio::spawn(monitor_connection);
    let missing = target("pgshard_test_locked")?;
    let request = LogicalSlotObservationRequest::new(vec![missing])?;
    let application_name = format!(
        "pgshard_locked_{}",
        request.targets()[0].generation().as_uuid().simple()
    );
    let mut observer_config: Config = database_url.parse()?;
    observer_config.application_name(&application_name);
    let (observer_client, observer_connection) = observer_config.connect(NoTls).await?;
    let backend_pid = backend_pid_for_application(&monitor, &application_name).await?;

    blocker
        .batch_execute("BEGIN; LOCK TABLE pg_catalog.pg_database IN ACCESS EXCLUSIVE MODE")
        .await?;
    let observation_result = assert_locked_observation_is_terminal(
        &monitor,
        observer_client,
        observer_connection,
        &request,
        backend_pid,
    )
    .await;
    let rollback_result = blocker.batch_execute("ROLLBACK").await;
    drop(blocker);
    drop(monitor);
    let blocker_result = finish_connection(blocker_task).await;
    let monitor_result = finish_connection(monitor_task).await;

    observation_result?;
    rollback_result?;
    blocker_result?;
    monitor_result
}
