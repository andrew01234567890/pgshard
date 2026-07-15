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
        LocalLogicalSlotObservationBatch, LocalPrimaryReplicationObservationBatch,
        LocalSlotObservationError, LocalSlotSyncWorkerActivity, LocalWalReceiverActivity,
        LocalWalSenderActivity, LogicalSlotObservationRequest,
        PrimaryReplicationObservationRequest, StandbyReplicationPathCorrelationError,
        correlate_standby_replication_path, observe_local_logical_slots,
        observe_local_primary_replication,
    },
    standby_slots::{
        FailoverSlotSynchronization, LogicalSlotKind, LogicalSlotPlugin, LogicalWalLevel,
        ManagedSlotTarget, ManagedTwoPhasePolicy, RecoveryState, ReplicationSlotName,
        ReplicationSourceIdentity, SettingState, SlotActivity, SlotGeneration, SlotOwnership,
        SlotPersistence, SlotWalRetention, StandbyDecoderEvidenceLimits, StandbyDecoderPolicy,
        StandbyDecoderTarget,
    },
};
use pgshard_types::{CatalogEpoch, PgLsn};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    task::JoinHandle,
    time::{Instant, sleep, timeout, timeout_at},
};
use tokio_postgres::{
    Client, Config, Connection, NoTls, error::SqlState, types::PgLsn as WirePgLsn,
};
use uuid::Uuid;

const CREATE_LOGICAL_SLOT_SQL: &str = "\
    SELECT slot_name::pg_catalog.text, lsn::pg_catalog.text \
      FROM pg_catalog.pg_create_logical_replication_slot( \
               $1::pg_catalog.name, 'pgoutput'::pg_catalog.name, \
               $2::pg_catalog.bool, $3::pg_catalog.bool, $4::pg_catalog.bool)";
const CONNECTION_EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const CLEANUP_RETRY_INTERVAL: Duration = Duration::from_millis(20);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const EXPECTED_PRIMARY_SLOT_NAME: &str = "pgshard_member_0001";
const EXPECTED_SYNCED_ANCHOR_NAME: &str = "pgshard_ci_anchor_00000000000000000000000000000001";
const STANDBY_CATCHUP_TIMEOUT: Duration = Duration::from_mins(1);
const STANDBY_POLL_INTERVAL: Duration = Duration::from_millis(200);
const RESTRICTED_OBSERVER_ROLE: &str = "pgshard_observer_restricted";
const RESTRICTED_OBSERVER_PASSWORD: &str = "pgshard-test-only";
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

fn pg_lsn_text(lsn: PgLsn) -> String {
    format!("{:X}/{:X}", lsn.0 >> 32, lsn.0 & u64::from(u32::MAX))
}

fn expected_synced_anchor() -> TestResult<ManagedSlotTarget> {
    let generation = SlotGeneration::new(Uuid::from_u128(1))?;
    Ok(ManagedSlotTarget::new(
        ReplicationSlotName::new(EXPECTED_SYNCED_ANCHOR_NAME)?,
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

async fn run_standby_observation_fixture(
    primary_database_url: String,
    standby_database_url: String,
    expected_system_identifier: u64,
) -> TestResult {
    let target = expected_synced_anchor()?;
    let request = LogicalSlotObservationRequest::new(vec![target.clone()])?;
    let deadline = Instant::now() + STANDBY_CATCHUP_TIMEOUT;
    let batch = loop {
        let batch = observe(&standby_database_url, &request).await?;
        if synchronized_anchor_waiting(&batch) {
            break batch;
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "standby did not synchronize failover slot {:?} within {STANDBY_CATCHUP_TIMEOUT:?}",
                target.name().as_str()
            ))
            .into());
        }
        sleep(STANDBY_POLL_INTERVAL).await;
    };

    assert_standby_observation(&batch, &target, expected_system_identifier);
    assert_correlated_replication_path(
        &primary_database_url,
        &standby_database_url,
        &request,
        &target,
    )
    .await?;

    let (role_check_client, role_check_connection) =
        tokio_postgres::connect(&standby_database_url, NoTls).await?;
    let role_check_task = tokio::spawn(role_check_connection);
    let role_options = role_check_client
        .query_one(
            "SELECT pg_catalog.pg_has_role( \
                        $1::pg_catalog.name, 'pg_read_all_stats', 'MEMBER' \
                    ), \
                    pg_catalog.pg_has_role( \
                        $1::pg_catalog.name, 'pg_read_all_stats', 'USAGE' \
                    )",
            &[&RESTRICTED_OBSERVER_ROLE],
        )
        .await?;
    assert!(role_options.try_get::<_, bool>(0)?);
    assert!(!role_options.try_get::<_, bool>(1)?);
    drop(role_check_client);
    finish_connection(role_check_task).await?;

    let mut restricted_config: Config = standby_database_url.parse()?;
    restricted_config.user(RESTRICTED_OBSERVER_ROLE);
    restricted_config.password(RESTRICTED_OBSERVER_PASSWORD);
    let (restricted_client, restricted_connection) = restricted_config.connect(NoTls).await?;
    let error = observe_local_logical_slots(
        restricted_client,
        restricted_connection,
        CatalogOperationTimeout::default(),
        &request,
    )
    .await
    .expect_err("a live receiver redacted from the observer role must fail closed");
    assert!(matches!(
        error,
        LocalSlotObservationError::WalReceiverDetailsUnavailable { .. }
    ));
    Ok(())
}

async fn assert_correlated_replication_path(
    primary_database_url: &str,
    standby_database_url: &str,
    standby_request: &LogicalSlotObservationRequest,
    anchor: &ManagedSlotTarget,
) -> TestResult {
    let physical_slot = ReplicationSlotName::new(EXPECTED_PRIMARY_SLOT_NAME)?;
    let primary_request =
        PrimaryReplicationObservationRequest::new(physical_slot.clone(), anchor.clone())?;
    let (replay_control, replay_control_connection) =
        tokio_postgres::connect(standby_database_url, NoTls).await?;
    let replay_control_task = tokio::spawn(replay_control_connection);
    let paused_phase: TestResult<_> = async {
        pause_standby_replay(&replay_control).await?;
        let required_checkpoint = create_primary_checkpoint(primary_database_url).await?;
        let initial_primary =
            wait_for_managed_primary_path(primary_database_url, &primary_request).await?;
        let initial_standby = observe(standby_database_url, standby_request).await?;
        let prerequisites = initial_standby.prerequisites();
        let pre_restartpoint_floor = prerequisites.checkpoint_lsn();
        if pre_restartpoint_floor.0 >= required_checkpoint.0 {
            return Err(
                io::Error::other("standby replay floor advanced while replay was paused").into(),
            );
        }
        let (source, policy) = test_decoder_policy(
            &initial_standby,
            &physical_slot,
            anchor,
            pre_restartpoint_floor,
            required_checkpoint,
        )?;
        Ok((
            required_checkpoint,
            initial_primary,
            initial_standby,
            pre_restartpoint_floor,
            source,
            policy,
        ))
    }
    .await;
    let resume_result = resume_standby_replay(&replay_control).await;
    drop(replay_control);
    let replay_control_connection_result = finish_connection(replay_control_task).await;
    let mut paused_phase_output = None;
    let paused_phase_result = paused_phase.map(|output| paused_phase_output = Some(output));
    combine_fixture_results(
        paused_phase_result,
        resume_result,
        replay_control_connection_result,
    )?;
    let (
        required_checkpoint,
        initial_primary,
        initial_standby,
        pre_restartpoint_floor,
        source,
        policy,
    ) = paused_phase_output.expect("successful paused phase has output");

    assert_eq!(
        correlate_standby_replication_path(&policy, &initial_standby, &initial_primary),
        Err(
            StandbyReplicationPathCorrelationError::StandbyReplayFloorBehind {
                observed: pre_restartpoint_floor,
                required: required_checkpoint,
            }
        )
    );

    wait_for_standby_replay_past_checkpoint(standby_database_url, required_checkpoint).await?;
    let standby =
        advance_standby_replay_floor(standby_database_url, standby_request, required_checkpoint)
            .await?;
    let primary = wait_for_managed_primary_path(primary_database_url, &primary_request).await?;
    let proof = correlate_standby_replication_path(&policy, &standby, &primary)?;
    assert_eq!(proof.source_identity(), source);
    assert!(proof.standby_replay_floor_lsn().0 > pre_restartpoint_floor.0);
    assert!(proof.standby_replay_floor_lsn().0 >= required_checkpoint.0);
    assert_eq!(proof.source_bound_replay_floor().source_identity(), source);
    assert_eq!(
        proof.source_bound_replay_floor().lsn(),
        proof.standby_replay_floor_lsn()
    );
    assert_eq!(proof.physical_slot(), &physical_slot);
    let observed_worker = standby
        .post_slot_sync_worker()
        .expect("eligible standby has a post-slot worker");
    assert_eq!(
        proof.slot_sync_worker_identity(),
        observed_worker.identity()
    );
    assert_eq!(proof.physical_slot_persistence(), SlotPersistence::Unproven);
    assert_ne!(proof.physical_catalog_xmin().get().get(), 0);
    assert_ne!(proof.peer_reply_epoch_micros().get(), 0);
    assert_eq!(proof.failover_anchor(), anchor.name());
    assert_ne!(proof.primary_failover_anchor_confirmed_lsn().0, 0);
    assert_ne!(proof.synchronized_failover_anchor_confirmed_lsn().0, 0);
    assert!(
        proof.synchronized_failover_anchor_confirmed_lsn().0
            <= proof.primary_failover_anchor_confirmed_lsn().0
    );
    Ok(())
}

fn test_decoder_policy(
    standby: &LocalLogicalSlotObservationBatch,
    physical_slot: &ReplicationSlotName,
    anchor: &ManagedSlotTarget,
    replay_floor: PgLsn,
    required_checkpoint: PgLsn,
) -> TestResult<(ReplicationSourceIdentity, StandbyDecoderPolicy)> {
    let prerequisites = standby.prerequisites();
    let failover_anchor_at = standby
        .entries()
        .iter()
        .find(|entry| entry.target() == anchor)
        .and_then(|entry| entry.observation())
        .and_then(|observation| observation.two_phase_at)
        .ok_or_else(|| {
            io::Error::other(
                "synchronized failover anchor has no prepared-decoding activation boundary",
            )
        })?;
    let source = ReplicationSourceIdentity::new(
        prerequisites.system_identifier(),
        prerequisites.checkpoint_timeline(),
        standby.database_oid(),
        Uuid::from_u128(0xc1),
        CatalogEpoch(1),
    )?;
    let decoder_target = StandbyDecoderTarget::new(
        1,
        physical_slot.clone(),
        anchor.clone(),
        target("pgshard_ci_local")?,
    )?;
    let policy = StandbyDecoderPolicy::new(
        source,
        decoder_target,
        ManagedTwoPhasePolicy {
            failover_anchor_at,
            local_decoder_at: replay_floor,
        },
        required_checkpoint,
        StandbyDecoderEvidenceLimits::new(
            Duration::from_secs(30),
            Duration::from_secs(3),
            Duration::from_secs(3),
        )?,
    )?;
    Ok((source, policy))
}

async fn pause_standby_replay(client: &Client) -> TestResult {
    let deadline = Instant::now() + STANDBY_CATCHUP_TIMEOUT;
    timeout_at(
        deadline,
        client.batch_execute("SELECT pg_catalog.pg_wal_replay_pause()"),
    )
    .await
    .map_err(|_| io::Error::other("standby replay pause request exceeded the bound"))??;
    loop {
        let state: String = timeout_at(
            deadline,
            client.query_one(
                "SELECT pg_catalog.pg_get_wal_replay_pause_state()::pg_catalog.text",
                &[],
            ),
        )
        .await
        .map_err(|_| io::Error::other("standby replay did not pause within the bound"))??
        .try_get(0)?;
        if state == "paused" {
            return Ok(());
        }
        sleep(STANDBY_POLL_INTERVAL).await;
    }
}

async fn resume_standby_replay(client: &Client) -> TestResult {
    timeout(
        STANDBY_CATCHUP_TIMEOUT,
        client.batch_execute("SELECT pg_catalog.pg_wal_replay_resume()"),
    )
    .await
    .map_err(|_| io::Error::other("standby replay resume request exceeded the bound"))??;
    Ok(())
}

async fn create_primary_checkpoint(primary_database_url: &str) -> TestResult<PgLsn> {
    let (primary, primary_connection) =
        tokio_postgres::connect(primary_database_url, NoTls).await?;
    let primary_task = tokio::spawn(primary_connection);

    let checkpoint_result: TestResult<PgLsn> = match timeout(STANDBY_CATCHUP_TIMEOUT, async {
        primary.batch_execute("CHECKPOINT").await?;
        let checkpoint_lsn: WirePgLsn = primary
            .query_one(
                "SELECT checkpoint_lsn \
                   FROM pg_catalog.pg_control_checkpoint()",
                &[],
            )
            .await?
            .try_get(0)?;
        Ok(PgLsn(checkpoint_lsn.into()))
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(io::Error::other("primary checkpoint exceeded the bound").into()),
    };

    drop(primary);
    let primary_connection_result = finish_connection(primary_task).await;
    let checkpoint_lsn = checkpoint_result?;
    primary_connection_result?;
    Ok(checkpoint_lsn)
}

async fn wait_for_standby_replay_past_checkpoint(
    standby_database_url: &str,
    required_checkpoint: PgLsn,
) -> TestResult {
    let (standby, standby_connection) =
        tokio_postgres::connect(standby_database_url, NoTls).await?;
    let standby_task = tokio::spawn(standby_connection);
    let checkpoint_lsn = WirePgLsn::from(required_checkpoint.0);
    let deadline = Instant::now() + STANDBY_CATCHUP_TIMEOUT;
    let replay_result: TestResult = async {
        loop {
            let replayed: bool = timeout_at(
                deadline,
                standby.query_one(
                    "SELECT COALESCE( \
                                pg_catalog.pg_last_wal_replay_lsn() \
                                    OPERATOR(pg_catalog.>) $1, \
                                false)",
                    &[&checkpoint_lsn],
                ),
            )
            .await
            .map_err(|_| {
                io::Error::other("standby did not replay the primary checkpoint within the bound")
            })??
            .try_get(0)?;
            if replayed {
                return Ok(());
            }
            sleep(STANDBY_POLL_INTERVAL).await;
        }
    }
    .await;
    drop(standby);
    let standby_connection_result = finish_connection(standby_task).await;
    replay_result?;
    standby_connection_result?;
    Ok(())
}

async fn advance_standby_replay_floor(
    standby_database_url: &str,
    standby_request: &LogicalSlotObservationRequest,
    required_checkpoint: PgLsn,
) -> TestResult<LocalLogicalSlotObservationBatch> {
    let (standby, standby_connection) =
        tokio_postgres::connect(standby_database_url, NoTls).await?;
    let standby_task = tokio::spawn(standby_connection);
    let checkpoint_result: TestResult =
        match timeout(STANDBY_CATCHUP_TIMEOUT, standby.batch_execute("CHECKPOINT")).await {
            Ok(result) => result.map_err(Into::into),
            Err(_) => {
                Err(io::Error::other("standby restartpoint request exceeded the bound").into())
            }
        };
    drop(standby);
    let standby_connection_result = finish_connection(standby_task).await;
    checkpoint_result?;
    standby_connection_result?;

    let deadline = Instant::now() + STANDBY_CATCHUP_TIMEOUT;
    loop {
        let batch = observe(standby_database_url, standby_request).await?;
        if batch.prerequisites().checkpoint_lsn().0 >= required_checkpoint.0 {
            return Ok(batch);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(
                "standby control-file replay floor did not advance within the bound",
            )
            .into());
        }
        sleep(STANDBY_POLL_INTERVAL).await;
    }
}

fn synchronized_anchor_waiting(batch: &LocalLogicalSlotObservationBatch) -> bool {
    let anchor_ready = batch.entries()[0].observation().is_some_and(|slot| {
        slot.kind == LogicalSlotKind::SynchronizedFailoverAnchor
            && slot.persistence == SlotPersistence::Unproven
            && slot.activity == SlotActivity::Inactive
            && slot.invalidation.is_none()
    });
    let worker_waiting = batch
        .prerequisites()
        .slot_sync_worker()
        .zip(batch.post_slot_sync_worker())
        .is_some_and(|(before, after)| {
            before.identity() == after.identity()
                && after.activity() == LocalSlotSyncWorkerActivity::WaitingAfterCycle
        });
    anchor_ready && worker_waiting
}

fn assert_standby_observation(
    batch: &LocalLogicalSlotObservationBatch,
    target: &ManagedSlotTarget,
    expected_system_identifier: u64,
) {
    let prerequisites = batch.prerequisites();
    assert_eq!(
        prerequisites.system_identifier(),
        expected_system_identifier
    );
    assert_ne!(prerequisites.checkpoint_lsn().0, 0);
    assert_ne!(prerequisites.checkpoint_timeline(), 0);
    assert_eq!(prerequisites.recovery(), RecoveryState::Standby);
    assert_eq!(prerequisites.wal_level(), LogicalWalLevel::Logical);
    assert_eq!(prerequisites.hot_standby_feedback(), SettingState::Enabled);
    assert_eq!(
        prerequisites.wal_receiver_status_interval(),
        Duration::from_secs(1)
    );
    assert_eq!(
        prerequisites.sync_replication_slots(),
        SettingState::Enabled
    );
    assert_eq!(
        prerequisites
            .primary_slot_name()
            .map(ReplicationSlotName::as_str),
        Some(EXPECTED_PRIMARY_SLOT_NAME)
    );
    assert!(prerequisites.replay_lsn().is_some());
    assert!(prerequisites.wal_receiver_pid().is_some());
    assert_eq!(
        prerequisites.wal_receiver_activity(),
        LocalWalReceiverActivity::Streaming
    );
    assert_eq!(
        prerequisites
            .wal_receiver_slot_name()
            .map(ReplicationSlotName::as_str),
        Some(EXPECTED_PRIMARY_SLOT_NAME)
    );
    assert_eq!(
        prerequisites.wal_receiver_received_timeline(),
        Some(prerequisites.checkpoint_timeline())
    );
    let slot_sync_worker = prerequisites
        .slot_sync_worker()
        .expect("continuous slot-sync worker");
    assert_ne!(slot_sync_worker.identity().pid().get(), 0);
    assert_ne!(slot_sync_worker.identity().start_epoch_micros().get(), 0);
    let post_slot_sync_worker = batch
        .post_slot_sync_worker()
        .expect("same continuous slot-sync worker after slot collection");
    assert_eq!(
        post_slot_sync_worker.identity(),
        slot_sync_worker.identity()
    );
    assert_eq!(
        post_slot_sync_worker.activity(),
        LocalSlotSyncWorkerActivity::WaitingAfterCycle
    );
    assert!(batch.slot_collection_finished_at() <= batch.post_worker_collection_started_at());
    assert!(
        batch.post_worker_collection_started_at() <= batch.post_worker_collection_finished_at()
    );

    let observation = batch.entries()[0]
        .observation()
        .expect("synchronized standby slot");
    assert_eq!(batch.entries()[0].target(), target);
    assert_eq!(observation.name, *target.name());
    assert_eq!(observation.plugin, LogicalSlotPlugin::PgOutput);
    assert_eq!(
        observation.kind,
        LogicalSlotKind::SynchronizedFailoverAnchor
    );
    assert_eq!(observation.persistence, SlotPersistence::Unproven);
    assert_eq!(observation.two_phase, SettingState::Enabled);
    assert_eq!(observation.activity, SlotActivity::Inactive);
    assert_eq!(observation.ownership, SlotOwnership::Unknown);
    assert_eq!(observation.invalidation, None);
}

async fn assert_primary_prerequisites(
    setup: &Client,
    batch: &LocalLogicalSlotObservationBatch,
) -> TestResult {
    let expected_database_oid: i64 = setup
        .query_one(
            "SELECT oid::pg_catalog.int8 FROM pg_catalog.pg_database \
              WHERE datname OPERATOR(pg_catalog.=) pg_catalog.current_database()",
            &[],
        )
        .await?
        .try_get(0)?;
    let expected_control = setup
        .query_one(
            "SELECT control.system_identifier::pg_catalog.int8, \
                    checkpoint_control.timeline_id, \
                    checkpoint_control.checkpoint_lsn::pg_catalog.text \
               FROM pg_catalog.pg_control_system() AS control \
              CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint_control",
            &[],
        )
        .await?;
    let expected_system_identifier = expected_control.try_get::<_, i64>(0)?.cast_unsigned();
    let expected_checkpoint_timeline = expected_control.try_get::<_, i32>(1)?.cast_unsigned();
    let expected_checkpoint_lsn: String = expected_control.try_get(2)?;

    assert_eq!(batch.database_name(), "shardschema");
    assert_eq!(u32::try_from(expected_database_oid)?, batch.database_oid());
    assert!(
        batch.prerequisite_collection_started_at() <= batch.prerequisite_collection_finished_at()
    );
    assert!(batch.prerequisite_collection_finished_at() <= batch.slot_collection_started_at());
    assert!(batch.slot_collection_started_at() <= batch.slot_collection_finished_at());
    assert!(batch.slot_collection_finished_at() <= batch.post_worker_collection_started_at());
    assert!(
        batch.post_worker_collection_started_at() <= batch.post_worker_collection_finished_at()
    );
    let prerequisites = batch.prerequisites();
    assert_eq!(
        prerequisites.system_identifier(),
        expected_system_identifier
    );
    assert_eq!(
        pg_lsn_text(prerequisites.checkpoint_lsn()),
        expected_checkpoint_lsn
    );
    assert_eq!(
        prerequisites.checkpoint_timeline(),
        expected_checkpoint_timeline
    );
    assert_eq!(prerequisites.recovery(), RecoveryState::Writable);
    assert_eq!(prerequisites.wal_level(), LogicalWalLevel::Logical);
    assert_eq!(prerequisites.hot_standby_feedback(), SettingState::Enabled);
    assert_eq!(
        prerequisites.wal_receiver_status_interval(),
        Duration::from_secs(1)
    );
    assert_eq!(
        prerequisites.sync_replication_slots(),
        SettingState::Enabled
    );
    assert_eq!(
        prerequisites
            .primary_slot_name()
            .map(ReplicationSlotName::as_str),
        Some(EXPECTED_PRIMARY_SLOT_NAME)
    );
    assert_eq!(prerequisites.replay_lsn(), None);
    assert_eq!(prerequisites.wal_receiver_pid(), None);
    assert_eq!(
        prerequisites.wal_receiver_activity(),
        LocalWalReceiverActivity::Absent
    );
    assert_eq!(prerequisites.wal_receiver_slot_name(), None);
    assert_eq!(prerequisites.wal_receiver_received_timeline(), None);
    assert_eq!(prerequisites.slot_sync_worker(), None);
    assert_eq!(batch.post_slot_sync_worker(), None);
    Ok(())
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
    assert_primary_prerequisites(setup, &batch).await?;
    assert_observed_slot_states(&batch, anchor, decoder, temporary);

    let mut restricted_config: Config = database_url.parse()?;
    restricted_config.user(RESTRICTED_OBSERVER_ROLE);
    restricted_config.password(RESTRICTED_OBSERVER_PASSWORD);
    let (restricted_client, restricted_connection) = restricted_config.connect(NoTls).await?;
    let error = observe_local_logical_slots(
        restricted_client,
        restricted_connection,
        CatalogOperationTimeout::default(),
        &request,
    )
    .await
    .expect_err(
        "non-inherited pg_read_all_stats membership must not classify workers as observable",
    );
    assert!(matches!(
        error,
        LocalSlotObservationError::StatisticsPrivilegeRequired
    ));

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
#[ignore = "requires the CI PostgreSQL 18 logical-slot and standby-prerequisite settings"]
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

async fn observe_primary_path(
    database_url: &str,
    request: &PrimaryReplicationObservationRequest,
) -> TestResult<LocalPrimaryReplicationObservationBatch> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    Ok(observe_local_primary_replication(
        client,
        connection,
        CatalogOperationTimeout::default(),
        request,
    )
    .await?)
}

async fn wait_for_managed_primary_path(
    database_url: &str,
    request: &PrimaryReplicationObservationRequest,
) -> TestResult<LocalPrimaryReplicationObservationBatch> {
    let deadline = Instant::now() + STANDBY_CATCHUP_TIMEOUT;
    loop {
        let batch = observe_primary_path(database_url, request).await?;
        let ready = batch.physical_slot().is_some_and(|slot| {
            slot.catalog_xmin().is_some()
                && slot.restart_lsn().is_some()
                && matches!(
                    slot.wal_retention(),
                    Some(SlotWalRetention::Reserved | SlotWalRetention::Extended)
                )
        }) && batch.wal_sender().is_some_and(|sender| {
            sender.activity() == LocalWalSenderActivity::Streaming
                && sender.reply_epoch_micros().is_some()
        }) && batch.failover_anchor().is_some_and(|anchor| {
            anchor.confirmed_flush_lsn.is_some_and(|lsn| lsn.0 != 0)
                && anchor.two_phase_at.is_some_and(|lsn| lsn.0 != 0)
        });
        if ready {
            return Ok(batch);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(
                "primary physical slot did not expose required streaming evidence within the bound",
            )
            .into());
        }
        sleep(STANDBY_POLL_INTERVAL).await;
    }
}

fn assert_managed_primary_path(
    batch: &LocalPrimaryReplicationObservationBatch,
    physical_slot: &ReplicationSlotName,
    failover_anchor: &ManagedSlotTarget,
) {
    assert_eq!(batch.database_name(), "shardschema");
    assert_ne!(batch.database_oid(), 0);
    assert!(batch.collection_started_at() <= batch.collection_finished_at());
    assert_ne!(batch.system_identifier(), 0);
    assert_ne!(batch.checkpoint_timeline(), 0);
    assert_eq!(batch.current_timeline(), Some(batch.checkpoint_timeline()));
    assert_eq!(batch.recovery(), RecoveryState::Writable);
    assert_eq!(batch.wal_level(), LogicalWalLevel::Logical);
    assert_eq!(
        batch.failover_slot_synchronization(),
        FailoverSlotSynchronization::GatedOnPhysicalSlot
    );
    let observed_slot = batch.physical_slot().expect("managed physical slot");
    assert_eq!(observed_slot.name(), physical_slot);
    assert_eq!(observed_slot.persistence(), SlotPersistence::Unproven);
    assert!(observed_slot.catalog_xmin().is_some());
    assert!(observed_slot.restart_lsn().is_some());
    assert_eq!(observed_slot.invalidation(), None);
    let active_pid = match observed_slot.activity() {
        SlotActivity::Active(pid) => pid,
        SlotActivity::Inactive => panic!("managed physical slot is inactive"),
    };
    let sender = batch.wal_sender().expect("physical slot walsender");
    assert_eq!(sender.identity().pid(), active_pid);
    assert_ne!(sender.identity().start_epoch_micros().get(), 0);
    assert_eq!(sender.application_name(), physical_slot);
    assert_eq!(sender.activity(), LocalWalSenderActivity::Streaming);
    assert!(sender.reply_epoch_micros().is_some());

    let anchor = batch.failover_anchor().expect("primary failover anchor");
    assert_eq!(anchor.name, *failover_anchor.name());
    assert_eq!(anchor.database_oid, batch.database_oid());
    assert_eq!(anchor.plugin, LogicalSlotPlugin::PgOutput);
    assert_eq!(anchor.kind, LogicalSlotKind::FailoverAnchor);
    assert_eq!(anchor.persistence, SlotPersistence::Unproven);
    assert_eq!(anchor.two_phase, SettingState::Enabled);
    assert!(anchor.two_phase_at.is_some_and(|lsn| lsn.0 != 0));
    assert_eq!(anchor.activity, SlotActivity::Inactive);
    assert_eq!(anchor.ownership, SlotOwnership::Unknown);
    assert_eq!(anchor.invalidation, None);
    assert!(matches!(
        anchor.wal_retention,
        Some(SlotWalRetention::Reserved | SlotWalRetention::Extended)
    ));
    let confirmed_flush_lsn = anchor
        .confirmed_flush_lsn
        .expect("primary anchor confirmed-flush LSN");
    assert_ne!(confirmed_flush_lsn.0, 0);
    assert!(anchor.two_phase_at.expect("checked boundary").0 <= confirmed_flush_lsn.0);
}

async fn wait_for_missing_primary_path(
    database_url: &str,
    request: &PrimaryReplicationObservationRequest,
) -> TestResult {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let observation = timeout_at(deadline, observe_primary_path(database_url, request))
            .await
            .map_err(|_| {
                io::Error::other(format!(
                    "replication slot {:?} remained after its owning backend exited",
                    request.physical_slot().as_str()
                ))
            })?;
        match observation {
            Ok(missing) if missing.physical_slot().is_none() => {
                assert_eq!(
                    missing.failover_slot_synchronization(),
                    FailoverSlotSynchronization::NotGated
                );
                assert_eq!(missing.wal_sender(), None);
                assert!(missing.failover_anchor().is_some());
                return Ok(());
            }
            Ok(_) => {}
            Err(error) => {
                let expected_collision = matches!(
                    error.downcast_ref::<LocalSlotObservationError>(),
                    Some(LocalSlotObservationError::PhysicalSlotNameCollision(name))
                        if name == request.physical_slot().as_str()
                );
                if !expected_collision {
                    return Err(error);
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "replication slot {:?} remained after its owning backend exited",
                request.physical_slot().as_str()
            ))
            .into());
        }
        sleep(CLEANUP_RETRY_INTERVAL).await;
    }
}

async fn exercise_temporary_physical_slot(database_url: &str) -> TestResult {
    let temporary_target = target("pgshard_test_physical")?;
    let temporary_name = temporary_target.name().clone();
    let (temporary_owner, temporary_connection) =
        tokio_postgres::connect(database_url, NoTls).await?;
    let temporary_task = tokio::spawn(temporary_connection);
    let temporary_request = PrimaryReplicationObservationRequest::new(
        temporary_name.clone(),
        expected_synced_anchor()?,
    )?;
    let fixture_result: TestResult = async {
        temporary_owner
            .query_one(
                "SELECT slot_name::pg_catalog.text, lsn::pg_catalog.text \
                   FROM pg_catalog.pg_create_physical_replication_slot( \
                        $1::pg_catalog.name, true, true)",
                &[&temporary_name.as_str()],
            )
            .await?;
        let owner_pid: i32 = temporary_owner
            .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
            .await?
            .try_get(0)?;
        let temporary = observe_primary_path(database_url, &temporary_request).await?;
        assert_eq!(
            temporary.failover_slot_synchronization(),
            FailoverSlotSynchronization::NotGated
        );
        let slot = temporary.physical_slot().expect("temporary physical slot");
        assert_eq!(slot.name(), &temporary_name);
        assert_eq!(slot.persistence(), SlotPersistence::NonPersistent);
        match slot.activity() {
            SlotActivity::Active(pid) => assert_eq!(pid.get(), u32::try_from(owner_pid)?),
            SlotActivity::Inactive => panic!("temporary slot lost its creating backend"),
        }
        assert!(slot.restart_lsn().is_some());
        assert_eq!(slot.catalog_xmin(), None);
        assert_eq!(temporary.wal_sender(), None);
        Ok(())
    }
    .await;
    drop(temporary_owner);
    let connection_result = finish_connection(temporary_task).await;
    fixture_result?;
    connection_result?;
    wait_for_missing_primary_path(database_url, &temporary_request).await
}

async fn exercise_logical_physical_name_collision(database_url: &str) -> TestResult {
    let collision_target = target("pgshard_test_collision")?;
    let collision_name = collision_target.name().clone();
    let (collision_owner, collision_connection) =
        tokio_postgres::connect(database_url, NoTls).await?;
    let collision_task = tokio::spawn(collision_connection);
    let collision_request = PrimaryReplicationObservationRequest::new(
        collision_name.clone(),
        expected_synced_anchor()?,
    )?;
    let fixture_result: TestResult = async {
        create_logical_slot(&collision_owner, &collision_target, true, false, false).await?;
        let error = observe_primary_path(database_url, &collision_request)
            .await
            .expect_err("a logical slot occupying the physical member name must fail closed");
        assert!(matches!(
            error.downcast_ref::<LocalSlotObservationError>(),
            Some(LocalSlotObservationError::PhysicalSlotNameCollision(name))
                if name == collision_name.as_str()
        ));
        Ok(())
    }
    .await;
    drop(collision_owner);
    let connection_result = finish_connection(collision_task).await;
    fixture_result?;
    connection_result?;
    wait_for_missing_primary_path(database_url, &collision_request).await
}

async fn assert_primary_observation_requires_effective_stats(
    database_url: &str,
    request: &PrimaryReplicationObservationRequest,
) -> TestResult {
    let mut restricted_config: Config = database_url.parse()?;
    restricted_config.user(RESTRICTED_OBSERVER_ROLE);
    restricted_config.password(RESTRICTED_OBSERVER_PASSWORD);
    let (restricted_client, restricted_connection) = restricted_config.connect(NoTls).await?;
    let error = observe_local_primary_replication(
        restricted_client,
        restricted_connection,
        CatalogOperationTimeout::default(),
        request,
    )
    .await
    .expect_err("non-inherited statistics membership must not expose walsender state");
    assert!(matches!(
        error,
        LocalSlotObservationError::StatisticsPrivilegeRequired
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a PostgreSQL 18 primary serving the managed physical standby slot"]
async fn observes_primary_physical_slot_and_exact_walsender() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let physical_slot = ReplicationSlotName::new(EXPECTED_PRIMARY_SLOT_NAME)?;
    let failover_anchor = expected_synced_anchor()?;
    let request =
        PrimaryReplicationObservationRequest::new(physical_slot.clone(), failover_anchor.clone())?;
    let batch = wait_for_managed_primary_path(&database_url, &request).await?;
    assert_managed_primary_path(&batch, &physical_slot, &failover_anchor);

    let absent_anchor = target("pgshard_test_missing_anchor")?;
    let absent_anchor_request =
        PrimaryReplicationObservationRequest::new(physical_slot.clone(), absent_anchor)?;
    let absent_anchor_batch = observe_primary_path(&database_url, &absent_anchor_request).await?;
    assert!(absent_anchor_batch.physical_slot().is_some());
    assert!(absent_anchor_batch.wal_sender().is_some());
    assert_eq!(absent_anchor_batch.failover_anchor(), None);

    let missing_request = PrimaryReplicationObservationRequest::new(
        ReplicationSlotName::new("pgshard_member_9999")?,
        failover_anchor,
    )?;
    wait_for_missing_primary_path(&database_url, &missing_request).await?;
    exercise_temporary_physical_slot(&database_url).await?;
    exercise_logical_physical_name_collision(&database_url).await?;
    assert_primary_observation_requires_effective_stats(&database_url, &request).await
}

#[tokio::test]
#[ignore = "requires a streaming PostgreSQL 18 standby with continuous slot synchronization"]
async fn observes_streaming_standby_slot_sync_and_rejects_redacted_receiver() -> TestResult {
    let primary_database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let standby_database_url = std::env::var("PGSHARD_TEST_STANDBY_DATABASE_URL")?;
    let (primary, primary_connection) =
        tokio_postgres::connect(&primary_database_url, NoTls).await?;
    let primary_task = tokio::spawn(primary_connection);
    let expected_system_identifier = primary
        .query_one(
            "SELECT system_identifier::pg_catalog.int8 \
               FROM pg_catalog.pg_control_system()",
            &[],
        )
        .await?
        .try_get::<_, i64>(0)?
        .cast_unsigned();
    drop(primary);
    finish_connection(primary_task).await?;

    run_standby_observation_fixture(
        primary_database_url,
        standby_database_url,
        expected_system_identifier,
    )
    .await
}

#[tokio::test]
#[ignore = "requires PGSHARD_TEST_LEGACY_DATABASE_URL pointing at PostgreSQL 17"]
async fn rejects_legacy_server_before_postgres18_prerequisites() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_LEGACY_DATABASE_URL")?;
    let request = LogicalSlotObservationRequest::new(vec![target("pgshard_test_legacy")?])?;
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let error = observe_local_logical_slots(
        client,
        connection,
        CatalogOperationTimeout::default(),
        &request,
    )
    .await
    .expect_err("PostgreSQL 17 must fail before PostgreSQL 18-only settings are read");
    assert!(matches!(
        error,
        LocalSlotObservationError::UnsupportedPostgresVersion(version) if version < 180_000
    ));

    let primary_request = PrimaryReplicationObservationRequest::new(
        ReplicationSlotName::new(EXPECTED_PRIMARY_SLOT_NAME)?,
        expected_synced_anchor()?,
    )?;
    let (primary_client, primary_connection) =
        tokio_postgres::connect(&database_url, NoTls).await?;
    let error = observe_local_primary_replication(
        primary_client,
        primary_connection,
        CatalogOperationTimeout::default(),
        &primary_request,
    )
    .await
    .expect_err("PostgreSQL 17 must fail before the primary-only PostgreSQL 18 reads");
    assert!(matches!(
        error,
        LocalSlotObservationError::UnsupportedPostgresVersion(version) if version < 180_000
    ));
    Ok(())
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
