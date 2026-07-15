//! Live `PostgreSQL` 18 coverage for bounded local logical-slot observation.

use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use pgshard_catalog::{CatalogOperationTimeout, MIGRATION_SQL};
use pgshard_orch::{
    slot_mutator::{
        LocalSlotMutationError, ManagedLogicalSlotCatalogActivationError,
        ManagedLogicalSlotCreateRequest, ManagedLogicalSlotDropFence,
        ManagedLogicalSlotDropOutcome, ManagedLogicalSlotReceipt, ManagedLogicalSlotRole,
        activate_managed_consumer_slot, complete_managed_consumer_slot_retirement,
        create_managed_logical_slot, drop_managed_logical_slot,
    },
    slot_observer::{
        CorrelatedStandbyReplicationPath, LocalLogicalSlotObservationBatch,
        LocalPrimaryReplicationObservationBatch, LocalSlotObservationError,
        LocalSlotSyncWorkerActivity, LocalWalReceiverActivity, LocalWalSenderActivity,
        LogicalSlotObservationRequest, PrimaryReplicationObservationRequest,
        StandbyReplicationPathCorrelationError, correlate_standby_replication_path,
        observe_local_logical_slots, observe_local_primary_replication,
    },
    slot_probe_catalog::{
        CatalogSlotSyncProbe, SlotSyncProbeAllocation, SlotSyncProbeAllocationSource,
        SlotSyncProbeCatalogError, SlotSyncProbeCatalogMutation, SlotSyncProbeState,
        activate_slot_sync_probe, allocate_slot_sync_probe, begin_slot_sync_probe_retirement,
        complete_slot_sync_probe_retirement, load_live_slot_sync_probe, load_slot_sync_probe,
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
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, tcp},
    sync::oneshot,
    task::JoinHandle,
    time::{Instant, sleep, timeout, timeout_at},
};
use tokio_postgres::{
    Client, Config, Connection, GenericClient, NoTls, error::SqlState, types::PgLsn as WirePgLsn,
};
use url::Url;
use uuid::Uuid;

const CREATE_LOGICAL_SLOT_SQL: &str = "\
    SELECT slot_name::pg_catalog.text, lsn::pg_catalog.text \
      FROM pg_catalog.pg_create_logical_replication_slot( \
               $1::pg_catalog.name, 'pgoutput'::pg_catalog.name, \
               $2::pg_catalog.bool, $3::pg_catalog.bool, $4::pg_catalog.bool)";
const CONNECTION_EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const FAULT_BACKEND_EXIT_TIMEOUT: Duration = Duration::from_secs(15);
const CLEANUP_RETRY_INTERVAL: Duration = Duration::from_millis(20);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PROXY_FRAME_BYTES: usize = 16 * 1024 * 1024;
const COMMIT_QUERY_FRAME: &[u8] = b"Q\0\0\0\x0bCOMMIT\0";
const COMMIT_COMPLETE_PAYLOAD: &[u8] = b"COMMIT\0";
const ADVISORY_FENCE_KEY_STRIDE: u64 = 32;
const EXPECTED_PRIMARY_SLOT_NAME: &str = "pgshard_member_0001";
const EXPECTED_SYNCED_ANCHOR_NAME: &str = "pgshard_ci_anchor_00000000000000000000000000000001";
// Correlation, slot-sync appearance/removal, snapshot-triggered standby create,
// and cleanup each retain their own one-minute bound. The outer mutation
// fixture must cover those sequential phases instead of racing any one of them.
const MUTATION_FIXTURE_TIMEOUT: Duration = Duration::from_mins(5);
const STANDBY_CATCHUP_TIMEOUT: Duration = Duration::from_mins(1);
const STANDBY_POLL_INTERVAL: Duration = Duration::from_millis(200);
const RESTRICTED_OBSERVER_ROLE: &str = "pgshard_observer_restricted";
const RESTRICTED_OBSERVER_PASSWORD: &str = "pgshard-test-only";
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_ADVISORY_FENCE: AtomicU64 = AtomicU64::new(1);

type TestError = Box<dyn Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

#[derive(Clone)]
struct CatalogAdminTestPrincipal {
    role_name: String,
    password: String,
}

impl CatalogAdminTestPrincipal {
    fn new() -> Self {
        let identity = Uuid::new_v4().simple().to_string();
        Self {
            role_name: format!("pgshard_test_{identity}"),
            password: Uuid::new_v4().simple().to_string(),
        }
    }

    fn config(&self, database_url: &str) -> TestResult<Config> {
        let mut config: Config = database_url.parse()?;
        config.user(&self.role_name);
        config.password(&self.password);
        Ok(config)
    }

    async fn create(&self, client: &Client) -> TestResult {
        client
            .batch_execute(&format!(
                "CREATE ROLE {} LOGIN PASSWORD '{}'; \
                 GRANT pgshard_catalog_admin TO {}",
                self.role_name, self.password, self.role_name
            ))
            .await?;
        Ok(())
    }

    async fn drop_if_exists(&self, client: &Client) -> TestResult {
        client
            .batch_execute(&format!("DROP ROLE IF EXISTS {}", self.role_name))
            .await?;
        Ok(())
    }
}

fn combine_fixture_results(
    fixture: TestResult,
    cleanup: TestResult,
    cleanup_connection: TestResult,
) -> TestResult {
    combine_named_results([
        ("fixture", fixture),
        ("fixture cleanup", cleanup),
        ("cleanup connection", cleanup_connection),
    ])
}

fn combine_named_results<const N: usize>(results: [(&'static str, TestResult); N]) -> TestResult {
    let mut errors = Vec::new();
    for (phase, result) in results {
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

struct AbortOnDropConnectionTask {
    task: Option<JoinHandle<Result<(), tokio_postgres::Error>>>,
}

impl AbortOnDropConnectionTask {
    fn new(task: JoinHandle<Result<(), tokio_postgres::Error>>) -> Self {
        Self { task: Some(task) }
    }

    async fn finish(mut self, description: &'static str) -> TestResult {
        let result = timeout(
            CONNECTION_EXIT_TIMEOUT,
            self.task.as_mut().expect("connection task is present"),
        )
        .await;
        if let Ok(result) = result {
            self.task.take();
            result??;
            Ok(())
        } else {
            self.abort();
            let _ = timeout(
                CONNECTION_EXIT_TIMEOUT,
                self.task.as_mut().expect("connection task is present"),
            )
            .await;
            self.task.take();
            Err(io::Error::other(format!(
                "{description} did not terminate after bounded abort"
            ))
            .into())
        }
    }

    fn abort(&self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl Drop for AbortOnDropConnectionTask {
    fn drop(&mut self) {
        self.abort();
    }
}

struct AbortOnDropTask<T> {
    task: JoinHandle<T>,
}

impl<T> AbortOnDropTask<T> {
    fn new(task: JoinHandle<T>) -> Self {
        Self { task }
    }

    fn task(&self) -> &JoinHandle<T> {
        &self.task
    }

    fn task_mut(&mut self) -> &mut JoinHandle<T> {
        &mut self.task
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn finish_bounded_fixture(
    mut task: JoinHandle<TestResult>,
    description: &'static str,
) -> TestResult {
    if let Ok(result) = timeout(MUTATION_FIXTURE_TIMEOUT, &mut task).await {
        return result?;
    }
    task.abort();
    let aborted = timeout(CONNECTION_EXIT_TIMEOUT, task).await.map_err(|_| {
        io::Error::other(format!(
            "{description} did not terminate after bounded abort"
        ))
    })?;
    drop(aborted);
    Err(io::Error::other(format!("{description} exceeded the test bound")).into())
}

struct CommitResponseLossProxy {
    database_url: String,
    arm: oneshot::Sender<oneshot::Sender<()>>,
    task: JoinHandle<TestResult>,
}

type TargetGateArm = (
    oneshot::Sender<()>,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
);

struct TargetPreflightGateProxy {
    database_url: String,
    arm: oneshot::Sender<TargetGateArm>,
    task: JoinHandle<TestResult>,
}

async fn start_target_preflight_gate_proxy(
    database_url: &str,
) -> TestResult<TargetPreflightGateProxy> {
    let mut url = Url::parse(database_url)?;
    let upstream_host = url
        .host_str()
        .ok_or_else(|| io::Error::other("target gate requires a TCP database host"))?
        .to_owned();
    let upstream_port = url.port().unwrap_or(5432);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let proxy_port = listener.local_addr()?.port();
    url.set_host(Some("127.0.0.1"))
        .map_err(|_| io::Error::other("target gate could not replace the database host"))?;
    url.set_port(Some(proxy_port))
        .map_err(|()| io::Error::other("target gate could not replace the database port"))?;
    let query_parameters = url
        .query_pairs()
        .filter(|(name, _)| name != "sslmode")
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (name, value) in query_parameters {
            query.append_pair(&name, &value);
        }
        query.append_pair("sslmode", "disable");
    }
    let (arm, armed) = oneshot::channel();
    let task = tokio::spawn(run_target_preflight_gate_proxy(
        listener,
        upstream_host,
        upstream_port,
        armed,
    ));
    Ok(TargetPreflightGateProxy {
        database_url: url.into(),
        arm,
        task,
    })
}

async fn run_target_preflight_gate_proxy(
    listener: TcpListener,
    upstream_host: String,
    upstream_port: u16,
    armed: oneshot::Receiver<TargetGateArm>,
) -> TestResult {
    let (downstream, _) = timeout(CONNECTION_EXIT_TIMEOUT, listener.accept())
        .await
        .map_err(|_| io::Error::other("target gate accept exceeded the bound"))??;
    let upstream = timeout(
        CONNECTION_EXIT_TIMEOUT,
        TcpStream::connect((upstream_host.as_str(), upstream_port)),
    )
    .await
    .map_err(|_| io::Error::other("target gate upstream connect exceeded the bound"))??;
    downstream.set_nodelay(true)?;
    upstream.set_nodelay(true)?;
    let (mut downstream_read, mut downstream_write) = downstream.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();
    let (acknowledge_armed, blocked, release) = Box::pin(relay_until_target_gate_armed(
        &mut downstream_read,
        &mut downstream_write,
        &mut upstream_read,
        &mut upstream_write,
        armed,
    ))
    .await?;
    acknowledge_armed
        .send(())
        .map_err(|()| io::Error::other("target gate arm acknowledgement was dropped"))?;
    let mut blocked_frontend = [0_u8; 8192];
    let blocked_bytes = timeout(
        CONNECTION_EXIT_TIMEOUT,
        downstream_read.read(&mut blocked_frontend),
    )
    .await
    .map_err(|_| io::Error::other("target preflight did not reach the armed gate"))??;
    if blocked_bytes == 0 {
        return Err(
            io::Error::other("target client closed before preflight reached the gate").into(),
        );
    }
    blocked
        .send(())
        .map_err(|()| io::Error::other("target gate blocked-query observer was dropped"))?;
    release
        .await
        .map_err(|_| io::Error::other("target gate release authority was dropped"))?;
    upstream_write
        .write_all(&blocked_frontend[..blocked_bytes])
        .await?;
    upstream_write.flush().await?;

    tokio::select! {
        result = tokio::io::copy(&mut downstream_read, &mut upstream_write) => {
            result?;
        }
        result = tokio::io::copy(&mut upstream_read, &mut downstream_write) => {
            result?;
        }
    }
    Ok(())
}

async fn relay_until_target_gate_armed(
    downstream_read: &mut tcp::OwnedReadHalf,
    downstream_write: &mut tcp::OwnedWriteHalf,
    upstream_read: &mut tcp::OwnedReadHalf,
    upstream_write: &mut tcp::OwnedWriteHalf,
    mut armed: oneshot::Receiver<TargetGateArm>,
) -> TestResult<TargetGateArm> {
    let mut frontend = [0_u8; 8192];
    let mut backend = [0_u8; 8192];
    loop {
        tokio::select! {
            arm_result = &mut armed => {
                return arm_result.map_err(|_| io::Error::other("target gate arm sender was dropped").into());
            }
            read = downstream_read.read(&mut frontend) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other("target client closed before the gate was armed").into());
                }
                upstream_write.write_all(&frontend[..read]).await?;
                upstream_write.flush().await?;
            }
            read = upstream_read.read(&mut backend) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other("target server closed before the gate was armed").into());
                }
                downstream_write.write_all(&backend[..read]).await?;
                downstream_write.flush().await?;
            }
        }
    }
}

async fn start_commit_response_loss_proxy(
    database_url: &str,
) -> TestResult<CommitResponseLossProxy> {
    let mut url = Url::parse(database_url)?;
    let upstream_host = url
        .host_str()
        .ok_or_else(|| io::Error::other("catalog fault proxy requires a TCP database host"))?
        .to_owned();
    let upstream_port = url.port().unwrap_or(5432);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let proxy_port = listener.local_addr()?.port();
    url.set_host(Some("127.0.0.1"))
        .map_err(|_| io::Error::other("catalog fault proxy could not replace the database host"))?;
    url.set_port(Some(proxy_port)).map_err(|()| {
        io::Error::other("catalog fault proxy could not replace the database port")
    })?;
    let query_parameters = url
        .query_pairs()
        .filter(|(name, _)| name != "sslmode")
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (name, value) in query_parameters {
            query.append_pair(&name, &value);
        }
        query.append_pair("sslmode", "disable");
    }
    let (arm, armed) = oneshot::channel();
    let task = tokio::spawn(run_commit_response_loss_proxy(
        listener,
        upstream_host,
        upstream_port,
        armed,
    ));
    Ok(CommitResponseLossProxy {
        database_url: url.into(),
        arm,
        task,
    })
}

async fn run_commit_response_loss_proxy(
    listener: TcpListener,
    upstream_host: String,
    upstream_port: u16,
    armed: oneshot::Receiver<oneshot::Sender<()>>,
) -> TestResult {
    let (downstream, _) = timeout(CONNECTION_EXIT_TIMEOUT, listener.accept())
        .await
        .map_err(|_| io::Error::other("catalog fault proxy accept exceeded the bound"))??;
    let upstream = timeout(
        CONNECTION_EXIT_TIMEOUT,
        TcpStream::connect((upstream_host.as_str(), upstream_port)),
    )
    .await
    .map_err(|_| io::Error::other("catalog fault proxy upstream connect exceeded the bound"))??;
    downstream.set_nodelay(true)?;
    upstream.set_nodelay(true)?;
    let (mut downstream_read, mut downstream_write) = downstream.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();
    let acknowledge_armed = Box::pin(relay_until_proxy_armed(
        &mut downstream_read,
        &mut downstream_write,
        &mut upstream_read,
        &mut upstream_write,
        armed,
    ))
    .await?;
    acknowledge_armed
        .send(())
        .map_err(|()| io::Error::other("catalog fault proxy arm acknowledgement was dropped"))?;
    Box::pin(relay_until_commit(
        &mut downstream_read,
        &mut downstream_write,
        &mut upstream_read,
        &mut upstream_write,
    ))
    .await
}

async fn relay_until_proxy_armed(
    downstream_read: &mut tcp::OwnedReadHalf,
    downstream_write: &mut tcp::OwnedWriteHalf,
    upstream_read: &mut tcp::OwnedReadHalf,
    upstream_write: &mut tcp::OwnedWriteHalf,
    mut armed: oneshot::Receiver<oneshot::Sender<()>>,
) -> TestResult<oneshot::Sender<()>> {
    let mut frontend = [0_u8; 8192];
    let mut backend = [0_u8; 8192];
    loop {
        tokio::select! {
            arm_result = &mut armed => {
                return arm_result.map_err(|_| io::Error::other("catalog fault proxy arm sender was dropped").into());
            }
            read = downstream_read.read(&mut frontend) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other("catalog client closed before the fault proxy was armed").into());
                }
                upstream_write.write_all(&frontend[..read]).await?;
                upstream_write.flush().await?;
            }
            read = upstream_read.read(&mut backend) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other("catalog server closed before the fault proxy was armed").into());
                }
                downstream_write.write_all(&backend[..read]).await?;
                downstream_write.flush().await?;
            }
        }
    }
}

async fn relay_until_commit(
    downstream_read: &mut tcp::OwnedReadHalf,
    downstream_write: &mut tcp::OwnedWriteHalf,
    upstream_read: &mut tcp::OwnedReadHalf,
    upstream_write: &mut tcp::OwnedWriteHalf,
) -> TestResult {
    let mut frontend_read = [0_u8; 8192];
    let mut backend_read = [0_u8; 8192];
    let mut frontend_frames = Vec::new();
    loop {
        tokio::select! {
            read = downstream_read.read(&mut frontend_read) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other("catalog client closed before COMMIT dispatch").into());
                }
                frontend_frames.extend_from_slice(&frontend_read[..read]);
                while let Some(frame) = take_protocol_frame(&mut frontend_frames)? {
                    if frame == COMMIT_QUERY_FRAME {
                        downstream_write.shutdown().await?;
                        upstream_write.write_all(&frame).await?;
                        upstream_write.flush().await?;
                        return wait_for_committed_response(upstream_read).await;
                    }
                    upstream_write.write_all(&frame).await?;
                    upstream_write.flush().await?;
                }
            }
            read = upstream_read.read(&mut backend_read) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other("catalog server closed before COMMIT dispatch").into());
                }
                downstream_write.write_all(&backend_read[..read]).await?;
                downstream_write.flush().await?;
            }
        }
    }
}

fn take_protocol_frame(buffer: &mut Vec<u8>) -> TestResult<Option<Vec<u8>>> {
    if buffer.len() < 5 {
        return Ok(None);
    }
    let body_length = u32::from_be_bytes(buffer[1..5].try_into()?) as usize;
    if !(4..=MAX_PROXY_FRAME_BYTES).contains(&body_length) {
        return Err(
            io::Error::other("catalog fault proxy observed an invalid protocol frame").into(),
        );
    }
    let frame_length = body_length
        .checked_add(1)
        .ok_or_else(|| io::Error::other("catalog fault proxy frame length overflowed"))?;
    if buffer.len() < frame_length {
        return Ok(None);
    }
    Ok(Some(buffer.drain(..frame_length).collect()))
}

async fn wait_for_committed_response(upstream_read: &mut tcp::OwnedReadHalf) -> TestResult {
    let deadline = Instant::now() + CONNECTION_EXIT_TIMEOUT;
    let mut read_buffer = [0_u8; 8192];
    let mut backend_frames = Vec::new();
    let mut commit_completed = false;
    loop {
        let read = timeout_at(deadline, upstream_read.read(&mut read_buffer))
            .await
            .map_err(|_| io::Error::other("catalog COMMIT response exceeded the proxy bound"))??;
        if read == 0 {
            return Err(io::Error::other("catalog server closed before confirming COMMIT").into());
        }
        backend_frames.extend_from_slice(&read_buffer[..read]);
        while let Some(frame) = take_protocol_frame(&mut backend_frames)? {
            match frame[0] {
                b'C' if &frame[5..] == COMMIT_COMPLETE_PAYLOAD => commit_completed = true,
                b'E' => {
                    return Err(
                        io::Error::other("catalog server rejected the injected COMMIT").into(),
                    );
                }
                b'Z' if commit_completed => return Ok(()),
                _ => {}
            }
        }
    }
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

async fn wait_for_synchronized_copy(
    database_url: &str,
    target: &ManagedSlotTarget,
    expected_present: bool,
) -> TestResult {
    let deadline = Instant::now() + STANDBY_CATCHUP_TIMEOUT;
    let (client, connection) = timeout_at(deadline, tokio_postgres::connect(database_url, NoTls))
        .await
        .map_err(|_| io::Error::other("standby copy connection exceeded the test bound"))??;
    let connection_task = tokio::spawn(connection);
    let observation = async {
        loop {
            let row = client
                .query_opt(
                    "SELECT synced, failover, temporary, active, \
                            plugin OPERATOR(pg_catalog.=) 'pgoutput'::pg_catalog.name, \
                            slot_type OPERATOR(pg_catalog.=) 'logical'::pg_catalog.text \
                       FROM pg_catalog.pg_replication_slots \
                      WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name",
                    &[&target.name().as_str()],
                )
                .await?;
            if !expected_present && row.is_none() {
                return Ok::<(), TestError>(());
            }
            if let Some(row) = row {
                let exact_synchronized_copy = row.try_get::<_, bool>(0)?
                    && row.try_get::<_, bool>(1)?
                    && !row.try_get::<_, bool>(2)?
                    && !row.try_get::<_, bool>(3)?
                    && row.try_get::<_, bool>(4)?
                    && row.try_get::<_, bool>(5)?;
                if expected_present && exact_synchronized_copy {
                    return Ok::<(), TestError>(());
                }
            }
            sleep(STANDBY_POLL_INTERVAL).await;
        }
    };
    let result = timeout_at(deadline, observation).await.map_err(|_| {
        let state = if expected_present {
            "appear"
        } else {
            "disappear"
        };
        io::Error::other(format!(
            "synchronized copy {:?} did not {state} within the test bound",
            target.name().as_str()
        ))
    })?;
    drop(client);
    let connection_result = finish_connection(connection_task).await;
    result?;
    connection_result
}

async fn cleanup_slot(client: &Client, target: &ManagedSlotTarget) -> TestResult {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let row = timeout_at(
            deadline,
            client.query_opt(
                "SELECT active FROM pg_catalog.pg_replication_slots \
                  WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name",
                &[&target.name().as_str()],
            ),
        )
        .await
        .map_err(|_| io::Error::other("slot cleanup observation exceeded the bound"))??;
        let Some(row) = row else {
            return Ok(());
        };
        if !row.try_get::<_, bool>(0)? {
            return timeout_at(deadline, drop_slot(client, target))
                .await
                .map_err(|_| io::Error::other("slot cleanup drop exceeded the bound"))?;
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "test slot {:?} remained active during cleanup",
                target.name().as_str()
            ))
            .into());
        }
        timeout_at(deadline, sleep(CLEANUP_RETRY_INTERVAL))
            .await
            .map_err(|_| io::Error::other("slot cleanup retry exceeded the bound"))?;
    }
}

async fn wait_for_mutation_backend_exit(client: &Client, backend_pid: &AtomicU32) -> TestResult {
    let pid = backend_pid.load(Ordering::Acquire);
    if pid == 0 {
        return Ok(());
    }
    let pid = i32::try_from(pid)?;
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let present: bool = timeout_at(
            deadline,
            client.query_one(
                "SELECT pg_catalog.count(*) OPERATOR(pg_catalog.>) 0 \
                   FROM pg_catalog.pg_stat_activity \
                  WHERE pid OPERATOR(pg_catalog.=) $1::pg_catalog.int4",
                &[&pid],
            ),
        )
        .await
        .map_err(|_| io::Error::other("mutation-backend cleanup exceeded the bound"))??
        .try_get(0)?;
        if !present {
            backend_pid.store(0, Ordering::Release);
            return Ok(());
        }
        timeout_at(deadline, sleep(CLEANUP_RETRY_INTERVAL))
            .await
            .map_err(|_| io::Error::other("mutation backend did not exit within the bound"))?;
    }
}

async fn wait_for_backend_target_fence_retry(database_url: &str, backend_pid: i32) -> TestResult {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let result = async {
        loop {
            let query = timeout_at(
                deadline,
                client.query_opt(
                    "SELECT query::pg_catalog.text \
                       FROM pg_catalog.pg_stat_activity \
                      WHERE pid OPERATOR(pg_catalog.=) $1::pg_catalog.int4",
                    &[&backend_pid],
                ),
            )
            .await
            .map_err(|_| io::Error::other("target-fence retry observation exceeded the bound"))??
            .ok_or_else(|| {
                io::Error::other("same-name recreation backend exited before fence retry")
            })?
            .try_get::<_, String>(0)?;
            if query.contains("acquire_managed_slot_target_fence") {
                return Ok::<(), TestError>(());
            }
            timeout_at(deadline, sleep(CLEANUP_RETRY_INTERVAL))
                .await
                .map_err(|_| {
                    io::Error::other("same-name recreation did not retry the hidden target fence")
                })?;
        }
    }
    .await;
    drop(client);
    let connection_result = finish_connection(connection_task).await;
    result?;
    connection_result
}

async fn terminate_backend(client: &Client, backend_pid: i32) -> TestResult {
    let terminated: bool = timeout(
        CLEANUP_TIMEOUT,
        client.query_one(
            "SELECT pg_catalog.pg_terminate_backend($1::pg_catalog.int4)",
            &[&backend_pid],
        ),
    )
    .await
    .map_err(|_| io::Error::other("backend termination exceeded the bound"))??
    .try_get(0)?;
    if !terminated {
        return Err(io::Error::other(format!(
            "PostgreSQL did not terminate backend {backend_pid}"
        ))
        .into());
    }
    Ok(())
}

async fn wait_for_pending_creation_attempt(
    client: &Client,
    target: &ManagedSlotTarget,
) -> TestResult<Uuid> {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let row = timeout_at(
            deadline,
            client.query_opt(
                "SELECT creation_receipt_id::pg_catalog.text \
                   FROM pgshard_catalog.managed_slot_creation_attempts \
                  WHERE slot_generation = $1::pg_catalog.text::pg_catalog.uuid \
                    AND state = 'pending'",
                &[&target.generation().as_uuid().to_string()],
            ),
        )
        .await
        .map_err(|_| io::Error::other("creation-attempt observation exceeded the bound"))??;
        if let Some(row) = row {
            return Ok(Uuid::parse_str(&row.try_get::<_, String>(0)?)?);
        }
        timeout_at(deadline, sleep(CLEANUP_RETRY_INTERVAL))
            .await
            .map_err(|_| {
                io::Error::other("durable managed-slot creation attempt did not appear")
            })?;
    }
}

async fn cleanup_standby_mutation_fixture(
    client: &Client,
    catalog_database_url: &str,
    target: &ManagedSlotTarget,
    backend_pid: &AtomicU32,
) -> TestResult {
    let backend_result = wait_for_mutation_backend_exit(client, backend_pid).await;
    let slot_result = cleanup_slot(client, target).await;
    backend_result?;
    slot_result?;
    retire_standby_decoder(catalog_database_url, target).await
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
    let drop_schema_result = timeout(
        CLEANUP_TIMEOUT,
        client.batch_execute(&format!("DROP SCHEMA IF EXISTS {hostile_schema} CASCADE")),
    )
    .await
    .map_err(|_| io::Error::other("fixture schema cleanup exceeded the bound"))?;
    if let Some(error) = first_error {
        return Err(error);
    }
    drop_schema_result?;
    Ok(())
}

async fn cleanup_primary_mutation_fixture(
    client: &Client,
    catalog_database_url: &str,
    standby_database_url: &str,
    target: &ManagedSlotTarget,
    hostile_schema: &str,
    mutation_role: &str,
) -> TestResult {
    let fixture_cleanup =
        cleanup_observation_fixture(client, std::slice::from_ref(target), hostile_schema).await;
    let role_cleanup = timeout(
        CLEANUP_TIMEOUT,
        client.batch_execute(&format!("DROP ROLE IF EXISTS {mutation_role}")),
    )
    .await
    .map_err(|_| io::Error::other("mutation-role cleanup exceeded the bound"))?;
    fixture_cleanup?;
    role_cleanup?;
    wait_for_synchronized_copy(standby_database_url, target, false).await?;
    cleanup_catalog_probe_row(catalog_database_url, target).await
}

async fn cleanup_catalog_probe_fixture(
    client: &Client,
    catalog_database_url: &str,
    standby_database_url: &str,
    target: &ManagedSlotTarget,
) -> TestResult {
    let slot_cleanup = cleanup_slot(client, target).await;
    let synchronized_copy_cleanup =
        wait_for_synchronized_copy(standby_database_url, target, false).await;
    slot_cleanup?;
    synchronized_copy_cleanup?;
    cleanup_catalog_probe_row(catalog_database_url, target).await
}

async fn cleanup_catalog_probe_row(
    catalog_database_url: &str,
    target: &ManagedSlotTarget,
) -> TestResult {
    let (client, connection) = timeout(
        CLEANUP_TIMEOUT,
        tokio_postgres::connect(catalog_database_url, NoTls),
    )
    .await
    .map_err(|_| io::Error::other("catalog-probe cleanup connection exceeded the bound"))??;
    let connection_task = tokio::spawn(connection);
    let result = timeout(
        CLEANUP_TIMEOUT,
        cleanup_catalog_probe_row_with_client(&client, target),
    )
    .await
    .map_err(|_| io::Error::other("catalog-probe row cleanup exceeded the bound"));
    drop(client);
    let connection_result = finish_connection(connection_task).await;
    result??;
    connection_result
}

async fn cleanup_catalog_probe_row_with_client(
    client: &Client,
    target: &ManagedSlotTarget,
) -> TestResult {
    client
        .batch_execute(
            "SET statement_timeout = '4s'; \
             SET lock_timeout = '4s'; \
             SET transaction_timeout = '4s'",
        )
        .await?;
    let table: Option<String> = client
        .query_one(
            "SELECT pg_catalog.to_regclass( \
                 'pgshard_catalog.slot_sync_probes')::pg_catalog.text",
            &[],
        )
        .await?
        .try_get(0)?;
    if table.is_none() {
        return Ok(());
    }
    let generation = target.generation().as_uuid().to_string();
    client
        .execute(
            "SELECT pgshard_catalog.abandon_managed_slot_creation_attempt( \
                        attempts.slot_generation, attempts.slot_name::pg_catalog.text, \
                        attempts.creation_receipt_id \
                    ) \
               FROM pgshard_catalog.managed_slot_creation_attempts AS attempts \
              WHERE attempts.slot_generation = $1::pg_catalog.text::pg_catalog.uuid \
                AND attempts.state = 'pending'",
            &[&generation],
        )
        .await?;
    let fallback_receipt_id = Uuid::new_v4().to_string();
    client
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
                SET state = 'retiring', \
                    cleanup_receipt_id = CASE \
                        WHEN state = 'active' THEN creation_receipt_id \
                        ELSE $2::pg_catalog.text::pg_catalog.uuid \
                    END, \
                    retiring_at = pg_catalog.statement_timestamp() \
              WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid \
                AND state IN ('allocated', 'active')",
            &[&generation, &fallback_receipt_id],
        )
        .await?;
    let retiring_name: Option<String> = client
        .query_opt(
            "SELECT slot_name::pg_catalog.text \
               FROM pgshard_catalog.slot_sync_probes \
              WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid \
                AND state = 'retiring'",
            &[&generation],
        )
        .await?
        .map(|row| row.get(0));
    if let Some(retiring_name) = retiring_name {
        complete_catalog_probe_cleanup(client, &generation, &retiring_name).await?;
    }
    let nonretired: i64 = client
        .query_one(
            "SELECT pg_catalog.count(*) \
               FROM pgshard_catalog.slot_sync_probes \
              WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid \
                AND state <> 'retired'",
            &[&generation],
        )
        .await?
        .try_get(0)?;
    if nonretired != 0 {
        return Err(io::Error::other(
            "catalog probe cleanup left the exact generation non-retired",
        )
        .into());
    }
    Ok(())
}

async fn complete_catalog_probe_cleanup(
    client: &Client,
    generation: &str,
    slot_name: &str,
) -> TestResult {
    let cleanup_receipt: String = client
        .query_one(
            "SELECT cleanup_receipt_id::pg_catalog.text \
               FROM pgshard_catalog.slot_sync_probes \
              WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid \
                AND state = 'retiring'",
            &[&generation],
        )
        .await?
        .try_get(0)?;
    let fence_id: String = client
        .query_one(
            "SELECT acquired_fence_id::pg_catalog.text \
               FROM pgshard_catalog.acquire_managed_slot_target_fence($1::pg_catalog.text)",
            &[&slot_name],
        )
        .await?
        .try_get(0)?;
    let retirement = client
        .query_one(
            "SELECT pgshard_catalog.complete_slot_sync_probe_retirement( \
                        $1::pg_catalog.text::pg_catalog.uuid, $2::pg_catalog.text, \
                        $3::pg_catalog.text::pg_catalog.uuid, \
                        $4::pg_catalog.text::pg_catalog.uuid \
                    )",
            &[&generation, &slot_name, &cleanup_receipt, &fence_id],
        )
        .await;
    let released: bool = client
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::pg_catalog.text, $2::pg_catalog.text::pg_catalog.uuid \
                    )",
            &[&slot_name, &fence_id],
        )
        .await?
        .try_get(0)?;
    assert!(released, "catalog cleanup must release the target fence");
    retirement?;
    Ok(())
}

async fn fence_mutation_session<S, T>(
    client: &Client,
    connection: Connection<S, T>,
    mutation_role: &str,
    target: &ManagedSlotTarget,
) -> TestResult<Connection<S, T>>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    fence_mutation_session_with_locks(client, connection, mutation_role, target, 1).await
}

async fn capture_mutation_backend_pid<S, T>(
    client: &Client,
    mut connection: Connection<S, T>,
    backend_pid: &AtomicU32,
) -> TestResult<Connection<S, T>>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let query = client.query_one("SELECT pg_catalog.pg_backend_pid()::pg_catalog.int4", &[]);
    tokio::pin!(query);
    tokio::select! {
        result = &mut query => {
            let pid = u32::try_from(result?.try_get::<_, i32>(0)?)?;
            if pid == 0 {
                return Err(io::Error::other("standby mutation backend PID was zero").into());
            }
            backend_pid.store(pid, Ordering::Release);
            Ok(connection)
        }
        result = &mut connection => {
            result?;
            Err(io::Error::other("standby mutation connection ended while capturing its backend PID").into())
        }
    }
}

async fn fence_mutation_session_with_locks<S, T>(
    client: &Client,
    mut connection: Connection<S, T>,
    mutation_role: &str,
    target: &ManagedSlotTarget,
    advisory_lock_count: usize,
) -> TestResult<Connection<S, T>>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let role_query = format!("SET ROLE {mutation_role}");
    let generation = target.generation().as_uuid();
    let key_bytes: [u8; 8] = generation.as_bytes()[8..].try_into()?;
    let session_nonce = NEXT_ADVISORY_FENCE.fetch_add(1, Ordering::Relaxed);
    let session_key_offset = session_nonce.wrapping_mul(ADVISORY_FENCE_KEY_STRIDE);
    let first_lock_key = i64::from_be_bytes(key_bytes)
        .wrapping_add(i64::from_ne_bytes(session_key_offset.to_ne_bytes()));
    let lock_keys = (0..advisory_lock_count)
        .map(|offset| first_lock_key.wrapping_add(i64::try_from(offset).expect("small test bound")))
        .collect::<Vec<_>>();
    let setup = async {
        client.batch_execute(&role_query).await?;
        let all_acquired: bool = client
            .query_one(
                "SELECT pg_catalog.bool_and( \
                            pg_catalog.pg_try_advisory_lock(lock_key) \
                        ) \
                   FROM pg_catalog.unnest($1::pg_catalog.int8[]) AS keys(lock_key)",
                &[&lock_keys],
            )
            .await?
            .try_get(0)?;
        if !all_acquired {
            client
                .query_one("SELECT pg_catalog.pg_advisory_unlock_all()", &[])
                .await?;
            return Err::<(), TestError>(
                io::Error::other("unique mutation advisory fence was already held").into(),
            );
        }
        Ok::<(), TestError>(())
    };
    tokio::pin!(setup);
    tokio::select! {
        result = &mut setup => {
            result?;
            Ok(connection)
        }
        result = &mut connection => {
            result?;
            Err(io::Error::other("mutation connection ended during session fencing").into())
        }
    }
}

async fn recover_receipt_after_legacy_drop_rejection(
    catalog_database_url: &str,
    legacy_database_url: &str,
    receipt: ManagedLogicalSlotReceipt,
) -> TestResult<ManagedLogicalSlotReceipt> {
    let (catalog_client, catalog_connection) =
        tokio_postgres::connect(catalog_database_url, NoTls).await?;
    let (client, connection) = tokio_postgres::connect(legacy_database_url, NoTls).await?;
    let error = drop_managed_logical_slot(
        catalog_client,
        catalog_connection,
        client,
        connection,
        CatalogOperationTimeout::default(),
        receipt,
    )
    .await
    .expect_err("PostgreSQL 17 drop preflight must return the cleanup receipt");
    assert!(!error.outcome_is_unknown());
    let (receipt, source) = error
        .into_retry_receipt()
        .expect("pre-dispatch rejection returns the receipt");
    assert!(matches!(
        source,
        LocalSlotMutationError::UnsupportedPostgresVersion(_)
    ));
    Ok(receipt)
}

async fn assert_oversized_advisory_fence_rejected(
    catalog_database_url: &str,
    config: &Config,
    mutation_role: &str,
    target: &ManagedSlotTarget,
    source: ReplicationSourceIdentity,
) -> TestResult {
    let (catalog_client, catalog_connection) =
        tokio_postgres::connect(catalog_database_url, NoTls).await?;
    let (client, connection) = config.connect(NoTls).await?;
    let connection =
        fence_mutation_session_with_locks(&client, connection, mutation_role, target, 17).await?;
    let error = create_managed_logical_slot(
        catalog_client,
        catalog_connection,
        client,
        connection,
        CatalogOperationTimeout::default(),
        ManagedLogicalSlotCreateRequest::primary_failover_anchor(target.clone(), source),
    )
    .await
    .expect_err("an oversized advisory-lock fence must fail before dispatch");
    assert!(matches!(
        error,
        LocalSlotMutationError::TooManyAdvisoryLocks { maximum: 16 }
    ));
    Ok(())
}

async fn catalog_restore_and_epoch(database_url: &str) -> TestResult<(Uuid, CatalogEpoch)> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let row = client
        .query_one(
            "SELECT restore.restore_incarnation::pg_catalog.text, state.catalog_epoch \
               FROM pgshard_catalog.shard_restore_incarnations AS restore \
               JOIN pgshard_catalog.cluster_state AS state ON state.singleton \
              WHERE restore.shard_id = 'shard-0000' AND restore.state = 'active'",
            &[],
        )
        .await?;
    let result = (
        Uuid::parse_str(&row.try_get::<_, String>(0)?)?,
        CatalogEpoch(u64::try_from(row.try_get::<_, i64>(1)?)?),
    );
    drop(client);
    finish_connection(connection_task).await?;
    Ok(result)
}

async fn slot_sync_probe_allocation(
    database_url: &str,
    target: ManagedSlotTarget,
) -> TestResult<SlotSyncProbeAllocation> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    client.batch_execute(MIGRATION_SQL).await?;
    let row = client
        .query_one(
            "SELECT control.system_identifier::pg_catalog.int8, \
                    pg_catalog.substring( \
                        pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_lsn()), 1, 8), \
                    database.oid::pg_catalog.int8, \
                    restore.restore_incarnation::pg_catalog.text, \
                    state.catalog_epoch \
               FROM pg_catalog.pg_control_system() AS control \
               JOIN pg_catalog.pg_database AS database \
                 ON database.datname OPERATOR(pg_catalog.=) pg_catalog.current_database() \
               JOIN pgshard_catalog.shard_restore_incarnations AS restore \
                 ON restore.shard_id = 'shard-0000' AND restore.state = 'active' \
               JOIN pgshard_catalog.cluster_state AS state ON state.singleton",
            &[],
        )
        .await?;
    let source = SlotSyncProbeAllocationSource::new(
        row.try_get::<_, i64>(0)?.cast_unsigned(),
        u32::from_str_radix(&row.try_get::<_, String>(1)?, 16)?,
        u32::try_from(row.try_get::<_, i64>(2)?)?,
        Uuid::parse_str(&row.try_get::<_, String>(3)?)?,
        CatalogEpoch(u64::try_from(row.try_get::<_, i64>(4)?)?),
    )?;
    drop(client);
    finish_connection(connection_task).await?;
    Ok(SlotSyncProbeAllocation::new(
        "shard-0000",
        target,
        source,
        "shardschema",
    )?)
}

async fn allocate_catalog_probe(
    database_url: &str,
    allocation: &SlotSyncProbeAllocation,
) -> TestResult<CatalogSlotSyncProbe> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    Ok(allocate_slot_sync_probe(
        client,
        connection,
        CatalogOperationTimeout::default(),
        allocation,
    )
    .await?)
}

async fn activate_catalog_probe(
    database_url: &str,
    probe: &CatalogSlotSyncProbe,
    receipt: &ManagedLogicalSlotReceipt,
) -> TestResult<CatalogSlotSyncProbe> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    Ok(activate_slot_sync_probe(
        client,
        connection,
        CatalogOperationTimeout::default(),
        probe,
        receipt,
    )
    .await?)
}

async fn activate_catalog_probe_with_commit_response_loss(
    database_url: &str,
    probe: &CatalogSlotSyncProbe,
    receipt: &ManagedLogicalSlotReceipt,
) -> TestResult<CatalogSlotSyncProbe> {
    let CommitResponseLossProxy {
        database_url: proxy_database_url,
        arm,
        mut task,
    } = start_commit_response_loss_proxy(database_url).await?;
    let (client, connection) = tokio_postgres::connect(&proxy_database_url, NoTls).await?;
    let (armed_acknowledgement, armed) = oneshot::channel();
    arm.send(armed_acknowledgement)
        .map_err(|_| io::Error::other("catalog fault proxy exited before it was armed"))?;
    timeout(CONNECTION_EXIT_TIMEOUT, armed)
        .await
        .map_err(|_| {
            io::Error::other("catalog fault proxy arm acknowledgement exceeded the bound")
        })?
        .map_err(|_| io::Error::other("catalog fault proxy exited before acknowledging its arm"))?;
    let error = activate_slot_sync_probe(
        client,
        connection,
        CatalogOperationTimeout::default(),
        probe,
        receipt,
    )
    .await
    .expect_err("the injected catalog COMMIT response loss must make activation ambiguous");
    match error {
        SlotSyncProbeCatalogError::OutcomeUnknown {
            operation: SlotSyncProbeCatalogMutation::Activate,
            target,
            ..
        } if target == *probe.target() => {}
        other => {
            task.abort();
            let _ = timeout(CONNECTION_EXIT_TIMEOUT, task).await;
            return Err(io::Error::other(format!(
                "catalog activation response loss returned a non-ambiguous error: {other}"
            ))
            .into());
        }
    }
    let proxy_result = timeout(CONNECTION_EXIT_TIMEOUT, &mut task).await;
    if let Ok(result) = proxy_result {
        result??;
    } else {
        task.abort();
        let _ = timeout(CONNECTION_EXIT_TIMEOUT, task).await;
        return Err(io::Error::other(
            "catalog fault proxy did not confirm the dispatched COMMIT within the bound",
        )
        .into());
    }
    let active = load_exact_catalog_probe(database_url, probe.target())
        .await?
        .expect("the committed activation remains durable after response loss");
    assert_eq!(active.state(), SlotSyncProbeState::Active);
    assert_eq!(active.consistent_point(), Some(receipt.creation_lsn()));
    assert!(active.creation_receipt_present());
    assert!(!active.cleanup_receipt_present());
    assert!(active.source().catalog_epoch().0 > probe.source().catalog_epoch().0);
    Ok(active)
}

async fn begin_catalog_probe_retirement(
    database_url: &str,
    probe: &CatalogSlotSyncProbe,
    receipt: &ManagedLogicalSlotReceipt,
) -> TestResult<CatalogSlotSyncProbe> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    Ok(begin_slot_sync_probe_retirement(
        client,
        connection,
        CatalogOperationTimeout::default(),
        probe,
        receipt,
    )
    .await?)
}

async fn load_exact_catalog_probe(
    database_url: &str,
    target: &ManagedSlotTarget,
) -> TestResult<Option<CatalogSlotSyncProbe>> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    Ok(load_slot_sync_probe(
        client,
        connection,
        CatalogOperationTimeout::default(),
        target,
    )
    .await?)
}

async fn load_live_catalog_probe(database_url: &str) -> TestResult<Option<CatalogSlotSyncProbe>> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    Ok(load_live_slot_sync_probe(
        client,
        connection,
        CatalogOperationTimeout::default(),
        "shard-0000",
    )
    .await?)
}

async fn complete_catalog_probe_retirement(
    probe: &CatalogSlotSyncProbe,
    absence: &mut ManagedLogicalSlotDropFence,
) -> TestResult<CatalogSlotSyncProbe> {
    Ok(
        complete_slot_sync_probe_retirement(CatalogOperationTimeout::default(), probe, absence)
            .await?,
    )
}

async fn complete_retirement_while_recreation_waits(
    database_url: &str,
    target: &ManagedSlotTarget,
    retiring: &CatalogSlotSyncProbe,
    mut absence: ManagedLogicalSlotDropFence,
) -> TestResult<CatalogSlotSyncProbe> {
    let expected_receipt_id = absence.receipt().receipt_id();
    let mut other_database_url = Url::parse(database_url)?;
    other_database_url.set_path("/postgres");
    let (recreate_catalog, recreate_catalog_connection) =
        tokio_postgres::connect(database_url, NoTls).await?;
    let (recreate_client, recreate_connection) =
        tokio_postgres::connect(other_database_url.as_str(), NoTls).await?;
    let recreate_backend = AtomicU32::new(0);
    let recreate_catalog_connection = capture_mutation_backend_pid(
        &recreate_catalog,
        recreate_catalog_connection,
        &recreate_backend,
    )
    .await?;
    let recreate_backend_pid = i32::try_from(recreate_backend.load(Ordering::Acquire))?;
    let request =
        ManagedLogicalSlotCreateRequest::primary_failover_anchor(target.clone(), retiring.source());
    let mut recreate = AbortOnDropTask::new(tokio::spawn(async move {
        create_managed_logical_slot(
            recreate_catalog,
            recreate_catalog_connection,
            recreate_client,
            recreate_connection,
            CatalogOperationTimeout::default(),
            request,
        )
        .await
    }));
    wait_for_backend_target_fence_retry(database_url, recreate_backend_pid).await?;
    assert!(
        !recreate.task().is_finished(),
        "same-name recreation must remain blocked before catalog retirement"
    );

    let retired = complete_catalog_probe_retirement(retiring, &mut absence).await?;
    assert_eq!(retired.state(), SlotSyncProbeState::Retired);
    assert!(
        !recreate.task().is_finished(),
        "catalog retirement must return while the canonical target fence is still held"
    );
    assert_eq!(
        complete_catalog_probe_retirement(&retired, &mut absence).await?,
        retired
    );
    assert_eq!(absence.release().await.receipt_id(), expected_receipt_id);
    let rejection = timeout(CONNECTION_EXIT_TIMEOUT, recreate.task_mut())
        .await
        .map_err(|_| io::Error::other("blocked same-name recreation did not reject in time"))??
        .expect_err("retired catalog authority must reject same-name recreation");
    let rejected_by_retired_authority = match &rejection {
        LocalSlotMutationError::CatalogAuthorizationStateChanged {
            operation: pgshard_orch::slot_mutator::LocalSlotMutationOperation::Create,
            target,
            state,
        } => target == retiring.target() && state == "retired",
        LocalSlotMutationError::PreflightPostgres {
            operation: pgshard_orch::slot_mutator::LocalSlotMutationOperation::Create,
            target,
            source,
        } => {
            target == retiring.target()
                && source.as_db_error().is_some_and(|error| {
                    matches!(
                        error.message(),
                        "managed slot allocation is not eligible for creation"
                            | "managed slot creation used a stale catalog epoch"
                    )
                })
        }
        _ => false,
    };
    if !rejected_by_retired_authority {
        return Err(io::Error::other(format!(
            "retired catalog authority returned an unexpected rejection: {rejection}"
        ))
        .into());
    }
    let observed = observe(
        database_url,
        &LogicalSlotObservationRequest::new(vec![target.clone()])?,
    )
    .await?;
    assert!(observed.entries()[0].observation().is_none());
    Ok(retired)
}

async fn complete_retirement_after_catalog_fence_loss(
    database_url: &str,
    retiring: &CatalogSlotSyncProbe,
    mut absence: ManagedLogicalSlotDropFence,
    proxy: CommitResponseLossProxy,
) -> TestResult<CatalogSlotSyncProbe> {
    let expected_receipt_id = absence.receipt().receipt_id();
    let CommitResponseLossProxy {
        database_url: _,
        arm,
        mut task,
    } = proxy;
    let (armed_acknowledgement, armed) = oneshot::channel();
    arm.send(armed_acknowledgement)
        .map_err(|_| io::Error::other("retirement fault proxy exited before it was armed"))?;
    timeout(CONNECTION_EXIT_TIMEOUT, armed)
        .await
        .map_err(|_| {
            io::Error::other("retirement fault proxy arm acknowledgement exceeded the bound")
        })?
        .map_err(|_| {
            io::Error::other("retirement fault proxy exited before acknowledging its arm")
        })?;

    let error = complete_slot_sync_probe_retirement(
        CatalogOperationTimeout::default(),
        retiring,
        &mut absence,
    )
    .await
    .expect_err("losing the catalog COMMIT response must reject retirement success");
    assert!(error.outcome_is_unknown());
    assert!(matches!(
        error,
        SlotSyncProbeCatalogError::TargetFenceLost { ref target, .. }
            if target == retiring.target()
    ));
    timeout(CONNECTION_EXIT_TIMEOUT, &mut task)
        .await
        .map_err(|_| {
            io::Error::other("retirement fault proxy did not confirm COMMIT in time")
        })???;
    let released = absence.release().await;
    assert_eq!(released.receipt_id(), expected_receipt_id);
    let retired = load_exact_catalog_probe(database_url, retiring.target())
        .await?
        .expect("the exact catalog generation committed before post-COMMIT fence verification");
    assert_eq!(retired.state(), SlotSyncProbeState::Retired);
    assert!(retired.creation_receipt_present());
    assert!(retired.cleanup_receipt_present());
    reclaim_stale_target_fence(database_url, retiring.target()).await?;
    Ok(retired)
}

async fn release_known_drop(fence: ManagedLogicalSlotDropFence) {
    assert_eq!(
        fence.receipt().outcome(),
        ManagedLogicalSlotDropOutcome::Dropped
    );
    let _receipt = fence.release().await;
}

async fn reclaim_stale_target_fence(database_url: &str, target: &ManagedSlotTarget) -> TestResult {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let fence_id: String = client
        .query_one(
            "SELECT acquired_fence_id::pg_catalog.text \
               FROM pgshard_catalog.acquire_managed_slot_target_fence($1::pg_catalog.text)",
            &[&target.name().as_str()],
        )
        .await?
        .try_get(0)?;
    let released: bool = client
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::pg_catalog.text, $2::pg_catalog.text::pg_catalog.uuid \
                    )",
            &[&target.name().as_str(), &fence_id],
        )
        .await?
        .try_get(0)?;
    assert!(released, "the stale target fence must be reclaimable");
    drop(client);
    finish_connection(connection_task).await
}

async fn release_known_drop_while_registry_row_is_blocked(
    database_url: &str,
    fence: ManagedLogicalSlotDropFence,
) -> TestResult {
    assert_eq!(
        fence.receipt().outcome(),
        ManagedLogicalSlotDropOutcome::Dropped
    );
    let target_name = fence.receipt().target().name().as_str().to_owned();
    let expected_receipt_id = fence.receipt().receipt_id();
    let fence_backend_pid = i32::try_from(fence.catalog_fence_backend_pid().get())?;
    let (blocker, blocker_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let blocker_task = tokio::spawn(blocker_connection);
    blocker.batch_execute("BEGIN").await?;
    blocker
        .query_one(
            "SELECT target_name::pg_catalog.text \
               FROM pgshard_catalog.managed_slot_target_fences \
              WHERE target_name::pg_catalog.text = $1::pg_catalog.text \
              FOR UPDATE",
            &[&target_name],
        )
        .await?;

    let released = timeout(Duration::from_secs(3), fence.release())
        .await
        .map_err(|_| io::Error::other("blocked target-fence release exceeded its hard bound"))?;
    assert_eq!(released.receipt_id(), expected_receipt_id);
    blocker.batch_execute("ROLLBACK").await?;
    wait_for_backend_exit(&blocker, fence_backend_pid).await?;
    drop(blocker);
    finish_connection(blocker_task).await?;

    let (reclaimer, reclaimer_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let reclaimer_task = tokio::spawn(reclaimer_connection);
    let replacement_fence: String = reclaimer
        .query_one(
            "SELECT acquired_fence_id::pg_catalog.text \
               FROM pgshard_catalog.acquire_managed_slot_target_fence($1::pg_catalog.text)",
            &[&target_name],
        )
        .await?
        .try_get(0)?;
    let release_succeeded: bool = reclaimer
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::pg_catalog.text, $2::pg_catalog.text::pg_catalog.uuid \
                    )",
            &[&target_name, &replacement_fence],
        )
        .await?
        .try_get(0)?;
    assert!(
        release_succeeded,
        "the timed-out stale fence must be reclaimable"
    );
    drop(reclaimer);
    finish_connection(reclaimer_task).await
}

async fn refresh_after_stale_activation(
    database_url: &str,
    stale: &CatalogSlotSyncProbe,
    receipt: &ManagedLogicalSlotReceipt,
) -> TestResult<CatalogSlotSyncProbe> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    client
        .batch_execute("UPDATE pgshard_catalog.slot_sync_probes SET state = state WHERE false")
        .await?;
    drop(client);
    finish_connection(connection_task).await?;

    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let error = activate_slot_sync_probe(
        client,
        connection,
        CatalogOperationTimeout::default(),
        stale,
        receipt,
    )
    .await
    .expect_err("a catalog change must fence the stale activation token");
    assert!(matches!(
        error,
        SlotSyncProbeCatalogError::StaleCatalogEpoch { .. }
    ));

    let refreshed = load_exact_catalog_probe(database_url, stale.target())
        .await?
        .expect("allocated probe remains durable after stale activation");
    assert_eq!(refreshed.state(), SlotSyncProbeState::Allocated);
    assert!(refreshed.source().catalog_epoch().0 > stale.source().catalog_epoch().0);
    Ok(refreshed)
}

async fn assert_catalog_probe_slot_present(
    database_url: &str,
    target: &ManagedSlotTarget,
) -> TestResult {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let exact: bool = client
        .query_one(
            "SELECT pg_catalog.count(*) OPERATOR(pg_catalog.=) 1 \
               FROM pg_catalog.pg_replication_slots \
              WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name \
                AND slot_type OPERATOR(pg_catalog.=) 'logical'::pg_catalog.text \
                AND plugin OPERATOR(pg_catalog.=) 'pgoutput'::pg_catalog.name \
                AND failover AND NOT synced AND NOT temporary",
            &[&target.name().as_str()],
        )
        .await?
        .try_get(0)?;
    drop(client);
    finish_connection(connection_task).await?;
    if !exact {
        return Err(io::Error::other(
            "stale catalog absence proof removed or changed the later slot recreation",
        )
        .into());
    }
    Ok(())
}

async fn create_catalog_probe_slot(
    primary_database_url: &str,
    standby_database_url: &str,
    target: &ManagedSlotTarget,
    source: ReplicationSourceIdentity,
) -> TestResult<ManagedLogicalSlotReceipt> {
    let (catalog, catalog_connection) =
        tokio_postgres::connect(primary_database_url, NoTls).await?;
    let (primary, primary_connection) =
        tokio_postgres::connect(primary_database_url, NoTls).await?;
    let receipt = create_managed_logical_slot(
        catalog,
        catalog_connection,
        primary,
        primary_connection,
        CatalogOperationTimeout::default(),
        ManagedLogicalSlotCreateRequest::primary_failover_anchor(target.clone(), source),
    )
    .await?;
    wait_for_synchronized_copy(standby_database_url, target, true).await?;
    Ok(receipt)
}

async fn run_catalog_probe_fixture(
    primary_database_url: String,
    standby_database_url: String,
    probe_target: ManagedSlotTarget,
) -> TestResult {
    let allocation =
        slot_sync_probe_allocation(&primary_database_url, probe_target.clone()).await?;
    let allocated = allocate_catalog_probe(&primary_database_url, &allocation).await?;
    assert_eq!(allocated.shard_id(), "shard-0000");
    assert_eq!(allocated.target(), &probe_target);
    assert_eq!(allocated.database_name(), "shardschema");
    assert_eq!(allocated.state(), SlotSyncProbeState::Allocated);
    assert_eq!(allocated.consistent_point(), None);
    assert!(!allocated.creation_receipt_present());
    assert!(!allocated.cleanup_receipt_present());

    assert_catalog_probe_allocation_fencing(&primary_database_url, &allocation, &allocated).await?;

    let receipt = create_catalog_probe_slot(
        &primary_database_url,
        &standby_database_url,
        &probe_target,
        allocated.source(),
    )
    .await?;

    let allocated =
        refresh_after_stale_activation(&primary_database_url, &allocated, &receipt).await?;
    let active = activate_catalog_probe_with_commit_response_loss(
        &primary_database_url,
        &allocated,
        &receipt,
    )
    .await?;
    assert_eq!(active.state(), SlotSyncProbeState::Active);
    assert_eq!(active.consistent_point(), Some(receipt.creation_lsn()));
    assert!(active.creation_receipt_present());
    assert!(!active.cleanup_receipt_present());
    let active_retry = allocate_catalog_probe(&primary_database_url, &allocation).await?;
    assert_eq!(active_retry.target(), active.target());
    assert_eq!(active_retry.state(), active.state());
    assert_eq!(active_retry.consistent_point(), active.consistent_point());
    assert!(
        active_retry.source().catalog_epoch().0 >= active.source().catalog_epoch().0,
        "an idempotent load may observe a newer unrelated catalog epoch"
    );
    assert_eq!(
        activate_catalog_probe(&primary_database_url, &active_retry, &receipt).await?,
        active_retry
    );

    let retiring =
        begin_catalog_probe_retirement(&primary_database_url, &active_retry, &receipt).await?;
    assert_eq!(retiring.state(), SlotSyncProbeState::Retiring);
    assert!(retiring.creation_receipt_present());
    assert!(retiring.cleanup_receipt_present());
    let retiring_retry =
        begin_catalog_probe_retirement(&primary_database_url, &retiring, &receipt).await?;
    assert_eq!(retiring_retry, retiring);

    let still_retiring = load_exact_catalog_probe(&primary_database_url, &probe_target)
        .await?
        .expect("the typed retirement API cannot accept an unfenced stale receipt");
    assert_eq!(still_retiring, retiring);
    assert_catalog_probe_slot_present(&primary_database_url, &probe_target).await?;

    let (catalog, catalog_connection) =
        tokio_postgres::connect(&primary_database_url, NoTls).await?;
    let (primary, primary_connection) =
        tokio_postgres::connect(&primary_database_url, NoTls).await?;
    let absence = drop_managed_logical_slot(
        catalog,
        catalog_connection,
        primary,
        primary_connection,
        CatalogOperationTimeout::default(),
        receipt,
    )
    .await?;
    assert_eq!(
        absence.receipt().outcome(),
        ManagedLogicalSlotDropOutcome::Dropped
    );
    wait_for_synchronized_copy(&standby_database_url, &probe_target, false).await?;
    let retired = complete_retirement_while_recreation_waits(
        &primary_database_url,
        &probe_target,
        &still_retiring,
        absence,
    )
    .await?;
    assert_eq!(
        load_exact_catalog_probe(&primary_database_url, &probe_target)
            .await?
            .expect("permanent retired probe"),
        retired
    );
    assert!(
        load_live_catalog_probe(&primary_database_url)
            .await?
            .is_none()
    );
    Ok(())
}

async fn run_catalog_probe_fence_loss_fixture(
    primary_database_url: String,
    standby_database_url: String,
    probe_target: ManagedSlotTarget,
) -> TestResult {
    let allocation =
        slot_sync_probe_allocation(&primary_database_url, probe_target.clone()).await?;
    let allocated = allocate_catalog_probe(&primary_database_url, &allocation).await?;
    let receipt = create_catalog_probe_slot(
        &primary_database_url,
        &standby_database_url,
        &probe_target,
        allocated.source(),
    )
    .await?;
    let active = activate_catalog_probe(&primary_database_url, &allocated, &receipt).await?;
    let retiring = begin_catalog_probe_retirement(&primary_database_url, &active, &receipt).await?;

    let proxy = start_commit_response_loss_proxy(&primary_database_url).await?;
    let (catalog, catalog_connection) = tokio_postgres::connect(&proxy.database_url, NoTls).await?;
    let (primary, primary_connection) =
        tokio_postgres::connect(&primary_database_url, NoTls).await?;
    let absence = drop_managed_logical_slot(
        catalog,
        catalog_connection,
        primary,
        primary_connection,
        CatalogOperationTimeout::default(),
        receipt,
    )
    .await?;
    assert_eq!(
        absence.receipt().outcome(),
        ManagedLogicalSlotDropOutcome::Dropped
    );
    wait_for_synchronized_copy(&standby_database_url, &probe_target, false).await?;
    let retired = complete_retirement_after_catalog_fence_loss(
        &primary_database_url,
        &retiring,
        absence,
        proxy,
    )
    .await?;
    assert_eq!(retired.target(), &probe_target);
    assert_eq!(retired.state(), SlotSyncProbeState::Retired);
    Ok(())
}

async fn run_catalog_creation_backend_loss_fixture(
    database_url: String,
    standby_database_url: String,
    probe_target: ManagedSlotTarget,
) -> TestResult {
    let allocation = slot_sync_probe_allocation(&database_url, probe_target.clone()).await?;
    let allocated = allocate_catalog_probe(&database_url, &allocation).await?;

    let TargetPreflightGateProxy {
        database_url: gated_database_url,
        arm,
        task: gate_task,
    } = start_target_preflight_gate_proxy(&database_url).await?;
    let mut gate_task = AbortOnDropTask::new(gate_task);
    let (primary, primary_connection) = tokio_postgres::connect(&gated_database_url, NoTls).await?;
    let (armed_acknowledgement, armed) = oneshot::channel();
    let (blocked_sender, blocked) = oneshot::channel();
    let (release_gate, gate_release) = oneshot::channel();
    arm.send((armed_acknowledgement, blocked_sender, gate_release))
        .map_err(|_| io::Error::other("target preflight gate exited before it was armed"))?;
    timeout(CONNECTION_EXIT_TIMEOUT, armed)
        .await
        .map_err(|_| io::Error::other("target preflight gate arm exceeded the bound"))?
        .map_err(|_| io::Error::other("target preflight gate exited before acknowledgement"))?;

    let (observer, observer_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let observer_task = AbortOnDropConnectionTask::new(tokio::spawn(observer_connection));
    let (catalog, catalog_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let catalog_backend = AtomicU32::new(0);
    let catalog_connection =
        capture_mutation_backend_pid(&catalog, catalog_connection, &catalog_backend).await?;
    let catalog_backend_pid = i32::try_from(catalog_backend.load(Ordering::Acquire))?;
    let request = ManagedLogicalSlotCreateRequest::primary_failover_anchor(
        probe_target.clone(),
        allocated.source(),
    );
    let mut create = AbortOnDropTask::new(tokio::spawn(async move {
        create_managed_logical_slot(
            catalog,
            catalog_connection,
            primary,
            primary_connection,
            CatalogOperationTimeout::default(),
            request,
        )
        .await
    }));

    timeout(CONNECTION_EXIT_TIMEOUT, blocked)
        .await
        .map_err(|_| io::Error::other("target preflight did not reach the armed gate in time"))?
        .map_err(|_| io::Error::other("target preflight gate exited before blocking a query"))?;
    let _pending_receipt = wait_for_pending_creation_attempt(&observer, &probe_target).await?;
    assert!(
        !create.task().is_finished(),
        "the armed target gate must hold create in target preflight"
    );
    assert_backend_loss_preserves_creation_fences(&observer, &probe_target, catalog_backend_pid)
        .await?;

    release_gate
        .send(())
        .map_err(|()| io::Error::other("target preflight gate exited before release"))?;
    let receipt = timeout(CONNECTION_EXIT_TIMEOUT, create.task_mut())
        .await
        .map_err(|_| io::Error::other("backend-loss create did not finish in time"))???;
    timeout(CONNECTION_EXIT_TIMEOUT, gate_task.task_mut())
        .await
        .map_err(|_| io::Error::other("target preflight gate did not finish in time"))???;
    assert_created_receipt(
        &receipt,
        &probe_target,
        allocated.source(),
        ManagedLogicalSlotRole::PrimaryFailoverAnchor,
    );
    wait_for_synchronized_copy(&standby_database_url, &probe_target, true).await?;
    reconcile_backend_loss_creation(
        &database_url,
        &standby_database_url,
        &probe_target,
        &allocated,
        receipt,
        &observer,
    )
    .await?;
    drop(observer);
    observer_task
        .finish("creation backend-loss observer connection")
        .await
}

async fn assert_backend_loss_preserves_creation_fences(
    observer: &Client,
    probe_target: &ManagedSlotTarget,
    catalog_backend_pid: i32,
) -> TestResult {
    terminate_backend(observer, catalog_backend_pid).await?;
    wait_for_backend_exit(observer, catalog_backend_pid).await?;

    let shard_error = observer
        .execute(
            "UPDATE pgshard_catalog.shards SET state = 'draining' WHERE shard_id = 'shard-0000'",
            &[],
        )
        .await
        .expect_err("a pending create must fence its shard lifecycle");
    assert_eq!(
        shard_error
            .as_db_error()
            .map(tokio_postgres::error::DbError::message),
        Some("shard lifecycle is blocked by a pending managed slot creation")
    );
    let retirement_error = observer
        .execute(
            "UPDATE pgshard_catalog.slot_sync_probes \
                SET state = 'retiring', cleanup_receipt_id = pg_catalog.gen_random_uuid(), \
                    retiring_at = pg_catalog.statement_timestamp() \
              WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid",
            &[&probe_target.generation().as_uuid().to_string()],
        )
        .await
        .expect_err("a mismatched cleanup cannot retire an unresolved create");
    assert_eq!(
        retirement_error
            .as_db_error()
            .map(tokio_postgres::error::DbError::message),
        Some("slot-sync probe retirement requires its exact pending creation attempt")
    );
    let catalog_state: String = observer
        .query_one(
            "SELECT state FROM pgshard_catalog.slot_sync_probes \
              WHERE probe_generation = $1::pg_catalog.text::pg_catalog.uuid",
            &[&probe_target.generation().as_uuid().to_string()],
        )
        .await?
        .try_get(0)?;
    assert_eq!(catalog_state, "allocated");
    let physical_slot_present: bool = observer
        .query_one(
            "SELECT EXISTS ( \
                        SELECT FROM pg_catalog.pg_replication_slots \
                         WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name \
                    )",
            &[&probe_target.name().as_str()],
        )
        .await?
        .try_get(0)?;
    assert!(
        !physical_slot_present,
        "catalog-backend loss before target dispatch cannot create the physical slot"
    );
    Ok(())
}

async fn reconcile_backend_loss_creation(
    database_url: &str,
    standby_database_url: &str,
    probe_target: &ManagedSlotTarget,
    allocated: &CatalogSlotSyncProbe,
    receipt: ManagedLogicalSlotReceipt,
    observer: &Client,
) -> TestResult {
    let active = activate_catalog_probe(database_url, allocated, &receipt).await?;
    let retiring = begin_catalog_probe_retirement(database_url, &active, &receipt).await?;
    let (catalog, catalog_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let (primary, primary_connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let mut absence = drop_managed_logical_slot(
        catalog,
        catalog_connection,
        primary,
        primary_connection,
        CatalogOperationTimeout::default(),
        receipt,
    )
    .await?;
    wait_for_synchronized_copy(standby_database_url, probe_target, false).await?;
    let retired = complete_catalog_probe_retirement(&retiring, &mut absence).await?;
    assert_eq!(retired.state(), SlotSyncProbeState::Retired);
    release_known_drop(absence).await;

    let physical_slot_present: bool = observer
        .query_one(
            "SELECT EXISTS ( \
                        SELECT FROM pg_catalog.pg_replication_slots \
                         WHERE slot_name OPERATOR(pg_catalog.=) $1::pg_catalog.name \
                    )",
            &[&probe_target.name().as_str()],
        )
        .await?
        .try_get(0)?;
    assert!(
        !physical_slot_present,
        "reconciled catalog retirement cannot retain the physical slot"
    );
    Ok(())
}

async fn assert_catalog_probe_allocation_fencing(
    primary_database_url: &str,
    allocation: &SlotSyncProbeAllocation,
    allocated: &CatalogSlotSyncProbe,
) -> TestResult {
    let stale_allocation = SlotSyncProbeAllocation::new(
        "shard-0000",
        target("pgshard_sync_probe_stale")?,
        allocation.source(),
        "shardschema",
    )?;
    let (client, connection) = tokio_postgres::connect(primary_database_url, NoTls).await?;
    let error = allocate_slot_sync_probe(
        client,
        connection,
        CatalogOperationTimeout::default(),
        &stale_allocation,
    )
    .await
    .expect_err("a new probe cannot use the pre-allocation catalog epoch");
    assert!(matches!(
        error,
        SlotSyncProbeCatalogError::StaleCatalogEpoch { .. }
    ));

    let current_source = allocated.source();
    let competing_allocation = SlotSyncProbeAllocation::new(
        "shard-0000",
        target("pgshard_sync_probe_competing")?,
        SlotSyncProbeAllocationSource::new(
            current_source.system_identifier(),
            current_source.timeline(),
            current_source.database_oid(),
            current_source.restore_incarnation(),
            current_source.catalog_epoch(),
        )?,
        "shardschema",
    )?;
    let (client, connection) = tokio_postgres::connect(primary_database_url, NoTls).await?;
    let error = allocate_slot_sync_probe(
        client,
        connection,
        CatalogOperationTimeout::default(),
        &competing_allocation,
    )
    .await
    .expect_err("one shard cannot allocate a second live probe");
    assert!(matches!(
        error,
        SlotSyncProbeCatalogError::LiveProbeExists { .. }
    ));
    Ok(())
}

fn assert_created_receipt(
    receipt: &ManagedLogicalSlotReceipt,
    target: &ManagedSlotTarget,
    source: ReplicationSourceIdentity,
    role: ManagedLogicalSlotRole,
) {
    assert_eq!(receipt.target(), target);
    assert_eq!(receipt.source(), source);
    assert_eq!(receipt.role(), role);
    assert_eq!(receipt.database_name(), "shardschema");
    assert_ne!(receipt.creation_lsn().0, 0);
    let observation = receipt.observation();
    assert_eq!(observation.name, *target.name());
    assert_eq!(observation.plugin, LogicalSlotPlugin::PgOutput);
    assert_eq!(observation.persistence, SlotPersistence::Persistent);
    assert_eq!(
        observation.ownership,
        SlotOwnership::Managed(target.generation())
    );
    assert_eq!(observation.two_phase, SettingState::Enabled);
    assert_eq!(observation.two_phase_at, Some(receipt.creation_lsn()));
    assert_eq!(
        observation.confirmed_flush_lsn,
        Some(receipt.creation_lsn())
    );
    assert_eq!(observation.activity, SlotActivity::Inactive);
    assert_eq!(observation.invalidation, None);
}

async fn setup_primary_mutation_role(
    database_url: &str,
    hostile_schema: &str,
    mutation_role: &str,
) -> TestResult<u32> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let result: TestResult<u32> = async {
        client
            .batch_execute(&format!(
                "CREATE ROLE {mutation_role} WITH REPLICATION; \
                 GRANT pg_monitor TO {mutation_role}; \
                 GRANT pgshard_slot_mutator TO {mutation_role}; \
                 CREATE SCHEMA {hostile_schema}; \
                 CREATE FUNCTION {hostile_schema}.current_database() \
                 RETURNS pg_catalog.name LANGUAGE SQL IMMUTABLE \
                 AS 'SELECT ''hostile''::pg_catalog.name'; \
                 CREATE FUNCTION {hostile_schema}.current_setting(pg_catalog.text) \
                 RETURNS pg_catalog.text LANGUAGE SQL IMMUTABLE \
                 AS 'SELECT ''0''::pg_catalog.text'"
            ))
            .await?;
        Ok(u32::try_from(
            client
                .query_one(
                    "SELECT oid::pg_catalog.int8 FROM pg_catalog.pg_roles \
                      WHERE rolname OPERATOR(pg_catalog.=) $1::pg_catalog.name",
                    &[&mutation_role],
                )
                .await?
                .try_get::<_, i64>(0)?,
        )?)
    }
    .await;
    drop(client);
    let connection_result = finish_connection(connection_task).await;
    let role_oid = result?;
    connection_result?;
    Ok(role_oid)
}

async fn run_primary_mutation_fixture(
    database_url: String,
    legacy_database_url: String,
    standby_database_url: String,
    target: ManagedSlotTarget,
    hostile_schema: String,
    mutation_role: String,
) -> TestResult {
    let mutation_role_oid =
        setup_primary_mutation_role(&database_url, &hostile_schema, &mutation_role).await?;

    let allocation = slot_sync_probe_allocation(&database_url, target.clone()).await?;
    let allocated = allocate_catalog_probe(&database_url, &allocation).await?;
    let source = allocated.source();

    let mut hostile_config: Config = database_url.parse()?;
    hostile_config.options(format!("-csearch_path={hostile_schema},pg_catalog"));
    assert_hostile_path_is_effective(&hostile_config).await?;
    assert_oversized_advisory_fence_rejected(
        &database_url,
        &hostile_config,
        &mutation_role,
        &target,
        source,
    )
    .await?;
    let (catalog_client, catalog_connection) =
        tokio_postgres::connect(&database_url, NoTls).await?;
    let (client, connection) = hostile_config.connect(NoTls).await?;
    let connection = fence_mutation_session(&client, connection, &mutation_role, &target).await?;
    let receipt = create_managed_logical_slot(
        catalog_client,
        catalog_connection,
        client,
        connection,
        CatalogOperationTimeout::default(),
        ManagedLogicalSlotCreateRequest::primary_failover_anchor(target.clone(), source),
    )
    .await?;
    assert_created_receipt(
        &receipt,
        &target,
        source,
        ManagedLogicalSlotRole::PrimaryFailoverAnchor,
    );
    assert_eq!(receipt.effective_role_oid(), mutation_role_oid);
    assert_eq!(receipt.advisory_lock_count(), 1);
    wait_for_synchronized_copy(&standby_database_url, &target, true).await?;
    let (catalog_client, catalog_connection) =
        tokio_postgres::connect(&database_url, NoTls).await?;
    let (client, connection) = hostile_config.connect(NoTls).await?;
    let connection = fence_mutation_session(&client, connection, &mutation_role, &target).await?;
    let error = create_managed_logical_slot(
        catalog_client,
        catalog_connection,
        client,
        connection,
        CatalogOperationTimeout::default(),
        ManagedLogicalSlotCreateRequest::primary_failover_anchor(target.clone(), source),
    )
    .await
    .expect_err("exact name collision must fail before dispatch");
    assert!(matches!(
        error,
        LocalSlotMutationError::CatalogCreationAttemptPending(ref pending) if pending == &target
    ));

    let observed = observe(
        &database_url,
        &LogicalSlotObservationRequest::new(vec![target.clone()])?,
    )
    .await?;
    let public_row = observed.entries()[0]
        .observation()
        .expect("created primary failover anchor");
    assert_eq!(public_row.persistence, SlotPersistence::Unproven);
    assert_eq!(public_row.ownership, SlotOwnership::Unknown);

    let active = activate_catalog_probe(&database_url, &allocated, &receipt).await?;
    let retiring = begin_catalog_probe_retirement(&database_url, &active, &receipt).await?;

    let receipt =
        recover_receipt_after_legacy_drop_rejection(&database_url, &legacy_database_url, receipt)
            .await?;

    let (catalog_client, catalog_connection) =
        tokio_postgres::connect(&database_url, NoTls).await?;
    let (client, connection) = hostile_config.connect(NoTls).await?;
    let connection = fence_mutation_session(&client, connection, &mutation_role, &target).await?;
    let mut drop_receipt = drop_managed_logical_slot(
        catalog_client,
        catalog_connection,
        client,
        connection,
        CatalogOperationTimeout::default(),
        receipt,
    )
    .await?;
    wait_for_synchronized_copy(&standby_database_url, &target, false).await?;
    let retired = complete_catalog_probe_retirement(&retiring, &mut drop_receipt).await?;
    assert_eq!(retired.state(), SlotSyncProbeState::Retired);
    release_known_drop_while_registry_row_is_blocked(&database_url, drop_receipt).await?;
    let absent = observe(
        &database_url,
        &LogicalSlotObservationRequest::new(vec![target])?,
    )
    .await?;
    assert!(absent.entries()[0].observation().is_none());
    Ok(())
}

async fn insert_standby_consumer_registry<C>(
    client: &C,
    logical_database: Uuid,
    consumer: Uuid,
    suffix: &str,
) -> TestResult
where
    C: GenericClient + Sync,
{
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_databases( \
                 logical_database_id, database_name \
             ) VALUES ($1::pg_catalog.text::pg_catalog.uuid, $2::pg_catalog.text)",
            &[
                &logical_database.to_string(),
                &format!("standby_{}", &suffix[..16]),
            ],
        )
        .await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumers( \
                 consumer_id, logical_database_id, consumer_name, purpose \
             ) VALUES ( \
                 $1::pg_catalog.text::pg_catalog.uuid, \
                 $2::pg_catalog.text::pg_catalog.uuid, $3::pg_catalog.text, \
                 'internal-materialization' \
             )",
            &[
                &consumer.to_string(),
                &logical_database.to_string(),
                &format!("standby-{}", &suffix[..16]),
            ],
        )
        .await?;
    client
        .execute(
            "INSERT INTO pgshard_catalog.logical_consumer_shards( \
                 consumer_id, logical_database_id, shard_id \
             ) VALUES ( \
                 $1::pg_catalog.text::pg_catalog.uuid, \
                 $2::pg_catalog.text::pg_catalog.uuid, 'shard-0000' \
             )",
            &[&consumer.to_string(), &logical_database.to_string()],
        )
        .await?;
    Ok(())
}

async fn allocate_standby_decoder(
    catalog_database_url: &str,
    target: &ManagedSlotTarget,
    source: ReplicationSourceIdentity,
) -> TestResult {
    let (mut client, connection) = tokio_postgres::connect(catalog_database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let logical_database = Uuid::new_v4();
    let consumer = Uuid::new_v4();
    let attachment = Uuid::new_v4();
    let suffix = logical_database.simple().to_string();
    let result: TestResult = async {
        let transaction = client.transaction().await?;
        insert_standby_consumer_registry(&transaction, logical_database, consumer, &suffix).await?;
        transaction
            .execute(
                "INSERT INTO pgshard_catalog.logical_consumer_attachments( \
                     attachment_generation, consumer_id, logical_database_id, shard_id, \
                     restore_incarnation, system_identifier, database_oid, database_name, \
                     selected_source_member_ordinal, selected_source_role, \
                     selected_source_timeline \
                 ) VALUES ( \
                     $1::pg_catalog.text::pg_catalog.uuid, \
                     $2::pg_catalog.text::pg_catalog.uuid, \
                     $3::pg_catalog.text::pg_catalog.uuid, 'shard-0000', \
                     $4::pg_catalog.text::pg_catalog.uuid, \
                     $5::pg_catalog.text::pg_catalog.numeric, $6::pg_catalog.int8, \
                     'shardschema', 1, 'standby-decoder', $7::pg_catalog.int8 \
                 )",
                &[
                    &attachment.to_string(),
                    &consumer.to_string(),
                    &logical_database.to_string(),
                    &source.restore_incarnation().to_string(),
                    &source.system_identifier().to_string(),
                    &i64::from(source.database_oid()),
                    &i64::from(source.timeline()),
                ],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO pgshard_catalog.managed_replication_slots( \
                     slot_generation, attachment_generation, consumer_id, \
                     logical_database_id, shard_id, slot_role, member_ordinal, slot_name \
                 ) VALUES ( \
                     $1::pg_catalog.text::pg_catalog.uuid, \
                     $2::pg_catalog.text::pg_catalog.uuid, \
                     $3::pg_catalog.text::pg_catalog.uuid, \
                     $4::pg_catalog.text::pg_catalog.uuid, 'shard-0000', \
                     'standby-decoder', 1, $5::pg_catalog.text \
                 )",
                &[
                    &target.generation().as_uuid().to_string(),
                    &attachment.to_string(),
                    &consumer.to_string(),
                    &logical_database.to_string(),
                    &target.name().as_str(),
                ],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }
    .await;
    drop(client);
    let connection_result = finish_connection(connection_task).await;
    result?;
    connection_result
}

async fn activate_standby_decoder(
    catalog_database_url: &str,
    target: &ManagedSlotTarget,
    receipt: &ManagedLogicalSlotReceipt,
    principal: &CatalogAdminTestPrincipal,
) -> TestResult {
    assert_eq!(receipt.target(), target);
    let config = principal.config(catalog_database_url)?;
    let (wrong_client, wrong_connection) = config.connect(NoTls).await?;
    let wrong_task = AbortOnDropConnectionTask::new(tokio::spawn(wrong_connection));
    let wrong_receipt = Uuid::new_v4().to_string();
    let creation_lsn = pg_lsn_text(receipt.creation_lsn());
    let validation: TestResult = async {
        let Err(wrong) = wrong_client
            .query_one(
                "SELECT pgshard_catalog.activate_managed_replication_slot( \
                            $1::pg_catalog.text::pg_catalog.uuid, \
                            $2::pg_catalog.text::pg_catalog.uuid, \
                            $3::pg_catalog.text::pg_catalog.pg_lsn, \
                            $3::pg_catalog.text::pg_catalog.pg_lsn \
                        )",
                &[
                    &target.generation().as_uuid().to_string(),
                    &wrong_receipt,
                    &creation_lsn,
                ],
            )
            .await
        else {
            return Err(
                io::Error::other("a guessed consumer activation receipt was accepted").into(),
            );
        };
        if wrong.code() != Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE) {
            return Err(io::Error::other(format!(
                "guessed consumer receipt returned unexpected SQLSTATE: {wrong}"
            ))
            .into());
        }
        let Err(hidden) = wrong_client
            .query_one(
                "SELECT creation_receipt_id \
                   FROM pgshard_catalog.managed_slot_creation_attempts \
                  LIMIT 1",
                &[],
            )
            .await
        else {
            return Err(io::Error::other("catalog admin reloaded hidden receipt authority").into());
        };
        if hidden.code() != Some(&SqlState::INSUFFICIENT_PRIVILEGE) {
            return Err(io::Error::other(format!(
                "hidden receipt query returned unexpected SQLSTATE: {hidden}"
            ))
            .into());
        }
        Ok(())
    }
    .await;
    drop(wrong_client);
    let connection_result = wrong_task
        .finish("catalog-admin receipt validation connection")
        .await;
    validation?;
    connection_result?;

    activate_managed_consumer_with_commit_response_loss(
        catalog_database_url,
        target,
        receipt,
        principal,
    )
    .await
}

async fn assert_managed_consumer_active(
    database_url: &str,
    target: &ManagedSlotTarget,
    receipt: &ManagedLogicalSlotReceipt,
    principal: &CatalogAdminTestPrincipal,
) -> TestResult {
    let (client, connection) = principal.config(database_url)?.connect(NoTls).await?;
    let connection_task = AbortOnDropConnectionTask::new(tokio::spawn(connection));
    let row = client
        .query_one(
            "SELECT state::pg_catalog.text, \
                    consistent_point = $2::pg_catalog.text::pg_catalog.pg_lsn \
               FROM pgshard_catalog.managed_replication_slots \
              WHERE slot_generation = $1::pg_catalog.text::pg_catalog.uuid",
            &[
                &target.generation().as_uuid().to_string(),
                &pg_lsn_text(receipt.creation_lsn()),
            ],
        )
        .await?;
    let state: String = row.try_get(0)?;
    let boundary_matches: bool = row.try_get(1)?;
    if state != "active" || !boundary_matches {
        return Err(io::Error::other(
            "consumer activation did not retain its exact durable state and boundary",
        )
        .into());
    }
    drop(client);
    connection_task
        .finish("catalog-admin durable-state connection")
        .await
}

async fn arm_commit_response_loss_proxy(arm: oneshot::Sender<oneshot::Sender<()>>) -> TestResult {
    let (armed_acknowledgement, armed) = oneshot::channel();
    arm.send(armed_acknowledgement)
        .map_err(|_| io::Error::other("catalog fault proxy exited before it was armed"))?;
    timeout(CONNECTION_EXIT_TIMEOUT, armed)
        .await
        .map_err(|_| {
            io::Error::other("catalog fault proxy arm acknowledgement exceeded the bound")
        })?
        .map_err(|_| io::Error::other("catalog fault proxy exited before acknowledging its arm"))?;
    Ok(())
}

async fn activate_managed_consumer_with_commit_response_loss(
    database_url: &str,
    target: &ManagedSlotTarget,
    receipt: &ManagedLogicalSlotReceipt,
    principal: &CatalogAdminTestPrincipal,
) -> TestResult {
    let CommitResponseLossProxy {
        database_url: proxy_database_url,
        arm,
        task,
    } = start_commit_response_loss_proxy(database_url).await?;
    let mut proxy_task = AbortOnDropTask::new(task);
    let (client, connection) = principal
        .config(&proxy_database_url)?
        .connect(NoTls)
        .await?;
    arm_commit_response_loss_proxy(arm).await?;
    let error = match activate_managed_consumer_slot(
        client,
        connection,
        CatalogOperationTimeout::default(),
        receipt,
    )
    .await
    {
        Ok(()) => {
            return Err(io::Error::other(
                "consumer activation reported success after its COMMIT response was lost",
            )
            .into());
        }
        Err(error) => error,
    };
    match error {
        ManagedLogicalSlotCatalogActivationError::OutcomeUnknownPostgres {
            target: ref failed_target,
            ..
        } if failed_target == target => {}
        other => {
            return Err(io::Error::other(format!(
                "consumer activation response loss returned a non-ambiguous error: {other}"
            ))
            .into());
        }
    }
    timeout(CONNECTION_EXIT_TIMEOUT, proxy_task.task_mut())
        .await
        .map_err(|_| {
            io::Error::other("catalog fault proxy did not confirm consumer activation COMMIT")
        })???;
    assert_managed_consumer_active(database_url, target, receipt, principal).await?;

    let (client, connection) = principal.config(database_url)?.connect(NoTls).await?;
    activate_managed_consumer_slot(
        client,
        connection,
        CatalogOperationTimeout::default(),
        receipt,
    )
    .await?;
    assert_managed_consumer_active(database_url, target, receipt, principal).await
}

async fn retire_standby_decoder(
    catalog_database_url: &str,
    target: &ManagedSlotTarget,
) -> TestResult {
    let (client, connection) = timeout(
        CLEANUP_TIMEOUT,
        tokio_postgres::connect(catalog_database_url, NoTls),
    )
    .await
    .map_err(|_| io::Error::other("standby-decoder cleanup connection exceeded the bound"))??;
    let connection_task = tokio::spawn(connection);
    let result = timeout(
        CLEANUP_TIMEOUT,
        retire_standby_decoder_with_client(&client, target),
    )
    .await
    .map_err(|_| io::Error::other("standby-decoder row cleanup exceeded the bound"));
    drop(client);
    let connection_result = finish_connection(connection_task).await;
    result??;
    connection_result
}

async fn retire_standby_decoder_with_client(
    client: &Client,
    target: &ManagedSlotTarget,
) -> TestResult {
    client
        .batch_execute(
            "SET statement_timeout = '4s'; \
             SET lock_timeout = '4s'; \
             SET transaction_timeout = '4s'",
        )
        .await?;
    let table: Option<String> = client
        .query_one(
            "SELECT pg_catalog.to_regclass( \
                 'pgshard_catalog.managed_replication_slots')::pg_catalog.text",
            &[],
        )
        .await?
        .try_get(0)?;
    if table.is_none() {
        return Ok(());
    }
    let generation = target.generation().as_uuid().to_string();
    let registry = client
        .query_opt(
            "SELECT attachment_generation::pg_catalog.text, \
                    consumer_id::pg_catalog.text, logical_database_id::pg_catalog.text \
               FROM pgshard_catalog.managed_replication_slots \
              WHERE slot_generation = $1::pg_catalog.text::pg_catalog.uuid",
            &[&generation],
        )
        .await?;
    reconcile_standby_creation_attempt(client, target).await?;
    client
        .execute(
            "UPDATE pgshard_catalog.managed_replication_slots \
                SET state = 'retired', retired_at = pg_catalog.statement_timestamp() \
              WHERE slot_generation = $1::pg_catalog.text::pg_catalog.uuid \
                AND state = 'allocated'",
            &[&generation],
        )
        .await?;
    let unresolved: i64 = client
        .query_one(
            "SELECT ( \
                        SELECT pg_catalog.count(*) \
                          FROM pgshard_catalog.managed_replication_slots \
                         WHERE slot_generation = $1::pg_catalog.text::pg_catalog.uuid \
                           AND state <> 'retired' \
                    ) + ( \
                        SELECT pg_catalog.count(*) \
                          FROM pgshard_catalog.managed_slot_creation_attempts \
                         WHERE slot_generation = $1::pg_catalog.text::pg_catalog.uuid \
                           AND state = 'pending' \
                    )",
            &[&generation],
        )
        .await?
        .try_get(0)?;
    if unresolved != 0 {
        return Err(io::Error::other("standby-decoder cleanup left live catalog state").into());
    }
    if let Some(registry) = registry {
        retire_standby_consumer_registry(
            client,
            &registry.try_get::<_, String>(0)?,
            &registry.try_get::<_, String>(1)?,
            &registry.try_get::<_, String>(2)?,
        )
        .await?;
    }
    Ok(())
}

async fn retire_standby_consumer_registry(
    client: &Client,
    attachment: &str,
    consumer: &str,
    logical_database: &str,
) -> TestResult {
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_attachments \
                SET state = 'retired', retired_at = pg_catalog.statement_timestamp() \
              WHERE attachment_generation = $1::pg_catalog.text::pg_catalog.uuid \
                AND state <> 'retired'",
            &[&attachment],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards \
                SET state = 'fenced', \
                    ownership_fence = CASE \
                        WHEN state = 'ready' THEN ownership_fence + 1 \
                        ELSE ownership_fence \
                    END \
              WHERE consumer_id = $1::pg_catalog.text::pg_catalog.uuid \
                AND logical_database_id = $2::pg_catalog.text::pg_catalog.uuid \
                AND shard_id = 'shard-0000' \
                AND state IN ('provisioning', 'ready')",
            &[&consumer, &logical_database],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumer_shards \
                SET state = 'retired' \
              WHERE consumer_id = $1::pg_catalog.text::pg_catalog.uuid \
                AND logical_database_id = $2::pg_catalog.text::pg_catalog.uuid \
                AND shard_id = 'shard-0000' AND state = 'fenced'",
            &[&consumer, &logical_database],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumers SET state = 'draining' \
              WHERE consumer_id = $1::pg_catalog.text::pg_catalog.uuid \
                AND logical_database_id = $2::pg_catalog.text::pg_catalog.uuid \
                AND state = 'active'",
            &[&consumer, &logical_database],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_consumers SET state = 'retired' \
              WHERE consumer_id = $1::pg_catalog.text::pg_catalog.uuid \
                AND logical_database_id = $2::pg_catalog.text::pg_catalog.uuid \
                AND state = 'draining'",
            &[&consumer, &logical_database],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_databases SET state = 'draining' \
              WHERE logical_database_id = $1::pg_catalog.text::pg_catalog.uuid \
                AND state = 'active'",
            &[&logical_database],
        )
        .await?;
    client
        .execute(
            "UPDATE pgshard_catalog.logical_databases SET state = 'retired' \
              WHERE logical_database_id = $1::pg_catalog.text::pg_catalog.uuid \
                AND state = 'draining'",
            &[&logical_database],
        )
        .await?;
    let remaining: i64 = client
        .query_one(
            "SELECT (SELECT pg_catalog.count(*) \
                       FROM pgshard_catalog.logical_consumer_attachments \
                      WHERE attachment_generation = $1::pg_catalog.text::pg_catalog.uuid \
                        AND state <> 'retired') \
                  + (SELECT pg_catalog.count(*) \
                       FROM pgshard_catalog.logical_consumer_shards \
                      WHERE consumer_id = $2::pg_catalog.text::pg_catalog.uuid \
                        AND logical_database_id = $3::pg_catalog.text::pg_catalog.uuid \
                        AND state <> 'retired') \
                  + (SELECT pg_catalog.count(*) \
                       FROM pgshard_catalog.logical_consumers \
                      WHERE consumer_id = $2::pg_catalog.text::pg_catalog.uuid \
                        AND logical_database_id = $3::pg_catalog.text::pg_catalog.uuid \
                        AND state <> 'retired') \
                  + (SELECT pg_catalog.count(*) \
                       FROM pgshard_catalog.logical_databases \
                      WHERE logical_database_id = $3::pg_catalog.text::pg_catalog.uuid \
                        AND state <> 'retired')",
            &[&attachment, &consumer, &logical_database],
        )
        .await?
        .try_get(0)?;
    if remaining != 0 {
        return Err(io::Error::other("standby-decoder cleanup left live registry state").into());
    }
    Ok(())
}

async fn reconcile_standby_creation_attempt(
    client: &Client,
    target: &ManagedSlotTarget,
) -> TestResult {
    let generation = target.generation().as_uuid().to_string();
    let attempt = client
        .query_opt(
            "SELECT attempts.slot_name::pg_catalog.text, \
                    attempts.creation_receipt_id::pg_catalog.text, attempts.state \
               FROM pgshard_catalog.managed_slot_creation_attempts AS attempts \
              WHERE attempts.slot_generation = $1::pg_catalog.text::pg_catalog.uuid \
                AND attempts.state IN ('pending', 'activated')",
            &[&generation],
        )
        .await?;
    let Some(row) = attempt else {
        return Ok(());
    };
    let slot_name: String = row.try_get(0)?;
    let receipt_id: String = row.try_get(1)?;
    let attempt_state: String = row.try_get(2)?;
    if attempt_state == "pending" {
        client
            .query_one(
                "SELECT pgshard_catalog.abandon_managed_slot_creation_attempt( \
                            $1::pg_catalog.text::pg_catalog.uuid, $2::pg_catalog.text, \
                            $3::pg_catalog.text::pg_catalog.uuid \
                        )",
                &[&generation, &slot_name, &receipt_id],
            )
            .await?;
        return Ok(());
    }
    complete_standby_catalog_cleanup(client, &generation, &slot_name, &receipt_id).await
}

async fn complete_standby_catalog_cleanup(
    client: &Client,
    generation: &str,
    slot_name: &str,
    receipt_id: &str,
) -> TestResult {
    let fence_id: String = client
        .query_one(
            "SELECT acquired_fence_id::pg_catalog.text \
               FROM pgshard_catalog.acquire_managed_slot_target_fence($1::pg_catalog.text)",
            &[&slot_name],
        )
        .await?
        .try_get(0)?;
    let retirement = client
        .query_one(
            "SELECT pgshard_catalog.complete_managed_replication_slot_retirement( \
                        $1::pg_catalog.text::pg_catalog.uuid, $2::pg_catalog.text, \
                        $3::pg_catalog.text::pg_catalog.uuid, \
                        $4::pg_catalog.text::pg_catalog.uuid \
                    )",
            &[&generation, &slot_name, &receipt_id, &fence_id],
        )
        .await;
    let released: bool = client
        .query_one(
            "SELECT pgshard_catalog.release_managed_slot_target_fence( \
                        $1::pg_catalog.text, $2::pg_catalog.text::pg_catalog.uuid \
                    )",
            &[&slot_name, &fence_id],
        )
        .await?
        .try_get(0)?;
    if !released {
        return Err(
            io::Error::other("standby-decoder cleanup failed to release its target fence").into(),
        );
    }
    retirement?;
    Ok(())
}

async fn create_standby_slot_with_snapshot_trigger(
    primary_database_url: &str,
    standby_database_url: &str,
    request: ManagedLogicalSlotCreateRequest,
    backend_pid: &AtomicU32,
) -> TestResult<ManagedLogicalSlotReceipt> {
    let deadline = Instant::now() + STANDBY_CATCHUP_TIMEOUT;
    let (primary, primary_connection) = timeout_at(
        deadline,
        tokio_postgres::connect(primary_database_url, NoTls),
    )
    .await
    .map_err(|_| io::Error::other("primary snapshot-trigger connection exceeded the bound"))??;
    let (catalog, catalog_connection) = timeout_at(
        deadline,
        tokio_postgres::connect(primary_database_url, NoTls),
    )
    .await
    .map_err(|_| io::Error::other("catalog-fence connection exceeded the bound"))??;
    let (standby, standby_connection) = timeout_at(
        deadline,
        tokio_postgres::connect(standby_database_url, NoTls),
    )
    .await
    .map_err(|_| io::Error::other("standby mutation connection exceeded the bound"))??;
    let standby_connection =
        capture_mutation_backend_pid(&standby, standby_connection, backend_pid).await?;
    let primary_task = AbortOnDropConnectionTask::new(tokio::spawn(primary_connection));
    let create_result: TestResult<ManagedLogicalSlotReceipt> = {
        let create = create_managed_logical_slot(
            catalog,
            catalog_connection,
            standby,
            standby_connection,
            CatalogOperationTimeout::default(),
            request,
        );
        let trigger = continuously_trigger_standby_snapshots(&primary);
        tokio::pin!(create);
        tokio::pin!(trigger);
        timeout_at(deadline, async {
            tokio::select! {
                result = &mut create => result.map_err(|error| Box::new(error) as TestError),
                result = &mut trigger => match result {
                    Ok(()) => Err(Box::new(io::Error::other("standby snapshot trigger stopped unexpectedly")) as TestError),
                    Err(error) => Err(Box::new(error) as TestError),
                },
            }
        })
        .await
        .map_err(|_| io::Error::other("standby slot creation exceeded the test bound"))?
    };
    drop(primary);
    let connection_result = primary_task
        .finish("primary snapshot-trigger connection")
        .await;
    let receipt = create_result?;
    connection_result?;
    Ok(receipt)
}

async fn continuously_trigger_standby_snapshots(
    primary: &Client,
) -> Result<(), tokio_postgres::Error> {
    loop {
        primary
            .query_one("SELECT pg_catalog.pg_log_standby_snapshot()", &[])
            .await?;
        sleep(STANDBY_POLL_INTERVAL).await;
    }
}

async fn correlated_standby_mutation_path(
    primary_database_url: &str,
    standby_database_url: &str,
    local_decoder: ManagedSlotTarget,
) -> TestResult<CorrelatedStandbyReplicationPath> {
    let deadline = Instant::now() + STANDBY_CATCHUP_TIMEOUT;
    let (restore_incarnation, catalog_epoch) =
        catalog_restore_and_epoch(primary_database_url).await?;
    let physical_slot = ReplicationSlotName::new(EXPECTED_PRIMARY_SLOT_NAME)?;
    let anchor = expected_synced_anchor()?;
    let standby_request = LogicalSlotObservationRequest::new(vec![anchor.clone()])?;
    let primary_request =
        PrimaryReplicationObservationRequest::new(physical_slot.clone(), anchor.clone())?;
    let required_checkpoint = create_primary_checkpoint(primary_database_url).await?;
    wait_for_standby_replay_past_checkpoint(standby_database_url, required_checkpoint).await?;
    advance_standby_replay_floor(standby_database_url, &standby_request, required_checkpoint)
        .await?;
    let mut last_rejection = "no correlated samples collected".to_owned();

    loop {
        let primary = timeout_at(
            deadline,
            observe_primary_path(primary_database_url, &primary_request),
        )
        .await
        .map_err(|_| io::Error::other("primary mutation-path observation exceeded the bound"))??;
        let standby = timeout_at(deadline, observe(standby_database_url, &standby_request))
            .await
            .map_err(|_| {
                io::Error::other("standby mutation-path observation exceeded the bound")
            })??;
        let prerequisites = standby.prerequisites();
        let failover_anchor_at = standby
            .entries()
            .first()
            .and_then(|entry| entry.observation())
            .and_then(|slot| slot.two_phase_at);
        if required_checkpoint.0 != 0
            && let Some(failover_anchor_at) = failover_anchor_at
        {
            let source = ReplicationSourceIdentity::new(
                prerequisites.system_identifier(),
                prerequisites.checkpoint_timeline(),
                standby.database_oid(),
                restore_incarnation,
                catalog_epoch,
            )?;
            let target = StandbyDecoderTarget::new(
                1,
                physical_slot.clone(),
                anchor.clone(),
                local_decoder.clone(),
            )?;
            let policy = StandbyDecoderPolicy::new(
                source,
                target,
                ManagedTwoPhasePolicy {
                    failover_anchor_at,
                    local_decoder_at: required_checkpoint,
                },
                required_checkpoint,
                StandbyDecoderEvidenceLimits::new(
                    Duration::from_secs(30),
                    Duration::from_secs(3),
                    Duration::from_secs(3),
                )?,
            );
            match policy {
                Ok(policy) => match correlate_standby_replication_path(&policy, &standby, &primary)
                {
                    Ok(proof) => return Ok(proof),
                    Err(error) => last_rejection = error.to_string(),
                },
                Err(error) => last_rejection = error.to_string(),
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "standby mutation path did not correlate within the bound: {last_rejection}"
            ))
            .into());
        }
        timeout_at(deadline, sleep(STANDBY_POLL_INTERVAL))
            .await
            .map_err(|_| io::Error::other("standby mutation-path wait exceeded the bound"))?;
    }
}

async fn ensure_nonzero_catalog_epoch(database_url: &str) -> TestResult {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let epoch: i64 = client
        .query_one(
            "SELECT catalog_epoch \
               FROM pgshard_catalog.cluster_state \
              WHERE singleton",
            &[],
        )
        .await?
        .try_get(0)?;
    if epoch == 0 {
        client
            .execute(
                "UPDATE pgshard_catalog.shards \
                    SET state = state \
                  WHERE shard_id = 'shard-0000'",
                &[],
            )
            .await?;
    }
    drop(client);
    finish_connection(connection_task).await
}

async fn run_standby_mutation_fixture(
    primary_database_url: String,
    standby_database_url: String,
    target: ManagedSlotTarget,
    backend_pid: Arc<AtomicU32>,
    principal: CatalogAdminTestPrincipal,
) -> TestResult {
    ensure_nonzero_catalog_epoch(&primary_database_url).await?;
    let provisional = correlated_standby_mutation_path(
        &primary_database_url,
        &standby_database_url,
        target.clone(),
    )
    .await?;
    allocate_standby_decoder(
        &primary_database_url,
        &target,
        provisional.source_identity(),
    )
    .await?;
    let proof = correlated_standby_mutation_path(
        &primary_database_url,
        &standby_database_url,
        target.clone(),
    )
    .await?;
    let source = proof.source_identity();
    let receipt = create_standby_slot_with_snapshot_trigger(
        &primary_database_url,
        &standby_database_url,
        ManagedLogicalSlotCreateRequest::standby_local_decoder(proof),
        &backend_pid,
    )
    .await?;
    assert_created_receipt(
        &receipt,
        &target,
        source,
        ManagedLogicalSlotRole::StandbyLocalDecoder,
    );
    assert_eq!(
        receipt.observation().kind,
        LogicalSlotKind::StandbyLocalDecoder
    );
    activate_standby_decoder(&primary_database_url, &target, &receipt, &principal).await?;

    let observed = observe(
        &standby_database_url,
        &LogicalSlotObservationRequest::new(vec![target.clone()])?,
    )
    .await?;
    assert_eq!(
        observed.entries()[0]
            .observation()
            .expect("standby-local decoder")
            .kind,
        LogicalSlotKind::StandbyLocalDecoder
    );
    let (catalog, catalog_connection) =
        tokio_postgres::connect(&primary_database_url, NoTls).await?;
    let (standby, standby_connection) =
        tokio_postgres::connect(&standby_database_url, NoTls).await?;
    let mut drop_fence = drop_managed_logical_slot(
        catalog,
        catalog_connection,
        standby,
        standby_connection,
        CatalogOperationTimeout::default(),
        receipt,
    )
    .await?;
    complete_managed_consumer_slot_retirement(CatalogOperationTimeout::default(), &mut drop_fence)
        .await?;
    release_known_drop(drop_fence).await;
    assert_standby_decoder_retired(&primary_database_url, &target).await?;
    let absent = observe(
        &standby_database_url,
        &LogicalSlotObservationRequest::new(vec![target])?,
    )
    .await?;
    assert!(absent.entries()[0].observation().is_none());
    Ok(())
}

async fn assert_standby_decoder_retired(
    catalog_database_url: &str,
    target: &ManagedSlotTarget,
) -> TestResult {
    let (client, connection) = tokio_postgres::connect(catalog_database_url, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let unresolved: i64 = client
        .query_one(
            "SELECT ( \
                        SELECT pg_catalog.count(*) \
                          FROM pgshard_catalog.managed_replication_slots \
                         WHERE slot_generation = $1::pg_catalog.text::pg_catalog.uuid \
                           AND state <> 'retired' \
                    ) + ( \
                        SELECT pg_catalog.count(*) \
                          FROM pgshard_catalog.managed_slot_creation_attempts \
                         WHERE slot_generation = $1::pg_catalog.text::pg_catalog.uuid \
                           AND state <> 'retired' \
                    )",
            &[&target.generation().as_uuid().to_string()],
        )
        .await?
        .try_get(0)?;
    drop(client);
    finish_connection(connection_task).await?;
    if unresolved != 0 {
        return Err(io::Error::other("standby-decoder retirement left live catalog state").into());
    }
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

    let fixture_result = finish_bounded_fixture(
        tokio::spawn(run_observation_fixture(
            database_url,
            anchor,
            decoder,
            temporary,
            hostile_schema.clone(),
        )),
        "slot-observation fixture",
    )
    .await;
    let cleanup_result = cleanup_observation_fixture(&cleanup, &targets, &hostile_schema).await;
    drop(cleanup);
    let cleanup_connection_result = finish_connection(cleanup_task).await;

    combine_fixture_results(fixture_result, cleanup_result, cleanup_connection_result)
}

#[tokio::test]
#[ignore = "requires the CI PostgreSQL 18 primary, streaming standby, and logical-slot settings"]
async fn creates_verifies_and_drops_primary_failover_anchor() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let legacy_database_url = std::env::var("PGSHARD_TEST_LEGACY_DATABASE_URL")?;
    let standby_database_url = std::env::var("PGSHARD_TEST_STANDBY_DATABASE_URL")?;
    let (cleanup, cleanup_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let cleanup_task = tokio::spawn(cleanup_connection);
    let target = target("pgshard_mutation_anchor")?;
    let hostile_schema = format!("hostile_{}", target.generation().as_uuid().simple());
    let mutation_role = format!(
        "pgshard_mutator_{}",
        &target.generation().as_uuid().simple().to_string()[..16]
    );
    let fixture_result = finish_bounded_fixture(
        tokio::spawn(run_primary_mutation_fixture(
            database_url.clone(),
            legacy_database_url,
            standby_database_url.clone(),
            target.clone(),
            hostile_schema.clone(),
            mutation_role.clone(),
        )),
        "primary slot-mutation fixture",
    )
    .await;
    let cleanup_result = cleanup_primary_mutation_fixture(
        &cleanup,
        &database_url,
        &standby_database_url,
        &target,
        &hostile_schema,
        &mutation_role,
    )
    .await;
    drop(cleanup);
    let cleanup_connection_result = finish_connection(cleanup_task).await;
    combine_fixture_results(fixture_result, cleanup_result, cleanup_connection_result)
}

#[tokio::test]
#[ignore = "requires the CI PostgreSQL 18 primary, streaming standby, catalog, and logical-slot settings"]
async fn persists_the_exact_slot_sync_probe_lifecycle() -> TestResult {
    let primary_database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let standby_database_url = std::env::var("PGSHARD_TEST_STANDBY_DATABASE_URL")?;
    let (cleanup, cleanup_connection) =
        tokio_postgres::connect(&primary_database_url, NoTls).await?;
    let cleanup_task = tokio::spawn(cleanup_connection);
    let probe = target("pgshard_sync_probe")?;
    let fixture_result = finish_bounded_fixture(
        tokio::spawn(run_catalog_probe_fixture(
            primary_database_url.clone(),
            standby_database_url.clone(),
            probe.clone(),
        )),
        "slot-sync probe catalog fixture",
    )
    .await;
    let cleanup_result = cleanup_catalog_probe_fixture(
        &cleanup,
        &primary_database_url,
        &standby_database_url,
        &probe,
    )
    .await;
    drop(cleanup);
    let cleanup_connection_result = finish_connection(cleanup_task).await;
    combine_fixture_results(fixture_result, cleanup_result, cleanup_connection_result)
}

#[tokio::test]
#[ignore = "requires the CI PostgreSQL 18 primary, streaming standby, catalog, and logical-slot settings"]
async fn reports_unknown_when_catalog_commit_outlives_its_target_fence() -> TestResult {
    let primary_database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let standby_database_url = std::env::var("PGSHARD_TEST_STANDBY_DATABASE_URL")?;
    let (cleanup, cleanup_connection) =
        tokio_postgres::connect(&primary_database_url, NoTls).await?;
    let cleanup_task = tokio::spawn(cleanup_connection);
    let probe = target("pgshard_sync_probe_fence_loss")?;
    let fixture_result = finish_bounded_fixture(
        tokio::spawn(run_catalog_probe_fence_loss_fixture(
            primary_database_url.clone(),
            standby_database_url.clone(),
            probe.clone(),
        )),
        "slot-sync probe post-COMMIT fence-loss fixture",
    )
    .await;
    let cleanup_result = cleanup_catalog_probe_fixture(
        &cleanup,
        &primary_database_url,
        &standby_database_url,
        &probe,
    )
    .await;
    drop(cleanup);
    let cleanup_connection_result = finish_connection(cleanup_task).await;
    combine_fixture_results(fixture_result, cleanup_result, cleanup_connection_result)
}

#[tokio::test]
#[ignore = "requires the CI PostgreSQL 18 primary, streaming standby, catalog, and logical-slot settings"]
async fn catalog_backend_loss_preserves_the_durable_creation_fence() -> TestResult {
    let database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let standby_database_url = std::env::var("PGSHARD_TEST_STANDBY_DATABASE_URL")?;
    let (cleanup, cleanup_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    let cleanup_task = tokio::spawn(cleanup_connection);
    let probe = target("pgshard_creation_fence_loss")?;
    let fixture_result = finish_bounded_fixture(
        tokio::spawn(run_catalog_creation_backend_loss_fixture(
            database_url.clone(),
            standby_database_url.clone(),
            probe.clone(),
        )),
        "managed-slot creation backend-loss fixture",
    )
    .await;
    let cleanup_result =
        cleanup_catalog_probe_fixture(&cleanup, &database_url, &standby_database_url, &probe).await;
    drop(cleanup);
    let cleanup_connection_result = finish_connection(cleanup_task).await;
    combine_fixture_results(fixture_result, cleanup_result, cleanup_connection_result)
}

#[tokio::test]
#[ignore = "requires the CI PostgreSQL 18 primary and streaming standby"]
async fn creates_verifies_and_drops_standby_local_decoder() -> TestResult {
    let primary_database_url = std::env::var("PGSHARD_TEST_DATABASE_URL")?;
    let standby_database_url = std::env::var("PGSHARD_TEST_STANDBY_DATABASE_URL")?;
    let (cleanup, cleanup_connection) =
        tokio_postgres::connect(&standby_database_url, NoTls).await?;
    let cleanup_task = tokio::spawn(cleanup_connection);
    let (catalog_admin, catalog_admin_connection) =
        tokio_postgres::connect(&primary_database_url, NoTls).await?;
    let catalog_admin_task = tokio::spawn(catalog_admin_connection);
    let target = target("pgshard_mutation_decoder")?;
    let mutation_backend_pid = Arc::new(AtomicU32::new(0));
    let principal = CatalogAdminTestPrincipal::new();
    let fixture_result: TestResult = async {
        principal.create(&catalog_admin).await?;
        finish_bounded_fixture(
            tokio::spawn(run_standby_mutation_fixture(
                primary_database_url.clone(),
                standby_database_url,
                target.clone(),
                Arc::clone(&mutation_backend_pid),
                principal.clone(),
            )),
            "standby slot-mutation fixture",
        )
        .await
    }
    .await;
    let cleanup_result = cleanup_standby_mutation_fixture(
        &cleanup,
        &primary_database_url,
        &target,
        &mutation_backend_pid,
    )
    .await;
    let principal_cleanup_result = principal.drop_if_exists(&catalog_admin).await;
    drop(cleanup);
    drop(catalog_admin);
    let cleanup_connection_result = finish_connection(cleanup_task).await;
    let catalog_admin_connection_result = finish_connection(catalog_admin_task).await;
    combine_named_results([
        ("fixture", fixture_result),
        ("slot cleanup", cleanup_result),
        ("catalog principal cleanup", principal_cleanup_result),
        ("slot cleanup connection", cleanup_connection_result),
        (
            "catalog principal connection",
            catalog_admin_connection_result,
        ),
    ])
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
    let catalog_target = target("pgshard_test_legacy_catalog")?;
    let (catalog_client, catalog_connection) =
        tokio_postgres::connect(&database_url, NoTls).await?;
    let error = load_slot_sync_probe(
        catalog_client,
        catalog_connection,
        CatalogOperationTimeout::default(),
        &catalog_target,
    )
    .await
    .expect_err("PostgreSQL 17 must fail before slot-sync probe catalog reads");
    assert!(matches!(
        error,
        SlotSyncProbeCatalogError::UnsupportedPostgresVersion(version) if version < 180_000
    ));

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
    let deadline = Instant::now() + FAULT_BACKEND_EXIT_TIMEOUT;
    while backend_exists(client, backend_pid).await? {
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "terminated test backend {backend_pid} remained connected past the bound"
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
