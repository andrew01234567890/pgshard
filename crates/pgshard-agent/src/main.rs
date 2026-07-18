//! `pgshard-agent` Linux container entry point.

use pgshard_agent::config::{AgentConfig, ConfigError};
use pgshard_agent::coordination::{WritableLeaseConfig, WritableLeaseError};
use pgshard_agent::domain::{AgentState, PostgresProcessState};
use pgshard_agent::postgres::{PostgresConfig, PostgresError, PreparedPostgres};
use std::future::Future;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::watch;

const PREPARATION_TIMEOUT: Duration = Duration::from_secs(30);
const BLOCKING_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const INITIAL_COORDINATION_RETRY: Duration = Duration::from_millis(250);
const MAX_COORDINATION_RETRY: Duration = Duration::from_secs(5);
const COORDINATION_RETRY_RESET: Duration = Duration::from_secs(30);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    // A filesystem syscall in a detached preflight worker can remain blocked
    // indefinitely. Do not let it turn SIGTERM into an unbounded runtime drop;
    // no postmaster has been spawned on either cancellation path.
    runtime.shutdown_timeout(BLOCKING_TASK_SHUTDOWN_TIMEOUT);
    result
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Register both handlers before configuration or PGDATA validation. Tokio
    // retains a notification until the streams are polled, so an early signal
    // cannot fall back to the operating system's terminating default action.
    let mut signals = ShutdownSignals::install()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_shutdown_tx = shutdown_tx.clone();
    let signal_task = tokio::spawn(async move {
        signals.recv().await;
        let _ = signal_shutdown_tx.send(true);
    });
    let config = match AgentConfig::from_env() {
        Ok(config) => config,
        Err(ConfigError::Arguments(error)) => error.exit(),
        Err(error) => return Err(error.into()),
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let AgentConfig {
        http_bind,
        identity,
        max_lease_ttl_ms,
        writable_lease,
        telemetry,
        postgres,
    } = config;
    let telemetry = telemetry.status();
    if telemetry.endpoint_configured {
        tracing::warn!(reason = telemetry.reason, "OpenTelemetry export disabled");
    } else {
        tracing::info!(reason = telemetry.reason, "OpenTelemetry export disabled");
    }

    let state = AgentState::with_identity(identity, max_lease_ttl_ms)?;
    let postgres_config = postgres;
    let postgres = match postgres_config.clone() {
        Some(config) => {
            if let Some(postgres) = prepare_postgres(config, shutdown_rx.clone()).await? {
                Some(postgres)
            } else {
                signal_task.abort();
                let _ = signal_task.await;
                tracing::info!("agent shutdown complete before PostgreSQL preparation");
                return Ok(());
            }
        }
        None => None,
    };
    if postgres.is_some() {
        state.set_postgres_process(PostgresProcessState::Validated);
    }
    tracing::info!(
        bind = %http_bind,
        postgres_quarantine = postgres.is_some(),
        version = pgshard_version::VERSION,
        git_sha = pgshard_version::GIT_SHA,
        "starting agent HTTP server"
    );
    let result = Box::pin(run_services(
        http_bind,
        state,
        postgres,
        postgres_config,
        writable_lease,
        shutdown_tx.clone(),
        shutdown_rx,
    ))
    .await;
    signal_task.abort();
    let _ = signal_task.await;
    result?;
    tracing::info!("agent shutdown complete");
    Ok(())
}

async fn run_services(
    http_bind: SocketAddr,
    state: AgentState,
    postgres: Option<PreparedPostgres>,
    postgres_config: Option<PostgresConfig>,
    writable_lease: Option<WritableLeaseConfig>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), AgentRunError> {
    if let Some(writable_lease) = writable_lease {
        let Some(postgres) = postgres else {
            return Err(AgentRunError::InvalidRuntimeComposition);
        };
        let Some(postgres_config) = postgres_config else {
            return Err(AgentRunError::InvalidRuntimeComposition);
        };
        return Box::pin(run_writable_services(
            http_bind,
            state,
            postgres,
            postgres_config,
            writable_lease,
            shutdown_tx,
            shutdown_rx,
        ))
        .await;
    }

    let run_shutdown_tx = shutdown_tx.clone();
    if let Some(postgres) = postgres {
        let listener = tokio::net::TcpListener::bind(http_bind)
            .await
            .map_err(AgentRunError::Http)?;
        let http = pgshard_agent::http::serve_on(
            listener,
            state.clone(),
            wait_for_shutdown(shutdown_rx.clone()),
        );
        let postmaster = postgres.supervise(state, wait_for_shutdown(shutdown_rx));
        tokio::pin!(http);
        tokio::pin!(postmaster);
        tokio::select! {
            result = &mut http => {
                let _ = run_shutdown_tx.send(true);
                let postmaster_result = postmaster.await;
                combine_component_results(result, postmaster_result)
            }
            result = &mut postmaster => {
                let _ = run_shutdown_tx.send(true);
                let http_result = http.await;
                combine_component_results(http_result, result)
            }
        }
    } else {
        pgshard_agent::http::serve(http_bind, state, wait_for_shutdown(shutdown_rx))
            .await
            .map_err(AgentRunError::Http)
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_writable_services(
    http_bind: SocketAddr,
    state: AgentState,
    postgres: PreparedPostgres,
    postgres_config: PostgresConfig,
    writable_lease: WritableLeaseConfig,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), AgentRunError> {
    let listener = tokio::net::TcpListener::bind(http_bind)
        .await
        .map_err(AgentRunError::Http)?;
    let http = pgshard_agent::http::serve_on(
        listener,
        state.clone(),
        wait_for_shutdown(shutdown_rx.clone()),
    );
    let runtime = supervise_writable_runtime(
        state,
        postgres,
        postgres_config,
        writable_lease,
        shutdown_rx,
    );
    tokio::pin!(http);
    tokio::pin!(runtime);
    tokio::select! {
        http_result = &mut http => {
            let _ = shutdown_tx.send(true);
            combine_http_runtime_results(http_result, runtime.await)
        }
        runtime_result = &mut runtime => {
            let _ = shutdown_tx.send(true);
            combine_http_runtime_results(http.await, runtime_result)
        }
    }
}

async fn supervise_writable_runtime(
    state: AgentState,
    mut postgres: PreparedPostgres,
    postgres_config: PostgresConfig,
    writable_lease: WritableLeaseConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<(), AgentRunError> {
    let mut retry = INITIAL_COORDINATION_RETRY;
    loop {
        state.clear_lease();
        let attempt_started = Instant::now();
        match Box::pin(run_writable_attempt(
            state.clone(),
            postgres,
            writable_lease.clone(),
            shutdown.clone(),
        ))
        .await?
        {
            WritableAttemptOutcome::Shutdown => return Ok(()),
            WritableAttemptOutcome::Retry(error) => {
                if attempt_started.elapsed() >= COORDINATION_RETRY_RESET {
                    retry = INITIAL_COORDINATION_RETRY;
                }
                tracing::warn!(reason = %error, retry_after_ms = retry.as_millis(), "writable-term Lease coordination lost; PostgreSQL fenced and coordination will retry");
            }
        }
        if wait_for_shutdown_or_delay(shutdown.clone(), retry).await {
            return Ok(());
        }
        retry = retry.saturating_mul(2).min(MAX_COORDINATION_RETRY);
        let Some(prepared) = prepare_postgres(postgres_config.clone(), shutdown.clone())
            .await
            .map_err(AgentRunError::Postgres)?
        else {
            return Ok(());
        };
        state.set_postgres_process(PostgresProcessState::Validated);
        postgres = prepared;
    }
}

async fn run_writable_attempt(
    state: AgentState,
    postgres: PreparedPostgres,
    writable_lease: WritableLeaseConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<WritableAttemptOutcome, AgentRunError> {
    let margin = writable_lease.shutdown_margin();
    let lease_changes = state.subscribe_lease_changes();
    let (attempt_shutdown_tx, attempt_shutdown_rx) = watch::channel(false);
    let postmaster_state = state.clone();
    let postmaster_shutdown = attempt_shutdown_rx.clone();
    let postmaster = async move {
        if !wait_for_initial_writable_authority(
            &postmaster_state,
            lease_changes,
            postmaster_shutdown.clone(),
            margin,
        )
        .await
        {
            return Ok(());
        }
        postgres
            .supervise_with_writable_authority(postmaster_state, postmaster_shutdown, margin)
            .await
    };
    let coordination =
        pgshard_agent::coordination::supervise(writable_lease, state, attempt_shutdown_rx);
    tokio::pin!(postmaster);
    tokio::pin!(coordination);
    tokio::select! {
        biased;
        () = wait_for_shutdown(shutdown) => {
            let _ = attempt_shutdown_tx.send(true);
            let postmaster_result = postmaster.await;
            if let Err(error) = coordination.await {
                tracing::warn!(reason = %error, "writable-term Lease coordination ended during agent shutdown");
            }
            postmaster_result.map_err(AgentRunError::Postgres)?;
            Ok(WritableAttemptOutcome::Shutdown)
        }
        coordination_result = &mut coordination => {
            let _ = attempt_shutdown_tx.send(true);
            let postmaster_result = postmaster.await.map_err(AgentRunError::Postgres);
            match coordination_result {
                Err(coordination) => match postmaster_result {
                    Ok(()) => Ok(WritableAttemptOutcome::Retry(coordination)),
                    Err(runtime) => Err(AgentRunError::CoordinationAndRuntime {
                        coordination,
                        runtime: Box::new(runtime),
                    }),
                },
                Ok(()) => postmaster_result.and(Err(AgentRunError::CoordinationStopped)),
            }
        }
        postmaster_result = &mut postmaster => {
            let _ = attempt_shutdown_tx.send(true);
            let runtime = postmaster_result.map_err(AgentRunError::Postgres);
            match (runtime, coordination.await) {
                (Ok(()), Err(coordination)) => Ok(WritableAttemptOutcome::Retry(coordination)),
                (Err(runtime), Err(coordination)) => Err(AgentRunError::CoordinationAndRuntime {
                    coordination,
                    runtime: Box::new(runtime),
                }),
                (Err(runtime), Ok(())) => Err(runtime),
                (Ok(()), Ok(())) => Err(AgentRunError::PostgresStopped),
            }
        }
    }
}

enum WritableAttemptOutcome {
    Retry(WritableLeaseError),
    Shutdown,
}

async fn wait_for_initial_writable_authority(
    state: &AgentState,
    mut lease_changes: watch::Receiver<()>,
    mut shutdown: watch::Receiver<bool>,
    required_margin: Duration,
) -> bool {
    loop {
        if *shutdown.borrow_and_update() {
            return false;
        }
        lease_changes.borrow_and_update();
        if state.lease_authority_valid_for(required_margin) {
            return true;
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            changed = lease_changes.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
        }
    }
}

async fn prepare_postgres(
    config: PostgresConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<Option<PreparedPostgres>, PostgresError> {
    let mut preparation = tokio::task::spawn_blocking(move || PreparedPostgres::prepare(config));
    await_preparation(&mut preparation, wait_for_shutdown(shutdown)).await
}

async fn wait_for_shutdown_or_delay(shutdown: watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        biased;
        () = wait_for_shutdown(shutdown) => true,
        () = tokio::time::sleep(delay) => false,
    }
}

async fn await_preparation<T, F>(
    task: &mut tokio::task::JoinHandle<Result<T, PostgresError>>,
    shutdown: F,
) -> Result<Option<T>, PostgresError>
where
    F: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    tokio::select! {
        biased;
        () = shutdown.as_mut() => Ok(None),
        result = tokio::time::timeout(PREPARATION_TIMEOUT, task) => {
            result
                .map_err(|_| PostgresError::ValidationTimeout(PREPARATION_TIMEOUT))?
                .map_err(PostgresError::ValidationTask)?
                .map(Some)
        }
    }
}

fn combine_component_results(
    http: std::io::Result<()>,
    postgres: Result<(), PostgresError>,
) -> Result<(), AgentRunError> {
    match (http, postgres) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), Ok(())) => Err(AgentRunError::Http(source)),
        (Ok(()), Err(source)) => Err(AgentRunError::Postgres(source)),
        (Err(http), Err(postgres)) => Err(AgentRunError::Combined { http, postgres }),
    }
}

fn combine_http_runtime_results(
    http: std::io::Result<()>,
    runtime: Result<(), AgentRunError>,
) -> Result<(), AgentRunError> {
    match (http, runtime) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), Ok(())) => Err(AgentRunError::Http(source)),
        (Ok(()), Err(runtime)) => Err(runtime),
        (Err(http), Err(runtime)) => Err(AgentRunError::HttpAndRuntime {
            http,
            runtime: Box::new(runtime),
        }),
    }
}

#[derive(Debug, thiserror::Error)]
enum AgentRunError {
    #[error("validated agent configuration produced an inconsistent runtime composition")]
    InvalidRuntimeComposition,
    #[error("agent HTTP server failed: {0}")]
    Http(#[source] std::io::Error),
    #[error("PostgreSQL supervisor failed: {0}")]
    Postgres(#[source] PostgresError),
    #[error("PostgreSQL supervisor failed: {postgres}; agent HTTP server also failed: {http}")]
    Combined {
        #[source]
        postgres: PostgresError,
        http: std::io::Error,
    },
    #[error("writable-term Lease coordination stopped without shutdown or an error")]
    CoordinationStopped,
    #[error("PostgreSQL supervision stopped without shutdown or an error")]
    PostgresStopped,
    #[error(
        "writable-term Lease coordination failed: {coordination}; agent runtime also failed: {runtime}"
    )]
    CoordinationAndRuntime {
        #[source]
        coordination: WritableLeaseError,
        runtime: Box<AgentRunError>,
    },
    #[error("agent runtime failed: {runtime}; agent HTTP server also failed: {http}")]
    HttpAndRuntime {
        http: std::io::Error,
        #[source]
        runtime: Box<AgentRunError>,
    },
}

struct ShutdownSignals {
    terminate: Signal,
    interrupt: Signal,
}

impl ShutdownSignals {
    fn install() -> std::io::Result<Self> {
        Ok(Self {
            terminate: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
        })
    }

    async fn recv(&mut self) {
        tokio::select! {
            _ = self.terminate.recv() => {}
            _ = self.interrupt.recv() => {}
        }
    }
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    if *receiver.borrow_and_update() {
        return;
    }
    while receiver.changed().await.is_ok() {
        if *receiver.borrow_and_update() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgshard_agent::domain::{AgentIdentity, FencingLease};
    use pgshard_types::ShardId;
    use std::sync::mpsc;

    #[test]
    fn preserves_simultaneous_http_and_postgres_failures() {
        let result = combine_component_results(
            Err(std::io::Error::other("HTTP failed")),
            Err(PostgresError::PreparedStateChanged),
        );
        let AgentRunError::Combined { http, postgres } = result.expect_err("both failures survive")
        else {
            panic!("expected a combined component failure");
        };
        assert_eq!(http.to_string(), "HTTP failed");
        assert!(matches!(postgres, PostgresError::PreparedStateChanged));
    }

    #[test]
    fn preserves_simultaneous_http_and_writable_runtime_failures() {
        let result = combine_http_runtime_results(
            Err(std::io::Error::other("HTTP failed")),
            Err(AgentRunError::Postgres(PostgresError::PreparedStateChanged)),
        );
        let AgentRunError::HttpAndRuntime { http, runtime } =
            result.expect_err("both failures survive")
        else {
            panic!("expected a combined HTTP and runtime failure");
        };
        assert_eq!(http.to_string(), "HTTP failed");
        assert!(matches!(
            *runtime,
            AgentRunError::Postgres(PostgresError::PreparedStateChanged)
        ));
    }

    #[tokio::test]
    async fn postgres_start_waits_for_authority_beyond_the_fencing_margin() {
        let state = agent_state(10_000);
        let lease_changes = state.subscribe_lease_changes();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut wait = Box::pin(wait_for_initial_writable_authority(
            &state,
            lease_changes,
            shutdown_rx,
            Duration::from_secs(1),
        ));

        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut wait)
                .await
                .is_err(),
            "PostgreSQL start advanced without authority"
        );
        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 5_001,
                },
                1,
            )
            .expect("install startup authority");

        assert!(
            tokio::time::timeout(Duration::from_millis(100), wait)
                .await
                .expect("authority notification is bounded")
        );
    }

    #[tokio::test]
    async fn postgres_start_waits_for_a_renewal_after_authority_enters_the_margin() {
        let state = agent_state(20_000);
        let lease_changes = state.subscribe_lease_changes();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut wait = Box::pin(wait_for_initial_writable_authority(
            &state,
            lease_changes,
            shutdown_rx,
            Duration::from_secs(6),
        ));

        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 5_001,
                },
                1,
            )
            .expect("install authority inside the startup margin");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut wait)
                .await
                .is_err(),
            "PostgreSQL start accepted authority inside the fencing margin"
        );

        state
            .install_lease(
                FencingLease {
                    owner_instance: "instance-1".to_owned(),
                    epoch: 1,
                    valid_until_unix_ms: 10_001,
                },
                1,
            )
            .expect("renew authority beyond the startup margin");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), wait)
                .await
                .expect("renewal notification is bounded")
        );
    }

    #[tokio::test]
    async fn shutdown_before_authority_leaves_postgres_unstarted() {
        let state = agent_state(10_000);
        let lease_changes = state.subscribe_lease_changes();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).expect("request shutdown");

        assert!(
            !wait_for_initial_writable_authority(
                &state,
                lease_changes,
                shutdown_rx,
                Duration::from_secs(1),
            )
            .await
        );
    }

    fn agent_state(max_lease_ttl_ms: u64) -> AgentState {
        AgentState::with_identity(
            AgentIdentity {
                cluster_id: "cluster-1".to_owned(),
                shard_id: ShardId(0),
                instance_id: "instance-1".to_owned(),
            },
            max_lease_ttl_ms,
        )
        .expect("valid state")
    }

    #[tokio::test]
    async fn shutdown_cancels_a_genuinely_blocked_preparation_wait() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let mut task = tokio::task::spawn_blocking(move || {
            started_tx.send(()).expect("publish blocked preparation");
            release_rx.recv().expect("release blocked preparation");
            Ok::<(), PostgresError>(())
        });
        let shutdown = async {
            started_rx.await.expect("observe blocked preparation");
        };

        let result = await_preparation(&mut task, shutdown)
            .await
            .expect("shutdown is not a preparation failure");

        assert!(result.is_none(), "blocked preparation must be abandoned");
        release_tx.send(()).expect("release preparation worker");
        task.await
            .expect("join released preparation worker")
            .expect("released preparation succeeds");
    }
}
