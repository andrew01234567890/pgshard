//! `pgshard-agent` Linux container entry point.

use pgshard_agent::catalog_activation_consumer::{
    CatalogActivationCapabilityState, spawn_catalog_activation_consumer,
};
use pgshard_agent::config::{AgentConfig, ConfigError};
use pgshard_agent::coordination::WritableLeaseConfig;
use pgshard_agent::domain::{AgentState, PostgresProcessState};
use pgshard_agent::postgres::{PostgresConfig, PostgresError, PreparedPostgres};
use pgshard_agent::writable::{WritableAttemptError, WritableAttemptOutcome};
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
        activation_config,
        catalog_activation_consumer,
    } = config;
    let telemetry = telemetry.status();
    if telemetry.endpoint_configured {
        tracing::warn!(reason = telemetry.reason, "OpenTelemetry export disabled");
    } else {
        tracing::info!(reason = telemetry.reason, "OpenTelemetry export disabled");
    }

    let state = AgentState::with_identity(identity, max_lease_ttl_ms)?;
    if let Some(activation_config) = activation_config {
        state.set_activation_config(activation_config);
    }
    let catalog_activation = if catalog_activation_consumer.is_some() {
        CatalogActivationCapabilityState::configured()
    } else {
        CatalogActivationCapabilityState::disabled()
    };
    spawn_catalog_activation_consumer(
        catalog_activation_consumer,
        catalog_activation.clone(),
        shutdown_rx.clone(),
    );
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
        postgres_supervision = postgres.is_some(),
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
        catalog_activation,
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

#[allow(clippy::too_many_arguments)]
async fn run_services(
    http_bind: SocketAddr,
    state: AgentState,
    postgres: Option<PreparedPostgres>,
    postgres_config: Option<PostgresConfig>,
    writable_lease: Option<WritableLeaseConfig>,
    catalog_activation: CatalogActivationCapabilityState,
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
            catalog_activation,
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
        let http = pgshard_agent::http::serve_on_with_catalog_activation(
            listener,
            state.clone(),
            catalog_activation,
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
        pgshard_agent::http::serve_with_catalog_activation(
            http_bind,
            state,
            catalog_activation,
            wait_for_shutdown(shutdown_rx),
        )
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
    catalog_activation: CatalogActivationCapabilityState,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), AgentRunError> {
    let listener = tokio::net::TcpListener::bind(http_bind)
        .await
        .map_err(AgentRunError::Http)?;
    let http = pgshard_agent::http::serve_on_with_catalog_activation(
        listener,
        state.clone(),
        catalog_activation,
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
        match Box::pin(pgshard_agent::writable::supervise_attempt(
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
                if error.is_permanent() {
                    tracing::error!(reason = %error, retry_after_ms = retry.as_millis(), "writable-term Lease coordination failed with a permanent error; PostgreSQL fenced and explicit operator recovery may be required");
                } else {
                    tracing::warn!(reason = %error, retry_after_ms = retry.as_millis(), "writable-term Lease coordination lost; PostgreSQL fenced and coordination will retry");
                }
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
    #[error("writable PostgreSQL runtime failed: {0}")]
    Writable(#[from] WritableAttemptError),
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
            Err(AgentRunError::Writable(WritableAttemptError::Postgres(
                PostgresError::PreparedStateChanged,
            ))),
        );
        let AgentRunError::HttpAndRuntime { http, runtime } =
            result.expect_err("both failures survive")
        else {
            panic!("expected a combined HTTP and runtime failure");
        };
        assert_eq!(http.to_string(), "HTTP failed");
        assert!(matches!(
            *runtime,
            AgentRunError::Writable(WritableAttemptError::Postgres(
                PostgresError::PreparedStateChanged
            ))
        ));
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
