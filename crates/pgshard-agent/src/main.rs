//! `pgshard-agent` Linux container entry point.

use pgshard_agent::config::{AgentConfig, ConfigError};
use pgshard_agent::domain::{AgentState, PostgresProcessState};
use pgshard_agent::postgres::{PostgresError, PreparedPostgres};
use std::future::Future;
use std::time::Duration;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::watch;

const PREPARATION_TIMEOUT: Duration = Duration::from_secs(30);
const BLOCKING_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

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
    let postgres = match postgres {
        Some(config) => {
            let mut preparation =
                tokio::task::spawn_blocking(move || PreparedPostgres::prepare(config));
            if let Some(postgres) =
                await_preparation(&mut preparation, wait_for_shutdown(shutdown_rx.clone())).await?
            {
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
    let run_shutdown_tx = shutdown_tx.clone();
    let run = async move {
        if let Some(postgres) = postgres {
            let listener = tokio::net::TcpListener::bind(http_bind).await?;
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
                    combine_component_results(result, postmaster_result)?;
                }
                result = &mut postmaster => {
                    let _ = run_shutdown_tx.send(true);
                    let http_result = http.await;
                    combine_component_results(http_result, result)?;
                }
            }
        } else {
            pgshard_agent::http::serve(http_bind, state, wait_for_shutdown(shutdown_rx)).await?;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    let result = run.await;
    signal_task.abort();
    let _ = signal_task.await;
    result?;
    tracing::info!("agent shutdown complete");
    Ok(())
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

#[derive(Debug, thiserror::Error)]
enum AgentRunError {
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
