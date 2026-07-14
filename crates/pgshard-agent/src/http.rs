//! Linux-container health, readiness, status, and metrics server.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use pgshard_types::PgLsn;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::task::{JoinError, JoinSet};
use tokio::time::timeout;

use crate::domain::{AgentSnapshot, AgentState, PostgresProcessState};

const MAX_HTTP_CONNECTIONS: usize = 128;
const MAX_HTTP_HEADERS: usize = 32;
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1_024;
const HTTP_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Runs the HTTP server until shutdown is requested.
///
/// # Errors
///
/// Returns an I/O error if the listener cannot bind or the server fails.
pub async fn serve(
    bind: SocketAddr,
    state: AgentState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    serve_on(listener, state, shutdown).await
}

/// Runs the HTTP server on an already-bound listener until shutdown.
///
/// Binding separately lets a process supervisor prove its control endpoint is
/// available before starting a child process. Once shutdown begins, active HTTP
/// connections receive a bounded drain period before they are dropped.
///
/// # Errors
///
/// Returns an I/O error if the server fails.
pub async fn serve_on(
    listener: tokio::net::TcpListener,
    state: AgentState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .with_state(state);
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = shutdown.as_mut() => break,
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(result) = completed {
                    connection_task_result(result)?;
                }
            }
            accepted = listener.accept(), if connections.len() < MAX_HTTP_CONNECTIONS => {
                let (stream, _) = accepted?;
                connections.spawn(serve_connection(stream, app.clone()));
            }
        }
    }
    drain_connections(&mut connections).await
}

async fn serve_connection(stream: TcpStream, app: Router) {
    let mut server = http1::Builder::new();
    server
        .keep_alive(false)
        .max_headers(MAX_HTTP_HEADERS)
        .max_buf_size(MAX_HTTP_HEADER_BYTES)
        .timer(TokioTimer::new())
        .header_read_timeout(HTTP_HEADER_TIMEOUT);
    let connection = server.serve_connection(TokioIo::new(stream), TowerToHyperService::new(app));
    match timeout(HTTP_CONNECTION_TIMEOUT, connection).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::debug!(%error, "agent HTTP connection closed with a protocol error");
        }
        Err(_) => tracing::debug!("agent HTTP connection exceeded its lifetime"),
    }
}

async fn drain_connections(connections: &mut JoinSet<()>) -> io::Result<()> {
    let drained = timeout(HTTP_DRAIN_TIMEOUT, async {
        while let Some(result) = connections.join_next().await {
            connection_task_result(result)?;
        }
        Ok(())
    })
    .await;
    if let Ok(result) = drained {
        result
    } else {
        tracing::warn!(
            timeout_ms = HTTP_DRAIN_TIMEOUT.as_millis(),
            remaining_connections = connections.len(),
            "forcing agent HTTP shutdown after drain timeout"
        );
        connections.abort_all();
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result
                && !error.is_cancelled()
            {
                return Err(io::Error::other(format!(
                    "join aborted agent HTTP connection: {error}"
                )));
            }
        }
        Ok(())
    }
}

fn connection_task_result(result: Result<(), JoinError>) -> io::Result<()> {
    result.map_err(|error| io::Error::other(format!("join agent HTTP connection: {error}")))
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    version: &'static str,
    git_sha: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health {
        status: "alive",
        version: pgshard_version::VERSION,
        git_sha: pgshard_version::GIT_SHA,
    })
}

async fn readiness(State(state): State<AgentState>) -> Response {
    let readiness = state.readiness();
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness)).into_response()
}

async fn status(State(state): State<AgentState>) -> Json<AgentSnapshot> {
    Json(state.snapshot())
}

async fn metrics(State(state): State<AgentState>) -> impl IntoResponse {
    let snapshot = state.snapshot();
    let readiness = state.readiness();
    let epoch = snapshot.lease.as_ref().map_or(0, |lease| lease.epoch);
    let timeline = snapshot
        .postgres
        .as_ref()
        .map_or(0, |postgres| postgres.timeline);
    let postgres_fencing_epoch = snapshot
        .postgres
        .as_ref()
        .map_or(0, |postgres| postgres.fencing_epoch);
    let flush_lsn = snapshot
        .postgres
        .as_ref()
        .and_then(|postgres| postgres.flush_lsn)
        .map_or(0, |PgLsn(lsn)| lsn);
    let replay_lsn = snapshot
        .postgres
        .as_ref()
        .and_then(|postgres| postgres.replay_lsn)
        .map_or(0, |PgLsn(lsn)| lsn);
    let postgres_process_up = u8::from(matches!(
        snapshot.postgres_process,
        PostgresProcessState::StartingQuarantined
            | PostgresProcessState::RunningQuarantined
            | PostgresProcessState::Stopping
    ));
    let body = format!(
        concat!(
            "# HELP pgshard_agent_up Whether the process health endpoint is running.\n",
            "# TYPE pgshard_agent_up gauge\n",
            "pgshard_agent_up 1\n",
            "# HELP pgshard_agent_build_info Build identity for this process.\n",
            "# TYPE pgshard_agent_build_info gauge\n",
            "pgshard_agent_build_info{{version=\"{}\",git_sha=\"{}\"}} 1\n",
            "# HELP pgshard_agent_ready Whether identity, fencing, and PostgreSQL state are ready.\n",
            "# TYPE pgshard_agent_ready gauge\n",
            "pgshard_agent_ready {}\n",
            "# HELP pgshard_agent_fencing_epoch Current locally installed fencing epoch.\n",
            "# TYPE pgshard_agent_fencing_epoch gauge\n",
            "pgshard_agent_fencing_epoch {epoch}\n",
            "# HELP pgshard_agent_postgres_timeline Current observed PostgreSQL timeline.\n",
            "# TYPE pgshard_agent_postgres_timeline gauge\n",
            "pgshard_agent_postgres_timeline {timeline}\n",
            "# HELP pgshard_agent_postgres_fencing_epoch Durable fencing epoch observed inside PostgreSQL.\n",
            "# TYPE pgshard_agent_postgres_fencing_epoch gauge\n",
            "pgshard_agent_postgres_fencing_epoch {postgres_fencing_epoch}\n",
            "# HELP pgshard_agent_postgres_flush_lsn Current flush LSN encoded as an integer.\n",
            "# TYPE pgshard_agent_postgres_flush_lsn gauge\n",
            "pgshard_agent_postgres_flush_lsn {flush_lsn}\n",
            "# HELP pgshard_agent_postgres_replay_lsn Current replay LSN encoded as an integer.\n",
            "# TYPE pgshard_agent_postgres_replay_lsn gauge\n",
            "pgshard_agent_postgres_replay_lsn {replay_lsn}\n",
            "# HELP pgshard_agent_postgres_process_up Whether a locally supervised postmaster process exists.\n",
            "# TYPE pgshard_agent_postgres_process_up gauge\n",
            "pgshard_agent_postgres_process_up {postgres_process_up}\n"
        ),
        pgshard_version::VERSION,
        pgshard_version::GIT_SHA,
        u8::from(readiness.ready),
        epoch = epoch,
        timeline = timeline,
        postgres_fencing_epoch = postgres_fencing_epoch,
        flush_lsn = flush_lsn,
        replay_lsn = replay_lsn,
        postgres_process_up = postgres_process_up,
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}
