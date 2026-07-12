//! Linux-container health, readiness, status, and metrics server.

use std::future::Future;
use std::net::SocketAddr;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use pgshard_types::PgLsn;
use serde::Serialize;

use crate::domain::{AgentSnapshot, AgentState};

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
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "alive" })
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
    let body = format!(
        concat!(
            "# HELP pgshard_agent_up Whether the process health endpoint is running.\n",
            "# TYPE pgshard_agent_up gauge\n",
            "pgshard_agent_up 1\n",
            "# HELP pgshard_agent_ready Whether identity, fencing, and PostgreSQL state are ready.\n",
            "# TYPE pgshard_agent_ready gauge\n",
            "pgshard_agent_ready {}\n",
            "# HELP pgshard_agent_fencing_epoch Current locally installed fencing epoch.\n",
            "# TYPE pgshard_agent_fencing_epoch gauge\n",
            "pgshard_agent_fencing_epoch {epoch}\n",
            "# HELP pgshard_agent_postgres_timeline Current observed PostgreSQL timeline.\n",
            "# TYPE pgshard_agent_postgres_timeline gauge\n",
            "pgshard_agent_postgres_timeline {timeline}\n",
            "# HELP pgshard_agent_postgres_flush_lsn Current flush LSN encoded as an integer.\n",
            "# TYPE pgshard_agent_postgres_flush_lsn gauge\n",
            "pgshard_agent_postgres_flush_lsn {flush_lsn}\n",
            "# HELP pgshard_agent_postgres_replay_lsn Current replay LSN encoded as an integer.\n",
            "# TYPE pgshard_agent_postgres_replay_lsn gauge\n",
            "pgshard_agent_postgres_replay_lsn {replay_lsn}\n"
        ),
        u8::from(readiness.ready),
        epoch = epoch,
        timeline = timeline,
        flush_lsn = flush_lsn,
        replay_lsn = replay_lsn,
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}
