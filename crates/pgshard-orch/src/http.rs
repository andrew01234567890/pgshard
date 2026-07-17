//! Linux-container health, readiness, status, and metrics server.

use std::future::Future;
use std::net::SocketAddr;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::domain::{OrchSnapshot, OrchState};

/// Runs the HTTP server until shutdown is requested.
///
/// # Errors
///
/// Returns an I/O error if the listener cannot bind or the server fails.
pub async fn serve(
    bind: SocketAddr,
    state: OrchState,
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

async fn readiness(State(state): State<OrchState>) -> Response {
    let readiness = state.readiness();
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness)).into_response()
}

async fn status(State(state): State<OrchState>) -> Json<OrchSnapshot> {
    Json(state.snapshot())
}

async fn metrics(State(state): State<OrchState>) -> impl IntoResponse {
    let snapshot = state.snapshot();
    let readiness = state.readiness();
    let ready = u8::from(readiness.ready);
    let body = format!(
        concat!(
            "# HELP pgshard_orch_up Whether the process health endpoint is running.\n",
            "# TYPE pgshard_orch_up gauge\n",
            "pgshard_orch_up 1\n",
            "# HELP pgshard_orch_build_info Build identity for this process.\n",
            "# TYPE pgshard_orch_build_info gauge\n",
            "pgshard_orch_build_info{{version=\"{}\",git_sha=\"{}\"}} 1\n",
            "# HELP pgshard_orch_ready Whether this process can observe the operator-owned Kubernetes Lease.\n",
            "# TYPE pgshard_orch_ready gauge\n",
            "pgshard_orch_ready {ready}\n",
            "# HELP pgshard_orch_coordination_ready Whether the operator-owned Kubernetes Lease was observed within the local deadline.\n",
            "# TYPE pgshard_orch_coordination_ready gauge\n",
            "pgshard_orch_coordination_ready {coordination_ready}\n",
            "# HELP pgshard_orch_leader Whether this process holds the orchestrator Kubernetes Lease.\n",
            "# TYPE pgshard_orch_leader gauge\n",
            "pgshard_orch_leader {leader}\n",
            "# HELP pgshard_orch_operations Registered idempotent operations.\n",
            "# TYPE pgshard_orch_operations gauge\n",
            "pgshard_orch_operations {}\n",
            "# HELP pgshard_orch_shard_leases Locally tracked shard leases.\n",
            "# TYPE pgshard_orch_shard_leases gauge\n",
            "pgshard_orch_shard_leases {}\n",
            "# HELP pgshard_orch_failover_automation_enabled Whether safe failover automation is implemented.\n",
            "# TYPE pgshard_orch_failover_automation_enabled gauge\n",
            "pgshard_orch_failover_automation_enabled 0\n",
            "# HELP pgshard_orch_persistence_enabled Whether operation and lease state survives restart.\n",
            "# TYPE pgshard_orch_persistence_enabled gauge\n",
            "pgshard_orch_persistence_enabled 0\n"
        ),
        pgshard_version::VERSION,
        pgshard_version::GIT_SHA,
        snapshot.operation_count,
        snapshot.leases.len(),
        ready = ready,
        coordination_ready = u8::from(snapshot.coordination_ready),
        leader = u8::from(snapshot.leader),
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}
