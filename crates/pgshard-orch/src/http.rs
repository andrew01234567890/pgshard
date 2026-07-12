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
}

#[derive(Serialize)]
struct Ready {
    ready: bool,
    reason: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "alive" })
}

async fn readiness(State(state): State<OrchState>) -> Response {
    let ready = state.is_ready();
    let response = Ready {
        ready,
        reason: if ready {
            "identity_established"
        } else {
            "identity_missing"
        },
    };
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(response)).into_response()
}

async fn status(State(state): State<OrchState>) -> Json<OrchSnapshot> {
    Json(state.snapshot())
}

async fn metrics(State(state): State<OrchState>) -> impl IntoResponse {
    let snapshot = state.snapshot();
    let ready = u8::from(snapshot.identity.is_some());
    let body = format!(
        concat!(
            "# HELP pgshard_orch_up Whether the process health endpoint is running.\n",
            "# TYPE pgshard_orch_up gauge\n",
            "pgshard_orch_up 1\n",
            "# HELP pgshard_orch_ready Whether the orchestrator identity is established.\n",
            "# TYPE pgshard_orch_ready gauge\n",
            "pgshard_orch_ready {ready}\n",
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
        snapshot.operation_count,
        snapshot.leases.len(),
        ready = ready,
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}
