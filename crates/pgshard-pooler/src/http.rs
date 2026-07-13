//! Linux-container health, readiness, status, and metrics server.

use std::future::Future;
use std::net::SocketAddr;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::state::{PoolerSnapshot, PoolerState};

/// Builds the pooler's low-frequency control-plane routes.
pub fn router(state: PoolerState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Runs the HTTP server until shutdown is requested.
///
/// # Errors
///
/// Returns an I/O error if the listener cannot bind or the server fails.
pub async fn serve(
    bind: SocketAddr,
    state: PoolerState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router(state))
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

async fn readiness(State(state): State<PoolerState>) -> Response {
    let readiness = state.readiness();
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness)).into_response()
}

async fn status(State(state): State<PoolerState>) -> Json<PoolerSnapshot> {
    Json(state.snapshot())
}

async fn metrics(State(state): State<PoolerState>) -> impl IntoResponse {
    let snapshot = state.snapshot();
    let catalog = snapshot.catalog;
    let epoch = catalog.catalog_epoch.unwrap_or(0);
    let cache_age_milliseconds = catalog.cache_age.map_or(0, |age| age.as_millis());
    let cache_initialized = u8::from(catalog.catalog_epoch.is_some());
    let last_failure = catalog.last_failure.unwrap_or("none");
    let body = format!(
        concat!(
            "# HELP pgshard_pooler_up Whether the process health endpoint is running.\n",
            "# TYPE pgshard_pooler_up gauge\n",
            "pgshard_pooler_up 1\n",
            "# HELP pgshard_pooler_build_info Build identity for this process.\n",
            "# TYPE pgshard_pooler_build_info gauge\n",
            "pgshard_pooler_build_info{{version=\"{}\",git_sha=\"{}\"}} 1\n",
            "# HELP pgshard_pooler_ready Whether the pooler may accept new application work.\n",
            "# TYPE pgshard_pooler_ready gauge\n",
            "pgshard_pooler_ready {}\n",
            "# HELP pgshard_pooler_catalog_connection_up Whether the catalog driver owns a connection.\n",
            "# TYPE pgshard_pooler_catalog_connection_up gauge\n",
            "pgshard_pooler_catalog_connection_up {}\n",
            "# HELP pgshard_pooler_catalog_phase_info Current bounded catalog connection phase.\n",
            "# TYPE pgshard_pooler_catalog_phase_info gauge\n",
            "pgshard_pooler_catalog_phase_info{{phase=\"{}\"}} 1\n",
            "# HELP pgshard_pooler_catalog_readiness_info Current bounded catalog readiness reason.\n",
            "# TYPE pgshard_pooler_catalog_readiness_info gauge\n",
            "pgshard_pooler_catalog_readiness_info{{reason=\"{}\"}} 1\n",
            "# HELP pgshard_pooler_catalog_cache_initialized Whether an authoritative catalog epoch has loaded.\n",
            "# TYPE pgshard_pooler_catalog_cache_initialized gauge\n",
            "pgshard_pooler_catalog_cache_initialized {cache_initialized}\n",
            "# HELP pgshard_pooler_catalog_epoch Latest authoritative catalog epoch.\n",
            "# TYPE pgshard_pooler_catalog_epoch gauge\n",
            "pgshard_pooler_catalog_epoch {epoch}\n",
            "# HELP pgshard_pooler_catalog_cache_age_milliseconds Monotonic age of the last authoritative load.\n",
            "# TYPE pgshard_pooler_catalog_cache_age_milliseconds gauge\n",
            "pgshard_pooler_catalog_cache_age_milliseconds {cache_age_milliseconds}\n",
            "# HELP pgshard_pooler_catalog_consecutive_failures Current consecutive catalog failures.\n",
            "# TYPE pgshard_pooler_catalog_consecutive_failures gauge\n",
            "pgshard_pooler_catalog_consecutive_failures {}\n",
            "# HELP pgshard_pooler_catalog_failures_total Catalog failures observed by this process.\n",
            "# TYPE pgshard_pooler_catalog_failures_total counter\n",
            "pgshard_pooler_catalog_failures_total {}\n",
            "# HELP pgshard_pooler_catalog_connect_attempts_total Catalog connection attempts by this process.\n",
            "# TYPE pgshard_pooler_catalog_connect_attempts_total counter\n",
            "pgshard_pooler_catalog_connect_attempts_total {}\n",
            "# HELP pgshard_pooler_catalog_successful_connections_total Connections completing their initial authoritative load.\n",
            "# TYPE pgshard_pooler_catalog_successful_connections_total counter\n",
            "pgshard_pooler_catalog_successful_connections_total {}\n",
            "# HELP pgshard_pooler_catalog_last_failure_info Latest bounded unresolved failure category.\n",
            "# TYPE pgshard_pooler_catalog_last_failure_info gauge\n",
            "pgshard_pooler_catalog_last_failure_info{{kind=\"{last_failure}\"}} 1\n"
        ),
        pgshard_version::VERSION,
        pgshard_version::GIT_SHA,
        u8::from(catalog.ready),
        u8::from(catalog.connection_up),
        catalog.phase,
        catalog.readiness_reason,
        catalog.consecutive_failures,
        catalog.total_failures,
        catalog.connect_attempts,
        catalog.successful_connections,
        cache_initialized = cache_initialized,
        epoch = epoch,
        cache_age_milliseconds = cache_age_milliseconds,
        last_failure = last_failure,
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::state::PoolerCatalogSnapshot;

    fn state(ready: bool) -> PoolerState {
        PoolerState::from_catalog(PoolerCatalogSnapshot {
            phase: if ready { "connected" } else { "backoff" },
            connection_up: ready,
            ready,
            readiness_reason: if ready { "ready" } else { "stale" },
            catalog_epoch: Some(u64::MAX),
            cache_age: Some(Duration::from_millis(42)),
            consecutive_failures: 2,
            total_failures: 3,
            connect_attempts: 4,
            successful_connections: 1,
            last_failure: (!ready).then_some("connection"),
        })
    }

    async fn request(path: &str, state: PoolerState) -> Response {
        router(state)
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("HTTP request"),
            )
            .await
            .expect("pooler route")
    }

    async fn body(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), 1_048_576)
            .await
            .expect("bounded response body");
        String::from_utf8(bytes.to_vec()).expect("UTF-8 response")
    }

    #[tokio::test]
    async fn health_is_independent_from_fail_closed_readiness() {
        let health = request("/healthz", state(false)).await;
        assert_eq!(health.status(), StatusCode::OK);

        let unready = request("/readyz", state(false)).await;
        assert_eq!(unready.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body(unready).await, r#"{"ready":false,"reason":"stale"}"#);

        let ready = request("/readyz", state(true)).await;
        assert_eq!(ready.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn status_and_metrics_publish_exact_bounded_catalog_state() {
        let status = request("/status", state(false)).await;
        let value: serde_json::Value =
            serde_json::from_str(&body(status).await).expect("status JSON");
        assert_eq!(value["catalog"]["catalog_epoch"], u64::MAX.to_string());
        assert_eq!(value["catalog"]["cache_age_milliseconds"], "42");

        let metrics = request("/metrics", state(false)).await;
        assert_eq!(
            metrics.headers()[header::CONTENT_TYPE],
            "text/plain; version=0.0.4"
        );
        let metrics = body(metrics).await;
        assert!(metrics.contains("pgshard_pooler_ready 0\n"));
        assert!(metrics.contains("pgshard_pooler_catalog_phase_info{phase=\"backoff\"} 1\n"));
        assert!(metrics.contains("pgshard_pooler_catalog_readiness_info{reason=\"stale\"} 1\n"));
        assert!(metrics.contains(&format!("pgshard_pooler_catalog_epoch {}\n", u64::MAX)));
        assert!(
            metrics.contains("pgshard_pooler_catalog_last_failure_info{kind=\"connection\"} 1\n")
        );
    }
}
