//! Linux-container health, readiness, status, and metrics server.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::server::{
    AcceptBackoff, TcpAcceptor, accept_bounded, connection_task_result, drain_connections,
};
use crate::state::{PoolerSnapshot, PoolerState};

const MAX_HTTP_CONNECTIONS: usize = 128;
const MAX_HTTP_HEADERS: usize = 32;
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
const HTTP_ACCEPT_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(10);
const HTTP_ACCEPT_MAX_RETRY_DELAY: Duration = Duration::from_secs(1);
const HTTP_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct HttpServerPolicy {
    maximum_connections: usize,
    maximum_headers: usize,
    maximum_header_bytes: usize,
    accept_initial_retry_delay: Duration,
    accept_max_retry_delay: Duration,
    header_timeout: Duration,
    connection_timeout: Duration,
    shutdown_timeout: Duration,
    #[cfg(test)]
    accepted_connections: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

const DEFAULT_HTTP_SERVER_POLICY: HttpServerPolicy = HttpServerPolicy {
    maximum_connections: MAX_HTTP_CONNECTIONS,
    maximum_headers: MAX_HTTP_HEADERS,
    maximum_header_bytes: MAX_HTTP_HEADER_BYTES,
    accept_initial_retry_delay: HTTP_ACCEPT_INITIAL_RETRY_DELAY,
    accept_max_retry_delay: HTTP_ACCEPT_MAX_RETRY_DELAY,
    header_timeout: HTTP_HEADER_TIMEOUT,
    connection_timeout: HTTP_CONNECTION_TIMEOUT,
    shutdown_timeout: HTTP_SHUTDOWN_TIMEOUT,
    #[cfg(test)]
    accepted_connections: None,
};

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
    serve_listener(listener, state, shutdown).await
}

/// Runs the HTTP server on an already bound listener until shutdown.
///
/// # Errors
///
/// Returns an I/O error if the server fails.
pub async fn serve_listener(
    listener: TcpListener,
    state: PoolerState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    serve_listener_with_policy(listener, state, shutdown, DEFAULT_HTTP_SERVER_POLICY).await
}

async fn serve_listener_with_policy(
    listener: TcpListener,
    state: PoolerState,
    shutdown: impl Future<Output = ()> + Send + 'static,
    policy: HttpServerPolicy,
) -> io::Result<()> {
    serve_acceptor_with_policy(listener, state, shutdown, policy).await
}

async fn serve_acceptor_with_policy<A>(
    mut acceptor: A,
    state: PoolerState,
    shutdown: impl Future<Output = ()> + Send + 'static,
    policy: HttpServerPolicy,
) -> io::Result<()>
where
    A: TcpAcceptor,
{
    let routes = router(state);
    let permits = Arc::new(Semaphore::new(policy.maximum_connections));
    let mut accept_backoff = AcceptBackoff::new(
        policy.accept_initial_retry_delay,
        policy.accept_max_retry_delay,
    );
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
            accepted = accept_bounded(
                &mut acceptor,
                Arc::clone(&permits),
                &mut accept_backoff,
                "HTTP",
            ) => {
                let (stream, permit) = accepted?;
                #[cfg(test)]
                if let Some(accepted) = &policy.accepted_connections {
                    accepted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                connections.spawn(serve_connection(
                    stream,
                    routes.clone(),
                    permit,
                    policy.clone(),
                ));
            }
        }
    }

    drain_connections(&mut connections, policy.shutdown_timeout).await
}

async fn serve_connection(
    stream: TcpStream,
    routes: Router,
    _permit: OwnedSemaphorePermit,
    policy: HttpServerPolicy,
) {
    let mut server = http1::Builder::new();
    server
        .keep_alive(false)
        .max_headers(policy.maximum_headers)
        .max_buf_size(policy.maximum_header_bytes)
        .timer(TokioTimer::new())
        .header_read_timeout(policy.header_timeout);
    let connection =
        server.serve_connection(TokioIo::new(stream), TowerToHyperService::new(routes));
    match tokio::time::timeout(policy.connection_timeout, connection).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::debug!(%error, "pooler HTTP connection closed with a protocol error");
        }
        Err(_) => {
            tracing::debug!("pooler HTTP connection exceeded its lifetime");
        }
    }
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
            "# HELP pgshard_pooler_catalog_ready Whether the cached catalog may be used for planning.\n",
            "# TYPE pgshard_pooler_catalog_ready gauge\n",
            "pgshard_pooler_catalog_ready {}\n",
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
        u8::from(snapshot.ready),
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::state::PoolerCatalogSnapshot;

    struct ErrorOnceAcceptor {
        listener: TcpListener,
        attempts: Arc<AtomicUsize>,
    }

    impl TcpAcceptor for ErrorOnceAcceptor {
        async fn accept(&mut self) -> io::Result<TcpStream> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(io::Error::other("injected accept failure"))
            } else {
                self.listener.accept().await.map(|(stream, _)| stream)
            }
        }
    }

    struct AlwaysFailAcceptor {
        attempts: Arc<AtomicUsize>,
    }

    impl TcpAcceptor for AlwaysFailAcceptor {
        async fn accept(&mut self) -> io::Result<TcpStream> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("injected persistent accept failure"))
        }
    }

    fn state(ready: bool) -> PoolerState {
        state_with_data_plane(ready, ready)
    }

    fn state_with_data_plane(catalog_ready: bool, data_plane_ready: bool) -> PoolerState {
        PoolerState::from_catalog(
            PoolerCatalogSnapshot {
                phase: if catalog_ready {
                    "connected"
                } else {
                    "backoff"
                },
                connection_up: catalog_ready,
                ready: catalog_ready,
                readiness_reason: if catalog_ready { "ready" } else { "stale" },
                catalog_epoch: Some(u64::MAX),
                cache_age: Some(Duration::from_millis(42)),
                consecutive_failures: 2,
                total_failures: 3,
                connect_attempts: 4,
                successful_connections: 1,
                last_failure: (!catalog_ready).then_some("connection"),
            },
            data_plane_ready,
        )
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

    fn test_policy(accepted_connections: Option<Arc<AtomicUsize>>) -> HttpServerPolicy {
        HttpServerPolicy {
            maximum_connections: 1,
            maximum_headers: MAX_HTTP_HEADERS,
            maximum_header_bytes: MAX_HTTP_HEADER_BYTES,
            accept_initial_retry_delay: Duration::from_millis(1),
            accept_max_retry_delay: Duration::from_millis(2),
            header_timeout: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_millis(25),
            accepted_connections,
        }
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
        assert!(metrics.contains("pgshard_pooler_catalog_ready 0\n"));
        assert!(metrics.contains("pgshard_pooler_catalog_phase_info{phase=\"backoff\"} 1\n"));
        assert!(metrics.contains("pgshard_pooler_catalog_readiness_info{reason=\"stale\"} 1\n"));
        assert!(metrics.contains(&format!("pgshard_pooler_catalog_epoch {}\n", u64::MAX)));
        assert!(
            metrics.contains("pgshard_pooler_catalog_last_failure_info{kind=\"connection\"} 1\n")
        );
    }

    #[tokio::test]
    async fn catalog_ready_control_process_remains_application_unready() {
        let control_only = state_with_data_plane(true, false);
        let readiness = request("/readyz", control_only.clone()).await;
        assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body(readiness).await,
            r#"{"ready":false,"reason":"data_plane_unavailable"}"#
        );

        let metrics = body(request("/metrics", control_only).await).await;
        assert!(metrics.contains("pgshard_pooler_ready 0\n"));
        assert!(metrics.contains("pgshard_pooler_catalog_ready 1\n"));
    }

    #[tokio::test]
    async fn bootstrap_mode_publishes_bounded_unconfigured_signals() {
        let bootstrap = PoolerState::bootstrap_unavailable();
        let readiness = request("/readyz", bootstrap.clone()).await;
        assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body(readiness).await,
            r#"{"ready":false,"reason":"catalog_not_configured"}"#
        );

        let metrics = body(request("/metrics", bootstrap).await).await;
        assert!(metrics.contains("pgshard_pooler_catalog_ready 0\n"));
        assert!(
            metrics.contains("pgshard_pooler_catalog_phase_info{phase=\"not_configured\"} 1\n")
        );
        assert!(metrics.contains(
            "pgshard_pooler_catalog_readiness_info{reason=\"catalog_not_configured\"} 1\n"
        ));
        assert!(metrics.contains("pgshard_pooler_catalog_connect_attempts_total 0\n"));
        assert!(metrics.contains("pgshard_pooler_catalog_last_failure_info{kind=\"none\"} 1\n"));
    }

    #[tokio::test]
    async fn retries_an_accept_error_then_serves_a_connection() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind retry test listener");
        let address = listener.local_addr().expect("retry test listener address");
        let attempts = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));
        let acceptor = ErrorOnceAcceptor {
            listener,
            attempts: Arc::clone(&attempts),
        };
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_acceptor_with_policy(
            acceptor,
            state(false),
            async move {
                let _ = shutdown_receiver.await;
            },
            test_policy(Some(Arc::clone(&accepted))),
        ));

        let connection = TcpStream::connect(address)
            .await
            .expect("connect after injected accept failure");
        tokio::time::timeout(Duration::from_secs(1), async {
            while accepted.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server retries and accepts a connection");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        shutdown_sender
            .send(())
            .expect("server retains retry-test shutdown receiver");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("retry-test server stops")
            .expect("retry-test server task")
            .expect("clean retry-test shutdown");
        drop(connection);
    }

    #[tokio::test]
    async fn shutdown_interrupts_accept_error_backoff() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let acceptor = AlwaysFailAcceptor {
            attempts: Arc::clone(&attempts),
        };
        let mut policy = test_policy(None);
        policy.accept_initial_retry_delay = Duration::from_secs(30);
        policy.accept_max_retry_delay = Duration::from_secs(30);
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_acceptor_with_policy(
            acceptor,
            state(false),
            async move {
                let _ = shutdown_receiver.await;
            },
            policy,
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while attempts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server reaches injected accept failure");

        shutdown_sender
            .send(())
            .expect("server retains backoff-test shutdown receiver");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("shutdown interrupts accept backoff")
            .expect("backoff-test server task")
            .expect("clean backoff-test shutdown");
    }

    #[tokio::test]
    async fn shutdown_aborts_a_held_partial_request_after_bounded_drain() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("test listener address");
        let accepted = Arc::new(AtomicUsize::new(0));
        let policy = test_policy(Some(Arc::clone(&accepted)));
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_listener_with_policy(
            listener,
            state(false),
            async move {
                let _ = shutdown_receiver.await;
            },
            policy,
        ));
        let connection = TcpStream::connect(address)
            .await
            .expect("connect partial request");
        let partial = b"GET /healthz HTTP/1.1\r\nHost:";
        let mut written = 0;
        while written < partial.len() {
            connection
                .writable()
                .await
                .expect("partial request socket writable");
            match connection.try_write(&partial[written..]) {
                Ok(0) => panic!("partial request socket closed while writing"),
                Ok(bytes) => written += bytes,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("write partial request: {error}"),
            }
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while accepted.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server accepts partial request");

        shutdown_sender
            .send(())
            .expect("server retains shutdown receiver");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server enforces bounded drain")
            .expect("server task")
            .expect("clean forced drain");
        drop(connection);
    }
}
