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
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, timeout};

use crate::domain::{AgentSnapshot, AgentState, PostgresProcessState};

const MAX_HTTP_CONNECTIONS: usize = 128;
const MAX_HTTP_HEADERS: usize = 32;
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1_024;
const HTTP_ACCEPT_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(10);
const HTTP_ACCEPT_MAX_RETRY_DELAY: Duration = Duration::from_secs(1);
const HTTP_ACCEPT_MAX_FAILURE_DURATION: Duration = Duration::from_secs(30);
const HTTP_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

trait TcpAcceptor: Send {
    async fn accept(&mut self) -> io::Result<TcpStream>;
}

impl TcpAcceptor for TcpListener {
    async fn accept(&mut self) -> io::Result<TcpStream> {
        TcpListener::accept(self).await.map(|(stream, _)| stream)
    }
}

#[derive(Clone, Copy)]
struct HttpServerPolicy {
    maximum_connections: usize,
    accept_initial_retry_delay: Duration,
    accept_max_retry_delay: Duration,
    accept_max_failure_duration: Duration,
}

const DEFAULT_HTTP_SERVER_POLICY: HttpServerPolicy = HttpServerPolicy {
    maximum_connections: MAX_HTTP_CONNECTIONS,
    accept_initial_retry_delay: HTTP_ACCEPT_INITIAL_RETRY_DELAY,
    accept_max_retry_delay: HTTP_ACCEPT_MAX_RETRY_DELAY,
    accept_max_failure_duration: HTTP_ACCEPT_MAX_FAILURE_DURATION,
};

struct AcceptBackoff {
    initial_delay: Duration,
    maximum_delay: Duration,
    maximum_failure_duration: Duration,
    next_delay: Duration,
    retry_at: Option<Instant>,
    failure_started_at: Option<Instant>,
    accept_started_at: Option<Instant>,
}

impl AcceptBackoff {
    fn new(
        initial_delay: Duration,
        maximum_delay: Duration,
        maximum_failure_duration: Duration,
    ) -> Self {
        let initial_delay = initial_delay.min(maximum_delay);
        Self {
            initial_delay,
            maximum_delay,
            maximum_failure_duration,
            next_delay: initial_delay,
            retry_at: None,
            failure_started_at: None,
            accept_started_at: None,
        }
    }

    async fn wait(&mut self) {
        if let Some(retry_at) = self.retry_at {
            tokio::time::sleep_until(retry_at).await;
            self.retry_at = None;
        }
    }

    fn failed(&mut self) -> Option<Duration> {
        let now = Instant::now();
        let started_at = self.failure_started_at.get_or_insert(now);
        let remaining = self
            .maximum_failure_duration
            .checked_sub(now.duration_since(*started_at))?;
        if remaining.is_zero() {
            return None;
        }

        let delay = self.next_delay.min(remaining);
        self.retry_at = Some(now + delay);
        self.next_delay = self.next_delay.saturating_mul(2).min(self.maximum_delay);
        Some(delay)
    }

    fn succeeded(&mut self) {
        self.next_delay = self.initial_delay;
        self.retry_at = None;
        self.failure_started_at = None;
        self.accept_started_at = None;
    }

    fn started_accept(&mut self) {
        self.accept_started_at.get_or_insert_with(Instant::now);
    }

    fn completed_accept(&mut self) {
        let Some(started_at) = self.accept_started_at.take() else {
            return;
        };
        if !self.maximum_delay.is_zero()
            && Instant::now().saturating_duration_since(started_at) >= self.maximum_delay
        {
            self.succeeded();
        }
    }
}

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
    listener: TcpListener,
    state: AgentState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    serve_acceptor_with_policy(listener, state, shutdown, DEFAULT_HTTP_SERVER_POLICY).await
}

async fn serve_acceptor_with_policy<A>(
    mut acceptor: A,
    state: AgentState,
    shutdown: impl Future<Output = ()> + Send + 'static,
    policy: HttpServerPolicy,
) -> io::Result<()>
where
    A: TcpAcceptor,
{
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .with_state(state);
    let mut accept_backoff = AcceptBackoff::new(
        policy.accept_initial_retry_delay,
        policy.accept_max_retry_delay,
        policy.accept_max_failure_duration,
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
            accepted = accept_with_retry(&mut acceptor, &mut accept_backoff),
                if connections.len() < policy.maximum_connections => {
                let stream = accepted?;
                connections.spawn(serve_connection(stream, app.clone()));
            }
        }
    }
    drain_connections(&mut connections).await
}

async fn accept_with_retry<A>(
    acceptor: &mut A,
    backoff: &mut AcceptBackoff,
) -> io::Result<TcpStream>
where
    A: TcpAcceptor,
{
    loop {
        backoff.wait().await;
        backoff.started_accept();
        let accepted = acceptor.accept().await;
        backoff.completed_accept();
        match accepted {
            Ok(stream) => {
                backoff.succeeded();
                return Ok(stream);
            }
            Err(error) if is_permanent_accept_error(&error) => {
                tracing::error!(%error, "agent HTTP listener is permanently unusable");
                return Err(error);
            }
            Err(error) if is_retryable_connection_accept_error(&error) => {
                // Linux can surface a pending connection's network error from
                // accept(2). It is not evidence that the listener is unusable
                // and therefore must not consume the listener-outage budget.
                tracing::debug!(%error, "transient agent HTTP connection accept error");
                tokio::task::yield_now().await;
            }
            Err(error) => {
                let Some(retry_delay) = backoff.failed() else {
                    tracing::error!(%error, "agent HTTP accept outage exceeded its retry budget");
                    return Err(error);
                };
                tracing::warn!(
                    %error,
                    retry_delay_milliseconds = retry_delay.as_millis(),
                    "agent HTTP accept failed; retrying"
                );
            }
        }
    }
}

fn is_permanent_accept_error(error: &io::Error) -> bool {
    [
        rustix::io::Errno::BADF,
        rustix::io::Errno::INVAL,
        rustix::io::Errno::NOTSOCK,
    ]
    .into_iter()
    .any(|errno| error.raw_os_error() == Some(errno.raw_os_error()))
}

fn is_retryable_connection_accept_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    ) || [
        rustix::io::Errno::NETDOWN,
        rustix::io::Errno::PROTO,
        rustix::io::Errno::NOPROTOOPT,
        rustix::io::Errno::HOSTDOWN,
        rustix::io::Errno::NONET,
        rustix::io::Errno::HOSTUNREACH,
        rustix::io::Errno::OPNOTSUPP,
        rustix::io::Errno::NETUNREACH,
    ]
    .into_iter()
    .any(|errno| error.raw_os_error() == Some(errno.raw_os_error()))
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
    let (postgres_process_up, postgres_replication_bootstrap, postgres_replication_standby) =
        postgres_process_metrics(snapshot.postgres_process);
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
            "pgshard_agent_postgres_process_up {postgres_process_up}\n",
            "# HELP pgshard_agent_postgres_replication_bootstrap Whether the postmaster is a non-serving physical-clone source.\n",
            "# TYPE pgshard_agent_postgres_replication_bootstrap gauge\n",
            "pgshard_agent_postgres_replication_bootstrap {postgres_replication_bootstrap}\n",
            "# HELP pgshard_agent_postgres_replication_standby Whether the postmaster is a non-serving physical-replication standby.\n",
            "# TYPE pgshard_agent_postgres_replication_standby gauge\n",
            "pgshard_agent_postgres_replication_standby {postgres_replication_standby}\n"
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
        postgres_replication_bootstrap = postgres_replication_bootstrap,
        postgres_replication_standby = postgres_replication_standby,
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

fn postgres_process_metrics(process: PostgresProcessState) -> (u8, u8, u8) {
    let process_up = u8::from(matches!(
        process,
        PostgresProcessState::StartingQuarantined
            | PostgresProcessState::RunningQuarantined
            | PostgresProcessState::StartingReplicationBootstrap
            | PostgresProcessState::RunningReplicationBootstrap
            | PostgresProcessState::StartingReplicationStandby
            | PostgresProcessState::RunningReplicationStandby
            | PostgresProcessState::Stopping
    ));
    let replication_bootstrap = u8::from(matches!(
        process,
        PostgresProcessState::StartingReplicationBootstrap
            | PostgresProcessState::RunningReplicationBootstrap
    ));
    let replication_standby = u8::from(matches!(
        process,
        PostgresProcessState::StartingReplicationStandby
            | PostgresProcessState::RunningReplicationStandby
    ));
    (process_up, replication_bootstrap, replication_standby)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::oneshot;

    use super::*;

    #[test]
    fn process_metrics_distinguish_replication_lifecycles_from_quarantine() {
        assert_eq!(
            postgres_process_metrics(PostgresProcessState::RunningQuarantined),
            (1, 0, 0)
        );
        assert_eq!(
            postgres_process_metrics(PostgresProcessState::StartingReplicationBootstrap),
            (1, 1, 0)
        );
        assert_eq!(
            postgres_process_metrics(PostgresProcessState::RunningReplicationBootstrap),
            (1, 1, 0)
        );
        assert_eq!(
            postgres_process_metrics(PostgresProcessState::StartingReplicationStandby),
            (1, 0, 1)
        );
        assert_eq!(
            postgres_process_metrics(PostgresProcessState::RunningReplicationStandby),
            (1, 0, 1)
        );
        assert_eq!(
            postgres_process_metrics(PostgresProcessState::Validated),
            (0, 0, 0)
        );
    }

    #[tokio::test]
    async fn metrics_publish_replication_standby_separately_from_bootstrap() {
        let state = AgentState::default();
        state.set_postgres_process(PostgresProcessState::RunningReplicationStandby);

        let response = metrics(State(state)).await.into_response();
        let body = axum::body::to_bytes(response.into_body(), 1_048_576)
            .await
            .expect("bounded metrics response");
        let body = String::from_utf8(body.to_vec()).expect("UTF-8 metrics response");

        assert!(body.contains("pgshard_agent_postgres_process_up 1\n"));
        assert!(body.contains("pgshard_agent_postgres_replication_bootstrap 0\n"));
        assert!(body.contains("pgshard_agent_postgres_replication_standby 1\n"));
    }

    struct ErrorOnceAcceptor {
        listener: TcpListener,
        attempts: Arc<AtomicUsize>,
    }

    impl TcpAcceptor for ErrorOnceAcceptor {
        async fn accept(&mut self) -> io::Result<TcpStream> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(io::Error::other("injected transient accept failure"))
            } else {
                self.listener.accept().await.map(|(stream, _)| stream)
            }
        }
    }

    struct FailingAcceptor {
        kind: io::ErrorKind,
        raw_os_error: Option<i32>,
        attempts: Arc<AtomicUsize>,
    }

    impl TcpAcceptor for FailingAcceptor {
        async fn accept(&mut self) -> io::Result<TcpStream> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(self
                .raw_os_error
                .map_or_else(|| io::Error::from(self.kind), io::Error::from_raw_os_error))
        }
    }

    struct RecoveringAcceptor {
        listener: TcpListener,
        attempts: Arc<AtomicUsize>,
        first_raw_os_error: Option<i32>,
        second_error_delay: Option<Duration>,
    }

    impl TcpAcceptor for RecoveringAcceptor {
        async fn accept(&mut self) -> io::Result<TcpStream> {
            match self.attempts.fetch_add(1, Ordering::SeqCst) {
                0 => Err(self.first_raw_os_error.map_or_else(
                    || io::Error::other("injected isolated accept failure"),
                    io::Error::from_raw_os_error,
                )),
                1 if self.second_error_delay.is_some() => {
                    tokio::time::sleep(self.second_error_delay.expect("guarded test delay")).await;
                    Err(io::Error::other("injected later accept failure"))
                }
                _ => self.listener.accept().await.map(|(stream, _)| stream),
            }
        }
    }

    struct CancelledAcceptAcceptor {
        attempts: AtomicUsize,
    }

    impl TcpAcceptor for CancelledAcceptAcceptor {
        async fn accept(&mut self) -> io::Result<TcpStream> {
            match self.attempts.fetch_add(1, Ordering::SeqCst) {
                0 => Err(io::Error::other("injected initial accept failure")),
                2 => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Err(io::Error::other("injected later accept failure"))
                }
                1 | 3.. => std::future::pending().await,
            }
        }
    }

    struct InterleavedAcceptErrorAcceptor {
        attempts: AtomicUsize,
    }

    impl TcpAcceptor for InterleavedAcceptErrorAcceptor {
        async fn accept(&mut self) -> io::Result<TcpStream> {
            match self.attempts.fetch_add(1, Ordering::SeqCst) {
                0 | 3 => Err(io::Error::other("injected listener resource failure")),
                1 | 2 => {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Err(io::Error::from_raw_os_error(
                        rustix::io::Errno::OPNOTSUPP.raw_os_error(),
                    ))
                }
                _ => std::future::pending().await,
            }
        }
    }

    fn test_policy() -> HttpServerPolicy {
        HttpServerPolicy {
            maximum_connections: 2,
            accept_initial_retry_delay: Duration::from_millis(1),
            accept_max_retry_delay: Duration::from_millis(2),
            accept_max_failure_duration: Duration::from_millis(50),
        }
    }

    #[tokio::test]
    async fn retries_one_transient_accept_failure_then_serves() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind retry test listener");
        let address = listener.local_addr().expect("retry test address");
        let attempts = Arc::new(AtomicUsize::new(0));
        let acceptor = ErrorOnceAcceptor {
            listener,
            attempts: Arc::clone(&attempts),
        };
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(serve_acceptor_with_policy(
            acceptor,
            AgentState::default(),
            async move {
                let _ = shutdown_receiver.await;
            },
            test_policy(),
        ));

        let mut stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(address))
            .await
            .expect("agent listener recovers")
            .expect("connect after transient failure");
        tokio::io::AsyncWriteExt::write_all(
            &mut stream,
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await
        .expect("send health request");
        let mut response = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut response)
            .await
            .expect("read health response");
        assert!(response.starts_with(b"HTTP/1.1 200"));
        assert!(attempts.load(Ordering::SeqCst) >= 2);

        shutdown_sender
            .send(())
            .expect("server retains shutdown receiver");
        server.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn permanent_accept_failure_is_immediately_terminal() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let result = serve_acceptor_with_policy(
            FailingAcceptor {
                kind: io::ErrorKind::Other,
                raw_os_error: Some(rustix::io::Errno::BADF.raw_os_error()),
                attempts: Arc::clone(&attempts),
            },
            AgentState::default(),
            std::future::pending(),
            test_policy(),
        )
        .await;

        assert_eq!(
            result.expect_err("permanent failure").raw_os_error(),
            Some(rustix::io::Errno::BADF.raw_os_error())
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exhausted_accept_failure_is_immediately_terminal() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let result = serve_acceptor_with_policy(
            FailingAcceptor {
                kind: io::ErrorKind::Other,
                raw_os_error: None,
                attempts: Arc::clone(&attempts),
            },
            AgentState::default(),
            std::future::pending(),
            HttpServerPolicy {
                accept_max_failure_duration: Duration::ZERO,
                ..test_policy()
            },
        )
        .await;

        assert_eq!(
            result.expect_err("exhausted failure budget").kind(),
            io::ErrorKind::Other
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pending_network_error_does_not_consume_listener_failure_budget() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind network-error test listener");
        let address = listener.local_addr().expect("network-error test address");
        let attempts = Arc::new(AtomicUsize::new(0));
        let acceptor = RecoveringAcceptor {
            listener,
            attempts: Arc::clone(&attempts),
            first_raw_os_error: Some(rustix::io::Errno::OPNOTSUPP.raw_os_error()),
            second_error_delay: None,
        };
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(serve_acceptor_with_policy(
            acceptor,
            AgentState::default(),
            async move {
                let _ = shutdown_receiver.await;
            },
            HttpServerPolicy {
                accept_max_failure_duration: Duration::ZERO,
                ..test_policy()
            },
        ));

        let stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(address))
            .await
            .expect("network error remains recoverable")
            .expect("connect after pending network error");
        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), async {
            while attempts.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("listener retries pending network error");
        shutdown_sender
            .send(())
            .expect("server retains shutdown receiver");
        server.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn quiet_accept_interval_resets_listener_failure_streak() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind spaced-error test listener");
        let address = listener.local_addr().expect("spaced-error test address");
        let attempts = Arc::new(AtomicUsize::new(0));
        let acceptor = RecoveringAcceptor {
            listener,
            attempts: Arc::clone(&attempts),
            first_raw_os_error: None,
            second_error_delay: Some(Duration::from_millis(10)),
        };
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(serve_acceptor_with_policy(
            acceptor,
            AgentState::default(),
            async move {
                let _ = shutdown_receiver.await;
            },
            HttpServerPolicy {
                accept_max_failure_duration: Duration::from_millis(5),
                ..test_policy()
            },
        ));

        let stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(address))
            .await
            .expect("isolated failures remain recoverable")
            .expect("connect after spaced failures");
        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), async {
            while attempts.load(Ordering::SeqCst) < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("listener accepts after spaced failures");
        shutdown_sender
            .send(())
            .expect("server retains shutdown receiver");
        server.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn cancelled_pending_accept_retains_its_quiet_interval() {
        let mut acceptor = CancelledAcceptAcceptor {
            attempts: AtomicUsize::new(0),
        };
        let mut backoff = AcceptBackoff::new(
            Duration::from_millis(1),
            Duration::from_millis(150),
            Duration::from_millis(180),
        );

        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                accept_with_retry(&mut acceptor, &mut backoff),
            )
            .await
            .is_err()
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(250),
                accept_with_retry(&mut acceptor, &mut backoff),
            )
            .await
            .is_err(),
            "a cancelled accept lost quiet time and exhausted the stale failure streak"
        );
        assert_eq!(acceptor.attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn pending_network_errors_do_not_clear_listener_failure_streak() {
        let mut acceptor = InterleavedAcceptErrorAcceptor {
            attempts: AtomicUsize::new(0),
        };
        let mut backoff = AcceptBackoff::new(
            Duration::from_millis(1),
            Duration::from_millis(150),
            Duration::from_millis(150),
        );
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            accept_with_retry(&mut acceptor, &mut backoff),
        )
        .await
        .expect("interleaved error test remains bounded")
        .expect_err("listener resource failure streak must remain active");

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(acceptor.attempts.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn classifies_linux_pending_network_errors_as_retryable() {
        for errno in [
            rustix::io::Errno::NETDOWN,
            rustix::io::Errno::PROTO,
            rustix::io::Errno::NOPROTOOPT,
            rustix::io::Errno::HOSTDOWN,
            rustix::io::Errno::NONET,
            rustix::io::Errno::HOSTUNREACH,
            rustix::io::Errno::OPNOTSUPP,
            rustix::io::Errno::NETUNREACH,
        ] {
            let error = io::Error::from_raw_os_error(errno.raw_os_error());
            assert!(is_retryable_connection_accept_error(&error));
            assert!(!is_permanent_accept_error(&error));
        }
    }

    #[tokio::test]
    async fn shutdown_interrupts_accept_retry_backoff() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(serve_acceptor_with_policy(
            FailingAcceptor {
                kind: io::ErrorKind::Other,
                raw_os_error: None,
                attempts: Arc::clone(&attempts),
            },
            AgentState::default(),
            async move {
                let _ = shutdown_receiver.await;
            },
            HttpServerPolicy {
                accept_initial_retry_delay: Duration::from_secs(30),
                accept_max_retry_delay: Duration::from_secs(30),
                accept_max_failure_duration: Duration::from_mins(1),
                ..test_policy()
            },
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            while attempts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server enters injected accept failure");
        shutdown_sender
            .send(())
            .expect("server retains shutdown receiver");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("shutdown interrupts retry wait")
            .expect("server task")
            .expect("clean shutdown");
    }
}
