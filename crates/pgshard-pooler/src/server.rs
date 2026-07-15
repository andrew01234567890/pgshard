//! Shared bounded TCP accept and drain mechanics.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;

/// Source of accepted TCP streams, abstracted for deterministic failure tests.
pub(crate) trait TcpAcceptor: Send {
    /// Accepts one stream.
    async fn accept(&mut self) -> io::Result<TcpStream>;
}

impl TcpAcceptor for TcpListener {
    async fn accept(&mut self) -> io::Result<TcpStream> {
        TcpListener::accept(self).await.map(|(stream, _)| stream)
    }
}

/// Retry state retained by a server even when its current accept future is
/// cancelled by another branch of the supervision loop.
pub(crate) struct AcceptBackoff {
    initial_delay: Duration,
    maximum_delay: Duration,
    maximum_failure_duration: Duration,
    next_delay: Duration,
    retry_at: Option<Instant>,
    failure_started_at: Option<Instant>,
    accept_started_at: Option<Instant>,
}

impl AcceptBackoff {
    /// Creates a capped exponential accept backoff.
    pub(crate) fn new(
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
        self.next_delay = next_accept_retry_delay(delay, self.maximum_delay);
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

/// Acquires one connection permit and retries accept failures with bounded backoff.
pub(crate) async fn accept_bounded<A>(
    acceptor: &mut A,
    permits: Arc<Semaphore>,
    backoff: &mut AcceptBackoff,
    server: &'static str,
) -> io::Result<(TcpStream, OwnedSemaphorePermit)>
where
    A: TcpAcceptor,
{
    let permit = permits.acquire_owned().await.map_err(io::Error::other)?;
    loop {
        backoff.wait().await;
        backoff.started_accept();
        let accepted = acceptor.accept().await;
        backoff.completed_accept();
        match accepted {
            Ok(stream) => {
                backoff.succeeded();
                return Ok((stream, permit));
            }
            Err(error) if is_permanent_accept_error(&error) => {
                tracing::error!(server, %error, "TCP listener is permanently unusable");
                return Err(error);
            }
            Err(error) if is_retryable_connection_accept_error(&error) => {
                // Linux can surface a pending connection's network error from
                // accept(2). It is not evidence that the listener is unusable
                // and therefore must not consume the listener-outage budget.
                tracing::debug!(server, %error, "transient TCP connection accept error");
                tokio::task::yield_now().await;
            }
            Err(error) => {
                let Some(retry_delay) = backoff.failed() else {
                    tracing::error!(server, %error, "TCP accept outage exceeded its retry budget");
                    return Err(error);
                };
                tracing::warn!(
                    server,
                    %error,
                    retry_delay_milliseconds = retry_delay.as_millis(),
                    "TCP accept failed; retrying"
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

/// Doubles an accept retry delay without crossing its ceiling.
pub(crate) fn next_accept_retry_delay(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

/// Drains connection tasks, then aborts anything past the hard deadline.
pub(crate) async fn drain_connections(
    connections: &mut JoinSet<()>,
    shutdown_timeout: Duration,
) -> io::Result<()> {
    let drained = tokio::time::timeout(shutdown_timeout, async {
        while let Some(result) = connections.join_next().await {
            connection_task_result(result)?;
        }
        Ok(())
    })
    .await;
    if let Ok(result) = drained {
        return result;
    }

    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        match result {
            Err(error) if error.is_cancelled() => {}
            result => connection_task_result(result)?,
        }
    }
    Ok(())
}

/// Converts a connection-task panic into an I/O failure.
pub(crate) fn connection_task_result(result: Result<(), JoinError>) -> io::Result<()> {
    result.map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct FailingAcceptor {
        kind: io::ErrorKind,
        raw_os_error: Option<i32>,
        attempts: AtomicUsize,
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
        attempts: AtomicUsize,
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

    #[test]
    fn accept_retry_delay_grows_to_but_not_past_its_bound() {
        let maximum = Duration::from_millis(25);
        assert_eq!(
            next_accept_retry_delay(Duration::from_millis(10), maximum),
            Duration::from_millis(20)
        );
        assert_eq!(
            next_accept_retry_delay(Duration::from_millis(20), maximum),
            maximum
        );
        assert_eq!(next_accept_retry_delay(maximum, maximum), maximum);
    }

    #[tokio::test]
    async fn cancelled_wait_retains_the_retry_deadline_and_growth() {
        let mut backoff = AcceptBackoff::new(
            Duration::from_secs(30),
            Duration::from_mins(1),
            Duration::from_mins(2),
        );
        assert_eq!(backoff.failed(), Some(Duration::from_secs(30)));
        let retry_at = backoff.retry_at;

        assert!(
            tokio::time::timeout(Duration::from_millis(1), backoff.wait())
                .await
                .is_err()
        );
        assert_eq!(backoff.retry_at, retry_at);
        assert_eq!(backoff.next_delay, Duration::from_mins(1));

        backoff.succeeded();
        assert_eq!(backoff.retry_at, None);
        assert_eq!(backoff.next_delay, Duration::from_secs(30));
        assert_eq!(backoff.failure_started_at, None);
    }

    #[test]
    fn accept_failure_budget_is_bounded_and_reset_by_success() {
        let maximum_failure_duration = Duration::from_secs(30);
        let mut backoff = AcceptBackoff::new(
            Duration::from_secs(1),
            Duration::from_secs(5),
            maximum_failure_duration,
        );
        assert_eq!(backoff.failed(), Some(Duration::from_secs(1)));
        backoff.retry_at = None;
        backoff.failure_started_at = Some(Instant::now() - maximum_failure_duration);
        assert_eq!(backoff.failed(), None);

        backoff.succeeded();
        assert_eq!(backoff.failed(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn classifies_unusable_listener_errors_as_permanent() {
        assert!(is_permanent_accept_error(&io::Error::from_raw_os_error(
            rustix::io::Errno::INVAL.raw_os_error()
        )));
        assert!(is_permanent_accept_error(&io::Error::from_raw_os_error(
            rustix::io::Errno::BADF.raw_os_error()
        )));
        assert!(!is_permanent_accept_error(&io::Error::from_raw_os_error(
            rustix::io::Errno::PERM.raw_os_error()
        )));
        assert!(!is_permanent_accept_error(&io::Error::other(
            "resource pressure"
        )));
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

    async fn accept_after_injected_errors(
        first_raw_os_error: Option<i32>,
        second_error_delay: Option<Duration>,
        maximum_failure_duration: Duration,
    ) -> usize {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind recovery test listener");
        let address = listener.local_addr().expect("recovery test address");
        let mut acceptor = RecoveringAcceptor {
            listener,
            attempts: AtomicUsize::new(0),
            first_raw_os_error,
            second_error_delay,
        };
        let connector = tokio::spawn(TcpStream::connect(address));
        let mut backoff = AcceptBackoff::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            maximum_failure_duration,
        );
        let (stream, permit) = tokio::time::timeout(
            Duration::from_secs(1),
            accept_bounded(
                &mut acceptor,
                Arc::new(Semaphore::new(1)),
                &mut backoff,
                "test",
            ),
        )
        .await
        .expect("listener recovery remains bounded")
        .expect("listener recovers");
        drop(stream);
        drop(permit);
        drop(
            connector
                .await
                .expect("connector task")
                .expect("test connection"),
        );
        acceptor.attempts.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn pending_network_error_does_not_consume_listener_failure_budget() {
        assert_eq!(
            accept_after_injected_errors(
                Some(rustix::io::Errno::OPNOTSUPP.raw_os_error()),
                None,
                Duration::ZERO,
            )
            .await,
            2
        );
    }

    #[tokio::test]
    async fn quiet_accept_interval_resets_listener_failure_streak() {
        assert_eq!(
            accept_after_injected_errors(
                None,
                Some(Duration::from_millis(10)),
                Duration::from_millis(5),
            )
            .await,
            3
        );
    }

    #[tokio::test]
    async fn cancelled_pending_accept_retains_its_quiet_interval() {
        let mut acceptor = CancelledAcceptAcceptor {
            attempts: AtomicUsize::new(0),
        };
        let permits = Arc::new(Semaphore::new(1));
        let mut backoff = AcceptBackoff::new(
            Duration::from_millis(1),
            Duration::from_millis(150),
            Duration::from_millis(180),
        );

        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                accept_bounded(&mut acceptor, Arc::clone(&permits), &mut backoff, "test",),
            )
            .await
            .is_err()
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(250),
                accept_bounded(&mut acceptor, permits, &mut backoff, "test"),
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
            accept_bounded(
                &mut acceptor,
                Arc::new(Semaphore::new(1)),
                &mut backoff,
                "test",
            ),
        )
        .await
        .expect("interleaved error test remains bounded")
        .expect_err("listener resource failure streak must remain active");

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(acceptor.attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn permanent_and_exhausted_accept_failures_are_terminal() {
        for (kind, raw_os_error, maximum_failure_duration) in [
            (
                io::ErrorKind::Other,
                Some(rustix::io::Errno::BADF.raw_os_error()),
                Duration::from_secs(30),
            ),
            (io::ErrorKind::Other, None, Duration::ZERO),
        ] {
            let mut acceptor = FailingAcceptor {
                kind,
                raw_os_error,
                attempts: AtomicUsize::new(0),
            };
            let mut backoff = AcceptBackoff::new(
                Duration::from_millis(1),
                Duration::from_millis(1),
                maximum_failure_duration,
            );
            let result = accept_bounded(
                &mut acceptor,
                Arc::new(Semaphore::new(1)),
                &mut backoff,
                "test",
            )
            .await;

            let error = result.expect_err("terminal accept error");
            assert_eq!(error.raw_os_error(), raw_os_error);
            if raw_os_error.is_none() {
                assert_eq!(error.kind(), kind);
            }
            assert_eq!(acceptor.attempts.load(Ordering::SeqCst), 1);
        }
    }
}
