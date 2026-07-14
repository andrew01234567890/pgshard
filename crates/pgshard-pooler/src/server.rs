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
    next_delay: Duration,
    retry_at: Option<Instant>,
}

impl AcceptBackoff {
    /// Creates a capped exponential accept backoff.
    pub(crate) fn new(initial_delay: Duration, maximum_delay: Duration) -> Self {
        let initial_delay = initial_delay.min(maximum_delay);
        Self {
            initial_delay,
            maximum_delay,
            next_delay: initial_delay,
            retry_at: None,
        }
    }

    async fn wait(&mut self) {
        if let Some(retry_at) = self.retry_at {
            tokio::time::sleep_until(retry_at).await;
            self.retry_at = None;
        }
    }

    fn failed(&mut self) -> Duration {
        let delay = self.next_delay;
        self.retry_at = Some(Instant::now() + delay);
        self.next_delay = next_accept_retry_delay(delay, self.maximum_delay);
        delay
    }

    fn succeeded(&mut self) {
        self.next_delay = self.initial_delay;
        self.retry_at = None;
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
        match acceptor.accept().await {
            Ok(stream) => {
                backoff.succeeded();
                return Ok((stream, permit));
            }
            Err(error) if is_connection_accept_error(&error) => {
                tracing::debug!(server, %error, "transient TCP accept error");
                tokio::task::yield_now().await;
            }
            Err(error) => {
                let retry_delay = backoff.failed();
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

fn is_connection_accept_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
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
    use super::*;

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
        let mut backoff = AcceptBackoff::new(Duration::from_secs(30), Duration::from_mins(1));
        assert_eq!(backoff.failed(), Duration::from_secs(30));
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
    }
}
