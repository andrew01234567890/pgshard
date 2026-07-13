//! Long-running notification and polling driver for the catalog cache.

use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_postgres::{AsyncMessage, Client, Connection};

use crate::{
    CatalogCache, CatalogNotification, CatalogReader, LoadError, NOTIFY_CHANNEL, RefreshDecision,
};

/// Shortest accepted authoritative polling interval.
pub const MIN_CATALOG_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Longest accepted authoritative polling interval.
pub const MAX_CATALOG_POLL_INTERVAL: Duration = Duration::from_mins(5);
/// Default authoritative polling interval.
pub const DEFAULT_CATALOG_POLL_INTERVAL: Duration = Duration::from_secs(30);

const NOTIFICATION_QUEUE_CAPACITY: usize = 1;

/// Bounded interval between authoritative catalog reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogPollInterval(Duration);

impl CatalogPollInterval {
    /// Validates an interval between one second and five minutes, inclusive.
    ///
    /// # Errors
    ///
    /// Rejects an interval outside the bounded range.
    pub fn new(interval: Duration) -> Result<Self, CatalogPollIntervalError> {
        if !(MIN_CATALOG_POLL_INTERVAL..=MAX_CATALOG_POLL_INTERVAL).contains(&interval) {
            return Err(CatalogPollIntervalError { interval });
        }
        Ok(Self(interval))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for CatalogPollInterval {
    fn default() -> Self {
        Self(DEFAULT_CATALOG_POLL_INTERVAL)
    }
}

/// Invalid authoritative catalog polling interval.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error(
    "catalog poll interval {interval:?} must be between {MIN_CATALOG_POLL_INTERVAL:?} and {MAX_CATALOG_POLL_INTERVAL:?}"
)]
pub struct CatalogPollIntervalError {
    interval: Duration,
}

impl CatalogPollIntervalError {
    /// Returns the rejected interval.
    #[must_use]
    pub const fn interval(self) -> Duration {
        self.interval
    }
}

/// Runs one dedicated catalog connection until shutdown or failure.
///
/// Construction commits `LISTEN` before publishing the initial snapshot. A
/// one-item wakeup queue deliberately coalesces bursts and may drop notification
/// hints; the periodic repeatable-read refresh remains authoritative. Invalid
/// payloads and notifications from other channels are ignored for the same
/// reason. Connection closure is terminal so the owner can fail readiness and
/// create a fresh connection rather than silently running on polling alone.
///
/// The caller remains responsible for TLS, authentication, connect and query
/// timeouts, retry backoff, and supervising this future. `shutdown` is observed
/// between refreshes and while a refresh query is pending.
///
/// # Errors
///
/// Returns an error when subscription, snapshot loading, cache publication, the
/// connection pump, or its task fails, or when the connection closes before
/// shutdown.
pub async fn run_catalog_refresh<S, T, F>(
    client: Client,
    connection: Connection<S, T>,
    cache: Arc<CatalogCache>,
    poll_interval: CatalogPollInterval,
    shutdown: F,
) -> Result<(), CatalogRefreshError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Future<Output = ()>,
{
    let (sender, receiver) = mpsc::channel(NOTIFICATION_QUEUE_CAPACITY);
    let connection_task = tokio::spawn(forward_notifications(connection, sender));
    let (reader, _) = match CatalogReader::subscribe(client, &cache).await {
        Ok(reader) => reader,
        Err(source) => {
            return Err(cleanup_after_load_error(connection_task, source).await);
        }
    };

    match refresh_loop(reader, receiver, &cache, poll_interval, shutdown).await {
        Ok(RefreshLoopExit::Shutdown) => stop_connection(connection_task).await,
        Ok(RefreshLoopExit::ConnectionClosed) => match connection_task.await {
            Ok(Ok(())) => Err(CatalogRefreshError::ConnectionClosed),
            Ok(Err(source)) => Err(CatalogRefreshError::Connection(source)),
            Err(source) => Err(CatalogRefreshError::ConnectionTask(source)),
        },
        Err(source) => Err(cleanup_after_load_error(connection_task, source).await),
    }
}

async fn forward_notifications<S, T>(
    mut connection: Connection<S, T>,
    sender: mpsc::Sender<CatalogNotification>,
) -> Result<(), tokio_postgres::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(message) = poll_fn(|context| connection.poll_message(context)).await {
        let message = message?;
        let AsyncMessage::Notification(notification) = message else {
            continue;
        };
        if notification.channel() != NOTIFY_CHANNEL {
            continue;
        }
        let Ok(notification) = CatalogNotification::parse(notification.payload()) else {
            continue;
        };
        match sender.try_send(notification) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
        }
    }
    Ok(())
}

enum RefreshLoopExit {
    Shutdown,
    ConnectionClosed,
}

async fn refresh_loop<F>(
    mut reader: CatalogReader,
    mut notifications: mpsc::Receiver<CatalogNotification>,
    cache: &CatalogCache,
    poll_interval: CatalogPollInterval,
    shutdown: F,
) -> Result<RefreshLoopExit, LoadError>
where
    F: Future<Output = ()>,
{
    let interval = poll_interval.get();
    let mut poll = tokio::time::interval_at(Instant::now() + interval, interval);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tokio::pin!(shutdown);

    loop {
        let should_refresh = tokio::select! {
            biased;
            () = &mut shutdown => return Ok(RefreshLoopExit::Shutdown),
            _ = poll.tick() => true,
            notification = notifications.recv() => {
                let Some(notification) = notification else {
                    return Ok(RefreshLoopExit::ConnectionClosed);
                };
                cache.refresh_decision(notification) == RefreshDecision::Refresh
            }
        };
        if !should_refresh {
            continue;
        }
        tokio::select! {
            biased;
            () = &mut shutdown => return Ok(RefreshLoopExit::Shutdown),
            result = reader.refresh(cache) => {
                result?;
            }
        }
    }
}

async fn cleanup_after_load_error(
    connection_task: JoinHandle<Result<(), tokio_postgres::Error>>,
    load: LoadError,
) -> CatalogRefreshError {
    connection_task.abort();
    match connection_task.await {
        Ok(Ok(())) => CatalogRefreshError::ConnectionClosed,
        Ok(Err(source)) => CatalogRefreshError::Connection(source),
        Err(source) if source.is_panic() => CatalogRefreshError::ConnectionTask(source),
        _ => CatalogRefreshError::Load(load),
    }
}

async fn stop_connection(
    connection_task: JoinHandle<Result<(), tokio_postgres::Error>>,
) -> Result<(), CatalogRefreshError> {
    connection_task.abort();
    match connection_task.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(CatalogRefreshError::Connection(source)),
        Err(source) if source.is_cancelled() => Ok(()),
        Err(source) => Err(CatalogRefreshError::ConnectionTask(source)),
    }
}

/// Long-running catalog refresh failure.
#[derive(Debug, Error)]
pub enum CatalogRefreshError {
    /// Subscription, authoritative loading, or cache publication failed.
    #[error(transparent)]
    Load(#[from] LoadError),
    /// The dedicated connection closed without an error before shutdown.
    #[error("catalog notification connection closed before shutdown")]
    ConnectionClosed,
    /// The dedicated connection failed.
    #[error("catalog notification connection failed: {0}")]
    Connection(#[source] tokio_postgres::Error),
    /// The connection pump task panicked.
    #[error("catalog notification connection task failed: {0}")]
    ConnectionTask(#[source] JoinError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_interval_is_bounded() {
        assert_eq!(
            CatalogPollInterval::default().get(),
            Duration::from_secs(30)
        );
        assert_eq!(
            CatalogPollInterval::new(MIN_CATALOG_POLL_INTERVAL)
                .expect("minimum interval")
                .get(),
            MIN_CATALOG_POLL_INTERVAL
        );
        assert_eq!(
            CatalogPollInterval::new(MAX_CATALOG_POLL_INTERVAL)
                .expect("maximum interval")
                .get(),
            MAX_CATALOG_POLL_INTERVAL
        );
        for rejected in [
            Duration::ZERO,
            MIN_CATALOG_POLL_INTERVAL
                .checked_sub(Duration::from_nanos(1))
                .expect("minimum interval exceeds one nanosecond"),
            MAX_CATALOG_POLL_INTERVAL + Duration::from_nanos(1),
        ] {
            let error = CatalogPollInterval::new(rejected).expect_err("invalid interval");
            assert_eq!(error.interval(), rejected);
        }
    }
}
