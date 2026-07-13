//! Long-running notification and polling driver for the catalog cache.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
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
/// single latest-wins wakeup slot deliberately coalesces bursts; the periodic
/// repeatable-read refresh remains authoritative. Invalid payloads and
/// notifications from other channels are ignored for the same reason.
/// Connection closure is terminal so the owner can fail readiness and create a
/// fresh connection rather than silently running on polling alone.
///
/// The caller remains responsible for TLS, authentication, connect and query
/// timeouts, retry backoff, and supervising this future. `shutdown` is observed
/// during subscription, between refreshes, and while a refresh query is
/// pending. Dropping this future aborts its connection pump rather than
/// detaching the socket task.
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
    let (sender, receiver) = watch::channel(None);
    let connection_task =
        ConnectionTask::new(tokio::spawn(forward_notifications(connection, sender)));
    tokio::pin!(shutdown);
    let subscription = {
        let subscription = CatalogReader::subscribe(client, &cache);
        tokio::pin!(subscription);
        tokio::select! {
            biased;
            () = shutdown.as_mut() => return stop_connection(connection_task).await,
            result = subscription.as_mut() => result,
        }
    };
    let (reader, _) = match subscription {
        Ok(reader) => reader,
        Err(source) => {
            return Err(cleanup_after_load_error(connection_task, source).await);
        }
    };

    match refresh_loop(reader, receiver, &cache, poll_interval, shutdown.as_mut()).await {
        Ok(RefreshLoopExit::Shutdown) => stop_connection(connection_task).await,
        Ok(RefreshLoopExit::ConnectionClosed) => finish_connection(connection_task).await,
        Err(source) => Err(cleanup_after_load_error(connection_task, source).await),
    }
}

struct ConnectionTask {
    handle: Option<JoinHandle<Result<(), tokio_postgres::Error>>>,
}

impl ConnectionTask {
    fn new(handle: JoinHandle<Result<(), tokio_postgres::Error>>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn into_handle(mut self) -> JoinHandle<Result<(), tokio_postgres::Error>> {
        self.handle
            .take()
            .expect("connection task handle is consumed exactly once")
    }
}

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

async fn forward_notifications<S, T>(
    mut connection: Connection<S, T>,
    sender: watch::Sender<Option<CatalogNotification>>,
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
        publish_notification(&sender, notification);
    }
    Ok(())
}

fn publish_notification(
    sender: &watch::Sender<Option<CatalogNotification>>,
    notification: CatalogNotification,
) {
    let _ = sender.send_replace(Some(notification));
}

enum RefreshLoopExit {
    Shutdown,
    ConnectionClosed,
}

async fn refresh_loop<F>(
    mut reader: CatalogReader,
    mut notifications: watch::Receiver<Option<CatalogNotification>>,
    cache: &CatalogCache,
    poll_interval: CatalogPollInterval,
    mut shutdown: Pin<&mut F>,
) -> Result<RefreshLoopExit, LoadError>
where
    F: Future<Output = ()>,
{
    let interval = poll_interval.get();
    let mut poll = tokio::time::interval_at(Instant::now() + interval, interval);
    poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        let should_refresh = tokio::select! {
            biased;
            () = shutdown.as_mut() => return Ok(RefreshLoopExit::Shutdown),
            _ = poll.tick() => true,
            notification = notifications.changed() => {
                if notification.is_err() {
                    return Ok(RefreshLoopExit::ConnectionClosed);
                }
                let notification = (*notifications.borrow_and_update())
                    .expect("a notification change always publishes an epoch");
                cache.refresh_decision(notification) == RefreshDecision::Refresh
            }
        };
        if !should_refresh {
            continue;
        }
        tokio::select! {
            biased;
            () = shutdown.as_mut() => return Ok(RefreshLoopExit::Shutdown),
            result = reader.refresh(cache) => {
                result?;
            }
        }
    }
}

async fn cleanup_after_load_error(
    connection_task: ConnectionTask,
    load: LoadError,
) -> CatalogRefreshError {
    let connection_task = connection_task.into_handle();
    connection_task.abort();
    let _ = connection_task.await;
    CatalogRefreshError::Load(load)
}

async fn stop_connection(connection_task: ConnectionTask) -> Result<(), CatalogRefreshError> {
    let connection_task = connection_task.into_handle();
    connection_task.abort();
    match connection_task.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(CatalogRefreshError::Connection(source)),
        Err(source) if source.is_cancelled() => Ok(()),
        Err(source) => Err(CatalogRefreshError::ConnectionTask(source)),
    }
}

async fn finish_connection(connection_task: ConnectionTask) -> Result<(), CatalogRefreshError> {
    match connection_task.into_handle().await {
        Ok(Ok(())) => Err(CatalogRefreshError::ConnectionClosed),
        Ok(Err(source)) => Err(CatalogRefreshError::Connection(source)),
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
    use std::future::pending;

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

    #[tokio::test]
    async fn load_error_precedes_clean_connection_exit() {
        let handle = tokio::spawn(async { Ok::<(), tokio_postgres::Error>(()) });
        tokio::task::yield_now().await;
        assert!(handle.is_finished(), "connection task did not finish");

        let error =
            cleanup_after_load_error(ConnectionTask::new(handle), LoadError::MissingSingleton)
                .await;
        assert!(matches!(
            error,
            CatalogRefreshError::Load(LoadError::MissingSingleton)
        ));
    }

    #[tokio::test]
    async fn load_error_precedes_connection_task_panic() {
        let handle: JoinHandle<Result<(), tokio_postgres::Error>> =
            tokio::spawn(async { panic!("connection task test panic") });
        tokio::task::yield_now().await;
        assert!(handle.is_finished(), "connection task did not finish");

        let error =
            cleanup_after_load_error(ConnectionTask::new(handle), LoadError::MissingSingleton)
                .await;
        assert!(matches!(
            error,
            CatalogRefreshError::Load(LoadError::MissingSingleton)
        ));
    }

    #[tokio::test]
    async fn dropping_connection_task_aborts_the_child() {
        struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let (dropped_sender, dropped_receiver) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(dropped_sender));
            let _ = started_sender.send(());
            pending::<Result<(), tokio_postgres::Error>>().await
        });
        started_receiver.await.expect("child task started");

        drop(ConnectionTask::new(handle));
        tokio::time::timeout(Duration::from_secs(1), dropped_receiver)
            .await
            .expect("aborted child dropped within one second")
            .expect("child retained its drop signal");
    }

    #[tokio::test]
    async fn notification_slot_retains_the_latest_epoch() {
        let (sender, mut receiver) = watch::channel(None);
        let older = CatalogNotification::parse("2").expect("older epoch");
        let newer = CatalogNotification::parse("3").expect("newer epoch");

        publish_notification(&sender, older);
        publish_notification(&sender, newer);
        receiver.changed().await.expect("notification published");

        assert_eq!(*receiver.borrow_and_update(), Some(newer));
    }
}
