//! Long-running notification and polling driver for the catalog cache.

use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{Instant, MissedTickBehavior, sleep_until};
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
/// Shortest accepted deadline for one subscription or authoritative load.
pub const MIN_CATALOG_OPERATION_TIMEOUT: Duration = Duration::from_millis(100);
/// Longest accepted deadline for one subscription or authoritative load.
pub const MAX_CATALOG_OPERATION_TIMEOUT: Duration = Duration::from_mins(5);
/// Default deadline for one subscription or authoritative load.
pub const DEFAULT_CATALOG_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Bounded deadline for one catalog subscription or authoritative load.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogOperationTimeout(Duration);

impl CatalogOperationTimeout {
    /// Validates an operation timeout between 100 milliseconds and five minutes.
    ///
    /// # Errors
    ///
    /// Rejects a timeout outside the bounded range.
    pub fn new(timeout: Duration) -> Result<Self, CatalogOperationTimeoutError> {
        if !(MIN_CATALOG_OPERATION_TIMEOUT..=MAX_CATALOG_OPERATION_TIMEOUT).contains(&timeout) {
            return Err(CatalogOperationTimeoutError { timeout });
        }
        Ok(Self(timeout))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for CatalogOperationTimeout {
    fn default() -> Self {
        Self(DEFAULT_CATALOG_OPERATION_TIMEOUT)
    }
}

/// Invalid catalog operation timeout.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error(
    "catalog operation timeout {timeout:?} must be between {MIN_CATALOG_OPERATION_TIMEOUT:?} and {MAX_CATALOG_OPERATION_TIMEOUT:?}"
)]
pub struct CatalogOperationTimeoutError {
    timeout: Duration,
}

impl CatalogOperationTimeoutError {
    /// Returns the rejected timeout.
    #[must_use]
    pub const fn timeout(self) -> Duration {
        self.timeout
    }
}

/// Authoritative catalog operation protected by a deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogOperation {
    /// Session reset, committed `LISTEN`, and the initial authoritative load.
    InitialLoad,
    /// A periodic or notification-triggered authoritative refresh.
    Refresh,
}

impl CatalogOperation {
    /// Returns the stable bounded label used in errors and telemetry.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InitialLoad => "initial_load",
            Self::Refresh => "refresh",
        }
    }
}

impl fmt::Display for CatalogOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
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
/// Direct callers remain responsible for TLS, authentication, connection
/// timeouts, retry backoff, and supervising this future. `operation_timeout`
/// bounds the committed subscription plus initial load and every later load.
/// [`crate::CatalogSupervisor`] provides the standard reconnect and readiness
/// policy. `shutdown` is observed during subscription, between refreshes, and
/// while a refresh query is pending. Dropping this future aborts its connection
/// pump rather than detaching the socket task.
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
    operation_timeout: CatalogOperationTimeout,
    shutdown: F,
) -> Result<(), CatalogRefreshError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Future<Output = ()>,
{
    run_catalog_refresh_observed(
        client,
        connection,
        cache,
        poll_interval,
        operation_timeout,
        || {},
        shutdown,
    )
    .await
}

pub(crate) async fn run_catalog_refresh_observed<S, T, F, R>(
    client: Client,
    connection: Connection<S, T>,
    cache: Arc<CatalogCache>,
    poll_interval: CatalogPollInterval,
    operation_timeout: CatalogOperationTimeout,
    refreshed: R,
    shutdown: F,
) -> Result<(), CatalogRefreshError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Future<Output = ()>,
    R: Fn() + Send + Sync,
{
    let (sender, receiver) = watch::channel(None);
    let connection_task =
        ConnectionTask::new(tokio::spawn(forward_notifications(connection, sender)));
    tokio::pin!(shutdown);
    let subscription = {
        let deadline = Instant::now() + operation_timeout.get();
        let subscription = CatalogReader::subscribe_before(client, &cache, deadline);
        tokio::pin!(subscription);
        let deadline_timer = sleep_until(deadline);
        tokio::pin!(deadline_timer);
        tokio::select! {
            biased;
            () = shutdown.as_mut() => return stop_connection(connection_task).await,
            () = deadline_timer.as_mut() => TimedOperation::Elapsed,
            result = subscription.as_mut() => {
                if Instant::now() >= deadline
                    || result
                        .as_ref()
                        .is_err_and(LoadError::is_operation_timeout)
                {
                    TimedOperation::Elapsed
                } else {
                    TimedOperation::Completed(result)
                }
            }
        }
    };
    let (reader, _) = match subscription {
        TimedOperation::Completed(Ok(reader)) => reader,
        TimedOperation::Completed(Err(source)) => {
            return Err(cleanup_after_load_error(connection_task, source).await);
        }
        TimedOperation::Elapsed => {
            return Err(cleanup_after_driver_error(
                connection_task,
                CatalogRefreshError::OperationTimeout {
                    operation: CatalogOperation::InitialLoad,
                    timeout: operation_timeout.get(),
                },
            )
            .await);
        }
    };
    refreshed();

    match refresh_loop(
        reader,
        receiver,
        &cache,
        poll_interval,
        operation_timeout,
        &refreshed,
        shutdown.as_mut(),
    )
    .await
    {
        Ok(RefreshLoopExit::Shutdown) => stop_connection(connection_task).await,
        Ok(RefreshLoopExit::ConnectionClosed) => finish_connection(connection_task).await,
        Err(RefreshLoopError::Load(source)) => {
            Err(cleanup_after_load_error(connection_task, source).await)
        }
        Err(RefreshLoopError::OperationTimeout) => Err(cleanup_after_driver_error(
            connection_task,
            CatalogRefreshError::OperationTimeout {
                operation: CatalogOperation::Refresh,
                timeout: operation_timeout.get(),
            },
        )
        .await),
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

enum TimedOperation<T> {
    Completed(T),
    Elapsed,
}

enum RefreshLoopError {
    Load(LoadError),
    OperationTimeout,
}

impl From<LoadError> for RefreshLoopError {
    fn from(error: LoadError) -> Self {
        Self::Load(error)
    }
}

async fn refresh_loop<F, R>(
    mut reader: CatalogReader,
    mut notifications: watch::Receiver<Option<CatalogNotification>>,
    cache: &CatalogCache,
    poll_interval: CatalogPollInterval,
    operation_timeout: CatalogOperationTimeout,
    refreshed: &R,
    mut shutdown: Pin<&mut F>,
) -> Result<RefreshLoopExit, RefreshLoopError>
where
    F: Future<Output = ()>,
    R: Fn(),
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
        let deadline = Instant::now() + operation_timeout.get();
        let refresh = reader.refresh_before(cache, deadline);
        tokio::pin!(refresh);
        let deadline_timer = sleep_until(deadline);
        tokio::pin!(deadline_timer);
        tokio::select! {
            biased;
            () = shutdown.as_mut() => return Ok(RefreshLoopExit::Shutdown),
            () = deadline_timer.as_mut() => return Err(RefreshLoopError::OperationTimeout),
            result = refresh.as_mut() => {
                if Instant::now() >= deadline
                    || result
                        .as_ref()
                        .is_err_and(LoadError::is_operation_timeout)
                {
                    return Err(RefreshLoopError::OperationTimeout);
                }
                result?;
                refreshed();
            }
        }
    }
}

async fn cleanup_after_load_error(
    connection_task: ConnectionTask,
    load: LoadError,
) -> CatalogRefreshError {
    cleanup_after_driver_error(connection_task, CatalogRefreshError::Load(load)).await
}

async fn cleanup_after_driver_error(
    connection_task: ConnectionTask,
    error: CatalogRefreshError,
) -> CatalogRefreshError {
    let connection_task = connection_task.into_handle();
    connection_task.abort();
    let _ = connection_task.await;
    error
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
    /// A subscription, initial load, or refresh exceeded its configured deadline.
    #[error("catalog {operation} exceeded configured operation timeout {timeout:?}")]
    OperationTimeout {
        /// Operation that exceeded its deadline.
        operation: CatalogOperation,
        /// Configured deadline.
        timeout: Duration,
    },
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

    #[test]
    fn operation_timeout_is_bounded() {
        assert_eq!(
            CatalogOperationTimeout::default().get(),
            DEFAULT_CATALOG_OPERATION_TIMEOUT
        );
        assert_eq!(
            CatalogOperationTimeout::new(MIN_CATALOG_OPERATION_TIMEOUT)
                .expect("minimum operation timeout")
                .get(),
            MIN_CATALOG_OPERATION_TIMEOUT
        );
        assert_eq!(
            CatalogOperationTimeout::new(MAX_CATALOG_OPERATION_TIMEOUT)
                .expect("maximum operation timeout")
                .get(),
            MAX_CATALOG_OPERATION_TIMEOUT
        );
        for rejected in [
            MIN_CATALOG_OPERATION_TIMEOUT
                .checked_sub(Duration::from_nanos(1))
                .expect("minimum timeout exceeds one nanosecond"),
            MAX_CATALOG_OPERATION_TIMEOUT + Duration::from_nanos(1),
        ] {
            let error = CatalogOperationTimeout::new(rejected).expect_err("invalid timeout");
            assert_eq!(error.timeout(), rejected);
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
