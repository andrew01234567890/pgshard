//! Reconnection and bounded-staleness supervision for the catalog driver.

use std::collections::hash_map::RandomState;
use std::error::Error as StdError;
use std::future::Future;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use pgshard_types::CatalogEpoch;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Instant as TokioInstant, sleep_until};
use tokio_postgres::{Client, Connection};

use crate::driver::run_catalog_refresh_observed;
use crate::{CatalogCache, CatalogOperationTimeout, CatalogPollInterval, CatalogRefreshError};

/// Shortest accepted interval for serving from the last validated snapshot.
pub const MIN_CATALOG_STALE_GRACE: Duration = Duration::from_secs(2);
/// Longest accepted interval for serving from the last validated snapshot.
pub const MAX_CATALOG_STALE_GRACE: Duration = Duration::from_mins(15);
/// Smallest accepted ceiling for the initial reconnect window.
pub const MIN_CATALOG_RECONNECT_DELAY: Duration = Duration::from_millis(10);
/// Largest accepted ceiling for the initial reconnect window.
pub const MAX_CATALOG_INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(5);
/// Longest accepted reconnect-delay ceiling.
pub const MAX_CATALOG_RECONNECT_DELAY: Duration = Duration::from_mins(1);
/// Shortest accepted deadline for establishing one catalog connection.
pub const MIN_CATALOG_CONNECT_TIMEOUT: Duration = Duration::from_millis(100);
/// Longest accepted deadline for establishing one catalog connection.
pub const MAX_CATALOG_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

const DEFAULT_CATALOG_STALE_GRACE: Duration = Duration::from_secs(90);
const DEFAULT_CATALOG_INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_CATALOG_MAX_RECONNECT_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_CATALOG_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Validated refresh, staleness, and reconnect policy for one pooler process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogSupervisorConfig {
    poll_interval: CatalogPollInterval,
    stale_grace: Duration,
    initial_reconnect_delay: Duration,
    maximum_reconnect_delay: Duration,
    connect_timeout: Duration,
    operation_timeout: CatalogOperationTimeout,
}

impl CatalogSupervisorConfig {
    /// Validates one catalog supervision policy.
    ///
    /// The stale grace must be strictly longer than the authoritative poll
    /// interval so a healthy driver does not age out between scheduled reads.
    /// Reconnect delays use bounded exponential backoff with per-process
    /// jitter in the upper half of each delay window.
    ///
    /// # Errors
    ///
    /// Rejects an out-of-range stale grace or reconnect delay, a stale grace
    /// no longer than the poll interval, or a reconnect ceiling shorter than
    /// its initial delay.
    pub fn new(
        poll_interval: CatalogPollInterval,
        stale_grace: Duration,
        initial_reconnect_delay: Duration,
        maximum_reconnect_delay: Duration,
    ) -> Result<Self, CatalogSupervisorConfigError> {
        if !(MIN_CATALOG_STALE_GRACE..=MAX_CATALOG_STALE_GRACE).contains(&stale_grace) {
            return Err(CatalogSupervisorConfigError::StaleGraceOutOfRange { stale_grace });
        }
        if stale_grace <= poll_interval.get() {
            return Err(CatalogSupervisorConfigError::StaleGraceNotLongerThanPoll {
                stale_grace,
                poll_interval: poll_interval.get(),
            });
        }
        if !(MIN_CATALOG_RECONNECT_DELAY..=MAX_CATALOG_INITIAL_RECONNECT_DELAY)
            .contains(&initial_reconnect_delay)
        {
            return Err(
                CatalogSupervisorConfigError::InitialReconnectDelayOutOfRange {
                    delay: initial_reconnect_delay,
                },
            );
        }
        if !(initial_reconnect_delay..=MAX_CATALOG_RECONNECT_DELAY)
            .contains(&maximum_reconnect_delay)
        {
            return Err(
                CatalogSupervisorConfigError::MaximumReconnectDelayOutOfRange {
                    delay: maximum_reconnect_delay,
                    initial: initial_reconnect_delay,
                },
            );
        }
        Ok(Self {
            poll_interval,
            stale_grace,
            initial_reconnect_delay,
            maximum_reconnect_delay,
            connect_timeout: DEFAULT_CATALOG_CONNECT_TIMEOUT,
            operation_timeout: CatalogOperationTimeout::default(),
        })
    }

    /// Overrides the connection and catalog-operation deadlines.
    ///
    /// # Errors
    ///
    /// Rejects a connection deadline outside 100 milliseconds to 30 seconds.
    pub fn with_timeouts(
        mut self,
        connect_timeout: Duration,
        operation_timeout: CatalogOperationTimeout,
    ) -> Result<Self, CatalogSupervisorConfigError> {
        if !(MIN_CATALOG_CONNECT_TIMEOUT..=MAX_CATALOG_CONNECT_TIMEOUT).contains(&connect_timeout) {
            return Err(CatalogSupervisorConfigError::ConnectTimeoutOutOfRange {
                timeout: connect_timeout,
            });
        }
        self.connect_timeout = connect_timeout;
        self.operation_timeout = operation_timeout;
        Ok(self)
    }

    /// Returns the authoritative polling interval.
    #[must_use]
    pub const fn poll_interval(self) -> CatalogPollInterval {
        self.poll_interval
    }

    /// Returns the maximum permitted age of the last validated refresh.
    #[must_use]
    pub const fn stale_grace(self) -> Duration {
        self.stale_grace
    }

    /// Returns the configured ceiling of the first jittered reconnect window.
    #[must_use]
    pub const fn initial_reconnect_delay(self) -> Duration {
        self.initial_reconnect_delay
    }

    /// Returns the reconnect-delay ceiling.
    #[must_use]
    pub const fn maximum_reconnect_delay(self) -> Duration {
        self.maximum_reconnect_delay
    }

    /// Returns the deadline for establishing one catalog connection.
    #[must_use]
    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    /// Returns the deadline for one subscription or authoritative load.
    #[must_use]
    pub const fn operation_timeout(self) -> CatalogOperationTimeout {
        self.operation_timeout
    }

    fn retry_delay(self, consecutive_failures: u64, entropy: u64) -> Duration {
        debug_assert!(consecutive_failures > 0);
        let exponent = u32::try_from(consecutive_failures.saturating_sub(1).min(31))
            .expect("bounded reconnect exponent fits u32");
        let multiplier = 1_u32 << exponent;
        let ceiling = self
            .initial_reconnect_delay
            .saturating_mul(multiplier)
            .min(self.maximum_reconnect_delay);
        let floor = ceiling / 2;
        let jitter_span = ceiling
            .checked_sub(floor)
            .expect("jitter floor never exceeds its ceiling");
        let jitter_nanos = u64::try_from(jitter_span.as_nanos())
            .expect("validated reconnect delay fits u64 nanoseconds");
        floor + Duration::from_nanos(entropy % jitter_nanos.saturating_add(1))
    }
}

impl Default for CatalogSupervisorConfig {
    fn default() -> Self {
        Self::new(
            CatalogPollInterval::default(),
            DEFAULT_CATALOG_STALE_GRACE,
            DEFAULT_CATALOG_INITIAL_RECONNECT_DELAY,
            DEFAULT_CATALOG_MAX_RECONNECT_DELAY,
        )
        .expect("default catalog supervision policy is valid")
    }
}

/// Invalid catalog supervision policy.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CatalogSupervisorConfigError {
    /// Stale grace is outside the global safety bounds.
    #[error(
        "catalog stale grace {stale_grace:?} must be between {MIN_CATALOG_STALE_GRACE:?} and {MAX_CATALOG_STALE_GRACE:?}"
    )]
    StaleGraceOutOfRange {
        /// Rejected stale grace.
        stale_grace: Duration,
    },
    /// Stale grace would expire before the next healthy poll.
    #[error(
        "catalog stale grace {stale_grace:?} must be longer than poll interval {poll_interval:?}"
    )]
    StaleGraceNotLongerThanPoll {
        /// Rejected stale grace.
        stale_grace: Duration,
        /// Configured authoritative polling interval.
        poll_interval: Duration,
    },
    /// Initial reconnect delay is outside the global safety bounds.
    #[error(
        "initial catalog reconnect delay {delay:?} must be between {MIN_CATALOG_RECONNECT_DELAY:?} and {MAX_CATALOG_INITIAL_RECONNECT_DELAY:?}"
    )]
    InitialReconnectDelayOutOfRange {
        /// Rejected delay.
        delay: Duration,
    },
    /// Reconnect ceiling is shorter than the initial delay or globally unsafe.
    #[error(
        "maximum catalog reconnect delay {delay:?} must be between initial delay {initial:?} and {MAX_CATALOG_RECONNECT_DELAY:?}"
    )]
    MaximumReconnectDelayOutOfRange {
        /// Rejected ceiling.
        delay: Duration,
        /// Validated initial delay.
        initial: Duration,
    },
    /// Connection deadline is outside the global safety bounds.
    #[error(
        "catalog connection timeout {timeout:?} must be between {MIN_CATALOG_CONNECT_TIMEOUT:?} and {MAX_CATALOG_CONNECT_TIMEOUT:?}"
    )]
    ConnectTimeoutOutOfRange {
        /// Rejected connection deadline.
        timeout: Duration,
    },
}

/// Lifecycle phase of the dedicated catalog connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogConnectionPhase {
    /// The runner has been constructed but has not attempted a connection.
    Starting,
    /// A connection attempt is pending.
    Connecting,
    /// A socket is connected but its initial authoritative load is pending.
    Loading,
    /// The connection has completed at least one authoritative load.
    Connected,
    /// The last connection or driver attempt failed and retry is delayed.
    Backoff,
    /// The runner has stopped or its future was dropped.
    Stopped,
}

impl CatalogConnectionPhase {
    /// Returns the stable bounded label used by status and metrics endpoints.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Connecting => "connecting",
            Self::Loading => "loading",
            Self::Connected => "connected",
            Self::Backoff => "backoff",
            Self::Stopped => "stopped",
        }
    }

    /// Returns whether a catalog socket is currently owned by the driver.
    #[must_use]
    pub const fn connection_up(self) -> bool {
        matches!(self, Self::Loading | Self::Connected)
    }
}

/// Coarse, credential-safe classification of the last supervision failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatalogFailureKind {
    /// Establishing a fresh `PostgreSQL` connection failed.
    Connect,
    /// The server rejected connection authentication or authorization.
    ConnectAuthentication,
    /// The connector or local socket configuration is invalid or unavailable.
    ConnectConfiguration,
    /// The remote endpoint refused the connection.
    ConnectRefused,
    /// The network or host was unreachable.
    ConnectNetwork,
    /// The operating system's connection attempt timed out.
    ConnectIoTimeout,
    /// A connected server returned another startup error.
    ConnectServer,
    /// Establishing a fresh `PostgreSQL` connection exceeded its deadline.
    ConnectTimeout,
    /// Subscription, snapshot loading, or cache validation failed.
    Load,
    /// Subscription or an authoritative load exceeded its deadline.
    OperationTimeout,
    /// The established `PostgreSQL` connection failed or closed.
    Connection,
    /// The spawned connection pump task failed.
    ConnectionTask,
}

impl CatalogFailureKind {
    /// Returns the stable bounded label used by status and metrics endpoints.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::ConnectAuthentication => "connect_authentication",
            Self::ConnectConfiguration => "connect_configuration",
            Self::ConnectRefused => "connect_refused",
            Self::ConnectNetwork => "connect_network",
            Self::ConnectIoTimeout => "connect_io_timeout",
            Self::ConnectServer => "connect_server",
            Self::ConnectTimeout => "connect_timeout",
            Self::Load => "load",
            Self::OperationTimeout => "operation_timeout",
            Self::Connection => "connection",
            Self::ConnectionTask => "connection_task",
        }
    }
}

impl From<tokio_postgres::Error> for CatalogFailureKind {
    fn from(error: tokio_postgres::Error) -> Self {
        if let Some(database_error) = error.as_db_error() {
            return if database_error.code().code().starts_with("28") {
                Self::ConnectAuthentication
            } else {
                Self::ConnectServer
            };
        }

        let mut source = error.source();
        while let Some(cause) = source {
            if let Some(error) = cause.downcast_ref::<std::io::Error>() {
                return classify_connect_io_error(error.kind());
            }
            source = cause.source();
        }
        Self::Connect
    }
}

fn classify_connect_io_error(kind: std::io::ErrorKind) -> CatalogFailureKind {
    match kind {
        std::io::ErrorKind::ConnectionRefused => CatalogFailureKind::ConnectRefused,
        std::io::ErrorKind::TimedOut => CatalogFailureKind::ConnectIoTimeout,
        std::io::ErrorKind::HostUnreachable | std::io::ErrorKind::NetworkUnreachable => {
            CatalogFailureKind::ConnectNetwork
        }
        std::io::ErrorKind::AddrNotAvailable
        | std::io::ErrorKind::InvalidInput
        | std::io::ErrorKind::NotFound
        | std::io::ErrorKind::PermissionDenied => CatalogFailureKind::ConnectConfiguration,
        _ => CatalogFailureKind::Connect,
    }
}

impl From<&CatalogRefreshError> for CatalogFailureKind {
    fn from(error: &CatalogRefreshError) -> Self {
        match error {
            CatalogRefreshError::Load(_) => Self::Load,
            CatalogRefreshError::OperationTimeout { .. } => Self::OperationTimeout,
            CatalogRefreshError::ConnectionClosed | CatalogRefreshError::Connection(_) => {
                Self::Connection
            }
            CatalogRefreshError::ConnectionTask(_) => Self::ConnectionTask,
        }
    }
}

/// Why a pooler may or may not accept new work using its catalog cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogReadinessReason {
    /// A current validated snapshot and its dedicated connection are healthy.
    Ready,
    /// A validated snapshot remains within grace while reconnecting or loading.
    ServingStale,
    /// No authoritative snapshot has been loaded by this supervisor.
    Uninitialized,
    /// The current snapshot is fenced or its epoch differs from supervisor state.
    CacheUnavailable,
    /// The last authoritative refresh has reached its staleness deadline.
    Stale,
    /// Supervision has stopped or its future was dropped.
    Stopped,
}

impl CatalogReadinessReason {
    /// Returns the stable bounded label used by status and metrics endpoints.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::ServingStale => "serving_stale",
            Self::Uninitialized => "uninitialized",
            Self::CacheUnavailable => "cache_unavailable",
            Self::Stale => "stale",
            Self::Stopped => "stopped",
        }
    }
}

/// Point-in-time, metrics-ready catalog supervision state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogSupervisorSnapshot {
    phase: CatalogConnectionPhase,
    ready: bool,
    readiness_reason: CatalogReadinessReason,
    catalog_epoch: Option<CatalogEpoch>,
    cache_age: Option<Duration>,
    consecutive_failures: u64,
    total_failures: u64,
    connect_attempts: u64,
    successful_connections: u64,
    last_failure: Option<CatalogFailureKind>,
}

impl CatalogSupervisorSnapshot {
    /// Returns the current connection lifecycle phase.
    #[must_use]
    pub const fn phase(self) -> CatalogConnectionPhase {
        self.phase
    }

    /// Returns whether new work may use the current catalog snapshot.
    #[must_use]
    pub const fn ready(self) -> bool {
        self.ready
    }

    /// Returns the exact reason for the readiness decision.
    #[must_use]
    pub const fn readiness_reason(self) -> CatalogReadinessReason {
        self.readiness_reason
    }

    /// Returns the epoch shared by supervisor and cache state.
    ///
    /// This is absent before the first load and while an epoch-changing cache
    /// publication awaits supervisor acknowledgement.
    #[must_use]
    pub const fn catalog_epoch(self) -> Option<CatalogEpoch> {
        self.catalog_epoch
    }

    /// Returns monotonic age paired with the reported catalog epoch.
    ///
    /// This is absent whenever [`Self::catalog_epoch`] is absent.
    #[must_use]
    pub const fn cache_age(self) -> Option<Duration> {
        self.cache_age
    }

    /// Returns failures since the last successful authoritative read.
    #[must_use]
    pub const fn consecutive_failures(self) -> u64 {
        self.consecutive_failures
    }

    /// Returns failures observed during the life of this supervisor.
    #[must_use]
    pub const fn total_failures(self) -> u64 {
        self.total_failures
    }

    /// Returns all connection attempts, including the initial attempt.
    #[must_use]
    pub const fn connect_attempts(self) -> u64 {
        self.connect_attempts
    }

    /// Returns connections that completed their initial authoritative load.
    #[must_use]
    pub const fn successful_connections(self) -> u64 {
        self.successful_connections
    }

    /// Returns the credential-safe class of the latest unresolved failure.
    #[must_use]
    pub const fn last_failure(self) -> Option<CatalogFailureKind> {
        self.last_failure
    }
}

#[derive(Debug)]
struct SupervisorState {
    phase: CatalogConnectionPhase,
    catalog_epoch: Option<CatalogEpoch>,
    last_observed: Instant,
    consecutive_failures: u64,
    total_failures: u64,
    connect_attempts: u64,
    successful_connections: u64,
    last_failure: Option<CatalogFailureKind>,
}

/// Cloneable read-only handle for readiness, status, and Prometheus handlers.
#[derive(Clone)]
pub struct CatalogSupervisorStatus {
    cache: Arc<CatalogCache>,
    stale_grace: Duration,
    inner: Arc<RwLock<SupervisorState>>,
}

impl CatalogSupervisorStatus {
    fn new(cache: Arc<CatalogCache>, stale_grace: Duration, now: Instant) -> Self {
        Self {
            cache,
            stale_grace,
            inner: Arc::new(RwLock::new(SupervisorState {
                phase: CatalogConnectionPhase::Starting,
                catalog_epoch: None,
                last_observed: now,
                consecutive_failures: 0,
                total_failures: 0,
                connect_attempts: 0,
                successful_connections: 0,
                last_failure: None,
            })),
        }
    }

    /// Evaluates readiness and counters at the current monotonic time.
    #[must_use]
    pub fn snapshot(&self) -> CatalogSupervisorSnapshot {
        self.snapshot_at(Instant::now())
    }

    fn snapshot_at(&self, now: Instant) -> CatalogSupervisorSnapshot {
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = state.last_observed.max(now);
        state.last_observed = now;
        let cache = self.cache.supervisor_view_at(now);
        let catalog_epoch = match (state.catalog_epoch, cache.epoch) {
            (Some(supervisor_epoch), Some(cache_epoch)) if supervisor_epoch == cache_epoch => {
                Some(cache_epoch)
            }
            _ => None,
        };
        let cache_age = catalog_epoch.and(cache.age);
        let cache_availability = match (catalog_epoch, cache.age) {
            (Some(epoch), Some(_)) if epoch.0 >= cache.minimum_epoch => CacheAvailability::Usable,
            _ => CacheAvailability::Unavailable,
        };
        let (ready, readiness_reason) =
            readiness(&state, cache_age, cache_availability, self.stale_grace);
        CatalogSupervisorSnapshot {
            phase: state.phase,
            ready,
            readiness_reason,
            catalog_epoch,
            cache_age,
            consecutive_failures: state.consecutive_failures,
            total_failures: state.total_failures,
            connect_attempts: state.connect_attempts,
            successful_connections: state.successful_connections,
            last_failure: state.last_failure,
        }
    }

    fn mutate(&self, now: Instant, mutation: impl FnOnce(&mut SupervisorState, Instant)) {
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = state.last_observed.max(now);
        state.last_observed = now;
        mutation(&mut state, now);
    }

    fn mark_connecting(&self, now: Instant) {
        self.mutate(now, |state, _| {
            state.phase = CatalogConnectionPhase::Connecting;
            state.connect_attempts = state.connect_attempts.saturating_add(1);
        });
    }

    fn mark_loading(&self, now: Instant) {
        self.mutate(now, |state, _| {
            state.phase = CatalogConnectionPhase::Loading;
        });
    }

    fn mark_refreshed(&self, now: Instant) {
        let Ok(snapshot) = self.cache.current_for_planning() else {
            return;
        };
        let epoch = snapshot.catalog_epoch();
        self.mutate(now, |state, _| {
            if state.phase == CatalogConnectionPhase::Loading {
                state.successful_connections = state.successful_connections.saturating_add(1);
            }
            state.phase = CatalogConnectionPhase::Connected;
            state.catalog_epoch = Some(epoch);
            state.consecutive_failures = 0;
            state.last_failure = None;
        });
    }

    fn mark_failure(&self, kind: CatalogFailureKind, now: Instant) -> (u64, u64) {
        let mut counters = (0, 0);
        self.mutate(now, |state, _| {
            state.phase = CatalogConnectionPhase::Backoff;
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            state.total_failures = state.total_failures.saturating_add(1);
            state.last_failure = Some(kind);
            counters = (state.consecutive_failures, state.total_failures);
        });
        counters
    }

    fn mark_stopped(&self, now: Instant) {
        self.mutate(now, |state, _| {
            state.phase = CatalogConnectionPhase::Stopped;
        });
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheAvailability {
    Usable,
    Unavailable,
}

fn readiness(
    state: &SupervisorState,
    cache_age: Option<Duration>,
    cache_availability: CacheAvailability,
    stale_grace: Duration,
) -> (bool, CatalogReadinessReason) {
    if state.phase == CatalogConnectionPhase::Stopped {
        return (false, CatalogReadinessReason::Stopped);
    }
    if cache_availability == CacheAvailability::Unavailable {
        let reason = if state.catalog_epoch.is_some() {
            CatalogReadinessReason::CacheUnavailable
        } else {
            CatalogReadinessReason::Uninitialized
        };
        return (false, reason);
    }
    let Some(cache_age) = cache_age else {
        return (false, CatalogReadinessReason::Uninitialized);
    };
    if cache_age >= stale_grace {
        return (false, CatalogReadinessReason::Stale);
    }
    if state.phase == CatalogConnectionPhase::Connected {
        (true, CatalogReadinessReason::Ready)
    } else {
        (true, CatalogReadinessReason::ServingStale)
    }
}

/// Single-owner runner that reconnects the authoritative catalog driver.
pub struct CatalogSupervisor {
    cache: Arc<CatalogCache>,
    config: CatalogSupervisorConfig,
    status: CatalogSupervisorStatus,
}

impl Drop for CatalogSupervisor {
    fn drop(&mut self) {
        self.status.mark_stopped(Instant::now());
    }
}

impl CatalogSupervisor {
    /// Creates one runner over an empty or already populated process cache.
    #[must_use]
    pub fn new(cache: Arc<CatalogCache>, config: CatalogSupervisorConfig) -> Self {
        let status =
            CatalogSupervisorStatus::new(Arc::clone(&cache), config.stale_grace, Instant::now());
        Self {
            cache,
            config,
            status,
        }
    }

    /// Returns a cloneable handle for readiness, status, and metrics handlers.
    #[must_use]
    pub fn status(&self) -> CatalogSupervisorStatus {
        self.status.clone()
    }

    /// Connects and drives the catalog until graceful shutdown.
    ///
    /// Every connection owns a fresh `PostgreSQL` session. Connector failures
    /// use the credential-safe fallback `connect` category, are delayed with
    /// bounded exponential backoff and per-process jitter, and are retried
    /// indefinitely. Use [`Self::run_classified`] when a connector can provide
    /// more precise credential-safe categories. Raw connection errors are never
    /// retained in the public status handle.
    ///
    /// `shutdown` interrupts connection attempts, reconnect delay, initial
    /// loads, and refresh queries. Consuming `self` enforces one active runner
    /// per status handle. Dropping this future immediately marks the status
    /// stopped; the inner driver's drop guard aborts its connection pump.
    pub async fn run<C, CF, S, T, E, F>(self, connect: C, shutdown: F)
    where
        C: FnMut() -> CF + Send,
        CF: Future<Output = Result<(Client, Connection<S, T>), E>> + Send,
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        E: Send,
        F: Future<Output = ()> + Send,
    {
        self.run_classified(connect, |_| CatalogFailureKind::Connect, shutdown)
            .await;
    }

    /// Connects and drives the catalog with explicit credential-safe error
    /// classification.
    ///
    /// `classify` must discard all raw connector error text. Only the returned
    /// bounded category is retained in status or telemetry.
    pub async fn run_classified<C, CF, S, T, E, F, K>(
        self,
        mut connect: C,
        classify: K,
        shutdown: F,
    ) where
        C: FnMut() -> CF + Send,
        CF: Future<Output = Result<(Client, Connection<S, T>), E>> + Send,
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        E: Send,
        F: Future<Output = ()> + Send,
        K: Fn(E) -> CatalogFailureKind + Send,
    {
        let random = RandomState::new();
        tokio::pin!(shutdown);

        loop {
            self.status.mark_connecting(Instant::now());
            let connected = {
                let deadline = TokioInstant::now() + self.config.connect_timeout;
                let connect_attempt = connect();
                tokio::pin!(connect_attempt);
                let deadline_timer = sleep_until(deadline);
                tokio::pin!(deadline_timer);
                tokio::select! {
                    biased;
                    () = shutdown.as_mut() => None,
                    () = deadline_timer.as_mut() => {
                        Some(Err(CatalogFailureKind::ConnectTimeout))
                    }
                    result = connect_attempt.as_mut() => {
                        if TokioInstant::now() >= deadline {
                            Some(Err(CatalogFailureKind::ConnectTimeout))
                        } else {
                            Some(result.map_err(&classify))
                        }
                    }
                }
            };
            let Some(connected) = connected else { break };
            let (client, connection) = match connected {
                Ok(connection) => connection,
                Err(failure_kind) => {
                    let (consecutive, total) =
                        self.status.mark_failure(failure_kind, Instant::now());
                    let delay = self
                        .config
                        .retry_delay(consecutive, retry_entropy(&random, consecutive, total));
                    tracing::warn!(
                        failure_kind = ?failure_kind,
                        consecutive_failures = consecutive,
                        total_failures = total,
                        retry_delay = ?delay,
                        "catalog connection failed; retrying"
                    );
                    if wait_for_retry(delay, shutdown.as_mut()).await {
                        break;
                    }
                    continue;
                }
            };

            self.status.mark_loading(Instant::now());
            let refresh_status = self.status.clone();
            let result = run_catalog_refresh_observed(
                client,
                connection,
                Arc::clone(&self.cache),
                self.config.poll_interval,
                self.config.operation_timeout,
                move || refresh_status.mark_refreshed(Instant::now()),
                shutdown.as_mut(),
            )
            .await;
            let Err(error) = result else {
                break;
            };
            let failure_kind = CatalogFailureKind::from(&error);
            let (consecutive, total) = self.status.mark_failure(failure_kind, Instant::now());
            let delay = self
                .config
                .retry_delay(consecutive, retry_entropy(&random, consecutive, total));
            tracing::warn!(
                failure_kind = ?failure_kind,
                error = %error,
                consecutive_failures = consecutive,
                total_failures = total,
                retry_delay = ?delay,
                "catalog driver failed; retrying"
            );
            if wait_for_retry(delay, shutdown.as_mut()).await {
                break;
            }
        }
    }
}

fn retry_entropy(random: &RandomState, consecutive_failures: u64, total_failures: u64) -> u64 {
    let mut hasher = random.build_hasher();
    consecutive_failures.hash(&mut hasher);
    total_failures.hash(&mut hasher);
    hasher.finish()
}

async fn wait_for_retry<F>(delay: Duration, mut shutdown: std::pin::Pin<&mut F>) -> bool
where
    F: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = shutdown.as_mut() => true,
        () = tokio::time::sleep(delay) => false,
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{CatalogSnapshot, ClusterId, RoutingHashConfig};

    fn snapshot(epoch: u64) -> CatalogSnapshot {
        CatalogSnapshot::new(
            ClusterId::new(Uuid::from_u128(1)).expect("cluster ID"),
            epoch,
            RoutingHashConfig::new(1, 7).expect("routing hash"),
            Vec::new(),
        )
        .expect("catalog snapshot")
    }

    fn cache_refresh_time(cache: &CatalogCache) -> Instant {
        let observed_at = Instant::now();
        let Some(age) = cache.supervisor_view_at(observed_at).age else {
            panic!("installed cache exposes its refresh age");
        };
        observed_at
            .checked_sub(age)
            .expect("cache refresh predates its observation")
    }

    fn config() -> CatalogSupervisorConfig {
        CatalogSupervisorConfig::new(
            CatalogPollInterval::new(Duration::from_secs(1)).expect("poll interval"),
            Duration::from_secs(3),
            Duration::from_millis(100),
            Duration::from_millis(400),
        )
        .expect("supervisor config")
    }

    #[test]
    fn configuration_enforces_staleness_and_retry_bounds() {
        let defaults = CatalogSupervisorConfig::default();
        assert_eq!(defaults.stale_grace(), Duration::from_secs(90));
        assert_eq!(defaults.connect_timeout(), DEFAULT_CATALOG_CONNECT_TIMEOUT);
        assert_eq!(
            defaults.operation_timeout(),
            CatalogOperationTimeout::default()
        );
        let poll = CatalogPollInterval::new(Duration::from_secs(2)).expect("poll interval");
        assert!(matches!(
            CatalogSupervisorConfig::new(
                poll,
                Duration::from_secs(2),
                MIN_CATALOG_RECONNECT_DELAY,
                MIN_CATALOG_RECONNECT_DELAY,
            ),
            Err(CatalogSupervisorConfigError::StaleGraceNotLongerThanPoll { .. })
        ));
        assert!(matches!(
            CatalogSupervisorConfig::new(
                poll,
                Duration::from_secs(3),
                MIN_CATALOG_RECONNECT_DELAY
                    .checked_sub(Duration::from_nanos(1))
                    .expect("minimum delay exceeds one nanosecond"),
                MIN_CATALOG_RECONNECT_DELAY,
            ),
            Err(CatalogSupervisorConfigError::InitialReconnectDelayOutOfRange { .. })
        ));
        assert!(matches!(
            CatalogSupervisorConfig::new(
                poll,
                Duration::from_secs(3),
                Duration::from_millis(20),
                Duration::from_millis(10),
            ),
            Err(CatalogSupervisorConfigError::MaximumReconnectDelayOutOfRange { .. })
        ));
        for accepted in [MIN_CATALOG_CONNECT_TIMEOUT, MAX_CATALOG_CONNECT_TIMEOUT] {
            assert_eq!(
                config()
                    .with_timeouts(accepted, CatalogOperationTimeout::default())
                    .expect("bounded connect timeout")
                    .connect_timeout(),
                accepted
            );
        }
        for rejected in [
            MIN_CATALOG_CONNECT_TIMEOUT
                .checked_sub(Duration::from_nanos(1))
                .expect("minimum timeout exceeds one nanosecond"),
            MAX_CATALOG_CONNECT_TIMEOUT + Duration::from_nanos(1),
        ] {
            assert!(matches!(
                config().with_timeouts(rejected, CatalogOperationTimeout::default()),
                Err(CatalogSupervisorConfigError::ConnectTimeoutOutOfRange { timeout })
                    if timeout == rejected
            ));
        }
        assert_eq!(
            CatalogFailureKind::from(&CatalogRefreshError::OperationTimeout {
                operation: crate::CatalogOperation::Refresh,
                timeout: Duration::from_secs(1),
            }),
            CatalogFailureKind::OperationTimeout
        );
        assert_eq!(
            CatalogFailureKind::ConnectTimeout.as_str(),
            "connect_timeout"
        );
        assert_eq!(
            CatalogFailureKind::OperationTimeout.as_str(),
            "operation_timeout"
        );
        assert_eq!(
            classify_connect_io_error(std::io::ErrorKind::ConnectionRefused),
            CatalogFailureKind::ConnectRefused
        );
        assert_eq!(
            classify_connect_io_error(std::io::ErrorKind::NetworkUnreachable),
            CatalogFailureKind::ConnectNetwork
        );
        assert_eq!(
            classify_connect_io_error(std::io::ErrorKind::PermissionDenied),
            CatalogFailureKind::ConnectConfiguration
        );
    }

    #[test]
    fn maximum_stale_grace_shares_the_hard_cache_age_boundary() {
        let poll = CatalogPollInterval::new(Duration::from_secs(2)).expect("poll interval");
        assert!(
            CatalogSupervisorConfig::new(
                poll,
                MAX_CATALOG_STALE_GRACE,
                MIN_CATALOG_RECONNECT_DELAY,
                MIN_CATALOG_RECONNECT_DELAY,
            )
            .is_ok()
        );
        assert_eq!(MAX_CATALOG_STALE_GRACE, crate::MAX_CATALOG_SNAPSHOT_AGE);
        assert!(matches!(
            CatalogSupervisorConfig::new(
                poll,
                MAX_CATALOG_STALE_GRACE + Duration::from_nanos(1),
                MIN_CATALOG_RECONNECT_DELAY,
                MIN_CATALOG_RECONNECT_DELAY,
            ),
            Err(CatalogSupervisorConfigError::StaleGraceOutOfRange { .. })
        ));

        let cache = Arc::new(CatalogCache::new());
        cache.install(snapshot(1)).expect("install snapshot");
        let refreshed_at = cache_refresh_time(&cache);
        let status =
            CatalogSupervisorStatus::new(Arc::clone(&cache), MAX_CATALOG_STALE_GRACE, refreshed_at);
        status.mark_loading(refreshed_at);
        status.mark_refreshed(refreshed_at);
        let boundary = refreshed_at
            .checked_add(MAX_CATALOG_STALE_GRACE)
            .expect("maximum stale-grace boundary is representable");
        let before_boundary = status.snapshot_at(
            boundary
                .checked_sub(Duration::from_nanos(1))
                .expect("stale-grace boundary exceeds one nanosecond"),
        );
        assert!(before_boundary.ready());
        assert_eq!(
            before_boundary.readiness_reason(),
            CatalogReadinessReason::Ready
        );

        let at_boundary = status.snapshot_at(boundary);
        assert!(!at_boundary.ready());
        assert_eq!(
            at_boundary.readiness_reason(),
            CatalogReadinessReason::Stale
        );
        assert_eq!(at_boundary.cache_age(), Some(MAX_CATALOG_STALE_GRACE));
    }

    #[test]
    fn retry_delay_is_jittered_and_saturates() {
        let config = config();
        for (failure, ceiling) in [
            (1, Duration::from_millis(100)),
            (2, Duration::from_millis(200)),
            (3, Duration::from_millis(400)),
            (64, Duration::from_millis(400)),
        ] {
            for entropy in [0, 1, u64::MAX] {
                let delay = config.retry_delay(failure, entropy);
                assert!(delay >= ceiling / 2, "delay {delay:?} below jitter floor");
                assert!(delay <= ceiling, "delay {delay:?} above ceiling");
            }
        }
    }

    #[test]
    fn readiness_expires_at_grace_and_clock_regression_cannot_reopen_it() {
        let cache = Arc::new(CatalogCache::new());
        cache.install(snapshot(1)).expect("install snapshot");
        let now = cache_refresh_time(&cache);
        let status = CatalogSupervisorStatus::new(Arc::clone(&cache), Duration::from_secs(3), now);

        assert_eq!(
            status.snapshot_at(now).readiness_reason(),
            CatalogReadinessReason::Uninitialized
        );
        status.mark_connecting(now);
        status.mark_loading(now);
        status.mark_refreshed(now);
        assert_eq!(
            status.snapshot_at(now).readiness_reason(),
            CatalogReadinessReason::Ready
        );

        status.mark_failure(CatalogFailureKind::Connection, now + Duration::from_secs(1));
        let stale = status.snapshot_at(now + Duration::from_secs(2));
        assert!(stale.ready());
        assert_eq!(
            stale.readiness_reason(),
            CatalogReadinessReason::ServingStale
        );
        assert_eq!(stale.catalog_epoch(), Some(CatalogEpoch(1)));

        let expired = status.snapshot_at(now + Duration::from_secs(3));
        assert!(!expired.ready());
        assert_eq!(expired.readiness_reason(), CatalogReadinessReason::Stale);
        assert_eq!(expired.cache_age(), Some(Duration::from_secs(3)));
        assert_eq!(
            status
                .snapshot_at(now + Duration::from_secs(2))
                .readiness_reason(),
            CatalogReadinessReason::Stale,
            "an earlier observation must not reopen readiness"
        );
    }

    #[test]
    fn cache_fence_overrides_recent_refresh() {
        let cache = Arc::new(CatalogCache::new());
        cache.install(snapshot(1)).expect("install snapshot");
        let now = cache_refresh_time(&cache);
        let status = CatalogSupervisorStatus::new(Arc::clone(&cache), Duration::from_secs(3), now);
        status.mark_loading(now);
        status.mark_refreshed(now);
        cache
            .fence_before(CatalogEpoch(2))
            .expect("advance cache fence");

        let fenced = status.snapshot_at(now + Duration::from_secs(1));
        assert!(!fenced.ready());
        assert_eq!(
            fenced.readiness_reason(),
            CatalogReadinessReason::CacheUnavailable
        );
        let hard_boundary = now
            .checked_add(crate::MAX_CATALOG_SNAPSHOT_AGE)
            .expect("hard cache-age boundary is representable");
        let still_fenced = status.snapshot_at(hard_boundary);
        assert_eq!(
            still_fenced.readiness_reason(),
            CatalogReadinessReason::CacheUnavailable,
            "a fence remains higher priority than simultaneous age expiry"
        );
        assert_eq!(still_fenced.catalog_epoch(), Some(CatalogEpoch(1)));
        assert_eq!(
            still_fenced.cache_age(),
            Some(crate::MAX_CATALOG_SNAPSHOT_AGE)
        );
    }

    #[test]
    fn cache_epoch_advancement_suppresses_incoherent_status_fields() {
        let cache = Arc::new(CatalogCache::new());
        cache.install(snapshot(1)).expect("install first snapshot");
        let first_refresh = cache_refresh_time(&cache);
        let status =
            CatalogSupervisorStatus::new(Arc::clone(&cache), Duration::from_secs(3), first_refresh);
        status.mark_loading(first_refresh);
        status.mark_refreshed(first_refresh);

        cache.install(snapshot(2)).expect("advance cache epoch");
        let advanced_at = Instant::now();
        let mismatch = status.snapshot_at(advanced_at);
        assert!(!mismatch.ready());
        assert_eq!(
            mismatch.readiness_reason(),
            CatalogReadinessReason::CacheUnavailable
        );
        assert_eq!(mismatch.catalog_epoch(), None);
        assert_eq!(mismatch.cache_age(), None);

        status.mark_refreshed(advanced_at);
        let coherent = status.snapshot_at(advanced_at);
        assert_eq!(coherent.catalog_epoch(), Some(CatalogEpoch(2)));
        assert!(coherent.cache_age().is_some());
    }

    #[test]
    fn successful_refresh_resets_failure_backoff_and_shutdown_fails_readiness() {
        let cache = Arc::new(CatalogCache::new());
        cache.install(snapshot(1)).expect("install snapshot");
        let now = Instant::now();
        let status = CatalogSupervisorStatus::new(Arc::clone(&cache), Duration::from_secs(3), now);
        status.mark_failure(CatalogFailureKind::Connect, now);
        status.mark_connecting(now);
        status.mark_loading(now);
        status.mark_refreshed(now);
        let recovered = status.snapshot_at(now);
        assert_eq!(recovered.consecutive_failures(), 0);
        assert_eq!(recovered.total_failures(), 1);
        assert_eq!(recovered.last_failure(), None);
        assert_eq!(recovered.connect_attempts(), 1);
        assert_eq!(recovered.successful_connections(), 1);

        status.mark_stopped(now);
        let stopped = status.snapshot_at(now);
        assert!(!stopped.ready());
        assert_eq!(stopped.readiness_reason(), CatalogReadinessReason::Stopped);
    }

    #[tokio::test]
    async fn pending_connector_hits_deadline_and_reports_safe_failure() {
        let config = config()
            .with_timeouts(
                MIN_CATALOG_CONNECT_TIMEOUT,
                CatalogOperationTimeout::default(),
            )
            .expect("timeout policy");
        let supervisor = CatalogSupervisor::new(Arc::new(CatalogCache::new()), config);
        let status = supervisor.status();
        let task = tokio::spawn(supervisor.run(
            || async {
                std::future::pending::<()>().await;
                tokio_postgres::connect("postgresql://unused", tokio_postgres::NoTls).await
            },
            std::future::pending(),
        ));

        let timed_out = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = status.snapshot();
                if snapshot.last_failure() == Some(CatalogFailureKind::ConnectTimeout) {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("pending connector must hit its configured deadline");
        assert!(!timed_out.ready());
        assert!(timed_out.total_failures() >= 1);
        assert_eq!(timed_out.successful_connections(), 0);

        task.abort();
        let cancellation = task
            .await
            .expect_err("aborted supervisor task must not complete normally");
        assert!(cancellation.is_cancelled());
        assert_eq!(status.snapshot().phase(), CatalogConnectionPhase::Stopped);
    }

    #[tokio::test]
    async fn timed_out_connector_is_dropped_before_reconnect_backoff() {
        struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let config = CatalogSupervisorConfig::new(
            CatalogPollInterval::new(Duration::from_secs(1)).expect("poll interval"),
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .expect("supervisor config")
        .with_timeouts(
            MIN_CATALOG_CONNECT_TIMEOUT,
            CatalogOperationTimeout::default(),
        )
        .expect("timeout policy");
        let supervisor = CatalogSupervisor::new(Arc::new(CatalogCache::new()), config);
        let status = supervisor.status();
        let (dropped_sender, dropped_receiver) = tokio::sync::oneshot::channel();
        let mut dropped_sender = Some(dropped_sender);
        let task = tokio::spawn(supervisor.run(
            move || {
                let drop_signal = DropSignal(dropped_sender.take());
                async move {
                    let _drop_signal = drop_signal;
                    std::future::pending::<()>().await;
                    tokio_postgres::connect("postgresql://unused", tokio_postgres::NoTls).await
                }
            },
            std::future::pending(),
        ));

        tokio::time::timeout(Duration::from_secs(1), dropped_receiver)
            .await
            .expect("timed-out connector dropped before multi-second backoff")
            .expect("connector retained its drop signal");
        assert_eq!(
            status.snapshot().last_failure(),
            Some(CatalogFailureKind::ConnectTimeout)
        );

        task.abort();
        let cancellation = task
            .await
            .expect_err("aborted supervisor task must not complete normally");
        assert!(cancellation.is_cancelled());
        assert_eq!(status.snapshot().phase(), CatalogConnectionPhase::Stopped);
    }

    #[test]
    fn dropping_an_unpolled_run_future_stops_status() {
        let supervisor = CatalogSupervisor::new(
            Arc::new(CatalogCache::new()),
            CatalogSupervisorConfig::default(),
        );
        let status = supervisor.status();
        let run = supervisor.run(
            || async {
                tokio_postgres::connect("postgresql://unused", tokio_postgres::NoTls)
                    .await
                    .map_err(|_| std::io::Error::other("redacted connector failure"))
            },
            std::future::pending(),
        );

        drop(run);

        let stopped = status.snapshot();
        assert!(!stopped.ready());
        assert_eq!(stopped.phase(), CatalogConnectionPhase::Stopped);
        assert_eq!(stopped.readiness_reason(), CatalogReadinessReason::Stopped);
    }
}
