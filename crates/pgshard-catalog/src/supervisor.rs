//! Reconnection and bounded-staleness supervision for the catalog driver.

use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use pgshard_types::CatalogEpoch;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{Client, Connection};

use crate::driver::run_catalog_refresh_observed;
use crate::{CatalogCache, CatalogPollInterval, CatalogRefreshError};

/// Shortest accepted interval for serving from the last validated snapshot.
pub const MIN_CATALOG_STALE_GRACE: Duration = Duration::from_secs(2);
/// Longest accepted interval for serving from the last validated snapshot.
pub const MAX_CATALOG_STALE_GRACE: Duration = Duration::from_mins(15);
/// Shortest accepted initial reconnect delay.
pub const MIN_CATALOG_RECONNECT_DELAY: Duration = Duration::from_millis(10);
/// Longest accepted initial reconnect delay.
pub const MAX_CATALOG_INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(5);
/// Longest accepted reconnect-delay ceiling.
pub const MAX_CATALOG_RECONNECT_DELAY: Duration = Duration::from_mins(1);

const DEFAULT_CATALOG_STALE_GRACE: Duration = Duration::from_secs(90);
const DEFAULT_CATALOG_INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_CATALOG_MAX_RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Validated refresh, staleness, and reconnect policy for one pooler process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogSupervisorConfig {
    poll_interval: CatalogPollInterval,
    stale_grace: Duration,
    initial_reconnect_delay: Duration,
    maximum_reconnect_delay: Duration,
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
        })
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

    /// Returns the lower bound for the first reconnect delay.
    #[must_use]
    pub const fn initial_reconnect_delay(self) -> Duration {
        self.initial_reconnect_delay
    }

    /// Returns the reconnect-delay ceiling.
    #[must_use]
    pub const fn maximum_reconnect_delay(self) -> Duration {
        self.maximum_reconnect_delay
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
    /// Graceful shutdown has stopped the runner.
    Stopped,
}

impl CatalogConnectionPhase {
    /// Returns whether a catalog socket is currently owned by the driver.
    #[must_use]
    pub const fn connection_up(self) -> bool {
        matches!(self, Self::Loading | Self::Connected)
    }
}

/// Coarse, credential-safe classification of the last supervision failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogFailureKind {
    /// Establishing a fresh `PostgreSQL` connection failed.
    Connect,
    /// Subscription, snapshot loading, or cache validation failed.
    Load,
    /// The established `PostgreSQL` connection failed or closed.
    Connection,
    /// The spawned connection pump task failed.
    ConnectionTask,
}

impl From<&CatalogRefreshError> for CatalogFailureKind {
    fn from(error: &CatalogRefreshError) -> Self {
        match error {
            CatalogRefreshError::Load(_) => Self::Load,
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
    /// The cache is empty or its current snapshot has been fenced.
    CacheUnavailable,
    /// The last authoritative refresh has reached its staleness deadline.
    Stale,
    /// Graceful shutdown has stopped supervision.
    Stopped,
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

    /// Returns the epoch installed by the last successful authoritative read.
    #[must_use]
    pub const fn catalog_epoch(self) -> Option<CatalogEpoch> {
        self.catalog_epoch
    }

    /// Returns monotonic age of the last successful authoritative read.
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

    /// Returns connections that reached the initial catalog load phase.
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
    last_refresh: Option<Instant>,
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
                last_refresh: None,
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
        let cache_age = state
            .last_refresh
            .map(|last_refresh| now.duration_since(last_refresh));
        let cache_usable = self.cache.current_for_planning().is_ok();
        let (ready, readiness_reason) =
            readiness(&state, cache_age, cache_usable, self.stale_grace);
        CatalogSupervisorSnapshot {
            phase: state.phase,
            ready,
            readiness_reason,
            catalog_epoch: state.catalog_epoch,
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
            state.successful_connections = state.successful_connections.saturating_add(1);
        });
    }

    fn mark_refreshed(&self, now: Instant) {
        let Ok(snapshot) = self.cache.current_for_planning() else {
            return;
        };
        let epoch = snapshot.catalog_epoch();
        self.mutate(now, |state, now| {
            state.phase = CatalogConnectionPhase::Connected;
            state.catalog_epoch = Some(epoch);
            state.last_refresh = Some(now);
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

fn readiness(
    state: &SupervisorState,
    cache_age: Option<Duration>,
    cache_usable: bool,
    stale_grace: Duration,
) -> (bool, CatalogReadinessReason) {
    if state.phase == CatalogConnectionPhase::Stopped {
        return (false, CatalogReadinessReason::Stopped);
    }
    let Some(cache_age) = cache_age else {
        return (false, CatalogReadinessReason::Uninitialized);
    };
    if !cache_usable {
        return (false, CatalogReadinessReason::CacheUnavailable);
    }
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
    /// Every connection owns a fresh `PostgreSQL` session. Failures are reduced
    /// to credential-safe status categories, delayed with bounded exponential
    /// backoff and per-process jitter, and retried indefinitely. The caller may
    /// log richer sanitized errors inside `connect`; raw connection errors are
    /// deliberately not retained in the public status handle.
    ///
    /// `shutdown` interrupts connection attempts, reconnect delay, initial
    /// loads, and refresh queries. Consuming `self` enforces one active runner
    /// per status handle.
    pub async fn run<C, CF, S, T, E, F>(self, mut connect: C, shutdown: F)
    where
        C: FnMut() -> CF + Send,
        CF: Future<Output = Result<(Client, Connection<S, T>), E>> + Send,
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        E: Send,
        F: Future<Output = ()> + Send,
    {
        let random = RandomState::new();
        tokio::pin!(shutdown);

        loop {
            self.status.mark_connecting(Instant::now());
            let connected = tokio::select! {
                biased;
                () = shutdown.as_mut() => break,
                result = connect() => result,
            };
            let Ok((client, connection)) = connected else {
                let (consecutive, total) = self
                    .status
                    .mark_failure(CatalogFailureKind::Connect, Instant::now());
                let delay = self
                    .config
                    .retry_delay(consecutive, retry_entropy(&random, consecutive, total));
                tracing::warn!(
                    failure_kind = ?CatalogFailureKind::Connect,
                    consecutive_failures = consecutive,
                    total_failures = total,
                    retry_delay = ?delay,
                    "catalog connection failed; retrying"
                );
                if wait_for_retry(delay, shutdown.as_mut()).await {
                    break;
                }
                continue;
            };

            self.status.mark_loading(Instant::now());
            let refresh_status = self.status.clone();
            let result = run_catalog_refresh_observed(
                client,
                connection,
                Arc::clone(&self.cache),
                self.config.poll_interval,
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

        self.status.mark_stopped(Instant::now());
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
        assert_eq!(
            CatalogSupervisorConfig::default().stale_grace(),
            Duration::from_secs(90)
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
        let now = Instant::now();
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
        let now = Instant::now();
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
}
