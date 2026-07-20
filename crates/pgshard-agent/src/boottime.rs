//! Linux suspend-aware monotonic time for local authority decisions.
//!
//! Writable authority must age while the host is suspended. `Instant` and
//! Tokio's time driver use `CLOCK_MONOTONIC` on Linux, which explicitly omits
//! suspend time. This module is the only authority-clock boundary: decisions
//! read `CLOCK_BOOTTIME`, and runtime deadline waits use an absolute
//! `CLOCK_BOOTTIME` timerfd. Wall time remains status-only elsewhere.

#[cfg(not(target_os = "linux"))]
compile_error!("pgshard-agent writable authority requires Linux CLOCK_BOOTTIME");

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rustix::time::{
    ClockId, DynamicClockId, Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags, Timespec,
    clock_gettime_dynamic, timerfd_create, timerfd_settime,
};
use thiserror::Error;
use tokio::io::unix::AsyncFd;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// A bounded timestamp on Linux's suspend-aware monotonic boot clock.
///
/// The representation deliberately stops at `u64::MAX` nanoseconds (roughly
/// 584 years). All duration arithmetic is checked so an unrepresentable
/// authority window is rejected instead of wrapped or saturated.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BoottimeInstant(u64);

impl BoottimeInstant {
    /// Reads the current Linux suspend-aware monotonic time.
    ///
    /// # Errors
    ///
    /// Returns an error if `CLOCK_BOOTTIME` is unavailable or malformed.
    pub fn now() -> Result<Self, BoottimeError> {
        SystemBoottimeClock.now()
    }

    /// Adds a duration without wrapping or silently extending authority.
    #[must_use]
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Self)
    }

    /// Subtracts a duration without crossing the boot-clock origin.
    #[must_use]
    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_sub(nanos).map(Self)
    }

    /// Returns elapsed boot time, clamped only for a defensive regressing
    /// observation. Authority callers separately reject regressive renewals.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    /// Returns the exact nanosecond position on Linux's boot clock.
    ///
    /// This representation is passed unchanged to the local `PostgreSQL`
    /// target fence. It is not a duration relative to the observation time.
    #[must_use]
    pub(crate) const fn as_nanos(self) -> u64 {
        self.0
    }

    fn to_timespec(self) -> Timespec {
        Timespec {
            tv_sec: i64::try_from(self.0 / NANOS_PER_SECOND)
                .expect("u64 nanoseconds always fit i64 seconds"),
            tv_nsec: i64::try_from(self.0 % NANOS_PER_SECOND)
                .expect("subsecond nanoseconds always fit i64"),
        }
    }

    #[cfg(test)]
    pub(crate) const fn from_nanos_for_test(nanos: u64) -> Self {
        Self(nanos)
    }
}

/// Failure to read or wait on the authority clock.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BoottimeError {
    /// `clock_gettime(CLOCK_BOOTTIME)` was unavailable.
    #[error("CLOCK_BOOTTIME is unavailable: {0}")]
    Clock(String),
    /// The kernel returned a malformed boot-clock timestamp.
    #[error("CLOCK_BOOTTIME returned an invalid timestamp")]
    InvalidTimestamp,
    /// A later authority observation moved before its dispatch observation.
    #[error("CLOCK_BOOTTIME observation regressed")]
    RegressiveObservation,
    /// An absolute `CLOCK_BOOTTIME` timer could not be created, armed, or read.
    #[error("absolute CLOCK_BOOTTIME timer failed: {0}")]
    Timer(String),
}

/// Injectable source for suspend-aware authority time.
pub(crate) trait BoottimeClock: fmt::Debug + Send + Sync {
    fn now(&self) -> Result<BoottimeInstant, BoottimeError>;

    fn wait_until(
        &self,
        deadline: BoottimeInstant,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoottimeError>> + Send + '_>>;
}

/// Production Linux `CLOCK_BOOTTIME` source.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SystemBoottimeClock;

impl BoottimeClock for SystemBoottimeClock {
    fn now(&self) -> Result<BoottimeInstant, BoottimeError> {
        let time = clock_gettime_dynamic(DynamicClockId::Known(ClockId::Boottime))
            .map_err(|error| BoottimeError::Clock(error.to_string()))?;
        timespec_to_instant(time)
    }

    fn wait_until(
        &self,
        deadline: BoottimeInstant,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoottimeError>> + Send + '_>> {
        Box::pin(wait_until_system(deadline))
    }
}

pub(crate) fn system_clock() -> Arc<dyn BoottimeClock> {
    Arc::new(SystemBoottimeClock)
}

fn timespec_to_instant(time: Timespec) -> Result<BoottimeInstant, BoottimeError> {
    let seconds = u64::try_from(time.tv_sec).map_err(|_| BoottimeError::InvalidTimestamp)?;
    let nanos = u64::try_from(time.tv_nsec)
        .ok()
        .filter(|nanos| *nanos < NANOS_PER_SECOND)
        .ok_or(BoottimeError::InvalidTimestamp)?;
    let total = seconds
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|value| value.checked_add(nanos))
        .ok_or(BoottimeError::InvalidTimestamp)?;
    Ok(BoottimeInstant(total))
}

/// Waits until an exact Linux boot-clock deadline.
///
/// The timer is absolute, so scheduling delay and host suspend both consume
/// the authority interval. Dropping this future closes the timerfd, making
/// shutdown/cancellation immediate and bounded by the surrounding select.
async fn wait_until_system(deadline: BoottimeInstant) -> Result<(), BoottimeError> {
    let now = BoottimeInstant::now()?;
    if now >= deadline {
        return Ok(());
    }
    let timer = timerfd_create(
        TimerfdClockId::Boottime,
        TimerfdFlags::CLOEXEC | TimerfdFlags::NONBLOCK,
    )
    .map_err(|error| BoottimeError::Timer(error.to_string()))?;
    timerfd_settime(
        &timer,
        TimerfdTimerFlags::ABSTIME,
        &Itimerspec {
            it_interval: Timespec::default(),
            it_value: deadline.to_timespec(),
        },
    )
    .map_err(|error| BoottimeError::Timer(error.to_string()))?;
    let timer = AsyncFd::new(timer).map_err(|error| BoottimeError::Timer(error.to_string()))?;
    let mut buffer = [0_u8; size_of::<u64>()];
    loop {
        let mut ready = timer
            .readable()
            .await
            .map_err(|error| BoottimeError::Timer(error.to_string()))?;
        match ready.try_io(|inner| {
            rustix::io::read(inner.get_ref(), &mut buffer).map_err(std::io::Error::from)
        }) {
            Ok(Ok(size)) if size == buffer.len() => return Ok(()),
            Ok(Ok(_)) => {
                return Err(BoottimeError::Timer(
                    "timerfd returned a truncated expiration counter".to_owned(),
                ));
            }
            Ok(Err(error)) => return Err(BoottimeError::Timer(error.to_string())),
            Err(_) => {}
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct FakeBoottimeClock {
    now: std::sync::atomic::AtomicU64,
    failed: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}

#[cfg(test)]
impl FakeBoottimeClock {
    pub(crate) const fn new(now: BoottimeInstant) -> Self {
        Self {
            now: std::sync::atomic::AtomicU64::new(now.0),
            failed: std::sync::atomic::AtomicBool::new(false),
            changed: tokio::sync::Notify::const_new(),
        }
    }

    pub(crate) fn advance(&self, duration: Duration) -> Result<(), BoottimeError> {
        let nanos =
            u64::try_from(duration.as_nanos()).map_err(|_| BoottimeError::InvalidTimestamp)?;
        self.now
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |now| now.checked_add(nanos),
            )
            .map_err(|_| BoottimeError::InvalidTimestamp)?;
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn fail(&self) {
        self.failed
            .store(true, std::sync::atomic::Ordering::Release);
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
impl BoottimeClock for FakeBoottimeClock {
    fn now(&self) -> Result<BoottimeInstant, BoottimeError> {
        if self.failed.load(std::sync::atomic::Ordering::Acquire) {
            Err(BoottimeError::Clock("injected failure".to_owned()))
        } else {
            Ok(BoottimeInstant(
                self.now.load(std::sync::atomic::Ordering::Acquire),
            ))
        }
    }

    fn wait_until(
        &self,
        deadline: BoottimeInstant,
    ) -> Pin<Box<dyn Future<Output = Result<(), BoottimeError>> + Send + '_>> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                if self.now()? >= deadline {
                    return Ok(());
                }
                changed.await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_is_bounded_and_overflow_safe() {
        let near_limit = BoottimeInstant::from_nanos_for_test(u64::MAX - 1);
        assert_eq!(
            near_limit.checked_add(Duration::from_nanos(1)),
            Some(BoottimeInstant::from_nanos_for_test(u64::MAX))
        );
        assert_eq!(near_limit.checked_add(Duration::from_nanos(2)), None);
        assert_eq!(
            BoottimeInstant::from_nanos_for_test(1).checked_sub(Duration::from_nanos(2)),
            None
        );
    }

    #[tokio::test]
    async fn absolute_boottime_wait_is_live_and_cancellation_is_bounded() {
        let now = BoottimeInstant::now().expect("read CLOCK_BOOTTIME");
        wait_until_system(
            now.checked_add(Duration::from_millis(1))
                .expect("short timer fits"),
        )
        .await
        .expect("absolute boot timer expires");

        let far_future = BoottimeInstant::now()
            .expect("read CLOCK_BOOTTIME")
            .checked_add(Duration::from_mins(1))
            .expect("test timer fits");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), wait_until_system(far_future))
                .await
                .is_err(),
            "dropping the absolute timer future must remain bounded"
        );
    }
}
