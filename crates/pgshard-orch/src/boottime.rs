//! Linux suspend-aware time for freshness and coordination evidence.
//!
//! `std::time::Instant` uses `CLOCK_MONOTONIC` on Linux, so it does not age
//! while the host is suspended. Evidence that can authorize future work
//! therefore carries both a normal monotonic deadline and a local
//! `CLOCK_BOOTTIME` deadline. Both clocks must remain live.

#[cfg(not(target_os = "linux"))]
compile_error!("pgshard-orch freshness authority requires Linux CLOCK_BOOTTIME");

use std::sync::Arc;
use std::time::{Duration, Instant};

use rustix::time::{ClockId, DynamicClockId, Timespec, clock_gettime_dynamic};
use thiserror::Error;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct BoottimeInstant(u64);

impl BoottimeInstant {
    pub(crate) fn checked_add(self, duration: Duration) -> Option<Self> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Self)
    }

    #[cfg(test)]
    pub(crate) const fn from_nanos_for_test(nanos: u64) -> Self {
        Self(nanos)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SuspendAwareInstant {
    pub(crate) monotonic: Instant,
    pub(crate) boottime: BoottimeInstant,
}

impl SuspendAwareInstant {
    pub(crate) fn checked_add(self, duration: Duration) -> Option<Self> {
        Some(Self {
            monotonic: self.monotonic.checked_add(duration)?,
            boottime: self.boottime.checked_add(duration)?,
        })
    }

    pub(crate) fn is_live_at(self, now: Self) -> bool {
        now.monotonic < self.monotonic && now.boottime < self.boottime
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum BoottimeError {
    #[error("CLOCK_BOOTTIME is unavailable: {0}")]
    Clock(String),
    #[error("CLOCK_BOOTTIME returned an invalid timestamp")]
    InvalidTimestamp,
}

pub(crate) trait BoottimeClock: Send + Sync {
    fn now(&self) -> Result<BoottimeInstant, BoottimeError>;
}

#[derive(Clone, Copy, Debug, Default)]
struct SystemBoottimeClock;

impl BoottimeClock for SystemBoottimeClock {
    fn now(&self) -> Result<BoottimeInstant, BoottimeError> {
        let time = clock_gettime_dynamic(DynamicClockId::Known(ClockId::Boottime))
            .map_err(|error| BoottimeError::Clock(error.to_string()))?;
        timespec_to_instant(time)
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

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct FakeBoottimeClock {
    now: std::sync::atomic::AtomicU64,
    failed: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl FakeBoottimeClock {
    pub(crate) const fn new(now: BoottimeInstant) -> Self {
        Self {
            now: std::sync::atomic::AtomicU64::new(now.0),
            failed: std::sync::atomic::AtomicBool::new(false),
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
        Ok(())
    }

    pub(crate) fn fail(&self) {
        self.failed
            .store(true, std::sync::atomic::Ordering::Release);
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_deadline_arithmetic_never_wraps() {
        let near_limit = BoottimeInstant::from_nanos_for_test(u64::MAX - 1);
        assert_eq!(
            near_limit.checked_add(Duration::from_nanos(1)),
            Some(BoottimeInstant::from_nanos_for_test(u64::MAX))
        );
        assert_eq!(near_limit.checked_add(Duration::from_nanos(2)), None);
    }

    #[test]
    fn injected_clock_failure_is_fail_closed() {
        let clock = FakeBoottimeClock::new(BoottimeInstant::from_nanos_for_test(1));
        assert_eq!(
            clock.now().expect("initial clock"),
            BoottimeInstant::from_nanos_for_test(1)
        );
        clock.fail();
        assert!(matches!(clock.now(), Err(BoottimeError::Clock(_))));
    }
}
