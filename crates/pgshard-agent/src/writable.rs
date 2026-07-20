//! Composed writable Lease and `PostgreSQL` supervision.
//!
//! The attempt identity and both linear halves stay private to this module so
//! callers cannot obtain a process-absence proof before, after, or from a
//! different Lease-supervision lifetime. Monotonic startup authority travels
//! only over the same identity-tagged private channel; [`AgentState`] is shared
//! for observability but cannot authorize this attempt's postmaster.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub(crate) use pgshard_types::writable_generation::DurableWritableGeneration;
use thiserror::Error;
use tokio::sync::watch;

#[cfg(test)]
use crate::boottime::system_clock;
use crate::boottime::{BoottimeClock, BoottimeInstant};
use crate::coordination::{
    self, WritableLeaseConfig, WritableLeaseError, WritableLeaseReleaseOutcome,
    WritableLeaseShutdown,
};
use crate::domain::AgentState;
use crate::postgres::{PostgresError, PreparedPostgres, WritablePostgresStopped};

#[cfg(test)]
pub(crate) fn durable_generation_for_test(term: u64) -> DurableWritableGeneration {
    DurableWritableGeneration::new(
        "cluster-1".to_owned(),
        "11111111-2222-3333-4444-555555555555".to_owned(),
        pgshard_types::ShardId(0),
        "database".to_owned(),
        "cluster-1-cell-0000-writable".to_owned(),
        "99999999-8888-7777-6666-555555555555".to_owned(),
        "cluster-1-shard-0-0/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/0123456789abcdef01234567"
            .to_owned(),
        term,
    )
    .expect("valid durable-generation fixture")
}

#[derive(Debug)]
struct WritableAttemptIdentity;

#[derive(Clone, Debug)]
struct WritableAuthority {
    identity: Arc<WritableAttemptIdentity>,
    deadline: BoottimeInstant,
    generation: DurableWritableGeneration,
}

/// Exact authority snapshot installed into the local `PostgreSQL` target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WritableAuthoritySnapshot {
    pub(crate) deadline: BoottimeInstant,
    pub(crate) generation: DurableWritableGeneration,
}

#[derive(Debug)]
pub(crate) struct WritableLeaseAttempt {
    identity: Arc<WritableAttemptIdentity>,
    authority: watch::Sender<Option<WritableAuthority>>,
}

#[derive(Debug)]
pub(crate) struct WritablePostgresAttempt {
    identity: Arc<WritableAttemptIdentity>,
    clock: Arc<dyn BoottimeClock>,
    authority: watch::Receiver<Option<WritableAuthority>>,
}

#[derive(Clone, Debug)]
pub(crate) struct WritableAuthorityObserver {
    identity: Arc<WritableAttemptIdentity>,
    clock: Arc<dyn BoottimeClock>,
    authority: watch::Receiver<Option<WritableAuthority>>,
}

impl WritableLeaseAttempt {
    pub(crate) fn install_authority(
        &self,
        deadline: BoottimeInstant,
        generation: DurableWritableGeneration,
    ) {
        self.authority.send_replace(Some(WritableAuthority {
            identity: Arc::clone(&self.identity),
            deadline,
            generation,
        }));
    }

    pub(crate) fn clear_authority(&self) {
        self.authority.send_replace(None);
    }
}

impl WritablePostgresAttempt {
    pub(crate) fn authority_valid_for(&self, required: Duration) -> bool {
        authority_valid_for(
            &self.identity,
            self.authority.borrow().as_ref(),
            required,
            self.clock.as_ref(),
        )
    }

    async fn authority_changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.authority.changed().await
    }

    pub(crate) fn authority_observer(&self) -> WritableAuthorityObserver {
        WritableAuthorityObserver {
            identity: Arc::clone(&self.identity),
            clock: Arc::clone(&self.clock),
            authority: self.authority.clone(),
        }
    }
}

impl WritableAuthorityObserver {
    pub(crate) fn snapshot_valid_for(
        &self,
        required: Duration,
    ) -> Option<WritableAuthoritySnapshot> {
        let authority = self.authority.borrow();
        let authority = authority.as_ref()?;
        authority_valid_for(
            &self.identity,
            Some(authority),
            required,
            self.clock.as_ref(),
        )
        .then(|| WritableAuthoritySnapshot {
            deadline: authority.deadline,
            generation: authority.generation.clone(),
        })
    }

    pub(crate) fn generation_valid_for(
        &self,
        required: Duration,
    ) -> Option<DurableWritableGeneration> {
        self.snapshot_valid_for(required)
            .map(|snapshot| snapshot.generation)
    }

    pub(crate) fn snapshot_is_current(
        &self,
        expected: &WritableAuthoritySnapshot,
        required: Duration,
    ) -> bool {
        self.snapshot_valid_for(required).as_ref() == Some(expected)
    }

    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.authority.changed().await
    }

    /// Binds synchronously to the current exact authorized generation, then
    /// waits until it no longer has authority beyond `required`, including
    /// when a suspend-aware absolute deadline elapses. Clock and timer failures
    /// are treated exactly like lost authority.
    pub(crate) fn wait_until_current_generation_invalid(
        mut self,
        required: Duration,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let expected = self.generation_valid_for(required);
        Box::pin(async move {
            let Some(expected) = expected else {
                return;
            };
            loop {
                let cutoff = {
                    let authority = self.authority.borrow();
                    let Some(authority) = authority.as_ref() else {
                        return;
                    };
                    if !Arc::ptr_eq(&self.identity, &authority.identity)
                        || authority.generation != expected
                    {
                        return;
                    }
                    let Some(cutoff) = authority.deadline.checked_sub(required) else {
                        return;
                    };
                    cutoff
                };
                match self.clock.now() {
                    Ok(now) if now < cutoff => {}
                    Ok(_) | Err(_) => return,
                }
                tokio::select! {
                    biased;
                    changed = self.authority.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    result = self.clock.wait_until(cutoff) => {
                        let _ = result;
                        return;
                    }
                }
            }
        })
    }
}

fn authority_valid_for(
    identity: &Arc<WritableAttemptIdentity>,
    authority: Option<&WritableAuthority>,
    required: Duration,
    clock: &dyn BoottimeClock,
) -> bool {
    authority.is_some_and(|authority| {
        Arc::ptr_eq(identity, &authority.identity)
            && clock
                .now()
                .is_ok_and(|now| authority.deadline.saturating_duration_since(now) > required)
    })
}

/// Terminal outcome of one composed writable supervision attempt.
#[derive(Debug)]
pub enum WritableAttemptOutcome {
    /// Coordination was lost only after the complete `PostgreSQL` process tree
    /// was fenced; the caller may prepare a fresh attempt after backoff.
    Retry(WritableLeaseError),
    /// The external agent shutdown request completed.
    Shutdown,
}

/// Failure of one composed writable supervision attempt.
#[derive(Debug, Error)]
pub enum WritableAttemptError {
    /// The `PostgreSQL` supervisor failed.
    #[error("PostgreSQL supervisor failed: {0}")]
    Postgres(#[from] PostgresError),
    /// Lease coordination stopped without an external shutdown or error.
    #[error("writable-term Lease coordination stopped without shutdown or an error")]
    CoordinationStopped,
    /// `PostgreSQL` supervision stopped without an external shutdown or error.
    #[error("PostgreSQL supervision stopped without shutdown or an error")]
    PostgresStopped,
    /// Coordination and `PostgreSQL` supervision both failed.
    #[error(
        "writable-term Lease coordination failed: {coordination}; PostgreSQL supervisor also failed: {postgres}"
    )]
    CoordinationAndPostgres {
        /// Lease-coordination failure.
        coordination: WritableLeaseError,
        /// Concurrent `PostgreSQL` supervision failure.
        #[source]
        postgres: PostgresError,
    },
}

/// Runs one inseparable writable Lease and `PostgreSQL` supervision attempt.
///
/// This is the only public entry point that can create the paired linear
/// capabilities. It starts both supervisors together, stops Lease renewal when
/// the `PostgreSQL` process tree must be fenced, and consumes the exact process
/// proof before attempting a conditional holder release.
///
/// # Errors
///
/// Returns a typed failure when either supervisor terminates unexpectedly or
/// both fail while being joined.
pub async fn supervise_attempt(
    state: AgentState,
    postgres: PreparedPostgres,
    writable_lease: WritableLeaseConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<WritableAttemptOutcome, WritableAttemptError> {
    let margin = writable_lease.shutdown_margin();
    let (attempt_shutdown_tx, attempt_shutdown_rx) = watch::channel(false);
    let (lease_attempt, mut postgres_attempt) = writable_attempt_pair(state.boottime_clock());
    let postmaster_state = state.clone();
    let postmaster_shutdown = attempt_shutdown_rx.clone();
    let postmaster = async move {
        let _authority_ready = wait_for_initial_writable_authority(
            &mut postgres_attempt,
            postmaster_shutdown.clone(),
            margin,
        )
        .await;
        // Even shutdown before acquisition flows through the writable
        // supervisor so it can produce the linear process-tree absence proof.
        postgres
            .supervise_with_writable_authority(
                postmaster_state,
                postmaster_shutdown,
                margin,
                postgres_attempt,
            )
            .await
    };
    let coordination =
        coordination::supervise(writable_lease, state, attempt_shutdown_rx, lease_attempt);
    Box::pin(join_supervisors(
        shutdown,
        attempt_shutdown_tx,
        postmaster,
        coordination,
    ))
    .await
}

pub(crate) async fn join_supervisors<P, C>(
    shutdown: watch::Receiver<bool>,
    attempt_shutdown_tx: watch::Sender<bool>,
    postmaster: P,
    coordination: C,
) -> Result<WritableAttemptOutcome, WritableAttemptError>
where
    P: Future<Output = Result<WritablePostgresStopped, PostgresError>>,
    C: Future<Output = Result<WritableLeaseShutdown, WritableLeaseError>>,
{
    tokio::pin!(postmaster);
    tokio::pin!(coordination);
    tokio::select! {
        biased;
        () = wait_for_shutdown(shutdown) => {
            let _ = attempt_shutdown_tx.send(true);
            let postgres_stopped = postmaster.await?;
            match coordination.await {
                Ok(coordination_shutdown) => {
                    release_after_postgres_stopped(coordination_shutdown, postgres_stopped).await;
                }
                Err(error) => {
                    tracing::warn!(reason = %error, "writable-term Lease coordination ended during agent shutdown");
                }
            }
            Ok(WritableAttemptOutcome::Shutdown)
        }
        coordination_result = &mut coordination => {
            let _ = attempt_shutdown_tx.send(true);
            let postmaster_result = postmaster.await;
            match coordination_result {
                Err(coordination) => match postmaster_result {
                    Ok(_) => Ok(WritableAttemptOutcome::Retry(coordination)),
                    Err(postgres) => Err(WritableAttemptError::CoordinationAndPostgres {
                        coordination,
                        postgres,
                    }),
                },
                Ok(_) => match postmaster_result {
                    Ok(_) => Err(WritableAttemptError::CoordinationStopped),
                    Err(postgres) => Err(postgres.into()),
                },
            }
        }
        postmaster_result = &mut postmaster => {
            let _ = attempt_shutdown_tx.send(true);
            match (postmaster_result, coordination.await) {
                (Ok(_), Err(coordination)) => Ok(WritableAttemptOutcome::Retry(coordination)),
                (Err(postgres), Err(coordination)) => {
                    Err(WritableAttemptError::CoordinationAndPostgres {
                        coordination,
                        postgres,
                    })
                }
                (Err(postgres), Ok(_)) => Err(postgres.into()),
                (Ok(postgres_stopped), Ok(coordination_shutdown)) => {
                    release_after_postgres_stopped(coordination_shutdown, postgres_stopped).await;
                    Err(WritableAttemptError::PostgresStopped)
                }
            }
        }
    }
}

fn writable_attempt_pair(
    clock: Arc<dyn BoottimeClock>,
) -> (WritableLeaseAttempt, WritablePostgresAttempt) {
    let identity = Arc::new(WritableAttemptIdentity);
    let (authority, authority_observer) = watch::channel(None::<WritableAuthority>);
    (
        WritableLeaseAttempt {
            identity: Arc::clone(&identity),
            authority,
        },
        WritablePostgresAttempt {
            identity,
            clock,
            authority: authority_observer,
        },
    )
}

#[cfg(test)]
pub(crate) fn writable_attempt_pair_for_test() -> (WritableLeaseAttempt, WritablePostgresAttempt) {
    writable_attempt_pair(system_clock())
}

#[cfg(test)]
pub(crate) fn writable_attempt_pair_with_clock_for_test(
    clock: Arc<dyn BoottimeClock>,
) -> (WritableLeaseAttempt, WritablePostgresAttempt) {
    writable_attempt_pair(clock)
}

pub(crate) fn same_writable_attempt(
    lease: &WritableLeaseAttempt,
    postgres: &WritablePostgresAttempt,
) -> bool {
    Arc::ptr_eq(&lease.identity, &postgres.identity)
}

async fn release_after_postgres_stopped(
    coordination: WritableLeaseShutdown,
    postgres: WritablePostgresStopped,
) {
    match coordination.release_after_postgres_stopped(postgres).await {
        Ok(WritableLeaseReleaseOutcome::Released) => {
            tracing::info!("released writable-term Lease after PostgreSQL process-tree fence");
        }
        Ok(WritableLeaseReleaseOutcome::NotHeld) => {}
        Err(error) => {
            tracing::warn!(reason = %error, "could not prove clean writable-term Lease release; expiry fallback remains active");
        }
    }
}

async fn wait_for_initial_writable_authority(
    attempt: &mut WritablePostgresAttempt,
    mut shutdown: watch::Receiver<bool>,
    required_margin: Duration,
) -> bool {
    loop {
        if *shutdown.borrow_and_update() {
            return false;
        }
        if attempt.authority_valid_for(required_margin) {
            return true;
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            changed = attempt.authority_changed() => {
                if changed.is_err() {
                    return false;
                }
            }
        }
    }
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    if *receiver.borrow_and_update() {
        return;
    }
    while receiver.changed().await.is_ok() {
        if *receiver.borrow_and_update() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> BoottimeInstant {
        system_clock().now().expect("read CLOCK_BOOTTIME")
    }

    #[test]
    fn durable_generation_has_one_canonical_bounded_encoding() {
        let generation = durable_generation_for_test(42);
        let canonical = generation.canonical_bytes();

        assert_eq!(
            DurableWritableGeneration::parse_canonical(&canonical),
            Some(generation)
        );
        assert_eq!(
            durable_generation_for_test(42).bootstrap_identity_bytes(),
            b"cluster_uid=11111111-2222-3333-4444-555555555555\nshard=0000\n"
        );

        for invalid in [
            canonical.strip_suffix(b"\n").expect("canonical newline"),
            &canonical[..canonical.len() - b"term=42\n".len()],
            b"format=2\ncluster_name=cluster-1\ncluster_uid=11111111-2222-3333-4444-555555555555\nshard=0\nlease_namespace=database\nlease_name=cluster-1-cell-0000-writable\nlease_uid=99999999-8888-7777-6666-555555555555\nholder=cluster-1-shard-0-0/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/0123456789abcdef01234567\nterm=42\n",
        ] {
            assert!(DurableWritableGeneration::parse_canonical(invalid).is_none());
        }

        let noncanonical_term = String::from_utf8(canonical)
            .expect("generation is UTF-8")
            .replace("term=42\n", "term=042\n");
        assert!(DurableWritableGeneration::parse_canonical(noncanonical_term.as_bytes()).is_none());
    }

    #[test]
    fn only_paired_capabilities_share_an_identity() {
        let (first_lease, first_postgres) = writable_attempt_pair(system_clock());
        let (second_lease, second_postgres) = writable_attempt_pair(system_clock());

        assert!(same_writable_attempt(&first_lease, &first_postgres));
        assert!(same_writable_attempt(&second_lease, &second_postgres));
        assert!(!same_writable_attempt(&first_lease, &second_postgres));
        assert!(!same_writable_attempt(&second_lease, &first_postgres));
    }

    #[test]
    fn authority_is_scoped_to_one_attempt() {
        let (first_lease, first_postgres) = writable_attempt_pair(system_clock());
        let (_second_lease, second_postgres) = writable_attempt_pair(system_clock());
        first_lease.install_authority(
            now()
                .checked_add(Duration::from_secs(5))
                .expect("test deadline fits"),
            durable_generation_for_test(1),
        );

        assert!(first_postgres.authority_valid_for(Duration::from_secs(1)));
        assert!(!second_postgres.authority_valid_for(Duration::ZERO));
    }

    #[test]
    fn suspend_like_jump_and_clock_failure_revoke_private_authority() {
        let clock = Arc::new(crate::boottime::FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let (lease_attempt, postgres_attempt) =
            writable_attempt_pair_with_clock_for_test(clock.clone());
        lease_attempt.install_authority(
            clock
                .now()
                .expect("fake clock")
                .checked_add(Duration::from_millis(100))
                .expect("test deadline fits"),
            durable_generation_for_test(1),
        );
        assert!(postgres_attempt.authority_valid_for(Duration::ZERO));

        clock
            .advance(Duration::from_millis(101))
            .expect("advance fake boot clock across suspend");
        assert!(!postgres_attempt.authority_valid_for(Duration::ZERO));

        lease_attempt.install_authority(
            clock
                .now()
                .expect("fake clock")
                .checked_add(Duration::from_secs(1))
                .expect("test deadline fits"),
            durable_generation_for_test(1),
        );
        clock.fail();
        assert!(!postgres_attempt.authority_valid_for(Duration::ZERO));
    }

    #[tokio::test]
    async fn private_authority_wait_uses_the_injected_absolute_boot_deadline() {
        let clock = Arc::new(crate::boottime::FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let (lease_attempt, postgres_attempt) =
            writable_attempt_pair_with_clock_for_test(clock.clone());
        lease_attempt.install_authority(
            clock
                .now()
                .expect("fake clock")
                .checked_add(Duration::from_millis(100))
                .expect("test deadline fits"),
            durable_generation_for_test(1),
        );
        let mut wait = Box::pin(
            postgres_attempt
                .authority_observer()
                .wait_until_current_generation_invalid(Duration::ZERO),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut wait)
                .await
                .is_err(),
            "authority wait completed before its boot deadline"
        );
        clock
            .advance(Duration::from_millis(100))
            .expect("advance fake boot clock to deadline");
        tokio::time::timeout(Duration::from_millis(100), wait)
            .await
            .expect("fake absolute boot deadline wakes promptly");
    }

    #[tokio::test]
    async fn private_authority_wait_cannot_rebind_before_its_first_poll() {
        let clock = Arc::new(crate::boottime::FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let (lease_attempt, postgres_attempt) =
            writable_attempt_pair_with_clock_for_test(clock.clone());
        let deadline = clock
            .now()
            .expect("fake clock")
            .checked_add(Duration::from_secs(1))
            .expect("test deadline fits");
        lease_attempt.install_authority(deadline, durable_generation_for_test(1));

        let wait = postgres_attempt
            .authority_observer()
            .wait_until_current_generation_invalid(Duration::ZERO);
        lease_attempt.install_authority(deadline, durable_generation_for_test(2));

        tokio::time::timeout(Duration::from_millis(100), wait)
            .await
            .expect("bound generation change wakes before first poll");
    }

    #[test]
    fn mismatched_authority_tag_is_rejected() {
        let (first_lease, first_postgres) = writable_attempt_pair(system_clock());
        let (second_lease, _second_postgres) = writable_attempt_pair(system_clock());
        first_lease.authority.send_replace(Some(WritableAuthority {
            identity: Arc::clone(&second_lease.identity),
            deadline: now()
                .checked_add(Duration::from_secs(5))
                .expect("test deadline fits"),
            generation: durable_generation_for_test(1),
        }));

        assert!(!first_postgres.authority_valid_for(Duration::ZERO));
    }

    #[tokio::test]
    async fn postgres_start_waits_for_authority_beyond_the_fencing_margin() {
        let (lease_attempt, mut postgres_attempt) = writable_attempt_pair(system_clock());
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut wait = Box::pin(wait_for_initial_writable_authority(
            &mut postgres_attempt,
            shutdown_rx,
            Duration::from_secs(1),
        ));

        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut wait)
                .await
                .is_err(),
            "PostgreSQL start advanced without authority"
        );
        lease_attempt.install_authority(
            now()
                .checked_add(Duration::from_secs(5))
                .expect("test deadline fits"),
            durable_generation_for_test(1),
        );

        assert!(
            tokio::time::timeout(Duration::from_millis(100), wait)
                .await
                .expect("authority notification is bounded")
        );
    }

    #[tokio::test]
    async fn postgres_start_waits_for_a_renewal_after_authority_enters_the_margin() {
        let (lease_attempt, mut postgres_attempt) = writable_attempt_pair(system_clock());
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut wait = Box::pin(wait_for_initial_writable_authority(
            &mut postgres_attempt,
            shutdown_rx,
            Duration::from_secs(6),
        ));

        lease_attempt.install_authority(
            now()
                .checked_add(Duration::from_secs(5))
                .expect("test deadline fits"),
            durable_generation_for_test(1),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut wait)
                .await
                .is_err(),
            "PostgreSQL start accepted authority inside the fencing margin"
        );

        lease_attempt.install_authority(
            now()
                .checked_add(Duration::from_secs(10))
                .expect("test deadline fits"),
            durable_generation_for_test(1),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), wait)
                .await
                .expect("renewal notification is bounded")
        );
    }

    #[tokio::test]
    async fn shutdown_before_authority_leaves_postgres_unstarted() {
        let (_lease_attempt, mut postgres_attempt) = writable_attempt_pair(system_clock());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).expect("request shutdown");

        assert!(
            !wait_for_initial_writable_authority(
                &mut postgres_attempt,
                shutdown_rx,
                Duration::from_secs(1),
            )
            .await
        );
    }
}
