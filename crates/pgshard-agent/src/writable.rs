//! Composed writable Lease and `PostgreSQL` supervision.
//!
//! The attempt identity and both linear halves stay private to this module so
//! callers cannot obtain a process-absence proof before, after, or from a
//! different Lease-supervision lifetime. Monotonic startup authority travels
//! only over the same identity-tagged private channel; [`AgentState`] is shared
//! for observability but cannot authorize this attempt's postmaster.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pgshard_types::ShardId;
use thiserror::Error;
use tokio::sync::watch;

use crate::coordination::{
    self, WritableLeaseConfig, WritableLeaseError, WritableLeaseReleaseOutcome,
    WritableLeaseShutdown,
};
use crate::domain::AgentState;
use crate::postgres::{PostgresError, PreparedPostgres, WritablePostgresStopped};

#[derive(Debug)]
struct WritableAttemptIdentity;

/// Exact cell and holder generation that must be durable before a postmaster
/// can start for one writable attempt.
///
/// This value is carried only by the attempt-private authority channel. It is
/// ordinary data rather than a capability: the non-cloneable attempt identity
/// remains the authority that decides whether the value is current.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DurableWritableGeneration {
    cluster_name: String,
    cluster_uid: String,
    shard_id: ShardId,
    lease_namespace: String,
    lease_name: String,
    lease_uid: String,
    holder: String,
    term: u64,
}

impl DurableWritableGeneration {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        cluster_name: String,
        cluster_uid: String,
        shard_id: ShardId,
        lease_namespace: String,
        lease_name: String,
        lease_uid: String,
        holder: String,
        term: u64,
    ) -> Self {
        Self {
            cluster_name,
            cluster_uid,
            shard_id,
            lease_namespace,
            lease_name,
            lease_uid,
            holder,
            term,
        }
    }

    pub(crate) fn term(&self) -> u64 {
        self.term
    }

    pub(crate) fn holder(&self) -> &str {
        &self.holder
    }

    pub(crate) fn same_cell(&self, other: &Self) -> bool {
        self.cluster_name == other.cluster_name
            && self.cluster_uid == other.cluster_uid
            && self.shard_id == other.shard_id
            && self.lease_namespace == other.lease_namespace
            && self.lease_name == other.lease_name
            && self.lease_uid == other.lease_uid
    }

    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        format!(
            "format=1\ncluster_name={}\ncluster_uid={}\nshard={}\nlease_namespace={}\nlease_name={}\nlease_uid={}\nholder={}\nterm={}\n",
            self.cluster_name,
            self.cluster_uid,
            self.shard_id.0,
            self.lease_namespace,
            self.lease_name,
            self.lease_uid,
            self.holder,
            self.term,
        )
        .into_bytes()
    }

    pub(crate) fn bootstrap_identity_bytes(&self) -> Vec<u8> {
        format!(
            "cluster_uid={}\nshard={:04}\n",
            self.cluster_uid, self.shard_id.0
        )
        .into_bytes()
    }

    pub(crate) fn parse_canonical(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.split_terminator('\n');
        if lines.next()? != "format=1" {
            return None;
        }
        let cluster_name = parse_field(lines.next()?, "cluster_name", 63, false)?;
        let cluster_uid = parse_field(lines.next()?, "cluster_uid", 128, false)?;
        let shard = parse_decimal::<u32>(lines.next()?, "shard")?;
        let lease_namespace = parse_field(lines.next()?, "lease_namespace", 63, false)?;
        let lease_name = parse_field(lines.next()?, "lease_name", 63, false)?;
        let lease_uid = parse_field(lines.next()?, "lease_uid", 128, false)?;
        let holder = parse_field(lines.next()?, "holder", 128, true)?;
        let term = parse_decimal::<u64>(lines.next()?, "term").filter(|term| *term > 0)?;
        if lines.next().is_some() || !text.ends_with('\n') {
            return None;
        }
        let parsed = Self::new(
            cluster_name,
            cluster_uid,
            ShardId(shard),
            lease_namespace,
            lease_name,
            lease_uid,
            holder,
            term,
        );
        (parsed.canonical_bytes() == bytes).then_some(parsed)
    }

    #[cfg(test)]
    pub(crate) fn for_test(term: u64) -> Self {
        Self::new(
            "cluster-1".to_owned(),
            "11111111-2222-3333-4444-555555555555".to_owned(),
            ShardId(0),
            "database".to_owned(),
            "cluster-1-cell-0000-writable".to_owned(),
            "99999999-8888-7777-6666-555555555555".to_owned(),
            "cluster-1-shard-0-0/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/0123456789abcdef01234567"
                .to_owned(),
            term,
        )
    }
}

fn parse_field(line: &str, name: &str, maximum: usize, allow_slash: bool) -> Option<String> {
    let value = line.strip_prefix(name)?.strip_prefix('=')?;
    if value.is_empty()
        || value.len() > maximum
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'.' | b'_' | b'-')
                || (allow_slash && byte == b'/')
        })
    {
        return None;
    }
    Some(value.to_owned())
}

fn parse_decimal<T>(line: &str, name: &str) -> Option<T>
where
    T: std::str::FromStr + ToString,
{
    let value = line.strip_prefix(name)?.strip_prefix('=')?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let parsed = value.parse::<T>().ok()?;
    (parsed.to_string() == value).then_some(parsed)
}

#[derive(Clone, Debug)]
struct WritableAuthority {
    identity: Arc<WritableAttemptIdentity>,
    deadline: Instant,
    generation: DurableWritableGeneration,
}

#[derive(Debug)]
pub(crate) struct WritableLeaseAttempt {
    identity: Arc<WritableAttemptIdentity>,
    authority: watch::Sender<Option<WritableAuthority>>,
}

#[derive(Debug)]
pub(crate) struct WritablePostgresAttempt {
    identity: Arc<WritableAttemptIdentity>,
    authority: watch::Receiver<Option<WritableAuthority>>,
}

#[derive(Debug)]
pub(crate) struct WritableAuthorityObserver {
    identity: Arc<WritableAttemptIdentity>,
    authority: watch::Receiver<Option<WritableAuthority>>,
}

impl WritableLeaseAttempt {
    pub(crate) fn install_authority(
        &self,
        deadline: Instant,
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
        authority_valid_for(&self.identity, self.authority.borrow().as_ref(), required)
    }

    async fn authority_changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.authority.changed().await
    }

    pub(crate) fn authority_observer(&self) -> WritableAuthorityObserver {
        WritableAuthorityObserver {
            identity: Arc::clone(&self.identity),
            authority: self.authority.clone(),
        }
    }
}

impl WritableAuthorityObserver {
    pub(crate) fn generation_valid_for(
        &self,
        required: Duration,
    ) -> Option<DurableWritableGeneration> {
        let authority = self.authority.borrow();
        let authority = authority.as_ref()?;
        authority_valid_for(&self.identity, Some(authority), required)
            .then(|| authority.generation.clone())
    }
}

fn authority_valid_for(
    identity: &Arc<WritableAttemptIdentity>,
    authority: Option<&WritableAuthority>,
    required: Duration,
) -> bool {
    authority.is_some_and(|authority| {
        Arc::ptr_eq(identity, &authority.identity)
            && authority.deadline.saturating_duration_since(Instant::now()) > required
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
    let (lease_attempt, mut postgres_attempt) = writable_attempt_pair();
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

fn writable_attempt_pair() -> (WritableLeaseAttempt, WritablePostgresAttempt) {
    let identity = Arc::new(WritableAttemptIdentity);
    let (authority, authority_observer) = watch::channel(None::<WritableAuthority>);
    (
        WritableLeaseAttempt {
            identity: Arc::clone(&identity),
            authority,
        },
        WritablePostgresAttempt {
            identity,
            authority: authority_observer,
        },
    )
}

#[cfg(test)]
pub(crate) fn writable_attempt_pair_for_test() -> (WritableLeaseAttempt, WritablePostgresAttempt) {
    writable_attempt_pair()
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

    #[test]
    fn durable_generation_has_one_canonical_bounded_encoding() {
        let generation = DurableWritableGeneration::for_test(42);
        let canonical = generation.canonical_bytes();

        assert_eq!(
            DurableWritableGeneration::parse_canonical(&canonical),
            Some(generation)
        );
        assert_eq!(
            DurableWritableGeneration::for_test(42).bootstrap_identity_bytes(),
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
        let (first_lease, first_postgres) = writable_attempt_pair();
        let (second_lease, second_postgres) = writable_attempt_pair();

        assert!(same_writable_attempt(&first_lease, &first_postgres));
        assert!(same_writable_attempt(&second_lease, &second_postgres));
        assert!(!same_writable_attempt(&first_lease, &second_postgres));
        assert!(!same_writable_attempt(&second_lease, &first_postgres));
    }

    #[test]
    fn authority_is_scoped_to_one_attempt() {
        let (first_lease, first_postgres) = writable_attempt_pair();
        let (_second_lease, second_postgres) = writable_attempt_pair();
        first_lease.install_authority(
            Instant::now() + Duration::from_secs(5),
            DurableWritableGeneration::for_test(1),
        );

        assert!(first_postgres.authority_valid_for(Duration::from_secs(1)));
        assert!(!second_postgres.authority_valid_for(Duration::ZERO));
    }

    #[test]
    fn mismatched_authority_tag_is_rejected() {
        let (first_lease, first_postgres) = writable_attempt_pair();
        let (second_lease, _second_postgres) = writable_attempt_pair();
        first_lease.authority.send_replace(Some(WritableAuthority {
            identity: Arc::clone(&second_lease.identity),
            deadline: Instant::now() + Duration::from_secs(5),
            generation: DurableWritableGeneration::for_test(1),
        }));

        assert!(!first_postgres.authority_valid_for(Duration::ZERO));
    }

    #[tokio::test]
    async fn postgres_start_waits_for_authority_beyond_the_fencing_margin() {
        let (lease_attempt, mut postgres_attempt) = writable_attempt_pair();
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
            Instant::now() + Duration::from_secs(5),
            DurableWritableGeneration::for_test(1),
        );

        assert!(
            tokio::time::timeout(Duration::from_millis(100), wait)
                .await
                .expect("authority notification is bounded")
        );
    }

    #[tokio::test]
    async fn postgres_start_waits_for_a_renewal_after_authority_enters_the_margin() {
        let (lease_attempt, mut postgres_attempt) = writable_attempt_pair();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut wait = Box::pin(wait_for_initial_writable_authority(
            &mut postgres_attempt,
            shutdown_rx,
            Duration::from_secs(6),
        ));

        lease_attempt.install_authority(
            Instant::now() + Duration::from_secs(5),
            DurableWritableGeneration::for_test(1),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut wait)
                .await
                .is_err(),
            "PostgreSQL start accepted authority inside the fencing margin"
        );

        lease_attempt.install_authority(
            Instant::now() + Duration::from_secs(10),
            DurableWritableGeneration::for_test(1),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), wait)
                .await
                .expect("renewal notification is bounded")
        );
    }

    #[tokio::test]
    async fn shutdown_before_authority_leaves_postgres_unstarted() {
        let (_lease_attempt, mut postgres_attempt) = writable_attempt_pair();
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
