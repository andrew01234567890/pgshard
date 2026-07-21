//! Per-cell Kubernetes Lease coordination for writable `PostgreSQL` terms.

use std::cmp;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::jiff::Timestamp;
use kube::api::{Api, PostParams};
use kube::{Client, Config};
use pgshard_types::writable_generation::WritableGenerationValidationError;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

#[cfg(test)]
use crate::boottime::system_clock;
use crate::boottime::{BoottimeClock, BoottimeError, BoottimeInstant};
use crate::domain::{
    ActivationConfigEvidence, ActivationPostgresConfig, AgentIdentity, AgentState, FencingLease,
    GenerationDurabilityEvidence, LeaseInstallError,
};
use crate::postgres::WritablePostgresStopped;
use crate::writable::{DurableWritableGeneration, WritableLeaseAttempt, same_writable_attempt};

const INITIAL_RETRY: Duration = Duration::from_millis(250);
const MAX_RETRY: Duration = Duration::from_secs(5);
const OWNER_API_VERSION: &str = "pgshard.io/v1alpha1";
const OWNER_KIND: &str = "PgShardCluster";
const PROCESS_INCARNATION_HEX_LENGTH: usize = 24;

/// Validated settings for one physical cell's writable-term Lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WritableLeaseConfig {
    namespace: String,
    lease_name: String,
    identity: AgentIdentity,
    cluster_uid: String,
    lease_uid: String,
    pod_uid: String,
    lease_duration: Duration,
    renew_deadline: Duration,
    retry_period: Duration,
    request_timeout: Duration,
}

impl WritableLeaseConfig {
    /// Creates a fail-closed per-cell Lease configuration.
    ///
    /// The expected fleet and Lease UIDs bind a process to exact Kubernetes
    /// object incarnations. Timings preserve the Kubernetes leader-election
    /// ordering: the Lease outlives the renewal deadline, the renewal deadline
    /// exceeds the retry period with jitter margin, and each request fits
    /// within one third of the renewal deadline and strictly inside the
    /// remaining shutdown margin.
    ///
    /// # Errors
    ///
    /// Returns [`WritableLeaseError::InvalidSettings`] for malformed identity
    /// or timing input.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        namespace: String,
        lease_name: String,
        identity: AgentIdentity,
        cluster_uid: String,
        lease_uid: String,
        pod_uid: String,
        lease_duration: Duration,
        renew_deadline: Duration,
        retry_period: Duration,
        request_timeout: Duration,
    ) -> Result<Self, WritableLeaseError> {
        let lease_seconds = lease_duration.as_secs();
        let renew_millis = duration_millis(renew_deadline);
        let retry_millis = duration_millis(retry_period);
        let request_millis = duration_millis(request_timeout);
        let shutdown_margin_millis = duration_millis(lease_duration.saturating_sub(renew_deadline));
        let holder_length = identity
            .instance_id
            .len()
            .saturating_add(1)
            .saturating_add(pod_uid.len())
            .saturating_add(1)
            .saturating_add(PROCESS_INCARNATION_HEX_LENGTH);
        if !dns_label(&namespace)
            || !dns_label(&lease_name)
            || !dns_label(&identity.cluster_id)
            || !dns_label(&identity.instance_id)
            || !uid(&cluster_uid)
            || !uid(&lease_uid)
            || !uid(&pod_uid)
            || holder_length > 128
            || lease_duration.subsec_nanos() != 0
            || renew_deadline.subsec_nanos() != 0
            || !(6..=300).contains(&lease_seconds)
            || renew_deadline >= lease_duration
            || renew_millis <= retry_millis.saturating_mul(6) / 5
            || !(100..=30_000).contains(&retry_millis)
            || !(100..=5_000).contains(&request_millis)
            || request_millis > renew_millis / 3
            || request_millis >= shutdown_margin_millis
        {
            return Err(WritableLeaseError::InvalidSettings);
        }
        Ok(Self {
            namespace,
            lease_name,
            identity,
            cluster_uid,
            lease_uid,
            pod_uid,
            lease_duration,
            renew_deadline,
            retry_period,
            request_timeout,
        })
    }

    fn durable_generation(
        &self,
        holder: &str,
        term: u64,
    ) -> Result<DurableWritableGeneration, WritableGenerationValidationError> {
        DurableWritableGeneration::new(
            self.identity.cluster_id.clone(),
            self.cluster_uid.clone(),
            self.identity.shard_id,
            self.namespace.clone(),
            self.lease_name.clone(),
            self.lease_uid.clone(),
            holder.to_owned(),
            term,
        )
    }

    fn holder_identity(&self, process_incarnation: &str) -> String {
        debug_assert_eq!(process_incarnation.len(), PROCESS_INCARNATION_HEX_LENGTH);
        format!(
            "{}/{}/{}",
            self.identity.instance_id, self.pod_uid, process_incarnation
        )
    }

    fn lease_duration_seconds(&self) -> i32 {
        i32::try_from(self.lease_duration.as_secs())
            .expect("validated Kubernetes Lease duration fits i32")
    }

    /// Returns the local interval reserved for target-side fencing after Lease
    /// renewal must stop.
    #[must_use]
    pub fn shutdown_margin(&self) -> Duration {
        self.lease_duration.saturating_sub(self.renew_deadline)
    }

    /// Builds status-only activation configuration from the exact writable
    /// Lease and `PostgreSQL` durability settings.
    pub(crate) fn activation_config(
        &self,
        durability: GenerationDurabilityEvidence,
        target_fence_required_margin_ms: u64,
    ) -> ActivationConfigEvidence {
        ActivationConfigEvidence {
            identity: self.identity.clone(),
            cluster_uid: self.cluster_uid.clone(),
            pod_uid: self.pod_uid.clone(),
            postgres: ActivationPostgresConfig::Source {
                lease_namespace: self.namespace.clone(),
                lease_name: self.lease_name.clone(),
                lease_uid: self.lease_uid.clone(),
                durability,
                target_fence_required_margin_ms,
            },
        }
    }
}

/// Requested-shutdown result for one writable-term Lease supervisor.
///
/// The optional release capability is deliberately sealed inside this type.
/// It can only be consumed together with proof that the supervised writable
/// `PostgreSQL` process tree is absent.
#[derive(Debug)]
#[must_use]
pub(crate) struct WritableLeaseShutdown {
    config: WritableLeaseConfig,
    release: Option<Box<WritableLeaseRelease>>,
    attempt: WritableLeaseAttempt,
}

/// Result of attempting the optional exact-holder release after `PostgreSQL`
/// shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WritableLeaseReleaseOutcome {
    /// This process never established writable Lease authority.
    NotHeld,
    /// The exact last observed holder was conditionally cleared.
    Released,
}

impl WritableLeaseShutdown {
    /// Clears the exact last observed holder only after the writable
    /// `PostgreSQL` supervisor proves its complete process tree is absent.
    ///
    /// A failed or outcome-unknown release is safe: the occupied Lease remains
    /// subject to the normal unchanged-record expiry protocol. The caller must
    /// not retry with stale evidence.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned Lease changed, the API request failed,
    /// or the response did not prove the exact empty-holder transition.
    pub(crate) async fn release_after_postgres_stopped(
        self,
        postgres_stopped: WritablePostgresStopped,
    ) -> Result<WritableLeaseReleaseOutcome, WritableLeaseError> {
        if !same_writable_attempt(&self.attempt, &postgres_stopped.attempt) {
            return Err(WritableLeaseError::PostgresProofMismatch);
        }
        let Some(release) = self.release else {
            return Ok(WritableLeaseReleaseOutcome::NotHeld);
        };
        let store = KubernetesLeaseStore::new(&self.config)?;
        release_with_store(&store, &self.config, release).await?;
        Ok(WritableLeaseReleaseOutcome::Released)
    }
}

#[derive(Debug)]
struct WritableLeaseRelease {
    lease: Lease,
}

/// Acquires and renews the exact operator-owned writable-term Lease.
///
/// The caller must start this future only for a candidate already proven safe
/// to promote. A successful Lease update is anchored to a local suspend-aware
/// instant captured before dispatch; response latency consumes the authority
/// window. Once authority has been held, preemption or local expiry is
/// terminal so the caller can stop `PostgreSQL`. Shutdown clears local authority
/// and returns a sealed capability for the latest exact holder while leaving
/// the Kubernetes Lease occupied. That capability requires the writable
/// supervisor's complete process-tree absence proof before it can release.
///
/// # Errors
///
/// Returns on permanent identity/protocol failure, preemption, local authority
/// expiry, or agent-state rejection. Transient API failures are retried only
/// within the last successfully established boot-time deadline.
pub(crate) async fn supervise(
    config: WritableLeaseConfig,
    state: AgentState,
    shutdown: watch::Receiver<bool>,
    attempt: WritableLeaseAttempt,
) -> Result<WritableLeaseShutdown, WritableLeaseError> {
    let store = KubernetesLeaseStore::new(&config)?;
    supervise_with_store(&store, &config, state, shutdown, attempt).await
}

#[allow(clippy::too_many_lines)]
async fn supervise_with_store<S: LeaseStore>(
    store: &S,
    config: &WritableLeaseConfig,
    state: AgentState,
    mut shutdown: watch::Receiver<bool>,
    attempt: WritableLeaseAttempt,
) -> Result<WritableLeaseShutdown, WritableLeaseError> {
    revoke_authority(&state, &attempt);
    let holder_identity = config.holder_identity(&new_process_incarnation());
    let mut observed_holder = None;
    let mut retry = INITIAL_RETRY;
    let mut held_deadline = None;
    let mut release = None;
    let clock = state.boottime_clock();

    loop {
        if stopping(&shutdown) {
            revoke_authority(&state, &attempt);
            return Ok(writable_lease_shutdown(config, release, attempt));
        }
        let current_cutoff = if let Some(deadline) = held_deadline {
            let Some(cutoff) = renewal_cutoff(deadline, config) else {
                revoke_authority(&state, &attempt);
                return Err(WritableLeaseError::RenewDeadlineExceeded);
            };
            Some(cutoff)
        } else {
            None
        };
        let result = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    revoke_authority(&state, &attempt);
                    return Ok(writable_lease_shutdown(config, release, attempt));
                }
                continue;
            }
            cutoff = wait_for_renewal_cutoff(current_cutoff, clock.as_ref()) => {
                revoke_authority(&state, &attempt);
                return match cutoff {
                    Ok(()) => Err(WritableLeaseError::RenewDeadlineExceeded),
                    Err(error) => Err(error.into()),
                };
            }
            result = reconcile_once_with_clock(
                store,
                config,
                &holder_identity,
                &mut observed_holder,
                clock.as_ref(),
            ) => result,
        };

        let delay = match result {
            Ok(CoordinationStep::Holder(authority)) => {
                if let Some(deadline) = held_deadline
                    && !renewal_window_open(deadline, config, authority.observed_at)
                {
                    revoke_authority(&state, &attempt);
                    return Err(WritableLeaseError::RenewDeadlineExceeded);
                }
                let generation = match config.durable_generation(&holder_identity, authority.epoch)
                {
                    Ok(generation) => generation,
                    Err(error) => {
                        revoke_authority(&state, &attempt);
                        return Err(error.into());
                    }
                };
                if let Err(error) = install_authority(&state, config, &authority) {
                    attempt.clear_authority();
                    return Err(error);
                }
                attempt.install_authority(authority.deadline, generation);
                held_deadline = Some(authority.deadline);
                release = Some(authority.release);
                retry = INITIAL_RETRY;
                authority.delay
            }
            Ok(CoordinationStep::Follower { delay: _ }) if held_deadline.is_some() => {
                revoke_authority(&state, &attempt);
                return Err(WritableLeaseError::AuthorityPreempted);
            }
            Ok(CoordinationStep::Follower { delay }) => {
                revoke_authority(&state, &attempt);
                retry = INITIAL_RETRY;
                delay
            }
            Err(error) if !error.is_permanent() => {
                let delay = if let Some(deadline) = held_deadline {
                    let now = match clock.now() {
                        Ok(now) => now,
                        Err(error) => {
                            revoke_authority(&state, &attempt);
                            return Err(error.into());
                        }
                    };
                    let remaining = deadline.saturating_duration_since(now);
                    let shutdown_margin =
                        config.lease_duration.saturating_sub(config.renew_deadline);
                    let renewal_time = remaining.saturating_sub(shutdown_margin);
                    if renewal_time.is_zero() {
                        revoke_authority(&state, &attempt);
                        return Err(WritableLeaseError::RenewDeadlineExceeded);
                    }
                    cmp::min(retry, renewal_time)
                } else {
                    retry
                };
                retry = retry.saturating_mul(2).min(MAX_RETRY);
                tracing::warn!(reason = %error, "writable-term Lease coordination unavailable");
                delay
            }
            Err(error) => {
                revoke_authority(&state, &attempt);
                return Err(error);
            }
        };

        let wait =
            wait_before_next_reconcile(&mut shutdown, delay, held_deadline, config, clock.as_ref())
                .await;
        let wait = match wait {
            Ok(wait) => wait,
            Err(error) => {
                revoke_authority(&state, &attempt);
                return Err(error);
            }
        };
        match wait {
            WaitOutcome::Stopped => {
                revoke_authority(&state, &attempt);
                return Ok(writable_lease_shutdown(config, release, attempt));
            }
            WaitOutcome::RenewalCutoff => {
                revoke_authority(&state, &attempt);
                return Err(WritableLeaseError::RenewDeadlineExceeded);
            }
            WaitOutcome::Elapsed => {}
        }
    }
}

fn revoke_authority(state: &AgentState, attempt: &WritableLeaseAttempt) {
    attempt.clear_authority();
    state.clear_lease();
}

fn writable_lease_shutdown(
    config: &WritableLeaseConfig,
    release: Option<Box<WritableLeaseRelease>>,
    attempt: WritableLeaseAttempt,
) -> WritableLeaseShutdown {
    WritableLeaseShutdown {
        config: config.clone(),
        release,
        attempt,
    }
}

fn install_authority(
    state: &AgentState,
    config: &WritableLeaseConfig,
    authority: &AuthorityObservation,
) -> Result<(), WritableLeaseError> {
    let result = state.install_lease_at(
        FencingLease {
            owner_instance: config.identity.instance_id.clone(),
            epoch: authority.epoch,
            valid_until_unix_ms: authority.valid_until_unix_ms,
        },
        authority.valid_from_unix_ms,
        authority.valid_from,
        authority.observed_at,
    );
    if let Err(error) = result {
        state.clear_lease();
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
async fn reconcile_once<S: LeaseStore>(
    store: &S,
    config: &WritableLeaseConfig,
    holder_identity: &str,
    observed_holder: &mut Option<ObservedHolder>,
) -> Result<CoordinationStep, WritableLeaseError> {
    let clock = system_clock();
    reconcile_once_with_clock(
        store,
        config,
        holder_identity,
        observed_holder,
        clock.as_ref(),
    )
    .await
}

async fn reconcile_once_with_clock<S: LeaseStore>(
    store: &S,
    config: &WritableLeaseConfig,
    holder_identity: &str,
    observed_holder: &mut Option<ObservedHolder>,
    clock: &dyn BoottimeClock,
) -> Result<CoordinationStep, WritableLeaseError> {
    let lease = store.get().await?;
    let evidence = validate_lease(&lease, config)?;
    let spec = lease.spec.clone().unwrap_or_default();
    let current_holder = spec
        .holder_identity
        .as_deref()
        .filter(|holder| !holder.is_empty());

    if current_holder == Some(holder_identity) {
        *observed_holder = None;
        return replace_as_holder(
            store,
            config,
            lease,
            evidence,
            holder_identity,
            false,
            clock,
        )
        .await;
    }
    if current_holder.is_none() {
        *observed_holder = None;
        return replace_as_holder(store, config, lease, evidence, holder_identity, true, clock)
            .await;
    }

    let occupied_duration = occupied_lease_duration(&spec)?;
    let record = HolderRecord {
        identity: current_holder.expect("occupied holder checked").to_owned(),
        renew_time: spec.renew_time,
        lease_duration_seconds: spec.lease_duration_seconds,
        resource_version: evidence.resource_version.clone(),
    };
    let now = clock.now()?;
    let unchanged_since = match observed_holder {
        Some(observed) if observed.record == record => observed.unchanged_since,
        _ => {
            *observed_holder = Some(ObservedHolder {
                record,
                unchanged_since: now,
            });
            return Ok(CoordinationStep::Follower {
                delay: config.retry_period,
            });
        }
    };
    let takeover_delay = cmp::max(config.lease_duration, occupied_duration);
    let elapsed = now.saturating_duration_since(unchanged_since);
    if elapsed < takeover_delay {
        return Ok(CoordinationStep::Follower {
            delay: cmp::min(config.retry_period, takeover_delay.saturating_sub(elapsed)),
        });
    }

    *observed_holder = None;
    replace_as_holder(store, config, lease, evidence, holder_identity, true, clock).await
}

async fn replace_as_holder<S: LeaseStore>(
    store: &S,
    config: &WritableLeaseConfig,
    mut lease: Lease,
    previous: LeaseEvidence,
    holder_identity: &str,
    transition: bool,
    clock: &dyn BoottimeClock,
) -> Result<CoordinationStep, WritableLeaseError> {
    let now = MicroTime(Timestamp::now());
    let mut spec = lease.spec.take().unwrap_or_default();
    reject_coordinated_election(&spec)?;
    let transitions = validated_transitions(spec.lease_transitions)?;
    let expected_transitions = if transition {
        transitions
            .checked_add(1)
            .ok_or(WritableLeaseError::LeaseTransitionOverflow)?
    } else {
        transitions
    };
    if transition {
        spec.acquire_time = Some(now.clone());
        spec.lease_transitions = Some(expected_transitions);
    }
    spec.holder_identity = Some(holder_identity.to_owned());
    spec.lease_duration_seconds = Some(config.lease_duration_seconds());
    spec.renew_time = Some(now);
    lease.spec = Some(spec);

    let valid_from = clock.now()?;
    let valid_from_unix_ms = unix_time_ms();
    let updated = store.replace(&lease).await?;
    let observed_at = clock.now()?;
    if observed_at < valid_from {
        return Err(BoottimeError::RegressiveObservation.into());
    }
    let evidence = validate_lease(&updated, config)?;
    let updated_spec = updated
        .spec
        .as_ref()
        .ok_or(WritableLeaseError::InvalidLeaseSpec)?;
    reject_coordinated_election(updated_spec)?;
    let transitions = validated_transitions(updated_spec.lease_transitions)?;
    let epoch = u64::try_from(transitions)
        .ok()
        .filter(|epoch| *epoch > 0)
        .ok_or(WritableLeaseError::InvalidLeaseSpec)?;
    if evidence.resource_version == previous.resource_version
        || updated_spec.holder_identity.as_deref() != Some(holder_identity)
        || updated_spec.lease_duration_seconds != Some(config.lease_duration_seconds())
        || updated_spec.acquire_time.is_none()
        || updated_spec.renew_time.is_none()
        || transitions != expected_transitions
    {
        return Err(WritableLeaseError::StateEvidenceRejected);
    }
    let valid_until_unix_ms = valid_from_unix_ms
        .checked_add(duration_millis(config.lease_duration))
        .ok_or(WritableLeaseError::StateEvidenceRejected)?;
    let deadline = valid_from
        .checked_add(config.lease_duration)
        .ok_or(WritableLeaseError::StateEvidenceRejected)?;
    let remaining = deadline.saturating_duration_since(observed_at);
    let shutdown_margin = config.lease_duration.saturating_sub(config.renew_deadline);
    let renewal_time = remaining.saturating_sub(shutdown_margin);
    if renewal_time.is_zero() {
        return Err(WritableLeaseError::RenewDeadlineExceeded);
    }
    Ok(CoordinationStep::Holder(AuthorityObservation {
        epoch,
        valid_from_unix_ms,
        valid_until_unix_ms,
        valid_from,
        observed_at,
        deadline,
        delay: cmp::min(config.retry_period, renewal_time / 2),
        release: Box::new(WritableLeaseRelease { lease: updated }),
    }))
}

async fn release_with_store<S: LeaseStore>(
    store: &S,
    config: &WritableLeaseConfig,
    release: Box<WritableLeaseRelease>,
) -> Result<(), WritableLeaseError> {
    let mut release = *release;
    let previous = validate_lease(&release.lease, config)?;
    let mut expected_spec = release
        .lease
        .spec
        .take()
        .ok_or(WritableLeaseError::InvalidLeaseSpec)?;
    reject_coordinated_election(&expected_spec)?;
    if occupied_lease_duration(&expected_spec)? != config.lease_duration
        || !expected_spec
            .holder_identity
            .as_deref()
            .is_some_and(|holder| holder_belongs_to_config(holder, config))
    {
        return Err(WritableLeaseError::StateEvidenceRejected);
    }
    expected_spec.holder_identity = None;
    release.lease.spec = Some(expected_spec.clone());

    let updated = store.replace(&release.lease).await?;
    let evidence = validate_lease(&updated, config)?;
    if evidence.resource_version == previous.resource_version
        || updated.spec.as_ref() != Some(&expected_spec)
    {
        return Err(WritableLeaseError::StateEvidenceRejected);
    }
    Ok(())
}

fn holder_belongs_to_config(holder: &str, config: &WritableLeaseConfig) -> bool {
    let prefix = format!("{}/{}/", config.identity.instance_id, config.pod_uid);
    holder.strip_prefix(&prefix).is_some_and(|incarnation| {
        incarnation.len() == PROCESS_INCARNATION_HEX_LENGTH
            && incarnation
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn occupied_lease_duration(spec: &LeaseSpec) -> Result<Duration, WritableLeaseError> {
    reject_coordinated_election(spec)?;
    let holder = spec
        .holder_identity
        .as_deref()
        .filter(|holder| !holder.is_empty() && holder.len() <= 128)
        .ok_or(WritableLeaseError::InvalidLeaseSpec)?;
    if holder.trim() != holder
        || spec.acquire_time.is_none()
        || spec.renew_time.is_none()
        || validated_transitions(spec.lease_transitions)? < 1
    {
        return Err(WritableLeaseError::InvalidLeaseSpec);
    }
    let seconds = spec
        .lease_duration_seconds
        .filter(|seconds| (1..=300).contains(seconds))
        .ok_or(WritableLeaseError::InvalidLeaseSpec)?;
    Ok(Duration::from_secs(
        u64::try_from(seconds).map_err(|_| WritableLeaseError::InvalidLeaseSpec)?,
    ))
}

fn reject_coordinated_election(spec: &LeaseSpec) -> Result<(), WritableLeaseError> {
    if spec.preferred_holder.is_some() || spec.strategy.is_some() {
        Err(WritableLeaseError::UnsupportedCoordinatedElection)
    } else {
        Ok(())
    }
}

fn validated_transitions(transitions: Option<i32>) -> Result<i32, WritableLeaseError> {
    let transitions = transitions.unwrap_or_default();
    if transitions < 0 {
        Err(WritableLeaseError::InvalidLeaseSpec)
    } else {
        Ok(transitions)
    }
}

fn validate_lease(
    lease: &Lease,
    config: &WritableLeaseConfig,
) -> Result<LeaseEvidence, WritableLeaseError> {
    if lease.metadata.name.as_deref() != Some(&config.lease_name)
        || lease.metadata.namespace.as_deref() != Some(&config.namespace)
        || lease.metadata.uid.as_deref() != Some(&config.lease_uid)
        || lease.metadata.deletion_timestamp.is_some()
    {
        return Err(WritableLeaseError::LeaseIdentityMismatch);
    }
    let resource_version = lease
        .metadata
        .resource_version
        .as_ref()
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or(WritableLeaseError::LeaseIdentityMismatch)?;
    let owners: Vec<_> = lease.metadata.owner_references.iter().flatten().collect();
    if owners.len() != 1 {
        return Err(WritableLeaseError::LeaseOwnershipMismatch);
    }
    let owner = owners[0];
    if owner.api_version != OWNER_API_VERSION
        || owner.kind != OWNER_KIND
        || owner.name != config.identity.cluster_id
        || owner.uid != config.cluster_uid
        || owner.controller != Some(true)
        || owner.block_owner_deletion != Some(true)
    {
        return Err(WritableLeaseError::LeaseOwnershipMismatch);
    }
    if lease
        .spec
        .as_ref()
        .is_some_and(|spec| spec.preferred_holder.is_some() || spec.strategy.is_some())
    {
        return Err(WritableLeaseError::UnsupportedCoordinatedElection);
    }
    Ok(LeaseEvidence { resource_version })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HolderRecord {
    identity: String,
    renew_time: Option<MicroTime>,
    lease_duration_seconds: Option<i32>,
    resource_version: String,
}

#[derive(Clone, Debug)]
struct ObservedHolder {
    record: HolderRecord,
    unchanged_since: BoottimeInstant,
}

#[derive(Clone, Debug)]
struct LeaseEvidence {
    resource_version: String,
}

enum CoordinationStep {
    Holder(AuthorityObservation),
    Follower { delay: Duration },
}

struct AuthorityObservation {
    epoch: u64,
    valid_from_unix_ms: u64,
    valid_until_unix_ms: u64,
    valid_from: BoottimeInstant,
    observed_at: BoottimeInstant,
    deadline: BoottimeInstant,
    delay: Duration,
    release: Box<WritableLeaseRelease>,
}

trait LeaseStore: Send + Sync {
    async fn get(&self) -> Result<Lease, WritableLeaseError>;
    async fn replace(&self, lease: &Lease) -> Result<Lease, WritableLeaseError>;
}

struct KubernetesLeaseStore {
    api: Api<Lease>,
    name: String,
    request_timeout: Duration,
}

impl KubernetesLeaseStore {
    fn new(config: &WritableLeaseConfig) -> Result<Self, WritableLeaseError> {
        let mut client_config = Config::incluster()
            .map_err(|error| WritableLeaseError::InClusterConfiguration(error.to_string()))?;
        client_config.connect_timeout = Some(config.request_timeout);
        client_config.read_timeout = Some(config.request_timeout);
        client_config.write_timeout = Some(config.request_timeout);
        client_config.default_retry = false;
        let client = Client::try_from(client_config)
            .map_err(|error| WritableLeaseError::KubernetesClient(error.to_string()))?;
        Ok(Self {
            api: Api::namespaced(client, &config.namespace),
            name: config.lease_name.clone(),
            request_timeout: config.request_timeout,
        })
    }
}

impl LeaseStore for KubernetesLeaseStore {
    async fn get(&self) -> Result<Lease, WritableLeaseError> {
        match tokio::time::timeout(self.request_timeout, self.api.get(&self.name)).await {
            Ok(Ok(lease)) => Ok(lease),
            Ok(Err(kube::Error::Api(status))) if status.code == 404 => {
                Err(WritableLeaseError::LeaseIdentityMismatch)
            }
            Ok(Err(source)) => Err(WritableLeaseError::Kubernetes {
                operation: "read Lease",
                source: Box::new(source),
            }),
            Err(_) => Err(WritableLeaseError::RequestTimedOut("read Lease")),
        }
    }

    async fn replace(&self, lease: &Lease) -> Result<Lease, WritableLeaseError> {
        match tokio::time::timeout(
            self.request_timeout,
            self.api.replace(&self.name, &PostParams::default(), lease),
        )
        .await
        {
            Ok(Ok(updated)) => Ok(updated),
            Ok(Err(kube::Error::Api(status))) if status.code == 404 => {
                Err(WritableLeaseError::LeaseIdentityMismatch)
            }
            Ok(Err(source)) => Err(WritableLeaseError::Kubernetes {
                operation: "replace Lease",
                source: Box::new(source),
            }),
            Err(_) => Err(WritableLeaseError::RequestTimedOut("replace Lease")),
        }
    }
}

fn stopping(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

async fn wait_or_stop(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    if stopping(shutdown) {
        return true;
    }
    tokio::select! {
        () = tokio::time::sleep(duration) => false,
        result = shutdown.changed() => result.is_err() || stopping(shutdown),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitOutcome {
    Elapsed,
    Stopped,
    RenewalCutoff,
}

async fn wait_before_next_reconcile(
    shutdown: &mut watch::Receiver<bool>,
    delay: Duration,
    held_deadline: Option<BoottimeInstant>,
    config: &WritableLeaseConfig,
    clock: &dyn BoottimeClock,
) -> Result<WaitOutcome, WritableLeaseError> {
    let Some(deadline) = held_deadline else {
        return Ok(if wait_or_stop(shutdown, delay).await {
            WaitOutcome::Stopped
        } else {
            WaitOutcome::Elapsed
        });
    };
    let cutoff =
        renewal_cutoff(deadline, config).ok_or(WritableLeaseError::RenewDeadlineExceeded)?;
    let wake_at = clock
        .now()?
        .checked_add(delay)
        .ok_or(WritableLeaseError::AuthorityDeadlineOverflow)?;
    let wake_at_cutoff = wake_at >= cutoff;
    let outcome = wait_until_or_stop(shutdown, cmp::min(wake_at, cutoff), clock).await?;
    Ok(if outcome == WaitOutcome::Elapsed && wake_at_cutoff {
        WaitOutcome::RenewalCutoff
    } else {
        outcome
    })
}

async fn wait_until_or_stop(
    shutdown: &mut watch::Receiver<bool>,
    deadline: BoottimeInstant,
    clock: &dyn BoottimeClock,
) -> Result<WaitOutcome, BoottimeError> {
    if stopping(shutdown) {
        return Ok(WaitOutcome::Stopped);
    }
    tokio::select! {
        biased;
        result = shutdown.changed() => {
            if result.is_err() || stopping(shutdown) {
                Ok(WaitOutcome::Stopped)
            } else {
                Ok(WaitOutcome::Elapsed)
            }
        }
        result = clock.wait_until(deadline) => result.map(|()| WaitOutcome::Elapsed),
    }
}

async fn wait_for_renewal_cutoff(
    cutoff: Option<BoottimeInstant>,
    clock: &dyn BoottimeClock,
) -> Result<(), BoottimeError> {
    match cutoff {
        Some(cutoff) => clock.wait_until(cutoff).await,
        None => std::future::pending().await,
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn new_process_incarnation() -> String {
    Uuid::new_v4().simple().to_string()[..PROCESS_INCARNATION_HEX_LENGTH].to_owned()
}

fn renewal_window_open(
    deadline: BoottimeInstant,
    config: &WritableLeaseConfig,
    now: BoottimeInstant,
) -> bool {
    renewal_cutoff(deadline, config).is_some_and(|cutoff| now < cutoff)
}

fn renewal_cutoff(
    deadline: BoottimeInstant,
    config: &WritableLeaseConfig,
) -> Option<BoottimeInstant> {
    deadline.checked_sub(config.shutdown_margin())
}

fn unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn uid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Per-cell writable-term Lease failure.
#[derive(Debug, Error)]
pub enum WritableLeaseError {
    /// Standalone configuration bypassed validated identity or timing bounds.
    #[error("invalid writable-term Kubernetes Lease settings")]
    InvalidSettings,
    /// The in-cluster service-account configuration is unavailable.
    #[error("in-cluster Kubernetes configuration is unavailable: {0}")]
    InClusterConfiguration(String),
    /// The authenticated Kubernetes client could not be constructed.
    #[error("Kubernetes client initialization failed: {0}")]
    KubernetesClient(String),
    /// A bounded API request exceeded its deadline.
    #[error("Kubernetes API request timed out while attempting to {0}")]
    RequestTimedOut(&'static str),
    /// Kubernetes rejected or could not serve one API request.
    #[error("Kubernetes API could not {operation}: {source}")]
    Kubernetes {
        /// Bounded operation name.
        operation: &'static str,
        /// Typed client failure.
        #[source]
        source: Box<kube::Error>,
    },
    /// The Lease name, namespace, UID, resource version, or lifecycle changed.
    #[error("writable-term Lease identity does not match the configured object")]
    LeaseIdentityMismatch,
    /// The Lease is not controlled by the exact `PgShardCluster` UID.
    #[error("writable-term Lease is not owned by the configured PgShardCluster UID")]
    LeaseOwnershipMismatch,
    /// The Lease spec cannot support safe local observation.
    #[error("writable-term Lease has an invalid holder, duration, renewal, or transition record")]
    InvalidLeaseSpec,
    /// Validated Lease identity could not form the canonical durable record.
    #[error("writable-term Lease identity cannot form a durable generation: {0}")]
    InvalidDurableGeneration(#[from] WritableGenerationValidationError),
    /// Coordinated leader-election fields would change the ownership protocol.
    #[error("Kubernetes coordinated leader-election fields are not supported")]
    UnsupportedCoordinatedElection,
    /// The transition counter cannot advance without wrapping.
    #[error("writable-term Lease transition counter is exhausted")]
    LeaseTransitionOverflow,
    /// The API response contradicted the exact resource-version write.
    #[error("writable-term Lease update response rejected local authority evidence")]
    StateEvidenceRejected,
    /// The `PostgreSQL` absence proof belongs to another writable attempt.
    #[error("PostgreSQL process-tree absence proof does not match the writable Lease attempt")]
    PostgresProofMismatch,
    /// The receiving agent rejected the term or deadline.
    #[error("agent rejected writable-term authority: {0}")]
    AgentState(#[from] LeaseInstallError),
    /// A process that previously held authority observed a different holder.
    #[error("writable-term Lease was preempted")]
    AuthorityPreempted,
    /// Renewal could not be proven while enough Lease time remained to fence.
    #[error("writable-term Lease renewal deadline was exceeded")]
    RenewDeadlineExceeded,
    /// `CLOCK_BOOTTIME` could not represent a local authority deadline.
    #[error("writable-term authority deadline overflowed")]
    AuthorityDeadlineOverflow,
    /// The suspend-aware authority clock or its absolute timer failed.
    #[error("suspend-aware authority clock failed: {0}")]
    AuthorityClock(#[from] BoottimeError),
}

impl WritableLeaseError {
    fn is_permanent(&self) -> bool {
        match self {
            Self::Kubernetes { source, .. } => matches!(
                source.as_ref(),
                kube::Error::Api(status) if matches!(status.code, 400 | 401 | 403 | 405 | 422)
            ),
            Self::RequestTimedOut(_) => false,
            Self::InvalidSettings
            | Self::InClusterConfiguration(_)
            | Self::KubernetesClient(_)
            | Self::LeaseIdentityMismatch
            | Self::LeaseOwnershipMismatch
            | Self::InvalidLeaseSpec
            | Self::InvalidDurableGeneration(_)
            | Self::UnsupportedCoordinatedElection
            | Self::LeaseTransitionOverflow
            | Self::StateEvidenceRejected
            | Self::PostgresProofMismatch
            | Self::AgentState(_)
            | Self::AuthorityPreempted
            | Self::RenewDeadlineExceeded
            | Self::AuthorityDeadlineOverflow
            | Self::AuthorityClock(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
    use pgshard_types::ShardId;

    use super::*;

    const PROCESS_A: &str = "0123456789abcdef01234567";
    const PROCESS_B: &str = "89abcdef0123456789abcdef";

    fn test_boottime_now() -> BoottimeInstant {
        system_clock().now().expect("read CLOCK_BOOTTIME")
    }

    fn advance(at: BoottimeInstant, duration: Duration) -> BoottimeInstant {
        at.checked_add(duration).expect("test boot time fits")
    }

    fn config() -> WritableLeaseConfig {
        WritableLeaseConfig::new(
            "database".to_owned(),
            "demo-cell-0000-writable".to_owned(),
            AgentIdentity {
                cluster_id: "demo".to_owned(),
                shard_id: ShardId(0),
                instance_id: "demo-shard-0000-0".to_owned(),
            },
            "11111111-2222-3333-4444-555555555555".to_owned(),
            "99999999-8888-7777-6666-555555555555".to_owned(),
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_owned(),
            Duration::from_secs(6),
            Duration::from_secs(4),
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .expect("valid writable Lease config")
    }

    fn holder(config: &WritableLeaseConfig) -> String {
        config.holder_identity(PROCESS_A)
    }

    fn lease(config: &WritableLeaseConfig) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(config.lease_name.clone()),
                namespace: Some(config.namespace.clone()),
                uid: Some(config.lease_uid.clone()),
                resource_version: Some("1".to_owned()),
                owner_references: Some(vec![OwnerReference {
                    api_version: OWNER_API_VERSION.to_owned(),
                    kind: OWNER_KIND.to_owned(),
                    name: config.identity.cluster_id.clone(),
                    uid: config.cluster_uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..ObjectMeta::default()
            },
            spec: Some(LeaseSpec::default()),
        }
    }

    struct FakeStore {
        lease: Mutex<Lease>,
        response_delay: Duration,
        response_transition_delta: i32,
        replace_failure: Mutex<Option<ReplaceFailure>>,
    }

    #[derive(Clone, Copy)]
    enum ReplaceFailure {
        BeforeCommit,
        AfterCommit,
    }

    impl FakeStore {
        fn new(lease: Lease) -> Self {
            Self {
                lease: Mutex::new(lease),
                response_delay: Duration::ZERO,
                response_transition_delta: 0,
                replace_failure: Mutex::new(None),
            }
        }

        fn fail_next_replace(mut self, failure: ReplaceFailure) -> Self {
            *self
                .replace_failure
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(failure);
            self
        }
    }

    impl LeaseStore for FakeStore {
        async fn get(&self) -> Result<Lease, WritableLeaseError> {
            Ok(self
                .lease
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone())
        }

        async fn replace(&self, lease: &Lease) -> Result<Lease, WritableLeaseError> {
            if !self.response_delay.is_zero() {
                tokio::time::sleep(self.response_delay).await;
            }
            let failure = self
                .replace_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if matches!(failure, Some(ReplaceFailure::BeforeCommit)) {
                return Err(WritableLeaseError::RequestTimedOut("replace Lease"));
            }
            let mut stored = self
                .lease
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if lease.metadata.resource_version != stored.metadata.resource_version {
                return Err(WritableLeaseError::StateEvidenceRejected);
            }
            let next = stored
                .metadata
                .resource_version
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .and_then(|value| value.checked_add(1))
                .ok_or(WritableLeaseError::StateEvidenceRejected)?;
            *stored = lease.clone();
            stored.metadata.resource_version = Some(next.to_string());
            if self.response_transition_delta != 0
                && let Some(spec) = stored.spec.as_mut()
                && let Some(transitions) = spec.lease_transitions.as_mut()
            {
                *transitions += self.response_transition_delta;
            }
            if matches!(failure, Some(ReplaceFailure::AfterCommit)) {
                return Err(WritableLeaseError::RequestTimedOut("replace Lease"));
            }
            Ok(stored.clone())
        }
    }

    struct DelayedRenewalStore {
        inner: FakeStore,
        replace_calls: AtomicUsize,
        renewal_delay: Duration,
        delay_after_commit: bool,
    }

    impl LeaseStore for DelayedRenewalStore {
        async fn get(&self) -> Result<Lease, WritableLeaseError> {
            self.inner.get().await
        }

        async fn replace(&self, lease: &Lease) -> Result<Lease, WritableLeaseError> {
            let delayed_renewal = self.replace_calls.fetch_add(1, Ordering::SeqCst) > 0;
            if delayed_renewal && !self.delay_after_commit {
                tokio::time::sleep(self.renewal_delay).await;
            }
            let result = self.inner.replace(lease).await;
            if delayed_renewal && self.delay_after_commit {
                tokio::time::sleep(self.renewal_delay).await;
            }
            result
        }
    }

    #[test]
    fn rejects_unpinned_or_unsafe_configuration() {
        let mut unpinned = config();
        unpinned.lease_uid.clear();
        assert!(matches!(
            WritableLeaseConfig::new(
                unpinned.namespace,
                unpinned.lease_name,
                unpinned.identity,
                unpinned.cluster_uid,
                unpinned.lease_uid,
                unpinned.pod_uid,
                unpinned.lease_duration,
                unpinned.renew_deadline,
                unpinned.retry_period,
                unpinned.request_timeout,
            ),
            Err(WritableLeaseError::InvalidSettings)
        ));

        let unsafe_margin = config();
        assert!(matches!(
            WritableLeaseConfig::new(
                unsafe_margin.namespace,
                unsafe_margin.lease_name,
                unsafe_margin.identity,
                unsafe_margin.cluster_uid,
                unsafe_margin.lease_uid,
                unsafe_margin.pod_uid,
                Duration::from_secs(6),
                Duration::from_secs(5),
                Duration::from_millis(100),
                Duration::from_secs(1),
            ),
            Err(WritableLeaseError::InvalidSettings)
        ));
    }

    #[tokio::test]
    async fn empty_lease_claim_bumps_term_and_anchors_deadline() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let mut observed = None;
        let step = reconcile_once(&store, &config, &holder(&config), &mut observed)
            .await
            .expect("claim empty Lease");
        let CoordinationStep::Holder(authority) = step else {
            panic!("empty Lease was not claimed");
        };
        assert_eq!(authority.epoch, 1);
        assert!(authority.observed_at < advance(authority.valid_from, config.lease_duration));

        let state =
            AgentState::with_identity(config.identity.clone(), 10_000).expect("valid agent state");
        state
            .install_lease_at(
                FencingLease {
                    owner_instance: config.identity.instance_id.clone(),
                    epoch: authority.epoch,
                    valid_until_unix_ms: authority.valid_until_unix_ms,
                },
                authority.valid_from_unix_ms,
                authority.valid_from,
                authority.observed_at,
            )
            .expect("install claimed authority");
        assert_eq!(
            state.lease_deadline(),
            Some(advance(authority.valid_from, config.lease_duration))
        );
    }

    #[tokio::test]
    async fn exact_clean_release_clears_only_the_holder_without_advancing_term() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let mut observed = None;
        let step = reconcile_once(&store, &config, &holder(&config), &mut observed)
            .await
            .expect("claim empty Lease");
        let CoordinationStep::Holder(authority) = step else {
            panic!("empty Lease was not claimed");
        };

        release_with_store(&store, &config, authority.release)
            .await
            .expect("release exact holder");

        let stored = store
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spec = stored.spec.as_ref().expect("released Lease spec");
        assert!(spec.holder_identity.is_none());
        assert_eq!(spec.lease_transitions, Some(1));
        assert_eq!(
            spec.lease_duration_seconds,
            Some(config.lease_duration_seconds())
        );
        assert!(spec.acquire_time.is_some());
        assert!(spec.renew_time.is_some());
        assert_eq!(stored.metadata.resource_version.as_deref(), Some("3"));
    }

    #[tokio::test]
    async fn process_absence_proof_from_another_attempt_cannot_release() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let mut observed = None;
        let step = reconcile_once(&store, &config, &holder(&config), &mut observed)
            .await
            .expect("claim empty Lease");
        let CoordinationStep::Holder(authority) = step else {
            panic!("empty Lease was not claimed");
        };
        let (lease_attempt, _) = crate::writable::writable_attempt_pair_for_test();
        let (_, other_postgres_attempt) = crate::writable::writable_attempt_pair_for_test();
        let shutdown = WritableLeaseShutdown {
            config,
            release: Some(authority.release),
            attempt: lease_attempt,
        };
        let stopped = WritablePostgresStopped {
            attempt: other_postgres_attempt,
        };

        assert!(matches!(
            shutdown.release_after_postgres_stopped(stopped).await,
            Err(WritableLeaseError::PostgresProofMismatch)
        ));
        let stored = store
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            stored
                .spec
                .as_ref()
                .and_then(|spec| spec.holder_identity.as_ref())
                .is_some()
        );
        assert_eq!(stored.metadata.resource_version.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn stale_clean_release_cannot_clear_a_later_renewal() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let process_holder = holder(&config);
        let mut observed = None;
        let first = reconcile_once(&store, &config, &process_holder, &mut observed)
            .await
            .expect("claim empty Lease");
        let CoordinationStep::Holder(first) = first else {
            panic!("empty Lease was not claimed");
        };
        let stale_release = first.release;

        let renewed = reconcile_once(&store, &config, &process_holder, &mut observed)
            .await
            .expect("renew current holder");
        assert!(matches!(renewed, CoordinationStep::Holder(_)));

        assert!(matches!(
            release_with_store(&store, &config, stale_release).await,
            Err(WritableLeaseError::StateEvidenceRejected)
        ));
        let stored = store
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spec = stored.spec.as_ref().expect("renewed Lease spec");
        assert_eq!(
            spec.holder_identity.as_deref(),
            Some(process_holder.as_str())
        );
        assert_eq!(spec.lease_transitions, Some(1));
        assert_eq!(stored.metadata.resource_version.as_deref(), Some("3"));
    }

    #[tokio::test]
    async fn outcome_unknown_clean_release_is_not_retried_with_stale_evidence() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let mut observed = None;
        let step = reconcile_once(&store, &config, &holder(&config), &mut observed)
            .await
            .expect("claim empty Lease");
        let CoordinationStep::Holder(authority) = step else {
            panic!("empty Lease was not claimed");
        };
        *store
            .replace_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ReplaceFailure::AfterCommit);

        assert!(matches!(
            release_with_store(&store, &config, authority.release).await,
            Err(WritableLeaseError::RequestTimedOut("replace Lease"))
        ));
        let stored = store
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spec = stored.spec.as_ref().expect("released Lease spec");
        assert!(spec.holder_identity.is_none());
        assert_eq!(spec.lease_transitions, Some(1));
        assert_eq!(stored.metadata.resource_version.as_deref(), Some("3"));
    }

    #[tokio::test]
    async fn requested_shutdown_returns_the_latest_exact_release_capability() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let state =
            AgentState::with_identity(config.identity.clone(), 10_000).expect("valid agent state");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (lease_attempt, postgres_attempt) = crate::writable::writable_attempt_pair_for_test();
        let holder_prefix = format!("{}/{}/", config.identity.instance_id, config.pod_uid);
        let request_shutdown = async {
            loop {
                let claimed = store
                    .lease
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.holder_identity.as_deref())
                    .is_some_and(|holder| holder.starts_with(&holder_prefix));
                if claimed && postgres_attempt.authority_valid_for(Duration::ZERO) {
                    shutdown_tx.send(true).expect("request clean shutdown");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };

        let (result, ()) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(1),
                supervise_with_store(&store, &config, state.clone(), shutdown_rx, lease_attempt,),
            ),
            request_shutdown,
        );
        let shutdown = result
            .expect("supervisor observes requested shutdown")
            .expect("requested shutdown succeeds");
        let release = shutdown.release.expect("held Lease has release capability");
        let current_resource_version = store
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .metadata
            .resource_version
            .clone();
        assert_eq!(
            release.lease.metadata.resource_version,
            current_resource_version
        );
        assert!(state.snapshot().lease.is_none());
        assert!(state.lease_deadline().is_none());
        assert!(!postgres_attempt.authority_valid_for(Duration::ZERO));
    }

    #[tokio::test]
    async fn shutdown_before_acquisition_has_no_release_capability() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let state =
            AgentState::with_identity(config.identity.clone(), 10_000).expect("valid agent state");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx
            .send(true)
            .expect("request shutdown before claim");

        let shutdown = supervise_with_store(
            &store,
            &config,
            state,
            shutdown_rx,
            crate::writable::writable_attempt_pair_for_test().0,
        )
        .await
        .expect("shutdown before claim succeeds");
        assert!(shutdown.release.is_none());
        assert_eq!(
            store
                .lease
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .metadata
                .resource_version
                .as_deref(),
            Some("1")
        );
    }

    #[tokio::test]
    async fn process_restart_cannot_renew_the_previous_incarnations_term() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let mut observed = None;
        let first = reconcile_once(&store, &config, &holder(&config), &mut observed)
            .await
            .expect("first process claims empty Lease");
        let CoordinationStep::Holder(first) = first else {
            panic!("first process did not claim Lease");
        };
        assert_eq!(first.epoch, 1);

        let restarted_holder = config.holder_identity(PROCESS_B);
        assert!(matches!(
            reconcile_once(&store, &config, &restarted_holder, &mut observed)
                .await
                .expect("restart observes prior process"),
            CoordinationStep::Follower { .. }
        ));
        let elapsed = config
            .lease_duration
            .checked_add(Duration::from_secs(1))
            .expect("test takeover interval fits Duration");
        observed
            .as_mut()
            .expect("recorded prior process")
            .unchanged_since = test_boottime_now()
            .checked_sub(elapsed)
            .expect("test instant can move before takeover window");
        let takeover = reconcile_once(&store, &config, &restarted_holder, &mut observed)
            .await
            .expect("restart takes over expired prior process");
        let CoordinationStep::Holder(takeover) = takeover else {
            panic!("restarted process did not take over Lease");
        };
        assert_eq!(takeover.epoch, 2);
        assert_eq!(
            store
                .lease
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .spec
                .as_ref()
                .and_then(|spec| spec.holder_identity.as_deref()),
            Some(restarted_holder.as_str())
        );
    }

    #[tokio::test]
    async fn foreign_holder_requires_unchanged_local_observation_window() {
        let mut config = config();
        config.lease_duration = Duration::from_millis(20);
        let mut occupied = lease(&config);
        occupied.spec = Some(LeaseSpec {
            holder_identity: Some("other-member/other-pod".to_owned()),
            lease_duration_seconds: Some(1),
            acquire_time: Some(MicroTime(Timestamp::now())),
            renew_time: Some(MicroTime(Timestamp::now())),
            lease_transitions: Some(4),
            ..LeaseSpec::default()
        });
        let store = FakeStore::new(occupied);
        let mut observed = None;
        assert!(matches!(
            reconcile_once(&store, &config, &holder(&config), &mut observed,)
                .await
                .expect("observe foreign holder"),
            CoordinationStep::Follower { .. }
        ));
        observed.as_mut().expect("recorded holder").unchanged_since = test_boottime_now()
            .checked_sub(Duration::from_secs(1))
            .expect("test instant can move back one second");
        let step = reconcile_once(&store, &config, &holder(&config), &mut observed)
            .await
            .expect("take over stale holder");
        let CoordinationStep::Holder(authority) = step else {
            panic!("stale holder was not taken over");
        };
        assert_eq!(authority.epoch, 5);
    }

    #[tokio::test]
    async fn changed_resource_version_restarts_takeover_observation() {
        let mut config = config();
        config.lease_duration = Duration::from_millis(20);
        let mut occupied = lease(&config);
        occupied.spec = Some(LeaseSpec {
            holder_identity: Some("other-member/other-pod".to_owned()),
            lease_duration_seconds: Some(1),
            acquire_time: Some(MicroTime(Timestamp::now())),
            renew_time: Some(MicroTime(Timestamp::now())),
            lease_transitions: Some(4),
            ..LeaseSpec::default()
        });
        let store = FakeStore::new(occupied);
        let mut observed = None;
        reconcile_once(&store, &config, &holder(&config), &mut observed)
            .await
            .expect("first observation");
        observed.as_mut().expect("recorded holder").unchanged_since = test_boottime_now()
            .checked_sub(Duration::from_secs(1))
            .expect("test instant can move back one second");
        store
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .metadata
            .resource_version = Some("2".to_owned());

        assert!(matches!(
            reconcile_once(&store, &config, &holder(&config), &mut observed,)
                .await
                .expect("observe renewed resource version"),
            CoordinationStep::Follower { .. }
        ));
    }

    #[tokio::test]
    async fn delayed_update_cannot_create_fresh_authority() {
        let mut config = config();
        config.lease_duration = Duration::from_millis(10);
        let store = FakeStore {
            lease: Mutex::new(lease(&config)),
            response_delay: Duration::from_millis(20),
            response_transition_delta: 0,
            replace_failure: Mutex::new(None),
        };
        let mut observed = None;
        assert!(matches!(
            reconcile_once(&store, &config, &holder(&config), &mut observed,).await,
            Err(WritableLeaseError::RenewDeadlineExceeded)
        ));
    }

    #[tokio::test]
    async fn authority_clock_failure_revokes_held_state_and_private_capability() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let clock = Arc::new(crate::boottime::FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = AgentState::with_test_clock(config.identity.clone(), 10_000, clock.clone())
            .expect("valid fake-clock state");
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (lease_attempt, postgres_attempt) =
            crate::writable::writable_attempt_pair_with_clock_for_test(clock.clone());
        let mut supervision = Box::pin(supervise_with_store(
            &store,
            &config,
            state.clone(),
            shutdown_rx,
            lease_attempt,
        ));
        loop {
            tokio::select! {
                result = &mut supervision => panic!("supervision stopped before acquisition: {result:?}"),
                () = tokio::task::yield_now() => {
                    if postgres_attempt.authority_valid_for(Duration::ZERO) {
                        break;
                    }
                }
            }
        }

        clock.fail();
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(100), supervision)
                .await
                .expect("clock failure stops supervision promptly"),
            Err(WritableLeaseError::AuthorityClock(_))
        ));
        assert!(state.snapshot().lease.is_none());
        assert!(state.lease_deadline().is_none());
        assert!(!postgres_attempt.authority_valid_for(Duration::ZERO));
    }

    #[tokio::test]
    async fn failed_update_before_commit_leaves_the_empty_term_unchanged() {
        let config = config();
        let store = FakeStore::new(lease(&config)).fail_next_replace(ReplaceFailure::BeforeCommit);
        let mut observed = None;

        assert!(matches!(
            reconcile_once(&store, &config, &holder(&config), &mut observed).await,
            Err(WritableLeaseError::RequestTimedOut("replace Lease"))
        ));
        let stored = store
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spec = stored.spec.as_ref().expect("stored Lease spec");
        assert!(spec.holder_identity.is_none());
        assert!(spec.lease_transitions.is_none());
        assert_eq!(stored.metadata.resource_version.as_deref(), Some("1"));
    }

    #[tokio::test]
    async fn committed_update_with_lost_response_requires_readback_without_a_new_term() {
        let config = config();
        let store = FakeStore::new(lease(&config)).fail_next_replace(ReplaceFailure::AfterCommit);
        let mut observed = None;
        let process_holder = holder(&config);

        assert!(matches!(
            reconcile_once(&store, &config, &process_holder, &mut observed).await,
            Err(WritableLeaseError::RequestTimedOut("replace Lease"))
        ));
        {
            let stored = store
                .lease
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let spec = stored.spec.as_ref().expect("committed Lease spec");
            assert_eq!(
                spec.holder_identity.as_deref(),
                Some(process_holder.as_str())
            );
            assert_eq!(spec.lease_transitions, Some(1));
            assert_eq!(stored.metadata.resource_version.as_deref(), Some("2"));
        }

        let read_back = reconcile_once(&store, &config, &process_holder, &mut observed)
            .await
            .expect("read back and renew the committed term");
        let CoordinationStep::Holder(authority) = read_back else {
            panic!("same process did not recover its committed term");
        };
        assert_eq!(authority.epoch, 1);
        let stored = store
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spec = stored.spec.as_ref().expect("renewed Lease spec");
        assert_eq!(spec.lease_transitions, Some(1));
        assert_eq!(stored.metadata.resource_version.as_deref(), Some("3"));
    }

    #[test]
    fn wall_clock_regression_preserves_later_boottime_renewal() {
        let config = config();
        let state =
            AgentState::with_identity(config.identity.clone(), 10_000).expect("valid agent state");
        let initial_valid_from = test_boottime_now();
        state
            .install_lease_at(
                FencingLease {
                    owner_instance: config.identity.instance_id.clone(),
                    epoch: 1,
                    valid_until_unix_ms: 7_000,
                },
                1_000,
                initial_valid_from,
                initial_valid_from,
            )
            .expect("install initial term");

        let later_valid_from = initial_valid_from
            .checked_add(Duration::from_secs(1))
            .expect("test instant can advance");
        let renewal = AuthorityObservation {
            epoch: 1,
            valid_from_unix_ms: 500,
            valid_until_unix_ms: 6_500,
            valid_from: later_valid_from,
            observed_at: later_valid_from,
            deadline: advance(later_valid_from, config.lease_duration),
            delay: config.retry_period,
            release: Box::new(WritableLeaseRelease {
                lease: lease(&config),
            }),
        };
        install_authority(&state, &config, &renewal).expect("later boot-time renewal");
        assert_eq!(
            state
                .snapshot()
                .lease
                .map(|lease| lease.valid_until_unix_ms),
            Some(7_000)
        );
        assert_eq!(
            state.lease_deadline(),
            Some(advance(initial_valid_from, Duration::from_secs(7)))
        );
    }

    #[test]
    fn occupied_holder_requires_a_complete_nonzero_term() {
        let now = MicroTime(Timestamp::now());
        let mut spec = LeaseSpec {
            holder_identity: Some("other-member/other-pod".to_owned()),
            lease_duration_seconds: Some(6),
            acquire_time: Some(now.clone()),
            renew_time: Some(now),
            lease_transitions: Some(4),
            ..LeaseSpec::default()
        };
        assert_eq!(
            occupied_lease_duration(&spec).expect("complete occupied term"),
            Duration::from_secs(6)
        );
        spec.acquire_time = None;
        assert!(matches!(
            occupied_lease_duration(&spec),
            Err(WritableLeaseError::InvalidLeaseSpec)
        ));
        spec.acquire_time = Some(MicroTime(Timestamp::now()));
        spec.lease_transitions = Some(0);
        assert!(matches!(
            occupied_lease_duration(&spec),
            Err(WritableLeaseError::InvalidLeaseSpec)
        ));
    }

    #[test]
    fn local_renewal_window_closes_before_the_kubernetes_lease() {
        let config = config();
        let state =
            AgentState::with_identity(config.identity.clone(), 10_000).expect("valid agent state");
        let valid_from = test_boottime_now();
        state
            .install_lease_at(
                FencingLease {
                    owner_instance: config.identity.instance_id.clone(),
                    epoch: 1,
                    valid_until_unix_ms: 7_000,
                },
                1_000,
                valid_from,
                valid_from,
            )
            .expect("install authority");
        let renewal_deadline = valid_from
            .checked_add(config.renew_deadline)
            .expect("test renewal deadline fits boot time");
        let immediately_before = renewal_deadline
            .checked_sub(Duration::from_nanos(1))
            .expect("test renewal deadline is after boot-clock origin");
        let deadline = state
            .lease_deadline()
            .expect("installed boot-time deadline");
        assert!(renewal_window_open(deadline, &config, immediately_before));
        assert!(!renewal_window_open(deadline, &config, renewal_deadline));
        assert!(
            state
                .lease_deadline()
                .is_some_and(|deadline| deadline > renewal_deadline)
        );
    }

    #[tokio::test]
    async fn update_response_cannot_substitute_a_different_fencing_term() {
        let config = config();
        let store = FakeStore {
            lease: Mutex::new(lease(&config)),
            response_delay: Duration::ZERO,
            response_transition_delta: 1,
            replace_failure: Mutex::new(None),
        };
        let mut observed = None;
        assert!(matches!(
            reconcile_once(&store, &config, &holder(&config), &mut observed).await,
            Err(WritableLeaseError::StateEvidenceRejected)
        ));
    }

    #[tokio::test]
    async fn held_lease_preemption_is_terminal_and_clears_local_authority() {
        let config = config();
        let store = FakeStore::new(lease(&config));
        let state =
            AgentState::with_identity(config.identity.clone(), 10_000).expect("valid agent state");
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let holder_prefix = format!("{}/{}/", config.identity.instance_id, config.pod_uid);
        let replace_holder = async {
            loop {
                {
                    let mut lease = store
                        .lease
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if lease
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.holder_identity.as_deref())
                        .is_some_and(|holder| holder.starts_with(&holder_prefix))
                    {
                        let spec = lease.spec.as_mut().expect("claimed Lease spec");
                        spec.holder_identity = Some("other-member/other-pod".to_owned());
                        lease.metadata.resource_version = Some("999".to_owned());
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        let supervise = tokio::time::timeout(
            Duration::from_secs(1),
            supervise_with_store(
                &store,
                &config,
                state.clone(),
                shutdown_rx,
                crate::writable::writable_attempt_pair_for_test().0,
            ),
        );
        let (result, ()) = tokio::join!(supervise, replace_holder);
        assert!(matches!(
            result.expect("supervisor observed preemption"),
            Err(WritableLeaseError::AuthorityPreempted)
        ));
        assert!(state.snapshot().lease.is_none());
        assert!(state.lease_deadline().is_none());
    }

    #[tokio::test]
    async fn renewal_cutoff_stops_waiting_for_an_inflight_request_before_fencing_margin() {
        let mut config = config();
        config.lease_duration = Duration::from_secs(2);
        config.renew_deadline = Duration::from_millis(200);
        config.retry_period = Duration::from_millis(10);
        let store = DelayedRenewalStore {
            inner: FakeStore::new(lease(&config)),
            replace_calls: AtomicUsize::new(0),
            renewal_delay: Duration::from_secs(5),
            delay_after_commit: false,
        };
        let state =
            AgentState::with_identity(config.identity.clone(), 10_000).expect("valid agent state");
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            supervise_with_store(
                &store,
                &config,
                state.clone(),
                shutdown_rx,
                crate::writable::writable_attempt_pair_for_test().0,
            ),
        )
        .await
        .expect("renewal cutoff must stop waiting for the delayed API request");

        assert!(matches!(
            result,
            Err(WritableLeaseError::RenewDeadlineExceeded)
        ));
        assert!(store.replace_calls.load(Ordering::SeqCst) >= 2);
        assert!(state.snapshot().lease.is_none());
        assert!(state.lease_deadline().is_none());
    }

    #[tokio::test]
    async fn renewal_cutoff_revokes_local_authority_after_an_unanswered_commit() {
        let mut config = config();
        config.lease_duration = Duration::from_secs(2);
        config.renew_deadline = Duration::from_millis(200);
        config.retry_period = Duration::from_millis(10);
        let store = DelayedRenewalStore {
            inner: FakeStore::new(lease(&config)),
            replace_calls: AtomicUsize::new(0),
            renewal_delay: Duration::from_secs(5),
            delay_after_commit: true,
        };
        let state =
            AgentState::with_identity(config.identity.clone(), 10_000).expect("valid agent state");
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            supervise_with_store(
                &store,
                &config,
                state.clone(),
                shutdown_rx,
                crate::writable::writable_attempt_pair_for_test().0,
            ),
        )
        .await
        .expect("renewal cutoff must not wait for a committed response");

        assert!(matches!(
            result,
            Err(WritableLeaseError::RenewDeadlineExceeded)
        ));
        assert_eq!(
            store
                .inner
                .lease
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .metadata
                .resource_version
                .as_deref(),
            Some("3")
        );
        assert!(state.snapshot().lease.is_none());
        assert!(state.lease_deadline().is_none());
    }

    #[test]
    fn recreated_or_foreign_owned_lease_is_rejected() {
        let config = config();
        let mut candidate = lease(&config);
        candidate.metadata.uid = Some("different-lease-uid".to_owned());
        assert!(matches!(
            validate_lease(&candidate, &config),
            Err(WritableLeaseError::LeaseIdentityMismatch)
        ));

        let mut candidate = lease(&config);
        candidate.metadata.owner_references.as_mut().unwrap()[0].uid =
            "different-cluster-uid".to_owned();
        assert!(matches!(
            validate_lease(&candidate, &config),
            Err(WritableLeaseError::LeaseOwnershipMismatch)
        ));

        let mut candidate = lease(&config);
        candidate
            .metadata
            .owner_references
            .as_mut()
            .unwrap()
            .push(OwnerReference {
                api_version: "v1".to_owned(),
                kind: "Secret".to_owned(),
                name: "extra-owner".to_owned(),
                uid: "extra-owner-uid".to_owned(),
                controller: None,
                block_owner_deletion: None,
            });
        assert!(matches!(
            validate_lease(&candidate, &config),
            Err(WritableLeaseError::LeaseOwnershipMismatch)
        ));

        let mut candidate = lease(&config);
        candidate.metadata.owner_references.as_mut().unwrap()[0].block_owner_deletion = Some(false);
        assert!(matches!(
            validate_lease(&candidate, &config),
            Err(WritableLeaseError::LeaseOwnershipMismatch)
        ));
    }
}
