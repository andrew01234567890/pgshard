//! Kubernetes Lease-backed orchestrator availability and leadership.

use std::cmp;
use std::time::{Duration, Instant};

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::jiff::Timestamp;
use kube::api::{Api, PostParams};
use kube::{Client, Config};
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use crate::boottime::SuspendAwareInstant;
use crate::domain::{OrchState, OrchestratorIdentity};

const INITIAL_RETRY: Duration = Duration::from_millis(250);
const MAX_RETRY: Duration = Duration::from_secs(5);
const RELEASE_TIMEOUT: Duration = Duration::from_secs(1);
const RELEASED_LEASE_DURATION_SECONDS: i32 = 1;
const OWNER_API_VERSION: &str = "pgshard.io/v1alpha1";
const OWNER_KIND: &str = "PgShardCluster";

/// Fully validated settings for one orchestrator coordination supervisor.
#[derive(Clone, Debug)]
pub struct CoordinationConfig {
    namespace: String,
    lease_name: String,
    identity: OrchestratorIdentity,
    cluster_uid: String,
    pod_uid: String,
    lease_duration: Duration,
    retry_period: Duration,
    request_timeout: Duration,
}

impl CoordinationConfig {
    /// Creates settings after checking the standalone caller supplied the same
    /// bounds enforced by [`crate::config::OrchConfig`].
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe identities or timing.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        namespace: String,
        lease_name: String,
        identity: OrchestratorIdentity,
        cluster_uid: String,
        pod_uid: String,
        lease_duration: Duration,
        retry_period: Duration,
        request_timeout: Duration,
    ) -> Result<Self, CoordinationError> {
        let dns_label = |value: &str| {
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
        };
        let uid = |value: &str| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        };
        let lease_seconds = lease_duration.as_secs();
        let request_millis = u64::try_from(request_timeout.as_millis()).unwrap_or(u64::MAX);
        let retry_millis = u64::try_from(retry_period.as_millis()).unwrap_or(u64::MAX);
        if !dns_label(&namespace)
            || !dns_label(&lease_name)
            || !dns_label(&identity.cluster_id)
            || !dns_label(&identity.orchestrator_id)
            || !uid(&cluster_uid)
            || !uid(&pod_uid)
            || lease_duration.subsec_nanos() != 0
            || !(6..=300).contains(&lease_seconds)
            || !(100..=5_000).contains(&request_millis)
            || !(100..=30_000).contains(&retry_millis)
            || request_millis > lease_seconds.saturating_mul(1_000) / 3
            || retry_millis > lease_seconds.saturating_mul(1_000) / 3
        {
            return Err(CoordinationError::InvalidSettings);
        }
        Ok(Self {
            namespace,
            lease_name,
            identity,
            cluster_uid,
            pod_uid,
            lease_duration,
            retry_period,
            request_timeout,
        })
    }

    fn holder_identity(&self) -> String {
        format!(
            "{}/{}/{}",
            self.identity.orchestrator_id,
            self.pod_uid,
            Uuid::new_v4()
        )
    }

    fn lease_duration_seconds(&self) -> i32 {
        i32::try_from(self.lease_duration.as_secs())
            .expect("validated Kubernetes Lease duration fits i32")
    }
}

/// Maintains API availability and exclusive orchestrator leadership through an
/// operator-owned `coordination.k8s.io/v1` Lease.
///
/// Every process becomes ready after an authoritative API read of the exact
/// cluster-owned Lease. Only the holder records leadership. A candidate never
/// compares the holder's wall clock: it takes an occupied Lease only after the
/// holder and renewal record remain byte-for-byte unchanged for a full locally
/// measured Lease duration. Every claim and renewal is a resource-version
/// conditional replacement.
///
/// # Errors
///
/// Returns only for a permanent configuration, identity, protocol, or evidence
/// violation, or after shutdown.
pub async fn supervise(
    config: CoordinationConfig,
    state: OrchState,
    shutdown: watch::Receiver<bool>,
) -> Result<(), CoordinationError> {
    let store = KubernetesLeaseStore::new(&config)?;
    supervise_with_store(&store, &config, state, shutdown).await
}

async fn supervise_with_store<S: LeaseStore>(
    store: &S,
    config: &CoordinationConfig,
    state: OrchState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), CoordinationError> {
    state.record_coordination_unavailable();
    let holder_identity = config.holder_identity();
    let mut observed_holder = None;
    let mut retry = INITIAL_RETRY;
    let mut previously_leader = false;

    loop {
        if stopping(&shutdown) {
            break;
        }
        let result = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            result = reconcile_once(
                store,
                config,
                &holder_identity,
                &mut observed_holder,
                &state,
            ) => result,
        };

        let delay = match result {
            Ok(observation) => {
                if !state.record_coordination_ready(
                    &observation.lease_uid,
                    &observation.resource_version,
                    observation.leader,
                    observation.valid_until,
                ) {
                    state.record_coordination_unavailable();
                    return Err(CoordinationError::StateEvidenceRejected);
                }
                if observation.leader && !previously_leader {
                    tracing::info!(
                        lease = %config.lease_name,
                        namespace = %config.namespace,
                        "orchestrator leadership acquired"
                    );
                } else if !observation.leader && previously_leader {
                    tracing::warn!(
                        lease = %config.lease_name,
                        namespace = %config.namespace,
                        "orchestrator leadership lost"
                    );
                }
                previously_leader = observation.leader;
                retry = INITIAL_RETRY;
                observation.delay
            }
            Err(error) if !error.is_permanent() => {
                state.record_coordination_unavailable();
                previously_leader = false;
                tracing::warn!(reason = %error, "Kubernetes Lease coordination unavailable");
                let delay = retry;
                retry = retry.saturating_mul(2).min(MAX_RETRY);
                delay
            }
            Err(error) => {
                state.record_coordination_unavailable();
                return Err(error);
            }
        };

        if wait_or_stop(&mut shutdown, delay).await {
            break;
        }
    }

    state.record_coordination_unavailable();
    best_effort_release(store, config, &holder_identity).await;
    Ok(())
}

async fn reconcile_once<S: LeaseStore>(
    store: &S,
    config: &CoordinationConfig,
    holder_identity: &str,
    observed_holder: &mut Option<ObservedHolder>,
    state: &OrchState,
) -> Result<CoordinationObservation, CoordinationError> {
    // Anchor follower readiness before the authoritative API read so request
    // delay and host suspend both consume, but never extend, its lifetime.
    let observation_deadline = state
        .suspend_aware_deadline_after(config.lease_duration)
        .map_err(|_| CoordinationError::AuthorityClockUnavailable)?;
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
            state,
        )
        .await;
    }

    if current_holder.is_none() {
        *observed_holder = None;
        return replace_as_holder(store, config, lease, evidence, holder_identity, true, state)
            .await;
    }

    let occupied_duration = occupied_lease_duration(&spec)?;
    let record = HolderRecord {
        identity: current_holder.expect("occupied holder checked").to_owned(),
        renew_time: spec.renew_time,
        lease_duration_seconds: spec.lease_duration_seconds,
    };
    let now = Instant::now();
    let unchanged_since = match observed_holder {
        Some(observed) if observed.record == record => observed.unchanged_since,
        _ => {
            *observed_holder = Some(ObservedHolder {
                record,
                unchanged_since: now,
            });
            return Ok(CoordinationObservation::follower(
                evidence,
                config.retry_period,
                observation_deadline,
            ));
        }
    };
    let takeover_delay = cmp::max(config.lease_duration, occupied_duration);
    let elapsed = now.saturating_duration_since(unchanged_since);
    if elapsed < takeover_delay {
        return Ok(CoordinationObservation::follower(
            evidence,
            cmp::min(config.retry_period, takeover_delay.saturating_sub(elapsed)),
            observation_deadline,
        ));
    }

    *observed_holder = None;
    replace_as_holder(store, config, lease, evidence, holder_identity, true, state).await
}

async fn replace_as_holder<S: LeaseStore>(
    store: &S,
    config: &CoordinationConfig,
    mut lease: Lease,
    previous: LeaseEvidence,
    holder_identity: &str,
    transition: bool,
    state: &OrchState,
) -> Result<CoordinationObservation, CoordinationError> {
    let now = MicroTime(Timestamp::now());
    let mut spec = lease.spec.take().unwrap_or_default();
    if spec.preferred_holder.is_some() || spec.strategy.is_some() {
        return Err(CoordinationError::UnsupportedCoordinatedElection);
    }
    let transitions = validated_transitions(spec.lease_transitions)?;
    if transition {
        spec.acquire_time = Some(now.clone());
        spec.lease_transitions = Some(
            transitions
                .checked_add(1)
                .ok_or(CoordinationError::LeaseTransitionOverflow)?,
        );
    }
    spec.holder_identity = Some(holder_identity.to_owned());
    spec.lease_duration_seconds = Some(config.lease_duration_seconds());
    spec.renew_time = Some(now.clone());
    lease.spec = Some(spec);

    // A committed replacement can be followed by an arbitrarily delayed API
    // response. Anchor local authority before dispatch so response latency can
    // consume, but never extend, the Lease validity window.
    let valid_until = state
        .suspend_aware_deadline_after(config.lease_duration)
        .map_err(|_| CoordinationError::AuthorityClockUnavailable)?;
    let updated = store.replace(&lease).await?;
    let evidence = validate_lease(&updated, config)?;
    let updated_spec = updated
        .spec
        .as_ref()
        .ok_or(CoordinationError::InvalidLeaseSpec)?;
    if evidence.lease_uid != previous.lease_uid
        || evidence.resource_version == previous.resource_version
        || updated_spec.holder_identity.as_deref() != Some(holder_identity)
        || updated_spec.lease_duration_seconds != Some(config.lease_duration_seconds())
        // Kubernetes stores Lease timestamps at microsecond precision. The
        // API server may therefore normalize the value sent by this process;
        // exact equality with `now` would reject a successful CAS update.
        // The returned UID, new resource version, process-unique holder, and
        // populated renewal record are the authoritative write evidence.
        || updated_spec.renew_time.is_none()
    {
        return Err(CoordinationError::StateEvidenceRejected);
    }
    Ok(CoordinationObservation {
        lease_uid: evidence.lease_uid,
        resource_version: evidence.resource_version,
        leader: true,
        valid_until,
        delay: config.lease_duration / 3,
    })
}

fn occupied_lease_duration(spec: &LeaseSpec) -> Result<Duration, CoordinationError> {
    let seconds = spec
        .lease_duration_seconds
        .ok_or(CoordinationError::InvalidLeaseSpec)?;
    if !(1..=300).contains(&seconds) || spec.renew_time.is_none() {
        return Err(CoordinationError::InvalidLeaseSpec);
    }
    Ok(Duration::from_secs(
        u64::try_from(seconds).map_err(|_| CoordinationError::InvalidLeaseSpec)?,
    ))
}

fn validated_transitions(transitions: Option<i32>) -> Result<i32, CoordinationError> {
    let transitions = transitions.unwrap_or_default();
    if transitions < 0 {
        return Err(CoordinationError::InvalidLeaseSpec);
    }
    Ok(transitions)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HolderRecord {
    identity: String,
    renew_time: Option<MicroTime>,
    lease_duration_seconds: Option<i32>,
}

#[derive(Clone, Debug)]
struct ObservedHolder {
    record: HolderRecord,
    unchanged_since: Instant,
}

struct CoordinationObservation {
    lease_uid: String,
    resource_version: String,
    leader: bool,
    valid_until: SuspendAwareInstant,
    delay: Duration,
}

impl CoordinationObservation {
    fn follower(
        evidence: LeaseEvidence,
        delay: Duration,
        valid_until: SuspendAwareInstant,
    ) -> Self {
        Self {
            lease_uid: evidence.lease_uid,
            resource_version: evidence.resource_version,
            leader: false,
            valid_until,
            delay,
        }
    }
}

#[derive(Clone, Debug)]
struct LeaseEvidence {
    lease_uid: String,
    resource_version: String,
}

fn validate_lease(
    lease: &Lease,
    config: &CoordinationConfig,
) -> Result<LeaseEvidence, CoordinationError> {
    if lease.metadata.name.as_deref() != Some(&config.lease_name)
        || lease.metadata.namespace.as_deref() != Some(&config.namespace)
        || lease.metadata.deletion_timestamp.is_some()
    {
        return Err(CoordinationError::LeaseIdentityMismatch);
    }
    let lease_uid = lease
        .metadata
        .uid
        .as_ref()
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or(CoordinationError::LeaseIdentityMismatch)?;
    let resource_version = lease
        .metadata
        .resource_version
        .as_ref()
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or(CoordinationError::LeaseIdentityMismatch)?;
    let controllers: Vec<_> = lease
        .metadata
        .owner_references
        .iter()
        .flatten()
        .filter(|owner| owner.controller == Some(true))
        .collect();
    if controllers.len() != 1 {
        return Err(CoordinationError::LeaseOwnershipMismatch);
    }
    let owner = controllers[0];
    if owner.api_version != OWNER_API_VERSION
        || owner.kind != OWNER_KIND
        || owner.name != config.identity.cluster_id
        || owner.uid != config.cluster_uid
    {
        return Err(CoordinationError::LeaseOwnershipMismatch);
    }
    if lease
        .spec
        .as_ref()
        .is_some_and(|spec| spec.preferred_holder.is_some() || spec.strategy.is_some())
    {
        return Err(CoordinationError::UnsupportedCoordinatedElection);
    }
    Ok(LeaseEvidence {
        lease_uid,
        resource_version,
    })
}

async fn best_effort_release<S: LeaseStore>(
    store: &S,
    config: &CoordinationConfig,
    holder_identity: &str,
) {
    let release = async {
        let mut lease = store.get().await?;
        validate_lease(&lease, config)?;
        let mut spec = lease.spec.take().unwrap_or_default();
        if spec.holder_identity.as_deref() != Some(holder_identity) {
            return Ok::<(), CoordinationError>(());
        }
        let now = MicroTime(Timestamp::now());
        spec.holder_identity = None;
        spec.lease_duration_seconds = Some(RELEASED_LEASE_DURATION_SECONDS);
        spec.acquire_time = Some(now.clone());
        spec.renew_time = Some(now);
        lease.spec = Some(spec);
        store.replace(&lease).await?;
        Ok(())
    };
    match tokio::time::timeout(RELEASE_TIMEOUT, release).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(reason = %error, "could not release Kubernetes Lease during shutdown");
        }
        Err(_) => tracing::warn!("Kubernetes Lease release exceeded shutdown bound"),
    }
}

trait LeaseStore: Send + Sync {
    async fn get(&self) -> Result<Lease, CoordinationError>;
    async fn replace(&self, lease: &Lease) -> Result<Lease, CoordinationError>;
}

struct KubernetesLeaseStore {
    api: Api<Lease>,
    name: String,
    request_timeout: Duration,
}

impl KubernetesLeaseStore {
    fn new(config: &CoordinationConfig) -> Result<Self, CoordinationError> {
        let mut client_config = Config::incluster()
            .map_err(|error| CoordinationError::InClusterConfiguration(error.to_string()))?;
        client_config.connect_timeout = Some(config.request_timeout);
        client_config.read_timeout = Some(config.request_timeout);
        client_config.write_timeout = Some(config.request_timeout);
        client_config.default_retry = false;
        let client = Client::try_from(client_config)
            .map_err(|error| CoordinationError::KubernetesClient(error.to_string()))?;
        Ok(Self {
            api: Api::namespaced(client, &config.namespace),
            name: config.lease_name.clone(),
            request_timeout: config.request_timeout,
        })
    }
}

impl LeaseStore for KubernetesLeaseStore {
    async fn get(&self) -> Result<Lease, CoordinationError> {
        match tokio::time::timeout(self.request_timeout, self.api.get(&self.name)).await {
            Ok(Ok(lease)) => Ok(lease),
            Ok(Err(source)) => Err(CoordinationError::Kubernetes {
                operation: "read Lease",
                source: Box::new(source),
            }),
            Err(_) => Err(CoordinationError::RequestTimedOut("read Lease")),
        }
    }

    async fn replace(&self, lease: &Lease) -> Result<Lease, CoordinationError> {
        match tokio::time::timeout(
            self.request_timeout,
            self.api.replace(&self.name, &PostParams::default(), lease),
        )
        .await
        {
            Ok(Ok(updated)) => Ok(updated),
            Ok(Err(source)) => Err(CoordinationError::Kubernetes {
                operation: "replace Lease",
                source: Box::new(source),
            }),
            Err(_) => Err(CoordinationError::RequestTimedOut("replace Lease")),
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

/// Kubernetes Lease coordination failure.
#[derive(Debug, Error)]
pub enum CoordinationError {
    /// Standalone configuration bypassed validated bounds.
    #[error("invalid Kubernetes Lease coordination settings")]
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
    /// The Lease name, namespace, UID, resource version, or lifecycle is invalid.
    #[error("Kubernetes Lease API identity does not match the configured object")]
    LeaseIdentityMismatch,
    /// The Lease is not controlled by the exact `PgShardCluster` UID.
    #[error("Kubernetes Lease is not owned by the configured PgShardCluster UID")]
    LeaseOwnershipMismatch,
    /// The Lease spec cannot support safe local observation.
    #[error("Kubernetes Lease has an invalid holder, duration, renewal, or transition record")]
    InvalidLeaseSpec,
    /// Coordinated leader-election fields would change the ownership protocol.
    #[error("Kubernetes coordinated leader-election fields are not supported")]
    UnsupportedCoordinatedElection,
    /// The transition counter cannot advance without wrapping.
    #[error("Kubernetes Lease transition counter is exhausted")]
    LeaseTransitionOverflow,
    /// The API response contradicted the exact resource-version write.
    #[error("Kubernetes Lease update response rejected local coordination evidence")]
    StateEvidenceRejected,
    /// Linux suspend-aware time could not be observed safely.
    #[error("Linux suspend-aware coordination clock is unavailable")]
    AuthorityClockUnavailable,
}

impl CoordinationError {
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
            | Self::UnsupportedCoordinatedElection
            | Self::LeaseTransitionOverflow
            | Self::StateEvidenceRejected
            | Self::AuthorityClockUnavailable => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference, Time};

    use super::*;
    use crate::boottime::{BoottimeInstant, FakeBoottimeClock};

    fn config() -> CoordinationConfig {
        CoordinationConfig::new(
            "database".to_owned(),
            "demo-orchestrator-leader".to_owned(),
            OrchestratorIdentity {
                cluster_id: "demo".to_owned(),
                orchestrator_id: "demo-orchestrator-abc12".to_owned(),
            },
            "11111111-2222-3333-4444-555555555555".to_owned(),
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_owned(),
            Duration::from_secs(6),
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .expect("valid coordination config")
    }

    fn lease(config: &CoordinationConfig) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(config.lease_name.clone()),
                namespace: Some(config.namespace.clone()),
                uid: Some("lease-uid-1".to_owned()),
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
            spec: None,
        }
    }

    struct MemoryStore {
        lease: Mutex<Lease>,
        normalize_renew_time: bool,
    }

    impl MemoryStore {
        fn new(lease: Lease) -> Self {
            Self {
                lease: Mutex::new(lease),
                normalize_renew_time: false,
            }
        }

        fn normalizing_renew_time(lease: Lease) -> Self {
            Self {
                lease: Mutex::new(lease),
                normalize_renew_time: true,
            }
        }
    }

    impl LeaseStore for MemoryStore {
        async fn get(&self) -> Result<Lease, CoordinationError> {
            Ok(self.lease.lock().expect("lock").clone())
        }

        async fn replace(&self, lease: &Lease) -> Result<Lease, CoordinationError> {
            let mut current = self.lease.lock().expect("lock");
            if current.metadata.resource_version != lease.metadata.resource_version {
                return Err(CoordinationError::StateEvidenceRejected);
            }
            let next = current
                .metadata
                .resource_version
                .as_deref()
                .expect("resource version")
                .parse::<u64>()
                .expect("numeric test resource version")
                + 1;
            *current = lease.clone();
            current.metadata.resource_version = Some(next.to_string());
            if self.normalize_renew_time {
                current
                    .spec
                    .as_mut()
                    .expect("replacement has a spec")
                    .renew_time = Some(MicroTime(
                    "2026-01-01T00:00:00.123456Z"
                        .parse()
                        .expect("fixed Kubernetes MicroTime"),
                ));
            }
            Ok(current.clone())
        }
    }

    struct DelayedReplaceResponseStore {
        inner: MemoryStore,
        response_delay: Duration,
        committed_at: Mutex<Option<Instant>>,
        suspend_clock: Option<Arc<FakeBoottimeClock>>,
    }

    struct SuspendingGetStore {
        inner: MemoryStore,
        clock: Arc<FakeBoottimeClock>,
    }

    impl LeaseStore for DelayedReplaceResponseStore {
        async fn get(&self) -> Result<Lease, CoordinationError> {
            self.inner.get().await
        }

        async fn replace(&self, lease: &Lease) -> Result<Lease, CoordinationError> {
            let committed = self.inner.replace(lease).await?;
            *self.committed_at.lock().expect("lock commit time") = Some(Instant::now());
            if let Some(clock) = &self.suspend_clock {
                clock
                    .advance(Duration::from_secs(7))
                    .expect("advance across Lease validity");
            }
            tokio::time::sleep(self.response_delay).await;
            Ok(committed)
        }
    }

    impl LeaseStore for SuspendingGetStore {
        async fn get(&self) -> Result<Lease, CoordinationError> {
            self.clock
                .advance(Duration::from_secs(7))
                .expect("advance across Lease observation");
            self.inner.get().await
        }

        async fn replace(&self, lease: &Lease) -> Result<Lease, CoordinationError> {
            self.inner.replace(lease).await
        }
    }

    #[test]
    fn rejects_recreated_or_foreign_owned_lease() {
        let config = config();
        let mut candidate = lease(&config);
        assert!(validate_lease(&candidate, &config).is_ok());

        candidate
            .metadata
            .owner_references
            .as_mut()
            .expect("owner references")[0]
            .uid = "different".to_owned();
        assert!(matches!(
            validate_lease(&candidate, &config),
            Err(CoordinationError::LeaseOwnershipMismatch)
        ));
        candidate = lease(&config);
        candidate.metadata.deletion_timestamp = Some(Time(Timestamp::now()));
        assert!(matches!(
            validate_lease(&candidate, &config),
            Err(CoordinationError::LeaseIdentityMismatch)
        ));
    }

    #[tokio::test]
    async fn empty_lease_is_claimed_with_resource_version_cas() {
        let config = config();
        let state = OrchState::with_identity(config.identity.clone(), 15_000).expect("valid state");
        let store = MemoryStore::new(lease(&config));
        let holder = config.holder_identity();
        let observation = reconcile_once(&store, &config, &holder, &mut None, &state)
            .await
            .expect("claim Lease");
        assert!(observation.leader);
        assert_eq!(observation.resource_version, "2");
        let current = store.get().await.expect("read claimed Lease");
        assert_eq!(
            current
                .spec
                .as_ref()
                .and_then(|spec| spec.holder_identity.as_deref()),
            Some(holder.as_str())
        );
        assert_eq!(
            current
                .spec
                .as_ref()
                .and_then(|spec| spec.lease_transitions),
            Some(1)
        );
    }

    #[tokio::test]
    async fn accepts_api_server_microtime_normalization_after_cas() {
        let config = config();
        let state = OrchState::with_identity(config.identity.clone(), 15_000).expect("valid state");
        let store = MemoryStore::normalizing_renew_time(lease(&config));
        let holder = config.holder_identity();

        let observation = reconcile_once(&store, &config, &holder, &mut None, &state)
            .await
            .expect("accept normalized API response");

        assert!(observation.leader);
        assert_eq!(observation.resource_version, "2");
    }

    #[tokio::test]
    async fn delayed_committed_replace_response_cannot_extend_leadership() {
        let config = config();
        let state = OrchState::with_identity(config.identity.clone(), 15_000).expect("valid state");
        let store = DelayedReplaceResponseStore {
            inner: MemoryStore::new(lease(&config)),
            response_delay: Duration::from_millis(150),
            committed_at: Mutex::new(None),
            suspend_clock: None,
        };
        let holder = config.holder_identity();
        let observation = reconcile_once(&store, &config, &holder, &mut None, &state)
            .await
            .expect("receive delayed committed replacement");

        assert!(observation.leader);
        let committed_at = store
            .committed_at
            .lock()
            .expect("lock commit time")
            .expect("store recorded commit time");
        assert!(
            observation.valid_until.monotonic <= committed_at + config.lease_duration,
            "local leadership must not begin after the committed replacement"
        );
        assert!(state.record_coordination_ready(
            &observation.lease_uid,
            &observation.resource_version,
            observation.leader,
            observation.valid_until,
        ));
        assert!(state.snapshot().leader);
    }

    #[tokio::test]
    async fn suspend_during_authoritative_get_cannot_install_follower_readiness() {
        let config = config();
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = OrchState::with_identity_and_clock_for_test(
            config.identity.clone(),
            15_000,
            clock.clone(),
        )
        .expect("valid state");
        let mut occupied = lease(&config);
        occupied.spec = Some(LeaseSpec {
            holder_identity: Some("foreign/pod/process".to_owned()),
            lease_duration_seconds: Some(6),
            renew_time: Some(MicroTime(Timestamp::now())),
            lease_transitions: Some(1),
            ..LeaseSpec::default()
        });
        let store = SuspendingGetStore {
            inner: MemoryStore::new(occupied),
            clock,
        };

        let observation = reconcile_once(
            &store,
            &config,
            &config.holder_identity(),
            &mut None,
            &state,
        )
        .await
        .expect("observe foreign holder");

        assert!(!observation.leader);
        assert!(!state.record_coordination_ready(
            &observation.lease_uid,
            &observation.resource_version,
            observation.leader,
            observation.valid_until,
        ));
        assert!(!state.snapshot().coordination_ready);
    }

    #[tokio::test]
    async fn suspend_during_committed_replace_response_cannot_install_leadership() {
        let config = config();
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = OrchState::with_identity_and_clock_for_test(
            config.identity.clone(),
            15_000,
            clock.clone(),
        )
        .expect("valid state");
        let store = DelayedReplaceResponseStore {
            inner: MemoryStore::new(lease(&config)),
            response_delay: Duration::ZERO,
            committed_at: Mutex::new(None),
            suspend_clock: Some(clock),
        };

        let observation = reconcile_once(
            &store,
            &config,
            &config.holder_identity(),
            &mut None,
            &state,
        )
        .await
        .expect("receive committed replacement");

        assert!(observation.leader);
        assert!(!state.record_coordination_ready(
            &observation.lease_uid,
            &observation.resource_version,
            observation.leader,
            observation.valid_until,
        ));
        assert!(!state.snapshot().coordination_ready);
    }

    #[tokio::test]
    async fn foreign_holder_requires_unchanged_local_observation_window() {
        let config = config();
        let state = OrchState::with_identity(config.identity.clone(), 15_000).expect("valid state");
        let mut occupied = lease(&config);
        occupied.spec = Some(LeaseSpec {
            holder_identity: Some("foreign/pod/process".to_owned()),
            lease_duration_seconds: Some(1),
            renew_time: Some(MicroTime(Timestamp::now())),
            lease_transitions: Some(7),
            ..LeaseSpec::default()
        });
        let store = MemoryStore::new(occupied);
        let holder = config.holder_identity();
        let mut observed = None;

        let first = reconcile_once(&store, &config, &holder, &mut observed, &state)
            .await
            .expect("observe foreign holder");
        assert!(!first.leader);
        observed.as_mut().expect("observation").unchanged_since = Instant::now()
            .checked_sub(Duration::from_secs(5))
            .expect("test Instant supports a five-second subtraction");
        let second = reconcile_once(&store, &config, &holder, &mut observed, &state)
            .await
            .expect("continue observing foreign holder");
        assert!(!second.leader, "local six-second duration must be honored");

        observed.as_mut().expect("observation").unchanged_since = Instant::now()
            .checked_sub(Duration::from_secs(6))
            .expect("test Instant supports a six-second subtraction");
        let claimed = reconcile_once(&store, &config, &holder, &mut observed, &state)
            .await
            .expect("claim expired foreign holder");
        assert!(claimed.leader);
    }

    #[tokio::test]
    async fn shutdown_release_clears_only_our_holder() {
        let config = config();
        let state = OrchState::with_identity(config.identity.clone(), 15_000).expect("valid state");
        let store = MemoryStore::new(lease(&config));
        let holder = config.holder_identity();
        reconcile_once(&store, &config, &holder, &mut None, &state)
            .await
            .expect("claim Lease");
        best_effort_release(&store, &config, &holder).await;
        let released = store.get().await.expect("read released Lease");
        let spec = released.spec.expect("released spec");
        assert!(spec.holder_identity.is_none());
        assert_eq!(
            spec.lease_duration_seconds,
            Some(RELEASED_LEASE_DURATION_SECONDS)
        );
    }

    struct PendingStore;

    impl LeaseStore for PendingStore {
        async fn get(&self) -> Result<Lease, CoordinationError> {
            std::future::pending().await
        }

        async fn replace(&self, _lease: &Lease) -> Result<Lease, CoordinationError> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_a_stalled_api_operation() {
        let config = config();
        let state = OrchState::with_identity(config.identity.clone(), 15_000).expect("valid state");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let supervisor = tokio::spawn(async move {
            supervise_with_store(&PendingStore, &config, state, shutdown_rx).await
        });
        tokio::task::yield_now().await;
        shutdown_tx.send(true).expect("send shutdown");
        tokio::time::timeout(Duration::from_secs(2), supervisor)
            .await
            .expect("bounded shutdown")
            .expect("supervisor task")
            .expect("clean shutdown");
    }
}
