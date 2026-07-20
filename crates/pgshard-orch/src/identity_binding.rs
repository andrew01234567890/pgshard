//! Read-only binding of discovery targets to live Kubernetes object identities.
//!
//! A successful observation is diagnostic evidence only. It does not grant a
//! runtime role, readiness, serving state, routing eligibility, promotion
//! permission, or writable authority. Detailed object identities remain local
//! to one reconciliation and are discarded immediately afterward.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::time::Duration;

use futures_util::stream::{self, StreamExt, TryStreamExt};
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{EndpointAddress, EndpointPort, Endpoints, Pod};
use kube::api::Api;
use kube::{Client, Config};
use thiserror::Error;
use tokio::sync::watch;

use crate::domain::OrchState;
use crate::topology::UnboundAgentObservationTarget;

const CLUSTER_LABEL: &str = "pgshard.io/cluster";
const COMPONENT_LABEL: &str = "app.kubernetes.io/component";
const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
const SHARD_LABEL: &str = "pgshard.io/shard";
const MEMBER_LABEL: &str = "pgshard.io/member";
const CLUSTER_UID_ANNOTATION: &str = "pgshard.io/postgresql-cluster-uid";
const MANAGED_BY_VALUE: &str = "pgshard-operator";
const OWNER_API_VERSION: &str = "pgshard.io/v1alpha1";
const OWNER_KIND: &str = "PgShardCluster";
const PROCESS_INCARNATION_HEX_LENGTH: usize = 24;
const MAX_CONCURRENT_BINDINGS: usize = 64;

/// Repeatedly observes the complete finite target set without retaining stale
/// evidence between attempts.
pub async fn supervise(
    targets: Vec<UnboundAgentObservationTarget>,
    state: OrchState,
    mut shutdown: watch::Receiver<bool>,
    request_timeout: Duration,
    retry_period: Duration,
    freshness: Duration,
) {
    state.record_agent_identity_binding(None);
    let store = match KubernetesIdentityStore::new(&targets, request_timeout) {
        Ok(store) => store,
        Err(error) => {
            tracing::warn!(reason = %error, "Kubernetes identity binding disabled");
            wait_until_shutdown(&mut shutdown).await;
            return;
        }
    };

    loop {
        if *shutdown.borrow() {
            break;
        }
        // Clear first: an earlier observation is never treated as last-good
        // evidence while a newer collection is incomplete.
        let result = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            result = observe_once(&store, &targets, &state, freshness) => result,
        };
        match result {
            Ok(()) => {}
            Err(error) => {
                tracing::warn!(reason = %error, "Kubernetes identity binding unavailable");
            }
        }
        if wait_or_stop(&mut shutdown, retry_period).await {
            break;
        }
    }
    state.record_agent_identity_binding(None);
}

async fn observe_once<S: IdentityStore>(
    store: &S,
    targets: &[UnboundAgentObservationTarget],
    state: &OrchState,
    freshness: Duration,
) -> Result<(), IdentityBindingError> {
    observe_once_with_clock(store, targets, state, freshness, std::time::Instant::now).await
}

async fn observe_once_with_clock<S: IdentityStore, F: FnMut() -> std::time::Instant>(
    store: &S,
    targets: &[UnboundAgentObservationTarget],
    state: &OrchState,
    freshness: Duration,
    mut clock: F,
) -> Result<(), IdentityBindingError> {
    state.record_agent_identity_binding(None);
    let deadline = clock()
        .checked_add(freshness)
        .ok_or(IdentityBindingError::InvalidFreshnessBound)?;
    match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        bind_once(store, targets),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => return Err(IdentityBindingError::FreshnessBoundExceeded(freshness)),
    }
    if clock() >= deadline {
        return Err(IdentityBindingError::FreshnessBoundExceeded(freshness));
    }
    state.record_agent_identity_binding(Some(deadline));
    Ok(())
}

async fn wait_until_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
}

async fn wait_or_stop(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        () = tokio::time::sleep(duration) => false,
        result = shutdown.changed() => result.is_err() || *shutdown.borrow(),
    }
}

async fn bind_once<S: IdentityStore>(
    store: &S,
    targets: &[UnboundAgentObservationTarget],
) -> Result<(), IdentityBindingError> {
    if targets.is_empty() {
        return Err(IdentityBindingError::InvalidTargetSet);
    }
    let shards = group_shards(targets)?;
    let mut pods = HashMap::with_capacity(targets.len());
    let mut stateful_set_uids = HashSet::with_capacity(targets.len());
    let mut pod_uids = HashSet::with_capacity(targets.len());
    let mut pod_ips = HashSet::with_capacity(targets.len());
    let members = stream::iter(targets)
        .map(|target| bind_member(store, target))
        .buffer_unordered(MAX_CONCURRENT_BINDINGS)
        .try_collect::<Vec<_>>()
        .await?;
    for member in members {
        let target = member.target;
        let stateful_set_uid = member.stateful_set_uid;
        let identity = member.pod;
        if !stateful_set_uids.insert(stateful_set_uid.clone()) {
            return Err(IdentityBindingError::InvalidTargetSet);
        }
        if !pod_uids.insert(identity.uid.clone())
            || !pod_ips.insert(identity.ip.clone())
            || pods.insert(target.instance_id(), identity).is_some()
        {
            return Err(IdentityBindingError::InvalidTargetSet);
        }
    }
    let pods = &pods;
    stream::iter(shards)
        .map(|shard| bind_shard(store, shard, pods))
        .buffer_unordered(MAX_CONCURRENT_BINDINGS)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

struct BoundMember<'a> {
    target: &'a UnboundAgentObservationTarget,
    stateful_set_uid: String,
    pod: PodIdentity,
}

async fn bind_member<'a, S: IdentityStore>(
    store: &S,
    target: &'a UnboundAgentObservationTarget,
) -> Result<BoundMember<'a>, IdentityBindingError> {
    let stateful_set = store.get_stateful_set(target.stateful_set()).await?;
    let stateful_set_uid = validate_stateful_set(&stateful_set, target)?;
    let pod = store.get_pod(target.instance_id()).await?;
    let pod = validate_pod(&pod, target, &stateful_set_uid)?;
    Ok(BoundMember {
        target,
        stateful_set_uid,
        pod,
    })
}

fn group_shards(
    targets: &[UnboundAgentObservationTarget],
) -> Result<Vec<Vec<&UnboundAgentObservationTarget>>, IdentityBindingError> {
    let mut shards: Vec<Vec<&UnboundAgentObservationTarget>> = Vec::new();
    for target in targets {
        match shards.last_mut() {
            Some(shard) if shard[0].shard_id() == target.shard_id() => shard.push(target),
            _ => shards.push(vec![target]),
        }
    }
    let mut seen_shards = HashSet::with_capacity(shards.len());
    for shard in &shards {
        let first = shard[0];
        if !seen_shards.insert(first.shard_id())
            || shard.iter().any(|target| {
                target.cluster_id() != first.cluster_id()
                    || target.cluster_uid() != first.cluster_uid()
                    || target.namespace() != first.namespace()
                    || target.shard_service() != first.shard_service()
                    || target.writable_lease_namespace() != first.writable_lease_namespace()
                    || target.writable_lease_name() != first.writable_lease_name()
                    || target.writable_lease_uid() != first.writable_lease_uid()
            })
        {
            return Err(IdentityBindingError::InvalidTargetSet);
        }
    }
    Ok(shards)
}

async fn bind_shard<S: IdentityStore>(
    store: &S,
    shard: Vec<&UnboundAgentObservationTarget>,
    pods: &HashMap<&str, PodIdentity>,
) -> Result<(), IdentityBindingError> {
    let first = shard[0];
    let endpoints = store.get_endpoints(first.shard_service()).await?;
    validate_endpoints(&endpoints, &shard, pods)?;
    let lease = store.get_lease(first.writable_lease_name()).await?;
    validate_lease(&lease, &shard, pods)?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PodIdentity {
    uid: String,
    ip: String,
}

fn validate_stateful_set(
    stateful_set: &StatefulSet,
    target: &UnboundAgentObservationTarget,
) -> Result<String, IdentityBindingError> {
    let metadata = &stateful_set.metadata;
    if metadata.name.as_deref() != Some(target.stateful_set())
        || metadata.namespace.as_deref() != Some(target.namespace())
        || metadata.deletion_timestamp.is_some()
        || metadata.labels.as_ref().is_none_or(|labels| {
            labels.get(CLUSTER_LABEL).map(String::as_str) != Some(target.cluster_id())
                || labels.get(COMPONENT_LABEL).map(String::as_str) != Some("postgresql")
                || labels.get(MANAGED_BY_LABEL).map(String::as_str) != Some(MANAGED_BY_VALUE)
                || labels.get(SHARD_LABEL).map(String::as_str)
                    != Some(&format!("{:04}", target.shard_id()))
                || labels.get(MEMBER_LABEL).map(String::as_str)
                    != Some(&format!("{:04}", target.member_ordinal()))
        })
    {
        return Err(IdentityBindingError::StatefulSetIdentityMismatch(
            target.stateful_set().to_owned(),
        ));
    }
    require_resource_version(metadata.resource_version.as_deref())?;
    let uid = require_uid(metadata.uid.as_deref())?.to_owned();
    validate_cluster_owner(metadata.owner_references.as_deref(), target).map_err(|()| {
        IdentityBindingError::StatefulSetIdentityMismatch(target.stateful_set().to_owned())
    })?;
    Ok(uid)
}

fn validate_cluster_owner(
    owners: Option<&[k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference]>,
    target: &UnboundAgentObservationTarget,
) -> Result<(), ()> {
    let owners = owners.unwrap_or_default();
    if owners.len() != 1
        || owners[0].api_version != OWNER_API_VERSION
        || owners[0].kind != OWNER_KIND
        || owners[0].name != target.cluster_id()
        || owners[0].uid != target.cluster_uid()
        || owners[0].controller != Some(true)
        || owners[0].block_owner_deletion != Some(true)
    {
        return Err(());
    }
    Ok(())
}

fn validate_pod(
    pod: &Pod,
    target: &UnboundAgentObservationTarget,
    stateful_set_uid: &str,
) -> Result<PodIdentity, IdentityBindingError> {
    let metadata = &pod.metadata;
    if metadata.name.as_deref() != Some(target.instance_id())
        || metadata.namespace.as_deref() != Some(target.namespace())
        || metadata.deletion_timestamp.is_some()
        || metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(CLUSTER_UID_ANNOTATION))
            .map(String::as_str)
            != Some(target.cluster_uid())
        || metadata.labels.as_ref().is_none_or(|labels| {
            labels.get(CLUSTER_LABEL).map(String::as_str) != Some(target.cluster_id())
                || labels.get(COMPONENT_LABEL).map(String::as_str) != Some("postgresql")
                || labels.get(MANAGED_BY_LABEL).map(String::as_str) != Some(MANAGED_BY_VALUE)
                || labels.get(SHARD_LABEL).map(String::as_str)
                    != Some(&format!("{:04}", target.shard_id()))
                || labels.get(MEMBER_LABEL).map(String::as_str)
                    != Some(&format!("{:04}", target.member_ordinal()))
        })
    {
        return Err(IdentityBindingError::PodIdentityMismatch(
            target.instance_id().to_owned(),
        ));
    }
    require_resource_version(metadata.resource_version.as_deref())?;
    let uid = require_uid(metadata.uid.as_deref())?.to_owned();
    let owners = metadata.owner_references.as_deref().unwrap_or_default();
    if owners.len() != 1
        || owners[0].api_version != "apps/v1"
        || owners[0].kind != "StatefulSet"
        || owners[0].name != target.stateful_set()
        || owners[0].uid != stateful_set_uid
        || owners[0].controller != Some(true)
        || owners[0].block_owner_deletion != Some(true)
    {
        return Err(IdentityBindingError::PodIdentityMismatch(
            target.instance_id().to_owned(),
        ));
    }
    let workload = target
        .writable_lease_name()
        .strip_suffix("-term")
        .ok_or(IdentityBindingError::InvalidTargetSet)?;
    let expected_service_account = if target.member_ordinal() == 0 {
        format!("{workload}-agent")
    } else {
        format!("{workload}-standby")
    };
    if pod
        .spec
        .as_ref()
        .and_then(|spec| spec.service_account_name.as_deref())
        != Some(expected_service_account.as_str())
    {
        return Err(IdentityBindingError::PodIdentityMismatch(
            target.instance_id().to_owned(),
        ));
    }
    let ip = pod
        .status
        .as_ref()
        .and_then(|status| status.pod_ip.as_deref())
        .filter(|value| value.parse::<IpAddr>().is_ok())
        .ok_or_else(|| IdentityBindingError::PodIdentityMismatch(target.instance_id().to_owned()))?
        .to_owned();
    Ok(PodIdentity { uid, ip })
}

fn validate_endpoints(
    endpoints: &Endpoints,
    targets: &[&UnboundAgentObservationTarget],
    pods: &HashMap<&str, PodIdentity>,
) -> Result<(), IdentityBindingError> {
    let first = targets[0];
    let metadata = &endpoints.metadata;
    if metadata.name.as_deref() != Some(first.shard_service())
        || metadata.namespace.as_deref() != Some(first.namespace())
        || metadata.deletion_timestamp.is_some()
    {
        return Err(IdentityBindingError::EndpointIdentityMismatch(
            first.shard_service().to_owned(),
        ));
    }
    require_uid(metadata.uid.as_deref())?;
    require_resource_version(metadata.resource_version.as_deref())?;

    let expected: HashMap<_, _> = targets
        .iter()
        .map(|target| {
            let pod = pods
                .get(target.instance_id())
                .expect("validated Pod set contains every target");
            (target.instance_id(), pod)
        })
        .collect();
    let mut observed = HashSet::with_capacity(expected.len());
    let subsets = endpoints.subsets.as_deref().unwrap_or_default();
    if subsets.is_empty() {
        return Err(IdentityBindingError::EndpointIdentityMismatch(
            first.shard_service().to_owned(),
        ));
    }
    for subset in subsets {
        validate_endpoint_ports(subset.ports.as_deref().unwrap_or_default(), first)?;
        let addresses = subset
            .addresses
            .iter()
            .flatten()
            .chain(subset.not_ready_addresses.iter().flatten());
        let mut subset_count = 0_usize;
        for address in addresses {
            subset_count += 1;
            let name = validate_endpoint_address(address, first, &expected)?;
            if !observed.insert(name) {
                return Err(IdentityBindingError::EndpointIdentityMismatch(
                    first.shard_service().to_owned(),
                ));
            }
        }
        if subset_count == 0 {
            return Err(IdentityBindingError::EndpointIdentityMismatch(
                first.shard_service().to_owned(),
            ));
        }
    }
    if observed.len() != expected.len() {
        return Err(IdentityBindingError::EndpointIdentityMismatch(
            first.shard_service().to_owned(),
        ));
    }
    Ok(())
}

fn validate_endpoint_ports(
    ports: &[EndpointPort],
    target: &UnboundAgentObservationTarget,
) -> Result<(), IdentityBindingError> {
    let mut observed = HashSet::with_capacity(ports.len());
    for port in ports {
        let name = port.name.as_deref().unwrap_or_default();
        let expected_port = (name == "postgresql"
            && port.port == i32::from(target.postgresql_port()))
            || (name == "agent-http" && port.port == i32::from(target.agent_http_port()));
        if port.protocol.as_deref() != Some("TCP")
            || !observed.insert((name, port.port))
            || !expected_port
        {
            return Err(IdentityBindingError::EndpointIdentityMismatch(
                target.shard_service().to_owned(),
            ));
        }
    }
    if observed.len() != 2
        || !observed.contains(&("postgresql", i32::from(target.postgresql_port())))
        || !observed.contains(&("agent-http", i32::from(target.agent_http_port())))
    {
        return Err(IdentityBindingError::EndpointIdentityMismatch(
            target.shard_service().to_owned(),
        ));
    }
    Ok(())
}

fn validate_endpoint_address<'a>(
    address: &'a EndpointAddress,
    target: &UnboundAgentObservationTarget,
    expected: &HashMap<&'a str, &'a PodIdentity>,
) -> Result<&'a str, IdentityBindingError> {
    let reference = address.target_ref.as_ref().ok_or_else(|| {
        IdentityBindingError::EndpointIdentityMismatch(target.shard_service().to_owned())
    })?;
    let name = reference.name.as_deref().unwrap_or_default();
    let pod = expected.get(name).ok_or_else(|| {
        IdentityBindingError::EndpointIdentityMismatch(target.shard_service().to_owned())
    })?;
    if reference
        .api_version
        .as_deref()
        .is_some_and(|version| version != "v1")
        || reference.kind.as_deref() != Some("Pod")
        || reference.namespace.as_deref() != Some(target.namespace())
        || reference.uid.as_deref() != Some(pod.uid.as_str())
        || address.ip != pod.ip
    {
        return Err(IdentityBindingError::EndpointIdentityMismatch(
            target.shard_service().to_owned(),
        ));
    }
    Ok(name)
}

fn validate_lease(
    lease: &Lease,
    targets: &[&UnboundAgentObservationTarget],
    pods: &HashMap<&str, PodIdentity>,
) -> Result<(), IdentityBindingError> {
    let first = targets[0];
    let metadata = &lease.metadata;
    if metadata.name.as_deref() != Some(first.writable_lease_name())
        || metadata.namespace.as_deref() != Some(first.writable_lease_namespace())
        || metadata.uid.as_deref() != Some(first.writable_lease_uid())
        || metadata.deletion_timestamp.is_some()
    {
        return Err(IdentityBindingError::LeaseIdentityMismatch(
            first.writable_lease_name().to_owned(),
        ));
    }
    require_resource_version(metadata.resource_version.as_deref())?;
    let owners: Vec<_> = metadata.owner_references.iter().flatten().collect();
    if owners.len() != 1
        || owners[0].api_version != OWNER_API_VERSION
        || owners[0].kind != OWNER_KIND
        || owners[0].name != first.cluster_id()
        || owners[0].uid != first.cluster_uid()
        || owners[0].controller != Some(true)
        || owners[0].block_owner_deletion != Some(true)
    {
        return Err(IdentityBindingError::LeaseIdentityMismatch(
            first.writable_lease_name().to_owned(),
        ));
    }
    let spec = lease.spec.as_ref();
    if spec.is_some_and(|spec| spec.preferred_holder.is_some() || spec.strategy.is_some()) {
        return Err(IdentityBindingError::LeaseIdentityMismatch(
            first.writable_lease_name().to_owned(),
        ));
    }
    if let Some(holder) = spec.and_then(|spec| spec.holder_identity.as_deref()) {
        let mut pieces = holder.split('/');
        let instance = pieces.next().unwrap_or_default();
        let pod_uid = pieces.next().unwrap_or_default();
        let incarnation = pieces.next().unwrap_or_default();
        if pieces.next().is_some()
            || incarnation.len() != PROCESS_INCARNATION_HEX_LENGTH
            || !incarnation
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || pods.get(instance).is_none_or(|pod| pod.uid != pod_uid)
            || !targets
                .iter()
                .any(|target| target.instance_id() == instance)
        {
            return Err(IdentityBindingError::LeaseIdentityMismatch(
                first.writable_lease_name().to_owned(),
            ));
        }
    }
    Ok(())
}

fn require_uid(value: Option<&str>) -> Result<&str, IdentityBindingError> {
    value
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
        .ok_or(IdentityBindingError::InvalidObjectMetadata)
}

fn require_resource_version(value: Option<&str>) -> Result<&str, IdentityBindingError> {
    value
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .ok_or(IdentityBindingError::InvalidObjectMetadata)
}

trait IdentityStore: Send + Sync {
    async fn get_stateful_set(&self, name: &str) -> Result<StatefulSet, IdentityBindingError>;
    async fn get_pod(&self, name: &str) -> Result<Pod, IdentityBindingError>;
    async fn get_endpoints(&self, name: &str) -> Result<Endpoints, IdentityBindingError>;
    async fn get_lease(&self, name: &str) -> Result<Lease, IdentityBindingError>;
}

struct KubernetesIdentityStore {
    stateful_sets: Api<StatefulSet>,
    pods: Api<Pod>,
    endpoints: Api<Endpoints>,
    leases: Api<Lease>,
    request_timeout: Duration,
}

impl KubernetesIdentityStore {
    fn new(
        targets: &[UnboundAgentObservationTarget],
        request_timeout: Duration,
    ) -> Result<Self, IdentityBindingError> {
        let namespace = targets
            .first()
            .map(UnboundAgentObservationTarget::namespace)
            .ok_or(IdentityBindingError::InvalidTargetSet)?;
        if targets.iter().any(|target| {
            target.namespace() != namespace || target.writable_lease_namespace() != namespace
        }) {
            return Err(IdentityBindingError::InvalidTargetSet);
        }
        let mut client_config = Config::incluster()
            .map_err(|error| IdentityBindingError::InClusterConfiguration(error.to_string()))?;
        client_config.connect_timeout = Some(request_timeout);
        client_config.read_timeout = Some(request_timeout);
        client_config.write_timeout = Some(request_timeout);
        client_config.default_retry = false;
        let client = Client::try_from(client_config)
            .map_err(|error| IdentityBindingError::KubernetesClient(error.to_string()))?;
        Ok(Self {
            stateful_sets: Api::namespaced(client.clone(), namespace),
            pods: Api::namespaced(client.clone(), namespace),
            endpoints: Api::namespaced(client.clone(), namespace),
            leases: Api::namespaced(client, namespace),
            request_timeout,
        })
    }

    async fn get<K>(
        &self,
        api: &Api<K>,
        name: &str,
        operation: &'static str,
    ) -> Result<K, IdentityBindingError>
    where
        K: Clone + std::fmt::Debug + serde::de::DeserializeOwned + kube::Resource<DynamicType = ()>,
    {
        match tokio::time::timeout(self.request_timeout, api.get(name)).await {
            Ok(Ok(object)) => Ok(object),
            Ok(Err(source)) => Err(IdentityBindingError::Kubernetes {
                operation,
                source: Box::new(source),
            }),
            Err(_) => Err(IdentityBindingError::RequestTimedOut(operation)),
        }
    }
}

impl IdentityStore for KubernetesIdentityStore {
    async fn get_stateful_set(&self, name: &str) -> Result<StatefulSet, IdentityBindingError> {
        self.get(&self.stateful_sets, name, "read StatefulSet")
            .await
    }

    async fn get_pod(&self, name: &str) -> Result<Pod, IdentityBindingError> {
        self.get(&self.pods, name, "read Pod").await
    }

    async fn get_endpoints(&self, name: &str) -> Result<Endpoints, IdentityBindingError> {
        self.get(&self.endpoints, name, "read Endpoints").await
    }

    async fn get_lease(&self, name: &str) -> Result<Lease, IdentityBindingError> {
        self.get(&self.leases, name, "read writable Lease").await
    }
}

/// One read-only identity observation failure.
#[derive(Debug, Error)]
enum IdentityBindingError {
    #[error("discovery target set is empty, inconsistent, or duplicated")]
    InvalidTargetSet,
    #[error("Kubernetes object UID or resource version is missing or malformed")]
    InvalidObjectMetadata,
    #[error("StatefulSet {0} does not match its exact workload identity")]
    StatefulSetIdentityMismatch(String),
    #[error("Pod {0} does not match its exact discovery identity")]
    PodIdentityMismatch(String),
    #[error("Endpoints {0} does not contain the exact member and port set")]
    EndpointIdentityMismatch(String),
    #[error("writable-term Lease {0} does not match its exact topology identity")]
    LeaseIdentityMismatch(String),
    #[error("identity-binding freshness deadline overflowed the monotonic clock")]
    InvalidFreshnessBound,
    #[error("identity-binding scan exceeded its freshness bound of {0:?}")]
    FreshnessBoundExceeded(Duration),
    #[error("in-cluster Kubernetes configuration is unavailable: {0}")]
    InClusterConfiguration(String),
    #[error("Kubernetes client initialization failed: {0}")]
    KubernetesClient(String),
    #[error("Kubernetes API request timed out while attempting to {0}")]
    RequestTimedOut(&'static str),
    #[error("Kubernetes API could not {operation}: {source}")]
    Kubernetes {
        operation: &'static str,
        #[source]
        source: Box<kube::Error>,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use k8s_openapi::api::apps::v1::StatefulSetSpec;
    use k8s_openapi::api::coordination::v1::LeaseSpec;
    use k8s_openapi::api::core::v1::{EndpointSubset, ObjectReference, PodSpec, PodStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

    use super::*;
    use crate::domain::OrchestratorIdentity;
    use crate::topology::{
        AgentStatusCollectionState, TOPOLOGY_SCHEMA_VERSION, TopologyDiagnostics,
    };

    const CLUSTER_UID: &str = "11111111-2222-3333-4444-555555555555";

    fn target(member: u32) -> UnboundAgentObservationTarget {
        target_at(0, member)
    }

    fn target_at(shard: u32, member: u32) -> UnboundAgentObservationTarget {
        let shard_service = format!("demo-shard-{shard:04}");
        let workload = if member == 0 {
            shard_service.clone()
        } else {
            format!("{shard_service}-m{member:04}")
        };
        UnboundAgentObservationTarget {
            cluster_id: "demo".to_owned(),
            cluster_uid: CLUSTER_UID.to_owned(),
            namespace: "database".to_owned(),
            shard_id: shard,
            shard_service: shard_service.clone(),
            member_ordinal: member,
            stateful_set: workload.clone(),
            instance_id: format!("{workload}-0"),
            dns_name: format!("{workload}-0.{shard_service}.database.svc"),
            agent_http_port: 8_080,
            postgresql_port: 5_432,
            physical_slot: format!("pgshard_member_{member:04}"),
            writable_lease_namespace: "database".to_owned(),
            writable_lease_name: format!("{shard_service}-term"),
            writable_lease_uid: format!("lease-uid-{shard}"),
        }
    }

    fn stateful_set(target: &UnboundAgentObservationTarget) -> StatefulSet {
        StatefulSet {
            metadata: ObjectMeta {
                name: Some(target.stateful_set().to_owned()),
                namespace: Some(target.namespace().to_owned()),
                uid: Some(format!(
                    "stateful-set-uid-{}-{}",
                    target.shard_id(),
                    target.member_ordinal()
                )),
                resource_version: Some(format!("stateful-set-rv-{}", target.member_ordinal())),
                labels: Some(BTreeMap::from([
                    (CLUSTER_LABEL.to_owned(), target.cluster_id().to_owned()),
                    (COMPONENT_LABEL.to_owned(), "postgresql".to_owned()),
                    (MANAGED_BY_LABEL.to_owned(), MANAGED_BY_VALUE.to_owned()),
                    (SHARD_LABEL.to_owned(), format!("{:04}", target.shard_id())),
                    (
                        MEMBER_LABEL.to_owned(),
                        format!("{:04}", target.member_ordinal()),
                    ),
                ])),
                annotations: Some(BTreeMap::from([(
                    CLUSTER_UID_ANNOTATION.to_owned(),
                    target.cluster_uid().to_owned(),
                )])),
                owner_references: Some(vec![OwnerReference {
                    api_version: OWNER_API_VERSION.to_owned(),
                    kind: OWNER_KIND.to_owned(),
                    name: target.cluster_id().to_owned(),
                    uid: target.cluster_uid().to_owned(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..ObjectMeta::default()
            },
            spec: Some(StatefulSetSpec::default()),
            status: None,
        }
    }

    fn pod(target: &UnboundAgentObservationTarget, stateful_set: &StatefulSet) -> Pod {
        let service_account_name = if target.member_ordinal() == 0 {
            format!("{}-agent", target.shard_service())
        } else {
            format!("{}-standby", target.shard_service())
        };
        Pod {
            metadata: ObjectMeta {
                name: Some(target.instance_id().to_owned()),
                namespace: Some(target.namespace().to_owned()),
                uid: Some(format!(
                    "pod-uid-{}-{}",
                    target.shard_id(),
                    target.member_ordinal()
                )),
                resource_version: Some(format!("pod-rv-{}", target.member_ordinal())),
                labels: Some(BTreeMap::from([
                    (CLUSTER_LABEL.to_owned(), target.cluster_id().to_owned()),
                    (COMPONENT_LABEL.to_owned(), "postgresql".to_owned()),
                    (MANAGED_BY_LABEL.to_owned(), MANAGED_BY_VALUE.to_owned()),
                    (SHARD_LABEL.to_owned(), format!("{:04}", target.shard_id())),
                    (
                        MEMBER_LABEL.to_owned(),
                        format!("{:04}", target.member_ordinal()),
                    ),
                ])),
                annotations: Some(BTreeMap::from([(
                    CLUSTER_UID_ANNOTATION.to_owned(),
                    target.cluster_uid().to_owned(),
                )])),
                owner_references: Some(vec![OwnerReference {
                    api_version: "apps/v1".to_owned(),
                    kind: "StatefulSet".to_owned(),
                    name: target.stateful_set().to_owned(),
                    uid: stateful_set.metadata.uid.clone().expect("StatefulSet UID"),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..ObjectMeta::default()
            },
            spec: Some(PodSpec {
                service_account_name: Some(service_account_name),
                containers: Vec::new(),
                ..PodSpec::default()
            }),
            status: Some(PodStatus {
                pod_ip: Some(format!(
                    "10.{}.0.{}",
                    target.shard_id() + 1,
                    target.member_ordinal() + 10
                )),
                ..PodStatus::default()
            }),
        }
    }

    fn endpoint_address(target: &UnboundAgentObservationTarget, pod: &Pod) -> EndpointAddress {
        EndpointAddress {
            ip: pod
                .status
                .as_ref()
                .expect("status")
                .pod_ip
                .clone()
                .expect("IP"),
            target_ref: Some(ObjectReference {
                api_version: Some("v1".to_owned()),
                kind: Some("Pod".to_owned()),
                name: Some(target.instance_id().to_owned()),
                namespace: Some(target.namespace().to_owned()),
                uid: pod.metadata.uid.clone(),
                ..ObjectReference::default()
            }),
            ..EndpointAddress::default()
        }
    }

    fn endpoints(targets: &[UnboundAgentObservationTarget], pods: &[Pod]) -> Endpoints {
        Endpoints {
            metadata: ObjectMeta {
                name: Some(targets[0].shard_service().to_owned()),
                namespace: Some("database".to_owned()),
                uid: Some("endpoints-uid".to_owned()),
                resource_version: Some("endpoints-rv".to_owned()),
                ..ObjectMeta::default()
            },
            subsets: Some(vec![EndpointSubset {
                addresses: Some(
                    targets
                        .iter()
                        .zip(pods)
                        .map(|(target, pod)| endpoint_address(target, pod))
                        .collect(),
                ),
                ports: Some(vec![
                    EndpointPort {
                        name: Some("postgresql".to_owned()),
                        port: 5_432,
                        protocol: Some("TCP".to_owned()),
                        ..EndpointPort::default()
                    },
                    EndpointPort {
                        name: Some("agent-http".to_owned()),
                        port: 8_080,
                        protocol: Some("TCP".to_owned()),
                        ..EndpointPort::default()
                    },
                ]),
                ..EndpointSubset::default()
            }]),
        }
    }

    fn lease(target: &UnboundAgentObservationTarget, holder: Option<&str>) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(target.writable_lease_name().to_owned()),
                namespace: Some("database".to_owned()),
                uid: Some(target.writable_lease_uid().to_owned()),
                resource_version: Some("lease-rv".to_owned()),
                owner_references: Some(vec![OwnerReference {
                    api_version: OWNER_API_VERSION.to_owned(),
                    kind: OWNER_KIND.to_owned(),
                    name: "demo".to_owned(),
                    uid: CLUSTER_UID.to_owned(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..ObjectMeta::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: holder.map(str::to_owned),
                ..LeaseSpec::default()
            }),
        }
    }

    struct MemoryStore {
        stateful_sets: Mutex<HashMap<String, StatefulSet>>,
        pods: Mutex<HashMap<String, Pod>>,
        endpoints: Mutex<Endpoints>,
        lease: Mutex<Lease>,
    }

    impl IdentityStore for MemoryStore {
        async fn get_stateful_set(&self, name: &str) -> Result<StatefulSet, IdentityBindingError> {
            self.stateful_sets
                .lock()
                .expect("StatefulSets")
                .get(name)
                .cloned()
                .ok_or_else(|| IdentityBindingError::StatefulSetIdentityMismatch(name.to_owned()))
        }

        async fn get_pod(&self, name: &str) -> Result<Pod, IdentityBindingError> {
            self.pods
                .lock()
                .expect("pods")
                .get(name)
                .cloned()
                .ok_or_else(|| IdentityBindingError::PodIdentityMismatch(name.to_owned()))
        }

        async fn get_endpoints(&self, name: &str) -> Result<Endpoints, IdentityBindingError> {
            let endpoints = self.endpoints.lock().expect("endpoints").clone();
            if endpoints.metadata.name.as_deref() != Some(name) {
                return Err(IdentityBindingError::EndpointIdentityMismatch(
                    name.to_owned(),
                ));
            }
            Ok(endpoints)
        }

        async fn get_lease(&self, name: &str) -> Result<Lease, IdentityBindingError> {
            let lease = self.lease.lock().expect("lease").clone();
            if lease.metadata.name.as_deref() != Some(name) {
                return Err(IdentityBindingError::LeaseIdentityMismatch(name.to_owned()));
            }
            Ok(lease)
        }
    }

    struct SlowRecreatingStore {
        inner: MemoryStore,
        recreated: AtomicBool,
        delay: Duration,
    }

    impl IdentityStore for SlowRecreatingStore {
        async fn get_stateful_set(&self, name: &str) -> Result<StatefulSet, IdentityBindingError> {
            self.inner.get_stateful_set(name).await
        }

        async fn get_pod(&self, name: &str) -> Result<Pod, IdentityBindingError> {
            if !self.recreated.swap(true, Ordering::SeqCst) {
                let stateful_set_name = name
                    .strip_suffix("-0")
                    .ok_or_else(|| IdentityBindingError::PodIdentityMismatch(name.to_owned()))?;
                self.inner
                    .stateful_sets
                    .lock()
                    .expect("StatefulSets")
                    .get_mut(stateful_set_name)
                    .expect("observed StatefulSet")
                    .metadata
                    .uid = Some("replacement-stateful-set".to_owned());
                tokio::time::sleep(self.delay).await;
            }
            self.inner.get_pod(name).await
        }

        async fn get_endpoints(&self, name: &str) -> Result<Endpoints, IdentityBindingError> {
            self.inner.get_endpoints(name).await
        }

        async fn get_lease(&self, name: &str) -> Result<Lease, IdentityBindingError> {
            self.inner.get_lease(name).await
        }
    }

    struct ActiveRequest<'a> {
        active: &'a AtomicUsize,
    }

    impl Drop for ActiveRequest<'_> {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct ScaleStore {
        stateful_sets: HashMap<String, StatefulSet>,
        pods: HashMap<String, Pod>,
        endpoints: HashMap<String, Endpoints>,
        leases: HashMap<String, Lease>,
        active: AtomicUsize,
        maximum_active: AtomicUsize,
        calls: AtomicUsize,
    }

    impl ScaleStore {
        async fn request(&self) -> ActiveRequest<'_> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum_active.fetch_max(active, Ordering::SeqCst);
            let request = ActiveRequest {
                active: &self.active,
            };
            tokio::time::sleep(Duration::from_millis(1)).await;
            request
        }
    }

    impl IdentityStore for ScaleStore {
        async fn get_stateful_set(&self, name: &str) -> Result<StatefulSet, IdentityBindingError> {
            let _request = self.request().await;
            self.stateful_sets
                .get(name)
                .cloned()
                .ok_or_else(|| IdentityBindingError::StatefulSetIdentityMismatch(name.to_owned()))
        }

        async fn get_pod(&self, name: &str) -> Result<Pod, IdentityBindingError> {
            let _request = self.request().await;
            self.pods
                .get(name)
                .cloned()
                .ok_or_else(|| IdentityBindingError::PodIdentityMismatch(name.to_owned()))
        }

        async fn get_endpoints(&self, name: &str) -> Result<Endpoints, IdentityBindingError> {
            let _request = self.request().await;
            self.endpoints
                .get(name)
                .cloned()
                .ok_or_else(|| IdentityBindingError::EndpointIdentityMismatch(name.to_owned()))
        }

        async fn get_lease(&self, name: &str) -> Result<Lease, IdentityBindingError> {
            let _request = self.request().await;
            self.leases
                .get(name)
                .cloned()
                .ok_or_else(|| IdentityBindingError::LeaseIdentityMismatch(name.to_owned()))
        }
    }

    fn store() -> (Vec<UnboundAgentObservationTarget>, MemoryStore) {
        let targets = (0..3).map(target).collect::<Vec<_>>();
        let stateful_sets = targets.iter().map(stateful_set).collect::<Vec<_>>();
        let pods = targets
            .iter()
            .zip(&stateful_sets)
            .map(|(target, stateful_set)| pod(target, stateful_set))
            .collect::<Vec<_>>();
        let holder = format!(
            "{}/{}/{}",
            targets[0].instance_id(),
            pods[0].metadata.uid.as_deref().expect("UID"),
            "0123456789abcdef01234567"
        );
        let store = MemoryStore {
            stateful_sets: Mutex::new(
                stateful_sets
                    .into_iter()
                    .map(|stateful_set| {
                        (
                            stateful_set.metadata.name.clone().expect("name"),
                            stateful_set,
                        )
                    })
                    .collect(),
            ),
            pods: Mutex::new(
                pods.iter()
                    .cloned()
                    .map(|pod| (pod.metadata.name.clone().expect("name"), pod))
                    .collect(),
            ),
            endpoints: Mutex::new(endpoints(&targets, &pods)),
            lease: Mutex::new(lease(&targets[0], Some(&holder))),
        };
        (targets, store)
    }

    fn scale_store(
        shard_count: u32,
        members_per_shard: u32,
    ) -> (Vec<UnboundAgentObservationTarget>, ScaleStore) {
        let mut targets = Vec::new();
        let mut stateful_sets = HashMap::new();
        let mut pods = HashMap::new();
        let mut endpoint_sets = HashMap::new();
        let mut leases = HashMap::new();
        for shard in 0..shard_count {
            let shard_targets = (0..members_per_shard)
                .map(|member| target_at(shard, member))
                .collect::<Vec<_>>();
            let shard_stateful_sets = shard_targets.iter().map(stateful_set).collect::<Vec<_>>();
            let shard_pods = shard_targets
                .iter()
                .zip(&shard_stateful_sets)
                .map(|(target, stateful_set)| pod(target, stateful_set))
                .collect::<Vec<_>>();
            for stateful_set in shard_stateful_sets {
                stateful_sets.insert(
                    stateful_set
                        .metadata
                        .name
                        .clone()
                        .expect("StatefulSet name"),
                    stateful_set,
                );
            }
            for pod in &shard_pods {
                pods.insert(pod.metadata.name.clone().expect("Pod name"), pod.clone());
            }
            let first = &shard_targets[0];
            endpoint_sets.insert(
                first.shard_service().to_owned(),
                endpoints(&shard_targets, &shard_pods),
            );
            let holder = format!(
                "{}/{}/{}",
                first.instance_id(),
                shard_pods[0].metadata.uid.as_deref().expect("Pod UID"),
                "0123456789abcdef01234567"
            );
            leases.insert(
                first.writable_lease_name().to_owned(),
                lease(first, Some(&holder)),
            );
            targets.extend(shard_targets);
        }
        (
            targets,
            ScaleStore {
                stateful_sets,
                pods,
                endpoints: endpoint_sets,
                leases,
                active: AtomicUsize::new(0),
                maximum_active: AtomicUsize::new(0),
                calls: AtomicUsize::new(0),
            },
        )
    }

    fn observation_state(targets: &[UnboundAgentObservationTarget]) -> OrchState {
        OrchState::with_identity_and_topology(
            OrchestratorIdentity {
                cluster_id: "demo".to_owned(),
                orchestrator_id: "demo-orchestrator-0".to_owned(),
            },
            1_000,
            TopologyDiagnostics {
                schema_version: TOPOLOGY_SCHEMA_VERSION.to_owned(),
                cluster_object_uid: CLUSTER_UID.to_owned(),
                shard_count: 1,
                member_count: targets.len(),
                agent_status_collection: AgentStatusCollectionState::DisabledPodIdentityRequired,
            },
        )
        .expect("state")
    }

    #[tokio::test]
    async fn binds_exact_pods_endpoints_and_holder_pod() {
        let (targets, store) = store();
        bind_once(&store, &targets).await.expect("bind identities");
    }

    #[tokio::test]
    async fn maximum_topology_reads_are_concurrent_but_strictly_bounded() {
        const SHARDS: u32 = 128;
        const MEMBERS_PER_SHARD: u32 = 5;
        let (targets, store) = scale_store(SHARDS, MEMBERS_PER_SHARD);

        bind_once(&store, &targets)
            .await
            .expect("bind maximum supported topology");

        assert_eq!(
            store.maximum_active.load(Ordering::SeqCst),
            MAX_CONCURRENT_BINDINGS
        );
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.calls.load(Ordering::SeqCst),
            (SHARDS * MEMBERS_PER_SHARD * 2 + SHARDS * 2) as usize
        );
    }

    #[tokio::test]
    async fn endpoint_target_api_version_accepts_only_omitted_or_exact_v1() {
        let (targets, store) = store();
        for subset in store
            .endpoints
            .lock()
            .expect("endpoints")
            .subsets
            .as_mut()
            .expect("subsets")
        {
            for address in subset.addresses.iter_mut().flatten() {
                address
                    .target_ref
                    .as_mut()
                    .expect("target reference")
                    .api_version = None;
            }
        }
        bind_once(&store, &targets)
            .await
            .expect("controller-style omitted API versions");

        for version in ["", "core/v1"] {
            store
                .endpoints
                .lock()
                .expect("endpoints")
                .subsets
                .as_mut()
                .expect("subsets")[0]
                .addresses
                .as_mut()
                .expect("addresses")[0]
                .target_ref
                .as_mut()
                .expect("target reference")
                .api_version = Some(version.to_owned());
            assert!(matches!(
                bind_once(&store, &targets).await,
                Err(IdentityBindingError::EndpointIdentityMismatch(_))
            ));
        }
    }

    #[tokio::test]
    async fn rejects_replaced_pod_and_stale_endpoint_target() {
        let (targets, store) = store();
        store
            .pods
            .lock()
            .expect("pods")
            .get_mut(targets[1].instance_id())
            .expect("member")
            .metadata
            .uid = Some("replacement-pod-uid".to_owned());

        assert!(matches!(
            bind_once(&store, &targets).await,
            Err(IdentityBindingError::EndpointIdentityMismatch(_))
        ));
    }

    #[tokio::test]
    async fn rejects_replaced_or_foreign_stateful_set_and_pod_controller() {
        let (targets, store) = store();
        let target = &targets[1];
        let original_stateful_set = store
            .stateful_sets
            .lock()
            .expect("StatefulSets")
            .get(target.stateful_set())
            .expect("member")
            .clone();
        let original_pod = store
            .pods
            .lock()
            .expect("pods")
            .get(target.instance_id())
            .expect("member")
            .clone();

        for case in ["cluster-owner", "stateful-set-uid", "pod-controller"] {
            *store
                .stateful_sets
                .lock()
                .expect("StatefulSets")
                .get_mut(target.stateful_set())
                .expect("member") = original_stateful_set.clone();
            *store
                .pods
                .lock()
                .expect("pods")
                .get_mut(target.instance_id())
                .expect("member") = original_pod.clone();
            match case {
                "cluster-owner" => {
                    store
                        .stateful_sets
                        .lock()
                        .expect("StatefulSets")
                        .get_mut(target.stateful_set())
                        .expect("member")
                        .metadata
                        .owner_references
                        .as_mut()
                        .expect("owner")[0]
                        .uid = "foreign-cluster".to_owned();
                }
                "stateful-set-uid" => {
                    store
                        .stateful_sets
                        .lock()
                        .expect("StatefulSets")
                        .get_mut(target.stateful_set())
                        .expect("member")
                        .metadata
                        .uid = Some("replacement-stateful-set".to_owned());
                }
                "pod-controller" => {
                    store
                        .pods
                        .lock()
                        .expect("pods")
                        .get_mut(target.instance_id())
                        .expect("member")
                        .metadata
                        .owner_references
                        .as_mut()
                        .expect("owner")[0]
                        .name = "foreign-stateful-set".to_owned();
                }
                _ => unreachable!(),
            }
            assert!(matches!(
                bind_once(&store, &targets).await,
                Err(IdentityBindingError::StatefulSetIdentityMismatch(_)
                    | IdentityBindingError::PodIdentityMismatch(_))
            ));
        }
    }

    #[tokio::test]
    async fn rejects_missing_duplicate_or_extra_endpoint_addresses_and_ports() {
        let (targets, store) = store();
        let original = store.endpoints.lock().expect("endpoints").clone();
        let cases = ["missing", "duplicate", "extra-port"];
        for case in cases {
            let mut candidate = original.clone();
            let subset = &mut candidate.subsets.as_mut().expect("subsets")[0];
            match case {
                "missing" => {
                    subset.addresses.as_mut().expect("addresses").pop();
                }
                "duplicate" => {
                    let duplicate = subset.addresses.as_ref().expect("addresses")[0].clone();
                    subset
                        .addresses
                        .as_mut()
                        .expect("addresses")
                        .push(duplicate);
                }
                "extra-port" => subset.ports.as_mut().expect("ports").push(EndpointPort {
                    name: Some("metrics".to_owned()),
                    port: 9_090,
                    protocol: Some("TCP".to_owned()),
                    ..EndpointPort::default()
                }),
                _ => unreachable!(),
            }
            *store.endpoints.lock().expect("endpoints") = candidate;
            assert!(matches!(
                bind_once(&store, &targets).await,
                Err(IdentityBindingError::EndpointIdentityMismatch(_))
            ));
        }
    }

    #[tokio::test]
    async fn rejects_foreign_or_recreated_lease_and_holder() {
        let (targets, store) = store();
        let original = store.lease.lock().expect("lease").clone();
        for case in ["uid", "owner", "holder"] {
            let mut candidate = original.clone();
            match case {
                "uid" => candidate.metadata.uid = Some("replacement-lease".to_owned()),
                "owner" => {
                    candidate.metadata.owner_references.as_mut().expect("owner")[0].uid =
                        "foreign-cluster".to_owned();
                }
                "holder" => {
                    candidate.spec.as_mut().expect("spec").holder_identity = Some(format!(
                        "{}/foreign-pod/0123456789abcdef01234567",
                        targets[0].instance_id()
                    ));
                }
                _ => unreachable!(),
            }
            *store.lease.lock().expect("lease") = candidate;
            assert!(matches!(
                bind_once(&store, &targets).await,
                Err(IdentityBindingError::LeaseIdentityMismatch(_))
            ));
        }
    }

    #[tokio::test]
    async fn slow_scan_with_recreated_early_object_cannot_publish_a_snapshot() {
        let (targets, inner) = store();
        let store = SlowRecreatingStore {
            inner,
            recreated: AtomicBool::new(false),
            delay: Duration::from_millis(250),
        };
        let state = observation_state(&targets);
        let freshness = Duration::from_millis(25);

        let error = observe_once(&store, &targets, &state, freshness)
            .await
            .expect_err("the complete scan must fit inside the freshness bound");

        assert!(matches!(
            error,
            IdentityBindingError::FreshnessBoundExceeded(bound) if bound == freshness
        ));
        assert!(store.recreated.load(Ordering::SeqCst));
        assert_eq!(
            store
                .inner
                .stateful_sets
                .lock()
                .expect("StatefulSets")
                .get(targets[0].stateful_set())
                .expect("recreated StatefulSet")
                .metadata
                .uid
                .as_deref(),
            Some("replacement-stateful-set")
        );
        assert_eq!(
            state
                .snapshot()
                .topology
                .expect("topology")
                .agent_status_collection,
            AgentStatusCollectionState::DisabledPodIdentityRequired,
        );
    }

    #[tokio::test]
    async fn scan_completing_at_exact_freshness_boundary_is_rejected() {
        let (targets, store) = store();
        let state = observation_state(&targets);
        let freshness = Duration::from_secs(1);
        let started_at = std::time::Instant::now();
        let mut clock = [started_at, started_at + freshness].into_iter();

        let error = observe_once_with_clock(&store, &targets, &state, freshness, || {
            clock.next().expect("start and completion readings")
        })
        .await
        .expect_err("evidence is stale at the exact freshness boundary");

        assert!(matches!(
            error,
            IdentityBindingError::FreshnessBoundExceeded(bound) if bound == freshness
        ));
        assert!(clock.next().is_none());
        assert_eq!(
            state
                .snapshot()
                .topology
                .expect("topology")
                .agent_status_collection,
            AgentStatusCollectionState::DisabledPodIdentityRequired,
        );
    }

    #[tokio::test]
    async fn failed_refresh_clears_diagnostics_without_changing_readiness() {
        let (targets, store) = store();
        let state = observation_state(&targets);
        assert!(state.record_coordination_ready(
            "orchestrator-lease-uid",
            "orchestrator-lease-rv",
            false,
            std::time::Instant::now() + Duration::from_secs(10),
        ));

        let observed_at = std::time::Instant::now();
        let freshness = Duration::from_secs(1);
        observe_once_with_clock(&store, &targets, &state, freshness, || observed_at)
            .await
            .expect("initial binding");
        let ready = state.readiness();
        assert!(ready.ready);
        assert_eq!(
            state
                .snapshot()
                .topology
                .expect("topology")
                .agent_status_collection,
            AgentStatusCollectionState::DisabledAgentStatusCollectorRequired,
        );
        assert_eq!(state.readiness(), ready);
        assert_eq!(
            state
                .snapshot_at_for_test(observed_at + freshness)
                .topology
                .expect("topology")
                .agent_status_collection,
            AgentStatusCollectionState::DisabledPodIdentityRequired,
        );
        assert_eq!(state.readiness(), ready);

        observe_once_with_clock(&store, &targets, &state, freshness, || observed_at)
            .await
            .expect("restore binding for refresh failure");

        store
            .endpoints
            .lock()
            .expect("endpoints")
            .subsets
            .as_mut()
            .expect("subsets")[0]
            .addresses
            .as_mut()
            .expect("addresses")
            .pop();
        assert!(
            observe_once(&store, &targets, &state, freshness)
                .await
                .is_err()
        );
        assert_eq!(state.readiness(), ready);
        assert_eq!(
            state
                .snapshot()
                .topology
                .expect("topology")
                .agent_status_collection,
            AgentStatusCollectionState::DisabledPodIdentityRequired,
        );
    }
}
