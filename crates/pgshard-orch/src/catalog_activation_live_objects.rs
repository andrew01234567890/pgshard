//! Bounded, read-only revalidation of catalog-activation live objects.
//!
//! The reader owns no mutation API and never reads a `Secret`. It performs four
//! exact namespaced GETs: the empty activation carrier, the selected source
//! Pod, its writable-term Lease, and the orchestrator leadership Lease. Typed
//! APIs fix the Pod and Lease GVKs by construction; the dynamic carrier GVK is
//! checked again after deserialization.

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Client;
use kube::api::{Api, DynamicObject};
use kube::config::Config;
use kube::core::{ApiResource, GroupVersionKind};
use thiserror::Error;

use crate::agent_status::WritableLeaseProofIdentity;
use crate::catalog_candidate::{BoundCandidateSet, ObjectReference};
use crate::catalog_materialization::{
    CatalogActivationDispatcherProof, CatalogActivationLiveObjectProofs,
    CatalogActivationPublicationTarget, CatalogBootstrapDispatch,
    bind_catalog_activation_live_objects, dispatcher_holder_matches,
};

const CARRIER_KIND: &str = "PgShardCatalogActivation";
const CLUSTER_OWNER_API_VERSION: &str = "pgshard.io/v1alpha1";
const CLUSTER_OWNER_KIND: &str = "PgShardCluster";
const STATEFUL_SET_API_VERSION: &str = "apps/v1";
const STATEFUL_SET_KIND: &str = "StatefulSet";
const CLUSTER_LABEL: &str = "pgshard.io/cluster";
const SHARD_LABEL: &str = "pgshard.io/shard";
const MEMBER_LABEL: &str = "pgshard.io/member";
const ROLE_LABEL: &str = "pgshard.io/role";
const COMPONENT_LABEL: &str = "app.kubernetes.io/component";
const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
const MAXIMUM_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// One-shot authoritative reader with no mutation, list, watch, or Secret API.
#[allow(dead_code)] // Dormant until the separately reviewed publisher composes it.
pub(crate) struct AuthoritativeCatalogActivationLiveObjectReader {
    store: KubernetesLiveObjectStore,
    expected: ExpectedLiveObjects,
    per_request_timeout: Duration,
    overall_timeout: Duration,
}

#[allow(dead_code)] // Dormant until the separately reviewed publisher composes it.
impl AuthoritativeCatalogActivationLiveObjectReader {
    pub(crate) fn new(
        target: &CatalogActivationPublicationTarget,
        per_request_timeout: Duration,
        overall_timeout: Duration,
    ) -> Result<Self, CatalogActivationLiveObjectError> {
        validate_timeouts(per_request_timeout, overall_timeout)?;
        let expected = ExpectedLiveObjects::from_target(target);
        let store = KubernetesLiveObjectStore::new(&expected, per_request_timeout)?;
        Ok(Self {
            store,
            expected,
            per_request_timeout,
            overall_timeout,
        })
    }

    /// Re-reads and validates every live object before cross-binding the full
    /// authoritative candidate set to the sealed dispatch.
    pub(crate) async fn read(
        &self,
        dispatch: &CatalogBootstrapDispatch,
        candidates: &BoundCandidateSet,
    ) -> Result<CatalogActivationLiveObjectProofs, CatalogActivationLiveObjectError> {
        read_and_bind(
            &self.store,
            &self.expected,
            self.per_request_timeout,
            self.overall_timeout,
            dispatch,
            candidates,
        )
        .await
    }
}

fn validate_timeouts(
    per_request_timeout: Duration,
    overall_timeout: Duration,
) -> Result<(), CatalogActivationLiveObjectError> {
    if !(Duration::from_millis(100)..=MAXIMUM_READ_TIMEOUT).contains(&per_request_timeout)
        || !(per_request_timeout..=MAXIMUM_READ_TIMEOUT).contains(&overall_timeout)
    {
        return Err(CatalogActivationLiveObjectError::InvalidTimeouts);
    }
    Ok(())
}

/// Expected identities copied from one sealed dispatch. Deliberately no
/// `Debug`: this contains the dispatcher Pod UID.
struct ExpectedLiveObjects {
    carrier_group: &'static str,
    carrier_version: &'static str,
    carrier_plural: &'static str,
    namespace: String,
    carrier_name: String,
    carrier_uid: String,
    cluster_name: String,
    cluster_uid: String,
    source_stateful_set_name: String,
    source_pod_name: String,
    source_pod_uid: String,
    source_service_account_name: String,
    writable_lease_name: String,
    writable_lease_uid: String,
    writable_lease_resource_version: String,
    writable_lease_holder: String,
    writable_lease_transitions: u64,
    dispatcher_pod_name: String,
    dispatcher_pod_uid: String,
    leadership_lease_name: String,
    leadership_lease_uid: String,
    leadership_lease_resource_version: String,
}

impl ExpectedLiveObjects {
    fn from_target(target: &CatalogActivationPublicationTarget) -> Self {
        let source_stateful_set_name = target.target_stateful_set_name().to_owned();
        Self {
            carrier_group: target.carrier_api_group(),
            carrier_version: target.carrier_api_version(),
            carrier_plural: target.carrier_api_plural(),
            namespace: target.carrier_namespace().to_owned(),
            carrier_name: target.carrier_name().to_owned(),
            carrier_uid: target.carrier_uid().to_owned(),
            cluster_name: target.cluster_name().to_owned(),
            cluster_uid: target.cluster_uid().to_owned(),
            source_service_account_name: source_service_account_name(&source_stateful_set_name),
            source_stateful_set_name,
            source_pod_name: target.target_pod_name().to_owned(),
            source_pod_uid: target.target_pod_uid().to_owned(),
            writable_lease_name: target.writable_lease_name().to_owned(),
            writable_lease_uid: target.writable_lease_uid().to_owned(),
            writable_lease_resource_version: target.writable_lease_resource_version().to_owned(),
            writable_lease_holder: target.writable_lease_holder().to_owned(),
            writable_lease_transitions: target.writable_lease_transitions(),
            dispatcher_pod_name: target.dispatcher_pod_name().to_owned(),
            dispatcher_pod_uid: target.dispatcher_pod_uid().to_owned(),
            leadership_lease_name: target.dispatcher_lease_name().to_owned(),
            leadership_lease_uid: target.dispatcher_lease_uid().to_owned(),
            leadership_lease_resource_version: target
                .dispatcher_lease_resource_version()
                .to_owned(),
        }
    }
}

fn source_service_account_name(source_stateful_set_name: &str) -> String {
    format!("{source_stateful_set_name}-agent")
}

struct ValidatedLiveObjects {
    carrier: ObjectReference,
    carrier_resource_version: String,
    source_pod: ObjectReference,
    writable_lease: WritableLeaseProofIdentity,
    dispatcher: CatalogActivationDispatcherProof,
}

async fn read_and_bind<S: LiveObjectStore>(
    store: &S,
    expected: &ExpectedLiveObjects,
    per_request_timeout: Duration,
    overall_timeout: Duration,
    dispatch: &CatalogBootstrapDispatch,
    candidates: &BoundCandidateSet,
) -> Result<CatalogActivationLiveObjectProofs, CatalogActivationLiveObjectError> {
    let validated =
        read_authoritative(store, expected, per_request_timeout, overall_timeout).await?;
    bind_catalog_activation_live_objects(
        dispatch,
        candidates,
        validated.carrier,
        validated.carrier_resource_version,
        validated.source_pod,
        validated.writable_lease,
        validated.dispatcher,
    )
    .ok_or(CatalogActivationLiveObjectError::DispatchMismatch)
}

async fn read_authoritative<S: LiveObjectStore>(
    store: &S,
    expected: &ExpectedLiveObjects,
    per_request_timeout: Duration,
    overall_timeout: Duration,
) -> Result<ValidatedLiveObjects, CatalogActivationLiveObjectError> {
    tokio::time::timeout(
        overall_timeout,
        read_and_validate(store, expected, per_request_timeout),
    )
    .await
    .map_err(|_| CatalogActivationLiveObjectError::OverallTimeout)?
}

async fn read_and_validate<S: LiveObjectStore>(
    store: &S,
    expected: &ExpectedLiveObjects,
    per_request_timeout: Duration,
) -> Result<ValidatedLiveObjects, CatalogActivationLiveObjectError> {
    let carrier = bounded_get(
        per_request_timeout,
        "read catalog-activation carrier",
        store.get_carrier(),
    )
    .await?;
    let source_pod = bounded_get(
        per_request_timeout,
        "read catalog source Pod",
        store.get_source_pod(),
    )
    .await?;
    let writable_lease = bounded_get(
        per_request_timeout,
        "read writable-term Lease",
        store.get_writable_lease(),
    )
    .await?;
    let leadership_lease = bounded_get(
        per_request_timeout,
        "read orchestrator leadership Lease",
        store.get_leadership_lease(),
    )
    .await?;

    validate_live_objects(
        expected,
        &carrier,
        &source_pod,
        &writable_lease,
        &leadership_lease,
    )
}

async fn bounded_get<T>(
    timeout: Duration,
    operation: &'static str,
    request: impl Future<Output = Result<T, CatalogActivationLiveObjectError>>,
) -> Result<T, CatalogActivationLiveObjectError> {
    tokio::time::timeout(timeout, request)
        .await
        .map_err(|_| CatalogActivationLiveObjectError::RequestTimeout(operation))?
}

fn validate_live_objects(
    expected: &ExpectedLiveObjects,
    carrier: &DynamicObject,
    source_pod: &Pod,
    writable_lease: &Lease,
    leadership_lease: &Lease,
) -> Result<ValidatedLiveObjects, CatalogActivationLiveObjectError> {
    let (carrier, carrier_resource_version) = validate_carrier(carrier, expected)?;
    let source_pod = validate_source_pod(source_pod, expected)?;
    let writable_lease = validate_writable_lease(writable_lease, expected)?;
    let dispatcher = validate_leadership_lease(leadership_lease, expected)?;
    Ok(ValidatedLiveObjects {
        carrier,
        carrier_resource_version,
        source_pod,
        writable_lease,
        dispatcher,
    })
}

fn validate_carrier(
    carrier: &DynamicObject,
    expected: &ExpectedLiveObjects,
) -> Result<(ObjectReference, String), CatalogActivationLiveObjectError> {
    let types = carrier
        .types
        .as_ref()
        .ok_or(CatalogActivationLiveObjectError::InvalidCarrier)?;
    if types.api_version != format!("{}/{}", expected.carrier_group, expected.carrier_version)
        || types.kind != CARRIER_KIND
        || !exact_metadata_identity(
            &carrier.metadata,
            &expected.carrier_name,
            &expected.namespace,
            &expected.carrier_uid,
            None,
        )
        || !exact_cluster_owner(&carrier.metadata, expected)
        || !exact_carrier_metadata(&carrier.metadata, expected)
        || !carrier.data.as_object().is_some_and(|data| {
            data.iter().all(|(key, value)| {
                matches!(key.as_str(), "spec" | "status")
                    && value.as_object().is_some_and(serde_json::Map::is_empty)
            })
        })
    {
        return Err(CatalogActivationLiveObjectError::InvalidCarrier);
    }
    // The carrier RV is deliberately not compared to a sealed value: the
    // candidate checkpoint seals its immutable UID, while the later write CAS
    // retains and uses this freshly observed RV.
    let resource_version =
        require_resource_version(carrier.metadata.resource_version.as_deref())?.to_owned();
    Ok((
        ObjectReference {
            name: expected.carrier_name.clone(),
            uid: expected.carrier_uid.clone(),
        },
        resource_version,
    ))
}

fn exact_carrier_metadata(metadata: &ObjectMeta, expected: &ExpectedLiveObjects) -> bool {
    let labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_owned(), "pgshard".to_owned()),
        (
            "app.kubernetes.io/managed-by".to_owned(),
            "pgshard-operator".to_owned(),
        ),
        (
            "app.kubernetes.io/instance".to_owned(),
            expected.cluster_name.clone(),
        ),
        (
            "app.kubernetes.io/component".to_owned(),
            "catalog-activation".to_owned(),
        ),
        (CLUSTER_LABEL.to_owned(), expected.cluster_name.clone()),
    ]);
    let annotations = BTreeMap::from([("pgshard.io/apply-ownership".to_owned(), "v1".to_owned())]);
    metadata.labels.as_ref() == Some(&labels)
        && metadata.annotations.as_ref() == Some(&annotations)
        && metadata.finalizers.as_ref().is_none_or(Vec::is_empty)
        && metadata.generate_name.is_none()
}

fn validate_source_pod(
    pod: &Pod,
    expected: &ExpectedLiveObjects,
) -> Result<ObjectReference, CatalogActivationLiveObjectError> {
    if !exact_metadata_identity(
        &pod.metadata,
        &expected.source_pod_name,
        &expected.namespace,
        &expected.source_pod_uid,
        None,
    ) || !exact_source_controller(&pod.metadata, expected)
        || pod.metadata.labels.as_ref().is_none_or(|labels| {
            labels.get(CLUSTER_LABEL).map(String::as_str) != Some(expected.cluster_name.as_str())
                || labels.get(SHARD_LABEL).map(String::as_str) != Some("0000")
                || labels.get(MEMBER_LABEL).map(String::as_str) != Some("0000")
                || labels.get(COMPONENT_LABEL).map(String::as_str) != Some("postgresql")
                || labels.get(MANAGED_BY_LABEL).map(String::as_str) != Some("pgshard-operator")
                || labels.contains_key(ROLE_LABEL)
        })
        || pod
            .spec
            .as_ref()
            .and_then(|spec| spec.service_account_name.as_deref())
            != Some(expected.source_service_account_name.as_str())
    {
        return Err(CatalogActivationLiveObjectError::InvalidSourcePod);
    }
    // Pod status can legitimately churn its RV. The immutable sealed Pod UID,
    // exact controller shape, labels, and service account remain authoritative.
    require_resource_version(pod.metadata.resource_version.as_deref())?;
    Ok(ObjectReference {
        name: expected.source_pod_name.clone(),
        uid: expected.source_pod_uid.clone(),
    })
}

fn exact_source_controller(metadata: &ObjectMeta, expected: &ExpectedLiveObjects) -> bool {
    let owners = metadata.owner_references.as_deref().unwrap_or_default();
    owners.len() == 1
        && owners[0].api_version == STATEFUL_SET_API_VERSION
        && owners[0].kind == STATEFUL_SET_KIND
        && owners[0].name == expected.source_stateful_set_name
        && valid_uid(&owners[0].uid)
        && owners[0].controller == Some(true)
        && owners[0].block_owner_deletion == Some(true)
}

fn validate_writable_lease(
    lease: &Lease,
    expected: &ExpectedLiveObjects,
) -> Result<WritableLeaseProofIdentity, CatalogActivationLiveObjectError> {
    if !exact_metadata_identity(
        &lease.metadata,
        &expected.writable_lease_name,
        &expected.namespace,
        &expected.writable_lease_uid,
        Some(&expected.writable_lease_resource_version),
    ) || !exact_cluster_owner(&lease.metadata, expected)
    {
        return Err(CatalogActivationLiveObjectError::InvalidWritableLease);
    }
    let spec = lease
        .spec
        .as_ref()
        .ok_or(CatalogActivationLiveObjectError::InvalidWritableLease)?;
    if spec.holder_identity.as_deref() != Some(expected.writable_lease_holder.as_str())
        || spec
            .lease_transitions
            .and_then(|value| u64::try_from(value).ok())
            != Some(expected.writable_lease_transitions)
        || spec.preferred_holder.is_some()
        || spec.strategy.is_some()
    {
        return Err(CatalogActivationLiveObjectError::InvalidWritableLease);
    }
    Ok(WritableLeaseProofIdentity {
        namespace: expected.namespace.clone(),
        name: expected.writable_lease_name.clone(),
        uid: expected.writable_lease_uid.clone(),
        resource_version: expected.writable_lease_resource_version.clone(),
        holder_identity: expected.writable_lease_holder.clone(),
        transitions: expected.writable_lease_transitions,
    })
}

fn validate_leadership_lease(
    lease: &Lease,
    expected: &ExpectedLiveObjects,
) -> Result<CatalogActivationDispatcherProof, CatalogActivationLiveObjectError> {
    if !exact_metadata_identity(
        &lease.metadata,
        &expected.leadership_lease_name,
        &expected.namespace,
        &expected.leadership_lease_uid,
        Some(&expected.leadership_lease_resource_version),
    ) || !exact_cluster_owner(&lease.metadata, expected)
    {
        return Err(CatalogActivationLiveObjectError::InvalidLeadershipLease);
    }
    let spec = lease
        .spec
        .as_ref()
        .ok_or(CatalogActivationLiveObjectError::InvalidLeadershipLease)?;
    let holder = spec
        .holder_identity
        .as_deref()
        .filter(|holder| {
            dispatcher_holder_matches(
                holder,
                &expected.dispatcher_pod_name,
                &expected.dispatcher_pod_uid,
            )
        })
        .ok_or(CatalogActivationLiveObjectError::InvalidLeadershipLease)?;
    if spec.preferred_holder.is_some() || spec.strategy.is_some() {
        return Err(CatalogActivationLiveObjectError::InvalidLeadershipLease);
    }
    Ok(CatalogActivationDispatcherProof {
        pod_name: expected.dispatcher_pod_name.clone(),
        pod_uid: expected.dispatcher_pod_uid.clone(),
        lease_name: expected.leadership_lease_name.clone(),
        lease_uid: expected.leadership_lease_uid.clone(),
        lease_resource_version: expected.leadership_lease_resource_version.clone(),
        lease_holder: holder.to_owned(),
    })
}

fn exact_metadata_identity(
    metadata: &ObjectMeta,
    name: &str,
    namespace: &str,
    uid: &str,
    resource_version: Option<&str>,
) -> bool {
    metadata.name.as_deref() == Some(name)
        && metadata.namespace.as_deref() == Some(namespace)
        && metadata.uid.as_deref() == Some(uid)
        && valid_uid(uid)
        && metadata.deletion_timestamp.is_none()
        && resource_version.is_none_or(|expected| {
            metadata.resource_version.as_deref() == Some(expected)
                && require_resource_version(Some(expected)).is_ok()
        })
}

fn exact_cluster_owner(metadata: &ObjectMeta, expected: &ExpectedLiveObjects) -> bool {
    metadata.owner_references.as_deref().is_some_and(|owners| {
        owners.len() == 1
            && owners[0].api_version == CLUSTER_OWNER_API_VERSION
            && owners[0].kind == CLUSTER_OWNER_KIND
            && owners[0].name == expected.cluster_name
            && owners[0].uid == expected.cluster_uid
            && owners[0].controller == Some(true)
            && owners[0].block_owner_deletion == Some(true)
    })
}

fn valid_uid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
}

fn require_resource_version(value: Option<&str>) -> Result<&str, CatalogActivationLiveObjectError> {
    value
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or(CatalogActivationLiveObjectError::InvalidObjectMetadata)
}

trait LiveObjectStore: Send + Sync {
    async fn get_carrier(&self) -> Result<DynamicObject, CatalogActivationLiveObjectError>;
    async fn get_source_pod(&self) -> Result<Pod, CatalogActivationLiveObjectError>;
    async fn get_writable_lease(&self) -> Result<Lease, CatalogActivationLiveObjectError>;
    async fn get_leadership_lease(&self) -> Result<Lease, CatalogActivationLiveObjectError>;
}

struct KubernetesLiveObjectStore {
    carrier_name: String,
    source_pod_name: String,
    writable_lease_name: String,
    leadership_lease_name: String,
    carriers: Api<DynamicObject>,
    pods: Api<Pod>,
    leases: Api<Lease>,
}

impl KubernetesLiveObjectStore {
    fn new(
        expected: &ExpectedLiveObjects,
        request_timeout: Duration,
    ) -> Result<Self, CatalogActivationLiveObjectError> {
        let mut client_config = Config::incluster().map_err(|error| {
            CatalogActivationLiveObjectError::InClusterConfiguration(error.to_string())
        })?;
        client_config.connect_timeout = Some(request_timeout);
        client_config.read_timeout = Some(request_timeout);
        client_config.write_timeout = Some(request_timeout);
        client_config.default_retry = false;
        let client = Client::try_from(client_config).map_err(|error| {
            CatalogActivationLiveObjectError::KubernetesClient(error.to_string())
        })?;
        let carrier_resource = ApiResource::from_gvk_with_plural(
            &GroupVersionKind::gvk(
                expected.carrier_group,
                expected.carrier_version,
                CARRIER_KIND,
            ),
            expected.carrier_plural,
        );
        Ok(Self {
            carrier_name: expected.carrier_name.clone(),
            source_pod_name: expected.source_pod_name.clone(),
            writable_lease_name: expected.writable_lease_name.clone(),
            leadership_lease_name: expected.leadership_lease_name.clone(),
            carriers: Api::namespaced_with(client.clone(), &expected.namespace, &carrier_resource),
            pods: Api::namespaced(client.clone(), &expected.namespace),
            leases: Api::namespaced(client, &expected.namespace),
        })
    }

    async fn get<K>(
        api: &Api<K>,
        name: &str,
        operation: &'static str,
    ) -> Result<K, CatalogActivationLiveObjectError>
    where
        K: Clone + std::fmt::Debug + serde::de::DeserializeOwned + kube::Resource<DynamicType = ()>,
    {
        api.get(name)
            .await
            .map_err(|source| CatalogActivationLiveObjectError::Kubernetes {
                operation,
                source: Box::new(source),
            })
    }
}

impl LiveObjectStore for KubernetesLiveObjectStore {
    async fn get_carrier(&self) -> Result<DynamicObject, CatalogActivationLiveObjectError> {
        self.carriers
            .get(&self.carrier_name)
            .await
            .map_err(|source| CatalogActivationLiveObjectError::Kubernetes {
                operation: "read catalog-activation carrier",
                source: Box::new(source),
            })
    }

    async fn get_source_pod(&self) -> Result<Pod, CatalogActivationLiveObjectError> {
        Self::get(&self.pods, &self.source_pod_name, "read catalog source Pod").await
    }

    async fn get_writable_lease(&self) -> Result<Lease, CatalogActivationLiveObjectError> {
        Self::get(
            &self.leases,
            &self.writable_lease_name,
            "read writable-term Lease",
        )
        .await
    }

    async fn get_leadership_lease(&self) -> Result<Lease, CatalogActivationLiveObjectError> {
        Self::get(
            &self.leases,
            &self.leadership_lease_name,
            "read orchestrator leadership Lease",
        )
        .await
    }
}

#[derive(Debug, Error)]
pub(crate) enum CatalogActivationLiveObjectError {
    #[error("catalog-activation live-object timeouts are invalid")]
    InvalidTimeouts,
    #[error("Kubernetes object UID or resource version is missing or malformed")]
    InvalidObjectMetadata,
    #[error("catalog-activation carrier is not the exact empty operator-owned object")]
    InvalidCarrier,
    #[error("catalog source Pod identity does not match the sealed dispatch")]
    InvalidSourcePod,
    #[error("writable-term Lease does not match the sealed term")]
    InvalidWritableLease,
    #[error("orchestrator leadership Lease does not match the sealed dispatcher")]
    InvalidLeadershipLease,
    #[error("Kubernetes API request timed out while attempting to {0}")]
    RequestTimeout(&'static str),
    #[error("catalog-activation live-object read exceeded its overall bound")]
    OverallTimeout,
    #[error("validated live objects do not match the sealed dispatch and candidate set")]
    DispatchMismatch,
    #[error("in-cluster Kubernetes configuration is unavailable: {0}")]
    InClusterConfiguration(String),
    #[error("Kubernetes client initialization failed: {0}")]
    KubernetesClient(String),
    #[error("Kubernetes API could not {operation}: {source}")]
    Kubernetes {
        operation: &'static str,
        #[source]
        source: Box<kube::Error>,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use k8s_openapi::api::coordination::v1::LeaseSpec;
    use k8s_openapi::api::core::v1::PodSpec;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
    use kube::core::TypeMeta;
    use serde_json::{Value, json};

    use super::*;

    const DISPATCHER_HOLDER: &str =
        "demo-orchestrator-0/dispatcher-pod-uid/123e4567-e89b-42d3-a456-426614174000";
    const WRITABLE_HOLDER: &str = "demo-shard-0000-0/source-pod-uid/0123456789abcdef01234567";

    type DynamicMutation = Box<dyn Fn(&mut DynamicObject)>;
    type PodMutation = Box<dyn Fn(&mut Pod)>;
    type LeaseMutation = Box<dyn Fn(&mut Lease)>;

    struct Fixture {
        expected: ExpectedLiveObjects,
        carrier: DynamicObject,
        source_pod: Pod,
        writable_lease: Lease,
        leadership_lease: Lease,
    }

    impl Fixture {
        fn valid() -> Self {
            let expected = expected();
            Self {
                carrier: carrier(&expected),
                source_pod: source_pod(&expected),
                writable_lease: writable_lease(&expected),
                leadership_lease: leadership_lease(&expected),
                expected,
            }
        }

        fn validate(&self) -> Result<ValidatedLiveObjects, CatalogActivationLiveObjectError> {
            validate_live_objects(
                &self.expected,
                &self.carrier,
                &self.source_pod,
                &self.writable_lease,
                &self.leadership_lease,
            )
        }
    }

    fn expected() -> ExpectedLiveObjects {
        ExpectedLiveObjects {
            carrier_group: "pgshard.io",
            carrier_version: "v1alpha1",
            carrier_plural: "pgshardcatalogactivations",
            namespace: "database".to_owned(),
            carrier_name: "demo-catalog-activation".to_owned(),
            carrier_uid: "carrier-uid".to_owned(),
            cluster_name: "demo".to_owned(),
            cluster_uid: "cluster-uid".to_owned(),
            source_stateful_set_name: "demo-shard-0000".to_owned(),
            source_pod_name: "demo-shard-0000-0".to_owned(),
            source_pod_uid: "source-pod-uid".to_owned(),
            source_service_account_name: "demo-shard-0000-agent".to_owned(),
            writable_lease_name: "demo-shard-0000-term".to_owned(),
            writable_lease_uid: "writable-lease-uid".to_owned(),
            writable_lease_resource_version: "writable-rv-7".to_owned(),
            writable_lease_holder: WRITABLE_HOLDER.to_owned(),
            writable_lease_transitions: 7,
            dispatcher_pod_name: "demo-orchestrator-0".to_owned(),
            dispatcher_pod_uid: "dispatcher-pod-uid".to_owned(),
            leadership_lease_name: "demo-orch-lease".to_owned(),
            leadership_lease_uid: "leadership-lease-uid".to_owned(),
            leadership_lease_resource_version: "leadership-rv-11".to_owned(),
        }
    }

    fn cluster_owner(expected: &ExpectedLiveObjects) -> OwnerReference {
        OwnerReference {
            api_version: CLUSTER_OWNER_API_VERSION.to_owned(),
            kind: CLUSTER_OWNER_KIND.to_owned(),
            name: expected.cluster_name.clone(),
            uid: expected.cluster_uid.clone(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    fn base_metadata(
        name: &str,
        uid: &str,
        resource_version: &str,
        expected: &ExpectedLiveObjects,
    ) -> ObjectMeta {
        ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(expected.namespace.clone()),
            uid: Some(uid.to_owned()),
            resource_version: Some(resource_version.to_owned()),
            owner_references: Some(vec![cluster_owner(expected)]),
            ..ObjectMeta::default()
        }
    }

    fn carrier(expected: &ExpectedLiveObjects) -> DynamicObject {
        let mut metadata = base_metadata(
            &expected.carrier_name,
            &expected.carrier_uid,
            "carrier-rv-3",
            expected,
        );
        metadata.labels = Some(BTreeMap::from([
            ("app.kubernetes.io/name".to_owned(), "pgshard".to_owned()),
            (
                "app.kubernetes.io/managed-by".to_owned(),
                "pgshard-operator".to_owned(),
            ),
            (
                "app.kubernetes.io/instance".to_owned(),
                expected.cluster_name.clone(),
            ),
            (
                "app.kubernetes.io/component".to_owned(),
                "catalog-activation".to_owned(),
            ),
            (CLUSTER_LABEL.to_owned(), expected.cluster_name.clone()),
        ]));
        metadata.annotations = Some(BTreeMap::from([(
            "pgshard.io/apply-ownership".to_owned(),
            "v1".to_owned(),
        )]));
        DynamicObject {
            types: Some(TypeMeta {
                api_version: "pgshard.io/v1alpha1".to_owned(),
                kind: CARRIER_KIND.to_owned(),
            }),
            metadata,
            data: json!({"spec": {}, "status": {}}),
        }
    }

    fn source_pod(expected: &ExpectedLiveObjects) -> Pod {
        let mut metadata = base_metadata(
            &expected.source_pod_name,
            &expected.source_pod_uid,
            "source-pod-rv-9",
            expected,
        );
        metadata.owner_references = Some(vec![OwnerReference {
            api_version: STATEFUL_SET_API_VERSION.to_owned(),
            kind: STATEFUL_SET_KIND.to_owned(),
            name: expected.source_stateful_set_name.clone(),
            uid: "source-stateful-set-uid".to_owned(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        metadata.labels = Some(BTreeMap::from([
            (CLUSTER_LABEL.to_owned(), expected.cluster_name.clone()),
            (SHARD_LABEL.to_owned(), "0000".to_owned()),
            (MEMBER_LABEL.to_owned(), "0000".to_owned()),
            (COMPONENT_LABEL.to_owned(), "postgresql".to_owned()),
            (MANAGED_BY_LABEL.to_owned(), "pgshard-operator".to_owned()),
        ]));
        Pod {
            metadata,
            spec: Some(PodSpec {
                containers: vec![],
                service_account_name: Some(expected.source_service_account_name.clone()),
                ..PodSpec::default()
            }),
            status: None,
        }
    }

    fn writable_lease(expected: &ExpectedLiveObjects) -> Lease {
        Lease {
            metadata: base_metadata(
                &expected.writable_lease_name,
                &expected.writable_lease_uid,
                &expected.writable_lease_resource_version,
                expected,
            ),
            spec: Some(LeaseSpec {
                holder_identity: Some(expected.writable_lease_holder.clone()),
                lease_transitions: Some(
                    i32::try_from(expected.writable_lease_transitions)
                        .expect("fixture transitions fit i32"),
                ),
                ..LeaseSpec::default()
            }),
        }
    }

    fn leadership_lease(expected: &ExpectedLiveObjects) -> Lease {
        Lease {
            metadata: base_metadata(
                &expected.leadership_lease_name,
                &expected.leadership_lease_uid,
                &expected.leadership_lease_resource_version,
                expected,
            ),
            spec: Some(LeaseSpec {
                holder_identity: Some(DISPATCHER_HOLDER.to_owned()),
                ..LeaseSpec::default()
            }),
        }
    }

    fn mark_deleting(metadata: &mut ObjectMeta) {
        metadata.deletion_timestamp = Some(
            serde_json::from_value(json!("2026-07-21T00:00:00Z"))
                .expect("fixed deletion timestamp"),
        );
    }

    #[test]
    fn accepts_exact_live_objects() {
        assert!(Fixture::valid().validate().is_ok());
    }

    #[test]
    fn accepts_only_independent_bounded_timeout_budgets() {
        assert!(validate_timeouts(Duration::from_millis(100), Duration::from_secs(5)).is_ok());
        for (per_request, overall) in [
            (Duration::from_millis(99), Duration::from_secs(1)),
            (Duration::from_secs(6), Duration::from_secs(6)),
            (Duration::from_secs(2), Duration::from_secs(1)),
            (Duration::from_secs(1), Duration::from_secs(6)),
        ] {
            assert!(matches!(
                validate_timeouts(per_request, overall),
                Err(CatalogActivationLiveObjectError::InvalidTimeouts)
            ));
        }
    }

    #[test]
    fn rejects_every_carrier_identity_owner_and_empty_state_drift() {
        let mutations: Vec<DynamicMutation> = vec![
            Box::new(|object| object.types.as_mut().expect("type").api_version = "v1".to_owned()),
            Box::new(|object| object.types.as_mut().expect("type").kind = "ConfigMap".to_owned()),
            Box::new(|object| object.metadata.name = Some("other".to_owned())),
            Box::new(|object| object.metadata.namespace = Some("other".to_owned())),
            Box::new(|object| object.metadata.uid = Some("other-uid".to_owned())),
            Box::new(|object| object.metadata.resource_version = Some(String::new())),
            Box::new(|object| mark_deleting(&mut object.metadata)),
            Box::new(|object| object.metadata.owner_references = None),
            Box::new(|object| {
                object.metadata.owner_references.as_mut().expect("owner")[0].api_version =
                    "pgshard.io/v2".to_owned();
            }),
            Box::new(|object| {
                object.metadata.owner_references.as_mut().expect("owner")[0].kind =
                    "Other".to_owned();
            }),
            Box::new(|object| {
                object.metadata.owner_references.as_mut().expect("owner")[0].name =
                    "other".to_owned();
            }),
            Box::new(|object| {
                object.metadata.owner_references.as_mut().expect("owner")[0].uid =
                    "other-cluster-uid".to_owned();
            }),
            Box::new(|object| {
                object.metadata.owner_references.as_mut().expect("owner")[0].controller =
                    Some(false);
            }),
            Box::new(|object| {
                object.metadata.owner_references.as_mut().expect("owner")[0].block_owner_deletion =
                    Some(false);
            }),
            Box::new(|object| {
                object
                    .metadata
                    .labels
                    .as_mut()
                    .expect("labels")
                    .insert("app.kubernetes.io/component".to_owned(), "other".to_owned());
            }),
            Box::new(|object| {
                object
                    .metadata
                    .annotations
                    .as_mut()
                    .expect("annotations")
                    .insert("pgshard.io/apply-ownership".to_owned(), "v2".to_owned());
            }),
            Box::new(|object| object.data["spec"]["request"] = json!({"unsafe": true})),
            Box::new(|object| object.data["status"]["acceptance"] = json!({"unsafe": true})),
            Box::new(|object| object.data["unexpected"] = Value::Bool(true)),
            Box::new(|object| object.metadata.finalizers = Some(vec!["retain".to_owned()])),
            Box::new(|object| object.metadata.generate_name = Some("demo-".to_owned())),
        ];
        for mutate in mutations {
            let mut fixture = Fixture::valid();
            mutate(&mut fixture.carrier);
            assert!(fixture.validate().is_err());
        }
    }

    #[test]
    fn accepts_fresh_carrier_resource_version_for_later_cas() {
        let mut fixture = Fixture::valid();
        fixture.carrier.metadata.resource_version = Some("new-carrier-rv".to_owned());
        let validated = fixture.validate().expect("fresh carrier RV is valid");
        assert_eq!(validated.carrier_resource_version, "new-carrier-rv");
    }

    #[test]
    fn rejects_source_pod_identity_deletion_controller_label_and_account_drift() {
        let mutations: Vec<PodMutation> = vec![
            Box::new(|pod| pod.metadata.name = Some("other".to_owned())),
            Box::new(|pod| pod.metadata.namespace = Some("other".to_owned())),
            Box::new(|pod| pod.metadata.uid = Some("other-pod-uid".to_owned())),
            Box::new(|pod| pod.metadata.resource_version = Some(String::new())),
            Box::new(|pod| mark_deleting(&mut pod.metadata)),
            Box::new(|pod| pod.metadata.owner_references = None),
            Box::new(|pod| {
                pod.metadata.owner_references.as_mut().expect("owner")[0].api_version =
                    "apps/v2".to_owned();
            }),
            Box::new(|pod| {
                pod.metadata.owner_references.as_mut().expect("owner")[0].kind =
                    "ReplicaSet".to_owned();
            }),
            Box::new(|pod| {
                pod.metadata.owner_references.as_mut().expect("owner")[0].name = "other".to_owned();
            }),
            Box::new(|pod| {
                pod.metadata.owner_references.as_mut().expect("owner")[0].uid = String::new();
            }),
            Box::new(|pod| {
                pod.metadata.owner_references.as_mut().expect("owner")[0].controller = Some(false);
            }),
            Box::new(|pod| {
                pod.metadata.owner_references.as_mut().expect("owner")[0].block_owner_deletion =
                    Some(false);
            }),
            Box::new(|pod| {
                pod.metadata
                    .labels
                    .as_mut()
                    .expect("labels")
                    .insert(CLUSTER_LABEL.to_owned(), "other".to_owned());
            }),
            Box::new(|pod| {
                pod.metadata
                    .labels
                    .as_mut()
                    .expect("labels")
                    .insert(SHARD_LABEL.to_owned(), "0001".to_owned());
            }),
            Box::new(|pod| {
                pod.metadata
                    .labels
                    .as_mut()
                    .expect("labels")
                    .insert(MEMBER_LABEL.to_owned(), "0001".to_owned());
            }),
            Box::new(|pod| {
                pod.metadata
                    .labels
                    .as_mut()
                    .expect("labels")
                    .remove(COMPONENT_LABEL);
            }),
            Box::new(|pod| {
                pod.metadata
                    .labels
                    .as_mut()
                    .expect("labels")
                    .insert(COMPONENT_LABEL.to_owned(), "pooler".to_owned());
            }),
            Box::new(|pod| {
                pod.metadata
                    .labels
                    .as_mut()
                    .expect("labels")
                    .remove(MANAGED_BY_LABEL);
            }),
            Box::new(|pod| {
                pod.metadata
                    .labels
                    .as_mut()
                    .expect("labels")
                    .insert(MANAGED_BY_LABEL.to_owned(), "foreign-controller".to_owned());
            }),
            Box::new(|pod| {
                pod.metadata
                    .labels
                    .as_mut()
                    .expect("labels")
                    .insert(ROLE_LABEL.to_owned(), "primary".to_owned());
            }),
            Box::new(|pod| {
                pod.spec.as_mut().expect("spec").service_account_name = Some("default".to_owned());
            }),
        ];
        for mutate in mutations {
            let mut fixture = Fixture::valid();
            mutate(&mut fixture.source_pod);
            assert!(fixture.validate().is_err());
        }
    }

    #[test]
    fn accepts_benign_pod_rv_and_controller_uid_churn_at_the_sealed_pod_uid() {
        let mut fixture = Fixture::valid();
        fixture.source_pod.metadata.resource_version = Some("new-status-rv".to_owned());
        fixture
            .source_pod
            .metadata
            .owner_references
            .as_mut()
            .expect("owner")[0]
            .uid = "replacement-stateful-set-uid".to_owned();
        assert!(fixture.validate().is_ok());
    }

    #[test]
    fn derives_exact_bounded_agent_service_account_at_cluster_name_boundaries() {
        for (cluster_name, stateful_set_name, expected_service_account) in [
            (
                "a".repeat(41),
                format!("{}-shard-0000", "a".repeat(41)),
                format!("{}-shard-0000-agent", "a".repeat(41)),
            ),
            (
                "a".repeat(42),
                "aaaaaaaaaaaaaaaaa-7a538607fdaab9296995929f-shard-0000".to_owned(),
                "aaaaaaaaaaaaaaaaa-7a538607fdaab9296995929f-shard-0000-agent".to_owned(),
            ),
            (
                "a".repeat(50),
                "aaaaaaaaaaaaaaaaa-160b4e433e384e05e537dc59-shard-0000".to_owned(),
                "aaaaaaaaaaaaaaaaa-160b4e433e384e05e537dc59-shard-0000-agent".to_owned(),
            ),
        ] {
            assert_eq!(
                source_service_account_name(&stateful_set_name),
                expected_service_account
            );
            let mut expected = expected();
            expected.cluster_name = cluster_name;
            expected.source_stateful_set_name = stateful_set_name;
            expected.source_pod_name = format!("{}-0", expected.source_stateful_set_name);
            expected.source_service_account_name = expected_service_account;
            let pod = source_pod(&expected);
            assert!(validate_source_pod(&pod, &expected).is_ok());
        }
    }

    #[test]
    fn rejects_writable_lease_identity_owner_holder_and_transition_drift() {
        let mutations: Vec<LeaseMutation> = vec![
            Box::new(|lease| lease.metadata.name = Some("other".to_owned())),
            Box::new(|lease| lease.metadata.namespace = Some("other".to_owned())),
            Box::new(|lease| lease.metadata.uid = Some("other-uid".to_owned())),
            Box::new(|lease| lease.metadata.resource_version = Some("other-rv".to_owned())),
            Box::new(|lease| mark_deleting(&mut lease.metadata)),
            Box::new(|lease| lease.metadata.owner_references = None),
            Box::new(|lease| {
                lease.metadata.owner_references.as_mut().expect("owner")[0].name =
                    "other".to_owned();
            }),
            Box::new(|lease| {
                lease.metadata.owner_references.as_mut().expect("owner")[0].uid =
                    "other-cluster-uid".to_owned();
            }),
            Box::new(|lease| {
                lease.metadata.owner_references.as_mut().expect("owner")[0].controller =
                    Some(false);
            }),
            Box::new(|lease| {
                lease.metadata.owner_references.as_mut().expect("owner")[0].block_owner_deletion =
                    Some(false);
            }),
            Box::new(|lease| {
                lease.spec.as_mut().expect("spec").holder_identity = Some("other".to_owned());
            }),
            Box::new(|lease| {
                lease.spec.as_mut().expect("spec").lease_transitions = Some(8);
            }),
            Box::new(|lease| {
                lease.spec.as_mut().expect("spec").preferred_holder = Some("other".to_owned());
            }),
        ];
        for mutate in mutations {
            let mut fixture = Fixture::valid();
            mutate(&mut fixture.writable_lease);
            assert!(fixture.validate().is_err());
        }
    }

    #[test]
    fn rejects_leadership_lease_identity_owner_and_holder_drift() {
        let mutations: Vec<LeaseMutation> = vec![
            Box::new(|lease| lease.metadata.name = Some("other".to_owned())),
            Box::new(|lease| lease.metadata.namespace = Some("other".to_owned())),
            Box::new(|lease| lease.metadata.uid = Some("other-uid".to_owned())),
            Box::new(|lease| lease.metadata.resource_version = Some("other-rv".to_owned())),
            Box::new(|lease| mark_deleting(&mut lease.metadata)),
            Box::new(|lease| lease.metadata.owner_references = None),
            Box::new(|lease| {
                lease.metadata.owner_references.as_mut().expect("owner")[0].uid =
                    "other-cluster-uid".to_owned();
            }),
            Box::new(|lease| {
                lease.metadata.owner_references.as_mut().expect("owner")[0].controller =
                    Some(false);
            }),
            Box::new(|lease| {
                lease.metadata.owner_references.as_mut().expect("owner")[0].block_owner_deletion =
                    Some(false);
            }),
            Box::new(|lease| {
                lease.spec.as_mut().expect("spec").holder_identity = Some(format!(
                    "other/dispatcher-pod-uid/{}",
                    "123e4567-e89b-42d3-a456-426614174000"
                ));
            }),
            Box::new(|lease| {
                lease.spec.as_mut().expect("spec").holder_identity = Some(
                    concat!(
                        "demo-orchestrator-0/other-pod-uid/",
                        "123e4567-e89b-42d3-a456-426614174000"
                    )
                    .to_owned(),
                );
            }),
            Box::new(|lease| {
                lease.spec.as_mut().expect("spec").holder_identity =
                    Some("demo-orchestrator-0/dispatcher-pod-uid/not-a-uuid".to_owned());
            }),
        ];
        for mutate in mutations {
            let mut fixture = Fixture::valid();
            mutate(&mut fixture.leadership_lease);
            assert!(fixture.validate().is_err());
        }
    }

    struct FakeStore {
        carrier: DynamicObject,
        source_pod: Pod,
        writable_lease: Lease,
        leadership_lease: Lease,
        delays: [Duration; 4],
        calls: Mutex<Vec<&'static str>>,
    }

    impl FakeStore {
        fn new(fixture: &Fixture, delays: [Duration; 4]) -> Self {
            Self {
                carrier: fixture.carrier.clone(),
                source_pod: fixture.source_pod.clone(),
                writable_lease: fixture.writable_lease.clone(),
                leadership_lease: fixture.leadership_lease.clone(),
                delays,
                calls: Mutex::new(Vec::new()),
            }
        }

        async fn record_and_wait(&self, call: &'static str, delay: Duration) {
            self.calls.lock().expect("call trace").push(call);
            tokio::time::sleep(delay).await;
        }
    }

    impl LiveObjectStore for FakeStore {
        async fn get_carrier(&self) -> Result<DynamicObject, CatalogActivationLiveObjectError> {
            self.record_and_wait("get carrier", self.delays[0]).await;
            Ok(self.carrier.clone())
        }

        async fn get_source_pod(&self) -> Result<Pod, CatalogActivationLiveObjectError> {
            self.record_and_wait("get source Pod", self.delays[1]).await;
            Ok(self.source_pod.clone())
        }

        async fn get_writable_lease(&self) -> Result<Lease, CatalogActivationLiveObjectError> {
            self.record_and_wait("get writable Lease", self.delays[2])
                .await;
            Ok(self.writable_lease.clone())
        }

        async fn get_leadership_lease(&self) -> Result<Lease, CatalogActivationLiveObjectError> {
            self.record_and_wait("get leadership Lease", self.delays[3])
                .await;
            Ok(self.leadership_lease.clone())
        }
    }

    #[tokio::test]
    async fn issues_only_four_exact_gets_and_no_dispatcher_pod_or_secret_read() {
        let fixture = Fixture::valid();
        let store = FakeStore::new(&fixture, [Duration::ZERO; 4]);
        let result = read_and_validate(&store, &fixture.expected, Duration::from_secs(1)).await;
        assert!(result.is_ok());
        assert_eq!(
            *store.calls.lock().expect("call trace"),
            vec![
                "get carrier",
                "get source Pod",
                "get writable Lease",
                "get leadership Lease"
            ]
        );
    }

    #[tokio::test]
    async fn bounds_each_get_independently() {
        let fixture = Fixture::valid();
        let store = FakeStore::new(
            &fixture,
            [
                Duration::from_millis(100),
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
            ],
        );
        let result = read_and_validate(&store, &fixture.expected, Duration::from_millis(10)).await;
        assert!(matches!(
            result,
            Err(CatalogActivationLiveObjectError::RequestTimeout(
                "read catalog-activation carrier"
            ))
        ));
        assert_eq!(
            *store.calls.lock().expect("call trace"),
            vec!["get carrier"]
        );
    }

    #[tokio::test]
    async fn overall_bound_includes_accumulated_successful_get_latency() {
        let fixture = Fixture::valid();
        let store = FakeStore::new(
            &fixture,
            [
                Duration::from_millis(40),
                Duration::from_millis(40),
                Duration::ZERO,
                Duration::ZERO,
            ],
        );
        let result = read_authoritative(
            &store,
            &fixture.expected,
            Duration::from_millis(50),
            Duration::from_millis(70),
        )
        .await;
        assert!(matches!(
            result,
            Err(CatalogActivationLiveObjectError::OverallTimeout)
        ));
        assert_eq!(
            *store.calls.lock().expect("call trace"),
            vec!["get carrier", "get source Pod"]
        );
    }
}
