//! Single-attempt, resource-version-bound catalog-activation publication.
//!
//! Preparing a replacement performs one exact carrier read and retains the
//! complete API object so the parent-resource `PUT` preserves operator-owned
//! metadata. Publication performs no read before its one `PUT`; callers can
//! therefore revalidate in-process authority immediately before polling the
//! returned future. An ambiguous write is resolved by exactly one authoritative
//! `GET` and is never retried.

use std::time::Duration;

use kube::api::{Api, DynamicObject, PostParams};
use kube::config::Config;
use kube::core::{ApiResource, GroupVersionKind};
use kube::{Client, ResourceExt};
use pgshard_types::catalog_activation::CatalogActivationRequest;
use serde::Deserialize;
use thiserror::Error;
use tokio::time::Instant;

use crate::catalog_materialization::{
    CatalogActivationPublicationTarget, PreparedCatalogActivationRequest,
};

const CARRIER_KIND: &str = "PgShardCatalogActivation";
const PUBLISH_REQUEST_TIMEOUT: Duration = Duration::from_secs(6);
pub(crate) const PUBLICATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(8);

/// A validated parent-resource replacement which has not been sent.
///
/// This type deliberately has no `Clone`, serialization, or `Debug`
/// implementation. It grants one logical publication attempt only.
#[allow(dead_code)] // Composed only after the runtime supervisor is reviewed.
pub(crate) struct PendingCatalogActivationPublication {
    replacement: DynamicObject,
    request: CatalogActivationRequest,
    request_sha256: String,
}

/// Terminal result of one publication attempt.
#[allow(dead_code)] // Composed only after the runtime supervisor is reviewed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogActivationPublicationOutcome {
    /// The exact prepared request is installed in the carrier.
    Installed,
    /// A different nonempty request or acceptance occupies the carrier.
    ForeignPublication,
    /// The write result could not be proven after its one resolution read.
    Indeterminate,
}

/// Exact, namespaced parent-resource publisher.
#[allow(dead_code)] // Composed only after the runtime supervisor is reviewed.
pub(crate) struct CatalogActivationPublisher {
    store: KubernetesCarrierPublicationStore,
    expected: ExpectedCarrier,
}

#[allow(dead_code)] // Composed only after the runtime supervisor is reviewed.
impl CatalogActivationPublisher {
    pub(crate) fn new(
        target: &CatalogActivationPublicationTarget,
    ) -> Result<Self, CatalogActivationPublisherError> {
        let expected = ExpectedCarrier::from_target(target)?;
        let store = KubernetesCarrierPublicationStore::new(&expected)?;
        Ok(Self { store, expected })
    }

    /// Reads the exact empty carrier at the live-reader resource version and
    /// builds a metadata-preserving parent-resource replacement.
    pub(crate) async fn prepare(
        &self,
        resource_version: &str,
        prepared: &PreparedCatalogActivationRequest,
    ) -> Result<PendingCatalogActivationPublication, CatalogActivationPublisherError> {
        prepare_with_store(&self.store, &self.expected, resource_version, prepared).await
    }

    /// Sends the replacement once. Any failed response gets exactly one read
    /// for resolution; the write itself is never retried.
    pub(crate) async fn publish(
        &self,
        pending: PendingCatalogActivationPublication,
    ) -> CatalogActivationPublicationOutcome {
        publish_with_store(&self.store, &self.expected, pending).await
    }
}

struct ExpectedCarrier {
    api_version: String,
    kind: &'static str,
    plural: &'static str,
    namespace: String,
    name: String,
    uid: String,
}

impl ExpectedCarrier {
    fn from_target(
        target: &CatalogActivationPublicationTarget,
    ) -> Result<Self, CatalogActivationPublisherError> {
        let api_version = format!(
            "{}/{}",
            target.carrier_api_group(),
            target.carrier_api_version()
        );
        if target.carrier_name().is_empty()
            || target.carrier_namespace().is_empty()
            || target.carrier_uid().is_empty()
            || target.carrier_api_plural().is_empty()
        {
            return Err(CatalogActivationPublisherError::InvalidTarget);
        }
        Ok(Self {
            api_version,
            kind: CARRIER_KIND,
            plural: target.carrier_api_plural(),
            namespace: target.carrier_namespace().to_owned(),
            name: target.carrier_name().to_owned(),
            uid: target.carrier_uid().to_owned(),
        })
    }
}

async fn prepare_with_store<S: CarrierPublicationStore>(
    store: &S,
    expected: &ExpectedCarrier,
    resource_version: &str,
    prepared: &PreparedCatalogActivationRequest,
) -> Result<PendingCatalogActivationPublication, CatalogActivationPublisherError> {
    prepare_parts(
        store,
        expected,
        resource_version,
        prepared.request(),
        prepared.sha256(),
    )
    .await
}

async fn prepare_parts<S: CarrierPublicationStore>(
    store: &S,
    expected: &ExpectedCarrier,
    resource_version: &str,
    request: &CatalogActivationRequest,
    request_sha256: &str,
) -> Result<PendingCatalogActivationPublication, CatalogActivationPublisherError> {
    if resource_version.is_empty() || resource_version.len() > 256 {
        return Err(CatalogActivationPublisherError::InvalidResourceVersion);
    }
    let mut carrier = store.get().await?;
    validate_identity_and_version(&carrier, expected, resource_version)?;
    if carrier_body(&carrier)?.is_nonempty() {
        return Err(CatalogActivationPublisherError::CarrierNotEmpty);
    }
    let data = carrier
        .data
        .as_object_mut()
        .ok_or(CatalogActivationPublisherError::InvalidCarrierBody)?;
    data.insert(
        "spec".to_owned(),
        serde_json::json!({
            "request": request,
            "requestSHA256": request_sha256,
        }),
    );
    Ok(PendingCatalogActivationPublication {
        replacement: carrier,
        request: request.clone(),
        request_sha256: request_sha256.to_owned(),
    })
}

async fn publish_with_store<S: CarrierPublicationStore>(
    store: &S,
    expected: &ExpectedCarrier,
    pending: PendingCatalogActivationPublication,
) -> CatalogActivationPublicationOutcome {
    let PendingCatalogActivationPublication {
        replacement,
        request,
        request_sha256,
    } = pending;
    let attempt_deadline = Instant::now() + PUBLICATION_ATTEMPT_TIMEOUT;
    match tokio::time::timeout(PUBLISH_REQUEST_TIMEOUT, store.replace(replacement)).await {
        Ok(Ok(carrier)) => classify_carrier(&carrier, expected, &request, &request_sha256),
        Ok(Err(_)) | Err(_) => {
            resolve_failed_publication(store, expected, &request, &request_sha256, attempt_deadline)
                .await
        }
    }
}

async fn resolve_failed_publication<S: CarrierPublicationStore>(
    store: &S,
    expected: &ExpectedCarrier,
    request: &CatalogActivationRequest,
    request_sha256: &str,
    attempt_deadline: Instant,
) -> CatalogActivationPublicationOutcome {
    let remaining = attempt_deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return CatalogActivationPublicationOutcome::Indeterminate;
    }
    match tokio::time::timeout(remaining, store.get()).await {
        Ok(Ok(carrier)) => classify_carrier(&carrier, expected, request, request_sha256),
        Ok(Err(_)) | Err(_) => CatalogActivationPublicationOutcome::Indeterminate,
    }
}

fn classify_carrier(
    carrier: &DynamicObject,
    expected: &ExpectedCarrier,
    request: &CatalogActivationRequest,
    request_sha256: &str,
) -> CatalogActivationPublicationOutcome {
    if validate_identity(carrier, expected).is_err() {
        return CatalogActivationPublicationOutcome::ForeignPublication;
    }
    match carrier_body(carrier) {
        Ok(body)
            if body.request() == Some(request) && body.request_sha256() == Some(request_sha256) =>
        {
            CatalogActivationPublicationOutcome::Installed
        }
        Ok(body) if body.is_nonempty() => CatalogActivationPublicationOutcome::ForeignPublication,
        _ => CatalogActivationPublicationOutcome::Indeterminate,
    }
}

fn validate_identity_and_version(
    carrier: &DynamicObject,
    expected: &ExpectedCarrier,
    resource_version: &str,
) -> Result<(), CatalogActivationPublisherError> {
    validate_identity(carrier, expected)?;
    if carrier.metadata.resource_version.as_deref() != Some(resource_version) {
        return Err(CatalogActivationPublisherError::ResourceVersionChanged);
    }
    Ok(())
}

fn validate_identity(
    carrier: &DynamicObject,
    expected: &ExpectedCarrier,
) -> Result<(), CatalogActivationPublisherError> {
    let types = carrier
        .types
        .as_ref()
        .ok_or(CatalogActivationPublisherError::InvalidCarrierIdentity)?;
    if types.api_version != expected.api_version
        || types.kind != expected.kind
        || carrier.name_any() != expected.name
        || carrier.namespace().as_deref() != Some(expected.namespace.as_str())
        || carrier.metadata.uid.as_deref() != Some(expected.uid.as_str())
        || carrier.metadata.deletion_timestamp.is_some()
    {
        return Err(CatalogActivationPublisherError::InvalidCarrierIdentity);
    }
    Ok(())
}

fn carrier_body(carrier: &DynamicObject) -> Result<CarrierBody, CatalogActivationPublisherError> {
    serde_json::from_value(carrier.data.clone())
        .map_err(|_| CatalogActivationPublisherError::InvalidCarrierBody)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CarrierBody {
    #[serde(default)]
    spec: CarrierSpec,
    #[serde(default)]
    status: CarrierStatus,
}

impl CarrierBody {
    fn is_nonempty(&self) -> bool {
        self.spec.request.is_some()
            || self.spec.request_sha256.is_some()
            || self.status.acceptance.is_some()
    }

    fn request(&self) -> Option<&CatalogActivationRequest> {
        self.spec.request.as_ref()
    }

    fn request_sha256(&self) -> Option<&str> {
        self.spec.request_sha256.as_deref()
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CarrierSpec {
    request: Option<CatalogActivationRequest>,
    #[serde(rename = "requestSHA256")]
    request_sha256: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CarrierStatus {
    acceptance: Option<serde_json::Value>,
}

trait CarrierPublicationStore: Send + Sync {
    async fn get(&self) -> Result<DynamicObject, CatalogActivationPublisherError>;
    async fn replace(
        &self,
        replacement: DynamicObject,
    ) -> Result<DynamicObject, CatalogActivationPublisherError>;
}

struct KubernetesCarrierPublicationStore {
    api: Api<DynamicObject>,
    name: String,
}

impl KubernetesCarrierPublicationStore {
    fn new(expected: &ExpectedCarrier) -> Result<Self, CatalogActivationPublisherError> {
        let mut client_config = Config::incluster().map_err(|error| {
            CatalogActivationPublisherError::KubernetesConfig(error.to_string())
        })?;
        client_config.connect_timeout = Some(PUBLISH_REQUEST_TIMEOUT);
        client_config.read_timeout = Some(PUBLISH_REQUEST_TIMEOUT);
        client_config.write_timeout = Some(PUBLISH_REQUEST_TIMEOUT);
        client_config.default_retry = false;
        let client = Client::try_from(client_config).map_err(|error| {
            CatalogActivationPublisherError::KubernetesClient(error.to_string())
        })?;
        let resource = ApiResource::from_gvk_with_plural(
            &GroupVersionKind::gvk("pgshard.io", "v1alpha1", expected.kind),
            expected.plural,
        );
        Ok(Self {
            api: Api::namespaced_with(client, &expected.namespace, &resource),
            name: expected.name.clone(),
        })
    }
}

impl CarrierPublicationStore for KubernetesCarrierPublicationStore {
    async fn get(&self) -> Result<DynamicObject, CatalogActivationPublisherError> {
        match tokio::time::timeout(PUBLISH_REQUEST_TIMEOUT, self.api.get(&self.name)).await {
            Ok(Ok(carrier)) => Ok(carrier),
            Ok(Err(error)) => Err(CatalogActivationPublisherError::Kubernetes(Box::new(error))),
            Err(_) => Err(CatalogActivationPublisherError::TimedOut),
        }
    }

    async fn replace(
        &self,
        replacement: DynamicObject,
    ) -> Result<DynamicObject, CatalogActivationPublisherError> {
        match tokio::time::timeout(
            PUBLISH_REQUEST_TIMEOUT,
            self.api
                .replace(&self.name, &PostParams::default(), &replacement),
        )
        .await
        {
            Ok(Ok(carrier)) => Ok(carrier),
            Ok(Err(error)) => Err(CatalogActivationPublisherError::Kubernetes(Box::new(error))),
            Err(_) => Err(CatalogActivationPublisherError::TimedOut),
        }
    }
}

/// Fail-closed publisher construction and preflight errors.
#[derive(Debug, Error)]
pub(crate) enum CatalogActivationPublisherError {
    #[error("invalid catalog-activation publication target")]
    InvalidTarget,
    #[error("invalid catalog-activation carrier resourceVersion")]
    InvalidResourceVersion,
    #[error("catalog-activation carrier identity is invalid")]
    InvalidCarrierIdentity,
    #[error("catalog-activation carrier resourceVersion changed before publication")]
    ResourceVersionChanged,
    #[error("catalog-activation carrier body is invalid")]
    InvalidCarrierBody,
    #[error("catalog-activation carrier is already nonempty")]
    CarrierNotEmpty,
    #[error("catalog-activation Kubernetes request timed out")]
    TimedOut,
    #[error("configure in-cluster catalog-activation publisher: {0}")]
    KubernetesConfig(String),
    #[error("build in-cluster catalog-activation publisher: {0}")]
    KubernetesClient(String),
    #[error("catalog-activation Kubernetes request failed: {0}")]
    Kubernetes(#[source] Box<kube::Error>),
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::future::pending as future_pending;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use kube::core::ApiResource;
    use pgshard_types::catalog_activation::{
        CatalogActivationBootstrap, CatalogActivationCandidate, CatalogActivationCluster,
        CatalogActivationDispatcher, CatalogActivationMaterials,
        CatalogActivationRemoteApplyWitness, CatalogActivationSource,
        CatalogActivationTargetFenceAcknowledgement, CatalogActivationWritableTerm,
        CatalogMaterialIdentity, KubernetesObjectIdentity, MaterialIdentity,
    };

    use super::*;

    struct FakeStore {
        gets: Mutex<VecDeque<Result<DynamicObject, CatalogActivationPublisherError>>>,
        replacement_result: Mutex<Option<Result<DynamicObject, CatalogActivationPublisherError>>>,
        replacements: Mutex<Vec<DynamicObject>>,
    }

    impl FakeStore {
        fn new(
            gets: Vec<Result<DynamicObject, CatalogActivationPublisherError>>,
            replacement_result: Result<DynamicObject, CatalogActivationPublisherError>,
        ) -> Self {
            Self {
                gets: Mutex::new(gets.into()),
                replacement_result: Mutex::new(Some(replacement_result)),
                replacements: Mutex::new(Vec::new()),
            }
        }

        fn get_count(&self) -> usize {
            self.gets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        }

        fn replacements(&self) -> std::sync::MutexGuard<'_, Vec<DynamicObject>> {
            self.replacements
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    impl CarrierPublicationStore for FakeStore {
        async fn get(&self) -> Result<DynamicObject, CatalogActivationPublisherError> {
            self.gets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("unexpected carrier GET")
        }

        async fn replace(
            &self,
            replacement: DynamicObject,
        ) -> Result<DynamicObject, CatalogActivationPublisherError> {
            self.replacements
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(replacement);
            self.replacement_result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .expect("publication PUT was retried")
        }
    }

    #[derive(Default)]
    struct StalledStore {
        gets: AtomicUsize,
        replacements: AtomicUsize,
    }

    impl CarrierPublicationStore for StalledStore {
        async fn get(&self) -> Result<DynamicObject, CatalogActivationPublisherError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            future_pending().await
        }

        async fn replace(
            &self,
            _replacement: DynamicObject,
        ) -> Result<DynamicObject, CatalogActivationPublisherError> {
            self.replacements.fetch_add(1, Ordering::SeqCst);
            future_pending().await
        }
    }

    fn expected() -> ExpectedCarrier {
        ExpectedCarrier {
            api_version: "pgshard.io/v1alpha1".to_owned(),
            kind: CARRIER_KIND,
            plural: "pgshardcatalogactivations",
            namespace: "database".to_owned(),
            name: "demo-catalog-activation".to_owned(),
            uid: "carrier-uid".to_owned(),
        }
    }

    fn carrier(data: serde_json::Value) -> DynamicObject {
        let resource = ApiResource::from_gvk_with_plural(
            &GroupVersionKind::gvk("pgshard.io", "v1alpha1", CARRIER_KIND),
            "pgshardcatalogactivations",
        );
        let mut carrier =
            DynamicObject::new("demo-catalog-activation", &resource).within("database");
        carrier.metadata.uid = Some("carrier-uid".to_owned());
        carrier.metadata.resource_version = Some("17".to_owned());
        carrier.metadata.labels = Some(BTreeMap::from([(
            "operator-owned".to_owned(),
            "preserved".to_owned(),
        )]));
        carrier.data = data;
        carrier
    }

    fn digest(value: u8) -> String {
        format!("{value:02x}").repeat(32)
    }

    #[allow(clippy::too_many_lines)]
    fn request() -> CatalogActivationRequest {
        CatalogActivationRequest {
            schema_version: "pgshard.catalog-activation-request.v1".to_owned(),
            carrier: KubernetesObjectIdentity {
                name: "demo-catalog-activation".to_owned(),
                uid: "carrier-uid".to_owned(),
            },
            cluster: CatalogActivationCluster {
                name: "demo".to_owned(),
                namespace: "database".to_owned(),
                uid: "cluster-uid".to_owned(),
                generation: "7".to_owned(),
                resource_version: "101".to_owned(),
                status_sha256: digest(1),
            },
            dispatcher: CatalogActivationDispatcher {
                pod_name: "demo-orchestrator-0".to_owned(),
                pod_uid: "dispatcher-uid".to_owned(),
                lease_name: "demo-orch-lease".to_owned(),
                lease_uid: "orchestrator-lease-uid".to_owned(),
                lease_resource_version: "102".to_owned(),
                lease_holder:
                    "demo-orchestrator-0/dispatcher-uid/11111111-2222-4333-8444-555555555555"
                        .to_owned(),
            },
            candidate: CatalogActivationCandidate {
                name: "demo-s0-m0000-cfg-00112233445566778899aabbccddeeff".to_owned(),
                uid: "candidate-uid".to_owned(),
                resource_version: "103".to_owned(),
                payload_sha256: digest(2),
            },
            bootstrap: CatalogActivationBootstrap {
                secret: KubernetesObjectIdentity {
                    name: "bootstrap-secret".to_owned(),
                    uid: "bootstrap-secret-uid".to_owned(),
                },
                pvc: KubernetesObjectIdentity {
                    name: "bootstrap-pvc".to_owned(),
                    uid: "bootstrap-pvc-uid".to_owned(),
                },
            },
            writable_term: CatalogActivationWritableTerm {
                name: "demo-shard-0000-term".to_owned(),
                uid: "writable-lease-uid".to_owned(),
                resource_version: "104".to_owned(),
                holder: "demo-shard-0000-member-0000-0/source-pod-uid/0123456789abcdef01234567"
                    .to_owned(),
                generation: "9".to_owned(),
            },
            materials: CatalogActivationMaterials {
                replication: MaterialIdentity {
                    name: "replication".to_owned(),
                    uid: "replication-uid".to_owned(),
                    material_sha256: digest(3),
                },
                catalog: CatalogMaterialIdentity {
                    name: "catalog".to_owned(),
                    uid: "catalog-uid".to_owned(),
                    client_sha256: digest(4),
                    server_sha256: digest(5),
                },
                operation_writer: MaterialIdentity {
                    name: "writer".to_owned(),
                    uid: "writer-uid".to_owned(),
                    material_sha256: digest(6),
                },
                postgresql_configuration: MaterialIdentity {
                    name: "configuration".to_owned(),
                    uid: "configuration-uid".to_owned(),
                    material_sha256: digest(7),
                },
                migration_sha256: digest(8),
                genesis_sha256: digest(9),
                preflight_sha256: digest(10),
                serving_hba_version: "pgshard.catalog-serving-hba.v1".to_owned(),
                serving_hba_sha256: digest(11),
                target_template_sha256: digest(12),
            },
            source: CatalogActivationSource {
                cluster_name: "demo".to_owned(),
                cluster_uid: "cluster-uid".to_owned(),
                pod_name: "demo-shard-0000-member-0000-0".to_owned(),
                pod_uid: "source-pod-uid".to_owned(),
                shard: 0,
                member: 0,
                instance_id: "demo-shard-0000-member-0000-0".to_owned(),
                boot_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_owned(),
                postmaster_pid: 100,
                system_identifier: "12345678901234567890".to_owned(),
                timeline: 3,
                generation_identity: "generation".to_owned(),
                generation_barrier_lsn: "4294967296".to_owned(),
                target_fence_acknowledgement: CatalogActivationTargetFenceAcknowledgement {
                    observed_at_unix_ms: "1700000000000".to_owned(),
                    deadline_boottime_ns: "9000000000".to_owned(),
                    remaining_validity_at_ack_ms: "5000".to_owned(),
                    remaining_validity_at_report_ms: "4500".to_owned(),
                    control_backend_pid: 101,
                },
            },
            remote_apply_witness: CatalogActivationRemoteApplyWitness {
                cluster_name: "demo".to_owned(),
                cluster_uid: "cluster-uid".to_owned(),
                pod_name: "demo-shard-0000-member-0001-0".to_owned(),
                pod_uid: "witness-pod-uid".to_owned(),
                shard: 0,
                member: 1,
                instance_id: "demo-shard-0000-member-0001-0".to_owned(),
                boot_id: "ffffffff-1111-2222-3333-444444444444".to_owned(),
                postmaster_pid: 200,
                member_slot_name: "pgshard_member_0001".to_owned(),
                system_identifier: "12345678901234567890".to_owned(),
                timeline: 3,
                generation_identity: "generation".to_owned(),
                generation_barrier_lsn: "4294967296".to_owned(),
                receive_lsn: "4294967396".to_owned(),
                replay_lsn: "4294967396".to_owned(),
            },
        }
    }

    fn installed(request: &CatalogActivationRequest, request_sha256: &str) -> DynamicObject {
        carrier(serde_json::json!({
            "spec": {"request": request, "requestSHA256": request_sha256},
            "status": {},
        }))
    }

    fn pending(
        request: &CatalogActivationRequest,
        request_sha256: &str,
    ) -> PendingCatalogActivationPublication {
        PendingCatalogActivationPublication {
            replacement: installed(request, request_sha256),
            request: request.clone(),
            request_sha256: request_sha256.to_owned(),
        }
    }

    #[tokio::test]
    async fn prepare_requires_the_observed_version_and_preserves_metadata() {
        let request = request();
        let request_sha256 = digest(13);
        let store = FakeStore::new(
            vec![Ok(carrier(serde_json::json!({"spec": {}, "status": {}})))],
            Err(CatalogActivationPublisherError::TimedOut),
        );
        let pending = prepare_parts(&store, &expected(), "17", &request, &request_sha256)
            .await
            .expect("exact empty carrier");
        assert_eq!(store.get_count(), 0);
        assert!(store.replacements().is_empty());
        assert_eq!(
            pending.replacement.metadata.labels,
            Some(BTreeMap::from([(
                "operator-owned".to_owned(),
                "preserved".to_owned()
            )]))
        );
        assert_eq!(
            carrier_body(&pending.replacement)
                .expect("prepared body")
                .request(),
            Some(&request)
        );

        let changed = FakeStore::new(
            vec![Ok(carrier(serde_json::json!({"spec": {}, "status": {}})))],
            Err(CatalogActivationPublisherError::TimedOut),
        );
        assert!(matches!(
            prepare_parts(&changed, &expected(), "18", &request, &request_sha256).await,
            Err(CatalogActivationPublisherError::ResourceVersionChanged)
        ));
    }

    #[tokio::test]
    async fn one_successful_put_installs_without_a_resolution_read() {
        let request = request();
        let request_sha256 = digest(13);
        let store = FakeStore::new(Vec::new(), Ok(installed(&request, &request_sha256)));
        assert_eq!(
            publish_with_store(&store, &expected(), pending(&request, &request_sha256)).await,
            CatalogActivationPublicationOutcome::Installed
        );
        assert_eq!(store.replacements().len(), 1);
        assert_eq!(store.get_count(), 0);
    }

    #[tokio::test]
    async fn ambiguous_put_uses_one_get_and_never_retries() {
        let request = request();
        let request_sha256 = digest(13);
        let store = FakeStore::new(
            vec![Ok(installed(&request, &request_sha256))],
            Err(CatalogActivationPublisherError::TimedOut),
        );
        assert_eq!(
            publish_with_store(&store, &expected(), pending(&request, &request_sha256)).await,
            CatalogActivationPublicationOutcome::Installed
        );
        assert_eq!(store.replacements().len(), 1);
        assert_eq!(store.get_count(), 0);
    }

    #[tokio::test]
    async fn unresolved_empty_and_foreign_carriers_are_terminal() {
        let request = request();
        let request_sha256 = digest(13);
        let empty = FakeStore::new(
            vec![Ok(carrier(serde_json::json!({"spec": {}, "status": {}})))],
            Err(CatalogActivationPublisherError::TimedOut),
        );
        assert_eq!(
            publish_with_store(&empty, &expected(), pending(&request, &request_sha256)).await,
            CatalogActivationPublicationOutcome::Indeterminate
        );
        assert_eq!(empty.replacements().len(), 1);
        assert_eq!(empty.get_count(), 0);

        let foreign = FakeStore::new(
            vec![Ok(installed(&request, &digest(14)))],
            Err(CatalogActivationPublisherError::TimedOut),
        );
        assert_eq!(
            publish_with_store(&foreign, &expected(), pending(&request, &request_sha256)).await,
            CatalogActivationPublicationOutcome::ForeignPublication
        );
        assert_eq!(foreign.replacements().len(), 1);
        assert_eq!(foreign.get_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_put_and_resolution_get_share_one_deadline() {
        let request = request();
        let request_sha256 = digest(13);
        let store = StalledStore::default();
        let started = Instant::now();

        assert_eq!(
            publish_with_store(&store, &expected(), pending(&request, &request_sha256)).await,
            CatalogActivationPublicationOutcome::Indeterminate
        );
        assert_eq!(started.elapsed(), PUBLICATION_ATTEMPT_TIMEOUT);
        assert_eq!(store.replacements.load(Ordering::SeqCst), 1);
        assert_eq!(store.gets.load(Ordering::SeqCst), 1);
    }
}
