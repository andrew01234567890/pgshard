//! Dormant, fault-isolated catalog-activation carrier consumer.
//!
//! This module can durably acknowledge one exact request, but deliberately has
//! no executor and no connection to `PostgreSQL`, readiness, serving, routing,
//! SQL, fencing, or Lease authority. Capability publication is held separately
//! from `/status` and is withdrawn whenever local recovery or an exact direct
//! carrier read fails.

use std::fs::{self, File, Metadata};
use std::future::Future;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use kube::api::{Api, DynamicObject, PostParams, TypeMeta};
use kube::core::{ApiResource, GroupVersionKind};
use kube::{Client, Config, Resource, ResourceExt};
use pgshard_types::ShardId;
use pgshard_types::catalog_activation::{
    CATALOG_ACTIVATION_ACCEPTANCE_VERSION, CATALOG_ACTIVATION_CAPABILITY_VERSION,
    CATALOG_ACTIVATION_CONSUMER_VERSION, CATALOG_ACTIVATION_FSYNC_PERSISTENCE,
    CATALOG_ACTIVATION_REQUEST_VERSION, CatalogActivationAcceptance, CatalogActivationCapability,
    CatalogActivationCapabilityCarrier, CatalogActivationCapabilityCluster,
    CatalogActivationCapabilityTarget, CatalogActivationRequest, KubernetesObjectIdentity,
    postgresql_member_pod_name,
};
use rustix::fs::{Mode, OFlags, mkdirat, open, openat};
use rustix::process::geteuid;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;

use crate::catalog_activation::{
    CatalogActivationJournal, CatalogActivationJournalError, DurableCatalogActivationAcceptance,
};
use crate::domain::AgentIdentity;

const CARRIER_API_VERSION: &str = "pgshard.io/v1alpha1";
const CARRIER_KIND: &str = "PgShardCatalogActivation";
const CARRIER_PLURAL: &str = "pgshardcatalogactivations";
const CLUSTER_KIND: &str = "PgShardCluster";
const JOURNAL_OWNER_DIRECTORY: &str = "owner";
const JOURNAL_DIRECTORY: &str = "journal";
const INITIAL_RETRY: Duration = Duration::from_millis(250);
const MAXIMUM_RETRY: Duration = Duration::from_secs(5);
const READY_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Validated configuration for the dormant shard-zero member-zero consumer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogActivationConsumerConfig {
    identity: AgentIdentity,
    cluster_uid: String,
    pod_uid: String,
    carrier_namespace: String,
    carrier_name: String,
    carrier_uid: String,
    journal_root: PathBuf,
    request_timeout: Duration,
}

impl CatalogActivationConsumerConfig {
    /// Builds one exact consumer identity.
    ///
    /// # Errors
    ///
    /// Returns an error unless the identity is the canonical shard-zero,
    /// member-zero Pod, or for an inconsistent fixed carrier name, unsafe
    /// Kubernetes identity, unsafe journal root, or unbounded request timeout.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        identity: AgentIdentity,
        cluster_uid: String,
        pod_uid: String,
        carrier_namespace: String,
        carrier_name: String,
        carrier_uid: String,
        journal_root: PathBuf,
        request_timeout: Duration,
    ) -> Result<Self, CatalogActivationConsumerConfigError> {
        if identity.shard_id != ShardId(0)
            || !valid_dns_label(&carrier_namespace)
            || !valid_dns_subdomain(&carrier_name, 253)
            || carrier_name != format!("{}-catalog-activation", identity.cluster_id)
            || identity.instance_id != postgresql_member_pod_name(&identity.cluster_id, 0, 0)
            || !valid_uid(&cluster_uid)
            || !valid_uid(&pod_uid)
            || !valid_uid(&carrier_uid)
            || identity.instance_id.is_empty()
            || identity.instance_id.len() > 253
            || !absolute_normal_path(&journal_root)
            || !(Duration::from_millis(100)..=Duration::from_secs(30)).contains(&request_timeout)
        {
            return Err(CatalogActivationConsumerConfigError);
        }
        Ok(Self {
            identity,
            cluster_uid,
            pod_uid,
            carrier_namespace,
            carrier_name,
            carrier_uid,
            journal_root,
            request_timeout,
        })
    }

    fn capability(&self) -> CatalogActivationCapability {
        CatalogActivationCapability {
            schema_version: CATALOG_ACTIVATION_CAPABILITY_VERSION.to_owned(),
            capability: CATALOG_ACTIVATION_CONSUMER_VERSION.to_owned(),
            request_schema_version: CATALOG_ACTIVATION_REQUEST_VERSION.to_owned(),
            acceptance_schema_version: CATALOG_ACTIVATION_ACCEPTANCE_VERSION.to_owned(),
            persistence: CATALOG_ACTIVATION_FSYNC_PERSISTENCE.to_owned(),
            cluster: CatalogActivationCapabilityCluster {
                name: self.identity.cluster_id.clone(),
                uid: self.cluster_uid.clone(),
            },
            carrier: CatalogActivationCapabilityCarrier {
                namespace: self.carrier_namespace.clone(),
                name: self.carrier_name.clone(),
                uid: self.carrier_uid.clone(),
            },
            target: CatalogActivationCapabilityTarget {
                shard: 0,
                member: 0,
                instance_id: self.identity.instance_id.clone(),
                pod_name: self.identity.instance_id.clone(),
                pod_uid: self.pod_uid.clone(),
            },
        }
    }

    fn target(&self) -> KubernetesObjectIdentity {
        KubernetesObjectIdentity {
            name: self.identity.instance_id.clone(),
            uid: self.pod_uid.clone(),
        }
    }

    fn journal_path(&self) -> PathBuf {
        self.journal_root
            .join(JOURNAL_OWNER_DIRECTORY)
            .join(JOURNAL_DIRECTORY)
    }
}

/// Configuration validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("invalid catalog-activation consumer configuration")]
pub struct CatalogActivationConsumerConfigError;

/// Independently stored endpoint availability.
#[derive(Clone, Debug)]
pub struct CatalogActivationCapabilityState {
    inner: Arc<RwLock<CatalogActivationEndpointState>>,
}

/// Current response class for the separate capability endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogActivationEndpointState {
    /// No consumer configuration was supplied.
    Disabled,
    /// Configured, but recovery and an exact carrier read are not both valid.
    Unavailable,
    /// Recovered and reconciled against an uncached exact carrier GET.
    Available(Box<CatalogActivationCapability>),
}

impl CatalogActivationCapabilityState {
    /// Creates a state for a disabled endpoint.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(RwLock::new(CatalogActivationEndpointState::Disabled)),
        }
    }

    /// Creates a configured endpoint which starts unavailable.
    #[must_use]
    pub fn configured() -> Self {
        Self {
            inner: Arc::new(RwLock::new(CatalogActivationEndpointState::Unavailable)),
        }
    }

    /// Returns one exact endpoint snapshot.
    #[must_use]
    pub fn snapshot(&self) -> CatalogActivationEndpointState {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn unavailable(&self) {
        *self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            CatalogActivationEndpointState::Unavailable;
    }

    pub(crate) fn available(&self, capability: CatalogActivationCapability) {
        *self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            CatalogActivationEndpointState::Available(Box::new(capability));
    }
}

impl Default for CatalogActivationCapabilityState {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Starts the optional consumer in a detached, fault-isolated task.
///
/// The task observes global shutdown but never sends it and its failures never
/// join the HTTP, `PostgreSQL`, writable-Lease, or readiness lifecycles.
pub fn spawn_catalog_activation_consumer(
    config: Option<CatalogActivationConsumerConfig>,
    capability: CatalogActivationCapabilityState,
    shutdown: watch::Receiver<bool>,
) {
    let Some(config) = config else {
        return;
    };
    tokio::spawn(async move {
        let _withdraw_on_exit = CapabilityWithdrawalGuard(capability.clone());
        supervise(config, capability, shutdown).await;
    });
}

struct CapabilityWithdrawalGuard(CatalogActivationCapabilityState);

impl Drop for CapabilityWithdrawalGuard {
    fn drop(&mut self) {
        self.0.unavailable();
    }
}

async fn supervise(
    config: CatalogActivationConsumerConfig,
    capability: CatalogActivationCapabilityState,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut retry = INITIAL_RETRY;
    while !*shutdown.borrow() {
        capability.unavailable();
        match run_attempt(&config, &capability, &mut shutdown).await {
            Ok(()) => return,
            Err(error) => {
                tracing::warn!(reason = %error, retry_after_ms = retry.as_millis(),
                    "catalog-activation consumer unavailable; retrying independently");
            }
        }
        if wait_or_stop(&mut shutdown, retry).await {
            return;
        }
        retry = retry.saturating_mul(2).min(MAXIMUM_RETRY);
    }
}

async fn run_attempt(
    config: &CatalogActivationConsumerConfig,
    capability: &CatalogActivationCapabilityState,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), CatalogActivationConsumerError> {
    let journal_root = config.journal_root.clone();
    let journal_path = config.journal_path();
    let mut journal_task = tokio::task::spawn_blocking(move || {
        prepare_journal_parent(&journal_root)?;
        CatalogActivationJournal::open_or_create(journal_path)
            .map_err(CatalogActivationConsumerError::Journal)
    });
    let Some(result) = complete_or_shutdown(&mut journal_task, capability, shutdown).await else {
        return Ok(());
    };
    let journal = result.map_err(CatalogActivationConsumerError::JournalTask)??;
    if *shutdown.borrow() {
        capability.unavailable();
        return Ok(());
    }
    let store = KubernetesCarrierStore::new(config)?;
    run_reconcile_loop(config, capability, &store, journal, shutdown).await
}

async fn run_reconcile_loop<S: CarrierStore>(
    config: &CatalogActivationConsumerConfig,
    capability: &CatalogActivationCapabilityState,
    store: &S,
    mut journal: CatalogActivationJournal,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), CatalogActivationConsumerError> {
    loop {
        let mut reconciliation = Box::pin(reconcile_once(config, store, journal));
        let Some(result) = complete_or_shutdown(&mut reconciliation, capability, shutdown).await
        else {
            return Ok(());
        };
        match result {
            Ok((returned, response)) => {
                journal = returned;
                if *shutdown.borrow() {
                    capability.unavailable();
                    return Ok(());
                }
                capability.available(response);
            }
            Err(error) => {
                capability.unavailable();
                return Err(error);
            }
        }
        if wait_or_stop(shutdown, READY_POLL_INTERVAL).await {
            capability.unavailable();
            return Ok(());
        }
    }
}

async fn reconcile_once<S: CarrierStore>(
    config: &CatalogActivationConsumerConfig,
    store: &S,
    journal: CatalogActivationJournal,
) -> Result<(CatalogActivationJournal, CatalogActivationCapability), CatalogActivationConsumerError>
{
    let carrier = store.get().await?;
    let validated = validate_carrier(config, &carrier)?;
    let Some(request) = validated.request else {
        return Ok((journal, config.capability()));
    };

    if let Some(existing) = validated.acceptance {
        validate_acceptance(config, &request, &existing)?;
        let target = config.target();
        let digest = request.declared_digest.clone();
        let exact_request = request.request.clone();
        let (journal, receipt) = run_journal(journal, move |journal| {
            journal
                .resolve_acceptance(&exact_request, &digest, &target)
                .and_then(|receipt| {
                    receipt.ok_or(CatalogActivationJournalError::AcceptedWithoutPrepared)
                })
        })
        .await?;
        require_receipt(&receipt, &existing)?;
        return Ok((journal, config.capability()));
    }

    let target = config.target();
    let digest = request.declared_digest.clone();
    let exact_request = request.request.clone();
    let (journal, receipt) = run_journal(journal, move |journal| {
        persist_acceptance(journal, &exact_request, &digest, &target)
    })
    .await?;
    let acceptance = acceptance_from_receipt(&receipt);
    drop(receipt);

    // Yield after the durable local barrier. The supervising biased shutdown
    // select observes cancellation before this task is polled again and before
    // a status write can be dispatched.
    tokio::task::yield_now().await;
    match store
        .replace_status(&validated.resource_version, &acceptance)
        .await
    {
        Ok(replaced) => {
            validate_replaced_acceptance(config, &request, &acceptance, &replaced)?;
            Ok((journal, config.capability()))
        }
        Err(replace_error) => {
            // A timeout is outcome-unknown and a 409 means another writer won
            // the resourceVersion race. Resolve both only from a new exact GET.
            tokio::task::yield_now().await;
            async {
                let current = store.get().await?;
                validate_replaced_acceptance(config, &request, &acceptance, &current)
                    .map_err(|error| CarrierStoreError::Validation(Box::new(error)))?;
                Ok(())
            }
            .await
            .map_err(|resolution| {
                CatalogActivationConsumerError::StatusResolution {
                    replace: replace_error,
                    resolution: Box::new(resolution),
                }
            })?;
            Ok((journal, config.capability()))
        }
    }
}

async fn run_journal<T: Send + 'static>(
    mut journal: CatalogActivationJournal,
    operation: impl FnOnce(&mut CatalogActivationJournal) -> Result<T, CatalogActivationJournalError>
    + Send
    + 'static,
) -> Result<(CatalogActivationJournal, T), CatalogActivationConsumerError> {
    let (journal, result) = tokio::task::spawn_blocking(move || {
        let result = operation(&mut journal);
        (journal, result)
    })
    .await
    .map_err(CatalogActivationConsumerError::JournalTask)?;
    result
        .map(|result| (journal, result))
        .map_err(CatalogActivationConsumerError::Journal)
}

fn persist_acceptance(
    journal: &mut CatalogActivationJournal,
    request: &CatalogActivationRequest,
    digest: &str,
    target: &KubernetesObjectIdentity,
) -> Result<DurableCatalogActivationAcceptance, CatalogActivationJournalError> {
    match journal.prepare(request, digest) {
        Ok(_) => {}
        Err(CatalogActivationJournalError::OutcomeUnknown { .. }) => {
            journal.prepare(request, digest)?;
        }
        Err(error) => return Err(error),
    }
    match journal.accept(request, digest, target) {
        Ok(receipt) => Ok(receipt),
        Err(CatalogActivationJournalError::OutcomeUnknown { .. }) => {
            if let Some(receipt) = journal.resolve_acceptance(request, digest, target)? {
                return Ok(receipt);
            }
            journal.accept(request, digest, target)
        }
        Err(error) => Err(error),
    }
}

fn acceptance_from_receipt(
    receipt: &DurableCatalogActivationAcceptance,
) -> CatalogActivationAcceptance {
    CatalogActivationAcceptance {
        schema_version: CATALOG_ACTIVATION_ACCEPTANCE_VERSION.to_owned(),
        carrier_uid: receipt.carrier_uid().to_owned(),
        request_sha256: receipt.request_sha256().to_owned(),
        target_pod_name: receipt.target_pod_name().to_owned(),
        target_pod_uid: receipt.target_pod_uid().to_owned(),
        persistence: CATALOG_ACTIVATION_FSYNC_PERSISTENCE.to_owned(),
        persisted_at_unix_ms: receipt.persisted_at_unix_ms().to_owned(),
    }
}

fn require_receipt(
    receipt: &DurableCatalogActivationAcceptance,
    expected: &CatalogActivationAcceptance,
) -> Result<(), CatalogActivationConsumerError> {
    if acceptance_from_receipt(receipt) == *expected {
        Ok(())
    } else {
        Err(CatalogActivationConsumerError::InvalidCarrierAcceptance)
    }
}

#[derive(Clone, Debug)]
struct ValidatedRequest {
    request: CatalogActivationRequest,
    declared_digest: String,
}

#[derive(Clone, Debug)]
struct ValidatedCarrier {
    resource_version: String,
    request: Option<ValidatedRequest>,
    acceptance: Option<CatalogActivationAcceptance>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CarrierBody {
    #[serde(default)]
    spec: CarrierSpec,
    #[serde(default)]
    status: CarrierStatus,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CarrierSpec {
    request: Option<CatalogActivationRequest>,
    #[serde(rename = "requestSHA256")]
    request_sha256: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CarrierStatus {
    acceptance: Option<CatalogActivationAcceptance>,
}

fn validate_carrier(
    config: &CatalogActivationConsumerConfig,
    carrier: &DynamicObject,
) -> Result<ValidatedCarrier, CatalogActivationConsumerError> {
    let types = carrier
        .types
        .as_ref()
        .ok_or(CatalogActivationConsumerError::InvalidCarrierIdentity)?;
    let owners = carrier
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default();
    let exact_owner = owners.first();
    if types.api_version != CARRIER_API_VERSION
        || types.kind != CARRIER_KIND
        || carrier.name_any() != config.carrier_name
        || carrier.namespace().as_deref() != Some(config.carrier_namespace.as_str())
        || carrier.uid().as_deref() != Some(config.carrier_uid.as_str())
        || carrier.meta().deletion_timestamp.is_some()
        || owners.len() != 1
        || !exact_owner.is_some_and(|reference| {
            reference.controller == Some(true)
                && reference.block_owner_deletion == Some(true)
                && reference.api_version == CARRIER_API_VERSION
                && reference.kind == CLUSTER_KIND
                && reference.name == config.identity.cluster_id
                && reference.uid == config.cluster_uid
        })
    {
        return Err(CatalogActivationConsumerError::InvalidCarrierIdentity);
    }
    let resource_version = carrier
        .resource_version()
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or(CatalogActivationConsumerError::InvalidCarrierIdentity)?;
    let body: CarrierBody = serde_json::from_value(carrier.data.clone())
        .map_err(CatalogActivationConsumerError::InvalidCarrierBody)?;
    let request = match (body.spec.request, body.spec.request_sha256) {
        (None, None) => None,
        (Some(request), Some(declared_digest)) => {
            validate_request(config, &request, &declared_digest)?;
            Some(ValidatedRequest {
                request,
                declared_digest,
            })
        }
        _ => return Err(CatalogActivationConsumerError::InvalidCarrierRequest),
    };
    if request.is_none() && body.status.acceptance.is_some() {
        return Err(CatalogActivationConsumerError::InvalidCarrierAcceptance);
    }
    Ok(ValidatedCarrier {
        resource_version,
        request,
        acceptance: body.status.acceptance,
    })
}

fn validate_request(
    config: &CatalogActivationConsumerConfig,
    request: &CatalogActivationRequest,
    declared_digest: &str,
) -> Result<(), CatalogActivationConsumerError> {
    if request.validate().is_err()
        || request.sha256().as_deref() != Ok(declared_digest)
        || request.carrier.name != config.carrier_name
        || request.carrier.uid != config.carrier_uid
        || request.cluster.name != config.identity.cluster_id
        || request.cluster.namespace != config.carrier_namespace
        || request.cluster.uid != config.cluster_uid
        || request.source.shard != 0
        || request.source.member != 0
        || request.source.instance_id != config.identity.instance_id
        || request.source.pod_name != config.identity.instance_id
        || request.source.pod_uid != config.pod_uid
    {
        return Err(CatalogActivationConsumerError::InvalidCarrierRequest);
    }
    Ok(())
}

fn validate_acceptance(
    config: &CatalogActivationConsumerConfig,
    request: &ValidatedRequest,
    acceptance: &CatalogActivationAcceptance,
) -> Result<(), CatalogActivationConsumerError> {
    if acceptance.schema_version != CATALOG_ACTIVATION_ACCEPTANCE_VERSION
        || acceptance.carrier_uid != config.carrier_uid
        || acceptance.request_sha256 != request.declared_digest
        || acceptance.target_pod_name != config.identity.instance_id
        || acceptance.target_pod_uid != config.pod_uid
        || acceptance.persistence != CATALOG_ACTIVATION_FSYNC_PERSISTENCE
        || !canonical_decimal(&acceptance.persisted_at_unix_ms)
    {
        return Err(CatalogActivationConsumerError::InvalidCarrierAcceptance);
    }
    Ok(())
}

fn validate_replaced_acceptance(
    config: &CatalogActivationConsumerConfig,
    request: &ValidatedRequest,
    acceptance: &CatalogActivationAcceptance,
    carrier: &DynamicObject,
) -> Result<(), CatalogActivationConsumerError> {
    let validated = validate_carrier(config, carrier)?;
    let returned_request = validated
        .request
        .ok_or(CatalogActivationConsumerError::InvalidCarrierRequest)?;
    if returned_request.request != request.request
        || returned_request.declared_digest != request.declared_digest
        || validated.acceptance.as_ref() != Some(acceptance)
    {
        return Err(CatalogActivationConsumerError::InvalidCarrierAcceptance);
    }
    validate_acceptance(config, request, acceptance)
}

trait CarrierStore: Send + Sync {
    async fn get(&self) -> Result<DynamicObject, CarrierStoreError>;
    async fn replace_status(
        &self,
        resource_version: &str,
        acceptance: &CatalogActivationAcceptance,
    ) -> Result<DynamicObject, CarrierStoreError>;
}

struct KubernetesCarrierStore {
    api: Api<DynamicObject>,
    name: String,
    namespace: String,
    request_timeout: Duration,
}

impl KubernetesCarrierStore {
    fn new(
        config: &CatalogActivationConsumerConfig,
    ) -> Result<Self, CatalogActivationConsumerError> {
        let mut client_config = Config::incluster()
            .map_err(|error| CatalogActivationConsumerError::KubernetesConfig(error.to_string()))?;
        client_config.connect_timeout = Some(config.request_timeout);
        client_config.read_timeout = Some(config.request_timeout);
        client_config.write_timeout = Some(config.request_timeout);
        client_config.default_retry = false;
        let client = Client::try_from(client_config)
            .map_err(|error| CatalogActivationConsumerError::KubernetesClient(error.to_string()))?;
        let resource = ApiResource::from_gvk_with_plural(
            &GroupVersionKind::gvk("pgshard.io", "v1alpha1", CARRIER_KIND),
            CARRIER_PLURAL,
        );
        Ok(Self {
            api: Api::namespaced_with(client, &config.carrier_namespace, &resource),
            name: config.carrier_name.clone(),
            namespace: config.carrier_namespace.clone(),
            request_timeout: config.request_timeout,
        })
    }
}

impl CarrierStore for KubernetesCarrierStore {
    async fn get(&self) -> Result<DynamicObject, CarrierStoreError> {
        match tokio::time::timeout(self.request_timeout, self.api.get(&self.name)).await {
            Ok(Ok(carrier)) => Ok(carrier),
            Ok(Err(error)) => Err(CarrierStoreError::Kubernetes(Box::new(error))),
            Err(_) => Err(CarrierStoreError::Timeout),
        }
    }

    async fn replace_status(
        &self,
        resource_version: &str,
        acceptance: &CatalogActivationAcceptance,
    ) -> Result<DynamicObject, CarrierStoreError> {
        let data = serde_json::json!({
            "status": serde_json::to_value(CarrierStatus {
                acceptance: Some(acceptance.clone()),
            })
            .map_err(CarrierStoreError::EncodeStatus)?,
        });
        let replacement = DynamicObject {
            types: Some(TypeMeta {
                api_version: CARRIER_API_VERSION.to_owned(),
                kind: CARRIER_KIND.to_owned(),
            }),
            metadata: kube::core::ObjectMeta {
                name: Some(self.name.clone()),
                namespace: Some(self.namespace.clone()),
                resource_version: Some(resource_version.to_owned()),
                ..kube::core::ObjectMeta::default()
            },
            data,
        };
        match tokio::time::timeout(
            self.request_timeout,
            self.api
                .replace_status(&self.name, &PostParams::default(), &replacement),
        )
        .await
        {
            Ok(Ok(carrier)) => Ok(carrier),
            Ok(Err(kube::Error::Api(status))) if status.code == 409 => {
                Err(CarrierStoreError::Conflict)
            }
            Ok(Err(error)) => Err(CarrierStoreError::Kubernetes(Box::new(error))),
            Err(_) => Err(CarrierStoreError::Timeout),
        }
    }
}

#[derive(Debug, Error)]
enum CarrierStoreError {
    #[error("catalog-activation carrier request timed out")]
    Timeout,
    #[error("catalog-activation carrier status resourceVersion conflicted")]
    Conflict,
    #[error("catalog-activation Kubernetes API request failed: {0}")]
    Kubernetes(#[source] Box<kube::Error>),
    #[error("encode catalog-activation carrier status: {0}")]
    EncodeStatus(#[source] serde_json::Error),
    #[error("replacement carrier validation failed: {0}")]
    Validation(#[source] Box<CatalogActivationConsumerError>),
}

/// Fail-closed consumer error. Every variant withdraws only this capability.
#[derive(Debug, Error)]
enum CatalogActivationConsumerError {
    #[error("catalog-activation journal failed: {0}")]
    Journal(#[source] CatalogActivationJournalError),
    #[error("catalog-activation journal task failed: {0}")]
    JournalTask(#[source] tokio::task::JoinError),
    #[error("catalog-activation carrier store failed: {0}")]
    Store(#[from] CarrierStoreError),
    #[error("invalid catalog-activation carrier API identity, metadata, or owner")]
    InvalidCarrierIdentity,
    #[error("invalid catalog-activation carrier body: {0}")]
    InvalidCarrierBody(#[source] serde_json::Error),
    #[error("invalid or foreign catalog-activation request")]
    InvalidCarrierRequest,
    #[error("catalog-activation carrier acceptance is absent, foreign, or not locally durable")]
    InvalidCarrierAcceptance,
    #[error("configure in-cluster catalog-activation client: {0}")]
    KubernetesConfig(String),
    #[error("build in-cluster catalog-activation client: {0}")]
    KubernetesClient(String),
    #[error(
        "catalog-activation status replace was unresolved: replace={replace}; resolution={resolution}"
    )]
    StatusResolution {
        replace: CarrierStoreError,
        resolution: Box<CarrierStoreError>,
    },
    #[error("unsafe catalog-activation journal mount root {path:?}: {reason}")]
    UnsafeJournalRoot { path: PathBuf, reason: &'static str },
    #[error("catalog-activation journal mount operation failed at {path:?}: {source}")]
    JournalRootIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn prepare_journal_parent(root: &Path) -> Result<(), CatalogActivationConsumerError> {
    let root_directory = open_directory_without_symlinks(root)?;
    let descriptor_metadata = root_directory.metadata().map_err(|source| {
        CatalogActivationConsumerError::JournalRootIo {
            path: root.to_owned(),
            source,
        }
    })?;
    validate_mount_root(root, &descriptor_metadata)?;
    let path_metadata = fs::symlink_metadata(root).map_err(|source| {
        CatalogActivationConsumerError::JournalRootIo {
            path: root.to_owned(),
            source,
        }
    })?;
    validate_mount_root(root, &path_metadata)?;
    if (path_metadata.dev(), path_metadata.ino())
        != (descriptor_metadata.dev(), descriptor_metadata.ino())
    {
        return Err(CatalogActivationConsumerError::UnsafeJournalRoot {
            path: root.to_owned(),
            reason: "mount root changed while opening",
        });
    }
    let created = match mkdirat(
        &root_directory,
        JOURNAL_OWNER_DIRECTORY,
        Mode::RUSR | Mode::WUSR | Mode::XUSR,
    ) {
        Ok(()) => true,
        Err(rustix::io::Errno::EXIST) => false,
        Err(source) => {
            return Err(CatalogActivationConsumerError::JournalRootIo {
                path: root.join(JOURNAL_OWNER_DIRECTORY),
                source: source.into(),
            });
        }
    };
    let owner_descriptor = openat(
        &root_directory,
        JOURNAL_OWNER_DIRECTORY,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|source| CatalogActivationConsumerError::JournalRootIo {
        path: root.join(JOURNAL_OWNER_DIRECTORY),
        source: source.into(),
    })?;
    let owner = File::from(owner_descriptor);
    validate_owner_directory(
        root,
        &owner
            .metadata()
            .map_err(|source| CatalogActivationConsumerError::JournalRootIo {
                path: root.join(JOURNAL_OWNER_DIRECTORY),
                source,
            })?,
    )?;
    if created {
        owner
            .sync_all()
            .map_err(|source| CatalogActivationConsumerError::JournalRootIo {
                path: root.join(JOURNAL_OWNER_DIRECTORY),
                source,
            })?;
    }
    root_directory
        .sync_all()
        .map_err(|source| CatalogActivationConsumerError::JournalRootIo {
            path: root.to_owned(),
            source,
        })?;
    validate_owner_directory(
        root,
        &owner
            .metadata()
            .map_err(|source| CatalogActivationConsumerError::JournalRootIo {
                path: root.join(JOURNAL_OWNER_DIRECTORY),
                source,
            })?,
    )
}

fn open_directory_without_symlinks(path: &Path) -> Result<File, CatalogActivationConsumerError> {
    if !absolute_normal_path(path) {
        return Err(CatalogActivationConsumerError::UnsafeJournalRoot {
            path: path.to_owned(),
            reason: "mount root must be absolute and normalized",
        });
    }
    let descriptor = open(
        "/",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|source| CatalogActivationConsumerError::JournalRootIo {
        path: PathBuf::from("/"),
        source: source.into(),
    })?;
    let mut directory = File::from(descriptor);
    let mut traversed = PathBuf::from("/");
    for component in path.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        traversed.push(name);
        let descriptor = openat(
            &directory,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(|source| CatalogActivationConsumerError::JournalRootIo {
            path: traversed.clone(),
            source: source.into(),
        })?;
        directory = File::from(descriptor);
    }
    Ok(directory)
}

fn validate_mount_root(
    path: &Path,
    metadata: &Metadata,
) -> Result<(), CatalogActivationConsumerError> {
    let euid = geteuid().as_raw();
    if !metadata.is_dir() {
        return Err(CatalogActivationConsumerError::UnsafeJournalRoot {
            path: path.to_owned(),
            reason: "mount root is not a directory",
        });
    }
    if metadata.uid() != euid && metadata.uid() != 0 {
        return Err(CatalogActivationConsumerError::UnsafeJournalRoot {
            path: path.to_owned(),
            reason: "mount root has an untrusted owner",
        });
    }
    if metadata.permissions().mode() & 0o002 != 0 {
        return Err(CatalogActivationConsumerError::UnsafeJournalRoot {
            path: path.to_owned(),
            reason: "mount root is world-writable",
        });
    }
    Ok(())
}

fn validate_owner_directory(
    root: &Path,
    metadata: &Metadata,
) -> Result<(), CatalogActivationConsumerError> {
    if !metadata.is_dir()
        || metadata.uid() != geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != 0o700
    {
        return Err(CatalogActivationConsumerError::UnsafeJournalRoot {
            path: root.join(JOURNAL_OWNER_DIRECTORY),
            reason: "owner directory must be an euid-owned 0700 directory",
        });
    }
    Ok(())
}

fn absolute_normal_path(path: &Path) -> bool {
    path.is_absolute()
        && path != Path::new("/")
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

fn valid_uid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn valid_dns_subdomain(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && value.split('.').all(valid_dns_part)
}

fn valid_dns_label(value: &str) -> bool {
    value.len() <= 63 && valid_dns_part(value)
}

fn valid_dns_part(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && value
            .bytes()
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

fn canonical_decimal(value: &str) -> bool {
    !value.is_empty()
        && (value == "0" || !value.starts_with('0'))
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<u64>().is_ok()
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

async fn wait_until_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn complete_or_shutdown<F: Future>(
    future: F,
    capability: &CatalogActivationCapabilityState,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<F::Output> {
    tokio::pin!(future);
    tokio::select! {
        biased;
        () = wait_until_shutdown(shutdown) => {
            capability.unavailable();
            None
        }
        output = &mut future => Some(output),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
    use pgshard_types::catalog_activation::{
        CatalogActivationBootstrap, CatalogActivationCandidate, CatalogActivationCluster,
        CatalogActivationDispatcher, CatalogActivationMaterials,
        CatalogActivationRemoteApplyWitness, CatalogActivationSource,
        CatalogActivationTargetFenceAcknowledgement, CatalogActivationWritableTerm,
        CatalogMaterialIdentity, MaterialIdentity,
    };
    use pgshard_types::writable_generation::DurableWritableGeneration;
    use tempfile::TempDir;
    use tokio::sync::Notify;

    use super::*;

    const SOURCE_HOLDER: &str = "demo-shard-0000-0/source-pod-uid/0123456789abcdef01234567";
    const DISPATCHER_HOLDER: &str =
        "demo-orchestrator-0/dispatcher-uid/11111111-2222-4333-8444-555555555555";

    #[derive(Clone, Copy)]
    enum ReplaceMode {
        Success,
        ConflictAfterInstall,
    }

    #[derive(Default)]
    struct TestPause {
        entered: Notify,
        release: Notify,
    }

    impl TestPause {
        async fn wait(&self) {
            self.entered.notify_one();
            self.release.notified().await;
        }

        async fn entered(&self) {
            self.entered.notified().await;
        }

        fn release(&self) {
            self.release.notify_one();
        }
    }

    struct FakeStore {
        carrier: Mutex<DynamicObject>,
        replacements: AtomicUsize,
        mode: ReplaceMode,
        get_pause: Option<Arc<TestPause>>,
        replace_pause: Option<Arc<TestPause>>,
    }

    impl FakeStore {
        fn new(carrier: DynamicObject, mode: ReplaceMode) -> Self {
            Self {
                carrier: Mutex::new(carrier),
                replacements: AtomicUsize::new(0),
                mode,
                get_pause: None,
                replace_pause: None,
            }
        }

        fn pausing_get(carrier: DynamicObject, pause: Arc<TestPause>) -> Self {
            let mut store = Self::new(carrier, ReplaceMode::Success);
            store.get_pause = Some(pause);
            store
        }

        fn pausing_replace(carrier: DynamicObject, pause: Arc<TestPause>) -> Self {
            let mut store = Self::new(carrier, ReplaceMode::Success);
            store.replace_pause = Some(pause);
            store
        }

        fn replacement_count(&self) -> usize {
            self.replacements.load(Ordering::SeqCst)
        }
    }

    impl CarrierStore for FakeStore {
        async fn get(&self) -> Result<DynamicObject, CarrierStoreError> {
            if let Some(pause) = &self.get_pause {
                pause.wait().await;
            }
            Ok(self.carrier.lock().expect("carrier lock").clone())
        }

        async fn replace_status(
            &self,
            resource_version: &str,
            acceptance: &CatalogActivationAcceptance,
        ) -> Result<DynamicObject, CarrierStoreError> {
            if let Some(pause) = &self.replace_pause {
                pause.wait().await;
            }
            self.replacements.fetch_add(1, Ordering::SeqCst);
            let mut carrier = self.carrier.lock().expect("carrier lock");
            if carrier.resource_version().as_deref() != Some(resource_version) {
                return Err(CarrierStoreError::Conflict);
            }
            carrier.data["status"] = serde_json::json!({"acceptance": acceptance});
            carrier.metadata.resource_version = Some("2".into());
            match self.mode {
                ReplaceMode::Success => Ok(carrier.clone()),
                ReplaceMode::ConflictAfterInstall => Err(CarrierStoreError::Conflict),
            }
        }
    }

    fn config(root: &TempDir) -> CatalogActivationConsumerConfig {
        CatalogActivationConsumerConfig::new(
            AgentIdentity {
                cluster_id: "demo".into(),
                shard_id: ShardId(0),
                instance_id: "demo-shard-0000-0".into(),
            },
            "cluster-uid".into(),
            "source-pod-uid".into(),
            "database".into(),
            "demo-catalog-activation".into(),
            "carrier-uid".into(),
            root.path().to_owned(),
            Duration::from_secs(2),
        )
        .expect("valid test consumer")
    }

    fn open_journal(config: &CatalogActivationConsumerConfig) -> CatalogActivationJournal {
        prepare_journal_parent(&config.journal_root).expect("prepare journal owner");
        CatalogActivationJournal::open_or_create(config.journal_path()).expect("open journal")
    }

    fn carrier(request: Option<CatalogActivationRequest>) -> DynamicObject {
        let spec = request.map_or_else(
            || serde_json::json!({}),
            |request| {
                let digest = request.sha256().expect("valid request digest");
                serde_json::json!({"request": request, "requestSHA256": digest})
            },
        );
        DynamicObject {
            types: Some(TypeMeta {
                api_version: CARRIER_API_VERSION.into(),
                kind: CARRIER_KIND.into(),
            }),
            metadata: kube::core::ObjectMeta {
                name: Some("demo-catalog-activation".into()),
                namespace: Some("database".into()),
                uid: Some("carrier-uid".into()),
                resource_version: Some("1".into()),
                owner_references: Some(vec![OwnerReference {
                    api_version: CARRIER_API_VERSION.into(),
                    kind: CLUSTER_KIND.into(),
                    name: "demo".into(),
                    uid: "cluster-uid".into(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..kube::core::ObjectMeta::default()
            },
            data: serde_json::json!({"spec": spec, "status": {}}),
        }
    }

    fn digest(value: u8) -> String {
        format!("{value:02x}").repeat(32)
    }

    fn generation_identity_for_holder(holder: &str) -> String {
        String::from_utf8(
            DurableWritableGeneration::new(
                "demo".into(),
                "cluster-uid".into(),
                ShardId(0),
                "database".into(),
                "demo-shard-0000-term".into(),
                "writable-lease-uid".into(),
                holder.into(),
                9,
            )
            .expect("valid generation")
            .canonical_bytes(),
        )
        .expect("UTF-8 generation")
    }

    fn generation_identity() -> String {
        generation_identity_for_holder(SOURCE_HOLDER)
    }

    #[allow(clippy::too_many_lines)]
    fn request() -> CatalogActivationRequest {
        CatalogActivationRequest {
            schema_version: CATALOG_ACTIVATION_REQUEST_VERSION.to_owned(),
            carrier: KubernetesObjectIdentity {
                name: "demo-catalog-activation".into(),
                uid: "carrier-uid".into(),
            },
            cluster: CatalogActivationCluster {
                name: "demo".into(),
                namespace: "database".into(),
                uid: "cluster-uid".into(),
                generation: "7".into(),
                resource_version: "101".into(),
                status_sha256: digest(1),
            },
            dispatcher: CatalogActivationDispatcher {
                pod_name: "demo-orchestrator-0".into(),
                pod_uid: "dispatcher-uid".into(),
                lease_name: "demo-orch-lease".into(),
                lease_uid: "orchestrator-lease-uid".into(),
                lease_resource_version: "102".into(),
                lease_holder: DISPATCHER_HOLDER.into(),
            },
            candidate: CatalogActivationCandidate {
                name: "demo-s0-m0000-cfg-00112233445566778899aabbccddeeff".into(),
                uid: "candidate-uid".into(),
                resource_version: "103".into(),
                payload_sha256: digest(2),
            },
            bootstrap: CatalogActivationBootstrap {
                secret: KubernetesObjectIdentity {
                    name: "bootstrap-secret".into(),
                    uid: "bootstrap-secret-uid".into(),
                },
                pvc: KubernetesObjectIdentity {
                    name: "bootstrap-pvc".into(),
                    uid: "bootstrap-pvc-uid".into(),
                },
            },
            writable_term: CatalogActivationWritableTerm {
                name: "demo-shard-0000-term".into(),
                uid: "writable-lease-uid".into(),
                resource_version: "104".into(),
                holder: SOURCE_HOLDER.into(),
                generation: "9".into(),
            },
            materials: CatalogActivationMaterials {
                replication: MaterialIdentity {
                    name: "replication".into(),
                    uid: "replication-uid".into(),
                    material_sha256: digest(3),
                },
                catalog: CatalogMaterialIdentity {
                    name: "catalog".into(),
                    uid: "catalog-uid".into(),
                    client_sha256: digest(4),
                    server_sha256: digest(5),
                },
                operation_writer: MaterialIdentity {
                    name: "writer".into(),
                    uid: "writer-uid".into(),
                    material_sha256: digest(6),
                },
                postgresql_configuration: MaterialIdentity {
                    name: "configuration".into(),
                    uid: "configuration-uid".into(),
                    material_sha256: digest(7),
                },
                migration_sha256: digest(8),
                genesis_sha256: digest(9),
                preflight_sha256: digest(10),
                serving_hba_version: "pgshard.catalog-serving-hba.v1".into(),
                serving_hba_sha256: digest(11),
                target_template_sha256: digest(12),
            },
            source: CatalogActivationSource {
                cluster_name: "demo".into(),
                cluster_uid: "cluster-uid".into(),
                pod_name: "demo-shard-0000-0".into(),
                pod_uid: "source-pod-uid".into(),
                shard: 0,
                member: 0,
                instance_id: "demo-shard-0000-0".into(),
                boot_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into(),
                postmaster_pid: 100,
                system_identifier: "12345678901234567890".into(),
                timeline: 3,
                generation_identity: generation_identity(),
                generation_barrier_lsn: "4294967296".into(),
                target_fence_acknowledgement: CatalogActivationTargetFenceAcknowledgement {
                    observed_at_unix_ms: "1700000000000".into(),
                    deadline_boottime_ns: "9000000000".into(),
                    remaining_validity_at_ack_ms: "5000".into(),
                    remaining_validity_at_report_ms: "4500".into(),
                    control_backend_pid: 101,
                },
            },
            remote_apply_witness: CatalogActivationRemoteApplyWitness {
                cluster_name: "demo".into(),
                cluster_uid: "cluster-uid".into(),
                pod_name: "demo-shard-0000-m0001-0".into(),
                pod_uid: "witness-pod-uid".into(),
                shard: 0,
                member: 1,
                instance_id: "demo-shard-0000-m0001-0".into(),
                boot_id: "ffffffff-1111-2222-3333-444444444444".into(),
                postmaster_pid: 200,
                member_slot_name: "pgshard_member_0001".into(),
                system_identifier: "12345678901234567890".into(),
                timeline: 3,
                generation_identity: generation_identity(),
                generation_barrier_lsn: "4294967296".into(),
                receive_lsn: "4294967396".into(),
                replay_lsn: "4294967396".into(),
            },
        }
    }

    #[test]
    fn config_rejects_foreign_roles_names_and_paths() {
        let root = TempDir::new().expect("temporary root");
        let valid = config(&root);
        assert_eq!(valid.capability().target.member, 0);

        let mut nonzero = valid.identity.clone();
        nonzero.shard_id = ShardId(1);
        assert!(
            CatalogActivationConsumerConfig::new(
                nonzero,
                valid.cluster_uid.clone(),
                valid.pod_uid.clone(),
                valid.carrier_namespace.clone(),
                valid.carrier_name.clone(),
                valid.carrier_uid.clone(),
                valid.journal_root.clone(),
                valid.request_timeout,
            )
            .is_err()
        );
        let mut member_one = valid.identity.clone();
        member_one.instance_id = postgresql_member_pod_name("demo", 0, 1);
        assert!(
            CatalogActivationConsumerConfig::new(
                member_one,
                valid.cluster_uid.clone(),
                valid.pod_uid.clone(),
                valid.carrier_namespace.clone(),
                valid.carrier_name.clone(),
                valid.carrier_uid.clone(),
                valid.journal_root.clone(),
                valid.request_timeout,
            )
            .is_err()
        );
        assert!(
            CatalogActivationConsumerConfig::new(
                valid.identity.clone(),
                valid.cluster_uid.clone(),
                valid.pod_uid.clone(),
                valid.carrier_namespace.clone(),
                "other-catalog-activation".into(),
                valid.carrier_uid.clone(),
                PathBuf::from("relative"),
                valid.request_timeout,
            )
            .is_err()
        );
    }

    #[test]
    fn every_detached_task_exit_withdraws_a_previous_capability() {
        let root = TempDir::new().expect("temporary root");
        let state = CatalogActivationCapabilityState::configured();
        state.available(config(&root).capability());
        {
            let _guard = CapabilityWithdrawalGuard(state.clone());
            assert!(matches!(
                state.snapshot(),
                CatalogActivationEndpointState::Available(_)
            ));
        }
        assert_eq!(
            state.snapshot(),
            CatalogActivationEndpointState::Unavailable
        );
    }

    #[tokio::test]
    async fn shutdown_cancels_a_paused_get_without_a_retry_or_mutation() {
        let root = TempDir::new().expect("temporary root");
        let configuration = Arc::new(config(&root));
        let journal = open_journal(&configuration);
        let pause = Arc::new(TestPause::default());
        let store = Arc::new(FakeStore::pausing_get(carrier(None), pause.clone()));
        let capability = CatalogActivationCapabilityState::configured();
        capability.available(configuration.capability());
        let task_capability = capability.clone();
        let task_configuration = configuration.clone();
        let task_store = store.clone();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _withdraw_on_exit = CapabilityWithdrawalGuard(task_capability.clone());
            run_reconcile_loop(
                &task_configuration,
                &task_capability,
                task_store.as_ref(),
                journal,
                &mut shutdown_rx,
            )
            .await
        });
        pause.entered().await;
        shutdown_tx.send(true).expect("request shutdown");
        task.await.expect("join consumer").expect("clean shutdown");
        pause.release();

        assert_eq!(
            capability.snapshot(),
            CatalogActivationEndpointState::Unavailable
        );
        assert_eq!(store.replacement_count(), 0);
        assert!(
            fs::read_dir(configuration.journal_path())
                .expect("journal directory")
                .next()
                .is_none(),
            "paused GET shutdown must not mutate the journal"
        );
    }

    #[tokio::test]
    async fn shutdown_cancels_a_paused_journal_worker_and_withdraws_capability() {
        let root = TempDir::new().expect("temporary root");
        let configuration = config(&root);
        let journal = open_journal(&configuration);
        let capability = CatalogActivationCapabilityState::configured();
        capability.available(configuration.capability());
        let task_capability = capability.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _withdraw_on_exit = CapabilityWithdrawalGuard(task_capability.clone());
            complete_or_shutdown(
                run_journal(journal, move |_| {
                    entered_tx.send(()).expect("signal journal entry");
                    release_rx.recv().expect("release journal worker");
                    Ok(())
                }),
                &task_capability,
                &mut shutdown_rx,
            )
            .await
        });
        tokio::task::spawn_blocking(move || entered_rx.recv().expect("journal worker entered"))
            .await
            .expect("join entry wait");
        shutdown_tx.send(true).expect("request shutdown");
        assert!(task.await.expect("join cancellation").is_none());
        assert_eq!(
            capability.snapshot(),
            CatalogActivationEndpointState::Unavailable
        );
        release_tx
            .send(())
            .expect("release detached journal worker");
    }

    #[tokio::test]
    async fn shutdown_cancels_a_paused_status_replace_before_dispatch() {
        let root = TempDir::new().expect("temporary root");
        let configuration = Arc::new(config(&root));
        let journal = open_journal(&configuration);
        let pause = Arc::new(TestPause::default());
        let store = Arc::new(FakeStore::pausing_replace(
            carrier(Some(request())),
            pause.clone(),
        ));
        let capability = CatalogActivationCapabilityState::configured();
        capability.available(configuration.capability());
        let task_capability = capability.clone();
        let task_configuration = configuration.clone();
        let task_store = store.clone();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _withdraw_on_exit = CapabilityWithdrawalGuard(task_capability.clone());
            run_reconcile_loop(
                &task_configuration,
                &task_capability,
                task_store.as_ref(),
                journal,
                &mut shutdown_rx,
            )
            .await
        });
        pause.entered().await;
        shutdown_tx.send(true).expect("request shutdown");
        task.await.expect("join consumer").expect("clean shutdown");
        pause.release();

        assert_eq!(
            capability.snapshot(),
            CatalogActivationEndpointState::Unavailable
        );
        assert_eq!(
            store.replacement_count(),
            0,
            "shutdown must cancel status replacement before the fake dispatch"
        );
        assert!(configuration.journal_path().join("accepted").is_file());
    }

    #[tokio::test]
    async fn journal_worker_panic_is_typed_and_withdraws_capability() {
        let root = TempDir::new().expect("temporary root");
        let configuration = config(&root);
        let journal = open_journal(&configuration);
        let capability = CatalogActivationCapabilityState::configured();
        capability.available(configuration.capability());
        let result = {
            let _withdraw_on_exit = CapabilityWithdrawalGuard(capability.clone());
            run_journal::<()>(journal, |_| panic!("injected journal worker panic")).await
        };
        assert!(matches!(
            result,
            Err(CatalogActivationConsumerError::JournalTask(error)) if error.is_panic()
        ));
        assert_eq!(
            capability.snapshot(),
            CatalogActivationEndpointState::Unavailable
        );
    }

    #[test]
    fn journal_root_uses_exact_private_owner_and_rejects_an_unsafe_existing_owner() {
        let root = TempDir::new().expect("temporary root");
        let configuration = config(&root);
        prepare_journal_parent(&configuration.journal_root).expect("prepare owner");
        let owner = configuration.journal_root.join(JOURNAL_OWNER_DIRECTORY);
        let metadata = fs::metadata(&owner).expect("owner metadata");
        assert_eq!(metadata.uid(), geteuid().as_raw());
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o700);
        assert_eq!(configuration.journal_path(), owner.join(JOURNAL_DIRECTORY));

        fs::set_permissions(&owner, fs::Permissions::from_mode(0o770)).expect("make owner unsafe");
        assert!(matches!(
            prepare_journal_parent(&configuration.journal_root),
            Err(CatalogActivationConsumerError::UnsafeJournalRoot { .. })
        ));

        for mode in [0o4700, 0o2700, 0o1700] {
            fs::set_permissions(&owner, fs::Permissions::from_mode(mode))
                .expect("set special owner mode");
            assert!(matches!(
                prepare_journal_parent(&configuration.journal_root),
                Err(CatalogActivationConsumerError::UnsafeJournalRoot { .. })
            ));
        }
    }

    #[test]
    fn symlinked_journal_ancestor_is_rejected_before_any_mutation() {
        let outer = TempDir::new().expect("temporary root");
        let real_parent = outer.path().join("real");
        let real_root = real_parent.join("root");
        fs::create_dir(&real_parent).expect("create real parent");
        fs::create_dir(&real_root).expect("create real root");
        let alias = outer.path().join("alias");
        symlink(&real_parent, &alias).expect("create ancestor symlink");
        let aliased_root = alias.join("root");

        assert!(prepare_journal_parent(&aliased_root).is_err());
        assert!(
            !real_root.join(JOURNAL_OWNER_DIRECTORY).exists(),
            "ancestor rejection must happen before mkdir"
        );
    }

    #[tokio::test]
    async fn empty_exact_carrier_advertises_without_journal_or_status_mutation() {
        let root = TempDir::new().expect("temporary root");
        let configuration = config(&root);
        let journal = open_journal(&configuration);
        let store = FakeStore::new(carrier(None), ReplaceMode::Success);
        let (journal, response) = reconcile_once(&configuration, &store, journal)
            .await
            .expect("empty exact carrier");
        assert_eq!(response, configuration.capability());
        assert_eq!(store.replacement_count(), 0);
        drop(journal);
        let entries = fs::read_dir(configuration.journal_path())
            .expect("journal directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("journal entries");
        assert!(
            entries.is_empty(),
            "empty carrier must not mutate the journal"
        );
    }

    #[tokio::test]
    async fn request_is_durable_before_status_and_restart_requires_local_acceptance() {
        let root = TempDir::new().expect("temporary root");
        let configuration = config(&root);
        let store = FakeStore::new(carrier(Some(request())), ReplaceMode::Success);
        let journal = open_journal(&configuration);
        let (journal, response) = reconcile_once(&configuration, &store, journal)
            .await
            .expect("persist and publish acceptance");
        assert_eq!(response, configuration.capability());
        assert_eq!(store.replacement_count(), 1);
        assert!(configuration.journal_path().join("prepared").is_file());
        assert!(configuration.journal_path().join("accepted").is_file());
        drop(journal);

        let restarted = open_journal(&configuration);
        let (restarted, _) = reconcile_once(&configuration, &store, restarted)
            .await
            .expect("restart resolves exact local accepted record");
        assert_eq!(
            store.replacement_count(),
            1,
            "restart must not rewrite status"
        );
        drop(restarted);

        let empty_root = TempDir::new().expect("second temporary root");
        let empty_configuration = config(&empty_root);
        let empty_journal = open_journal(&empty_configuration);
        assert!(matches!(
            reconcile_once(&empty_configuration, &store, empty_journal).await,
            Err(CatalogActivationConsumerError::Journal(
                CatalogActivationJournalError::AcceptedWithoutPrepared
            ))
        ));
    }

    #[tokio::test]
    async fn status_conflict_is_resolved_only_by_a_new_exact_get() {
        let root = TempDir::new().expect("temporary root");
        let configuration = config(&root);
        let store = FakeStore::new(carrier(Some(request())), ReplaceMode::ConflictAfterInstall);
        let journal = open_journal(&configuration);
        let (_, response) = reconcile_once(&configuration, &store, journal)
            .await
            .expect("exact GET resolves installed conflict");
        assert_eq!(response, configuration.capability());
        assert_eq!(store.replacement_count(), 1);
    }

    #[test]
    fn strict_carrier_validation_rejects_foreign_owner_and_target() {
        let root = TempDir::new().expect("temporary root");
        let configuration = config(&root);
        let mut foreign_owner = carrier(None);
        foreign_owner
            .metadata
            .owner_references
            .as_mut()
            .expect("owner")[0]
            .uid = "other".into();
        assert!(matches!(
            validate_carrier(&configuration, &foreign_owner),
            Err(CatalogActivationConsumerError::InvalidCarrierIdentity)
        ));

        let mut extra_owner = carrier(None);
        let duplicate = extra_owner
            .metadata
            .owner_references
            .as_ref()
            .expect("owner")[0]
            .clone();
        extra_owner
            .metadata
            .owner_references
            .as_mut()
            .expect("owner")
            .push(duplicate);
        assert!(matches!(
            validate_carrier(&configuration, &extra_owner),
            Err(CatalogActivationConsumerError::InvalidCarrierIdentity)
        ));

        let mut non_controller = carrier(None);
        non_controller
            .metadata
            .owner_references
            .as_mut()
            .expect("owner")[0]
            .controller = Some(false);
        assert!(matches!(
            validate_carrier(&configuration, &non_controller),
            Err(CatalogActivationConsumerError::InvalidCarrierIdentity)
        ));

        let mut deletions_unblocked = carrier(None);
        deletions_unblocked
            .metadata
            .owner_references
            .as_mut()
            .expect("owner")[0]
            .block_owner_deletion = Some(false);
        assert!(matches!(
            validate_carrier(&configuration, &deletions_unblocked),
            Err(CatalogActivationConsumerError::InvalidCarrierIdentity)
        ));

        let mut foreign_target = carrier(Some(request()));
        foreign_target.data["spec"]["request"]["source"]["podUID"] = serde_json::json!("other-pod");
        assert!(matches!(
            validate_carrier(&configuration, &foreign_target),
            Err(CatalogActivationConsumerError::InvalidCarrierRequest)
        ));

        let mut wrong_member = request();
        wrong_member.source.pod_name = postgresql_member_pod_name("demo", 0, 1);
        wrong_member.source.instance_id = wrong_member.source.pod_name.clone();
        wrong_member.writable_term.holder = format!(
            "{}/source-pod-uid/0123456789abcdef01234567",
            wrong_member.source.instance_id
        );
        wrong_member.source.generation_identity =
            generation_identity_for_holder(&wrong_member.writable_term.holder);
        wrong_member
            .remote_apply_witness
            .generation_identity
            .clone_from(&wrong_member.source.generation_identity);
        let wrong_member = carrier(Some(wrong_member));
        assert!(matches!(
            validate_carrier(&configuration, &wrong_member),
            Err(CatalogActivationConsumerError::InvalidCarrierRequest)
        ));
    }
}
