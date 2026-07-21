//! Diagnostic-only observation of inert shard-zero catalog candidates.
//!
//! This module validates immutable operator publications. It does not read
//! referenced Secrets or PVCs, mount candidate data, connect to `PostgreSQL`, or
//! grant serving, routing, writable, promotion, failover, or bootstrap authority.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::Client;
use kube::api::{Api, DynamicObject};
use kube::config::Config;
use kube::core::{ApiResource, GroupVersionKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::watch;

use crate::boottime::BoottimeError;
use crate::domain::{CatalogCandidateFailureReason, OrchState};
use crate::topology::{CatalogCandidateObservationPlan, CatalogCandidateTopologyMember};

const CANDIDATE_SCHEMA_VERSION: &str = "pgshard.catalog-bootstrap-candidate.v1";
const CANDIDATE_PAYLOAD_KEY: &str = "candidate.json";
const MAXIMUM_CANDIDATE_PAYLOAD_BYTES: usize = 16 * 1_024;
const MAXIMUM_CANDIDATE_FRESHNESS: Duration = Duration::from_secs(5);
const CANDIDATE_PAYLOAD_DOMAIN: &[u8] = b"pgshard-catalog-bootstrap-candidate-payload-v1\0";
const DISCOVERY_TOPOLOGY_DOMAIN: &[u8] = b"pgshard-catalog-candidate-discovery-topology-v1\0";
const CLUSTER_STATUS_FINGERPRINT_DOMAIN: &[u8] = b"pgshard-catalog-candidate-cluster-status-v1\0";
const MATERIALIZATION_BUNDLE_DOMAIN: &[u8] = b"pgshard-catalog-materialization-bundle-v1\0";
const TARGET_POD_TEMPLATE_DOMAIN: &[u8] = b"pgshard-catalog-target-pod-template-v1\0";
const CATALOG_SERVING_HBA_POLICY_VERSION: &str = "pgshard.catalog-serving-hba.v1";
#[cfg(test)]
const CATALOG_SERVING_HBA_POLICY: &str = concat!(
    "local postgres postgres peer\n",
    "local all all reject\n",
    "local replication all reject\n",
    "host replication pgshard_replication 0.0.0.0/0 scram-sha-256\n",
    "host replication pgshard_replication ::0/0 scram-sha-256\n",
    "hostnossl shardschema all all reject\n",
    "hostssl shardschema pgshard_pooler_catalog all scram-sha-256\n",
    "hostssl shardschema pgshard_orchestrator_catalog all scram-sha-256\n",
    "hostssl shardschema all all reject\n",
    "host all all 0.0.0.0/0 reject\n",
    "host all all ::0/0 reject\n",
);
const CATALOG_SERVING_HBA_POLICY_SHA256: &str =
    "6753a3516fef3a8a459042ca474935f5b306d1b1cf591018f0698e585027af17";

const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
const INSTANCE_LABEL: &str = "app.kubernetes.io/instance";
const COMPONENT_LABEL: &str = "app.kubernetes.io/component";
const CLUSTER_LABEL: &str = "pgshard.io/cluster";
const SHARD_LABEL: &str = "pgshard.io/shard";
const MEMBER_LABEL: &str = "pgshard.io/member";
const ROLE_LABEL: &str = "pgshard.io/role";
const APPLY_OWNERSHIP_ANNOTATION: &str = "pgshard.io/apply-ownership";
const CONFIG_HASH_ANNOTATION: &str = "pgshard.io/config-hash";

/// Runs the independent, diagnostic-only candidate observer until shutdown.
pub async fn supervise(
    plan: CatalogCandidateObservationPlan,
    state: OrchState,
    mut shutdown: watch::Receiver<bool>,
    request_timeout: Duration,
    retry_period: Duration,
    freshness: Duration,
) {
    state.record_catalog_candidates_collecting(plan.members.len(), freshness);
    if validate_plan(&plan, freshness).is_err() {
        state.record_catalog_candidate_failure(CatalogCandidateFailureReason::ValidationFailed);
        wait_until_shutdown(&mut shutdown).await;
        state.record_catalog_candidate_shutdown();
        return;
    }
    let store = match KubernetesCandidateStore::new(&plan, request_timeout) {
        Ok(store) => store,
        Err(error) => {
            state.record_catalog_candidate_failure(error.failure_reason());
            tracing::warn!(reason = %error, "catalog-candidate observation disabled");
            wait_until_shutdown(&mut shutdown).await;
            state.record_catalog_candidate_shutdown();
            return;
        }
    };
    supervise_with_store(
        &store,
        &plan,
        &state,
        &mut shutdown,
        retry_period,
        freshness,
    )
    .await;
    state.record_catalog_candidate_shutdown();
}

async fn supervise_with_store<S: CandidateStore>(
    store: &S,
    plan: &CatalogCandidateObservationPlan,
    state: &OrchState,
    shutdown: &mut watch::Receiver<bool>,
    retry_period: Duration,
    freshness: Duration,
) {
    loop {
        if *shutdown.borrow() {
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
            result = observe_once(store, plan, state, freshness) => result,
        };
        if let Err(error) = result {
            tracing::warn!(reason = %error, "catalog-candidate diagnostics unavailable");
        }
        if wait_or_stop(shutdown, retry_period).await {
            break;
        }
    }
    state.record_catalog_candidate_shutdown();
}

async fn observe_once<S: CandidateStore>(
    store: &S,
    plan: &CatalogCandidateObservationPlan,
    state: &OrchState,
    freshness: Duration,
) -> Result<(), CatalogCandidateError> {
    state.record_catalog_candidates_collecting(plan.members.len(), freshness);
    let result = async {
        // Anchor both local clocks before either Kubernetes read. Host suspend
        // and I/O latency consume this observation window.
        let started = state.suspend_aware_now()?;
        let before = read_stable_bound_candidates(store, plan).await?;
        let deadline = started
            .checked_add(freshness)
            .ok_or(CatalogCandidateError::FreshnessExpired)?;
        if !state.record_catalog_candidates_fresh_exact(before, deadline) {
            return Err(CatalogCandidateError::FreshnessExpired);
        }
        Ok(())
    }
    .await;
    if let Err(error) = &result {
        state.record_catalog_candidate_failure(error.failure_reason());
    }
    result
}

/// Reusable one-shot reader for an exact, stable pair of authoritative
/// non-Secret Kubernetes observations.
///
/// This reader carries no mutation client and no publication capability. It
/// reuses the diagnostic candidate parser so a later publisher cannot develop
/// a second, weaker interpretation of cluster status or candidate `ConfigMaps`.
#[allow(dead_code)] // Deliberately inert until publisher composition is reviewed.
pub(crate) struct AuthoritativeCandidateReader {
    store: KubernetesCandidateStore,
    plan: CatalogCandidateObservationPlan,
    overall_timeout: Duration,
}

#[allow(dead_code)] // Deliberately inert until publisher composition is reviewed.
impl AuthoritativeCandidateReader {
    /// Builds a read-only client with independent per-GET and whole-bracket
    /// budgets. The overall budget may accumulate multiple individually
    /// healthy reads but can never exceed the existing five-second evidence
    /// window.
    pub(crate) fn new(
        plan: CatalogCandidateObservationPlan,
        per_request_timeout: Duration,
        overall_timeout: Duration,
    ) -> Result<Self, CatalogCandidateError> {
        validate_plan(&plan, MAXIMUM_CANDIDATE_FRESHNESS)?;
        if !(Duration::from_millis(100)..=MAXIMUM_CANDIDATE_FRESHNESS)
            .contains(&per_request_timeout)
            || !(per_request_timeout..=MAXIMUM_CANDIDATE_FRESHNESS).contains(&overall_timeout)
        {
            return Err(CatalogCandidateError::InvalidPlan);
        }
        let store = KubernetesCandidateStore::new(&plan, per_request_timeout)?;
        Ok(Self {
            store,
            plan,
            overall_timeout,
        })
    }

    pub(crate) async fn read(&self) -> Result<BoundCandidateSet, CatalogCandidateError> {
        read_authoritative_with_store(&self.store, &self.plan, self.overall_timeout).await
    }
}

async fn read_authoritative_with_store<S: CandidateStore>(
    store: &S,
    plan: &CatalogCandidateObservationPlan,
    request_timeout: Duration,
) -> Result<BoundCandidateSet, CatalogCandidateError> {
    tokio::time::timeout(request_timeout, read_stable_bound_candidates(store, plan))
        .await
        .map_err(|_| CatalogCandidateError::AuthoritativeReadTimedOut)?
}

async fn read_stable_bound_candidates<S: CandidateStore>(
    store: &S,
    plan: &CatalogCandidateObservationPlan,
) -> Result<BoundCandidateSet, CatalogCandidateError> {
    let before = read_bound_candidates(store, plan).await?;
    let after = read_bound_candidates(store, plan).await?;
    if before != after {
        return Err(CatalogCandidateError::EvidenceChanged);
    }
    Ok(before)
}

async fn read_bound_candidates<S: CandidateStore>(
    store: &S,
    plan: &CatalogCandidateObservationPlan,
) -> Result<BoundCandidateSet, CatalogCandidateError> {
    let cluster = store.get_cluster_status().await?;
    let status = validate_cluster_status(&cluster, plan)?;
    let mut objects = Vec::with_capacity(plan.members.len());
    for (member, checkpoint) in plan.members.iter().zip(&status.candidates) {
        let configuration = store.get_candidate(&checkpoint.config_map_name).await?;
        objects.push(validate_candidate(
            &configuration,
            plan,
            member,
            checkpoint,
            &status,
        )?);
    }
    validate_candidate_set(&objects)?;
    let materialization_bundles = objects
        .iter()
        .map(|candidate| candidate.document.materialization_bundle.clone())
        .collect();
    Ok(BoundCandidateSet {
        cluster: status.fingerprint,
        catalog_activation: status.catalog_activation,
        candidates: objects,
        shard_zero_bootstraps: status
            .bootstraps
            .into_iter()
            .map(|checkpoint| BootstrapReference {
                secret: ObjectReference {
                    name: checkpoint.secret_name,
                    uid: checkpoint.secret_uid,
                },
                pvc: ObjectReference {
                    name: checkpoint.pvc_name,
                    uid: checkpoint.pvc_uid,
                },
            })
            .collect(),
        writable_lease: status.writable_lease,
        replication_credential: status.replication_credential,
        catalog_access: status.catalog_access,
        operation_writer_access: status.operation_writer_access,
        materialization_bundles,
    })
}

fn validate_plan(
    plan: &CatalogCandidateObservationPlan,
    freshness: Duration,
) -> Result<(), CatalogCandidateError> {
    if !matches!(plan.members.len(), 3 | 5)
        || plan.shard_count == 0
        || freshness.is_zero()
        || freshness > MAXIMUM_CANDIDATE_FRESHNESS
        || !valid_name(&plan.cluster_id)
        || !valid_name(&plan.namespace)
        || !valid_uid(&plan.cluster_uid)
        || !valid_name(&plan.topology_config_map)
        || !valid_name(&plan.writable_lease_name)
        || !valid_uid(&plan.writable_lease_uid)
    {
        return Err(CatalogCandidateError::InvalidPlan);
    }
    for (ordinal, member) in plan.members.iter().enumerate() {
        if member.ordinal != u32::try_from(ordinal).unwrap_or(u32::MAX)
            || !valid_name(&member.config_map_name())
        {
            return Err(CatalogCandidateError::InvalidPlan);
        }
    }
    Ok(())
}

/// Bounded exact evidence from one stable read bracket. This type is private to
/// the crate, deliberately non-serializable, and grants no mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundCandidateSet {
    pub(crate) cluster: ClusterFingerprint,
    pub(crate) catalog_activation: ObjectReference,
    pub(crate) candidates: Vec<CandidateFingerprint>,
    pub(crate) shard_zero_bootstraps: Vec<BootstrapReference>,
    pub(crate) writable_lease: ObjectReference,
    pub(crate) replication_credential: MaterialReference,
    pub(crate) catalog_access: CatalogAccessReference,
    pub(crate) operation_writer_access: MaterialReference,
    pub(crate) materialization_bundles: Vec<MaterializationBundle>,
}

impl BoundCandidateSet {
    /// Returns the number of exact shard-zero candidates in this proof.
    #[must_use]
    pub(crate) fn candidate_count(&self) -> usize {
        self.candidates.len()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterFingerprint {
    pub(crate) name: String,
    pub(crate) namespace: String,
    pub(crate) uid: String,
    pub(crate) resource_version: String,
    pub(crate) generation: i64,
    pub(crate) status_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CandidateFingerprint {
    pub(crate) name: String,
    pub(crate) uid: String,
    pub(crate) resource_version: String,
    pub(crate) payload_sha256: String,
    pub(crate) document: CandidateDocumentV1,
}

#[derive(Clone, Debug)]
struct ValidatedClusterStatus {
    fingerprint: ClusterFingerprint,
    catalog_activation: ObjectReference,
    candidates: Vec<CandidateCheckpoint>,
    bootstraps: Vec<BootstrapCheckpoint>,
    writable_lease: ObjectReference,
    replication_credential: MaterialReference,
    catalog_access: CatalogAccessReference,
    operation_writer_access: MaterialReference,
    postgresql_configuration: ConfigurationReference,
}

fn validate_cluster_status(
    cluster: &DynamicObject,
    plan: &CatalogCandidateObservationPlan,
) -> Result<ValidatedClusterStatus, CatalogCandidateError> {
    let types = cluster
        .types
        .as_ref()
        .ok_or(CatalogCandidateError::InvalidClusterStatus)?;
    if types.api_version != "pgshard.io/v1alpha1" || types.kind != "PgShardCluster" {
        return Err(CatalogCandidateError::InvalidClusterStatus);
    }
    let metadata = &cluster.metadata;
    let uid = exact_metadata_value(metadata.uid.as_deref(), &plan.cluster_uid)?;
    let resource_version = require_resource_version(metadata.resource_version.as_deref())?;
    let generation = metadata
        .generation
        .filter(|generation| *generation > 0)
        .ok_or(CatalogCandidateError::InvalidClusterStatus)?;
    if metadata.name.as_deref() != Some(plan.cluster_id.as_str())
        || metadata.namespace.as_deref() != Some(plan.namespace.as_str())
        || metadata.deletion_timestamp.is_some()
    {
        return Err(CatalogCandidateError::InvalidClusterStatus);
    }
    let status_value = cluster
        .data
        .get("status")
        .cloned()
        .ok_or(CatalogCandidateError::InvalidClusterStatus)?;
    if !go_compatible_json_domain(&status_value) {
        return Err(CatalogCandidateError::InvalidClusterStatus);
    }
    let status: ClusterStatus = serde_json::from_value(status_value.clone())?;
    if status.observed_generation != generation
        || status.bootstrap_spec.shards != plan.shard_count
        || status.bootstrap_spec.members_per_shard != plan.members.len()
        || status.bootstrap_spec.postgresql_runtime != "agent-quarantine"
        || status.bootstrap_spec.durability
            != if plan.synchronous_durability {
                "Synchronous"
            } else {
                "Asynchronous"
            }
        || !matches!(
            status.phase.as_str(),
            "Pending" | "Reconciling" | "Ready" | "Degraded"
        )
        || status.conditions.len() > 32
        || !valid_digest(&status.bootstrap_spec.database_topology_sha256)
        || status.bootstrap_spec.storage_size.is_empty()
        || status.bootstrap_spec.storage_size.len() > 64
        || status
            .bootstrap_spec
            .storage_class_name
            .as_ref()
            .is_some_and(|name| name.len() > 253)
        || !matches!(
            status.bootstrap_spec.deletion_policy.as_str(),
            "Retain" | "Delete"
        )
    {
        return Err(CatalogCandidateError::InvalidClusterStatus);
    }
    let candidates = validate_candidate_checkpoints(&status.catalog_candidates, plan)?;
    let bootstraps = validate_bootstrap_checkpoints(&status.bootstraps, plan)?;
    let writable_lease = select_writable_lease(&status.writable_leases, plan)?;
    let replication_credential =
        select_replication_credential(&status.replication_credentials, plan)?;
    let catalog_access = validate_catalog_access(status.catalog_access)?;
    let operation_writer_access =
        validate_operation_writer_access(status.operation_writer_access, &plan.cluster_id)?;
    let postgresql_configuration =
        validate_postgresql_configuration(status.postgresql_configuration, &plan.cluster_id)?;
    let catalog_activation = validate_catalog_activation(status.catalog_activation, plan)?;
    Ok(ValidatedClusterStatus {
        fingerprint: ClusterFingerprint {
            name: plan.cluster_id.clone(),
            namespace: plan.namespace.clone(),
            uid: uid.to_owned(),
            resource_version: resource_version.to_owned(),
            generation,
            status_sha256: domain_digest(
                CLUSTER_STATUS_FINGERPRINT_DOMAIN,
                &go_compatible_json(&status_value)?,
            ),
        },
        catalog_activation,
        candidates,
        bootstraps,
        writable_lease,
        replication_credential,
        catalog_access,
        operation_writer_access,
        postgresql_configuration,
    })
}

// Kubernetes admission recomputes this digest with Go's encoding/json. Match
// its additional string escaping without changing JSON structure or literals.
// The caller first restricts values to this shared integer-only number domain.
fn go_compatible_json(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    let encoded = serde_json::to_vec(value)?;
    let mut compatible = Vec::with_capacity(encoded.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < encoded.len() {
        let byte = encoded[index];
        if !in_string {
            compatible.push(byte);
            if byte == b'"' {
                in_string = true;
            }
            index += 1;
            continue;
        }
        if escaped {
            compatible.push(byte);
            escaped = false;
            index += 1;
            continue;
        }
        match byte {
            b'\\' => {
                compatible.push(byte);
                escaped = true;
                index += 1;
            }
            b'"' => {
                compatible.push(byte);
                in_string = false;
                index += 1;
            }
            b'<' => {
                compatible.extend_from_slice(br"\u003c");
                index += 1;
            }
            b'>' => {
                compatible.extend_from_slice(br"\u003e");
                index += 1;
            }
            b'&' => {
                compatible.extend_from_slice(br"\u0026");
                index += 1;
            }
            0xe2 if encoded.get(index..index + 3) == Some(&[0xe2, 0x80, 0xa8]) => {
                compatible.extend_from_slice(br"\u2028");
                index += 3;
            }
            0xe2 if encoded.get(index..index + 3) == Some(&[0xe2, 0x80, 0xa9]) => {
                compatible.extend_from_slice(br"\u2029");
                index += 3;
            }
            _ => {
                compatible.push(byte);
                index += 1;
            }
        }
    }
    debug_assert!(!in_string && !escaped);
    Ok(compatible)
}

fn go_compatible_json_domain(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) => true,
        Value::Number(number) => number.is_i64() || number.is_u64(),
        Value::Array(values) => values.iter().all(go_compatible_json_domain),
        Value::Object(values) => values.values().all(go_compatible_json_domain),
    }
}

fn validate_catalog_activation(
    checkpoint: CatalogActivationCheckpoint,
    plan: &CatalogCandidateObservationPlan,
) -> Result<ObjectReference, CatalogCandidateError> {
    let expected_name = format!("{}-catalog-activation", plan.cluster_id);
    if checkpoint.name != expected_name
        || !valid_name(&checkpoint.name)
        || !valid_uid(&checkpoint.uid)
    {
        return Err(CatalogCandidateError::InvalidCheckpointSet);
    }
    Ok(ObjectReference {
        name: checkpoint.name,
        uid: checkpoint.uid,
    })
}

fn validate_candidate_checkpoints(
    checkpoints: &[CandidateCheckpoint],
    plan: &CatalogCandidateObservationPlan,
) -> Result<Vec<CandidateCheckpoint>, CatalogCandidateError> {
    if checkpoints.len() != plan.members.len() {
        return Err(CatalogCandidateError::InvalidCheckpointSet);
    }
    let mut ordered = vec![None; plan.members.len()];
    let mut names = HashSet::with_capacity(checkpoints.len());
    let mut uids = HashSet::with_capacity(checkpoints.len());
    let mut digests = HashSet::with_capacity(checkpoints.len());
    for checkpoint in checkpoints {
        let Some(member) = plan.members.get(checkpoint.member) else {
            return Err(CatalogCandidateError::InvalidCheckpointSet);
        };
        if (checkpoint.config_map_name != member.config_map_name()
            && checkpoint.config_map_name
                != catalog_candidate_config_map_name(
                    &plan.cluster_id,
                    member.ordinal,
                    &checkpoint.payload_sha256,
                )
                .ok_or(CatalogCandidateError::InvalidCheckpointSet)?)
            || !valid_uid(&checkpoint.config_map_uid)
            || !valid_digest(&checkpoint.payload_sha256)
            || !names.insert(checkpoint.config_map_name.as_str())
            || !uids.insert(checkpoint.config_map_uid.as_str())
            || !digests.insert(checkpoint.payload_sha256.as_str())
            || ordered[checkpoint.member]
                .replace(checkpoint.clone())
                .is_some()
        {
            return Err(CatalogCandidateError::InvalidCheckpointSet);
        }
    }
    ordered
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(CatalogCandidateError::InvalidCheckpointSet)
}

fn validate_bootstrap_checkpoints(
    checkpoints: &[BootstrapCheckpoint],
    plan: &CatalogCandidateObservationPlan,
) -> Result<Vec<BootstrapCheckpoint>, CatalogCandidateError> {
    let checkpoint_count = plan
        .shard_count
        .checked_mul(plan.members.len())
        .ok_or(CatalogCandidateError::InvalidCheckpointSet)?;
    if checkpoints.len() != checkpoint_count {
        return Err(CatalogCandidateError::InvalidCheckpointSet);
    }
    let mut ordered = vec![None; checkpoint_count];
    let mut secret_names = HashSet::with_capacity(checkpoints.len());
    let mut secret_uids = HashSet::with_capacity(checkpoints.len());
    let mut pvc_names = HashSet::with_capacity(checkpoints.len());
    let mut pvc_uids = HashSet::with_capacity(checkpoints.len());
    for checkpoint in checkpoints {
        if checkpoint.shard >= plan.shard_count
            || checkpoint.member >= plan.members.len()
            || !valid_name(&checkpoint.secret_name)
            || !valid_uid(&checkpoint.secret_uid)
            || !checkpoint.pvc_fence_detached
            || checkpoint.pvc_creation_abandoned
            || !valid_name(&checkpoint.pvc_name)
            || !valid_uid(&checkpoint.pvc_uid)
            || checkpoint.pvc_storage_class_name.is_none()
            || !secret_names.insert(checkpoint.secret_name.as_str())
            || !secret_uids.insert(checkpoint.secret_uid.as_str())
            || !pvc_names.insert(checkpoint.pvc_name.as_str())
            || !pvc_uids.insert(checkpoint.pvc_uid.as_str())
        {
            return Err(CatalogCandidateError::InvalidCheckpointSet);
        }
        let index = checkpoint
            .shard
            .checked_mul(plan.members.len())
            .and_then(|index| index.checked_add(checkpoint.member))
            .ok_or(CatalogCandidateError::InvalidCheckpointSet)?;
        if ordered[index].replace(checkpoint.clone()).is_some() {
            return Err(CatalogCandidateError::InvalidCheckpointSet);
        }
    }
    ordered
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .map(|ordered| ordered.into_iter().take(plan.members.len()).collect())
        .ok_or(CatalogCandidateError::InvalidCheckpointSet)
}

fn select_writable_lease(
    checkpoints: &[WritableLeaseCheckpoint],
    plan: &CatalogCandidateObservationPlan,
) -> Result<ObjectReference, CatalogCandidateError> {
    if checkpoints.len() != plan.shard_count {
        return Err(CatalogCandidateError::InvalidCheckpointSet);
    }
    let mut ordered = vec![None; plan.shard_count];
    let mut names = HashSet::with_capacity(checkpoints.len());
    let mut uids = HashSet::with_capacity(checkpoints.len());
    for checkpoint in checkpoints {
        if checkpoint.shard >= plan.shard_count
            || !valid_name(&checkpoint.lease_name)
            || !valid_uid(&checkpoint.lease_uid)
            || !names.insert(checkpoint.lease_name.as_str())
            || !uids.insert(checkpoint.lease_uid.as_str())
            || ordered[checkpoint.shard]
                .replace(ObjectReference {
                    name: checkpoint.lease_name.clone(),
                    uid: checkpoint.lease_uid.clone(),
                })
                .is_some()
        {
            return Err(CatalogCandidateError::InvalidCheckpointSet);
        }
    }
    let checkpoint = ordered
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(CatalogCandidateError::InvalidCheckpointSet)?
        .into_iter()
        .next()
        .ok_or(CatalogCandidateError::InvalidCheckpointSet)?;
    if checkpoint.name != plan.writable_lease_name || checkpoint.uid != plan.writable_lease_uid {
        return Err(CatalogCandidateError::InvalidCheckpointSet);
    }
    Ok(checkpoint)
}

fn select_replication_credential(
    checkpoints: &[ReplicationCredentialCheckpoint],
    plan: &CatalogCandidateObservationPlan,
) -> Result<MaterialReference, CatalogCandidateError> {
    if checkpoints.len() != plan.shard_count {
        return Err(CatalogCandidateError::InvalidCheckpointSet);
    }
    let mut ordered = vec![None; plan.shard_count];
    let mut names = HashSet::with_capacity(checkpoints.len());
    let mut uids = HashSet::with_capacity(checkpoints.len());
    for checkpoint in checkpoints {
        if checkpoint.shard >= plan.shard_count
            || !valid_name(&checkpoint.secret_name)
            || !valid_uid(&checkpoint.secret_uid)
            || !valid_digest(&checkpoint.material_sha256)
            || !names.insert(checkpoint.secret_name.as_str())
            || !uids.insert(checkpoint.secret_uid.as_str())
            || ordered[checkpoint.shard]
                .replace(MaterialReference {
                    name: checkpoint.secret_name.clone(),
                    uid: checkpoint.secret_uid.clone(),
                    material_sha256: checkpoint.material_sha256.clone(),
                })
                .is_some()
        {
            return Err(CatalogCandidateError::InvalidCheckpointSet);
        }
    }
    ordered
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(CatalogCandidateError::InvalidCheckpointSet)?
        .into_iter()
        .next()
        .ok_or(CatalogCandidateError::InvalidCheckpointSet)
}

fn validate_catalog_access(
    checkpoint: CatalogAccessCheckpoint,
) -> Result<CatalogAccessReference, CatalogCandidateError> {
    if !valid_name(&checkpoint.secret_name)
        || !valid_uid(&checkpoint.secret_uid)
        || !valid_digest(&checkpoint.client_sha256)
        || !valid_digest(&checkpoint.server_sha256)
    {
        return Err(CatalogCandidateError::InvalidCheckpointSet);
    }
    Ok(CatalogAccessReference {
        name: checkpoint.secret_name,
        uid: checkpoint.secret_uid,
        client_sha256: checkpoint.client_sha256,
        server_sha256: checkpoint.server_sha256,
    })
}

fn validate_operation_writer_access(
    checkpoint: OperationWriterAccessCheckpoint,
    cluster_id: &str,
) -> Result<MaterialReference, CatalogCandidateError> {
    if !operation_writer_secret_name_is_valid(cluster_id, &checkpoint.secret_name)
        || !valid_uid(&checkpoint.secret_uid)
        || !valid_digest(&checkpoint.material_sha256)
    {
        return Err(CatalogCandidateError::InvalidCheckpointSet);
    }
    Ok(MaterialReference {
        name: checkpoint.secret_name,
        uid: checkpoint.secret_uid,
        material_sha256: checkpoint.material_sha256,
    })
}

fn validate_postgresql_configuration(
    checkpoint: PostgreSQLConfigurationCheckpoint,
    cluster_id: &str,
) -> Result<ConfigurationReference, CatalogCandidateError> {
    if !valid_name(&checkpoint.config_map_name)
        || !valid_uid(&checkpoint.config_map_uid)
        || !valid_digest(&checkpoint.data_sha256)
        || checkpoint.config_map_name
            != format!("{cluster_id}-postgresql-config-{}", checkpoint.data_sha256)
    {
        return Err(CatalogCandidateError::InvalidCheckpointSet);
    }
    Ok(ConfigurationReference {
        name: checkpoint.config_map_name,
        uid: checkpoint.config_map_uid,
        data_sha256: checkpoint.data_sha256,
    })
}

fn operation_writer_secret_name_is_valid(cluster_id: &str, name: &str) -> bool {
    if !valid_name(cluster_id) || !valid_name(name) {
        return false;
    }
    let literal = format!("{cluster_id}-writer-");
    let prefix = if literal.len() <= 31 {
        literal
    } else {
        let Some(cluster_prefix) = cluster_id.get(..14) else {
            return false;
        };
        let digest = Sha256::digest(cluster_id.as_bytes());
        let mut encoded = String::with_capacity(12);
        for byte in &digest[..6] {
            let _ = write!(encoded, "{byte:02x}");
        }
        format!("{cluster_prefix}-wr-{encoded}-")
    };
    name.strip_prefix(&prefix).is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    })
}

fn catalog_candidate_config_map_name(
    cluster_id: &str,
    member: u32,
    payload_sha256: &str,
) -> Option<String> {
    if !valid_name(cluster_id) || !valid_digest(payload_sha256) {
        return None;
    }
    let mut prefix = format!("{cluster_id}-s0-m{member:04}-cfg-");
    if prefix.len() > 31 {
        let cluster_prefix = cluster_id.get(..11)?;
        let digest = Sha256::digest(cluster_id.as_bytes());
        let mut encoded = String::with_capacity(12);
        for byte in &digest[..6] {
            let _ = write!(encoded, "{byte:02x}");
        }
        prefix = format!("{cluster_prefix}-{encoded}-c{member:04}-");
    }
    Some(format!("{prefix}{}", payload_sha256.get(..32)?))
}

fn validate_candidate(
    configuration: &ConfigMap,
    plan: &CatalogCandidateObservationPlan,
    member: &CatalogCandidateTopologyMember,
    checkpoint: &CandidateCheckpoint,
    status: &ValidatedClusterStatus,
) -> Result<CandidateFingerprint, CatalogCandidateError> {
    let metadata = &configuration.metadata;
    if metadata.name.as_deref() != Some(checkpoint.config_map_name.as_str())
        || metadata.namespace.as_deref() != Some(plan.namespace.as_str())
        || metadata.uid.as_deref() != Some(checkpoint.config_map_uid.as_str())
        || metadata.deletion_timestamp.is_some()
        || metadata
            .finalizers
            .as_ref()
            .is_some_and(|items| !items.is_empty())
        || configuration.immutable != Some(true)
        || configuration
            .binary_data
            .as_ref()
            .is_some_and(|items| !items.is_empty())
    {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    let uid = require_uid(metadata.uid.as_deref())?;
    let resource_version = require_resource_version(metadata.resource_version.as_deref())?;
    validate_candidate_metadata(configuration, plan, member, checkpoint)?;
    let data = configuration
        .data
        .as_ref()
        .ok_or(CatalogCandidateError::InvalidCandidate)?;
    if data.len() != 1 {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    let payload = data
        .get(CANDIDATE_PAYLOAD_KEY)
        .ok_or(CatalogCandidateError::InvalidCandidate)?;
    if payload.is_empty()
        || payload.len() > MAXIMUM_CANDIDATE_PAYLOAD_BYTES
        || !payload.ends_with('\n')
        || payload
            .strip_suffix('\n')
            .is_none_or(|body| body.ends_with('\n'))
    {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    let document: CandidateDocumentV1 = serde_json::from_slice(payload.as_bytes())?;
    let mut canonical = serde_json::to_vec(&document)?;
    canonical.push(b'\n');
    if canonical != payload.as_bytes() {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    let payload_sha256 = domain_digest(CANDIDATE_PAYLOAD_DOMAIN, payload.as_bytes());
    if payload_sha256 != checkpoint.payload_sha256 {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    validate_candidate_document(&document, plan, member, status)?;
    Ok(CandidateFingerprint {
        name: checkpoint.config_map_name.clone(),
        uid: uid.to_owned(),
        resource_version: resource_version.to_owned(),
        payload_sha256,
        document,
    })
}

fn validate_candidate_metadata(
    configuration: &ConfigMap,
    plan: &CatalogCandidateObservationPlan,
    member: &CatalogCandidateTopologyMember,
    checkpoint: &CandidateCheckpoint,
) -> Result<(), CatalogCandidateError> {
    let expected_labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_owned(), "pgshard".to_owned()),
        (MANAGED_BY_LABEL.to_owned(), "pgshard-operator".to_owned()),
        (INSTANCE_LABEL.to_owned(), plan.cluster_id.clone()),
        (
            COMPONENT_LABEL.to_owned(),
            "postgresql-catalog-bootstrap".to_owned(),
        ),
        (CLUSTER_LABEL.to_owned(), plan.cluster_id.clone()),
        (SHARD_LABEL.to_owned(), "0000".to_owned()),
        (MEMBER_LABEL.to_owned(), format!("{:04}", member.ordinal)),
    ]);
    let expected_annotations = BTreeMap::from([
        (APPLY_OWNERSHIP_ANNOTATION.to_owned(), "v1".to_owned()),
        (
            CONFIG_HASH_ANNOTATION.to_owned(),
            checkpoint.payload_sha256.clone(),
        ),
    ]);
    if configuration.metadata.labels.as_ref() != Some(&expected_labels)
        || configuration.metadata.annotations.as_ref() != Some(&expected_annotations)
        || configuration
            .metadata
            .labels
            .as_ref()
            .is_some_and(|labels| labels.contains_key(ROLE_LABEL))
    {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    let owners = configuration
        .metadata
        .owner_references
        .as_deref()
        .ok_or(CatalogCandidateError::InvalidCandidate)?;
    if owners.len() != 1 {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    let owner = &owners[0];
    if owner.api_version != "pgshard.io/v1alpha1"
        || owner.kind != "PgShardCluster"
        || owner.name != plan.cluster_id
        || owner.uid != plan.cluster_uid
        || owner.controller != Some(true)
        || owner.block_owner_deletion != Some(true)
    {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    Ok(())
}

fn validate_candidate_document(
    document: &CandidateDocumentV1,
    plan: &CatalogCandidateObservationPlan,
    member: &CatalogCandidateTopologyMember,
    status: &ValidatedClusterStatus,
) -> Result<(), CatalogCandidateError> {
    let bootstrap = status
        .bootstraps
        .get(member.ordinal as usize)
        .ok_or(CatalogCandidateError::InvalidCandidate)?;
    if document.schema_version != CANDIDATE_SCHEMA_VERSION
        || document.cluster_object_uid != plan.cluster_uid
        || document.shard != 0
        || document.member != member.ordinal
        || document.instance_id != member.instance_id
        || document.discovery_topology.config_map.name != plan.topology_config_map
        || document.discovery_topology.members.len() != plan.members.len()
        || document.bootstrap.secret.name != bootstrap.secret_name
        || document.bootstrap.secret.uid != bootstrap.secret_uid
        || document.bootstrap.pvc.name != bootstrap.pvc_name
        || document.bootstrap.pvc.uid != bootstrap.pvc_uid
        || document.writable_lease != status.writable_lease
        || document.replication_credential != status.replication_credential
        || document.catalog_access != status.catalog_access
        || document.materialization_bundle.postgresql_configuration
            != status.postgresql_configuration
        || document.materialization_bundle.catalog_access != status.catalog_access
        || document.materialization_bundle.operation_writer_access != status.operation_writer_access
        || !validate_materialization_bundle(document, plan)
    {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    for (actual, expected) in document
        .discovery_topology
        .members
        .iter()
        .zip(&plan.members)
    {
        if actual.ordinal != expected.ordinal
            || actual.instance_id != expected.instance_id
            || actual.dns_name != expected.dns_name
            || actual.postgresql_port != expected.postgresql_port
            || actual.agent_http_port != expected.agent_http_port
            || actual.physical_slot != expected.physical_slot
        {
            return Err(CatalogCandidateError::InvalidCandidate);
        }
    }
    let digest_input = DiscoveryDigestInput {
        config_map: document.discovery_topology.config_map.clone(),
        members: document.discovery_topology.members.clone(),
    };
    let encoded = serde_json::to_vec(&digest_input)?;
    let digest = domain_digest(DISCOVERY_TOPOLOGY_DOMAIN, &encoded);
    if document.discovery_topology.sha256 != digest {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    Ok(())
}

fn validate_materialization_bundle(
    document: &CandidateDocumentV1,
    plan: &CatalogCandidateObservationPlan,
) -> bool {
    let bundle = &document.materialization_bundle;
    let target = &bundle.target_pod_template;
    if !valid_name(&bundle.postgresql_configuration.name)
        || !valid_uid(&bundle.postgresql_configuration.uid)
        || !valid_digest(&bundle.postgresql_configuration.data_sha256)
        || !valid_digest(&bundle.shardschema_migration.sha256)
        || !valid_digest(&bundle.database_genesis.sha256)
        || !valid_digest(&bundle.database_topology_preflight.sha256)
        || bundle.serving_hba.version != CATALOG_SERVING_HBA_POLICY_VERSION
        || bundle.serving_hba.sha256 != CATALOG_SERVING_HBA_POLICY_SHA256
        || target.stateful_set_name
            != plan
                .members
                .first()
                .and_then(|source| source.instance_id.strip_suffix("-0"))
                .unwrap_or_default()
        || target.postgresql_runtime != "agent-quarantine"
        || target.bootstrap_hba_mode != "replication-bootstrap-primary"
        || target.configuration_annotation.key != "pgshard.io/config-hash"
        || target.configuration_annotation.value != bundle.postgresql_configuration.data_sha256
        || target.shardschema_migration_annotation.key != "pgshard.io/shardschema-migration-sha256"
        || target.shardschema_migration_annotation.value != bundle.shardschema_migration.sha256
        || !valid_digest(&target.sha256)
        || !valid_digest(&bundle.sha256)
        || document.cluster_object_uid != plan.cluster_uid
    {
        return false;
    }
    let target_input = TargetPodTemplateDigestInput {
        stateful_set_name: target.stateful_set_name.clone(),
        postgresql_runtime: target.postgresql_runtime.clone(),
        bootstrap_hba_mode: target.bootstrap_hba_mode.clone(),
        configuration_annotation: target.configuration_annotation.clone(),
        shardschema_migration_annotation: target.shardschema_migration_annotation.clone(),
    };
    if serde_json::to_vec(&target_input)
        .ok()
        .is_none_or(|encoded| domain_digest(TARGET_POD_TEMPLATE_DOMAIN, &encoded) != target.sha256)
    {
        return false;
    }
    let bundle_input = MaterializationBundleDigestInput {
        postgresql_configuration: bundle.postgresql_configuration.clone(),
        shardschema_migration: bundle.shardschema_migration.clone(),
        database_genesis: bundle.database_genesis.clone(),
        database_topology_preflight: bundle.database_topology_preflight.clone(),
        catalog_access: bundle.catalog_access.clone(),
        operation_writer_access: bundle.operation_writer_access.clone(),
        serving_hba: bundle.serving_hba.clone(),
        target_pod_template: bundle.target_pod_template.clone(),
    };
    serde_json::to_vec(&bundle_input)
        .ok()
        .is_some_and(|encoded| {
            domain_digest(MATERIALIZATION_BUNDLE_DOMAIN, &encoded) == bundle.sha256
        })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TargetPodTemplateDigestInput {
    stateful_set_name: String,
    postgresql_runtime: String,
    #[serde(rename = "bootstrapHBAMode")]
    bootstrap_hba_mode: String,
    configuration_annotation: AnnotationIdentity,
    shardschema_migration_annotation: AnnotationIdentity,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MaterializationBundleDigestInput {
    postgresql_configuration: ConfigurationReference,
    shardschema_migration: ContentReference,
    database_genesis: ContentReference,
    database_topology_preflight: ContentReference,
    catalog_access: CatalogAccessReference,
    operation_writer_access: MaterialReference,
    #[serde(rename = "servingHBA")]
    serving_hba: PolicyReference,
    target_pod_template: PodTemplateReference,
}

fn validate_candidate_set(
    candidates: &[CandidateFingerprint],
) -> Result<(), CatalogCandidateError> {
    if !matches!(candidates.len(), 3 | 5) {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    let mut names = HashSet::with_capacity(candidates.len());
    let mut uids = HashSet::with_capacity(candidates.len());
    let mut digests = HashSet::with_capacity(candidates.len());
    let mut secret_names = HashSet::with_capacity(candidates.len());
    let mut secret_uids = HashSet::with_capacity(candidates.len());
    let mut pvc_names = HashSet::with_capacity(candidates.len());
    let mut pvc_uids = HashSet::with_capacity(candidates.len());
    for candidate in candidates {
        let document = &candidate.document;
        if !names.insert(candidate.name.as_str())
            || !uids.insert(candidate.uid.as_str())
            || !digests.insert(candidate.payload_sha256.as_str())
            || !secret_names.insert(document.bootstrap.secret.name.as_str())
            || !secret_uids.insert(document.bootstrap.secret.uid.as_str())
            || !pvc_names.insert(document.bootstrap.pvc.name.as_str())
            || !pvc_uids.insert(document.bootstrap.pvc.uid.as_str())
        {
            return Err(CatalogCandidateError::InvalidCandidate);
        }
    }
    let first = &candidates[0].document;
    if candidates.iter().skip(1).any(|candidate| {
        candidate.document.discovery_topology != first.discovery_topology
            || candidate.document.writable_lease != first.writable_lease
            || candidate.document.replication_credential != first.replication_credential
            || candidate.document.catalog_access != first.catalog_access
            || candidate
                .document
                .materialization_bundle
                .postgresql_configuration
                != first.materialization_bundle.postgresql_configuration
            || candidate
                .document
                .materialization_bundle
                .shardschema_migration
                != first.materialization_bundle.shardschema_migration
            || candidate.document.materialization_bundle.database_genesis
                != first.materialization_bundle.database_genesis
            || candidate
                .document
                .materialization_bundle
                .database_topology_preflight
                != first.materialization_bundle.database_topology_preflight
            || candidate.document.materialization_bundle.catalog_access
                != first.materialization_bundle.catalog_access
            || candidate
                .document
                .materialization_bundle
                .operation_writer_access
                != first.materialization_bundle.operation_writer_access
            || candidate.document.materialization_bundle.serving_hba
                != first.materialization_bundle.serving_hba
            || candidate
                .document
                .materialization_bundle
                .target_pod_template
                != first.materialization_bundle.target_pod_template
            || candidate.document.materialization_bundle.sha256
                != first.materialization_bundle.sha256
    }) {
        return Err(CatalogCandidateError::InvalidCandidate);
    }
    Ok(())
}

fn domain_digest(domain: &[u8], bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    let digest = hash.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

#[cfg(test)]
fn content_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn exact_metadata_value<'a>(
    value: Option<&'a str>,
    expected: &str,
) -> Result<&'a str, CatalogCandidateError> {
    let value = require_uid(value)?;
    if value != expected {
        return Err(CatalogCandidateError::InvalidClusterStatus);
    }
    Ok(value)
}

fn require_uid(value: Option<&str>) -> Result<&str, CatalogCandidateError> {
    value
        .filter(|value| valid_uid(value))
        .ok_or(CatalogCandidateError::InvalidObjectMetadata)
}

fn require_resource_version(value: Option<&str>) -> Result<&str, CatalogCandidateError> {
    value
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .ok_or(CatalogCandidateError::InvalidObjectMetadata)
}

fn valid_uid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'/' | b'\\'))
}

fn valid_name(value: &str) -> bool {
    // Kubernetes DNS1123 subdomain names bound the complete value to 253
    // bytes; unlike DNS labels, their regex does not cap each dot-separated
    // segment at 63 bytes.
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
        })
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
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

trait CandidateStore: Send + Sync {
    async fn get_cluster_status(&self) -> Result<DynamicObject, CatalogCandidateError>;
    async fn get_candidate(&self, name: &str) -> Result<ConfigMap, CatalogCandidateError>;
}

struct KubernetesCandidateStore {
    cluster_name: String,
    clusters: Api<DynamicObject>,
    candidates: Api<ConfigMap>,
    request_timeout: Duration,
}

impl KubernetesCandidateStore {
    fn new(
        plan: &CatalogCandidateObservationPlan,
        request_timeout: Duration,
    ) -> Result<Self, CatalogCandidateError> {
        let mut client_config = Config::incluster()
            .map_err(|error| CatalogCandidateError::InClusterConfiguration(error.to_string()))?;
        client_config.connect_timeout = Some(request_timeout);
        client_config.read_timeout = Some(request_timeout);
        client_config.write_timeout = Some(request_timeout);
        client_config.default_retry = false;
        let client = Client::try_from(client_config)
            .map_err(|error| CatalogCandidateError::KubernetesClient(error.to_string()))?;
        let resource = ApiResource::from_gvk_with_plural(
            &GroupVersionKind::gvk("pgshard.io", "v1alpha1", "PgShardCluster"),
            "pgshardclusters",
        );
        Ok(Self {
            cluster_name: plan.cluster_id.clone(),
            clusters: Api::namespaced_with(client.clone(), &plan.namespace, &resource),
            candidates: Api::namespaced(client, &plan.namespace),
            request_timeout,
        })
    }
}

impl CandidateStore for KubernetesCandidateStore {
    async fn get_cluster_status(&self) -> Result<DynamicObject, CatalogCandidateError> {
        match tokio::time::timeout(
            self.request_timeout,
            self.clusters.get_subresource("status", &self.cluster_name),
        )
        .await
        {
            Ok(Ok(cluster)) => Ok(cluster),
            Ok(Err(source)) => Err(CatalogCandidateError::KubernetesStatus(Box::new(source))),
            Err(_) => Err(CatalogCandidateError::StatusRequestTimedOut),
        }
    }

    async fn get_candidate(&self, name: &str) -> Result<ConfigMap, CatalogCandidateError> {
        match tokio::time::timeout(self.request_timeout, self.candidates.get(name)).await {
            Ok(Ok(configuration)) => Ok(configuration),
            Ok(Err(source)) => Err(CatalogCandidateError::KubernetesCandidate(Box::new(source))),
            Err(_) => Err(CatalogCandidateError::CandidateRequestTimedOut),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum CatalogCandidateError {
    #[error("catalog-candidate observation plan is invalid")]
    InvalidPlan,
    #[error("PgShardCluster status is absent, stale, deleting, or inconsistent")]
    InvalidClusterStatus,
    #[error("catalog-candidate checkpoint set is incomplete or inconsistent")]
    InvalidCheckpointSet,
    #[error("catalog-candidate ConfigMap identity or payload is invalid")]
    InvalidCandidate,
    #[error("Kubernetes object UID or resource version is missing or malformed")]
    InvalidObjectMetadata,
    #[error("catalog-candidate evidence changed across the observation bracket")]
    EvidenceChanged,
    #[error("catalog-candidate observation expired before atomic publication")]
    FreshnessExpired,
    #[error("authoritative catalog-candidate reread timed out")]
    AuthoritativeReadTimedOut,
    #[error(transparent)]
    AuthorityClock(#[from] BoottimeError),
    #[error("in-cluster Kubernetes configuration is unavailable: {0}")]
    InClusterConfiguration(String),
    #[error("Kubernetes client initialization failed: {0}")]
    KubernetesClient(String),
    #[error("PgShardCluster status request timed out")]
    StatusRequestTimedOut,
    #[error("catalog-candidate ConfigMap request timed out")]
    CandidateRequestTimedOut,
    #[error("Kubernetes API could not read PgShardCluster status: {0}")]
    KubernetesStatus(#[source] Box<kube::Error>),
    #[error("Kubernetes API could not read catalog-candidate ConfigMap: {0}")]
    KubernetesCandidate(#[source] Box<kube::Error>),
    #[error("catalog-candidate JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
}

impl CatalogCandidateError {
    const fn failure_reason(&self) -> CatalogCandidateFailureReason {
        match self {
            Self::StatusRequestTimedOut
            | Self::KubernetesStatus(_)
            | Self::InClusterConfiguration(_)
            | Self::KubernetesClient(_) => CatalogCandidateFailureReason::ClusterStatusUnavailable,
            Self::CandidateRequestTimedOut | Self::KubernetesCandidate(_) => {
                CatalogCandidateFailureReason::CandidateUnavailable
            }
            Self::EvidenceChanged => CatalogCandidateFailureReason::EvidenceChanged,
            Self::FreshnessExpired | Self::AuthorityClock(_) => {
                CatalogCandidateFailureReason::FreshnessExpired
            }
            Self::InvalidPlan
            | Self::InvalidClusterStatus
            | Self::InvalidCheckpointSet
            | Self::InvalidCandidate
            | Self::InvalidObjectMetadata
            | Self::Json(_) => CatalogCandidateFailureReason::ValidationFailed,
            Self::AuthoritativeReadTimedOut => CatalogCandidateFailureReason::CandidateUnavailable,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterStatus {
    observed_generation: i64,
    phase: String,
    #[serde(default)]
    conditions: Vec<Value>,
    #[serde(rename = "postgresqlBootstrapSpec")]
    bootstrap_spec: BootstrapSpec,
    #[serde(rename = "postgresqlBootstraps")]
    bootstraps: Vec<BootstrapCheckpoint>,
    #[serde(rename = "postgresqlWritableLeases")]
    writable_leases: Vec<WritableLeaseCheckpoint>,
    #[serde(rename = "postgresqlReplicationCredentials")]
    replication_credentials: Vec<ReplicationCredentialCheckpoint>,
    #[serde(rename = "postgresqlCatalogCandidates")]
    catalog_candidates: Vec<CandidateCheckpoint>,
    catalog_access: CatalogAccessCheckpoint,
    operation_writer_access: OperationWriterAccessCheckpoint,
    catalog_activation: CatalogActivationCheckpoint,
    postgresql_configuration: PostgreSQLConfigurationCheckpoint,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CatalogActivationCheckpoint {
    name: String,
    uid: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PostgreSQLConfigurationCheckpoint {
    config_map_name: String,
    #[serde(rename = "configMapUID")]
    config_map_uid: String,
    #[serde(rename = "dataSHA256")]
    data_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BootstrapSpec {
    shards: usize,
    members_per_shard: usize,
    durability: String,
    #[serde(rename = "postgresqlRuntime")]
    postgresql_runtime: String,
    #[serde(rename = "databaseTopologySHA256", default)]
    database_topology_sha256: String,
    storage_size: String,
    #[serde(default)]
    storage_class_name: Option<String>,
    deletion_policy: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BootstrapCheckpoint {
    shard: usize,
    member: usize,
    secret_name: String,
    #[serde(rename = "secretUID")]
    secret_uid: String,
    #[serde(default)]
    pvc_fence_detached: bool,
    #[serde(rename = "pvcName", default)]
    pvc_name: String,
    #[serde(rename = "pvcUID", default)]
    pvc_uid: String,
    #[serde(rename = "pvcCreationAbandoned", default)]
    pvc_creation_abandoned: bool,
    #[serde(rename = "pvcStorageClassName", default)]
    pvc_storage_class_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WritableLeaseCheckpoint {
    shard: usize,
    lease_name: String,
    #[serde(rename = "leaseUID")]
    lease_uid: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplicationCredentialCheckpoint {
    shard: usize,
    secret_name: String,
    #[serde(rename = "secretUID")]
    secret_uid: String,
    #[serde(rename = "materialSHA256")]
    material_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CatalogAccessCheckpoint {
    secret_name: String,
    #[serde(rename = "secretUID")]
    secret_uid: String,
    #[serde(rename = "clientSHA256")]
    client_sha256: String,
    #[serde(rename = "serverSHA256")]
    server_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationWriterAccessCheckpoint {
    secret_name: String,
    #[serde(rename = "secretUID")]
    secret_uid: String,
    #[serde(rename = "materialSHA256")]
    material_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CandidateCheckpoint {
    member: usize,
    config_map_name: String,
    #[serde(rename = "configMapUID")]
    config_map_uid: String,
    #[serde(rename = "payloadSHA256")]
    payload_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CandidateDocumentV1 {
    pub(crate) schema_version: String,
    #[serde(rename = "clusterObjectUID")]
    pub(crate) cluster_object_uid: String,
    pub(crate) shard: u32,
    pub(crate) member: u32,
    #[serde(rename = "instanceID")]
    pub(crate) instance_id: String,
    pub(crate) discovery_topology: DiscoveryTopology,
    pub(crate) bootstrap: BootstrapReference,
    pub(crate) writable_lease: ObjectReference,
    pub(crate) replication_credential: MaterialReference,
    pub(crate) catalog_access: CatalogAccessReference,
    pub(crate) materialization_bundle: MaterializationBundle,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DiscoveryTopology {
    pub(crate) config_map: NameReference,
    pub(crate) members: Vec<DiscoveryMember>,
    pub(crate) sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DiscoveryDigestInput {
    config_map: NameReference,
    members: Vec<DiscoveryMember>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NameReference {
    pub(crate) name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DiscoveryMember {
    pub(crate) ordinal: u32,
    pub(crate) instance_id: String,
    pub(crate) dns_name: String,
    pub(crate) postgresql_port: u16,
    pub(crate) agent_http_port: u16,
    pub(crate) physical_slot: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectReference {
    pub(crate) name: String,
    pub(crate) uid: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BootstrapReference {
    pub(crate) secret: ObjectReference,
    pub(crate) pvc: ObjectReference,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MaterialReference {
    pub(crate) name: String,
    pub(crate) uid: String,
    #[serde(rename = "materialSHA256")]
    pub(crate) material_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CatalogAccessReference {
    pub(crate) name: String,
    pub(crate) uid: String,
    #[serde(rename = "clientSHA256")]
    pub(crate) client_sha256: String,
    #[serde(rename = "serverSHA256")]
    pub(crate) server_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ConfigurationReference {
    pub(crate) name: String,
    pub(crate) uid: String,
    #[serde(rename = "dataSHA256")]
    pub(crate) data_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContentReference {
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PolicyReference {
    pub(crate) version: String,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AnnotationIdentity {
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PodTemplateReference {
    pub(crate) stateful_set_name: String,
    pub(crate) postgresql_runtime: String,
    #[serde(rename = "bootstrapHBAMode")]
    pub(crate) bootstrap_hba_mode: String,
    pub(crate) configuration_annotation: AnnotationIdentity,
    pub(crate) shardschema_migration_annotation: AnnotationIdentity,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MaterializationBundle {
    pub(crate) postgresql_configuration: ConfigurationReference,
    pub(crate) shardschema_migration: ContentReference,
    pub(crate) database_genesis: ContentReference,
    pub(crate) database_topology_preflight: ContentReference,
    pub(crate) catalog_access: CatalogAccessReference,
    pub(crate) operation_writer_access: MaterialReference,
    #[serde(rename = "servingHBA")]
    pub(crate) serving_hba: PolicyReference,
    pub(crate) target_pod_template: PodTemplateReference,
    pub(crate) sha256: String,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::future::pending;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use k8s_openapi::ByteString;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
    use kube::core::TypeMeta;
    use pgshard_types::ShardId;
    use pgshard_types::writable_generation::DurableWritableGeneration;
    use serde_json::json;

    use super::*;
    use crate::agent_status::{
        RemoteApplyWitnessProof, ReplicationCorrelationSummary, ReplicationProofMemberIdentity,
        ShardZeroReplicationProof, ShardZeroSourceReplicationProof,
        ShardZeroStandbyReplicationProof, TargetFenceAcknowledgementProof,
        WritableLeaseProofIdentity,
    };
    use crate::boottime::{BoottimeInstant, FakeBoottimeClock};
    use crate::catalog_activation_challenge::ExpectedCatalogActivationCapability;
    use crate::catalog_materialization::{
        CatalogActivationDispatcherProof, PreparedCatalogActivationRequest,
        bind_catalog_activation_live_objects, catalog_activation_publication_target,
        prepare_catalog_activation_request,
    };
    use crate::domain::{AgentStatusPhase, CatalogCandidatePhase, OrchState, OrchestratorIdentity};
    use crate::topology::{
        AgentStatusCollectionState, TOPOLOGY_SCHEMA_VERSION, TopologyDiagnostics,
    };

    struct StubStore {
        clusters: Mutex<VecDeque<DynamicObject>>,
        candidates: BTreeMap<String, ConfigMap>,
    }

    struct BracketStore {
        clusters: Mutex<VecDeque<DynamicObject>>,
        candidate_reads: Mutex<usize>,
        candidates_per_read: usize,
        before_candidates: BTreeMap<String, ConfigMap>,
        after_candidates: BTreeMap<String, ConfigMap>,
    }

    impl CandidateStore for StubStore {
        async fn get_cluster_status(&self) -> Result<DynamicObject, CatalogCandidateError> {
            let mut clusters = self.clusters.lock().expect("clusters");
            if clusters.len() > 1 {
                Ok(clusters.pop_front().expect("cluster response"))
            } else {
                Ok(clusters.front().expect("cluster response").clone())
            }
        }

        async fn get_candidate(&self, name: &str) -> Result<ConfigMap, CatalogCandidateError> {
            self.candidates
                .get(name)
                .cloned()
                .ok_or(CatalogCandidateError::InvalidCandidate)
        }
    }

    impl CandidateStore for BracketStore {
        async fn get_cluster_status(&self) -> Result<DynamicObject, CatalogCandidateError> {
            let mut clusters = self.clusters.lock().expect("clusters");
            if clusters.len() > 1 {
                Ok(clusters.pop_front().expect("cluster response"))
            } else {
                Ok(clusters.front().expect("cluster response").clone())
            }
        }

        async fn get_candidate(&self, name: &str) -> Result<ConfigMap, CatalogCandidateError> {
            let mut reads = self.candidate_reads.lock().expect("candidate reads");
            let candidates = if *reads < self.candidates_per_read {
                &self.before_candidates
            } else {
                &self.after_candidates
            };
            *reads += 1;
            candidates
                .get(name)
                .cloned()
                .ok_or(CatalogCandidateError::InvalidCandidate)
        }
    }

    struct BlockingStore;

    struct DelayedStore {
        inner: StubStore,
        delay: Duration,
        per_request_timeout: Duration,
    }

    impl CandidateStore for BlockingStore {
        async fn get_cluster_status(&self) -> Result<DynamicObject, CatalogCandidateError> {
            pending().await
        }

        async fn get_candidate(&self, _name: &str) -> Result<ConfigMap, CatalogCandidateError> {
            pending().await
        }
    }

    impl CandidateStore for DelayedStore {
        async fn get_cluster_status(&self) -> Result<DynamicObject, CatalogCandidateError> {
            tokio::time::timeout(self.per_request_timeout, async {
                tokio::time::sleep(self.delay).await;
                self.inner.get_cluster_status().await
            })
            .await
            .map_err(|_| CatalogCandidateError::StatusRequestTimedOut)?
        }

        async fn get_candidate(&self, name: &str) -> Result<ConfigMap, CatalogCandidateError> {
            tokio::time::timeout(self.per_request_timeout, async {
                tokio::time::sleep(self.delay).await;
                self.inner.get_candidate(name).await
            })
            .await
            .map_err(|_| CatalogCandidateError::CandidateRequestTimedOut)?
        }
    }

    struct SuspendingStore {
        inner: StubStore,
        clock: Arc<FakeBoottimeClock>,
        advanced: AtomicBool,
    }

    impl CandidateStore for SuspendingStore {
        async fn get_cluster_status(&self) -> Result<DynamicObject, CatalogCandidateError> {
            if !self.advanced.swap(true, Ordering::AcqRel) {
                self.clock
                    .advance(Duration::from_secs(6))
                    .expect("advance across candidate window");
            }
            self.inner.get_cluster_status().await
        }

        async fn get_candidate(&self, name: &str) -> Result<ConfigMap, CatalogCandidateError> {
            self.inner.get_candidate(name).await
        }
    }

    fn plan_with_members(member_count: u32) -> CatalogCandidateObservationPlan {
        let members = (0..member_count)
            .map(|ordinal| {
                let suffix = if ordinal == 0 {
                    String::new()
                } else {
                    format!("-m{ordinal:04}")
                };
                let stateful_set = format!("demo-shard-0000{suffix}");
                CatalogCandidateTopologyMember {
                    ordinal,
                    stateful_set: stateful_set.clone(),
                    instance_id: format!("{stateful_set}-0"),
                    dns_name: format!("{stateful_set}-0.demo-shard-0000.ns.svc"),
                    postgresql_port: 5_432,
                    agent_http_port: 8_080,
                    physical_slot: format!("pgshard_member_{ordinal:04}"),
                }
            })
            .collect();
        CatalogCandidateObservationPlan {
            cluster_id: "demo".to_owned(),
            cluster_uid: "cluster-uid".to_owned(),
            namespace: "ns".to_owned(),
            shard_count: 2,
            synchronous_durability: true,
            topology_config_map: "demo-topology".to_owned(),
            writable_lease_name: "demo-shard-0000-term".to_owned(),
            writable_lease_uid: "lease-uid-0".to_owned(),
            members,
        }
    }

    fn plan() -> CatalogCandidateObservationPlan {
        plan_with_members(3)
    }

    fn bootstrap_secret_name(cluster: &str, shard: usize, member: usize) -> String {
        format!(
            "{cluster}-shard-{shard:04}-member-{member:04}-auth-{}",
            "a".repeat(32)
        )
    }

    fn bootstrap_pvc_name(cluster: &str, shard: usize, member: usize) -> String {
        format!(
            "{cluster}-shard-{shard:04}-member-{member:04}-data-{}",
            "b".repeat(32)
        )
    }

    #[allow(clippy::too_many_lines)]
    fn fixture_for_plan(
        plan: CatalogCandidateObservationPlan,
    ) -> (
        CatalogCandidateObservationPlan,
        DynamicObject,
        BTreeMap<String, ConfigMap>,
    ) {
        let discovery_members = plan
            .members
            .iter()
            .map(|member| DiscoveryMember {
                ordinal: member.ordinal,
                instance_id: member.instance_id.clone(),
                dns_name: member.dns_name.clone(),
                postgresql_port: member.postgresql_port,
                agent_http_port: member.agent_http_port,
                physical_slot: member.physical_slot.clone(),
            })
            .collect::<Vec<_>>();
        let config_map = NameReference {
            name: plan.topology_config_map.clone(),
        };
        let discovery_digest = domain_digest(
            DISCOVERY_TOPOLOGY_DOMAIN,
            &serde_json::to_vec(&DiscoveryDigestInput {
                config_map: config_map.clone(),
                members: discovery_members.clone(),
            })
            .expect("discovery JSON"),
        );
        let writable_lease = ObjectReference {
            name: plan.writable_lease_name.clone(),
            uid: plan.writable_lease_uid.clone(),
        };
        let replication_credential = MaterialReference {
            name: "demo-replication-aabb".to_owned(),
            uid: "replication-uid-0".to_owned(),
            material_sha256: "e".repeat(64),
        };
        let catalog_access = CatalogAccessReference {
            name: "demo-catalog-aabb".to_owned(),
            uid: "catalog-uid".to_owned(),
            client_sha256: "b".repeat(64),
            server_sha256: "c".repeat(64),
        };
        let operation_writer_access = MaterialReference {
            name: format!("demo-writer-{}", "d".repeat(32)),
            uid: "operation-writer-uid".to_owned(),
            material_sha256: "9".repeat(64),
        };
        let postgresql_configuration = ConfigurationReference {
            name: format!("{}-postgresql-config-{}", plan.cluster_id, "7".repeat(64)),
            uid: "postgresql-configuration-uid".to_owned(),
            data_sha256: "7".repeat(64),
        };
        let mut candidates = BTreeMap::new();
        let mut candidate_checkpoints = Vec::new();
        let mut bootstrap_checkpoints = Vec::new();
        for shard in 0..plan.shard_count {
            for member in 0..plan.members.len() {
                bootstrap_checkpoints.push(json!({
                    "shard": shard,
                    "member": member,
                    "secretName": bootstrap_secret_name(&plan.cluster_id, shard, member),
                    "secretUID": format!("bootstrap-uid-{shard}-{member}"),
                    "pvcFenceDetached": true,
                    "pvcName": bootstrap_pvc_name(&plan.cluster_id, shard, member),
                    "pvcUID": format!("data-uid-{shard}-{member}"),
                    "pvcStorageClassName": "fast"
                }));
            }
        }
        for member in &plan.members {
            let materialization_bundle = fixture_materialization_bundle(
                &plan.members[0],
                &postgresql_configuration,
                &catalog_access,
                &operation_writer_access,
            );
            let document = CandidateDocumentV1 {
                schema_version: CANDIDATE_SCHEMA_VERSION.to_owned(),
                cluster_object_uid: plan.cluster_uid.clone(),
                shard: 0,
                member: member.ordinal,
                instance_id: member.instance_id.clone(),
                discovery_topology: DiscoveryTopology {
                    config_map: config_map.clone(),
                    members: discovery_members.clone(),
                    sha256: discovery_digest.clone(),
                },
                bootstrap: BootstrapReference {
                    secret: ObjectReference {
                        name: bootstrap_secret_name(&plan.cluster_id, 0, member.ordinal as usize),
                        uid: format!("bootstrap-uid-0-{}", member.ordinal),
                    },
                    pvc: ObjectReference {
                        name: bootstrap_pvc_name(&plan.cluster_id, 0, member.ordinal as usize),
                        uid: format!("data-uid-0-{}", member.ordinal),
                    },
                },
                writable_lease: writable_lease.clone(),
                replication_credential: replication_credential.clone(),
                catalog_access: catalog_access.clone(),
                materialization_bundle,
            };
            let mut payload = serde_json::to_vec(&document).expect("candidate JSON");
            payload.push(b'\n');
            let payload = String::from_utf8(payload).expect("UTF-8 candidate");
            let payload_sha256 = domain_digest(CANDIDATE_PAYLOAD_DOMAIN, payload.as_bytes());
            let name = member.config_map_name();
            let uid = format!("candidate-uid-{}", member.ordinal);
            candidate_checkpoints.push(json!({
                "member": member.ordinal,
                "configMapName": name,
                "configMapUID": uid,
                "payloadSHA256": payload_sha256
            }));
            let labels = BTreeMap::from([
                ("app.kubernetes.io/name".to_owned(), "pgshard".to_owned()),
                (MANAGED_BY_LABEL.to_owned(), "pgshard-operator".to_owned()),
                (INSTANCE_LABEL.to_owned(), plan.cluster_id.clone()),
                (
                    COMPONENT_LABEL.to_owned(),
                    "postgresql-catalog-bootstrap".to_owned(),
                ),
                (CLUSTER_LABEL.to_owned(), plan.cluster_id.clone()),
                (SHARD_LABEL.to_owned(), "0000".to_owned()),
                (MEMBER_LABEL.to_owned(), format!("{:04}", member.ordinal)),
            ]);
            let annotations = BTreeMap::from([
                (APPLY_OWNERSHIP_ANNOTATION.to_owned(), "v1".to_owned()),
                (CONFIG_HASH_ANNOTATION.to_owned(), payload_sha256),
            ]);
            candidates.insert(
                name.clone(),
                ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(name),
                        namespace: Some(plan.namespace.clone()),
                        uid: Some(uid),
                        resource_version: Some(format!("rv-{}", member.ordinal)),
                        labels: Some(labels),
                        annotations: Some(annotations),
                        owner_references: Some(vec![OwnerReference {
                            api_version: "pgshard.io/v1alpha1".to_owned(),
                            kind: "PgShardCluster".to_owned(),
                            name: plan.cluster_id.clone(),
                            uid: plan.cluster_uid.clone(),
                            controller: Some(true),
                            block_owner_deletion: Some(true),
                        }]),
                        ..ObjectMeta::default()
                    },
                    immutable: Some(true),
                    data: Some(BTreeMap::from([(
                        CANDIDATE_PAYLOAD_KEY.to_owned(),
                        payload,
                    )])),
                    ..ConfigMap::default()
                },
            );
        }
        let status = json!({
            "observedGeneration": 7,
            "phase": "Ready",
            "conditions": [],
            "postgresqlBootstrapSpec": {
                "shards": 2,
                "membersPerShard": plan.members.len(),
                "durability": "Synchronous",
                "postgresqlRuntime": "agent-quarantine",
                "databaseTopologySHA256": "a".repeat(64),
                "storageSize": "10Gi",
                "storageClassName": "fast",
                "deletionPolicy": "Retain"
            },
            "postgresqlBootstraps": bootstrap_checkpoints,
            "postgresqlWritableLeases": [
                {"shard": 0, "leaseName": plan.writable_lease_name, "leaseUID": plan.writable_lease_uid},
                {"shard": 1, "leaseName": "demo-shard-0001-term", "leaseUID": "lease-uid-1"}
            ],
            "postgresqlReplicationCredentials": [
                {"shard": 0, "secretName": replication_credential.name, "secretUID": replication_credential.uid, "materialSHA256": replication_credential.material_sha256},
                {"shard": 1, "secretName": "demo-replication-ccdd", "secretUID": "replication-uid-1", "materialSHA256": "f".repeat(64)}
            ],
            "postgresqlCatalogCandidates": candidate_checkpoints,
            "catalogAccess": {"secretName": catalog_access.name, "secretUID": catalog_access.uid, "clientSHA256": catalog_access.client_sha256, "serverSHA256": catalog_access.server_sha256},
            "operationWriterAccess": {"secretName": operation_writer_access.name, "secretUID": operation_writer_access.uid, "materialSHA256": operation_writer_access.material_sha256},
            "catalogActivation": {"name": format!("{}-catalog-activation", plan.cluster_id), "uid": "catalog-activation-uid"},
            "postgresqlConfiguration": {"configMapName": postgresql_configuration.name, "configMapUID": postgresql_configuration.uid, "dataSHA256": postgresql_configuration.data_sha256}
        });
        let cluster = DynamicObject {
            types: Some(TypeMeta {
                api_version: "pgshard.io/v1alpha1".to_owned(),
                kind: "PgShardCluster".to_owned(),
            }),
            metadata: ObjectMeta {
                name: Some(plan.cluster_id.clone()),
                namespace: Some(plan.namespace.clone()),
                uid: Some(plan.cluster_uid.clone()),
                resource_version: Some("cluster-rv".to_owned()),
                generation: Some(7),
                ..ObjectMeta::default()
            },
            data: json!({"status": status}),
        };
        (plan, cluster, candidates)
    }

    fn fixture_materialization_bundle(
        member: &CatalogCandidateTopologyMember,
        postgresql_configuration: &ConfigurationReference,
        catalog_access: &CatalogAccessReference,
        operation_writer_access: &MaterialReference,
    ) -> MaterializationBundle {
        let mut target = PodTemplateReference {
            stateful_set_name: member
                .instance_id
                .strip_suffix("-0")
                .expect("fixture StatefulSet")
                .to_owned(),
            postgresql_runtime: "agent-quarantine".to_owned(),
            bootstrap_hba_mode: "replication-bootstrap-primary".to_owned(),
            configuration_annotation: AnnotationIdentity {
                key: "pgshard.io/config-hash".to_owned(),
                value: postgresql_configuration.data_sha256.clone(),
            },
            shardschema_migration_annotation: AnnotationIdentity {
                key: "pgshard.io/shardschema-migration-sha256".to_owned(),
                value: "8".repeat(64),
            },
            sha256: String::new(),
        };
        target.sha256 = domain_digest(
            TARGET_POD_TEMPLATE_DOMAIN,
            &serde_json::to_vec(&TargetPodTemplateDigestInput {
                stateful_set_name: target.stateful_set_name.clone(),
                postgresql_runtime: target.postgresql_runtime.clone(),
                bootstrap_hba_mode: target.bootstrap_hba_mode.clone(),
                configuration_annotation: target.configuration_annotation.clone(),
                shardschema_migration_annotation: target.shardschema_migration_annotation.clone(),
            })
            .expect("target digest JSON"),
        );
        let mut bundle = MaterializationBundle {
            postgresql_configuration: postgresql_configuration.clone(),
            shardschema_migration: ContentReference {
                sha256: "8".repeat(64),
            },
            database_genesis: ContentReference {
                sha256: "a".repeat(64),
            },
            database_topology_preflight: ContentReference {
                sha256: "b".repeat(64),
            },
            catalog_access: catalog_access.clone(),
            operation_writer_access: operation_writer_access.clone(),
            serving_hba: PolicyReference {
                version: CATALOG_SERVING_HBA_POLICY_VERSION.to_owned(),
                sha256: CATALOG_SERVING_HBA_POLICY_SHA256.to_owned(),
            },
            target_pod_template: target,
            sha256: String::new(),
        };
        bundle.sha256 = domain_digest(
            MATERIALIZATION_BUNDLE_DOMAIN,
            &serde_json::to_vec(&MaterializationBundleDigestInput {
                postgresql_configuration: bundle.postgresql_configuration.clone(),
                shardschema_migration: bundle.shardschema_migration.clone(),
                database_genesis: bundle.database_genesis.clone(),
                database_topology_preflight: bundle.database_topology_preflight.clone(),
                catalog_access: bundle.catalog_access.clone(),
                operation_writer_access: bundle.operation_writer_access.clone(),
                serving_hba: bundle.serving_hba.clone(),
                target_pod_template: bundle.target_pod_template.clone(),
            })
            .expect("bundle digest JSON"),
        );
        bundle
    }

    fn fixture() -> (
        CatalogCandidateObservationPlan,
        DynamicObject,
        BTreeMap<String, ConfigMap>,
    ) {
        fixture_for_plan(plan())
    }

    fn store(cluster: DynamicObject, candidates: BTreeMap<String, ConfigMap>) -> StubStore {
        StubStore {
            clusters: Mutex::new(VecDeque::from([cluster])),
            candidates,
        }
    }

    fn bracket_store(
        before_cluster: DynamicObject,
        after_cluster: DynamicObject,
        before_candidates: BTreeMap<String, ConfigMap>,
        after_candidates: BTreeMap<String, ConfigMap>,
    ) -> BracketStore {
        let candidates_per_read = before_candidates.len();
        BracketStore {
            clusters: Mutex::new(VecDeque::from([before_cluster, after_cluster])),
            candidate_reads: Mutex::new(0),
            candidates_per_read,
            before_candidates,
            after_candidates,
        }
    }

    fn reverse_status_array(cluster: &mut DynamicObject, field: &str) {
        cluster.data["status"][field]
            .as_array_mut()
            .expect("status map-list")
            .reverse();
    }

    fn state() -> OrchState {
        OrchState::with_identity(
            OrchestratorIdentity {
                cluster_id: "demo".to_owned(),
                orchestrator_id: "orch-0".to_owned(),
            },
            15_000,
        )
        .expect("state")
    }

    fn state_with_clock(clock: Arc<FakeBoottimeClock>) -> OrchState {
        OrchState::with_identity_and_clock_for_test(
            OrchestratorIdentity {
                cluster_id: "demo".to_owned(),
                orchestrator_id: "orch-0".to_owned(),
            },
            15_000,
            clock,
        )
        .expect("state")
    }

    fn proof_member(
        plan: &CatalogCandidateObservationPlan,
        ordinal: u32,
    ) -> ReplicationProofMemberIdentity {
        let member = &plan.members[ordinal as usize];
        ReplicationProofMemberIdentity {
            cluster_id: plan.cluster_id.clone(),
            cluster_uid: plan.cluster_uid.clone(),
            shard_id: 0,
            member_ordinal: ordinal,
            instance_id: member.instance_id.clone(),
            pod_uid: format!("pod-uid-{ordinal}"),
            postmaster_pid: 100 + ordinal,
            boot_id: format!("00000000-0000-0000-0000-{ordinal:012}"),
        }
    }

    fn exact_replication_proof(
        plan: &CatalogCandidateObservationPlan,
    ) -> ShardZeroReplicationProof {
        let source = proof_member(plan, 0);
        let holder_identity = format!(
            "{}/{}/0123456789abcdef01234567",
            source.instance_id, source.pod_uid
        );
        let transitions = 7;
        let generation_identity = String::from_utf8(
            DurableWritableGeneration::new(
                plan.cluster_id.clone(),
                plan.cluster_uid.clone(),
                ShardId(0),
                plan.namespace.clone(),
                plan.writable_lease_name.clone(),
                plan.writable_lease_uid.clone(),
                holder_identity.clone(),
                transitions,
            )
            .expect("fixture writable generation")
            .canonical_bytes(),
        )
        .expect("canonical generation UTF-8");
        let standbys = plan
            .members
            .iter()
            .skip(1)
            .map(|member| ShardZeroStandbyReplicationProof {
                member: proof_member(plan, member.ordinal),
                source_instance_id: source.instance_id.clone(),
                member_slot_name: member.physical_slot.clone(),
                system_identifier: 42,
                timeline: 3,
                canonical_generation_identity: generation_identity.clone(),
                generation_barrier_lsn: 100,
                receive_lsn: 120 + u64::from(member.ordinal),
                replay_lsn: 110 + u64::from(member.ordinal),
            })
            .collect::<Vec<_>>();
        ShardZeroReplicationProof {
            writable_lease: WritableLeaseProofIdentity {
                namespace: plan.namespace.clone(),
                name: plan.writable_lease_name.clone(),
                uid: plan.writable_lease_uid.clone(),
                resource_version: "writable-lease-rv-7".to_owned(),
                holder_identity,
                transitions,
            },
            source: ShardZeroSourceReplicationProof {
                member: source,
                system_identifier: 42,
                timeline: 3,
                canonical_generation_identity: generation_identity.clone(),
                generation_barrier_lsn: 100,
                target_fence_acknowledgement: TargetFenceAcknowledgementProof {
                    observed_at_unix_ms: 1,
                    canonical_generation_identity: generation_identity.clone(),
                    deadline_boottime_ns: 5_000_000_000,
                    remaining_validity_at_ack_ms: 5_000,
                    remaining_validity_at_report_ms: 5_000,
                    boot_id: "00000000-0000-0000-0000-000000000000".to_owned(),
                    postmaster_pid: 100,
                    control_backend_pid: 200,
                },
            },
            remote_apply_witness: RemoteApplyWitnessProof {
                member: proof_member(plan, 1),
                member_slot_name: plan.members[1].physical_slot.clone(),
                system_identifier: 42,
                timeline: 3,
                canonical_generation_identity: generation_identity,
                generation_barrier_lsn: 100,
                receive_lsn: 121,
                replay_lsn: 111,
            },
            standbys,
        }
    }

    #[tokio::test]
    async fn accepts_exact_canonical_candidate_set() {
        let (plan, cluster, candidates) = fixture();
        let bound = read_bound_candidates(&store(cluster, candidates), &plan)
            .await
            .expect("bound candidates");
        assert_eq!(bound.candidates.len(), 3);
        assert_eq!(bound.materialization_bundles.len(), 3);
        assert!(
            bound
                .materialization_bundles
                .iter()
                .all(|bundle| bundle == &bound.materialization_bundles[0])
        );
        assert_eq!(bound.shard_zero_bootstraps.len(), 3);
        assert_eq!(bound.cluster.name, "demo");
        assert_eq!(bound.cluster.namespace, "ns");
        assert_eq!(bound.cluster.uid, "cluster-uid");
        assert_eq!(bound.cluster.resource_version, "cluster-rv");
        assert_eq!(bound.cluster.generation, 7);
        assert_eq!(bound.cluster.status_sha256.len(), 64);
        assert_eq!(bound.writable_lease.name, "demo-shard-0000-term");
        assert_eq!(bound.writable_lease.uid, "lease-uid-0");
        assert_eq!(bound.catalog_activation.name, "demo-catalog-activation");
        assert_eq!(bound.catalog_activation.uid, "catalog-activation-uid");
        assert_eq!(bound.replication_credential.uid, "replication-uid-0");
        assert_eq!(bound.catalog_access.uid, "catalog-uid");
        assert_eq!(
            bound.operation_writer_access.name,
            format!("demo-writer-{}", "d".repeat(32))
        );
        assert_eq!(bound.operation_writer_access.uid, "operation-writer-uid");
        assert_eq!(
            bound.operation_writer_access.material_sha256,
            "9".repeat(64)
        );
    }

    #[tokio::test]
    async fn accepts_content_addressed_candidate_incarnations() {
        assert_eq!(
            catalog_candidate_config_map_name("demo", 2, &"f".repeat(64)).as_deref(),
            Some("demo-s0-m0002-cfg-ffffffffffffffffffffffffffffffff")
        );
        assert_eq!(
            catalog_candidate_config_map_name(&"c".repeat(50), 4, &"f".repeat(64)).as_deref(),
            Some("ccccccccccc-5de6bf7f73e3-c0004-ffffffffffffffffffffffffffffffff")
        );
        let (plan, mut cluster, mut candidates) = fixture();
        for (index, member) in plan.members.iter().enumerate() {
            let old_name = member.config_map_name();
            let mut candidate = candidates.remove(&old_name).expect("legacy candidate");
            let payload_sha256 =
                cluster.data["status"]["postgresqlCatalogCandidates"][index]["payloadSHA256"]
                    .as_str()
                    .expect("payload digest");
            let name =
                catalog_candidate_config_map_name(&plan.cluster_id, member.ordinal, payload_sha256)
                    .expect("content-addressed name");
            assert!(valid_name(&name));
            assert!(name.len() <= 63);
            candidate.metadata.name = Some(name.clone());
            cluster.data["status"]["postgresqlCatalogCandidates"][index]["configMapName"] =
                json!(name.clone());
            candidates.insert(name, candidate);
        }

        let bound = read_bound_candidates(&store(cluster, candidates), &plan)
            .await
            .expect("content-addressed candidates");
        assert_eq!(bound.candidates.len(), plan.members.len());
        assert!(
            bound
                .candidates
                .iter()
                .all(|candidate| candidate.name.contains("-cfg-"))
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Exercises the complete inert proof-to-request chain.
    async fn validated_three_and_five_member_proofs_gate_exact_capability() {
        for member_count in [3_u32, 5] {
            let (plan, cluster, candidates) = fixture_for_plan(plan_with_members(member_count));
            let validated_status =
                validate_cluster_status(&cluster, &plan).expect("validated live status fixture");
            let live_carrier = validated_status.catalog_activation.clone();
            let live_candidates =
                read_bound_candidates(&store(cluster.clone(), candidates.clone()), &plan)
                    .await
                    .expect("authoritative live candidates");
            let total_members = plan.shard_count * plan.members.len();
            let state = OrchState::with_identity_topology_and_dispatcher(
                OrchestratorIdentity {
                    cluster_id: plan.cluster_id.clone(),
                    orchestrator_id: "demo-orchestrator-0".to_owned(),
                },
                "dispatcher-pod-uid".to_owned(),
                15_000,
                TopologyDiagnostics {
                    schema_version: TOPOLOGY_SCHEMA_VERSION.to_owned(),
                    cluster_object_uid: plan.cluster_uid.clone(),
                    shard_count: plan.shard_count,
                    member_count: total_members,
                    agent_status_collection:
                        AgentStatusCollectionState::DisabledPodIdentityRequired,
                },
            )
            .expect("state");
            let deadline = Instant::now() + Duration::from_secs(5);
            let summary = ReplicationCorrelationSummary {
                correlated_shards: 1,
                shard_zero_correlated: true,
                acknowledged_correlated_shards: 1,
                shard_zero_target_fence_acknowledged: true,
                remote_apply_witnessed_shards: 1,
                shard_zero_remote_apply_witnessed: true,
            };

            state.record_agent_status_collecting(Duration::from_secs(5));
            assert!(state.record_agent_status_fresh_exact(
                total_members,
                summary,
                Some(exact_replication_proof(&plan)),
                deadline,
            ));
            observe_once(
                &store(cluster, candidates),
                &plan,
                &state,
                Duration::from_secs(5),
            )
            .await
            .expect("real candidate validator accepted exact proof");
            assert!(state.record_coordination_ready(
                "coordination-uid",
                "coordination-rv-1",
                true,
                deadline,
            ));
            let capability = state
                .catalog_materialization_capability()
                .expect("validated exact proof overlap");
            assert!(state.revalidate_catalog_materialization_capability(&capability));
            let dispatch = state
                .catalog_bootstrap_dispatch(capability)
                .expect("exact target and material dispatch");
            assert!(state.revalidate_catalog_bootstrap_dispatch(&dispatch));

            let target = catalog_activation_publication_target(&dispatch)
                .expect("sealed publication target");
            assert_eq!(target.carrier_api_group(), "pgshard.io");
            assert_eq!(target.carrier_api_version(), "v1alpha1");
            assert_eq!(target.carrier_api_plural(), "pgshardcatalogactivations");
            assert_eq!(target.carrier_namespace(), "ns");
            assert_eq!(target.carrier_name(), "demo-catalog-activation");
            assert_eq!(target.carrier_uid(), "catalog-activation-uid");
            assert_eq!(target.cluster_name(), "demo");
            assert_eq!(target.cluster_uid(), "cluster-uid");
            assert_eq!(target.dispatcher_pod_name(), "demo-orchestrator-0");
            assert_eq!(target.dispatcher_pod_uid(), "dispatcher-pod-uid");
            assert_eq!(target.dispatcher_lease_name(), "demo-orch-lease");
            assert_eq!(target.dispatcher_lease_uid(), "coordination-uid");
            assert_eq!(
                target.dispatcher_lease_resource_version(),
                "coordination-rv-1"
            );
            assert_eq!(target.writable_lease_name(), "demo-shard-0000-term");
            assert_eq!(target.writable_lease_uid(), "lease-uid-0");
            assert_eq!(
                target.writable_lease_resource_version(),
                "writable-lease-rv-7"
            );
            assert_eq!(
                target.writable_lease_holder(),
                "demo-shard-0000-0/pod-uid-0/0123456789abcdef01234567"
            );
            assert_eq!(target.writable_lease_transitions(), 7);
            assert_eq!(target.target_stateful_set_name(), "demo-shard-0000");
            assert_eq!(target.target_pod_name(), "demo-shard-0000-0");
            assert_eq!(target.target_pod_uid(), "pod-uid-0");
            assert_eq!(
                target.target_agent_dns_name(),
                "demo-shard-0000-0.demo-shard-0000.ns.svc"
            );

            let live_writable_lease = exact_replication_proof(&plan).writable_lease;
            let dispatcher = CatalogActivationDispatcherProof {
                pod_name: "demo-orchestrator-0".to_owned(),
                pod_uid: "dispatcher-pod-uid".to_owned(),
                lease_name: "demo-orch-lease".to_owned(),
                lease_uid: "coordination-uid".to_owned(),
                lease_resource_version: "coordination-rv-1".to_owned(),
                lease_holder: concat!(
                    "demo-orchestrator-0/dispatcher-pod-uid/",
                    "123e4567-e89b-42d3-a456-426614174000"
                )
                .to_owned(),
            };
            let bind_dispatcher = |dispatcher| {
                bind_catalog_activation_live_objects(
                    &dispatch,
                    &live_candidates,
                    live_carrier.clone(),
                    "carrier-rv-1".to_owned(),
                    ObjectReference {
                        name: "demo-shard-0000-0".to_owned(),
                        uid: "pod-uid-0".to_owned(),
                    },
                    live_writable_lease.clone(),
                    dispatcher,
                )
            };
            let bind_candidates = |candidates: &BoundCandidateSet| {
                bind_catalog_activation_live_objects(
                    &dispatch,
                    candidates,
                    live_carrier.clone(),
                    "carrier-rv-1".to_owned(),
                    ObjectReference {
                        name: "demo-shard-0000-0".to_owned(),
                        uid: "pod-uid-0".to_owned(),
                    },
                    live_writable_lease.clone(),
                    dispatcher.clone(),
                )
            };

            let mut foreign_name = dispatcher.clone();
            foreign_name.pod_name = "foreign-orchestrator-0".to_owned();
            foreign_name.lease_holder = concat!(
                "foreign-orchestrator-0/dispatcher-pod-uid/",
                "123e4567-e89b-42d3-a456-426614174000"
            )
            .to_owned();
            assert!(bind_dispatcher(foreign_name).is_none());

            let mut foreign_uid = dispatcher.clone();
            foreign_uid.pod_uid = "foreign-dispatcher-pod-uid".to_owned();
            foreign_uid.lease_holder = concat!(
                "demo-orchestrator-0/foreign-dispatcher-pod-uid/",
                "123e4567-e89b-42d3-a456-426614174000"
            )
            .to_owned();
            assert!(bind_dispatcher(foreign_uid).is_none());

            let mut holder_name = dispatcher.clone();
            holder_name.lease_holder = concat!(
                "foreign-orchestrator-0/dispatcher-pod-uid/",
                "123e4567-e89b-42d3-a456-426614174000"
            )
            .to_owned();
            assert!(bind_dispatcher(holder_name).is_none());

            let mut holder_uid = dispatcher.clone();
            holder_uid.lease_holder = concat!(
                "demo-orchestrator-0/foreign-dispatcher-pod-uid/",
                "123e4567-e89b-42d3-a456-426614174000"
            )
            .to_owned();
            assert!(bind_dispatcher(holder_uid).is_none());

            let mut foreign_lease_name = dispatcher.clone();
            foreign_lease_name.lease_name = "foreign-orch-lease".to_owned();
            assert!(bind_dispatcher(foreign_lease_name).is_none());

            let mut foreign_lease_uid = dispatcher.clone();
            foreign_lease_uid.lease_uid = "foreign-coordination-uid".to_owned();
            assert!(bind_dispatcher(foreign_lease_uid).is_none());

            let mut foreign_lease_resource_version = dispatcher.clone();
            foreign_lease_resource_version.lease_resource_version =
                "foreign-coordination-rv".to_owned();
            assert!(bind_dispatcher(foreign_lease_resource_version).is_none());

            for invalid_incarnation in [
                "not-a-uuid",
                "123e4567-e89b-12d3-a456-426614174000",
                "123e4567-e89b-42d3-7456-426614174000",
                "123E4567-E89B-42D3-A456-426614174000",
            ] {
                let mut invalid_holder = dispatcher.clone();
                invalid_holder.lease_holder =
                    format!("demo-orchestrator-0/dispatcher-pod-uid/{invalid_incarnation}");
                assert!(bind_dispatcher(invalid_holder).is_none());
            }

            let live = bind_dispatcher(dispatcher.clone())
                .expect("exact configured dispatcher identities cross-bind");
            assert_eq!(live.carrier_resource_version(), "carrier-rv-1");
            let prepared = prepare_catalog_activation_request(&dispatch, &live)
                .expect("canonical activation request");
            prepared.request().validate().expect("validated request");
            assert_eq!(
                prepared.sha256(),
                prepared.request().sha256().expect("canonical digest")
            );
            assert_eq!(prepared.request().carrier.name, "demo-catalog-activation");
            assert_eq!(prepared.request().cluster.name, "demo");
            assert_eq!(prepared.request().cluster.namespace, "ns");
            assert_eq!(
                prepared.request().writable_term.resource_version,
                "writable-lease-rv-7"
            );
            assert_eq!(prepared.request().writable_term.generation, "7");
            let challenge_identity = ExpectedCatalogActivationCapability::from_prepared(&prepared)
                .expect("request-derived challenge identity");
            assert_eq!(challenge_identity.cluster.name, "demo");
            assert_eq!(challenge_identity.cluster.uid, "cluster-uid");
            assert_eq!(challenge_identity.carrier.namespace, "ns");
            assert_eq!(challenge_identity.carrier.name, "demo-catalog-activation");
            assert_eq!(challenge_identity.carrier.uid, "catalog-activation-uid");
            assert_eq!(challenge_identity.target.shard, 0);
            assert_eq!(challenge_identity.target.member, 0);
            assert_eq!(challenge_identity.target.instance_id, "demo-shard-0000-0");
            assert_eq!(challenge_identity.target.pod_name, "demo-shard-0000-0");
            assert_eq!(challenge_identity.target.pod_uid, "pod-uid-0");

            let mismatched_digest = PreparedCatalogActivationRequest::from_test_parts(
                prepared.request().clone(),
                "f".repeat(64),
            );
            assert!(
                ExpectedCatalogActivationCapability::from_prepared(&mismatched_digest).is_err()
            );
            let mut nonzero_member_request = prepared.request().clone();
            nonzero_member_request.source.member = 1;
            let nonzero_member_digest = nonzero_member_request
                .sha256()
                .unwrap_or_else(|_| "e".repeat(64));
            let nonzero_member = PreparedCatalogActivationRequest::from_test_parts(
                nonzero_member_request,
                nonzero_member_digest,
            );
            assert!(ExpectedCatalogActivationCapability::from_prepared(&nonzero_member).is_err());

            for invalid_carrier_rv in [String::new(), "x".repeat(257)] {
                assert!(
                    bind_catalog_activation_live_objects(
                        &dispatch,
                        &live_candidates,
                        live_carrier.clone(),
                        invalid_carrier_rv,
                        ObjectReference {
                            name: "demo-shard-0000-0".to_owned(),
                            uid: "pod-uid-0".to_owned(),
                        },
                        live_writable_lease.clone(),
                        dispatcher.clone(),
                    )
                    .is_none()
                );
            }

            for index in 1..live_candidates.candidates.len() {
                let mut drifted = live_candidates.clone();
                drifted.candidates[index].resource_version =
                    format!("replacement-candidate-rv-{index}");
                assert!(
                    bind_candidates(&drifted).is_none(),
                    "non-target candidate {index} drift cross-bound"
                );

                let mut drifted = live_candidates.clone();
                drifted.shard_zero_bootstraps[index].secret.uid =
                    format!("replacement-bootstrap-uid-{index}");
                assert!(
                    bind_candidates(&drifted).is_none(),
                    "non-target bootstrap {index} drift cross-bound"
                );

                let mut drifted = live_candidates.clone();
                drifted.materialization_bundles[index].sha256 = "f".repeat(64);
                assert!(
                    bind_candidates(&drifted).is_none(),
                    "non-target materialization bundle {index} drift cross-bound"
                );
            }

            let mut foreign_carrier = live_carrier.clone();
            foreign_carrier.uid = "replacement-carrier-uid".to_owned();
            assert!(
                bind_catalog_activation_live_objects(
                    &dispatch,
                    &live_candidates,
                    foreign_carrier,
                    "carrier-rv-1".to_owned(),
                    ObjectReference {
                        name: "demo-shard-0000-0".to_owned(),
                        uid: "pod-uid-0".to_owned(),
                    },
                    live_writable_lease.clone(),
                    dispatcher.clone(),
                )
                .is_none()
            );

            let mut drifted_candidates = live_candidates.clone();
            drifted_candidates.candidates[0].resource_version =
                "replacement-candidate-rv".to_owned();
            assert!(bind_candidates(&drifted_candidates).is_none());

            let public = serde_json::to_string(&state.snapshot()).expect("public snapshot JSON");
            for private_value in [
                "pod-uid-0",
                "dispatcher-pod-uid",
                "generation-7",
                "operation-writer-uid",
            ] {
                assert!(
                    !public.contains(private_value),
                    "private exact proof leaked through public diagnostics"
                );
            }
        }
    }

    #[tokio::test]
    async fn exact_proof_keeps_public_diagnostics_summary_only() {
        let (plan, cluster, candidates) = fixture();
        let state = state();
        observe_once(
            &store(cluster, candidates),
            &plan,
            &state,
            Duration::from_secs(5),
        )
        .await
        .expect("fresh exact candidate proof");
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot.catalog_candidates.phase,
            CatalogCandidatePhase::Fresh
        );
        assert_eq!(snapshot.catalog_candidates.expected_candidates, 3);
        assert_eq!(snapshot.catalog_candidates.fresh_candidates, 3);
        assert_eq!(snapshot.catalog_candidates.failure, None);
        let public = serde_json::to_string(&snapshot).expect("public snapshot JSON");
        let writer_digest = "9".repeat(64);
        for private_value in [
            "cluster-rv",
            "candidate-uid-0",
            "bootstrap-uid-0-0",
            "operation-writer-uid",
            writer_digest.as_str(),
        ] {
            assert!(
                !public.contains(private_value),
                "private exact proof leaked through public diagnostics"
            );
        }
    }

    #[tokio::test]
    async fn suspend_during_double_read_cannot_publish_candidate_evidence() {
        let (plan, cluster, candidates) = fixture();
        let clock = Arc::new(FakeBoottimeClock::new(
            BoottimeInstant::from_nanos_for_test(1_000_000_000),
        ));
        let state = state_with_clock(clock.clone());
        let store = SuspendingStore {
            inner: store(cluster, candidates),
            clock,
            advanced: AtomicBool::new(false),
        };

        let error = observe_once(&store, &plan, &state, Duration::from_secs(5))
            .await
            .expect_err("suspend must consume candidate freshness");

        assert!(matches!(error, CatalogCandidateError::FreshnessExpired));
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot.catalog_candidates.phase,
            CatalogCandidatePhase::Unavailable
        );
        assert_eq!(snapshot.catalog_candidates.fresh_candidates, 0);
    }

    #[tokio::test]
    async fn accepts_shuffled_map_lists_for_complete_three_and_five_member_sets() {
        for member_count in [3, 5] {
            let (plan, mut cluster, candidates) = fixture_for_plan(plan_with_members(member_count));
            for field in [
                "postgresqlBootstraps",
                "postgresqlWritableLeases",
                "postgresqlReplicationCredentials",
                "postgresqlCatalogCandidates",
            ] {
                reverse_status_array(&mut cluster, field);
            }
            let validated = validate_cluster_status(&cluster, &plan).expect("shuffled status");
            assert_eq!(validated.candidates.len(), member_count as usize);
            assert!(
                validated
                    .candidates
                    .iter()
                    .enumerate()
                    .all(|(member, checkpoint)| checkpoint.member == member)
            );
            assert!(
                validated
                    .bootstraps
                    .iter()
                    .enumerate()
                    .all(
                        |(member, checkpoint)| checkpoint.shard == 0 && checkpoint.member == member
                    )
            );
            let bound = read_bound_candidates(&store(cluster, candidates), &plan)
                .await
                .expect("complete shuffled candidate set");
            assert_eq!(bound.candidates.len(), member_count as usize);
            assert_eq!(bound.shard_zero_bootstraps.len(), member_count as usize);
            assert!(bound.candidates.iter().all(|candidate| {
                candidate.document.discovery_topology.members.len() == member_count as usize
            }));
        }
    }

    #[test]
    fn accepts_additive_unversioned_status_fields_with_bounded_fingerprint() {
        let (plan, mut cluster, _) = fixture();
        let status = cluster.data["status"]
            .as_object_mut()
            .expect("status object");
        status.insert(
            "futureTopLevelField".to_owned(),
            json!({"value": "x".repeat(64 * 1_024)}),
        );
        status["postgresqlBootstrapSpec"]
            .as_object_mut()
            .expect("bootstrap spec")
            .insert("futureSpecField".to_owned(), json!(true));
        for field in [
            "postgresqlBootstraps",
            "postgresqlWritableLeases",
            "postgresqlReplicationCredentials",
            "postgresqlCatalogCandidates",
        ] {
            status[field][0]
                .as_object_mut()
                .expect("nested checkpoint")
                .insert("futureCheckpointField".to_owned(), json!("ignored"));
        }
        status["catalogAccess"]
            .as_object_mut()
            .expect("catalog access")
            .insert("futureAccessField".to_owned(), json!([1, 2, 3]));
        let raw_status = cluster.data["status"].clone();
        let expected = domain_digest(
            CLUSTER_STATUS_FINGERPRINT_DOMAIN,
            &serde_json::to_vec(&raw_status).expect("status JSON"),
        );

        let validated = validate_cluster_status(&cluster, &plan).expect("additive status fields");
        assert_eq!(validated.fingerprint.status_sha256, expected);
        assert_eq!(validated.fingerprint.status_sha256.len(), 64);
    }

    #[test]
    fn payload_digest_uses_exact_domain_and_bytes() {
        assert_eq!(
            domain_digest(CANDIDATE_PAYLOAD_DOMAIN, b"{}\n"),
            "050e522beb772aada3dd0c85e282839ec56ed4388c5ac0c2e77c243ff738ebbf"
        );
        assert_ne!(
            domain_digest(CANDIDATE_PAYLOAD_DOMAIN, b"{}"),
            domain_digest(CANDIDATE_PAYLOAD_DOMAIN, b"{}\n")
        );
    }

    #[test]
    fn cluster_status_digest_matches_go_canonical_json() {
        let status = json!({
            "condition": "<ready>&\u{2028}\u{2029}",
            "literal": r"\u003c\u2028",
        });
        let encoded = go_compatible_json(&status).expect("status JSON");
        assert_eq!(
            encoded,
            br#"{"condition":"\u003cready\u003e\u0026\u2028\u2029","literal":"\\u003c\\u2028"}"#
        );
        assert_eq!(
            domain_digest(CLUSTER_STATUS_FINGERPRINT_DOMAIN, &encoded),
            "cbe68afa073340a29cc0a806ed69aba0ecd325f6d7b50df8c06f29b76e0f4471"
        );
    }

    #[test]
    fn cluster_status_digest_rejects_non_integer_numbers() {
        assert!(go_compatible_json_domain(&json!({
            "null": null,
            "bool": true,
            "string": "value",
            "signed": -1,
            "unsigned": 1,
            "nested": [{"integer": 2}],
        })));
        assert!(!go_compatible_json_domain(&json!({"value": 1.0})));
        assert!(!go_compatible_json_domain(
            &json!({"value": [{"nested": 1.25}]})
        ));

        let (plan, mut cluster, _) = fixture();
        cluster.data["status"]["numericDrift"] = json!(1.0);
        assert!(matches!(
            validate_cluster_status(&cluster, &plan),
            Err(CatalogCandidateError::InvalidClusterStatus)
        ));
    }

    #[test]
    fn rejects_noncanonical_payloads_and_metadata() {
        let (plan, cluster, candidates) = fixture();
        let status = validate_cluster_status(&cluster, &plan).expect("status");
        let member = &plan.members[0];
        let checkpoint = &status.candidates[0];
        for mutation in [
            "unknown-field",
            "extra-newline",
            "binary-data",
            "role-label",
        ] {
            let mut candidate = candidates[&member.config_map_name()].clone();
            match mutation {
                "unknown-field" => {
                    let payload = candidate
                        .data
                        .as_mut()
                        .expect("data")
                        .get_mut(CANDIDATE_PAYLOAD_KEY)
                        .expect("payload");
                    let mut value: Value = serde_json::from_str(payload).expect("JSON");
                    value
                        .as_object_mut()
                        .expect("object")
                        .insert("unexpected".to_owned(), json!(true));
                    *payload = format!("{}\n", serde_json::to_string(&value).expect("JSON"));
                }
                "extra-newline" => candidate
                    .data
                    .as_mut()
                    .expect("data")
                    .get_mut(CANDIDATE_PAYLOAD_KEY)
                    .expect("payload")
                    .push('\n'),
                "binary-data" => {
                    candidate.binary_data =
                        Some(BTreeMap::from([("x".to_owned(), ByteString::default())]));
                }
                "role-label" => {
                    candidate
                        .metadata
                        .labels
                        .as_mut()
                        .expect("labels")
                        .insert(ROLE_LABEL.to_owned(), "primary".to_owned());
                }
                _ => unreachable!(),
            }
            assert!(
                validate_candidate(&candidate, &plan, member, checkpoint, &status).is_err(),
                "mutation {mutation} was accepted"
            );
        }
    }

    #[test]
    fn rejects_recheckpointed_materialization_bundle_mismatch() {
        let (plan, cluster, candidates) = fixture();
        let status = validate_cluster_status(&cluster, &plan).expect("status");
        let member = &plan.members[0];
        let mut candidate = candidates[&member.config_map_name()].clone();
        let payload = candidate
            .data
            .as_mut()
            .expect("data")
            .get_mut(CANDIDATE_PAYLOAD_KEY)
            .expect("payload");
        let mut document: CandidateDocumentV1 =
            serde_json::from_str(payload).expect("candidate document");
        document
            .materialization_bundle
            .target_pod_template
            .configuration_annotation
            .value = "f".repeat(64);
        *payload = format!(
            "{}\n",
            serde_json::to_string(&document).expect("candidate JSON")
        );
        let payload_sha256 = domain_digest(CANDIDATE_PAYLOAD_DOMAIN, payload.as_bytes());
        candidate
            .metadata
            .annotations
            .as_mut()
            .expect("annotations")
            .insert(CONFIG_HASH_ANNOTATION.to_owned(), payload_sha256.clone());
        let mut checkpoint = status.candidates[0].clone();
        checkpoint.payload_sha256 = payload_sha256;
        assert!(matches!(
            validate_candidate(&candidate, &plan, member, &checkpoint, &status),
            Err(CatalogCandidateError::InvalidCandidate)
        ));
    }

    #[test]
    fn rejects_recheckpointed_foreign_serving_hba_policy() {
        let (plan, cluster, candidates) = fixture();
        let status = validate_cluster_status(&cluster, &plan).expect("status");
        let member = &plan.members[0];
        let mut candidate = candidates[&member.config_map_name()].clone();
        let payload = candidate
            .data
            .as_mut()
            .expect("data")
            .get_mut(CANDIDATE_PAYLOAD_KEY)
            .expect("payload");
        let mut document: CandidateDocumentV1 =
            serde_json::from_str(payload).expect("candidate document");
        let bundle = &mut document.materialization_bundle;
        assert_eq!(
            content_digest(CATALOG_SERVING_HBA_POLICY.as_bytes()),
            CATALOG_SERVING_HBA_POLICY_SHA256
        );
        assert_eq!(bundle.serving_hba.sha256, CATALOG_SERVING_HBA_POLICY_SHA256);
        bundle.serving_hba.sha256 = "d".repeat(64);
        bundle.sha256 = domain_digest(
            MATERIALIZATION_BUNDLE_DOMAIN,
            &serde_json::to_vec(&MaterializationBundleDigestInput {
                postgresql_configuration: bundle.postgresql_configuration.clone(),
                shardschema_migration: bundle.shardschema_migration.clone(),
                database_genesis: bundle.database_genesis.clone(),
                database_topology_preflight: bundle.database_topology_preflight.clone(),
                catalog_access: bundle.catalog_access.clone(),
                operation_writer_access: bundle.operation_writer_access.clone(),
                serving_hba: bundle.serving_hba.clone(),
                target_pod_template: bundle.target_pod_template.clone(),
            })
            .expect("bundle digest JSON"),
        );
        *payload = format!(
            "{}\n",
            serde_json::to_string(&document).expect("candidate JSON")
        );
        let payload_sha256 = domain_digest(CANDIDATE_PAYLOAD_DOMAIN, payload.as_bytes());
        candidate
            .metadata
            .annotations
            .as_mut()
            .expect("annotations")
            .insert(CONFIG_HASH_ANNOTATION.to_owned(), payload_sha256.clone());
        let mut checkpoint = status.candidates[0].clone();
        checkpoint.payload_sha256 = payload_sha256;

        assert!(matches!(
            validate_candidate(&candidate, &plan, member, &checkpoint, &status),
            Err(CatalogCandidateError::InvalidCandidate)
        ));
    }

    #[test]
    fn rejects_candidate_checkpoint_cardinality_and_duplicates() {
        let (plan, cluster, _) = fixture();
        let status_value = cluster.data.get("status").expect("status");
        let status: ClusterStatus =
            serde_json::from_value(status_value.clone()).expect("status DTO");
        let mut missing = status.catalog_candidates.clone();
        missing.pop();
        assert!(validate_candidate_checkpoints(&missing, &plan).is_err());
        let mut duplicate = status.catalog_candidates;
        duplicate[1].payload_sha256 = duplicate[0].payload_sha256.clone();
        assert!(validate_candidate_checkpoints(&duplicate, &plan).is_err());

        let mut duplicate_lease = status.writable_leases.clone();
        duplicate_lease[1].lease_uid = duplicate_lease[0].lease_uid.clone();
        assert!(select_writable_lease(&duplicate_lease, &plan).is_err());

        let mut missing_credential = status.replication_credentials;
        missing_credential.pop();
        assert!(select_replication_credential(&missing_credential, &plan).is_err());
    }

    #[test]
    fn rejects_missing_extra_duplicate_and_mismatched_map_keys() {
        let (plan, cluster, _) = fixture();

        let mut missing = cluster.clone();
        missing.data["status"]["postgresqlCatalogCandidates"]
            .as_array_mut()
            .expect("candidates")
            .pop();
        assert!(validate_cluster_status(&missing, &plan).is_err());

        let mut extra = cluster.clone();
        let extra_candidate = extra.data["status"]["postgresqlCatalogCandidates"][0].clone();
        extra.data["status"]["postgresqlCatalogCandidates"]
            .as_array_mut()
            .expect("candidates")
            .push(extra_candidate);
        assert!(validate_cluster_status(&extra, &plan).is_err());

        let mut duplicate = cluster.clone();
        duplicate.data["status"]["postgresqlBootstraps"][1]["shard"] = json!(0);
        duplicate.data["status"]["postgresqlBootstraps"][1]["member"] = json!(0);
        assert!(validate_cluster_status(&duplicate, &plan).is_err());

        let mut mismatched = cluster.clone();
        mismatched.data["status"]["postgresqlWritableLeases"][0]["shard"] = json!(99);
        assert!(validate_cluster_status(&mismatched, &plan).is_err());

        let mut duplicate_replication = cluster;
        duplicate_replication.data["status"]["postgresqlReplicationCredentials"][1]["shard"] =
            json!(0);
        assert!(validate_cluster_status(&duplicate_replication, &plan).is_err());
    }

    #[test]
    fn rejects_missing_or_malformed_operation_writer_checkpoint() {
        let (plan, cluster, _) = fixture();

        let mut missing = cluster.clone();
        missing.data["status"]
            .as_object_mut()
            .expect("status")
            .remove("operationWriterAccess");
        assert!(validate_cluster_status(&missing, &plan).is_err());

        for (field, value) in [
            ("secretName", json!("demo-writer-not-random")),
            ("secretUID", json!("")),
            ("materialSHA256", json!("A".repeat(64))),
        ] {
            let mut malformed = cluster.clone();
            malformed.data["status"]["operationWriterAccess"][field] = value;
            assert!(
                validate_cluster_status(&malformed, &plan).is_err(),
                "malformed operation-writer {field} was accepted"
            );
        }
    }

    #[test]
    fn rejects_missing_or_foreign_catalog_activation_checkpoint() {
        let (plan, cluster, _) = fixture();

        let mut missing = cluster.clone();
        missing.data["status"]
            .as_object_mut()
            .expect("status")
            .remove("catalogActivation");
        assert!(validate_cluster_status(&missing, &plan).is_err());

        for (field, value) in [
            ("name", json!("other-catalog-activation")),
            ("uid", json!("")),
        ] {
            let mut malformed = cluster.clone();
            malformed.data["status"]["catalogActivation"][field] = value;
            assert!(
                validate_cluster_status(&malformed, &plan).is_err(),
                "malformed catalog activation {field} was accepted"
            );
        }
    }

    #[test]
    fn rejects_missing_or_malformed_postgresql_configuration_checkpoint() {
        let (plan, cluster, _) = fixture();

        let mut missing = cluster.clone();
        missing.data["status"]
            .as_object_mut()
            .expect("status")
            .remove("postgresqlConfiguration");
        assert!(validate_cluster_status(&missing, &plan).is_err());

        for (field, value) in [
            ("configMapName", json!("foreign-postgresql-config")),
            ("configMapUID", json!("")),
            ("dataSHA256", json!("A".repeat(64))),
        ] {
            let mut malformed = cluster.clone();
            malformed.data["status"]["postgresqlConfiguration"][field] = value;
            assert!(
                validate_cluster_status(&malformed, &plan).is_err(),
                "malformed PostgreSQL configuration {field} was accepted"
            );
        }
    }

    #[test]
    fn operation_writer_name_matches_the_cluster_derived_bounded_contract() {
        assert!(operation_writer_secret_name_is_valid(
            "demo",
            &format!("demo-writer-{}", "a".repeat(32))
        ));
        assert!(!operation_writer_secret_name_is_valid(
            "demo",
            &format!("other-writer-{}", "a".repeat(32))
        ));

        let cluster = "this-is-a-cluster-name-longer-than-prefix";
        let digest = Sha256::digest(cluster.as_bytes());
        let suffix = digest[..6].iter().fold(String::new(), |mut suffix, byte| {
            let _ = write!(suffix, "{byte:02x}");
            suffix
        });
        let name = format!("{}-wr-{suffix}-{}", &cluster[..14], "b".repeat(32));
        assert_eq!(
            name,
            format!("this-is-a-clus-wr-b9e961f439c9-{}", "b".repeat(32))
        );
        assert_eq!(name.len(), 63);
        assert!(operation_writer_secret_name_is_valid(cluster, &name));
    }

    #[test]
    fn accepts_only_supported_three_or_five_member_plans() {
        let mut five = plan();
        for ordinal in 3..5 {
            let stateful_set = format!("demo-shard-0000-m{ordinal:04}");
            five.members.push(CatalogCandidateTopologyMember {
                ordinal,
                stateful_set: stateful_set.clone(),
                instance_id: format!("{stateful_set}-0"),
                dns_name: format!("{stateful_set}-0.demo-shard-0000.ns.svc"),
                postgresql_port: 5_432,
                agent_http_port: 8_080,
                physical_slot: format!("pgshard_member_{ordinal:04}"),
            });
        }
        assert!(validate_plan(&five, Duration::from_secs(5)).is_ok());
        five.members.pop();
        assert!(validate_plan(&five, Duration::from_secs(5)).is_err());
        assert!(validate_plan(&plan(), Duration::from_millis(5_001)).is_err());
    }

    #[test]
    fn authoritative_reader_rejects_invalid_or_unbounded_budgets_before_client_creation() {
        for (per_request, overall) in [
            (Duration::from_millis(99), Duration::from_secs(1)),
            (Duration::from_secs(1), Duration::from_millis(999)),
            (Duration::from_secs(1), Duration::from_millis(5_001)),
        ] {
            assert!(matches!(
                AuthoritativeCandidateReader::new(plan(), per_request, overall),
                Err(CatalogCandidateError::InvalidPlan)
            ));
        }
    }

    #[tokio::test]
    async fn rejects_pre_post_cluster_resource_version_or_generation_change_without_last_good() {
        for field in ["resource-version", "generation"] {
            let (plan, cluster, candidates) = fixture();
            let mut changed = cluster.clone();
            match field {
                "resource-version" => {
                    changed.metadata.resource_version = Some("cluster-rv-2".to_owned());
                }
                "generation" => {
                    changed.metadata.generation = Some(8);
                    changed.data["status"]["observedGeneration"] = json!(8);
                }
                _ => unreachable!(),
            }
            let store = StubStore {
                clusters: Mutex::new(VecDeque::from([cluster, changed])),
                candidates,
            };
            let state = state();
            let error = observe_once(&store, &plan, &state, Duration::from_secs(5))
                .await
                .expect_err("changed bracket");
            assert!(
                matches!(error, CatalogCandidateError::EvidenceChanged),
                "cluster {field} drift returned {error:?}"
            );
            let snapshot = state.snapshot();
            assert_eq!(
                snapshot.catalog_candidates.phase,
                CatalogCandidatePhase::Unavailable
            );
            assert_eq!(snapshot.catalog_candidates.fresh_candidates, 0);
            assert_eq!(
                snapshot.catalog_candidates.failure,
                Some(CatalogCandidateFailureReason::EvidenceChanged)
            );
        }
    }

    #[tokio::test]
    async fn authoritative_reread_has_one_global_deadline() {
        let error =
            read_authoritative_with_store(&BlockingStore, &plan(), Duration::from_millis(10))
                .await
                .expect_err("stalled authoritative read");
        assert!(matches!(
            error,
            CatalogCandidateError::AuthoritativeReadTimedOut
        ));
    }

    #[tokio::test]
    async fn authoritative_reread_accumulates_healthy_gets_under_a_distinct_total_budget() {
        let (plan, cluster, candidates) = fixture();
        let per_request_timeout = Duration::from_millis(100);
        let store = DelayedStore {
            inner: store(cluster, candidates),
            delay: Duration::from_millis(20),
            per_request_timeout,
        };
        let started = Instant::now();
        let bound = read_authoritative_with_store(&store, &plan, Duration::from_millis(500))
            .await
            .expect("individually healthy stable reads fit the total budget");
        assert_eq!(bound.candidate_count(), 3);
        assert!(
            started.elapsed() > per_request_timeout,
            "the test must accumulate more latency than one GET budget"
        );
    }

    #[tokio::test]
    async fn rejects_cluster_uid_drift_across_the_double_read() {
        let (plan, cluster, candidates) = fixture();
        let mut changed = cluster.clone();
        changed.metadata.uid = Some("replacement-cluster-uid".to_owned());
        let store = StubStore {
            clusters: Mutex::new(VecDeque::from([cluster, changed])),
            candidates,
        };
        let state = state();
        let error = observe_once(&store, &plan, &state, Duration::from_secs(5))
            .await
            .expect_err("changed cluster UID");
        assert!(matches!(error, CatalogCandidateError::InvalidClusterStatus));
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot.catalog_candidates.phase,
            CatalogCandidatePhase::Unavailable
        );
        assert_eq!(snapshot.catalog_candidates.fresh_candidates, 0);
    }

    #[tokio::test]
    async fn rejects_candidate_uid_or_resource_version_drift_across_the_double_read() {
        for field in ["uid", "resource-version"] {
            let (plan, cluster, candidates) = fixture();
            let mut changed_cluster = cluster.clone();
            let mut changed_candidates = candidates.clone();
            let name = plan.members[0].config_map_name();
            let changed = changed_candidates.get_mut(&name).expect("candidate");
            match field {
                "uid" => {
                    changed.metadata.uid = Some("replacement-candidate-uid".to_owned());
                    changed_cluster.data["status"]["postgresqlCatalogCandidates"][0]["configMapUID"] =
                        json!("replacement-candidate-uid");
                }
                "resource-version" => {
                    changed.metadata.resource_version = Some("replacement-rv".to_owned());
                }
                _ => unreachable!(),
            }
            let store = bracket_store(cluster, changed_cluster, candidates, changed_candidates);
            let state = state();
            let error = observe_once(&store, &plan, &state, Duration::from_secs(5))
                .await
                .expect_err("changed candidate identity");
            assert!(
                matches!(error, CatalogCandidateError::EvidenceChanged),
                "candidate {field} drift returned {error:?}"
            );
            let snapshot = state.snapshot();
            assert_eq!(
                snapshot.catalog_candidates.failure,
                Some(CatalogCandidateFailureReason::EvidenceChanged)
            );
            assert_eq!(snapshot.catalog_candidates.fresh_candidates, 0);
        }
    }

    #[tokio::test]
    async fn rejects_operation_writer_checkpoint_not_bound_by_candidate() {
        let (plan, cluster, candidates) = fixture();
        let mut changed = cluster.clone();
        changed.data["status"]["operationWriterAccess"]["secretUID"] =
            json!("replacement-operation-writer-uid");
        let state = state();
        let error = observe_once(
            &StubStore {
                clusters: Mutex::new(VecDeque::from([cluster, changed])),
                candidates,
            },
            &plan,
            &state,
            Duration::from_secs(5),
        )
        .await
        .expect_err("replaced operation-writer checkpoint");
        assert!(matches!(error, CatalogCandidateError::InvalidCandidate));
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot.catalog_candidates.failure,
            Some(CatalogCandidateFailureReason::ValidationFailed)
        );
        assert_eq!(snapshot.catalog_candidates.fresh_candidates, 0);
    }

    #[tokio::test]
    async fn expiration_and_shutdown_are_terminal_and_do_not_couple_state() {
        let (plan, cluster, candidates) = fixture();
        let store = store(cluster, candidates);
        let state = state();
        assert!(state.record_coordination_ready(
            "coordination-uid",
            "coordination-rv",
            true,
            Instant::now() + Duration::from_secs(30),
        ));
        state.record_agent_status_collecting(Duration::from_secs(5));
        let error = observe_once(&store, &plan, &state, Duration::ZERO)
            .await
            .expect_err("zero freshness");
        assert!(matches!(error, CatalogCandidateError::FreshnessExpired));
        let snapshot = state.snapshot();
        assert!(snapshot.coordination_ready);
        assert!(snapshot.leader);
        assert_eq!(snapshot.agent_status.phase, AgentStatusPhase::Collecting);
        assert_eq!(
            snapshot.catalog_candidates.phase,
            CatalogCandidatePhase::Unavailable
        );

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn({
            let state = state.clone();
            let plan = plan.clone();
            async move {
                supervise_with_store(
                    &BlockingStore,
                    &plan,
                    &state,
                    &mut shutdown_rx,
                    Duration::from_secs(30),
                    Duration::from_secs(5),
                )
                .await;
            }
        });
        tokio::task::yield_now().await;
        shutdown_tx.send(true).expect("shutdown");
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("bounded shutdown")
            .expect("supervisor task");
        let snapshot = state.snapshot();
        assert_eq!(
            snapshot.catalog_candidates.phase,
            CatalogCandidatePhase::ShuttingDown
        );
        assert!(snapshot.coordination_ready);
        assert_eq!(snapshot.agent_status.phase, AgentStatusPhase::Collecting);
    }
}
