//! Canonical, inert catalog-activation request contract.
//!
//! The types in this module carry exact identities and freshness evidence for
//! a future bootstrap executor. They grant no serving, routing, SQL, or
//! `PostgreSQL` process authority by themselves.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ShardId, writable_generation::DurableWritableGeneration};

/// Version of the fixed-order catalog-activation request encoding.
pub const CATALOG_ACTIVATION_REQUEST_VERSION: &str = "pgshard.catalog-activation-request.v1";

/// Version of the agent's separately advertised consumer capability.
pub const CATALOG_ACTIVATION_CAPABILITY_VERSION: &str =
    "pgshard.agent.catalog-activation-capability.v1";

/// Name of the inert journal-and-acknowledgement consumer capability.
pub const CATALOG_ACTIVATION_CONSUMER_VERSION: &str = "pgshard.catalog-activation-consumer.v1";

/// Version of the fsync-backed carrier acceptance record.
pub const CATALOG_ACTIVATION_ACCEPTANCE_VERSION: &str = "pgshard.catalog-activation-acceptance.v1";

/// Persistence contract used by a durable catalog-activation acceptance.
pub const CATALOG_ACTIVATION_FSYNC_PERSISTENCE: &str = "fsync";

/// Domain separator for the fixed-order request digest.
pub const CATALOG_ACTIVATION_REQUEST_DIGEST_DOMAIN: &str = "pgshard-catalog-activation-request-v1";

const POSTGRESQL_WORKLOAD_PREFIX_MAXIMUM: usize = 42;
const POSTGRESQL_WORKLOAD_DIGEST_BYTES: usize = 12;
const PROCESS_INCARNATION_HEX_LENGTH: usize = 24;

/// Exact cluster identity advertised by the target agent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationCapabilityCluster {
    /// `PgShardCluster` name.
    pub name: String,
    /// API-assigned `PgShardCluster` UID.
    pub uid: String,
}

/// Exact carrier identity advertised by the target agent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationCapabilityCarrier {
    /// Carrier namespace.
    pub namespace: String,
    /// Fixed carrier name.
    pub name: String,
    /// API-assigned carrier UID.
    pub uid: String,
}

/// Exact shard-zero member-zero target identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationCapabilityTarget {
    /// Physical shard ordinal. This capability supports only zero.
    pub shard: u32,
    /// Physical member ordinal. This capability supports only zero.
    pub member: u32,
    /// Stable target instance identity.
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    /// Target Pod name.
    pub pod_name: String,
    /// API-assigned target Pod UID.
    #[serde(rename = "podUID")]
    pub pod_uid: String,
}

/// Versioned, non-authoritative advertisement for the dormant consumer.
///
/// This document proves only that the exact carrier was read after local
/// journal recovery. It grants no serving, routing, SQL, process, or fencing
/// authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationCapability {
    /// Schema of this response.
    pub schema_version: String,
    /// Implemented consumer contract.
    pub capability: String,
    /// Accepted request schema.
    pub request_schema_version: String,
    /// Emitted acceptance schema.
    pub acceptance_schema_version: String,
    /// Local acceptance persistence contract.
    pub persistence: String,
    /// Exact cluster identity.
    pub cluster: CatalogActivationCapabilityCluster,
    /// Exact carrier identity.
    pub carrier: CatalogActivationCapabilityCarrier,
    /// Exact target identity.
    pub target: CatalogActivationCapabilityTarget,
}

/// Fsync-backed acceptance stored in the carrier status.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationAcceptance {
    /// Acceptance schema.
    pub schema_version: String,
    /// Exact carrier UID.
    #[serde(rename = "carrierUID")]
    pub carrier_uid: String,
    /// Digest of the exact durable request.
    #[serde(rename = "requestSHA256")]
    pub request_sha256: String,
    /// Exact accepting target Pod name.
    pub target_pod_name: String,
    /// API-assigned accepting target Pod UID.
    #[serde(rename = "targetPodUID")]
    pub target_pod_uid: String,
    /// Persistence contract, always `fsync` in v1.
    pub persistence: String,
    /// Diagnostic original persistence time as canonical unsigned decimal.
    #[serde(rename = "persistedAtUnixMS")]
    pub persisted_at_unix_ms: String,
}

fn postgresql_workload_prefix(cluster: &str) -> String {
    if cluster.len() < POSTGRESQL_WORKLOAD_PREFIX_MAXIMUM {
        return cluster.to_owned();
    }
    let digest = Sha256::digest(cluster.as_bytes());
    let suffix = lower_hex(&digest[..POSTGRESQL_WORKLOAD_DIGEST_BYTES]);
    format!(
        "{}-{suffix}",
        &cluster[..POSTGRESQL_WORKLOAD_PREFIX_MAXIMUM - suffix.len() - 1]
    )
}

/// Returns the exact Pod name for one singleton `PostgreSQL` member workload.
///
/// Member zero retains the original shard `StatefulSet` name. Additional stable
/// members use the operator's `-mNNNN` suffix before the `StatefulSet` Pod
/// ordinal.
#[must_use]
pub fn postgresql_member_pod_name(cluster: &str, shard: u32, member: u32) -> String {
    let workload = format!("{}-shard-{shard:04}", postgresql_workload_prefix(cluster));
    if member == 0 {
        format!("{workload}-0")
    } else {
        format!("{workload}-m{member:04}-0")
    }
}

fn postgresql_writable_lease_name(cluster: &str, shard: u32) -> String {
    format!(
        "{}-shard-{shard:04}-term",
        postgresql_workload_prefix(cluster)
    )
}

fn writable_holder_belongs_to_source(holder: &str, source: &CatalogActivationSource) -> bool {
    let mut pieces = holder.split('/');
    let instance_id = pieces.next().unwrap_or_default();
    let pod_uid = pieces.next().unwrap_or_default();
    let process_incarnation = pieces.next().unwrap_or_default();
    pieces.next().is_none()
        && instance_id == source.instance_id
        && pod_uid == source.pod_uid
        && process_incarnation.len() == PROCESS_INCARNATION_HEX_LENGTH
        && process_incarnation
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn dispatcher_holder_belongs_to_pod(
    holder: &str,
    dispatcher: &CatalogActivationDispatcher,
) -> bool {
    let mut pieces = holder.split('/');
    let pod_name = pieces.next().unwrap_or_default();
    let pod_uid = pieces.next().unwrap_or_default();
    let process_incarnation = pieces.next().unwrap_or_default();
    pieces.next().is_none()
        && pod_name == dispatcher.pod_name
        && pod_uid == dispatcher.pod_uid
        && valid_uuid_v4(process_incarnation)
}

fn valid_uuid_v4(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes[8] == b'-'
        && bytes[13] == b'-'
        && bytes[18] == b'-'
        && bytes[23] == b'-'
        && bytes[14] == b'4'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
        && bytes.iter().enumerate().all(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23)
                || byte.is_ascii_digit()
                || (b'a'..=b'f').contains(byte)
        })
}

/// One Kubernetes object identity, including the version read by the publisher.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KubernetesObjectVersion {
    /// Object name.
    pub name: String,
    /// API-assigned object UID.
    pub uid: String,
    /// Opaque API resource version.
    pub resource_version: String,
}

/// Exact immutable Secret or PVC identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KubernetesObjectIdentity {
    /// Object name.
    pub name: String,
    /// API-assigned object UID.
    pub uid: String,
}

/// Exact material-bearing object and its content fingerprint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MaterialIdentity {
    /// Object name.
    pub name: String,
    /// API-assigned object UID.
    pub uid: String,
    /// Lowercase SHA-256 digest of the projected material.
    #[serde(rename = "materialSHA256")]
    pub material_sha256: String,
}

/// Exact catalog TLS and client material.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogMaterialIdentity {
    /// Secret name.
    pub name: String,
    /// API-assigned Secret UID.
    pub uid: String,
    /// Lowercase SHA-256 digest of client material.
    #[serde(rename = "clientSHA256")]
    pub client_sha256: String,
    /// Lowercase SHA-256 digest of server material.
    #[serde(rename = "serverSHA256")]
    pub server_sha256: String,
}

/// Exact cluster object and status snapshot observed by the dispatcher.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationCluster {
    /// `PgShardCluster` name.
    pub name: String,
    /// Kubernetes namespace containing every request object.
    pub namespace: String,
    /// API-assigned `PgShardCluster` UID.
    pub uid: String,
    /// Canonical unsigned-decimal metadata generation.
    pub generation: String,
    /// Opaque `PgShardCluster` resource version.
    pub resource_version: String,
    /// Lowercase SHA-256 digest of the exact status projection.
    #[serde(rename = "statusSHA256")]
    pub status_sha256: String,
}

/// Publisher Pod and elected orchestrator term.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationDispatcher {
    /// Dispatcher Pod name.
    pub pod_name: String,
    /// API-assigned dispatcher Pod UID.
    #[serde(rename = "podUID")]
    pub pod_uid: String,
    /// Orchestrator Lease name.
    pub lease_name: String,
    /// API-assigned orchestrator Lease UID.
    #[serde(rename = "leaseUID")]
    pub lease_uid: String,
    /// Opaque orchestrator Lease resource version.
    pub lease_resource_version: String,
    /// Canonical non-empty orchestrator Lease holder.
    pub lease_holder: String,
}

/// Candidate document selected for the one-shot request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationCandidate {
    /// Candidate `ConfigMap` name.
    pub name: String,
    /// API-assigned candidate `ConfigMap` UID.
    pub uid: String,
    /// Opaque candidate `ConfigMap` resource version.
    pub resource_version: String,
    /// Lowercase SHA-256 digest of the complete candidate payload.
    #[serde(rename = "payloadSHA256")]
    pub payload_sha256: String,
}

/// Exact bootstrap Secret and PVC selected for the target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationBootstrap {
    /// Immutable bootstrap credential Secret.
    pub secret: KubernetesObjectIdentity,
    /// Exact target data PVC.
    pub pvc: KubernetesObjectIdentity,
}

/// Writable-term Lease identity and canonical term generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationWritableTerm {
    /// Writable Lease name.
    pub name: String,
    /// API-assigned writable Lease UID.
    pub uid: String,
    /// Opaque writable Lease resource version.
    pub resource_version: String,
    /// Canonical non-empty holder identity.
    pub holder: String,
    /// Canonical unsigned-decimal holder generation.
    pub generation: String,
}

/// Digests and API identities sealed into one materialization bundle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationMaterials {
    /// Physical-replication credential identity.
    pub replication: MaterialIdentity,
    /// Catalog client and server material identity.
    pub catalog: CatalogMaterialIdentity,
    /// Operation-writer material identity.
    pub operation_writer: MaterialIdentity,
    /// `PostgreSQL` configuration `ConfigMap` identity.
    pub postgresql_configuration: MaterialIdentity,
    /// Lowercase SHA-256 digest of the exact shardschema migration.
    #[serde(rename = "migrationSHA256")]
    pub migration_sha256: String,
    /// Lowercase SHA-256 digest of database genesis input.
    #[serde(rename = "genesisSHA256")]
    pub genesis_sha256: String,
    /// Lowercase SHA-256 digest of topology preflight input.
    #[serde(rename = "preflightSHA256")]
    pub preflight_sha256: String,
    /// Version of the sealed serving HBA policy.
    #[serde(rename = "servingHBAVersion")]
    pub serving_hba_version: String,
    /// Lowercase SHA-256 digest of the serving HBA policy.
    #[serde(rename = "servingHBASHA256")]
    pub serving_hba_sha256: String,
    /// Lowercase SHA-256 digest of the exact target Pod template.
    #[serde(rename = "targetTemplateSHA256")]
    pub target_template_sha256: String,
}

/// Source target-fence acknowledgement observed by the dispatcher.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationTargetFenceAcknowledgement {
    /// Canonical unsigned-decimal Unix observation time in milliseconds.
    #[serde(rename = "observedAtUnixMS")]
    pub observed_at_unix_ms: String,
    /// Canonical unsigned-decimal suspend-aware deadline in nanoseconds.
    #[serde(rename = "deadlineBoottimeNS")]
    pub deadline_boottime_ns: String,
    /// Canonical unsigned-decimal validity at acknowledgement in milliseconds.
    #[serde(rename = "remainingValidityAtAckMS")]
    pub remaining_validity_at_ack_ms: String,
    /// Canonical unsigned-decimal validity at report in milliseconds.
    #[serde(rename = "remainingValidityAtReportMS")]
    pub remaining_validity_at_report_ms: String,
    /// `PostgreSQL` control backend PID that produced the acknowledgement.
    #[serde(rename = "controlBackendPID")]
    pub control_backend_pid: u32,
}

/// Exact shard-zero source incarnation and generation barrier.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationSource {
    /// Cluster name carried by the source proof.
    pub cluster_name: String,
    /// Cluster UID carried by the source proof.
    #[serde(rename = "clusterUID")]
    pub cluster_uid: String,
    /// Source Pod name.
    pub pod_name: String,
    /// API-assigned source Pod UID.
    #[serde(rename = "podUID")]
    pub pod_uid: String,
    /// Physical shard ordinal.
    pub shard: u32,
    /// Physical member ordinal.
    pub member: u32,
    /// Stable source instance identifier.
    #[serde(rename = "instanceID")]
    pub instance_id: String,
    /// Host boot identifier.
    #[serde(rename = "bootID")]
    pub boot_id: String,
    /// `PostgreSQL` postmaster PID.
    #[serde(rename = "postmasterPID")]
    pub postmaster_pid: u32,
    /// Canonical unsigned-decimal `PostgreSQL` system identifier.
    pub system_identifier: String,
    /// `PostgreSQL` timeline.
    pub timeline: u32,
    /// Canonical generation identity validated by every participant.
    pub generation_identity: String,
    /// Canonical unsigned-decimal generation-barrier LSN.
    #[serde(rename = "generationBarrierLSN")]
    pub generation_barrier_lsn: String,
    /// Exact source target-fence acknowledgement.
    pub target_fence_acknowledgement: CatalogActivationTargetFenceAcknowledgement,
}

/// Complete remote-apply witness for one standby incarnation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationRemoteApplyWitness {
    /// Cluster name carried by the witness proof.
    pub cluster_name: String,
    /// Cluster UID carried by the witness proof.
    #[serde(rename = "clusterUID")]
    pub cluster_uid: String,
    /// Witness Pod name.
    pub pod_name: String,
    /// API-assigned witness Pod UID.
    #[serde(rename = "podUID")]
    pub pod_uid: String,
    /// Physical shard ordinal.
    pub shard: u32,
    /// Physical member ordinal.
    pub member: u32,
    /// Stable witness instance identifier.
    #[serde(rename = "instanceID")]
    pub instance_id: String,
    /// Host boot identifier.
    #[serde(rename = "bootID")]
    pub boot_id: String,
    /// `PostgreSQL` postmaster PID.
    #[serde(rename = "postmasterPID")]
    pub postmaster_pid: u32,
    /// Primary-side physical slot name for this witness.
    pub member_slot_name: String,
    /// Canonical unsigned-decimal `PostgreSQL` system identifier.
    pub system_identifier: String,
    /// `PostgreSQL` timeline.
    pub timeline: u32,
    /// Canonical generation identity validated by every participant.
    pub generation_identity: String,
    /// Canonical unsigned-decimal generation-barrier LSN.
    #[serde(rename = "generationBarrierLSN")]
    pub generation_barrier_lsn: String,
    /// Canonical unsigned-decimal remote receive LSN.
    #[serde(rename = "receiveLSN")]
    pub receive_lsn: String,
    /// Canonical unsigned-decimal remote replay LSN.
    #[serde(rename = "replayLSN")]
    pub replay_lsn: String,
}

/// One immutable, fully bound catalog-activation request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogActivationRequest {
    /// Exact schema and digest version.
    pub schema_version: String,
    /// Fixed carrier object identity.
    pub carrier: KubernetesObjectIdentity,
    /// Exact cluster status snapshot.
    pub cluster: CatalogActivationCluster,
    /// Exact publisher and leadership identity.
    pub dispatcher: CatalogActivationDispatcher,
    /// Exact selected candidate document.
    pub candidate: CatalogActivationCandidate,
    /// Exact target bootstrap identities.
    pub bootstrap: CatalogActivationBootstrap,
    /// Exact writable-term Lease and generation.
    pub writable_term: CatalogActivationWritableTerm,
    /// Exact immutable material bundle.
    pub materials: CatalogActivationMaterials,
    /// Exact source evidence.
    pub source: CatalogActivationSource,
    /// Complete remote-apply witness.
    pub remote_apply_witness: CatalogActivationRemoteApplyWitness,
}

/// Validation failure for a canonical catalog-activation request.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CatalogActivationRequestError {
    /// The schema version does not select the supported encoding.
    #[error("unsupported catalog activation request schema version")]
    UnsupportedVersion,
    /// A bounded text field is empty, too long, or contains unsafe bytes.
    #[error("catalog activation request contains an invalid bounded text field")]
    InvalidText,
    /// A Kubernetes UID is not bounded safe ASCII.
    #[error("catalog activation request contains an invalid Kubernetes UID")]
    InvalidUid,
    /// A SHA-256 field is not canonical lowercase hexadecimal.
    #[error("catalog activation request contains an invalid SHA-256 digest")]
    InvalidDigest,
    /// A 64-bit value is not canonical unsigned decimal.
    #[error("catalog activation request contains an invalid canonical decimal")]
    InvalidDecimal,
    /// A role or topology cross-binding is inconsistent.
    #[error("catalog activation request contains inconsistent topology bindings")]
    InconsistentBinding,
}

impl CatalogActivationRequest {
    /// Validates every bounded field and cross-binding before hashing or use.
    ///
    /// # Errors
    ///
    /// Returns a typed error for a non-canonical or inconsistent request.
    #[allow(clippy::too_many_lines)] // One pass makes the complete digest contract auditable.
    pub fn validate(&self) -> Result<(), CatalogActivationRequestError> {
        if self.schema_version != CATALOG_ACTIVATION_REQUEST_VERSION {
            return Err(CatalogActivationRequestError::UnsupportedVersion);
        }
        for value in [
            &self.carrier.name,
            &self.cluster.name,
            &self.cluster.namespace,
            &self.dispatcher.pod_name,
            &self.dispatcher.lease_name,
            &self.dispatcher.lease_holder,
            &self.candidate.name,
            &self.bootstrap.secret.name,
            &self.bootstrap.pvc.name,
            &self.writable_term.name,
            &self.writable_term.holder,
            &self.materials.replication.name,
            &self.materials.catalog.name,
            &self.materials.operation_writer.name,
            &self.materials.postgresql_configuration.name,
            &self.materials.serving_hba_version,
            &self.source.pod_name,
            &self.source.cluster_name,
            &self.source.instance_id,
            &self.source.boot_id,
            &self.remote_apply_witness.pod_name,
            &self.remote_apply_witness.cluster_name,
            &self.remote_apply_witness.instance_id,
            &self.remote_apply_witness.boot_id,
            &self.remote_apply_witness.member_slot_name,
        ] {
            validate_text(value, 253)?;
        }
        for value in [
            &self.cluster.resource_version,
            &self.dispatcher.lease_resource_version,
            &self.candidate.resource_version,
            &self.writable_term.resource_version,
        ] {
            validate_text(value, 256)?;
        }
        for value in [
            &self.carrier.uid,
            &self.cluster.uid,
            &self.dispatcher.pod_uid,
            &self.dispatcher.lease_uid,
            &self.candidate.uid,
            &self.bootstrap.secret.uid,
            &self.bootstrap.pvc.uid,
            &self.writable_term.uid,
            &self.materials.replication.uid,
            &self.materials.catalog.uid,
            &self.materials.operation_writer.uid,
            &self.materials.postgresql_configuration.uid,
            &self.source.pod_uid,
            &self.source.cluster_uid,
            &self.remote_apply_witness.pod_uid,
            &self.remote_apply_witness.cluster_uid,
        ] {
            validate_uid(value)?;
        }
        for value in [
            &self.cluster.status_sha256,
            &self.candidate.payload_sha256,
            &self.materials.replication.material_sha256,
            &self.materials.catalog.client_sha256,
            &self.materials.catalog.server_sha256,
            &self.materials.operation_writer.material_sha256,
            &self.materials.postgresql_configuration.material_sha256,
            &self.materials.migration_sha256,
            &self.materials.genesis_sha256,
            &self.materials.preflight_sha256,
            &self.materials.serving_hba_sha256,
            &self.materials.target_template_sha256,
        ] {
            validate_digest(value)?;
        }
        for value in [
            &self.cluster.generation,
            &self.writable_term.generation,
            &self.source.system_identifier,
            &self.source.generation_barrier_lsn,
            &self.source.target_fence_acknowledgement.observed_at_unix_ms,
            &self
                .source
                .target_fence_acknowledgement
                .deadline_boottime_ns,
            &self
                .source
                .target_fence_acknowledgement
                .remaining_validity_at_ack_ms,
            &self
                .source
                .target_fence_acknowledgement
                .remaining_validity_at_report_ms,
            &self.remote_apply_witness.system_identifier,
            &self.remote_apply_witness.generation_barrier_lsn,
            &self.remote_apply_witness.receive_lsn,
            &self.remote_apply_witness.replay_lsn,
        ] {
            validate_decimal(value)?;
        }
        let writable_term = self
            .writable_term
            .generation
            .parse::<u64>()
            .map_err(|_| CatalogActivationRequestError::InvalidDecimal)?;
        let expected_generation = DurableWritableGeneration::new(
            self.cluster.name.clone(),
            self.cluster.uid.clone(),
            ShardId(self.source.shard),
            self.cluster.namespace.clone(),
            self.writable_term.name.clone(),
            self.writable_term.uid.clone(),
            self.writable_term.holder.clone(),
            writable_term,
        )
        .map_err(|_| CatalogActivationRequestError::InconsistentBinding)?;
        let expected_generation_identity = expected_generation.canonical_bytes();
        let system_identifier = self
            .source
            .system_identifier
            .parse::<u64>()
            .map_err(|_| CatalogActivationRequestError::InvalidDecimal)?;
        let generation_barrier_lsn = self
            .source
            .generation_barrier_lsn
            .parse::<u64>()
            .map_err(|_| CatalogActivationRequestError::InvalidDecimal)?;
        let receive_lsn = self
            .remote_apply_witness
            .receive_lsn
            .parse::<u64>()
            .map_err(|_| CatalogActivationRequestError::InvalidDecimal)?;
        let replay_lsn = self
            .remote_apply_witness
            .replay_lsn
            .parse::<u64>()
            .map_err(|_| CatalogActivationRequestError::InvalidDecimal)?;
        let fence_observed_at = self
            .source
            .target_fence_acknowledgement
            .observed_at_unix_ms
            .parse::<u64>()
            .map_err(|_| CatalogActivationRequestError::InvalidDecimal)?;
        let fence_deadline = self
            .source
            .target_fence_acknowledgement
            .deadline_boottime_ns
            .parse::<u64>()
            .map_err(|_| CatalogActivationRequestError::InvalidDecimal)?;
        let fence_remaining_at_ack = self
            .source
            .target_fence_acknowledgement
            .remaining_validity_at_ack_ms
            .parse::<u64>()
            .map_err(|_| CatalogActivationRequestError::InvalidDecimal)?;
        let fence_remaining_at_report = self
            .source
            .target_fence_acknowledgement
            .remaining_validity_at_report_ms
            .parse::<u64>()
            .map_err(|_| CatalogActivationRequestError::InvalidDecimal)?;
        if self.carrier.name != format!("{}-catalog-activation", self.cluster.name)
            || self.dispatcher.lease_name != format!("{}-orch-lease", self.cluster.name)
            || !dispatcher_holder_belongs_to_pod(&self.dispatcher.lease_holder, &self.dispatcher)
            || self.writable_term.name != postgresql_writable_lease_name(&self.cluster.name, 0)
            || self.source.cluster_name != self.cluster.name
            || self.source.cluster_uid != self.cluster.uid
            || self.remote_apply_witness.cluster_name != self.cluster.name
            || self.remote_apply_witness.cluster_uid != self.cluster.uid
            || self.source.shard != 0
            || self.remote_apply_witness.shard != 0
            || self.source.member > 4
            || self.remote_apply_witness.member > 4
            || self.source.member == self.remote_apply_witness.member
            || self.source.postmaster_pid == 0
            || self.remote_apply_witness.postmaster_pid == 0
            || self.source.target_fence_acknowledgement.control_backend_pid == 0
            || self.source.timeline == 0
            || self.remote_apply_witness.timeline == 0
            || self.source.system_identifier != self.remote_apply_witness.system_identifier
            || self.source.timeline != self.remote_apply_witness.timeline
            || self.source.generation_identity.as_bytes() != expected_generation_identity.as_slice()
            || self.remote_apply_witness.generation_identity.as_bytes()
                != expected_generation_identity.as_slice()
            || !writable_holder_belongs_to_source(&self.writable_term.holder, &self.source)
            || self.source.generation_barrier_lsn
                != self.remote_apply_witness.generation_barrier_lsn
            || system_identifier == 0
            || generation_barrier_lsn == 0
            || receive_lsn < generation_barrier_lsn
            || replay_lsn < generation_barrier_lsn
            || receive_lsn < replay_lsn
            || fence_observed_at == 0
            || fence_deadline == 0
            || fence_remaining_at_ack == 0
            || fence_remaining_at_report == 0
            || fence_remaining_at_report > fence_remaining_at_ack
        {
            return Err(CatalogActivationRequestError::InconsistentBinding);
        }
        Ok(())
    }

    /// Returns the lowercase SHA-256 digest of the validated fixed-order,
    /// length-framed contract.
    ///
    /// # Errors
    ///
    /// Returns a validation error rather than hashing a non-canonical request.
    pub fn sha256(&self) -> Result<String, CatalogActivationRequestError> {
        self.validate()?;
        let mut hash = Sha256::new();
        frame(&mut hash, CATALOG_ACTIVATION_REQUEST_DIGEST_DOMAIN);
        self.for_each_component(|component| frame(&mut hash, component));
        Ok(lower_hex(&hash.finalize()))
    }

    fn for_each_component(&self, mut visit: impl FnMut(&str)) {
        visit(&self.schema_version);
        visit(&self.carrier.name);
        visit(&self.carrier.uid);
        visit(&self.cluster.name);
        visit(&self.cluster.namespace);
        visit(&self.cluster.uid);
        visit(&self.cluster.generation);
        visit(&self.cluster.resource_version);
        visit(&self.cluster.status_sha256);
        visit(&self.dispatcher.pod_name);
        visit(&self.dispatcher.pod_uid);
        visit(&self.dispatcher.lease_name);
        visit(&self.dispatcher.lease_uid);
        visit(&self.dispatcher.lease_resource_version);
        visit(&self.dispatcher.lease_holder);
        visit(&self.candidate.name);
        visit(&self.candidate.uid);
        visit(&self.candidate.resource_version);
        visit(&self.candidate.payload_sha256);
        object_identity_components(&self.bootstrap.secret, &mut visit);
        object_identity_components(&self.bootstrap.pvc, &mut visit);
        visit(&self.writable_term.name);
        visit(&self.writable_term.uid);
        visit(&self.writable_term.resource_version);
        visit(&self.writable_term.holder);
        visit(&self.writable_term.generation);
        material_components(&self.materials.replication, &mut visit);
        visit(&self.materials.catalog.name);
        visit(&self.materials.catalog.uid);
        visit(&self.materials.catalog.client_sha256);
        visit(&self.materials.catalog.server_sha256);
        material_components(&self.materials.operation_writer, &mut visit);
        material_components(&self.materials.postgresql_configuration, &mut visit);
        visit(&self.materials.migration_sha256);
        visit(&self.materials.genesis_sha256);
        visit(&self.materials.preflight_sha256);
        visit(&self.materials.serving_hba_version);
        visit(&self.materials.serving_hba_sha256);
        visit(&self.materials.target_template_sha256);
        visit(&self.source.cluster_name);
        visit(&self.source.cluster_uid);
        visit(&self.source.pod_name);
        visit(&self.source.pod_uid);
        visit_u32(self.source.shard, &mut visit);
        visit_u32(self.source.member, &mut visit);
        visit(&self.source.instance_id);
        visit(&self.source.boot_id);
        visit_u32(self.source.postmaster_pid, &mut visit);
        visit(&self.source.system_identifier);
        visit_u32(self.source.timeline, &mut visit);
        visit(&self.source.generation_identity);
        visit(&self.source.generation_barrier_lsn);
        let acknowledgement = &self.source.target_fence_acknowledgement;
        visit(&acknowledgement.observed_at_unix_ms);
        visit(&acknowledgement.deadline_boottime_ns);
        visit(&acknowledgement.remaining_validity_at_ack_ms);
        visit(&acknowledgement.remaining_validity_at_report_ms);
        visit_u32(acknowledgement.control_backend_pid, &mut visit);
        let witness = &self.remote_apply_witness;
        visit(&witness.cluster_name);
        visit(&witness.cluster_uid);
        visit(&witness.pod_name);
        visit(&witness.pod_uid);
        visit_u32(witness.shard, &mut visit);
        visit_u32(witness.member, &mut visit);
        visit(&witness.instance_id);
        visit(&witness.boot_id);
        visit_u32(witness.postmaster_pid, &mut visit);
        visit(&witness.member_slot_name);
        visit(&witness.system_identifier);
        visit_u32(witness.timeline, &mut visit);
        visit(&witness.generation_identity);
        visit(&witness.generation_barrier_lsn);
        visit(&witness.receive_lsn);
        visit(&witness.replay_lsn);
    }
}

fn object_identity_components(identity: &KubernetesObjectIdentity, visit: &mut impl FnMut(&str)) {
    visit(&identity.name);
    visit(&identity.uid);
}

fn material_components(identity: &MaterialIdentity, visit: &mut impl FnMut(&str)) {
    visit(&identity.name);
    visit(&identity.uid);
    visit(&identity.material_sha256);
}

fn visit_u32(value: u32, visit: &mut impl FnMut(&str)) {
    let decimal = value.to_string();
    visit(&decimal);
}

fn frame(hash: &mut Sha256, value: &str) {
    hash.update(
        u64::try_from(value.len())
            .expect("bounded activation component length fits u64")
            .to_be_bytes(),
    );
    hash.update(value.as_bytes());
}

fn validate_text(value: &str, maximum: usize) -> Result<(), CatalogActivationRequestError> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !byte.is_ascii_whitespace())
    {
        return Err(CatalogActivationRequestError::InvalidText);
    }
    Ok(())
}

fn validate_uid(value: &str) -> Result<(), CatalogActivationRequestError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(CatalogActivationRequestError::InvalidUid);
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), CatalogActivationRequestError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CatalogActivationRequestError::InvalidDigest);
    }
    Ok(())
}

fn validate_decimal(value: &str) -> Result<(), CatalogActivationRequestError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || value.parse::<u64>().is_err()
    {
        return Err(CatalogActivationRequestError::InvalidDecimal);
    }
    Ok(())
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE_HOLDER: &str =
        "demo-shard-0000-member-0000-0/source-pod-uid/0123456789abcdef01234567";
    const DISPATCHER_HOLDER: &str =
        "demo-orchestrator-0/dispatcher-uid/11111111-2222-4333-8444-555555555555";

    fn digest(value: u8) -> String {
        format!("{value:02x}").repeat(32)
    }

    fn generation_identity_for_holder(
        cluster_name: &str,
        lease_name: &str,
        holder: &str,
    ) -> String {
        String::from_utf8(
            DurableWritableGeneration::new(
                cluster_name.into(),
                "cluster-uid".into(),
                ShardId(0),
                "database".into(),
                lease_name.into(),
                "writable-lease-uid".into(),
                holder.into(),
                9,
            )
            .expect("valid generation")
            .canonical_bytes(),
        )
        .expect("canonical generation is UTF-8")
    }

    fn generation_identity_for(cluster_name: &str, lease_name: &str) -> String {
        generation_identity_for_holder(cluster_name, lease_name, SOURCE_HOLDER)
    }

    fn generation_identity() -> String {
        generation_identity_for("demo", "demo-shard-0000-term")
    }

    #[allow(clippy::too_many_lines)] // A complete fixture proves every request component.
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
                pod_name: "demo-shard-0000-member-0000-0".into(),
                pod_uid: "source-pod-uid".into(),
                shard: 0,
                member: 0,
                instance_id: "demo-shard-0000-member-0000-0".into(),
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
                pod_name: "demo-shard-0000-member-0001-0".into(),
                pod_uid: "witness-pod-uid".into(),
                shard: 0,
                member: 1,
                instance_id: "demo-shard-0000-member-0001-0".into(),
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

    fn request_for_cluster(cluster_name: &str) -> CatalogActivationRequest {
        let mut request = request();
        let lease_name = postgresql_writable_lease_name(cluster_name, 0);
        request.carrier.name = format!("{cluster_name}-catalog-activation");
        request.cluster.name = cluster_name.into();
        request.dispatcher.lease_name = format!("{cluster_name}-orch-lease");
        request.writable_term.name.clone_from(&lease_name);
        request.source.cluster_name = cluster_name.into();
        request.remote_apply_witness.cluster_name = cluster_name.into();
        request.source.generation_identity = generation_identity_for(cluster_name, &lease_name);
        request
            .remote_apply_witness
            .generation_identity
            .clone_from(&request.source.generation_identity);
        request
    }

    #[test]
    fn digest_is_fixed_order_and_sensitive_to_every_binding() {
        let request = request();
        let digest = request.sha256().expect("valid request");
        assert_eq!(
            digest,
            "2272dfe2f91126128f51746efed94637f326ea31fa8e83f1dff0e90be5d2f3aa"
        );

        let mut changed = request.clone();
        changed.remote_apply_witness.receive_lsn = "4294967397".into();
        changed.remote_apply_witness.replay_lsn = "4294967397".into();
        assert_ne!(changed.sha256().expect("valid changed request"), digest);

        let long_request = request_for_cluster(&"a".repeat(50));
        assert_eq!(
            long_request.sha256().expect("valid long-name request"),
            "3c747fc699f1711f61e5f2be413de395a1eb3e081bc7b1eaa25455eb2a881809"
        );
    }

    #[test]
    fn writable_lease_name_matches_workload_boundaries() {
        assert_eq!(
            postgresql_writable_lease_name(&"a".repeat(41), 0),
            format!("{}-shard-0000-term", "a".repeat(41))
        );
        assert_eq!(
            postgresql_writable_lease_name(&"a".repeat(42), 0),
            "aaaaaaaaaaaaaaaaa-7a538607fdaab9296995929f-shard-0000-term"
        );
        assert_eq!(
            postgresql_writable_lease_name(&"a".repeat(50), 0),
            "aaaaaaaaaaaaaaaaa-160b4e433e384e05e537dc59-shard-0000-term"
        );
    }

    #[test]
    fn member_pod_name_matches_operator_topology() {
        assert_eq!(
            postgresql_member_pod_name("demo", 0, 0),
            "demo-shard-0000-0"
        );
        assert_eq!(
            postgresql_member_pod_name("demo", 0, 1),
            "demo-shard-0000-m0001-0"
        );
        assert_eq!(
            postgresql_member_pod_name(&"a".repeat(42), 0, 0),
            "aaaaaaaaaaaaaaaaa-7a538607fdaab9296995929f-shard-0000-0"
        );
    }

    #[test]
    fn rejects_noncanonical_decimals_and_cross_binding() {
        let mut invalid = request();
        invalid.source.system_identifier = "0123".into();
        assert_eq!(
            invalid.sha256(),
            Err(CatalogActivationRequestError::InvalidDecimal)
        );

        let mut inconsistent = request();
        inconsistent.remote_apply_witness.generation_barrier_lsn = "4294967297".into();
        assert_eq!(
            inconsistent.sha256(),
            Err(CatalogActivationRequestError::InconsistentBinding)
        );

        let mut foreign_generation = request();
        foreign_generation.source.generation_identity = foreign_generation
            .source
            .generation_identity
            .replace("lease_namespace=database", "lease_namespace=other");
        foreign_generation.remote_apply_witness.generation_identity =
            foreign_generation.source.generation_identity.clone();
        assert_eq!(
            foreign_generation.sha256(),
            Err(CatalogActivationRequestError::InconsistentBinding)
        );

        let mut lagging_witness = request();
        lagging_witness.remote_apply_witness.replay_lsn = "4294967295".into();
        assert_eq!(
            lagging_witness.sha256(),
            Err(CatalogActivationRequestError::InconsistentBinding)
        );

        let mut foreign_holder = request();
        foreign_holder.writable_term.holder =
            "demo-shard-0000-member-0000-0/witness-pod-uid/0123456789abcdef01234567".into();
        foreign_holder.source.generation_identity = generation_identity_for_holder(
            "demo",
            &foreign_holder.writable_term.name,
            &foreign_holder.writable_term.holder,
        );
        foreign_holder
            .remote_apply_witness
            .generation_identity
            .clone_from(&foreign_holder.source.generation_identity);
        assert_eq!(
            foreign_holder.sha256(),
            Err(CatalogActivationRequestError::InconsistentBinding)
        );

        let mut malformed_holder = request();
        malformed_holder.writable_term.holder =
            "demo-shard-0000-member-0000-0/source-pod-uid/ABCDEFABCDEFABCDEFABCDEF".into();
        malformed_holder.source.generation_identity = generation_identity_for_holder(
            "demo",
            &malformed_holder.writable_term.name,
            &malformed_holder.writable_term.holder,
        );
        malformed_holder
            .remote_apply_witness
            .generation_identity
            .clone_from(&malformed_holder.source.generation_identity);
        assert_eq!(
            malformed_holder.sha256(),
            Err(CatalogActivationRequestError::InconsistentBinding)
        );

        let mut foreign_dispatcher = request();
        foreign_dispatcher.dispatcher.lease_holder =
            "demo-orchestrator-1/other-dispatcher-uid/11111111-2222-4333-8444-555555555555".into();
        assert_eq!(
            foreign_dispatcher.sha256(),
            Err(CatalogActivationRequestError::InconsistentBinding)
        );

        let mut malformed_dispatcher = request();
        malformed_dispatcher.dispatcher.lease_holder =
            "demo-orchestrator-0/dispatcher-uid/11111111-2222-1333-8444-555555555555".into();
        assert_eq!(
            malformed_dispatcher.sha256(),
            Err(CatalogActivationRequestError::InconsistentBinding)
        );
    }
}
