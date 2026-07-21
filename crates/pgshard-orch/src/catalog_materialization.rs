//! Private, freshness-scoped proof correlation for future catalog materialization.
//!
//! This module performs no I/O and grants no serving, routing, promotion, or
//! writable authority. The capability is an in-process revalidation token; no
//! current runtime path consumes it.

use crate::agent_status::{
    RemoteApplyWitnessProof, ReplicationProofMemberIdentity, ShardZeroReplicationProof,
    ShardZeroSourceReplicationProof, ShardZeroStandbyReplicationProof, WritableLeaseProofIdentity,
};
use crate::boottime::SuspendAwareInstant;
use crate::catalog_candidate::{
    BootstrapReference, BoundCandidateSet, CandidateFingerprint, CatalogAccessReference,
    ClusterFingerprint, MaterialReference, MaterializationBundle, ObjectReference,
};
use pgshard_types::catalog_activation::{
    CATALOG_ACTIVATION_REQUEST_VERSION, CatalogActivationBootstrap, CatalogActivationCandidate,
    CatalogActivationCluster, CatalogActivationDispatcher, CatalogActivationMaterials,
    CatalogActivationRemoteApplyWitness, CatalogActivationRequest, CatalogActivationRequestError,
    CatalogActivationSource, CatalogActivationTargetFenceAcknowledgement,
    CatalogActivationWritableTerm, CatalogMaterialIdentity, KubernetesObjectIdentity,
    MaterialIdentity,
};
use thiserror::Error;

const CATALOG_ACTIVATION_API_GROUP: &str = "pgshard.io";
const CATALOG_ACTIVATION_API_VERSION: &str = "v1alpha1";
const CATALOG_ACTIVATION_API_PLURAL: &str = "pgshardcatalogactivations";

/// Move-only, non-serializable token for one exact overlap of live evidence.
///
/// Its private fields prevent external construction and keep raw Kubernetes
/// identities out of public diagnostics.
pub(crate) struct CatalogMaterializationCapability {
    coordination_generation: u64,
    agent_generation: u64,
    candidate_generation: u64,
    coordination_lease_uid: String,
    coordination_resource_version: String,
    deadline: SuspendAwareInstant,
}

/// Move-only, non-serializable input envelope for one future catalog bootstrap.
///
/// The envelope owns the capability that admitted it and exact copies of the
/// source target, immutable Kubernetes publications, material references, and
/// remote-apply witness. Its fields intentionally remain module-private so a
/// future I/O path cannot use them without adding an explicit revalidation
/// boundary here.
pub(crate) struct CatalogBootstrapDispatch {
    capability: CatalogMaterializationCapability,
    dispatcher: ConfiguredDispatcherIdentity,
    bound_candidates: BoundCandidateSet,
    cluster: ClusterFingerprint,
    catalog_activation: ObjectReference,
    target_candidate: CandidateFingerprint,
    target_bootstrap: BootstrapReference,
    writable_lease: WritableLeaseProofIdentity,
    replication_credential: MaterialReference,
    catalog_access: CatalogAccessReference,
    operation_writer_access: MaterialReference,
    materialization_bundle: MaterializationBundle,
    source: CatalogBootstrapSource,
    remote_apply_witness: CatalogBootstrapWitness,
}

/// Exact configured publisher identity sealed into one dispatch.
///
/// This value is private, non-serializable, and intentionally has no `Debug`
/// implementation so the downward-API Pod UID cannot enter diagnostics.
struct ConfiguredDispatcherIdentity {
    pod_name: String,
    pod_uid: String,
}

/// Exact objects and agent endpoint a future publisher would have to re-read.
///
/// This value is inert and contains no client, token, request body, or mutation
/// capability. Every identifier is derived from the sealed dispatch. It has no
/// `Debug` implementation so the dispatcher Pod UID cannot enter diagnostics.
#[allow(dead_code)] // Intentionally dormant until a separately reviewed publisher is composed.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CatalogActivationPublicationTarget {
    carrier_api_group: &'static str,
    carrier_api_version: &'static str,
    carrier_api_plural: &'static str,
    carrier_namespace: String,
    carrier_name: String,
    carrier_uid: String,
    cluster_name: String,
    cluster_uid: String,
    dispatcher_pod_name: String,
    dispatcher_pod_uid: String,
    dispatcher_lease_name: String,
    dispatcher_lease_uid: String,
    dispatcher_lease_resource_version: String,
    writable_lease_name: String,
    writable_lease_uid: String,
    writable_lease_resource_version: String,
    writable_lease_holder: String,
    writable_lease_transitions: u64,
    target_stateful_set_name: String,
    target_pod_name: String,
    target_pod_uid: String,
    target_agent_dns_name: String,
}

/// Exact publisher Pod and coordination Lease identity supplied by a future
/// live-object validator. Construction alone grants no mutation authority.
#[allow(dead_code)] // Input contract for a future separately reviewed live validator.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CatalogActivationDispatcherProof {
    pub(crate) pod_name: String,
    pub(crate) pod_uid: String,
    pub(crate) lease_name: String,
    pub(crate) lease_uid: String,
    pub(crate) lease_resource_version: String,
    pub(crate) lease_holder: String,
}

/// Cross-bound copies of already-validated live Kubernetes identities.
///
/// Private fields make this proof constructible only through the exact
/// dispatch comparison below. It remains inert and non-serializable.
#[allow(dead_code)] // Proof boundary for a future separately reviewed publisher.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CatalogActivationLiveObjectProofs {
    cluster: ClusterFingerprint,
    carrier: ObjectReference,
    carrier_resource_version: String,
    target_pod: ObjectReference,
    writable_lease: WritableLeaseProofIdentity,
    dispatcher: CatalogActivationDispatcherProof,
}

#[allow(dead_code)] // A future CAS publisher consumes only this freshly observed version.
impl CatalogActivationLiveObjectProofs {
    pub(crate) fn carrier_resource_version(&self) -> &str {
        &self.carrier_resource_version
    }
}

/// Validated canonical request bytes-by-contract and their canonical digest.
/// This value performs no I/O and carries no authority to publish itself.
#[allow(dead_code)] // Prepared value for a future separately reviewed publisher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedCatalogActivationRequest {
    request: CatalogActivationRequest,
    sha256: String,
}

#[derive(Debug, Error)]
pub(crate) enum CatalogActivationPreparationError {
    #[error("catalog activation live-object proof does not match the sealed dispatch")]
    LiveObjectMismatch,
    #[error(transparent)]
    InvalidRequest(#[from] CatalogActivationRequestError),
}

struct CatalogBootstrapSource {
    cluster_id: String,
    cluster_uid: String,
    shard_id: u32,
    member_ordinal: u32,
    instance_id: String,
    pod_uid: String,
    postmaster_pid: u32,
    boot_id: String,
    system_identifier: u64,
    timeline: u32,
    canonical_generation_identity: String,
    generation_barrier_lsn: u64,
    acknowledgement_observed_at_unix_ms: u64,
    acknowledgement_deadline_boottime_ns: u64,
    acknowledgement_remaining_validity_ms: u64,
    acknowledgement_remaining_validity_at_report_ms: u64,
    acknowledgement_control_backend_pid: u32,
}

struct CatalogBootstrapWitness {
    cluster_id: String,
    cluster_uid: String,
    shard_id: u32,
    member_ordinal: u32,
    instance_id: String,
    pod_uid: String,
    postmaster_pid: u32,
    boot_id: String,
    member_slot_name: String,
    system_identifier: u64,
    timeline: u32,
    canonical_generation_identity: String,
    generation_barrier_lsn: u64,
    receive_lsn: u64,
    replay_lsn: u64,
}

#[allow(dead_code)] // Intentionally dormant until a separately reviewed publisher is composed.
impl CatalogActivationPublicationTarget {
    pub(crate) fn carrier_api_group(&self) -> &'static str {
        self.carrier_api_group
    }

    pub(crate) fn carrier_api_version(&self) -> &'static str {
        self.carrier_api_version
    }

    pub(crate) fn carrier_api_plural(&self) -> &'static str {
        self.carrier_api_plural
    }

    pub(crate) fn carrier_namespace(&self) -> &str {
        &self.carrier_namespace
    }

    pub(crate) fn carrier_name(&self) -> &str {
        &self.carrier_name
    }

    pub(crate) fn carrier_uid(&self) -> &str {
        &self.carrier_uid
    }

    pub(crate) fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub(crate) fn cluster_uid(&self) -> &str {
        &self.cluster_uid
    }

    pub(crate) fn dispatcher_pod_name(&self) -> &str {
        &self.dispatcher_pod_name
    }

    pub(crate) fn dispatcher_pod_uid(&self) -> &str {
        &self.dispatcher_pod_uid
    }

    pub(crate) fn dispatcher_lease_name(&self) -> &str {
        &self.dispatcher_lease_name
    }

    pub(crate) fn dispatcher_lease_uid(&self) -> &str {
        &self.dispatcher_lease_uid
    }

    pub(crate) fn dispatcher_lease_resource_version(&self) -> &str {
        &self.dispatcher_lease_resource_version
    }

    pub(crate) fn writable_lease_name(&self) -> &str {
        &self.writable_lease_name
    }

    pub(crate) fn writable_lease_uid(&self) -> &str {
        &self.writable_lease_uid
    }

    pub(crate) fn writable_lease_resource_version(&self) -> &str {
        &self.writable_lease_resource_version
    }

    pub(crate) fn writable_lease_holder(&self) -> &str {
        &self.writable_lease_holder
    }

    pub(crate) const fn writable_lease_transitions(&self) -> u64 {
        self.writable_lease_transitions
    }

    pub(crate) fn target_stateful_set_name(&self) -> &str {
        &self.target_stateful_set_name
    }

    pub(crate) fn target_pod_name(&self) -> &str {
        &self.target_pod_name
    }

    pub(crate) fn target_pod_uid(&self) -> &str {
        &self.target_pod_uid
    }

    pub(crate) fn target_agent_dns_name(&self) -> &str {
        &self.target_agent_dns_name
    }
}

#[allow(dead_code)] // Intentionally dormant until a separately reviewed publisher is composed.
impl PreparedCatalogActivationRequest {
    pub(crate) fn request(&self) -> &CatalogActivationRequest {
        &self.request
    }

    pub(crate) fn sha256(&self) -> &str {
        &self.sha256
    }

    #[cfg(test)]
    pub(crate) fn from_test_parts(request: CatalogActivationRequest, sha256: String) -> Self {
        Self { request, sha256 }
    }
}

/// Derives the exact API object and direct target-agent endpoint identifiers
/// from a sealed dispatch. It neither resolves DNS nor performs I/O.
#[allow(dead_code)] // Intentionally dormant until a separately reviewed publisher is composed.
pub(crate) fn catalog_activation_publication_target(
    dispatch: &CatalogBootstrapDispatch,
) -> Option<CatalogActivationPublicationTarget> {
    let member_index = usize::try_from(dispatch.source.member_ordinal).ok()?;
    let target_member = dispatch
        .target_candidate
        .document
        .discovery_topology
        .members
        .get(member_index)?;
    if target_member.ordinal != dispatch.source.member_ordinal
        || target_member.instance_id != dispatch.source.instance_id
        || dispatch.target_candidate.document.instance_id != dispatch.source.instance_id
    {
        return None;
    }
    Some(CatalogActivationPublicationTarget {
        carrier_api_group: CATALOG_ACTIVATION_API_GROUP,
        carrier_api_version: CATALOG_ACTIVATION_API_VERSION,
        carrier_api_plural: CATALOG_ACTIVATION_API_PLURAL,
        carrier_namespace: dispatch.cluster.namespace.clone(),
        carrier_name: dispatch.catalog_activation.name.clone(),
        carrier_uid: dispatch.catalog_activation.uid.clone(),
        cluster_name: dispatch.cluster.name.clone(),
        cluster_uid: dispatch.cluster.uid.clone(),
        dispatcher_pod_name: dispatch.dispatcher.pod_name.clone(),
        dispatcher_pod_uid: dispatch.dispatcher.pod_uid.clone(),
        dispatcher_lease_name: format!("{}-orch-lease", dispatch.cluster.name),
        dispatcher_lease_uid: dispatch.capability.coordination_lease_uid.clone(),
        dispatcher_lease_resource_version: dispatch
            .capability
            .coordination_resource_version
            .clone(),
        writable_lease_name: dispatch.writable_lease.name.clone(),
        writable_lease_uid: dispatch.writable_lease.uid.clone(),
        writable_lease_resource_version: dispatch.writable_lease.resource_version.clone(),
        writable_lease_holder: dispatch.writable_lease.holder_identity.clone(),
        writable_lease_transitions: dispatch.writable_lease.transitions,
        target_stateful_set_name: dispatch
            .materialization_bundle
            .target_pod_template
            .stateful_set_name
            .clone(),
        target_pod_name: dispatch.source.instance_id.clone(),
        target_pod_uid: dispatch.source.pod_uid.clone(),
        target_agent_dns_name: target_member.dns_name.clone(),
    })
}

/// Cross-binds already-validated live Kubernetes identities to one sealed
/// dispatch. No observation, request, or mutation occurs here.
#[allow(dead_code)] // Proof boundary for a future separately reviewed publisher.
pub(crate) fn bind_catalog_activation_live_objects(
    dispatch: &CatalogBootstrapDispatch,
    candidates: &BoundCandidateSet,
    carrier: ObjectReference,
    carrier_resource_version: String,
    target_pod: ObjectReference,
    writable_lease: WritableLeaseProofIdentity,
    dispatcher: CatalogActivationDispatcherProof,
) -> Option<CatalogActivationLiveObjectProofs> {
    if !dispatch_matches_candidates(dispatch, candidates)
        || carrier != dispatch.catalog_activation
        || carrier_resource_version.is_empty()
        || carrier_resource_version.len() > 256
        || target_pod.name != dispatch.source.instance_id
        || target_pod.uid != dispatch.source.pod_uid
        || writable_lease != dispatch.writable_lease
        || dispatcher.lease_name != format!("{}-orch-lease", dispatch.cluster.name)
        || dispatcher.lease_uid != dispatch.capability.coordination_lease_uid
        || dispatcher.lease_resource_version != dispatch.capability.coordination_resource_version
        || dispatcher.pod_name != dispatch.dispatcher.pod_name
        || dispatcher.pod_uid != dispatch.dispatcher.pod_uid
        || !dispatcher_holder_matches(
            &dispatcher.lease_holder,
            &dispatch.dispatcher.pod_name,
            &dispatch.dispatcher.pod_uid,
        )
    {
        return None;
    }
    Some(CatalogActivationLiveObjectProofs {
        cluster: candidates.cluster.clone(),
        carrier,
        carrier_resource_version,
        target_pod,
        writable_lease,
        dispatcher,
    })
}

/// Converts one sealed dispatch and exact live-object proof into the existing
/// canonical request contract, validates it, and computes its canonical SHA-256.
/// The function is pure and performs no I/O or publication.
#[allow(dead_code)] // Intentionally dormant until a separately reviewed publisher is composed.
#[allow(clippy::too_many_lines)] // One pass keeps the complete canonical mapping auditable.
pub(crate) fn prepare_catalog_activation_request(
    dispatch: &CatalogBootstrapDispatch,
    live: &CatalogActivationLiveObjectProofs,
) -> Result<PreparedCatalogActivationRequest, CatalogActivationPreparationError> {
    if !live_objects_match_dispatch(dispatch, live) {
        return Err(CatalogActivationPreparationError::LiveObjectMismatch);
    }
    let request = CatalogActivationRequest {
        schema_version: CATALOG_ACTIVATION_REQUEST_VERSION.to_owned(),
        carrier: object_identity(&dispatch.catalog_activation),
        cluster: CatalogActivationCluster {
            name: dispatch.cluster.name.clone(),
            namespace: dispatch.cluster.namespace.clone(),
            uid: dispatch.cluster.uid.clone(),
            generation: dispatch.cluster.generation.to_string(),
            resource_version: dispatch.cluster.resource_version.clone(),
            status_sha256: dispatch.cluster.status_sha256.clone(),
        },
        dispatcher: CatalogActivationDispatcher {
            pod_name: live.dispatcher.pod_name.clone(),
            pod_uid: live.dispatcher.pod_uid.clone(),
            lease_name: live.dispatcher.lease_name.clone(),
            lease_uid: live.dispatcher.lease_uid.clone(),
            lease_resource_version: live.dispatcher.lease_resource_version.clone(),
            lease_holder: live.dispatcher.lease_holder.clone(),
        },
        candidate: CatalogActivationCandidate {
            name: dispatch.target_candidate.name.clone(),
            uid: dispatch.target_candidate.uid.clone(),
            resource_version: dispatch.target_candidate.resource_version.clone(),
            payload_sha256: dispatch.target_candidate.payload_sha256.clone(),
        },
        bootstrap: CatalogActivationBootstrap {
            secret: object_identity(&dispatch.target_bootstrap.secret),
            pvc: object_identity(&dispatch.target_bootstrap.pvc),
        },
        writable_term: CatalogActivationWritableTerm {
            name: dispatch.writable_lease.name.clone(),
            uid: dispatch.writable_lease.uid.clone(),
            resource_version: dispatch.writable_lease.resource_version.clone(),
            holder: dispatch.writable_lease.holder_identity.clone(),
            generation: dispatch.writable_lease.transitions.to_string(),
        },
        materials: CatalogActivationMaterials {
            replication: material_identity(&dispatch.replication_credential),
            catalog: CatalogMaterialIdentity {
                name: dispatch.catalog_access.name.clone(),
                uid: dispatch.catalog_access.uid.clone(),
                client_sha256: dispatch.catalog_access.client_sha256.clone(),
                server_sha256: dispatch.catalog_access.server_sha256.clone(),
            },
            operation_writer: material_identity(&dispatch.operation_writer_access),
            postgresql_configuration: MaterialIdentity {
                name: dispatch
                    .materialization_bundle
                    .postgresql_configuration
                    .name
                    .clone(),
                uid: dispatch
                    .materialization_bundle
                    .postgresql_configuration
                    .uid
                    .clone(),
                material_sha256: dispatch
                    .materialization_bundle
                    .postgresql_configuration
                    .data_sha256
                    .clone(),
            },
            migration_sha256: dispatch
                .materialization_bundle
                .shardschema_migration
                .sha256
                .clone(),
            genesis_sha256: dispatch
                .materialization_bundle
                .database_genesis
                .sha256
                .clone(),
            preflight_sha256: dispatch
                .materialization_bundle
                .database_topology_preflight
                .sha256
                .clone(),
            serving_hba_version: dispatch.materialization_bundle.serving_hba.version.clone(),
            serving_hba_sha256: dispatch.materialization_bundle.serving_hba.sha256.clone(),
            target_template_sha256: dispatch
                .materialization_bundle
                .target_pod_template
                .sha256
                .clone(),
        },
        source: CatalogActivationSource {
            cluster_name: dispatch.source.cluster_id.clone(),
            cluster_uid: dispatch.source.cluster_uid.clone(),
            pod_name: dispatch.source.instance_id.clone(),
            pod_uid: dispatch.source.pod_uid.clone(),
            shard: dispatch.source.shard_id,
            member: dispatch.source.member_ordinal,
            instance_id: dispatch.source.instance_id.clone(),
            boot_id: dispatch.source.boot_id.clone(),
            postmaster_pid: dispatch.source.postmaster_pid,
            system_identifier: dispatch.source.system_identifier.to_string(),
            timeline: dispatch.source.timeline,
            generation_identity: dispatch.source.canonical_generation_identity.clone(),
            generation_barrier_lsn: dispatch.source.generation_barrier_lsn.to_string(),
            target_fence_acknowledgement: CatalogActivationTargetFenceAcknowledgement {
                observed_at_unix_ms: dispatch
                    .source
                    .acknowledgement_observed_at_unix_ms
                    .to_string(),
                deadline_boottime_ns: dispatch
                    .source
                    .acknowledgement_deadline_boottime_ns
                    .to_string(),
                remaining_validity_at_ack_ms: dispatch
                    .source
                    .acknowledgement_remaining_validity_ms
                    .to_string(),
                remaining_validity_at_report_ms: dispatch
                    .source
                    .acknowledgement_remaining_validity_at_report_ms
                    .to_string(),
                control_backend_pid: dispatch.source.acknowledgement_control_backend_pid,
            },
        },
        remote_apply_witness: CatalogActivationRemoteApplyWitness {
            cluster_name: dispatch.remote_apply_witness.cluster_id.clone(),
            cluster_uid: dispatch.remote_apply_witness.cluster_uid.clone(),
            pod_name: dispatch.remote_apply_witness.instance_id.clone(),
            pod_uid: dispatch.remote_apply_witness.pod_uid.clone(),
            shard: dispatch.remote_apply_witness.shard_id,
            member: dispatch.remote_apply_witness.member_ordinal,
            instance_id: dispatch.remote_apply_witness.instance_id.clone(),
            boot_id: dispatch.remote_apply_witness.boot_id.clone(),
            postmaster_pid: dispatch.remote_apply_witness.postmaster_pid,
            member_slot_name: dispatch.remote_apply_witness.member_slot_name.clone(),
            system_identifier: dispatch.remote_apply_witness.system_identifier.to_string(),
            timeline: dispatch.remote_apply_witness.timeline,
            generation_identity: dispatch
                .remote_apply_witness
                .canonical_generation_identity
                .clone(),
            generation_barrier_lsn: dispatch
                .remote_apply_witness
                .generation_barrier_lsn
                .to_string(),
            receive_lsn: dispatch.remote_apply_witness.receive_lsn.to_string(),
            replay_lsn: dispatch.remote_apply_witness.replay_lsn.to_string(),
        },
    };
    request.validate()?;
    let sha256 = request.sha256()?;
    Ok(PreparedCatalogActivationRequest { request, sha256 })
}

fn live_objects_match_dispatch(
    dispatch: &CatalogBootstrapDispatch,
    live: &CatalogActivationLiveObjectProofs,
) -> bool {
    live.cluster == dispatch.cluster
        && live.carrier == dispatch.catalog_activation
        && live.target_pod.name == dispatch.source.instance_id
        && live.target_pod.uid == dispatch.source.pod_uid
        && live.writable_lease == dispatch.writable_lease
        && live.dispatcher.lease_name == format!("{}-orch-lease", dispatch.cluster.name)
        && live.dispatcher.lease_uid == dispatch.capability.coordination_lease_uid
        && live.dispatcher.lease_resource_version
            == dispatch.capability.coordination_resource_version
        && live.dispatcher.pod_name == dispatch.dispatcher.pod_name
        && live.dispatcher.pod_uid == dispatch.dispatcher.pod_uid
        && dispatcher_holder_matches(
            &live.dispatcher.lease_holder,
            &dispatch.dispatcher.pod_name,
            &dispatch.dispatcher.pod_uid,
        )
}

pub(crate) fn dispatcher_holder_matches(holder: &str, pod_name: &str, pod_uid: &str) -> bool {
    let mut pieces = holder.split('/');
    let holder_pod_name = pieces.next().unwrap_or_default();
    let holder_pod_uid = pieces.next().unwrap_or_default();
    let process_incarnation = pieces.next().unwrap_or_default();
    pieces.next().is_none()
        && holder_pod_name == pod_name
        && holder_pod_uid == pod_uid
        && valid_uuid_v4(process_incarnation)
}

// Keep this byte-for-byte contract aligned with the canonical activation
// request validator in pgshard-types: lowercase hexadecimal, UUID version 4,
// and an RFC 4122 variant nibble.
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

fn object_identity(reference: &ObjectReference) -> KubernetesObjectIdentity {
    KubernetesObjectIdentity {
        name: reference.name.clone(),
        uid: reference.uid.clone(),
    }
}

fn material_identity(reference: &MaterialReference) -> MaterialIdentity {
    MaterialIdentity {
        name: reference.name.clone(),
        uid: reference.uid.clone(),
        material_sha256: reference.material_sha256.clone(),
    }
}

impl CatalogMaterializationCapability {
    fn issue(
        coordination_generation: u64,
        agent_generation: u64,
        candidate_generation: u64,
        coordination_lease_uid: String,
        coordination_resource_version: String,
        deadline: SuspendAwareInstant,
    ) -> Self {
        Self {
            coordination_generation,
            agent_generation,
            candidate_generation,
            coordination_lease_uid,
            coordination_resource_version,
            deadline,
        }
    }

    fn matches(
        &self,
        coordination_generation: u64,
        agent_generation: u64,
        candidate_generation: u64,
        coordination_lease_uid: &str,
        coordination_resource_version: &str,
        now: SuspendAwareInstant,
    ) -> bool {
        self.coordination_generation == coordination_generation
            && self.agent_generation == agent_generation
            && self.candidate_generation == candidate_generation
            && self.coordination_lease_uid == coordination_lease_uid
            && self.coordination_resource_version == coordination_resource_version
            && self.deadline.is_live_at(now)
    }
}

/// Issues only from one exact, currently overlapping proof pair and live
/// leadership observation. Raw capability construction remains private.
#[allow(clippy::too_many_arguments)]
pub(crate) fn issue_catalog_materialization_capability(
    replication: &ShardZeroReplicationProof,
    candidates: &BoundCandidateSet,
    expected_cluster_name: &str,
    expected_cluster_object_uid: &str,
    coordination_ready: bool,
    leader: bool,
    coordination_generation: u64,
    agent_generation: u64,
    candidate_generation: u64,
    coordination_lease_uid: &str,
    coordination_resource_version: &str,
    coordination_deadline: SuspendAwareInstant,
    agent_deadline: SuspendAwareInstant,
    candidate_deadline: SuspendAwareInstant,
    now: SuspendAwareInstant,
) -> Option<CatalogMaterializationCapability> {
    let deadline = validated_deadline(
        replication,
        candidates,
        expected_cluster_name,
        expected_cluster_object_uid,
        coordination_ready,
        leader,
        coordination_deadline,
        agent_deadline,
        candidate_deadline,
        now,
    )?;
    if coordination_generation == 0
        || agent_generation == 0
        || candidate_generation == 0
        || coordination_lease_uid.is_empty()
        || coordination_resource_version.is_empty()
    {
        return None;
    }
    Some(CatalogMaterializationCapability::issue(
        coordination_generation,
        agent_generation,
        candidate_generation,
        coordination_lease_uid.to_owned(),
        coordination_resource_version.to_owned(),
        deadline,
    ))
}

/// Revalidates the same proof and leadership gate without extending the
/// capability's original deadline.
#[allow(clippy::too_many_arguments)]
pub(crate) fn revalidate_catalog_materialization_capability(
    capability: &CatalogMaterializationCapability,
    replication: &ShardZeroReplicationProof,
    candidates: &BoundCandidateSet,
    expected_cluster_name: &str,
    expected_cluster_object_uid: &str,
    coordination_ready: bool,
    leader: bool,
    coordination_generation: u64,
    agent_generation: u64,
    candidate_generation: u64,
    coordination_lease_uid: &str,
    coordination_resource_version: &str,
    coordination_deadline: SuspendAwareInstant,
    agent_deadline: SuspendAwareInstant,
    candidate_deadline: SuspendAwareInstant,
    now: SuspendAwareInstant,
) -> bool {
    validated_deadline(
        replication,
        candidates,
        expected_cluster_name,
        expected_cluster_object_uid,
        coordination_ready,
        leader,
        coordination_deadline,
        agent_deadline,
        candidate_deadline,
        now,
    )
    .is_some()
        && capability.matches(
            coordination_generation,
            agent_generation,
            candidate_generation,
            coordination_lease_uid,
            coordination_resource_version,
            now,
        )
}

/// Consumes a currently valid capability and seals all future catalog
/// bootstrap inputs into one opaque dispatch envelope. No I/O occurs here.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_catalog_bootstrap_dispatch(
    capability: CatalogMaterializationCapability,
    dispatcher_pod_name: &str,
    dispatcher_pod_uid: &str,
    replication: &ShardZeroReplicationProof,
    candidates: &BoundCandidateSet,
    expected_cluster_name: &str,
    expected_cluster_object_uid: &str,
    coordination_ready: bool,
    leader: bool,
    coordination_generation: u64,
    agent_generation: u64,
    candidate_generation: u64,
    coordination_lease_uid: &str,
    coordination_resource_version: &str,
    coordination_deadline: SuspendAwareInstant,
    agent_deadline: SuspendAwareInstant,
    candidate_deadline: SuspendAwareInstant,
    now: SuspendAwareInstant,
) -> Option<CatalogBootstrapDispatch> {
    if dispatcher_pod_name.is_empty()
        || dispatcher_pod_name.len() > 253
        || dispatcher_pod_name.contains('/')
        || dispatcher_pod_uid.is_empty()
        || dispatcher_pod_uid.len() > 128
        || dispatcher_pod_uid.contains('/')
    {
        return None;
    }
    if !revalidate_catalog_materialization_capability(
        &capability,
        replication,
        candidates,
        expected_cluster_name,
        expected_cluster_object_uid,
        coordination_ready,
        leader,
        coordination_generation,
        agent_generation,
        candidate_generation,
        coordination_lease_uid,
        coordination_resource_version,
        coordination_deadline,
        agent_deadline,
        candidate_deadline,
        now,
    ) {
        return None;
    }
    let target_candidate = candidates.candidates.first()?.clone();
    let target_bootstrap = candidates.shard_zero_bootstraps.first()?.clone();
    let materialization_bundle = candidates.materialization_bundles.first()?.clone();
    let source = catalog_bootstrap_source(&replication.source);
    let remote_apply_witness = catalog_bootstrap_witness(&replication.remote_apply_witness);
    let dispatch = CatalogBootstrapDispatch {
        capability,
        dispatcher: ConfiguredDispatcherIdentity {
            pod_name: dispatcher_pod_name.to_owned(),
            pod_uid: dispatcher_pod_uid.to_owned(),
        },
        bound_candidates: candidates.clone(),
        cluster: candidates.cluster.clone(),
        catalog_activation: candidates.catalog_activation.clone(),
        target_candidate,
        target_bootstrap,
        writable_lease: replication.writable_lease.clone(),
        replication_credential: candidates.replication_credential.clone(),
        catalog_access: candidates.catalog_access.clone(),
        operation_writer_access: candidates.operation_writer_access.clone(),
        materialization_bundle,
        source,
        remote_apply_witness,
    };
    dispatch_matches_proofs(&dispatch, replication, candidates).then_some(dispatch)
}

/// Rechecks the sealed dispatch against the still-current proof generations,
/// evidence, leadership observation, and original capability deadline.
#[allow(clippy::too_many_arguments)]
pub(crate) fn revalidate_catalog_bootstrap_dispatch(
    dispatch: &CatalogBootstrapDispatch,
    expected_dispatcher_pod_name: &str,
    expected_dispatcher_pod_uid: &str,
    replication: &ShardZeroReplicationProof,
    candidates: &BoundCandidateSet,
    expected_cluster_name: &str,
    expected_cluster_object_uid: &str,
    coordination_ready: bool,
    leader: bool,
    coordination_generation: u64,
    agent_generation: u64,
    candidate_generation: u64,
    coordination_lease_uid: &str,
    coordination_resource_version: &str,
    coordination_deadline: SuspendAwareInstant,
    agent_deadline: SuspendAwareInstant,
    candidate_deadline: SuspendAwareInstant,
    now: SuspendAwareInstant,
) -> bool {
    dispatch.dispatcher.pod_name == expected_dispatcher_pod_name
        && dispatch.dispatcher.pod_uid == expected_dispatcher_pod_uid
        && revalidate_catalog_materialization_capability(
            &dispatch.capability,
            replication,
            candidates,
            expected_cluster_name,
            expected_cluster_object_uid,
            coordination_ready,
            leader,
            coordination_generation,
            agent_generation,
            candidate_generation,
            coordination_lease_uid,
            coordination_resource_version,
            coordination_deadline,
            agent_deadline,
            candidate_deadline,
            now,
        )
        && dispatch_matches_proofs(dispatch, replication, candidates)
}

fn catalog_bootstrap_source(source: &ShardZeroSourceReplicationProof) -> CatalogBootstrapSource {
    CatalogBootstrapSource {
        cluster_id: source.member.cluster_id.clone(),
        cluster_uid: source.member.cluster_uid.clone(),
        shard_id: source.member.shard_id,
        member_ordinal: source.member.member_ordinal,
        instance_id: source.member.instance_id.clone(),
        pod_uid: source.member.pod_uid.clone(),
        postmaster_pid: source.member.postmaster_pid,
        boot_id: source.member.boot_id.clone(),
        system_identifier: source.system_identifier,
        timeline: source.timeline,
        canonical_generation_identity: source.canonical_generation_identity.clone(),
        generation_barrier_lsn: source.generation_barrier_lsn,
        acknowledgement_observed_at_unix_ms: source
            .target_fence_acknowledgement
            .observed_at_unix_ms,
        acknowledgement_deadline_boottime_ns: source
            .target_fence_acknowledgement
            .deadline_boottime_ns,
        acknowledgement_remaining_validity_ms: source
            .target_fence_acknowledgement
            .remaining_validity_at_ack_ms,
        acknowledgement_remaining_validity_at_report_ms: source
            .target_fence_acknowledgement
            .remaining_validity_at_report_ms,
        acknowledgement_control_backend_pid: source
            .target_fence_acknowledgement
            .control_backend_pid,
    }
}

fn catalog_bootstrap_witness(witness: &RemoteApplyWitnessProof) -> CatalogBootstrapWitness {
    CatalogBootstrapWitness {
        cluster_id: witness.member.cluster_id.clone(),
        cluster_uid: witness.member.cluster_uid.clone(),
        shard_id: witness.member.shard_id,
        member_ordinal: witness.member.member_ordinal,
        instance_id: witness.member.instance_id.clone(),
        pod_uid: witness.member.pod_uid.clone(),
        postmaster_pid: witness.member.postmaster_pid,
        boot_id: witness.member.boot_id.clone(),
        member_slot_name: witness.member_slot_name.clone(),
        system_identifier: witness.system_identifier,
        timeline: witness.timeline,
        canonical_generation_identity: witness.canonical_generation_identity.clone(),
        generation_barrier_lsn: witness.generation_barrier_lsn,
        receive_lsn: witness.receive_lsn,
        replay_lsn: witness.replay_lsn,
    }
}

fn dispatch_matches_proofs(
    dispatch: &CatalogBootstrapDispatch,
    replication: &ShardZeroReplicationProof,
    candidates: &BoundCandidateSet,
) -> bool {
    dispatch_matches_candidates(dispatch, candidates)
        && dispatch.writable_lease == replication.writable_lease
        && source_matches(&dispatch.source, &replication.source)
        && witness_matches(
            &dispatch.remote_apply_witness,
            &replication.remote_apply_witness,
        )
}

fn dispatch_matches_candidates(
    dispatch: &CatalogBootstrapDispatch,
    candidates: &BoundCandidateSet,
) -> bool {
    let Some(target_candidate) = candidates.candidates.first() else {
        return false;
    };
    let Some(target_bootstrap) = candidates.shard_zero_bootstraps.first() else {
        return false;
    };
    dispatch.bound_candidates == *candidates
        && dispatch.cluster == candidates.cluster
        && dispatch.catalog_activation == candidates.catalog_activation
        && dispatch.target_candidate == *target_candidate
        && dispatch.target_bootstrap == *target_bootstrap
        && dispatch.replication_credential == candidates.replication_credential
        && dispatch.catalog_access == candidates.catalog_access
        && dispatch.operation_writer_access == candidates.operation_writer_access
        && candidates.materialization_bundles.first() == Some(&dispatch.materialization_bundle)
}

fn source_matches(
    dispatch: &CatalogBootstrapSource,
    source: &ShardZeroSourceReplicationProof,
) -> bool {
    dispatch.cluster_id == source.member.cluster_id
        && dispatch.cluster_uid == source.member.cluster_uid
        && dispatch.shard_id == source.member.shard_id
        && dispatch.member_ordinal == source.member.member_ordinal
        && dispatch.instance_id == source.member.instance_id
        && dispatch.pod_uid == source.member.pod_uid
        && dispatch.postmaster_pid == source.member.postmaster_pid
        && dispatch.boot_id == source.member.boot_id
        && dispatch.system_identifier == source.system_identifier
        && dispatch.timeline == source.timeline
        && dispatch.canonical_generation_identity == source.canonical_generation_identity
        && dispatch.generation_barrier_lsn == source.generation_barrier_lsn
        && dispatch.acknowledgement_observed_at_unix_ms
            == source.target_fence_acknowledgement.observed_at_unix_ms
        && dispatch.acknowledgement_deadline_boottime_ns
            == source.target_fence_acknowledgement.deadline_boottime_ns
        && dispatch.acknowledgement_remaining_validity_ms
            == source
                .target_fence_acknowledgement
                .remaining_validity_at_ack_ms
        && dispatch.acknowledgement_remaining_validity_at_report_ms
            == source
                .target_fence_acknowledgement
                .remaining_validity_at_report_ms
        && dispatch.acknowledgement_control_backend_pid
            == source.target_fence_acknowledgement.control_backend_pid
}

fn witness_matches(dispatch: &CatalogBootstrapWitness, witness: &RemoteApplyWitnessProof) -> bool {
    dispatch.cluster_id == witness.member.cluster_id
        && dispatch.cluster_uid == witness.member.cluster_uid
        && dispatch.shard_id == witness.member.shard_id
        && dispatch.member_ordinal == witness.member.member_ordinal
        && dispatch.instance_id == witness.member.instance_id
        && dispatch.pod_uid == witness.member.pod_uid
        && dispatch.postmaster_pid == witness.member.postmaster_pid
        && dispatch.boot_id == witness.member.boot_id
        && dispatch.member_slot_name == witness.member_slot_name
        && dispatch.system_identifier == witness.system_identifier
        && dispatch.timeline == witness.timeline
        && dispatch.canonical_generation_identity == witness.canonical_generation_identity
        && dispatch.generation_barrier_lsn == witness.generation_barrier_lsn
        && dispatch.receive_lsn == witness.receive_lsn
        && dispatch.replay_lsn == witness.replay_lsn
}

#[allow(clippy::too_many_arguments)]
fn validated_deadline(
    replication: &ShardZeroReplicationProof,
    candidates: &BoundCandidateSet,
    expected_cluster_name: &str,
    expected_cluster_object_uid: &str,
    coordination_ready: bool,
    leader: bool,
    coordination_deadline: SuspendAwareInstant,
    agent_deadline: SuspendAwareInstant,
    candidate_deadline: SuspendAwareInstant,
    now: SuspendAwareInstant,
) -> Option<SuspendAwareInstant> {
    if !coordination_ready
        || !leader
        || replication.source.member.cluster_id != expected_cluster_name
        || replication.source.member.cluster_uid != expected_cluster_object_uid
        || !proofs_cross_bind(replication, candidates)
    {
        return None;
    }
    let deadline = coordination_deadline
        .min(agent_deadline)
        .min(candidate_deadline);
    deadline.is_live_at(now).then_some(deadline)
}

/// Requires the two independently collected proof graphs to describe the same
/// exact shard-zero cluster, member set, Lease incarnation, and replication
/// coordinates.
fn proofs_cross_bind(
    replication: &ShardZeroReplicationProof,
    candidates: &BoundCandidateSet,
) -> bool {
    let member_count = replication.standbys.len().saturating_add(1);
    if !matches!(member_count, 3 | 5)
        || candidates.candidates.len() != member_count
        || candidates.shard_zero_bootstraps.len() != member_count
        || candidates.cluster.uid != replication.source.member.cluster_uid
        || candidates.cluster.name != replication.source.member.cluster_id
        || candidates.cluster.namespace != replication.writable_lease.namespace
        || candidates.writable_lease.name != replication.writable_lease.name
        || candidates.writable_lease.uid != replication.writable_lease.uid
        || replication.writable_lease.resource_version.is_empty()
        || replication.writable_lease.holder_identity.is_empty()
        || replication.writable_lease.transitions == 0
        || replication.source.member.shard_id != 0
        || replication.source.member.member_ordinal != 0
        || replication.source.system_identifier == 0
        || replication.source.timeline == 0
        || replication.source.generation_barrier_lsn == 0
        || replication.source.canonical_generation_identity
            != replication
                .source
                .target_fence_acknowledgement
                .canonical_generation_identity
        || replication.source.member.boot_id
            != replication.source.target_fence_acknowledgement.boot_id
        || replication.source.member.postmaster_pid
            != replication
                .source
                .target_fence_acknowledgement
                .postmaster_pid
    {
        return false;
    }

    let source = &replication.source.member;
    if !candidate_matches_member(candidates, 0, source, None) {
        return false;
    }

    for (index, standby) in replication.standbys.iter().enumerate() {
        let expected_ordinal = u32::try_from(index + 1).unwrap_or(u32::MAX);
        if standby.member.member_ordinal != expected_ordinal
            || standby.member.cluster_id != source.cluster_id
            || standby.member.cluster_uid != source.cluster_uid
            || standby.member.shard_id != 0
            || standby.source_instance_id != source.instance_id
            || standby.system_identifier != replication.source.system_identifier
            || standby.timeline != replication.source.timeline
            || standby.canonical_generation_identity
                != replication.source.canonical_generation_identity
            || standby.generation_barrier_lsn != replication.source.generation_barrier_lsn
            || standby.receive_lsn < standby.replay_lsn
            || !candidate_matches_member(candidates, index + 1, &standby.member, Some(standby))
        {
            return false;
        }
    }

    let witness = &replication.remote_apply_witness;
    replication.standbys.iter().any(|standby| {
        witness.member == standby.member
            && witness.member_slot_name == standby.member_slot_name
            && witness.system_identifier == standby.system_identifier
            && witness.timeline == standby.timeline
            && witness.canonical_generation_identity == standby.canonical_generation_identity
            && witness.generation_barrier_lsn == standby.generation_barrier_lsn
            && witness.receive_lsn == standby.receive_lsn
            && witness.replay_lsn == standby.replay_lsn
            && witness.receive_lsn >= replication.source.generation_barrier_lsn
            && witness.replay_lsn >= replication.source.generation_barrier_lsn
    })
}

fn candidate_matches_member(
    candidates: &BoundCandidateSet,
    index: usize,
    member: &ReplicationProofMemberIdentity,
    standby: Option<&ShardZeroStandbyReplicationProof>,
) -> bool {
    let Some(candidate) = candidates.candidates.get(index) else {
        return false;
    };
    let document = &candidate.document;
    let Some(discovery) = document.discovery_topology.members.get(index) else {
        return false;
    };
    let Some(bootstrap) = candidates.shard_zero_bootstraps.get(index) else {
        return false;
    };
    let Some(materialization_bundle) = candidates.materialization_bundles.get(index) else {
        return false;
    };
    document.cluster_object_uid == member.cluster_uid
        && document.shard == 0
        && document.member == member.member_ordinal
        && document.instance_id == member.instance_id
        && document.bootstrap == *bootstrap
        && document.writable_lease == candidates.writable_lease
        && document.replication_credential == candidates.replication_credential
        && document.catalog_access == candidates.catalog_access
        && document.materialization_bundle == *materialization_bundle
        && document.discovery_topology.members.len() == candidates.candidates.len()
        && discovery.ordinal == member.member_ordinal
        && discovery.instance_id == member.instance_id
        && standby.is_none_or(|standby| discovery.physical_slot == standby.member_slot_name)
}
