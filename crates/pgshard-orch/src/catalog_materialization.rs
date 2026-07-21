//! Private, freshness-scoped proof correlation for future catalog materialization.
//!
//! This module performs no I/O and grants no serving, routing, promotion, or
//! writable authority. The capability is an in-process revalidation token; no
//! current runtime path consumes it.

use crate::agent_status::{
    RemoteApplyWitnessProof, ReplicationProofMemberIdentity, ShardZeroReplicationProof,
    ShardZeroSourceReplicationProof, ShardZeroStandbyReplicationProof,
};
use crate::boottime::SuspendAwareInstant;
use crate::catalog_candidate::{
    BootstrapReference, BoundCandidateSet, CandidateFingerprint, CatalogAccessReference,
    ClusterFingerprint, MaterialReference, ObjectReference,
};

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
    cluster: ClusterFingerprint,
    target_candidate: CandidateFingerprint,
    target_bootstrap: BootstrapReference,
    writable_lease: ObjectReference,
    replication_credential: MaterialReference,
    catalog_access: CatalogAccessReference,
    operation_writer_access: MaterialReference,
    source: CatalogBootstrapSource,
    remote_apply_witness: CatalogBootstrapWitness,
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
    let source = catalog_bootstrap_source(&replication.source);
    let remote_apply_witness = catalog_bootstrap_witness(&replication.remote_apply_witness);
    let dispatch = CatalogBootstrapDispatch {
        capability,
        cluster: candidates.cluster.clone(),
        target_candidate,
        target_bootstrap,
        writable_lease: candidates.writable_lease.clone(),
        replication_credential: candidates.replication_credential.clone(),
        catalog_access: candidates.catalog_access.clone(),
        operation_writer_access: candidates.operation_writer_access.clone(),
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
    revalidate_catalog_materialization_capability(
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
    ) && dispatch_matches_proofs(dispatch, replication, candidates)
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
    let Some(target_candidate) = candidates.candidates.first() else {
        return false;
    };
    let Some(target_bootstrap) = candidates.shard_zero_bootstraps.first() else {
        return false;
    };
    dispatch.cluster == candidates.cluster
        && dispatch.target_candidate == *target_candidate
        && dispatch.target_bootstrap == *target_bootstrap
        && dispatch.writable_lease == candidates.writable_lease
        && dispatch.replication_credential == candidates.replication_credential
        && dispatch.catalog_access == candidates.catalog_access
        && dispatch.operation_writer_access == candidates.operation_writer_access
        && source_matches(&dispatch.source, &replication.source)
        && witness_matches(
            &dispatch.remote_apply_witness,
            &replication.remote_apply_witness,
        )
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
        || candidates.writable_lease.name != replication.writable_lease.name
        || candidates.writable_lease.uid != replication.writable_lease.uid
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
    document.cluster_object_uid == member.cluster_uid
        && document.shard == 0
        && document.member == member.member_ordinal
        && document.instance_id == member.instance_id
        && document.bootstrap == *bootstrap
        && document.writable_lease == candidates.writable_lease
        && document.replication_credential == candidates.replication_credential
        && document.catalog_access == candidates.catalog_access
        && document.discovery_topology.members.len() == candidates.candidates.len()
        && discovery.ordinal == member.member_ordinal
        && discovery.instance_id == member.instance_id
        && standby.is_none_or(|standby| discovery.physical_slot == standby.member_slot_name)
}
