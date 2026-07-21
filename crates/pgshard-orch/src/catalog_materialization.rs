//! Private, freshness-scoped proof correlation for future catalog materialization.
//!
//! This module performs no I/O and grants no serving, routing, promotion, or
//! writable authority. The capability is an in-process revalidation token; no
//! current runtime path consumes it.

use crate::agent_status::{
    ReplicationProofMemberIdentity, ShardZeroReplicationProof, ShardZeroStandbyReplicationProof,
};
use crate::boottime::SuspendAwareInstant;
use crate::catalog_candidate::BoundCandidateSet;

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
