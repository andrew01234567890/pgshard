//! Strict, read-only loading of the operator-published discovery topology.
//!
//! The topology identifies the finite set of expected agent endpoints. It does
//! not report or grant a runtime role, readiness, serving state, promotion
//! permission, or writable authority.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use rustix::fs::{Mode, OFlags};

use crate::endpoint::valid_credential_free_http_endpoint;

/// Exact schema identifier emitted by the operator.
pub const TOPOLOGY_SCHEMA_VERSION: &str = "pgshard.topology.v1";
/// Default path of the projected operator topology document.
pub const DEFAULT_TOPOLOGY_FILE: &str = "/etc/pgshard/topology/cluster.json";
/// Maximum accepted topology payload, including any trailing newline.
pub const MAXIMUM_TOPOLOGY_PAYLOAD_BYTES: usize = 900 * 1_024;

const MAXIMUM_SHARDS: usize = 128;
const MAXIMUM_DATABASES: usize = 512;
const MAXIMUM_TOTAL_ROUTING_RANGES: usize = MAXIMUM_SHARDS * MAXIMUM_DATABASES;
const MAXIMUM_CLUSTER_NAME_BYTES: usize = 50;
const MAXIMUM_UID_BYTES: usize = 128;

/// Identity that the mounted topology must match exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpectedTopologyIdentity<'a> {
    /// Operator-assigned cluster name.
    pub cluster_id: &'a str,
    /// Kubernetes UID of the cluster object.
    pub cluster_uid: &'a str,
    /// Namespace containing the cluster and its writable-term Leases.
    pub namespace: &'a str,
}

/// Validated version-one discovery topology.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TopologyV1 {
    schema_version: String,
    cluster: String,
    #[serde(rename = "clusterObjectUID")]
    cluster_object_uid: String,
    namespace: String,
    durability: Durability,
    members_per_shard: u32,
    listeners: Vec<TopologyListener>,
    shards: Vec<TopologyShard>,
    #[serde(default)]
    databases: Vec<TopologyDatabase>,
    backup: TopologyBackup,
    observability: TopologyObservability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
enum Durability {
    Synchronous,
    Asynchronous,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopologyListener {
    mode: String,
    service: String,
    target_port: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopologyShard {
    id: u32,
    service: String,
    writable_lease: TopologyWritableLease,
    members: Vec<TopologyMember>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopologyWritableLease {
    namespace: String,
    name: String,
    uid: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopologyMember {
    ordinal: u32,
    instance_id: String,
    dns_name: String,
    postgresql_port: u16,
    agent_http_port: u16,
    physical_slot: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct TopologyDatabase {
    name: String,
    shards: u32,
    cells: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
enum BackupType {
    S3,
    Filesystem,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopologyBackup {
    r#type: BackupType,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    bucket: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    endpoint: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    region: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    prefix: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    credentials_secret: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    persistent_volume_claim: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopologyObservability {
    prometheus: bool,
    service_monitor_requested: bool,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    open_telemetry_endpoint: Option<String>,
}

/// Diagnostic summary of one validated topology.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TopologyDiagnostics {
    /// Exact schema accepted by this process.
    pub schema_version: String,
    /// Kubernetes UID of the cluster object named by the document.
    pub cluster_object_uid: String,
    /// Number of canonical physical shards.
    pub shard_count: usize,
    /// Number of canonical member endpoints across all shards.
    pub member_count: usize,
    /// Current state of remote agent-status collection.
    pub agent_status_collection: AgentStatusCollectionState,
}

/// Why remote agent status is or is not being collected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatusCollectionState {
    /// This process was started without the explicit agent-quarantine runtime
    /// contract. No agent endpoint observation is attempted.
    DisabledAgentRuntimeRequired,
    /// Discovery is valid, but the document deliberately lacks runtime Pod
    /// UIDs. Querying agents before another authoritative source supplies
    /// those UIDs could accept status from a replaced Pod incarnation.
    DisabledPodIdentityRequired,
    /// Every expected response was validated inside one Kubernetes identity
    /// bracket and remains inside a process-local freshness window. This is
    /// diagnostic evidence only and grants no runtime authority.
    FreshDiagnosticEvidence,
}

/// One topology-derived endpoint that is not yet bound to a runtime Pod UID.
///
/// Callers must not query or accept status from this endpoint. The type has no
/// public constructor or Pod-UID binding API; a future authoritative observer
/// must define that boundary separately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnboundAgentObservationTarget {
    pub(crate) cluster_id: String,
    pub(crate) cluster_uid: String,
    pub(crate) namespace: String,
    pub(crate) shard_id: u32,
    pub(crate) shard_service: String,
    pub(crate) member_ordinal: u32,
    pub(crate) stateful_set: String,
    pub(crate) instance_id: String,
    pub(crate) dns_name: String,
    pub(crate) agent_http_port: u16,
    pub(crate) postgresql_port: u16,
    pub(crate) physical_slot: String,
    pub(crate) writable_lease_namespace: String,
    pub(crate) writable_lease_name: String,
    pub(crate) writable_lease_uid: String,
    pub(crate) synchronous_durability: bool,
}

/// Exact, role-neutral shard-zero catalog-candidate identities derived from
/// the already validated discovery topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogCandidateObservationPlan {
    pub(crate) cluster_id: String,
    pub(crate) cluster_uid: String,
    pub(crate) namespace: String,
    pub(crate) shard_count: usize,
    pub(crate) synchronous_durability: bool,
    pub(crate) topology_config_map: String,
    pub(crate) writable_lease_name: String,
    pub(crate) writable_lease_uid: String,
    pub(crate) members: Vec<CatalogCandidateTopologyMember>,
}

/// One canonical shard-zero member carried by a candidate document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogCandidateTopologyMember {
    pub(crate) ordinal: u32,
    pub(crate) stateful_set: String,
    pub(crate) instance_id: String,
    pub(crate) dns_name: String,
    pub(crate) postgresql_port: u16,
    pub(crate) agent_http_port: u16,
    pub(crate) physical_slot: String,
}

impl CatalogCandidateTopologyMember {
    pub(crate) fn config_map_name(&self) -> String {
        format!("{}-cfg", self.stateful_set)
    }
}

impl UnboundAgentObservationTarget {
    /// Returns the cluster name expected in agent status.
    #[must_use]
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Returns the cluster-object UID expected in activation evidence.
    #[must_use]
    pub fn cluster_uid(&self) -> &str {
        &self.cluster_uid
    }

    /// Returns the namespace containing the discovered member.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the physical shard ordinal.
    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    /// Returns the deterministic headless Service and Endpoints object name.
    #[must_use]
    pub fn shard_service(&self) -> &str {
        &self.shard_service
    }

    /// Returns the stable member ordinal.
    #[must_use]
    pub const fn member_ordinal(&self) -> u32 {
        self.member_ordinal
    }

    /// Returns the exact controller `StatefulSet` name for this member.
    #[must_use]
    pub fn stateful_set(&self) -> &str {
        &self.stateful_set
    }

    /// Returns the stable member instance name.
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Returns the canonical agent DNS name.
    #[must_use]
    pub fn dns_name(&self) -> &str {
        &self.dns_name
    }

    /// Returns the canonical agent HTTP port.
    #[must_use]
    pub const fn agent_http_port(&self) -> u16 {
        self.agent_http_port
    }

    /// Returns the canonical `PostgreSQL` port.
    #[must_use]
    pub const fn postgresql_port(&self) -> u16 {
        self.postgresql_port
    }

    /// Returns the canonical physical replication slot identity.
    #[must_use]
    pub fn physical_slot(&self) -> &str {
        &self.physical_slot
    }

    /// Returns the exact writable-term Lease namespace.
    #[must_use]
    pub fn writable_lease_namespace(&self) -> &str {
        &self.writable_lease_namespace
    }

    /// Returns the exact writable-term Lease name.
    #[must_use]
    pub fn writable_lease_name(&self) -> &str {
        &self.writable_lease_name
    }

    /// Returns the exact writable-term Lease UID.
    #[must_use]
    pub fn writable_lease_uid(&self) -> &str {
        &self.writable_lease_uid
    }

    /// Returns whether the topology requires remote-apply durability.
    #[must_use]
    pub const fn synchronous_durability(&self) -> bool {
        self.synchronous_durability
    }
}

impl TopologyV1 {
    /// Loads and validates one topology file against its expected cluster
    /// incarnation.
    ///
    /// # Errors
    ///
    /// Returns an error for I/O failure, a non-regular or oversized file,
    /// malformed JSON, unknown fields, identity mismatch, or a noncanonical
    /// topology.
    pub fn load(
        path: impl AsRef<Path>,
        expected: ExpectedTopologyIdentity<'_>,
    ) -> Result<Self, TopologyError> {
        let path = path.as_ref();
        let metadata = fs::metadata(path).map_err(|source| TopologyError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        validate_file_metadata(path, &metadata)?;
        let descriptor = rustix::fs::open(
            path,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOCTTY,
            Mode::empty(),
        )
        .map_err(|source| TopologyError::Read {
            path: path.to_path_buf(),
            source: source.into(),
        })?;
        let file = File::from(descriptor);
        let metadata = file.metadata().map_err(|source| TopologyError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        validate_file_metadata(path, &metadata)?;
        let mut payload = Vec::with_capacity(
            usize::try_from(metadata.len())
                .unwrap_or(MAXIMUM_TOPOLOGY_PAYLOAD_BYTES)
                .min(MAXIMUM_TOPOLOGY_PAYLOAD_BYTES),
        );
        file.take((MAXIMUM_TOPOLOGY_PAYLOAD_BYTES + 1) as u64)
            .read_to_end(&mut payload)
            .map_err(|source| TopologyError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        if payload.len() > MAXIMUM_TOPOLOGY_PAYLOAD_BYTES {
            return Err(TopologyError::PayloadTooLarge {
                maximum: MAXIMUM_TOPOLOGY_PAYLOAD_BYTES,
            });
        }
        Self::parse_and_validate(&payload, expected)
    }

    fn parse_and_validate(
        payload: &[u8],
        expected: ExpectedTopologyIdentity<'_>,
    ) -> Result<Self, TopologyError> {
        let topology: Self = serde_json::from_slice(payload)?;
        topology.validate(expected)?;
        Ok(topology)
    }

    /// Returns a bounded, non-authoritative status summary.
    #[must_use]
    pub fn diagnostics(&self, identity_binding_enabled: bool) -> TopologyDiagnostics {
        TopologyDiagnostics {
            schema_version: self.schema_version.clone(),
            cluster_object_uid: self.cluster_object_uid.clone(),
            shard_count: self.shards.len(),
            member_count: self.shards.iter().map(|shard| shard.members.len()).sum(),
            agent_status_collection: if identity_binding_enabled {
                AgentStatusCollectionState::DisabledPodIdentityRequired
            } else {
                AgentStatusCollectionState::DisabledAgentRuntimeRequired
            },
        }
    }

    /// Returns the finite canonical agent endpoint set.
    ///
    /// The returned targets are deliberately unbound and non-constructible by
    /// external callers. The topology contains no current Pod UID, so no HTTP
    /// request or observation acceptance is authorized by this API.
    #[must_use]
    pub fn agent_observation_targets(&self) -> Vec<UnboundAgentObservationTarget> {
        let mut targets =
            Vec::with_capacity(self.shards.iter().map(|shard| shard.members.len()).sum());
        for shard in &self.shards {
            for member in &shard.members {
                let stateful_set = member
                    .instance_id
                    .strip_suffix("-0")
                    .unwrap_or(&member.instance_id)
                    .to_owned();
                targets.push(UnboundAgentObservationTarget {
                    cluster_id: self.cluster.clone(),
                    cluster_uid: self.cluster_object_uid.clone(),
                    namespace: self.namespace.clone(),
                    shard_id: shard.id,
                    shard_service: shard.service.clone(),
                    member_ordinal: member.ordinal,
                    stateful_set,
                    instance_id: member.instance_id.clone(),
                    dns_name: member.dns_name.clone(),
                    agent_http_port: member.agent_http_port,
                    postgresql_port: member.postgresql_port,
                    physical_slot: member.physical_slot.clone(),
                    writable_lease_namespace: shard.writable_lease.namespace.clone(),
                    writable_lease_name: shard.writable_lease.name.clone(),
                    writable_lease_uid: shard.writable_lease.uid.clone(),
                    synchronous_durability: self.durability == Durability::Synchronous,
                });
            }
        }
        targets
    }

    /// Returns the finite shard-zero candidate set only for the explicit
    /// multi-member topology supported by the operator contract.
    #[must_use]
    pub fn catalog_candidate_observation_plan(&self) -> Option<CatalogCandidateObservationPlan> {
        if !matches!(self.members_per_shard, 3 | 5) {
            return None;
        }
        let shard = self.shards.first()?;
        let members = shard
            .members
            .iter()
            .map(|member| {
                let stateful_set = member.instance_id.strip_suffix("-0")?.to_owned();
                Some(CatalogCandidateTopologyMember {
                    ordinal: member.ordinal,
                    stateful_set,
                    instance_id: member.instance_id.clone(),
                    dns_name: member.dns_name.clone(),
                    postgresql_port: member.postgresql_port,
                    agent_http_port: member.agent_http_port,
                    physical_slot: member.physical_slot.clone(),
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(CatalogCandidateObservationPlan {
            cluster_id: self.cluster.clone(),
            cluster_uid: self.cluster_object_uid.clone(),
            namespace: self.namespace.clone(),
            shard_count: self.shards.len(),
            synchronous_durability: self.durability == Durability::Synchronous,
            topology_config_map: format!("{}-topology", self.cluster),
            writable_lease_name: shard.writable_lease.name.clone(),
            writable_lease_uid: shard.writable_lease.uid.clone(),
            members,
        })
    }

    fn validate(&self, expected: ExpectedTopologyIdentity<'_>) -> Result<(), TopologyError> {
        if self.schema_version != TOPOLOGY_SCHEMA_VERSION {
            return Err(TopologyError::Invalid(
                "unsupported topology schema version",
            ));
        }
        if self.cluster != expected.cluster_id
            || self.cluster_object_uid != expected.cluster_uid
            || self.namespace != expected.namespace
        {
            return Err(TopologyError::Invalid(
                "topology cluster name, object UID, or namespace does not match process identity",
            ));
        }
        if !dns_label(&self.cluster, MAXIMUM_CLUSTER_NAME_BYTES)
            || !dns_label(&self.namespace, 63)
            || validate_uid(&self.cluster_object_uid).is_err()
        {
            return Err(TopologyError::Invalid(
                "topology cluster identity is not canonical",
            ));
        }
        if !matches!(self.members_per_shard, 1 | 3 | 5)
            || (self.durability == Durability::Synchronous && self.members_per_shard < 3)
        {
            return Err(TopologyError::Invalid(
                "topology durability and member count are inconsistent",
            ));
        }
        self.validate_listeners()?;
        self.validate_shards()?;
        self.validate_databases()?;
        self.backup.validate()?;
        self.observability.validate()?;
        Ok(())
    }

    fn validate_listeners(&self) -> Result<(), TopologyError> {
        let expected = [
            ("rw", format!("{}-rw", self.cluster), 5_432),
            ("ro", format!("{}-ro", self.cluster), 5_433),
            ("r", format!("{}-r", self.cluster), 5_434),
        ];
        if self.listeners.len() != expected.len()
            || self
                .listeners
                .iter()
                .zip(expected)
                .any(|(actual, expected)| {
                    actual.mode != expected.0
                        || actual.service != expected.1
                        || actual.target_port != expected.2
                        || !dns_label(&actual.service, 63)
                })
        {
            return Err(TopologyError::Invalid(
                "topology listeners are not the canonical ordered set",
            ));
        }
        Ok(())
    }

    fn validate_shards(&self) -> Result<(), TopologyError> {
        if self.shards.is_empty() || self.shards.len() > MAXIMUM_SHARDS {
            return Err(TopologyError::Invalid(
                "topology shard count is outside the supported bound",
            ));
        }
        let workload_prefix = bounded_postgresql_workload_prefix(&self.cluster);
        let mut writable_lease_uids = HashSet::with_capacity(self.shards.len());
        for (shard_ordinal, shard) in self.shards.iter().enumerate() {
            let shard_id = u32::try_from(shard_ordinal)
                .map_err(|_| TopologyError::Invalid("topology shard ordinal overflow"))?;
            let service = format!("{}-shard-{shard_id:04}", self.cluster);
            let stateful_set = format!("{workload_prefix}-shard-{shard_id:04}");
            if shard.id != shard_id
                || shard.service != service
                || !dns_label(&shard.service, 63)
                || shard.writable_lease.namespace != self.namespace
                || shard.writable_lease.name != format!("{stateful_set}-term")
                || !dns_label(&shard.writable_lease.name, 63)
                || validate_uid(&shard.writable_lease.uid).is_err()
            {
                return Err(TopologyError::Invalid(
                    "topology shard or writable Lease identity is not canonical",
                ));
            }
            if !writable_lease_uids.insert(shard.writable_lease.uid.as_str()) {
                return Err(TopologyError::Invalid(
                    "topology writable Lease UID is reused across shards",
                ));
            }
            if shard.members.len() != self.members_per_shard as usize {
                return Err(TopologyError::Invalid(
                    "topology member roster is incomplete",
                ));
            }
            for (member_ordinal, member) in shard.members.iter().enumerate() {
                let member_id = u32::try_from(member_ordinal)
                    .map_err(|_| TopologyError::Invalid("topology member ordinal overflow"))?;
                let member_stateful_set = if member_id == 0 {
                    stateful_set.clone()
                } else {
                    format!("{stateful_set}-m{member_id:04}")
                };
                let instance_id = format!("{member_stateful_set}-0");
                let dns_name = format!("{instance_id}.{service}.{}.svc", self.namespace);
                if member.ordinal != member_id
                    || member.instance_id != instance_id
                    || member.dns_name != dns_name
                    || !dns_subdomain(&member.dns_name)
                    || member.postgresql_port != 5_432
                    || member.agent_http_port != 8_080
                    || member.physical_slot != format!("pgshard_member_{member_id:04}")
                {
                    return Err(TopologyError::Invalid(
                        "topology member endpoint or slot identity is not canonical",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_databases(&self) -> Result<(), TopologyError> {
        if self.databases.len() > MAXIMUM_DATABASES {
            return Err(TopologyError::Invalid(
                "topology database count exceeds the supported bound",
            ));
        }
        let mut total_ranges = 0_usize;
        let mut previous_name: Option<&str> = None;
        for database in &self.databases {
            if !dns_label(&database.name, 63)
                || matches!(
                    database.name.as_str(),
                    "postgres" | "shardschema" | "template0" | "template1"
                )
                || previous_name.is_some_and(|previous| previous >= database.name.as_str())
                || database.shards == 0
                || database.shards as usize > self.shards.len()
                || database.cells.len() != database.shards as usize
            {
                return Err(TopologyError::Invalid(
                    "topology database placement is not canonical",
                ));
            }
            let mut seen = HashSet::with_capacity(database.cells.len());
            if database
                .cells
                .iter()
                .any(|cell| *cell as usize >= self.shards.len() || !seen.insert(*cell))
            {
                return Err(TopologyError::Invalid(
                    "topology database cells are duplicated or outside the shard set",
                ));
            }
            total_ranges = total_ranges.checked_add(database.shards as usize).ok_or(
                TopologyError::Invalid("topology routing-range count overflow"),
            )?;
            previous_name = Some(&database.name);
        }
        if total_ranges > MAXIMUM_TOTAL_ROUTING_RANGES {
            return Err(TopologyError::Invalid(
                "topology routing-range count exceeds the supported bound",
            ));
        }
        Ok(())
    }
}

impl TopologyBackup {
    fn validate(&self) -> Result<(), TopologyError> {
        match self.r#type {
            BackupType::S3 => {
                if self
                    .bucket
                    .as_deref()
                    .is_none_or(|value| value.is_empty() || value.len() > 255)
                    || self
                        .credentials_secret
                        .as_deref()
                        .is_none_or(|value| !dns_subdomain(value))
                    || self.persistent_volume_claim.is_some()
                    || self
                        .region
                        .as_ref()
                        .is_some_and(|value| value.is_empty() || value.len() > 128)
                    || self
                        .prefix
                        .as_ref()
                        .is_some_and(|value| value.is_empty() || value.len() > 1_024)
                    || self
                        .endpoint
                        .as_deref()
                        .is_some_and(|value| !valid_credential_free_http_endpoint(value))
                {
                    return Err(TopologyError::Invalid(
                        "topology S3 backup identity is incomplete or malformed",
                    ));
                }
            }
            BackupType::Filesystem => {
                if self
                    .persistent_volume_claim
                    .as_deref()
                    .is_none_or(|value| !dns_subdomain(value))
                    || self.bucket.is_some()
                    || self.endpoint.is_some()
                    || self.region.is_some()
                    || self.prefix.is_some()
                    || self.credentials_secret.is_some()
                {
                    return Err(TopologyError::Invalid(
                        "topology filesystem backup identity is incomplete or malformed",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl TopologyObservability {
    fn validate(&self) -> Result<(), TopologyError> {
        if self.service_monitor_requested && !self.prometheus {
            return Err(TopologyError::Invalid(
                "topology ServiceMonitor requires Prometheus metrics",
            ));
        }
        if self
            .open_telemetry_endpoint
            .as_deref()
            .is_some_and(|value| !valid_credential_free_http_endpoint(value))
        {
            return Err(TopologyError::Invalid(
                "topology OpenTelemetry endpoint is malformed",
            ));
        }
        Ok(())
    }
}

fn bounded_postgresql_workload_prefix(cluster: &str) -> String {
    const MAXIMUM_PREFIX_LENGTH: usize = 42;
    const DIGEST_BYTES: usize = 12;
    if cluster.len() < MAXIMUM_PREFIX_LENGTH {
        return cluster.to_owned();
    }
    let digest = Sha256::digest(cluster.as_bytes());
    let mut suffix = String::with_capacity(DIGEST_BYTES * 2);
    for byte in &digest[..DIGEST_BYTES] {
        let _ = write!(suffix, "{byte:02x}");
    }
    format!(
        "{}-{suffix}",
        &cluster[..MAXIMUM_PREFIX_LENGTH - suffix.len() - 1]
    )
}

fn validate_file_metadata(path: &Path, metadata: &fs::Metadata) -> Result<(), TopologyError> {
    if !metadata.is_file() {
        return Err(TopologyError::NotRegularFile(path.to_path_buf()));
    }
    if metadata.len() > MAXIMUM_TOPOLOGY_PAYLOAD_BYTES as u64 {
        return Err(TopologyError::PayloadTooLarge {
            maximum: MAXIMUM_TOPOLOGY_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

fn dns_label(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
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
}

fn dns_subdomain(value: &str) -> bool {
    !value.is_empty() && value.len() <= 253 && value.split('.').all(|label| dns_label(label, 63))
}

fn validate_uid(value: &str) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > MAXIMUM_UID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(());
    }
    Ok(())
}

fn deserialize_optional_non_null<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

/// Topology loading or validation failure.
#[derive(Debug, Error)]
pub enum TopologyError {
    /// The topology file could not be opened, inspected, or read.
    #[error("could not read topology file {path}: {source}")]
    Read {
        /// Path that failed.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// The opened path is not a regular file.
    #[error("topology path {0} is not a regular file")]
    NotRegularFile(PathBuf),
    /// The bounded input limit was exceeded.
    #[error("topology payload exceeds the {maximum}-byte safety limit")]
    PayloadTooLarge {
        /// Maximum accepted payload size.
        maximum: usize,
    },
    /// JSON parsing, duplicate-field, or unknown-field validation failed.
    #[error("topology JSON does not match the exact version-one schema: {0}")]
    Json(#[from] serde_json::Error),
    /// Semantic validation failed.
    #[error("invalid topology: {0}")]
    Invalid(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLUSTER_UID: &str = "11111111-2222-3333-4444-555555555555";

    fn valid_topology() -> String {
        let mut payload = format!(
            concat!(
                r#"{{"schemaVersion":"pgshard.topology.v1","cluster":"demo","clusterObjectUID":"{CLUSTER_UID}","namespace":"database","durability":"Synchronous","membersPerShard":3,"listeners":["#,
                r#"{{"mode":"rw","service":"demo-rw","targetPort":5432}},"#,
                r#"{{"mode":"ro","service":"demo-ro","targetPort":5433}},"#,
                r#"{{"mode":"r","service":"demo-r","targetPort":5434}}],"#,
                r#""shards":[{{"id":0,"service":"demo-shard-0000","writableLease":{{"namespace":"database","name":"demo-shard-0000-term","uid":"lease-uid-0"}},"members":["#,
                r#"{{"ordinal":0,"instanceId":"demo-shard-0000-0","dnsName":"demo-shard-0000-0.demo-shard-0000.database.svc","postgresqlPort":5432,"agentHttpPort":8080,"physicalSlot":"pgshard_member_0000"}},"#,
                r#"{{"ordinal":1,"instanceId":"demo-shard-0000-m0001-0","dnsName":"demo-shard-0000-m0001-0.demo-shard-0000.database.svc","postgresqlPort":5432,"agentHttpPort":8080,"physicalSlot":"pgshard_member_0001"}},"#,
                r#"{{"ordinal":2,"instanceId":"demo-shard-0000-m0002-0","dnsName":"demo-shard-0000-m0002-0.demo-shard-0000.database.svc","postgresqlPort":5432,"agentHttpPort":8080,"physicalSlot":"pgshard_member_0002"}}]}}],"#,
                r#""databases":[{{"name":"app","shards":1,"cells":[0]}}],"backup":{{"type":"Filesystem","persistentVolumeClaim":"backups"}},"observability":{{"prometheus":true,"serviceMonitorRequested":false}}}}"#
            ),
            CLUSTER_UID = CLUSTER_UID
        );
        payload.push('\n');
        payload
    }

    fn expected() -> ExpectedTopologyIdentity<'static> {
        ExpectedTopologyIdentity {
            cluster_id: "demo",
            cluster_uid: CLUSTER_UID,
            namespace: "database",
        }
    }

    fn parse(payload: &str) -> Result<TopologyV1, TopologyError> {
        TopologyV1::parse_and_validate(payload.as_bytes(), expected())
    }

    #[test]
    fn validates_exact_topology_and_yields_only_unbound_agent_targets() {
        let topology = parse(&valid_topology()).expect("valid topology");
        assert_eq!(
            topology.diagnostics(false).agent_status_collection,
            AgentStatusCollectionState::DisabledAgentRuntimeRequired,
        );
        assert_eq!(
            topology.diagnostics(true),
            TopologyDiagnostics {
                schema_version: TOPOLOGY_SCHEMA_VERSION.to_owned(),
                cluster_object_uid: CLUSTER_UID.to_owned(),
                shard_count: 1,
                member_count: 3,
                agent_status_collection: AgentStatusCollectionState::DisabledPodIdentityRequired,
            }
        );
        let targets = topology.agent_observation_targets();
        assert_eq!(targets.len(), 3);
        assert_eq!(targets[0].instance_id(), "demo-shard-0000-0");
        assert_eq!(targets[2].physical_slot(), "pgshard_member_0002");
        assert_eq!(targets[2].writable_lease_uid(), "lease-uid-0");
        let candidates = topology
            .catalog_candidate_observation_plan()
            .expect("multi-member candidate plan");
        assert_eq!(candidates.members.len(), 3);
        assert_eq!(
            candidates.members[0].config_map_name(),
            "demo-shard-0000-cfg"
        );
        assert_eq!(
            candidates.members[2].config_map_name(),
            "demo-shard-0000-m0002-cfg"
        );
        assert_eq!(candidates.writable_lease_uid, "lease-uid-0");
    }

    #[test]
    fn rejects_unknown_duplicate_missing_and_wrong_schema_fields() {
        let valid = valid_topology();
        let cases = [
            valid.replace(
                r#""schemaVersion":"pgshard.topology.v1""#,
                r#""schemaVersion":"future""#,
            ),
            valid.replace(
                r#""schemaVersion":"pgshard.topology.v1""#,
                r#""schemaVersion":"pgshard.topology.v1","unexpected":true"#,
            ),
            valid.replace(
                r#""serviceMonitorRequested":false"#,
                r#""serviceMonitorRequested":false,"openTelemetryEndpoint":null"#,
            ),
            valid.replace(
                r#""cluster":"demo""#,
                r#""cluster":"demo","cluster":"demo""#,
            ),
            valid.replace(&format!(r#","clusterObjectUID":"{CLUSTER_UID}""#), ""),
        ];
        for payload in cases {
            assert!(parse(&payload).is_err(), "accepted {payload}");
        }
    }

    #[test]
    fn rejects_misbound_and_noncanonical_ha_identity() {
        let valid = valid_topology();
        let cases = [
            valid.replace(CLUSTER_UID, "other-uid"),
            valid.replace("lease-uid-0", ""),
            valid.replace("demo-shard-0000-term", "demo-shard-0000-other"),
            valid.replace("pgshard_member_0002", "pgshard_member_2"),
            valid.replace("agentHttpPort\":8080", "agentHttpPort\":8081"),
            valid.replace("\"ordinal\":2", "\"ordinal\":1"),
            valid.replace("demo-shard-0000-m0002-0", "demo-shard-0000-m0001-0"),
        ];
        for payload in cases {
            assert!(parse(&payload).is_err(), "accepted {payload}");
        }
    }

    #[test]
    fn rejects_incomplete_extra_and_unsorted_rosters() {
        let valid = valid_topology();
        let cases = [
            valid.replace("\"membersPerShard\":3", "\"membersPerShard\":5"),
            valid.replace(
                r#""databases":[{"name":"app","shards":1,"cells":[0]}]"#,
                r#""databases":[{"name":"z","shards":1,"cells":[0]},{"name":"a","shards":1,"cells":[0]}]"#,
            ),
            valid.replace("\"cells\":[0]", "\"cells\":[1]"),
            valid.replace(
                r#""persistentVolumeClaim":"backups""#,
                r#""persistentVolumeClaim":"backups","bucket":"extra""#,
            ),
            valid.replace(
                r#""serviceMonitorRequested":false"#,
                r#""serviceMonitorRequested":false,"openTelemetryEndpoint":"https://example.invalid?token=value""#,
            ),
            valid.replace(
                r#""prometheus":true,"serviceMonitorRequested":false"#,
                r#""prometheus":false,"serviceMonitorRequested":true"#,
            ),
            valid.replace(
                r#""type":"Filesystem","persistentVolumeClaim":"backups""#,
                r#""type":"S3","bucket":"backups","region":"","prefix":"demo","credentialsSecret":"backup-auth""#,
            ),
        ];
        for payload in cases {
            assert!(parse(&payload).is_err(), "accepted {payload}");
        }
    }

    #[test]
    fn validates_operator_name_hash_boundary() {
        assert_eq!(bounded_postgresql_workload_prefix("short"), "short");
        assert_eq!(
            bounded_postgresql_workload_prefix(&"a".repeat(42)),
            "aaaaaaaaaaaaaaaaa-7a538607fdaab9296995929f"
        );
    }

    #[test]
    fn rejects_a_writable_lease_uid_reused_by_another_shard() {
        let mut topology = parse(&valid_topology()).expect("valid topology");
        let mut second = topology.shards[0].clone();
        second.id = 1;
        second.service = "demo-shard-0001".to_owned();
        second.writable_lease.name = "demo-shard-0001-term".to_owned();
        for (ordinal, member) in second.members.iter_mut().enumerate() {
            let suffix = if ordinal == 0 {
                String::new()
            } else {
                format!("-m{ordinal:04}")
            };
            member.instance_id = format!("demo-shard-0001{suffix}-0");
            member.dns_name = format!("{}.demo-shard-0001.database.svc", member.instance_id);
        }
        topology.shards.push(second);

        assert!(matches!(
            topology.validate(expected()),
            Err(TopologyError::Invalid(
                "topology writable Lease UID is reused across shards"
            ))
        ));
    }
}
