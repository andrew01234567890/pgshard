//! Bounded, diagnostic-only collection of exact agent status snapshots.
//!
//! A successful response is self-reported evidence bound to one freshly
//! observed Kubernetes Pod and writable-term Lease. It never grants serving,
//! routing, promotion, failover, role, or writable authority.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use pgshard_types::ShardId;
use pgshard_types::writable_generation::DurableWritableGeneration;
use serde::Deserialize;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;

/// Exact status schema emitted by the agent HTTP handler.
pub(crate) const AGENT_STATUS_SCHEMA_VERSION: &str = "pgshard.agent.status.v1";
/// Maximum number of topology targets in one bounded collection.
pub(crate) const MAXIMUM_AGENT_STATUS_TARGETS: usize = 128 * 5;
/// Maximum simultaneous status connections from one orchestrator.
pub(crate) const MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS: usize = 16;
/// Maximum accepted response header count.
pub(crate) const MAXIMUM_AGENT_STATUS_HEADERS: usize = 32;
/// Maximum accepted response header bytes.
pub(crate) const MAXIMUM_AGENT_STATUS_HEADER_BYTES: usize = 16 * 1_024;
/// Maximum accepted response body bytes.
pub(crate) const MAXIMUM_AGENT_STATUS_BODY_BYTES: usize = 64 * 1_024;
/// Monotonic lifetime of one complete network request.
pub(crate) const AGENT_STATUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const EXPECTED_TARGET_FENCE_MARGIN_MS: u64 = 3_500;
const EXPECTED_AGENT_LEASE_MAXIMUM_MS: u64 = 15_000;
const EXPECTED_POSTGRESQL_PORT: u16 = 5_432;

/// Exact Kubernetes Lease state bracketed around one HTTP collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExpectedWritableLease {
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) uid: String,
    pub(crate) holder_identity: Option<String>,
    pub(crate) transitions: u64,
}

/// Exact topology and Kubernetes identity expected from one response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentStatusExpectation {
    pub(crate) cluster_id: String,
    pub(crate) cluster_uid: String,
    pub(crate) shard_id: u32,
    pub(crate) member_ordinal: u32,
    pub(crate) instance_id: String,
    pub(crate) pod_uid: String,
    pub(crate) source_instance_id: String,
    pub(crate) source_dns_name: String,
    pub(crate) member_slot_name: String,
    pub(crate) standby_slot_names: Vec<String>,
    pub(crate) synchronous_durability: bool,
    pub(crate) writable_lease: ExpectedWritableLease,
}

/// One direct-Pod-IP request and its expected response identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentStatusQuery {
    pub(crate) address: SocketAddr,
    pub(crate) expected: AgentStatusExpectation,
}

/// Bounded summary of one complete collection. Raw status is discarded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AgentStatusCollection {
    pub(crate) member_count: usize,
    pub(crate) earliest_receipt: Instant,
}

/// Minimal, ephemeral replication facts needed to correlate one shard.
///
/// These values exist only inside one bounded collection. The returned
/// collection deliberately retains only member count and receipt time.
#[derive(Clone, Debug, Eq, PartialEq)]
enum MemberReplicationEvidence {
    Source {
        system_identifier: u64,
        timeline: u32,
        generation_identity: String,
        generation_barrier_lsn: u64,
    },
    Standby {
        system_identifier: u64,
        timeline: u32,
        generation_identity: String,
        receive_lsn: u64,
        replay_lsn: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CollectedAgentStatus {
    shard_id: u32,
    member_ordinal: u32,
    receipt: Instant,
    replication: Option<MemberReplicationEvidence>,
}

/// Collects every status with a fixed global concurrency bound.
///
/// Dropping this future drops the `JoinSet`, aborting every in-flight request.
/// No task can publish state: publication remains with the caller after all
/// tasks and the post-request Kubernetes bracket complete.
pub(crate) async fn collect_agent_statuses(
    queries: Vec<AgentStatusQuery>,
) -> Result<AgentStatusCollection, AgentStatusError> {
    if queries.is_empty() || queries.len() > MAXIMUM_AGENT_STATUS_TARGETS {
        return Err(AgentStatusError::InvalidTargetSet);
    }
    let member_count = queries.len();
    let observations = run_bounded(
        queries,
        MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS,
        collect_one,
    )
    .await?;
    let earliest_receipt = observations
        .iter()
        .map(|observation| observation.receipt)
        .min()
        .ok_or(AgentStatusError::InvalidTargetSet)?;
    validate_replication_correlation(&observations)?;

    Ok(AgentStatusCollection {
        member_count,
        earliest_receipt,
    })
}

/// Runs one finite job set under an exact hard concurrency ceiling.
///
/// The first worker error or task failure aborts and drains every outstanding
/// task before returning. This keeps request-local data from surviving a
/// failed collection and makes late publication by a cancelled worker
/// impossible.
async fn run_bounded<I, O, W, F>(
    jobs: Vec<I>,
    maximum_concurrency: usize,
    worker: W,
) -> Result<Vec<O>, AgentStatusError>
where
    I: Send + 'static,
    O: Send + 'static,
    W: Fn(I) -> F + Clone,
    F: Future<Output = Result<O, AgentStatusError>> + Send + 'static,
{
    if maximum_concurrency == 0 {
        return Err(AgentStatusError::InvalidTargetSet);
    }
    let job_count = jobs.len();
    let mut jobs = jobs.into_iter();
    let mut tasks = JoinSet::new();
    let mut outputs = Vec::with_capacity(job_count);

    loop {
        while tasks.len() < maximum_concurrency {
            let Some(job) = jobs.next() else {
                break;
            };
            tasks.spawn(worker.clone()(job));
        }
        let Some(joined) = tasks.join_next().await else {
            break;
        };
        match joined {
            Ok(Ok(output)) => outputs.push(output),
            Ok(Err(error)) => {
                abort_and_drain(&mut tasks).await;
                return Err(error);
            }
            Err(_) => {
                abort_and_drain(&mut tasks).await;
                return Err(AgentStatusError::RequestTaskFailed);
            }
        }
    }
    Ok(outputs)
}

async fn abort_and_drain<O: 'static>(tasks: &mut JoinSet<Result<O, AgentStatusError>>) {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

async fn collect_one(query: AgentStatusQuery) -> Result<CollectedAgentStatus, AgentStatusError> {
    tokio::time::timeout(AGENT_STATUS_REQUEST_TIMEOUT, collect_one_inner(query))
        .await
        .map_err(|_| AgentStatusError::RequestTimedOut)?
}

async fn collect_one_inner(
    query: AgentStatusQuery,
) -> Result<CollectedAgentStatus, AgentStatusError> {
    let mut stream = TcpStream::connect(query.address)
        .await
        .map_err(AgentStatusError::Connect)?;
    let request = format!(
        "GET /status HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
        query.address
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(AgentStatusError::Write)?;
    let body = read_bounded_response(&mut stream).await?;
    let status: AgentStatusV1 = serde_json::from_slice(&body)?;
    let replication = validate_status(&status, &query.expected)?;
    Ok(CollectedAgentStatus {
        shard_id: query.expected.shard_id,
        member_ordinal: query.expected.member_ordinal,
        receipt: Instant::now(),
        replication,
    })
}

async fn read_bounded_response(stream: &mut TcpStream) -> Result<Vec<u8>, AgentStatusError> {
    let mut received = Vec::with_capacity(4 * 1_024);
    let header_end = loop {
        if let Some(index) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if received.len() >= MAXIMUM_AGENT_STATUS_HEADER_BYTES {
            return Err(AgentStatusError::HeadersTooLarge);
        }
        let mut chunk = [0_u8; 2_048];
        let maximum = (MAXIMUM_AGENT_STATUS_HEADER_BYTES + 1 - received.len()).min(chunk.len());
        let count = stream
            .read(&mut chunk[..maximum])
            .await
            .map_err(AgentStatusError::Read)?;
        if count == 0 {
            return Err(AgentStatusError::TruncatedResponse);
        }
        received.extend_from_slice(&chunk[..count]);
        if received.len() > MAXIMUM_AGENT_STATUS_HEADER_BYTES
            && received
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .is_none()
        {
            return Err(AgentStatusError::HeadersTooLarge);
        }
    };
    if header_end > MAXIMUM_AGENT_STATUS_HEADER_BYTES {
        return Err(AgentStatusError::HeadersTooLarge);
    }
    let content_length = parse_response_headers(&received[..header_end])?;
    if content_length > MAXIMUM_AGENT_STATUS_BODY_BYTES {
        return Err(AgentStatusError::BodyTooLarge);
    }
    let mut body = received.split_off(header_end);
    if body.len() > content_length {
        return Err(AgentStatusError::UnexpectedTrailingBytes);
    }
    while body.len() < content_length {
        let remaining = content_length - body.len();
        let mut chunk = [0_u8; 4 * 1_024];
        let maximum = remaining.min(chunk.len());
        let count = stream
            .read(&mut chunk[..maximum])
            .await
            .map_err(AgentStatusError::Read)?;
        if count == 0 {
            return Err(AgentStatusError::TruncatedResponse);
        }
        body.extend_from_slice(&chunk[..count]);
    }
    let mut trailing = [0_u8; 1];
    let count = stream
        .read(&mut trailing)
        .await
        .map_err(AgentStatusError::Read)?;
    if count != 0 {
        return Err(AgentStatusError::UnexpectedTrailingBytes);
    }
    Ok(body)
}

fn parse_response_headers(bytes: &[u8]) -> Result<usize, AgentStatusError> {
    let text = std::str::from_utf8(bytes).map_err(|_| AgentStatusError::InvalidHeaders)?;
    let mut lines = text
        .strip_suffix("\r\n\r\n")
        .ok_or(AgentStatusError::InvalidHeaders)?
        .split("\r\n");
    let status = lines.next().ok_or(AgentStatusError::InvalidHeaders)?;
    let mut status_parts = status.splitn(3, ' ');
    if status_parts.next() != Some("HTTP/1.1")
        || status_parts.next() != Some("200")
        || status_parts.next().is_none()
    {
        return Err(AgentStatusError::UnexpectedHttpStatus);
    }
    let mut count = 0_usize;
    let mut content_type = None;
    let mut content_length = None;
    let mut cache_control = None;
    for line in lines {
        count += 1;
        if count > MAXIMUM_AGENT_STATUS_HEADERS || line.starts_with([' ', '\t']) || !line.is_ascii()
        {
            return Err(AgentStatusError::InvalidHeaders);
        }
        let (name, value) = line
            .split_once(':')
            .ok_or(AgentStatusError::InvalidHeaders)?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(AgentStatusError::InvalidHeaders);
        }
        if !value
            .bytes()
            .all(|byte| byte == b'\t' || matches!(byte, 0x20..=0x7e))
        {
            return Err(AgentStatusError::InvalidHeaders);
        }
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-type") {
            set_once(&mut content_type, value)?;
        } else if name.eq_ignore_ascii_case("content-length") {
            set_once(&mut content_length, value)?;
        } else if name.eq_ignore_ascii_case("cache-control") {
            set_once(&mut cache_control, value)?;
        } else if name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("content-encoding")
            || name.eq_ignore_ascii_case("upgrade")
            || name.eq_ignore_ascii_case("location")
            || (name.eq_ignore_ascii_case("connection")
                && value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("upgrade")))
        {
            return Err(AgentStatusError::UnsupportedResponseFraming);
        }
    }
    if content_type != Some("application/json") || cache_control != Some("no-store") {
        return Err(AgentStatusError::UnexpectedContentType);
    }
    let length = content_length.ok_or(AgentStatusError::ContentLengthRequired)?;
    if length.is_empty() || !length.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(AgentStatusError::InvalidContentLength);
    }
    let parsed = length
        .parse::<usize>()
        .map_err(|_| AgentStatusError::InvalidContentLength)?;
    if parsed.to_string() != length {
        return Err(AgentStatusError::InvalidContentLength);
    }
    Ok(parsed)
}

fn set_once<'a>(slot: &mut Option<&'a str>, value: &'a str) -> Result<(), AgentStatusError> {
    if slot.replace(value).is_some() {
        return Err(AgentStatusError::DuplicateHeader);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum PostgresRoleWire {
    Unknown,
    Primary,
    Replica,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum PostgresProcessStateWire {
    Disabled,
    Validated,
    StartingQuarantined,
    RunningQuarantined,
    StartingReplicationBootstrap,
    RunningReplicationBootstrap,
    StartingReplicationStandby,
    RunningReplicationStandby,
    Stopping,
    Fenced,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ReplicationStreamStateWire {
    Startup,
    Catchup,
    Streaming,
    Backup,
    Stopping,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ReplicationSyncStateWire {
    Async,
    Potential,
    Sync,
    Quorum,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)] // Exact external wire names are intentional.
struct AgentIdentityWire {
    cluster_id: String,
    shard_id: u32,
    instance_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PostgresObservationWire {
    role: PostgresRoleWire,
    timeline: u32,
    fencing_epoch: CanonicalU64,
    observed_at_unix_ms: CanonicalU64,
    flush_lsn: Option<CanonicalU64>,
    replay_lsn: Option<CanonicalU64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct FencingLeaseWire {
    owner_instance: String,
    epoch: CanonicalU64,
    valid_until_unix_ms: CanonicalU64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum GenerationDurabilityWire {
    Local,
    RemoteApplyAnyOne { candidates: Vec<String> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct SourceReplicationCandidateWire {
    member_slot_name: String,
    slot_active: bool,
    slot_walsender_match: bool,
    stream_state: Option<ReplicationStreamStateWire>,
    sync_state: Option<ReplicationSyncStateWire>,
    flush_lsn: Option<CanonicalU64>,
    replay_lsn: Option<CanonicalU64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct SourceReplicationEvidenceWire {
    observed_at_unix_ms: CanonicalU64,
    system_identifier: CanonicalU64,
    timeline: u32,
    in_recovery: bool,
    generation_identity: String,
    generation_barrier_lsn: CanonicalU64,
    durability: GenerationDurabilityWire,
    candidates: Vec<SourceReplicationCandidateWire>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct StandbyReplicationEvidenceWire {
    observed_at_unix_ms: CanonicalU64,
    system_identifier: CanonicalU64,
    timeline: u32,
    in_recovery: bool,
    generation_identity: String,
    member_slot_name: String,
    receive_lsn: CanonicalU64,
    replay_lsn: CanonicalU64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
enum ReplicationEvidenceWire {
    Source(SourceReplicationEvidenceWire),
    Standby(StandbyReplicationEvidenceWire),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
enum ActivationConfigWire {
    Source {
        identity: AgentIdentityWire,
        cluster_uid: String,
        pod_uid: String,
        lease_namespace: String,
        lease_name: String,
        lease_uid: String,
        durability: GenerationDurabilityWire,
        target_fence_required_margin_ms: CanonicalU64,
    },
    Standby {
        identity: AgentIdentityWire,
        cluster_uid: String,
        pod_uid: String,
        primary_host: String,
        primary_port: u16,
        member_slot_name: String,
    },
}

impl ActivationConfigWire {
    fn common(&self) -> (&AgentIdentityWire, &str, &str) {
        match self {
            Self::Source {
                identity,
                cluster_uid,
                pod_uid,
                ..
            }
            | Self::Standby {
                identity,
                cluster_uid,
                pod_uid,
                ..
            } => (identity, cluster_uid, pod_uid),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TargetFenceAcknowledgementWire {
    observed_at_unix_ms: CanonicalU64,
    generation_identity: String,
    deadline_boottime_ns: CanonicalU64,
    remaining_validity_at_ack_ms: CanonicalU64,
    boot_id: String,
    postmaster_pid: u32,
    control_backend_pid: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PostgresProcessIdentityWire {
    postmaster_pid: u32,
    boot_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct AgentStatusV1 {
    schema_version: String,
    identity: Option<AgentIdentityWire>,
    postgres: Option<PostgresObservationWire>,
    postgres_process: PostgresProcessStateWire,
    lease: Option<FencingLeaseWire>,
    replication_evidence: Option<ReplicationEvidenceWire>,
    activation_config: Option<ActivationConfigWire>,
    target_fence_acknowledgement: Option<TargetFenceAcknowledgementWire>,
    postgres_process_identity: Option<PostgresProcessIdentityWire>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CanonicalU64(u64);

impl<'de> Deserialize<'de> for CanonicalU64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(serde::de::Error::custom(
                "expected a canonical decimal string",
            ));
        }
        let parsed = value.parse::<u64>().map_err(serde::de::Error::custom)?;
        if parsed.to_string() != value {
            return Err(serde::de::Error::custom("noncanonical decimal string"));
        }
        Ok(Self(parsed))
    }
}

fn validate_status(
    status: &AgentStatusV1,
    expected: &AgentStatusExpectation,
) -> Result<Option<MemberReplicationEvidence>, AgentStatusError> {
    if status.schema_version != AGENT_STATUS_SCHEMA_VERSION {
        return Err(AgentStatusError::SchemaMismatch);
    }
    let identity = status
        .identity
        .as_ref()
        .ok_or(AgentStatusError::IdentityMismatch)?;
    validate_identity(identity, expected)?;
    let source = expected.member_ordinal == 0;
    if source && expected.standby_slot_names.is_empty() {
        validate_quarantine(status, expected)?;
        return Ok(None);
    }
    let activation = status
        .activation_config
        .as_ref()
        .ok_or(AgentStatusError::ConfigurationMismatch)?;
    let (activation_identity, cluster_uid, pod_uid) = activation.common();
    validate_identity(activation_identity, expected)?;
    if cluster_uid != expected.cluster_uid
        || pod_uid != expected.pod_uid
        || !canonical_uid(cluster_uid)
        || !canonical_uid(pod_uid)
    {
        return Err(AgentStatusError::ConfigurationMismatch);
    }

    validate_process_shape(status, source)?;
    let replication = if source {
        validate_source(status, activation, expected)?
    } else {
        validate_standby(status, activation, expected)?
    };
    Ok(replication)
}

fn validate_quarantine(
    status: &AgentStatusV1,
    expected: &AgentStatusExpectation,
) -> Result<(), AgentStatusError> {
    if expected.member_ordinal != 0
        || expected.synchronous_durability
        || !expected.standby_slot_names.is_empty()
        || status.activation_config.is_some()
        || status.postgres.is_some()
        || status.replication_evidence.is_some()
        || status.target_fence_acknowledgement.is_some()
    {
        return Err(AgentStatusError::ConfigurationMismatch);
    }
    if !matches!(
        status.postgres_process,
        PostgresProcessStateWire::StartingQuarantined
            | PostgresProcessStateWire::RunningQuarantined
    ) {
        return Err(AgentStatusError::ProcessMismatch);
    }
    let process = status
        .postgres_process_identity
        .as_ref()
        .ok_or(AgentStatusError::ProcessMismatch)?;
    if process.postmaster_pid == 0 || !canonical_boot_id(&process.boot_id) {
        return Err(AgentStatusError::ProcessMismatch);
    }
    expected_generation(expected)?.ok_or(AgentStatusError::LeaseMismatch)?;
    let lease = status
        .lease
        .as_ref()
        .ok_or(AgentStatusError::LeaseMismatch)?;
    if lease.owner_instance != expected.instance_id
        || lease.epoch.0 == 0
        || lease.epoch.0 != expected.writable_lease.transitions
        || lease.valid_until_unix_ms.0 == 0
    {
        return Err(AgentStatusError::LeaseMismatch);
    }
    Ok(())
}

fn validate_identity(
    identity: &AgentIdentityWire,
    expected: &AgentStatusExpectation,
) -> Result<(), AgentStatusError> {
    if identity.cluster_id != expected.cluster_id
        || identity.shard_id != expected.shard_id
        || identity.instance_id != expected.instance_id
        || !dns_label(&identity.cluster_id)
        || !dns_label(&identity.instance_id)
    {
        return Err(AgentStatusError::IdentityMismatch);
    }
    Ok(())
}

fn validate_process_shape(status: &AgentStatusV1, source: bool) -> Result<(), AgentStatusError> {
    let running = matches!(
        status.postgres_process,
        PostgresProcessStateWire::StartingReplicationBootstrap
            | PostgresProcessStateWire::RunningReplicationBootstrap
            | PostgresProcessStateWire::StartingReplicationStandby
            | PostgresProcessStateWire::RunningReplicationStandby
    );
    let role_matches = if source {
        matches!(
            status.postgres_process,
            PostgresProcessStateWire::Validated
                | PostgresProcessStateWire::StartingReplicationBootstrap
                | PostgresProcessStateWire::RunningReplicationBootstrap
                | PostgresProcessStateWire::Stopping
                | PostgresProcessStateWire::Fenced
                | PostgresProcessStateWire::Failed
        )
    } else {
        matches!(
            status.postgres_process,
            PostgresProcessStateWire::Validated
                | PostgresProcessStateWire::StartingReplicationStandby
                | PostgresProcessStateWire::RunningReplicationStandby
                | PostgresProcessStateWire::Stopping
                | PostgresProcessStateWire::Fenced
                | PostgresProcessStateWire::Failed
        )
    };
    if !role_matches || running != status.postgres_process_identity.is_some() {
        return Err(AgentStatusError::ProcessMismatch);
    }
    if let Some(process) = &status.postgres_process_identity
        && (process.postmaster_pid == 0 || !canonical_boot_id(&process.boot_id))
    {
        return Err(AgentStatusError::ProcessMismatch);
    }
    Ok(())
}

fn validate_source(
    status: &AgentStatusV1,
    activation: &ActivationConfigWire,
    expected: &AgentStatusExpectation,
) -> Result<Option<MemberReplicationEvidence>, AgentStatusError> {
    let ActivationConfigWire::Source {
        lease_namespace,
        lease_name,
        lease_uid,
        durability,
        target_fence_required_margin_ms,
        ..
    } = activation
    else {
        return Err(AgentStatusError::ConfigurationMismatch);
    };
    if lease_namespace != &expected.writable_lease.namespace
        || lease_name != &expected.writable_lease.name
        || lease_uid != &expected.writable_lease.uid
        || target_fence_required_margin_ms.0 != EXPECTED_TARGET_FENCE_MARGIN_MS
        || !durability_matches(durability, expected)
    {
        return Err(AgentStatusError::ConfigurationMismatch);
    }

    let expected_generation = expected_generation(expected)?;
    let process_running = matches!(
        status.postgres_process,
        PostgresProcessStateWire::StartingReplicationBootstrap
            | PostgresProcessStateWire::RunningReplicationBootstrap
    );
    match (&status.lease, expected_generation.as_ref()) {
        (Some(lease), Some(generation)) if process_running => {
            if lease.owner_instance != expected.instance_id
                || lease.epoch.0 != expected.writable_lease.transitions
                || lease.epoch.0 == 0
                || lease.valid_until_unix_ms.0 == 0
            {
                return Err(AgentStatusError::LeaseMismatch);
            }
            validate_generation_evidence(status, generation, expected)?;
        }
        (None, None) => validate_generation_absent(status)?,
        (None, Some(_)) if !process_running => validate_generation_absent(status)?,
        _ => return Err(AgentStatusError::LeaseMismatch),
    }
    if let Some(postgres) = &status.postgres
        && (postgres.role != PostgresRoleWire::Primary
            || postgres.timeline == 0
            || expected.writable_lease.transitions == 0
            || postgres.fencing_epoch.0 != expected.writable_lease.transitions
            || postgres.observed_at_unix_ms.0 == 0
            || postgres.flush_lsn.is_none_or(|lsn| lsn.0 == 0)
            || postgres.replay_lsn.is_some())
    {
        return Err(AgentStatusError::ProcessMismatch);
    }
    Ok(status.replication_evidence.as_ref().map(|evidence| {
        let ReplicationEvidenceWire::Source(evidence) = evidence else {
            unreachable!("validated source evidence has the source role")
        };
        MemberReplicationEvidence::Source {
            system_identifier: evidence.system_identifier.0,
            timeline: evidence.timeline,
            generation_identity: evidence.generation_identity.clone(),
            generation_barrier_lsn: evidence.generation_barrier_lsn.0,
        }
    }))
}

fn validate_standby(
    status: &AgentStatusV1,
    activation: &ActivationConfigWire,
    expected: &AgentStatusExpectation,
) -> Result<Option<MemberReplicationEvidence>, AgentStatusError> {
    let ActivationConfigWire::Standby {
        primary_host,
        primary_port,
        member_slot_name,
        ..
    } = activation
    else {
        return Err(AgentStatusError::ConfigurationMismatch);
    };
    if primary_host != &expected.source_dns_name
        || *primary_port != EXPECTED_POSTGRESQL_PORT
        || member_slot_name != &expected.member_slot_name
        || status.lease.is_some()
        || status.target_fence_acknowledgement.is_some()
    {
        return Err(AgentStatusError::ConfigurationMismatch);
    }
    if let Some(evidence) = &status.replication_evidence {
        let ReplicationEvidenceWire::Standby(evidence) = evidence else {
            return Err(AgentStatusError::ReplicationEvidenceMismatch);
        };
        let generation =
            expected_generation(expected)?.ok_or(AgentStatusError::ReplicationEvidenceMismatch)?;
        if evidence.member_slot_name != expected.member_slot_name
            || !evidence.in_recovery
            || evidence.timeline == 0
            || evidence.system_identifier.0 == 0
            || evidence.observed_at_unix_ms.0 == 0
            || evidence.receive_lsn.0 == 0
            || evidence.replay_lsn.0 == 0
            || evidence.receive_lsn.0 < evidence.replay_lsn.0
            || !generation_matches(&evidence.generation_identity, &generation)
        {
            return Err(AgentStatusError::ReplicationEvidenceMismatch);
        }
    }
    if let Some(postgres) = &status.postgres
        && (postgres.role != PostgresRoleWire::Replica
            || postgres.timeline == 0
            || postgres.observed_at_unix_ms.0 == 0)
    {
        return Err(AgentStatusError::ProcessMismatch);
    }
    Ok(status.replication_evidence.as_ref().map(|evidence| {
        let ReplicationEvidenceWire::Standby(evidence) = evidence else {
            unreachable!("validated standby evidence has the standby role")
        };
        MemberReplicationEvidence::Standby {
            system_identifier: evidence.system_identifier.0,
            timeline: evidence.timeline,
            generation_identity: evidence.generation_identity.clone(),
            receive_lsn: evidence.receive_lsn.0,
            replay_lsn: evidence.replay_lsn.0,
        }
    }))
}

fn validate_generation_evidence(
    status: &AgentStatusV1,
    generation: &DurableWritableGeneration,
    expected: &AgentStatusExpectation,
) -> Result<(), AgentStatusError> {
    if let Some(evidence) = &status.replication_evidence {
        let ReplicationEvidenceWire::Source(evidence) = evidence else {
            return Err(AgentStatusError::ReplicationEvidenceMismatch);
        };
        let expected_candidates: &[String] = if expected.synchronous_durability {
            &expected.standby_slot_names
        } else {
            &[]
        };
        if evidence.in_recovery
            || evidence.timeline == 0
            || evidence.system_identifier.0 == 0
            || evidence.observed_at_unix_ms.0 == 0
            || evidence.generation_barrier_lsn.0 == 0
            || !generation_matches(&evidence.generation_identity, generation)
            || !durability_matches(&evidence.durability, expected)
            || evidence.candidates.len() != expected_candidates.len()
            || evidence
                .candidates
                .iter()
                .zip(expected_candidates)
                .any(|(candidate, slot)| {
                    candidate.member_slot_name != *slot || !valid_candidate_shape(candidate)
                })
        {
            return Err(AgentStatusError::ReplicationEvidenceMismatch);
        }
    }
    let acknowledgement_required =
        status.postgres_process == PostgresProcessStateWire::RunningReplicationBootstrap;
    if acknowledgement_required && status.target_fence_acknowledgement.is_none() {
        return Err(AgentStatusError::AcknowledgementMismatch);
    }
    if let Some(ack) = &status.target_fence_acknowledgement {
        let process = status
            .postgres_process_identity
            .as_ref()
            .ok_or(AgentStatusError::AcknowledgementMismatch)?;
        if ack.observed_at_unix_ms.0 == 0
            || ack.deadline_boottime_ns.0 == 0
            || ack.remaining_validity_at_ack_ms.0 < EXPECTED_TARGET_FENCE_MARGIN_MS
            || ack.remaining_validity_at_ack_ms.0 > EXPECTED_AGENT_LEASE_MAXIMUM_MS
            || ack.postmaster_pid != process.postmaster_pid
            || ack.boot_id != process.boot_id
            || ack.control_backend_pid == 0
            || !generation_matches(&ack.generation_identity, generation)
        {
            return Err(AgentStatusError::AcknowledgementMismatch);
        }
    }
    Ok(())
}

fn valid_candidate_shape(candidate: &SourceReplicationCandidateWire) -> bool {
    candidate.stream_state.is_some() == candidate.sync_state.is_some()
        && (!candidate.slot_walsender_match
            || (candidate.slot_active && candidate.stream_state.is_some()))
        && !matches!(
            (candidate.flush_lsn, candidate.replay_lsn),
            (Some(flush), Some(replay)) if replay.0 > flush.0
        )
}

/// Correlates only the bounded facts that remain meaningful across separately
/// sampled agent endpoints.
///
/// A shard with no replication evidence is a valid diagnostic result: absence
/// is reported by the agents and does not become health or authority. Partial
/// evidence fails closed because it cannot support a coherent shard-level
/// correlation. When every member supplies evidence, the physical system,
/// timeline, and durable generation must match exactly.
///
/// LSN ordering is intentionally limited to each agent's atomic evidence:
/// standby replay cannot exceed receive, and source candidate replay cannot
/// exceed flush (validated before this function). The generation barrier is a
/// lower eligibility threshold, not an upper bound. Because endpoints are
/// sampled concurrently without a shared snapshot and remote wall clocks are
/// deliberately untrusted, comparing a standby position to a source position
/// as an upper bound would reject valid WAL progress based solely on sampling
/// order. Matching system, timeline, and generation first establishes that the
/// locally ordered positions share one coordinate system without inventing an
/// unsafe cross-endpoint time order.
fn validate_replication_correlation(
    observations: &[CollectedAgentStatus],
) -> Result<(), AgentStatusError> {
    let mut shards: BTreeMap<u32, Vec<&CollectedAgentStatus>> = BTreeMap::new();
    let mut members = HashSet::with_capacity(observations.len());
    for observation in observations {
        if !members.insert((observation.shard_id, observation.member_ordinal)) {
            return Err(AgentStatusError::ReplicationEvidenceMismatch);
        }
        shards
            .entry(observation.shard_id)
            .or_default()
            .push(observation);
    }

    for observations in shards.values() {
        let evidence_count = observations
            .iter()
            .filter(|observation| observation.replication.is_some())
            .count();
        if evidence_count == 0 {
            continue;
        }
        if evidence_count != observations.len() {
            return Err(AgentStatusError::PartialReplicationEvidence);
        }

        let source = observations
            .iter()
            .find(|observation| observation.member_ordinal == 0)
            .ok_or(AgentStatusError::ReplicationEvidenceMismatch)?;
        let Some(MemberReplicationEvidence::Source {
            system_identifier,
            timeline,
            generation_identity,
            generation_barrier_lsn,
        }) = source.replication.as_ref()
        else {
            return Err(AgentStatusError::ReplicationEvidenceMismatch);
        };
        if *system_identifier == 0 || *timeline == 0 || *generation_barrier_lsn == 0 {
            return Err(AgentStatusError::ReplicationEvidenceMismatch);
        }

        for observation in observations {
            if observation.member_ordinal == 0 {
                continue;
            }
            let Some(MemberReplicationEvidence::Standby {
                system_identifier: standby_system_identifier,
                timeline: standby_timeline,
                generation_identity: standby_generation,
                receive_lsn,
                replay_lsn,
            }) = observation.replication.as_ref()
            else {
                return Err(AgentStatusError::ReplicationEvidenceMismatch);
            };
            if standby_system_identifier != system_identifier
                || standby_timeline != timeline
                || standby_generation != generation_identity
                || replay_lsn > receive_lsn
            {
                return Err(AgentStatusError::ReplicationEvidenceMismatch);
            }
        }
    }
    Ok(())
}

fn validate_generation_absent(status: &AgentStatusV1) -> Result<(), AgentStatusError> {
    if status.lease.is_some()
        || status.replication_evidence.is_some()
        || status.target_fence_acknowledgement.is_some()
    {
        return Err(AgentStatusError::LeaseMismatch);
    }
    Ok(())
}

fn durability_matches(
    actual: &GenerationDurabilityWire,
    expected: &AgentStatusExpectation,
) -> bool {
    match (actual, expected.synchronous_durability) {
        (GenerationDurabilityWire::Local, false) => true,
        (GenerationDurabilityWire::RemoteApplyAnyOne { candidates }, true) => {
            candidates == &expected.standby_slot_names
        }
        _ => false,
    }
}

fn expected_generation(
    expected: &AgentStatusExpectation,
) -> Result<Option<DurableWritableGeneration>, AgentStatusError> {
    let Some(holder) = expected.writable_lease.holder_identity.as_ref() else {
        return Ok(None);
    };
    if holder.split('/').next() != Some(expected.source_instance_id.as_str()) {
        return Err(AgentStatusError::LeaseMismatch);
    }
    if expected.writable_lease.transitions == 0 {
        return Err(AgentStatusError::LeaseMismatch);
    }
    DurableWritableGeneration::new(
        expected.cluster_id.clone(),
        expected.cluster_uid.clone(),
        ShardId(expected.shard_id),
        expected.writable_lease.namespace.clone(),
        expected.writable_lease.name.clone(),
        expected.writable_lease.uid.clone(),
        holder.clone(),
        expected.writable_lease.transitions,
    )
    .map(Some)
    .map_err(|_| AgentStatusError::LeaseMismatch)
}

fn generation_matches(value: &str, expected: &DurableWritableGeneration) -> bool {
    DurableWritableGeneration::parse_canonical(value.as_bytes()).as_ref() == Some(expected)
}

fn canonical_uid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
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

fn canonical_boot_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
            }
        })
}

/// One bounded collection failure.
#[derive(Debug, Error)]
pub(crate) enum AgentStatusError {
    #[error("agent status target set is empty or exceeds 640 members")]
    InvalidTargetSet,
    #[error("agent status request task failed")]
    RequestTaskFailed,
    #[error("agent status request exceeded its one-second monotonic deadline")]
    RequestTimedOut,
    #[error("connect to agent status endpoint failed: {0}")]
    Connect(std::io::Error),
    #[error("write agent status request failed: {0}")]
    Write(std::io::Error),
    #[error("read agent status response failed: {0}")]
    Read(std::io::Error),
    #[error("agent status response headers exceed 16 KiB")]
    HeadersTooLarge,
    #[error("agent status response body exceeds 64 KiB")]
    BodyTooLarge,
    #[error("agent status response was truncated")]
    TruncatedResponse,
    #[error("agent status response contains bytes after its declared body")]
    UnexpectedTrailingBytes,
    #[error("agent status response headers are malformed")]
    InvalidHeaders,
    #[error("agent status response did not return HTTP/1.1 200")]
    UnexpectedHttpStatus,
    #[error("agent status response uses unsupported framing, encoding, redirect, or upgrade")]
    UnsupportedResponseFraming,
    #[error("agent status response repeats a singleton header")]
    DuplicateHeader,
    #[error("agent status response is not non-cacheable application/json")]
    UnexpectedContentType,
    #[error("agent status response requires Content-Length")]
    ContentLengthRequired,
    #[error("agent status response has a noncanonical Content-Length")]
    InvalidContentLength,
    #[error("agent status JSON is invalid: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("agent status schema does not match")]
    SchemaMismatch,
    #[error("agent status identity does not match the bound Pod")]
    IdentityMismatch,
    #[error("agent activation configuration does not match topology")]
    ConfigurationMismatch,
    #[error("agent PostgreSQL process identity is inconsistent")]
    ProcessMismatch,
    #[error("agent writable Lease evidence does not match Kubernetes")]
    LeaseMismatch,
    #[error("agent replication evidence is inconsistent")]
    ReplicationEvidenceMismatch,
    #[error("agent replication evidence is present for only part of a shard")]
    PartialReplicationEvidence,
    #[error("agent target-fence acknowledgement is inconsistent")]
    AcknowledgementMismatch,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, Semaphore};

    use super::*;

    struct ActiveJob {
        active: Arc<AtomicUsize>,
    }

    impl ActiveJob {
        fn begin(
            active: &Arc<AtomicUsize>,
            peak: &Arc<AtomicUsize>,
            started: &Arc<Notify>,
        ) -> Self {
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(current, Ordering::SeqCst);
            started.notify_one();
            Self {
                active: Arc::clone(active),
            }
        }
    }

    impl Drop for ActiveJob {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn expectation() -> AgentStatusExpectation {
        AgentStatusExpectation {
            cluster_id: "demo".to_owned(),
            cluster_uid: "11111111-2222-3333-4444-555555555555".to_owned(),
            shard_id: 0,
            member_ordinal: 0,
            instance_id: "demo-shard-0000-0".to_owned(),
            pod_uid: "pod-uid-0".to_owned(),
            source_instance_id: "demo-shard-0000-0".to_owned(),
            source_dns_name: "demo-shard-0000-0.demo-shard-0000.database.svc".to_owned(),
            member_slot_name: "pgshard_member_0000".to_owned(),
            standby_slot_names: vec![
                "pgshard_member_0001".to_owned(),
                "pgshard_member_0002".to_owned(),
            ],
            synchronous_durability: true,
            writable_lease: ExpectedWritableLease {
                namespace: "database".to_owned(),
                name: "demo-shard-0000-term".to_owned(),
                uid: "lease-uid-0".to_owned(),
                holder_identity: None,
                transitions: 0,
            },
        }
    }

    fn source_status() -> Value {
        json!({
            "schema_version": AGENT_STATUS_SCHEMA_VERSION,
            "identity": {
                "cluster_id": "demo",
                "shard_id": 0,
                "instance_id": "demo-shard-0000-0"
            },
            "postgres": null,
            "postgres_process": "validated",
            "lease": null,
            "replication_evidence": null,
            "activation_config": {
                "identity": {
                    "cluster_id": "demo",
                    "shard_id": 0,
                    "instance_id": "demo-shard-0000-0"
                },
                "cluster_uid": "11111111-2222-3333-4444-555555555555",
                "pod_uid": "pod-uid-0",
                "role": "source",
                "lease_namespace": "database",
                "lease_name": "demo-shard-0000-term",
                "lease_uid": "lease-uid-0",
                "durability": {
                    "mode": "remote_apply_any_one",
                    "candidates": ["pgshard_member_0001", "pgshard_member_0002"]
                },
                "target_fence_required_margin_ms": "3500"
            },
            "target_fence_acknowledgement": null,
            "postgres_process_identity": null
        })
    }

    fn running_source_status() -> (AgentStatusExpectation, Value) {
        let mut expected = expectation();
        expected.writable_lease.holder_identity =
            Some("demo-shard-0000-0/pod-uid-0/0123456789abcdef01234567".to_owned());
        expected.writable_lease.transitions = 7;
        let generation = String::from_utf8(
            expected_generation(&expected)
                .expect("generation validation")
                .expect("held generation")
                .canonical_bytes(),
        )
        .expect("canonical UTF-8 generation");
        let mut value = source_status();
        value["postgres_process"] = json!("running_replication_bootstrap");
        value["lease"] = json!({
            "owner_instance": "demo-shard-0000-0",
            "epoch": "7",
            "valid_until_unix_ms": "10000"
        });
        value["postgres"] = json!({
            "role": "primary",
            "timeline": 1,
            "fencing_epoch": "7",
            "observed_at_unix_ms": "1",
            "flush_lsn": "100",
            "replay_lsn": null
        });
        value["replication_evidence"] = json!({
            "role": "source",
            "observed_at_unix_ms": "1",
            "system_identifier": "1",
            "timeline": 1,
            "in_recovery": false,
            "generation_identity": generation,
            "generation_barrier_lsn": "100",
            "durability": {
                "mode": "remote_apply_any_one",
                "candidates": ["pgshard_member_0001", "pgshard_member_0002"]
            },
            "candidates": [
                {
                    "member_slot_name": "pgshard_member_0001",
                    "slot_active": false,
                    "slot_walsender_match": false,
                    "stream_state": null,
                    "sync_state": null,
                    "flush_lsn": null,
                    "replay_lsn": null
                },
                {
                    "member_slot_name": "pgshard_member_0002",
                    "slot_active": true,
                    "slot_walsender_match": true,
                    "stream_state": "streaming",
                    "sync_state": "sync",
                    "flush_lsn": "100",
                    "replay_lsn": "100"
                }
            ]
        });
        value["target_fence_acknowledgement"] = json!({
            "observed_at_unix_ms": "1",
            "generation_identity": value["replication_evidence"]["generation_identity"],
            "deadline_boottime_ns": "1",
            "remaining_validity_at_ack_ms": "3500",
            "boot_id": "11111111-2222-3333-4444-555555555555",
            "postmaster_pid": 10,
            "control_backend_pid": 11
        });
        value["postgres_process_identity"] = json!({
            "postmaster_pid": 10,
            "boot_id": "11111111-2222-3333-4444-555555555555"
        });
        (expected, value)
    }

    fn running_quarantine_status() -> (AgentStatusExpectation, Value) {
        let (mut expected, source) = running_source_status();
        expected.standby_slot_names.clear();
        expected.synchronous_durability = false;
        let status = json!({
            "schema_version": AGENT_STATUS_SCHEMA_VERSION,
            "identity": source["identity"],
            "postgres": null,
            "postgres_process": "running_quarantined",
            "lease": source["lease"],
            "replication_evidence": null,
            "activation_config": null,
            "target_fence_acknowledgement": null,
            "postgres_process_identity": source["postgres_process_identity"]
        });
        (expected, status)
    }

    fn running_standby_status() -> (AgentStatusExpectation, Value) {
        let (mut expected, source) = running_source_status();
        expected.member_ordinal = 1;
        expected.instance_id = "demo-shard-0000-m0001-0".to_owned();
        expected.pod_uid = "pod-uid-1".to_owned();
        expected.member_slot_name = "pgshard_member_0001".to_owned();
        let generation = source["replication_evidence"]["generation_identity"].clone();
        let status = json!({
            "schema_version": AGENT_STATUS_SCHEMA_VERSION,
            "identity": {
                "cluster_id": "demo",
                "shard_id": 0,
                "instance_id": "demo-shard-0000-m0001-0"
            },
            "postgres": {
                "role": "replica",
                "timeline": 1,
                "fencing_epoch": "7",
                "observed_at_unix_ms": "1",
                "flush_lsn": null,
                "replay_lsn": "100"
            },
            "postgres_process": "running_replication_standby",
            "lease": null,
            "replication_evidence": {
                "role": "standby",
                "observed_at_unix_ms": "1",
                "system_identifier": "1",
                "timeline": 1,
                "in_recovery": true,
                "generation_identity": generation,
                "member_slot_name": "pgshard_member_0001",
                "receive_lsn": "100",
                "replay_lsn": "100"
            },
            "activation_config": {
                "identity": {
                    "cluster_id": "demo",
                    "shard_id": 0,
                    "instance_id": "demo-shard-0000-m0001-0"
                },
                "cluster_uid": "11111111-2222-3333-4444-555555555555",
                "pod_uid": "pod-uid-1",
                "role": "standby",
                "primary_host": "demo-shard-0000-0.demo-shard-0000.database.svc",
                "primary_port": 5432,
                "member_slot_name": "pgshard_member_0001"
            },
            "target_fence_acknowledgement": null,
            "postgres_process_identity": {
                "postmaster_pid": 20,
                "boot_id": "22222222-3333-4444-5555-666666666666"
            }
        });
        (expected, status)
    }

    fn correlated_observations() -> Vec<CollectedAgentStatus> {
        let receipt = Instant::now();
        vec![
            CollectedAgentStatus {
                shard_id: 0,
                member_ordinal: 0,
                receipt,
                replication: Some(MemberReplicationEvidence::Source {
                    system_identifier: 42,
                    timeline: 3,
                    generation_identity: "generation-7".to_owned(),
                    generation_barrier_lsn: 100,
                }),
            },
            CollectedAgentStatus {
                shard_id: 0,
                member_ordinal: 1,
                receipt,
                replication: Some(MemberReplicationEvidence::Standby {
                    system_identifier: 42,
                    timeline: 3,
                    generation_identity: "generation-7".to_owned(),
                    receive_lsn: 120,
                    replay_lsn: 110,
                }),
            },
        ]
    }

    #[test]
    fn accepts_the_exact_schema_and_rejects_wire_drift() {
        let status: AgentStatusV1 =
            serde_json::from_value(source_status()).expect("exact status wire");
        validate_status(&status, &expectation()).expect("bound source status");

        let mut unknown = source_status();
        unknown
            .as_object_mut()
            .expect("status object")
            .insert("future_field".to_owned(), json!(true));
        assert!(serde_json::from_value::<AgentStatusV1>(unknown).is_err());

        let mut numeric = source_status();
        numeric["activation_config"]["target_fence_required_margin_ms"] = json!(3500);
        assert!(serde_json::from_value::<AgentStatusV1>(numeric).is_err());
    }

    #[test]
    fn running_source_requires_the_bracketed_fence_and_exact_ack() {
        let (expected, value) = running_source_status();
        let status: AgentStatusV1 =
            serde_json::from_value(value.clone()).expect("running source wire");
        validate_status(&status, &expected).expect("coherent running source");

        let mut wrong_fence = value.clone();
        wrong_fence["postgres"]["fencing_epoch"] = json!("8");
        let status = serde_json::from_value(wrong_fence).expect("wrong-fence wire");
        assert!(matches!(
            validate_status(&status, &expected),
            Err(AgentStatusError::ProcessMismatch)
        ));

        let mut missing_ack = value.clone();
        missing_ack["target_fence_acknowledgement"] = Value::Null;
        let status = serde_json::from_value(missing_ack).expect("missing-ACK wire");
        assert!(matches!(
            validate_status(&status, &expected),
            Err(AgentStatusError::AcknowledgementMismatch)
        ));

        let mut short_ack = value;
        short_ack["target_fence_acknowledgement"]["remaining_validity_at_ack_ms"] = json!("3499");
        let status = serde_json::from_value(short_ack).expect("short-ACK wire");
        assert!(matches!(
            validate_status(&status, &expected),
            Err(AgentStatusError::AcknowledgementMismatch)
        ));
    }

    #[test]
    fn singleton_quarantine_requires_held_lease_and_exact_process_shape() {
        let (expected, value) = running_quarantine_status();
        let status: AgentStatusV1 =
            serde_json::from_value(value.clone()).expect("running quarantine wire");
        validate_status(&status, &expected).expect("coherent running quarantine");

        let mut starting = value.clone();
        starting["postgres_process"] = json!("starting_quarantined");
        let status = serde_json::from_value(starting).expect("starting quarantine wire");
        validate_status(&status, &expected).expect("coherent starting quarantine");

        let mut missing_lease = value.clone();
        missing_lease["lease"] = Value::Null;
        let status = serde_json::from_value(missing_lease).expect("missing-Lease wire");
        assert!(matches!(
            validate_status(&status, &expected),
            Err(AgentStatusError::LeaseMismatch)
        ));

        let mut missing_process = value.clone();
        missing_process["postgres_process_identity"] = Value::Null;
        let status = serde_json::from_value(missing_process).expect("missing-process wire");
        assert!(matches!(
            validate_status(&status, &expected),
            Err(AgentStatusError::ProcessMismatch)
        ));

        for (field, replacement) in [
            ("owner_instance", json!("demo-shard-0000-m0001-0")),
            ("epoch", json!("8")),
            ("valid_until_unix_ms", json!("0")),
        ] {
            let mut wrong_lease = value.clone();
            wrong_lease["lease"][field] = replacement;
            let status = serde_json::from_value(wrong_lease).expect("wrong-Lease wire");
            assert!(matches!(
                validate_status(&status, &expected),
                Err(AgentStatusError::LeaseMismatch)
            ));
        }

        let mut no_expected_holder = expected.clone();
        no_expected_holder.writable_lease.holder_identity = None;
        assert!(matches!(
            validate_status(
                &serde_json::from_value(value.clone()).expect("released expectation wire"),
                &no_expected_holder
            ),
            Err(AgentStatusError::LeaseMismatch)
        ));

        let mut wrong_lifecycle = value.clone();
        wrong_lifecycle["postgres_process"] = json!("running_replication_bootstrap");
        let status = serde_json::from_value(wrong_lifecycle).expect("wrong-lifecycle wire");
        assert!(matches!(
            validate_status(&status, &expected),
            Err(AgentStatusError::ProcessMismatch)
        ));

        let (_, source) = running_source_status();
        for field in [
            "activation_config",
            "postgres",
            "replication_evidence",
            "target_fence_acknowledgement",
        ] {
            let mut unexpected = value.clone();
            unexpected[field] = source[field].clone();
            let status = serde_json::from_value(unexpected).expect("unexpected evidence wire");
            assert!(matches!(
                validate_status(&status, &expected),
                Err(AgentStatusError::ConfigurationMismatch)
            ));
        }
    }

    #[test]
    fn source_candidate_shape_is_cross_validated() {
        let (expected, mut value) = running_source_status();
        value["replication_evidence"]["candidates"][0]["stream_state"] = json!("streaming");
        let status = serde_json::from_value(value).expect("candidate mismatch wire");
        assert!(matches!(
            validate_status(&status, &expected),
            Err(AgentStatusError::ReplicationEvidenceMismatch)
        ));
    }

    #[test]
    fn standby_replication_positions_must_be_nonzero_and_locally_ordered() {
        let (expected, value) = running_standby_status();
        let status = serde_json::from_value(value.clone()).expect("standby wire");
        validate_status(&status, &expected).expect("valid standby evidence");

        for field in ["receive_lsn", "replay_lsn"] {
            let mut invalid = value.clone();
            invalid["replication_evidence"][field] = json!("0");
            let status = serde_json::from_value(invalid).expect("zero-LSN wire");
            assert!(matches!(
                validate_status(&status, &expected),
                Err(AgentStatusError::ReplicationEvidenceMismatch)
            ));
        }

        let mut invalid = value;
        invalid["replication_evidence"]["receive_lsn"] = json!("99");
        invalid["replication_evidence"]["replay_lsn"] = json!("100");
        let status = serde_json::from_value(invalid).expect("regressive-LSN wire");
        assert!(matches!(
            validate_status(&status, &expected),
            Err(AgentStatusError::ReplicationEvidenceMismatch)
        ));
    }

    #[test]
    fn correlates_complete_replication_evidence_and_accepts_wholly_missing_evidence() {
        validate_replication_correlation(&correlated_observations())
            .expect("coherent shard evidence");

        let mut missing = correlated_observations();
        for observation in &mut missing {
            observation.replication = None;
        }
        validate_replication_correlation(&missing).expect("optional evidence wholly absent");

        let mut partial = correlated_observations();
        partial[1].replication = None;
        assert!(matches!(
            validate_replication_correlation(&partial),
            Err(AgentStatusError::PartialReplicationEvidence)
        ));
    }

    #[test]
    fn rejects_cross_member_system_timeline_generation_and_lsn_mismatches() {
        for mismatch in ["system", "timeline", "generation", "lsn"] {
            let mut observations = correlated_observations();
            let Some(MemberReplicationEvidence::Standby {
                system_identifier,
                timeline,
                generation_identity,
                receive_lsn,
                replay_lsn,
            }) = observations[1].replication.as_mut()
            else {
                panic!("standby fixture")
            };
            match mismatch {
                "system" => *system_identifier += 1,
                "timeline" => *timeline += 1,
                "generation" => generation_identity.push_str("-other"),
                "lsn" => *replay_lsn = *receive_lsn + 1,
                _ => unreachable!(),
            }
            assert!(matches!(
                validate_replication_correlation(&observations),
                Err(AgentStatusError::ReplicationEvidenceMismatch)
            ));
        }
    }

    #[tokio::test]
    async fn bounded_scheduler_completes_640_gated_jobs_at_exact_concurrency_16() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let gate = Arc::new(Semaphore::new(0));
        let worker = {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            let started = Arc::clone(&started);
            let gate = Arc::clone(&gate);
            move |job| {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let started = Arc::clone(&started);
                let gate = Arc::clone(&gate);
                async move {
                    let _active_job = ActiveJob::begin(&active, &peak, &started);
                    gate.acquire_owned()
                        .await
                        .map_err(|_| AgentStatusError::RequestTaskFailed)?
                        .forget();
                    Ok(job)
                }
            }
        };
        let scheduler = tokio::spawn(run_bounded(
            (0..MAXIMUM_AGENT_STATUS_TARGETS).collect(),
            MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS,
            worker,
        ));
        while active.load(Ordering::SeqCst) < MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS {
            started.notified().await;
        }
        assert_eq!(
            active.load(Ordering::SeqCst),
            MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS,
        );
        assert_eq!(
            peak.load(Ordering::SeqCst),
            MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS,
        );

        gate.add_permits(MAXIMUM_AGENT_STATUS_TARGETS);
        let mut outputs = scheduler
            .await
            .expect("scheduler task")
            .expect("complete bounded work");
        outputs.sort_unstable();
        assert_eq!(
            outputs,
            (0..MAXIMUM_AGENT_STATUS_TARGETS).collect::<Vec<_>>()
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(
            peak.load(Ordering::SeqCst),
            MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS,
        );
    }

    #[tokio::test]
    async fn bounded_scheduler_error_aborts_and_drains_without_late_output() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let fail = Arc::new(Notify::new());
        let completion_gate = Arc::new(Semaphore::new(0));
        let late_outputs = Arc::new(AtomicUsize::new(0));
        let worker = {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            let started = Arc::clone(&started);
            let fail = Arc::clone(&fail);
            let completion_gate = Arc::clone(&completion_gate);
            let late_outputs = Arc::clone(&late_outputs);
            move |job| {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let started = Arc::clone(&started);
                let fail = Arc::clone(&fail);
                let completion_gate = Arc::clone(&completion_gate);
                let late_outputs = Arc::clone(&late_outputs);
                async move {
                    let _active_job = ActiveJob::begin(&active, &peak, &started);
                    if job == 0 {
                        fail.notified().await;
                        return Err(AgentStatusError::InvalidTargetSet);
                    }
                    completion_gate
                        .acquire_owned()
                        .await
                        .map_err(|_| AgentStatusError::RequestTaskFailed)?
                        .forget();
                    late_outputs.fetch_add(1, Ordering::SeqCst);
                    Ok(job)
                }
            }
        };
        let scheduler = tokio::spawn(run_bounded(
            (0..MAXIMUM_AGENT_STATUS_TARGETS).collect(),
            MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS,
            worker,
        ));
        while active.load(Ordering::SeqCst) < MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS {
            started.notified().await;
        }
        fail.notify_one();
        assert!(matches!(
            scheduler.await.expect("scheduler task"),
            Err(AgentStatusError::InvalidTargetSet)
        ));
        assert_eq!(active.load(Ordering::SeqCst), 0, "all tasks were drained");
        assert_eq!(
            peak.load(Ordering::SeqCst),
            MAXIMUM_CONCURRENT_AGENT_STATUS_REQUESTS,
        );

        completion_gate.add_permits(MAXIMUM_AGENT_STATUS_TARGETS);
        tokio::task::yield_now().await;
        assert_eq!(late_outputs.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn accepts_only_exact_bounded_http_framing() {
        assert_eq!(
            parse_response_headers(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nContent-Length: 12\r\n\r\n"
            )
            .expect("canonical headers"),
            12,
        );
        assert!(matches!(
            parse_response_headers(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nTransfer-Encoding: chunked\r\nContent-Length: 12\r\n\r\n"
            ),
            Err(AgentStatusError::UnsupportedResponseFraming)
        ));
        assert!(matches!(
            parse_response_headers(
                b"HTTP/1.1 302 Found\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nContent-Length: 0\r\n\r\n"
            ),
            Err(AgentStatusError::UnexpectedHttpStatus)
        ));
    }

    #[tokio::test]
    async fn requests_the_fixed_status_path_at_the_direct_socket_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("listener address");
        let body = serde_json::to_vec(&source_status()).expect("status body");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 256];
                let count = stream.read(&mut chunk).await.expect("request read");
                assert_ne!(count, 0, "complete request");
                request.extend_from_slice(&chunk[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            assert_eq!(
                request,
                format!(
                    "GET /status HTTP/1.1\r\nHost: {address}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
                )
                .as_bytes()
            );
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await.expect("headers");
            stream.write_all(&body).await.expect("body");
        });

        let observation = collect_one(AgentStatusQuery {
            address,
            expected: expectation(),
        })
        .await
        .expect("direct status request");
        assert!(observation.receipt <= Instant::now());
        assert_eq!(observation.shard_id, 0);
        assert_eq!(observation.member_ordinal, 0);
        assert!(observation.replication.is_none());
        server.await.expect("server task");
    }
}
