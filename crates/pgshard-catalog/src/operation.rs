//! Durable, immutable operation acceptance in the authoritative catalog.
//!
//! This repository is intentionally not composed into the orchestrator yet.
//! It establishes the storage and ambiguous-commit contract without claiming
//! that operation execution, leases, or recovery are durable.

use std::fmt;

use pgshard_types::ShardId;
use thiserror::Error;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, IsolationLevel, Row};
use uuid::Uuid;

use crate::ClusterId;

/// Maximum exact operation-specific payload retained by the catalog.
pub const MAX_OPERATION_PAYLOAD_BYTES: usize = 64 * 1024;

const IDLE_SESSION_PROBE: &str = "SAVEPOINT pgshard_operation_acceptance_idle_probe; \
                                  RELEASE SAVEPOINT pgshard_operation_acceptance_idle_probe";

/// A canonical, non-nil operation UUID from the public wire contract.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(Uuid);

impl OperationId {
    /// Parses the exact lowercase, hyphenated wire representation.
    ///
    /// Alternate textual UUID spellings are rejected instead of normalized
    /// into the same idempotency key.
    ///
    /// # Errors
    ///
    /// Returns [`OperationRequestError::InvalidOperationId`] for a nil UUID or
    /// a non-canonical representation.
    pub fn parse_wire(value: &str) -> Result<Self, OperationRequestError> {
        let parsed = Uuid::parse_str(value)
            .map_err(|_| OperationRequestError::InvalidOperationId(value.to_owned()))?;
        if parsed.is_nil() || parsed.hyphenated().to_string() != value {
            return Err(OperationRequestError::InvalidOperationId(value.to_owned()));
        }
        Ok(Self(parsed))
    }

    /// Returns the UUID value used by the catalog.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Immutable Kubernetes cluster incarnation trusted by the caller.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KubernetesClusterUid(String);

impl KubernetesClusterUid {
    /// Creates a bounded Kubernetes UID containing only portable characters.
    ///
    /// # Errors
    ///
    /// Returns [`OperationRequestError::InvalidKubernetesClusterUid`] when the
    /// value is empty, longer than 128 bytes, or contains an unsafe character.
    pub fn new(value: impl Into<String>) -> Result<Self, OperationRequestError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(OperationRequestError::InvalidKubernetesClusterUid(value));
        }
        Ok(Self(value))
    }

    /// Returns the exact UID bytes supplied by the trusted identity observer.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Closed set of operation classes accepted by the Milestone 1 catalog.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OperationKind {
    /// Planned primary movement.
    Switchover,
    /// Recovery workflow.
    Failover,
    /// Coordinated backup.
    Backup,
    /// Empty-target restore.
    Restore,
    /// Managed schema operation.
    Ddl,
    /// Online shard topology change.
    Reshard,
    /// Role or grant reconciliation.
    Authorization,
}

impl OperationKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Switchover => "switchover",
            Self::Failover => "failover",
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::Ddl => "ddl",
            Self::Reshard => "reshard",
            Self::Authorization => "authorization",
        }
    }
}

/// Trusted cluster identity included in every immutable request fingerprint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationIdentity {
    catalog_cluster_id: ClusterId,
    kubernetes_cluster_uid: KubernetesClusterUid,
    cluster_name: String,
}

impl OperationIdentity {
    /// Creates a trusted identity for one catalog and Kubernetes incarnation.
    ///
    /// # Errors
    ///
    /// Returns [`OperationRequestError::InvalidClusterName`] unless the logical
    /// cluster name is a lowercase DNS label of 1 through 63 bytes.
    pub fn new(
        catalog_cluster_id: ClusterId,
        kubernetes_cluster_uid: KubernetesClusterUid,
        cluster_name: impl Into<String>,
    ) -> Result<Self, OperationRequestError> {
        let cluster_name = cluster_name.into();
        let bytes = cluster_name.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 63
            || !bytes[0].is_ascii_alphanumeric()
            || !bytes[bytes.len() - 1].is_ascii_alphanumeric()
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
        {
            return Err(OperationRequestError::InvalidClusterName(cluster_name));
        }
        Ok(Self {
            catalog_cluster_id,
            kubernetes_cluster_uid,
            cluster_name,
        })
    }
}

/// Positive, PostgreSQL-encodable request preconditions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationPreconditions {
    required_catalog_epoch: i64,
    required_fencing_epoch: i64,
    deadline_unix_micros: i64,
}

impl OperationPreconditions {
    /// Validates positive values that fit `PostgreSQL` `bigint` exactly.
    ///
    /// # Errors
    ///
    /// Returns [`OperationRequestError::InvalidPositiveBigint`] for zero or a
    /// value greater than `i64::MAX`.
    pub fn new(
        required_catalog_epoch: u64,
        required_fencing_epoch: u64,
        deadline_unix_micros: u64,
    ) -> Result<Self, OperationRequestError> {
        Ok(Self {
            required_catalog_epoch: positive_bigint(
                "required_catalog_epoch",
                required_catalog_epoch,
            )?,
            required_fencing_epoch: positive_bigint(
                "required_fencing_epoch",
                required_fencing_epoch,
            )?,
            deadline_unix_micros: positive_bigint("deadline_unix_micros", deadline_unix_micros)?,
        })
    }
}

fn positive_bigint(field: &'static str, value: u64) -> Result<i64, OperationRequestError> {
    i64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OperationRequestError::InvalidPositiveBigint { field, value })
}

/// Exact immutable request accepted by the authoritative catalog.
#[derive(Clone, Eq, PartialEq)]
pub struct OperationRequest {
    operation_id: OperationId,
    identity: OperationIdentity,
    shard_id: ShardId,
    kind: OperationKind,
    payload: Vec<u8>,
    preconditions: OperationPreconditions,
}

impl fmt::Debug for OperationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OperationRequest")
            .field("operation_id", &self.operation_id)
            .field("shard_id", &self.shard_id)
            .field("kind", &self.kind)
            .field("payload_len", &self.payload.len())
            .field(
                "required_catalog_epoch",
                &self.preconditions.required_catalog_epoch,
            )
            .field(
                "required_fencing_epoch",
                &self.preconditions.required_fencing_epoch,
            )
            .field(
                "deadline_unix_micros",
                &self.preconditions.deadline_unix_micros,
            )
            .finish_non_exhaustive()
    }
}

impl OperationRequest {
    /// Creates one bounded operation request.
    ///
    /// # Errors
    ///
    /// Returns [`OperationRequestError::PayloadTooLarge`] above 64 KiB.
    pub fn new(
        operation_id: OperationId,
        identity: OperationIdentity,
        shard_id: ShardId,
        kind: OperationKind,
        payload: Vec<u8>,
        preconditions: OperationPreconditions,
    ) -> Result<Self, OperationRequestError> {
        if payload.len() > MAX_OPERATION_PAYLOAD_BYTES {
            return Err(OperationRequestError::PayloadTooLarge(payload.len()));
        }
        Ok(Self {
            operation_id,
            identity,
            shard_id,
            kind,
            payload,
            preconditions,
        })
    }

    /// Returns the global operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }
}

/// Client-side validation failure before any catalog transaction begins.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum OperationRequestError {
    /// The wire UUID was nil or not exact lowercase hyphenated form.
    #[error("operation ID {0:?} must be a canonical lowercase, hyphenated, non-nil UUID")]
    InvalidOperationId(String),
    /// The Kubernetes identity was empty, oversized, or contained unsafe bytes.
    #[error("Kubernetes cluster UID {0:?} must contain 1-128 portable ASCII bytes")]
    InvalidKubernetesClusterUid(String),
    /// The cluster name was not a lowercase DNS label.
    #[error("cluster name {0:?} must be a 1-63 byte lowercase DNS label")]
    InvalidClusterName(String),
    /// A required positive value cannot be encoded losslessly in `PostgreSQL`.
    #[error("{field} value {value} must be between 1 and i64::MAX")]
    InvalidPositiveBigint {
        /// Rejected field.
        field: &'static str,
        /// Rejected value.
        value: u64,
    },
    /// Exact request payload exceeded the catalog bound.
    #[error("operation payload is {0} bytes; maximum is 65536")]
    PayloadTooLarge(usize),
}

/// Durable catalog result of one acceptance attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptanceOutcome {
    /// A new immutable request and pending status were committed.
    Accepted,
    /// Every stored field exactly matched the already accepted request.
    Replay,
    /// The UUID was already reserved by different immutable bytes.
    Conflict,
}

/// Only phase established by this uncomposed repository slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationPhase {
    /// Accepted but not wired to an executor.
    Pending,
}

/// Receipt returned only after the acceptance transaction commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptanceReceipt {
    /// Mutually exclusive acceptance result.
    pub outcome: AcceptanceOutcome,
    /// Exact global operation identity.
    pub operation_id: OperationId,
    /// Server-computed, versioned SHA-256 request fingerprint.
    pub request_fingerprint: [u8; 32],
    /// Catalog acceptance time in Unix microseconds.
    ///
    /// A legacy tombstone reserves its UUID but has no trustworthy acceptance
    /// timestamp, so its conflict receipt returns `None`.
    pub accepted_at_unix_micros: Option<u64>,
}

/// Bounded operation state visible through the writer-only routine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedOperation {
    /// Exact global operation identity.
    pub operation_id: OperationId,
    /// Server-computed, versioned SHA-256 request fingerprint.
    pub request_fingerprint: [u8; 32],
    /// The only phase supported by this slice.
    pub phase: OperationPhase,
    /// Catalog acceptance time in Unix microseconds.
    pub accepted_at_unix_micros: u64,
}

/// Uncomposed durable operation acceptance repository.
#[derive(Clone, Copy, Debug, Default)]
pub struct OperationRepository;

impl OperationRepository {
    /// Atomically accepts an exact request, detects a replay, or reports a
    /// permanent UUID conflict.
    ///
    /// A commit error is always [`OperationAcceptanceError::OutcomeUnknown`].
    /// Reconciliation must use the exact same request on a fresh session; a
    /// changed request is never a retry.
    ///
    /// The supplied client must be idle. Acceptance rejects a client that is
    /// already inside a transaction so its COMMIT cannot close unrelated
    /// caller work.
    ///
    /// # Errors
    ///
    /// Returns a session-not-idle error before acceptance starts, a database
    /// error before COMMIT, an outcome-unknown error for any COMMIT failure, or
    /// an invalid-response error for a broken catalog ABI.
    pub async fn accept(
        &self,
        client: &mut Client,
        request: &OperationRequest,
    ) -> Result<AcceptanceReceipt, OperationAcceptanceError> {
        require_idle_acceptance_session(client).await?;

        let transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await
            .map_err(OperationAcceptanceError::Database)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .await
            .map_err(OperationAcceptanceError::Database)?;

        let operation_id = request.operation_id.as_uuid().hyphenated().to_string();
        let catalog_cluster_id = request
            .identity
            .catalog_cluster_id
            .as_uuid()
            .hyphenated()
            .to_string();
        let shard_id = i64::from(request.shard_id.0);
        let row = transaction
            .query_one(
                "SELECT acceptance, request_fingerprint, accepted_at_unix_micros \
                   FROM pgshard_catalog.accept_operation( \
                       $1::text::uuid, $2::text::uuid, $3::text, $4::text, $5::bigint, \
                       $6::text, $7::bytea, $8::bigint, $9::bigint, $10::bigint \
                   )",
                &[
                    &operation_id,
                    &catalog_cluster_id,
                    &request.identity.kubernetes_cluster_uid.as_str(),
                    &request.identity.cluster_name,
                    &shard_id,
                    &request.kind.as_str(),
                    &request.payload,
                    &request.preconditions.required_catalog_epoch,
                    &request.preconditions.required_fencing_epoch,
                    &request.preconditions.deadline_unix_micros,
                ],
            )
            .await
            .map_err(OperationAcceptanceError::Database)?;
        let receipt = acceptance_receipt(request.operation_id, &row)?;

        transaction
            .commit()
            .await
            .map_err(|source| OperationAcceptanceError::OutcomeUnknown {
                operation_id: request.operation_id,
                source,
            })?;
        Ok(receipt)
    }

    /// Loads bounded acceptance state without exposing the raw request body.
    ///
    /// # Errors
    ///
    /// Returns a database error or an invalid-response error for a broken
    /// catalog ABI.
    pub async fn get(
        &self,
        client: &Client,
        operation_id: OperationId,
    ) -> Result<Option<AcceptedOperation>, OperationAcceptanceError> {
        let operation_id_text = operation_id.as_uuid().hyphenated().to_string();
        let row = client
            .query_opt(
                "SELECT request_fingerprint, phase, accepted_at_unix_micros \
                   FROM pgshard_catalog.get_operation($1::text::uuid)",
                &[&operation_id_text],
            )
            .await
            .map_err(OperationAcceptanceError::Database)?;
        row.map(|row| accepted_operation(operation_id, &row))
            .transpose()
    }
}

async fn require_idle_acceptance_session(client: &Client) -> Result<(), OperationAcceptanceError> {
    match client.batch_execute(IDLE_SESSION_PROBE).await {
        Err(error) if error.code() == Some(&SqlState::NO_ACTIVE_SQL_TRANSACTION) => Ok(()),
        Ok(()) => Err(OperationAcceptanceError::SessionNotIdle),
        Err(error) if error.code() == Some(&SqlState::IN_FAILED_SQL_TRANSACTION) => {
            Err(OperationAcceptanceError::SessionNotIdle)
        }
        Err(error) => Err(OperationAcceptanceError::Database(error)),
    }
}

fn acceptance_receipt(
    operation_id: OperationId,
    row: &Row,
) -> Result<AcceptanceReceipt, OperationAcceptanceError> {
    let outcome_text = row
        .try_get::<_, String>(0)
        .map_err(|_| OperationAcceptanceError::InvalidCatalogResponse("acceptance"))?;
    let outcome = match outcome_text.as_str() {
        "accepted" => AcceptanceOutcome::Accepted,
        "replay" => AcceptanceOutcome::Replay,
        "conflict" => AcceptanceOutcome::Conflict,
        _ => {
            return Err(OperationAcceptanceError::InvalidCatalogResponse(
                "acceptance",
            ));
        }
    };
    let accepted_at_unix_micros = optional_positive_timestamp(row, 2)?;
    if matches!(
        outcome,
        AcceptanceOutcome::Accepted | AcceptanceOutcome::Replay
    ) && accepted_at_unix_micros.is_none()
    {
        return Err(OperationAcceptanceError::InvalidCatalogResponse(
            "accepted_at_unix_micros",
        ));
    }
    Ok(AcceptanceReceipt {
        outcome,
        operation_id,
        request_fingerprint: fingerprint(row, 1)?,
        accepted_at_unix_micros,
    })
}

fn accepted_operation(
    operation_id: OperationId,
    row: &Row,
) -> Result<AcceptedOperation, OperationAcceptanceError> {
    if row
        .try_get::<_, String>(1)
        .map_err(|_| OperationAcceptanceError::InvalidCatalogResponse("phase"))?
        != "pending"
    {
        return Err(OperationAcceptanceError::InvalidCatalogResponse("phase"));
    }
    Ok(AcceptedOperation {
        operation_id,
        request_fingerprint: fingerprint(row, 0)?,
        phase: OperationPhase::Pending,
        accepted_at_unix_micros: positive_timestamp(row, 2)?,
    })
}

fn fingerprint(row: &Row, index: usize) -> Result<[u8; 32], OperationAcceptanceError> {
    row.try_get::<_, Vec<u8>>(index)
        .map_err(|_| OperationAcceptanceError::InvalidCatalogResponse("request_fingerprint"))?
        .try_into()
        .map_err(|_| OperationAcceptanceError::InvalidCatalogResponse("request_fingerprint"))
}

fn positive_timestamp_value(value: i64) -> Result<u64, OperationAcceptanceError> {
    u64::try_from(value).ok().filter(|value| *value > 0).ok_or(
        OperationAcceptanceError::InvalidCatalogResponse("accepted_at_unix_micros"),
    )
}

fn positive_timestamp(row: &Row, index: usize) -> Result<u64, OperationAcceptanceError> {
    positive_timestamp_value(
        row.try_get(index).map_err(|_| {
            OperationAcceptanceError::InvalidCatalogResponse("accepted_at_unix_micros")
        })?,
    )
}

fn optional_positive_timestamp(
    row: &Row,
    index: usize,
) -> Result<Option<u64>, OperationAcceptanceError> {
    row.try_get::<_, Option<i64>>(index)
        .map_err(|_| OperationAcceptanceError::InvalidCatalogResponse("accepted_at_unix_micros"))?
        .map(positive_timestamp_value)
        .transpose()
}

/// Failure to establish a definite durable acceptance outcome.
#[derive(Debug, Error)]
pub enum OperationAcceptanceError {
    /// The borrowed client was already inside a caller-owned transaction.
    #[error("operation acceptance requires an idle PostgreSQL session")]
    SessionNotIdle,
    /// The transaction failed before COMMIT established an ambiguous boundary.
    #[error("operation acceptance database error")]
    Database(#[source] tokio_postgres::Error),
    /// COMMIT may or may not have become durable.
    #[error(
        "operation {operation_id:?} acceptance outcome is unknown; retry exact bytes on a fresh session"
    )]
    OutcomeUnknown {
        /// Exact request identity required for reconciliation.
        operation_id: OperationId,
        /// Transport or server error observed at COMMIT.
        #[source]
        source: tokio_postgres::Error,
    },
    /// A routine returned a shape outside the versioned repository ABI.
    #[error("catalog returned an invalid operation {0}")]
    InvalidCatalogResponse(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster_id() -> ClusterId {
        ClusterId::new(Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap()).unwrap()
    }

    fn operation_id() -> OperationId {
        OperationId::parse_wire("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap()
    }

    fn identity() -> OperationIdentity {
        OperationIdentity::new(
            cluster_id(),
            KubernetesClusterUid::new("cluster-uid_1.example").unwrap(),
            "cluster-a",
        )
        .unwrap()
    }

    #[test]
    fn operation_id_requires_exact_wire_form() {
        assert!(OperationId::parse_wire("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").is_ok());
        for invalid in [
            "00000000-0000-0000-0000-000000000000",
            "BBBBBBBB-BBBB-4BBB-8BBB-BBBBBBBBBBBB",
            "bbbbbbbbbbbb4bbb8bbbbbbbbbbbbbbb",
            "{bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb}",
        ] {
            assert!(
                OperationId::parse_wire(invalid).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn identity_bounds_are_exact() {
        assert!(KubernetesClusterUid::new("a".repeat(128)).is_ok());
        assert!(KubernetesClusterUid::new("a".repeat(129)).is_err());
        assert!(KubernetesClusterUid::new("unsafe uid").is_err());
        assert!(
            OperationIdentity::new(cluster_id(), KubernetesClusterUid::new("uid").unwrap(), "a")
                .is_ok()
        );
        assert!(
            OperationIdentity::new(
                cluster_id(),
                KubernetesClusterUid::new("uid").unwrap(),
                "a".repeat(63)
            )
            .is_ok()
        );
        assert!(
            OperationIdentity::new(
                cluster_id(),
                KubernetesClusterUid::new("uid").unwrap(),
                "a".repeat(64)
            )
            .is_err()
        );
        assert!(
            OperationIdentity::new(
                cluster_id(),
                KubernetesClusterUid::new("uid").unwrap(),
                "Bad"
            )
            .is_err()
        );
    }

    #[test]
    fn numeric_and_payload_bounds_are_exact() {
        assert!(OperationPreconditions::new(1, 1, 1).is_ok());
        assert!(OperationPreconditions::new(i64::MAX as u64, 1, 1).is_ok());
        assert!(OperationPreconditions::new(0, 1, 1).is_err());
        assert!(OperationPreconditions::new(i64::MAX as u64 + 1, 1, 1).is_err());

        let preconditions = OperationPreconditions::new(1, 2, 3).unwrap();
        assert!(
            OperationRequest::new(
                operation_id(),
                identity(),
                ShardId(u32::MAX),
                OperationKind::Backup,
                vec![0; MAX_OPERATION_PAYLOAD_BYTES],
                preconditions
            )
            .is_ok()
        );
        assert!(
            OperationRequest::new(
                operation_id(),
                identity(),
                ShardId(0),
                OperationKind::Backup,
                vec![0; MAX_OPERATION_PAYLOAD_BYTES + 1],
                preconditions
            )
            .is_err()
        );
    }

    #[test]
    fn operation_kind_encoding_is_closed_and_stable() {
        assert_eq!(OperationKind::Switchover.as_str(), "switchover");
        assert_eq!(OperationKind::Failover.as_str(), "failover");
        assert_eq!(OperationKind::Backup.as_str(), "backup");
        assert_eq!(OperationKind::Restore.as_str(), "restore");
        assert_eq!(OperationKind::Ddl.as_str(), "ddl");
        assert_eq!(OperationKind::Reshard.as_str(), "reshard");
        assert_eq!(OperationKind::Authorization.as_str(), "authorization");
    }

    #[test]
    fn operation_request_debug_redacts_payload_and_trust_anchors() {
        let request = OperationRequest::new(
            operation_id(),
            identity(),
            ShardId(3),
            OperationKind::Backup,
            b"do-not-log-this-secret".to_vec(),
            OperationPreconditions::new(1, 2, 3).unwrap(),
        )
        .unwrap();
        let debug = format!("{request:?}");
        assert!(debug.contains("payload_len: 22"));
        assert!(!debug.contains("do-not-log-this-secret"));
        assert!(!debug.contains("cluster-uid_1.example"));
        assert!(!debug.contains("cluster-a"));
    }
}
