//! Bounded `shardschema` loading for one standby-decoder attachment.
//!
//! This reader turns one transactionally consistent catalog row set into the
//! pure [`crate::standby_slots::StandbyDecoderPolicy`] used by attachment
//! validation. It does not select a standby, observe a shard server, create or
//! drop replication slots, mutate catalog state, or authorize a replication
//! connection.

use std::time::Duration;

use pgshard_catalog::{CatalogOperationTimeout, SHARDSCHEMA_DATABASE};
use pgshard_types::{CatalogEpoch, PgLsn};
use thiserror::Error;
use tokio::time::{Instant, timeout_at};
use tokio_postgres::{Client, IsolationLevel, Row, Transaction};
use uuid::Uuid;

use crate::parse_lsn;
use crate::standby_slots::{
    ManagedSlotTarget, ManagedSlotTargetError, ManagedTwoPhasePolicy, ReplicationSlotName,
    ReplicationSourceIdentity, SlotGeneration, SlotGenerationError, SlotNameError,
    SourceIdentityError, StandbyDecoderEvidenceLimits, StandbyDecoderPolicy,
    StandbyDecoderPolicyError, StandbyDecoderTarget, StandbyDecoderTargetError,
};

const REQUIREMENTS_SQL: &str = "\
    SELECT pg_catalog.current_setting('server_version_num')::pg_catalog.int4, \
           pg_catalog.current_database()::pg_catalog.text, \
           pg_catalog.getdatabaseencoding()::pg_catalog.text";

const SINGLETONS_SQL: &str = "\
    SELECT ( \
               SELECT configuration.cluster_id::text \
                 FROM pgshard_catalog.cluster_configuration AS configuration \
                WHERE configuration.singleton \
           ) AS cluster_id, \
           ( \
               SELECT state.catalog_epoch \
                 FROM pgshard_catalog.cluster_state AS state \
                WHERE state.singleton \
           ) AS catalog_epoch";

const READY_OWNER_SQL: &str = "\
    SELECT consumers.purpose, consumers.state AS consumer_state, \
           consumer_shards.state AS shard_state, consumer_shards.ownership_fence \
      FROM pgshard_catalog.logical_consumers AS consumers \
      JOIN pgshard_catalog.logical_consumer_shards AS consumer_shards \
        ON consumer_shards.consumer_id = consumers.consumer_id \
       AND consumer_shards.logical_database_id = consumers.logical_database_id \
       AND consumer_shards.shard_id = $3::text \
     WHERE consumers.consumer_id = $1::text::uuid \
       AND consumers.logical_database_id = $2::text::uuid \
     LIMIT 2";

const STANDBY_POLICY_SQL: &str = "\
    SELECT attachments.selected_source_role, \
           attachments.selected_source_member_ordinal, \
           checkpoints.checkpoint_generation::text AS checkpoint_generation, \
           checkpoints.checkpoint_lsn::text AS checkpoint_lsn, \
           checkpoints.checkpoint_ordinal, checkpoints.snapshot_required, \
           attachments.attachment_generation::text AS attachment_generation, \
           attachments.restore_incarnation::text AS restore_incarnation, \
           attachments.database_name::text AS database_name, \
           attachments.system_identifier::text AS system_identifier, \
           attachments.database_oid, attachments.selected_source_timeline, \
           anchors.slot_generation::text AS anchor_generation, \
           anchors.slot_name::text AS anchor_name, \
           anchors.consistent_point::text AS anchor_consistent_point, \
           anchors.two_phase_at::text AS anchor_two_phase_at, \
           decoders.slot_generation::text AS decoder_generation, \
           decoders.slot_name::text AS decoder_name, \
           decoders.consistent_point::text AS decoder_consistent_point, \
           decoders.two_phase_at::text AS decoder_two_phase_at \
      FROM pgshard_catalog.logical_consumer_attachments AS attachments \
      LEFT JOIN pgshard_catalog.logical_consumer_checkpoints AS checkpoints \
        ON checkpoints.consumer_id = attachments.consumer_id \
       AND checkpoints.logical_database_id = attachments.logical_database_id \
       AND checkpoints.shard_id = attachments.shard_id \
       AND checkpoints.state = 'current' \
       AND attachments.restore_incarnation = checkpoints.restore_incarnation \
       AND attachments.system_identifier = checkpoints.system_identifier \
       AND attachments.database_oid = checkpoints.database_oid \
       AND attachments.selected_source_timeline = checkpoints.source_timeline \
      LEFT JOIN pgshard_catalog.managed_replication_slots AS anchors \
        ON anchors.attachment_generation = attachments.attachment_generation \
       AND anchors.consumer_id = attachments.consumer_id \
       AND anchors.logical_database_id = attachments.logical_database_id \
       AND anchors.shard_id = attachments.shard_id \
       AND anchors.slot_role = 'primary-anchor' \
       AND anchors.state = 'active' \
      LEFT JOIN pgshard_catalog.managed_replication_slots AS decoders \
        ON decoders.attachment_generation = attachments.attachment_generation \
       AND decoders.consumer_id = attachments.consumer_id \
       AND decoders.logical_database_id = attachments.logical_database_id \
       AND decoders.shard_id = attachments.shard_id \
       AND decoders.slot_role = 'standby-decoder' \
       AND decoders.member_ordinal = attachments.selected_source_member_ordinal \
       AND decoders.state = 'active' \
     WHERE attachments.consumer_id = $1::text::uuid \
       AND attachments.logical_database_id = $2::text::uuid \
       AND attachments.shard_id = $3::text \
       AND attachments.state = 'active' \
     LIMIT 2";

const MIN_POSTGRES_VERSION_NUM: i32 = 180_000;
// The client deadline is authoritative. A slightly earlier statement timeout
// gives PostgreSQL time to cancel a lock wait and process ROLLBACK so the
// dedicated connection can be retried. The later PostgreSQL 18 transaction
// timeout remains the fail-closed backstop for a stalled COMMIT or backend.
const SERVER_STATEMENT_TIMEOUT_HEADROOM: Duration = Duration::from_millis(25);
const SERVER_TRANSACTION_TIMEOUT_GRACE: Duration = Duration::from_millis(101);
const PIN_SEARCH_PATH_SQL: &str = "SELECT pg_catalog.set_config('search_path', '', false)";
const SET_SESSION_TIMEOUTS_SQL: &str = "\
    SELECT pg_catalog.set_config('statement_timeout', $1, false), \
           pg_catalog.set_config('transaction_timeout', $2, false)";
const SET_LOCAL_TIMEOUTS_SQL: &str = "\
    SELECT pg_catalog.set_config('statement_timeout', $1, true), \
           pg_catalog.set_config('transaction_timeout', $2, true)";
const SET_LOCAL_STATEMENT_TIMEOUT_SQL: &str =
    "SELECT pg_catalog.set_config('statement_timeout', $1, true)";
const DISABLE_LOCAL_STATEMENT_TIMEOUT_SQL: &str = "SET LOCAL statement_timeout = 0";
const RESET_SESSION_TIMEOUTS_SQL: &str = "SET statement_timeout = 0; SET transaction_timeout = 0";

/// Stable key for one logical consumer's shard-local state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalConsumerShardKey {
    consumer: Uuid,
    logical_database: Uuid,
    shard: String,
}

impl LogicalConsumerShardKey {
    /// Creates a non-nil consumer/database key with a canonical resource name.
    ///
    /// # Errors
    ///
    /// Rejects nil UUIDs and shard names outside the catalog's lowercase
    /// resource-name contract.
    pub fn new(
        consumer_id: Uuid,
        logical_database_id: Uuid,
        shard_id: impl Into<String>,
    ) -> Result<Self, LogicalConsumerShardKeyError> {
        if consumer_id.is_nil() {
            return Err(LogicalConsumerShardKeyError::NilConsumerId);
        }
        if logical_database_id.is_nil() {
            return Err(LogicalConsumerShardKeyError::NilLogicalDatabaseId);
        }
        let shard_id = shard_id.into();
        if !valid_resource_name(&shard_id) {
            return Err(LogicalConsumerShardKeyError::InvalidShardId);
        }
        Ok(Self {
            consumer: consumer_id,
            logical_database: logical_database_id,
            shard: shard_id,
        })
    }

    /// Returns the permanent logical-consumer UUID.
    #[must_use]
    pub const fn consumer_id(&self) -> Uuid {
        self.consumer
    }

    /// Returns the stable logical-database UUID.
    #[must_use]
    pub const fn logical_database_id(&self) -> Uuid {
        self.logical_database
    }

    /// Returns the canonical shard resource name.
    #[must_use]
    pub fn shard_id(&self) -> &str {
        &self.shard
    }
}

pub(crate) fn valid_resource_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && is_ascii_lowercase_or_digit(bytes[0])
        && is_ascii_lowercase_or_digit(bytes[bytes.len() - 1])
        && bytes
            .iter()
            .all(|byte| is_ascii_lowercase_or_digit(*byte) || *byte == b'-')
}

fn is_ascii_lowercase_or_digit(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

/// Invalid logical-consumer shard key.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LogicalConsumerShardKeyError {
    /// Consumer UUID is nil.
    #[error("logical consumer ID must be non-nil")]
    NilConsumerId,
    /// Logical-database UUID is nil.
    #[error("logical database ID must be non-nil")]
    NilLogicalDatabaseId,
    /// Shard name violates the catalog domain.
    #[error("shard ID must be 1-63 lowercase ASCII letters, digits, or interior hyphens")]
    InvalidShardId,
}

/// Managed logical-consumer purpose stored in `shardschema`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalConsumerPurpose {
    /// Public VStream-like change stream.
    ChangeStream,
    /// Source-side online-resharding catch-up.
    ReshardMaterializer,
    /// Internal materialized data flow.
    InternalMaterialization,
}

/// One transactionally consistent, catalog-fenced standby policy.
#[derive(Debug)]
pub struct LoadedStandbyDecoderPolicy {
    cluster_id: Uuid,
    key: LogicalConsumerShardKey,
    purpose: LogicalConsumerPurpose,
    ownership_fence: u64,
    checkpoint_generation: Uuid,
    checkpoint_ordinal: u64,
    attachment_generation: Uuid,
    database_name: String,
    policy: StandbyDecoderPolicy,
}

impl LoadedStandbyDecoderPolicy {
    /// Returns the immutable pgshard cluster UUID.
    #[must_use]
    pub const fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }

    /// Returns the consumer/database/shard lookup key.
    #[must_use]
    pub const fn key(&self) -> &LogicalConsumerShardKey {
        &self.key
    }

    /// Returns the consumer's bounded catalog purpose.
    #[must_use]
    pub const fn purpose(&self) -> LogicalConsumerPurpose {
        self.purpose
    }

    /// Returns the per-shard ownership fence that must guard later mutations.
    #[must_use]
    pub const fn ownership_fence(&self) -> u64 {
        self.ownership_fence
    }

    /// Returns the never-reused checkpoint generation.
    #[must_use]
    pub const fn checkpoint_generation(&self) -> Uuid {
        self.checkpoint_generation
    }

    /// Returns the checkpoint compare-and-swap ordinal.
    #[must_use]
    pub const fn checkpoint_ordinal(&self) -> u64 {
        self.checkpoint_ordinal
    }

    /// Returns the immutable source-attachment generation.
    #[must_use]
    pub const fn attachment_generation(&self) -> Uuid {
        self.attachment_generation
    }

    /// Returns the exact descriptive `PostgreSQL` database name.
    #[must_use]
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Returns the pure standby eligibility policy built from this snapshot.
    #[must_use]
    pub const fn policy(&self) -> &StandbyDecoderPolicy {
        &self.policy
    }
}

/// Owned, idle `shardschema` connection for on-demand slot-policy reads.
pub struct SlotCatalogReader {
    client: Option<Client>,
    operation_timeout: CatalogOperationTimeout,
}

impl SlotCatalogReader {
    /// Takes ownership of a dedicated idle `PostgreSQL` 18 `shardschema` client.
    ///
    /// `DISCARD ALL` removes inherited roles and session state before the
    /// database, encoding, and minimum-version requirements are checked.
    /// `operation_timeout` is mandatory and bounds the reset and requirements
    /// check as well as every later transaction through `COMMIT`.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-idle client, SQL failure, wrong database,
    /// non-UTF8 catalog, or `PostgreSQL` older than 18.
    pub async fn new(
        client: Client,
        operation_timeout: CatalogOperationTimeout,
    ) -> Result<Self, SlotCatalogLoadError> {
        let timeout = operation_timeout.get();
        let deadline = Instant::now() + timeout;
        let construction = async {
            client.batch_execute("DISCARD ALL").await?;
            client.query_one(PIN_SEARCH_PATH_SQL, &[]).await?;
            set_session_timeouts(&client, deadline).await?;
            let requirements = client.query_one(REQUIREMENTS_SQL, &[]).await?;
            client.batch_execute(RESET_SESSION_TIMEOUTS_SQL).await?;
            let version: i32 = requirements.try_get(0)?;
            let database: String = requirements.try_get(1)?;
            let encoding: String = requirements.try_get(2)?;
            if version < MIN_POSTGRES_VERSION_NUM {
                return Err(SlotCatalogLoadError::UnsupportedPostgresVersion(version));
            }
            if database != SHARDSCHEMA_DATABASE {
                return Err(SlotCatalogLoadError::WrongDatabase(database));
            }
            if encoding != "UTF8" {
                return Err(SlotCatalogLoadError::WrongEncoding(encoding));
            }
            Ok(Self {
                client: Some(client),
                operation_timeout,
            })
        };
        finish_before(construction, deadline, timeout).await
    }

    /// Loads one ready, standby-selected policy in a read-only repeatable-read transaction.
    ///
    /// `Ok(None)` means the exact consumer shard is absent, fenced, assigned
    /// to another member, or currently using primary fallback. Callers must
    /// not infer which condition from an earlier snapshot.
    ///
    /// # Errors
    ///
    /// Fails closed on a deadline, SQL or typed-row error, a missing singleton
    /// or ready-policy component, catalog cardinality, conversion, identity,
    /// slot naming, snapshot/seed state, or activation-boundary error. A
    /// completed statement cancellation and rollback is retryable; an absolute
    /// client or transaction timeout closes this reader, and later calls return
    /// [`SlotCatalogLoadError::ReaderTerminated`].
    pub async fn load_standby_policy(
        &mut self,
        key: &LogicalConsumerShardKey,
        member_ordinal: u16,
        evidence_limits: StandbyDecoderEvidenceLimits,
    ) -> Result<Option<LoadedStandbyDecoderPolicy>, SlotCatalogLoadError> {
        let timeout = self.operation_timeout.get();
        let deadline = Instant::now() + timeout;
        let client = self
            .client
            .as_mut()
            .ok_or(SlotCatalogLoadError::ReaderTerminated)?;
        let operation = load_before(
            client,
            key,
            member_ordinal,
            evidence_limits,
            deadline,
            timeout,
        );
        let result = finish_before(operation, deadline, timeout).await;
        let terminal_error = result
            .as_ref()
            .is_err_and(SlotCatalogLoadError::terminates_reader);
        let closed_client = self.client.as_ref().is_some_and(Client::is_closed);
        if terminal_error || closed_client {
            self.client.take();
        }
        result
    }
}

async fn load_before(
    client: &mut Client,
    key: &LogicalConsumerShardKey,
    member_ordinal: u16,
    evidence_limits: StandbyDecoderEvidenceLimits,
    deadline: Instant,
    timeout: Duration,
) -> Result<Option<LoadedStandbyDecoderPolicy>, SlotCatalogLoadError> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    set_local_timeouts(&transaction, deadline).await?;
    let loaded =
        load_in_transaction(&transaction, key, member_ordinal, evidence_limits, deadline).await;
    match loaded {
        Ok(loaded) => {
            if let Err(source) = transaction
                .batch_execute(DISABLE_LOCAL_STATEMENT_TIMEOUT_SQL)
                .await
            {
                return rollback_load_error(transaction, source.into(), timeout).await;
            }
            transaction.commit().await?;
            Ok(loaded)
        }
        Err(error) => rollback_load_error(transaction, error, timeout).await,
    }
}

async fn rollback_load_error(
    transaction: Transaction<'_>,
    error: SlotCatalogLoadError,
    timeout: Duration,
) -> Result<Option<LoadedStandbyDecoderPolicy>, SlotCatalogLoadError> {
    let statement_canceled = error.is_statement_cancellation();
    if let Err(source) = transaction.rollback().await {
        return Err(SlotCatalogLoadError::RollbackFailed { source });
    }
    if statement_canceled {
        Err(SlotCatalogLoadError::StatementTimeout { timeout })
    } else {
        Err(error)
    }
}

async fn finish_before<T>(
    operation: impl std::future::Future<Output = Result<T, SlotCatalogLoadError>>,
    deadline: Instant,
    timeout: Duration,
) -> Result<T, SlotCatalogLoadError> {
    match timeout_at(deadline, operation).await {
        Err(_) => Err(SlotCatalogLoadError::OperationTimeout { timeout }),
        Ok(Err(SlotCatalogLoadError::Postgres(error))) if is_transaction_timeout(&error) => {
            Err(SlotCatalogLoadError::OperationTimeout { timeout })
        }
        Ok(result) => result,
    }
}

fn is_transaction_timeout(error: &tokio_postgres::Error) -> bool {
    error.code().is_some_and(|code| code.code() == "25P04")
}

async fn set_session_timeouts(
    client: &Client,
    deadline: Instant,
) -> Result<(), tokio_postgres::Error> {
    let (statement, transaction) = server_timeout_settings(deadline);
    client
        .query_one(SET_SESSION_TIMEOUTS_SQL, &[&statement, &transaction])
        .await?;
    Ok(())
}

async fn set_local_timeouts(
    transaction: &Transaction<'_>,
    deadline: Instant,
) -> Result<(), tokio_postgres::Error> {
    let (statement, transaction_timeout) = server_timeout_settings(deadline);
    transaction
        .query_one(SET_LOCAL_TIMEOUTS_SQL, &[&statement, &transaction_timeout])
        .await?;
    Ok(())
}

async fn set_local_statement_timeout(
    transaction: &Transaction<'_>,
    deadline: Instant,
) -> Result<(), tokio_postgres::Error> {
    let setting = server_statement_timeout_setting(deadline);
    transaction
        .query_one(SET_LOCAL_STATEMENT_TIMEOUT_SQL, &[&setting])
        .await?;
    Ok(())
}

fn server_timeout_settings(deadline: Instant) -> (String, String) {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let transaction = remaining.saturating_add(SERVER_TRANSACTION_TIMEOUT_GRACE);
    (
        server_statement_timeout_setting(deadline),
        postgres_milliseconds(transaction),
    )
}

fn server_statement_timeout_setting(deadline: Instant) -> String {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let timeout = remaining
        .saturating_sub(SERVER_STATEMENT_TIMEOUT_HEADROOM)
        .max(Duration::from_millis(1));
    postgres_milliseconds(timeout)
}

fn postgres_milliseconds(timeout: Duration) -> String {
    let milliseconds = u64::try_from(timeout.as_millis())
        .expect("bounded catalog operation timeout fits PostgreSQL milliseconds");
    format!("{milliseconds}ms")
}

async fn load_in_transaction(
    transaction: &Transaction<'_>,
    key: &LogicalConsumerShardKey,
    member_ordinal: u16,
    evidence_limits: StandbyDecoderEvidenceLimits,
    deadline: Instant,
) -> Result<Option<LoadedStandbyDecoderPolicy>, SlotCatalogLoadError> {
    let singletons = transaction.query_one(SINGLETONS_SQL, &[]).await?;
    let cluster_id = optional_string(&singletons, "cluster_id")?.ok_or(
        SlotCatalogLoadError::MissingSingleton("cluster_configuration"),
    )?;
    let cluster_id = parse_uuid("cluster_id", cluster_id)?;
    let catalog_epoch = optional_i64(&singletons, "catalog_epoch")?
        .ok_or(SlotCatalogLoadError::MissingSingleton("cluster_state"))?;
    let catalog_epoch = nonnegative_i64("catalog_epoch", catalog_epoch)?;

    let consumer_id = key.consumer.to_string();
    let logical_database_id = key.logical_database.to_string();
    set_local_statement_timeout(transaction, deadline).await?;
    let owner_rows = transaction
        .query(
            READY_OWNER_SQL,
            &[&consumer_id, &logical_database_id, &key.shard],
        )
        .await?;
    let owner = match owner_rows.as_slice() {
        [] => return Ok(None),
        [owner] => owner,
        _ => return Err(SlotCatalogLoadError::DuplicateConsumerShard),
    };
    let consumer_state: String = owner.try_get("consumer_state")?;
    match consumer_state.as_str() {
        "active" | "draining" => {}
        "retired" => return Ok(None),
        other => {
            return Err(SlotCatalogLoadError::UnsupportedConsumerState(
                other.to_owned(),
            ));
        }
    }
    let shard_state: String = owner.try_get("shard_state")?;
    match shard_state.as_str() {
        "ready" => {}
        "provisioning" | "fenced" | "retired" => return Ok(None),
        other => {
            return Err(SlotCatalogLoadError::UnsupportedShardState(
                other.to_owned(),
            ));
        }
    }
    let purpose_text: String = owner.try_get("purpose")?;
    let purpose = parse_purpose(&purpose_text)?;
    let ownership_fence = positive_i64(
        "ownership_fence",
        owner.try_get::<_, i64>("ownership_fence")?,
    )?;

    set_local_statement_timeout(transaction, deadline).await?;
    let rows = transaction
        .query(
            STANDBY_POLICY_SQL,
            &[&consumer_id, &logical_database_id, &key.shard],
        )
        .await?;
    let row = match rows.as_slice() {
        [] => {
            return Err(SlotCatalogLoadError::IncompleteReadyPolicy(
                "active source attachment",
            ));
        }
        [row] => row,
        _ => return Err(SlotCatalogLoadError::DuplicateActivePolicy),
    };
    let selected_role: String = row.try_get("selected_source_role")?;
    match selected_role.as_str() {
        "primary-anchor" => return Ok(None),
        "standby-decoder" => {}
        other => {
            return Err(SlotCatalogLoadError::UnsupportedSelectedSourceRole(
                other.to_owned(),
            ));
        }
    }
    let selected_member = nonnegative_u16(
        "selected_source_member_ordinal",
        row.try_get::<_, i32>("selected_source_member_ordinal")?,
    )?;
    if selected_member != member_ordinal {
        return Ok(None);
    }
    let context = PolicyContext {
        key,
        member_ordinal,
        cluster_id,
        catalog_epoch,
        purpose,
        ownership_fence,
    };
    parse_policy(row, &context, evidence_limits).map(Some)
}

struct PolicyContext<'a> {
    key: &'a LogicalConsumerShardKey,
    member_ordinal: u16,
    cluster_id: Uuid,
    catalog_epoch: u64,
    purpose: LogicalConsumerPurpose,
    ownership_fence: u64,
}

fn parse_policy(
    row: &Row,
    context: &PolicyContext<'_>,
    evidence_limits: StandbyDecoderEvidenceLimits,
) -> Result<LoadedStandbyDecoderPolicy, SlotCatalogLoadError> {
    require_component(
        row,
        "checkpoint_generation",
        "current exact-lineage checkpoint",
    )?;
    require_component(row, "anchor_generation", "active primary anchor")?;
    require_component(row, "decoder_generation", "active standby decoder")?;
    let checkpoint_generation = uuid_field(row, "checkpoint_generation")?;
    let checkpoint = lsn_field(row, "checkpoint_lsn")?;
    let checkpoint_ordinal = checkpoint_ordinal(required_i64(row, "checkpoint_ordinal")?)?;
    if required_bool(row, "snapshot_required")? {
        return Err(SlotCatalogLoadError::SnapshotRequired);
    }
    let attachment_generation = uuid_field(row, "attachment_generation")?;
    let restore_incarnation = uuid_field(row, "restore_incarnation")?;
    let database_name = required_string(row, "database_name")?;
    if database_name.is_empty() || database_name.len() > 63 || database_name.contains('\0') {
        return Err(SlotCatalogLoadError::InvalidDatabaseName);
    }
    let system_identifier = parse_u64_field(row, "system_identifier")?;
    let database_oid = positive_u32(row, "database_oid")?;
    let timeline = positive_u32(row, "selected_source_timeline")?;
    let source = ReplicationSourceIdentity::new(
        system_identifier,
        timeline,
        database_oid,
        restore_incarnation,
        CatalogEpoch(context.catalog_epoch),
    )?;

    let anchor_consistent_point = lsn_field(row, "anchor_consistent_point")?;
    let anchor_two_phase_at = lsn_field(row, "anchor_two_phase_at")?;
    let decoder_consistent_point = lsn_field(row, "decoder_consistent_point")?;
    let decoder_two_phase_at = lsn_field(row, "decoder_two_phase_at")?;
    for (field, boundary) in [
        ("anchor_consistent_point", anchor_consistent_point),
        ("anchor_two_phase_at", anchor_two_phase_at),
        ("decoder_consistent_point", decoder_consistent_point),
        ("decoder_two_phase_at", decoder_two_phase_at),
    ] {
        if boundary.0 == 0 || boundary.0 > checkpoint.0 {
            return Err(SlotCatalogLoadError::BoundaryAhead {
                field,
                boundary,
                checkpoint,
            });
        }
    }

    let anchor = managed_slot_target(row, "anchor_name", "anchor_generation")?;
    let decoder = managed_slot_target(row, "decoder_name", "decoder_generation")?;
    let physical_slot =
        ReplicationSlotName::new(member_replication_slot_name(context.member_ordinal))?;
    let target = StandbyDecoderTarget::new(context.member_ordinal, physical_slot, anchor, decoder)?;
    let policy = StandbyDecoderPolicy::new(
        source,
        target,
        ManagedTwoPhasePolicy {
            failover_anchor_at: anchor_two_phase_at,
            local_decoder_at: decoder_two_phase_at,
        },
        checkpoint,
        evidence_limits,
    )?;

    Ok(LoadedStandbyDecoderPolicy {
        cluster_id: context.cluster_id,
        key: context.key.clone(),
        purpose: context.purpose,
        ownership_fence: context.ownership_fence,
        checkpoint_generation,
        checkpoint_ordinal,
        attachment_generation,
        database_name,
        policy,
    })
}

fn require_component(
    row: &Row,
    probe_field: &'static str,
    component: &'static str,
) -> Result<(), SlotCatalogLoadError> {
    if optional_string(row, probe_field)?.is_none() {
        return Err(SlotCatalogLoadError::IncompleteReadyPolicy(component));
    }
    Ok(())
}

fn member_replication_slot_name(member_ordinal: u16) -> String {
    format!("pgshard_member_{member_ordinal:04}")
}

fn managed_slot_target(
    row: &Row,
    name_field: &'static str,
    generation_field: &'static str,
) -> Result<ManagedSlotTarget, SlotCatalogLoadError> {
    let generation = SlotGeneration::new(uuid_field(row, generation_field)?)?;
    Ok(ManagedSlotTarget::new(
        ReplicationSlotName::new(required_string(row, name_field)?)?,
        generation,
    )?)
}

fn parse_purpose(value: &str) -> Result<LogicalConsumerPurpose, SlotCatalogLoadError> {
    match value {
        "change-stream" => Ok(LogicalConsumerPurpose::ChangeStream),
        "reshard-materializer" => Ok(LogicalConsumerPurpose::ReshardMaterializer),
        "internal-materialization" => Ok(LogicalConsumerPurpose::InternalMaterialization),
        other => Err(SlotCatalogLoadError::UnsupportedPurpose(other.to_owned())),
    }
}

fn uuid_field(row: &Row, field: &'static str) -> Result<Uuid, SlotCatalogLoadError> {
    parse_uuid(field, required_string(row, field)?)
}

fn parse_uuid(field: &'static str, value: String) -> Result<Uuid, SlotCatalogLoadError> {
    let parsed = Uuid::parse_str(&value).map_err(|source| SlotCatalogLoadError::InvalidUuid {
        field,
        value,
        source,
    })?;
    if parsed.is_nil() {
        return Err(SlotCatalogLoadError::NilUuid(field));
    }
    Ok(parsed)
}

fn parse_u64_field(row: &Row, field: &'static str) -> Result<u64, SlotCatalogLoadError> {
    let value = required_string(row, field)?;
    value
        .parse()
        .map_err(|source| SlotCatalogLoadError::InvalidUnsigned {
            field,
            value,
            source,
        })
}

fn positive_i64(field: &'static str, value: i64) -> Result<u64, SlotCatalogLoadError> {
    let value = nonnegative_i64(field, value)?;
    if value == 0 {
        return Err(SlotCatalogLoadError::ZeroNumeric(field));
    }
    Ok(value)
}

fn nonnegative_i64(field: &'static str, value: i64) -> Result<u64, SlotCatalogLoadError> {
    u64::try_from(value).map_err(|_| SlotCatalogLoadError::NumericOutOfRange(field))
}

fn checkpoint_ordinal(value: i64) -> Result<u64, SlotCatalogLoadError> {
    let value = nonnegative_i64("checkpoint_ordinal", value)?;
    if value == 0 {
        return Err(SlotCatalogLoadError::ZeroCheckpointOrdinal);
    }
    Ok(value)
}

fn nonnegative_u16(field: &'static str, value: i32) -> Result<u16, SlotCatalogLoadError> {
    u16::try_from(value).map_err(|_| SlotCatalogLoadError::NumericOutOfRange(field))
}

fn positive_u32(row: &Row, field: &'static str) -> Result<u32, SlotCatalogLoadError> {
    let value = required_i64(row, field)?;
    let value = u32::try_from(value).map_err(|_| SlotCatalogLoadError::NumericOutOfRange(field))?;
    if value == 0 {
        return Err(SlotCatalogLoadError::ZeroNumeric(field));
    }
    Ok(value)
}

fn lsn_field(row: &Row, field: &'static str) -> Result<PgLsn, SlotCatalogLoadError> {
    let value = required_string(row, field)?;
    parse_lsn(&value).ok_or(SlotCatalogLoadError::InvalidLsn { field, value })
}

fn optional_string(row: &Row, field: &'static str) -> Result<Option<String>, SlotCatalogLoadError> {
    Ok(row.try_get(field)?)
}

fn optional_i64(row: &Row, field: &'static str) -> Result<Option<i64>, SlotCatalogLoadError> {
    Ok(row.try_get(field)?)
}

fn required_string(row: &Row, field: &'static str) -> Result<String, SlotCatalogLoadError> {
    optional_string(row, field)?.ok_or(SlotCatalogLoadError::IncompleteReadyPolicy(field))
}

fn required_i64(row: &Row, field: &'static str) -> Result<i64, SlotCatalogLoadError> {
    optional_i64(row, field)?.ok_or(SlotCatalogLoadError::IncompleteReadyPolicy(field))
}

fn required_bool(row: &Row, field: &'static str) -> Result<bool, SlotCatalogLoadError> {
    row.try_get::<_, Option<bool>>(field)?
        .ok_or(SlotCatalogLoadError::IncompleteReadyPolicy(field))
}

/// Fail-closed catalog-policy loading error.
#[derive(Debug, Error)]
pub enum SlotCatalogLoadError {
    /// `PostgreSQL` query or transaction failure.
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    /// The absolute client deadline elapsed or `PostgreSQL` terminated the transaction.
    ///
    /// The reader drops its client and becomes terminal before returning this
    /// error because queued protocol work or backend survival is not proven.
    #[error("slot catalog operation exceeded its terminal deadline {timeout:?}")]
    OperationTimeout {
        /// Validated deadline applied to the whole operation, including commit.
        timeout: Duration,
    },
    /// `PostgreSQL` canceled a read statement before the client deadline and rollback completed.
    #[error("slot catalog read statement exceeded its retryable deadline {timeout:?}")]
    StatementTimeout {
        /// Validated deadline applied to the whole operation.
        timeout: Duration,
    },
    /// Transaction cleanup failed, so connection reuse cannot be proven safe.
    #[error("slot catalog transaction rollback failed; reader is terminal: {source}")]
    RollbackFailed {
        /// `PostgreSQL` rollback failure.
        #[source]
        source: tokio_postgres::Error,
    },
    /// A previous terminal failure closed the dedicated client.
    #[error("slot catalog reader is terminal after an earlier operation failure")]
    ReaderTerminated,
    /// Server is older than the minimum supported release.
    #[error("slot catalog reader requires PostgreSQL 18 or newer; observed server_version_num {0}")]
    UnsupportedPostgresVersion(i32),
    /// Reader was connected to a database other than `shardschema`.
    #[error("slot catalog reader requires the dedicated shardschema database; observed {0:?}")]
    WrongDatabase(String),
    /// Catalog database is not UTF8.
    #[error("slot catalog reader requires UTF8; observed {0:?}")]
    WrongEncoding(String),
    /// A required catalog singleton is absent.
    #[error("required shardschema singleton {0} is missing")]
    MissingSingleton(&'static str),
    /// A primary-key invariant unexpectedly returned duplicate consumer shards.
    #[error("catalog returned more than one owner row for one consumer shard")]
    DuplicateConsumerShard,
    /// Active uniqueness constraints did not hold.
    #[error("catalog returned more than one active standby policy for one consumer shard")]
    DuplicateActivePolicy,
    /// A ready consumer is missing one exact-lineage component.
    #[error("ready consumer policy is missing required field or component {0}")]
    IncompleteReadyPolicy(&'static str),
    /// UUID text was malformed.
    #[error("catalog field {field} contains invalid UUID {value:?}: {source}")]
    InvalidUuid {
        /// Catalog field.
        field: &'static str,
        /// Rejected value.
        value: String,
        /// UUID parser failure.
        source: uuid::Error,
    },
    /// UUID was nil.
    #[error("catalog field {0} must contain a non-nil UUID")]
    NilUuid(&'static str),
    /// Unsigned decimal text was malformed.
    #[error("catalog field {field} contains invalid unsigned integer {value:?}: {source}")]
    InvalidUnsigned {
        /// Catalog field.
        field: &'static str,
        /// Rejected value.
        value: String,
        /// Integer parser failure.
        source: std::num::ParseIntError,
    },
    /// Signed `PostgreSQL` integer did not fit the Rust model.
    #[error("catalog field {0} is outside its supported unsigned range")]
    NumericOutOfRange(&'static str),
    /// Required numeric identity was zero.
    #[error("catalog field {0} must be positive")]
    ZeroNumeric(&'static str),
    /// A ready consumer cannot attach from its unadvanced seed checkpoint.
    #[error("ready consumer checkpoint ordinal must be positive")]
    ZeroCheckpointOrdinal,
    /// A ready consumer cannot attach until its initial snapshot is durable.
    #[error("ready consumer checkpoint still requires a snapshot")]
    SnapshotRequired,
    /// `pg_lsn` text was malformed.
    #[error("catalog field {field} contains invalid PostgreSQL LSN {value:?}")]
    InvalidLsn {
        /// Catalog field.
        field: &'static str,
        /// Rejected value.
        value: String,
    },
    /// Active slot boundary cannot resume the checkpoint.
    #[error(
        "catalog field {field} boundary {boundary:?} is zero or ahead of checkpoint {checkpoint:?}"
    )]
    BoundaryAhead {
        /// Boundary field.
        field: &'static str,
        /// Rejected boundary.
        boundary: PgLsn,
        /// Durable checkpoint.
        checkpoint: PgLsn,
    },
    /// Consumer purpose is outside the closed Milestone 1 set.
    #[error("unsupported logical consumer purpose {0:?}")]
    UnsupportedPurpose(String),
    /// Consumer lifecycle state was outside the closed catalog set.
    #[error("unsupported logical consumer state {0:?}")]
    UnsupportedConsumerState(String),
    /// Consumer-shard lifecycle state was outside the closed catalog set.
    #[error("unsupported logical consumer shard state {0:?}")]
    UnsupportedShardState(String),
    /// Active attachment source role was outside the closed catalog set.
    #[error("unsupported selected source role {0:?}")]
    UnsupportedSelectedSourceRole(String),
    /// Database name violated `PostgreSQL` identifier bounds.
    #[error("catalog database name must contain 1-63 bytes and no NUL")]
    InvalidDatabaseName,
    /// Source lineage was incomplete.
    #[error(transparent)]
    SourceIdentity(#[from] SourceIdentityError),
    /// Slot generation was nil.
    #[error(transparent)]
    SlotGeneration(#[from] SlotGenerationError),
    /// Slot name was invalid.
    #[error(transparent)]
    SlotName(#[from] SlotNameError),
    /// Slot name did not encode its generation.
    #[error(transparent)]
    ManagedSlotTarget(#[from] ManagedSlotTargetError),
    /// Physical, anchor, or decoder namespace collided.
    #[error(transparent)]
    StandbyTarget(#[from] StandbyDecoderTargetError),
    /// Checkpoint or prepared-decoding boundary was unsafe.
    #[error(transparent)]
    StandbyPolicy(#[from] StandbyDecoderPolicyError),
}

impl SlotCatalogLoadError {
    fn is_statement_cancellation(&self) -> bool {
        matches!(
            self,
            Self::Postgres(error)
                if error
                    .code()
                    .is_some_and(|code| code == &tokio_postgres::error::SqlState::QUERY_CANCELED)
        )
    }

    fn terminates_reader(&self) -> bool {
        matches!(
            self,
            Self::OperationTimeout { .. } | Self::RollbackFailed { .. } | Self::ReaderTerminated
        ) || matches!(self, Self::Postgres(error) if error.is_closed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct ReplicationSlotNameContract {
        member_physical_slots: Vec<MemberPhysicalSlotCase>,
    }

    #[derive(serde::Deserialize)]
    struct MemberPhysicalSlotCase {
        member_ordinal: u16,
        slot_name: String,
    }

    #[test]
    fn member_replication_name_matches_shared_contract() {
        let contract: ReplicationSlotNameContract = serde_json::from_str(include_str!(
            "../../../contracts/replication-slot-names.json"
        ))
        .expect("valid shared replication-slot naming contract");
        assert!(!contract.member_physical_slots.is_empty());
        for case in contract.member_physical_slots {
            assert_eq!(
                member_replication_slot_name(case.member_ordinal),
                case.slot_name
            );
        }
    }

    #[test]
    fn validates_catalog_resource_names() {
        for valid in ["a", "shard-0000", "0", "a1-b2"] {
            assert!(valid_resource_name(valid), "expected {valid:?} to pass");
        }
        for invalid in ["", "-shard", "shard-", "Shard", "shard_0", "shard--?"] {
            assert!(
                !valid_resource_name(invalid),
                "expected {invalid:?} to fail"
            );
        }
        assert!(!valid_resource_name(&"a".repeat(64)));
    }

    #[test]
    fn parses_canonical_postgres_lsn_without_cross_timeline_ordering() {
        assert_eq!(parse_lsn("0/0"), Some(PgLsn(0)));
        assert_eq!(parse_lsn("1/2"), Some(PgLsn(0x1_0000_0002)));
        assert_eq!(parse_lsn("FFFFFFFF/FFFFFFFF"), Some(PgLsn(u64::MAX)));
        for invalid in ["", "0", "/0", "0/", "0/000000000", "g/0", "0/xyz", "0/0/0"] {
            assert_eq!(parse_lsn(invalid), None, "expected {invalid:?} to fail");
        }
    }

    #[test]
    fn rejects_nil_or_noncanonical_consumer_keys() {
        let id = Uuid::from_u128(1);
        assert!(matches!(
            LogicalConsumerShardKey::new(Uuid::nil(), id, "shard-0000"),
            Err(LogicalConsumerShardKeyError::NilConsumerId)
        ));
        assert!(matches!(
            LogicalConsumerShardKey::new(id, Uuid::nil(), "shard-0000"),
            Err(LogicalConsumerShardKeyError::NilLogicalDatabaseId)
        ));
        assert!(matches!(
            LogicalConsumerShardKey::new(id, id, "SHARD"),
            Err(LogicalConsumerShardKeyError::InvalidShardId)
        ));
    }

    #[test]
    fn accepts_genesis_epoch_but_rejects_negative_catalog_values() {
        assert_eq!(
            nonnegative_i64("catalog_epoch", 0).expect("genesis epoch"),
            0
        );
        assert!(matches!(
            nonnegative_i64("catalog_epoch", -1),
            Err(SlotCatalogLoadError::NumericOutOfRange("catalog_epoch"))
        ));
    }

    #[tokio::test]
    async fn absolute_client_deadline_is_terminal_but_statement_cancellation_is_retryable() {
        let operation_timeout =
            CatalogOperationTimeout::new(Duration::from_millis(100)).expect("minimum timeout");
        let timeout = operation_timeout.get();
        let deadline = Instant::now() + timeout;
        let error = finish_before(
            std::future::pending::<Result<(), SlotCatalogLoadError>>(),
            deadline,
            timeout,
        )
        .await
        .expect_err("pending client operation must reach its absolute deadline");
        assert!(matches!(
            error,
            SlotCatalogLoadError::OperationTimeout { timeout: observed }
                if observed == timeout
        ));
        assert!(error.terminates_reader());
        assert!(
            !SlotCatalogLoadError::StatementTimeout { timeout }.terminates_reader(),
            "rolled-back statement cancellation must retain the reader"
        );
        let source = "not-a-key-value-pair"
            .parse::<tokio_postgres::Config>()
            .expect_err("invalid PostgreSQL configuration must produce a test error");
        assert!(
            SlotCatalogLoadError::RollbackFailed { source }.terminates_reader(),
            "failed rollback must discard the reader even when the source error is not closed"
        );
    }
}
