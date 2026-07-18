//! Transactionally consistent `PostgreSQL` catalog loading.

use std::collections::HashMap;

use pgshard_types::{KeyRange, KeyRangeError, ShardId};
use thiserror::Error;
use tokio::time::Instant;
use tokio_postgres::{Client, IsolationLevel, Row, Transaction};
use uuid::Uuid;

use crate::cache::InstallBeforeError;
use crate::{
    CatalogCache, CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, DatabaseId,
    IdentifierError, InstallOutcome, NOTIFY_CHANNEL, RegisteredTable, RoutingHashConfig,
    ShardKeyType, ShardRoute, SnapshotError, TableName,
};

/// Maximum logical databases published in one process snapshot.
pub const MAX_LOGICAL_DATABASES: usize = 1_024;
/// Maximum routing ranges loaded for one logical database.
pub const MAX_ROUTING_RANGES_PER_DATABASE: usize = 4_096;
/// Maximum registered tables loaded for one logical database.
pub const MAX_REGISTERED_TABLES_PER_DATABASE: usize = 16_384;
/// Maximum routing ranges retained across one process snapshot.
pub const MAX_TOTAL_ROUTING_RANGES: usize = 65_536;
/// Maximum registered tables retained across one process snapshot.
pub const MAX_TOTAL_REGISTERED_TABLES: usize = 65_536;

const DATABASE_QUERY_LIMIT: i64 = 1_025;
const ROUTE_QUERY_LIMIT: i64 = 65_537;
const TABLE_QUERY_LIMIT: i64 = 65_537;
// The client deadline remains authoritative. This slightly later PostgreSQL 18
// transaction timeout interrupts lock waits and rolls back even if a backend
// does not notice that the client socket was dropped while it is blocked.
const SERVER_TRANSACTION_TIMEOUT_GRACE: std::time::Duration = std::time::Duration::from_millis(101);
const SET_SESSION_TRANSACTION_TIMEOUT_SQL: &str =
    "SELECT pg_catalog.set_config('transaction_timeout', $1, false)";
const SET_LOCAL_TRANSACTION_TIMEOUT_SQL: &str =
    "SELECT pg_catalog.set_config('transaction_timeout', $1, true)";

const CONFIGURATION_SQL: &str = "\
    SELECT configuration.cluster_id::text, configuration.hash_version, \
           configuration.hash_seed::text, state.catalog_epoch \
      FROM pgshard_catalog.cluster_configuration AS configuration \
      CROSS JOIN pgshard_catalog.cluster_state AS state \
     WHERE configuration.singleton AND state.singleton";

const DATABASES_SQL: &str = "\
    SELECT databases.logical_database_id::text, databases.database_name, \
           databases.schema_epoch, databases.authorization_epoch, active.routing_epoch, \
           coalesce(epochs.logical_database_id = databases.logical_database_id \
                    AND epochs.state = 'active', false), \
           (SELECT pg_catalog.count(*) = 1 \
              FROM pgshard_catalog.routing_epochs AS active_epochs \
             WHERE active_epochs.logical_database_id = databases.logical_database_id \
               AND active_epochs.state = 'active') \
      FROM pgshard_catalog.logical_databases AS databases \
      JOIN pgshard_catalog.active_routing_epochs AS active \
        ON active.logical_database_id = databases.logical_database_id \
      LEFT JOIN pgshard_catalog.routing_epochs AS epochs \
        ON epochs.routing_epoch = active.routing_epoch \
     WHERE databases.state IN ('active', 'draining') \
      ORDER BY databases.logical_database_id \
      LIMIT $1";

const ROUTES_SQL: &str = "\
    SELECT databases.logical_database_id::text, shards.shard_number, \
           ranges.range_start::text, ranges.range_end::text, shards.state, \
           ranges.shard_id::text \
      FROM pgshard_catalog.logical_databases AS databases \
      JOIN pgshard_catalog.active_routing_epochs AS active \
        ON active.logical_database_id = databases.logical_database_id \
      JOIN pgshard_catalog.routing_epochs AS epochs \
        ON epochs.routing_epoch = active.routing_epoch \
       AND epochs.logical_database_id = databases.logical_database_id \
       AND epochs.state = 'active' \
      JOIN pgshard_catalog.routing_ranges AS ranges \
        ON ranges.routing_epoch = active.routing_epoch \
       LEFT JOIN pgshard_catalog.shards AS shards ON shards.shard_id = ranges.shard_id \
     WHERE databases.state IN ('active', 'draining') \
     ORDER BY databases.logical_database_id, ranges.range_start, ranges.range_end \
     LIMIT $1";

const TABLES_SQL: &str = "\
    SELECT databases.logical_database_id::text, tables.schema_name, tables.table_name, \
           tables.shard_key_column, tables.shard_key_type, tables.hash_version \
      FROM pgshard_catalog.logical_databases AS databases \
      JOIN pgshard_catalog.active_routing_epochs AS active \
        ON active.logical_database_id = databases.logical_database_id \
      JOIN pgshard_catalog.registered_tables AS tables \
        ON tables.logical_database_id = databases.logical_database_id \
       AND tables.state = 'active' \
     WHERE databases.state IN ('active', 'draining') \
     ORDER BY databases.logical_database_id, tables.schema_name, tables.table_name \
     LIMIT $1";

/// Dedicated authoritative catalog connection.
///
/// The raw [`Client`] is owned rather than borrowed so callers cannot defer a
/// `LISTEN` subscription inside a manual transaction. Construction first
/// executes `DISCARD ALL`, which both requires an idle connection and removes
/// inherited session state, then commits `LISTEN` before the initial snapshot
/// read.
pub struct CatalogReader {
    client: Client,
}

impl CatalogReader {
    /// Takes ownership of an idle, dedicated connection, subscribes to catalog
    /// notifications, and publishes the initial snapshot.
    ///
    /// The connection should authenticate as a principal that can directly
    /// read the catalog; `DISCARD ALL` intentionally clears session-local role
    /// and setting changes. A manually opened transaction fails closed rather
    /// than being committed or reused.
    ///
    /// [`crate::run_catalog_refresh`] provides the standard long-running loop.
    /// Direct callers must continuously drive the associated `tokio-postgres`
    /// connection, parse every notification with
    /// [`crate::CatalogNotification`], call [`Self::refresh`] for unseen
    /// epochs, and poll periodically because notifications can be lost across
    /// disconnects.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is not idle, the clean `LISTEN` cannot
    /// commit, or the initial refresh fails.
    pub async fn subscribe(
        client: Client,
        cache: &CatalogCache,
    ) -> Result<(Self, InstallOutcome), LoadError> {
        Self::subscribe_inner(client, cache, None).await
    }

    pub(crate) async fn subscribe_before(
        client: Client,
        cache: &CatalogCache,
        deadline: Instant,
    ) -> Result<(Self, InstallOutcome), LoadError> {
        Self::subscribe_inner(client, cache, Some(deadline)).await
    }

    async fn subscribe_inner(
        client: Client,
        cache: &CatalogCache,
        deadline: Option<Instant>,
    ) -> Result<(Self, InstallOutcome), LoadError> {
        client.batch_execute("DISCARD ALL").await?;
        if let Some(deadline) = deadline {
            let setting = server_transaction_timeout_setting(deadline);
            client
                .query_one(SET_SESSION_TRANSACTION_TIMEOUT_SQL, &[&setting])
                .await?;
        }
        client
            .batch_execute(&format!("LISTEN {NOTIFY_CHANNEL}"))
            .await?;
        if deadline.is_some() {
            client.batch_execute("SET transaction_timeout = 0").await?;
        }
        let mut reader = Self { client };
        let outcome = match deadline {
            Some(deadline) => reader.refresh_before(cache, deadline).await?,
            None => reader.refresh(cache).await?,
        };
        Ok((reader, outcome))
    }

    /// Reads one complete immutable snapshot in a read-only repeatable-read
    /// transaction.
    ///
    /// Only logical databases with an active routing epoch are published.
    /// Staged metadata remains invisible until the catalog's dual-CAS
    /// activation commits.
    ///
    /// # Errors
    ///
    /// Fails closed on SQL errors, missing singleton state, unsupported catalog
    /// values, integer conversion errors, or any snapshot invariant violation.
    pub async fn load_snapshot(&mut self) -> Result<CatalogSnapshot, LoadError> {
        self.load_snapshot_inner(None).await
    }

    async fn load_snapshot_inner(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<CatalogSnapshot, LoadError> {
        let transaction = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        if let Some(deadline) = deadline {
            set_server_transaction_timeout(&transaction, deadline).await?;
        }
        let snapshot = load_in_transaction(&transaction).await?;
        transaction.commit().await?;
        Ok(snapshot)
    }

    /// Loads and monotonically publishes the current authoritative snapshot.
    ///
    /// # Errors
    ///
    /// Returns a load or cache integrity/monotonicity error without changing
    /// the current cache state.
    pub async fn refresh(&mut self, cache: &CatalogCache) -> Result<InstallOutcome, LoadError> {
        let snapshot = self.load_snapshot().await?;
        cache.install(snapshot).map_err(LoadError::Cache)
    }

    pub(crate) async fn refresh_before(
        &mut self,
        cache: &CatalogCache,
        deadline: Instant,
    ) -> Result<InstallOutcome, LoadError> {
        let snapshot = self.load_snapshot_inner(Some(deadline)).await?;
        match cache.install_before(snapshot, deadline).await {
            Ok(outcome) => Ok(outcome),
            Err(InstallBeforeError::Cache(error)) => Err(LoadError::Cache(error)),
            Err(InstallBeforeError::DeadlineElapsed) => Err(LoadError::DeadlineElapsed),
        }
    }
}

async fn set_server_transaction_timeout(
    transaction: &Transaction<'_>,
    deadline: Instant,
) -> Result<(), tokio_postgres::Error> {
    let setting = server_transaction_timeout_setting(deadline);
    transaction
        .query_one(SET_LOCAL_TRANSACTION_TIMEOUT_SQL, &[&setting])
        .await?;
    Ok(())
}

fn server_transaction_timeout_setting(deadline: Instant) -> String {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let timeout = remaining.saturating_add(SERVER_TRANSACTION_TIMEOUT_GRACE);
    let milliseconds = u64::try_from(timeout.as_millis())
        .expect("bounded catalog operation timeout fits PostgreSQL milliseconds");
    format!("{milliseconds}ms")
}

async fn load_in_transaction(transaction: &Transaction<'_>) -> Result<CatalogSnapshot, LoadError> {
    let configuration = transaction.query_opt(CONFIGURATION_SQL, &[]).await?;
    let configuration = configuration.ok_or(LoadError::MissingSingleton)?;
    let cluster_id = parse_uuid("cluster_id", configuration.get::<_, &str>(0))?;
    let hash_version = positive_u16("hash_version", configuration.get::<_, i16>(1))?;
    let hash_seed = parse_u64("hash_seed", &configuration.get::<_, String>(2))?;
    let catalog_epoch = nonnegative_u64("catalog_epoch", configuration.get::<_, i64>(3))?;

    let rows = transaction
        .query(DATABASES_SQL, &[&DATABASE_QUERY_LIMIT])
        .await?;
    ensure_cardinality("logical databases", rows.len(), MAX_LOGICAL_DATABASES)?;
    let mut pending = Vec::with_capacity(rows.len());
    let mut database_indices = HashMap::with_capacity(rows.len());
    for row in rows {
        let database = parse_database(&row)?;
        let database_index = pending.len();
        if database_indices
            .insert(database.id, database_index)
            .is_some()
        {
            return Err(LoadError::DuplicateLogicalDatabase(database.id));
        }
        pending.push(database);
    }

    load_routes(transaction, &mut pending, &database_indices).await?;
    load_tables(transaction, &mut pending, &database_indices).await?;
    let databases = pending
        .into_iter()
        .map(PendingDatabase::finish)
        .collect::<Result<Vec<_>, _>>()?;
    CatalogSnapshot::new(
        ClusterId::new(cluster_id)?,
        catalog_epoch,
        RoutingHashConfig::new(hash_version, hash_seed)?,
        databases,
    )
    .map_err(LoadError::Snapshot)
}

async fn load_routes(
    transaction: &Transaction<'_>,
    pending: &mut [PendingDatabase],
    database_indices: &HashMap<DatabaseId, usize>,
) -> Result<(), LoadError> {
    let route_rows = transaction.query(ROUTES_SQL, &[&ROUTE_QUERY_LIMIT]).await?;
    ensure_cardinality(
        "routing ranges in one snapshot",
        route_rows.len(),
        MAX_TOTAL_ROUTING_RANGES,
    )?;
    for row in route_rows {
        let id = DatabaseId::new(parse_uuid(
            "routing logical_database_id",
            row.get::<_, &str>(0),
        )?)?;
        let database_index =
            database_indices
                .get(&id)
                .copied()
                .ok_or(LoadError::UnexpectedDatabaseReference {
                    resource: "routing range",
                    database_id: id,
                })?;
        let database = &mut pending[database_index];
        ensure_cardinality(
            "routing ranges for one database",
            database.routes.len() + 1,
            MAX_ROUTING_RANGES_PER_DATABASE,
        )?;
        let target_shard_id = row.get::<_, &str>(5);
        let shard_number =
            row.get::<_, Option<i64>>(1)
                .ok_or_else(|| LoadError::MissingRoutingShard {
                    database_id: id,
                    shard_id: target_shard_id.to_owned(),
                })?;
        let shard_number = nonnegative_u32("shard_number", shard_number)?;
        let shard_state =
            row.get::<_, Option<String>>(4)
                .ok_or_else(|| LoadError::MissingRoutingShard {
                    database_id: id,
                    shard_id: target_shard_id.to_owned(),
                })?;
        if !matches!(shard_state.as_str(), "active" | "draining") {
            return Err(LoadError::InvalidRoutingShardState {
                database_id: id,
                shard_number,
                state: shard_state,
            });
        }
        let start = parse_u128("range_start", &row.get::<_, String>(2))?;
        let end = parse_u128("range_end", &row.get::<_, String>(3))?;
        database.routes.push(ShardRoute::new(
            ShardId(shard_number),
            KeyRange::new(start, end)?,
        ));
    }
    Ok(())
}

async fn load_tables(
    transaction: &Transaction<'_>,
    pending: &mut [PendingDatabase],
    database_indices: &HashMap<DatabaseId, usize>,
) -> Result<(), LoadError> {
    let table_rows = transaction.query(TABLES_SQL, &[&TABLE_QUERY_LIMIT]).await?;
    ensure_cardinality(
        "registered tables in one snapshot",
        table_rows.len(),
        MAX_TOTAL_REGISTERED_TABLES,
    )?;
    for row in table_rows {
        let id = DatabaseId::new(parse_uuid(
            "table logical_database_id",
            row.get::<_, &str>(0),
        )?)?;
        let database_index =
            database_indices
                .get(&id)
                .copied()
                .ok_or(LoadError::UnexpectedDatabaseReference {
                    resource: "registered table",
                    database_id: id,
                })?;
        let database = &mut pending[database_index];
        ensure_cardinality(
            "registered tables for one database",
            database.tables.len() + 1,
            MAX_REGISTERED_TABLES_PER_DATABASE,
        )?;
        let schema_name = row.get::<_, String>(1);
        let table_name = row.get::<_, String>(2);
        let column = row.get::<_, String>(3);
        let key_type = parse_shard_key_type(&row.get::<_, String>(4))?;
        let hash_version = positive_u16("table hash_version", row.get::<_, i16>(5))?;
        database.tables.push(RegisteredTable::new(
            TableName::new(schema_name, table_name)?,
            column,
            key_type,
            hash_version,
        )?);
    }
    Ok(())
}

fn parse_database(row: &Row) -> Result<PendingDatabase, LoadError> {
    let id = DatabaseId::new(parse_uuid("logical_database_id", row.get::<_, &str>(0))?)?;
    let name = row.get::<_, String>(1);
    let schema_epoch = positive_u64("schema_epoch", row.get::<_, i64>(2))?;
    let authorization_epoch = positive_u64("authorization_epoch", row.get::<_, i64>(3))?;
    let routing_epoch_i64 = row
        .get::<_, Option<i64>>(4)
        .ok_or(LoadError::InvalidActiveRoutingEpoch { database_id: id })?;
    let routing_epoch_is_owned_and_active = row.get::<_, bool>(5);
    let has_exactly_one_active_epoch = row.get::<_, bool>(6);
    if !routing_epoch_is_owned_and_active || !has_exactly_one_active_epoch {
        return Err(LoadError::InvalidActiveRoutingEpoch { database_id: id });
    }
    let routing_epoch = positive_u64("routing_epoch", routing_epoch_i64)?;

    Ok(PendingDatabase {
        id,
        name,
        epochs: DatabaseEpochs::new(routing_epoch, schema_epoch, authorization_epoch)?,
        routes: Vec::new(),
        tables: Vec::new(),
    })
}

fn parse_shard_key_type(value: &str) -> Result<ShardKeyType, LoadError> {
    match value {
        "bigint" => Ok(ShardKeyType::Int64),
        "uuid" => Ok(ShardKeyType::Uuid),
        "text" => Ok(ShardKeyType::Text),
        "bytea" => Ok(ShardKeyType::Bytes),
        _ => Err(LoadError::UnsupportedShardKeyType(value.to_owned())),
    }
}

fn parse_uuid(field: &'static str, value: &str) -> Result<Uuid, LoadError> {
    Uuid::parse_str(value).map_err(|source| LoadError::InvalidUuid {
        field,
        value: value.to_owned(),
        source,
    })
}

fn positive_u16(field: &'static str, value: i16) -> Result<u16, LoadError> {
    let value = u16::try_from(value).map_err(|_| invalid_integer(field, &value))?;
    if value == 0 {
        return Err(invalid_integer(field, &value));
    }
    Ok(value)
}

fn positive_u64(field: &'static str, value: i64) -> Result<u64, LoadError> {
    let value = u64::try_from(value).map_err(|_| invalid_integer(field, &value))?;
    if value == 0 {
        return Err(invalid_integer(field, &value));
    }
    Ok(value)
}

fn nonnegative_u64(field: &'static str, value: i64) -> Result<u64, LoadError> {
    u64::try_from(value).map_err(|_| invalid_integer(field, &value))
}

struct PendingDatabase {
    id: DatabaseId,
    name: String,
    epochs: DatabaseEpochs,
    routes: Vec<ShardRoute>,
    tables: Vec<RegisteredTable>,
}

impl PendingDatabase {
    fn finish(self) -> Result<DatabaseCatalog, LoadError> {
        DatabaseCatalog::new(self.id, self.name, self.epochs, self.routes, self.tables)
            .map_err(LoadError::Snapshot)
    }
}

fn ensure_cardinality(
    resource: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), LoadError> {
    if actual > maximum {
        Err(LoadError::CardinalityLimit { resource, maximum })
    } else {
        Ok(())
    }
}

fn nonnegative_u32(field: &'static str, value: i64) -> Result<u32, LoadError> {
    u32::try_from(value).map_err(|_| invalid_integer(field, &value))
}

fn parse_u64(field: &'static str, value: &str) -> Result<u64, LoadError> {
    value.parse().map_err(|_| invalid_integer(field, &value))
}

fn parse_u128(field: &'static str, value: &str) -> Result<u128, LoadError> {
    value.parse().map_err(|_| invalid_integer(field, &value))
}

fn invalid_integer(field: &'static str, value: &impl ToString) -> LoadError {
    LoadError::InvalidInteger {
        field,
        value: value.to_string(),
    }
}

/// Authoritative snapshot load or publication failure.
#[derive(Debug, Error)]
pub enum LoadError {
    /// `PostgreSQL` query, transaction, or `LISTEN` failure.
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    /// The configured operation deadline elapsed before cache publication.
    #[error("catalog operation deadline elapsed before snapshot publication")]
    DeadlineElapsed,
    /// A required singleton configuration/state row is absent.
    #[error("shardschema singleton configuration or state row is missing")]
    MissingSingleton,
    /// An active logical database does not point at exactly one owned active
    /// routing epoch.
    #[error(
        "logical database {database_id} does not reference exactly one owned active routing epoch"
    )]
    InvalidActiveRoutingEpoch {
        /// Logical database whose serving pointer is inconsistent.
        database_id: DatabaseId,
    },
    /// A set query returned a row for a database omitted by the authoritative
    /// database query in the same repeatable-read snapshot.
    #[error("{resource} references unpublished logical database {database_id}")]
    UnexpectedDatabaseReference {
        /// Catalog resource carrying the reference.
        resource: &'static str,
        /// Referenced logical database.
        database_id: DatabaseId,
    },
    /// The authoritative database query returned a duplicate primary key.
    #[error("logical database {0} was loaded more than once")]
    DuplicateLogicalDatabase(DatabaseId),
    /// An active route references a shard identity absent from the catalog.
    #[error("logical database {database_id} routes to missing shard {shard_id:?}")]
    MissingRoutingShard {
        /// Logical database whose active route is malformed.
        database_id: DatabaseId,
        /// Absent physical shard identity referenced by the route.
        shard_id: String,
    },
    /// An active route references a physical shard that cannot serve traffic.
    #[error(
        "logical database {database_id} routes to unavailable shard {shard_number} in state {state:?}"
    )]
    InvalidRoutingShardState {
        /// Logical database whose active route is malformed.
        database_id: DatabaseId,
        /// Physical shard ordinal referenced by the route.
        shard_number: u32,
        /// Rejected catalog shard state.
        state: String,
    },
    /// A catalog UUID cannot be represented by the Rust model.
    #[error("invalid {field} UUID {value:?}: {source}")]
    InvalidUuid {
        /// Catalog field.
        field: &'static str,
        /// Rejected textual value.
        value: String,
        /// UUID parser error.
        source: uuid::Error,
    },
    /// A numeric catalog field is negative, zero when positive is required, or
    /// outside its target integer range.
    #[error("invalid {field} integer {value:?}")]
    InvalidInteger {
        /// Catalog field.
        field: &'static str,
        /// Rejected exact decimal value.
        value: String,
    },
    /// A future or corrupt catalog contains an unsupported shard-key type.
    #[error("unsupported shard-key type {0:?}")]
    UnsupportedShardKeyType(String),
    /// A catalog result exceeds a process memory/cardinality safety bound.
    #[error("{resource} exceeds supported maximum {maximum}")]
    CardinalityLimit {
        /// Bounded catalog resource.
        resource: &'static str,
        /// Largest accepted row count.
        maximum: usize,
    },
    /// A route boundary is outside the unsigned 64-bit keyspace.
    #[error(transparent)]
    KeyRange(#[from] KeyRangeError),
    /// A `PostgreSQL` identifier violates the Rust snapshot contract.
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    /// The loaded snapshot violates a cross-row invariant.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// The valid snapshot cannot be monotonically published.
    #[error(transparent)]
    Cache(#[from] crate::CacheError),
}

impl LoadError {
    pub(crate) fn is_operation_timeout(&self) -> bool {
        match self {
            Self::DeadlineElapsed => true,
            Self::Postgres(error) => error
                .code()
                .is_some_and(|sqlstate| sqlstate.code() == "25P04"),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cardinality_limits_accept_boundary_and_reject_cap_plus_one() {
        assert!(DATABASES_SQL.contains("LIMIT $1"));
        assert!(ROUTES_SQL.contains("LIMIT $1"));
        assert!(TABLES_SQL.contains("LIMIT $1"));
        assert_eq!(
            usize::try_from(DATABASE_QUERY_LIMIT).expect("database query limit"),
            MAX_LOGICAL_DATABASES + 1
        );
        assert_eq!(
            usize::try_from(ROUTE_QUERY_LIMIT).expect("route query limit"),
            MAX_TOTAL_ROUTING_RANGES + 1
        );
        assert_eq!(
            usize::try_from(TABLE_QUERY_LIMIT).expect("table query limit"),
            MAX_TOTAL_REGISTERED_TABLES + 1
        );
        assert!(
            ensure_cardinality("test rows", MAX_LOGICAL_DATABASES, MAX_LOGICAL_DATABASES).is_ok()
        );
        assert!(matches!(
            ensure_cardinality(
                "test rows",
                MAX_LOGICAL_DATABASES + 1,
                MAX_LOGICAL_DATABASES
            ),
            Err(LoadError::CardinalityLimit {
                resource: "test rows",
                maximum: MAX_LOGICAL_DATABASES
            })
        ));
    }
}
