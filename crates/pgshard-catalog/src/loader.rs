//! Transactionally consistent `PostgreSQL` catalog loading.

use pgshard_types::{KeyRange, KeyRangeError, ShardId};
use thiserror::Error;
use tokio_postgres::{Client, IsolationLevel, Row, Transaction};
use uuid::Uuid;

use crate::{
    CatalogCache, CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, DatabaseId,
    IdentifierError, InstallOutcome, RegisteredTable, RoutingHashConfig, ShardKeyType, ShardRoute,
    SnapshotError, TableName,
};

const CONFIGURATION_SQL: &str = "\
    SELECT configuration.cluster_id::text, configuration.hash_version, \
           configuration.hash_seed::text, state.catalog_epoch \
      FROM pgshard_catalog.cluster_configuration AS configuration \
      CROSS JOIN pgshard_catalog.cluster_state AS state \
     WHERE configuration.singleton AND state.singleton";

const DATABASES_SQL: &str = "\
    SELECT databases.logical_database_id::text, databases.database_name, \
           databases.schema_epoch, databases.authorization_epoch, active.routing_epoch \
      FROM pgshard_catalog.logical_databases AS databases \
      JOIN pgshard_catalog.active_routing_epochs AS active \
        ON active.logical_database_id = databases.logical_database_id \
     WHERE databases.state IN ('active', 'draining') \
     ORDER BY databases.logical_database_id";

const ROUTES_SQL: &str = "\
    SELECT shards.shard_number, ranges.range_start::text, ranges.range_end::text \
      FROM pgshard_catalog.routing_ranges AS ranges \
      JOIN pgshard_catalog.shards AS shards ON shards.shard_id = ranges.shard_id \
     WHERE ranges.routing_epoch = $1 \
     ORDER BY ranges.range_start, ranges.range_end";

const TABLES_SQL: &str = "\
    SELECT schema_name, table_name, shard_key_column, shard_key_type, hash_version \
      FROM pgshard_catalog.registered_tables \
     WHERE logical_database_id = $1::text::uuid AND state = 'active' \
     ORDER BY schema_name, table_name";

/// Reads one complete immutable snapshot in a read-only repeatable-read
/// transaction.
///
/// Only logical databases with an active routing epoch are published. Staged
/// metadata remains invisible until the catalog's dual-CAS activation commits.
///
/// # Errors
///
/// Fails closed on SQL errors, missing singleton state, unsupported catalog
/// values, integer conversion errors, or any snapshot invariant violation.
pub async fn load_snapshot(client: &mut Client) -> Result<CatalogSnapshot, LoadError> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let snapshot = load_in_transaction(&transaction).await?;
    transaction.commit().await?;
    Ok(snapshot)
}

/// Loads and monotonically publishes the current authoritative snapshot.
///
/// # Errors
///
/// Returns a load or cache integrity/monotonicity error without changing the
/// current cache state.
pub async fn refresh(
    client: &mut Client,
    cache: &CatalogCache,
) -> Result<InstallOutcome, LoadError> {
    let snapshot = load_snapshot(client).await?;
    cache.install(snapshot).map_err(LoadError::Cache)
}

/// Establishes the notification subscription before the initial snapshot
/// read, closing the standard `LISTEN` startup race.
///
/// The caller must continuously drive the associated `tokio-postgres`
/// connection, parse every notification with [`crate::CatalogNotification`],
/// call [`refresh`] for unseen epochs, and poll periodically because
/// notifications can be lost across disconnects.
///
/// # Errors
///
/// Returns an error if `LISTEN` cannot commit or the initial refresh fails.
pub async fn listen_and_refresh(
    client: &mut Client,
    cache: &CatalogCache,
) -> Result<InstallOutcome, LoadError> {
    client
        .batch_execute("LISTEN pgshard_catalog_changed")
        .await?;
    refresh(client, cache).await
}

async fn load_in_transaction(transaction: &Transaction<'_>) -> Result<CatalogSnapshot, LoadError> {
    let configuration = transaction.query_opt(CONFIGURATION_SQL, &[]).await?;
    let configuration = configuration.ok_or(LoadError::MissingSingleton)?;
    let cluster_id = parse_uuid("cluster_id", configuration.get::<_, String>(0))?;
    let hash_version = positive_u16("hash_version", configuration.get::<_, i16>(1))?;
    let hash_seed = parse_u64("hash_seed", &configuration.get::<_, String>(2))?;
    let catalog_epoch = positive_u64("catalog_epoch", configuration.get::<_, i64>(3))?;

    let mut databases = Vec::new();
    for row in transaction.query(DATABASES_SQL, &[]).await? {
        databases.push(load_database(transaction, &row).await?);
    }
    CatalogSnapshot::new(
        ClusterId::new(cluster_id)?,
        catalog_epoch,
        RoutingHashConfig::new(hash_version, hash_seed)?,
        databases,
    )
    .map_err(LoadError::Snapshot)
}

async fn load_database(
    transaction: &Transaction<'_>,
    row: &Row,
) -> Result<DatabaseCatalog, LoadError> {
    let id_text = row.get::<_, String>(0);
    let id = DatabaseId::new(parse_uuid("logical_database_id", id_text.clone())?)?;
    let name = row.get::<_, String>(1);
    let schema_epoch = positive_u64("schema_epoch", row.get::<_, i64>(2))?;
    let authorization_epoch = positive_u64("authorization_epoch", row.get::<_, i64>(3))?;
    let routing_epoch_i64 = row.get::<_, i64>(4);
    let routing_epoch = positive_u64("routing_epoch", routing_epoch_i64)?;

    let mut routes = Vec::new();
    for route in transaction.query(ROUTES_SQL, &[&routing_epoch_i64]).await? {
        let shard_number = nonnegative_u32("shard_number", route.get::<_, i64>(0))?;
        let start = parse_u128("range_start", &route.get::<_, String>(1))?;
        let end = parse_u128("range_end", &route.get::<_, String>(2))?;
        routes.push(ShardRoute::new(
            ShardId(shard_number),
            KeyRange::new(start, end)?,
        ));
    }

    let mut tables = Vec::new();
    for table in transaction.query(TABLES_SQL, &[&id_text]).await? {
        let schema_name = table.get::<_, String>(0);
        let table_name = table.get::<_, String>(1);
        let column = table.get::<_, String>(2);
        let key_type = parse_shard_key_type(&table.get::<_, String>(3))?;
        let hash_version = positive_u16("table hash_version", table.get::<_, i16>(4))?;
        tables.push(RegisteredTable::new(
            TableName::new(schema_name, table_name)?,
            column,
            key_type,
            hash_version,
        )?);
    }

    DatabaseCatalog::new(
        id,
        name,
        DatabaseEpochs::new(routing_epoch, schema_epoch, authorization_epoch)?,
        routes,
        tables,
    )
    .map_err(LoadError::Snapshot)
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

fn parse_uuid(field: &'static str, value: String) -> Result<Uuid, LoadError> {
    Uuid::parse_str(&value).map_err(|source| LoadError::InvalidUuid {
        field,
        value,
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
    /// A required singleton configuration/state row is absent.
    #[error("shardschema singleton configuration or state row is missing")]
    MissingSingleton,
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
