//! Immutable catalog model and canonical snapshot checksum.

use std::collections::{HashMap, HashSet};
use std::fmt;

use pgshard_types::{CatalogEpoch, KEYSPACE_END, KeyRange, RoutingEpoch, ShardId};
use thiserror::Error;
use uuid::Uuid;

/// Stable cluster identity stored in the singleton catalog-state row.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClusterId(Uuid);

impl ClusterId {
    /// Creates a non-nil cluster identity.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::NilClusterId`] for the nil UUID.
    pub fn new(value: Uuid) -> Result<Self, SnapshotError> {
        if value.is_nil() {
            return Err(SnapshotError::NilClusterId);
        }
        Ok(Self(value))
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable identity of one logical `PostgreSQL` database.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DatabaseId(Uuid);

impl DatabaseId {
    /// Creates a non-nil database identity.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::NilDatabaseId`] for the nil UUID.
    pub fn new(value: Uuid) -> Result<Self, SnapshotError> {
        if value.is_nil() {
            return Err(SnapshotError::NilDatabaseId);
        }
        Ok(Self(value))
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for DatabaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable identity of one shard within a logical database.
///
/// This identity survives physical-cell moves. A restore or reshard creates
/// fresh identities instead of rebinding an existing shard UUID.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DatabaseShardId(Uuid);

impl DatabaseShardId {
    /// Creates a non-nil database-shard identity.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::NilDatabaseShardId`] for the nil UUID.
    pub fn new(value: Uuid) -> Result<Self, SnapshotError> {
        if value.is_nil() {
            return Err(SnapshotError::NilDatabaseShardId);
        }
        Ok(Self(value))
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for DatabaseShardId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Validated `PostgreSQL` schema/table name used as an exact lookup key.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TableName {
    schema: String,
    table: String,
}

impl TableName {
    /// Creates an exact, case-sensitive `PostgreSQL` table name.
    ///
    /// `PostgreSQL` identifiers are limited to 63 UTF-8 bytes by the server.
    /// Quoted names remain case sensitive; no client-side folding occurs.
    ///
    /// # Errors
    ///
    /// Returns [`IdentifierError`] for an empty, overlong, or NUL-containing
    /// component.
    pub fn new(
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, IdentifierError> {
        let schema = schema.into();
        let table = table.into();
        validate_identifier("schema", &schema)?;
        validate_identifier("table", &table)?;
        Ok(Self { schema, table })
    }

    /// Returns the exact schema name.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the exact table name.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }
}

/// Rejected `PostgreSQL` identifier.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind} identifier must contain 1-63 UTF-8 bytes and no NUL")]
pub struct IdentifierError {
    kind: &'static str,
}

fn validate_identifier(kind: &'static str, value: &str) -> Result<(), IdentifierError> {
    if value.is_empty() || value.len() > 63 || value.contains('\0') {
        return Err(IdentifierError { kind });
    }
    Ok(())
}

/// Immutable cluster-wide routing-hash configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingHashConfig {
    version: u16,
    seed: u64,
}

impl RoutingHashConfig {
    /// Creates the supported routing-hash configuration.
    ///
    /// # Errors
    ///
    /// Rejects any version other than [`pgshard_types::RoutingHashV1::VERSION`].
    pub fn new(version: u16, seed: u64) -> Result<Self, SnapshotError> {
        if version != pgshard_types::RoutingHashV1::VERSION {
            return Err(SnapshotError::UnsupportedHashVersion(version));
        }
        Ok(Self { version, seed })
    }

    /// Returns the immutable algorithm version.
    #[must_use]
    pub const fn version(self) -> u16 {
        self.version
    }

    /// Returns the immutable creation-time seed.
    #[must_use]
    pub const fn seed(self) -> u64 {
        self.seed
    }
}

/// Active per-database epoch vector captured in one catalog transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DatabaseEpochs {
    routing: RoutingEpoch,
    schema: u64,
    authorization: u64,
}

impl DatabaseEpochs {
    /// Creates a nonzero epoch vector.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::ZeroEpoch`] when any component is zero.
    pub fn new(routing: u64, schema: u64, authorization: u64) -> Result<Self, SnapshotError> {
        for (kind, value) in [
            ("routing", routing),
            ("schema", schema),
            ("authorization", authorization),
        ] {
            if value == 0 {
                return Err(SnapshotError::ZeroEpoch(kind));
            }
        }
        Ok(Self {
            routing: RoutingEpoch(routing),
            schema,
            authorization,
        })
    }

    /// Returns the active routing epoch.
    #[must_use]
    pub const fn routing(self) -> RoutingEpoch {
        self.routing
    }

    /// Returns the active managed-schema epoch.
    #[must_use]
    pub const fn schema(self) -> u64 {
        self.schema
    }

    /// Returns the active authorization epoch.
    #[must_use]
    pub const fn authorization(self) -> u64 {
        self.authorization
    }
}

/// One active half-open key range, stable database shard, and physical placement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardRoute {
    database_shard_id: DatabaseShardId,
    placement_generation: u64,
    shard_id: ShardId,
    key_range: KeyRange,
}

impl ShardRoute {
    /// Creates a route from an already validated key range.
    #[must_use]
    pub const fn new(
        database_shard_id: DatabaseShardId,
        placement_generation: u64,
        shard_id: ShardId,
        key_range: KeyRange,
    ) -> Self {
        Self {
            database_shard_id,
            placement_generation,
            shard_id,
            key_range,
        }
    }

    /// Returns the stable logical database-shard identity.
    #[must_use]
    pub const fn database_shard_id(self) -> DatabaseShardId {
        self.database_shard_id
    }

    /// Returns the active physical-placement generation.
    #[must_use]
    pub const fn placement_generation(self) -> u64 {
        self.placement_generation
    }

    /// Returns the currently placed physical shard-cell ordinal.
    #[must_use]
    pub const fn shard_id(self) -> ShardId {
        self.shard_id
    }

    /// Returns the half-open key range.
    #[must_use]
    pub const fn key_range(self) -> KeyRange {
        self.key_range
    }
}

/// Supported shard-key representation and canonical hash type tag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardKeyType {
    /// `PostgreSQL` `bigint` / signed 64-bit integer.
    Int64,
    /// `PostgreSQL` UUID in network byte order.
    Uuid,
    /// UTF8 text under `PostgreSQL` built-in `C` collation.
    Text,
    /// `PostgreSQL` `bytea`.
    Bytes,
}

impl ShardKeyType {
    fn tag(self) -> u8 {
        match self {
            Self::Int64 => 1,
            Self::Uuid => 2,
            Self::Text => 3,
            Self::Bytes => 4,
        }
    }
}

/// Routing metadata for one registered application table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredTable {
    name: TableName,
    shard_key_column: String,
    shard_key_type: ShardKeyType,
    hash_version: u16,
}

impl RegisteredTable {
    /// Creates a registered table definition.
    ///
    /// # Errors
    ///
    /// Returns [`IdentifierError`] when the shard-key column is not a valid
    /// `PostgreSQL` identifier.
    pub fn new(
        name: TableName,
        shard_key_column: impl Into<String>,
        shard_key_type: ShardKeyType,
        hash_version: u16,
    ) -> Result<Self, IdentifierError> {
        let shard_key_column = shard_key_column.into();
        validate_identifier("shard-key column", &shard_key_column)?;
        Ok(Self {
            name,
            shard_key_column,
            shard_key_type,
            hash_version,
        })
    }

    /// Returns the exact table lookup key.
    #[must_use]
    pub fn name(&self) -> &TableName {
        &self.name
    }

    /// Returns the exact shard-key column.
    #[must_use]
    pub fn shard_key_column(&self) -> &str {
        &self.shard_key_column
    }

    /// Returns the canonical shard-key type.
    #[must_use]
    pub const fn shard_key_type(&self) -> ShardKeyType {
        self.shard_key_type
    }

    /// Returns the required routing-hash algorithm version.
    #[must_use]
    pub const fn hash_version(&self) -> u16 {
        self.hash_version
    }
}

/// Validated active catalog for one logical database.
#[derive(Clone, Debug)]
pub struct DatabaseCatalog {
    id: DatabaseId,
    name: String,
    epochs: DatabaseEpochs,
    routes: Vec<ShardRoute>,
    tables: HashMap<TableName, RegisteredTable>,
}

impl DatabaseCatalog {
    /// Builds a database catalog with complete, contiguous routing coverage.
    ///
    /// # Errors
    ///
    /// Rejects invalid database names, empty/gapped/overlapping ranges, and
    /// duplicate registered table names.
    pub fn new(
        id: DatabaseId,
        name: impl Into<String>,
        epochs: DatabaseEpochs,
        mut routes: Vec<ShardRoute>,
        tables: Vec<RegisteredTable>,
    ) -> Result<Self, SnapshotError> {
        let name = name.into();
        validate_identifier("database", &name)?;
        routes.sort_by_key(|route| route.key_range.start());
        validate_routes(&routes)?;

        let mut placements = HashMap::with_capacity(routes.len());
        let mut database_shards = HashMap::with_capacity(routes.len());
        for route in &routes {
            if route.placement_generation == 0 {
                return Err(SnapshotError::ZeroPlacementGeneration(
                    route.database_shard_id,
                ));
            }
            if let Some((existing_generation, existing_shard)) = placements.insert(
                route.database_shard_id,
                (route.placement_generation, route.shard_id),
            ) {
                if existing_generation != route.placement_generation {
                    return Err(SnapshotError::DatabaseShardGenerationConflict {
                        database_shard_id: route.database_shard_id,
                        first: existing_generation,
                        second: route.placement_generation,
                    });
                }
                if existing_shard != route.shard_id {
                    return Err(SnapshotError::DatabaseShardPlacementConflict {
                        database_shard_id: route.database_shard_id,
                        first: existing_shard,
                        second: route.shard_id,
                    });
                }
            }
            if let Some(existing) = database_shards.insert(route.shard_id, route.database_shard_id)
                && existing != route.database_shard_id
            {
                return Err(SnapshotError::PhysicalShardPlacementConflict {
                    shard_id: route.shard_id,
                    first: existing,
                    second: route.database_shard_id,
                });
            }
        }

        let mut table_map = HashMap::with_capacity(tables.len());
        for table in tables {
            let key = table.name.clone();
            if table_map.insert(key.clone(), table).is_some() {
                return Err(SnapshotError::DuplicateTable {
                    schema: key.schema,
                    table: key.table,
                });
            }
        }
        Ok(Self {
            id,
            name,
            epochs,
            routes,
            tables: table_map,
        })
    }

    /// Returns the stable database identity.
    #[must_use]
    pub const fn id(&self) -> DatabaseId {
        self.id
    }

    /// Returns the exact `PostgreSQL` database name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the active epoch vector.
    #[must_use]
    pub const fn epochs(&self) -> DatabaseEpochs {
        self.epochs
    }

    /// Returns active routes ordered by range start.
    #[must_use]
    pub fn routes(&self) -> &[ShardRoute] {
        &self.routes
    }

    /// Looks up registered routing metadata by exact table name.
    #[must_use]
    pub fn table(&self, name: &TableName) -> Option<&RegisteredTable> {
        self.tables.get(name)
    }

    /// Returns the number of registered tables.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Maps a version-one hash to its active shard in logarithmic time.
    #[must_use]
    pub fn route(&self, hash: u64) -> ShardId {
        let hash = u128::from(hash);
        let upper = self
            .routes
            .partition_point(|route| route.key_range.start() <= hash);
        self.routes[upper.saturating_sub(1)].shard_id
    }
}

fn validate_routes(routes: &[ShardRoute]) -> Result<(), SnapshotError> {
    let Some(first) = routes.first() else {
        return Err(SnapshotError::EmptyRouting);
    };
    if first.key_range.start() != 0 {
        return Err(SnapshotError::RoutingDoesNotStartAtZero(
            first.key_range.start(),
        ));
    }
    for pair in routes.windows(2) {
        let previous_end = pair[0].key_range.end();
        let next_start = pair[1].key_range.start();
        if previous_end != next_start {
            return Err(SnapshotError::RoutingBoundaryMismatch {
                previous_end,
                next_start,
            });
        }
    }
    let end = routes
        .last()
        .expect("nonempty checked above")
        .key_range
        .end();
    if end != KEYSPACE_END {
        return Err(SnapshotError::RoutingDoesNotEndAtKeyspace(end));
    }
    Ok(())
}

/// Fully validated, immutable cluster catalog captured at one catalog epoch.
#[derive(Clone, Debug)]
pub struct CatalogSnapshot {
    cluster_id: ClusterId,
    catalog_epoch: CatalogEpoch,
    routing_hash: RoutingHashConfig,
    databases: HashMap<DatabaseId, DatabaseCatalog>,
    checksum: [u8; 32],
}

impl CatalogSnapshot {
    /// Builds and canonically checksums a catalog snapshot.
    ///
    /// # Errors
    ///
    /// Rejects regression-shaped input, duplicate database IDs or names, and
    /// table/hash-version inconsistencies. Catalog epoch zero is the valid
    /// empty genesis state created by the migration.
    pub fn new(
        cluster_id: ClusterId,
        catalog_epoch: u64,
        routing_hash: RoutingHashConfig,
        databases: Vec<DatabaseCatalog>,
    ) -> Result<Self, SnapshotError> {
        let catalog_epoch = CatalogEpoch(catalog_epoch);
        let mut database_map = HashMap::with_capacity(databases.len());
        let mut names = HashSet::with_capacity(databases.len());
        let mut database_shard_owners = HashMap::new();
        for database in databases {
            for (kind, epoch) in [
                ("routing", database.epochs.routing.0),
                ("schema", database.epochs.schema),
                ("authorization", database.epochs.authorization),
            ] {
                if epoch > catalog_epoch.0 {
                    return Err(SnapshotError::EpochExceedsCatalog {
                        kind,
                        epoch,
                        catalog_epoch: catalog_epoch.0,
                    });
                }
            }
            for table in database.tables.values() {
                if table.hash_version != routing_hash.version {
                    return Err(SnapshotError::TableHashVersionMismatch {
                        schema: table.name.schema.clone(),
                        table: table.name.table.clone(),
                        expected: routing_hash.version,
                        actual: table.hash_version,
                    });
                }
            }
            if !names.insert(database.name.clone()) {
                return Err(SnapshotError::DuplicateDatabaseName(database.name));
            }
            for route in &database.routes {
                if let Some(owner) =
                    database_shard_owners.insert(route.database_shard_id, database.id)
                    && owner != database.id
                {
                    return Err(SnapshotError::DatabaseShardIdentityReused {
                        database_shard_id: route.database_shard_id,
                        first: owner,
                        second: database.id,
                    });
                }
            }
            let id = database.id;
            if database_map.insert(id, database).is_some() {
                return Err(SnapshotError::DuplicateDatabaseId(id));
            }
        }
        let mut snapshot = Self {
            cluster_id,
            catalog_epoch,
            routing_hash,
            databases: database_map,
            checksum: [0; 32],
        };
        snapshot.checksum = snapshot.compute_checksum();
        Ok(snapshot)
    }

    /// Recomputes and validates the in-memory checksum.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::ChecksumMismatch`] if the snapshot is corrupt.
    pub fn verify_checksum(&self) -> Result<(), SnapshotError> {
        let actual = self.compute_checksum();
        if self.checksum != actual {
            return Err(SnapshotError::ChecksumMismatch {
                expected: self.checksum,
                actual,
            });
        }
        Ok(())
    }

    /// Returns the immutable cluster identity.
    #[must_use]
    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// Returns the global catalog epoch.
    #[must_use]
    pub const fn catalog_epoch(&self) -> CatalogEpoch {
        self.catalog_epoch
    }

    /// Returns the immutable routing-hash configuration.
    #[must_use]
    pub const fn routing_hash(&self) -> RoutingHashConfig {
        self.routing_hash
    }

    /// Returns the canonical BLAKE3 snapshot checksum.
    #[must_use]
    pub const fn checksum(&self) -> [u8; 32] {
        self.checksum
    }

    /// Finds one logical database by stable identity.
    #[must_use]
    pub fn database(&self, id: DatabaseId) -> Option<&DatabaseCatalog> {
        self.databases.get(&id)
    }

    /// Finds one logical database by its immutable exact name.
    #[must_use]
    pub fn database_by_name(&self, name: &str) -> Option<&DatabaseCatalog> {
        self.databases
            .values()
            .find(|database| database.name == name)
    }

    /// Returns the number of logical databases.
    #[must_use]
    pub fn database_count(&self) -> usize {
        self.databases.len()
    }

    pub(crate) fn databases(&self) -> impl Iterator<Item = &DatabaseCatalog> {
        self.databases.values()
    }

    fn compute_checksum(&self) -> [u8; 32] {
        let mut writer = ChecksumWriter::new();
        writer.bytes(b"pgshard-catalog-snapshot-v2");
        writer.bytes(self.cluster_id.0.as_bytes());
        writer.u64(self.catalog_epoch.0);
        writer.u16(self.routing_hash.version);
        writer.u64(self.routing_hash.seed);

        let mut databases: Vec<_> = self.databases.values().collect();
        databases.sort_by_key(|database| database.id);
        writer.u64(databases.len() as u64);
        for database in databases {
            writer.bytes(database.id.0.as_bytes());
            writer.string(&database.name);
            writer.u64(database.epochs.routing.0);
            writer.u64(database.epochs.schema);
            writer.u64(database.epochs.authorization);
            writer.u64(database.routes.len() as u64);
            for route in &database.routes {
                writer.bytes(route.database_shard_id.0.as_bytes());
                writer.u64(route.placement_generation);
                writer.u32(route.shard_id.0);
                writer.u128(route.key_range.start());
                writer.u128(route.key_range.end());
            }

            let mut tables: Vec<_> = database.tables.values().collect();
            tables.sort_by(|left, right| left.name.cmp(&right.name));
            writer.u64(tables.len() as u64);
            for table in tables {
                writer.string(&table.name.schema);
                writer.string(&table.name.table);
                writer.string(&table.shard_key_column);
                writer.byte(table.shard_key_type.tag());
                writer.u16(table.hash_version);
            }
        }
        *writer.finish().as_bytes()
    }
}

struct ChecksumWriter(blake3::Hasher);

impl ChecksumWriter {
    fn new() -> Self {
        Self(blake3::Hasher::new())
    }

    fn byte(&mut self, value: u8) {
        self.0.update(&[value]);
    }

    fn u16(&mut self, value: u16) {
        self.0.update(&value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.0.update(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(&value.to_be_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.0.update(&value.to_be_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.0.update(value);
    }

    fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    fn finish(self) -> blake3::Hash {
        self.0.finalize()
    }
}

/// Catalog snapshot construction or integrity failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SnapshotError {
    /// The cluster UUID is nil.
    #[error("cluster ID must not be nil")]
    NilClusterId,
    /// A database UUID is nil.
    #[error("database ID must not be nil")]
    NilDatabaseId,
    /// A database-shard UUID is nil.
    #[error("database shard ID must not be nil")]
    NilDatabaseShardId,
    /// An identifier violates `PostgreSQL` length/content rules.
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    /// An epoch is zero.
    #[error("{0} epoch must be positive")]
    ZeroEpoch(&'static str),
    /// A database subepoch is ahead of its containing catalog epoch.
    #[error("{kind} epoch {epoch} exceeds catalog epoch {catalog_epoch}")]
    EpochExceedsCatalog {
        /// Epoch category.
        kind: &'static str,
        /// Invalid subepoch.
        epoch: u64,
        /// Containing global epoch.
        catalog_epoch: u64,
    },
    /// The cache cannot execute an unknown hash contract.
    #[error("unsupported routing hash version {0}")]
    UnsupportedHashVersion(u16),
    /// No active shard ranges exist.
    #[error("active routing must contain at least one range")]
    EmptyRouting,
    /// Active coverage does not begin at zero.
    #[error("active routing begins at {0}, not zero")]
    RoutingDoesNotStartAtZero(u128),
    /// Adjacent active ranges have a gap or overlap.
    #[error("routing boundary mismatch: previous end {previous_end}, next start {next_start}")]
    RoutingBoundaryMismatch {
        /// Exclusive end of the prior range.
        previous_end: u128,
        /// Inclusive start of the next range.
        next_start: u128,
    },
    /// Active coverage does not end at `2^64`.
    #[error("active routing ends at {0}, not 2^64")]
    RoutingDoesNotEndAtKeyspace(u128),
    /// A table name appears more than once.
    #[error("duplicate registered table {schema}.{table}")]
    DuplicateTable {
        /// Exact schema name.
        schema: String,
        /// Exact table name.
        table: String,
    },
    /// A database ID appears more than once.
    #[error("duplicate database ID {0}")]
    DuplicateDatabaseId(DatabaseId),
    /// A database name appears more than once.
    #[error("duplicate database name {0:?}")]
    DuplicateDatabaseName(String),
    /// A physical placement generation is not a positive fencing token.
    #[error("database shard {0} has zero placement generation")]
    ZeroPlacementGeneration(DatabaseShardId),
    /// One database-shard identity has multiple active placement generations.
    #[error(
        "database shard {database_shard_id} resolves to placement generations {first} and {second}"
    )]
    DatabaseShardGenerationConflict {
        /// Stable database-shard identity.
        database_shard_id: DatabaseShardId,
        /// First active placement generation.
        first: u64,
        /// Conflicting active placement generation.
        second: u64,
    },
    /// One database-shard identity resolves to two physical cells.
    #[error(
        "database shard {database_shard_id} resolves to physical shards {first:?} and {second:?}"
    )]
    DatabaseShardPlacementConflict {
        /// Stable database-shard identity.
        database_shard_id: DatabaseShardId,
        /// First physical shard.
        first: ShardId,
        /// Conflicting physical shard.
        second: ShardId,
    },
    /// One physical cell serves two database-shard identities in one database.
    #[error(
        "physical shard {shard_id:?} resolves database shards {first} and {second} in one logical database"
    )]
    PhysicalShardPlacementConflict {
        /// Reused physical shard.
        shard_id: ShardId,
        /// First permanent database-shard identity.
        first: DatabaseShardId,
        /// Conflicting permanent database-shard identity.
        second: DatabaseShardId,
    },
    /// A globally unique database-shard identity appears in two databases.
    #[error(
        "database shard {database_shard_id} is reused by logical databases {first} and {second}"
    )]
    DatabaseShardIdentityReused {
        /// Reused database-shard identity.
        database_shard_id: DatabaseShardId,
        /// First logical database owner.
        first: DatabaseId,
        /// Conflicting logical database owner.
        second: DatabaseId,
    },
    /// A table refers to a different hash algorithm.
    #[error("table {schema}.{table} uses hash version {actual}, expected {expected}")]
    TableHashVersionMismatch {
        /// Exact schema name.
        schema: String,
        /// Exact table name.
        table: String,
        /// Cluster hash version.
        expected: u16,
        /// Table hash version.
        actual: u16,
    },
    /// Canonical checksum differs from the stored value.
    #[error("catalog snapshot checksum mismatch")]
    ChecksumMismatch {
        /// Checksum expected by the caller or snapshot.
        expected: [u8; 32],
        /// Recomputed checksum.
        actual: [u8; 32],
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: u128) -> DatabaseId {
        DatabaseId::new(Uuid::from_u128(value)).expect("nonzero database ID")
    }

    fn database_shard_id(value: u128) -> DatabaseShardId {
        DatabaseShardId::new(Uuid::from_u128(value)).expect("nonzero database shard ID")
    }

    fn route(shard_number: u32, key_range: KeyRange) -> ShardRoute {
        ShardRoute::new(
            database_shard_id(100 + u128::from(shard_number)),
            1,
            ShardId(shard_number),
            key_range,
        )
    }

    fn cluster_id() -> ClusterId {
        ClusterId::new(Uuid::from_u128(1)).expect("nonzero cluster ID")
    }

    fn table(name: &str) -> RegisteredTable {
        RegisteredTable::new(
            TableName::new("public", name).expect("table name"),
            "tenant_id",
            ShardKeyType::Int64,
            1,
        )
        .expect("registered table")
    }

    fn database(epoch: u64) -> DatabaseCatalog {
        DatabaseCatalog::new(
            id(2),
            "app",
            DatabaseEpochs::new(epoch, epoch, epoch).expect("epochs"),
            vec![
                route(0, KeyRange::new(0, 1_u128 << 63).expect("first range")),
                route(
                    1,
                    KeyRange::new(1_u128 << 63, KEYSPACE_END).expect("second range"),
                ),
            ],
            vec![table("events")],
        )
        .expect("database catalog")
    }

    #[test]
    fn routes_cover_every_hash_boundary() {
        let database = database(1);
        assert_eq!(database.route(0), ShardId(0));
        assert_eq!(database.route((1_u64 << 63) - 1), ShardId(0));
        assert_eq!(database.route(1_u64 << 63), ShardId(1));
        assert_eq!(database.route(u64::MAX), ShardId(1));
    }

    #[test]
    fn empty_genesis_snapshot_accepts_catalog_epoch_zero() {
        let snapshot = CatalogSnapshot::new(
            cluster_id(),
            0,
            RoutingHashConfig::new(1, 42).expect("hash"),
            vec![],
        )
        .expect("genesis snapshot");
        assert_eq!(snapshot.catalog_epoch(), CatalogEpoch(0));
        assert_eq!(snapshot.database_count(), 0);
        snapshot.verify_checksum().expect("genesis checksum");
    }

    #[test]
    fn rejects_gaps_overlaps_and_incomplete_coverage() {
        let epochs = DatabaseEpochs::new(1, 1, 1).expect("epochs");
        for ranges in [
            vec![route(0, KeyRange::new(1, KEYSPACE_END).expect("range"))],
            vec![
                route(0, KeyRange::new(0, 10).expect("range")),
                route(1, KeyRange::new(11, KEYSPACE_END).expect("range")),
            ],
            vec![
                route(0, KeyRange::new(0, 11).expect("range")),
                route(1, KeyRange::new(10, KEYSPACE_END).expect("range")),
            ],
            vec![route(0, KeyRange::new(0, 10).expect("range"))],
        ] {
            assert!(DatabaseCatalog::new(id(2), "app", epochs, ranges, vec![]).is_err());
        }
    }

    #[test]
    fn database_shard_identity_must_be_non_nil() {
        assert!(matches!(
            DatabaseShardId::new(Uuid::nil()),
            Err(SnapshotError::NilDatabaseShardId)
        ));
    }

    #[test]
    fn placement_generation_must_be_positive_and_consistent() {
        let database_shard_id = database_shard_id(100);
        let epochs = DatabaseEpochs::new(1, 1, 1).expect("epochs");
        let zero = vec![ShardRoute::new(
            database_shard_id,
            0,
            ShardId(0),
            KeyRange::new(0, KEYSPACE_END).expect("range"),
        )];
        assert!(matches!(
            DatabaseCatalog::new(id(2), "app", epochs, zero, vec![]),
            Err(SnapshotError::ZeroPlacementGeneration(_))
        ));

        let inconsistent = vec![
            ShardRoute::new(
                database_shard_id,
                1,
                ShardId(0),
                KeyRange::new(0, 1_u128 << 63).expect("first range"),
            ),
            ShardRoute::new(
                database_shard_id,
                2,
                ShardId(0),
                KeyRange::new(1_u128 << 63, KEYSPACE_END).expect("second range"),
            ),
        ];
        assert!(matches!(
            DatabaseCatalog::new(id(2), "app", epochs, inconsistent, vec![]),
            Err(SnapshotError::DatabaseShardGenerationConflict { .. })
        ));
    }

    #[test]
    fn rejects_one_database_shard_on_two_physical_shards() {
        let database_shard_id = database_shard_id(100);
        let routes = vec![
            ShardRoute::new(
                database_shard_id,
                1,
                ShardId(0),
                KeyRange::new(0, 1_u128 << 63).expect("first range"),
            ),
            ShardRoute::new(
                database_shard_id,
                1,
                ShardId(1),
                KeyRange::new(1_u128 << 63, KEYSPACE_END).expect("second range"),
            ),
        ];
        assert!(matches!(
            DatabaseCatalog::new(
                id(2),
                "app",
                DatabaseEpochs::new(1, 1, 1).expect("epochs"),
                routes,
                vec![]
            ),
            Err(SnapshotError::DatabaseShardPlacementConflict { .. })
        ));
    }

    #[test]
    fn rejects_two_database_shards_on_one_physical_shard() {
        let routes = vec![
            ShardRoute::new(
                database_shard_id(100),
                1,
                ShardId(0),
                KeyRange::new(0, 1_u128 << 63).expect("first range"),
            ),
            ShardRoute::new(
                database_shard_id(101),
                1,
                ShardId(0),
                KeyRange::new(1_u128 << 63, KEYSPACE_END).expect("second range"),
            ),
        ];
        assert!(matches!(
            DatabaseCatalog::new(
                id(2),
                "app",
                DatabaseEpochs::new(1, 1, 1).expect("epochs"),
                routes,
                vec![]
            ),
            Err(SnapshotError::PhysicalShardPlacementConflict { .. })
        ));
    }

    #[test]
    fn rejects_database_shard_identity_reuse_across_databases() {
        let first = database(1);
        let mut second = database(1);
        second.id = id(3);
        second.name = "other".to_owned();
        assert!(matches!(
            CatalogSnapshot::new(
                cluster_id(),
                2,
                RoutingHashConfig::new(1, 42).expect("hash"),
                vec![first, second]
            ),
            Err(SnapshotError::DatabaseShardIdentityReused { .. })
        ));
    }

    #[test]
    fn checksum_covers_database_shard_identity_and_placement() {
        let hash = RoutingHashConfig::new(1, 42).expect("hash");
        let original = CatalogSnapshot::new(cluster_id(), 2, hash, vec![database(1)])
            .expect("original")
            .checksum();

        let mut changed_identity = database(1);
        changed_identity.routes[0].database_shard_id = database_shard_id(999);
        let changed_identity = CatalogSnapshot::new(cluster_id(), 2, hash, vec![changed_identity])
            .expect("changed identity")
            .checksum();

        let mut changed_placement = database(1);
        changed_placement.routes[0].shard_id = ShardId(9);
        let changed_placement =
            CatalogSnapshot::new(cluster_id(), 2, hash, vec![changed_placement])
                .expect("changed placement")
                .checksum();

        let mut changed_generation = database(1);
        changed_generation.routes[0].placement_generation = 2;
        let changed_generation =
            CatalogSnapshot::new(cluster_id(), 2, hash, vec![changed_generation])
                .expect("changed generation")
                .checksum();

        assert_ne!(original, changed_identity);
        assert_ne!(original, changed_placement);
        assert_ne!(original, changed_generation);
    }

    #[test]
    fn checksum_is_order_independent_but_detects_content_change() {
        let hash = RoutingHashConfig::new(1, 42).expect("hash");
        let left = CatalogSnapshot::new(cluster_id(), 2, hash, vec![database(1)]).expect("left");

        let mut second = database(1);
        second.id = id(3);
        second.name = "other".to_owned();
        for (index, route) in second.routes.iter_mut().enumerate() {
            route.database_shard_id =
                database_shard_id(200 + u128::try_from(index).expect("route index fits u128"));
        }
        let first = database(1);
        let ordered =
            CatalogSnapshot::new(cluster_id(), 2, hash, vec![first.clone(), second.clone()])
                .expect("ordered");
        let reversed =
            CatalogSnapshot::new(cluster_id(), 2, hash, vec![second, first]).expect("reversed");
        assert_eq!(ordered.checksum(), reversed.checksum());
        assert_ne!(left.checksum(), ordered.checksum());
        assert!(left.verify_checksum().is_ok());
    }

    #[test]
    fn rejects_mismatched_hash_version() {
        let hash = RoutingHashConfig::new(1, 42).expect("hash");
        let mut wrong_table = table("events");
        wrong_table.hash_version = 2;
        let database = DatabaseCatalog::new(
            id(2),
            "app",
            DatabaseEpochs::new(1, 1, 1).expect("epochs"),
            vec![route(0, KeyRange::new(0, KEYSPACE_END).expect("range"))],
            vec![wrong_table],
        )
        .expect("database model permits snapshot-level hash check");
        assert!(matches!(
            CatalogSnapshot::new(cluster_id(), 1, hash, vec![database]),
            Err(SnapshotError::TableHashVersionMismatch { .. })
        ));
    }

    #[test]
    fn identifiers_follow_postgres_byte_limit() {
        assert!(TableName::new("public", "x".repeat(63)).is_ok());
        assert!(TableName::new("public", "x".repeat(64)).is_err());
        assert!(TableName::new("", "events").is_err());
        assert!(TableName::new("public", "bad\0name").is_err());
    }
}
