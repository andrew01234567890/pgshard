//! Fail-closed routing for already-resolved `PostgreSQL` bind parameters.
//!
//! SQL parsing and wire-protocol handling are deliberately outside this crate.
//! The caller must resolve one registered table and its shard-key parameter
//! before invoking this hot-path core.

use pgshard_catalog::{CatalogSnapshot, DatabaseId, ShardKeyType, TableName};
pub use pgshard_pgwire::{ClientEncoding, ClientEncodingError};
use pgshard_types::{CatalogEpoch, RoutingHashV1, ShardId, ShardKey};
use thiserror::Error;

/// `PostgreSQL` bind-parameter representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParameterFormat {
    /// `PostgreSQL` text format.
    Text,
    /// `PostgreSQL` binary format.
    Binary,
}

/// Immutable result attached to a planned request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutePlan {
    catalog_epoch: CatalogEpoch,
    shard_id: ShardId,
    hash: u64,
}

impl RoutePlan {
    /// Returns the exact catalog epoch retained for execution fencing.
    #[must_use]
    pub const fn catalog_epoch(self) -> CatalogEpoch {
        self.catalog_epoch
    }

    /// Returns the logical target shard.
    #[must_use]
    pub const fn shard_id(self) -> ShardId {
        self.shard_id
    }

    /// Returns the version-one hash used for routing.
    #[must_use]
    pub const fn hash(self) -> u64 {
        self.hash
    }
}

/// Routes one non-null shard-key bind value against an immutable snapshot.
///
/// Binary format is required for `bigint`, UUID and `bytea` keys so routing
/// never approximates `PostgreSQL`'s input grammar. Text keys accept either
/// format only after the session layer proves `client_encoding=UTF8`.
/// `PostgreSQL` converts both text and binary `text` binds from `client_encoding`
/// before storage, so hashing raw bytes from any other encoding is unsafe.
///
/// # Errors
///
/// Fails closed for an unknown database/table, NULL, unsupported parameter
/// format, malformed length, or invalid UTF8.
pub fn route_bound_parameter(
    snapshot: &CatalogSnapshot,
    database_id: DatabaseId,
    table_name: &TableName,
    _client_encoding: ClientEncoding,
    format: ParameterFormat,
    value: Option<&[u8]>,
) -> Result<RoutePlan, RouteError> {
    let database = snapshot
        .database(database_id)
        .ok_or(RouteError::UnknownDatabase(database_id))?;
    let table = database
        .table(table_name)
        .ok_or_else(|| RouteError::UnknownTable(table_name.clone()))?;
    let value = value.ok_or(RouteError::NullShardKey)?;
    let decoded = DecodedKey::decode(table.shard_key_type(), format, value)?;
    let hash_configuration = snapshot.routing_hash();
    let hash = RoutingHashV1::new(hash_configuration.seed()).hash(decoded.as_key());
    Ok(RoutePlan {
        catalog_epoch: snapshot.catalog_epoch(),
        shard_id: database.route(hash),
        hash,
    })
}

enum DecodedKey<'a> {
    Int64(i64),
    Uuid([u8; 16]),
    Text(&'a str),
    Bytes(&'a [u8]),
}

impl<'a> DecodedKey<'a> {
    fn decode(
        key_type: ShardKeyType,
        format: ParameterFormat,
        value: &'a [u8],
    ) -> Result<Self, RouteError> {
        match (key_type, format) {
            (ShardKeyType::Int64, ParameterFormat::Binary) => {
                let bytes: [u8; 8] = value.try_into().map_err(|_| RouteError::InvalidLength {
                    key_type,
                    expected: 8,
                    actual: value.len(),
                })?;
                Ok(Self::Int64(i64::from_be_bytes(bytes)))
            }
            (ShardKeyType::Uuid, ParameterFormat::Binary) => {
                let bytes: [u8; 16] = value.try_into().map_err(|_| RouteError::InvalidLength {
                    key_type,
                    expected: 16,
                    actual: value.len(),
                })?;
                Ok(Self::Uuid(bytes))
            }
            (ShardKeyType::Text, ParameterFormat::Text | ParameterFormat::Binary) => {
                let value = std::str::from_utf8(value)?;
                if value.as_bytes().contains(&0) {
                    return Err(RouteError::TextContainsNul);
                }
                Ok(Self::Text(value))
            }
            (ShardKeyType::Bytes, ParameterFormat::Binary) => Ok(Self::Bytes(value)),
            _ => Err(RouteError::UnsupportedFormat { key_type, format }),
        }
    }

    fn as_key(&self) -> ShardKey<'_> {
        match self {
            Self::Int64(value) => ShardKey::Integer(*value),
            Self::Uuid(value) => ShardKey::Uuid(value),
            Self::Text(value) => ShardKey::Text(value),
            Self::Bytes(value) => ShardKey::Bytes(value),
        }
    }
}

/// Bound-parameter routing failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RouteError {
    /// The snapshot does not contain the requested logical database.
    #[error("unknown logical database {0}")]
    UnknownDatabase(DatabaseId),
    /// The table is not registered for shard routing.
    #[error("unregistered table {}.{}", .0.schema(), .0.table())]
    UnknownTable(TableName),
    /// Shard keys cannot be NULL.
    #[error("shard-key parameter must not be NULL")]
    NullShardKey,
    /// Text input is intentionally unsupported for a type whose `PostgreSQL`
    /// input grammar is not implemented here.
    #[error("unsupported {format:?} format for {key_type:?} shard key")]
    UnsupportedFormat {
        /// Registered shard-key type.
        key_type: ShardKeyType,
        /// Received wire format.
        format: ParameterFormat,
    },
    /// Binary input has the wrong fixed width.
    #[error("invalid {key_type:?} shard-key length {actual}; expected {expected}")]
    InvalidLength {
        /// Registered shard-key type.
        key_type: ShardKeyType,
        /// Required byte width.
        expected: usize,
        /// Received byte width.
        actual: usize,
    },
    /// A text shard key is not valid UTF8.
    #[error("text shard key is not valid UTF8")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    /// `PostgreSQL` rejects the zero byte in both text and binary `text`
    /// input before storing a value.
    #[error("text shard key contains a NUL byte")]
    TextContainsNul,
}

#[cfg(test)]
mod tests {
    use pgshard_catalog::{
        CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, RegisteredTable,
        RoutingHashConfig, ShardRoute,
    };
    use pgshard_types::{KEYSPACE_END, KeyRange};
    use uuid::Uuid;

    use super::*;

    fn utf8() -> ClientEncoding {
        ClientEncoding::require_utf8("UTF8").expect("UTF8")
    }

    fn ids() -> (ClusterId, DatabaseId) {
        (
            ClusterId::new(Uuid::from_u128(1)).expect("cluster ID"),
            DatabaseId::new(Uuid::from_u128(2)).expect("database ID"),
        )
    }

    fn snapshot(key_type: ShardKeyType) -> (CatalogSnapshot, DatabaseId, TableName) {
        let (cluster_id, database_id) = ids();
        let table_name = TableName::new("public", "events").expect("table name");
        let table = RegisteredTable::new(
            table_name.clone(),
            "tenant_id",
            key_type,
            RoutingHashV1::VERSION,
        )
        .expect("registered table");
        let database = DatabaseCatalog::new(
            database_id,
            "app",
            DatabaseEpochs::new(1, 1, 1).expect("epochs"),
            vec![
                ShardRoute::new(ShardId(0), KeyRange::new(0, 1_u128 << 63).expect("range")),
                ShardRoute::new(
                    ShardId(1),
                    KeyRange::new(1_u128 << 63, KEYSPACE_END).expect("range"),
                ),
            ],
            vec![table],
        )
        .expect("database");
        (
            CatalogSnapshot::new(
                cluster_id,
                1,
                RoutingHashConfig::new(1, 42).expect("hash"),
                vec![database],
            )
            .expect("snapshot"),
            database_id,
            table_name,
        )
    }

    #[test]
    fn bigint_binary_matches_canonical_hash_and_route() {
        let (snapshot, database_id, table) = snapshot(ShardKeyType::Int64);
        let value = 42_i64.to_be_bytes();
        let plan = route_bound_parameter(
            &snapshot,
            database_id,
            &table,
            utf8(),
            ParameterFormat::Binary,
            Some(&value),
        )
        .expect("route");
        let expected = RoutingHashV1::new(42).hash(ShardKey::Integer(42));
        assert_eq!(plan.hash(), expected);
        assert_eq!(
            plan.shard_id(),
            if expected < (1_u64 << 63) {
                ShardId(0)
            } else {
                ShardId(1)
            }
        );
        assert_eq!(plan.catalog_epoch(), CatalogEpoch(1));
    }

    #[test]
    fn every_supported_binary_type_matches_core_hash() {
        let uuid = Uuid::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff);
        let cases = [
            (
                ShardKeyType::Uuid,
                uuid.as_bytes().as_slice(),
                ShardKey::Uuid(uuid.as_bytes()),
            ),
            (
                ShardKeyType::Text,
                "tenant-α".as_bytes(),
                ShardKey::Text("tenant-α"),
            ),
            (
                ShardKeyType::Bytes,
                b"\0\xff".as_slice(),
                ShardKey::Bytes(b"\0\xff"),
            ),
        ];
        for (key_type, bytes, key) in cases {
            let (snapshot, database_id, table) = snapshot(key_type);
            let plan = route_bound_parameter(
                &snapshot,
                database_id,
                &table,
                utf8(),
                ParameterFormat::Binary,
                Some(bytes),
            )
            .expect("route");
            assert_eq!(plan.hash(), RoutingHashV1::new(42).hash(key));
        }
    }

    #[test]
    fn malformed_or_ambiguous_values_fail_closed() {
        for (key_type, format, value) in [
            (ShardKeyType::Int64, ParameterFormat::Text, b"42".as_slice()),
            (
                ShardKeyType::Uuid,
                ParameterFormat::Text,
                b"00000000-0000-0000-0000-000000000001".as_slice(),
            ),
            (
                ShardKeyType::Bytes,
                ParameterFormat::Text,
                b"\\x00".as_slice(),
            ),
            (
                ShardKeyType::Int64,
                ParameterFormat::Binary,
                b"short".as_slice(),
            ),
            (
                ShardKeyType::Text,
                ParameterFormat::Text,
                b"\xff".as_slice(),
            ),
        ] {
            let (snapshot, database_id, table) = snapshot(key_type);
            assert!(
                route_bound_parameter(&snapshot, database_id, &table, utf8(), format, Some(value),)
                    .is_err()
            );
        }
        let (integer_snapshot, database_id, table) = snapshot(ShardKeyType::Int64);
        assert_eq!(
            route_bound_parameter(
                &integer_snapshot,
                database_id,
                &table,
                utf8(),
                ParameterFormat::Binary,
                None,
            ),
            Err(RouteError::NullShardKey)
        );

        let (snapshot, database_id, table) = snapshot(ShardKeyType::Text);
        for format in [ParameterFormat::Text, ParameterFormat::Binary] {
            assert_eq!(
                route_bound_parameter(
                    &snapshot,
                    database_id,
                    &table,
                    utf8(),
                    format,
                    Some(b"before\0after"),
                ),
                Err(RouteError::TextContainsNul)
            );
        }
    }

    #[test]
    fn non_utf8_sessions_cannot_create_routing_proof_for_text_binds() {
        for encoding in ["LATIN1", "SQL_ASCII", "utf8", "UNICODE"] {
            assert_eq!(
                ClientEncoding::require_utf8(encoding),
                Err(ClientEncodingError),
                "encoding {encoding:?}"
            );
        }

        // C3 A9 is valid UTF-8 for é, but in a LATIN1 session PostgreSQL
        // converts those two source bytes to UTF-8 for Ã©. A caller cannot
        // obtain the required proof and accidentally hash the raw bytes for
        // either wire format.
        let latin1_source = b"\xc3\xa9";
        assert!(std::str::from_utf8(latin1_source).is_ok());
        for format in [ParameterFormat::Text, ParameterFormat::Binary] {
            assert_eq!(
                ClientEncoding::require_utf8("LATIN1"),
                Err(ClientEncodingError),
                "LATIN1 {format:?} bind must not obtain routing proof"
            );
        }
    }
}
