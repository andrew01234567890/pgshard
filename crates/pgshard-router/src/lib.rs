//! Fail-closed routing for already-resolved `PostgreSQL` bind parameters.
//!
//! SQL parsing and wire-protocol handling are deliberately outside this crate.
//! The resolved routing composition remains private test scaffolding until a
//! connection-owning physical-catalog reader can issue an opaque capability. A
//! caller-supplied planner observation is not authority to inspect bind bytes.

#[cfg(test)]
use pgshard_catalog::{CatalogSnapshot, DatabaseId, ShardKeyType, TableName};
#[cfg(test)]
use pgshard_pgwire::{BindParameters, ClientEncoding, FormatCode};
#[cfg(test)]
use pgshard_planner::{CatalogOnlySearchPath, ResolvedParameterRoute};
#[cfg(test)]
use pgshard_types::{CatalogEpoch, RoutingHashV1, ShardId, ShardKey};
#[cfg(test)]
use thiserror::Error;

/// `PostgreSQL` bind-parameter representation.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParameterFormat {
    /// `PostgreSQL` text format.
    Text,
    /// `PostgreSQL` binary format.
    Binary,
}

/// Immutable result attached to a planned request.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RoutePlan {
    catalog_epoch: CatalogEpoch,
    shard_id: ShardId,
    hash: u64,
}

#[cfg(test)]
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
#[cfg(test)]
fn route_bound_parameter(
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

/// Routes the selected parameter from one decoded extended-query `Bind`.
///
/// The resolved route must have been loaded from the exact prepared-statement
/// entry named by the `Bind` message. This function deliberately accepts only
/// that message's parameter collection: the future session layer remains
/// responsible for virtualizing statement and portal names, retaining the same
/// backend generation from Parse through Execute, and rejecting stale entries.
/// It must rebuild `search_path` from an authoritative read on that backend
/// immediately before this call.
///
/// Before inspecting parameter bytes, this function requires the complete
/// retained catalog snapshot to match the cluster, catalog epoch, checksum,
/// database, schema epoch, table, and shard-key type captured by the resolved
/// route. It then requires the Bind parameter count to exactly match
/// `PostgreSQL`'s authoritative `ParameterDescription`, selects the proven
/// one-based parameter without copying it, and applies the canonical routing
/// hash.
///
/// The returned plan is not permission to Execute. The session runtime must
/// retain the resolved route and snapshot lease, recheck prepared-statement and
/// backend identity, and fence the catalog/schema proof through execution.
///
/// # Errors
///
/// Fails closed for a different or stale snapshot, a parameter-count mismatch,
/// an internally inconsistent resolved route, NULL, an unsupported parameter
/// format, malformed bytes, invalid UTF8, or a text NUL byte.
#[cfg(test)]
fn route_resolved_bind(
    snapshot: &CatalogSnapshot,
    resolved: &ResolvedParameterRoute,
    _search_path: CatalogOnlySearchPath,
    client_encoding: ClientEncoding,
    parameters: BindParameters<'_>,
) -> Result<RoutePlan, BindRouteError> {
    let template = resolved.template();
    if snapshot.cluster_id() != template.cluster_id()
        || snapshot.catalog_epoch() != template.catalog_epoch()
        || snapshot.checksum() != template.snapshot_checksum()
    {
        return Err(BindRouteError::SnapshotMismatch);
    }

    let actual_count = parameters.len();
    let expected_count = resolved.parameter_count();
    if actual_count != usize::from(expected_count) {
        return Err(BindRouteError::ParameterCountMismatch {
            expected: expected_count,
            actual: actual_count,
        });
    }

    let parameter_index = usize::from(template.parameter_number().get()) - 1;
    let parameter = parameters
        .iter()
        .nth(parameter_index)
        .ok_or(BindRouteError::ResolvedParameterMissing)?;
    let format = match parameter.format() {
        FormatCode::Text => ParameterFormat::Text,
        FormatCode::Binary => ParameterFormat::Binary,
    };
    route_bound_parameter(
        snapshot,
        template.database_id(),
        template.table_name(),
        client_encoding,
        format,
        parameter.value(),
    )
    .map_err(BindRouteError::Route)
}

#[cfg(test)]
enum DecodedKey<'a> {
    Int64(i64),
    Uuid([u8; 16]),
    Text(&'a str),
    Bytes(&'a [u8]),
}

#[cfg(test)]
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
#[cfg(test)]
#[derive(Clone, Debug, Error, Eq, PartialEq)]
enum RouteError {
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

/// Failure to compose a resolved Parse-time route with one decoded Bind.
#[cfg(test)]
#[derive(Clone, Debug, Error, Eq, PartialEq)]
enum BindRouteError {
    /// The retained snapshot is not the exact snapshot used for route proof.
    #[error("resolved route does not match the retained catalog snapshot")]
    SnapshotMismatch,
    /// The Bind does not supply `PostgreSQL`'s authoritative parameter count.
    #[error("Bind supplies {actual} parameters but the prepared statement requires {expected}")]
    ParameterCountMismatch {
        /// Parameter count reported by `PostgreSQL` after Parse.
        expected: u16,
        /// Parameter count carried by the Bind message.
        actual: usize,
    },
    /// A private resolved-route invariant was inconsistent with the Bind count.
    #[error("resolved route parameter is absent from the validated Bind")]
    ResolvedParameterMissing,
    /// The selected shard-key value cannot be routed exactly.
    #[error(transparent)]
    Route(RouteError),
}

#[cfg(test)]
mod tests {
    use pgshard_catalog::{
        CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, RegisteredTable,
        RoutingHashConfig, ShardRoute,
    };
    use pgshard_pgwire::{
        BindParameters, ClientEncodingError, DEFAULT_LARGE_MESSAGE_LENGTH, Decode, FormatCode,
        FrontendPhase, decode_bind, decode_frontend,
    };
    use pgshard_planner::{
        CatalogOnlySearchPath, PhysicalShardKeyCatalogIdentity, PhysicalShardKeyObservation,
        PhysicalShardKeyProof, ResolvedParameterRoute, parse_one,
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
        snapshot_with_seed(key_type, 42)
    }

    fn snapshot_with_seed(
        key_type: ShardKeyType,
        seed: u64,
    ) -> (CatalogSnapshot, DatabaseId, TableName) {
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
                RoutingHashConfig::new(1, seed).expect("hash"),
                vec![database],
            )
            .expect("snapshot"),
            database_id,
            table_name,
        )
    }

    fn empty_search_path() -> CatalogOnlySearchPath {
        CatalogOnlySearchPath::require_empty("").expect("empty search path")
    }

    fn resolved_route(
        snapshot: &CatalogSnapshot,
        database_id: DatabaseId,
        table_name: &TableName,
        sql: &str,
        parameter_type_oids: &[u32],
    ) -> ResolvedParameterRoute {
        let database = snapshot.database(database_id).expect("database");
        let table = database.table(table_name).expect("table");
        let (type_oid, collation_oid) = match table.shard_key_type() {
            ShardKeyType::Int64 => (20, 0),
            ShardKeyType::Uuid => (2_950, 0),
            ShardKeyType::Text => (25, 950),
            ShardKeyType::Bytes => (17, 0),
        };
        let observations = database
            .routes()
            .iter()
            .map(|route| {
                PhysicalShardKeyObservation::new(
                    route.shard_id(),
                    PhysicalShardKeyCatalogIdentity::new(
                        database_id,
                        database.name(),
                        table_name.clone(),
                        table.shard_key_column(),
                        b'r',
                        b'p',
                        false,
                    ),
                    database.epochs().schema(),
                    type_oid,
                    collation_oid,
                    6,
                )
            })
            .collect::<Vec<_>>();
        let physical_schema =
            PhysicalShardKeyProof::verify(snapshot, database_id, table_name, &observations)
                .expect("physical schema proof");
        parse_one(sql)
            .expect("statement")
            .parameter_route_template(snapshot, database_id)
            .expect("route template")
            .resolve_parameter_types(empty_search_path(), &physical_schema, parameter_type_oids)
            .expect("resolved parameter route")
    }

    fn bind_frame(formats: &[FormatCode], values: &[Option<&[u8]>]) -> Vec<u8> {
        assert!(formats.len() <= 1 || formats.len() == values.len());
        let mut body = b"\0statement\0".to_vec();
        body.extend_from_slice(
            &u16::try_from(formats.len())
                .expect("format count")
                .to_be_bytes(),
        );
        for format in formats {
            let code = match format {
                FormatCode::Text => 0_u16,
                FormatCode::Binary => 1_u16,
            };
            body.extend_from_slice(&code.to_be_bytes());
        }
        body.extend_from_slice(
            &u16::try_from(values.len())
                .expect("parameter count")
                .to_be_bytes(),
        );
        for value in values {
            match value {
                Some(value) => {
                    body.extend_from_slice(
                        &i32::try_from(value.len())
                            .expect("parameter length")
                            .to_be_bytes(),
                    );
                    body.extend_from_slice(value);
                }
                None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
            }
        }
        body.extend_from_slice(&0_u16.to_be_bytes());

        let mut frame = vec![b'B'];
        frame.extend_from_slice(
            &u32::try_from(4 + body.len())
                .expect("frame length")
                .to_be_bytes(),
        );
        frame.extend_from_slice(&body);
        frame
    }

    fn bind_parameters(frame_bytes: &[u8]) -> BindParameters<'_> {
        let Decode::Complete { frame, consumed } = decode_frontend(
            frame_bytes,
            FrontendPhase::Regular,
            DEFAULT_LARGE_MESSAGE_LENGTH,
        )
        .expect("frontend frame") else {
            panic!("complete Bind frame was incomplete");
        };
        assert_eq!(consumed, frame_bytes.len());
        decode_bind(frame, utf8()).expect("Bind body").parameters()
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
    fn text_format_matches_canonical_hash() {
        let (snapshot, database_id, table) = snapshot(ShardKeyType::Text);
        let value = "tenant-α";
        let plan = route_bound_parameter(
            &snapshot,
            database_id,
            &table,
            utf8(),
            ParameterFormat::Text,
            Some(value.as_bytes()),
        )
        .expect("route text-format text key");
        assert_eq!(
            plan.hash(),
            RoutingHashV1::new(42).hash(ShardKey::Text(value))
        );
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

    #[test]
    fn resolved_bind_routes_the_selected_parameter_without_copying() {
        let (snapshot, database_id, table) = snapshot(ShardKeyType::Int64);
        let resolved = resolved_route(
            &snapshot,
            database_id,
            &table,
            "select * from public.events where tenant_id = $2",
            &[25, 20],
        );
        let key = 42_i64.to_be_bytes();
        let values: [Option<&[u8]>; 2] = [Some(b"not-the-key"), Some(&key)];
        let bytes = bind_frame(&[FormatCode::Text, FormatCode::Binary], &values);

        let actual = route_resolved_bind(
            &snapshot,
            &resolved,
            empty_search_path(),
            utf8(),
            bind_parameters(&bytes),
        )
        .expect("resolved Bind route");
        let expected = route_bound_parameter(
            &snapshot,
            database_id,
            &table,
            utf8(),
            ParameterFormat::Binary,
            Some(&key),
        )
        .expect("direct route");
        assert_eq!(actual, expected);
    }

    #[test]
    fn resolved_bind_requires_the_exact_snapshot_and_parameter_count() {
        let (snapshot, database_id, table) = snapshot(ShardKeyType::Int64);
        let resolved = resolved_route(
            &snapshot,
            database_id,
            &table,
            "select * from public.events where tenant_id = $2",
            &[25, 20],
        );
        let key = 42_i64.to_be_bytes();
        let values: [Option<&[u8]>; 2] = [Some(b"ignored"), Some(&key)];
        let exact = bind_frame(&[FormatCode::Text, FormatCode::Binary], &values);
        let (changed_snapshot, _, _) = snapshot_with_seed(ShardKeyType::Int64, 43);
        assert_eq!(
            route_resolved_bind(
                &changed_snapshot,
                &resolved,
                empty_search_path(),
                utf8(),
                bind_parameters(&exact),
            ),
            Err(BindRouteError::SnapshotMismatch)
        );

        let too_few = bind_frame(&[FormatCode::Binary], &[Some(&key)]);
        assert_eq!(
            route_resolved_bind(
                &snapshot,
                &resolved,
                empty_search_path(),
                utf8(),
                bind_parameters(&too_few),
            ),
            Err(BindRouteError::ParameterCountMismatch {
                expected: 2,
                actual: 1,
            })
        );
    }

    #[test]
    fn resolved_bind_preserves_fail_closed_value_checks() {
        let (snapshot, database_id, table) = snapshot(ShardKeyType::Int64);
        let resolved = resolved_route(
            &snapshot,
            database_id,
            &table,
            "select * from public.events where tenant_id = $1",
            &[20],
        );
        for (format, value, expected) in [
            (
                FormatCode::Binary,
                None,
                BindRouteError::Route(RouteError::NullShardKey),
            ),
            (
                FormatCode::Text,
                Some(b"42".as_slice()),
                BindRouteError::Route(RouteError::UnsupportedFormat {
                    key_type: ShardKeyType::Int64,
                    format: ParameterFormat::Text,
                }),
            ),
            (
                FormatCode::Binary,
                Some(b"short".as_slice()),
                BindRouteError::Route(RouteError::InvalidLength {
                    key_type: ShardKeyType::Int64,
                    expected: 8,
                    actual: 5,
                }),
            ),
        ] {
            let bytes = bind_frame(&[format], &[value]);
            assert_eq!(
                route_resolved_bind(
                    &snapshot,
                    &resolved,
                    empty_search_path(),
                    utf8(),
                    bind_parameters(&bytes),
                ),
                Err(expected)
            );
        }

        let secret = b"bind-value-sentinel";
        let bytes = bind_frame(&[FormatCode::Binary], &[Some(secret)]);
        let error = route_resolved_bind(
            &snapshot,
            &resolved,
            empty_search_path(),
            utf8(),
            bind_parameters(&bytes),
        )
        .expect_err("wrong-width secret must fail");
        assert_eq!(
            error,
            BindRouteError::Route(RouteError::InvalidLength {
                key_type: ShardKeyType::Int64,
                expected: 8,
                actual: secret.len(),
            })
        );
        let secret = std::str::from_utf8(secret).expect("ASCII sentinel");
        assert!(!format!("{error}").contains(secret));
        assert!(!format!("{error:?}").contains(secret));
    }
}
