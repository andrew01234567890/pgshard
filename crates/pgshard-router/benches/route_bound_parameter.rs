//! Standalone end-to-end bound-parameter routing microbenchmark.

use std::hint::black_box;
use std::time::Instant;

use pgshard_catalog::{
    CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, DatabaseId, RegisteredTable,
    RoutingHashConfig, ShardKeyType, ShardRoute, TableName,
};
use pgshard_pgwire::{
    BindParameters, DEFAULT_LARGE_MESSAGE_LENGTH, Decode, FrontendPhase, decode_bind,
    decode_frontend,
};
use pgshard_planner::{
    CatalogOnlySearchPath, PhysicalShardKeyCatalogIdentity, PhysicalShardKeyObservation,
    PhysicalShardKeyProof, ResolvedParameterRoute, parse_one,
};
use pgshard_router::{ClientEncoding, ParameterFormat, route_bound_parameter, route_resolved_bind};
use pgshard_types::{KEYSPACE_END, KeyRange, RoutingHashV1, ShardId};
use uuid::Uuid;

const ITERATIONS: u64 = 5_000_000;

fn fixture() -> (CatalogSnapshot, DatabaseId, TableName) {
    let database_id = DatabaseId::new(Uuid::from_u128(2)).expect("database ID");
    let table_name = TableName::new("public", "events").expect("table name");
    let database = DatabaseCatalog::new(
        database_id,
        "app",
        DatabaseEpochs::new(1, 1, 1).expect("epochs"),
        vec![ShardRoute::new(
            ShardId(0),
            KeyRange::new(0, KEYSPACE_END).expect("range"),
        )],
        vec![
            RegisteredTable::new(
                table_name.clone(),
                "tenant_id",
                ShardKeyType::Int64,
                RoutingHashV1::VERSION,
            )
            .expect("table"),
        ],
    )
    .expect("database");
    let snapshot = CatalogSnapshot::new(
        ClusterId::new(Uuid::from_u128(1)).expect("cluster ID"),
        1,
        RoutingHashConfig::new(1, 42).expect("hash"),
        vec![database],
    )
    .expect("snapshot");
    (snapshot, database_id, table_name)
}

fn resolved_route(
    snapshot: &CatalogSnapshot,
    database_id: DatabaseId,
    table_name: &TableName,
) -> ResolvedParameterRoute {
    let physical_schema = PhysicalShardKeyProof::verify(
        snapshot,
        database_id,
        table_name,
        &[PhysicalShardKeyObservation::new(
            ShardId(0),
            PhysicalShardKeyCatalogIdentity::new(
                database_id,
                "app",
                table_name.clone(),
                "tenant_id",
                b'r',
                b'p',
                false,
            ),
            1,
            20,
            0,
            6,
        )],
    )
    .expect("physical schema proof");
    parse_one("SELECT * FROM public.events WHERE tenant_id = $1")
        .expect("statement")
        .parameter_route_template(snapshot, database_id)
        .expect("route template")
        .resolve_parameter_types(
            CatalogOnlySearchPath::require_empty("").expect("empty search path"),
            &physical_schema,
            &[20],
        )
        .expect("resolved parameter route")
}

fn bind_frame(value: &[u8]) -> Vec<u8> {
    let mut body = b"\0statement\0".to_vec();
    body.extend_from_slice(&1_u16.to_be_bytes());
    body.extend_from_slice(&1_u16.to_be_bytes());
    body.extend_from_slice(&1_u16.to_be_bytes());
    body.extend_from_slice(
        &i32::try_from(value.len())
            .expect("parameter length")
            .to_be_bytes(),
    );
    body.extend_from_slice(value);
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

fn bind_parameters(frame_bytes: &[u8], client_encoding: ClientEncoding) -> BindParameters<'_> {
    let Decode::Complete { frame, consumed } = decode_frontend(
        frame_bytes,
        FrontendPhase::Regular,
        DEFAULT_LARGE_MESSAGE_LENGTH,
    )
    .expect("Bind frame") else {
        panic!("complete benchmark frame was incomplete");
    };
    assert_eq!(consumed, frame_bytes.len());
    let bind = decode_bind(frame, client_encoding).expect("Bind message");
    bind.parameters()
}

fn main() {
    let (snapshot, database_id, table_name) = fixture();
    let client_encoding = ClientEncoding::require_utf8("UTF8").expect("UTF8");
    let started = Instant::now();
    let mut digest = 0_u64;

    for iteration in 0..ITERATIONS {
        let value = i64::try_from(iteration)
            .expect("iteration fits i64")
            .to_be_bytes();
        let plan = route_bound_parameter(
            black_box(&snapshot),
            database_id,
            black_box(&table_name),
            client_encoding,
            ParameterFormat::Binary,
            Some(black_box(&value)),
        )
        .expect("route");
        digest = digest.wrapping_add(plan.hash());
    }

    let elapsed = started.elapsed();
    let nanos_per_route = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "route_bound_parameter: {ITERATIONS} iterations in {elapsed:?} ({nanos_per_route} ns/route); digest={digest}"
    );

    let resolved = resolved_route(&snapshot, database_id, &table_name);
    let value = 42_i64.to_be_bytes();
    let bind_frame = bind_frame(&value);
    let parameters = bind_parameters(&bind_frame, client_encoding);
    let search_path = CatalogOnlySearchPath::require_empty("").expect("empty search path");
    let started = Instant::now();
    let mut digest = 0_u64;

    for _ in 0..ITERATIONS {
        let plan = route_resolved_bind(
            black_box(&snapshot),
            black_box(&resolved),
            search_path,
            client_encoding,
            black_box(parameters),
        )
        .expect("resolved Bind route");
        digest = digest.wrapping_add(plan.hash());
    }

    let elapsed = started.elapsed();
    let nanos_per_route = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "route_resolved_bind: {ITERATIONS} iterations in {elapsed:?} ({nanos_per_route} ns/route); digest={digest}"
    );
}
