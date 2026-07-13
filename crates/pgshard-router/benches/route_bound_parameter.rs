//! Standalone end-to-end bound-parameter routing microbenchmark.

use std::hint::black_box;
use std::time::Instant;

use pgshard_catalog::{
    CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, DatabaseId, RegisteredTable,
    RoutingHashConfig, ShardKeyType, ShardRoute, TableName,
};
use pgshard_router::{ClientEncoding, ParameterFormat, route_bound_parameter};
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
}
