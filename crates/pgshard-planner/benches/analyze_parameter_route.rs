//! Standalone parse plus catalog-bound route-template microbenchmark.

use std::{hint::black_box, time::Instant};

use pgshard_catalog::{
    CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, DatabaseId, DatabaseShardId,
    RegisteredTable, RoutingHashConfig, ShardKeyType, ShardRoute, TableName,
};
use pgshard_planner::{
    CatalogOnlySearchPath, PhysicalShardKeyCatalogIdentity, PhysicalShardKeyObservation,
    PhysicalShardKeyProof, parse_one,
};
use pgshard_types::{KEYSPACE_END, KeyRange, RoutingHashV1, ShardId};
use uuid::Uuid;

const ITERATIONS: u64 = 100_000;
const SQL: &str = "SELECT * FROM public.events WHERE tenant_id = $1";

fn fixture() -> (CatalogSnapshot, DatabaseId) {
    let database_id = DatabaseId::new(Uuid::from_u128(2)).expect("database ID");
    let table = RegisteredTable::new(
        TableName::new("public", "events").expect("table name"),
        "tenant_id",
        ShardKeyType::Int64,
        RoutingHashV1::VERSION,
    )
    .expect("registered table");
    let database = DatabaseCatalog::new(
        database_id,
        "app",
        DatabaseEpochs::new(1, 1, 1).expect("database epochs"),
        vec![ShardRoute::new(
            DatabaseShardId::new(Uuid::from_u128(100)).expect("database shard ID"),
            1,
            ShardId(0),
            KeyRange::new(0, KEYSPACE_END).expect("complete range"),
        )],
        vec![table],
    )
    .expect("database catalog");
    let snapshot = CatalogSnapshot::new(
        ClusterId::new(Uuid::from_u128(1)).expect("cluster ID"),
        1,
        RoutingHashConfig::new(1, 42).expect("routing hash"),
        vec![database],
    )
    .expect("catalog snapshot");
    (snapshot, database_id)
}

fn main() {
    let (snapshot, database_id) = fixture();
    let search_path = CatalogOnlySearchPath::require_empty("").expect("empty search path");
    let physical_schema = PhysicalShardKeyProof::verify(
        &snapshot,
        database_id,
        &TableName::new("public", "events").expect("table name"),
        &[PhysicalShardKeyObservation::new(
            ShardId(0),
            PhysicalShardKeyCatalogIdentity::new(
                database_id,
                "app",
                TableName::new("public", "events").expect("table name"),
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
    let started = Instant::now();
    let mut digest = 0_u64;

    for _ in 0..ITERATIONS {
        let statement = parse_one(black_box(SQL)).expect("benchmark statement");
        let resolved = statement
            .parameter_route_template(black_box(&snapshot), database_id)
            .expect("benchmark route template")
            .resolve_parameter_types(search_path, &physical_schema, black_box(&[20]))
            .expect("benchmark parameter resolution");
        digest = digest.wrapping_add(u64::from(resolved.template().parameter_number().get()));
    }

    let elapsed = started.elapsed();
    let nanos_per_template = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "analyze_parameter_route: {ITERATIONS} iterations in {elapsed:?} \
         ({nanos_per_template} ns/template); digest={digest}"
    );
}
