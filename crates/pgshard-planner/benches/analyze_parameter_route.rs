//! Standalone parse plus catalog-bound route-template microbenchmark.

use std::{hint::black_box, time::Instant};

use pgshard_catalog::{
    CatalogSnapshot, ClusterId, DatabaseCatalog, DatabaseEpochs, DatabaseId, RegisteredTable,
    RoutingHashConfig, ShardKeyType, ShardRoute, TableName,
};
use pgshard_planner::parse_one;
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
    let started = Instant::now();
    let mut digest = 0_u64;

    for _ in 0..ITERATIONS {
        let statement = parse_one(black_box(SQL)).expect("benchmark statement");
        let template = statement
            .parameter_route_template(black_box(&snapshot), database_id)
            .expect("benchmark route template");
        digest = digest.wrapping_add(u64::from(template.parameter_number().get()));
    }

    let elapsed = started.elapsed();
    let nanos_per_template = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "analyze_parameter_route: {ITERATIONS} iterations in {elapsed:?} \
         ({nanos_per_template} ns/template); digest={digest}"
    );
}
