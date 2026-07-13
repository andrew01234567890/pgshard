//! Standalone bounded candidate-parser microbenchmark.

use std::{hint::black_box, time::Instant};

use pgshard_planner::{StatementKind, parse_one};

const ITERATIONS: u64 = 100_000;
const PADDED_ITERATIONS: u64 = 1_000;
const SQL: &str = "SELECT value FROM events WHERE tenant_id = $1 AND created_at >= $2";

fn main() {
    benchmark("route_candidate", SQL, ITERATIONS);

    let whitespace_padded = format!("{}select 1", " ".repeat(4_000));
    benchmark("whitespace_padding", &whitespace_padded, PADDED_ITERATIONS);

    let semicolon_padded = format!("{}select 1{}", ";".repeat(2_000), ";".repeat(2_000));
    benchmark("semicolon_padding", &semicolon_padded, PADDED_ITERATIONS);
}

fn benchmark(name: &str, sql: &str, iterations: u64) {
    let started = Instant::now();
    let mut digest = 0_u64;

    for _ in 0..iterations {
        let statement = parse_one(black_box(sql)).expect("benchmark statement");
        digest = digest.wrapping_add(match statement.kind() {
            StatementKind::Query => 1,
            StatementKind::Insert => 2,
            StatementKind::Update => 3,
            StatementKind::Delete => 4,
            StatementKind::Merge => 5,
            _ => 6,
        });
    }

    let elapsed = started.elapsed();
    let nanos_per_statement = elapsed.as_nanos() / u128::from(iterations);
    println!(
        "{name}: {iterations} iterations in {elapsed:?} \
         ({nanos_per_statement} ns/statement); digest={digest}"
    );
}
