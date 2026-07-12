//! Standalone allocation-free routing-hash microbenchmark.

use std::hint::black_box;
use std::time::Instant;

use pgshard_types::{RoutingHashV1, ShardKey};

const ITERATIONS: u64 = 5_000_000;

fn main() {
    let routing_hash = RoutingHashV1::new(0x7067_7368_6172_6431);
    let key = "a representative tenant routing key";
    let started = Instant::now();
    let mut digest = 0_u64;

    for _ in 0..ITERATIONS {
        digest ^= routing_hash.hash(ShardKey::Text(black_box(key)));
    }

    let elapsed = started.elapsed();
    let nanos_per_route = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "routing_hash_v1: {ITERATIONS} iterations in {elapsed:?} ({nanos_per_route} ns/route); digest={digest}"
    );
}
