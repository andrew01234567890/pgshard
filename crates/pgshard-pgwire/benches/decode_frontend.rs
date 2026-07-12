//! Standalone zero-copy frontend-frame decoder microbenchmark.

use std::hint::black_box;
use std::time::Instant;

use pgshard_pgwire::{DEFAULT_LARGE_MESSAGE_LENGTH, Decode, decode_frontend};

const ITERATIONS: u64 = 5_000_000;

fn main() {
    let query = b"select value from events where tenant_id = $1\0";
    let length = u32::try_from(4 + query.len()).expect("benchmark frame length");
    let mut frame = Vec::with_capacity(1 + length as usize);
    frame.push(b'Q');
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(query);

    let started = Instant::now();
    let mut digest = 0_usize;
    for _ in 0..ITERATIONS {
        let Decode::Complete {
            frame: decoded,
            consumed,
        } = decode_frontend(black_box(&frame), DEFAULT_LARGE_MESSAGE_LENGTH).expect("decode")
        else {
            panic!("complete benchmark frame was incomplete");
        };
        digest = digest.wrapping_add(consumed ^ decoded.body().len());
    }

    let elapsed = started.elapsed();
    let nanos_per_frame = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "decode_frontend: {ITERATIONS} iterations in {elapsed:?} ({nanos_per_frame} ns/frame); digest={digest}"
    );
}
