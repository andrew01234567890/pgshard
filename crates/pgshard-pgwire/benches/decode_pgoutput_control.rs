//! Standalone zero-copy `pgoutput` transaction-control microbenchmark.

use std::hint::black_box;
use std::time::Instant;

use pgshard_pgwire::{
    ClientEncoding, PgOutputConfiguration, PgOutputControlMessage, PgOutputStreaming,
    PgOutputVersion, decode_pgoutput_control,
};

const ITERATIONS: u64 = 5_000_000;

fn main() {
    let mut message = vec![b'B'];
    message.extend_from_slice(&11_u64.to_be_bytes());
    message.extend_from_slice(&22_i64.to_be_bytes());
    message.extend_from_slice(&33_u32.to_be_bytes());
    let configuration =
        PgOutputConfiguration::new(PgOutputVersion::V4, PgOutputStreaming::Parallel, true)
            .expect("benchmark configuration");
    let client_encoding = ClientEncoding::require_utf8("UTF8").expect("UTF8");

    let started = Instant::now();
    let mut digest = 0_u64;
    for _ in 0..ITERATIONS {
        let PgOutputControlMessage::Begin(begin) =
            decode_pgoutput_control(black_box(&message), configuration, client_encoding)
                .expect("decode")
        else {
            panic!("benchmark message decoded as another control");
        };
        digest = digest.wrapping_add(begin.final_lsn() ^ u64::from(begin.xid()));
    }

    let elapsed = started.elapsed();
    let nanos_per_message = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "decode_pgoutput_control: {ITERATIONS} iterations in {elapsed:?} ({nanos_per_message} ns/message); digest={digest}"
    );
}
