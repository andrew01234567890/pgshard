//! Standalone zero-copy `pgoutput` Relation microbenchmark.

use std::hint::black_box;
use std::time::Instant;

use pgshard_pgwire::{
    ClientEncoding, PgOutputConfiguration, PgOutputDecoder, PgOutputEncoding, PgOutputMessage,
    PgOutputStreaming, PgOutputVersion,
};

const ITERATIONS: u64 = 5_000_000;

fn main() {
    let mut message = vec![b'R'];
    message.extend_from_slice(&4_242_u32.to_be_bytes());
    message.extend_from_slice(b"public\0items\0d");
    message.extend_from_slice(&2_u16.to_be_bytes());
    message.extend_from_slice(b"\x01id\0");
    message.extend_from_slice(&23_u32.to_be_bytes());
    message.extend_from_slice(&(-1_i32).to_be_bytes());
    message.extend_from_slice(b"\x00value\0");
    message.extend_from_slice(&25_u32.to_be_bytes());
    message.extend_from_slice(&(-1_i32).to_be_bytes());

    let configuration =
        PgOutputConfiguration::new(PgOutputVersion::V1, PgOutputStreaming::Off, false, false)
            .expect("benchmark configuration");
    let encoding = PgOutputEncoding::require_utf8(
        ClientEncoding::require_utf8("UTF8").expect("client UTF8"),
        "UTF8",
    )
    .expect("server UTF8");
    let mut decoder = PgOutputDecoder::new(configuration, encoding);

    let started = Instant::now();
    let mut digest = 0_u64;
    for _ in 0..ITERATIONS {
        let PgOutputMessage::Relation(relation) = decoder
            .decode(black_box(&message))
            .expect("decode Relation")
        else {
            panic!("benchmark message decoded as another variant");
        };
        digest = digest.wrapping_add(u64::from(relation.relation_id()));
        for column in relation.columns() {
            digest = digest
                .wrapping_add(u64::from(column.type_oid()))
                .wrapping_add(column.name().len() as u64);
        }
    }

    let elapsed = started.elapsed();
    let nanos_per_message = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "decode_pgoutput_relation: {ITERATIONS} iterations in {elapsed:?} ({nanos_per_message} ns/message); digest={digest}"
    );
}
