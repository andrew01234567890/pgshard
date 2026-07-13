//! Standalone zero-copy `pgoutput` logical Message microbenchmark.

use std::hint::black_box;
use std::time::Instant;

use pgshard_pgwire::{
    ClientEncoding, PgOutputConfiguration, PgOutputDecoder, PgOutputEncoding, PgOutputMessage,
    PgOutputStreaming, PgOutputVersion,
};

const ITERATIONS: u64 = 5_000_000;

fn main() {
    let prefix = b"pgshard";
    let content = [0_u8, 1, 2, 3, 4, 5, 6, 0xff];
    let mut message = vec![b'M', 1];
    message.extend_from_slice(&4_242_u64.to_be_bytes());
    message.extend_from_slice(prefix);
    message.push(0);
    message.extend_from_slice(
        &i32::try_from(content.len())
            .expect("content length")
            .to_be_bytes(),
    );
    message.extend_from_slice(&content);

    let configuration = PgOutputConfiguration::new(
        PgOutputVersion::V1,
        PgOutputStreaming::Off,
        false,
        false,
        true,
    )
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
        let PgOutputMessage::LogicalMessage(logical) = decoder
            .decode(black_box(&message))
            .expect("decode logical Message")
        else {
            panic!("benchmark message decoded as another variant");
        };
        digest = digest
            .wrapping_add(logical.lsn())
            .wrapping_add(u64::try_from(logical.prefix().len()).expect("prefix length fits u64"))
            .wrapping_add(u64::try_from(logical.content().len()).expect("content length fits u64"));
    }

    let elapsed = started.elapsed();
    let nanos_per_message = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "decode_pgoutput_message: {ITERATIONS} iterations in {elapsed:?} ({nanos_per_message} ns/message); digest={digest}"
    );
}
