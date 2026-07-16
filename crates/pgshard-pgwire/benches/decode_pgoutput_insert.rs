//! Standalone zero-copy `pgoutput` Insert microbenchmark.

use std::hint::black_box;
use std::time::Instant;

use pgshard_pgwire::{
    ClientEncoding, PgOutputConfiguration, PgOutputDecoder, PgOutputEncoding, PgOutputMessage,
    PgOutputStreaming, PgOutputTupleColumn, PgOutputVersion,
};

const ITERATIONS: u64 = 5_000_000;

fn main() {
    let mut message = vec![b'I'];
    message.extend_from_slice(&4_242_u32.to_be_bytes());
    message.push(b'N');
    message.extend_from_slice(&2_u16.to_be_bytes());
    message.push(b't');
    message.extend_from_slice(&1_i32.to_be_bytes());
    message.push(b'7');
    message.push(b'b');
    message.extend_from_slice(&4_i32.to_be_bytes());
    message.extend_from_slice(&[0, 1, 2, 3]);

    let configuration = PgOutputConfiguration::new(
        PgOutputVersion::V1,
        PgOutputStreaming::Off,
        false,
        false,
        false,
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
        let PgOutputMessage::Insert(inserted) =
            decoder.decode(black_box(&message)).expect("decode Insert")
        else {
            panic!("benchmark message decoded as another variant");
        };
        digest = digest.wrapping_add(u64::from(inserted.relation_id()));
        for column in inserted.new_tuple().columns() {
            let column = column.expect("validated benchmark tuple column");
            let length = match column {
                PgOutputTupleColumn::Null | PgOutputTupleColumn::UnchangedToast => 0,
                PgOutputTupleColumn::Text(value) => value.len(),
                PgOutputTupleColumn::Binary(value) => value.len(),
            };
            digest = digest.wrapping_add(u64::try_from(length).expect("column length fits u64"));
        }
    }

    let elapsed = started.elapsed();
    let nanos_per_message = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "decode_pgoutput_insert: {ITERATIONS} iterations in {elapsed:?} ({nanos_per_message} ns/message); digest={digest}"
    );
}
