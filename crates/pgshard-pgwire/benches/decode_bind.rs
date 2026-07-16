//! Standalone end-to-end extended-query `Bind` decoder microbenchmark.

use std::hint::black_box;
use std::time::Instant;

use pgshard_pgwire::{
    ClientEncoding, DEFAULT_LARGE_MESSAGE_LENGTH, Decode, FrontendPhase, decode_bind,
    decode_frontend,
};

const ITERATIONS: u64 = 5_000_000;

fn push_i16(bytes: &mut Vec<u8>, value: i16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_i32(bytes: &mut Vec<u8>, value: i32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn fixture() -> Vec<u8> {
    let mut body = b"portal\0lookup\0".to_vec();
    push_i16(&mut body, 1);
    push_i16(&mut body, 1);
    push_i16(&mut body, 4);
    for value in [42_i64, 43, 44, 45] {
        push_i32(&mut body, 8);
        body.extend_from_slice(&value.to_be_bytes());
    }
    push_i16(&mut body, 1);
    push_i16(&mut body, 1);

    let length = u32::try_from(4 + body.len()).expect("benchmark frame length");
    let mut frame = Vec::with_capacity(1 + length as usize);
    frame.push(b'B');
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn main() {
    let frame = fixture();
    let client_encoding = ClientEncoding::require_utf8("UTF8").expect("UTF8");
    let started = Instant::now();
    let mut digest = 0_usize;
    for _ in 0..ITERATIONS {
        let Decode::Complete { frame, .. } = decode_frontend(
            black_box(&frame),
            FrontendPhase::Regular,
            DEFAULT_LARGE_MESSAGE_LENGTH,
        )
        .expect("frame decode") else {
            panic!("complete benchmark frame was incomplete");
        };
        let bind = decode_bind(frame, client_encoding).expect("bind decode");
        for parameter in bind.parameters().iter() {
            let parameter = parameter.expect("validated benchmark Bind parameter");
            digest = digest.wrapping_add(parameter.value().map_or(0, <[u8]>::len));
        }
    }

    let elapsed = started.elapsed();
    let nanos_per_bind = elapsed.as_nanos() / u128::from(ITERATIONS);
    println!(
        "decode_bind: {ITERATIONS} iterations in {elapsed:?} ({nanos_per_bind} ns/bind); digest={digest}"
    );
}
