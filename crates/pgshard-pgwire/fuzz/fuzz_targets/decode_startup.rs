#![no_main]

use libfuzzer_sys::fuzz_target;
use pgshard_pgwire::{Decode, StartupFrame, decode_startup};

// A random length word is never one of PostgreSQL's special request codes, so
// the cancellation key bound, the exact SSL/GSS lengths, and the buffered-GSS
// rule are unreachable unless the code is spliced in deliberately.
const SPECIAL_REQUEST_CODES: [u32; 3] = [
    (1234 << 16) | 5678, // CancelRequest
    (1234 << 16) | 5679, // SSLRequest
    (1234 << 16) | 5680, // GSSENCRequest
];

fuzz_target!(|input: &[u8]| {
    exercise(input);
    for code in SPECIAL_REQUEST_CODES {
        if input.len() >= 8 {
            let mut special = input.to_vec();
            special[4..8].copy_from_slice(&code.to_be_bytes());
            exercise(&special);
        }
    }
});

fn exercise(input: &[u8]) {
    let Ok(Decode::Complete { frame, consumed }) = decode_startup(input) else {
        return;
    };
    assert!(consumed <= input.len(), "a frame cannot outrun its buffer");
    match frame {
        StartupFrame::Startup { parameters, .. } => {
            for parameter in parameters.iter() {
                std::hint::black_box(parameter.expect("decoded startup iterator invariant"));
            }
        }
        StartupFrame::CancelRequest { backend_pid, key } => {
            std::hint::black_box(backend_pid);
            assert!(
                (1..=256).contains(&key.len()),
                "PostgreSQL 18 cancellation keys are one to 256 bytes"
            );
        }
        StartupFrame::SslRequest | StartupFrame::GssEncryptionRequest => {
            assert_eq!(consumed, 8, "negotiation requests have an exact length");
        }
    }
}
