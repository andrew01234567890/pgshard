#![no_main]

use libfuzzer_sys::fuzz_target;
use pgshard_pgwire::{Decode, StartupFrame, decode_startup};

fuzz_target!(|input: &[u8]| {
    if let Ok(Decode::Complete {
        frame: StartupFrame::Startup { parameters, .. },
        ..
    }) = decode_startup(input)
    {
        for parameter in parameters.iter() {
            std::hint::black_box(parameter.expect("decoded startup iterator invariant"));
        }
    }
});
