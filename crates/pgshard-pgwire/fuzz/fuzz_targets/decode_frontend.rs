#![no_main]

use libfuzzer_sys::fuzz_target;
use pgshard_pgwire::{
    ClientEncoding, DEFAULT_LARGE_MESSAGE_LENGTH, Decode, FrontendFrame, FrontendPhase,
    ScramMechanisms, decode_bind, decode_close, decode_describe, decode_execute, decode_frontend,
    decode_parse, decode_query, decode_sasl_initial_response, decode_sasl_response,
    encode_authentication_sasl, require_empty_body,
};

fuzz_target!(|input: &[u8]| {
    let mut advertisement = [0_u8; 64];
    let scram = encode_authentication_sasl(ScramMechanisms::Sha256, &mut advertisement)
        .expect("fixed output holds a SCRAM advertisement")
        .frontend_phase();
    for phase in [
        FrontendPhase::Authentication,
        FrontendPhase::ScramAuthentication(scram),
        FrontendPhase::Regular,
        FrontendPhase::CopyIn,
        FrontendPhase::ReplicationStreaming,
    ] {
        if let Ok(Decode::Complete { frame, .. }) =
            decode_frontend(input, phase, DEFAULT_LARGE_MESSAGE_LENGTH)
        {
            exercise_typed_decoders(frame);
        }
    }
});

fn exercise_typed_decoders(frame: FrontendFrame<'_>) {
    let utf8 = ClientEncoding::require_utf8("UTF8").expect("fixed canonical encoding");
    if let Ok(parse) = decode_parse(frame, utf8) {
        for parameter_type in parse.parameter_types() {
            std::hint::black_box(parameter_type.expect("decoded Parse iterator invariant"));
        }
    }
    if let Ok(bind) = decode_bind(frame, utf8) {
        for parameter in bind.parameters().iter() {
            std::hint::black_box(parameter.expect("decoded Bind iterator invariant"));
        }
        for format in bind.result_formats() {
            std::hint::black_box(format.expect("decoded result-format iterator invariant"));
        }
    }
    let _ = std::hint::black_box(decode_query(frame, utf8));
    let _ = std::hint::black_box(decode_describe(frame, utf8));
    let _ = std::hint::black_box(decode_close(frame, utf8));
    let _ = std::hint::black_box(decode_execute(frame, utf8));
    let _ = std::hint::black_box(decode_sasl_initial_response(frame));
    let _ = std::hint::black_box(decode_sasl_response(frame));
    let _ = std::hint::black_box(require_empty_body(frame));
}
