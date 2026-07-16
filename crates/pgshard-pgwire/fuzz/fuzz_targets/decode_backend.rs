#![no_main]

use libfuzzer_sys::fuzz_target;
use pgshard_pgwire::{
    AuthenticationRequest, BackendFrame, DEFAULT_LARGE_MESSAGE_LENGTH, Decode,
    decode_authentication_request, decode_backend, decode_backend_key_data,
    decode_parameter_description, decode_parameter_status, decode_protocol_negotiation,
    decode_ready_for_query, require_empty_backend_body,
};

fuzz_target!(|input: &[u8]| {
    if let Ok(Decode::Complete { frame, .. }) = decode_backend(input, DEFAULT_LARGE_MESSAGE_LENGTH)
    {
        exercise_typed_decoders(frame);
    }
});

fn exercise_typed_decoders(frame: BackendFrame<'_>) {
    if let Ok(AuthenticationRequest::Sasl { mechanisms }) = decode_authentication_request(frame) {
        for mechanism in mechanisms {
            std::hint::black_box(mechanism.expect("decoded SASL iterator invariant"));
        }
    }
    if let Ok(options) = decode_protocol_negotiation(frame) {
        for option in options.unsupported_options() {
            std::hint::black_box(option.expect("decoded protocol-option iterator invariant"));
        }
    }
    if let Ok(parameters) = decode_parameter_description(frame) {
        for parameter_type in parameters.parameter_types() {
            std::hint::black_box(
                parameter_type.expect("decoded parameter-type iterator invariant"),
            );
        }
    }
    let _ = std::hint::black_box(decode_parameter_status(frame));
    let _ = std::hint::black_box(decode_backend_key_data(frame));
    let _ = std::hint::black_box(decode_ready_for_query(frame));
    let _ = std::hint::black_box(require_empty_backend_body(frame));
}
