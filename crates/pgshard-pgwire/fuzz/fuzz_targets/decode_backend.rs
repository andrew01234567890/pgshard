#![no_main]

use libfuzzer_sys::fuzz_target;
use pgshard_pgwire::{
    AuthenticationRequest, BACKEND_SHORT_MESSAGE_LENGTH, BACKEND_STARTUP_MESSAGE_LENGTH,
    BackendFrame, DEFAULT_LARGE_MESSAGE_LENGTH, Decode, DecodeError, MAX_LARGE_MESSAGE_LENGTH,
    ReplicationCopyData, decode_authentication_request, decode_backend, decode_backend_key_data,
    decode_parameter_description, decode_parameter_status, decode_protocol_negotiation,
    decode_ready_for_query, decode_replication_copy_data, require_empty_backend_body,
};

// Caller policies from the decoder's minimum to PostgreSQL's own large-message
// limit, so the family-versus-caller minimum is exercised on both sides of every
// family bound instead of only at the 16 MiB default.
const LIMITS: [usize; 5] = [
    4,
    BACKEND_STARTUP_MESSAGE_LENGTH,
    BACKEND_SHORT_MESSAGE_LENGTH,
    DEFAULT_LARGE_MESSAGE_LENGTH,
    MAX_LARGE_MESSAGE_LENGTH,
];

fuzz_target!(|input: &[u8]| {
    for limit in LIMITS {
        if let Ok(Decode::Complete { frame, .. }) = decode_backend(input, limit) {
            exercise_typed_decoders(frame);
        }
    }
    for rejected in [3, MAX_LARGE_MESSAGE_LENGTH + 1] {
        assert!(
            matches!(
                decode_backend(input, rejected),
                Err(DecodeError::InvalidMaximum { .. })
            ),
            "an out-of-range caller policy must be rejected before the input"
        );
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
    if let Ok(payload) = decode_replication_copy_data(frame) {
        match payload {
            ReplicationCopyData::XLogData(data) => {
                std::hint::black_box(data.data());
            }
            ReplicationCopyData::PrimaryKeepalive(keepalive) => {
                std::hint::black_box(keepalive);
            }
        }
    }
    let _ = std::hint::black_box(decode_parameter_status(frame));
    let _ = std::hint::black_box(decode_backend_key_data(frame));
    let _ = std::hint::black_box(decode_ready_for_query(frame));
    let _ = std::hint::black_box(require_empty_backend_body(frame));
}
