//! External API coverage for bounded client-facing diagnostic encoding.

use pgshard_pgwire::{
    BACKEND_STARTUP_ERROR_MESSAGE_LENGTH, ErrorResponseSeverity, encode_error_response,
};

#[test]
fn public_error_response_encoder_emits_required_postgres18_fields() {
    let mut output = [0; 128];
    let length = encode_error_response(
        ErrorResponseSeverity::Fatal,
        *b"08006",
        "authentication timed out",
        BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
        &mut output,
    )
    .expect("bounded ErrorResponse");

    let body = b"SFATAL\0VFATAL\0C08006\0Mauthentication timed out\0\0";
    let mut expected = vec![b'E'];
    expected.extend_from_slice(
        &u32::try_from(4 + body.len())
            .expect("bounded ErrorResponse length")
            .to_be_bytes(),
    );
    expected.extend_from_slice(body);
    assert_eq!(&output[..length], expected);
}
