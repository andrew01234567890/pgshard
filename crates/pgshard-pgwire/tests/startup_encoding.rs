//! External API coverage for frontend startup-request encoding.

use pgshard_pgwire::{
    DEFAULT_LARGE_MESSAGE_LENGTH, Decode, MAX_STARTUP_FRAME_LENGTH, Postgres18StartupNegotiation,
    ProtocolVersion, StartupFrame, decode_backend, decode_backend_key_data, decode_startup,
    encode_gss_encryption_request, encode_postgres18_cancel_request, encode_ssl_request,
    encode_startup,
};

fn decoded_startup(input: &[u8]) -> StartupFrame<'_> {
    let Decode::Complete { frame, consumed } = decode_startup(input).expect("startup frame") else {
        panic!("complete startup frame was incomplete");
    };
    assert_eq!(consumed, input.len());
    frame
}

#[test]
fn public_startup_encoders_compose_with_decoders_and_protocol_proof() {
    assert_eq!(
        decoded_startup(&encode_ssl_request()),
        StartupFrame::SslRequest
    );
    assert_eq!(
        decoded_startup(&encode_gss_encryption_request()),
        StartupFrame::GssEncryptionRequest
    );

    let parameters = [
        (b"user".as_slice(), b"postgres".as_slice()),
        (b"client_encoding".as_slice(), b"UTF8".as_slice()),
    ];
    let mut startup = [0; MAX_STARTUP_FRAME_LENGTH];
    let startup_length = encode_startup(ProtocolVersion::new(3, 2), &parameters, &mut startup)
        .expect("bounded regular startup");
    let regular = decoded_startup(&startup[..startup_length]);
    let StartupFrame::Startup {
        protocol,
        parameters: decoded_parameters,
    } = regular
    else {
        panic!("encoded a special startup request");
    };
    assert_eq!(protocol, ProtocolVersion::new(3, 2));
    assert_eq!(
        decoded_parameters
            .iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("validated startup parameters"),
        parameters
    );

    let protocol = Postgres18StartupNegotiation::begin(regular)
        .expect("protocol-three startup")
        .finish()
        .expect("native PostgreSQL 18 protocol");
    let key = [0xa5; 32];
    let mut backend_key = vec![b'K'];
    backend_key.extend_from_slice(&(40_u32).to_be_bytes());
    backend_key.extend_from_slice(&7_u32.to_be_bytes());
    backend_key.extend_from_slice(&key);
    let Decode::Complete { frame, consumed } =
        decode_backend(&backend_key, DEFAULT_LARGE_MESSAGE_LENGTH).expect("backend key frame")
    else {
        panic!("complete backend key frame was incomplete");
    };
    assert_eq!(consumed, backend_key.len());
    let data = decode_backend_key_data(frame).expect("backend key body");

    let mut cancel = [0; 44];
    let cancel_length = encode_postgres18_cancel_request(protocol, data, &mut cancel)
        .expect("protocol-specific cancel request");
    assert_eq!(
        decoded_startup(&cancel[..cancel_length]),
        StartupFrame::CancelRequest {
            backend_pid: 7,
            key: &key,
        }
    );
}
