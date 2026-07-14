//! External API coverage for bounded frontend and backend SASL wire controls.

use pgshard_pgwire::{
    AuthenticationRequest, DEFAULT_LARGE_MESSAGE_LENGTH, Decode, FrontendPhase, ScramMechanisms,
    decode_authentication_request, decode_backend, decode_frontend, decode_sasl_initial_response,
    encode_authentication_sasl,
};

#[test]
fn public_sasl_primitives_compose_without_internal_access() {
    let initial_data = b"n,,n=user,r=client-nonce";
    let mut frontend = vec![b'p'];
    let message_length = 4 + b"SCRAM-SHA-256\0".len() + 4 + initial_data.len();
    frontend.extend_from_slice(
        &u32::try_from(message_length)
            .expect("bounded frontend message length")
            .to_be_bytes(),
    );
    frontend.extend_from_slice(b"SCRAM-SHA-256\0");
    frontend.extend_from_slice(
        &i32::try_from(initial_data.len())
            .expect("bounded initial response length")
            .to_be_bytes(),
    );
    frontend.extend_from_slice(initial_data);

    let Decode::Complete { frame, consumed } = decode_frontend(
        &frontend,
        FrontendPhase::ScramAuthentication,
        DEFAULT_LARGE_MESSAGE_LENGTH,
    )
    .expect("bounded SASL frontend frame") else {
        panic!("complete SCRAM frame decoded as incomplete");
    };
    assert_eq!(consumed, frontend.len());
    let initial = decode_sasl_initial_response(frame).expect("typed SASL initial response");
    assert_eq!(initial.mechanism(), b"SCRAM-SHA-256");
    assert_eq!(initial.initial_response(), Some(initial_data.as_slice()));

    let mut backend = [0; 64];
    let length = encode_authentication_sasl(ScramMechanisms::Sha256, &mut backend)
        .expect("bounded SCRAM advertisement");
    let Decode::Complete { frame, consumed } =
        decode_backend(&backend[..length], DEFAULT_LARGE_MESSAGE_LENGTH)
            .expect("encoded backend frame")
    else {
        panic!("complete SCRAM advertisement decoded as incomplete");
    };
    assert_eq!(consumed, length);
    let AuthenticationRequest::Sasl { mechanisms } =
        decode_authentication_request(frame).expect("typed SCRAM advertisement")
    else {
        panic!("SCRAM advertisement decoded as another authentication request");
    };
    assert_eq!(mechanisms.collect::<Vec<_>>(), [b"SCRAM-SHA-256"]);
}
