//! External API coverage for bounded frontend and backend SASL wire controls.

use pgshard_pgwire::{
    AuthenticationRequest, DEFAULT_LARGE_MESSAGE_LENGTH, Decode, FrontendPhase, MessageError,
    ScramMechanisms, decode_authentication_request, decode_backend, decode_frontend,
    decode_sasl_initial_response, decode_sasl_response, encode_authentication_sasl,
    encode_authentication_sasl_continue, encode_authentication_sasl_final,
};

const INITIAL_DATA: &[u8] = b"n,,n=user,r=client-nonce";

fn initial_packet() -> Vec<u8> {
    let mut frontend = vec![b'p'];
    let message_length = 4 + b"SCRAM-SHA-256\0".len() + 4 + INITIAL_DATA.len();
    frontend.extend_from_slice(
        &u32::try_from(message_length)
            .expect("bounded frontend message length")
            .to_be_bytes(),
    );
    frontend.extend_from_slice(b"SCRAM-SHA-256\0");
    frontend.extend_from_slice(
        &i32::try_from(INITIAL_DATA.len())
            .expect("bounded initial response length")
            .to_be_bytes(),
    );
    frontend.extend_from_slice(INITIAL_DATA);
    frontend
}

#[test]
fn public_sasl_primitives_compose_without_internal_access() {
    let mut backend = [0; 64];
    let advertisement = encode_authentication_sasl(ScramMechanisms::Sha256, &mut backend)
        .expect("bounded SCRAM advertisement");
    let frontend = initial_packet();

    let Decode::Complete { frame, consumed } = decode_frontend(
        &frontend,
        FrontendPhase::ScramAuthentication(advertisement.frontend_phase()),
        DEFAULT_LARGE_MESSAGE_LENGTH,
    )
    .expect("bounded SASL frontend frame") else {
        panic!("complete SCRAM frame decoded as incomplete");
    };
    assert_eq!(consumed, frontend.len());
    let initial = decode_sasl_initial_response(frame).expect("typed SASL initial response");
    assert_eq!(initial.mechanism(), b"SCRAM-SHA-256");
    assert_eq!(initial.initial_response(), Some(INITIAL_DATA));

    let length = advertisement.frame_length();
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
    assert_eq!(
        mechanisms
            .collect::<Result<Vec<_>, _>>()
            .expect("validated mechanisms"),
        [b"SCRAM-SHA-256"]
    );

    let phase = advertisement.frontend_phase();
    let continue_data = b"r=nonce,s=c2FsdA==,i=4096";
    let length = encode_authentication_sasl_continue(phase, continue_data, &mut backend)
        .expect("phase-bound SCRAM continuation");
    let Decode::Complete { frame, .. } =
        decode_backend(&backend[..length], DEFAULT_LARGE_MESSAGE_LENGTH)
            .expect("encoded SCRAM continuation")
    else {
        panic!("complete SCRAM continuation decoded as incomplete");
    };
    let AuthenticationRequest::SaslContinue { data } =
        decode_authentication_request(frame).expect("typed SCRAM continuation")
    else {
        panic!("SCRAM continuation decoded as another authentication request");
    };
    assert_eq!(data, continue_data);

    let final_data = b"v=c2lnbmF0dXJl";
    let length = encode_authentication_sasl_final(phase, final_data, &mut backend)
        .expect("phase-bound SCRAM completion");
    let Decode::Complete { frame, .. } =
        decode_backend(&backend[..length], DEFAULT_LARGE_MESSAGE_LENGTH)
            .expect("encoded SCRAM completion")
    else {
        panic!("complete SCRAM completion decoded as incomplete");
    };
    let AuthenticationRequest::SaslFinal { data } =
        decode_authentication_request(frame).expect("typed SCRAM completion")
    else {
        panic!("SCRAM completion decoded as another authentication request");
    };
    assert_eq!(data, final_data);
}

#[test]
fn generic_authentication_frames_do_not_authorize_sasl_decoders() {
    let frontend = initial_packet();
    let Decode::Complete { frame, .. } = decode_frontend(
        &frontend,
        FrontendPhase::Authentication,
        DEFAULT_LARGE_MESSAGE_LENGTH,
    )
    .expect("generic authentication frame") else {
        panic!("complete generic authentication frame decoded as incomplete");
    };
    assert_eq!(
        decode_sasl_initial_response(frame),
        Err(MessageError::ScramPhaseRequired)
    );
    assert_eq!(
        decode_sasl_response(frame),
        Err(MessageError::ScramPhaseRequired)
    );

    let mut oversized = vec![b'p'];
    oversized.extend_from_slice(&1_025_u32.to_be_bytes());
    oversized.extend_from_slice(&vec![b'x'; 1_021]);
    let Decode::Complete { frame, .. } = decode_frontend(
        &oversized,
        FrontendPhase::Authentication,
        DEFAULT_LARGE_MESSAGE_LENGTH,
    )
    .expect("generic authentication permits the broader protocol bound") else {
        panic!("complete oversized generic authentication frame decoded as incomplete");
    };
    assert_eq!(
        decode_sasl_response(frame),
        Err(MessageError::ScramPhaseRequired)
    );
}
