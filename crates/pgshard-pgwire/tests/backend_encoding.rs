//! External API coverage for backend startup-control encoding.

use pgshard_pgwire::{
    BACKEND_STARTUP_MESSAGE_LENGTH, DEFAULT_LARGE_MESSAGE_LENGTH, Decode, StartupFrame,
    decode_backend, decode_protocol_negotiation, decode_startup, encode_protocol_negotiation,
};

#[test]
fn decoded_future_minor_can_drive_postgres18_negotiation_encoding() {
    let startup = [0, 0, 0, 9, 0, 3, 0, 99, 0];
    let Decode::Complete {
        frame: StartupFrame::Startup { protocol, .. },
        consumed,
    } = decode_startup(&startup).expect("protocol 3.99 startup")
    else {
        panic!("complete startup packet decoded as incomplete or special");
    };
    assert_eq!(consumed, startup.len());

    let selected = protocol
        .postgres18_selected_version()
        .expect("PostgreSQL 18 supports protocol major three");
    assert_eq!((selected.major(), selected.minor()), (3, 2));

    let mut output = [0; BACKEND_STARTUP_MESSAGE_LENGTH + 1];
    let length = encode_protocol_negotiation(selected, &[], &mut output)
        .expect("selected version is encodable");
    let Decode::Complete { frame, consumed } =
        decode_backend(&output[..length], DEFAULT_LARGE_MESSAGE_LENGTH)
            .expect("encoded backend frame")
    else {
        panic!("complete negotiation frame decoded as incomplete");
    };
    assert_eq!(consumed, length);
    let negotiation = decode_protocol_negotiation(frame).expect("encoded negotiation body");
    assert_eq!(negotiation.selected_protocol(), selected);
    assert_eq!(negotiation.unsupported_options().len(), 0);
}
