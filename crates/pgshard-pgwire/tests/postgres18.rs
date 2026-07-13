//! Live `PostgreSQL` 18 startup-control, metadata, and query decoder contract.
//!
//! The fixture must allow TCP trust authentication because this deliberately
//! small raw protocol client does not implement password or SASL exchange.
//! Run with:
//! `PGSHARD_PGWIRE_TEST_ADDRESS=127.0.0.1:5432 cargo test -p pgshard-pgwire --test postgres18 -- --ignored`

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use pgshard_pgwire::{
    AuthenticationRequest, BackendTag, ClientEncoding, DEFAULT_LARGE_MESSAGE_LENGTH, Decode,
    ExtendedQueryObject, FrontendPhase, Postgres18StartupNegotiation, TransactionStatus,
    decode_authentication_request, decode_backend, decode_backend_key_data, decode_close,
    decode_describe, decode_frontend, decode_parameter_description, decode_parameter_status,
    decode_protocol_negotiation, decode_ready_for_query, decode_startup,
    require_empty_backend_body,
};

const POSTGRES_PROTOCOL_3_0: u32 = 3 << 16;
const POSTGRES_PROTOCOL_3_2: u32 = (3 << 16) | 2;
const POSTGRES_PROTOCOL_3_99: u32 = (3 << 16) | 0x0063;

fn startup(
    protocol: u32,
    user: &str,
    database: &str,
    extra_parameters: &[(&str, &str)],
) -> Vec<u8> {
    assert!(
        !user.as_bytes().contains(&0),
        "test user contains zero byte"
    );
    assert!(
        !database.as_bytes().contains(&0),
        "test database contains zero byte"
    );
    let mut body = protocol.to_be_bytes().to_vec();
    for (name, value) in [
        ("user", user),
        ("database", database),
        ("client_encoding", "UTF8"),
    ]
    .into_iter()
    .chain(extra_parameters.iter().copied())
    {
        assert!(
            !name.as_bytes().contains(&0),
            "test parameter name contains zero byte"
        );
        assert!(
            !value.as_bytes().contains(&0),
            "test parameter value contains zero byte"
        );
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);

    let length = u32::try_from(4 + body.len()).expect("startup length");
    let mut packet = length.to_be_bytes().to_vec();
    packet.extend_from_slice(&body);
    packet
}

fn frontend(tag: u8, body: &[u8]) -> Vec<u8> {
    let length = u32::try_from(4 + body.len()).expect("frontend frame length");
    let mut packet = Vec::with_capacity(1 + length as usize);
    packet.push(tag);
    packet.extend_from_slice(&length.to_be_bytes());
    packet.extend_from_slice(body);
    packet
}

fn read_backend(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0_u8; 5];
    stream.read_exact(&mut header).expect("read backend header");
    let message_length = usize::try_from(u32::from_be_bytes([
        header[1], header[2], header[3], header[4],
    ]))
    .expect("backend length fits usize");
    assert!(message_length >= 4, "server sent impossible frame length");
    assert!(
        message_length <= DEFAULT_LARGE_MESSAGE_LENGTH,
        "server frame exceeds test bound"
    );
    let mut packet = Vec::with_capacity(message_length + 1);
    packet.extend_from_slice(&header);
    packet.resize(message_length + 1, 0);
    stream
        .read_exact(&mut packet[5..])
        .expect("read backend body");
    packet
}

fn decoded_frame(bytes: &[u8]) -> pgshard_pgwire::BackendFrame<'_> {
    let Decode::Complete { frame, consumed } =
        decode_backend(bytes, DEFAULT_LARGE_MESSAGE_LENGTH).expect("decode backend frame")
    else {
        panic!("complete server frame decoded as incomplete");
    };
    assert_eq!(consumed, bytes.len());
    frame
}

fn decoded_frontend(bytes: &[u8]) -> pgshard_pgwire::FrontendFrame<'_> {
    let Decode::Complete { frame, consumed } =
        decode_frontend(bytes, FrontendPhase::Regular, DEFAULT_LARGE_MESSAGE_LENGTH)
            .expect("decode frontend frame")
    else {
        panic!("complete frontend frame decoded as incomplete");
    };
    assert_eq!(consumed, bytes.len());
    frame
}

fn startup_negotiation(bytes: &[u8]) -> Postgres18StartupNegotiation<'_> {
    let Decode::Complete { frame, consumed } =
        decode_startup(bytes).expect("decode the exact outbound startup packet")
    else {
        panic!("complete outbound startup packet decoded as incomplete");
    };
    assert_eq!(consumed, bytes.len());
    Postgres18StartupNegotiation::begin(frame).expect("PostgreSQL 18 startup protocol")
}

fn finish_startup(
    stream: &mut TcpStream,
    negotiation: Postgres18StartupNegotiation<'_>,
) -> ClientEncoding {
    let mut negotiation = Some(negotiation);
    let mut protocol = None;
    let mut authenticated = false;
    let mut backend_key_data = false;
    let mut postgres18 = false;
    let mut client_encoding = None;
    loop {
        let bytes = read_backend(stream);
        let frame = decoded_frame(&bytes);
        match frame.tag() {
            BackendTag::AuthenticationRequest => {
                if protocol.is_none() {
                    protocol = Some(
                        negotiation
                            .take()
                            .expect("authentication followed an earlier authentication response")
                            .finish()
                            .expect("PostgreSQL 18 completed required protocol negotiation"),
                    );
                }
                let request = decode_authentication_request(frame)
                    .expect("valid PostgreSQL 18 authentication body");
                assert!(
                    matches!(request, AuthenticationRequest::Ok),
                    "fixture must use trust authentication; received {request:?}"
                );
                authenticated = true;
            }
            BackendTag::NegotiateProtocolVersion => {
                let response = decode_protocol_negotiation(frame)
                    .expect("valid PostgreSQL 18 protocol negotiation body");
                let state = negotiation
                    .take()
                    .expect("protocol negotiation arrived after authentication");
                negotiation = Some(
                    state
                        .accept(&response)
                        .expect("response matches the exact outbound startup packet"),
                );
            }
            BackendTag::ParameterStatus => {
                let status =
                    decode_parameter_status(frame).expect("valid UTF-8 ParameterStatus body");
                if status.name() == "server_version" {
                    postgres18 = status.value() == "18" || status.value().starts_with("18.");
                } else if status.name() == "client_encoding" {
                    client_encoding = Some(
                        ClientEncoding::require_utf8(status.value())
                            .expect("PostgreSQL honored client_encoding=UTF8"),
                    );
                }
            }
            BackendTag::BackendKeyData => {
                let data = decode_backend_key_data(frame).expect("valid BackendKeyData body");
                assert_ne!(data.backend_pid(), 0, "PostgreSQL sent a zero backend PID");
                protocol
                    .expect("BackendKeyData arrived before authentication")
                    .validate_backend_key_data(data)
                    .expect("key length matches the effective PostgreSQL 18 protocol");
                backend_key_data = true;
            }
            BackendTag::ErrorResponse => panic!("PostgreSQL rejected startup"),
            BackendTag::ReadyForQuery => {
                assert_eq!(decode_ready_for_query(frame), Ok(TransactionStatus::Idle));
                break;
            }
            _ => {}
        }
    }
    assert!(authenticated, "server omitted AuthenticationOk");
    assert!(backend_key_data, "server omitted BackendKeyData");
    assert!(protocol.is_some(), "server omitted authentication response");
    assert!(postgres18, "test requires PostgreSQL major version 18");
    client_encoding.expect("server omitted client_encoding ParameterStatus")
}

fn connect(address: &str) -> TcpStream {
    let stream = TcpStream::connect(address).expect("connect to PostgreSQL 18");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("set write timeout");
    stream
}

fn describe_statement(stream: &mut TcpStream, utf8: ClientEncoding) {
    let mut parse_body = b"pgshard_types\0SELECT $1::bigint, $2::uuid, $3::bytea\0".to_vec();
    parse_body.extend_from_slice(&0_u16.to_be_bytes());
    let parse = frontend(b'P', &parse_body);
    let describe = frontend(b'D', b"Spgshard_types\0");
    let sync = frontend(b'S', b"");
    let decoded_describe = decode_describe(decoded_frontend(&describe), utf8).expect("Describe");
    assert_eq!(decoded_describe.object(), ExtendedQueryObject::Statement);
    assert_eq!(decoded_describe.name(), "pgshard_types");
    stream.write_all(&parse).expect("send Parse");
    stream.write_all(&describe).expect("send Describe");
    stream.write_all(&sync).expect("send Sync");

    let mut parse_complete = false;
    let mut row_description = false;
    let mut parameter_description_count = 0;
    loop {
        let bytes = read_backend(stream);
        let frame = decoded_frame(&bytes);
        match frame.tag() {
            BackendTag::ParseComplete => {
                require_empty_backend_body(frame).expect("empty ParseComplete");
                parse_complete = true;
            }
            BackendTag::ParameterDescription => {
                let description = decode_parameter_description(frame)
                    .expect("decode live ParameterDescription body");
                assert_eq!(
                    description.parameter_types().collect::<Vec<_>>(),
                    vec![20, 2950, 17]
                );
                parameter_description_count += 1;
            }
            BackendTag::RowDescription => row_description = true,
            BackendTag::ErrorResponse => panic!("PostgreSQL rejected Parse or Describe"),
            BackendTag::ReadyForQuery => {
                assert_eq!(decode_ready_for_query(frame), Ok(TransactionStatus::Idle));
                break;
            }
            _ => {}
        }
    }
    assert!(parse_complete, "server omitted ParseComplete");
    assert!(row_description, "server omitted RowDescription");
    assert_eq!(parameter_description_count, 1);
}

fn close_statement(stream: &mut TcpStream, utf8: ClientEncoding) {
    let close = frontend(b'C', b"Spgshard_types\0");
    let decoded_close = decode_close(decoded_frontend(&close), utf8).expect("Close");
    assert_eq!(decoded_close.object(), ExtendedQueryObject::Statement);
    assert_eq!(decoded_close.name(), "pgshard_types");
    stream.write_all(&close).expect("send Close");
    stream
        .write_all(&frontend(b'S', b""))
        .expect("send Sync after Close");
    let mut close_complete = false;
    loop {
        let bytes = read_backend(stream);
        let frame = decoded_frame(&bytes);
        match frame.tag() {
            BackendTag::CloseComplete => {
                require_empty_backend_body(frame).expect("empty CloseComplete");
                close_complete = true;
            }
            BackendTag::ErrorResponse => panic!("PostgreSQL rejected Close"),
            BackendTag::ReadyForQuery => {
                assert_eq!(decode_ready_for_query(frame), Ok(TransactionStatus::Idle));
                break;
            }
            _ => {}
        }
    }
    assert!(close_complete, "server omitted CloseComplete");
}

#[test]
#[ignore = "requires a trust-authenticated PostgreSQL 18 TCP fixture"]
fn postgres18_startup_and_extended_query_controls_decode_from_real_wire_bytes() {
    let address = std::env::var("PGSHARD_PGWIRE_TEST_ADDRESS")
        .expect("PGSHARD_PGWIRE_TEST_ADDRESS is required");
    let user = std::env::var("PGSHARD_PGWIRE_TEST_USER").unwrap_or_else(|_| "postgres".into());
    let database =
        std::env::var("PGSHARD_PGWIRE_TEST_DATABASE").unwrap_or_else(|_| "postgres".into());
    let mut legacy = connect(&address);
    let legacy_startup = startup(POSTGRES_PROTOCOL_3_0, &user, &database, &[]);
    let legacy_negotiation = startup_negotiation(&legacy_startup);
    legacy
        .write_all(&legacy_startup)
        .expect("send protocol 3.0 startup packet");
    finish_startup(&mut legacy, legacy_negotiation);
    legacy
        .write_all(&frontend(b'X', b""))
        .expect("terminate protocol 3.0 connection");

    let mut stream = connect(&address);
    let native_startup = startup(POSTGRES_PROTOCOL_3_2, &user, &database, &[]);
    let native_negotiation = startup_negotiation(&native_startup);
    stream
        .write_all(&native_startup)
        .expect("send protocol 3.2 startup packet");
    let utf8 = finish_startup(&mut stream, native_negotiation);
    describe_statement(&mut stream, utf8);
    close_statement(&mut stream, utf8);

    stream
        .write_all(&frontend(b'X', b""))
        .expect("send Terminate");

    let mut negotiated = connect(&address);
    let newer_startup = startup(
        POSTGRES_PROTOCOL_3_99,
        &user,
        &database,
        &[("_pq_.pgshard_test", "1")],
    );
    let newer_negotiation = startup_negotiation(&newer_startup);
    negotiated
        .write_all(&newer_startup)
        .expect("send protocol 3.99 startup packet with a reserved option");
    finish_startup(&mut negotiated, newer_negotiation);
    negotiated
        .write_all(&frontend(b'X', b""))
        .expect("terminate negotiated protocol connection");
}
