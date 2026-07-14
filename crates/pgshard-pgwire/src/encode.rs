//! Allocation-free encoding of `PostgreSQL` 18 backend controls and minimal errors.

use thiserror::Error;

use crate::{
    BACKEND_SHORT_MESSAGE_LENGTH, BACKEND_STARTUP_MESSAGE_LENGTH, MAX_BACKEND_KEY_DATA_LENGTH,
    MAX_CANCEL_KEY_LENGTH, MAX_LARGE_MESSAGE_LENGTH, MIN_BACKEND_CANCEL_KEY_LENGTH,
    ProtocolVersion, TransactionStatus,
};

/// Exact wire length of an `AuthenticationOk` frame.
pub const AUTHENTICATION_OK_FRAME_LENGTH: usize = 9;
/// Exact wire length of a `ReadyForQuery` frame.
pub const READY_FOR_QUERY_FRAME_LENGTH: usize = 6;

const SCRAM_SHA_256: &[u8] = b"SCRAM-SHA-256";
const SCRAM_SHA_256_PLUS: &[u8] = b"SCRAM-SHA-256-PLUS";
const SCRAM_SHA_256_MECHANISMS: [&[u8]; 1] = [SCRAM_SHA_256];
const SCRAM_SHA_256_PLUS_MECHANISMS: [&[u8]; 2] = [SCRAM_SHA_256_PLUS, SCRAM_SHA_256];

/// Severity supported by a minimal client-facing `ErrorResponse`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorResponseSeverity {
    /// Recoverable statement or transaction error.
    Error,
    /// Session-ending startup or connection error.
    Fatal,
}

impl ErrorResponseSeverity {
    const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Error => b"ERROR",
            Self::Fatal => b"FATAL",
        }
    }
}

const PROTOCOL_NEGOTIATION_FIXED_MESSAGE_LENGTH: usize = 12;
const MIN_PROTOCOL_OPTION_MESSAGE_LENGTH: usize = b"_pq_.".len() + 1;
const MAX_PROTOCOL_NEGOTIATION_OPTIONS: usize = (BACKEND_STARTUP_MESSAGE_LENGTH
    - PROTOCOL_NEGOTIATION_FIXED_MESSAGE_LENGTH)
    / MIN_PROTOCOL_OPTION_MESSAGE_LENGTH;

/// `PostgreSQL` 18 SCRAM mechanisms to advertise to a frontend client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScramMechanisms {
    /// Advertise only `SCRAM-SHA-256`.
    Sha256,
    /// Advertise `SCRAM-SHA-256-PLUS` first, then `SCRAM-SHA-256`.
    Sha256Plus,
}

impl ScramMechanisms {
    const fn ordered(self) -> &'static [&'static [u8]] {
        match self {
            Self::Sha256 => &SCRAM_SHA_256_MECHANISMS,
            Self::Sha256Plus => &SCRAM_SHA_256_PLUS_MECHANISMS,
        }
    }
}

/// Encodes the fixed `AuthenticationOk` frame.
#[must_use]
pub const fn encode_authentication_ok() -> [u8; AUTHENTICATION_OK_FRAME_LENGTH] {
    [b'R', 0, 0, 0, 8, 0, 0, 0, 0]
}

/// Encodes a `PostgreSQL` 18 `AuthenticationSASL` SCRAM advertisement.
///
/// The channel-binding form always lists `SCRAM-SHA-256-PLUS` first and the
/// base mechanism second, matching `PostgreSQL` 18. The transport and
/// authentication state machine must offer the channel-binding form only when
/// they possess the matching TLS channel-binding context.
///
/// The output is not modified on error.
///
/// # Errors
///
/// Returns an error when caller-owned storage cannot hold the complete frame.
pub fn encode_authentication_sasl(
    mechanisms: ScramMechanisms,
    output: &mut [u8],
) -> Result<usize, BackendEncodeError> {
    let mechanisms = mechanisms.ordered();
    let body_length = mechanisms.iter().try_fold(
        5_usize,
        |body_length, mechanism| -> Result<usize, BackendEncodeError> {
            body_length
                .checked_add(mechanism.len())
                .and_then(|length| length.checked_add(1))
                .ok_or(BackendEncodeError::LengthOverflow)
        },
    )?;
    let (message_length, frame_length) =
        checked_frame_length(body_length, BACKEND_STARTUP_MESSAGE_LENGTH)?;
    require_output(output, frame_length)?;

    write_header(output, b'R', message_length);
    output[5..9].copy_from_slice(&10_u32.to_be_bytes());
    let mut offset = 9;
    for mechanism in mechanisms {
        output[offset..offset + mechanism.len()].copy_from_slice(mechanism);
        offset += mechanism.len();
        output[offset] = 0;
        offset += 1;
    }
    output[offset] = 0;
    offset += 1;
    debug_assert_eq!(offset, frame_length);
    Ok(frame_length)
}

/// Encodes a minimal `PostgreSQL` 18 `ErrorResponse` into caller-owned storage.
///
/// The canonical field order is localized severity (`S`), nonlocalized
/// severity (`V`), five-byte SQLSTATE (`C`), primary UTF-8 message (`M`), and
/// the final zero byte. pgshard does not localize severity names. Optional
/// diagnostic fields remain a future extension. `maximum_message_length`
/// includes the four-byte length word. Pre-authentication callers must use
/// [`crate::BACKEND_STARTUP_ERROR_MESSAGE_LENGTH`]; authenticated session
/// policy may choose a larger bound no greater than
/// [`MAX_LARGE_MESSAGE_LENGTH`].
///
/// The output is not modified on error.
///
/// # Errors
///
/// Rejects an invalid caller limit, an empty message, an embedded zero byte, a
/// SQLSTATE byte outside uppercase ASCII letters and digits, a complete
/// message above the caller limit, arithmetic overflow, or caller-owned storage
/// smaller than the complete frame.
pub fn encode_error_response(
    severity: ErrorResponseSeverity,
    sqlstate: [u8; 5],
    message: &str,
    maximum_message_length: usize,
    output: &mut [u8],
) -> Result<usize, BackendEncodeError> {
    if maximum_message_length > MAX_LARGE_MESSAGE_LENGTH {
        return Err(BackendEncodeError::MessageLimitTooLarge {
            actual: maximum_message_length,
            maximum: MAX_LARGE_MESSAGE_LENGTH,
        });
    }
    if message.is_empty() {
        return Err(BackendEncodeError::EmptyDiagnosticMessage);
    }

    let severity = severity.as_bytes();
    let fields = [
        (b'S', severity),
        (b'V', severity),
        (b'C', sqlstate.as_slice()),
        (b'M', message.as_bytes()),
    ];
    let body_length = fields.iter().try_fold(
        1_usize,
        |body_length, (_, field)| -> Result<usize, BackendEncodeError> {
            body_length
                .checked_add(1)
                .and_then(|length| length.checked_add(field.len()))
                .and_then(|length| length.checked_add(1))
                .ok_or(BackendEncodeError::LengthOverflow)
        },
    )?;
    let (message_length, frame_length) = checked_frame_length(body_length, maximum_message_length)?;

    for (index, byte) in sqlstate.iter().copied().enumerate() {
        if !(byte.is_ascii_uppercase() || byte.is_ascii_digit()) {
            return Err(BackendEncodeError::InvalidSqlStateByte(index));
        }
    }
    if message.as_bytes().contains(&0) {
        return Err(BackendEncodeError::EmbeddedNull("diagnostic message"));
    }
    require_output(output, frame_length)?;

    write_header(output, b'E', message_length);
    let mut offset = 5;
    for (tag, field) in fields {
        offset = write_cstring_field(output, offset, tag, field);
    }
    output[offset] = 0;
    offset += 1;
    debug_assert_eq!(offset, frame_length);
    Ok(frame_length)
}

/// Encodes opaque `AuthenticationSASLContinue` bytes.
///
/// The output is not modified on error. The exchange owner must validate the
/// SCRAM state and generated payload before calling this wire primitive.
///
/// # Errors
///
/// Rejects a complete frame above libpq's startup-message bound, arithmetic
/// overflow, or caller-owned storage smaller than the complete frame.
pub fn encode_authentication_sasl_continue(
    data: &[u8],
    output: &mut [u8],
) -> Result<usize, BackendEncodeError> {
    encode_authentication_sasl_data(11, data, output)
}

/// Encodes opaque `AuthenticationSASLFinal` bytes.
///
/// The output is not modified on error. The exchange owner must validate the
/// SCRAM state and generated payload before calling this wire primitive.
///
/// # Errors
///
/// Rejects a complete frame above libpq's startup-message bound, arithmetic
/// overflow, or caller-owned storage smaller than the complete frame.
pub fn encode_authentication_sasl_final(
    data: &[u8],
    output: &mut [u8],
) -> Result<usize, BackendEncodeError> {
    encode_authentication_sasl_data(12, data, output)
}

/// Encodes a fixed `ReadyForQuery` frame for the exact transaction state.
#[must_use]
pub const fn encode_ready_for_query(
    status: TransactionStatus,
) -> [u8; READY_FOR_QUERY_FRAME_LENGTH] {
    let status = match status {
        TransactionStatus::Idle => b'I',
        TransactionStatus::InTransaction => b'T',
        TransactionStatus::FailedTransaction => b'E',
    };
    [b'Z', 0, 0, 0, 5, status]
}

/// Encodes a `BackendKeyData` frame into caller-owned storage.
///
/// The output is not modified on error. The returned length covers the tag,
/// length word, process identifier, and opaque cancellation key.
///
/// # Errors
///
/// Rejects cancellation keys outside `PostgreSQL` 18's four-to-256-byte
/// boundary or an output slice smaller than the complete frame.
pub fn encode_backend_key_data(
    backend_pid: u32,
    cancellation_key: &[u8],
    output: &mut [u8],
) -> Result<usize, BackendEncodeError> {
    if !(MIN_BACKEND_CANCEL_KEY_LENGTH..=MAX_CANCEL_KEY_LENGTH).contains(&cancellation_key.len()) {
        return Err(BackendEncodeError::InvalidCancellationKeyLength(
            cancellation_key.len(),
        ));
    }
    let body_length = 4_usize
        .checked_add(cancellation_key.len())
        .ok_or(BackendEncodeError::LengthOverflow)?;
    let (message_length, frame_length) =
        checked_frame_length(body_length, MAX_BACKEND_KEY_DATA_LENGTH)?;
    require_output(output, frame_length)?;

    write_header(output, b'K', message_length);
    output[5..9].copy_from_slice(&backend_pid.to_be_bytes());
    output[9..frame_length].copy_from_slice(cancellation_key);
    Ok(frame_length)
}

/// Encodes one UTF-8 `ParameterStatus` frame into caller-owned storage.
///
/// The output is not modified on error. Empty values are permitted; embedded
/// zero bytes are not because both fields are protocol C strings.
///
/// # Errors
///
/// Rejects embedded zero bytes, a frame above libpq's short-message bound, or
/// an output slice smaller than the complete frame.
pub fn encode_parameter_status(
    name: &str,
    value: &str,
    output: &mut [u8],
) -> Result<usize, BackendEncodeError> {
    let body_length = name
        .len()
        .checked_add(1)
        .and_then(|length| length.checked_add(value.len()))
        .and_then(|length| length.checked_add(1))
        .ok_or(BackendEncodeError::LengthOverflow)?;
    let (message_length, frame_length) =
        checked_frame_length(body_length, BACKEND_SHORT_MESSAGE_LENGTH)?;
    if name.as_bytes().contains(&0) {
        return Err(BackendEncodeError::EmbeddedNull("parameter name"));
    }
    if value.as_bytes().contains(&0) {
        return Err(BackendEncodeError::EmbeddedNull("parameter value"));
    }
    require_output(output, frame_length)?;

    write_header(output, b'S', message_length);
    let mut offset = 5;
    output[offset..offset + name.len()].copy_from_slice(name.as_bytes());
    offset += name.len();
    output[offset] = 0;
    offset += 1;
    output[offset..offset + value.len()].copy_from_slice(value.as_bytes());
    offset += value.len();
    output[offset] = 0;
    Ok(frame_length)
}

/// Encodes a `NegotiateProtocolVersion` frame from borrowed option names.
///
/// The borrowed slice permits a validation pass before any bytes are written,
/// without allocating or trusting a replayable iterator. The output is not
/// modified on error. Option names remain arbitrary protocol bytes, but must
/// use the reserved `_pq_.` prefix and contain no zero byte.
///
/// # Errors
///
/// Rejects a protocol version `PostgreSQL` 18 could not select, invalid option
/// names, an option set above the startup-message bound, arithmetic overflow,
/// or an output slice smaller than the complete frame.
pub fn encode_protocol_negotiation(
    selected_protocol: ProtocolVersion,
    unsupported_options: &[&[u8]],
    output: &mut [u8],
) -> Result<usize, BackendEncodeError> {
    if selected_protocol.postgres18_selected_version() != Some(selected_protocol) {
        return Err(BackendEncodeError::InvalidProtocolVersion(
            selected_protocol,
        ));
    }

    if unsupported_options.len() > MAX_PROTOCOL_NEGOTIATION_OPTIONS {
        return Err(BackendEncodeError::TooManyProtocolOptions {
            actual: unsupported_options.len(),
            maximum: MAX_PROTOCOL_NEGOTIATION_OPTIONS,
        });
    }

    let body_length = unsupported_options.iter().try_fold(
        8_usize,
        |body_length, option| -> Result<usize, BackendEncodeError> {
            body_length
                .checked_add(option.len())
                .and_then(|length| length.checked_add(1))
                .ok_or(BackendEncodeError::LengthOverflow)
        },
    )?;
    let (message_length, frame_length) =
        checked_frame_length(body_length, BACKEND_STARTUP_MESSAGE_LENGTH)?;

    for (index, option) in unsupported_options.iter().copied().enumerate() {
        if !option.starts_with(b"_pq_.") {
            return Err(BackendEncodeError::InvalidProtocolOptionName(index));
        }
        if option.contains(&0) {
            return Err(BackendEncodeError::ProtocolOptionContainsNull(index));
        }
    }
    let option_count = i32::try_from(unsupported_options.len()).map_err(|_| {
        BackendEncodeError::TooManyProtocolOptions {
            actual: unsupported_options.len(),
            maximum: MAX_PROTOCOL_NEGOTIATION_OPTIONS,
        }
    })?;
    require_output(output, frame_length)?;

    write_header(output, b'v', message_length);
    let protocol =
        u32::from(selected_protocol.major()) << 16 | u32::from(selected_protocol.minor());
    output[5..9].copy_from_slice(&protocol.to_be_bytes());
    output[9..13].copy_from_slice(&option_count.to_be_bytes());
    let mut offset = 13;
    for option in unsupported_options.iter().copied() {
        output[offset..offset + option.len()].copy_from_slice(option);
        offset += option.len();
        output[offset] = 0;
        offset += 1;
    }
    debug_assert_eq!(offset, frame_length);
    Ok(frame_length)
}

fn encode_authentication_sasl_data(
    code: u32,
    data: &[u8],
    output: &mut [u8],
) -> Result<usize, BackendEncodeError> {
    let body_length = 4_usize
        .checked_add(data.len())
        .ok_or(BackendEncodeError::LengthOverflow)?;
    let (message_length, frame_length) =
        checked_frame_length(body_length, BACKEND_STARTUP_MESSAGE_LENGTH)?;
    require_output(output, frame_length)?;

    write_header(output, b'R', message_length);
    output[5..9].copy_from_slice(&code.to_be_bytes());
    output[9..frame_length].copy_from_slice(data);
    Ok(frame_length)
}

fn checked_frame_length(
    body_length: usize,
    maximum_message_length: usize,
) -> Result<(u32, usize), BackendEncodeError> {
    let message_length = 4_usize
        .checked_add(body_length)
        .ok_or(BackendEncodeError::LengthOverflow)?;
    if message_length > maximum_message_length {
        return Err(BackendEncodeError::MessageTooLarge {
            actual: message_length,
            maximum: maximum_message_length,
        });
    }
    let frame_length = message_length
        .checked_add(1)
        .ok_or(BackendEncodeError::LengthOverflow)?;
    let message_length =
        u32::try_from(message_length).map_err(|_| BackendEncodeError::LengthOverflow)?;
    Ok((message_length, frame_length))
}

fn require_output(output: &[u8], required: usize) -> Result<(), BackendEncodeError> {
    if output.len() < required {
        Err(BackendEncodeError::OutputTooSmall {
            actual: output.len(),
            required,
        })
    } else {
        Ok(())
    }
}

fn write_header(output: &mut [u8], tag: u8, message_length: u32) {
    output[0] = tag;
    output[1..5].copy_from_slice(&message_length.to_be_bytes());
}

fn write_cstring_field(output: &mut [u8], offset: usize, tag: u8, value: &[u8]) -> usize {
    output[offset] = tag;
    let value_start = offset + 1;
    let value_end = value_start + value.len();
    output[value_start..value_end].copy_from_slice(value);
    output[value_end] = 0;
    value_end + 1
}

/// Backend startup/control encoding failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum BackendEncodeError {
    /// A caller-selected message limit exceeds the pooler's hard ceiling.
    #[error("backend message limit {actual} exceeds maximum {maximum}")]
    MessageLimitTooLarge {
        /// Rejected caller limit.
        actual: usize,
        /// Pooler-wide maximum message length.
        maximum: usize,
    },
    /// A required diagnostic primary message is empty.
    #[error("diagnostic primary message is empty")]
    EmptyDiagnosticMessage,
    /// One SQLSTATE byte is outside the protocol's uppercase alphanumeric set.
    #[error("invalid SQLSTATE byte at index {0}")]
    InvalidSqlStateByte(usize),
    /// A cancellation key is outside `PostgreSQL` 18's generic bounds.
    #[error("invalid PostgreSQL 18 cancellation key length {0}")]
    InvalidCancellationKeyLength(usize),
    /// A selected version is outside `PostgreSQL` 18's protocol range.
    #[error("PostgreSQL 18 cannot select protocol version {0:?}")]
    InvalidProtocolVersion(ProtocolVersion),
    /// A parameter C string contains an embedded terminator.
    #[error("{0} contains an embedded zero byte")]
    EmbeddedNull(&'static str),
    /// An unsupported option is empty or outside the reserved namespace.
    #[error("protocol negotiation option {0} does not begin with _pq_.")]
    InvalidProtocolOptionName(usize),
    /// An unsupported option contains its own protocol terminator.
    #[error("protocol negotiation option {0} contains an embedded zero byte")]
    ProtocolOptionContainsNull(usize),
    /// The option count exceeds what any bounded negotiation frame can hold.
    #[error("protocol negotiation has {actual} options; maximum is {maximum}")]
    TooManyProtocolOptions {
        /// Number of borrowed options supplied by the caller.
        actual: usize,
        /// Maximum number of minimum-length options the frame can hold.
        maximum: usize,
    },
    /// The complete message exceeds its protocol-family bound.
    #[error("backend message length {actual} exceeds maximum {maximum}")]
    MessageTooLarge {
        /// Length word that would have been encoded.
        actual: usize,
        /// Maximum length word for this message family.
        maximum: usize,
    },
    /// Caller-owned storage cannot hold the complete frame.
    #[error("backend output has {actual} bytes; {required} required")]
    OutputTooSmall {
        /// Available output length.
        actual: usize,
        /// Required complete frame length.
        required: usize,
    },
    /// Frame sizing overflowed the platform or protocol length type.
    #[error("backend frame length overflow")]
    LengthOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AuthenticationRequest, BACKEND_STARTUP_ERROR_MESSAGE_LENGTH, Decode,
        decode_authentication_request, decode_backend, decode_backend_key_data,
        decode_parameter_status, decode_protocol_negotiation, decode_ready_for_query,
    };

    fn protocol(major: u16, minor: u16) -> ProtocolVersion {
        ProtocolVersion { major, minor }
    }

    fn complete(input: &[u8]) -> crate::BackendFrame<'_> {
        let Decode::Complete { frame, consumed } =
            decode_backend(input, crate::DEFAULT_LARGE_MESSAGE_LENGTH).expect("encoded frame")
        else {
            panic!("encoded frame was incomplete");
        };
        assert_eq!(consumed, input.len());
        frame
    }

    #[test]
    fn fixed_startup_controls_round_trip() {
        let authentication = encode_authentication_ok();
        assert!(matches!(
            decode_authentication_request(complete(&authentication)),
            Ok(AuthenticationRequest::Ok)
        ));

        for status in [
            TransactionStatus::Idle,
            TransactionStatus::InTransaction,
            TransactionStatus::FailedTransaction,
        ] {
            let encoded = encode_ready_for_query(status);
            assert_eq!(decode_ready_for_query(complete(&encoded)), Ok(status));
        }
    }

    #[test]
    fn sasl_authentication_frames_round_trip() {
        for (advertisement, expected) in [
            (ScramMechanisms::Sha256, vec![SCRAM_SHA_256]),
            (
                ScramMechanisms::Sha256Plus,
                vec![SCRAM_SHA_256_PLUS, SCRAM_SHA_256],
            ),
        ] {
            let mut output = [0; 64];
            let length = encode_authentication_sasl(advertisement, &mut output)
                .expect("fixed SCRAM advertisement");
            let request = decode_authentication_request(complete(&output[..length]))
                .expect("encoded AuthenticationSASL");
            let AuthenticationRequest::Sasl { mechanisms } = request else {
                panic!("encoded another authentication request");
            };
            assert_eq!(mechanisms.collect::<Vec<_>>(), expected);
        }

        let mut output = [0; 64];
        let continue_data = b"r=nonce,s=c2FsdA==,i=4096";
        let length = encode_authentication_sasl_continue(continue_data, &mut output)
            .expect("bounded AuthenticationSASLContinue");
        let request = decode_authentication_request(complete(&output[..length]))
            .expect("encoded AuthenticationSASLContinue");
        let AuthenticationRequest::SaslContinue { data } = request else {
            panic!("encoded another authentication request");
        };
        assert_eq!(data, continue_data);

        let final_data = b"v=c2lnbmF0dXJl";
        let length = encode_authentication_sasl_final(final_data, &mut output)
            .expect("bounded AuthenticationSASLFinal");
        let request = decode_authentication_request(complete(&output[..length]))
            .expect("encoded AuthenticationSASLFinal");
        let AuthenticationRequest::SaslFinal { data } = request else {
            panic!("encoded another authentication request");
        };
        assert_eq!(data, final_data);
    }

    #[test]
    fn error_responses_use_postgres18_required_fields_and_order() {
        for (severity, severity_bytes) in [
            (ErrorResponseSeverity::Error, b"ERROR".as_slice()),
            (ErrorResponseSeverity::Fatal, b"FATAL".as_slice()),
        ] {
            let body = [
                [b"S".as_slice(), severity_bytes, b"\0"].concat(),
                [b"V".as_slice(), severity_bytes, b"\0"].concat(),
                b"C28P01\0".to_vec(),
                b"Mpassword authentication failed\0".to_vec(),
                b"\0".to_vec(),
            ]
            .concat();
            let mut expected = vec![b'E'];
            expected.extend_from_slice(
                &u32::try_from(4 + body.len())
                    .expect("bounded diagnostic length")
                    .to_be_bytes(),
            );
            expected.extend_from_slice(&body);

            let mut output = [0; 128];
            let length = encode_error_response(
                severity,
                *b"28P01",
                "password authentication failed",
                BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
                &mut output,
            )
            .expect("bounded ErrorResponse");
            assert_eq!(&output[..length], expected);
        }
    }

    #[test]
    fn backend_key_data_round_trips_exact_bounds() {
        for key_length in [MIN_BACKEND_CANCEL_KEY_LENGTH, 32, MAX_CANCEL_KEY_LENGTH] {
            let key = vec![0xa5; key_length];
            let mut output = vec![0; 1 + MAX_BACKEND_KEY_DATA_LENGTH];
            let length = encode_backend_key_data(0x0102_0304, &key, &mut output)
                .expect("bounded cancellation key");
            let decoded = decode_backend_key_data(complete(&output[..length]))
                .expect("encoded BackendKeyData");
            assert_eq!(decoded.backend_pid(), 0x0102_0304);
            assert_eq!(decoded.cancellation_key(), key);
        }
    }

    #[test]
    fn parameter_status_round_trips_utf8_and_empty_values() {
        for (name, value) in [("client_encoding", "UTF8"), ("application_name", "")] {
            let mut output = vec![0; BACKEND_SHORT_MESSAGE_LENGTH + 1];
            let length =
                encode_parameter_status(name, value, &mut output).expect("bounded ParameterStatus");
            let decoded = decode_parameter_status(complete(&output[..length]))
                .expect("encoded ParameterStatus");
            assert_eq!(decoded.name(), name);
            assert_eq!(decoded.value(), value);
        }
    }

    #[test]
    fn protocol_negotiation_round_trips_order_and_duplicates() {
        let options = [
            b"_pq_.first".as_slice(),
            b"_pq_.second".as_slice(),
            b"_pq_.first".as_slice(),
        ];
        let mut output = vec![0; BACKEND_STARTUP_MESSAGE_LENGTH + 1];
        let length = encode_protocol_negotiation(protocol(3, 2), &options, &mut output)
            .expect("bounded protocol negotiation");
        let decoded = decode_protocol_negotiation(complete(&output[..length]))
            .expect("encoded protocol negotiation");
        assert_eq!(decoded.selected_protocol(), protocol(3, 2));
        assert_eq!(decoded.unsupported_options().collect::<Vec<_>>(), options);

        let length = encode_protocol_negotiation(protocol(3, 0), &[], &mut output)
            .expect("empty option list");
        let decoded = decode_protocol_negotiation(complete(&output[..length]))
            .expect("empty protocol negotiation");
        assert_eq!(decoded.unsupported_options().len(), 0);

        let length = encode_protocol_negotiation(protocol(3, 1), &[b"_pq_.reserved"], &mut output)
            .expect("PostgreSQL 18 preserves requested protocol 3.1");
        let decoded = decode_protocol_negotiation(complete(&output[..length]))
            .expect("protocol 3.1 negotiation");
        assert_eq!(decoded.selected_protocol(), protocol(3, 1));
    }

    fn assert_error_does_not_modify(
        action: impl FnOnce(&mut [u8]) -> Result<usize, BackendEncodeError>,
    ) -> BackendEncodeError {
        let mut output = [0x5a; 32];
        let original = output;
        let error = action(&mut output).expect_err("invalid input");
        assert_eq!(output, original);
        error
    }

    #[test]
    fn diagnostic_encoder_rejects_invalid_input_without_modification() {
        assert_eq!(
            assert_error_does_not_modify(|output| encode_error_response(
                ErrorResponseSeverity::Fatal,
                *b"28P01",
                "failure",
                MAX_LARGE_MESSAGE_LENGTH + 1,
                output,
            )),
            BackendEncodeError::MessageLimitTooLarge {
                actual: MAX_LARGE_MESSAGE_LENGTH + 1,
                maximum: MAX_LARGE_MESSAGE_LENGTH,
            }
        );
        assert_eq!(
            assert_error_does_not_modify(|output| encode_error_response(
                ErrorResponseSeverity::Fatal,
                *b"28P01",
                "",
                BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
                output,
            )),
            BackendEncodeError::EmptyDiagnosticMessage
        );
        for index in 0..5 {
            let mut sqlstate = *b"28P01";
            sqlstate[index] = b'_';
            assert_eq!(
                assert_error_does_not_modify(|output| encode_error_response(
                    ErrorResponseSeverity::Fatal,
                    sqlstate,
                    "failure",
                    BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
                    output,
                )),
                BackendEncodeError::InvalidSqlStateByte(index)
            );
        }
        assert_eq!(
            assert_error_does_not_modify(|output| encode_error_response(
                ErrorResponseSeverity::Fatal,
                *b"28P01",
                "hidden\0suffix",
                BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
                output,
            )),
            BackendEncodeError::EmbeddedNull("diagnostic message")
        );
        assert!(matches!(
            assert_error_does_not_modify(|output| encode_error_response(
                ErrorResponseSeverity::Fatal,
                *b"28P01",
                "failure",
                BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
                &mut output[..8],
            )),
            BackendEncodeError::OutputTooSmall { .. }
        ));
    }

    #[test]
    fn variable_encoders_fail_before_modifying_output() {
        assert!(matches!(
            assert_error_does_not_modify(|output| encode_authentication_sasl(
                ScramMechanisms::Sha256Plus,
                &mut output[..8],
            )),
            BackendEncodeError::OutputTooSmall { .. }
        ));
        assert!(matches!(
            assert_error_does_not_modify(|output| encode_authentication_sasl_continue(
                b"challenge",
                &mut output[..8],
            )),
            BackendEncodeError::OutputTooSmall { .. }
        ));
        assert_eq!(
            assert_error_does_not_modify(|output| encode_backend_key_data(1, b"bad", output)),
            BackendEncodeError::InvalidCancellationKeyLength(3)
        );
        assert!(matches!(
            assert_error_does_not_modify(|output| encode_backend_key_data(
                1,
                b"key!",
                &mut output[..12],
            )),
            BackendEncodeError::OutputTooSmall { required: 13, .. }
        ));
        assert_eq!(
            assert_error_does_not_modify(|output| encode_parameter_status(
                "bad\0name",
                "value",
                output,
            )),
            BackendEncodeError::EmbeddedNull("parameter name")
        );
        assert_eq!(
            assert_error_does_not_modify(|output| encode_parameter_status(
                "name",
                "bad\0value",
                output,
            )),
            BackendEncodeError::EmbeddedNull("parameter value")
        );
        assert!(matches!(
            assert_error_does_not_modify(|output| encode_protocol_negotiation(
                protocol(4, 0),
                &[],
                output,
            )),
            BackendEncodeError::InvalidProtocolVersion(version) if version == protocol(4, 0)
        ));
        assert!(matches!(
            assert_error_does_not_modify(|output| encode_protocol_negotiation(
                protocol(3, 3),
                &[],
                output,
            )),
            BackendEncodeError::InvalidProtocolVersion(version) if version == protocol(3, 3)
        ));
        assert_eq!(
            assert_error_does_not_modify(|output| encode_protocol_negotiation(
                protocol(3, 2),
                &[b"public"],
                output,
            )),
            BackendEncodeError::InvalidProtocolOptionName(0)
        );
        assert_eq!(
            assert_error_does_not_modify(|output| encode_protocol_negotiation(
                protocol(3, 2),
                &[b"_pq_.bad\0name"],
                output,
            )),
            BackendEncodeError::ProtocolOptionContainsNull(0)
        );
        assert!(matches!(
            assert_error_does_not_modify(|output| encode_parameter_status(
                "n",
                "v",
                &mut output[..8],
            )),
            BackendEncodeError::OutputTooSmall { required: 9, .. }
        ));
        assert!(matches!(
            assert_error_does_not_modify(|output| encode_protocol_negotiation(
                protocol(3, 2),
                &[],
                &mut output[..12],
            )),
            BackendEncodeError::OutputTooSmall { required: 13, .. }
        ));
    }

    #[test]
    fn variable_encoder_maximum_frames_are_exact() {
        let maximum_sasl_data_length = BACKEND_STARTUP_MESSAGE_LENGTH - 8;
        let sasl_data = vec![b'x'; maximum_sasl_data_length];
        let mut sasl_output = vec![0; BACKEND_STARTUP_MESSAGE_LENGTH + 1];
        let length = encode_authentication_sasl_continue(&sasl_data, &mut sasl_output)
            .expect("maximum AuthenticationSASLContinue");
        assert_eq!(length, BACKEND_STARTUP_MESSAGE_LENGTH + 1);
        let AuthenticationRequest::SaslContinue { data } =
            decode_authentication_request(complete(&sasl_output))
                .expect("maximum AuthenticationSASLContinue frame")
        else {
            panic!("encoded another authentication request");
        };
        assert_eq!(data.len(), maximum_sasl_data_length);

        let maximum_diagnostic_message_length = BACKEND_STARTUP_ERROR_MESSAGE_LENGTH - 28;
        let message = "x".repeat(maximum_diagnostic_message_length);
        let mut diagnostic_output = vec![0; BACKEND_STARTUP_ERROR_MESSAGE_LENGTH + 1];
        let length = encode_error_response(
            ErrorResponseSeverity::Fatal,
            *b"08006",
            &message,
            BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
            &mut diagnostic_output,
        )
        .expect("maximum ErrorResponse");
        assert_eq!(length, BACKEND_STARTUP_ERROR_MESSAGE_LENGTH + 1);
        assert_eq!(diagnostic_output[0], b'E');
        assert_eq!(
            u32::from_be_bytes(diagnostic_output[1..5].try_into().expect("length word")),
            u32::try_from(BACKEND_STARTUP_ERROR_MESSAGE_LENGTH).expect("protocol bound fits")
        );
        assert_eq!(diagnostic_output[length - 2], 0);
        assert_eq!(diagnostic_output[length - 1], 0);

        let maximum_value_length = BACKEND_SHORT_MESSAGE_LENGTH - 7;
        let value = "x".repeat(maximum_value_length);
        let mut parameter_output = vec![0; BACKEND_SHORT_MESSAGE_LENGTH + 1];
        let length = encode_parameter_status("n", &value, &mut parameter_output)
            .expect("maximum ParameterStatus");
        assert_eq!(length, BACKEND_SHORT_MESSAGE_LENGTH + 1);
        let decoded = decode_parameter_status(complete(&parameter_output))
            .expect("maximum ParameterStatus frame");
        assert_eq!(decoded.name(), "n");
        assert_eq!(decoded.value().len(), maximum_value_length);

        let maximum_option_length = BACKEND_STARTUP_MESSAGE_LENGTH - 13;
        let mut option = b"_pq_.".to_vec();
        option.resize(maximum_option_length, b'x');
        let mut negotiation_output = vec![0; BACKEND_STARTUP_MESSAGE_LENGTH + 1];
        let length = encode_protocol_negotiation(
            protocol(3, 2),
            &[option.as_slice()],
            &mut negotiation_output,
        )
        .expect("maximum NegotiateProtocolVersion");
        assert_eq!(length, BACKEND_STARTUP_MESSAGE_LENGTH + 1);
        let decoded = decode_protocol_negotiation(complete(&negotiation_output))
            .expect("maximum NegotiateProtocolVersion frame");
        assert_eq!(
            decoded.unsupported_options().next(),
            Some(option.as_slice())
        );
    }

    #[test]
    fn variable_encoder_bounds_are_exact_and_non_mutating() {
        let mut output = [0x5a; 32];
        let original = output;
        let oversized_sasl_data = vec![b'x'; BACKEND_STARTUP_MESSAGE_LENGTH - 7];
        assert_eq!(
            encode_authentication_sasl_final(&oversized_sasl_data, &mut output),
            Err(BackendEncodeError::MessageTooLarge {
                actual: BACKEND_STARTUP_MESSAGE_LENGTH + 1,
                maximum: BACKEND_STARTUP_MESSAGE_LENGTH,
            })
        );
        assert_eq!(output, original);

        let oversized_diagnostic = "x".repeat(BACKEND_STARTUP_ERROR_MESSAGE_LENGTH - 27);
        assert_eq!(
            encode_error_response(
                ErrorResponseSeverity::Error,
                *b"XX000",
                &oversized_diagnostic,
                BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
                &mut output,
            ),
            Err(BackendEncodeError::MessageTooLarge {
                actual: BACKEND_STARTUP_ERROR_MESSAGE_LENGTH + 1,
                maximum: BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
            })
        );
        assert_eq!(output, original);

        let authenticated_limit = BACKEND_STARTUP_ERROR_MESSAGE_LENGTH + 1;
        let mut authenticated_output = vec![0; authenticated_limit + 1];
        let length = encode_error_response(
            ErrorResponseSeverity::Error,
            *b"XX000",
            &oversized_diagnostic,
            authenticated_limit,
            &mut authenticated_output,
        )
        .expect("authenticated caller-selected error bound");
        assert_eq!(length, authenticated_limit + 1);

        let oversized = "x".repeat(BACKEND_SHORT_MESSAGE_LENGTH);
        assert!(matches!(
            encode_parameter_status("name", &oversized, &mut output),
            Err(BackendEncodeError::MessageTooLarge { .. })
        ));
        assert_eq!(output, original);

        let oversized = vec![b'x'; BACKEND_STARTUP_MESSAGE_LENGTH];
        let mut option = b"_pq_.".to_vec();
        option.extend_from_slice(&oversized);
        assert!(matches!(
            encode_protocol_negotiation(protocol(3, 2), &[option.as_slice()], &mut output),
            Err(BackendEncodeError::MessageTooLarge { .. })
        ));
        assert_eq!(output, original);
    }

    #[test]
    fn size_bounds_precede_payload_scans() {
        let mut output = [0x5a; 32];
        let original = output;

        let mut oversized_diagnostic = "x".repeat(BACKEND_STARTUP_ERROR_MESSAGE_LENGTH);
        oversized_diagnostic.push('\0');
        assert!(matches!(
            encode_error_response(
                ErrorResponseSeverity::Error,
                *b"inval",
                &oversized_diagnostic,
                BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
                &mut output,
            ),
            Err(BackendEncodeError::MessageTooLarge { .. })
        ));
        assert_eq!(output, original);

        let mut oversized_value = "x".repeat(BACKEND_SHORT_MESSAGE_LENGTH);
        oversized_value.push('\0');
        assert!(matches!(
            encode_parameter_status("name", &oversized_value, &mut output),
            Err(BackendEncodeError::MessageTooLarge { .. })
        ));
        assert_eq!(output, original);

        let minimum_options = vec![b"_pq_.".as_slice(); MAX_PROTOCOL_NEGOTIATION_OPTIONS + 1];
        assert_eq!(
            encode_protocol_negotiation(protocol(3, 2), &minimum_options, &mut output),
            Err(BackendEncodeError::TooManyProtocolOptions {
                actual: MAX_PROTOCOL_NEGOTIATION_OPTIONS + 1,
                maximum: MAX_PROTOCOL_NEGOTIATION_OPTIONS,
            })
        );
        assert_eq!(output, original);

        let mut first_option = b"_pq_.".to_vec();
        first_option.resize(1_500, b'x');
        let mut second_option = b"_pq_.".to_vec();
        second_option.resize(1_000, b'y');
        assert_eq!(
            encode_protocol_negotiation(
                protocol(3, 2),
                &[first_option.as_slice(), second_option.as_slice()],
                &mut output,
            ),
            Err(BackendEncodeError::MessageTooLarge {
                actual: 2_514,
                maximum: BACKEND_STARTUP_MESSAGE_LENGTH,
            })
        );
        assert_eq!(output, original);

        let mut oversized_option = b"_pq_.".to_vec();
        oversized_option.resize(BACKEND_STARTUP_MESSAGE_LENGTH, b'x');
        oversized_option.push(0);
        assert_eq!(
            encode_protocol_negotiation(
                protocol(3, 2),
                &[oversized_option.as_slice()],
                &mut output,
            ),
            Err(BackendEncodeError::MessageTooLarge {
                actual: BACKEND_STARTUP_MESSAGE_LENGTH + 14,
                maximum: BACKEND_STARTUP_MESSAGE_LENGTH,
            })
        );
        assert_eq!(output, original);
    }

    #[test]
    fn encoding_errors_never_render_payloads() {
        let mut output = [0; 64];
        let errors = [
            encode_error_response(
                ErrorResponseSeverity::Fatal,
                *b"28P01",
                "do-not-render-this",
                BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
                &mut output[..8],
            )
            .expect_err("short diagnostic output"),
            encode_backend_key_data(1, b"s3k", &mut output).expect_err("short secret key"),
            encode_authentication_sasl_continue(b"server-nonce-secret", &mut output[..8])
                .expect_err("short SASL output"),
            encode_parameter_status("name", "topsecret\0payload", &mut output)
                .expect_err("embedded parameter terminator"),
            encode_protocol_negotiation(protocol(3, 2), &[b"private-option"], &mut output)
                .expect_err("non-reserved protocol option"),
        ];
        for error in errors {
            let rendered = format!("{error:?} {error}");
            for marker in [
                "do-not-render-this",
                "s3k",
                "server-nonce-secret",
                "topsecret",
                "payload",
                "private-option",
            ] {
                assert!(!rendered.contains(marker));
            }
        }
    }
}
