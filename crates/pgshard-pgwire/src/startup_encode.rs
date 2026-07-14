//! Allocation-free encoding of `PostgreSQL` 18 startup-phase requests.

use thiserror::Error;

use crate::{
    BackendKeyData, CANCEL_REQUEST_CODE, MAX_STARTUP_BODY_LENGTH, MAX_STARTUP_FRAME_LENGTH,
    NEGOTIATE_GSS_CODE, NEGOTIATE_SSL_CODE, Postgres18BackendKeyError, Postgres18Protocol,
    ProtocolVersion, STARTUP_HEADER_LENGTH, STARTUP_HEADER_LENGTH_WORD,
};

/// Exact wire length of an SSL or GSS encryption-negotiation request.
pub const ENCRYPTION_REQUEST_FRAME_LENGTH: usize = STARTUP_HEADER_LENGTH;

/// Maximum number of minimally sized name/value pairs in a startup packet.
pub const MAX_STARTUP_PARAMETERS: usize = (MAX_STARTUP_BODY_LENGTH - 5) / 3;

/// Encodes `PostgreSQL`'s exact eight-byte SSL negotiation request.
#[must_use]
pub const fn encode_ssl_request() -> [u8; ENCRYPTION_REQUEST_FRAME_LENGTH] {
    encode_encryption_request(NEGOTIATE_SSL_CODE)
}

/// Encodes `PostgreSQL`'s exact eight-byte GSS encryption negotiation request.
#[must_use]
pub const fn encode_gss_encryption_request() -> [u8; ENCRYPTION_REQUEST_FRAME_LENGTH] {
    encode_encryption_request(NEGOTIATE_GSS_CODE)
}

/// Encodes a regular protocol-three startup packet into caller-owned storage.
///
/// Parameter order and duplicates are preserved. Names must be nonempty;
/// values may be empty. Both remain protocol bytes rather than UTF-8 because
/// client encoding has not yet been established.
///
/// The output is not modified on error.
///
/// # Errors
///
/// Rejects a protocol major other than three, too many parameters, an empty
/// name, an embedded zero byte, a frame above `PostgreSQL` 18's 10,004-byte
/// startup limit, arithmetic overflow, or caller-owned storage smaller than
/// the complete packet.
pub fn encode_startup(
    protocol: ProtocolVersion,
    parameters: &[(&[u8], &[u8])],
    output: &mut [u8],
) -> Result<usize, StartupEncodeError> {
    if !protocol.is_postgres18_supported_major() {
        return Err(StartupEncodeError::UnsupportedProtocolMajor(
            protocol.major(),
        ));
    }
    if parameters.len() > MAX_STARTUP_PARAMETERS {
        return Err(StartupEncodeError::TooManyParameters {
            actual: parameters.len(),
            maximum: MAX_STARTUP_PARAMETERS,
        });
    }

    let body_length = parameters.iter().try_fold(
        5_usize,
        |body_length, (name, value)| -> Result<usize, StartupEncodeError> {
            body_length
                .checked_add(name.len())
                .and_then(|length| length.checked_add(1))
                .and_then(|length| length.checked_add(value.len()))
                .and_then(|length| length.checked_add(1))
                .ok_or(StartupEncodeError::LengthOverflow)
        },
    )?;
    let frame_length = body_length
        .checked_add(4)
        .ok_or(StartupEncodeError::LengthOverflow)?;
    if frame_length > MAX_STARTUP_FRAME_LENGTH {
        return Err(StartupEncodeError::FrameTooLarge {
            actual: frame_length,
            maximum: MAX_STARTUP_FRAME_LENGTH,
        });
    }

    for (index, (name, value)) in parameters.iter().copied().enumerate() {
        if name.is_empty() {
            return Err(StartupEncodeError::EmptyParameterName(index));
        }
        if name.contains(&0) {
            return Err(StartupEncodeError::ParameterNameContainsNull(index));
        }
        if value.contains(&0) {
            return Err(StartupEncodeError::ParameterValueContainsNull(index));
        }
    }
    require_output(output, frame_length)?;

    let frame_length_word =
        u32::try_from(frame_length).map_err(|_| StartupEncodeError::LengthOverflow)?;
    output[..4].copy_from_slice(&frame_length_word.to_be_bytes());
    output[4..8].copy_from_slice(&protocol.wire_code().to_be_bytes());
    let mut offset = 8;
    for (name, value) in parameters.iter().copied() {
        offset = write_cstring(output, offset, name);
        offset = write_cstring(output, offset, value);
    }
    output[offset] = 0;
    offset += 1;
    debug_assert_eq!(offset, frame_length);
    Ok(frame_length)
}

/// Encodes a `PostgreSQL` 18 cancellation request for decoded backend key data.
///
/// The protocol proof enforces `PostgreSQL` 18's exact four-byte key for
/// protocol 3.0/3.1 or 32-byte key for protocol 3.2. The connection owner must
/// still bind both values to the exact upstream socket that produced them.
/// The output is not modified on error.
///
/// # Errors
///
/// Rejects key data that does not match the negotiated protocol or
/// caller-owned storage smaller than the complete packet.
pub fn encode_postgres18_cancel_request(
    protocol: Postgres18Protocol,
    data: BackendKeyData<'_>,
    output: &mut [u8],
) -> Result<usize, StartupEncodeError> {
    protocol.validate_backend_key_data(data)?;
    let key = data.cancellation_key();
    let frame_length = 12_usize
        .checked_add(key.len())
        .ok_or(StartupEncodeError::LengthOverflow)?;
    require_output(output, frame_length)?;

    let frame_length_word =
        u32::try_from(frame_length).map_err(|_| StartupEncodeError::LengthOverflow)?;
    output[..4].copy_from_slice(&frame_length_word.to_be_bytes());
    output[4..8].copy_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
    output[8..12].copy_from_slice(&data.backend_pid().to_be_bytes());
    output[12..frame_length].copy_from_slice(key);
    Ok(frame_length)
}

const fn encode_encryption_request(code: u32) -> [u8; ENCRYPTION_REQUEST_FRAME_LENGTH] {
    let length = STARTUP_HEADER_LENGTH_WORD.to_be_bytes();
    let code = code.to_be_bytes();
    [
        length[0], length[1], length[2], length[3], code[0], code[1], code[2], code[3],
    ]
}

fn require_output(output: &[u8], required: usize) -> Result<(), StartupEncodeError> {
    if output.len() < required {
        Err(StartupEncodeError::OutputTooSmall {
            actual: output.len(),
            required,
        })
    } else {
        Ok(())
    }
}

fn write_cstring(output: &mut [u8], offset: usize, value: &[u8]) -> usize {
    let value_end = offset + value.len();
    output[offset..value_end].copy_from_slice(value);
    output[value_end] = 0;
    value_end + 1
}

/// Startup-phase request encoding failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum StartupEncodeError {
    /// `PostgreSQL` 18 accepts only protocol major version three.
    #[error("PostgreSQL 18 does not support startup protocol major version {0}")]
    UnsupportedProtocolMajor(u16),
    /// The parameter count cannot fit even with minimal valid pairs.
    #[error("startup parameter count {actual} exceeds maximum {maximum}")]
    TooManyParameters {
        /// Rejected parameter count.
        actual: usize,
        /// Maximum count that can fit in `PostgreSQL` 18's startup bound.
        maximum: usize,
    },
    /// One parameter name would terminate the complete list.
    #[error("startup parameter {0} has an empty name")]
    EmptyParameterName(usize),
    /// One parameter name contains its own protocol terminator.
    #[error("startup parameter name {0} contains an embedded zero byte")]
    ParameterNameContainsNull(usize),
    /// One parameter value contains its own protocol terminator.
    #[error("startup parameter value {0} contains an embedded zero byte")]
    ParameterValueContainsNull(usize),
    /// The packet exceeds `PostgreSQL` 18's startup frame bound.
    #[error("startup frame length {actual} exceeds maximum {maximum}")]
    FrameTooLarge {
        /// Computed total packet length.
        actual: usize,
        /// `PostgreSQL` 18's maximum total packet length.
        maximum: usize,
    },
    /// Backend cancellation metadata disagrees with its negotiated protocol.
    #[error("invalid PostgreSQL 18 cancellation metadata: {0}")]
    InvalidPostgres18CancelKey(#[from] Postgres18BackendKeyError),
    /// Caller-owned storage cannot hold the complete packet.
    #[error("startup output length {actual} is smaller than required {required}")]
    OutputTooSmall {
        /// Available caller-owned bytes.
        actual: usize,
        /// Complete packet length.
        required: usize,
    },
    /// Packet sizing overflowed the platform or protocol length type.
    #[error("startup frame length overflow")]
    LengthOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DEFAULT_LARGE_MESSAGE_LENGTH, Decode, StartupFrame, decode_backend,
        decode_backend_key_data, decode_startup,
    };

    fn complete_startup(input: &[u8]) -> StartupFrame<'_> {
        let Decode::Complete { frame, consumed } = decode_startup(input).expect("startup frame")
        else {
            panic!("encoded startup frame was incomplete");
        };
        assert_eq!(consumed, input.len());
        frame
    }

    fn protocol(minor: u16) -> Postgres18Protocol {
        let minor = minor.to_be_bytes();
        let packet = [0, 0, 0, 9, 0, 3, minor[0], minor[1], 0];
        crate::Postgres18StartupNegotiation::begin(complete_startup(&packet))
            .expect("protocol-three startup")
            .finish()
            .expect("native PostgreSQL 18 protocol")
    }

    fn backend_key_packet(pid: u32, key: &[u8]) -> Vec<u8> {
        let message_length = u32::try_from(8 + key.len()).expect("bounded backend key");
        let mut packet = vec![b'K'];
        packet.extend_from_slice(&message_length.to_be_bytes());
        packet.extend_from_slice(&pid.to_be_bytes());
        packet.extend_from_slice(key);
        packet
    }

    fn backend_key(input: &[u8]) -> BackendKeyData<'_> {
        let Decode::Complete { frame, consumed } =
            decode_backend(input, DEFAULT_LARGE_MESSAGE_LENGTH).expect("backend key frame")
        else {
            panic!("backend key frame was incomplete");
        };
        assert_eq!(consumed, input.len());
        decode_backend_key_data(frame).expect("backend key body")
    }

    #[test]
    fn fixed_encryption_requests_round_trip() {
        assert_eq!(encode_ssl_request(), [0, 0, 0, 8, 4, 210, 22, 47]);
        assert_eq!(
            encode_gss_encryption_request(),
            [0, 0, 0, 8, 4, 210, 22, 48]
        );
        assert_eq!(
            complete_startup(&encode_ssl_request()),
            StartupFrame::SslRequest
        );
        assert_eq!(
            complete_startup(&encode_gss_encryption_request()),
            StartupFrame::GssEncryptionRequest
        );
    }

    #[test]
    fn regular_startup_preserves_order_duplicates_and_empty_values() {
        let parameters = [
            (b"user".as_slice(), b"postgres".as_slice()),
            (b"application_name".as_slice(), b"".as_slice()),
            (b"_pq_.one".as_slice(), b"first".as_slice()),
            (b"_pq_.one".as_slice(), b"second".as_slice()),
        ];
        let mut output = [0; 256];
        let length = encode_startup(ProtocolVersion::new(3, 99), &parameters, &mut output)
            .expect("bounded startup packet");
        let StartupFrame::Startup {
            protocol,
            parameters: decoded,
        } = complete_startup(&output[..length])
        else {
            panic!("encoded a special startup request");
        };
        assert_eq!(protocol, ProtocolVersion::new(3, 99));
        assert_eq!(decoded.iter().collect::<Vec<_>>(), parameters);
    }

    #[test]
    fn regular_startup_without_parameters_has_the_minimum_layout() {
        let mut output = [0; 9];
        let length = encode_startup(ProtocolVersion::new(3, 2), &[], &mut output)
            .expect("minimum regular startup packet");
        assert_eq!(length, output.len());
        let StartupFrame::Startup { parameters, .. } = complete_startup(&output) else {
            panic!("encoded a special startup request");
        };
        assert_eq!(parameters.iter().count(), 0);
    }

    #[test]
    fn maximum_regular_startup_frame_is_exact() {
        let name = vec![b'n'; MAX_STARTUP_BODY_LENGTH - 7];
        let parameters = [(name.as_slice(), b"".as_slice())];
        let mut output = vec![0; MAX_STARTUP_FRAME_LENGTH];
        let length = encode_startup(ProtocolVersion::new(3, 2), &parameters, &mut output)
            .expect("maximum startup packet");
        assert_eq!(length, MAX_STARTUP_FRAME_LENGTH);
        let StartupFrame::Startup {
            parameters: decoded,
            ..
        } = complete_startup(&output)
        else {
            panic!("encoded a special startup request");
        };
        assert_eq!(decoded.iter().next(), Some(parameters[0]));
    }

    #[test]
    fn maximum_startup_parameter_count_is_accepted() {
        let parameters = vec![(b"n".as_slice(), b"".as_slice()); MAX_STARTUP_PARAMETERS];
        let mut output = vec![0; MAX_STARTUP_FRAME_LENGTH];
        let length = encode_startup(ProtocolVersion::new(3, 2), &parameters, &mut output)
            .expect("maximum startup parameter count");
        let StartupFrame::Startup {
            parameters: decoded,
            ..
        } = complete_startup(&output[..length])
        else {
            panic!("encoded a special startup request");
        };
        assert_eq!(decoded.iter().count(), MAX_STARTUP_PARAMETERS);
    }

    #[test]
    fn cancellation_uses_protocol_specific_backend_keys() {
        for (minor, key_length) in [(0, 4), (1, 4), (2, 32)] {
            let key = vec![0xa5; key_length];
            let packet = backend_key_packet(0x0102_0304, &key);
            let data = backend_key(&packet);
            let mut output = [0; 64];
            let length = encode_postgres18_cancel_request(protocol(minor), data, &mut output)
                .expect("protocol-specific cancellation packet");
            assert_eq!(length, 12 + key_length);
            assert_eq!(&output[4..8], &[4, 210, 22, 46]);
            let StartupFrame::CancelRequest { backend_pid, key } =
                complete_startup(&output[..length])
            else {
                panic!("encoded another startup request");
            };
            assert_eq!(backend_pid, 0x0102_0304);
            assert_eq!(key, vec![0xa5; key_length]);
        }
    }

    fn assert_error_does_not_modify(
        action: impl FnOnce(&mut [u8]) -> Result<usize, StartupEncodeError>,
    ) -> StartupEncodeError {
        let mut output = [0x5a; 64];
        let original = output;
        let error = action(&mut output).expect_err("invalid startup input");
        assert_eq!(output, original);
        error
    }

    #[test]
    fn regular_startup_rejects_invalid_input_without_modification() {
        assert_eq!(
            assert_error_does_not_modify(|output| encode_startup(
                ProtocolVersion::new(4, 0),
                &[],
                output,
            )),
            StartupEncodeError::UnsupportedProtocolMajor(4)
        );

        let too_many = vec![(b"n".as_slice(), b"".as_slice()); MAX_STARTUP_PARAMETERS + 1];
        assert_eq!(
            assert_error_does_not_modify(|output| encode_startup(
                ProtocolVersion::new(3, 2),
                &too_many,
                output,
            )),
            StartupEncodeError::TooManyParameters {
                actual: MAX_STARTUP_PARAMETERS + 1,
                maximum: MAX_STARTUP_PARAMETERS,
            }
        );

        for (parameters, expected) in [
            (
                vec![(b"".as_slice(), b"value".as_slice())],
                StartupEncodeError::EmptyParameterName(0),
            ),
            (
                vec![(b"bad\0name".as_slice(), b"value".as_slice())],
                StartupEncodeError::ParameterNameContainsNull(0),
            ),
            (
                vec![(b"name".as_slice(), b"bad\0value".as_slice())],
                StartupEncodeError::ParameterValueContainsNull(0),
            ),
        ] {
            assert_eq!(
                assert_error_does_not_modify(|output| encode_startup(
                    ProtocolVersion::new(3, 2),
                    &parameters,
                    output,
                )),
                expected
            );
        }

        assert!(matches!(
            assert_error_does_not_modify(|output| encode_startup(
                ProtocolVersion::new(3, 2),
                &[(b"user".as_slice(), b"postgres".as_slice())],
                &mut output[..8],
            )),
            StartupEncodeError::OutputTooSmall { .. }
        ));
    }

    #[test]
    fn startup_size_bound_precedes_payload_scans() {
        let mut oversized_name = vec![b'n'; MAX_STARTUP_BODY_LENGTH];
        oversized_name.push(0);
        let parameters = [(oversized_name.as_slice(), b"hidden\0value".as_slice())];
        let actual = 4 + 5 + parameters[0].0.len() + 1 + parameters[0].1.len() + 1;
        assert_eq!(
            assert_error_does_not_modify(|output| encode_startup(
                ProtocolVersion::new(3, 2),
                &parameters,
                output,
            )),
            StartupEncodeError::FrameTooLarge {
                actual,
                maximum: MAX_STARTUP_FRAME_LENGTH,
            }
        );
    }

    #[test]
    fn cancellation_errors_are_non_mutating_and_redacted() {
        let short_key_packet = backend_key_packet(7, &[0xa5; 4]);
        let short_key = backend_key(&short_key_packet);
        let mismatch = assert_error_does_not_modify(|output| {
            encode_postgres18_cancel_request(protocol(2), short_key, output)
        });
        assert!(matches!(
            mismatch,
            StartupEncodeError::InvalidPostgres18CancelKey(_)
        ));

        let valid_key_packet = backend_key_packet(7, &[0xa5; 32]);
        let valid_key = backend_key(&valid_key_packet);
        assert!(matches!(
            assert_error_does_not_modify(|output| encode_postgres18_cancel_request(
                protocol(2),
                valid_key,
                &mut output[..43],
            )),
            StartupEncodeError::OutputTooSmall { required: 44, .. }
        ));

        for error in [
            mismatch,
            assert_error_does_not_modify(|output| {
                encode_startup(
                    ProtocolVersion::new(3, 2),
                    &[(
                        b"private-name".as_slice(),
                        b"private-value\0tail".as_slice(),
                    )],
                    output,
                )
            }),
        ] {
            let rendered = format!("{error:?} {error}");
            for marker in ["a5", "private-name", "private-value", "tail"] {
                assert!(!rendered.contains(marker));
            }
        }
    }
}
