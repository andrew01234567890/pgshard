//! Bounded zero-copy decoding of `PostgreSQL` 18 backend frames.

use std::fmt;

use thiserror::Error;

use crate::messages::ParameterTypeIter;
use crate::{
    Decode, DecodeError, MAX_CANCEL_KEY_LENGTH, MAX_LARGE_MESSAGE_LENGTH,
    MIN_BACKEND_CANCEL_KEY_LENGTH,
};

/// `PostgreSQL` libpq's maximum length word for backend tags not classified as
/// long messages.
pub const BACKEND_SHORT_MESSAGE_LENGTH: usize = 30_000;
/// `PostgreSQL` libpq's startup-phase ceiling for authentication and protocol
/// negotiation backend messages.
pub const BACKEND_STARTUP_MESSAGE_LENGTH: usize = 2_000;
/// Exact maximum length word for a backend `ParameterDescription`.
pub const MAX_PARAMETER_DESCRIPTION_LENGTH: usize = 4 + 2 + 65_535 * 4;
/// Maximum length word for `PostgreSQL` 18 `BackendKeyData`.
pub const MAX_BACKEND_KEY_DATA_LENGTH: usize = 4 + 4 + MAX_CANCEL_KEY_LENGTH;

/// `PostgreSQL` 18 backend message tag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendTag {
    /// An extended-query Parse completed.
    ParseComplete,
    /// An extended-query Bind completed.
    BindComplete,
    /// An extended-query Close completed.
    CloseComplete,
    /// An asynchronous `LISTEN` notification.
    NotificationResponse,
    /// A SQL command completed.
    CommandComplete,
    /// One query-result row.
    DataRow,
    /// An error report.
    ErrorResponse,
    /// The backend is ready to receive COPY data.
    CopyInResponse,
    /// The backend will send COPY data.
    CopyOutResponse,
    /// A simple query contained no statement.
    EmptyQueryResponse,
    /// Backend process and cancellation-key data.
    BackendKeyData,
    /// A notice report.
    NoticeResponse,
    /// An authentication request or result.
    AuthenticationRequest,
    /// A changed run-time parameter.
    ParameterStatus,
    /// Query result-column metadata.
    RowDescription,
    /// A legacy function-call result.
    FunctionCallResponse,
    /// The backend and frontend may both stream COPY data.
    CopyBothResponse,
    /// The backend is ready for a new query cycle.
    ReadyForQuery,
    /// A described statement or portal returns no row data.
    NoData,
    /// Execute stopped after reaching its row limit.
    PortalSuspended,
    /// Prepared-statement parameter type metadata.
    ParameterDescription,
    /// A protocol-version negotiation response.
    NegotiateProtocolVersion,
    /// The backend completed a COPY stream.
    CopyDone,
    /// One chunk of COPY data.
    CopyData,
}

impl BackendTag {
    fn from_byte(value: u8) -> Option<Self> {
        Some(match value {
            b'1' => Self::ParseComplete,
            b'2' => Self::BindComplete,
            b'3' => Self::CloseComplete,
            b'A' => Self::NotificationResponse,
            b'C' => Self::CommandComplete,
            b'D' => Self::DataRow,
            b'E' => Self::ErrorResponse,
            b'G' => Self::CopyInResponse,
            b'H' => Self::CopyOutResponse,
            b'I' => Self::EmptyQueryResponse,
            b'K' => Self::BackendKeyData,
            b'N' => Self::NoticeResponse,
            b'R' => Self::AuthenticationRequest,
            b'S' => Self::ParameterStatus,
            b'T' => Self::RowDescription,
            b'V' => Self::FunctionCallResponse,
            b'W' => Self::CopyBothResponse,
            b'Z' => Self::ReadyForQuery,
            b'n' => Self::NoData,
            b's' => Self::PortalSuspended,
            b't' => Self::ParameterDescription,
            b'v' => Self::NegotiateProtocolVersion,
            b'c' => Self::CopyDone,
            b'd' => Self::CopyData,
            _ => return None,
        })
    }

    const fn maximum_message_length(self, caller_maximum: usize) -> usize {
        let protocol_maximum = match self {
            Self::ParseComplete
            | Self::BindComplete
            | Self::CloseComplete
            | Self::EmptyQueryResponse
            | Self::NoData
            | Self::PortalSuspended
            | Self::CopyDone => 4,
            Self::ReadyForQuery => 5,
            Self::AuthenticationRequest | Self::NegotiateProtocolVersion => {
                BACKEND_STARTUP_MESSAGE_LENGTH
            }
            Self::BackendKeyData => MAX_BACKEND_KEY_DATA_LENGTH,
            Self::ParameterDescription => MAX_PARAMETER_DESCRIPTION_LENGTH,
            Self::CopyData
            | Self::DataRow
            | Self::ErrorResponse
            | Self::FunctionCallResponse
            | Self::NoticeResponse
            | Self::NotificationResponse
            | Self::RowDescription => MAX_LARGE_MESSAGE_LENGTH,
            Self::CommandComplete
            | Self::CopyInResponse
            | Self::CopyOutResponse
            | Self::ParameterStatus
            | Self::CopyBothResponse => BACKEND_SHORT_MESSAGE_LENGTH,
        };
        if caller_maximum < protocol_maximum {
            caller_maximum
        } else {
            protocol_maximum
        }
    }

    const fn minimum_message_length(self) -> usize {
        match self {
            Self::ReadyForQuery => 5,
            Self::AuthenticationRequest | Self::NegotiateProtocolVersion => 8,
            Self::BackendKeyData => 4 + 4 + MIN_BACKEND_CANCEL_KEY_LENGTH,
            _ => 4,
        }
    }
}

/// One decoded backend frame.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BackendFrame<'a> {
    tag: BackendTag,
    body: &'a [u8],
}

impl fmt::Debug for BackendFrame<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendFrame")
            .field("tag", &self.tag)
            .field("body_length", &self.body.len())
            .finish()
    }
}

impl<'a> BackendFrame<'a> {
    /// Returns the validated backend message tag.
    #[must_use]
    pub const fn tag(self) -> BackendTag {
        self.tag
    }

    /// Returns the exact borrowed message body, excluding tag and length.
    #[must_use]
    pub const fn body(self) -> &'a [u8] {
        self.body
    }
}

/// Transaction state carried by a backend `ReadyForQuery` message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionStatus {
    /// Not inside a transaction block.
    Idle,
    /// Inside a live transaction block.
    InTransaction,
    /// Inside a failed transaction block.
    FailedTransaction,
}

/// Decodes one backend frame from the beginning of `input`.
///
/// Unknown tags are rejected before their length is trusted. Framing does not
/// prove that a known message is legal in the current authentication, query,
/// COPY, or replication phase; the session state machine must enforce that.
///
/// # Errors
///
/// Returns an error for an invalid caller bound, unknown tag, impossible
/// length, arithmetic overflow, or a message above the configured bound.
pub fn decode_backend(
    input: &[u8],
    maximum_message_length: usize,
) -> Result<Decode<BackendFrame<'_>>, DecodeError> {
    if !(4..=MAX_LARGE_MESSAGE_LENGTH).contains(&maximum_message_length) {
        return Err(DecodeError::InvalidMaximum {
            actual: maximum_message_length,
            minimum: 4,
            maximum: MAX_LARGE_MESSAGE_LENGTH,
        });
    }
    let Some(tag_byte) = input.first().copied() else {
        return Ok(Decode::Incomplete { required: 1 });
    };
    let tag = BackendTag::from_byte(tag_byte).ok_or(DecodeError::UnknownBackendTag(tag_byte))?;
    if input.len() < 5 {
        return Ok(Decode::Incomplete { required: 5 });
    }
    let message_length =
        usize::try_from(u32::from_be_bytes([input[1], input[2], input[3], input[4]]))
            .map_err(|_| DecodeError::LengthOverflow)?;
    let minimum = tag.minimum_message_length();
    if message_length < minimum {
        return Err(DecodeError::InvalidLength {
            actual: message_length,
            minimum,
        });
    }
    let maximum = tag.maximum_message_length(maximum_message_length);
    if message_length > maximum {
        return Err(DecodeError::FrameTooLarge {
            actual: message_length,
            maximum,
        });
    }
    let consumed = message_length
        .checked_add(1)
        .ok_or(DecodeError::LengthOverflow)?;
    if input.len() < consumed {
        return Ok(Decode::Incomplete { required: consumed });
    }
    Ok(Decode::Complete {
        frame: BackendFrame {
            tag,
            body: &input[5..consumed],
        },
        consumed,
    })
}

/// A validated backend `ParameterDescription` body.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ParameterDescription<'a> {
    parameter_type_bytes: &'a [u8],
}

impl fmt::Debug for ParameterDescription<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParameterDescription")
            .field("parameter_type_count", &self.parameter_type_count())
            .finish()
    }
}

impl<'a> ParameterDescription<'a> {
    /// Returns the number of parameter type OIDs.
    #[must_use]
    pub const fn parameter_type_count(self) -> usize {
        self.parameter_type_bytes.len() / 4
    }

    /// Iterates parameter type OIDs in wire order without allocation.
    #[must_use]
    pub const fn parameter_types(self) -> ParameterTypeIter<'a> {
        ParameterTypeIter::from_validated_bytes(self.parameter_type_bytes)
    }
}

/// Decodes an exact backend `ParameterDescription` body.
///
/// # Errors
///
/// Rejects the wrong tag, a missing count or OID, and bytes beyond the count
/// declared by the backend.
pub fn decode_parameter_description(
    frame: BackendFrame<'_>,
) -> Result<ParameterDescription<'_>, BackendMessageError> {
    if frame.tag() != BackendTag::ParameterDescription {
        return Err(BackendMessageError::WrongTag {
            expected: BackendTag::ParameterDescription,
            actual: frame.tag(),
        });
    }
    let Some(count_bytes) = frame.body().get(..2) else {
        return Err(BackendMessageError::Truncated("parameter count"));
    };
    let count = usize::from(u16::from_be_bytes([count_bytes[0], count_bytes[1]]));
    let expected_body_length = 2 + count * 4;
    match frame.body().len().cmp(&expected_body_length) {
        std::cmp::Ordering::Less => Err(BackendMessageError::Truncated("parameter type OIDs")),
        std::cmp::Ordering::Greater => Err(BackendMessageError::TrailingData(
            frame.body().len() - expected_body_length,
        )),
        std::cmp::Ordering::Equal => Ok(ParameterDescription {
            parameter_type_bytes: &frame.body()[2..],
        }),
    }
}

/// Decodes the exact transaction status in a backend `ReadyForQuery` body.
///
/// # Errors
///
/// Rejects another backend tag, a missing status byte, an unknown status, or
/// trailing bytes.
pub fn decode_ready_for_query(
    frame: BackendFrame<'_>,
) -> Result<TransactionStatus, BackendMessageError> {
    if frame.tag() != BackendTag::ReadyForQuery {
        return Err(BackendMessageError::WrongTag {
            expected: BackendTag::ReadyForQuery,
            actual: frame.tag(),
        });
    }
    let status = match frame.body() {
        b"I" => TransactionStatus::Idle,
        b"T" => TransactionStatus::InTransaction,
        b"E" => TransactionStatus::FailedTransaction,
        [] => return Err(BackendMessageError::Truncated("transaction status")),
        [actual] => return Err(BackendMessageError::InvalidTransactionStatus(*actual)),
        body => return Err(BackendMessageError::TrailingData(body.len() - 1)),
    };
    Ok(status)
}

/// Validates a backend message whose `PostgreSQL` 18 body must be empty.
///
/// This accepts `ParseComplete`, `BindComplete`, `CloseComplete`,
/// `EmptyQueryResponse`, `NoData`, `PortalSuspended`, and backend `CopyDone`.
/// Other tags require their own typed decoder even when a malformed frame
/// happens to carry no bytes.
///
/// # Errors
///
/// Rejects a tag outside that exact family or any nonempty body.
pub fn require_empty_backend_body(frame: BackendFrame<'_>) -> Result<(), BackendMessageError> {
    if !matches!(
        frame.tag(),
        BackendTag::ParseComplete
            | BackendTag::BindComplete
            | BackendTag::CloseComplete
            | BackendTag::EmptyQueryResponse
            | BackendTag::NoData
            | BackendTag::PortalSuspended
            | BackendTag::CopyDone
    ) {
        return Err(BackendMessageError::ExpectedEmptyBodyTag(frame.tag()));
    }
    if frame.body().is_empty() {
        Ok(())
    } else {
        Err(BackendMessageError::TrailingData(frame.body().len()))
    }
}

/// Backend message-body decoding failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum BackendMessageError {
    /// A typed decoder was called for another backend tag.
    #[error("expected {expected:?} backend message, received {actual:?}")]
    WrongTag {
        /// Required tag.
        expected: BackendTag,
        /// Actual tag.
        actual: BackendTag,
    },
    /// A fixed-width or counted field extends beyond the frame body.
    #[error("{0} is truncated")]
    Truncated(&'static str),
    /// A `ReadyForQuery` status is not idle `I`, transaction `T`, or failed `E`.
    #[error("invalid ReadyForQuery transaction status {0}")]
    InvalidTransactionStatus(u8),
    /// An empty-body validator was called for a message with another layout.
    #[error("backend message {0:?} does not belong to the empty-body family")]
    ExpectedEmptyBodyTag(BackendTag),
    /// Valid fields did not consume the exact frame body.
    #[error("message has {0} trailing bytes")]
    TrailingData(usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_LARGE_MESSAGE_LENGTH;

    fn backend(tag: u8, body: &[u8]) -> Vec<u8> {
        let length = u32::try_from(4 + body.len()).expect("test frame length");
        let mut packet = Vec::with_capacity(1 + length as usize);
        packet.push(tag);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    fn complete(input: &[u8]) -> BackendFrame<'_> {
        let Decode::Complete { frame, consumed } =
            decode_backend(input, DEFAULT_LARGE_MESSAGE_LENGTH).expect("backend frame")
        else {
            panic!("complete backend frame was incomplete");
        };
        assert_eq!(consumed, input.len());
        frame
    }

    fn unchecked(tag: u8, body: &[u8]) -> BackendFrame<'_> {
        BackendFrame {
            tag: BackendTag::from_byte(tag).expect("test backend tag"),
            body,
        }
    }

    #[test]
    fn decodes_every_postgres18_backend_tag() {
        for byte in *b"123ACDEGHIKNRSTVWZnstdcv" {
            let body = match byte {
                b'1' | b'2' | b'3' | b'I' | b'n' | b's' | b'c' => b"".as_slice(),
                b'Z' => b"I".as_slice(),
                b'K' => b"\0\0\0\x01key!".as_slice(),
                _ => b"body".as_slice(),
            };
            let packet = backend(byte, body);
            assert!(matches!(
                decode_backend(&packet, DEFAULT_LARGE_MESSAGE_LENGTH),
                Ok(Decode::Complete { consumed, .. }) if consumed == packet.len()
            ));
        }
    }

    #[test]
    fn fragmentation_and_concatenation_preserve_backend_boundaries() {
        let first = backend(b't', b"\0\0");
        for split in 0..first.len() {
            assert!(matches!(
                decode_backend(&first[..split], DEFAULT_LARGE_MESSAGE_LENGTH),
                Ok(Decode::Incomplete { .. })
            ));
        }

        let second = backend(b'Z', b"I");
        let mut input = first.clone();
        input.extend_from_slice(&second);
        let Decode::Complete { consumed, .. } =
            decode_backend(&input, DEFAULT_LARGE_MESSAGE_LENGTH).expect("first")
        else {
            panic!("first frame incomplete");
        };
        assert_eq!(consumed, first.len());
        assert!(matches!(
            decode_backend(&input[consumed..], DEFAULT_LARGE_MESSAGE_LENGTH),
            Ok(Decode::Complete { consumed, .. }) if consumed == second.len()
        ));
    }

    #[test]
    fn backend_tags_and_lengths_fail_closed_before_buffering() {
        assert_eq!(
            decode_backend(b"P\xff\xff\xff\xff", DEFAULT_LARGE_MESSAGE_LENGTH),
            Err(DecodeError::UnknownBackendTag(b'P'))
        );
        for invalid in [0_u32, 3] {
            let mut header = vec![b'Z'];
            header.extend_from_slice(&invalid.to_be_bytes());
            assert!(matches!(
                decode_backend(&header, DEFAULT_LARGE_MESSAGE_LENGTH),
                Err(DecodeError::InvalidLength { .. })
            ));
        }
        for invalid in [3, MAX_LARGE_MESSAGE_LENGTH + 1] {
            assert!(matches!(
                decode_backend(&[], invalid),
                Err(DecodeError::InvalidMaximum { actual, .. }) if actual == invalid
            ));
        }

        assert!(decode_backend(&backend(b'1', b""), 4).is_ok());
        let maximum = 5;
        assert!(decode_backend(&backend(b'Z', b"I"), maximum).is_ok());
        let oversized = backend(b'Z', b"II");
        assert!(matches!(
            decode_backend(&oversized[..5], DEFAULT_LARGE_MESSAGE_LENGTH),
            Err(DecodeError::FrameTooLarge {
                actual: 6,
                maximum: 5
            })
        ));
    }

    #[test]
    fn every_backend_family_is_bounded_from_its_header() {
        for (tag, actual, expected_maximum) in [
            (b'1', 5, 4),
            (b'Z', 6, 5),
            (
                b'R',
                BACKEND_STARTUP_MESSAGE_LENGTH + 1,
                BACKEND_STARTUP_MESSAGE_LENGTH,
            ),
            (
                b'v',
                BACKEND_STARTUP_MESSAGE_LENGTH + 1,
                BACKEND_STARTUP_MESSAGE_LENGTH,
            ),
            (
                b'K',
                MAX_BACKEND_KEY_DATA_LENGTH + 1,
                MAX_BACKEND_KEY_DATA_LENGTH,
            ),
            (
                b't',
                MAX_PARAMETER_DESCRIPTION_LENGTH + 1,
                MAX_PARAMETER_DESCRIPTION_LENGTH,
            ),
            (
                b'S',
                BACKEND_SHORT_MESSAGE_LENGTH + 1,
                BACKEND_SHORT_MESSAGE_LENGTH,
            ),
        ] {
            let mut header = vec![tag];
            header.extend_from_slice(
                &u32::try_from(actual)
                    .expect("backend test length fits u32")
                    .to_be_bytes(),
            );
            assert_eq!(
                decode_backend(&header, DEFAULT_LARGE_MESSAGE_LENGTH),
                Err(DecodeError::FrameTooLarge {
                    actual,
                    maximum: expected_maximum,
                })
            );
        }

        let long_header = [b'D', 0, 0, 0x75, 0x31];
        assert_eq!(
            decode_backend(&long_header, DEFAULT_LARGE_MESSAGE_LENGTH),
            Ok(Decode::Incomplete { required: 30_002 })
        );

        for tag in *b"Rv" {
            let minimum = [tag, 0, 0, 0, 8];
            assert_eq!(
                decode_backend(&minimum, DEFAULT_LARGE_MESSAGE_LENGTH),
                Ok(Decode::Incomplete { required: 9 })
            );
            let maximum = [tag, 0, 0, 7, 0xd0];
            assert_eq!(
                decode_backend(&maximum, DEFAULT_LARGE_MESSAGE_LENGTH),
                Ok(Decode::Incomplete {
                    required: BACKEND_STARTUP_MESSAGE_LENGTH + 1,
                })
            );
        }
    }

    #[test]
    fn backend_family_minimums_fail_closed_from_the_header() {
        for (tag, actual, minimum) in [
            (b'Z', 4, 5),
            (b'R', 7, 8),
            (b'v', 7, 8),
            (
                b'K',
                4 + 4 + MIN_BACKEND_CANCEL_KEY_LENGTH - 1,
                4 + 4 + MIN_BACKEND_CANCEL_KEY_LENGTH,
            ),
        ] {
            let mut header = vec![tag];
            header.extend_from_slice(
                &u32::try_from(actual)
                    .expect("backend test length fits u32")
                    .to_be_bytes(),
            );
            assert_eq!(
                decode_backend(&header, DEFAULT_LARGE_MESSAGE_LENGTH),
                Err(DecodeError::InvalidLength { actual, minimum })
            );
        }
    }

    #[test]
    fn parameter_description_is_exact_zero_copy_metadata() {
        let mut body = 3_u16.to_be_bytes().to_vec();
        for oid in [20_u32, 2950, 17] {
            body.extend_from_slice(&oid.to_be_bytes());
        }
        let packet = backend(b't', &body);
        let description =
            decode_parameter_description(complete(&packet)).expect("parameter description");
        assert_eq!(description.parameter_type_count(), 3);
        assert_eq!(description.parameter_types().len(), 3);
        assert_eq!(
            description.parameter_types().collect::<Vec<_>>(),
            vec![20, 2950, 17]
        );
        let first_oid_address = description.parameter_type_bytes.as_ptr();
        assert_eq!(first_oid_address, packet[7..].as_ptr());
    }

    #[test]
    fn zero_parameter_description_is_valid() {
        let packet = backend(b't', b"\0\0");
        let description =
            decode_parameter_description(complete(&packet)).expect("parameter description");
        assert_eq!(description.parameter_type_count(), 0);
        assert!(description.parameter_types().next().is_none());
    }

    #[test]
    fn maximum_parameter_description_count_is_preserved() {
        let count = u16::MAX;
        let mut body = Vec::with_capacity(2 + usize::from(count) * 4);
        body.extend_from_slice(&count.to_be_bytes());
        for oid in 1..=u32::from(count) {
            body.extend_from_slice(&oid.to_be_bytes());
        }
        let packet = backend(b't', &body);
        let description =
            decode_parameter_description(complete(&packet)).expect("maximum parameter count");
        assert_eq!(description.parameter_type_count(), usize::from(count));
        assert_eq!(description.parameter_types().next(), Some(1));
        assert_eq!(description.parameter_types().last(), Some(u32::from(count)));
    }

    #[test]
    fn ready_for_query_reports_exact_transaction_state() {
        for (body, expected) in [
            (b"I".as_slice(), TransactionStatus::Idle),
            (b"T".as_slice(), TransactionStatus::InTransaction),
            (b"E".as_slice(), TransactionStatus::FailedTransaction),
        ] {
            assert_eq!(
                decode_ready_for_query(complete(&backend(b'Z', body))),
                Ok(expected)
            );
        }
        assert_eq!(
            decode_ready_for_query(unchecked(b'Z', b"")),
            Err(BackendMessageError::Truncated("transaction status"))
        );
        assert_eq!(
            decode_ready_for_query(complete(&backend(b'Z', b"X"))),
            Err(BackendMessageError::InvalidTransactionStatus(b'X'))
        );
        assert_eq!(
            decode_ready_for_query(unchecked(b'Z', b"II")),
            Err(BackendMessageError::TrailingData(1))
        );
        assert!(matches!(
            decode_ready_for_query(complete(&backend(b'1', b""))),
            Err(BackendMessageError::WrongTag { .. })
        ));
    }

    #[test]
    fn exact_empty_backend_message_family_is_validated() {
        for tag in *b"123Insc" {
            assert_eq!(
                require_empty_backend_body(complete(&backend(tag, b""))),
                Ok(())
            );
            assert_eq!(
                require_empty_backend_body(unchecked(tag, b"x")),
                Err(BackendMessageError::TrailingData(1))
            );
        }
        assert_eq!(
            require_empty_backend_body(unchecked(b'Z', b"")),
            Err(BackendMessageError::ExpectedEmptyBodyTag(
                BackendTag::ReadyForQuery
            ))
        );
    }

    #[test]
    fn malformed_parameter_descriptions_fail_closed() {
        let mut body = 2_u16.to_be_bytes().to_vec();
        body.extend_from_slice(&20_u32.to_be_bytes());
        body.extend_from_slice(&2950_u32.to_be_bytes());
        for split in 0..body.len() {
            let packet = backend(b't', &body[..split]);
            assert!(decode_parameter_description(complete(&packet)).is_err());
        }

        let mut trailing = body.clone();
        trailing.push(0);
        assert_eq!(
            decode_parameter_description(complete(&backend(b't', &trailing))),
            Err(BackendMessageError::TrailingData(1))
        );
        assert!(matches!(
            decode_parameter_description(complete(&backend(b'Z', b"I"))),
            Err(BackendMessageError::WrongTag { .. })
        ));
    }

    #[test]
    fn backend_debug_output_never_exposes_payloads() {
        let packet = backend(b'E', b"do-not-log-this");
        let debug = format!("{:?}", complete(&packet));
        assert!(!debug.contains("do-not-log-this"));
        assert!(debug.contains("body_length"));

        let parameter_debug = format!(
            "{:?}",
            decode_parameter_description(complete(&backend(b't', b"\0\0")))
                .expect("parameter description")
        );
        assert!(parameter_debug.contains("parameter_type_count"));
    }
}
