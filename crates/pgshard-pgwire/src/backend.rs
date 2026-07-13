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
            Self::ParameterStatus | Self::ParameterDescription => 6,
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

/// One validated backend run-time parameter report.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ParameterStatus<'a> {
    name: &'a str,
    value: &'a str,
}

impl fmt::Debug for ParameterStatus<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParameterStatus")
            .field("name_length", &self.name.len())
            .field("value_length", &self.value.len())
            .finish()
    }
}

impl<'a> ParameterStatus<'a> {
    /// Returns the UTF-8 run-time parameter name without its wire terminator.
    #[must_use]
    pub const fn name(self) -> &'a str {
        self.name
    }

    /// Returns the UTF-8 run-time parameter value without its wire terminator.
    #[must_use]
    pub const fn value(self) -> &'a str {
        self.value
    }
}

/// Borrowed process and secret-key metadata needed to cancel a backend query.
#[derive(Clone, Copy)]
pub struct BackendKeyData<'a> {
    backend_pid: u32,
    cancellation_key: &'a [u8],
}

impl fmt::Debug for BackendKeyData<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendKeyData")
            .field("backend_pid", &self.backend_pid)
            .field("cancellation_key_length", &self.cancellation_key.len())
            .finish()
    }
}

impl<'a> BackendKeyData<'a> {
    /// Returns the backend process identifier in host byte order.
    #[must_use]
    pub const fn backend_pid(self) -> u32 {
        self.backend_pid
    }

    /// Returns the opaque borrowed cancellation authentication key.
    #[must_use]
    pub const fn cancellation_key(self) -> &'a [u8] {
        self.cancellation_key
    }
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

/// Decodes an exact backend `ParameterStatus` body without allocating.
///
/// This decoder validates UTF-8 directly because `ParameterStatus` is how the
/// session establishes and refreshes its authoritative `client_encoding`
/// proof. The session must reject a reported encoding other than canonical
/// `UTF8` before decoding any more query-protocol bodies.
///
/// # Errors
///
/// Rejects the wrong tag, missing string terminators, invalid UTF-8, or bytes
/// after the two protocol strings.
pub fn decode_parameter_status(
    frame: BackendFrame<'_>,
) -> Result<ParameterStatus<'_>, BackendMessageError> {
    require_tag(frame, BackendTag::ParameterStatus)?;
    let (name, remaining) = cstring_utf8(frame.body(), "parameter name")?;
    let (value, remaining) = cstring_utf8(remaining, "parameter value")?;
    if !remaining.is_empty() {
        return Err(BackendMessageError::TrailingData(remaining.len()));
    }
    Ok(ParameterStatus { name, value })
}

/// Decodes a backend `BackendKeyData` body without copying its secret key.
///
/// The generic `PostgreSQL` 18 boundary is four to 256 key bytes. The session
/// layer must bind this data to the exact upstream connection and negotiated
/// protocol version; protocol 3.0 additionally requires exactly four bytes.
///
/// # Errors
///
/// Rejects the wrong tag, a truncated process identifier, or a cancellation
/// key outside `PostgreSQL` 18's generic length boundary.
pub fn decode_backend_key_data(
    frame: BackendFrame<'_>,
) -> Result<BackendKeyData<'_>, BackendMessageError> {
    require_tag(frame, BackendTag::BackendKeyData)?;
    let Some(pid_bytes) = frame.body().get(..4) else {
        return Err(BackendMessageError::Truncated("backend process identifier"));
    };
    let cancellation_key = &frame.body()[4..];
    if !(MIN_BACKEND_CANCEL_KEY_LENGTH..=MAX_CANCEL_KEY_LENGTH).contains(&cancellation_key.len()) {
        return Err(BackendMessageError::InvalidCancellationKeyLength(
            cancellation_key.len(),
        ));
    }
    Ok(BackendKeyData {
        backend_pid: u32::from_be_bytes([pid_bytes[0], pid_bytes[1], pid_bytes[2], pid_bytes[3]]),
        cancellation_key,
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
    require_tag(frame, BackendTag::ParameterDescription)?;
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
    require_tag(frame, BackendTag::ReadyForQuery)?;
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

fn require_tag(frame: BackendFrame<'_>, expected: BackendTag) -> Result<(), BackendMessageError> {
    if frame.tag() == expected {
        Ok(())
    } else {
        Err(BackendMessageError::WrongTag {
            expected,
            actual: frame.tag(),
        })
    }
}

fn cstring_utf8<'a>(
    input: &'a [u8],
    field: &'static str,
) -> Result<(&'a str, &'a [u8]), BackendMessageError> {
    let Some(end) = input.iter().position(|byte| *byte == 0) else {
        return Err(BackendMessageError::Truncated(field));
    };
    let value =
        std::str::from_utf8(&input[..end]).map_err(|_| BackendMessageError::InvalidUtf8(field))?;
    Ok((value, &input[end + 1..]))
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
    /// A backend protocol string is not valid UTF-8.
    #[error("{0} is not valid UTF-8")]
    InvalidUtf8(&'static str),
    /// A backend cancellation key is outside `PostgreSQL` 18's generic bounds.
    #[error("invalid PostgreSQL 18 cancellation key length {0}")]
    InvalidCancellationKeyLength(usize),
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
            (b'S', 5, 6),
            (b't', 5, 6),
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
    fn parameter_status_is_exact_zero_copy_metadata() {
        let packet = backend(b'S', b"client_encoding\0UTF8\0");
        let status = decode_parameter_status(complete(&packet)).expect("parameter status");
        assert_eq!(status.name(), "client_encoding");
        assert_eq!(status.value(), "UTF8");
        assert_eq!(status.name.as_ptr(), packet[5..].as_ptr());
        assert_eq!(
            status.value.as_ptr(),
            packet[5 + b"client_encoding\0".len()..].as_ptr()
        );

        let rendered = format!("{status:?}");
        assert!(!rendered.contains("client_encoding"));
        assert!(!rendered.contains("UTF8"));
        assert!(rendered.contains("name_length"));
        assert!(rendered.contains("value_length"));
    }

    #[test]
    fn malformed_parameter_statuses_fail_closed() {
        let complete_body = b"client_encoding\0UTF8\0";
        for split in 0..complete_body.len() {
            assert!(decode_parameter_status(unchecked(b'S', &complete_body[..split])).is_err());
        }
        for (body, expected) in [
            (
                b"".as_slice(),
                BackendMessageError::Truncated("parameter name"),
            ),
            (
                b"client_encoding".as_slice(),
                BackendMessageError::Truncated("parameter name"),
            ),
            (
                b"client_encoding\0UTF8".as_slice(),
                BackendMessageError::Truncated("parameter value"),
            ),
            (
                b"\xff\0UTF8\0".as_slice(),
                BackendMessageError::InvalidUtf8("parameter name"),
            ),
            (
                b"client_encoding\0\xff\0".as_slice(),
                BackendMessageError::InvalidUtf8("parameter value"),
            ),
            (
                b"client_encoding\0UTF8\0x".as_slice(),
                BackendMessageError::TrailingData(1),
            ),
        ] {
            assert_eq!(
                decode_parameter_status(unchecked(b'S', body)),
                Err(expected)
            );
        }
        assert!(matches!(
            decode_parameter_status(complete(&backend(b't', b"\0\0"))),
            Err(BackendMessageError::WrongTag { .. })
        ));
        let empty_value_packet = backend(b'S', b"application_name\0\0");
        let empty_value =
            decode_parameter_status(complete(&empty_value_packet)).expect("empty parameter value");
        assert_eq!(empty_value.name(), "application_name");
        assert_eq!(empty_value.value(), "");
    }

    #[test]
    fn backend_key_data_is_bounded_zero_copy_secret_metadata() {
        for key_length in [MIN_BACKEND_CANCEL_KEY_LENGTH, 32, MAX_CANCEL_KEY_LENGTH] {
            let key = vec![0xa5; key_length];
            let mut body = 0x0102_0304_u32.to_be_bytes().to_vec();
            body.extend_from_slice(&key);
            let packet = backend(b'K', &body);
            let data = decode_backend_key_data(complete(&packet)).expect("backend key data");
            assert_eq!(data.backend_pid(), 0x0102_0304);
            assert_eq!(data.cancellation_key(), key);
            assert_eq!(data.cancellation_key.as_ptr(), packet[9..].as_ptr());
        }

        let packet = backend(b'K', b"\0\0\0\x07do-not-log");
        let data = decode_backend_key_data(complete(&packet)).expect("backend key data");
        let rendered = format!("{data:?}");
        assert!(!rendered.contains("do-not-log"));
        assert!(rendered.contains("cancellation_key_length"));
    }

    #[test]
    fn malformed_backend_key_data_fails_closed() {
        for pid_length in 0..4 {
            assert!(matches!(
                decode_backend_key_data(unchecked(b'K', &0_u32.to_be_bytes()[..pid_length])),
                Err(BackendMessageError::Truncated("backend process identifier"))
            ));
        }
        for key_length in 0..MIN_BACKEND_CANCEL_KEY_LENGTH {
            let mut body = 7_u32.to_be_bytes().to_vec();
            body.resize(4 + key_length, 0xa5);
            assert!(matches!(
                decode_backend_key_data(unchecked(b'K', &body)),
                Err(BackendMessageError::InvalidCancellationKeyLength(actual))
                    if actual == key_length
            ));
        }
        let mut oversized = 7_u32.to_be_bytes().to_vec();
        oversized.resize(4 + MAX_CANCEL_KEY_LENGTH + 1, 0xa5);
        assert!(matches!(
            decode_backend_key_data(unchecked(b'K', &oversized)),
            Err(BackendMessageError::InvalidCancellationKeyLength(actual))
                if actual == MAX_CANCEL_KEY_LENGTH + 1
        ));
        assert!(matches!(
            decode_backend_key_data(complete(&backend(b'S', b"name\0value\0"))),
            Err(BackendMessageError::WrongTag { .. })
        ));
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
            assert!(decode_parameter_description(unchecked(b't', &body[..split])).is_err());
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
