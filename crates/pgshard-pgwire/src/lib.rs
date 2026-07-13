//! Bounded zero-copy decoding of `PostgreSQL` 18 frontend and backend frames.
//!
//! Framing is deliberately separate from session state. This crate recognizes
//! byte-level messages; a pooler must still reject messages that are invalid in
//! the current authentication, query, copy, or transaction phase.

use std::fmt;

use thiserror::Error;

mod backend;
mod messages;

pub use backend::{
    BACKEND_SHORT_MESSAGE_LENGTH, BACKEND_STARTUP_MESSAGE_LENGTH, BackendFrame,
    BackendMessageError, BackendTag, MAX_BACKEND_KEY_DATA_LENGTH, MAX_PARAMETER_DESCRIPTION_LENGTH,
    ParameterDescription, decode_backend, decode_parameter_description,
};

pub use messages::{
    BindMessage, BindParameter, BindParameterIter, BindParameters, ExecuteMessage, FormatCode,
    FormatCodeIter, MessageError, ParameterTypeIter, ParseMessage, QueryMessage, decode_bind,
    decode_execute, decode_parse, decode_query, require_empty_body,
};

/// Maximum body size accepted by `PostgreSQL` 18 for a startup packet.
pub const MAX_STARTUP_BODY_LENGTH: usize = 10_000;
/// Maximum total startup frame size, including its four-byte length word.
pub const MAX_STARTUP_FRAME_LENGTH: usize = MAX_STARTUP_BODY_LENGTH + 4;
/// `PostgreSQL` 18 small-message bound, including the four-byte length word.
pub const SMALL_MESSAGE_LENGTH: usize = 10_000;
/// `PostgreSQL` 18 authentication-message bound, including the length word.
pub const AUTHENTICATION_MESSAGE_LENGTH: usize = 65_535;
/// Default bound for typed protocol messages that may carry large payloads.
pub const DEFAULT_LARGE_MESSAGE_LENGTH: usize = 16 * 1024 * 1024;
/// Hard pooler bound for one typed protocol message, regardless of caller policy.
pub const MAX_LARGE_MESSAGE_LENGTH: usize = 64 * 1024 * 1024;
/// Maximum `PostgreSQL` 18 cancellation authentication key size.
pub const MAX_CANCEL_KEY_LENGTH: usize = 256;

const CANCEL_REQUEST_CODE: u32 = protocol_code(1234, 5678);
const NEGOTIATE_SSL_CODE: u32 = protocol_code(1234, 5679);
const NEGOTIATE_GSS_CODE: u32 = protocol_code(1234, 5680);
const LATEST_PROTOCOL_MINOR: u16 = 2;

const fn protocol_code(major: u16, minor: u16) -> u32 {
    (major as u32) << 16 | minor as u32
}

/// Proof that the frontend session is pinned to `PostgreSQL`'s canonical
/// `UTF8` client encoding.
///
/// The session layer must rebuild this token from authoritative state after
/// startup and every accepted setting change. It must reject any attempt to
/// select another encoding before decoding more query-protocol bodies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientEncoding {
    _private: (),
}

impl ClientEncoding {
    /// Validates `PostgreSQL`'s canonical reported client-encoding name.
    ///
    /// # Errors
    ///
    /// Returns an error unless `value` is exactly `UTF8`.
    pub fn require_utf8(value: &str) -> Result<Self, ClientEncodingError> {
        if value == "UTF8" {
            Ok(Self { _private: () })
        } else {
            Err(ClientEncodingError)
        }
    }
}

/// A session selected an encoding whose conversion semantics are unsupported.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("pooler sessions must use canonical PostgreSQL client_encoding UTF8")]
pub struct ClientEncodingError;

/// Result of attempting to decode one complete frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decode<T> {
    /// A complete frame and the number of input bytes it occupies.
    Complete {
        /// Borrowed decoded frame.
        frame: T,
        /// Bytes to remove before decoding the next frame.
        consumed: usize,
    },
    /// More input is required. `required` is the minimum total input length.
    Incomplete {
        /// Minimum total bytes required to continue this frame.
        required: usize,
    },
}

/// Frontend protocol version requested in a startup packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolVersion {
    major: u16,
    minor: u16,
}

impl ProtocolVersion {
    /// Returns the requested major version.
    #[must_use]
    pub const fn major(self) -> u16 {
        self.major
    }

    /// Returns the requested minor version.
    #[must_use]
    pub const fn minor(self) -> u16 {
        self.minor
    }

    /// Whether the major version is supported by `PostgreSQL` 18.
    #[must_use]
    pub const fn is_postgres18_supported_major(self) -> bool {
        self.major == 3
    }

    /// Whether the requested version alone requires a `PostgreSQL` 18
    /// version-negotiation response.
    #[must_use]
    pub const fn version_requires_postgres18_negotiation(self) -> bool {
        self.major == 3 && self.minor > LATEST_PROTOCOL_MINOR
    }
}

/// One decoded startup-phase frame.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum StartupFrame<'a> {
    /// A regular protocol startup request.
    Startup {
        /// Requested protocol version.
        protocol: ProtocolVersion,
        /// Validated zero-copy name/value parameters.
        parameters: StartupParameters<'a>,
    },
    /// Eight-byte SSL negotiation request.
    SslRequest,
    /// Eight-byte GSSAPI encryption negotiation request.
    GssEncryptionRequest,
    /// `PostgreSQL` 18 cancellation request with a variable-length key.
    CancelRequest {
        /// Backend process identifier in host byte order.
        backend_pid: u32,
        /// Opaque one-to-256-byte cancellation authentication key.
        key: &'a [u8],
    },
}

impl fmt::Debug for StartupFrame<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Startup {
                protocol,
                parameters,
            } => formatter
                .debug_struct("Startup")
                .field("protocol", protocol)
                .field("parameter_count", &parameters.iter().count())
                .finish(),
            Self::SslRequest => formatter.write_str("SslRequest"),
            Self::GssEncryptionRequest => formatter.write_str("GssEncryptionRequest"),
            Self::CancelRequest { backend_pid, key } => formatter
                .debug_struct("CancelRequest")
                .field("backend_pid", backend_pid)
                .field("key_length", &key.len())
                .finish(),
        }
    }
}

impl StartupFrame<'_> {
    /// Whether `PostgreSQL` 18 must send `NegotiateProtocolVersion`.
    ///
    /// `PostgreSQL` 18 responds for either a newer protocol-three minor version
    /// or any unrecognized startup parameter in the reserved `_pq_.` namespace.
    #[must_use]
    pub fn requires_postgres18_negotiation(self) -> bool {
        match self {
            Self::Startup {
                protocol,
                parameters,
            } => {
                protocol.is_postgres18_supported_major()
                    && (protocol.version_requires_postgres18_negotiation()
                        || parameters.has_postgres18_protocol_option())
            }
            Self::SslRequest | Self::GssEncryptionRequest | Self::CancelRequest { .. } => false,
        }
    }
}

/// Validated startup parameter bytes.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct StartupParameters<'a> {
    bytes: &'a [u8],
}

impl fmt::Debug for StartupParameters<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartupParameters")
            .field("parameter_count", &self.iter().count())
            .finish()
    }
}

impl<'a> StartupParameters<'a> {
    /// Iterates borrowed parameter name/value pairs without allocation.
    #[must_use]
    pub const fn iter(self) -> StartupParameterIter<'a> {
        StartupParameterIter {
            remaining: self.bytes,
        }
    }

    fn has_postgres18_protocol_option(self) -> bool {
        self.iter().any(|(name, _)| name.starts_with(b"_pq_."))
    }
}

/// Iterator over validated startup name/value pairs.
#[derive(Clone)]
pub struct StartupParameterIter<'a> {
    remaining: &'a [u8],
}

impl<'a> Iterator for StartupParameterIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == b"\0" {
            self.remaining = &[];
            return None;
        }
        if self.remaining.is_empty() {
            return None;
        }
        let name_end = self
            .remaining
            .iter()
            .position(|byte| *byte == 0)
            .expect("startup layout was validated");
        let name = &self.remaining[..name_end];
        let after_name = &self.remaining[name_end + 1..];
        let value_end = after_name
            .iter()
            .position(|byte| *byte == 0)
            .expect("startup layout was validated");
        let value = &after_name[..value_end];
        self.remaining = &after_name[value_end + 1..];
        Some((name, value))
    }
}

/// `PostgreSQL` 18 frontend message tag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendTag {
    /// Extended-query bind.
    Bind,
    /// Close a prepared statement or portal.
    Close,
    /// Describe a prepared statement or portal.
    Describe,
    /// Execute a portal.
    Execute,
    /// Legacy function call.
    FunctionCall,
    /// Flush pending output.
    Flush,
    /// Extended-query parse.
    Parse,
    /// Simple query.
    Query,
    /// Extended-query synchronization point.
    Sync,
    /// Terminate the session.
    Terminate,
    /// Authentication response; exact meaning depends on authentication state.
    AuthenticationResponse,
    /// COPY data.
    CopyData,
    /// COPY completion.
    CopyDone,
    /// COPY failure.
    CopyFail,
}

/// Session phase used to reject illegal message types before body buffering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendPhase {
    /// Authentication exchange; only the overloaded `p` response is legal.
    Authentication,
    /// Ordinary post-authentication query protocol, including COPY messages
    /// that `PostgreSQL` accepts and ignores while resynchronizing after a
    /// failed COPY.
    Regular,
    /// Frontend-to-server COPY stream.
    CopyIn,
    /// Physical or logical replication COPY-BOTH stream handled by the
    /// `PostgreSQL` WAL sender.
    ReplicationStreaming,
}

impl FrontendPhase {
    const fn allows(self, tag: FrontendTag) -> bool {
        match self {
            Self::Authentication => matches!(tag, FrontendTag::AuthenticationResponse),
            Self::Regular => matches!(
                tag,
                FrontendTag::Bind
                    | FrontendTag::Close
                    | FrontendTag::Describe
                    | FrontendTag::Execute
                    | FrontendTag::FunctionCall
                    | FrontendTag::Flush
                    | FrontendTag::Parse
                    | FrontendTag::Query
                    | FrontendTag::Sync
                    | FrontendTag::Terminate
                    | FrontendTag::CopyData
                    | FrontendTag::CopyDone
                    | FrontendTag::CopyFail
            ),
            Self::CopyIn => matches!(
                tag,
                FrontendTag::CopyData
                    | FrontendTag::CopyDone
                    | FrontendTag::CopyFail
                    | FrontendTag::Flush
                    | FrontendTag::Sync
            ),
            Self::ReplicationStreaming => matches!(
                tag,
                FrontendTag::CopyData | FrontendTag::CopyDone | FrontendTag::Terminate
            ),
        }
    }
}

impl FrontendTag {
    fn from_byte(value: u8) -> Option<Self> {
        Some(match value {
            b'B' => Self::Bind,
            b'C' => Self::Close,
            b'D' => Self::Describe,
            b'E' => Self::Execute,
            b'F' => Self::FunctionCall,
            b'H' => Self::Flush,
            b'P' => Self::Parse,
            b'Q' => Self::Query,
            b'S' => Self::Sync,
            b'X' => Self::Terminate,
            b'p' => Self::AuthenticationResponse,
            b'd' => Self::CopyData,
            b'c' => Self::CopyDone,
            b'f' => Self::CopyFail,
            _ => return None,
        })
    }

    const fn uses_small_limit(self) -> bool {
        matches!(
            self,
            Self::Close
                | Self::Describe
                | Self::Execute
                | Self::Flush
                | Self::Sync
                | Self::Terminate
                | Self::CopyDone
                | Self::CopyFail
        )
    }
}

/// One decoded post-startup frontend frame.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct FrontendFrame<'a> {
    tag: FrontendTag,
    body: &'a [u8],
}

impl fmt::Debug for FrontendFrame<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FrontendFrame")
            .field("tag", &self.tag)
            .field("body_length", &self.body.len())
            .finish()
    }
}

impl<'a> FrontendFrame<'a> {
    /// Returns the validated frontend message tag.
    #[must_use]
    pub const fn tag(self) -> FrontendTag {
        self.tag
    }

    /// Returns the exact borrowed message body, excluding tag and length.
    #[must_use]
    pub const fn body(self) -> &'a [u8] {
        self.body
    }
}

/// Decodes one startup-phase frame from the beginning of `input`.
///
/// A regular startup packet is framing-valid for any protocol number. The
/// session layer must reject unsupported major versions and negotiate newer
/// protocol-three minor versions. Special request codes require exact lengths.
///
/// # Errors
///
/// Returns an error for impossible or over-limit lengths, malformed startup
/// parameter pairs, invalid SSL, GSS, or cancellation request lengths, or
/// buffered data after a GSS request.
pub fn decode_startup(input: &[u8]) -> Result<Decode<StartupFrame<'_>>, DecodeError> {
    let Some(total_length) = frame_length(input, 0)? else {
        return Ok(Decode::Incomplete { required: 4 });
    };
    if total_length < 8 {
        return Err(DecodeError::InvalidLength {
            actual: total_length,
            minimum: 8,
        });
    }
    if total_length > MAX_STARTUP_FRAME_LENGTH {
        return Err(DecodeError::FrameTooLarge {
            actual: total_length,
            maximum: MAX_STARTUP_FRAME_LENGTH,
        });
    }
    if input.len() < total_length {
        return Ok(Decode::Incomplete {
            required: total_length,
        });
    }

    let code = u32::from_be_bytes([input[4], input[5], input[6], input[7]]);
    let frame = match code {
        NEGOTIATE_SSL_CODE => {
            require_special_length("SSL", total_length, 8)?;
            StartupFrame::SslRequest
        }
        NEGOTIATE_GSS_CODE => {
            require_special_length("GSS", total_length, 8)?;
            reject_buffered_gss_data(input.len(), total_length)?;
            StartupFrame::GssEncryptionRequest
        }
        CANCEL_REQUEST_CODE => {
            let key_length = total_length.saturating_sub(12);
            if !(1..=MAX_CANCEL_KEY_LENGTH).contains(&key_length) {
                return Err(DecodeError::InvalidCancelKeyLength(key_length));
            }
            let backend_pid = u32::from_be_bytes([input[8], input[9], input[10], input[11]]);
            StartupFrame::CancelRequest {
                backend_pid,
                key: &input[12..total_length],
            }
        }
        _ => {
            let parameter_bytes = &input[8..total_length];
            validate_startup_layout(parameter_bytes)?;
            StartupFrame::Startup {
                protocol: ProtocolVersion {
                    major: u16::from_be_bytes([input[4], input[5]]),
                    minor: u16::from_be_bytes([input[6], input[7]]),
                },
                parameters: StartupParameters {
                    bytes: parameter_bytes,
                },
            }
        }
    };
    Ok(Decode::Complete {
        frame,
        consumed: total_length,
    })
}

/// Decodes one post-startup frontend frame from the beginning of `input`.
///
/// Unknown tags are rejected before their length is trusted. The caller must
/// still enforce whether a recognized tag is legal in the current session
/// phase.
///
/// # Errors
///
/// Returns an error for unknown message tags, impossible lengths, or lengths
/// above the applicable PostgreSQL/caller bound.
pub fn decode_frontend(
    input: &[u8],
    phase: FrontendPhase,
    maximum_large_message_length: usize,
) -> Result<Decode<FrontendFrame<'_>>, DecodeError> {
    if !(4..=MAX_LARGE_MESSAGE_LENGTH).contains(&maximum_large_message_length) {
        return Err(DecodeError::InvalidMaximum {
            actual: maximum_large_message_length,
            minimum: 4,
            maximum: MAX_LARGE_MESSAGE_LENGTH,
        });
    }
    let Some(tag_byte) = input.first().copied() else {
        return Ok(Decode::Incomplete { required: 1 });
    };
    let tag = FrontendTag::from_byte(tag_byte).ok_or(DecodeError::UnknownFrontendTag(tag_byte))?;
    if !phase.allows(tag) {
        return Err(DecodeError::UnexpectedTagForPhase { phase, tag });
    }
    if input.len() < 5 {
        return Ok(Decode::Incomplete { required: 5 });
    }
    let total_length =
        usize::try_from(u32::from_be_bytes([input[1], input[2], input[3], input[4]]))
            .map_err(|_| DecodeError::LengthOverflow)?;
    if total_length < 4 {
        return Err(DecodeError::InvalidLength {
            actual: total_length,
            minimum: 4,
        });
    }
    let maximum = match tag {
        _ if tag.uses_small_limit() => SMALL_MESSAGE_LENGTH.min(maximum_large_message_length),
        FrontendTag::AuthenticationResponse => {
            AUTHENTICATION_MESSAGE_LENGTH.min(maximum_large_message_length)
        }
        _ => maximum_large_message_length,
    };
    if total_length > maximum {
        return Err(DecodeError::FrameTooLarge {
            actual: total_length,
            maximum,
        });
    }
    let consumed = total_length
        .checked_add(1)
        .ok_or(DecodeError::LengthOverflow)?;
    if input.len() < consumed {
        return Ok(Decode::Incomplete { required: consumed });
    }
    Ok(Decode::Complete {
        frame: FrontendFrame {
            tag,
            body: &input[5..consumed],
        },
        consumed,
    })
}

fn frame_length(input: &[u8], offset: usize) -> Result<Option<usize>, DecodeError> {
    let Some(bytes) = input.get(offset..offset + 4) else {
        return Ok(None);
    };
    usize::try_from(u32::from_be_bytes(
        bytes.try_into().expect("length slice is four bytes"),
    ))
    .map(Some)
    .map_err(|_| DecodeError::LengthOverflow)
}

fn require_special_length(
    request: &'static str,
    actual: usize,
    expected: usize,
) -> Result<(), DecodeError> {
    if actual == expected {
        Ok(())
    } else {
        Err(DecodeError::InvalidSpecialLength {
            request,
            actual,
            expected,
        })
    }
}

fn reject_buffered_gss_data(buffered: usize, frame_length: usize) -> Result<(), DecodeError> {
    if buffered == frame_length {
        Ok(())
    } else {
        Err(DecodeError::BufferedGssData {
            extra: buffered - frame_length,
        })
    }
}

fn validate_startup_layout(mut bytes: &[u8]) -> Result<(), DecodeError> {
    loop {
        if bytes == b"\0" {
            return Ok(());
        }
        if bytes.is_empty() || bytes[0] == 0 {
            return Err(DecodeError::InvalidStartupLayout);
        }
        let name_end = bytes
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(DecodeError::InvalidStartupLayout)?;
        bytes = &bytes[name_end + 1..];
        let value_end = bytes
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(DecodeError::InvalidStartupLayout)?;
        bytes = &bytes[value_end + 1..];
    }
}

/// `PostgreSQL` frame decoding failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    /// A length is smaller than the protocol header it describes.
    #[error("invalid frame length {actual}; minimum is {minimum}")]
    InvalidLength {
        /// Received length.
        actual: usize,
        /// Smallest legal length.
        minimum: usize,
    },
    /// A frame exceeds its bounded allocation/processing policy.
    #[error("frame length {actual} exceeds maximum {maximum}")]
    FrameTooLarge {
        /// Received length.
        actual: usize,
        /// Applicable maximum.
        maximum: usize,
    },
    /// A caller supplied an unsafe large-message bound.
    #[error("large-message maximum {actual} must be between {minimum} and {maximum}")]
    InvalidMaximum {
        /// Supplied maximum.
        actual: usize,
        /// Smallest configurable maximum.
        minimum: usize,
        /// Hard pooler maximum.
        maximum: usize,
    },
    /// A platform integer cannot represent a wire length plus framing.
    #[error("wire frame length overflows this platform")]
    LengthOverflow,
    /// A fixed-size startup negotiation request had trailing bytes.
    #[error("invalid {request} request length {actual}; expected {expected}")]
    InvalidSpecialLength {
        /// Request family.
        request: &'static str,
        /// Received total length.
        actual: usize,
        /// Required total length.
        expected: usize,
    },
    /// Bytes followed a GSS request before negotiation.
    #[error("GSS request has {extra} buffered bytes after its frame")]
    BufferedGssData {
        /// Bytes received after the fixed request.
        extra: usize,
    },
    /// A `PostgreSQL` 18 cancel key is empty or over 256 bytes.
    #[error("invalid PostgreSQL 18 cancellation key length {0}")]
    InvalidCancelKeyLength(usize),
    /// Startup name/value pairs are missing a value or final terminator.
    #[error("invalid startup packet layout")]
    InvalidStartupLayout,
    /// The byte is not a `PostgreSQL` 18 frontend message tag.
    #[error("unknown PostgreSQL 18 frontend message tag {0}")]
    UnknownFrontendTag(u8),
    /// The byte is not a `PostgreSQL` 18 backend message tag.
    #[error("unknown PostgreSQL 18 backend message tag {0}")]
    UnknownBackendTag(u8),
    /// A known tag is illegal in the current session phase.
    #[error("frontend message {tag:?} is not allowed during {phase:?}")]
    UnexpectedTagForPhase {
        /// Current session phase.
        phase: FrontendPhase,
        /// Rejected known tag.
        tag: FrontendTag,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn startup(code: u32, body: &[u8]) -> Vec<u8> {
        let length = u32::try_from(8 + body.len()).expect("test packet length");
        let mut packet = Vec::with_capacity(length as usize);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(&code.to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    fn frontend(tag: u8, body: &[u8]) -> Vec<u8> {
        let length = u32::try_from(4 + body.len()).expect("test frame length");
        let mut packet = Vec::with_capacity(1 + length as usize);
        packet.push(tag);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    #[test]
    fn decodes_protocol_32_startup_parameters_without_allocation() {
        let packet = startup(protocol_code(3, 2), b"user\0alice\0database\0app\0\0");
        let Decode::Complete { frame, consumed } = decode_startup(&packet).expect("startup") else {
            panic!("complete packet was incomplete");
        };
        assert_eq!(consumed, packet.len());
        let StartupFrame::Startup {
            protocol,
            parameters,
        } = frame
        else {
            panic!("unexpected startup frame");
        };
        assert_eq!(protocol.major(), 3);
        assert_eq!(protocol.minor(), 2);
        assert!(protocol.is_postgres18_supported_major());
        assert!(!protocol.version_requires_postgres18_negotiation());
        assert!(!frame.requires_postgres18_negotiation());
        assert_eq!(
            parameters.iter().collect::<Vec<_>>(),
            vec![
                (b"user".as_slice(), b"alice".as_slice()),
                (b"database".as_slice(), b"app".as_slice())
            ]
        );
    }

    #[test]
    fn marks_newer_protocol_three_minor_for_negotiation() {
        let packet = startup(protocol_code(3, 99), b"\0");
        let Decode::Complete {
            frame: StartupFrame::Startup { protocol, .. },
            ..
        } = decode_startup(&packet).expect("startup")
        else {
            panic!("unexpected decode");
        };
        assert!(protocol.version_requires_postgres18_negotiation());

        for major in [2, 4] {
            let packet = startup(protocol_code(major, 0), b"\0");
            let Decode::Complete {
                frame: StartupFrame::Startup { protocol, .. },
                ..
            } = decode_startup(&packet).expect("framing-valid startup")
            else {
                panic!("unexpected decode");
            };
            assert!(!protocol.is_postgres18_supported_major());
        }
    }

    #[test]
    fn reserved_protocol_options_require_negotiation_at_version_32() {
        let packet = startup(
            protocol_code(3, 2),
            b"user\0alice\0_pq_.future_feature\0enabled\0\0",
        );
        let Decode::Complete { frame, .. } = decode_startup(&packet).expect("startup") else {
            panic!("unexpected decode");
        };
        assert!(frame.requires_postgres18_negotiation());

        let packet = startup(
            protocol_code(3, 2),
            b"user\0alice\0application_name\0test\0\0",
        );
        let Decode::Complete { frame, .. } = decode_startup(&packet).expect("startup") else {
            panic!("unexpected decode");
        };
        assert!(!frame.requires_postgres18_negotiation());

        for major in [2, 4] {
            let packet = startup(
                protocol_code(major, 0),
                b"user\0alice\0_pq_.future_feature\0enabled\0\0",
            );
            let Decode::Complete { frame, .. } = decode_startup(&packet).expect("startup") else {
                panic!("unexpected decode");
            };
            assert!(matches!(
                frame,
                StartupFrame::Startup { protocol, .. }
                    if !protocol.is_postgres18_supported_major()
            ));
            assert!(!frame.requires_postgres18_negotiation());
        }
    }

    #[test]
    fn startup_fragmentation_never_consumes_a_partial_packet() {
        let packet = startup(protocol_code(3, 0), b"user\0alice\0\0");
        for split in 0..packet.len() {
            assert!(matches!(
                decode_startup(&packet[..split]),
                Ok(Decode::Incomplete { .. })
            ));
        }
        assert!(matches!(
            decode_startup(&packet),
            Ok(Decode::Complete { .. })
        ));
    }

    #[test]
    fn special_requests_require_postgres18_lengths() {
        for (code, expected) in [
            (NEGOTIATE_SSL_CODE, StartupFrame::SslRequest),
            (NEGOTIATE_GSS_CODE, StartupFrame::GssEncryptionRequest),
        ] {
            let packet = startup(code, &[]);
            assert_eq!(
                decode_startup(&packet),
                Ok(Decode::Complete {
                    frame: expected,
                    consumed: 8
                })
            );
            assert!(matches!(
                decode_startup(&startup(code, b"x")),
                Err(DecodeError::InvalidSpecialLength { .. })
            ));
        }

        let mut ssl_with_client_hello = startup(NEGOTIATE_SSL_CODE, &[]);
        ssl_with_client_hello.extend_from_slice(b"\x16\x03\x03\0\0");
        assert_eq!(
            decode_startup(&ssl_with_client_hello),
            Ok(Decode::Complete {
                frame: StartupFrame::SslRequest,
                consumed: 8,
            })
        );

        let mut pipelined_gss = startup(NEGOTIATE_GSS_CODE, &[]);
        pipelined_gss.extend_from_slice(&startup(protocol_code(3, 2), b"\0"));
        assert!(matches!(
            decode_startup(&pipelined_gss),
            Err(DecodeError::BufferedGssData { .. })
        ));
    }

    #[test]
    fn cancellation_keys_cover_postgres18_boundaries() {
        for key_length in [1, 4, MAX_CANCEL_KEY_LENGTH] {
            let key = vec![7; key_length];
            let mut body = 42_u32.to_be_bytes().to_vec();
            body.extend_from_slice(&key);
            let packet = startup(CANCEL_REQUEST_CODE, &body);
            let Decode::Complete {
                frame:
                    StartupFrame::CancelRequest {
                        backend_pid,
                        key: decoded,
                    },
                ..
            } = decode_startup(&packet).expect("cancel request")
            else {
                panic!("unexpected cancellation decode");
            };
            assert_eq!(backend_pid, 42);
            assert_eq!(decoded, key);
        }
        for key_length in [0, MAX_CANCEL_KEY_LENGTH + 1] {
            let mut body = 42_u32.to_be_bytes().to_vec();
            body.resize(4 + key_length, 7);
            assert!(matches!(
                decode_startup(&startup(CANCEL_REQUEST_CODE, &body)),
                Err(DecodeError::InvalidCancelKeyLength(actual)) if actual == key_length
            ));
        }
    }

    #[test]
    fn malformed_startup_layouts_fail_closed() {
        for body in [
            b"".as_slice(),
            b"user\0alice".as_slice(),
            b"user\0\0".as_slice(),
            b"\0trailing".as_slice(),
            b"user\0alice\0\0trailing".as_slice(),
        ] {
            assert_eq!(
                decode_startup(&startup(protocol_code(3, 2), body)),
                Err(DecodeError::InvalidStartupLayout),
                "body {body:?}"
            );
        }
    }

    #[test]
    fn startup_length_is_bounded_before_body_arrives() {
        for length in [0_u32, 7] {
            assert!(matches!(
                decode_startup(&length.to_be_bytes()),
                Err(DecodeError::InvalidLength { .. })
            ));
        }
        let too_large = u32::try_from(MAX_STARTUP_FRAME_LENGTH + 1)
            .expect("maximum fits")
            .to_be_bytes();
        assert!(matches!(
            decode_startup(&too_large),
            Err(DecodeError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn decodes_every_postgres18_frontend_tag() {
        for (byte, phase) in [
            (b'B', FrontendPhase::Regular),
            (b'C', FrontendPhase::Regular),
            (b'D', FrontendPhase::Regular),
            (b'E', FrontendPhase::Regular),
            (b'F', FrontendPhase::Regular),
            (b'H', FrontendPhase::Regular),
            (b'P', FrontendPhase::Regular),
            (b'Q', FrontendPhase::Regular),
            (b'S', FrontendPhase::Regular),
            (b'X', FrontendPhase::Regular),
            (b'p', FrontendPhase::Authentication),
            (b'd', FrontendPhase::CopyIn),
            (b'c', FrontendPhase::CopyIn),
            (b'f', FrontendPhase::CopyIn),
        ] {
            let packet = frontend(byte, b"body");
            assert!(matches!(
                decode_frontend(&packet, phase, DEFAULT_LARGE_MESSAGE_LENGTH),
                Ok(Decode::Complete { consumed, .. }) if consumed == packet.len()
            ));
        }
    }

    #[test]
    fn frontend_fragmentation_and_concatenation_preserve_boundaries() {
        let first = frontend(b'Q', b"select 1\0");
        for split in 0..first.len() {
            assert!(matches!(
                decode_frontend(
                    &first[..split],
                    FrontendPhase::Regular,
                    DEFAULT_LARGE_MESSAGE_LENGTH,
                ),
                Ok(Decode::Incomplete { .. })
            ));
        }
        let second = frontend(b'S', &[]);
        let mut input = first.clone();
        input.extend_from_slice(&second);
        let Decode::Complete { consumed, .. } =
            decode_frontend(&input, FrontendPhase::Regular, DEFAULT_LARGE_MESSAGE_LENGTH)
                .expect("first")
        else {
            panic!("first frame incomplete");
        };
        assert_eq!(consumed, first.len());
        assert!(matches!(
            decode_frontend(
                &input[consumed..],
                FrontendPhase::Regular,
                DEFAULT_LARGE_MESSAGE_LENGTH,
            ),
            Ok(Decode::Complete { consumed, .. }) if consumed == second.len()
        ));
    }

    #[test]
    fn regular_phase_accepts_copy_recovery_messages() {
        for tag in *b"dcf" {
            let packet = frontend(tag, b"body");
            assert!(matches!(
                decode_frontend(
                    &packet,
                    FrontendPhase::Regular,
                    DEFAULT_LARGE_MESSAGE_LENGTH,
                ),
                Ok(Decode::Complete { consumed, .. }) if consumed == packet.len()
            ));
        }
    }

    #[test]
    fn replication_phase_matches_postgres18_walsender() {
        for tag in *b"dcX" {
            let packet = frontend(tag, b"body");
            assert!(matches!(
                decode_frontend(
                    &packet,
                    FrontendPhase::ReplicationStreaming,
                    DEFAULT_LARGE_MESSAGE_LENGTH,
                ),
                Ok(Decode::Complete { consumed, .. }) if consumed == packet.len()
            ));
        }
        for tag in *b"fHSQ" {
            assert!(matches!(
                decode_frontend(
                    &[tag],
                    FrontendPhase::ReplicationStreaming,
                    DEFAULT_LARGE_MESSAGE_LENGTH,
                ),
                Err(DecodeError::UnexpectedTagForPhase { .. })
            ));
        }
    }

    #[test]
    fn unknown_tags_and_untrusted_lengths_fail_before_buffering() {
        assert_eq!(
            decode_frontend(
                b"Z\xff\xff\xff\xff",
                FrontendPhase::Regular,
                DEFAULT_LARGE_MESSAGE_LENGTH,
            ),
            Err(DecodeError::UnknownFrontendTag(b'Z'))
        );
        let oversized = u32::try_from(DEFAULT_LARGE_MESSAGE_LENGTH + 1)
            .expect("limit fits")
            .to_be_bytes();
        let mut header = vec![b'Q'];
        header.extend_from_slice(&oversized);
        assert!(matches!(
            decode_frontend(
                &header,
                FrontendPhase::Regular,
                DEFAULT_LARGE_MESSAGE_LENGTH,
            ),
            Err(DecodeError::FrameTooLarge { .. })
        ));

        for invalid in [0_u32, 3] {
            let mut header = vec![b'Q'];
            header.extend_from_slice(&invalid.to_be_bytes());
            assert!(matches!(
                decode_frontend(
                    &header,
                    FrontendPhase::Regular,
                    DEFAULT_LARGE_MESSAGE_LENGTH,
                ),
                Err(DecodeError::InvalidLength { .. })
            ));
        }

        for invalid in [3, MAX_LARGE_MESSAGE_LENGTH + 1] {
            assert!(matches!(
                decode_frontend(&[], FrontendPhase::Regular, invalid),
                Err(DecodeError::InvalidMaximum { actual, .. }) if actual == invalid
            ));
        }
    }

    #[test]
    fn phase_illegal_tags_fail_before_their_lengths_are_trusted() {
        for (tag, phase) in [
            (b'Q', FrontendPhase::Authentication),
            (b'Q', FrontendPhase::CopyIn),
            (b'B', FrontendPhase::CopyIn),
            (b'p', FrontendPhase::Regular),
        ] {
            let mut header = vec![tag];
            header.extend_from_slice(
                &u32::try_from(MAX_LARGE_MESSAGE_LENGTH)
                    .expect("maximum fits")
                    .to_be_bytes(),
            );
            assert!(matches!(
                decode_frontend(&header, phase, DEFAULT_LARGE_MESSAGE_LENGTH),
                Err(DecodeError::UnexpectedTagForPhase { .. })
            ));
            assert!(matches!(
                decode_frontend(&header[..1], phase, DEFAULT_LARGE_MESSAGE_LENGTH),
                Err(DecodeError::UnexpectedTagForPhase { .. })
            ));
        }
    }

    #[test]
    fn small_messages_keep_the_postgres18_limit() {
        let exact = frontend(b'S', &vec![0; SMALL_MESSAGE_LENGTH - 4]);
        assert!(
            decode_frontend(&exact, FrontendPhase::Regular, DEFAULT_LARGE_MESSAGE_LENGTH,).is_ok()
        );
        let oversized = frontend(b'S', &vec![0; SMALL_MESSAGE_LENGTH - 3]);
        assert!(matches!(
            decode_frontend(
                &oversized,
                FrontendPhase::Regular,
                DEFAULT_LARGE_MESSAGE_LENGTH,
            ),
            Err(DecodeError::FrameTooLarge {
                maximum: SMALL_MESSAGE_LENGTH,
                ..
            })
        ));
    }

    #[test]
    fn authentication_messages_keep_the_postgres18_limit() {
        let exact = frontend(
            b'p',
            &vec![0; AUTHENTICATION_MESSAGE_LENGTH.saturating_sub(4)],
        );
        assert!(
            decode_frontend(
                &exact,
                FrontendPhase::Authentication,
                DEFAULT_LARGE_MESSAGE_LENGTH,
            )
            .is_ok()
        );
        let oversized = frontend(
            b'p',
            &vec![0; AUTHENTICATION_MESSAGE_LENGTH.saturating_sub(3)],
        );
        assert!(matches!(
            decode_frontend(
                &oversized,
                FrontendPhase::Authentication,
                DEFAULT_LARGE_MESSAGE_LENGTH,
            ),
            Err(DecodeError::FrameTooLarge {
                maximum: AUTHENTICATION_MESSAGE_LENGTH,
                ..
            })
        ));
    }

    #[test]
    fn debug_output_redacts_startup_cancel_and_frontend_payloads() {
        let startup_packet = startup(
            protocol_code(3, 2),
            b"user\0alice\0options\0do-not-log-this\0\0",
        );
        let Decode::Complete {
            frame: startup_frame,
            ..
        } = decode_startup(&startup_packet).expect("startup")
        else {
            panic!("complete startup frame was incomplete");
        };
        let startup_debug = format!("{startup_frame:?}");
        assert!(!startup_debug.contains("do-not-log-this"));
        assert!(startup_debug.contains("parameter_count"));

        let mut cancel_body = 42_u32.to_be_bytes().to_vec();
        cancel_body.extend_from_slice(b"do-not-log-this");
        let cancel_packet = startup(CANCEL_REQUEST_CODE, &cancel_body);
        let Decode::Complete {
            frame: cancel_frame,
            ..
        } = decode_startup(&cancel_packet).expect("cancel")
        else {
            panic!("complete cancel frame was incomplete");
        };
        let cancel_debug = format!("{cancel_frame:?}");
        assert!(!cancel_debug.contains("do-not-log-this"));
        assert!(cancel_debug.contains("key_length"));

        let frontend_packet = frontend(b'Q', b"do-not-log-this\0");
        let Decode::Complete {
            frame: frontend_frame,
            ..
        } = decode_frontend(
            &frontend_packet,
            FrontendPhase::Regular,
            DEFAULT_LARGE_MESSAGE_LENGTH,
        )
        .expect("frontend")
        else {
            panic!("complete frontend frame was incomplete");
        };
        let frontend_debug = format!("{frontend_frame:?}");
        assert!(!frontend_debug.contains("do-not-log-this"));
        assert!(frontend_debug.contains("body_length"));
    }
}
