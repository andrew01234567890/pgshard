//! Linear validation of `PostgreSQL` 18 startup protocol negotiation.

use std::fmt;

use thiserror::Error;

use crate::{
    BackendKeyData, LATEST_PROTOCOL_MINOR, MIN_BACKEND_CANCEL_KEY_LENGTH, ProtocolNegotiation,
    ProtocolVersion, StartupFrame, StartupParameters,
};

const POSTGRES18_MODERN_CANCEL_KEY_LENGTH: usize = 32;

/// A linear validator for the exact startup packet sent to `PostgreSQL` 18.
///
/// The validator borrows the startup parameters until the first authentication
/// response is received. This lets it compare a negotiation response with the
/// exact reserved options sent on that connection without copying names or
/// values. Pass the validator through [`Self::accept`] for a
/// `NegotiateProtocolVersion` response, then consume the returned validator
/// with [`Self::finish`] before authentication.
pub struct Postgres18StartupNegotiation<'a> {
    requested_protocol: ProtocolVersion,
    parameters: StartupParameters<'a>,
    negotiation_required: bool,
    negotiated: bool,
}

impl fmt::Debug for Postgres18StartupNegotiation<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Postgres18StartupNegotiation")
            .field("requested_protocol", &self.requested_protocol)
            .field("protocol_option_count", &self.protocol_option_count())
            .field("negotiation_required", &self.negotiation_required)
            .field("negotiated", &self.negotiated)
            .finish()
    }
}

impl<'a> Postgres18StartupNegotiation<'a> {
    /// Begins validation for the exact regular startup packet sent upstream.
    ///
    /// This models the server's behavior, including its acceptance of reserved
    /// protocol 3.1. A connection policy must separately reject protocol 3.1
    /// when matching the versions offered by `PostgreSQL` clients.
    ///
    /// # Errors
    ///
    /// Rejects SSL, GSS, and cancellation requests, and protocol major
    /// versions which `PostgreSQL` 18 does not support.
    pub fn begin(startup: StartupFrame<'a>) -> Result<Self, Postgres18StartupError> {
        let negotiation_required = startup.requires_postgres18_negotiation();
        let StartupFrame::Startup {
            protocol,
            parameters,
        } = startup
        else {
            return Err(Postgres18StartupError::ExpectedRegularStartup);
        };
        if !protocol.is_postgres18_supported_major() {
            return Err(Postgres18StartupError::UnsupportedProtocolMajor(
                protocol.major(),
            ));
        }
        Ok(Self {
            requested_protocol: protocol,
            parameters,
            negotiation_required,
            negotiated: false,
        })
    }

    /// Validates `PostgreSQL` 18's single protocol-negotiation response.
    ///
    /// The selected version must equal the lesser of the requested version and
    /// `PostgreSQL` 18's latest protocol. `PostgreSQL` 18 currently implements no
    /// reserved startup options, so its unsupported-option response must name
    /// every `_pq_.` parameter from the request in the same order, including
    /// duplicates.
    ///
    /// This consumes the current validator. A rejected response therefore
    /// cannot be retried or followed by authentication with the same proof.
    ///
    /// # Errors
    ///
    /// Rejects an unwarranted or duplicate response, a selected protocol which
    /// `PostgreSQL` 18 could not have selected, or any option-list mismatch.
    pub fn accept(
        mut self,
        response: &ProtocolNegotiation<'_>,
    ) -> Result<Self, Postgres18StartupError> {
        if self.negotiated {
            return Err(Postgres18StartupError::DuplicateNegotiation);
        }
        if !self.negotiation_required {
            return Err(Postgres18StartupError::UnexpectedNegotiation);
        }

        let expected_protocol = self.expected_selected_protocol();
        let actual_protocol = response.selected_protocol();
        if actual_protocol != expected_protocol {
            return Err(Postgres18StartupError::SelectedProtocolMismatch {
                expected: expected_protocol,
                actual: actual_protocol,
            });
        }

        let mut expected_options = self
            .parameters
            .iter()
            .filter_map(|(name, _)| name.starts_with(b"_pq_.").then_some(name));
        let mut actual_options = response.unsupported_options();
        let mut index = 0;
        loop {
            match (expected_options.next(), actual_options.next()) {
                (Some(expected), Some(actual)) if expected == actual => index += 1,
                (None, None) => break,
                _ => return Err(Postgres18StartupError::ProtocolOptionsMismatch(index)),
            }
        }

        self.negotiated = true;
        Ok(self)
    }

    /// Finishes negotiation before the first authentication response.
    ///
    /// # Errors
    ///
    /// Rejects a startup request for which `PostgreSQL` 18 was required to send a
    /// negotiation response but did not.
    pub fn finish(self) -> Result<Postgres18Protocol, Postgres18StartupError> {
        if self.negotiation_required && !self.negotiated {
            return Err(Postgres18StartupError::MissingNegotiation);
        }
        Ok(Postgres18Protocol {
            version: self.expected_selected_protocol(),
        })
    }

    fn expected_selected_protocol(&self) -> ProtocolVersion {
        self.requested_protocol
            .postgres18_selected_version()
            .expect("startup negotiation accepts only PostgreSQL protocol major three")
    }

    fn protocol_option_count(&self) -> usize {
        self.parameters
            .iter()
            .filter(|(name, _)| name.starts_with(b"_pq_."))
            .count()
    }
}

/// Proof of the effective protocol selected for one `PostgreSQL` 18 connection.
///
/// This token proves only negotiation semantics. The connection owner must
/// still associate it and any backend cancellation key with the exact socket
/// from which they were received.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Postgres18Protocol {
    version: ProtocolVersion,
}

impl Postgres18Protocol {
    /// Returns the effective protocol version for this upstream connection.
    #[must_use]
    pub const fn version(self) -> ProtocolVersion {
        self.version
    }

    /// Returns the cancellation-key length emitted by `PostgreSQL` 18.
    ///
    /// `PostgreSQL` 18 emits its 32-byte key for protocol 3.2 and later, and the
    /// legacy four-byte key for earlier protocol-three minor versions.
    #[must_use]
    pub const fn expected_backend_key_length(self) -> usize {
        if self.version.minor() >= LATEST_PROTOCOL_MINOR {
            POSTGRES18_MODERN_CANCEL_KEY_LENGTH
        } else {
            MIN_BACKEND_CANCEL_KEY_LENGTH
        }
    }

    /// Validates `PostgreSQL` 18's exact key length for the selected protocol.
    ///
    /// The generic backend decoder accepts the protocol-wide four-to-256-byte
    /// range. This stricter check is valid only after `PostgreSQL` 18 negotiation
    /// has produced this proof token.
    ///
    /// # Errors
    ///
    /// Returns an error when the borrowed cancellation key does not have the
    /// exact length `PostgreSQL` 18 emits for this protocol.
    pub fn validate_backend_key_data(
        self,
        data: BackendKeyData<'_>,
    ) -> Result<(), Postgres18BackendKeyError> {
        let expected = self.expected_backend_key_length();
        let actual = data.cancellation_key().len();
        if actual == expected {
            Ok(())
        } else {
            Err(Postgres18BackendKeyError {
                protocol: self.version,
                expected,
                actual,
            })
        }
    }
}

/// `PostgreSQL` 18 startup protocol-negotiation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum Postgres18StartupError {
    /// The validator was given a startup-phase request with another layout.
    #[error("expected a regular PostgreSQL startup packet")]
    ExpectedRegularStartup,
    /// `PostgreSQL` 18 supports only protocol major version three.
    #[error("PostgreSQL 18 does not support frontend protocol major version {0}")]
    UnsupportedProtocolMajor(u16),
    /// The backend negotiated although neither version nor options required it.
    #[error("PostgreSQL 18 sent an unexpected protocol negotiation response")]
    UnexpectedNegotiation,
    /// A second negotiation response was received on the same connection.
    #[error("PostgreSQL 18 sent a duplicate protocol negotiation response")]
    DuplicateNegotiation,
    /// The backend-selected version does not match `PostgreSQL` 18 semantics.
    #[error("PostgreSQL 18 selected protocol {actual:?}; expected {expected:?}")]
    SelectedProtocolMismatch {
        /// Version `PostgreSQL` 18 must select for the request.
        expected: ProtocolVersion,
        /// Version reported by the backend.
        actual: ProtocolVersion,
    },
    /// Unsupported option names differ at this zero-based request position.
    #[error("PostgreSQL 18 unsupported protocol options differ at index {0}")]
    ProtocolOptionsMismatch(usize),
    /// Authentication began before a required negotiation response arrived.
    #[error("PostgreSQL 18 omitted a required protocol negotiation response")]
    MissingNegotiation,
}

/// A `PostgreSQL` 18 `BackendKeyData` key length disagrees with its protocol.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error(
    "PostgreSQL 18 protocol {protocol:?} requires a {expected}-byte cancellation key; received {actual} bytes"
)]
pub struct Postgres18BackendKeyError {
    protocol: ProtocolVersion,
    expected: usize,
    actual: usize,
}

impl Postgres18BackendKeyError {
    /// Returns the negotiated protocol whose key was rejected.
    #[must_use]
    pub const fn protocol(self) -> ProtocolVersion {
        self.protocol
    }

    /// Returns the exact key length required from `PostgreSQL` 18.
    #[must_use]
    pub const fn expected(self) -> usize {
        self.expected
    }

    /// Returns the rejected key length without exposing the key.
    #[must_use]
    pub const fn actual(self) -> usize {
        self.actual
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DEFAULT_LARGE_MESSAGE_LENGTH, Decode, decode_backend, decode_backend_key_data,
        decode_protocol_negotiation, decode_startup,
    };

    fn startup(major: u16, minor: u16, parameters: &[u8]) -> Vec<u8> {
        let body_length = 4 + parameters.len();
        let total_length = u32::try_from(4 + body_length).expect("startup test length");
        let mut packet = Vec::with_capacity(total_length as usize);
        packet.extend_from_slice(&total_length.to_be_bytes());
        packet.extend_from_slice(&major.to_be_bytes());
        packet.extend_from_slice(&minor.to_be_bytes());
        packet.extend_from_slice(parameters);
        packet
    }

    fn startup_frame(input: &[u8]) -> StartupFrame<'_> {
        let Decode::Complete { frame, consumed } = decode_startup(input).expect("startup frame")
        else {
            panic!("complete startup frame was incomplete");
        };
        assert_eq!(consumed, input.len());
        frame
    }

    fn backend(tag: u8, body: &[u8]) -> Vec<u8> {
        let length = u32::try_from(4 + body.len()).expect("backend test length");
        let mut packet = Vec::with_capacity(length as usize + 1);
        packet.push(tag);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    fn negotiation(input: &[u8]) -> ProtocolNegotiation<'_> {
        let Decode::Complete { frame, consumed } =
            decode_backend(input, DEFAULT_LARGE_MESSAGE_LENGTH).expect("backend frame")
        else {
            panic!("complete negotiation frame was incomplete");
        };
        assert_eq!(consumed, input.len());
        decode_protocol_negotiation(frame).expect("protocol negotiation body")
    }

    fn negotiation_packet(major: u16, minor: u16, options: &[&[u8]]) -> Vec<u8> {
        let mut body = major.to_be_bytes().to_vec();
        body.extend_from_slice(&minor.to_be_bytes());
        body.extend_from_slice(
            &i32::try_from(options.len())
                .expect("protocol option count")
                .to_be_bytes(),
        );
        for option in options {
            body.extend_from_slice(option);
            body.push(0);
        }
        backend(b'v', &body)
    }

    fn backend_key(input: &[u8]) -> BackendKeyData<'_> {
        let Decode::Complete { frame, consumed } =
            decode_backend(input, DEFAULT_LARGE_MESSAGE_LENGTH).expect("backend key frame")
        else {
            panic!("complete backend key frame was incomplete");
        };
        assert_eq!(consumed, input.len());
        decode_backend_key_data(frame).expect("backend key body")
    }

    fn backend_key_packet(key_length: usize) -> Vec<u8> {
        let mut body = 7_u32.to_be_bytes().to_vec();
        body.resize(4 + key_length, 0xa5);
        backend(b'K', &body)
    }

    #[test]
    fn native_protocol_versions_finish_without_negotiation() {
        // PostgreSQL 18 itself accepts reserved protocol 3.1 even though libpq
        // never offers it. Client-facing version policy remains a later layer.
        for (minor, expected_key_length) in [(0, 4), (1, 4), (2, 32)] {
            let packet = startup(3, minor, b"user\0postgres\0\0");
            let state = Postgres18StartupNegotiation::begin(startup_frame(&packet))
                .expect("supported startup protocol");
            let rendered = format!("{state:?}");
            assert!(!rendered.contains("postgres"));
            assert!(!rendered.contains("user"));
            let protocol = state.finish().expect("negotiation not required");
            assert_eq!(protocol.version().major(), 3);
            assert_eq!(protocol.version().minor(), minor);
            assert_eq!(protocol.expected_backend_key_length(), expected_key_length);
        }
    }

    #[test]
    fn newer_minor_is_downgraded_exactly_once() {
        let packet = startup(3, 99, b"user\0postgres\0\0");
        let state = Postgres18StartupNegotiation::begin(startup_frame(&packet))
            .expect("newer protocol-three startup");
        assert_eq!(
            Postgres18StartupNegotiation::begin(startup_frame(&packet))
                .expect("newer protocol-three startup")
                .finish(),
            Err(Postgres18StartupError::MissingNegotiation)
        );

        let response_packet = negotiation_packet(3, 2, &[]);
        let response = negotiation(&response_packet);
        let state = state.accept(&response).expect("valid downgrade");
        let duplicate_state = Postgres18StartupNegotiation::begin(startup_frame(&packet))
            .expect("newer protocol-three startup")
            .accept(&response)
            .expect("valid first downgrade");
        assert_eq!(
            duplicate_state
                .accept(&response)
                .expect_err("second response must fail closed"),
            Postgres18StartupError::DuplicateNegotiation
        );
        let protocol = state.finish().expect("completed downgrade");
        assert_eq!(protocol.version().minor(), 2);
    }

    #[test]
    fn reserved_options_match_names_order_and_duplicates_without_values() {
        let packet = startup(
            3,
            2,
            b"user\0postgres\0_pq_.first\0secret-one\0application_name\0private\0_pq_.first\0secret-two\0\0",
        );
        let state =
            Postgres18StartupNegotiation::begin(startup_frame(&packet)).expect("startup options");
        let rendered = format!("{state:?}");
        for private in ["postgres", "first", "secret", "application_name", "private"] {
            assert!(!rendered.contains(private));
        }
        assert!(rendered.contains("protocol_option_count: 2"));

        let response_packet = negotiation_packet(3, 2, &[b"_pq_.first", b"_pq_.first"]);
        let state = state
            .accept(&negotiation(&response_packet))
            .expect("exact unsupported option sequence");
        assert_eq!(state.finish().expect("negotiated").version().minor(), 2);
    }

    #[test]
    fn rejected_negotiation_consumes_the_linear_proof() {
        let packet = startup(3, 99, b"user\0postgres\0_pq_.one\x001\0_pq_.two\x002\0\0");
        let state = || {
            Postgres18StartupNegotiation::begin(startup_frame(&packet)).expect("startup options")
        };

        let wrong_version_packet = negotiation_packet(3, 0, &[b"_pq_.one", b"_pq_.two"]);
        assert_eq!(
            state()
                .accept(&negotiation(&wrong_version_packet))
                .expect_err("wrong selected protocol must fail closed"),
            Postgres18StartupError::SelectedProtocolMismatch {
                expected: ProtocolVersion { major: 3, minor: 2 },
                actual: ProtocolVersion { major: 3, minor: 0 },
            }
        );

        for options in [
            vec![b"_pq_.two".as_slice(), b"_pq_.one".as_slice()],
            vec![b"_pq_.one".as_slice()],
            vec![
                b"_pq_.one".as_slice(),
                b"_pq_.two".as_slice(),
                b"_pq_.extra".as_slice(),
            ],
        ] {
            let response_packet = negotiation_packet(3, 2, &options);
            assert!(matches!(
                state().accept(&negotiation(&response_packet)),
                Err(Postgres18StartupError::ProtocolOptionsMismatch(_))
            ));
        }

        let valid_packet = negotiation_packet(3, 2, &[b"_pq_.one", b"_pq_.two"]);
        state()
            .accept(&negotiation(&valid_packet))
            .expect("valid response")
            .finish()
            .expect("completed negotiation");
    }

    #[test]
    fn unwarranted_special_and_unsupported_startups_fail_closed() {
        let native_packet = startup(3, 2, b"user\0postgres\0\0");
        let native = Postgres18StartupNegotiation::begin(startup_frame(&native_packet))
            .expect("native protocol");
        let response_packet = negotiation_packet(3, 2, &[]);
        assert_eq!(
            native
                .accept(&negotiation(&response_packet))
                .expect_err("native startup must reject negotiation"),
            Postgres18StartupError::UnexpectedNegotiation
        );

        for major in [2, 4] {
            let packet = startup(major, 0, b"\0");
            assert!(matches!(
                Postgres18StartupNegotiation::begin(startup_frame(&packet)),
                Err(Postgres18StartupError::UnsupportedProtocolMajor(actual))
                    if actual == major
            ));
        }

        let mut cancel_packet = 13_u32.to_be_bytes().to_vec();
        cancel_packet.extend_from_slice(&80_877_102_u32.to_be_bytes());
        cancel_packet.extend_from_slice(&7_u32.to_be_bytes());
        cancel_packet.push(0xa5);

        for packet in [
            8_u32
                .to_be_bytes()
                .into_iter()
                .chain(80_877_103_u32.to_be_bytes())
                .collect::<Vec<_>>(),
            8_u32
                .to_be_bytes()
                .into_iter()
                .chain(80_877_104_u32.to_be_bytes())
                .collect::<Vec<_>>(),
            cancel_packet,
        ] {
            assert!(matches!(
                Postgres18StartupNegotiation::begin(startup_frame(&packet)),
                Err(Postgres18StartupError::ExpectedRegularStartup)
            ));
        }
    }

    #[test]
    fn protocol_proof_enforces_postgres18_backend_key_lengths_without_leaking_keys() {
        for (minor, valid_length, invalid_length) in [(0, 4, 32), (1, 4, 32), (2, 32, 4)] {
            let startup_packet = startup(3, minor, b"user\0postgres\0\0");
            let protocol = Postgres18StartupNegotiation::begin(startup_frame(&startup_packet))
                .expect("native protocol")
                .finish()
                .expect("negotiation not required");

            let valid_packet = backend_key_packet(valid_length);
            protocol
                .validate_backend_key_data(backend_key(&valid_packet))
                .expect("PostgreSQL 18 key length");

            let invalid_packet = backend_key_packet(invalid_length);
            let error = protocol
                .validate_backend_key_data(backend_key(&invalid_packet))
                .expect_err("wrong PostgreSQL 18 key length");
            assert_eq!(error.protocol(), protocol.version());
            assert_eq!(error.expected(), valid_length);
            assert_eq!(error.actual(), invalid_length);
            let rendered = format!("{error:?}");
            assert!(!rendered.contains("a5"));
        }
    }
}
