//! Zero-copy `PostgreSQL` 18 frontend message-body decoding.

use std::fmt;

use thiserror::Error;

use crate::{ClientEncoding, FrontendFrame, FrontendTag, ValidatedIteratorError};

/// First frontend response to an advertised SASL mechanism list.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SaslInitialResponse<'a> {
    mechanism: &'a [u8],
    initial_response: Option<&'a [u8]>,
}

impl<'a> SaslInitialResponse<'a> {
    /// Returns the selected mechanism name as uninterpreted protocol bytes.
    #[must_use]
    pub const fn mechanism(self) -> &'a [u8] {
        self.mechanism
    }

    /// Returns the optional initial client response.
    ///
    /// `None` is `PostgreSQL`'s `-1` sentinel. `Some(&[])` is a present,
    /// zero-length response and remains distinct.
    #[must_use]
    pub const fn initial_response(self) -> Option<&'a [u8]> {
        self.initial_response
    }
}

impl fmt::Debug for SaslInitialResponse<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SaslInitialResponse")
            .field("mechanism_length", &self.mechanism.len())
            .field(
                "initial_response_length",
                &self.initial_response.map(<[u8]>::len),
            )
            .finish()
    }
}

/// Subsequent opaque frontend SASL response bytes.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SaslResponse<'a> {
    data: &'a [u8],
}

impl<'a> SaslResponse<'a> {
    /// Returns the complete borrowed response bytes.
    #[must_use]
    pub const fn data(self) -> &'a [u8] {
        self.data
    }
}

impl fmt::Debug for SaslResponse<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SaslResponse")
            .field("data_length", &self.data.len())
            .finish()
    }
}

/// Simple-query message body.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct QueryMessage<'a> {
    query: &'a str,
}

impl<'a> QueryMessage<'a> {
    /// Returns the UTF-8 query without its wire terminator.
    #[must_use]
    pub const fn query(self) -> &'a str {
        self.query
    }
}

impl fmt::Debug for QueryMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryMessage")
            .field("query_length", &self.query.len())
            .finish()
    }
}

/// The extended-query object selected by a `Describe` or `Close` message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtendedQueryObject {
    /// A prepared statement.
    Statement,
    /// A portal.
    Portal,
}

impl ExtendedQueryObject {
    fn decode(value: u8) -> Result<Self, MessageError> {
        match value {
            b'S' => Ok(Self::Statement),
            b'P' => Ok(Self::Portal),
            _ => Err(MessageError::InvalidExtendedQueryObject(value)),
        }
    }
}

/// Extended-query `Describe` message body.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct DescribeMessage<'a> {
    object: ExtendedQueryObject,
    name: &'a str,
}

impl<'a> DescribeMessage<'a> {
    /// Returns whether the message describes a statement or portal.
    #[must_use]
    pub const fn object(self) -> ExtendedQueryObject {
        self.object
    }

    /// Returns the object name; empty selects the unnamed object.
    #[must_use]
    pub const fn name(self) -> &'a str {
        self.name
    }
}

impl fmt::Debug for DescribeMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribeMessage")
            .field("object", &self.object)
            .field("name_length", &self.name.len())
            .finish()
    }
}

/// Extended-query `Close` message body.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CloseMessage<'a> {
    object: ExtendedQueryObject,
    name: &'a str,
}

impl<'a> CloseMessage<'a> {
    /// Returns whether the message closes a statement or portal.
    #[must_use]
    pub const fn object(self) -> ExtendedQueryObject {
        self.object
    }

    /// Returns the object name; empty selects the unnamed object.
    #[must_use]
    pub const fn name(self) -> &'a str {
        self.name
    }
}

impl fmt::Debug for CloseMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CloseMessage")
            .field("object", &self.object)
            .field("name_length", &self.name.len())
            .finish()
    }
}

/// Extended-query `Parse` message body.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ParseMessage<'a> {
    statement_name: &'a str,
    query: &'a str,
    parameter_type_bytes: &'a [u8],
}

impl<'a> ParseMessage<'a> {
    /// Returns the prepared-statement name; empty denotes the unnamed statement.
    #[must_use]
    pub const fn statement_name(self) -> &'a str {
        self.statement_name
    }

    /// Returns the UTF-8 SQL text without its wire terminator.
    #[must_use]
    pub const fn query(self) -> &'a str {
        self.query
    }

    /// Iterates declared parameter type OIDs in wire order.
    #[must_use]
    pub const fn parameter_types(self) -> ParameterTypeIter<'a> {
        ParameterTypeIter::from_validated_bytes(self.parameter_type_bytes)
    }

    /// Returns the number of declared parameter types.
    #[must_use]
    pub const fn parameter_type_count(self) -> usize {
        self.parameter_type_bytes.len() / 4
    }
}

impl fmt::Debug for ParseMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParseMessage")
            .field("statement_name_length", &self.statement_name.len())
            .field("query_length", &self.query.len())
            .field("parameter_type_count", &self.parameter_type_count())
            .finish()
    }
}

/// Iterator over big-endian parameter type OIDs.
#[derive(Clone)]
pub struct ParameterTypeIter<'a> {
    remaining: &'a [u8],
}

impl<'a> ParameterTypeIter<'a> {
    pub(crate) const fn from_validated_bytes(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    /// Returns the number of validated type OIDs not yet consumed.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.remaining.len() / 4
    }

    /// Whether no validated type OIDs remain.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

impl Iterator for ParameterTypeIter<'_> {
    type Item = Result<u32, ValidatedIteratorError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        let Some(bytes) = self.remaining.get(..4) else {
            self.remaining = &[];
            return Some(Err(ValidatedIteratorError::new("parameter type")));
        };
        let value = u32::from_be_bytes(
            bytes
                .try_into()
                .expect("a checked four-byte slice has array length four"),
        );
        self.remaining = &self.remaining[4..];
        Some(Ok(value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let upper = self.remaining.len() / 4 + usize::from(!self.remaining.len().is_multiple_of(4));
        (0, Some(upper))
    }
}

/// `PostgreSQL` text or binary field format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormatCode {
    /// Text representation.
    Text,
    /// Binary representation.
    Binary,
}

impl FormatCode {
    fn decode(value: u16) -> Result<Self, MessageError> {
        match value {
            0 => Ok(Self::Text),
            1 => Ok(Self::Binary),
            _ => Err(MessageError::InvalidFormatCode(value)),
        }
    }
}

/// Extended-query `Bind` message body.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BindMessage<'a> {
    portal_name: &'a str,
    statement_name: &'a str,
    parameters: BindParameters<'a>,
    result_format_bytes: &'a [u8],
}

impl<'a> BindMessage<'a> {
    /// Returns the portal name; empty denotes the unnamed portal.
    #[must_use]
    pub const fn portal_name(self) -> &'a str {
        self.portal_name
    }

    /// Returns the prepared-statement name; empty denotes the unnamed statement.
    #[must_use]
    pub const fn statement_name(self) -> &'a str {
        self.statement_name
    }

    /// Returns the validated parameter collection.
    #[must_use]
    pub const fn parameters(self) -> BindParameters<'a> {
        self.parameters
    }

    /// Iterates requested result-column format codes.
    #[must_use]
    pub const fn result_formats(self) -> FormatCodeIter<'a> {
        FormatCodeIter {
            remaining: self.result_format_bytes,
        }
    }

    /// Returns the number of requested result format codes.
    #[must_use]
    pub const fn result_format_count(self) -> usize {
        self.result_format_bytes.len() / 2
    }
}

impl fmt::Debug for BindMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BindMessage")
            .field("portal_name_length", &self.portal_name.len())
            .field("statement_name_length", &self.statement_name.len())
            .field("parameter_count", &self.parameters.len())
            .field("result_format_count", &self.result_format_count())
            .finish()
    }
}

/// Validated zero-copy bound parameter collection.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BindParameters<'a> {
    format_bytes: &'a [u8],
    value_bytes: &'a [u8],
    count: u16,
}

impl fmt::Debug for BindParameters<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BindParameters")
            .field("parameter_count", &self.len())
            .finish()
    }
}

impl<'a> BindParameters<'a> {
    /// Returns the number of bound parameters.
    #[must_use]
    pub const fn len(self) -> usize {
        self.count as usize
    }

    /// Whether no parameters were supplied.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.count == 0
    }

    /// Iterates parameters with their effective text/binary format.
    #[must_use]
    pub const fn iter(self) -> BindParameterIter<'a> {
        BindParameterIter {
            format_bytes: self.format_bytes,
            value_bytes: self.value_bytes,
            index: 0,
            count: self.count,
        }
    }
}

/// One bound parameter.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BindParameter<'a> {
    format: FormatCode,
    value: Option<&'a [u8]>,
}

impl fmt::Debug for BindParameter<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BindParameter")
            .field("format", &self.format)
            .field("is_null", &self.value.is_none())
            .field("value_length", &self.value.map(<[u8]>::len))
            .finish()
    }
}

impl<'a> BindParameter<'a> {
    /// Returns the effective wire format.
    #[must_use]
    pub const fn format(self) -> FormatCode {
        self.format
    }

    /// Returns the exact borrowed value, or `None` for SQL NULL.
    #[must_use]
    pub const fn value(self) -> Option<&'a [u8]> {
        self.value
    }
}

/// Iterator over validated bound parameters.
#[derive(Clone)]
pub struct BindParameterIter<'a> {
    format_bytes: &'a [u8],
    value_bytes: &'a [u8],
    index: u16,
    count: u16,
}

impl<'a> Iterator for BindParameterIter<'a> {
    type Item = Result<BindParameter<'a>, ValidatedIteratorError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.format_layout_is_valid() {
            self.index = self.count;
            self.format_bytes = &[];
            self.value_bytes = &[];
            return Some(Err(ValidatedIteratorError::new("Bind parameter")));
        }
        if self.index == self.count {
            if self.value_bytes.is_empty() {
                return None;
            }
            self.value_bytes = &[];
            return Some(Err(ValidatedIteratorError::new("Bind parameter")));
        }
        if self.index > self.count {
            self.index = self.count;
            self.value_bytes = &[];
            return Some(Err(ValidatedIteratorError::new("Bind parameter")));
        }
        match self.next_validated() {
            Ok(parameter) => {
                self.index += 1;
                Some(Ok(parameter))
            }
            Err(error) => {
                self.index = self.count;
                self.value_bytes = &[];
                Some(Err(error))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::from(self.count.saturating_sub(self.index));
        let upper = if self.index == self.count
            && self.value_bytes.is_empty()
            && self.format_layout_is_valid()
        {
            0
        } else {
            remaining.saturating_add(1)
        };
        (0, Some(upper))
    }
}

impl<'a> BindParameterIter<'a> {
    /// Returns the number of validated parameters not yet consumed.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.count.saturating_sub(self.index) as usize
    }

    /// Whether no validated parameters remain.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.index >= self.count
    }

    fn format_layout_is_valid(&self) -> bool {
        let length = self.format_bytes.len();
        let expected = usize::from(self.count) * 2;
        if !(length == 0 || length == 2 || length == expected) {
            return false;
        }
        if self.count == 0 && length == 2 {
            let bytes = [self.format_bytes[0], self.format_bytes[1]];
            return FormatCode::decode(u16::from_be_bytes(bytes)).is_ok();
        }
        true
    }

    fn next_validated(&mut self) -> Result<BindParameter<'a>, ValidatedIteratorError> {
        let invalid = || ValidatedIteratorError::new("Bind parameter");
        let parameter_format_bytes = usize::from(self.count).checked_mul(2).ok_or_else(invalid)?;
        let format_index = match self.format_bytes.len() {
            0 => None,
            2 => Some(0),
            length if length == parameter_format_bytes => {
                Some(usize::from(self.index).checked_mul(2).ok_or_else(invalid)?)
            }
            _ => return Err(invalid()),
        };
        let format = match format_index {
            None => FormatCode::Text,
            Some(offset) => {
                let end = offset.checked_add(2).ok_or_else(invalid)?;
                let bytes: &[u8; 2] = self
                    .format_bytes
                    .get(offset..end)
                    .ok_or_else(invalid)?
                    .try_into()
                    .map_err(|_| invalid())?;
                FormatCode::decode(u16::from_be_bytes(*bytes)).map_err(|_| invalid())?
            }
        };
        let length_bytes: &[u8; 4] = self
            .value_bytes
            .get(..4)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?;
        let remaining = self.value_bytes.get(4..).ok_or_else(invalid)?;
        let length = i32::from_be_bytes(*length_bytes);
        let value = if length == -1 {
            None
        } else {
            let length = usize::try_from(length).map_err(|_| invalid())?;
            let value = remaining.get(..length).ok_or_else(invalid)?;
            Some(value)
        };
        let consumed = if length == -1 {
            0
        } else {
            usize::try_from(length).map_err(|_| invalid())?
        };
        self.value_bytes = remaining.get(consumed..).ok_or_else(invalid)?;
        Ok(BindParameter { format, value })
    }
}

/// Iterator over validated format codes.
#[derive(Clone)]
pub struct FormatCodeIter<'a> {
    remaining: &'a [u8],
}

impl FormatCodeIter<'_> {
    /// Returns the number of validated format codes not yet consumed.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.remaining.len() / 2
    }

    /// Whether no validated format codes remain.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

impl Iterator for FormatCodeIter<'_> {
    type Item = Result<FormatCode, ValidatedIteratorError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        let Some(bytes) = self.remaining.get(..2) else {
            self.remaining = &[];
            return Some(Err(ValidatedIteratorError::new("format code")));
        };
        let bytes: &[u8; 2] = bytes
            .try_into()
            .expect("a checked two-byte slice has array length two");
        let Ok(value) = FormatCode::decode(u16::from_be_bytes(*bytes)) else {
            self.remaining = &[];
            return Some(Err(ValidatedIteratorError::new("format code")));
        };
        self.remaining = &self.remaining[2..];
        Some(Ok(value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let upper = self.remaining.len() / 2 + usize::from(!self.remaining.len().is_multiple_of(2));
        (0, Some(upper))
    }
}

/// Extended-query `Execute` message body.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ExecuteMessage<'a> {
    portal_name: &'a str,
    max_rows: u32,
}

impl<'a> ExecuteMessage<'a> {
    /// Returns the portal name; empty denotes the unnamed portal.
    #[must_use]
    pub const fn portal_name(self) -> &'a str {
        self.portal_name
    }

    /// Returns the exact maximum row count; zero means no limit.
    #[must_use]
    pub const fn max_rows(self) -> u32 {
        self.max_rows
    }
}

impl fmt::Debug for ExecuteMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecuteMessage")
            .field("portal_name_length", &self.portal_name.len())
            .field("max_rows", &self.max_rows)
            .finish()
    }
}

/// Decodes a complete frontend `SASLInitialResponse` body.
///
/// The caller must first frame this message under
/// [`crate::FrontendPhase::ScramAuthentication`] using the proof returned by
/// [`crate::encode_authentication_sasl`], so `PostgreSQL` 18's 1,024-byte
/// SCRAM limit is enforced before buffering the body. Mechanism policy and
/// comparison with the advertised list belong to the authentication state machine.
///
/// # Errors
///
/// Rejects the wrong frame tag, a missing mechanism terminator or response
/// length, a negative length other than `PostgreSQL`'s `-1` sentinel, a response
/// outside the frame, or trailing bytes.
pub fn decode_sasl_initial_response(
    frame: FrontendFrame<'_>,
) -> Result<SaslInitialResponse<'_>, MessageError> {
    require_tag(frame, FrontendTag::AuthenticationResponse)?;
    require_scram_bound(frame)?;
    let mut cursor = Cursor::new(frame.body());
    let mechanism = cursor.cstring_bytes("SASL mechanism")?;
    let response_length = cursor.i32("SASL initial response length")?;
    let initial_response = match response_length {
        -1 => None,
        0.. => {
            let response_length =
                usize::try_from(response_length).map_err(|_| MessageError::LengthOverflow)?;
            Some(cursor.take(response_length, "SASL initial response")?)
        }
        _ => return Err(MessageError::InvalidSaslResponseLength(response_length)),
    };
    cursor.finish()?;
    Ok(SaslInitialResponse {
        mechanism,
        initial_response,
    })
}

/// Borrows the complete body of a subsequent frontend `SASLResponse`.
///
/// The caller must first frame this message under
/// [`crate::FrontendPhase::ScramAuthentication`] with the encoder-issued proof
/// and ensure it follows a valid initial response in the same authentication
/// exchange.
///
/// # Errors
///
/// Rejects a frame with any tag other than the overloaded authentication
/// response tag `p`.
pub fn decode_sasl_response(frame: FrontendFrame<'_>) -> Result<SaslResponse<'_>, MessageError> {
    require_tag(frame, FrontendTag::AuthenticationResponse)?;
    require_scram_bound(frame)?;
    Ok(SaslResponse { data: frame.body() })
}

fn require_scram_bound(frame: FrontendFrame<'_>) -> Result<(), MessageError> {
    if frame.is_scram_bounded() {
        Ok(())
    } else {
        Err(MessageError::ScramPhaseRequired)
    }
}

/// Decodes a complete simple-query body.
///
/// # Errors
///
/// Rejects the wrong frame tag, a missing string terminator, or trailing data.
pub fn decode_query(
    frame: FrontendFrame<'_>,
    _client_encoding: ClientEncoding,
) -> Result<QueryMessage<'_>, MessageError> {
    require_tag(frame, FrontendTag::Query)?;
    let mut cursor = Cursor::new(frame.body());
    let query = cursor.cstring_utf8("query")?;
    cursor.finish()?;
    Ok(QueryMessage { query })
}

/// Decodes a complete extended-query `Describe` body.
///
/// # Errors
///
/// Rejects the wrong tag, a target other than statement `S` or portal `P`, a
/// missing/invalid UTF-8 name, or trailing bytes.
pub fn decode_describe(
    frame: FrontendFrame<'_>,
    _client_encoding: ClientEncoding,
) -> Result<DescribeMessage<'_>, MessageError> {
    require_tag(frame, FrontendTag::Describe)?;
    let (object, name) = decode_extended_query_object(frame.body())?;
    Ok(DescribeMessage { object, name })
}

/// Decodes a complete extended-query `Close` body.
///
/// # Errors
///
/// Rejects the wrong tag, a target other than statement `S` or portal `P`, a
/// missing/invalid UTF-8 name, or trailing bytes.
pub fn decode_close(
    frame: FrontendFrame<'_>,
    _client_encoding: ClientEncoding,
) -> Result<CloseMessage<'_>, MessageError> {
    require_tag(frame, FrontendTag::Close)?;
    let (object, name) = decode_extended_query_object(frame.body())?;
    Ok(CloseMessage { object, name })
}

/// Decodes a complete extended-query `Parse` body.
///
/// # Errors
///
/// Rejects the wrong tag, missing strings/counts/OIDs, arithmetic overflow, or
/// trailing bytes.
pub fn decode_parse(
    frame: FrontendFrame<'_>,
    _client_encoding: ClientEncoding,
) -> Result<ParseMessage<'_>, MessageError> {
    require_tag(frame, FrontendTag::Parse)?;
    let mut cursor = Cursor::new(frame.body());
    let statement_name = cursor.cstring_utf8("statement name")?;
    let query = cursor.cstring_utf8("query")?;
    let count = usize::from(cursor.u16("parameter type count")?);
    let byte_count = count.checked_mul(4).ok_or(MessageError::LengthOverflow)?;
    let parameter_type_bytes = cursor.take(byte_count, "parameter type OIDs")?;
    cursor.finish()?;
    Ok(ParseMessage {
        statement_name,
        query,
        parameter_type_bytes,
    })
}

/// Decodes a complete extended-query `Bind` body.
///
/// # Errors
///
/// Rejects the wrong tag, truncated or negative non-NULL values, unsupported
/// format codes, format/parameter count mismatches, overflow, or trailing data.
pub fn decode_bind(
    frame: FrontendFrame<'_>,
    _client_encoding: ClientEncoding,
) -> Result<BindMessage<'_>, MessageError> {
    require_tag(frame, FrontendTag::Bind)?;
    let mut cursor = Cursor::new(frame.body());
    let portal_name = cursor.cstring_utf8("portal name")?;
    let statement_name = cursor.cstring_utf8("statement name")?;

    let format_count = usize::from(cursor.u16("parameter format count")?);
    let format_byte_count = format_count
        .checked_mul(2)
        .ok_or(MessageError::LengthOverflow)?;
    let format_bytes = cursor.take(format_byte_count, "parameter formats")?;
    validate_formats(format_bytes)?;

    let parameter_count = cursor.u16("parameter count")?;
    if format_count > 1 && format_count != usize::from(parameter_count) {
        return Err(MessageError::ParameterFormatCountMismatch {
            formats: format_count,
            parameters: usize::from(parameter_count),
        });
    }
    let value_start = cursor.position();
    for _ in 0..parameter_count {
        let length = cursor.i32("parameter length")?;
        match length {
            -1 => {}
            0.. => {
                let length = usize::try_from(length).map_err(|_| MessageError::LengthOverflow)?;
                cursor.take(length, "parameter value")?;
            }
            _ => return Err(MessageError::InvalidParameterLength(length)),
        }
    }
    let value_end = cursor.position();
    let value_bytes = &frame.body()[value_start..value_end];

    let result_count = usize::from(cursor.u16("result format count")?);
    let result_byte_count = result_count
        .checked_mul(2)
        .ok_or(MessageError::LengthOverflow)?;
    let result_format_bytes = cursor.take(result_byte_count, "result formats")?;
    validate_formats(result_format_bytes)?;
    cursor.finish()?;

    Ok(BindMessage {
        portal_name,
        statement_name,
        parameters: BindParameters {
            format_bytes,
            value_bytes,
            count: parameter_count,
        },
        result_format_bytes,
    })
}

/// Decodes a complete extended-query `Execute` body.
///
/// # Errors
///
/// Rejects the wrong tag, a missing portal terminator/row count, or trailing
/// bytes.
pub fn decode_execute(
    frame: FrontendFrame<'_>,
    _client_encoding: ClientEncoding,
) -> Result<ExecuteMessage<'_>, MessageError> {
    require_tag(frame, FrontendTag::Execute)?;
    let mut cursor = Cursor::new(frame.body());
    let portal_name = cursor.cstring_utf8("portal name")?;
    let max_rows = cursor.i32("maximum rows")?;
    let max_rows = if max_rows <= 0 {
        0
    } else {
        u32::try_from(max_rows).map_err(|_| MessageError::LengthOverflow)?
    };
    cursor.finish()?;
    Ok(ExecuteMessage {
        portal_name,
        max_rows,
    })
}

/// Requires a frame body to be empty for messages such as Sync or Terminate.
///
/// # Errors
///
/// Returns a trailing-data error when the body is not empty.
pub fn require_empty_body(frame: FrontendFrame<'_>) -> Result<(), MessageError> {
    if frame.body().is_empty() {
        Ok(())
    } else {
        Err(MessageError::TrailingData(frame.body().len()))
    }
}

fn require_tag(frame: FrontendFrame<'_>, expected: FrontendTag) -> Result<(), MessageError> {
    if frame.tag() == expected {
        Ok(())
    } else {
        Err(MessageError::WrongTag {
            expected,
            actual: frame.tag(),
        })
    }
}

fn decode_extended_query_object(body: &[u8]) -> Result<(ExtendedQueryObject, &str), MessageError> {
    let mut cursor = Cursor::new(body);
    let object = ExtendedQueryObject::decode(cursor.byte("extended-query object")?)?;
    let name = cursor.cstring_utf8("extended-query object name")?;
    cursor.finish()?;
    Ok((object, name))
}

fn validate_formats(mut bytes: &[u8]) -> Result<(), MessageError> {
    while let Some(value) = bytes.get(..2) {
        FormatCode::decode(u16::from_be_bytes([value[0], value[1]]))?;
        bytes = &bytes[2..];
    }
    debug_assert!(bytes.is_empty());
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    const fn position(&self) -> usize {
        self.position
    }

    fn cstring_utf8(&mut self, field: &'static str) -> Result<&'a str, MessageError> {
        let value = self.cstring_bytes(field)?;
        std::str::from_utf8(value).map_err(|_| MessageError::InvalidUtf8(field))
    }

    fn cstring_bytes(&mut self, field: &'static str) -> Result<&'a [u8], MessageError> {
        let remaining = self
            .bytes
            .get(self.position..)
            .ok_or(MessageError::Truncated(field))?;
        let end = remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(MessageError::MissingTerminator(field))?;
        let value = &remaining[..end];
        self.position += end + 1;
        Ok(value)
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, MessageError> {
        let bytes = self.take(2, field)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn byte(&mut self, field: &'static str) -> Result<u8, MessageError> {
        Ok(self.take(1, field)?[0])
    }

    fn i32(&mut self, field: &'static str) -> Result<i32, MessageError> {
        let bytes = self.take(4, field)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn take(&mut self, length: usize, field: &'static str) -> Result<&'a [u8], MessageError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(MessageError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(MessageError::Truncated(field))?;
        self.position = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), MessageError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(MessageError::TrailingData(self.bytes.len() - self.position))
        }
    }
}

/// Frontend message-body decoding failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum MessageError {
    /// A typed decoder was called for another frontend tag.
    #[error("expected {expected:?} frontend message, received {actual:?}")]
    WrongTag {
        /// Required tag.
        expected: FrontendTag,
        /// Actual tag.
        actual: FrontendTag,
    },
    /// A SCRAM body decoder received a frame buffered under a broader limit.
    #[error("SASL response was not framed under the bounded SCRAM authentication phase")]
    ScramPhaseRequired,
    /// A zero-terminated field has no terminator in the frame body.
    #[error("{0} is missing its zero terminator")]
    MissingTerminator(&'static str),
    /// A protocol C-string is not valid under pinned `client_encoding=UTF8`.
    #[error("{0} is not valid UTF8")]
    InvalidUtf8(&'static str),
    /// A `Describe` or `Close` target is neither statement `S` nor portal `P`.
    #[error("invalid extended-query object code {0}")]
    InvalidExtendedQueryObject(u8),
    /// A fixed-width or length-prefixed field extends beyond the frame body.
    #[error("{0} is truncated")]
    Truncated(&'static str),
    /// A count-to-byte-length calculation overflowed.
    #[error("message field length overflow")]
    LengthOverflow,
    /// Parameter format cardinality is neither zero, one, nor one per value.
    #[error("bind has {formats} parameter formats for {parameters} parameters")]
    ParameterFormatCountMismatch {
        /// Supplied format count.
        formats: usize,
        /// Supplied value count.
        parameters: usize,
    },
    /// A format code is neither text zero nor binary one.
    #[error("unsupported PostgreSQL format code {0}")]
    InvalidFormatCode(u16),
    /// A parameter length is negative but not the NULL sentinel `-1`.
    #[error("invalid bind parameter length {0}")]
    InvalidParameterLength(i32),
    /// A SASL initial-response length is negative but not the `-1` sentinel.
    #[error("invalid SASL initial response length {0}")]
    InvalidSaslResponseLength(i32),
    /// Valid fields did not consume the exact frame body.
    #[error("message has {0} trailing bytes")]
    TrailingData(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf8() -> ClientEncoding {
        ClientEncoding::require_utf8("UTF8").expect("UTF8")
    }

    fn frame(tag: u8, body: &[u8]) -> FrontendFrame<'_> {
        FrontendFrame {
            tag: FrontendTag::from_byte(tag).expect("test frontend tag"),
            body,
            scram_bounded: false,
        }
    }

    fn scram_frame(tag: u8, body: &[u8]) -> FrontendFrame<'_> {
        FrontendFrame {
            tag: FrontendTag::from_byte(tag).expect("test frontend tag"),
            body,
            scram_bounded: true,
        }
    }

    fn push_i16(bytes: &mut Vec<u8>, value: i16) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn push_i32(bytes: &mut Vec<u8>, value: i32) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    #[test]
    fn sasl_initial_and_followup_responses_are_exact_zero_copy_and_redacted() {
        let initial_bytes = b"n,,n=user,r=nonce";
        let mut body = b"SCRAM-SHA-256\0".to_vec();
        push_i32(
            &mut body,
            i32::try_from(initial_bytes.len()).expect("test response length"),
        );
        body.extend_from_slice(initial_bytes);
        let initial = decode_sasl_initial_response(scram_frame(b'p', &body)).expect("SASL initial");
        assert_eq!(initial.mechanism(), b"SCRAM-SHA-256");
        assert_eq!(initial.initial_response(), Some(initial_bytes.as_slice()));
        assert!(std::ptr::eq(
            initial
                .initial_response()
                .expect("present response")
                .as_ptr(),
            body[body.len() - initial_bytes.len()..].as_ptr(),
        ));

        let rendered = format!("{initial:?}");
        for secret in ["SCRAM", "user", "nonce"] {
            assert!(!rendered.contains(secret));
        }

        let response_bytes = b"c=biws,r=nonce,p=proof";
        let response =
            decode_sasl_response(scram_frame(b'p', response_bytes)).expect("SASL response");
        assert_eq!(response.data(), response_bytes);
        let rendered = format!("{response:?}");
        for secret in ["nonce", "proof"] {
            assert!(!rendered.contains(secret));
        }
    }

    #[test]
    fn sasl_initial_response_preserves_absent_and_empty() {
        for (length, expected) in [(-1, None), (0, Some(b"".as_slice()))] {
            let mut body = b"SCRAM-SHA-256\0".to_vec();
            push_i32(&mut body, length);
            let response = decode_sasl_initial_response(scram_frame(b'p', &body))
                .expect("bounded SASL initial");
            assert_eq!(response.initial_response(), expected);
        }
    }

    #[test]
    fn malformed_sasl_initial_responses_fail_closed() {
        assert_eq!(
            decode_sasl_initial_response(scram_frame(b'p', b"SCRAM-SHA-256")),
            Err(MessageError::MissingTerminator("SASL mechanism"))
        );
        assert_eq!(
            decode_sasl_initial_response(scram_frame(b'p', b"SCRAM-SHA-256\0")),
            Err(MessageError::Truncated("SASL initial response length"))
        );

        let mut invalid_negative = b"SCRAM-SHA-256\0".to_vec();
        push_i32(&mut invalid_negative, -2);
        assert_eq!(
            decode_sasl_initial_response(scram_frame(b'p', &invalid_negative)),
            Err(MessageError::InvalidSaslResponseLength(-2))
        );

        let mut truncated = b"SCRAM-SHA-256\0".to_vec();
        push_i32(&mut truncated, 3);
        truncated.extend_from_slice(b"ab");
        assert_eq!(
            decode_sasl_initial_response(scram_frame(b'p', &truncated)),
            Err(MessageError::Truncated("SASL initial response"))
        );

        for length in [-1, 1] {
            let mut trailing = b"SCRAM-SHA-256\0".to_vec();
            push_i32(&mut trailing, length);
            trailing.extend_from_slice(b"xy");
            assert_eq!(
                decode_sasl_initial_response(scram_frame(b'p', &trailing)),
                Err(MessageError::TrailingData(if length == -1 { 2 } else { 1 }))
            );
        }

        assert!(matches!(
            decode_sasl_initial_response(scram_frame(b'Q', b"")),
            Err(MessageError::WrongTag { .. })
        ));
        assert!(matches!(
            decode_sasl_response(scram_frame(b'Q', b"")),
            Err(MessageError::WrongTag { .. })
        ));
    }

    #[test]
    fn every_truncated_sasl_initial_response_prefix_fails() {
        let mut body = b"SCRAM-SHA-256\0".to_vec();
        push_i32(&mut body, 4);
        body.extend_from_slice(b"n,,,");

        for length in 0..body.len() {
            assert!(
                decode_sasl_initial_response(scram_frame(b'p', &body[..length])).is_err(),
                "accepted truncated prefix of {length} bytes"
            );
        }
        assert!(decode_sasl_initial_response(scram_frame(b'p', &body)).is_ok());
    }

    #[test]
    fn decodes_query_parse_and_execute_exactly() {
        let query = decode_query(frame(b'Q', b"select 1\0"), utf8()).expect("query");
        assert_eq!(query.query(), "select 1");

        let mut parse_body = b"find\0select * from t where id = $1\0".to_vec();
        push_i16(&mut parse_body, 2);
        parse_body.extend_from_slice(&20_u32.to_be_bytes());
        parse_body.extend_from_slice(&0_u32.to_be_bytes());
        let parse = decode_parse(frame(b'P', &parse_body), utf8()).expect("parse");
        assert_eq!(parse.statement_name(), "find");
        assert_eq!(
            parse
                .parameter_types()
                .collect::<Result<Vec<_>, _>>()
                .expect("validated parameter types"),
            vec![20, 0]
        );

        let mut execute_body = b"portal\0".to_vec();
        execute_body.extend_from_slice(&42_u32.to_be_bytes());
        let execute = decode_execute(frame(b'E', &execute_body), utf8()).expect("execute");
        assert_eq!(execute.portal_name(), "portal");
        assert_eq!(execute.max_rows(), 42);

        for maximum_rows in [-1, i32::MIN] {
            let mut negative_execute = b"portal\0".to_vec();
            push_i32(&mut negative_execute, maximum_rows);
            assert_eq!(
                decode_execute(frame(b'E', &negative_execute), utf8())
                    .expect("PostgreSQL normalizes nonpositive limits")
                    .max_rows(),
                0
            );
        }
    }

    #[test]
    fn decodes_statement_and_portal_describe_and_close() {
        let statement = decode_describe(frame(b'D', b"Slookup\0"), utf8()).expect("describe");
        assert_eq!(statement.object(), ExtendedQueryObject::Statement);
        assert_eq!(statement.name(), "lookup");

        let unnamed_portal = decode_describe(frame(b'D', b"P\0"), utf8()).expect("describe");
        assert_eq!(unnamed_portal.object(), ExtendedQueryObject::Portal);
        assert_eq!(unnamed_portal.name(), "");

        let portal = decode_close(frame(b'C', b"Presults\0"), utf8()).expect("close");
        assert_eq!(portal.object(), ExtendedQueryObject::Portal);
        assert_eq!(portal.name(), "results");

        let unnamed_statement = decode_close(frame(b'C', b"S\0"), utf8()).expect("close");
        assert_eq!(unnamed_statement.object(), ExtendedQueryObject::Statement);
        assert_eq!(unnamed_statement.name(), "");
    }

    #[test]
    fn malformed_describe_and_close_bodies_fail_closed() {
        assert_eq!(
            decode_describe(frame(b'D', b""), utf8()),
            Err(MessageError::Truncated("extended-query object"))
        );
        assert_eq!(
            decode_describe(frame(b'D', b"Xname\0"), utf8()),
            Err(MessageError::InvalidExtendedQueryObject(b'X'))
        );
        assert_eq!(
            decode_describe(frame(b'D', b"Sname"), utf8()),
            Err(MessageError::MissingTerminator(
                "extended-query object name"
            ))
        );
        assert_eq!(
            decode_close(frame(b'C', b"S\xff\0"), utf8()),
            Err(MessageError::InvalidUtf8("extended-query object name"))
        );
        assert_eq!(
            decode_close(frame(b'C', b"Sname\0x"), utf8()),
            Err(MessageError::TrailingData(1))
        );
        assert!(matches!(
            decode_close(frame(b'D', b"Sname\0"), utf8()),
            Err(MessageError::WrongTag { .. })
        ));
    }

    #[test]
    fn bind_decodes_formats_null_empty_and_binary_without_copying() {
        let mut body = b"portal\0statement\0".to_vec();
        push_i16(&mut body, 3);
        for format in [0, 1, 1] {
            push_i16(&mut body, format);
        }
        push_i16(&mut body, 3);
        push_i32(&mut body, -1);
        push_i32(&mut body, 0);
        push_i32(&mut body, 3);
        body.extend_from_slice(b"abc");
        push_i16(&mut body, 2);
        push_i16(&mut body, 0);
        push_i16(&mut body, 1);

        let bind = decode_bind(frame(b'B', &body), utf8()).expect("bind");
        assert_eq!(bind.portal_name(), "portal");
        assert_eq!(bind.statement_name(), "statement");
        assert_eq!(
            bind.parameters()
                .iter()
                .collect::<Result<Vec<_>, _>>()
                .expect("validated Bind parameters"),
            vec![
                BindParameter {
                    format: FormatCode::Text,
                    value: None
                },
                BindParameter {
                    format: FormatCode::Binary,
                    value: Some(b""),
                },
                BindParameter {
                    format: FormatCode::Binary,
                    value: Some(b"abc"),
                }
            ]
        );
        assert_eq!(
            bind.result_formats()
                .collect::<Result<Vec<_>, _>>()
                .expect("validated result formats"),
            vec![FormatCode::Text, FormatCode::Binary]
        );
    }

    #[test]
    fn frame_and_bind_decoders_compose_without_copying_values() {
        let mut body = b"\0lookup\0".to_vec();
        push_i16(&mut body, 1);
        push_i16(&mut body, 1);
        push_i16(&mut body, 1);
        push_i32(&mut body, 8);
        body.extend_from_slice(&42_i64.to_be_bytes());
        push_i16(&mut body, 0);

        let length = u32::try_from(4 + body.len()).expect("frame length");
        let mut bytes = vec![b'B'];
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(&body);
        let crate::Decode::Complete { frame, consumed } = crate::decode_frontend(
            &bytes,
            crate::FrontendPhase::Regular,
            crate::DEFAULT_LARGE_MESSAGE_LENGTH,
        )
        .expect("frame") else {
            panic!("complete frame was incomplete");
        };
        let bind = decode_bind(frame, utf8()).expect("bind");
        assert_eq!(consumed, bytes.len());
        assert_eq!(
            bind.parameters().iter().next(),
            Some(Ok(BindParameter {
                format: FormatCode::Binary,
                value: Some(42_i64.to_be_bytes().as_slice()),
            }))
        );
    }

    #[test]
    fn zero_and_single_parameter_formats_follow_postgres_rules() {
        for (formats, expected) in [
            (vec![], vec![FormatCode::Text, FormatCode::Text]),
            (
                vec![FormatCode::Binary],
                vec![FormatCode::Binary, FormatCode::Binary],
            ),
        ] {
            let mut body = b"\0\0".to_vec();
            push_i16(
                &mut body,
                i16::try_from(formats.len()).expect("format count"),
            );
            for format in &formats {
                push_i16(
                    &mut body,
                    match format {
                        FormatCode::Text => 0,
                        FormatCode::Binary => 1,
                    },
                );
            }
            push_i16(&mut body, 2);
            push_i32(&mut body, 1);
            body.push(b'a');
            push_i32(&mut body, 1);
            body.push(b'b');
            push_i16(&mut body, 0);
            let actual = decode_bind(frame(b'B', &body), utf8())
                .expect("bind")
                .parameters()
                .iter()
                .map(|parameter| parameter.map(BindParameter::format))
                .collect::<Result<Vec<_>, _>>()
                .expect("validated Bind parameters");
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn malformed_bind_fields_fail_closed() {
        let mut mismatch = b"\0\0".to_vec();
        push_i16(&mut mismatch, 2);
        push_i16(&mut mismatch, 0);
        push_i16(&mut mismatch, 1);
        push_i16(&mut mismatch, 1);
        assert!(matches!(
            decode_bind(frame(b'B', &mismatch), utf8()),
            Err(MessageError::ParameterFormatCountMismatch { .. })
        ));

        let mut invalid_format = b"\0\0".to_vec();
        push_i16(&mut invalid_format, 1);
        push_i16(&mut invalid_format, 2);
        push_i16(&mut invalid_format, 0);
        push_i16(&mut invalid_format, 0);
        assert_eq!(
            decode_bind(frame(b'B', &invalid_format), utf8()),
            Err(MessageError::InvalidFormatCode(2))
        );

        let mut negative = b"\0\0".to_vec();
        push_i16(&mut negative, 0);
        push_i16(&mut negative, 1);
        push_i32(&mut negative, -2);
        assert_eq!(
            decode_bind(frame(b'B', &negative), utf8()),
            Err(MessageError::InvalidParameterLength(-2))
        );
    }

    #[test]
    fn every_body_truncation_is_rejected_without_panicking() {
        let mut parse = b"name\0select $1\0".to_vec();
        push_i16(&mut parse, 1);
        parse.extend_from_slice(&20_u32.to_be_bytes());
        for split in 0..parse.len() {
            assert!(decode_parse(frame(b'P', &parse[..split]), utf8()).is_err());
        }

        let mut bind = b"portal\0statement\0".to_vec();
        push_i16(&mut bind, 1);
        push_i16(&mut bind, 1);
        push_i16(&mut bind, 1);
        push_i32(&mut bind, 4);
        bind.extend_from_slice(b"data");
        push_i16(&mut bind, 1);
        push_i16(&mut bind, 0);
        for split in 0..bind.len() {
            assert!(decode_bind(frame(b'B', &bind[..split]), utf8()).is_err());
        }
    }

    #[test]
    fn typed_decoders_reject_wrong_tags_and_trailing_bytes() {
        assert!(matches!(
            decode_query(frame(b'P', b"\0\0\0\0"), utf8()),
            Err(MessageError::WrongTag { .. })
        ));
        assert_eq!(
            decode_query(frame(b'Q', b"select 1\0x"), utf8()),
            Err(MessageError::TrailingData(1))
        );
        assert_eq!(
            require_empty_body(frame(b'S', b"x")),
            Err(MessageError::TrailingData(1))
        );
    }

    #[test]
    fn every_query_protocol_cstring_requires_valid_utf8() {
        assert_eq!(
            decode_query(frame(b'Q', b"\xff\0"), utf8()),
            Err(MessageError::InvalidUtf8("query"))
        );

        let mut parse_statement = b"\xff\0select 1\0".to_vec();
        push_i16(&mut parse_statement, 0);
        assert_eq!(
            decode_parse(frame(b'P', &parse_statement), utf8()),
            Err(MessageError::InvalidUtf8("statement name"))
        );
        let mut parse_query = b"name\0\xff\0".to_vec();
        push_i16(&mut parse_query, 0);
        assert_eq!(
            decode_parse(frame(b'P', &parse_query), utf8()),
            Err(MessageError::InvalidUtf8("query"))
        );

        let mut bind_portal = b"\xff\0statement\0".to_vec();
        push_i16(&mut bind_portal, 0);
        push_i16(&mut bind_portal, 0);
        push_i16(&mut bind_portal, 0);
        assert_eq!(
            decode_bind(frame(b'B', &bind_portal), utf8()),
            Err(MessageError::InvalidUtf8("portal name"))
        );
        let mut bind_statement = b"portal\0\xff\0".to_vec();
        push_i16(&mut bind_statement, 0);
        push_i16(&mut bind_statement, 0);
        push_i16(&mut bind_statement, 0);
        assert_eq!(
            decode_bind(frame(b'B', &bind_statement), utf8()),
            Err(MessageError::InvalidUtf8("statement name"))
        );

        let mut execute = b"\xff\0".to_vec();
        push_i32(&mut execute, 0);
        assert_eq!(
            decode_execute(frame(b'E', &execute), utf8()),
            Err(MessageError::InvalidUtf8("portal name"))
        );
    }

    #[test]
    fn debug_output_redacts_sql_names_and_bind_values() {
        let query = decode_query(frame(b'Q', b"do-not-log-this\0"), utf8()).expect("query");
        assert!(!format!("{query:?}").contains("do-not-log-this"));

        let mut parse_body = b"do-not-log-this\0do-not-log-this\0".to_vec();
        push_i16(&mut parse_body, 0);
        let parse = decode_parse(frame(b'P', &parse_body), utf8()).expect("parse");
        assert!(!format!("{parse:?}").contains("do-not-log-this"));

        let mut bind_body = b"do-not-log-this\0do-not-log-this\0".to_vec();
        push_i16(&mut bind_body, 0);
        push_i16(&mut bind_body, 1);
        push_i32(&mut bind_body, 15);
        bind_body.extend_from_slice(b"do-not-log-this");
        push_i16(&mut bind_body, 0);
        let bind = decode_bind(frame(b'B', &bind_body), utf8()).expect("bind");
        let bind_debug = format!("{bind:?} {:?}", bind.parameters().iter().next());
        assert!(!bind_debug.contains("do-not-log-this"));
        assert!(bind_debug.contains("value_length"));

        let describe =
            decode_describe(frame(b'D', b"Sdo-not-log-this\0"), utf8()).expect("describe");
        let close = decode_close(frame(b'C', b"Pdo-not-log-this\0"), utf8()).expect("close");
        assert!(!format!("{describe:?} {close:?}").contains("do-not-log-this"));
    }

    #[test]
    fn validated_iterators_fail_closed_if_internal_state_is_inconsistent() {
        let mut parameter_types = ParameterTypeIter {
            remaining: &[0, 0, 0],
        };
        assert!(matches!(parameter_types.next(), Some(Err(_))));
        assert_eq!(parameter_types.len(), 0);

        let mut formats = FormatCodeIter { remaining: &[0, 2] };
        assert!(matches!(formats.next(), Some(Err(_))));
        assert_eq!(formats.len(), 0);

        for mut parameters in [
            BindParameterIter {
                format_bytes: &[0],
                value_bytes: &[0, 0, 0, 0],
                index: 0,
                count: 1,
            },
            BindParameterIter {
                format_bytes: &[0, 1, 0],
                value_bytes: &[0, 0, 0, 0],
                index: 0,
                count: 1,
            },
            BindParameterIter {
                format_bytes: &[0, 2],
                value_bytes: &[0, 0, 0, 0],
                index: 0,
                count: 1,
            },
            BindParameterIter {
                format_bytes: &[],
                value_bytes: &[0, 0, 0],
                index: 0,
                count: 1,
            },
            BindParameterIter {
                format_bytes: &[],
                value_bytes: &[0],
                index: 0,
                count: 0,
            },
            BindParameterIter {
                format_bytes: &[0],
                value_bytes: &[],
                index: 0,
                count: 0,
            },
            BindParameterIter {
                format_bytes: &[0, 2],
                value_bytes: &[],
                index: 0,
                count: 0,
            },
        ] {
            assert!(matches!(parameters.next(), Some(Err(_))));
            assert_eq!(parameters.len(), 0);
            assert_eq!(parameters.next(), None);
        }

        let mut parameter_with_trailing_bytes = BindParameterIter {
            format_bytes: &[],
            value_bytes: &[0, 0, 0, 0, 1],
            index: 0,
            count: 1,
        };
        let upper = parameter_with_trailing_bytes
            .size_hint()
            .1
            .expect("finite upper bound");
        assert_eq!(upper, 2);
        assert!(matches!(parameter_with_trailing_bytes.next(), Some(Ok(_))));
        assert!(matches!(parameter_with_trailing_bytes.next(), Some(Err(_))));
        assert_eq!(parameter_with_trailing_bytes.next(), None);
    }
}
