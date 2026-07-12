//! Zero-copy `PostgreSQL` 18 frontend message-body decoding.

use std::fmt;

use thiserror::Error;

use crate::{ClientEncoding, FrontendFrame, FrontendTag};

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
        ParameterTypeIter {
            remaining: self.parameter_type_bytes,
        }
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

impl Iterator for ParameterTypeIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.remaining.get(..4)?;
        self.remaining = &self.remaining[4..];
        Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining.len() / 4;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for ParameterTypeIter<'_> {}

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
    type Item = BindParameter<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.count {
            return None;
        }
        let format_index = match self.format_bytes.len() / 2 {
            0 => None,
            1 => Some(0),
            _ => Some(usize::from(self.index) * 2),
        };
        let format = format_index.map_or(FormatCode::Text, |offset| {
            FormatCode::decode(u16::from_be_bytes([
                self.format_bytes[offset],
                self.format_bytes[offset + 1],
            ]))
            .expect("bind formats were validated")
        });
        let length = i32::from_be_bytes([
            self.value_bytes[0],
            self.value_bytes[1],
            self.value_bytes[2],
            self.value_bytes[3],
        ]);
        self.value_bytes = &self.value_bytes[4..];
        let value = if length == -1 {
            None
        } else {
            let length = usize::try_from(length).expect("bind lengths were validated");
            let value = &self.value_bytes[..length];
            self.value_bytes = &self.value_bytes[length..];
            Some(value)
        };
        self.index += 1;
        Some(BindParameter { format, value })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::from(self.count - self.index);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for BindParameterIter<'_> {}

/// Iterator over validated format codes.
#[derive(Clone)]
pub struct FormatCodeIter<'a> {
    remaining: &'a [u8],
}

impl Iterator for FormatCodeIter<'_> {
    type Item = FormatCode;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.remaining.get(..2)?;
        self.remaining = &self.remaining[2..];
        Some(
            FormatCode::decode(u16::from_be_bytes([bytes[0], bytes[1]]))
                .expect("format codes were validated"),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining.len() / 2;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for FormatCodeIter<'_> {}

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
        let remaining = &self.bytes[self.position..];
        let end = remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(MessageError::MissingTerminator(field))?;
        let value = &remaining[..end];
        self.position += end + 1;
        std::str::from_utf8(value).map_err(|_| MessageError::InvalidUtf8(field))
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, MessageError> {
        let bytes = self.take(2, field)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
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
    /// A zero-terminated field has no terminator in the frame body.
    #[error("{0} is missing its zero terminator")]
    MissingTerminator(&'static str),
    /// A protocol C-string is not valid under pinned `client_encoding=UTF8`.
    #[error("{0} is not valid UTF8")]
    InvalidUtf8(&'static str),
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
        }
    }

    fn push_i16(bytes: &mut Vec<u8>, value: i16) {
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn push_i32(bytes: &mut Vec<u8>, value: i32) {
        bytes.extend_from_slice(&value.to_be_bytes());
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
        assert_eq!(parse.parameter_types().collect::<Vec<_>>(), vec![20, 0]);

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
            bind.parameters().iter().collect::<Vec<_>>(),
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
            bind.result_formats().collect::<Vec<_>>(),
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
            Some(BindParameter {
                format: FormatCode::Binary,
                value: Some(42_i64.to_be_bytes().as_slice()),
            })
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
                .map(BindParameter::format)
                .collect::<Vec<_>>();
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
    }
}
