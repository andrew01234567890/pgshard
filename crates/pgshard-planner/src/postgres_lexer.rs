//! Statement boundaries as `PostgreSQL`'s own backend lexer computes them.
//!
//! The candidate parser's tokenizer is not `PostgreSQL`'s. Where the two
//! disagree about which bytes belong to a string, a comment, a dollar-quoted
//! body or an identifier, only `PostgreSQL` 18's `src/backend/parser/scan.l`
//! decides what a backend would execute, so the single-statement contract is
//! settled here rather than by counting leftover candidate-parser tokens.
//!
//! This models exactly the `scan.l` rules that can hide a `;` — comments,
//! quoted strings, dollar-quoted bodies and quoted identifiers — plus the
//! `{identifier}` rule, which is the only other rule that can absorb a `$` and
//! therefore change where a dollar-quoted body begins (`scan.l:332-335`; note
//! that `ident_cont` contains `$` while `dolq_cont` does not). Everything else
//! is scanned one byte at a time, which finds exactly `PostgreSQL`'s comment
//! starts: `;` is in `{self}` and not in `{op_chars}` (`scan.l:366-367`), and
//! the `{operator}` rule truncates its match at an embedded `/*` or `--`
//! (`scan.l:885-906`), so no multi-character token can straddle either.
//!
//! Two `scan.l` behaviours are deliberately not modelled, because in every
//! input where they change the byte partition `PostgreSQL` raises a lexical
//! error and therefore executes nothing:
//!
//! * The numeric rules (`scan.l:381-424`). Their successful forms absorb only
//!   digits, `.`, `_` and letters, which are inert or identifier bytes here, so
//!   the partition is unchanged; `{real}` absorbs a sign only when a digit
//!   follows, so it can never take the first `-` of a `--` comment. Their
//!   failing forms — `{realfail}`, `{integer_junk}`, `{numeric_junk}`,
//!   `{real_junk}` and `{param_junk}` — all reach a `yyerror`
//!   (`scan.l:1005-1008`, `scan.l:1054-1069`).
//! * `{param}` (`scan.l:402`). It absorbs only `$` and digits, neither of which
//!   opens a hiding region here.
//!
//! Two further `scan.l` behaviours are not modelled because `PostgreSQL` then
//! hides at least as many bytes as this scanner does, which can only make this
//! scanner report more statements and therefore reject more:
//!
//! * `{quotecontinue}` (`scan.l:230`, `scan.l:586`). A continued literal covers
//!   the same bytes as the two adjacent literals scanned separately, except
//!   that the continuation resumes the original state — so an `E'…'` that
//!   continues keeps its backslash escapes and can only extend further.
//! * `standard_conforming_strings = off`, which starts `'…'` in the
//!   backslash-escaping `xe` state instead of `xq` (`scan.l:545-551`). `xe`
//!   accepts every closing quote `xq` does and additionally skips `\'`, so an
//!   ordinary string can only end later, never earlier.
//!
//! Consequently a report of one statement is a fail-closed claim: `PostgreSQL`
//! either agrees, or refuses the whole input before executing any of it.

/// `PostgreSQL`'s lexer would reach end of input inside an unterminated
/// comment, string, dollar-quoted body or quoted identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct UnterminatedLexeme;

/// Counts the statements `PostgreSQL`'s grammar would see in `sql`.
///
/// Empty statements are not counted, matching `stmtmulti`'s acceptance of
/// redundant separators.
pub(crate) fn count_statements(sql: &str) -> Result<usize, UnterminatedLexeme> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut completed = 0;
    let mut has_token = false;

    while let Some(&byte) = bytes.get(index) {
        match byte {
            b';' => {
                completed += usize::from(has_token);
                has_token = false;
                index += 1;
            }
            _ if is_space(byte) => index += 1,
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index = line_comment_end(bytes, index + 2);
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = block_comment_end(bytes, index + 2)?;
            }
            // `{xestart}` is two characters and so outruns the one-character
            // `{identifier}` match for a bare `e`; every other string prefix
            // (`b`, `x`, `n`, `u&`) leaves the closing quote where an ordinary
            // string would have it.
            b'e' | b'E' if bytes.get(index + 1) == Some(&b'\'') => {
                index = quoted_end(bytes, index + 2, b'\'', BackslashEscapes::Yes)?;
                has_token = true;
            }
            b'\'' => {
                index = quoted_end(bytes, index + 1, b'\'', BackslashEscapes::No)?;
                has_token = true;
            }
            b'"' => {
                index = quoted_end(bytes, index + 1, b'"', BackslashEscapes::No)?;
                has_token = true;
            }
            b'$' => {
                index = dollar_end(bytes, index)?;
                has_token = true;
            }
            _ if is_identifier_start(byte) => {
                index = identifier_end(bytes, index + 1);
                has_token = true;
            }
            _ => {
                index += 1;
                has_token = true;
            }
        }
    }

    Ok(completed + usize::from(has_token))
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum BackslashEscapes {
    Yes,
    No,
}

/// `scan.l:208`: `space [ \t\n\r\f\v]`.
const fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
}

/// `scan.l:332`: `ident_start [A-Za-z\200-\377_]`. Flex character classes are
/// byte classes, so every continuation byte of a multi-byte UTF-8 character is
/// an identifier byte.
const fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

/// `scan.l:333`: `ident_cont [A-Za-z\200-\377_0-9\$]`.
const fn is_identifier_continuation(byte: u8) -> bool {
    is_identifier_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

/// `scan.l:285`: `dolq_start [A-Za-z\200-\377_]`. A digit cannot start a tag.
const fn is_tag_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

/// `scan.l:286`: `dolq_cont [A-Za-z\200-\377_0-9]`. `$` is deliberately absent.
const fn is_tag_continuation(byte: u8) -> bool {
    is_tag_start(byte) || byte.is_ascii_digit()
}

/// `scan.l:213`: `comment ("--"{non_newline}*)` with `non_newline [^\n\r]`.
fn line_comment_end(bytes: &[u8], mut index: usize) -> usize {
    while let Some(&byte) = bytes.get(index) {
        if byte == b'\n' || byte == b'\r' {
            return index;
        }
        index += 1;
    }
    index
}

/// `scan.l:447-485`: `/*` nests and `{xcstop} \*+\/` closes one level.
fn block_comment_end(bytes: &[u8], mut index: usize) -> Result<usize, UnterminatedLexeme> {
    let mut depth = 1_usize;
    while let Some(&byte) = bytes.get(index) {
        match (byte, bytes.get(index + 1)) {
            (b'/', Some(&b'*')) => {
                depth += 1;
                index += 2;
            }
            (b'*', Some(&b'/')) => {
                index += 2;
                depth -= 1;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => index += 1,
        }
    }
    Err(UnterminatedLexeme)
}

/// `scan.l:273` and `scan.l:297`: a doubled delimiter is an embedded delimiter,
/// and in the `xe` state a backslash escape covers the character after it
/// (`scan.l:263-267`).
fn quoted_end(
    bytes: &[u8],
    mut index: usize,
    delimiter: u8,
    escapes: BackslashEscapes,
) -> Result<usize, UnterminatedLexeme> {
    while let Some(&byte) = bytes.get(index) {
        if byte == b'\\' && escapes == BackslashEscapes::Yes {
            index += 2;
            continue;
        }
        if byte == delimiter {
            if bytes.get(index + 1) == Some(&delimiter) {
                index += 2;
                continue;
            }
            return Ok(index + 1);
        }
        index += 1;
    }
    Err(UnterminatedLexeme)
}

/// `scan.l:287`: `dolqdelim \$({dolq_start}{dolq_cont}*)?\$`. A `$` that does
/// not open one is `{param}`, `{dolqfailed}` or `{other}`, each of which leaves
/// every following byte exposed (`scan.l:750-756`, `scan.l:994-1004`).
fn dollar_end(bytes: &[u8], index: usize) -> Result<usize, UnterminatedLexeme> {
    let Some(body_start) = dollar_delimiter_end(bytes, index) else {
        return Ok(index + 1);
    };
    let Some(delimiter) = bytes.get(index..body_start) else {
        return Err(UnterminatedLexeme);
    };
    // `scan.l:757-776` puts back the trailing `$` of a non-matching delimiter,
    // and a tag cannot contain `$`, so the body ends at the first occurrence of
    // the exact opening delimiter.
    let mut search = body_start;
    while let Some(candidate) = bytes.get(search..search + delimiter.len()) {
        if candidate == delimiter {
            return Ok(search + delimiter.len());
        }
        search += 1;
    }
    Err(UnterminatedLexeme)
}

fn dollar_delimiter_end(bytes: &[u8], index: usize) -> Option<usize> {
    let mut cursor = index + 1;
    if bytes.get(cursor) == Some(&b'$') {
        return Some(cursor + 1);
    }
    if !bytes.get(cursor).copied().is_some_and(is_tag_start) {
        return None;
    }
    cursor += 1;
    while bytes.get(cursor).copied().is_some_and(is_tag_continuation) {
        cursor += 1;
    }
    (bytes.get(cursor) == Some(&b'$')).then_some(cursor + 1)
}

fn identifier_end(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .copied()
        .is_some_and(is_identifier_continuation)
    {
        index += 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_digit_cannot_start_a_dollar_quote_tag() {
        // PostgreSQL lexes `$1` as {param} and the following `$` as {other},
        // so the `;` separates two statements; the candidate parser instead
        // reads one dollar-quoted string tagged `1` that swallows `; select 2`.
        assert_eq!(count_statements("select $1$; select 2 $1$"), Ok(2));
        assert_eq!(count_statements("select $1$;$1$;$1$"), Ok(3));
        assert_eq!(count_statements("select $1"), Ok(1));
    }

    #[test]
    fn a_high_byte_can_start_a_dollar_quote_tag() {
        // `dolq_start` is a byte class, so both UTF-8 bytes of `§` qualify even
        // though `§` is not alphanumeric.
        assert_eq!(
            count_statements("select $\u{a7}$; select 2 $\u{a7}$"),
            Ok(1)
        );
        assert_eq!(count_statements("select $\u{e9}$;$\u{e9}$"), Ok(1));
        assert_eq!(count_statements("select $tag$;$tag$"), Ok(1));
        assert_eq!(count_statements("select $t1$;$t1$"), Ok(1));
        assert_eq!(count_statements("select $_$;$_$"), Ok(1));
        assert_eq!(count_statements("select $$;$$"), Ok(1));
    }

    #[test]
    fn an_identifier_absorbs_the_dollar_that_would_open_a_body() {
        // `ident_cont` contains `$`, so `x$q$` is one identifier and the `;`
        // that follows is a separator.
        assert_eq!(count_statements("select x$q$; select 2"), Ok(2));
        assert_eq!(count_statements("select $q$; select 2 $q$"), Ok(1));
    }

    #[test]
    fn nonmatching_inner_delimiters_stay_inside_the_body() {
        assert_eq!(count_statements("select $a$ $b$ ; $b$ $a$"), Ok(1));
        assert_eq!(count_statements("select $$ $a$ ; $a$ $$"), Ok(1));
        assert_eq!(
            count_statements("select $a$ unterminated"),
            Err(UnterminatedLexeme)
        );
    }

    #[test]
    fn comments_hide_separators_without_becoming_statements() {
        assert_eq!(count_statements("select 1 -- ; select 2"), Ok(1));
        assert_eq!(count_statements("select 1; /* ; */"), Ok(1));
        assert_eq!(count_statements("select 1 /* /* ; */ ; */ + 1"), Ok(1));
        assert_eq!(count_statements("-- ; \n select 1"), Ok(1));
        assert_eq!(count_statements("select 1 -- ; \n ; select 2"), Ok(2));
        // `non_newline` excludes carriage return as well as newline.
        assert_eq!(count_statements("select 1 -- ; \r ; select 2"), Ok(2));
        assert_eq!(count_statements("select 1 /* ;"), Err(UnterminatedLexeme));
    }

    #[test]
    fn only_escape_strings_let_a_backslash_hide_the_closing_quote() {
        assert_eq!(count_statements("select e'a\\'; select 2 '"), Ok(1));
        assert_eq!(count_statements("select E'a\\'; select 2 '"), Ok(1));
        assert_eq!(count_statements("select 'a\\', 1; select 2"), Ok(2));
        assert_eq!(
            count_statements("select e'a\\', 1; select 2"),
            Err(UnterminatedLexeme)
        );
        // `ae` is a two-character identifier, so the following quote opens an
        // ordinary string rather than an escape string.
        assert_eq!(count_statements("select ae'a\\', 1; select 2"), Ok(2));
        assert_eq!(count_statements("select b'0'';'"), Ok(1));
        assert_eq!(count_statements("select u&'a'';'"), Ok(1));
    }

    #[test]
    fn quoted_identifiers_hide_separators_and_double_their_delimiter() {
        assert_eq!(count_statements("select \"a;b\""), Ok(1));
        assert_eq!(count_statements("select \"a\"\";\"\"b\""), Ok(1));
        assert_eq!(count_statements("select U&\"a;b\""), Ok(1));
        assert_eq!(count_statements("select \"a"), Err(UnterminatedLexeme));
    }

    #[test]
    fn redundant_separators_are_not_statements() {
        assert_eq!(count_statements(""), Ok(0));
        assert_eq!(count_statements("   \n\t "), Ok(0));
        assert_eq!(count_statements(";;;"), Ok(0));
        assert_eq!(count_statements(";;; select 1;;;"), Ok(1));
        assert_eq!(count_statements("-- only a comment"), Ok(0));
        assert_eq!(count_statements("select 1;"), Ok(1));
        assert_eq!(count_statements("select 1; select 2"), Ok(2));
    }

    #[test]
    fn arbitrary_input_terminates() {
        const ALPHABET: &[char] = &[
            '$',
            '\'',
            '"',
            '\\',
            ';',
            '-',
            '/',
            '*',
            'e',
            'E',
            'b',
            'u',
            'U',
            '&',
            'x',
            'n',
            '_',
            '0',
            '1',
            'a',
            ' ',
            '\n',
            '\u{a7}',
            '\u{e9}',
            '\u{10348}',
        ];
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        for _ in 0..50_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let length = usize::try_from(state & 63).expect("length");
            let mut sql = String::with_capacity(length);
            for _ in 0..length {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let index = usize::try_from(state % ALPHABET.len() as u64).expect("index");
                sql.push(ALPHABET[index]);
            }
            let _ = count_statements(&sql);
        }
    }
}
