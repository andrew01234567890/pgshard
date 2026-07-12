//! Fail-closed parsing foundations for future `PostgreSQL` 18 route analysis.
//!
//! This parser deliberately accepts only a conservative PostgreSQL-dialect
//! subset. A successful parse is not `PostgreSQL` semantic validation and never
//! authorizes routing by itself.

use std::fmt;

use sqlparser::{
    ast::Statement,
    dialect::PostgreSqlDialect,
    parser::{Parser, ParserError},
};
use thiserror::Error;

/// Maximum SQL text accepted by the planner.
///
/// `PostgreSQL`'s wire protocol permits larger frames, but planning an unbounded
/// syntax tree would let one client consume disproportionate pooler memory.
pub const MAX_SQL_BYTES: usize = 1_048_576;

const MAX_RECURSION_DEPTH: usize = 50;

/// Coarse top-level syntax kind.
///
/// This is only a parser result. In particular, [`Self::Query`] does not mean
/// read-only: a `PostgreSQL` query can contain data-modifying CTEs. Future route
/// analysis must inspect and prove the complete tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StatementKind {
    /// A top-level query expression.
    Query,
    /// A top-level `INSERT`.
    Insert,
    /// A top-level `UPDATE`.
    Update,
    /// A top-level `DELETE`.
    Delete,
    /// A top-level `MERGE`.
    Merge,
    /// Any statement not yet admitted to route analysis.
    Other,
}

/// One bounded parsed statement with its SQL-bearing syntax tree kept private.
pub struct ParsedStatement {
    statement: Statement,
}

impl ParsedStatement {
    /// Returns the coarse top-level syntax kind.
    #[must_use]
    pub const fn kind(&self) -> StatementKind {
        match self.statement {
            Statement::Query(_) => StatementKind::Query,
            Statement::Insert(_) => StatementKind::Insert,
            Statement::Update(_) => StatementKind::Update,
            Statement::Delete(_) => StatementKind::Delete,
            Statement::Merge(_) => StatementKind::Merge,
            _ => StatementKind::Other,
        }
    }
}

impl fmt::Debug for ParsedStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParsedStatement")
            .field("kind", &self.kind())
            .finish()
    }
}

/// Parses exactly one bounded PostgreSQL-dialect statement.
///
/// # Errors
///
/// Rejects oversized input, embedded zero bytes, invalid or overly recursive
/// syntax, and zero or multiple statements. Errors intentionally omit SQL and
/// upstream parser messages because those can contain query fragments.
pub fn parse_one(sql: &str) -> Result<ParsedStatement, ParseError> {
    if sql.len() > MAX_SQL_BYTES {
        return Err(ParseError::TooLarge {
            actual_bytes: sql.len(),
            maximum_bytes: MAX_SQL_BYTES,
        });
    }
    if sql.as_bytes().contains(&0) {
        return Err(ParseError::EmbeddedZero);
    }

    let dialect = PostgreSqlDialect {};
    let statements = Parser::new(&dialect)
        .with_recursion_limit(MAX_RECURSION_DEPTH)
        .try_with_sql(sql)
        .map_err(|error| ParseError::from_upstream(&error))?
        .parse_statements()
        .map_err(|error| ParseError::from_upstream(&error))?;

    let actual = statements.len();
    let mut statements = statements.into_iter();
    let Some(statement) = statements.next() else {
        return Err(ParseError::StatementCount { actual });
    };
    if statements.next().is_some() {
        return Err(ParseError::StatementCount { actual });
    }

    Ok(ParsedStatement { statement })
}

/// Fail-closed SQL parsing failure with no query fragments.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ParseError {
    /// The input exceeds the pooler's planner allocation boundary.
    #[error("SQL is {actual_bytes} bytes; planner maximum is {maximum_bytes} bytes")]
    TooLarge {
        /// Actual UTF-8 byte length.
        actual_bytes: usize,
        /// Configured hard maximum.
        maximum_bytes: usize,
    },
    /// `PostgreSQL` protocol strings cannot contain embedded zero bytes.
    #[error("SQL contains an embedded zero byte")]
    EmbeddedZero,
    /// The conservative PostgreSQL-dialect parser rejected the syntax.
    #[error("SQL syntax is not supported")]
    InvalidSyntax,
    /// The syntax tree exceeds the bounded recursion depth.
    #[error("SQL syntax exceeds the planner recursion limit")]
    RecursionLimit,
    /// Planning accepts exactly one statement at a time.
    #[error("expected exactly one SQL statement, received {actual}")]
    StatementCount {
        /// Parsed top-level statement count.
        actual: usize,
    },
}

impl ParseError {
    fn from_upstream(error: &ParserError) -> Self {
        match error {
            ParserError::RecursionLimitExceeded => Self::RecursionLimit,
            ParserError::TokenizerError(_) | ParserError::ParserError(_) => Self::InvalidSyntax,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_only_top_level_syntax() {
        for (sql, expected) in [
            ("select 1", StatementKind::Query),
            ("insert into t values (1)", StatementKind::Insert),
            ("update t set value = 1", StatementKind::Update),
            ("delete from t", StatementKind::Delete),
            (
                "merge into t using s on t.id = s.id when matched then delete",
                StatementKind::Merge,
            ),
            ("begin", StatementKind::Other),
            ("create table t (id bigint)", StatementKind::Other),
        ] {
            assert_eq!(parse_one(sql).expect("supported syntax").kind(), expected);
        }
    }

    #[test]
    fn requires_exactly_one_statement() {
        assert_eq!(
            parse_one("").expect_err("empty input"),
            ParseError::StatementCount { actual: 0 }
        );
        assert_eq!(
            parse_one("select 1; select 2").expect_err("two statements"),
            ParseError::StatementCount { actual: 2 }
        );
    }

    #[test]
    fn bounds_input_before_parsing() {
        let oversized = "x".repeat(MAX_SQL_BYTES + 1);
        assert_eq!(
            parse_one(&oversized).expect_err("oversized SQL"),
            ParseError::TooLarge {
                actual_bytes: MAX_SQL_BYTES + 1,
                maximum_bytes: MAX_SQL_BYTES,
            }
        );
        assert_eq!(
            parse_one("select '\0'").expect_err("embedded zero"),
            ParseError::EmbeddedZero
        );
    }

    #[test]
    fn rejects_excessive_recursion_without_panicking() {
        let nested = format!(
            "select {}1{}",
            "(".repeat(MAX_RECURSION_DEPTH * 4),
            ")".repeat(MAX_RECURSION_DEPTH * 4)
        );
        assert_eq!(
            parse_one(&nested).expect_err("excessive recursion"),
            ParseError::RecursionLimit
        );
    }

    #[test]
    fn debug_and_errors_redact_sql() {
        const SECRET: &str = "never-log-this-literal";
        let parsed = parse_one(&format!("select '{SECRET}'")).expect("query");
        assert!(!format!("{parsed:?}").contains(SECRET));

        let error = parse_one(&format!("select {SECRET} @@@")).expect_err("invalid syntax");
        assert!(!format!("{error:?} {error}").contains(SECRET));
    }

    #[test]
    fn deterministic_malformed_corpus_never_panics() {
        const ALPHABET: &[u8] =
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'\"$(),;:+-*/=<>&| ";
        let mut state = 0x4d59_5df4_d0f3_3173_u64;

        for _ in 0..20_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let length = usize::try_from(state & 127).expect("corpus length");
            let mut sql = String::with_capacity(length);
            for _ in 0..length {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let index = usize::try_from(state % ALPHABET.len() as u64).expect("alphabet index");
                sql.push(char::from(ALPHABET[index]));
            }
            let _ = parse_one(&sql);
        }
    }
}
