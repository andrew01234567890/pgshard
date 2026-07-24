//! Fail-closed compilation of exact catalog materialization inputs.
//!
//! Every fragment a future materializer executes must be provably free of
//! `psql` meta-commands, `psql` variable interpolation, and transaction-control
//! statements outside the declared envelope. A substring check cannot prove
//! that, so this module runs a scanner that assigns each byte the same lexical
//! state `psql`'s `psqlscan.l` would and inspects tokens only in the INITIAL
//! state. It performs no I/O and grants no SQL, serving, routing, or process
//! authority.

use std::str;

use thiserror::Error;

/// One immutable, protocol-safe materialization program.
///
/// Each fragment is a verbatim copy of its digest-verified snapshot: the
/// compiler validates and copies, and never re-renders, normalizes, splits, or
/// concatenates, so the digest guarantees cover the executed bytes.
#[allow(
    dead_code,
    reason = "sealed program for the next dormant catalog materializer stage"
)]
pub(crate) struct CatalogMaterializationProgram {
    pub(crate) migration: Box<str>,
    pub(crate) inventory: Box<str>,
    pub(crate) preflight: Box<str>,
    pub(crate) genesis: Box<str>,
}

/// Compiles four digest-verified snapshots into the one accepted protocol
/// program shape.
pub(crate) fn compile_catalog_materialization_program(
    migration: &[u8],
    inventory: &[u8],
    genesis: &[u8],
    preflight: &[u8],
) -> Result<CatalogMaterializationProgram, CatalogMaterializationProgramError> {
    let migration = scan("migration", migration, Envelope::SelfFramed)?;
    let inventory = scan("inventory", inventory, Envelope::CallerFramed)?;
    let genesis = scan("genesis", genesis, Envelope::CallerFramed)?;
    let preflight = scan("preflight", preflight, Envelope::CallerFramed)?;
    Ok(CatalogMaterializationProgram {
        migration: migration.into(),
        inventory: inventory.into(),
        preflight: preflight.into(),
        genesis: genesis.into(),
    })
}

/// Transaction-framing contract of one input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Envelope {
    /// The input owns its transaction: `BEGIN` is the literal first executable
    /// statement, a bare `COMMIT` the literal last, and no other statement
    /// starts with a transaction-control keyword.
    SelfFramed,
    /// The executor owns the transaction: no statement starts with a
    /// transaction-control keyword.
    CallerFramed,
}

fn scan<'a>(
    name: &'static str,
    bytes: &'a [u8],
    envelope: Envelope,
) -> Result<&'a str, CatalogMaterializationProgramError> {
    let sql = str::from_utf8(bytes).map_err(|_| invalid_shape(name))?;
    if sql.contains(['\0', '\r']) || !sql.ends_with('\n') {
        return Err(invalid_shape(name));
    }
    Scanner::new(name, sql.as_bytes()).run(envelope)?;
    Ok(sql)
}

/// Lexical states mirroring `psqlscan.l`. `psql` only interprets `\` and
/// `:name` in INITIAL, so token inspection is confined to `Initial`; bytes in
/// every other state are inert for `psql` and for this scanner alike.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Initial,
    /// `xq`; also carries `B'…'` and `X'…'` bodies, whose quote handling is
    /// identical.
    OrdinaryString,
    /// `xe`
    EscapeString,
    /// `xus`
    UnicodeString,
    /// `xd`
    QuotedIdentifier,
    /// `xui`
    UnicodeIdentifier,
    /// `xdolq`; the exact opening tag is `input[tag_start..tag_start + tag_len]`
    DollarBody {
        tag_start: usize,
        tag_len: usize,
    },
    /// `xc`; nestable
    BlockComment {
        depth: u32,
    },
    LineComment,
}

/// Single-quote string kinds that share close/continuation handling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StringKind {
    Ordinary,
    Escape,
    Unicode,
}

impl StringKind {
    const fn state(self) -> State {
        match self {
            Self::Ordinary => State::OrdinaryString,
            Self::Escape => State::EscapeString,
            Self::Unicode => State::UnicodeString,
        }
    }
}

struct Scanner<'a> {
    name: &'static str,
    input: &'a [u8],
    pos: usize,
    statements: StatementAccounting,
}

impl<'a> Scanner<'a> {
    const fn new(name: &'static str, input: &'a [u8]) -> Self {
        Self {
            name,
            input,
            pos: 0,
            statements: StatementAccounting::new(),
        }
    }

    fn run(mut self, envelope: Envelope) -> Result<(), CatalogMaterializationProgramError> {
        let mut state = State::Initial;
        while self.pos < self.input.len() {
            state = match state {
                State::Initial => self.step_initial()?,
                State::OrdinaryString => self.step_single_quoted(StringKind::Ordinary)?,
                State::EscapeString => self.step_single_quoted(StringKind::Escape)?,
                State::UnicodeString => self.step_single_quoted(StringKind::Unicode)?,
                State::QuotedIdentifier => self.step_double_quoted(State::QuotedIdentifier),
                State::UnicodeIdentifier => self.step_double_quoted(State::UnicodeIdentifier),
                State::DollarBody { tag_start, tag_len } => {
                    self.step_dollar_body(tag_start, tag_len)
                }
                State::BlockComment { depth } => self.step_block_comment(depth),
                State::LineComment => self.step_line_comment(),
            };
        }
        if state != State::Initial {
            return Err(self.reject());
        }
        self.statements
            .finish(envelope)
            .map_err(|()| invalid_shape(self.name))
    }

    fn reject(&self) -> CatalogMaterializationProgramError {
        invalid_shape(self.name)
    }

    fn peek(&self, offset: usize) -> Option<u8> {
        self.input.get(self.pos + offset).copied()
    }

    fn step_initial(&mut self) -> Result<State, CatalogMaterializationProgramError> {
        let byte = self.input[self.pos];
        match byte {
            b' ' | b'\t' | b'\n' => {
                self.pos += 1;
                Ok(State::Initial)
            }
            b';' => {
                self.pos += 1;
                self.statements.end_statement();
                Ok(State::Initial)
            }
            b'\\' => Err(self.reject()),
            b'-' if self.peek(1) == Some(b'-') => {
                self.pos += 2;
                Ok(State::LineComment)
            }
            b'/' if self.peek(1) == Some(b'*') => {
                self.pos += 2;
                Ok(State::BlockComment { depth: 1 })
            }
            b'\'' => {
                self.pos += 1;
                self.statements.other_token();
                Ok(State::OrdinaryString)
            }
            b'"' => {
                self.pos += 1;
                self.statements.other_token();
                Ok(State::QuotedIdentifier)
            }
            b'$' => Ok(self.classify_dollar()),
            b':' => self.classify_colon(),
            b'0'..=b'9' => {
                self.consume_number();
                self.statements.other_token();
                Ok(State::Initial)
            }
            _ if is_identifier_start(byte) => Ok(self.classify_word()),
            // INITIAL ASCII control characters other than newline and tab are
            // rejected outright: none can appear in a legitimate rendered
            // input, and psql whitespace quirks must not become a smuggling
            // channel.
            0x00..=0x1f | 0x7f => Err(self.reject()),
            _ => {
                self.pos += 1;
                self.statements.other_token();
                Ok(State::Initial)
            }
        }
    }

    /// `psql` recognizes `E'`, `B'`, `X'`, `U&'`, and `U&"` only where flex's
    /// longest-match would not fold the prefix letter into a longer
    /// identifier, and Postgres `ident_cont` includes `$`, so `foo$tag$` is
    /// one identifier and never a dollar-quote opener.
    fn classify_word(&mut self) -> State {
        let byte = self.input[self.pos];
        match byte {
            b'e' | b'E' if self.peek(1) == Some(b'\'') => {
                self.pos += 2;
                self.statements.other_token();
                return State::EscapeString;
            }
            b'b' | b'B' | b'x' | b'X' if self.peek(1) == Some(b'\'') => {
                self.pos += 2;
                self.statements.other_token();
                return State::OrdinaryString;
            }
            b'u' | b'U' if self.peek(1) == Some(b'&') && self.peek(2) == Some(b'\'') => {
                self.pos += 3;
                self.statements.other_token();
                return State::UnicodeString;
            }
            b'u' | b'U' if self.peek(1) == Some(b'&') && self.peek(2) == Some(b'"') => {
                self.pos += 3;
                self.statements.other_token();
                return State::UnicodeIdentifier;
            }
            _ => {}
        }
        let start = self.pos;
        while self.peek(0).is_some_and(is_identifier_continuation) {
            self.pos += 1;
        }
        self.statements.word(&self.input[start..self.pos]);
        State::Initial
    }

    /// Numeric literals absorb trailing identifier characters exactly like
    /// flex's `*_junk` and `realfail` rules, so `1E'…'` is numeric junk `1E`
    /// followed by an ordinary string, never an `E'…'` string.
    fn consume_number(&mut self) {
        self.consume_digits();
        if self.peek(0) == Some(b'.') && self.peek(1).is_some_and(|byte| byte.is_ascii_digit()) {
            self.pos += 1;
            self.consume_digits();
        } else if self.peek(0) == Some(b'.') && self.peek(1) != Some(b'.') {
            self.pos += 1;
        }
        if let Some(b'e' | b'E') = self.peek(0) {
            match (self.peek(1), self.peek(2)) {
                (Some(digit), _) if digit.is_ascii_digit() => {
                    self.pos += 1;
                    self.consume_digits();
                    self.consume_identifier_junk();
                }
                (Some(b'+' | b'-'), Some(digit)) if digit.is_ascii_digit() => {
                    self.pos += 2;
                    self.consume_digits();
                    self.consume_identifier_junk();
                }
                (Some(b'+' | b'-'), _) => {
                    self.pos += 2;
                }
                _ => {
                    self.consume_identifier_junk();
                }
            }
        } else {
            self.consume_identifier_junk();
        }
    }

    fn consume_digits(&mut self) {
        while self.peek(0).is_some_and(|byte| byte.is_ascii_digit()) {
            self.pos += 1;
        }
    }

    fn consume_identifier_junk(&mut self) {
        if self.peek(0).is_some_and(is_identifier_start) {
            while self.peek(0).is_some_and(is_identifier_continuation) {
                self.pos += 1;
            }
        }
    }

    fn classify_dollar(&mut self) -> State {
        let mut end = self.pos + 1;
        if self
            .input
            .get(end)
            .copied()
            .is_some_and(is_dollar_tag_start)
        {
            end += 1;
            while self
                .input
                .get(end)
                .copied()
                .is_some_and(is_dollar_tag_continuation)
            {
                end += 1;
            }
        }
        if self.input.get(end) == Some(&b'$') {
            let tag_start = self.pos + 1;
            let tag_len = end - tag_start;
            self.pos = end + 1;
            self.statements.other_token();
            State::DollarBody { tag_start, tag_len }
        } else {
            self.pos += 1;
            self.statements.other_token();
            State::Initial
        }
    }

    fn classify_colon(&mut self) -> Result<State, CatalogMaterializationProgramError> {
        match self.peek(1) {
            Some(b':' | b'=') => {
                self.pos += 2;
                self.statements.other_token();
                Ok(State::Initial)
            }
            Some(b'\'' | b'"' | b'{') => Err(self.reject()),
            Some(byte) if is_variable_char(byte) => Err(self.reject()),
            _ => {
                self.pos += 1;
                self.statements.other_token();
                Ok(State::Initial)
            }
        }
    }

    fn step_single_quoted(
        &mut self,
        kind: StringKind,
    ) -> Result<State, CatalogMaterializationProgramError> {
        while self.pos < self.input.len() {
            match self.input[self.pos] {
                b'\'' if self.peek(1) == Some(b'\'') => self.pos += 2,
                b'\'' => {
                    self.pos += 1;
                    return Ok(self.string_stop(kind));
                }
                b'\\' => match kind {
                    StringKind::Escape => self.pos += 2,
                    // standard_conforming_strings is not pinned for the legacy
                    // psql flow, so a backslash in an ordinary string makes the
                    // closing quote position ambiguous between the two server
                    // modes. Reject the ambiguity instead of guessing.
                    StringKind::Ordinary => return Err(self.reject()),
                    // Unicode escape processing happens after lexing; a
                    // backslash never moves the closing quote of `U&'…'`.
                    StringKind::Unicode => self.pos += 1,
                },
                _ => self.pos += 1,
            }
        }
        Ok(kind.state())
    }

    /// Mirrors the `xqs` lookahead: horizontal whitespace and line comments
    /// containing at least one newline before another `'` continue the string
    /// in its original state (`state_before_str_stop`), never unconditionally
    /// `xq`.
    fn string_stop(&mut self, kind: StringKind) -> State {
        let mut probe = self.pos;
        let mut saw_newline = false;
        while probe < self.input.len() {
            match self.input[probe] {
                b' ' | b'\t' => probe += 1,
                b'\n' => {
                    saw_newline = true;
                    probe += 1;
                }
                b'-' if self.input.get(probe + 1) == Some(&b'-') => {
                    probe += 2;
                    while probe < self.input.len() && self.input[probe] != b'\n' {
                        probe += 1;
                    }
                }
                b'\'' if saw_newline => {
                    self.pos = probe + 1;
                    return kind.state();
                }
                _ => break,
            }
        }
        State::Initial
    }

    fn step_double_quoted(&mut self, state: State) -> State {
        while self.pos < self.input.len() {
            match self.input[self.pos] {
                b'"' if self.peek(1) == Some(b'"') => self.pos += 2,
                b'"' => {
                    self.pos += 1;
                    return State::Initial;
                }
                _ => self.pos += 1,
            }
        }
        state
    }

    /// Not nestable and not re-entrant: only the exact opening `$tag$` closes
    /// the body; any other `$other$` inside is literal content.
    fn step_dollar_body(&mut self, tag_start: usize, tag_len: usize) -> State {
        let tag = &self.input[tag_start..tag_start + tag_len];
        while self.pos < self.input.len() {
            if self.input[self.pos] == b'$'
                && self.input[self.pos + 1..]
                    .strip_prefix(tag)
                    .is_some_and(|rest| rest.first() == Some(&b'$'))
            {
                self.pos += tag_len + 2;
                return State::Initial;
            }
            self.pos += 1;
        }
        State::DollarBody { tag_start, tag_len }
    }

    fn step_block_comment(&mut self, mut depth: u32) -> State {
        while self.pos < self.input.len() {
            match self.input[self.pos] {
                b'/' if self.peek(1) == Some(b'*') => {
                    self.pos += 2;
                    depth += 1;
                }
                b'*' if self.peek(1) == Some(b'/') => {
                    self.pos += 2;
                    depth -= 1;
                    if depth == 0 {
                        return State::Initial;
                    }
                }
                _ => self.pos += 1,
            }
        }
        State::BlockComment { depth }
    }

    fn step_line_comment(&mut self) -> State {
        while self.pos < self.input.len() {
            let byte = self.input[self.pos];
            self.pos += 1;
            if byte == b'\n' {
                return State::Initial;
            }
        }
        State::LineComment
    }
}

/// Flex's `\200-\377` classes are byte classes: every byte of a multi-byte
/// UTF-8 character is an identifier byte for `psql`, so the same must hold
/// here or `:` followed by a character above U+00FF would slip through.
const fn is_high_byte(byte: u8) -> bool {
    byte >= 0x80
}

const fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || is_high_byte(byte)
}

const fn is_identifier_continuation(byte: u8) -> bool {
    is_identifier_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

const fn is_dollar_tag_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || is_high_byte(byte)
}

const fn is_dollar_tag_continuation(byte: u8) -> bool {
    is_dollar_tag_start(byte) || byte.is_ascii_digit()
}

/// `psql` `variable_char` is `[A-Za-z\200-\377_0-9]`: digits are variable
/// characters, so `:1` is interpolation.
const fn is_variable_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || is_high_byte(byte)
}

/// Statement-leading transaction-control keywords. `SET` is absent on
/// purpose: `SET TRANSACTION` is a settings statement, not transaction
/// control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlKeyword {
    Begin,
    Start,
    Commit,
    End,
    Rollback,
    Abort,
    Savepoint,
    Release,
    Prepare,
}

impl ControlKeyword {
    fn classify(word: &[u8]) -> Option<Self> {
        const CONTROL: [(&[u8], ControlKeyword); 9] = [
            (b"begin", ControlKeyword::Begin),
            (b"start", ControlKeyword::Start),
            (b"commit", ControlKeyword::Commit),
            (b"end", ControlKeyword::End),
            (b"rollback", ControlKeyword::Rollback),
            (b"abort", ControlKeyword::Abort),
            (b"savepoint", ControlKeyword::Savepoint),
            (b"release", ControlKeyword::Release),
            (b"prepare", ControlKeyword::Prepare),
        ];
        CONTROL
            .into_iter()
            .find(|(keyword, _)| word.eq_ignore_ascii_case(keyword))
            .map(|(_, control)| control)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Leading {
    Control(ControlKeyword),
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClosedStatement {
    leading: Leading,
    tokens: u32,
}

/// Envelope accounting over statement-leading INITIAL tokens. Keying on the
/// leading token keeps `BEGIN ATOMIC` function bodies and quoted or
/// dollar-quoted `COMMIT` text inert while still counting every real
/// transaction-control statement.
struct StatementAccounting {
    current_leading: Option<Leading>,
    current_tokens: u32,
    first_leading: Option<Leading>,
    control_statements: u32,
    executable_statements: u32,
    last_closed: Option<ClosedStatement>,
}

impl StatementAccounting {
    const fn new() -> Self {
        Self {
            current_leading: None,
            current_tokens: 0,
            first_leading: None,
            control_statements: 0,
            executable_statements: 0,
            last_closed: None,
        }
    }

    fn word(&mut self, word: &[u8]) {
        if self.current_tokens == 0 {
            self.current_leading = Some(match ControlKeyword::classify(word) {
                Some(control) => Leading::Control(control),
                None => Leading::Other,
            });
        }
        self.current_tokens = self.current_tokens.saturating_add(1);
    }

    fn other_token(&mut self) {
        if self.current_tokens == 0 {
            self.current_leading = Some(Leading::Other);
        }
        self.current_tokens = self.current_tokens.saturating_add(1);
    }

    fn end_statement(&mut self) {
        let Some(leading) = self.current_leading.take() else {
            self.current_tokens = 0;
            return;
        };
        self.executable_statements = self.executable_statements.saturating_add(1);
        if self.first_leading.is_none() {
            self.first_leading = Some(leading);
        }
        if matches!(leading, Leading::Control(_)) {
            self.control_statements = self.control_statements.saturating_add(1);
        }
        self.last_closed = Some(ClosedStatement {
            leading,
            tokens: self.current_tokens,
        });
        self.current_tokens = 0;
    }

    fn finish(&self, envelope: Envelope) -> Result<(), ()> {
        if self.current_tokens != 0 || self.executable_statements == 0 {
            return Err(());
        }
        match envelope {
            Envelope::SelfFramed => {
                let terminal_is_bare_commit = self.last_closed
                    == Some(ClosedStatement {
                        leading: Leading::Control(ControlKeyword::Commit),
                        tokens: 1,
                    });
                if self.first_leading == Some(Leading::Control(ControlKeyword::Begin))
                    && terminal_is_bare_commit
                    && self.control_statements == 2
                {
                    Ok(())
                } else {
                    Err(())
                }
            }
            Envelope::CallerFramed => {
                if self.control_statements == 0 {
                    Ok(())
                } else {
                    Err(())
                }
            }
        }
    }
}

const fn invalid_shape(name: &'static str) -> CatalogMaterializationProgramError {
    CatalogMaterializationProgramError { name }
}

/// Redacted materialization-program validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("catalog-activation {name} input is not an exact supported materialization program")]
pub(crate) struct CatalogMaterializationProgramError {
    name: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIGRATION: &str = "BEGIN;\nSELECT 1;\nCOMMIT;\n";
    const INVENTORY: &str = concat!(
        "SET LOCAL session_replication_role = origin;\n",
        "SELECT pg_catalog.current_setting('pgshard.expected_shard_count')::bigint;\n",
    );
    const GENESIS: &str = concat!(
        "SET LOCAL search_path = pg_catalog;\n",
        "SELECT pgshard_catalog.install_database_genesis('app', ARRAY[0]::bigint[]);\n",
    );
    const PREFLIGHT: &str = concat!(
        "DO $pgshard_database_topology_preflight$\nBEGIN\n",
        "  IF NOT COALESCE(pg_catalog.current_setting(",
        "'pgshard.bootstrap_allow_empty_database_topology', true)::boolean, false) THEN\n",
        "    RAISE EXCEPTION 'mismatch';\n  END IF;\nEND\n",
        "$pgshard_database_topology_preflight$;\n",
    );

    fn compile(
        migration: &str,
        inventory: &str,
        genesis: &str,
        preflight: &str,
    ) -> Result<CatalogMaterializationProgram, CatalogMaterializationProgramError> {
        compile_catalog_materialization_program(
            migration.as_bytes(),
            inventory.as_bytes(),
            genesis.as_bytes(),
            preflight.as_bytes(),
        )
    }

    fn scan_self_framed(sql: &str) -> Result<&str, CatalogMaterializationProgramError> {
        scan("migration", sql.as_bytes(), Envelope::SelfFramed)
    }

    fn scan_caller_framed(sql: &str) -> Result<&str, CatalogMaterializationProgramError> {
        scan("genesis", sql.as_bytes(), Envelope::CallerFramed)
    }

    fn assert_rejects_self_framed(sql: &str) {
        assert_eq!(
            scan_self_framed(sql),
            Err(invalid_shape("migration")),
            "self-framed input was accepted: {sql:?}"
        );
    }

    fn assert_rejects_caller_framed(sql: &str) {
        assert_eq!(
            scan_caller_framed(sql),
            Err(invalid_shape("genesis")),
            "caller-framed input was accepted: {sql:?}"
        );
    }

    fn assert_accepts_caller_framed(sql: &str) {
        assert_eq!(
            scan_caller_framed(sql),
            Ok(sql),
            "caller-framed input was rejected: {sql:?}"
        );
    }

    #[test]
    fn compiles_verbatim_copies_of_all_four_snapshots() {
        let program =
            compile(MIGRATION, INVENTORY, GENESIS, PREFLIGHT).expect("protocol-safe program");
        assert_eq!(&*program.migration, MIGRATION);
        assert_eq!(&*program.inventory, INVENTORY);
        assert_eq!(&*program.genesis, GENESIS);
        assert_eq!(&*program.preflight, PREFLIGHT);
    }

    #[test]
    fn compiles_the_image_baked_inputs_and_committed_renderer_goldens() {
        let migration: &[u8] =
            include_bytes!("../../pgshard-catalog/migrations/0001_shardschema.sql");
        let inventory: &[u8] =
            include_bytes!("../../pgshard-catalog/inventory/0001_shard_inventory.sql");
        let genesis =
            include_str!("../../pgshard-catalog/testdata/materialization/genesis.golden.sql");
        let preflight =
            include_str!("../../pgshard-catalog/testdata/materialization/preflight.golden.sql");
        let program = compile_catalog_materialization_program(
            migration,
            inventory,
            genesis.as_bytes(),
            preflight.as_bytes(),
        )
        .expect("committed inputs compile");

        assert_eq!(program.migration.as_bytes(), migration);
        assert_eq!(program.inventory.as_bytes(), inventory);
        assert_eq!(&*program.genesis, genesis);
        assert_eq!(&*program.preflight, preflight);
        assert!(program.migration.contains("CREATE SCHEMA"));
        assert!(
            program
                .inventory
                .contains("current_setting('pgshard.expected_shard_count')")
        );
        assert!(program.genesis.contains("install_database_genesis"));
        assert!(
            program
                .preflight
                .contains("pgshard.bootstrap_allow_empty_database_topology")
        );
    }

    #[test]
    fn rejects_encoding_and_framing_violations() {
        for invalid in [
            &b"\xffSELECT 1;\n"[..],
            b"SELECT\x001;\n",
            b"SELECT 1;\r\n",
            b"SELECT 1;",
        ] {
            assert_eq!(
                scan("genesis", invalid, Envelope::CallerFramed),
                Err(invalid_shape("genesis"))
            );
        }
    }

    #[test]
    fn rejects_every_initial_backslash() {
        for invalid in [
            "\\echo pwned\n",
            "SELECT 1; \\echo pwned\n",
            "SELECT 1;\\gexec\n",
            "SELECT 'closed';\\gexec\n",
            "SELECT 1 \\g\n",
            "\\i /etc/passwd\n",
            "SELECT 1;\u{2028}\\echo pwned\n",
            "SELECT foo$x$ \\echo pwned $x$;\n",
        ] {
            assert_rejects_caller_framed(invalid);
        }
    }

    #[test]
    fn rejects_every_initial_interpolation_form() {
        for invalid in [
            "SELECT :'variable';\n",
            "SELECT :\"variable\";\n",
            "SELECT :{?variable};\n",
            "SELECT :variable;\n",
            "SELECT :1;\n",
            "SELECT :007;\n",
            "SELECT :_v;\n",
            "SELECT :\u{100}name;\n",
            "SELECT 1:::text;\n",
        ] {
            assert_rejects_caller_framed(invalid);
        }
    }

    #[test]
    fn allows_typecasts_and_named_argument_assignment() {
        assert_accepts_caller_framed("SELECT 1::bigint;\n");
        assert_accepts_caller_framed("SELECT some_function(argument := 1);\n");
        assert_accepts_caller_framed("SELECT ARRAY[1, 2]::bigint[];\n");
    }

    #[test]
    fn rejects_initial_control_characters_other_than_newline_and_tab() {
        for invalid in ["SELECT\x0b1;\n", "SELECT\x0c1;\n", "SELECT\x1b1;\n"] {
            assert_rejects_caller_framed(invalid);
        }
        assert_accepts_caller_framed("SELECT\t1;\n");
    }

    #[test]
    fn rejects_every_state_left_open_at_end_of_input() {
        for invalid in [
            "SELECT 'open\n",
            "SELECT E'open\n",
            "SELECT E'open\\'\n",
            "SELECT U&'open\n",
            "SELECT \"open\n",
            "SELECT U&\"open\n",
            "SELECT $tag$ open $other$\n",
            "SELECT $$ open\n",
            "/* open /* nested */\n",
            "SELECT B'01\n",
        ] {
            assert_rejects_caller_framed(invalid);
        }
    }

    #[test]
    fn keeps_non_initial_bytes_inert() {
        assert_accepts_caller_framed("SELECT $body$ \\echo :'var' COMMIT; $body$;\n");
        assert_accepts_caller_framed("SELECT 'COMMIT; ROLLBACK;';\n");
        assert_accepts_caller_framed("SELECT ':name and :{?flag}';\n");
        assert_accepts_caller_framed("-- comment mentioning \\echo and :name\nSELECT 1;\n");
        assert_accepts_caller_framed("/* \\i /etc/passwd and :'var' */\nSELECT 1;\n");
        assert_accepts_caller_framed("SELECT \"quoted :name identifier\";\n");
        assert_accepts_caller_framed("SELECT U&'d\\0061t\\0061';\n");
        assert_accepts_caller_framed("SELECT E'\\\\ \\' :name';\n");
    }

    #[test]
    fn dollar_quoting_matches_psql_exactly() {
        assert_accepts_caller_framed("SELECT $$ \\echo hidden $$;\n");
        assert_accepts_caller_framed("SELECT $tag$ inner $other$ text $tag$;\n");
        assert_accepts_caller_framed("SELECT $\u{100}tag$ \\echo hidden $\u{100}tag$;\n");
        assert_accepts_caller_framed("SELECT $1 FROM x WHERE y = $2;\n");
        assert_rejects_caller_framed("SELECT $tag$ open $tagg$ \\echo pwned\n");
        assert_rejects_caller_framed("SELECT foo$tag$ \\echo pwned $tag$;\n");
        assert_rejects_caller_framed("SELECT 1a$tag$ \\echo pwned $tag$;\n");
    }

    #[test]
    fn prefixed_strings_only_open_at_a_token_boundary() {
        assert_accepts_caller_framed("SELECT E'ok';\n");
        assert_accepts_caller_framed("SELECT B'0101', X'ff', U&'ok';\n");
        assert_accepts_caller_framed("SELECT 1E5, 1e+5, 1.5e-2;\n");
        assert_rejects_caller_framed("SELECT 1E'\\' ; \\echo pwned ; ';\n");
        assert_rejects_caller_framed("SELECT junk1e'\\' ; \\echo pwned ; ';\n");
        assert_rejects_caller_framed("SELECT 1x'\\' ; \\echo pwned ; ';\n");
    }

    #[test]
    fn string_continuation_restores_the_original_string_state() {
        assert_accepts_caller_framed("SELECT E'a'\n'\\'b';\n");
        assert_accepts_caller_framed("SELECT E'a' -- note\n'\\'b';\n");
        assert_accepts_caller_framed("SELECT E'a'\n\t \n'\\'b';\n");
        assert_rejects_caller_framed("SELECT E'a' '\\'b';\n");
        assert_rejects_caller_framed("SELECT E'a' /* block */\n'\\'b';\n");
        assert_accepts_caller_framed("SELECT 'a'\n'b';\n");
        assert_accepts_caller_framed("SELECT 'a' 'b';\n");
    }

    #[test]
    fn rejects_the_ambiguous_backslash_inside_ordinary_strings() {
        assert_rejects_caller_framed("SELECT 'a\\'; \\echo pwned --';\n");
        assert_rejects_caller_framed("SELECT 'a\\\\b';\n");
        assert_rejects_caller_framed("SELECT B'\\'';\n");
        assert_accepts_caller_framed("SELECT E'a\\\\b';\n");
        assert_accepts_caller_framed("SELECT 'doubled '' quote';\n");
    }

    #[test]
    fn self_framed_envelope_requires_the_exact_migration_shape() {
        assert_eq!(
            scan_self_framed("BEGIN;\nSELECT 1;\nCOMMIT;\n"),
            Ok("BEGIN;\nSELECT 1;\nCOMMIT;\n")
        );
        assert_eq!(
            scan_self_framed("-- header\n;\nBEGIN;\nSELECT 1;\nCOMMIT;\n-- trailer\n"),
            Ok("-- header\n;\nBEGIN;\nSELECT 1;\nCOMMIT;\n-- trailer\n")
        );
        assert_eq!(
            scan_self_framed(
                "BEGIN;\nSET TRANSACTION ISOLATION LEVEL READ COMMITTED;\nSELECT 1;\nCOMMIT;\n"
            ),
            Ok("BEGIN;\nSET TRANSACTION ISOLATION LEVEL READ COMMITTED;\nSELECT 1;\nCOMMIT;\n")
        );

        for invalid in [
            "SELECT 1;\nCOMMIT;\n",
            "SELECT 1;\nBEGIN;\nSELECT 2;\nCOMMIT;\n",
            "BEGIN;\nSELECT 1;\n",
            "BEGIN;\nCOMMIT;\nDROP TABLE x;\nBEGIN;\nSELECT 1;\nCOMMIT;\n",
            "BEGIN;\nSELECT 1;\nCOMMIT;\nSELECT 2;\n",
            "BEGIN;\nSELECT 1;\nCOMMIT AND CHAIN;\n",
            "BEGIN;\nSELECT 1;\nCOMMIT PREPARED 'x';\n",
            "BEGIN;\nSELECT 1;\nEND;\n",
            "BEGIN;\nSAVEPOINT s;\nSELECT 1;\nCOMMIT;\n",
            "BEGIN;\nSELECT 1;\nROLLBACK;\nCOMMIT;\n",
            "BEGIN;\nSELECT 1;\nCOMMIT;\nSELECT 2\n",
            "BEGIN;\nSELECT 1;\nCOMMIT\n",
        ] {
            assert_rejects_self_framed(invalid);
        }
    }

    #[test]
    fn caller_framed_envelope_rejects_every_transaction_control_statement() {
        for invalid in [
            "BEGIN;\nSELECT 1;\nCOMMIT;\n",
            "START TRANSACTION;\n",
            "COMMIT;\n",
            "END;\n",
            "ROLLBACK;\n",
            "ROLLBACK PREPARED 'x';\n",
            "ABORT;\n",
            "SAVEPOINT s;\n",
            "RELEASE s;\n",
            "RELEASE SAVEPOINT s;\n",
            "PREPARE plan AS SELECT 1;\n",
            "PREPARE TRANSACTION 'x';\n",
            "SELECT 1;\ncommit;\n",
            "SELECT 1;\n  Begin;\nSELECT 2;\n",
        ] {
            assert_rejects_caller_framed(invalid);
        }
    }

    #[test]
    fn control_keywords_are_whole_statement_leading_tokens_only() {
        assert_accepts_caller_framed("SET LOCAL synchronous_commit = on;\n");
        assert_accepts_caller_framed("SELECT committed, range_start, backend_start FROM x;\n");
        assert_accepts_caller_framed("SELECT begin_time FROM commits;\n");
        assert_accepts_caller_framed("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\n");
        assert_accepts_caller_framed("SELECT \"BEGIN\", \"COMMIT\" FROM x;\n");
        assert_accepts_caller_framed("DO $x$ BEGIN COMMIT; END $x$;\n");
        // BEGIN ATOMIC bodies split at the inner semicolon, so the trailing
        // END reads as a statement-leading control token: over-rejected, never
        // silently accepted with an unscanned body.
        assert_rejects_caller_framed(
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; END;\n",
        );
    }

    #[test]
    fn rejects_inputs_without_an_executable_statement() {
        for invalid in ["\n", "-- comment only\n", ";\n", "/* comment */\n"] {
            assert_rejects_caller_framed(invalid);
        }
    }

    #[test]
    fn errors_never_render_input_contents() {
        let secret = "do-not-log-catalog-password";
        let invalid = format!("BEGIN;\n\\echo {secret}\nCOMMIT;\n");
        let Err(error) = compile(&invalid, INVENTORY, GENESIS, PREFLIGHT) else {
            panic!("meta command was accepted");
        };
        assert_eq!(error, invalid_shape("migration"));
        assert!(!error.to_string().contains(secret));
        assert!(error.to_string().contains("migration input"));
    }
}
