//! Fail-closed parsing foundations for future `PostgreSQL` 18 route analysis.
//!
//! The candidate parser is configured with its `PostgreSQL` dialect but is
//! intentionally treated as permissive. A successful parse is not
//! `PostgreSQL` semantic validation and never authorizes routing by itself.

use std::{fmt, num::NonZeroU16, ops::ControlFlow};

use pgshard_catalog::{CatalogSnapshot, DatabaseId, ShardKeyType, TableName};
use pgshard_types::CatalogEpoch;
use sqlparser::{
    ast::{
        BinaryOperator, Expr, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select,
        SelectFlavor, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
        ValueWithSpan, Visit, Visitor, WildcardAdditionalOptions,
    },
    dialect::PostgreSqlDialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};
use thiserror::Error;

/// Maximum SQL text accepted by the planner.
///
/// `PostgreSQL`'s wire protocol permits larger frames, but planning an unbounded
/// syntax tree would let one client consume disproportionate pooler memory.
pub const MAX_SQL_BYTES: usize = 16_384;

/// Maximum lexer tokens retained for one statement, including whitespace.
pub const MAX_SQL_TOKENS: usize = 4_096;

/// Maximum counted syntax nodes retained for one parsed statement.
pub const MAX_AST_NODES: usize = 2_048;

const MAX_RECURSION_DEPTH: usize = 50;
// Flat parser trees can be much deeper than the delimiter and parser recursion
// limits. The fixed reserve covers parser/visitor frames for small statements;
// the per-structural-token reserve scales syntax without rewarding trivia.
const MIN_AST_STACK_BYTES: usize = 256 * 1024;
const AST_STACK_BYTES_PER_TOKEN: usize = 2 * 1024;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Delimiter {
    Parenthesis,
    Bracket,
    Brace,
}

struct LexicalFacts {
    structural_tokens: usize,
    contains_double_equality: bool,
}

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
    statement: Option<Statement>,
    kind: StatementKind,
    contains_double_equality: bool,
    stack_reserve: usize,
}

impl ParsedStatement {
    /// Returns the coarse top-level syntax kind.
    #[must_use]
    pub const fn kind(&self) -> StatementKind {
        self.kind
    }

    /// Extracts the deliberately narrow first extended-query routing shape.
    ///
    /// The accepted shape is exactly `SELECT * FROM schema.table WHERE
    /// shard_key = $n` (or the equality operands reversed), with no alias,
    /// join, CTE, subquery, modifier, extra predicate, locking, ordering, or
    /// limit. The table and shard-key column must match the retained catalog
    /// snapshot. The returned template is not an execution proof: Parse-time
    /// parameter type/operator resolution and Bind-time format/value checks
    /// remain mandatory.
    ///
    /// # Errors
    ///
    /// Fails closed for every other syntax tree, invalid `PostgreSQL` identifier
    /// normalization, unknown catalog object, or invalid bind placeholder.
    pub fn parameter_route_template(
        &self,
        snapshot: &CatalogSnapshot,
        database_id: DatabaseId,
    ) -> Result<ParameterRouteTemplate, RouteTemplateError> {
        // sqlparser represents `==` with the same AST operator as `=` even
        // though PostgreSQL resolves `==` as a distinct, potentially custom
        // operator. Fail closed using privacy-safe lexical provenance retained
        // before parsing because their semantics are otherwise indistinguishable.
        if self.contains_double_equality {
            return Err(RouteTemplateError::UnsupportedShape);
        }
        let Some(statement) = self.statement.as_ref() else {
            return Err(RouteTemplateError::UnsupportedShape);
        };
        let (table_name, column, parameter_number) = prove_select_shape(statement)?;
        let database = snapshot
            .database(database_id)
            .ok_or(RouteTemplateError::UnknownDatabase)?;
        let table = database
            .table(&table_name)
            .ok_or(RouteTemplateError::UnknownTable)?;
        if column != table.shard_key_column() {
            return Err(RouteTemplateError::NotShardKeyEquality);
        }

        Ok(ParameterRouteTemplate {
            catalog_epoch: snapshot.catalog_epoch(),
            database_id,
            table_name,
            parameter_number,
            shard_key_type: table.shard_key_type(),
        })
    }
}

impl Drop for ParsedStatement {
    fn drop(&mut self) {
        let Some(statement) = self.statement.take() else {
            return;
        };
        // sqlparser's AST uses recursive destruction. Reuse the parse-time
        // reserve because callers may release a valid tree on a smaller stack.
        stacker::maybe_grow(self.stack_reserve, self.stack_reserve, move || {
            drop(statement);
        });
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

/// Catalog-bound candidate for routing one bind parameter by a registered
/// table's shard key.
///
/// This is deliberately not an execution proof. The session layer must still
/// validate Parse-time parameter types and operator resolution, then Bind-time
/// parameter count, format, NULL state, and value bytes against this template.
#[derive(Clone, Eq, PartialEq)]
pub struct ParameterRouteTemplate {
    catalog_epoch: CatalogEpoch,
    database_id: DatabaseId,
    table_name: TableName,
    parameter_number: NonZeroU16,
    shard_key_type: ShardKeyType,
}

impl ParameterRouteTemplate {
    /// Returns the exact catalog epoch used to prove the route template.
    #[must_use]
    pub const fn catalog_epoch(&self) -> CatalogEpoch {
        self.catalog_epoch
    }

    /// Returns the logical database containing the registered table.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the exact catalog-normalized table name.
    #[must_use]
    pub fn table_name(&self) -> &TableName {
        &self.table_name
    }

    /// Returns `PostgreSQL`'s one-based bind parameter number.
    #[must_use]
    pub const fn parameter_number(&self) -> NonZeroU16 {
        self.parameter_number
    }

    /// Returns the catalog's canonical shard-key type.
    #[must_use]
    pub const fn shard_key_type(&self) -> ShardKeyType {
        self.shard_key_type
    }
}

impl fmt::Debug for ParameterRouteTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParameterRouteTemplate")
            .field("catalog_epoch", &self.catalog_epoch)
            .field("database_id", &self.database_id)
            .field("parameter_number", &self.parameter_number)
            .field("shard_key_type", &self.shard_key_type)
            .finish_non_exhaustive()
    }
}

fn prove_select_shape(
    statement: &Statement,
) -> Result<(TableName, String, NonZeroU16), RouteTemplateError> {
    let Statement::Query(query) = statement else {
        return Err(RouteTemplateError::UnsupportedShape);
    };
    let Query {
        with: None,
        body,
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks,
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators,
    } = query.as_ref()
    else {
        return Err(RouteTemplateError::UnsupportedShape);
    };
    if !locks.is_empty() || !pipe_operators.is_empty() {
        return Err(RouteTemplateError::UnsupportedShape);
    }
    let SetExpr::Select(select) = body.as_ref() else {
        return Err(RouteTemplateError::UnsupportedShape);
    };
    prove_select(select)
}

fn prove_select(select: &Select) -> Result<(TableName, String, NonZeroU16), RouteTemplateError> {
    let Select {
        select_token: _,
        optimizer_hints,
        distinct: None,
        select_modifiers: None,
        top: None,
        top_before_distinct: _,
        projection,
        exclude: None,
        into: None,
        from,
        lateral_views,
        prewhere: None,
        selection: Some(selection),
        connect_by,
        group_by: GroupByExpr::Expressions(group_expressions, group_modifiers),
        cluster_by,
        distribute_by,
        sort_by,
        having: None,
        named_window,
        qualify: None,
        window_before_qualify: _,
        value_table_mode: None,
        flavor: SelectFlavor::Standard,
    } = select
    else {
        return Err(RouteTemplateError::UnsupportedShape);
    };
    if !optimizer_hints.is_empty()
        || !is_plain_wildcard(projection)
        || from.len() != 1
        || !lateral_views.is_empty()
        || !connect_by.is_empty()
        || !group_expressions.is_empty()
        || !group_modifiers.is_empty()
        || !cluster_by.is_empty()
        || !distribute_by.is_empty()
        || !sort_by.is_empty()
        || !named_window.is_empty()
    {
        return Err(RouteTemplateError::UnsupportedShape);
    }

    let table_name = prove_table(&from[0])?;
    let (column, parameter_number) = prove_equality(selection)?;
    Ok((table_name, column, parameter_number))
}

fn is_plain_wildcard(projection: &[SelectItem]) -> bool {
    let [SelectItem::Wildcard(options)] = projection else {
        return false;
    };
    let WildcardAdditionalOptions {
        wildcard_token: _,
        opt_ilike: None,
        opt_exclude: None,
        opt_except: None,
        opt_replace: None,
        opt_rename: None,
        opt_alias: None,
    } = options
    else {
        return false;
    };
    true
}

fn prove_table(table: &TableWithJoins) -> Result<TableName, RouteTemplateError> {
    if !table.joins.is_empty() {
        return Err(RouteTemplateError::UnsupportedShape);
    }
    let TableFactor::Table {
        name,
        alias: None,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
    } = &table.relation
    else {
        return Err(RouteTemplateError::UnsupportedShape);
    };
    if !with_hints.is_empty() || !partitions.is_empty() || !index_hints.is_empty() {
        return Err(RouteTemplateError::UnsupportedShape);
    }
    normalize_table_name(name)
}

fn normalize_table_name(name: &ObjectName) -> Result<TableName, RouteTemplateError> {
    let [
        ObjectNamePart::Identifier(schema),
        ObjectNamePart::Identifier(table),
    ] = name.0.as_slice()
    else {
        return Err(RouteTemplateError::SchemaQualificationRequired);
    };
    let schema = normalize_identifier(schema)?;
    let table = normalize_identifier(table)?;
    TableName::new(schema, table).map_err(|_| RouteTemplateError::InvalidIdentifier)
}

fn normalize_identifier(identifier: &Ident) -> Result<String, RouteTemplateError> {
    match identifier.quote_style {
        None if identifier.value.is_ascii() => Ok(identifier.value.to_ascii_lowercase()),
        Some('"') => Ok(identifier.value.clone()),
        None | Some(_) => Err(RouteTemplateError::InvalidIdentifier),
    }
}

fn prove_equality(expression: &Expr) -> Result<(String, NonZeroU16), RouteTemplateError> {
    let expression = peel_nested(expression);
    let Expr::BinaryOp { left, op, right } = expression else {
        return Err(RouteTemplateError::NotShardKeyEquality);
    };
    if *op != BinaryOperator::Eq {
        return Err(RouteTemplateError::NotShardKeyEquality);
    }

    prove_column_and_placeholder(left, right).or_else(|_| prove_column_and_placeholder(right, left))
}

fn prove_column_and_placeholder(
    column: &Expr,
    placeholder: &Expr,
) -> Result<(String, NonZeroU16), RouteTemplateError> {
    let Expr::Identifier(column) = peel_nested(column) else {
        return Err(RouteTemplateError::NotShardKeyEquality);
    };
    let column = normalize_identifier(column)?;
    let Expr::Value(value) = peel_nested(placeholder) else {
        return Err(RouteTemplateError::InvalidPlaceholder);
    };
    let Value::Placeholder(placeholder) = &value.value else {
        return Err(RouteTemplateError::InvalidPlaceholder);
    };
    let parameter_number = parse_parameter_number(placeholder)?;
    Ok((column, parameter_number))
}

fn peel_nested(mut expression: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expression {
        expression = inner;
    }
    expression
}

fn parse_parameter_number(value: &str) -> Result<NonZeroU16, RouteTemplateError> {
    let digits = value
        .strip_prefix('$')
        .filter(|digits| !digits.is_empty() && !digits.starts_with('0'))
        .ok_or(RouteTemplateError::InvalidPlaceholder)?;
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RouteTemplateError::InvalidPlaceholder);
    }
    digits
        .parse::<u16>()
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or(RouteTemplateError::InvalidPlaceholder)
}

/// Failure to prove the first deliberately narrow parameter route shape.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RouteTemplateError {
    /// The syntax tree contains a construct not admitted by this route template.
    #[error("statement shape is not supported for parameter routing")]
    UnsupportedShape,
    /// Milestone 1 route templates do not resolve session `search_path`.
    #[error("routed table must be explicitly schema-qualified")]
    SchemaQualificationRequired,
    /// Identifier folding would not exactly match the strict route contract.
    #[error("statement contains an unsupported PostgreSQL identifier")]
    InvalidIdentifier,
    /// The retained snapshot does not contain the logical database.
    #[error("logical database is not present in the catalog snapshot")]
    UnknownDatabase,
    /// The referenced relation is not a registered sharded table.
    #[error("relation is not a registered sharded table")]
    UnknownTable,
    /// The exact predicate is not equality on the registered shard key.
    #[error("predicate is not direct equality on the registered shard key")]
    NotShardKeyEquality,
    /// The equality value is not a canonical `$1` through `$65535` placeholder.
    #[error("shard-key value is not a canonical PostgreSQL bind placeholder")]
    InvalidPlaceholder,
}

/// Parses exactly one bounded candidate statement using a `PostgreSQL` dialect.
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
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(|_| ParseError::InvalidSyntax)?;
    if tokens.len() > MAX_SQL_TOKENS {
        return Err(ParseError::TooManyTokens {
            actual: tokens.len(),
            maximum: MAX_SQL_TOKENS,
        });
    }
    let lexical_facts = inspect_lexical_structure(&tokens)?;

    let stack_reserve = ast_stack_reserve(lexical_facts.structural_tokens);
    // Keep parsing, recursive validation, and every rejected-tree drop inside
    // the protected segment. Only an already-budgeted tree can leave it.
    stacker::maybe_grow(stack_reserve, stack_reserve, move || {
        parse_tokens(
            dialect,
            tokens,
            lexical_facts.contains_double_equality,
            stack_reserve,
        )
    })
}

fn parse_tokens(
    dialect: PostgreSqlDialect,
    tokens: Vec<TokenWithSpan>,
    contains_double_equality: bool,
    stack_reserve: usize,
) -> Result<ParsedStatement, ParseError> {
    let mut parser = Parser::new(&dialect)
        .with_recursion_limit(MAX_RECURSION_DEPTH)
        .with_tokens_with_locations(tokens);
    while parser.consume_token(&Token::SemiColon) {}
    if parser.peek_token_ref().token == Token::EOF {
        return Err(ParseError::NoStatement);
    }
    let statement = parser
        .parse_statement()
        .map_err(|error| ParseError::from_upstream(&error))?;
    while parser.consume_token(&Token::SemiColon) {}
    if parser.peek_token_ref().token != Token::EOF {
        return Err(ParseError::MultipleStatements);
    }
    if statement.visit(&mut AstBudget::new()).is_break() {
        return Err(ParseError::TooManyAstNodes {
            maximum: MAX_AST_NODES,
        });
    }

    let kind = match &statement {
        Statement::Query(_) => StatementKind::Query,
        Statement::Insert(_) => StatementKind::Insert,
        Statement::Update(_) => StatementKind::Update,
        Statement::Delete(_) => StatementKind::Delete,
        Statement::Merge(_) => StatementKind::Merge,
        _ => StatementKind::Other,
    };
    Ok(ParsedStatement {
        statement: Some(statement),
        kind,
        contains_double_equality,
        stack_reserve,
    })
}

const fn ast_stack_reserve(token_count: usize) -> usize {
    MIN_AST_STACK_BYTES + token_count * AST_STACK_BYTES_PER_TOKEN
}

fn inspect_lexical_structure(tokens: &[TokenWithSpan]) -> Result<LexicalFacts, ParseError> {
    let mut delimiters = [Delimiter::Parenthesis; MAX_RECURSION_DEPTH];
    let mut depth = 0_usize;
    let mut array_type_prefix_depth = 0_usize;
    let mut awaiting_array_angle = false;
    let mut awaiting_nested_array = false;
    let mut structural_tokens = 0_usize;
    let mut contains_double_equality = false;

    for token in tokens {
        if matches!(&token.token, Token::Whitespace(_)) {
            continue;
        }
        if matches!(&token.token, Token::SemiColon | Token::EOF) {
            array_type_prefix_depth = 0;
            awaiting_array_angle = false;
            awaiting_nested_array = false;
            continue;
        }
        structural_tokens += 1;
        contains_double_equality |= token.token == Token::DoubleEq;

        if matches!(
            &token.token,
            Token::Word(word) if word.keyword == Keyword::ARRAY
        ) {
            if !awaiting_nested_array {
                array_type_prefix_depth = 0;
            }
            awaiting_array_angle = true;
            awaiting_nested_array = false;
            continue;
        }
        if token.token == Token::Lt && awaiting_array_angle {
            if array_type_prefix_depth == MAX_RECURSION_DEPTH {
                return Err(ParseError::RecursionLimit);
            }
            array_type_prefix_depth += 1;
            awaiting_array_angle = false;
            awaiting_nested_array = true;
            continue;
        }
        // sqlparser recursively parses a directly nested ARRAY<ARRAY<...>>
        // prefix without consulting its recursion counter. Reset the lexical
        // guard at any other structural token so qualified attributes such as
        // `t.array < 1` remain ordinary PostgreSQL comparisons.
        array_type_prefix_depth = 0;
        awaiting_array_angle = false;
        awaiting_nested_array = false;

        let delimiter = match token.token {
            Token::LParen => Some(Delimiter::Parenthesis),
            Token::LBracket => Some(Delimiter::Bracket),
            Token::LBrace => Some(Delimiter::Brace),
            _ => None,
        };
        if let Some(delimiter) = delimiter {
            if depth == MAX_RECURSION_DEPTH {
                return Err(ParseError::RecursionLimit);
            }
            delimiters[depth] = delimiter;
            depth += 1;
            continue;
        }

        let closing = match token.token {
            Token::RParen => Some(Delimiter::Parenthesis),
            Token::RBracket => Some(Delimiter::Bracket),
            Token::RBrace => Some(Delimiter::Brace),
            _ => None,
        };
        if closing.is_some_and(|delimiter| depth > 0 && delimiters[depth - 1] == delimiter) {
            depth -= 1;
        }
    }

    Ok(LexicalFacts {
        structural_tokens,
        contains_double_equality,
    })
}

struct AstBudget {
    visited: usize,
}

impl AstBudget {
    const fn new() -> Self {
        Self { visited: 0 }
    }

    fn count(&mut self) -> ControlFlow<()> {
        self.visited += 1;
        if self.visited > MAX_AST_NODES {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

impl Visitor for AstBudget {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.count()
    }

    fn pre_visit_select(&mut self, _select: &Select) -> ControlFlow<Self::Break> {
        self.count()
    }

    fn pre_visit_relation(&mut self, _relation: &ObjectName) -> ControlFlow<Self::Break> {
        self.count()
    }

    fn pre_visit_table_factor(&mut self, _table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        self.count()
    }

    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        self.count()
    }

    fn pre_visit_statement(&mut self, _statement: &Statement) -> ControlFlow<Self::Break> {
        self.count()
    }

    fn pre_visit_value(&mut self, _value: &ValueWithSpan) -> ControlFlow<Self::Break> {
        self.count()
    }
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
    /// Lexing produced too many retained tokens.
    #[error("SQL contains {actual} tokens; planner maximum is {maximum}")]
    TooManyTokens {
        /// Actual token count, including whitespace.
        actual: usize,
        /// Configured hard maximum.
        maximum: usize,
    },
    /// Parsed syntax contains too many retained AST nodes.
    #[error("SQL syntax exceeds the planner AST-node limit of {maximum}")]
    TooManyAstNodes {
        /// Configured hard maximum.
        maximum: usize,
    },
    /// `PostgreSQL` protocol strings cannot contain embedded zero bytes.
    #[error("SQL contains an embedded zero byte")]
    EmbeddedZero,
    /// The candidate parser rejected the syntax.
    #[error("SQL syntax is not supported")]
    InvalidSyntax,
    /// The syntax tree exceeds the bounded recursion depth.
    #[error("SQL syntax exceeds the planner recursion limit")]
    RecursionLimit,
    /// No nonempty statement was supplied.
    #[error("expected one SQL statement, received none")]
    NoStatement,
    /// Input remains after the first statement.
    #[error("expected one SQL statement, received multiple")]
    MultipleStatements,
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
    use pgshard_catalog::{
        ClusterId, DatabaseCatalog, DatabaseEpochs, RegisteredTable, RoutingHashConfig, ShardRoute,
    };
    use pgshard_types::{KEYSPACE_END, KeyRange, RoutingHashV1, ShardId};
    use uuid::Uuid;

    use super::*;

    fn route_snapshot() -> (CatalogSnapshot, DatabaseId) {
        let database_id = DatabaseId::new(Uuid::from_u128(2)).expect("database ID");
        let table_name = TableName::new("public", "events").expect("table name");
        let table = RegisteredTable::new(
            table_name,
            "tenant_id",
            ShardKeyType::Int64,
            RoutingHashV1::VERSION,
        )
        .expect("registered table");
        let database = DatabaseCatalog::new(
            database_id,
            "app",
            DatabaseEpochs::new(1, 1, 1).expect("database epochs"),
            vec![ShardRoute::new(
                ShardId(0),
                KeyRange::new(0, KEYSPACE_END).expect("complete range"),
            )],
            vec![table],
        )
        .expect("database catalog");
        let snapshot = CatalogSnapshot::new(
            ClusterId::new(Uuid::from_u128(1)).expect("cluster ID"),
            7,
            RoutingHashConfig::new(1, 42).expect("routing hash"),
            vec![database],
        )
        .expect("catalog snapshot");
        (snapshot, database_id)
    }

    fn analyze_route(sql: &str) -> Result<ParameterRouteTemplate, RouteTemplateError> {
        let (snapshot, database_id) = route_snapshot();
        parse_one(sql)
            .expect("test SQL parses")
            .parameter_route_template(&snapshot, database_id)
    }

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
            ParseError::NoStatement
        );
        assert_eq!(
            parse_one("select 1; select 2").expect_err("two statements"),
            ParseError::MultipleStatements
        );
        assert_eq!(
            parse_one("select 1; select (((").expect_err("second invalid statement"),
            ParseError::MultipleStatements
        );
        assert!(parse_one(";;; select 1;;;").is_ok());
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
    fn enforces_token_and_ast_budgets() {
        let token_heavy = format!("select {}", vec!["1"; MAX_SQL_TOKENS].join(","));
        assert!(matches!(
            parse_one(&token_heavy),
            Err(ParseError::TooManyTokens {
                actual,
                maximum: MAX_SQL_TOKENS,
            }) if actual > MAX_SQL_TOKENS
        ));

        let ast_heavy = format!("select {}", vec!["1"; 1_100].join(","));
        assert_eq!(
            parse_one(&ast_heavy).expect_err("AST budget"),
            ParseError::TooManyAstNodes {
                maximum: MAX_AST_NODES,
            }
        );

        let many_statements = "select 1;".repeat(1_000);
        assert_eq!(
            parse_one(&many_statements).expect_err("many statements"),
            ParseError::MultipleStatements
        );

        let payload = "x".repeat(MAX_SQL_BYTES - "select ''".len());
        let exact_limit = format!("select '{payload}'");
        assert_eq!(exact_limit.len(), MAX_SQL_BYTES);
        assert!(parse_one(&exact_limit).is_ok());
    }

    #[test]
    fn candidate_parser_is_not_postgres_validation() {
        for non_postgres_sql in [
            "select top 1 * from planner_target",
            "insert overwrite planner_target values (1, 2)",
            "delete from planner_target order by tenant_id limit 1",
        ] {
            assert!(
                parse_one(non_postgres_sql).is_ok(),
                "candidate parser changed for {non_postgres_sql}"
            );
        }
    }

    #[test]
    fn rejects_excessive_delimiter_nesting_on_a_small_stack() {
        let nested = format!(
            "select {}1{}",
            "(".repeat(MAX_RECURSION_DEPTH * 4),
            ")".repeat(MAX_RECURSION_DEPTH * 4)
        );
        let result = std::thread::Builder::new()
            .name("planner-delimiter-small-stack".into())
            .stack_size(64 * 1024)
            .spawn(move || parse_one(&nested).map(|statement| statement.kind()))
            .expect("spawn small-stack parser")
            .join()
            .expect("small-stack parser must not panic");
        assert_eq!(result, Err(ParseError::RecursionLimit));
    }

    #[test]
    fn rejects_data_type_nesting_on_a_small_stack() {
        let nesting = MAX_RECURSION_DEPTH * 24;
        let nested = format!(
            "create table t (value {}int{})",
            "array<".repeat(nesting),
            ">".repeat(nesting)
        );
        let result = std::thread::Builder::new()
            .name("planner-small-stack".into())
            .stack_size(64 * 1024)
            .spawn(move || parse_one(&nested).map(|statement| statement.kind()))
            .expect("spawn small-stack parser")
            .join()
            .expect("small-stack parser must not panic");
        assert_eq!(result, Err(ParseError::RecursionLimit));
    }

    #[test]
    fn qualified_array_comparisons_do_not_consume_type_prefix_depth() {
        let comparisons = vec!["t.array < 1"; MAX_RECURSION_DEPTH + 1].join(", ");
        assert_eq!(
            parse_one(&format!("select {comparisons} from t"))
                .expect("independent comparisons")
                .kind(),
            StatementKind::Query
        );
    }

    #[test]
    fn trivia_does_not_inflate_the_ast_stack_reserve() {
        let dialect = PostgreSqlDialect {};
        let plain = Tokenizer::new(&dialect, "select 1")
            .tokenize_with_location()
            .expect("plain tokens");
        let padded_sql = format!("{}select 1{}", ";".repeat(2_000), " ".repeat(2_000));
        let padded = Tokenizer::new(&dialect, &padded_sql)
            .tokenize_with_location()
            .expect("padded tokens");

        let plain_structure = inspect_lexical_structure(&plain).expect("plain structure");
        let padded_structure = inspect_lexical_structure(&padded).expect("padded structure");
        assert_eq!(plain_structure.structural_tokens, 2);
        assert_eq!(padded_structure.structural_tokens, 2);
        assert_eq!(
            ast_stack_reserve(plain_structure.structural_tokens),
            ast_stack_reserve(padded_structure.structural_tokens)
        );
        assert_eq!(
            parse_one(&padded_sql).expect("padded query").kind(),
            StatementKind::Query
        );
    }

    #[test]
    fn bounds_flat_trees_on_a_small_stack() {
        let bounded_expression = format!("select {}", vec!["1"; 600].join("+"));
        let bounded_set_operation = format!("select 1{}", " union all select 1".repeat(400));
        let excessive_expression = format!("select {}", vec!["1"; 2_000].join("+"));
        let result = std::thread::Builder::new()
            .name("planner-flat-tree-small-stack".into())
            .stack_size(64 * 1024)
            .spawn(move || {
                let expression = parse_one(&bounded_expression).map(|statement| statement.kind());
                let set_operation =
                    parse_one(&bounded_set_operation).map(|statement| statement.kind());
                let excessive = parse_one(&excessive_expression).map(|statement| statement.kind());
                (expression, set_operation, excessive)
            })
            .expect("spawn small-stack parser")
            .join()
            .expect("small-stack parser must not panic");
        assert_eq!(result.0, Ok(StatementKind::Query));
        assert_eq!(result.1, Ok(StatementKind::Query));
        assert!(matches!(
            result.2,
            Err(ParseError::TooManyAstNodes { .. } | ParseError::RecursionLimit)
        ));
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
        let (snapshot, database_id) = route_snapshot();

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
            if let Ok(statement) = parse_one(&sql) {
                let _ = statement.parameter_route_template(&snapshot, database_id);
            }
        }
    }

    #[test]
    fn proves_only_direct_schema_qualified_shard_key_parameters() {
        for (sql, parameter_number) in [
            ("select * from public.events where tenant_id = $1", 1),
            (
                "select /* == inside a comment */ * from public.events where tenant_id = $1",
                1,
            ),
            ("select * from PUBLIC.EVENTS where $2 = TENANT_ID", 2),
            (
                "select * from \"public\".\"events\" where \"tenant_id\" = $3",
                3,
            ),
            (
                "select * from public.events where ((tenant_id)) = (($65535))",
                65_535,
            ),
        ] {
            let template = analyze_route(sql).expect("route template");
            assert_eq!(template.catalog_epoch(), CatalogEpoch(7));
            assert_eq!(template.table_name().schema(), "public");
            assert_eq!(template.table_name().table(), "events");
            assert_eq!(template.parameter_number().get(), parameter_number);
            assert_eq!(template.shard_key_type(), ShardKeyType::Int64);
        }
    }

    #[test]
    fn rejects_every_unproven_select_shape() {
        for sql in [
            "select tenant_id from public.events where tenant_id = $1",
            "select * from events where tenant_id = $1",
            "select * from public.events as e where tenant_id = $1",
            "select * from public.events join public.events as e using (tenant_id) where tenant_id = $1",
            "select * from public.events where tenant_id = $1 and tenant_id > 0",
            "with changed as (delete from public.events returning *) select * from changed",
            "select * from public.events where events.tenant_id = $1",
            "select * from public.events where tenant_id = $1::bigint",
            "select * from public.events where tenant_id = 1",
            "select * from public.events where tenant_id = $0",
            "select * from public.events where tenant_id = $01",
            "select * from public.events where tenant_id = $65536",
            "select * from public.events where tenant_id = ?",
            "select * from public.events where other_id = $1",
            "select * from public.events where tenant_id = $1 order by tenant_id",
            "select * from public.events where tenant_id = $1 limit 1",
            "select * from public.events where tenant_id = $1 for update",
            "select distinct * from public.events where tenant_id = $1",
            "select * from public.events where tenant_id == $1",
            "select top 1 * from public.events where tenant_id = $1",
            "select * from públic.events where tenant_id = $1",
        ] {
            let rejected = parse_one(sql).map_or(true, |statement| {
                let (snapshot, database_id) = route_snapshot();
                statement
                    .parameter_route_template(&snapshot, database_id)
                    .is_err()
            });
            assert!(rejected, "unexpected route template for {sql}");
        }
    }

    #[test]
    fn catalog_binding_and_debug_output_fail_closed() {
        let (snapshot, database_id) = route_snapshot();
        let unknown_database = DatabaseId::new(Uuid::from_u128(99)).expect("database ID");
        let parsed = parse_one("select * from public.events where tenant_id = $1").expect("SQL");
        assert_eq!(
            parsed.parameter_route_template(&snapshot, unknown_database),
            Err(RouteTemplateError::UnknownDatabase)
        );

        let unknown_table = parse_one("select * from public.missing where tenant_id = $1")
            .expect("unknown table SQL");
        assert_eq!(
            unknown_table.parameter_route_template(&snapshot, database_id),
            Err(RouteTemplateError::UnknownTable)
        );

        let template = parsed
            .parameter_route_template(&snapshot, database_id)
            .expect("route template");
        assert!(!format!("{template:?}").contains("events"));
    }
}
