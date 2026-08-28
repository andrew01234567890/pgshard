// Package pgparser parses SQL with the exact PostgreSQL grammar the router
// targets. It fronts a per-major cgo binding (internal/pgparser/pg18 today,
// selected by build tag) with limits, a bounded LRU cache and a metrics hook.
package pgparser

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SQLSTATE codes surfaced by this package.
const (
	SyntaxErrorSQLState          = "42601"
	ProgramLimitExceededSQLState = "54000"
	InternalErrorSQLState        = "XX000"
)

// Defaults applied by New when the corresponding Option is zero.
const (
	DefaultMaxSQLBytes   = 1 << 20
	DefaultMaxStatements = 1000
	DefaultCacheEntries  = 4096
	DefaultCacheBytes    = 64 << 20
)

// Error is a parse failure with PostgreSQL semantics: SQLState is the code
// the server itself would report and Position the 1-based character offset
// (0 when unknown).
type Error struct {
	Message  string
	Position int
	SQLState string
}

func (e *Error) Error() string {
	if e.Position > 0 {
		return fmt.Sprintf("%s (SQLSTATE %s, position %d)", e.Message, e.SQLState, e.Position)
	}
	return fmt.Sprintf("%s (SQLSTATE %s)", e.Message, e.SQLState)
}

// Stmt is one statement of a parsed batch.
type Stmt struct {
	// Kind is the protobuf message name of the statement node, e.g. "SelectStmt".
	Kind string
	// RawStmt is the version-specific RawStmt message. It is shared with the
	// cache and must be treated as immutable.
	RawStmt proto.Message
	// Location and Length locate the statement text inside the input.
	Location int
	Length   int
}

// ParseResult is an immutable parse of one SQL string.
type ParseResult struct {
	// Tree is the whole version-specific ParseResult message; pass it to Deparse.
	Tree  proto.Message
	Stmts []Stmt
}

// Metrics receives cache events. Implementations must be safe for concurrent use.
type Metrics interface {
	CacheHit()
	CacheMiss()
	// CacheEvicted reports the running total of entries evicted, and
	// CacheLiveBytes the heap the cache accounts for, after each parse
	// the cache admitted.
	CacheEvicted(total int)
	CacheLiveBytes(n int)
}

// Options configure a Parser. Zero values take the Default* constants;
// negative values disable the corresponding limit or the cache.
type Options struct {
	MaxSQLBytes   int
	MaxStatements int
	CacheEntries  int
	CacheBytes    int
	Metrics       Metrics
}

// Parser is a safe-for-concurrent-use SQL parser with limits and a cache.
type Parser struct {
	maxSQLBytes   int
	maxStatements int
	cache         *lru
	metrics       Metrics
}

// New returns a Parser configured by opts.
func New(opts Options) *Parser {
	pick := func(v, def int) int {
		if v == 0 {
			return def
		}
		return v
	}
	p := &Parser{
		maxSQLBytes:   pick(opts.MaxSQLBytes, DefaultMaxSQLBytes),
		maxStatements: pick(opts.MaxStatements, DefaultMaxStatements),
		metrics:       opts.Metrics,
	}
	if p.metrics == nil {
		p.metrics = noMetrics{}
	}
	entries, bytes := pick(opts.CacheEntries, DefaultCacheEntries), pick(opts.CacheBytes, DefaultCacheBytes)
	if entries > 0 && bytes > 0 {
		p.cache = newLRU(entries, bytes)
	}
	return p
}

type noMetrics struct{}

func (noMetrics) CacheHit()            {}
func (noMetrics) CacheMiss()           {}
func (noMetrics) CacheEvicted(_ int)   {}
func (noMetrics) CacheLiveBytes(_ int) {}

// Parse parses sql, honouring ctx for cancellation and the configured limits.
// Results may be shared between callers and must not be mutated.
func (p *Parser) Parse(ctx context.Context, sql string) (*ParseResult, error) {
	if p.maxSQLBytes > 0 && len(sql) > p.maxSQLBytes {
		return nil, &Error{
			Message:  fmt.Sprintf("statement is %d bytes, exceeds limit of %d", len(sql), p.maxSQLBytes),
			SQLState: ProgramLimitExceededSQLState,
		}
	}
	if p.cache != nil {
		if res, ok := p.cache.get(sql); ok {
			p.metrics.CacheHit()
			return res, nil
		}
		p.metrics.CacheMiss()
	}
	res, err := parseWithContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	if p.maxStatements > 0 && len(res.Stmts) > p.maxStatements {
		return nil, &Error{
			Message:  fmt.Sprintf("batch has %d statements, exceeds limit of %d", len(res.Stmts), p.maxStatements),
			SQLState: ProgramLimitExceededSQLState,
		}
	}
	if p.cache != nil {
		p.cache.put(sql, res, astWeight(sql, res.Tree))
		bytes, evicted := p.cache.stats()
		p.metrics.CacheLiveBytes(bytes)
		p.metrics.CacheEvicted(evicted)
	}
	return res, nil
}

// astHeapFactor turns a tree's serialized size into an estimate of the heap
// it actually holds. The wire encoding is compact; the Go objects it becomes
// are structs, slices, maps and pointers, and there are far more bytes of
// them.
//
// Measured by parsing two thousand statements of a shape, holding them, and
// comparing HeapAlloc against the serialized total: a simple SELECT is
// 22.2x, a join 13.5x, a multi-column INSERT 14.5x, and one with a long
// string literal 10.1x -- literals raise the serialized size without adding
// nodes, which is why they score lowest.
//
// The factor is the top of that range rather than the middle, deliberately.
// CacheBytes is a memory bound an operator sets, so the accounting must not
// exceed it; erring high costs cache entries, which erring low pays for in
// resident memory the operator did not ask for. Before this the factor was
// effectively one, so a nominal 32 MiB cache could hold several hundred
// megabytes.
const astHeapFactor = 24

// astWeight estimates the heap a cached parse holds.
func astWeight(sql string, tree proto.Message) int {
	return len(sql) + proto.Size(tree)*astHeapFactor
}

// parseWithContext parses sql, refusing before and after if the caller has
// gone away.
//
// It used to run the parse on a goroutine and select on the context. That
// bought nothing: the parse itself cannot be interrupted, so a cancelled
// caller only abandoned a parse that carried on running and threw its
// result away. What it cost was a channel and a goroutine on every cache
// miss. The check after the parse keeps the caller's side of that
// behaviour -- a context that ends mid-parse still yields its error rather
// than a result -- and the length limit enforced above bounds how long a
// cancelled caller can be kept waiting.
func parseWithContext(ctx context.Context, sql string) (*ParseResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	res, err := Parse(sql)
	if cerr := ctx.Err(); cerr != nil {
		return nil, cerr
	}
	return res, err
}

// Parse parses sql without limits or caching.
func Parse(sql string) (*ParseResult, error) {
	tree, err := engineParse(sql)
	if err != nil {
		return nil, err
	}
	stmts, err := statementsOf(tree)
	if err != nil {
		return nil, err
	}
	return &ParseResult{Tree: tree, Stmts: stmts}, nil
}

// Fingerprint returns the statement-shape hash PostgreSQL tooling agrees on:
// stable across literal values, whitespace and letter case.
func Fingerprint(sql string) (string, error) { return engineFingerprint(sql) }

// Normalize replaces constants in sql with $n placeholders.
func Normalize(sql string) (string, error) { return engineNormalize(sql) }

// Deparse renders a ParseResult tree back to SQL.
func Deparse(tree proto.Message) (string, error) { return engineDeparse(tree) }

// Kinds returns the statement kinds of res in order.
func (r *ParseResult) Kinds() []string {
	kinds := make([]string, len(r.Stmts))
	for i, s := range r.Stmts {
		kinds[i] = s.Kind
	}
	return kinds
}

var errUnexpectedTree = errors.New("pgparser: parse tree does not have the expected shape")

// statementsOf walks the version-specific tree reflectively: ParseResult.stmts
// is a list of RawStmt{stmt: Node{oneof node}, stmt_location, stmt_len}.
func statementsOf(tree proto.Message) ([]Stmt, error) {
	m := tree.ProtoReflect()
	fields := m.Descriptor().Fields()
	stmtsField := fields.ByName("stmts")
	if stmtsField == nil || !stmtsField.IsList() {
		return nil, errUnexpectedTree
	}
	list := m.Get(stmtsField).List()
	stmts := make([]Stmt, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		raw := list.Get(i).Message()
		kind, err := kindOf(raw)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, Stmt{
			Kind:     kind,
			RawStmt:  raw.Interface(),
			Location: int(intField(raw, "stmt_location")),
			Length:   int(intField(raw, "stmt_len")),
		})
	}
	return stmts, nil
}

func kindOf(raw protoreflect.Message) (string, error) {
	stmtField := raw.Descriptor().Fields().ByName("stmt")
	if stmtField == nil || !raw.Has(stmtField) {
		return "", errUnexpectedTree
	}
	node := raw.Get(stmtField).Message()
	oneof := node.Descriptor().Oneofs().ByName("node")
	if oneof == nil {
		return "", errUnexpectedTree
	}
	which := node.WhichOneof(oneof)
	if which == nil {
		return "", errUnexpectedTree
	}
	return string(which.Message().Name()), nil
}

func intField(m protoreflect.Message, name protoreflect.Name) int64 {
	f := m.Descriptor().Fields().ByName(name)
	if f == nil {
		return 0
	}
	return m.Get(f).Int()
}
