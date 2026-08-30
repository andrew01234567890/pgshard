package pgparser

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

type golden struct {
	sql         string
	kinds       []string
	deparse     string
	fingerprint string
}

// Deparse strings are what libpg_query 18 emits; fingerprints are the
// libpg_query 18 values and must never change without a deliberate bump.
var goldens = []golden{
	// PostgreSQL 18-only syntax.
	{"INSERT INTO t (a) VALUES (1) RETURNING OLD.*, NEW.*", []string{"InsertStmt"},
		"INSERT INTO t (a) VALUES (1) RETURNING old.*, new.*", "1fc870be2714d700"},
	{"UPDATE t SET a = 1 WHERE b = 2 RETURNING WITH (OLD AS o, NEW AS n) o.a, n.a", []string{"UpdateStmt"},
		"UPDATE t SET a = 1 WHERE b = 2 RETURNING WITH (OLD AS o, NEW AS n) o.a, n.a", "fd387c6c4a52fe3a"},
	{"ALTER TABLE t ADD CONSTRAINT c NOT NULL x NOT VALID", []string{"AlterTableStmt"},
		"ALTER TABLE t ADD CONSTRAINT c NOT NULL x NOT VALID", "7a7e55e0a3da6f41"},
	{"CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a*2) VIRTUAL)", []string{"CreateStmt"},
		"CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)", "1af8764b8561b30b"},
	{"ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES u NOT ENFORCED", []string{"AlterTableStmt"},
		"ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES u NOT ENFORCED NOT VALID", "a7bd33d274aec099"},
	{"COPY t FROM STDIN (ON_ERROR ignore, REJECT_LIMIT 5)", []string{"CopyStmt"},
		"COPY t FROM STDIN WITH (on_error ignore, reject_limit 5)", "c41e3bed48196eb3"},
	// Classic DML.
	{"select a,b from t where a=1", []string{"SelectStmt"}, "SELECT a, b FROM t WHERE a = 1", "08071b8a2ab75ee4"},
	{"SELECT a FROM t WHERE b = ANY($1)", []string{"SelectStmt"}, "SELECT a FROM t WHERE b = ANY($1)", "f5ccd2dc9a65d441"},
	{"WITH x AS (SELECT 1) SELECT * FROM x", []string{"SelectStmt"}, "WITH x AS (SELECT 1) SELECT * FROM x", "7248ed4d4464e530"},
	{"DELETE FROM t WHERE a = 1", []string{"DeleteStmt"}, "DELETE FROM t WHERE a = 1", "08f5564875424f95"},
	{"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", []string{"MergeStmt"},
		"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", "7d0b37f7e05f0675"},
	{"BEGIN; SELECT 1; COMMIT", []string{"TransactionStmt", "SelectStmt", "TransactionStmt"}, "BEGIN; SELECT 1; COMMIT", "f91eac147ae33a01"},
	// Classic DDL and utility statements.
	{"CREATE TABLE t (a int) PARTITION BY HASH (a)", []string{"CreateStmt"}, "CREATE TABLE t (a int) PARTITION BY HASH (a)", "0f4348962973fba4"},
	{"CREATE INDEX i ON t (a) WITH (fillfactor = 70)", []string{"IndexStmt"}, "CREATE INDEX i ON t USING btree (a) WITH (fillfactor=70)", "d1ee512f5c9d3244"},
	{"DROP TABLE IF EXISTS t", []string{"DropStmt"}, "DROP TABLE IF EXISTS t", "9f906b068865c957"},
	{"TRUNCATE t CASCADE", []string{"TruncateStmt"}, "TRUNCATE t CASCADE", "abaa0ec82eb01a53"},
	{"CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$", []string{"CreateFunctionStmt"},
		"CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$", "cd77ffb0fec0ac64"},
	{"EXPLAIN ANALYZE SELECT 1", []string{"ExplainStmt"}, "EXPLAIN (ANALYZE) SELECT 1", "27aa8a72e026061c"},
	{"PREPARE p AS SELECT $1::int", []string{"PrepareStmt"}, "PREPARE p AS SELECT $1::int", "080de42af0124f43"},
	{"DEALLOCATE ALL", []string{"DeallocateStmt"}, "DEALLOCATE ALL", "2debfb8745df64a7"},
	{"SET search_path TO a, b", []string{"VariableSetStmt"}, "SET search_path TO a, b", "972eb2e22f47f95c"},
	{"SHOW all", []string{"VariableShowStmt"}, "SHOW ALL", "83d64e4fe320bf3a"},
	{"VACUUM (ANALYZE) t", []string{"VacuumStmt"}, "VACUUM (ANALYZE) t", "e89279715fc612be"},
	{"", nil, "", "d8d13f8b2da6c9ad"},
	{"  ; ;", nil, "", "d8d13f8b2da6c9ad"},
}

func TestGoldens(t *testing.T) {
	for _, g := range goldens {
		t.Run(g.sql, func(t *testing.T) {
			res, err := Parse(g.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := res.Kinds(); strings.Join(got, ",") != strings.Join(g.kinds, ",") {
				t.Errorf("kinds = %v, want %v", got, g.kinds)
			}
			out, err := Deparse(res.Tree)
			if err != nil || out != g.deparse {
				t.Errorf("Deparse = %q, %v; want %q", out, err, g.deparse)
			}
			fp, err := Fingerprint(g.sql)
			if err != nil || fp != g.fingerprint {
				t.Errorf("Fingerprint = %q, %v; want %q", fp, err, g.fingerprint)
			}
			// The deparsed text must parse to the same tree.
			again, err := Parse(out)
			if err != nil {
				t.Fatalf("re-parse of %q: %v", out, err)
			}
			if !proto.Equal(stripLocations(res.Tree), stripLocations(again.Tree)) {
				t.Errorf("round trip changed the tree:\n%v\n%v", res.Tree, again.Tree)
			}
		})
	}
}

// stripLocations clears every location/len field so trees can be compared
// after whitespace changes.
func stripLocations(m proto.Message) proto.Message {
	c := proto.Clone(m)
	clearLocations(c.ProtoReflect())
	return c
}

func clearLocations(m interface {
	Range(func(protoFieldDescriptor, protoValue) bool)
	Clear(protoFieldDescriptor)
}) {
	m.Range(func(fd protoFieldDescriptor, v protoValue) bool {
		name := string(fd.Name())
		if name == "location" || name == "stmt_location" || name == "stmt_len" {
			m.Clear(fd)
			return true
		}
		switch {
		case fd.IsList() && fd.Message() != nil:
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				clearLocations(l.Get(i).Message())
			}
		case fd.Message() != nil && !fd.IsMap():
			clearLocations(v.Message())
		}
		return true
	})
}

func TestFingerprintIgnoresLiteralsCaseAndWhitespace(t *testing.T) {
	a, _ := Fingerprint("select * from t where a = 1")
	b, _ := Fingerprint("SELECT  *  FROM t WHERE a = 'zz'")
	c, _ := Fingerprint("select * from t where b = 1")
	if a != b {
		t.Errorf("fingerprint should be stable: %s != %s", a, b)
	}
	if a == c {
		t.Errorf("different columns must differ: %s", a)
	}
}

func TestNormalize(t *testing.T) {
	n, err := Normalize("select * from t where a = 1 and b = 'x'")
	if err != nil || n != "select * from t where a = $1 and b = $2" {
		t.Fatalf("n=%q err=%v", n, err)
	}
}

func TestSyntaxErrorsSurfaceWithSQLStateAndPosition(t *testing.T) {
	cases := []struct {
		sql string
		msg string
		pos int
	}{
		{"SELECT 1 FROM t LIMIT 1 x", `syntax error at or near "x"`, 25},
		{"SELECT ) 1", `syntax error at or near ")"`, 8},
		{"SELECT 1 FROM", "syntax error at end of input", 14},
		{"CREATE TABLE t (a int GENERATED ALWAYS AS (1) STORED VIRTUAL)", `syntax error at or near "VIRTUAL"`, 54},
	}
	for _, c := range cases {
		for name, fn := range map[string]func(string) error{
			"Parse":       func(s string) error { _, err := Parse(s); return err },
			"Fingerprint": func(s string) error { _, err := Fingerprint(s); return err },
			"Normalize":   func(s string) error { _, err := Normalize(s); return err },
		} {
			err := fn(c.sql)
			var pe *Error
			if !errors.As(err, &pe) {
				t.Fatalf("%s(%q) err = %v, want *Error", name, c.sql, err)
			}
			if pe.SQLState != SyntaxErrorSQLState || pe.Message != c.msg || pe.Position != c.pos {
				t.Errorf("%s(%q) = %+v, want %q at %d", name, c.sql, *pe, c.msg, c.pos)
			}
			if !strings.Contains(err.Error(), "SQLSTATE 42601") {
				t.Errorf("Error() = %q lacks SQLSTATE", err.Error())
			}
		}
	}
}

func TestDeparseRejectsForeignTree(t *testing.T) {
	res, _ := Parse("SELECT 1")
	_, err := Deparse(res.Stmts[0].RawStmt)
	var pe *Error
	if !errors.As(err, &pe) || pe.SQLState != InternalErrorSQLState {
		t.Fatalf("err = %v", err)
	}
}

func TestStmtLocations(t *testing.T) {
	res, err := Parse("SELECT 1;  UPDATE t SET a = 2")
	if err != nil {
		t.Fatal(err)
	}
	// PostgreSQL reports stmt_len 0 for the final statement (runs to end of input).
	if res.Stmts[0].Location != 0 || res.Stmts[0].Length != 8 || res.Stmts[1].Location != 11 || res.Stmts[1].Length != 0 {
		t.Fatalf("stmts = %+v", res.Stmts)
	}
}

type countingMetrics struct{ hits, misses, evicted, bytes int }

func (m *countingMetrics) CacheHit()            { m.hits++ }
func (m *countingMetrics) CacheMiss()           { m.misses++ }
func (m *countingMetrics) CacheEvicted(n int)   { m.evicted = n }
func (m *countingMetrics) CacheLiveBytes(n int) { m.bytes = n }

func TestParserLimits(t *testing.T) {
	p := New(Options{MaxSQLBytes: 30, MaxStatements: 2})
	_, err := p.Parse(context.Background(), "SELECT 1 FROM a_long_table_name")
	var pe *Error
	if !errors.As(err, &pe) || pe.SQLState != ProgramLimitExceededSQLState || !strings.Contains(pe.Message, "exceeds limit of 30") {
		t.Fatalf("size limit: %v", err)
	}
	if _, err := p.Parse(context.Background(), "SELECT 1;SELECT 2"); err != nil {
		t.Fatalf("at limit: %v", err)
	}
	_, err = p.Parse(context.Background(), "SELECT 1;SELECT 2;SELECT 3")
	if !errors.As(err, &pe) || pe.SQLState != ProgramLimitExceededSQLState || !strings.Contains(pe.Message, "3 statements") {
		t.Fatalf("statement limit: %v", err)
	}
	unlimited := New(Options{MaxSQLBytes: -1, MaxStatements: -1})
	if _, err := unlimited.Parse(context.Background(), strings.Repeat("SELECT 1;", 2000)); err != nil {
		t.Fatalf("unlimited: %v", err)
	}
	if got := New(Options{}); got.maxSQLBytes != DefaultMaxSQLBytes || got.maxStatements != DefaultMaxStatements || got.cache == nil {
		t.Fatalf("defaults not applied: %+v", got)
	}
}

func TestParserContextCancellation(t *testing.T) {
	p := New(Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Parse(ctx, "SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := p.Parse(ctx, "SELECT 1"); err != nil {
		t.Fatalf("live ctx: %v", err)
	}
}

func TestParserCache(t *testing.T) {
	m := &countingMetrics{}
	p := New(Options{CacheEntries: 2, CacheBytes: 1 << 20, Metrics: m})
	ctx := context.Background()
	// The first sighting only opens the door; the second is admitted and
	// the third is served from the cache.
	_, _ = p.Parse(ctx, "SELECT 1")
	if p.cache.len() != 0 {
		t.Fatal("a statement seen once must not be admitted")
	}
	first, _ := p.Parse(ctx, "SELECT 1")
	second, _ := p.Parse(ctx, "SELECT 1")
	if first != second {
		t.Fatal("cache hit must return the same result")
	}
	if m.hits != 1 || m.misses != 2 {
		t.Fatalf("hits=%d misses=%d", m.hits, m.misses)
	}
	if _, err := p.Parse(ctx, "SELEC 1"); err == nil {
		t.Fatal("want error")
	}
	if p.cache.len() != 1 {
		t.Fatalf("errors must not be cached, len=%d", p.cache.len())
	}
	_, _ = p.Parse(ctx, "SELECT 2")
	if _, _ = p.Parse(ctx, "SELECT 2"); p.cache.len() != 2 {
		t.Fatalf("len=%d", p.cache.len())
	}
	if _, _ = p.Parse(ctx, "SELECT 1"); m.hits != 2 {
		t.Fatalf("hits=%d", m.hits)
	}
	_, _ = p.Parse(ctx, "SELECT 3")
	if _, _ = p.Parse(ctx, "SELECT 3"); p.cache.len() != 2 {
		t.Fatalf("len=%d", p.cache.len())
	}
	if m.evicted != 1 || m.bytes <= 0 {
		t.Fatalf("evicted=%d live bytes=%d", m.evicted, m.bytes)
	}
	if _, ok := p.cache.get("SELECT 2"); ok {
		t.Fatal("least recently used entry should have been evicted")
	}
	if _, ok := p.cache.get("SELECT 1"); !ok {
		t.Fatal("recently used entry should survive")
	}
	if _, err := New(Options{}).Parse(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if New(Options{CacheEntries: -1}).cache != nil {
		t.Fatal("negative entries must disable the cache")
	}
}

// twice admits key through the doorkeeper, which holds a statement out of
// the cache until it has been seen a second time.
func twice(c *lru, key string, r *ParseResult, size int) {
	c.put(key, r, size)
	c.put(key, r, size)
}

func TestCacheBytesBound(t *testing.T) {
	c := newLRU(100, 100)
	r := &ParseResult{}
	twice(c, "a", r, 60)
	twice(c, "b", r, 60)
	if _, ok := c.get("a"); ok || c.len() != 1 || c.bytes != 60 {
		t.Fatalf("byte bound not enforced: len=%d bytes=%d", c.len(), c.bytes)
	}
	twice(c, "huge", r, 101)
	if _, ok := c.get("huge"); ok {
		t.Fatal("oversized entries must not be stored")
	}
	c.put("b", r, 30)
	if c.bytes != 30 || c.len() != 1 {
		t.Fatalf("resize: len=%d bytes=%d", c.len(), c.bytes)
	}
}

// TestCachedParseWeightCoversRealHeap: CacheBytes is a memory bound an
// operator sets, so what the cache charges an entry has to be at least
// what that entry actually holds. The serialized size alone is not: the
// wire encoding is compact, and the Go objects it becomes are structs,
// slices, maps and pointers.
func TestCachedParseWeightCoversRealHeap(t *testing.T) {
	const n = 2000
	shapes := map[string]func(int) string{
		"simple": func(i int) string { return fmt.Sprintf("SELECT a FROM t WHERE id = %d", i) },
		"join": func(i int) string {
			return fmt.Sprintf("SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.cid WHERE o.id = %d", i)
		},
		"insert": func(i int) string { return fmt.Sprintf("INSERT INTO t (a,b,c) VALUES (%d,%d,%d)", i, i, i) },
	}
	for name, gen := range shapes {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		held := make([]*ParseResult, 0, n)
		charged := 0
		for i := range n {
			sql := gen(i)
			r, err := Parse(sql)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			held = append(held, r)
			charged += astWeight(sql, r.Tree)
		}
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		live := int(after.HeapAlloc - before.HeapAlloc)
		runtime.KeepAlive(held)
		if charged < live {
			t.Errorf("%s: charged %d bytes for %d bytes of heap; a cache bound that undercounts is not a bound", name, charged, live)
		}
	}
}

// TestOneHitStatementsCannotEvictAReusedPlan: the cache admitted every
// miss, so a stream of literal-varying statements -- each parsed once and
// never seen again -- pushed out the prepared statement the workload
// actually reuses.
func TestOneHitStatementsCannotEvictAReusedPlan(t *testing.T) {
	p := New(Options{CacheEntries: 8, CacheBytes: 1 << 20})
	ctx := context.Background()
	const hot = "SELECT * FROM orders WHERE id = $1"
	for range 2 {
		if _, err := p.Parse(ctx, hot); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 200 {
		if _, err := p.Parse(ctx, fmt.Sprintf("SELECT * FROM orders WHERE id = %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := p.cache.get(hot); !ok {
		t.Fatalf("the reused statement was evicted by %d one-hit statements", 200)
	}
}

// TestMemoRunsOncePerParseResult: the parse cache hands the same result to
// every session that sends the same SQL, and a memo is how a whole-tree
// walk that would otherwise run per statement runs per distinct statement.
func TestMemoRunsOncePerParseResult(t *testing.T) {
	res := &ParseResult{}
	var runs int32
	compute := func() any { atomic.AddInt32(&runs, 1); return "answer" }

	var wg sync.WaitGroup
	got := make([]any, 8)
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i] = res.Memo(compute)
		}(i)
	}
	wg.Wait()
	if n := atomic.LoadInt32(&runs); n != 1 {
		t.Errorf("compute ran %d times, want once", n)
	}
	for i, v := range got {
		if v != "answer" {
			t.Errorf("caller %d saw %v", i, v)
		}
	}
	// A different result computes its own.
	if other := (&ParseResult{}).Memo(compute); other != "answer" || atomic.LoadInt32(&runs) != 2 {
		t.Errorf("a second parse result must compute its own: %v runs=%d", other, runs)
	}
}
