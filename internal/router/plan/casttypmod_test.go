package plan

import (
	"context"
	"strings"
	"testing"
)

// PostgreSQL evaluates a cast before the comparison or the insert, so a
// length the router does not apply is a value the router does not route:
// 'abcdef'::varchar(3) is 'abc' on the shard, and hashing 'abcdef' here
// picks a different one. The SELECT then finds nothing and the INSERT goes
// to a shard whose range constraint rejects it.
//
// The column is plain text so that nothing but the cast can change the
// value: normaliseKey truncates for a varchar(n) COLUMN, which would hide
// what this is about.
func TestACastLengthIsAppliedBeforeTheKeyIsHashed(t *testing.T) {
	snap := varcharFixture(t, "text")
	truncated := shardOf(t, snap, "abc")
	whole := shardOf(t, snap, "abcdef")
	if truncated == whole {
		t.Fatal("fixture no longer separates 'abc' from 'abcdef'; the test proves nothing")
	}
	for _, sql := range []string{
		"select * from codes where code = 'abcdef'::varchar(3)",
		"select * from codes where code = 'abcdef'::character varying(3)",
		"select * from codes where code = 'abcdef'::char(3)",
		"select * from codes where code = 'abcdef'::bpchar(3)",
		// Nested: the inner cast truncates and the outer one only widens,
		// so the value that reaches the comparison is still 'abc'.
		"select * from codes where code = 'abcdef'::varchar(3)::text",
		"delete from codes where code = 'abcdef'::varchar(3)",
	} {
		if got := routeOf(t, snap, sql); len(got) != 1 || got[0] != truncated {
			t.Errorf("%s: routed to %v, want the shard of 'abc' (%d)", sql, got, truncated)
		}
	}
	// A length that does not truncate leaves the value alone.
	for _, sql := range []string{
		"select * from codes where code = 'abcdef'::varchar(6)",
		"select * from codes where code = 'abcdef'::varchar(99)",
		"select * from codes where code = 'abcdef'::text",
	} {
		if got := routeOf(t, snap, sql); len(got) != 1 || got[0] != whole {
			t.Errorf("%s: routed to %v, want the shard of 'abcdef' (%d)", sql, got, whole)
		}
	}
}

// A length this cannot evaluate exactly is refused rather than guessed: the
// statement scatters or is refused, which is slower or louder, but never
// routed to the wrong shard. A parameter is the same case -- its value
// arrives at Bind and nothing carries the length that far.
func TestALengthTheRouterCannotApplyIsNotRoutedOn(t *testing.T) {
	snap := varcharFixture(t, "text")
	// The control: without a length the parameter is a shard key, and the
	// plan defers its single shard to Bind.
	p, err := New().Plan(context.Background(), session(snap), "select * from codes where code = $1::text")
	if err != nil || p.Kind != EqualUnique || !p.Deferred {
		t.Fatalf("control: kind=%v deferred=%v err=%v, want a deferred EqualUnique", p.Kind, p.Deferred, err)
	}
	for _, sql := range []string{
		// The value arrives at Bind and nothing carries the length that
		// far, so it must not be routed on at all.
		"select * from codes where code = $1::varchar(3)",
		"select * from codes where code = 'abcdef'::text(3)",
	} {
		p, err := New().Plan(context.Background(), session(snap), sql)
		if err != nil {
			continue // refused outright is also fail-closed
		}
		if p.Kind == EqualUnique || len(p.Shards) == 1 {
			t.Errorf("%s: routed as %v to %v on a length it cannot apply", sql, p.Kind, p.Shards)
		}
	}
}

// The QUOTED "char" is one byte and nothing like character(n): unquoted char
// reaches the planner as bpchar with a typmod of 1, but `"char"` carries its
// length in the type itself and so had no typmod to apply. PostgreSQL
// evaluates 'abcdef'::"char" to 'a'; the router hashed the whole string and
// picked another shard.
func TestTheQuotedCharCastTakesOneByte(t *testing.T) {
	snap := varcharFixture(t, "text")
	first := shardOf(t, snap, "a")
	whole := shardOf(t, snap, "abcdef")
	if first == whole {
		t.Fatal("fixture no longer separates 'a' from 'abcdef'; the test proves nothing")
	}
	for _, sql := range []string{
		`select * from codes where code = 'abcdef'::"char"`,
		`select * from codes where code = 'abcdef'::pg_catalog."char"`,
		// Unquoted char is bpchar(1) and already routed correctly; it must
		// keep doing so.
		"select * from codes where code = 'abcdef'::char",
	} {
		if got := routeOf(t, snap, sql); len(got) != 1 || got[0] != first {
			t.Errorf("%s: routed to %v, want the shard of 'a' (%d)", sql, got, first)
		}
	}
}

// ::name truncates at 63 bytes. Doing that exactly means not splitting a
// multibyte character, so a value that could reach the limit is refused
// rather than guessed at; a shorter one is unaffected.
func TestANameCastIsRefusedOnlyWhenItCouldTruncate(t *testing.T) {
	snap := varcharFixture(t, "text")
	short := shardOf(t, snap, "abcdef")
	if got := routeOf(t, snap, "select * from codes where code = 'abcdef'::name"); len(got) != 1 || got[0] != short {
		t.Errorf("a short ::name was not routed: %v", got)
	}
	long := strings.Repeat("x", 70)
	p, err := New().Plan(context.Background(), session(snap), "select * from codes where code = '"+long+"'::name")
	if err == nil && p.Kind == EqualUnique {
		t.Error("a ::name longer than 63 bytes was routed on the untruncated value")
	}
}
