package plan

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

func be64(v int64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, uint64(v)); return b }
func be32(v int32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, uint32(v)); return b }
func be16(v int16) []byte { b := make([]byte, 2); binary.BigEndian.PutUint16(b, uint16(v)); return b }

func TestDecodeShardKey(t *testing.T) {
	cases := []struct {
		name   string
		oid    uint32
		hint   TypeHint
		format int16
		raw    []byte
		want   any
		err    error
		fails  bool
	}{
		{name: "int8 text", oid: oidInt8, raw: []byte("42"), want: int64(42)},
		{name: "int4 text", oid: oidInt4, raw: []byte(" 7 "), want: int64(7)},
		{name: "int2 text negative", oid: oidInt2, raw: []byte("-3"), want: int64(-3)},
		{name: "int8 text garbage", oid: oidInt8, raw: []byte("x"), fails: true},
		{name: "text", oid: oidText, raw: []byte("acme"), want: "acme"},
		{name: "varchar numeric string stays text", oid: oidVarchar, raw: []byte("123"), want: "123"},
		{name: "unknown text non-numeric", oid: 0, raw: []byte("acme"), want: "acme"},
		{name: "unknown text numeric is ambiguous", oid: 0, raw: []byte("123"), err: ErrAmbiguousKey},
		{name: "unknown oid 705 numeric is ambiguous", oid: oidUnknown, raw: []byte("123"), err: ErrAmbiguousKey},
		{name: "unknown text numeric with int hint", oid: 0, hint: HintInt, raw: []byte("123"), want: int64(123)},
		{name: "unknown text numeric with text hint", oid: 0, hint: HintText, raw: []byte("123"), want: "123"},
		{name: "declared type beats hint", oid: oidText, hint: HintInt, raw: []byte("123"), want: "123"},
		{name: "int8 binary", oid: oidInt8, format: 1, raw: be64(-99), want: int64(-99)},
		{name: "int4 binary", oid: oidInt4, format: 1, raw: be32(5), want: int64(5)},
		{name: "int2 binary", oid: oidInt2, format: 1, raw: be16(-2), want: int64(-2)},
		{name: "int8 binary wrong length", oid: oidInt8, format: 1, raw: be32(5), fails: true},
		{name: "unknown binary 8 bytes", oid: 0, format: 1, raw: be64(77), want: int64(77)},
		{name: "unknown binary 4 bytes", oid: 0, format: 1, raw: be32(77), want: int64(77)},
		{name: "unknown binary 2 bytes", oid: 0, format: 1, raw: be16(77), want: int64(77)},
		{name: "unknown binary other length", oid: 0, format: 1, raw: []byte("abc"), err: ErrAmbiguousKey},
		{name: "text binary", oid: oidText, format: 1, raw: []byte("acme"), want: "acme"},
		{name: "unknown binary with text hint", oid: 0, hint: HintText, format: 1, raw: []byte("12345678"), want: "12345678"},
		{name: "unsupported binary type", oid: 1700, format: 1, raw: be64(1), fails: true},
		{name: "unsupported text type", oid: 1700, raw: []byte("1.5"), fails: true},
		{name: "bpchar text is refused", oid: oidBpchar, raw: []byte("ab "), err: errBlankPaddedKey},
		{name: "bpchar binary is refused", oid: oidBpchar, format: 1, raw: []byte("ab "), err: errBlankPaddedKey},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := DecodeShardKey(c.oid, c.hint, c.format, c.raw)
			switch {
			case c.err != nil:
				if !errors.Is(err, c.err) {
					t.Fatalf("err = %v, want %v", err, c.err)
				}
			case c.fails:
				if err == nil {
					t.Fatalf("expected an error, got %v", got)
				}
			default:
				if err != nil || got != c.want {
					t.Fatalf("got %v (%T), %v; want %v (%T)", got, got, err, c.want, c.want)
				}
			}
		})
	}
}

func TestBindParamsFormatsAndNulls(t *testing.T) {
	b := BindParams{OIDs: []uint32{oidInt8, 0}, Formats: []int16{0, 1}, Values: [][]byte{[]byte("42"), be64(7)}}
	if v, err := b.ShardKey(1, HintNone); err != nil || v != int64(42) {
		t.Fatalf("$1 = %v, %v", v, err)
	}
	if v, err := b.ShardKey(2, HintNone); err != nil || v != int64(7) {
		t.Fatalf("$2 = %v, %v", v, err)
	}
	if _, err := b.ShardKey(3, HintNone); err == nil {
		t.Fatal("unbound parameter must fail")
	}
	if _, err := b.ShardKey(0, HintNone); err == nil {
		t.Fatal("$0 must fail")
	}
	all := BindParams{Formats: []int16{1}, Values: [][]byte{be64(1), be64(2)}}
	if v, err := all.ShardKey(2, HintNone); err != nil || v != int64(2) {
		t.Fatalf("single format applies to all: %v, %v", v, err)
	}
	null := BindParams{Values: [][]byte{nil}}
	if _, err := null.ShardKey(1, HintNone); err == nil {
		t.Fatal("NULL shard key must fail")
	}
}

func TestResolveUsesCastHints(t *testing.T) {
	snap := fixture(t)
	p := New()
	pl, err := p.Plan(context.Background(), session(snap), "select * from docs where slug = $1::text")
	if err != nil || !pl.Deferred {
		t.Fatalf("plan: %+v %v", pl, err)
	}
	got, err := pl.Resolve(BindParams{Values: [][]byte{[]byte("123")}})
	if err != nil {
		t.Fatal(err)
	}
	if w := shardOf(t, snap, "123"); len(got.Shards) != 1 || got.Shards[0] != w || got.ShardKeyValues[0] != "123" {
		t.Fatalf("resolved %+v, want shard %d of text 123", got, w)
	}
	pl, err = p.Plan(context.Background(), session(snap), "select * from docs where slug = $1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pl.Resolve(BindParams{Values: [][]byte{[]byte("123")}}); err == nil || !contains([]string{"0A000: parameter $1 cannot be a shard key: value is untyped and looks numeric: cast it to int8 or text"}, err.Error()) {
		t.Fatalf("ambiguous untyped parameter must be refused: %v", err)
	}
	pl, err = p.Plan(context.Background(), session(snap), "select * from orders where tenant_id = $1")
	if err != nil {
		t.Fatal(err)
	}
	got, err = pl.Resolve(BindParams{OIDs: []uint32{oidInt8}, Values: [][]byte{[]byte("42")}})
	if err != nil {
		t.Fatal(err)
	}
	if w := shardOf(t, snap, int64(42)); got.Shards[0] != w || got.Kind != EqualUnique || got.Deferred {
		t.Fatalf("resolved %+v, want shard %d", got, w)
	}
	again, err := got.Resolve(BindParams{})
	if err != nil || again.Shards[0] != got.Shards[0] {
		t.Fatalf("resolving a resolved plan must be a no-op: %+v %v", again, err)
	}
}

func TestPlanRefusesUnknownDatabaseDefaults(t *testing.T) {
	snap := fixture(t)
	db := snap.Databases[fixtureDB]
	db.DefaultPlacement = "sharded"
	snap.Databases[fixtureDB] = db
	_, err := New().Plan(context.Background(), session(snap), "select * from undeclared")
	if err == nil || !contains([]string{`0A000: table "undeclared" is not declared in the catalog and the database defaults to sharded placement`}, err.Error()) {
		t.Fatalf("got %v", err)
	}
	db.DefaultPlacement = "reference"
	snap.Databases[fixtureDB] = db
	pl, err := New().Plan(context.Background(), session(snap), "select * from undeclared")
	if err != nil || pl.Kind != Reference {
		t.Fatalf("reference default: %+v %v", pl, err)
	}
	pl, err = New().Plan(context.Background(), Session{Database: fixtureDB}, "select * from orders where tenant_id = 1")
	if err != nil || pl.Kind != Unsharded || pl.Generation != 0 {
		t.Fatalf("nil snapshot plans onto home: %+v %v", pl, err)
	}
}

func TestSearchPathAndReferenceSpread(t *testing.T) {
	snap := fixture(t)
	sess := session(snap)
	sess.SearchPath = []string{"audit", "public"}
	pl, err := New().Plan(context.Background(), sess, "select * from events where tenant_id = 1")
	if err != nil || pl.Kind != EqualUnique {
		t.Fatalf("search_path lookup: %+v %v", pl, err)
	}
	// System schemas ahead of user schemas must be skipped, not treated as
	// "found on the home shard": a common hardening search_path.
	sess.SearchPath = []string{"pg_catalog", "public"}
	pl, err = New().Plan(context.Background(), sess, "insert into orders (tenant_id, id) values (7, 1)")
	if err != nil || pl.Kind != EqualUnique {
		t.Fatalf("pg_catalog first in search_path must still find public.orders: %+v %v", pl, err)
	}
	pl, err = New().Plan(context.Background(), sess, "select * from pg_catalog.pg_class")
	if err != nil || pl.Kind != Unsharded {
		t.Fatalf("explicit pg_catalog qualifier resolves to the home shard: %+v %v", pl, err)
	}
	seen := map[int32]bool{}
	for id := uint64(0); id < 8; id++ {
		sess := session(snap)
		sess.ID = id
		pl, err := New().Plan(context.Background(), sess, "select * from regions")
		if err != nil || pl.Kind != Reference || len(pl.Shards) != 1 {
			t.Fatalf("reference read: %+v %v", pl, err)
		}
		seen[pl.Shards[0]] = true
	}
	if len(seen) != 4 {
		t.Fatalf("reference reads used shards %v, want all four", seen)
	}
}

func TestKindString(t *testing.T) {
	if Scatter.String() != "Scatter" || Kind(99).String() != "Kind(99)" {
		t.Fatal("Kind.String")
	}
}

func TestSearchPathClassification(t *testing.T) {
	snap := fixture(t)
	cases := []struct {
		sql  string
		want []string
		err  string
	}{
		{sql: "set search_path = audit, public", want: []string{"audit", "public"}},
		{sql: `set search_path to "Audit", 'x, y'`, want: []string{"Audit", "x", "y"}},
		{sql: "set search_path to default", want: nil},
		{sql: "reset search_path", want: nil},
		{sql: "reset all", want: nil},
		{sql: "set local search_path = audit", err: "SET LOCAL search_path"},
		{sql: "set search_path from current", err: "FROM CURRENT"},
	}
	for _, c := range cases {
		pl, err := New().Plan(context.Background(), session(snap), c.sql)
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Fatalf("%s: err = %v, want %q", c.sql, err, c.err)
			}
			continue
		}
		if err != nil || !pl.Class.SetGUC {
			t.Fatalf("%s: %+v %v", c.sql, pl, err)
		}
		if !slices.Equal(pl.Class.SearchPath, c.want) || (pl.Class.SearchPath == nil) != (c.want == nil) {
			t.Fatalf("%s: search path %q, want %q", c.sql, pl.Class.SearchPath, c.want)
		}
	}
}

// TestControlPlaneGUCRefused: pgshard's own settings are how the control
// plane tells a shard that a session is its own - the placement write fence
// reads one. A client able to set them could exempt itself from a fence
// that exists to refuse it, so the whole namespace is closed.
func TestControlPlaneGUCRefused(t *testing.T) {
	snap := fixture(t)
	for _, sql := range []string{
		"set pgshard.maintenance = 'on'",
		"set local pgshard.maintenance = 'on'",
		"SET PGSHARD.MAINTENANCE TO 'on'",
		"select set_config('pgshard.maintenance', 'on', false)",
		"select pg_catalog.set_config('pgshard.maintenance', 'on', true)",
		"alter role app set pgshard.maintenance = 'on'",
		"set pgshard.anything = '1'",
	} {
		pl, err := New().Plan(context.Background(), session(snap), sql)
		if err == nil || pl.Kind != Refuse {
			t.Fatalf("%s: expected refusal, got %+v %v", sql, pl, err)
		}
		var pe *pgwire.Error
		if !errors.As(err, &pe) || pe.Code != pgwire.CodeInsufficientPrivilege {
			t.Fatalf("%s: want SQLSTATE %s, got %v", sql, pgwire.CodeInsufficientPrivilege, err)
		}
	}
	// The client-facing pgshard settings stay settable.
	for _, sql := range []string{
		"set work_mem = '64MB'",
		"set pgshard.ddl_async = on",
		"set pgshard.transaction_mode = 'single'",
	} {
		if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
			t.Fatalf("%s refused: %v", sql, err)
		}
	}
}

func TestProtectedDurabilityGUCRefused(t *testing.T) {
	snap := fixture(t)
	refuse := []string{
		"set synchronous_commit = off",
		"set synchronous_commit to remote_write",
		"set local synchronous_commit = off",
		"alter role app set synchronous_commit = off",
		"alter role app in database d set synchronous_commit to off",
		"alter role current_user set synchronous_commit = off",
		"alter role session_user set synchronous_commit to remote_write",
		"alter role all set synchronous_commit = local",
		"select set_config('synchronous_commit', 'off', true)",
		"update orders set note = set_config('synchronous_commit','off',true) where tenant_id = 1",
		"select pg_catalog.set_config('synchronous_commit', 'off', false)",
		"select set_config(note, 'off', true) from orders",
		`select U&"\0073et_config"('synchronous_commit','off',true)`,
		"select appdb.pg_catalog.set_config('synchronous_commit','off',true)",
		"update pg_settings set setting = 'off' where name = 'synchronous_commit'",
		"update pg_catalog.pg_settings set setting = 'off' where name = 'synchronous_commit'",
	}
	for _, sql := range refuse {
		pl, err := New().Plan(context.Background(), session(snap), sql)
		if err == nil || pl.Kind != Refuse {
			t.Fatalf("%s: expected refusal, got %+v %v", sql, pl, err)
		}
		var pe *pgwire.Error
		if !errors.As(err, &pe) || pe.Code != pgwire.CodeInsufficientPrivilege {
			t.Fatalf("%s: want SQLSTATE %s, got %v", sql, pgwire.CodeInsufficientPrivilege, err)
		}
	}
	// Restoring the forced-safe default, or touching an unprotected GUC, is allowed.
	allow := []string{
		"set work_mem = '64MB'",
		"reset all",
		"reset synchronous_commit",
		"set synchronous_commit to default",
		"set local synchronous_commit to default",
		"alter role app reset synchronous_commit",
		"alter role app reset all",
		"select set_config('statement_timeout', '5s', false)",
		"select set_config('work_mem', '64MB', true)",
	}
	for _, sql := range allow {
		if _, err := New().Plan(context.Background(), session(snap), sql); err != nil {
			t.Fatalf("%s: unexpected refusal: %v", sql, err)
		}
	}
}

// TestBarrierPauseGUCsRefused: the barrier pauses writes with
// default_transaction_read_only, and the router replays a client's SETs onto
// every shard session it opens -- so a client that turned either read-only
// GUC off wrote straight through a pause taken to make a cluster-consistent
// restore point, and the override followed the session to every shard.
func TestBarrierPauseGUCsRefused(t *testing.T) {
	snap := fixture(t)
	refuse := []string{
		"set default_transaction_read_only = off",
		"set transaction_read_only = off",
		"set local default_transaction_read_only to off",
		"set session transaction_read_only = false",
		"alter role app set default_transaction_read_only = off",
		"alter role all set default_transaction_read_only to off",
		"select set_config('default_transaction_read_only', 'off', false)",
		"select pg_catalog.set_config('transaction_read_only','off',true)",
		"update pg_settings set setting = 'off' where name = 'default_transaction_read_only'",
	}
	for _, sql := range refuse {
		pl, err := New().Plan(context.Background(), session(snap), sql)
		if err == nil || pl.Kind != Refuse {
			t.Fatalf("%s: expected refusal, got %+v %v", sql, pl, err)
		}
		var pe *pgwire.Error
		if !errors.As(err, &pe) || pe.Code != pgwire.CodeInsufficientPrivilege {
			t.Fatalf("%s: want SQLSTATE %s, got %v", sql, pgwire.CodeInsufficientPrivilege, err)
		}
	}
	// A client that wants a read-only transaction still has one: only
	// overriding the cluster's own setting is refused.
	if _, err := New().Plan(context.Background(), session(snap), "begin read only"); err != nil {
		t.Fatalf("BEGIN READ ONLY refused: %v", err)
	}
}

// TestBarrierPauseTransactionModesNeutralised: the same override spelled as
// a transaction mode. A statement that turns transaction_read_only off is
// the pause defeated whether it says so as a setting or as BEGIN READ WRITE,
// but refusing it would break every client whose pool calls setReadOnly
// (false) on the way back, so the mode is dropped and the cluster's own
// default put back instead.
func TestBarrierPauseTransactionModesNeutralised(t *testing.T) {
	snap := fixture(t)
	for sql, want := range map[string]string{
		"begin read write":                                                                     "BEGIN",
		"start transaction read write":                                                         "START TRANSACTION",
		"begin isolation level read committed read write":                                      "BEGIN ISOLATION LEVEL READ COMMITTED",
		"start transaction read write, isolation level serializable":                           "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		"set transaction read write":                                                           "SET transaction_read_only = DEFAULT",
		"set session characteristics as transaction read write":                                "SET default_transaction_read_only = DEFAULT",
		"set local transaction read write":                                                     "SET LOCAL transaction_read_only = DEFAULT",
		"set session characteristics as transaction isolation level read committed read write": "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
	} {
		pl, err := New().Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if pl.Rewritten != want {
			t.Errorf("%s\n rewrote to %q\n want      %q", sql, pl.Rewritten, want)
		}
		if strings.Contains(strings.ToUpper(pl.Rewritten), "READ WRITE") {
			t.Errorf("%s: the rewrite still declares READ WRITE: %q", sql, pl.Rewritten)
		}
	}
	// Anything that does not lift the pause is left exactly as it was.
	for _, sql := range []string{
		"begin",
		"start transaction",
		"begin read only",
		"start transaction isolation level repeatable read read only",
		"set session characteristics as transaction read only",
		"set transaction read only",
		"begin isolation level read committed",
		"commit",
	} {
		pl, err := New().Plan(context.Background(), session(snap), sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if pl.Rewritten != "" {
			t.Errorf("%s: rewritten to %q, want untouched", sql, pl.Rewritten)
		}
	}
}
