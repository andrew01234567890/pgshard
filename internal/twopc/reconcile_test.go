package twopc

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDecide(t *testing.T) {
	cases := []struct {
		state    string
		prepared bool
		status   XactStatus
		want     Action
	}{
		{StateCommit, true, "", Commit},
		{StateAbort, true, "", Rollback},
		{StatePreparing, true, "", Rollback},
		{"", true, "", Rollback},
		{StateCommit, false, StatusCommitted, Nothing},
		{StateCommit, false, StatusAborted, Contradiction},
		{StateCommit, false, StatusInProgress, Contradiction},
		{StateCommit, false, "", Contradiction},
		{StateAbort, false, "", Nothing},
		{StatePreparing, false, "", Nothing},
		{"", false, StatusCommitted, Nothing},
	}
	for _, c := range cases {
		if got := Decide(c.state, c.prepared, c.status); got != c.want {
			t.Errorf("Decide(%q, %v, %q) = %s, want %s", c.state, c.prepared, c.status, got, c.want)
		}
	}
}

// fakeConn is one participant: its pg_prepared_xacts view, the commit
// status of known transaction ids and the statements it ran.
type fakeConn struct {
	prepared []string
	status   map[string]XactStatus
	ran      []string
	failExec error
}

type fakeTag struct{}

func (fakeTag) RowsAffected() int64 { return 1 }

func (f *fakeConn) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	switch {
	case strings.Contains(sql, "pg_prepared_xacts"):
		var rows [][]any
		for _, g := range f.prepared {
			rows = append(rows, []any{g, "app"})
		}
		return &fakeRows{rows: rows}, nil
	case strings.Contains(sql, "pg_xact_status"):
		xid := args[0].(string)
		if xid == "future" {
			return nil, errors.New(`transaction ID 99 is in the future`)
		}
		if xid == "broken" {
			return nil, errors.New("connection lost")
		}
		return &fakeRows{rows: [][]any{{string(f.status[xid])}}}, nil
	}
	return nil, errors.New("unexpected query " + sql)
}

func (f *fakeConn) Exec(_ context.Context, sql string, _ ...any) (Tag, error) {
	f.ran = append(f.ran, sql)
	if f.failExec != nil {
		return nil, f.failExec
	}
	verb, rest, _ := strings.Cut(sql, " PREPARED '")
	gid := strings.TrimSuffix(rest, "'")
	if !slices.Contains(f.prepared, gid) {
		return nil, errors.New(verb + ": prepared transaction " + gid + " does not exist")
	}
	f.prepared = slices.DeleteFunc(f.prepared, func(g string) bool { return g == gid })
	return fakeTag{}, nil
}

// fakeRows serves single-column rows.
type fakeRows struct {
	rows [][]any
	i    int
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() (t pgconn.CommandTag)            { return }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *fakeRows) Next() bool                                   { r.i++; return r.i <= len(r.rows) }
func (r *fakeRows) Scan(dest ...any) error {
	for i, d := range dest {
		*(d.(*string)) = r.rows[r.i-1][i].(string)
	}
	return nil
}

func TestReconcileAppliesTheTable(t *testing.T) {
	decisions := []Decision{
		{GID: "pgshard-c1", State: StateCommit, Participants: []int32{0, 1}, ParticipantXIDs: []string{"10", "20"}},
		{GID: "pgshard-a1", State: StateAbort, Participants: []int32{0, 1}},
		{GID: "pgshard-p1", State: StatePreparing, Participants: []int32{0}},
		{GID: "pgshard-done", State: StateCommit, Participants: []int32{0}, ParticipantXIDs: []string{"30"}},
		{GID: "pgshard-lost", State: StateCommit, Participants: []int32{0}, ParticipantXIDs: []string{"40"}},
		{GID: "pgshard-noxid", State: StateCommit, Participants: []int32{0}},
		{GID: "pgshard-future", State: StateCommit, Participants: []int32{0}, ParticipantXIDs: []string{"future"}},
		{GID: "pgshard-other", State: StateCommit, Participants: []int32{1}, ParticipantXIDs: []string{"50"}},
	}
	conn := &fakeConn{prepared: []string{"pgshard-c1", "pgshard-a1", "pgshard-p1", "pgshard-orphan"},
		status: map[string]XactStatus{"30": StatusCommitted, "40": StatusAborted}}
	out, err := Reconcile(context.Background(), ConnParticipant{conn}, 0, decisions)
	if err != nil {
		t.Fatal(err)
	}
	if out.Committed != 1 || out.RolledBack != 3 {
		t.Fatalf("outcome %+v", out)
	}
	if want := []string{"pgshard-lost", "pgshard-noxid", "pgshard-future"}; !slices.Equal(out.Contradictions, want) {
		t.Fatalf("contradictions %v, want %v", out.Contradictions, want)
	}
	if len(conn.prepared) != 0 {
		t.Fatalf("left prepared: %v", conn.prepared)
	}
	if want := []string{"COMMIT PREPARED 'pgshard-c1'", "ROLLBACK PREPARED 'pgshard-a1'", "ROLLBACK PREPARED 'pgshard-p1'", "ROLLBACK PREPARED 'pgshard-orphan'"}; !slices.Equal(conn.ran, want) {
		t.Fatalf("ran %v", conn.ran)
	}
	err = Contradictions(map[int32]Outcome{1: {Contradictions: []string{"pgshard-z"}}, 0: out})
	if err == nil || !strings.HasPrefix(err.Error(), "two-phase reconciliation contradictions: shard 0: pgshard-lost is decided commit but is neither prepared nor committed; shard 0: pgshard-noxid") || !strings.HasSuffix(err.Error(), "shard 1: pgshard-z is decided commit but is neither prepared nor committed") {
		t.Fatalf("contradictions error %v", err)
	}
	if Contradictions(map[int32]Outcome{0: {Committed: 2}}) != nil {
		t.Fatal("clean outcome must not be an error")
	}
}

func TestReconcileReportsErrors(t *testing.T) {
	conn := &fakeConn{prepared: []string{"pgshard-c1"}, failExec: errors.New("read only")}
	_, err := Reconcile(context.Background(), ConnParticipant{conn}, 0, []Decision{{GID: "pgshard-c1", State: StateCommit, Participants: []int32{0}}})
	if err == nil || !strings.Contains(err.Error(), "commit prepared pgshard-c1: read only") {
		t.Fatalf("err %v", err)
	}
	conn = &fakeConn{}
	_, err = Reconcile(context.Background(), ConnParticipant{conn}, 0, []Decision{{GID: "pgshard-x", State: StateCommit, Participants: []int32{0}, ParticipantXIDs: []string{"broken"}}})
	if err == nil || !strings.Contains(err.Error(), "pg_xact_status(broken): connection lost") {
		t.Fatalf("err %v", err)
	}
}
