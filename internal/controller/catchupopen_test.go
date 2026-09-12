package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// peekRows is the two-column result of pg_logical_slot_peek_binary_changes.
type peekRows struct {
	lsn  []string
	data [][]byte
	i    int
}

func (r *peekRows) Close()                                       {}
func (r *peekRows) Err() error                                   { return nil }
func (r *peekRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *peekRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *peekRows) Next() bool                                   { r.i++; return r.i <= len(r.lsn) }
func (r *peekRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.lsn[r.i-1]
	*(dest[1].(*[]byte)) = r.data[r.i-1]
	return nil
}
func (r *peekRows) Values() ([]any, error) { return nil, nil }
func (r *peekRows) RawValues() [][]byte    { return nil }

// TypeMap is pgx 5.11's addition to the Rows interface. A fake that
// decodes nothing still has to answer it, and a default map is the
// honest answer: these rows carry values the caller reads directly.
func (r *peekRows) TypeMap() *pgtype.Map { return pgtype.NewMap() }
func (r *peekRows) Conn() *pgx.Conn      { return nil }

type peekConn struct {
	rows *peekRows
	last *peekRows // the peek this connection actually served
}

func (c *peekConn) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "peek_binary_changes") {
		c.last = &peekRows{lsn: c.rows.lsn, data: c.rows.data}
		return c.last, nil
	}
	// The WAL end catch-up reads BEFORE each peek, so its advance can never
	// pass a commit the peek did not see. It comes back as text.
	if strings.Contains(sql, "SELECT pg_current_wal_lsn()::text") {
		return &textRows{v: "0/1"}, nil
	}
	// Whatever else catch-up asks this connection is the slot lag, which
	// this test does not reach unless the bound failed to trip.
	return &lagRows{}, nil
}

// lagRows is the one-row bigint of slotLag.
type lagRows struct{ i int }

func (r *lagRows) Close()                                       {}
func (r *lagRows) Err() error                                   { return nil }
func (r *lagRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *lagRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *lagRows) Next() bool                                   { r.i++; return r.i <= 1 }
func (r *lagRows) Scan(dest ...any) error                       { *(dest[0].(*int64)) = 0; return nil }
func (r *lagRows) Values() ([]any, error)                       { return nil, nil }
func (r *lagRows) RawValues() [][]byte                          { return nil }

// TypeMap is pgx 5.11's addition to the Rows interface. A fake that
// decodes nothing still has to answer it, and a default map is the
// honest answer: these rows carry values the caller reads directly.
func (r *lagRows) TypeMap() *pgtype.Map                                      { return pgtype.NewMap() }
func (r *lagRows) Conn() *pgx.Conn                                           { return nil }
func (c *peekConn) Exec(context.Context, string, ...any) (CommandTag, error) { return nil, nil }
func (c *peekConn) Close(context.Context) error                              { return nil }

// The bound on the ONE transaction being decoded exists because those
// operations cannot be applied until it commits, so nothing can shorten
// them: a source transaction larger than the controller can hold has to be
// refused rather than allowed to take the process, and every other
// workflow with it.
//
// This drives catchUpSource with an uncommitted transaction of plain
// upserts, which is the ordinary case and the one whose size is carried on
// the tuple rather than on a rendered statement. An accounting that reads
// only the rendered form sees a transaction of zero bytes and never trips.
func TestAnOpenTransactionOfPlainUpsertsTripsTheOpenBound(t *testing.T) {
	defer func(v int) { catchUpMaxOpenBytes = v }(catchUpMaxOpenBytes)
	catchUpMaxOpenBytes = 4096

	cols := []string{"id", "tenant_id", "region_id", "note"}
	shape := rowShape{Schema: "public", Name: "orders", Columns: cols, PK: []string{"id"}}
	r := twoShardRouter(TablePlacement{Placement: "sharded", ShardKey: s("region_id")}, cols)

	var region *string
	for v := int64(1); ; v++ {
		if shardOf(t, v) == 0 {
			region = s(itoa(v))
			break
		}
	}
	big := strings.Repeat("x", 1024)
	rows := &peekRows{}
	add := func(lsn string, data []byte) { rows.lsn = append(rows.lsn, lsn); rows.data = append(rows.data, data) }
	add("0/1", relationMsg(7, cols...))
	add("0/2", (&msgBuilder{}).byte('B').u64(1).u64(2).u32(3).b)
	// Never a commit: this is one transaction that keeps growing.
	for i := range 8 {
		add("0/3", tuple((&msgBuilder{}).byte('I').u32(7).byte('N'), s(itoa(int64(i))), s("1"), region, s(big)).b)
	}

	wf := &placementWorkflow{
		id:    "0123456789abcdef",
		spec:  placementSpec{SchemaName: "public", TableName: "orders"},
		st:    placementState{Columns: cols, SourceSet: "a"},
		rt:    r,
		shape: shape,
	}
	conn := &peekConn{rows: rows}
	_, _, err := (&Placer{}).catchUpSource(context.Background(), wf, conn, targetConns{}, 0, false)
	if err == nil {
		t.Fatal("an open transaction of 8 KiB of rows did not trip a 4 KiB bound; its size is not being counted")
	}
	if !strings.Contains(err.Error(), "catch-up bound") {
		t.Fatalf("failed for another reason: %v", err)
	}
	// And it tripped BEFORE reading the whole peek. That is the point of
	// decoding the rows as they arrive: a peek returns whole transactions
	// however small a limit it is given, so a result that is collected
	// first is already resident by the time any bound is consulted, and
	// the bound saves nothing it was written to save.
	if conn.last.i >= len(rows.lsn) {
		t.Fatalf("the bound tripped only after reading all %d rows of the peek; the whole transaction was resident first", len(rows.lsn))
	}
}
