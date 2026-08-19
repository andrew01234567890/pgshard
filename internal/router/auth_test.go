package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeRows struct {
	rows [][2]string
	i    int
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Next() bool                                   { r.i++; return r.i <= len(r.rows) }
func (r *fakeRows) Values() ([]any, error)                       { return nil, nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) Scan(dest ...any) error {
	*dest[0].(*string) = r.rows[r.i-1][0]
	*dest[1].(*string) = r.rows[r.i-1][1]
	return nil
}

type fakeQuerier struct {
	roles map[string]string
	calls int
	err   error
}

func (q *fakeQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	q.calls++
	if q.err != nil {
		return nil, q.err
	}
	var rows [][2]string
	for k, v := range q.roles {
		rows = append(rows, [2]string{k, v})
	}
	return &fakeRows{rows: rows}, nil
}

func TestRoleCacheReloadsOnMissAndTTL(t *testing.T) {
	q := &fakeQuerier{roles: map[string]string{"alice": "v1"}}
	c := NewRoleCache(q, time.Minute)
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	ctx := context.Background()
	if v, err := c.Lookup(ctx, "alice"); err != nil || v != "v1" || q.calls != 1 {
		t.Fatalf("first lookup: %q %v calls=%d", v, err, q.calls)
	}
	if v, err := c.Lookup(ctx, "alice"); err != nil || v != "v1" || q.calls != 1 {
		t.Fatalf("cached lookup must not hit the catalog: %q %v calls=%d", v, err, q.calls)
	}
	q.roles["bob"] = "v2"
	if v, err := c.Lookup(ctx, "bob"); err != nil || v != "v2" || q.calls != 2 {
		t.Fatalf("miss must reload once: %q %v calls=%d", v, err, q.calls)
	}
	if _, err := c.Lookup(ctx, "carol"); !errors.Is(err, ErrUnknownRole) || q.calls != 3 {
		t.Fatalf("unknown role: %v calls=%d", err, q.calls)
	}
	q.roles["alice"] = "v3"
	if v, _ := c.Lookup(ctx, "alice"); v != "v1" {
		t.Fatalf("within TTL the old verifier is served, got %q", v)
	}
	now = now.Add(2 * time.Minute)
	if v, _ := c.Lookup(ctx, "alice"); v != "v3" {
		t.Fatalf("after TTL the verifier must refresh, got %q", v)
	}
	q.err = errors.New("catalog down")
	now = now.Add(2 * time.Minute)
	if _, err := c.Lookup(ctx, "alice"); err == nil || errors.Is(err, ErrUnknownRole) {
		t.Fatalf("catalog failure must surface, got %v", err)
	}
}
