package router

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

type fakeRole struct {
	name, verifier string
	nologin        bool
	validUntil     *time.Time
}

type fakeRows struct {
	rows []fakeRole
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
	row := r.rows[r.i-1]
	*dest[0].(*string) = row.name
	*dest[1].(*string) = row.verifier
	*dest[2].(*bool) = !row.nologin
	*dest[3].(**time.Time) = row.validUntil
	return nil
}

type fakeQuerier struct {
	roles map[string]string
	attrs map[string]fakeRole
	calls int
	err   error
}

func (q *fakeQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	q.calls++
	if q.err != nil {
		return nil, q.err
	}
	var rows []fakeRole
	for k, v := range q.roles {
		row := q.attrs[k]
		row.name, row.verifier = k, v
		rows = append(rows, row)
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

func TestRoleCacheRefusesNologinAndExpired(t *testing.T) {
	now := time.Unix(1000, 0)
	past, future := now.Add(-time.Hour), now.Add(time.Hour)
	q := &fakeQuerier{
		roles: map[string]string{"batch": "v1", "expired": "v2", "current": "v3", "eternal": "v4"},
		attrs: map[string]fakeRole{
			"batch":   {nologin: true},
			"expired": {validUntil: &past},
			"current": {validUntil: &future},
		},
	}
	c := NewRoleCache(q, time.Minute)
	c.now = func() time.Time { return now }
	ctx := context.Background()
	assert28000 := func(user, want string) {
		t.Helper()
		_, err := c.Lookup(ctx, user)
		var pe *pgwire.Error
		if !errors.As(err, &pe) || pe.Code != pgwire.CodeInvalidAuthorization {
			t.Fatalf("%s: want 28000 refusal, got %v", user, err)
		}
		if !strings.Contains(pe.Message, want) {
			t.Fatalf("%s: message %q lacks %q", user, pe.Message, want)
		}
	}
	assert28000("batch", "not permitted to log in")
	assert28000("expired", "expired")
	if v, err := c.Lookup(ctx, "current"); err != nil || v != "v3" {
		t.Fatalf("valid_until in the future must pass: %q %v", v, err)
	}
	if v, err := c.Lookup(ctx, "eternal"); err != nil || v != "v4" {
		t.Fatalf("no valid_until must pass: %q %v", v, err)
	}
	now = future
	assert28000("current", "expired")
}

// TestConnectionLimitRefusesBeyondTheRolesAllowance: a role's
// connection_limit is a cluster setting, not documentation - once the role
// holds as many sessions as it may, the next one is refused rather than
// quietly admitted.
func TestConnectionLimitRefusesBeyondTheRolesAllowance(t *testing.T) {
	fp := newFakePooler()
	h := newHarnessWith(t, fp, startFakePooler(t, fp), func(cfg *Config) {
		cfg.RoleLimits = limiter(func(user string) (int32, bool) {
			if user == "app" {
				return 2, true
			}
			return 0, false
		})
	})
	var open []*pgx.Conn
	for i := range 2 {
		c, err := pgx.Connect(context.Background(), h.dsn("app", "secret", "app"))
		if err != nil {
			t.Fatalf("session %d of an allowance of 2: %v", i, err)
		}
		open = append(open, c)
	}
	if _, err := pgx.Connect(context.Background(), h.dsn("app", "secret", "app")); sqlstate(err) != "53300" {
		t.Fatalf("third session past a limit of 2: %v", err)
	}
	// Closing one frees the allowance again.
	if err := open[0].Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := pgx.Connect(context.Background(), h.dsn("app", "secret", "app"))
		if err == nil {
			_ = c.Close(context.Background())
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a closed session must free its slot: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type limiter func(string) (int32, bool)

func (f limiter) ConnectionLimit(user string) (int32, bool) { return f(user) }

// TestMayLogInAnswersFromCurrentState: Lookup reloads the cache on its own
// TTL and on every miss, so a check that watched for the moment a role
// flipped would miss every revocation an authentication attempt noticed
// first. The answer has to come from the roles as they stand.
func TestMayLogInAnswersFromCurrentState(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	rows := map[string]snapshot.RoleCred{
		"stays":   {Verifier: "v", CanLogin: true},
		"nologin": {Verifier: "v", CanLogin: true},
		"expires": {Verifier: "v", CanLogin: true},
		"dropped": {Verifier: "v", CanLogin: true},
	}
	c := &RoleCache{ttl: time.Hour, now: time.Now}
	c.load = func(context.Context) (*snapshot.Roles, error) { return snapshot.NewRoles(rows), nil }
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"stays", "nologin", "expires", "dropped"} {
		if !c.MayLogIn(user) {
			t.Fatalf("%s must be admitted before the change", user)
		}
	}
	rows = map[string]snapshot.RoleCred{
		"stays":   {Verifier: "v", CanLogin: true},
		"nologin": {Verifier: "v", CanLogin: false},
		"expires": {Verifier: "v", CanLogin: true, ValidUntil: &past},
	}
	// Something else refreshes the cache first - an authentication miss, or
	// the TTL expiring under Lookup. The verdict must not depend on who did.
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"nologin", "expires", "dropped"} {
		if c.MayLogIn(user) {
			t.Fatalf("%s may no longer log in", user)
		}
	}
	if !c.MayLogIn("stays") {
		t.Fatal("an untouched role must still be admitted")
	}
	if c.MayLogIn("never-existed") {
		t.Fatal("an unknown role may not log in")
	}
}
