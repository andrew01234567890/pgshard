package router

import (
	"context"
	"errors"
	"strings"
	"sync"
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
	// A second unknown name inside the same window must not reload again.
	// Every miss used to, so a burst of invalid usernames -- which anyone
	// who can reach the port can send -- turned into catalog traffic.
	if _, err := c.Lookup(ctx, "carol"); !errors.Is(err, ErrUnknownRole) || q.calls != 2 {
		t.Fatalf("unknown role reloaded inside the window: %v calls=%d", err, q.calls)
	}
	if _, err := c.Lookup(ctx, "dave"); !errors.Is(err, ErrUnknownRole) || q.calls != 2 {
		t.Fatalf("a third unknown role reloaded: %v calls=%d", err, q.calls)
	}
	q.roles["alice"] = "v3"
	if v, _ := c.Lookup(ctx, "alice"); v != "v1" {
		t.Fatalf("within TTL the old verifier is served, got %q", v)
	}
	now = now.Add(2 * time.Minute)
	if v, _ := c.Lookup(ctx, "alice"); v != "v3" {
		t.Fatalf("after TTL the verifier must refresh, got %q", v)
	}
	// Past the window an unknown name is worth looking again: a role
	// created since the last read has to become usable.
	q.roles["erin"] = "v5"
	now = now.Add(2 * time.Minute)
	before := q.calls
	if v, err := c.Lookup(ctx, "erin"); err != nil || v != "v5" {
		t.Fatalf("a role created since the last read must be found: %q %v", v, err)
	}
	if q.calls <= before {
		t.Errorf("finding a new role took no catalog read: calls %d -> %d", before, q.calls)
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
	// The refusal comes back with the role's verifier: pgwire runs the
	// exchange anyway and only relays the refusal to a client that proved
	// the password, so a caller who has proved nothing cannot tell a
	// disabled role from an absent one.
	assert28000 := func(user, want, verifier string) {
		t.Helper()
		v, err := c.Lookup(ctx, user)
		var pe *pgwire.Error
		if !errors.As(err, &pe) || pe.Code != pgwire.CodeInvalidAuthorization {
			t.Fatalf("%s: want 28000 refusal, got %v", user, err)
		}
		if !strings.Contains(pe.Message, want) {
			t.Fatalf("%s: message %q lacks %q", user, pe.Message, want)
		}
		if v != verifier {
			t.Fatalf("%s: verifier %q, want %q -- a refusal without one cannot run a real exchange", user, v, verifier)
		}
	}
	assert28000("batch", "not permitted to log in", "v1")
	assert28000("expired", "expired", "v2")
	if v, err := c.Lookup(ctx, "current"); err != nil || v != "v3" {
		t.Fatalf("valid_until in the future must pass: %q %v", v, err)
	}
	if v, err := c.Lookup(ctx, "eternal"); err != nil || v != "v4" {
		t.Fatalf("no valid_until must pass: %q %v", v, err)
	}
	now = future
	assert28000("current", "expired", "v3")
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

// TestRoleCacheLooksUpConcurrently: a lookup that finds the roles fresh
// must not take a lock, and a refresh must not serialize the logins that
// arrive during it. The cache held one mutex across its catalog round
// trip, so every login waited for it.
func TestRoleCacheLooksUpConcurrently(t *testing.T) {
	q := &fakeQuerier{roles: map[string]string{"alice": "v1"}}
	c := NewRoleCache(q, time.Minute)
	release := make(chan struct{})
	loads := make(chan struct{}, 16)
	inner := c.load
	c.load = func(ctx context.Context) (*snapshot.Roles, error) {
		loads <- struct{}{}
		<-release
		return inner(ctx)
	}
	ctx := context.Background()

	// Several logins arrive with nothing cached. One reads the catalog;
	// the others wait for it rather than opening their own.
	done := make(chan error, 4)
	for range 4 {
		go func() {
			_, err := c.Lookup(ctx, "alice")
			done <- err
		}()
	}
	<-loads
	select {
	case <-loads:
		t.Error("a second login opened its own catalog read")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for range 4 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	// With the roles published, a lookup does no catalog work at all.
	c.load = func(context.Context) (*snapshot.Roles, error) {
		t.Error("a fresh lookup read the catalog")
		return nil, errors.New("unexpected")
	}
	for range 8 {
		if v, err := c.Lookup(ctx, "alice"); err != nil || v != "v1" {
			t.Fatalf("cached lookup: %q %v", v, err)
		}
	}
}

// TestMaxSessionsCapsWhatOneLoginCanHold: the pre-authentication cap
// releases its slot the moment a session authenticates, and a role carries
// no connection limit unless one was set on it. Without a cap of its own
// one valid login could hold sessions until the router ran out of memory,
// taking every tenant on it down.
func TestMaxSessionsCapsWhatOneLoginCanHold(t *testing.T) {
	fp := newFakePooler()
	h := newHarnessWith(t, fp, startFakePooler(t, fp), func(cfg *Config) {
		cfg.MaxSessions = 2
	})
	var open []*pgx.Conn
	for i := range 2 {
		c, err := pgx.Connect(context.Background(), h.dsn("app", "secret", "app"))
		if err != nil {
			t.Fatalf("session %d of a cap of 2: %v", i, err)
		}
		open = append(open, c)
	}
	_, err := pgx.Connect(context.Background(), h.dsn("app", "secret", "app"))
	if sqlstate(err) != "53300" {
		t.Fatalf("third session past a cap of 2: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "too many clients already") {
		t.Errorf("the refusal must say what happened: %v", err)
	}

	// A closed session gives its slot back.
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
	for _, c := range open[1:] {
		_ = c.Close(context.Background())
	}
}

// TestMaxSessionsAdmitsConcurrentlyWithoutExceedingTheCap: admission reads
// the count and records the session in one critical section, so racing
// logins cannot both see room for the last slot.
func TestMaxSessionsAdmitsConcurrentlyWithoutExceedingTheCap(t *testing.T) {
	fp := newFakePooler()
	const capacity = 4
	h := newHarnessWith(t, fp, startFakePooler(t, fp), func(cfg *Config) {
		cfg.MaxSessions = capacity
	})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var opened []*pgx.Conn
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := pgx.Connect(context.Background(), h.dsn("app", "secret", "app"))
			if err != nil {
				return
			}
			mu.Lock()
			opened = append(opened, c)
			mu.Unlock()
		}()
	}
	wg.Wait()
	defer func() {
		for _, c := range opened {
			_ = c.Close(context.Background())
		}
	}()
	if len(opened) > capacity {
		t.Errorf("%d sessions admitted past a cap of %d", len(opened), capacity)
	}
	if len(opened) == 0 {
		t.Error("no session was admitted at all")
	}
}
