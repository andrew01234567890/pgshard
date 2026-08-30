package router

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// ErrUnknownRole is returned by RoleCache.Lookup for roles without a
// verifier; the SCRAM authenticator turns it into 28P01.
var ErrUnknownRole = errors.New("router: unknown role")

// RoleCache serves SCRAM verifiers from pgshard.roles, reloading them from
// the catalog when the cached copy is older than TTL or a lookup misses.
type RoleCache struct {
	q   catalog.Querier
	ttl time.Duration
	now func() time.Time
	// load reads the roles; the catalog by default.
	load func(context.Context) (*snapshot.Roles, error)

	// cur is the published roles. A lookup that finds them fresh takes no
	// lock at all: every login used to serialize on the mutex a refresh
	// held across its catalog round trip.
	cur atomic.Pointer[rolesAt]
	// mu admits one loader at a time; the rest wait for its result rather
	// than each opening the catalog.
	mu sync.Mutex
	// lastMiss is when an unknown role last drove a reload. A role that
	// was just created is worth one, but bounding them to one per ttl is
	// what stops a burst of invalid usernames turning into catalog
	// traffic -- an unauthenticated client should not be able to make the
	// router work.
	lastMiss time.Time
}

// rolesAt is one published set of roles and when it was read.
type rolesAt struct {
	roles *snapshot.Roles
	at    time.Time
}

// NewRoleCache builds a cache over q; ttl <= 0 means 5s.
func NewRoleCache(q catalog.Querier, ttl time.Duration) *RoleCache {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	c := &RoleCache{q: q, ttl: ttl, now: time.Now}
	c.load = func(ctx context.Context) (*snapshot.Roles, error) { return snapshot.LoadRoles(ctx, c.q) }
	return c
}

// Lookup implements pgwire.PasswordLookup.
func (c *RoleCache) Lookup(ctx context.Context, user string) (string, error) {
	cur, err := c.fresh(ctx)
	if err != nil {
		return "", err
	}
	if cred, ok := cur.roles.Cred(user); ok {
		return c.admit(user, cred)
	}
	cur, err = c.reloadForMiss(ctx)
	if err != nil {
		return "", err
	}
	if cur != nil {
		if cred, ok := cur.roles.Cred(user); ok {
			return c.admit(user, cred)
		}
	}
	return "", ErrUnknownRole
}

// fresh is the published roles, loaded when there are none or they have
// aged past the ttl.
func (c *RoleCache) fresh(ctx context.Context) (*rolesAt, error) {
	if cur := c.cur.Load(); cur != nil && c.now().Sub(cur.at) <= c.ttl {
		return cur, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur := c.cur.Load(); cur != nil && c.now().Sub(cur.at) <= c.ttl {
		return cur, nil
	}
	return c.loadLocked(ctx)
}

// reloadForMiss reads the catalog again because a role was not in the
// published set, which a role created moments ago would not be. It returns
// nil without reading when one already ran inside the ttl, so the work an
// unknown name can ask for is bounded however many arrive.
func (c *RoleCache) reloadForMiss(ctx context.Context) (*rolesAt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if !c.lastMiss.IsZero() && now.Sub(c.lastMiss) <= c.ttl {
		return nil, nil
	}
	c.lastMiss = now
	return c.loadLocked(ctx)
}

// admit refuses roles that must not log in. The verifier comes back with
// the refusal so the SCRAM exchange still runs: answering "role %q is not
// permitted to log in" before the client has proved anything tells an
// unauthenticated caller that the role exists, while an unknown role gets
// the mock exchange. PostgreSQL draws the same distinction only after
// authentication.
func (c *RoleCache) admit(user string, cred snapshot.RoleCred) (string, error) {
	if !cred.CanLogin {
		return cred.Verifier, pgwire.Errorf(pgwire.CodeInvalidAuthorization, "role %q is not permitted to log in", user)
	}
	if cred.ValidUntil != nil && !c.now().Before(*cred.ValidUntil) {
		return cred.Verifier, pgwire.Errorf(pgwire.CodeInvalidAuthorization, "password for role %q has expired", user)
	}
	return cred.Verifier, nil
}

// ConnectionLimit reports how many sessions user may hold open at once from
// the cache the authentication just populated; ok is false when the role is
// unknown or the limit is unlimited.
func (c *RoleCache) ConnectionLimit(user string) (int32, bool) {
	cur := c.cur.Load()
	if cur == nil {
		return 0, false
	}
	cred, ok := cur.roles.Cred(user)
	if !ok || cred.ConnectionLimit < 0 {
		return 0, false
	}
	return cred.ConnectionLimit, true
}

// Refresh reloads the roles from the catalog.
func (c *RoleCache) Refresh(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.loadLocked(ctx)
	return err
}

// MayLogIn reports whether user may open a session right now. It answers
// from the cache as it stands rather than from what changed since the last
// look: Lookup reloads the cache on its own TTL and on every miss, so a
// caller watching for the moment a role flips would simply miss the ones
// an authentication attempt noticed first.
func (c *RoleCache) MayLogIn(user string) bool {
	cur := c.cur.Load()
	if cur == nil {
		return false
	}
	cred, ok := cur.roles.Cred(user)
	if !ok {
		return false
	}
	_, err := c.admit(user, cred)
	return err == nil
}

// loadLocked reads the roles and publishes them; call with mu held.
func (c *RoleCache) loadLocked(ctx context.Context) (*rolesAt, error) {
	r, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	cur := &rolesAt{roles: r, at: c.now()}
	c.cur.Store(cur)
	return cur, nil
}
