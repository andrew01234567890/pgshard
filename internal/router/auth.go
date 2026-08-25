package router

import (
	"context"
	"errors"
	"sync"
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

	mu       sync.Mutex
	roles    *snapshot.Roles
	loadedAt time.Time
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.roles == nil || c.now().Sub(c.loadedAt) > c.ttl {
		if err := c.reload(ctx); err != nil {
			return "", err
		}
	}
	if cred, ok := c.roles.Cred(user); ok {
		return c.admit(user, cred)
	}
	if err := c.reload(ctx); err != nil {
		return "", err
	}
	if cred, ok := c.roles.Cred(user); ok {
		return c.admit(user, cred)
	}
	return "", ErrUnknownRole
}

// admit refuses roles that must not log in; the 28000 error is relayed to
// the client as-is instead of the mock SCRAM exchange for unknown roles.
func (c *RoleCache) admit(user string, cred snapshot.RoleCred) (string, error) {
	if !cred.CanLogin {
		return "", pgwire.Errorf(pgwire.CodeInvalidAuthorization, "role %q is not permitted to log in", user)
	}
	if cred.ValidUntil != nil && !c.now().Before(*cred.ValidUntil) {
		return "", pgwire.Errorf(pgwire.CodeInvalidAuthorization, "password for role %q has expired", user)
	}
	return cred.Verifier, nil
}

// ConnectionLimit reports how many sessions user may hold open at once from
// the cache the authentication just populated; ok is false when the role is
// unknown or the limit is unlimited.
func (c *RoleCache) ConnectionLimit(user string) (int32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cred, ok := c.roles.Cred(user)
	if !ok || cred.ConnectionLimit < 0 {
		return 0, false
	}
	return cred.ConnectionLimit, true
}

// Refresh reloads the roles from the catalog.
func (c *RoleCache) Refresh(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reload(ctx)
}

// MayLogIn reports whether user may open a session right now. It answers
// from the cache as it stands rather than from what changed since the last
// look: Lookup reloads the cache on its own TTL and on every miss, so a
// caller watching for the moment a role flips would simply miss the ones
// an authentication attempt noticed first.
func (c *RoleCache) MayLogIn(user string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cred, ok := c.roles.Cred(user)
	if !ok {
		return false
	}
	_, err := c.admit(user, cred)
	return err == nil
}

func (c *RoleCache) reload(ctx context.Context) error {
	r, err := c.load(ctx)
	if err != nil {
		return err
	}
	c.roles, c.loadedAt = r, c.now()
	return nil
}
