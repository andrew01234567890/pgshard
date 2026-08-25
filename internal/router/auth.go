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

	mu       sync.Mutex
	roles    *snapshot.Roles
	loadedAt time.Time
}

// NewRoleCache builds a cache over q; ttl <= 0 means 5s.
func NewRoleCache(q catalog.Querier, ttl time.Duration) *RoleCache {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	return &RoleCache{q: q, ttl: ttl, now: time.Now}
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

func (c *RoleCache) reload(ctx context.Context) error {
	r, err := snapshot.LoadRoles(ctx, c.q)
	if err != nil {
		return err
	}
	c.roles, c.loadedAt = r, c.now()
	return nil
}
