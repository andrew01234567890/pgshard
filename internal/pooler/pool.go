package pooler

import (
	"context"
	"errors"
	"sync"
	"time"
)

// PoolConfig bounds the backends a pooler opens against its shard.
type PoolConfig struct {
	// MaxBackends caps backends across all roles.
	MaxBackends int
	// MaxPerRole caps backends any one role may hold, so a hot role cannot
	// starve the others. Must be <= MaxBackends.
	MaxPerRole int
	// MaxLifetime retires a backend after this age; zero means never.
	MaxLifetime time.Duration
	// MaxIdleTime closes an idle backend after this long unused; zero means never.
	MaxIdleTime time.Duration
	// AcquireTimeout bounds waiting for a budget slot; zero means 5s.
	AcquireTimeout time.Duration
}

func (c PoolConfig) withDefaults() PoolConfig {
	if c.MaxBackends <= 0 {
		c.MaxBackends = 100
	}
	if c.MaxPerRole <= 0 || c.MaxPerRole > c.MaxBackends {
		c.MaxPerRole = c.MaxBackends
	}
	if c.AcquireTimeout <= 0 {
		c.AcquireTimeout = 5 * time.Second
	}
	return c
}

// ErrPoolClosed is returned by Acquire after Close.
var ErrPoolClosed = errors.New("pooler: pool closed")

// ErrBudgetExhausted is returned when no slot frees within AcquireTimeout.
var ErrBudgetExhausted = errors.New("pooler: backend budget exhausted")

// dialFunc opens a backend; tests substitute it.
type dialFunc func(ctx context.Context, database, role string, clientKey, serverKey []byte) (*Backend, error)

// Pool holds per-role backends within a shared budget.
type Pool struct {
	cfg   PoolConfig
	dial  dialFunc
	total chan struct{}

	mu      sync.Mutex
	roles   map[string]*rolePool
	closed  bool
	changed chan struct{}
}

type rolePool struct {
	sem  chan struct{}
	idle []*Backend
}

// NewPool builds a pool that dials through d.
func NewPool(cfg PoolConfig, d Dialer) *Pool {
	return newPool(cfg, func(ctx context.Context, database, role string, ck, sk []byte) (*Backend, error) {
		return dialBackend(ctx, d, database, role, ck, sk)
	})
}

func newPool(cfg PoolConfig, dial dialFunc) *Pool {
	cfg = cfg.withDefaults()
	return &Pool{cfg: cfg, dial: dial, total: make(chan struct{}, cfg.MaxBackends), roles: map[string]*rolePool{}, changed: make(chan struct{})}
}

func (p *Pool) role(name string) *rolePool {
	rp, ok := p.roles[name]
	if !ok {
		rp = &rolePool{sem: make(chan struct{}, p.cfg.MaxPerRole)}
		p.roles[name] = rp
	}
	return rp
}

// Acquire returns an idle backend for role or dials one within budget. The
// caller owns it until Release. Keys are used only for a fresh dial and are
// never retained by the pool. Budget slots count existing backends, idle or
// held; when the shard budget is full of idle backends of other roles one is
// evicted so a quiet role is never starved by a hot one's idle set.
func (p *Pool) Acquire(ctx context.Context, database, role string, clientKey, serverKey []byte) (*Backend, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	rp := p.role(role)
	p.mu.Unlock()

	if b := p.popIdle(rp); b != nil {
		return b, nil
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.AcquireTimeout)
	defer cancel()
	if err := acquireSlot(ctx, rp.sem); err != nil {
		return nil, err
	}
	if err := p.acquireTotal(ctx); err != nil {
		<-rp.sem
		return nil, err
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		p.free(rp)
		return nil, ErrPoolClosed
	}
	b, err := p.dial(ctx, database, role, clientKey, serverKey)
	if err != nil {
		p.free(rp)
		return nil, err
	}
	return b, nil
}

// acquireTotal takes a shard-wide slot, evicting idle backends of any role
// while the budget is full and waking whenever a backend is released.
func (p *Pool) acquireTotal(ctx context.Context) error {
	for {
		select {
		case p.total <- struct{}{}:
			return nil
		default:
		}
		if p.evictIdle() {
			continue
		}
		p.mu.Lock()
		changed := p.changed
		p.mu.Unlock()
		select {
		case p.total <- struct{}{}:
			return nil
		case <-changed:
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ErrBudgetExhausted
			}
			return ctx.Err()
		}
	}
}

// notify wakes acquirers waiting for the budget; call with mu held.
func (p *Pool) notify() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// popIdle returns a live idle backend of rp, closing expired ones on the way.
func (p *Pool) popIdle(rp *rolePool) *Backend {
	for {
		p.mu.Lock()
		var b *Backend
		if n := len(rp.idle); n > 0 {
			b = rp.idle[n-1]
			rp.idle = rp.idle[:n-1]
		}
		p.mu.Unlock()
		if b == nil {
			return nil
		}
		if p.expired(b) {
			b.close()
			p.free(rp)
			continue
		}
		return b
	}
}

// evictIdle closes one idle backend of any role, freeing its slot.
func (p *Pool) evictIdle() bool {
	p.mu.Lock()
	var victim *Backend
	var owner *rolePool
	for _, rp := range p.roles {
		if n := len(rp.idle); n > 0 {
			victim, owner = rp.idle[n-1], rp
			rp.idle = rp.idle[:n-1]
			break
		}
	}
	p.mu.Unlock()
	if victim == nil {
		return false
	}
	victim.close()
	p.free(owner)
	return true
}

func acquireSlot(ctx context.Context, sem chan struct{}) error {
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ErrBudgetExhausted
		}
		return ctx.Err()
	}
}

func (p *Pool) free(rp *rolePool) {
	<-p.total
	<-rp.sem
}

func (p *Pool) expired(b *Backend) bool {
	now := time.Now()
	if p.cfg.MaxLifetime > 0 && now.Sub(b.born) > p.cfg.MaxLifetime {
		return true
	}
	if p.cfg.MaxIdleTime > 0 && now.Sub(b.lastUsed) > p.cfg.MaxIdleTime {
		return true
	}
	return false
}

// Release returns b to its role's idle set, or closes it when broken,
// expired, not idle, or the pool is closed. Its budget slot is freed in the
// latter cases.
func (p *Pool) Release(b *Backend) {
	p.mu.Lock()
	rp := p.role(b.role)
	// A backend with buffered, unflushed messages is never reused: the next
	// user's flush would push another session's pipeline into PostgreSQL.
	keep := !p.closed && !b.broken && !b.hasUnflushed() && b.idle() && !p.expired(b)
	if keep {
		rp.idle = append(rp.idle, b)
		p.notify()
	}
	p.mu.Unlock()
	if !keep {
		b.close()
		p.free(rp)
	}
}

// Discard closes b and frees its slot without reuse.
func (p *Pool) Discard(b *Backend) {
	b.broken = true
	p.Release(b)
}

// Close closes idle backends and makes further Acquire calls fail. Backends
// held by callers are closed when released.
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	type held struct {
		b  *Backend
		rp *rolePool
	}
	var idle []held
	for _, rp := range p.roles {
		for _, b := range rp.idle {
			idle = append(idle, held{b, rp})
		}
		rp.idle = nil
	}
	p.mu.Unlock()
	for _, h := range idle {
		h.b.close()
		p.free(h.rp)
	}
}

// Stats reports live and idle counts for observability.
func (p *Pool) Stats() (live, idle int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, rp := range p.roles {
		idle += len(rp.idle)
	}
	return len(p.total), idle
}
