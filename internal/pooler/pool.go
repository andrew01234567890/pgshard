package pooler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
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
	// OnDial observes every backend dial attempt and its outcome.
	OnDial func(err error)
	// OnWait observes acquires that could not use an idle backend or a
	// free budget slot immediately.
	OnWait func()
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

	// stop ends the idle reaper; wg waits for it.
	stop chan struct{}
	wg   sync.WaitGroup

	mu      sync.Mutex
	roles   map[poolKey]*rolePool
	sems    map[string]chan struct{}
	closed  bool
	changed chan struct{}
	// waiters counts acquirers parked on changed. A release with nobody to
	// wake skips the broadcast, and the channel allocation it needs. Every
	// waiter registers under mu before it looks at the resource it wants,
	// so a release that sees none cannot be racing one that is about to
	// park: it would have to take mu to do so, and would then find the
	// resource already there.
	waiters int
}

// poolKey identifies one backend pool: backends are only interchangeable
// within the same database and role.
type poolKey struct {
	database string
	role     string
}

// rolePool holds the idle backends of one (database, role); sem is the
// role's shard-wide quota, shared by every database the role connects to.
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
	p := &Pool{cfg: cfg, dial: dial, total: make(chan struct{}, cfg.MaxBackends), roles: map[poolKey]*rolePool{}, sems: map[string]chan struct{}{}, changed: make(chan struct{}), stop: make(chan struct{})}
	if cfg.MaxIdleTime > 0 {
		p.wg.Add(1)
		go p.reap()
	}
	return p
}

// reap closes backends that have sat idle past MaxIdleTime. Without it an
// idle lifetime was only noticed by the next acquire, so the connections a
// spike created -- and the pgx buffers, the PostgreSQL backends and their
// memory behind them -- stayed for as long as the pool was quiet, which is
// exactly when nothing comes to notice.
func (p *Pool) reap() {
	defer p.wg.Done()
	every := max(p.cfg.MaxIdleTime/2, time.Second)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
		}
		p.reapOnce()
	}
}

func (p *Pool) reapOnce() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	type held struct {
		b  *Backend
		rp *rolePool
	}
	var dead []held
	for _, rp := range p.roles {
		kept := rp.idle[:0]
		for _, b := range rp.idle {
			if p.expired(b) {
				dead = append(dead, held{b, rp})
				continue
			}
			kept = append(kept, b)
		}
		rp.idle = kept
	}
	p.mu.Unlock()
	for _, h := range dead {
		h.b.close()
		p.free(h.rp)
	}
	if len(dead) > 0 {
		p.mu.Lock()
		p.notify()
		p.mu.Unlock()
	}
}

func (p *Pool) role(database, role string) *rolePool {
	k := poolKey{database: database, role: role}
	rp, ok := p.roles[k]
	if !ok {
		sem, ok := p.sems[role]
		if !ok {
			sem = make(chan struct{}, p.cfg.MaxPerRole)
			p.sems[role] = sem
		}
		rp = &rolePool{sem: sem}
		p.roles[k] = rp
	}
	return rp
}

// credDigest fingerprints SCRAM keys so an idle backend can be bound to the
// exact credentials that authenticated it; only the digest is retained.
func credDigest(clientKey, serverKey []byte) [32]byte {
	h := sha256.New()
	h.Write(clientKey)
	h.Write([]byte{0})
	h.Write(serverKey)
	var d [32]byte
	h.Sum(d[:0])
	return d
}

// Acquire returns an idle backend for role or dials one within budget. The
// caller owns it until Release; an idle backend is reused only when the
// caller presents the same SCRAM keys that authenticated it, so a session
// that has not proven the role's credentials cannot ride an already
// authenticated backend. The keys themselves are never retained by the
// pool, only their digest. Budget slots count existing backends, idle or
// held; when the shard budget is full of idle backends of other roles one is
// evicted so a quiet role is never starved by a hot one's idle set.
func (p *Pool) Acquire(ctx context.Context, database, role string, clientKey, serverKey []byte) (*Backend, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	rp := p.role(database, role)
	p.mu.Unlock()

	digest := credDigest(clientKey, serverKey)
	if b := p.popIdle(rp, digest); b != nil {
		return b, nil
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.AcquireTimeout)
	defer cancel()
	select {
	case rp.sem <- struct{}{}:
	default:
		if p.cfg.OnWait != nil {
			p.cfg.OnWait()
		}
		reused, err := p.awaitSlot(ctx, rp, digest)
		if err != nil {
			return nil, err
		}
		if reused != nil {
			return reused, nil
		}
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
	if p.cfg.OnDial != nil {
		p.cfg.OnDial(err)
	}
	if err != nil {
		p.free(rp)
		return nil, err
	}
	b.credDigest = digest
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
		p.waiters++
		p.mu.Unlock()
		err := func() error {
			defer p.unwait()
			select {
			case p.total <- struct{}{}:
				return errGotSlot
			case <-changed:
				return nil
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return ErrBudgetExhausted
				}
				return ctx.Err()
			}
		}()
		if errors.Is(err, errGotSlot) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// unwait deregisters a parked acquirer.
func (p *Pool) unwait() {
	p.mu.Lock()
	p.waiters--
	p.mu.Unlock()
}

// errGotSlot marks the budget slot having been taken inside the wait.
var errGotSlot = errors.New("slot acquired")

// notify wakes acquirers waiting for the budget; call with mu held.
func (p *Pool) notify() {
	if p.waiters == 0 {
		return
	}
	close(p.changed)
	p.changed = make(chan struct{})
}

// popIdle returns a live idle backend of rp whose credential digest matches
// the caller's keys, closing expired or credential-stale ones on the way. A
// digest mismatch means the role's password changed or the caller never
// proved these credentials; either way the backend is not reusable by them.
func (p *Pool) popIdle(rp *rolePool, digest [32]byte) *Backend {
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
		if p.expired(b) || !hmac.Equal(b.credDigest[:], digest[:]) {
			b.close()
			p.free(rp)
			continue
		}
		return b
	}
}

// evictIdle closes one idle backend of any role, freeing its slot.
// evictIdle closes the least recently used idle backend to make room, and
// reports whether it found one. It used to take the last entry of whichever
// role pool the map happened to yield first: the idle list is LIFO, so the
// last entry is the warmest backend there is -- the one with the hottest
// prepared statements and the one most likely to be wanted next -- while
// older connections sat untouched.
func (p *Pool) evictIdle() bool {
	p.mu.Lock()
	var victim *Backend
	var owner *rolePool
	var at int
	for _, rp := range p.roles {
		for i, b := range rp.idle {
			if victim == nil || b.lastUsed.Before(victim.lastUsed) {
				victim, owner, at = b, rp, i
			}
		}
	}
	if victim != nil {
		owner.idle = append(owner.idle[:at], owner.idle[at+1:]...)
	}
	p.mu.Unlock()
	if victim == nil {
		return false
	}
	victim.close()
	p.free(owner)
	return true
}

// awaitSlot waits for the role to have room. It returns a backend when one
// was released to the idle list while waiting, or nil when the caller now
// holds a slot of its own and should dial.
//
// Watching the semaphore alone was not enough: a released backend keeps its
// slot and joins the idle list, so nothing frees the semaphore and a waiter
// sat there until its acquire timeout while a backend it could have used
// was idle. It watches p.changed too and rechecks the idle list, the same
// way acquireTotal waits for the pool-wide budget.
func (p *Pool) awaitSlot(ctx context.Context, rp *rolePool, digest [32]byte) (*Backend, error) {
	for {
		// Snapshot the wake channel before looking, so a release between
		// the look and the wait closes the channel this select watches.
		p.mu.Lock()
		changed := p.changed
		closed := p.closed
		p.waiters++
		p.mu.Unlock()
		if closed {
			p.unwait()
			return nil, ErrPoolClosed
		}
		if b := p.popIdle(rp, digest); b != nil {
			p.unwait()
			return b, nil
		}
		got, err := func() (bool, error) {
			defer p.unwait()
			select {
			case rp.sem <- struct{}{}:
				return true, nil
			case <-changed:
				return false, nil
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return false, ErrBudgetExhausted
				}
				return false, ctx.Err()
			}
		}()
		if err != nil {
			return nil, err
		}
		if got {
			return nil, nil
		}
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
	rp := p.role(b.database, b.role)
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
	if !p.closed {
		close(p.stop)
	}
	p.closed = true
	// Waiters watch this channel; without the wake a shutdown leaves them
	// blocked until their acquire timeout for a pool that is already gone.
	p.notify()
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
	p.wg.Wait()
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
