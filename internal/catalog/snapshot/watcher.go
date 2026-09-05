package snapshot

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// Watcher keeps a current Snapshot fresh: it LISTENs on the desired and
// serving channels, reloads periodically and after every reconnect.
type Watcher struct {
	dsn            string
	reloadInterval time.Duration
	listen         bool
	debounce       time.Duration

	// conn is the connection reloads run on, kept between them. Every
	// reload used to dial the catalog, so a router paid a TCP handshake,
	// a TLS handshake and SCRAM before it could read a snapshot -- on the
	// periodic reload and again on every notification. Only Run's
	// goroutine touches it.
	conn *pgx.Conn

	servingOnly bool

	current atomic.Pointer[Snapshot]
	mu      sync.Mutex
	subs    map[chan Change]struct{}
	kick    chan struct{}
	logf    func(format string, args ...any)

	// Notification budget, touched only by Run's own goroutine.
	tokens     float64
	lastRefill time.Time
	now        func() time.Time
}

// Change is published to subscribers whenever a reload observes a different
// generation pair.
type Change struct {
	ShardMapGeneration int64
	DesiredGeneration  int64
}

// DefaultReloadInterval is the fallback full reload period when LISTEN
// delivers nothing (or its connection is down).
const DefaultReloadInterval = 30 * time.Second

// A NOTIFY costs its sender nothing and costs every router a full catalog
// load and every pooler a serving load. pg_notify() is an ordinary function
// call, so anything that can reach the catalog database can send one in a
// loop, and the debounce alone still allowed twenty loads a second on every
// component in the cluster.
//
// Notification-driven reloads are therefore drawn from a budget: notifyBurst
// of them immediately, then one per notifyRefill. The burst is what keeps a
// real change fast -- a cutover flip bumps the generation and wants every
// router reloading now, and that window is the write pause it is measured by
// -- while a flood settles to one load per second per component. The
// periodic reload is not drawn from the budget, so a component still
// converges on its own.
const (
	notifyBurst  = 5
	notifyRefill = time.Second
)

// Backoff for the first reload, which has to keep trying: nothing restarts a
// watcher that gives up.
const (
	firstReloadBackoff    = 250 * time.Millisecond
	maxFirstReloadBackoff = 5 * time.Second
)

// Options tunes a Watcher. Zero values pick defaults.
type Options struct {
	ReloadInterval time.Duration                    // default DefaultReloadInterval
	DisableListen  bool                             // periodic reload only
	Logf           func(format string, args ...any) // nil discards
	// ServingOnly loads the generations and the serving rows and nothing
	// else, which is all a pooler enforces. The snapshots it produces are
	// marked Partial and must not be used to plan.
	ServingOnly bool
}

// NewWatcher builds a Watcher for the catalog at dsn; Run starts it.
func NewWatcher(dsn string, opts Options) *Watcher {
	if opts.ReloadInterval <= 0 {
		opts.ReloadInterval = DefaultReloadInterval
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	return &Watcher{
		dsn:            dsn,
		reloadInterval: opts.ReloadInterval,
		listen:         !opts.DisableListen,
		servingOnly:    opts.ServingOnly,
		debounce:       50 * time.Millisecond,
		subs:           map[chan Change]struct{}{},
		kick:           make(chan struct{}, 1),
		logf:           opts.Logf,
		tokens:         notifyBurst,
		now:            time.Now,
	}
}

// Current returns the latest Snapshot, or nil before the first load.
func (w *Watcher) Current() *Snapshot { return w.current.Load() }

// Fresh reports whether the watcher holds a snapshot recent enough to act
// on. A component whose reloads are failing stops being a valid
// participant rather than serving a view it can no longer trust.
func (w *Watcher) Fresh(now time.Time) bool {
	snap := w.Current()
	return snap != nil && !snap.Stale(now)
}

// AgeSeconds is the age of the held snapshot for a metrics gauge; it is
// negative when there is no snapshot to age.
func (w *Watcher) AgeSeconds(now time.Time) float64 {
	age, ok := w.Current().Age(now)
	if !ok {
		return -1
	}
	return age.Seconds()
}

// SetForTest installs a snapshot without a catalog behind it.
func (w *Watcher) SetForTest(s *Snapshot) { w.current.Store(s) }

// Subscribe returns a channel that receives generation changes. Slow
// receivers miss intermediate changes but always get the latest one.
func (w *Watcher) Subscribe() (<-chan Change, func()) {
	ch := make(chan Change, 1)
	w.mu.Lock()
	w.subs[ch] = struct{}{}
	w.mu.Unlock()
	return ch, func() {
		w.mu.Lock()
		delete(w.subs, ch)
		w.mu.Unlock()
	}
}

// Run blocks until ctx is done. It returns after the first snapshot fails to
// load so callers can fail fast at startup.
func (w *Watcher) Run(ctx context.Context) error {
	defer w.closeConn(ctx)
	// The first reload used to be fatal. A router or pooler that started
	// before the catalog accepted connections lost its watcher there and then
	// served for the rest of its life with no snapshot, stamping every request
	// with generation zero and having each one refused as a stale generation,
	// with no recovery short of restarting the pod. A pod starting before its
	// dependencies are up is ordinary, so keep trying until it works or the
	// context ends. Once the loop below is running a failed reload is already
	// only logged.
	for delay := firstReloadBackoff; ; {
		err := w.reload(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.logf("snapshot reload: %v; retrying in %s", err, delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay *= 2; delay > maxFirstReloadBackoff {
			delay = maxFirstReloadBackoff
		}
	}
	if w.listen {
		go w.listenLoop(ctx)
	}
	ticker := time.NewTicker(w.reloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-w.kick:
			time.Sleep(w.debounce)
			select {
			case <-w.kick:
			default:
			}
			if wait := w.notifyDelay(); wait > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				case <-time.After(wait):
				}
			}
		}
		if err := w.reload(ctx); err != nil && ctx.Err() == nil {
			w.logf("snapshot reload: %v", err)
		}
	}
}

// notifyDelay draws one notification-driven reload from the budget and
// returns how long to wait for it. Called only from Run's goroutine.
func (w *Watcher) notifyDelay() time.Duration {
	now := w.now()
	if !w.lastRefill.IsZero() {
		w.tokens += now.Sub(w.lastRefill).Seconds() / notifyRefill.Seconds()
	}
	w.lastRefill = now
	if w.tokens > notifyBurst {
		w.tokens = notifyBurst
	}
	if w.tokens >= 1 {
		w.tokens--
		return 0
	}
	// The reload this returns for is charged now, so the balance goes
	// negative and the refill has to catch up: charging it on the next call
	// instead would let a flood through at twice the rate.
	wait := time.Duration((1 - w.tokens) * float64(notifyRefill))
	w.tokens--
	return wait
}

func (w *Watcher) requestReload() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

func (w *Watcher) reload(ctx context.Context) error {
	reused := w.conn != nil
	s, err := w.loadOnce(ctx)
	if err != nil && reused && ctx.Err() == nil {
		// The kept connection is the likeliest reason -- the catalog may
		// have restarted under it -- and the failure already dropped it,
		// so this attempt dials. Worth one immediate retry rather than
		// waiting for the next tick: a snapshot that waits can age past
		// the bound the router refuses to plan against.
		s, err = w.loadOnce(ctx)
	}
	if err != nil {
		return err
	}
	prev := w.current.Swap(s)
	if prev == nil || prev.ShardMapGeneration != s.ShardMapGeneration || prev.DesiredGeneration != s.DesiredGeneration {
		w.publish(Change{s.ShardMapGeneration, s.DesiredGeneration})
	}
	return nil
}

// loadOnce reads a snapshot on the kept connection, dialling one first if
// there is none. Any failure drops the connection: it may be the reason,
// and a wedged one must not be kept forever.
func (w *Watcher) loadOnce(ctx context.Context) (*Snapshot, error) {
	if w.conn == nil {
		conn, err := pgx.Connect(ctx, w.dsn)
		if err != nil {
			return nil, err
		}
		w.conn = conn
	}
	load := Load
	if w.servingOnly {
		load = LoadServing
	}
	s, err := load(ctx, w.conn)
	if err != nil {
		w.closeConn(ctx)
		return nil, err
	}
	return s, nil
}

func (w *Watcher) closeConn(ctx context.Context) {
	if w.conn != nil {
		_ = w.conn.Close(context.WithoutCancel(ctx))
		w.conn = nil
	}
}

func (w *Watcher) publish(c Change) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for ch := range w.subs {
		select {
		case <-ch:
		default:
		}
		ch <- c
	}
}

func (w *Watcher) listenLoop(ctx context.Context) {
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		err := w.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		w.logf("snapshot listener: %v; reconnecting in %s", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (w *Watcher) listenOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, w.dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	for _, ch := range []string{catalog.DesiredChannel, catalog.ServingChannel} {
		if _, err := conn.Exec(ctx, "LISTEN "+ch); err != nil {
			return err
		}
	}
	// A notification may have fired between the last load and LISTEN.
	w.requestReload()
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return errors.Join(err, ctx.Err())
		}
		w.requestReload()
	}
}
