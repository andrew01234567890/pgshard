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
	// refresh carries Refresh's requests to Run, which owns the connection
	// reloads use; stopped is closed when Run returns, so a request made
	// after that fails at once rather than at its caller's deadline.
	refresh chan chan error
	stopped chan struct{}
	logf    func(format string, args ...any)

	// servingKick records that the pending kick was raised by a serving
	// notification, so Run charges the right budget. Set from the LISTEN
	// goroutine, read and cleared by Run.
	servingKick atomic.Bool

	// Notification budgets, touched only by Run's own goroutine. Two of
	// them: see notifyDelay.
	desired budget
	serving budget
	now     func() time.Time
}

// budget is a token bucket of notification-driven reloads.
type budget struct {
	tokens     float64
	lastRefill time.Time
}

// Change is published to subscribers whenever a reload observes a snapshot
// that says anything different. Its fields are what the reload saw, not what
// changed: subscribers use it as a wakeup and re-read the snapshot.
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
// of them immediately, then one per notifyRefill. There are two, one per
// channel -- see notifyDelay for why the serving map does not share. The burst is what keeps a
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
		refresh:        make(chan chan error),
		stopped:        make(chan struct{}),
		logf:           opts.Logf,
		desired:        budget{tokens: notifyBurst},
		serving:        budget{tokens: notifyBurst},
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

// Refresh reloads the snapshot now, outside the notification budget, and
// returns once a load that began after the call has been published.
//
// A router that has just applied a migration answers its client only after
// this: a notification-driven reload can be waiting out a drained budget --
// every per-shard step of a migration notifies -- so the next statement on
// the same session could be planned without the view or column the
// migration just recorded, and a view missing from the snapshot is read from
// one shard with no error (PGS-871). It is not charged to the budget: it is
// asked for by the session that ran the DDL, at the rate DDL completes.
func (w *Watcher) Refresh(ctx context.Context) error {
	reply := make(chan error, 1)
	select {
	case w.refresh <- reply:
	case <-w.stopped:
		return errWatcherStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

var errWatcherStopped = errors.New("the catalog watcher has stopped")

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
	defer close(w.stopped)
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
		var asked []chan error
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case reply := <-w.refresh:
			asked = append(asked, reply)
		case <-w.kick:
			time.Sleep(w.debounce)
			select {
			case <-w.kick:
			default:
			}
			// A kick raised by both kinds at once counts as serving: the
			// urgent one must not be charged to the bucket the other may
			// have drained.
			if wait := w.notifyDelay(w.servingKick.Swap(false)); wait > 0 {
				timer := time.NewTimer(wait)
				// Waiting out a delay the DESIRED budget incurred is the
				// window this exists to shorten, so a serving change that
				// arrives mid-wait is heard rather than queued behind it.
				// Separate buckets alone do not buy that: under sustained
				// churn the watcher is inside this wait most of the time,
				// which is precisely when a flip lands.
			waiting:
				for {
					select {
					case <-ctx.Done():
						timer.Stop()
						return ctx.Err()
					case <-ticker.C:
						timer.Stop()
						break waiting
					case <-timer.C:
						break waiting
					case reply := <-w.refresh:
						timer.Stop()
						asked = append(asked, reply)
						break waiting
					case <-w.kick:
						// Only its own budget can cut the wait short. If
						// the serving bucket is empty too, the remaining
						// wait stands -- on the SAME timer, because
						// restarting it would make a stream of kicks
						// postpone the reload for ever.
						if w.servingKick.Swap(false) && w.notifyDelay(true) == 0 {
							timer.Stop()
							break waiting
						}
					}
				}
			}
		}
		// Every request already waiting shares this reload: each began
		// before it, so it answers all of them.
	drain:
		for {
			select {
			case reply := <-w.refresh:
				asked = append(asked, reply)
			default:
				break drain
			}
		}
		err := w.reload(ctx)
		if err != nil && ctx.Err() == nil {
			w.logf("snapshot reload: %v", err)
		}
		for _, reply := range asked {
			reply <- err
		}
	}
}

// notifyDelay draws one notification-driven reload from the budget and
// returns how long to wait for it. Called only from Run's goroutine.
//
// The serving channel has its OWN budget, because the two carry different
// urgency and one used to be able to spend the other's. A cutover flip is a
// serving change, and the burst above exists to make exactly that reload
// immediate -- "a cutover flip bumps the generation and wants every router
// reloading now". Desired-state churn is ordinary administration: tables,
// databases, roles. With one shared bucket, enough of the second delays the
// first, and a busy cluster produces that without anybody meaning to.
//
// Measured on the shipped defaults: sustained desired-state notifications
// delayed a flip by close to a full refill period. That interval is the
// window in which routers and poolers still route by the old map, which is
// where a straggling write to a retiring source comes from (PGS-750, and
// TestAFlipIsSeenWhileDesiredStateChurns, which is the test that can
// actually see it -- separate buckets alone do not buy it).
func (w *Watcher) notifyDelay(serving bool) time.Duration {
	b := &w.desired
	if serving {
		b = &w.serving
	}
	now := w.now()
	if !b.lastRefill.IsZero() {
		b.tokens += now.Sub(b.lastRefill).Seconds() / notifyRefill.Seconds()
	}
	b.lastRefill = now
	if b.tokens > notifyBurst {
		b.tokens = notifyBurst
	}
	if b.tokens >= 1 {
		b.tokens--
		return 0
	}
	// The reload this returns for is charged now, so the balance goes
	// negative and the refill has to catch up: charging it on the next call
	// instead would let a flood through at twice the rate.
	wait := time.Duration((1 - b.tokens) * float64(notifyRefill))
	b.tokens--
	return wait
}

// requestReload asks Run for a reload. serving says the notification was a
// change to the map routers route by, which gets its own budget.
func (w *Watcher) requestReload(serving bool) {
	if serving {
		w.servingKick.Store(true)
	}
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
	// The generation pair is not a complete change detector. It misses
	// anything the snapshot holds that no desired_generation stamp covers --
	// shard_status edits (epoch, serving state, migrating), which is what
	// the buffering loops are waiting on, and the desired-state tables
	// catalog.Generations leaves out. Subscribers only use a Change as a
	// wakeup and re-read the snapshot themselves, so the honest test is
	// whether the snapshot says anything different at all, which is what
	// the fingerprint answers.
	//
	// The pair is still compared first so this can only ever publish MORE
	// than it used to: the fingerprint covers both generations, but it is a
	// 64-bit hash, and a missed wakeup is not worth a collision argument.
	if prev == nil ||
		prev.ShardMapGeneration != s.ShardMapGeneration ||
		prev.DesiredGeneration != s.DesiredGeneration ||
		!SamePlanning(prev, s) {
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
	// Charged to the serving budget: what it may have missed is unknown,
	// and the serving map is the half that cannot wait.
	w.requestReload(true)
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return errors.Join(err, ctx.Err())
		}
		w.requestReload(n.Channel == catalog.ServingChannel)
	}
}
