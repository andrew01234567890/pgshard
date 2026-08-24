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

	current atomic.Pointer[Snapshot]
	mu      sync.Mutex
	subs    map[chan Change]struct{}
	kick    chan struct{}
	logf    func(format string, args ...any)
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

// Options tunes a Watcher. Zero values pick defaults.
type Options struct {
	ReloadInterval time.Duration                    // default DefaultReloadInterval
	DisableListen  bool                             // periodic reload only
	Logf           func(format string, args ...any) // nil discards
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
		debounce:       50 * time.Millisecond,
		subs:           map[chan Change]struct{}{},
		kick:           make(chan struct{}, 1),
		logf:           opts.Logf,
	}
}

// Current returns the latest Snapshot, or nil before the first load.
func (w *Watcher) Current() *Snapshot { return w.current.Load() }

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
	if err := w.reload(ctx); err != nil {
		return err
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
		}
		if err := w.reload(ctx); err != nil && ctx.Err() == nil {
			w.logf("snapshot reload: %v", err)
		}
	}
}

func (w *Watcher) requestReload() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

func (w *Watcher) reload(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, w.dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	s, err := Load(ctx, conn)
	if err != nil {
		return err
	}
	prev := w.current.Swap(s)
	if prev == nil || prev.ShardMapGeneration != s.ShardMapGeneration || prev.DesiredGeneration != s.DesiredGeneration {
		w.publish(Change{s.ShardMapGeneration, s.DesiredGeneration})
	}
	return nil
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
