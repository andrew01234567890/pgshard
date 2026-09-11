package agent

import (
	"context"
	"io/fs"
	"path/filepath"
	"sync"
	"time"
)

// StallWindow is how long a bootstrap may go without growing the data
// directory before it is reported stalled.
//
// Generous on purpose. A clone is not steady: pg_basebackup takes a
// checkpoint before it sends anything, and a restore from the repository
// spends whole minutes on WAL replay that adds little. What this has to
// separate is "slow" from "not happening", and only the second is worth a
// restart -- the first is what the old startup budget kept killing.
const StallWindow = 10 * time.Minute

// sampleEvery is how often the data directory is measured. A walk of a
// large PGDATA is a few thousand stats, which is cheap next to the copy it
// is watching, but not cheap enough to do on every probe.
const sampleEvery = 15 * time.Second

// bootstrapProgress is what a member building its data directory can say
// about itself while it does.
//
// The kubelet has no "in progress": a startup probe that fails is retried
// until its budget runs out and then the container is killed. With a 10
// minute budget, any clone slower than that was killed, restarted from an
// empty directory, and killed again -- a permanent failure that got worse
// as the data grew, and the only thing that would have fixed it was the
// member saying "I am still copying".
type bootstrapProgress struct {
	mu      sync.Mutex
	phase   string
	last    time.Time
	size    int64
	done    bool
	now     func() time.Time
	measure func() int64
}

func newBootstrapProgress(pgdata string) *bootstrapProgress {
	return &bootstrapProgress{now: time.Now, measure: func() int64 { return dirSize(pgdata) }}
}

// begin names what is being done and starts the clock. Every phase resets
// it: work that has just changed shape has by definition just progressed.
func (b *bootstrapProgress) begin(phase string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.phase, b.last, b.done = phase, b.now(), false
}

// finish ends the bootstrap. The probes go back to answering about
// PostgreSQL itself.
func (b *bootstrapProgress) finish() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.phase, b.done = "", true
}

// sample records the data directory's size and, when it has grown, that
// the bootstrap is getting somewhere.
func (b *bootstrapProgress) sample() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done || b.phase == "" {
		return
	}
	if size := b.measure(); size != b.size {
		b.size, b.last = size, b.now()
	}
}

// state answers the probes: whether a bootstrap is running, whether it has
// stalled, and what it is doing.
func (b *bootstrapProgress) state() (active, stalled bool, what string) {
	if b == nil {
		return false, false, ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done || b.phase == "" {
		return false, false, ""
	}
	return true, b.now().Sub(b.last) > StallWindow, b.phase
}

// watch samples until the bootstrap finishes or ctx ends.
func (b *bootstrapProgress) watch(ctx context.Context) {
	t := time.NewTicker(sampleEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if active, _, _ := b.state(); !active {
				continue
			}
			b.sample()
		}
	}
}

// dirSize totals what the data directory holds. Errors are ignored on
// purpose: a file that vanishes between the walk and the stat is a copy in
// flight, which is the thing this is here to notice.
func dirSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // an unreadable entry is not a reason to stop measuring
		}
		if fi, err := d.Info(); err == nil {
			total += fi.Size()
		}
		return nil
	})
	return total
}
