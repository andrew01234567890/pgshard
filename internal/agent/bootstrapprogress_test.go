package agent

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestACloneLongerThanTheProbeBudgetIsNotKilled.
//
// The startup probe is 5s x 120: ten minutes, after which the kubelet kills
// the container. Nothing answered it during Bootstrap -- the HTTP listener
// did not exist yet -- so a clone of anything large failed the probe for
// its whole duration, the container was killed, the restart cleared the
// data directory and copied again, and it was killed again. A permanent
// failure that got worse as the data grew.
//
// A member that is still copying now says so, and is started and alive
// while it is. One that has stopped copying says that instead, which is the
// case a restart actually helps.
func TestACloneLongerThanTheProbeBudgetIsNotKilled(t *testing.T) {
	now := time.Unix(1000, 0)
	var copied int64
	b := &bootstrapProgress{now: func() time.Time { return now }, measure: func() int64 { return copied }}
	p := &Probes{Health: &fakeHealth{primary: true}, Bootstrapping: b.state,
		KubeReachable: func(context.Context) bool { return true }}
	ctx := context.Background()

	b.begin("cloning from the primary")
	// Well past the old ten-minute budget, still copying.
	for i := 0; i < 6; i++ {
		now = now.Add(5 * time.Minute)
		copied += 1 << 30
		b.sample()
		if err := p.Start(ctx); err != nil {
			t.Fatalf("after %v of a clone that is still copying: %v", time.Duration(i+1)*5*time.Minute, err)
		}
		if err := p.Live(ctx); err != nil {
			t.Fatalf("a member copying its data directory is not dead: %v", err)
		}
	}

	// It stops getting anywhere. That is the case a restart helps.
	now = now.Add(StallWindow + time.Minute)
	b.sample()
	err := p.Start(ctx)
	if err == nil {
		t.Fatal("a clone that has made no progress for the stall window must fail the startup probe")
	}
	if !strings.Contains(err.Error(), "cloning from the primary") {
		t.Fatalf("the failure has to say what stopped: %v", err)
	}
	if p.Live(ctx) == nil {
		t.Fatal("a stalled bootstrap is not alive either; nothing else will restart it")
	}

	// A member building its data directory is never READY, whatever
	// PostgreSQL says while it does it: a restore starts PostgreSQL to
	// replay and promote, so there is a window mid-bootstrap where it
	// accepts writes and the member is still nowhere near serving.
	b.begin("restoring from the repository")
	if p.Ready(ctx) == nil {
		t.Fatal("a member still restoring reported itself ready; a Service would send it traffic")
	}

	// And once it finishes, the probes answer about PostgreSQL again.
	b.finish()
	if active, _, _ := b.state(); active {
		t.Fatal("a finished bootstrap still reports itself active")
	}
}

// TestBootstrapProgressFollowsTheDataDirectory checks the signal itself:
// growth is progress, and a phase change is progress whatever the size did.
func TestBootstrapProgressFollowsTheDataDirectory(t *testing.T) {
	now := time.Unix(1000, 0)
	var size int64
	b := &bootstrapProgress{now: func() time.Time { return now }, measure: func() int64 { return size }}

	if active, _, _ := b.state(); active {
		t.Fatal("nothing has begun")
	}
	b.begin("restoring from the repository")
	now = now.Add(StallWindow + time.Second)
	b.sample()
	if _, stalled, _ := b.state(); !stalled {
		t.Fatal("a restore that has not grown the data directory for the whole window has stalled")
	}
	size = 1 << 20
	b.sample()
	if _, stalled, _ := b.state(); stalled {
		t.Fatal("it grew; that is progress")
	}
	// A new phase is progress in itself: work that has just changed shape
	// has by definition just got somewhere.
	now = now.Add(StallWindow + time.Second)
	b.sample()
	b.begin("rejoining as a standby")
	if _, stalled, what := b.state(); stalled || what != "rejoining as a standby" {
		t.Fatalf("a fresh phase reports stalled=%v what=%q", stalled, what)
	}
}
