package router

import (
	"context"
	"testing"
	"time"
)

func (f *fakePooler) isAttached(sid string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.attached[sid]
	return ok
}

func (f *fakePooler) reservedSessions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, id := range f.reserves {
		if len(out) == 0 || out[len(out)-1] != id {
			out = append(out, id)
		}
	}
	return out
}

// TestAnIdleClientKeepsItsExecuteStream is what makes the pooler's
// reservation timeout safe, and it is not obvious from either side alone.
//
// The pooler releases a reserved session that has had no Execute stream for
// ReserveTimeout, rolling back whatever it held, so a router that died does
// not hold a backend for ever. A client thinking between statements would
// be indistinguishable from that if the router closed the stream between
// statements -- it would lose its transaction, and the next attach would
// look like a new session.
//
// It does not close it. A pinned session holds its stream open while the
// client is idle, so the pooler sees it attached and the timeout skips it;
// only a router that has actually gone leaves a session detached. A change
// that closed the stream between statements to free it would reintroduce
// exactly that bug, which is why this asserts the property rather than the
// timeout.
func TestAnIdleClientKeepsItsExecuteStream(t *testing.T) {
	fp := newFakePooler()
	h := newHarnessWith(t, fp, startFakePooler(t, fp), nil)
	ctx := context.Background()
	conn := h.connect(t, h.dsn("app", "secret", "app"))

	// A session setting is what pins a backend: the router holds this one
	// so the state it replays does not have to be replayed per statement.
	if _, err := conn.Exec(ctx, "set time zone 'UTC'"); err != nil {
		t.Fatal(err)
	}
	ids := fp.reservedSessions()
	if len(ids) != 1 {
		t.Fatalf("expected one reserved session, got %v", ids)
	}

	// The client now thinks. Nothing it does drives the router.
	time.Sleep(150 * time.Millisecond)
	if !fp.isAttached(ids[0]) {
		t.Fatal("an idle pinned session has no Execute stream, so the reservation timeout can expire a client that is merely thinking")
	}
}
