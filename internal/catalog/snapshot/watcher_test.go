package snapshot

import (
	"testing"
	"time"
)

// TestNotificationsDrawFromABudget: pg_notify() is an ordinary function
// call, so anything that can reach the catalog database can make every
// router load the whole catalog and every pooler load the serving rows, as
// fast as it can send. The budget keeps a real change immediate -- a cutover
// flip is measured by how fast routers reload -- and settles a flood to one
// load per refill per component.
func TestNotificationsDrawFromABudget(t *testing.T) {
	now := time.Now()
	w := NewWatcher("", Options{})
	w.now = func() time.Time { return now }

	for i := range notifyBurst {
		if d := w.notifyDelay(false); d != 0 {
			t.Fatalf("notification %d of the burst waited %s; a flip must reload every router at once", i, d)
		}
	}
	d := w.notifyDelay(false)
	if d < notifyRefill/2 || d > notifyRefill {
		t.Fatalf("a flood's next reload waits %s, want about %s", d, notifyRefill)
	}
	// Sustained: one reload per refill, not two.
	now = now.Add(d)
	if d := w.notifyDelay(false); d < notifyRefill/2 {
		t.Fatalf("after waiting out one reload the next waited %s, want another full refill", d)
	}
	// A quiet minute restores the whole burst.
	now = now.Add(time.Minute)
	for i := range notifyBurst {
		if d := w.notifyDelay(false); d != 0 {
			t.Fatalf("after a quiet period, notification %d waited %s", i, d)
		}
	}
}

// TestServingNotificationsHaveTheirOwnBudget: desired-state churn must not
// make a cutover flip wait.
//
// The burst exists to make a flip's reload immediate -- routers and poolers
// route by the map it publishes, and the interval in which they still route
// by the old one is where a straggling write to a retiring source comes
// from. With one shared bucket, ordinary administration (tables, databases,
// roles) could spend the flip's budget; measured on real PostgreSQL that
// took a flip from 52ms to 976ms.
func TestServingNotificationsHaveTheirOwnBudget(t *testing.T) {
	now := time.Now()
	w := NewWatcher("", Options{})
	w.now = func() time.Time { return now }

	// Drain the desired budget the way a busy cluster does, and keep it
	// drained: sustained churn, one per refill, for a good while.
	for range notifyBurst {
		if d := w.notifyDelay(false); d != 0 {
			t.Fatalf("desired burst waited %s", d)
		}
	}
	for range 20 {
		d := w.notifyDelay(false)
		now = now.Add(d)
	}

	// The flip lands in the middle of that, and must not wait.
	for i := range notifyBurst {
		if d := w.notifyDelay(true); d != 0 {
			t.Fatalf("a flip's reload %d waited %s while desired-state churn held the budget; that wait is the window a straggling write lives in", i, d)
		}
	}
	// The serving budget is still a budget: a flood of serving changes
	// settles to the refill like any other.
	if d := w.notifyDelay(true); d < notifyRefill/2 {
		t.Fatalf("serving notifications past the burst waited %s, want about %s", d, notifyRefill)
	}
}
