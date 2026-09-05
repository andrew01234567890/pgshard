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
		if d := w.notifyDelay(); d != 0 {
			t.Fatalf("notification %d of the burst waited %s; a flip must reload every router at once", i, d)
		}
	}
	d := w.notifyDelay()
	if d < notifyRefill/2 || d > notifyRefill {
		t.Fatalf("a flood's next reload waits %s, want about %s", d, notifyRefill)
	}
	// Sustained: one reload per refill, not two.
	now = now.Add(d)
	if d := w.notifyDelay(); d < notifyRefill/2 {
		t.Fatalf("after waiting out one reload the next waited %s, want another full refill", d)
	}
	// A quiet minute restores the whole burst.
	now = now.Add(time.Minute)
	for i := range notifyBurst {
		if d := w.notifyDelay(); d != 0 {
			t.Fatalf("after a quiet period, notification %d waited %s", i, d)
		}
	}
}
