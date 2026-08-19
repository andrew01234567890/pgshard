package admin

import "sync"

// Notifier fans a "topology changed" tick out to every subscribed SSE stream.
type Notifier struct {
	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

// NewNotifier returns an empty Notifier.
func NewNotifier() *Notifier { return &Notifier{subs: map[chan struct{}]struct{}{}} }

// Subscribe returns a channel that receives a value on every Notify and a
// function that unsubscribes it.
func (n *Notifier) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	n.subs[ch] = struct{}{}
	n.mu.Unlock()
	return ch, func() {
		n.mu.Lock()
		delete(n.subs, ch)
		n.mu.Unlock()
	}
}

// Notify wakes every subscriber; ticks coalesce while a subscriber is slow.
func (n *Notifier) Notify() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
