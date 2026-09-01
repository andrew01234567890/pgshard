package agent

// startupRollback undoes what a partially completed startup acquired.
//
// The agent takes a primary lease, starts PostgreSQL and binds two
// listeners, and steps after the first of those can still fail. Returning
// then left PostgreSQL serving, HTTP answering and the lease renewing, with
// the caller exiting and the lease left to expire on its own -- a window in
// which nothing else may promote, for a member that is not running.
//
// Steps are undone in reverse, because each is built on the ones before it.
type startupRollback struct {
	steps []func()
	done  bool
}

// push records how to undo the resource just acquired.
func (r *startupRollback) push(undo func()) { r.steps = append(r.steps, undo) }

// succeed marks startup complete, after which the steady-state shutdown path
// owns these resources and run does nothing.
func (r *startupRollback) succeed() { r.done = true }

func (r *startupRollback) run() {
	if r.done {
		return
	}
	for i := len(r.steps) - 1; i >= 0; i-- {
		r.steps[i]()
	}
}
