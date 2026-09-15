package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// queueRecheck is how long a component that found no operation queue in its
// catalog waits before looking again: the operator migrates the catalog
// after the new binaries are running.
const queueRecheck = 30 * time.Second

// queueSchema remembers whether the catalog has the operation queue. Once
// it has, it always will.
type queueSchema struct {
	mu        sync.Mutex
	present   bool
	checkedAt time.Time
}

// queueSchemas holds one queueSchema per catalog pool, so that the
// components sharing a pool share what it found, and copying a component
// copies no lock.
var queueSchemas sync.Map

// queueOn reports whether the catalog behind pool has the operation queue.
func queueOn(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	q, _ := queueSchemas.LoadOrStore(pool, &queueSchema{})
	return q.(*queueSchema).on(ctx, pool)
}

func (q *queueSchema) on(ctx context.Context, db catalog.RowQuerier) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.present || (!q.checkedAt.IsZero() && time.Since(q.checkedAt) < queueRecheck) {
		return q.present, nil
	}
	present, err := catalog.QueueSchema(ctx, db)
	if err != nil {
		return false, err
	}
	q.present, q.checkedAt = present, time.Now()
	return present, nil
}

// errWaitingInQueue reports an operation that may not start yet, naming
// what it waits for.
type errWaitingInQueue struct {
	blockers []catalog.Blocker
	// reason, when set, is the wait described in words instead.
	reason string
}

func (e errWaitingInQueue) Error() string {
	if e.reason != "" {
		return e.reason
	}
	names := make([]string, 0, len(e.blockers))
	for _, b := range e.blockers {
		names = append(names, describeBlocker(b))
	}
	return "waiting in the operation queue for " + strings.Join(names, ", ")
}

func describeBlocker(b catalog.Blocker) string {
	what := map[string]string{catalog.OperationDDL: "migration", catalog.OperationReshard: "reshard",
		catalog.OperationUpgrade: "upgrade", catalog.OperationPlacement: "table placement"}[b.Kind]
	if what == "" {
		what = b.Kind
	}
	if b.Reason == catalog.BlockedByStarted {
		return fmt.Sprintf("%s %s, in progress", what, b.ID)
	}
	return fmt.Sprintf("%s %s, queued before it", what, b.ID)
}

// waitInQueue returns errWaitingInQueue when operation (kind, id) waits for
// anything.
func waitInQueue(ctx context.Context, q catalog.Querier, kind, id string) error {
	blockers, err := catalog.OperationBlockers(ctx, q, kind, id)
	if err != nil {
		return fmt.Errorf("operation queue: %w", err)
	}
	if len(blockers) > 0 {
		return errWaitingInQueue{blockers: blockers}
	}
	return nil
}

func isWaitingInQueue(err error) bool {
	var w errWaitingInQueue
	return errors.As(err, &w)
}
