package plan

import (
	"strings"

	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
)

// A session advisory lock belongs to a PostgreSQL backend and is released
// only by an explicit unlock or by that backend going away. Under
// transaction pooling the router hands the backend back at the end of the
// statement, so the lock stays with a backend the session no longer has:
// the next session on it inherits or releases a lock it never took, and
// the one that "holds" the lock has moved elsewhere. Both clients believe
// they hold it, which for the thing advisory locks are used for -- leader
// election, migrations, mutual exclusion -- means both do the work.
//
// Refusing is not the whole answer, only the safe part of it. Holding the
// lock properly means pinning the backend until the last one is released
// and scrubbing on recycle; until that exists, a client that asks for a
// guarantee pgshard cannot keep is told so rather than told yes.
//
// The transaction-scoped variants are allowed and unaffected: PostgreSQL
// releases them at the end of the transaction, and a transaction is
// already pinned to one backend, so they mean there what they mean on a
// single node.
func sessionAdvisoryLock(root *pgquerypb.Node) string {
	found := ""
	visit(root, func(n *pgquerypb.Node) bool {
		if found != "" {
			return false
		}
		fc := n.GetFuncCall()
		if fc == nil {
			return true
		}
		names := stringList(fc.GetFuncname())
		if len(names) == 0 || !builtinName(names) {
			return true
		}
		if name := strings.ToLower(names[len(names)-1]); isSessionAdvisoryLock(name) {
			found = name
			return false
		}
		return true
	})
	return found
}

// isSessionAdvisoryLock reports whether name is one of the advisory-lock
// functions whose lock outlives the transaction. Every advisory function
// is either session-scoped or names itself xact; matching on that rather
// than listing the eight session ones means a variant added later is
// refused rather than quietly let through.
func isSessionAdvisoryLock(name string) bool {
	switch {
	case !strings.HasPrefix(name, "pg_advisory_") && !strings.HasPrefix(name, "pg_try_advisory_"):
		return false
	case strings.Contains(name, "_xact_"):
		return false
	}
	return true
}
