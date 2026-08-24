package pooler

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
)

// preparedState is what the pooler knows about one named statement on a
// backend. A statement is certain once a batch that parsed it ended without
// an error; anything else (a parse whose batch failed, a Close the batch may
// not have reached, a DEALLOCATE in a simple query) leaves it uncertain, and
// an uncertain name is closed before it is parsed again.
type preparedState struct {
	fingerprint string
	certain     bool
}

// preparedSet tracks the named statements a backend may hold.
type preparedSet map[string]preparedState

func statementFingerprint(p *pgproto3.Parse) string {
	var sb strings.Builder
	sb.WriteString(p.Query)
	for _, oid := range p.ParameterOIDs {
		sb.WriteByte(0)
		sb.WriteString(strconv.FormatUint(uint64(oid), 10))
	}
	return sb.String()
}

// holds reports whether name is certainly prepared with fingerprint fp.
func (ps preparedSet) holds(name, fp string) bool {
	st, ok := ps[name]
	return ok && st.certain && st.fingerprint == fp
}

// mayHold reports whether name may be prepared at all.
func (ps preparedSet) mayHold(name string) bool {
	_, ok := ps[name]
	return ok
}

// doubtAll marks every tracked name uncertain: the backend ran a statement
// that may have deallocated any of them.
func (ps preparedSet) doubtAll() {
	for name, st := range ps {
		st.certain = false
		ps[name] = st
	}
}

// touchesPrepared reports whether a simple query may deallocate
// statements.
func touchesPrepared(sql string) bool {
	u := strings.ToUpper(sql)
	return strings.Contains(u, "DEALLOCATE") || strings.Contains(u, "DISCARD")
}

var sqlPrepareRE = regexp.MustCompile(`(?i)\bPREPARE\s+(?:TRANSACTION\b)?`)

// createsPrepared reports whether the SQL may create a statement with a
// SQL-level PREPARE the pooler cannot name. PREPARE TRANSACTION creates
// none, but a conservative match only costs an extra Close and a DISCARD
// ALL on reuse.
func createsPrepared(sql string) bool {
	for _, m := range sqlPrepareRE.FindAllString(sql, -1) {
		if !strings.Contains(strings.ToUpper(m), "TRANSACTION") {
			return true
		}
	}
	return false
}

// closeAction says what the relay does with the CloseComplete answering a
// Close it forwarded.
type closeAction uint8

const (
	// closeForwarded is a router Close: its CloseComplete is dropped like
	// every CloseComplete, but it must keep its place in the queue.
	closeForwarded closeAction = iota
	// closeInjected is the pooler's own Close ahead of a re-parse: its
	// CloseComplete is swallowed.
	closeInjected
	// closeAsParse stands in for a Parse the backend already holds: its
	// CloseComplete is relayed as ParseComplete.
	closeAsParse
)

// noopPortal is the never-bound portal whose Close answers a skipped Parse
// with exactly one CloseComplete.
const noopPortal = "pgshard_noop"
