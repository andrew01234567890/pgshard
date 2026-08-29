package pooler

import (
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

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
	if !isASCII(sql) {
		// Unicode case folding can turn a non-ASCII letter into an ASCII
		// one -- dotless i uppercases to I -- so a scan that folds only
		// ASCII could miss a statement this has to catch. Vanishingly
		// rare, and a miss leaves stale prepared state on a recycled
		// backend, so those strings take the old path.
		u := strings.ToUpper(sql)
		return strings.Contains(u, "DEALLOCATE") || strings.Contains(u, "DISCARD")
	}
	return containsFold(sql, "DEALLOCATE") || containsFold(sql, "DISCARD")
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// containsFold reports whether s contains word, which must be upper-case
// ASCII, ignoring case and without allocating. Every SimpleQuery and Parse
// passes through here, and uppercasing the whole statement to look for a
// rare state transition cost an allocation and a full scan on every
// ordinary SELECT.
func containsFold(s, word string) bool {
	n := len(word)
	for i := 0; i+n <= len(s); i++ {
		if upperASCII(s[i]) != word[0] {
			continue
		}
		j := 1
		for ; j < n; j++ {
			if upperASCII(s[i+j]) != word[j] {
				break
			}
		}
		if j == n {
			return true
		}
	}
	return false
}

func upperASCII(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - ('a' - 'A')
	}
	return b
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
