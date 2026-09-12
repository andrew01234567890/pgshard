package plan

import (
	"strings"

	pgquerypb "github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
)

// Copy is what a COPY ... FROM STDIN into a sharded table needs at
// execution: which column carries the shard key, and how many columns the
// statement names.
//
// The router reads the stream, takes that column out of every row and sends
// the row to the shard the key belongs to. It needs the column's POSITION
// rather than its name because the stream carries no names.
type Copy struct {
	// KeyColumn is the zero-based position of the shard key among the
	// copied columns.
	KeyColumn int
	// Columns is how many columns the statement names, which is how a row
	// with too few is caught before it is sent anywhere.
	Columns int
}

// copyIn plans COPY <table> FROM STDIN.
//
// The shard key must be among the copied columns, because a row whose key
// the router cannot read is a row it cannot place. PostgreSQL's own COPY
// takes the table's column order when the statement names none, so an
// unnamed list is resolved against the declared columns.
func (w *walker) copyIn(c *pgquerypb.CopyStmt, r *rel) error {
	if err := refuseCopyOptions(c); err != nil {
		return err
	}
	cols := stringListOf(c.GetAttlist())
	if len(cols) == 0 {
		return notYet("COPY into a sharded table must name its columns",
			"list the columns, including the shard key \""+r.shardKey+"\": COPY "+r.name+" (…) FROM STDIN")
	}
	at := -1
	for i, name := range cols {
		if strings.EqualFold(name, r.shardKey) {
			at = i
		}
	}
	if at < 0 {
		return notYet("COPY into a sharded table must include the shard key \""+r.shardKey+"\" in its columns",
			"the router reads the key out of every row to know which shard it belongs to")
	}
	w.plan.Copy = &Copy{KeyColumn: at, Columns: len(cols)}
	w.plan.Kind, w.plan.Shards = Scatter, w.sess.servingShards()
	return nil
}

// refuseCopyOptions refuses by name what a sharded COPY cannot carry.
//
// Per-row error handling is the piece that does not survive a fan-out: the
// router has already sent earlier rows to other shards by the time one is
// rejected, so ON_ERROR and its friends would report a count that is not
// true of the load as a whole. FORMAT csv and binary are refused because
// splitting them needs quote- and length-aware reading, which is its own
// piece of work.
func refuseCopyOptions(c *pgquerypb.CopyStmt) error {
	for _, o := range c.GetOptions() {
		d := o.GetDefElem()
		if d == nil {
			continue
		}
		name := strings.ToLower(d.GetDefname())
		switch name {
		case "format":
			f := strings.ToLower(defElemText(d))
			if f != "" && f != "text" {
				return notYet("COPY FORMAT "+f+" into a sharded table is not available yet",
					"the text format is what COPY FROM STDIN uses by default; load with it, or filter on one shard key value")
			}
		case "on_error", "reject_limit", "log_verbosity":
			return notYet("COPY "+strings.ToUpper(name)+" into a sharded table is not available yet",
				"a row rejected on one shard cannot unsend the rows already accepted on the others")
		case "where":
			return notYet("COPY ... WHERE into a sharded table is not available yet", "filter the data before loading it")
		}
	}
	if c.GetIsProgram() {
		return notYet("COPY FROM PROGRAM into a sharded table is not available yet", "pipe the program's output into COPY FROM STDIN")
	}
	if f := c.GetFilename(); f != "" {
		return notYet("COPY FROM a file into a sharded table is not available yet",
			"the file is read by the server, which is one shard; use COPY FROM STDIN so the router can route the rows")
	}
	return nil
}

// defElemText reads a COPY option's argument. The grammar hands it over as
// a bare String node rather than an A_Const, which is the same shape
// EXPLAIN's options arrive in and not the shape a value expression has.
func defElemText(d *pgquerypb.DefElem) string {
	arg := d.GetArg()
	switch {
	case arg == nil:
		return ""
	case arg.GetString_() != nil:
		return arg.GetString_().GetSval()
	case arg.GetInteger() != nil:
		return "integer"
	}
	return constText(arg.GetAConst())
}

// stringListOf renders a list of String nodes, which is how the grammar
// carries a COPY column list.
func stringListOf(list []*pgquerypb.Node) []string {
	out := make([]string, 0, len(list))
	for _, n := range list {
		if s := n.GetString_(); s != nil {
			out = append(out, s.GetSval())
		}
	}
	return out
}
