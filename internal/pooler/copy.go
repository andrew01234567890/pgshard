package pooler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgrepl"
)

// copyTable is one table of the publication with its column layout and the
// key the copy is paginated on (the primary key, or ctid when there is none).
type copyTable struct {
	schema, table string
	relation      *pgshardv1.ChangeEvent_Relation
	keyNames      []string
	keyTypes      []string
	byCtid        bool
}

func (t copyTable) qualified() string { return t.schema + "." + t.table }

// EncodeLastPK renders the key values of a row as a checkpoint: a JSON array
// of text values (ctid is a single element). DecodeLastPK reverses it.
func EncodeLastPK(vals []string) []byte {
	b, _ := json.Marshal(vals)
	return b
}

// DecodeLastPK parses a checkpoint produced by EncodeLastPK.
func DecodeLastPK(b []byte) ([]string, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var vals []string
	if err := json.Unmarshal(b, &vals); err != nil {
		return nil, fmt.Errorf("lastpk: %w", err)
	}
	return vals, nil
}

// CopyTables implements Pooler.CopyTables.
func (s *Server) CopyTables(req *pgshardv1.CopyTablesRequest, srv pgshardv1.Pooler_CopyTablesServer) error {
	return s.runCopy(srv.Context(), req, srv.Send)
}

// exportSnapshot creates the slot the copy is consistent with and returns
// the replication connection holding the exported snapshot open. The
// stream's own slot is created when it does not exist yet; otherwise a
// temporary slot exports the snapshot and dies with the connection.
func (s *Server) exportSnapshot(ctx context.Context, cfg *pgconn.Config, slot string, twoPhase bool) (*pgrepl.Conn, pgrepl.SlotInfo, bool, error) {
	conn, err := pgrepl.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, pgrepl.SlotInfo{}, false, status.Errorf(codes.Unavailable, "replication connection: %v", err)
	}
	info, err := conn.CreateLogicalSlot(ctx, slot, "pgoutput", pgrepl.SlotOptions{Failover: true, TwoPhase: twoPhase, Snapshot: "export"})
	if err == nil {
		return conn, info, true, nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42710" {
		_ = conn.Close(ctx)
		return nil, pgrepl.SlotInfo{}, false, status.Errorf(codes.FailedPrecondition, "create slot %s: %v", slot, err)
	}
	var suffix [6]byte
	_, _ = rand.Read(suffix[:])
	tmp := "pgshard_copy_" + hex.EncodeToString(suffix[:])
	info, err = conn.CreateLogicalSlot(ctx, tmp, "pgoutput", pgrepl.SlotOptions{Temporary: true, Snapshot: "export"})
	if err != nil {
		_ = conn.Close(ctx)
		return nil, pgrepl.SlotInfo{}, false, status.Errorf(codes.FailedPrecondition, "create temporary slot: %v", err)
	}
	return conn, info, false, nil
}

func (s *Server) runCopy(ctx context.Context, req *pgshardv1.CopyTablesRequest, send func(*pgshardv1.CopyTablesResponse) error) error {
	cfg := s.streamDefaults()
	if cfg.DSN == "" {
		return status.Error(codes.FailedPrecondition, "change streams are not configured on this pooler")
	}
	if s.draining.Load() {
		return errUnavailable
	}
	if !catalog.ValidStreamName(req.GetStream()) {
		return status.Error(codes.InvalidArgument, "a valid stream name is required")
	}
	slot := catalog.StreamSlotName(req.GetStream(), cfg.Shard)
	pc, err := pgconn.ParseConfig(cfg.DSN)
	if err != nil {
		return status.Errorf(codes.Internal, "stream dsn: %v", err)
	}
	if req.GetDatabase() != "" {
		pc.Database = req.GetDatabase()
	}
	repl, info, streamSlot, err := s.exportSnapshot(ctx, pc, slot, req.GetTwoPhase())
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = repl.Close(cctx)
	}()
	if err := send(&pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_Snapshot_{Snapshot: &pgshardv1.CopyTablesResponse_Snapshot{
		Slot: info.Name, StreamSlot: streamSlot, ConsistentPoint: uint64(info.ConsistentPoint), SnapshotName: info.SnapshotName}}}); err != nil {
		return err
	}

	conn, err := pgconn.ConnectConfig(ctx, pc)
	if err != nil {
		return status.Errorf(codes.Unavailable, "copy connection: %v", err)
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()
	publication := req.GetPublication()
	if publication == "" {
		publication = "pgshard_all"
	}
	if err := conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT "+quoteLiteral(info.SnapshotName)).Close(); err != nil {
		return status.Errorf(codes.FailedPrecondition, "import snapshot: %v", err)
	}
	tables, err := publicationTables(ctx, conn, publication)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "publication tables: %v", err)
	}
	batch := int(req.GetBatchRows())
	if batch <= 0 {
		batch = 1000
	}
	done := map[string]bool{}
	for _, t := range req.GetDoneTables() {
		done[t] = true
	}
	for _, t := range tables {
		if done[t.qualified()] {
			continue
		}
		var lastpk []string
		if t.schema == req.GetResumeSchema() && t.table == req.GetResumeTable() {
			if lastpk, err = DecodeLastPK(req.GetResumeLastpk()); err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
		}
		if err := send(&pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_TableBegin_{TableBegin: &pgshardv1.CopyTablesResponse_TableBegin{Relation: t.relation, ByCtid: t.byCtid}}}); err != nil {
			return err
		}
		if err := copyRows(ctx, conn, t, lastpk, batch, send); err != nil {
			return err
		}
		if err := send(&pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_TableDone_{TableDone: &pgshardv1.CopyTablesResponse_TableDone{Schema: t.schema, Table: t.table}}}); err != nil {
			return err
		}
	}
	return send(&pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_Done_{Done: &pgshardv1.CopyTablesResponse_Done{}}})
}

const publicationTablesSQL = `
SELECT pt.schemaname, pt.tablename, c.oid, c.relreplident::text,
       a.attname, a.atttypid, a.atttypmod, format_type(a.atttypid, a.atttypmod),
       COALESCE(array_position((SELECT array_agg(k) FROM unnest(i.indkey[0:i.indnkeyatts-1]) k), a.attnum), 0) AS pkpos
FROM pg_publication_tables pt
JOIN pg_namespace n ON n.nspname = pt.schemaname
JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
WHERE pt.pubname = $1
ORDER BY pt.schemaname, pt.tablename, a.attnum`

// publicationTables lists the tables of a publication with their columns
// inside the current snapshot, in schema.table order.
func publicationTables(ctx context.Context, conn *pgconn.PgConn, publication string) ([]copyTable, error) {
	res := conn.ExecParams(ctx, publicationTablesSQL, [][]byte{[]byte(publication)}, nil, nil, nil).Read()
	if res.Err != nil {
		return nil, res.Err
	}
	var out []copyTable
	type keyCol struct {
		pos        int
		name, kind string
	}
	keys := map[string][]keyCol{}
	for _, row := range res.Rows {
		schema, table := string(row[0]), string(row[1])
		if len(out) == 0 || out[len(out)-1].schema != schema || out[len(out)-1].table != table {
			oid, _ := parseUint(string(row[2]))
			out = append(out, copyTable{schema: schema, table: table,
				relation: &pgshardv1.ChangeEvent_Relation{RelationId: uint32(oid), Schema: schema, Table: table, ReplicaIdentity: string(row[3])}})
		}
		t := &out[len(out)-1]
		typ, _ := parseUint(string(row[5]))
		mod, _ := parseInt(string(row[6]))
		pos, _ := parseInt(string(row[8]))
		t.relation.Columns = append(t.relation.Columns, &pgshardv1.ChangeEvent_Relation_Column{Name: string(row[4]), TypeOid: uint32(typ), TypeModifier: int32(mod), Key: pos > 0})
		if pos > 0 {
			keys[t.qualified()] = append(keys[t.qualified()], keyCol{pos: int(pos), name: string(row[4]), kind: string(row[7])})
		}
	}
	for i := range out {
		t := &out[i]
		cols := keys[t.qualified()]
		if len(cols) == 0 {
			t.byCtid = true
			t.keyNames, t.keyTypes = []string{"ctid"}, []string{"tid"}
			continue
		}
		for pos := 1; pos <= len(cols); pos++ {
			for _, c := range cols {
				if c.pos == pos {
					t.keyNames = append(t.keyNames, pgx.Identifier{c.name}.Sanitize())
					t.keyTypes = append(t.keyTypes, c.kind)
				}
			}
		}
	}
	return out, nil
}

// keysetSQL builds the paginated select of one table: rows with a key above
// the checkpoint (when any), in key order, limit rows.
func keysetSQL(t copyTable, afterKey bool, limit int) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	if t.byCtid {
		b.WriteString("ctid::text, ")
	}
	fmt.Fprintf(&b, "* FROM %s", pgx.Identifier{t.schema, t.table}.Sanitize())
	if afterKey {
		params := make([]string, len(t.keyTypes))
		for i, typ := range t.keyTypes {
			params[i] = fmt.Sprintf("$%d::%s", i+1, typ)
		}
		fmt.Fprintf(&b, " WHERE (%s) > (%s)", strings.Join(t.keyNames, ", "), strings.Join(params, ", "))
	}
	fmt.Fprintf(&b, " ORDER BY %s LIMIT %d", strings.Join(t.keyNames, ", "), limit)
	return b.String()
}

// keyColumnIndexes maps the key order to column positions of SELECT *.
func keyColumnIndexes(t copyTable) []int {
	if t.byCtid {
		return nil
	}
	idx := make([]int, 0, len(t.keyNames))
	for _, name := range t.keyNames {
		for i, c := range t.relation.Columns {
			if (pgx.Identifier{c.Name}).Sanitize() == name {
				idx = append(idx, i)
			}
		}
	}
	return idx
}

func copyRows(ctx context.Context, conn *pgconn.PgConn, t copyTable, lastpk []string, batch int, send func(*pgshardv1.CopyTablesResponse) error) error {
	keyIdx := keyColumnIndexes(t)
	for {
		sql := keysetSQL(t, lastpk != nil, batch)
		var args [][]byte
		for _, v := range lastpk {
			args = append(args, []byte(v))
		}
		res := conn.ExecParams(ctx, sql, args, nil, nil, nil).Read()
		if res.Err != nil {
			return status.Errorf(codes.Aborted, "copy %s: %v", t.qualified(), res.Err)
		}
		if len(res.Rows) == 0 {
			return nil
		}
		out := &pgshardv1.CopyTablesResponse_Rows{}
		for _, row := range res.Rows {
			vals := row
			if t.byCtid {
				vals = row[1:]
			}
			r := &pgshardv1.CopyTablesResponse_Row{Values: make([]*pgshardv1.Value, len(vals))}
			for i, v := range vals {
				if v == nil {
					r.Values[i] = &pgshardv1.Value{Null: true}
				} else {
					r.Values[i] = &pgshardv1.Value{Data: append([]byte(nil), v...)}
				}
			}
			out.Rows = append(out.Rows, r)
		}
		last := res.Rows[len(res.Rows)-1]
		if t.byCtid {
			lastpk = []string{string(last[0])}
		} else {
			lastpk = make([]string, len(keyIdx))
			for i, ci := range keyIdx {
				lastpk[i] = string(last[ci])
			}
		}
		out.Lastpk = EncodeLastPK(lastpk)
		if err := send(&pgshardv1.CopyTablesResponse{Response: &pgshardv1.CopyTablesResponse_Rows_{Rows: out}}); err != nil {
			return err
		}
		if len(res.Rows) < batch {
			return nil
		}
	}
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func parseUint(s string) (uint64, error) {
	var v uint64
	_, err := fmt.Sscan(s, &v)
	return v, err
}

func parseInt(s string) (int64, error) {
	var v int64
	_, err := fmt.Sscan(s, &v)
	return v, err
}
