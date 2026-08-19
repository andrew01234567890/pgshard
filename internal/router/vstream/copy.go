package vstream

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// copyPhase is one shard's initial copy: the tables already copied and the
// checkpoint of the table in progress. It is rebuilt from a VCopyState on
// resume and advanced as the pooler delivers batches, so a reconnect or a
// resumed stream continues after the last delivered key.
type copyPhase struct {
	done    []string
	current *pgshardv1.VCopyState_Table
	batch   uint32
}

func copyPhaseFrom(st *pgshardv1.VCopyState, batch uint32) *copyPhase {
	p := &copyPhase{batch: batch}
	if st == nil {
		return p
	}
	p.done = append(p.done, st.GetDone()...)
	if c := st.GetCurrent(); c != nil {
		p.current = &pgshardv1.VCopyState_Table{Schema: c.GetSchema(), Table: c.GetTable(), Lastpk: append([]byte(nil), c.GetLastpk()...)}
	}
	return p
}

// state snapshots the phase as the VCopyState of sh.
func (p *copyPhase) state(sh router.Shard) *pgshardv1.VCopyState {
	st := &pgshardv1.VCopyState{Shard: shardRef(sh), Done: append([]string(nil), p.done...)}
	if p.current != nil {
		st.Current = &pgshardv1.VCopyState_Table{Schema: p.current.Schema, Table: p.current.Table, Lastpk: append([]byte(nil), p.current.Lastpk...)}
	}
	return st
}

// request builds the pooler request that continues the copy from the phase.
func (p *copyPhase) request(stream, database string, twoPhase bool) *pgshardv1.CopyTablesRequest {
	req := &pgshardv1.CopyTablesRequest{Stream: stream, Database: database, TwoPhase: twoPhase, BatchRows: p.batch, DoneTables: append([]string(nil), p.done...)}
	if p.current != nil && len(p.current.Lastpk) > 0 {
		req.ResumeSchema, req.ResumeTable, req.ResumeLastpk = p.current.Schema, p.current.Table, p.current.Lastpk
	}
	return req
}

func (p *copyPhase) isDone(schema, table string) bool {
	for _, d := range p.done {
		if d == schema+"."+table {
			return true
		}
	}
	return false
}

// copyOnce runs one CopyTables call from the current checkpoint and pushes
// its messages as units; it returns nil once the pooler reported Done.
func (r *reader) copyOnce(ctx context.Context) error {
	client, err := r.topo.Client(r.shard)
	if err != nil {
		return err
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.CopyTables(sctx, r.copy.request(r.stream, r.database, r.twoPhase))
	if err != nil {
		return err
	}
	sh := shardRef(r.shard)
	var rel *relMeta
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		var u *unit
		switch m := msg.GetResponse().(type) {
		case *pgshardv1.CopyTablesResponse_Snapshot_:
			if m.Snapshot.GetConsistentPoint() == 0 {
				return status.Error(codes.FailedPrecondition, "copy: snapshot without a consistent point")
			}
			u = &unit{shard: r.shard, copy: r.copy.state(r.shard)}
			if r.delivered == 0 {
				u.position, u.endLSN = true, m.Snapshot.GetConsistentPoint()
			}
		case *pgshardv1.CopyTablesResponse_TableBegin_:
			relation := m.TableBegin.GetRelation()
			if r.copy.isDone(relation.GetSchema(), relation.GetTable()) {
				return status.Errorf(codes.FailedPrecondition, "copy: pooler resent done table %s.%s", relation.GetSchema(), relation.GetTable())
			}
			rel = relMetaOf(relation)
			if r.copy.current == nil || r.copy.current.Schema != rel.schema || r.copy.current.Table != rel.table {
				r.copy.current = &pgshardv1.VCopyState_Table{Schema: rel.schema, Table: rel.table}
			}
			u = &unit{shard: r.shard, copy: r.copy.state(r.shard),
				events: []*pgshardv1.VEvent{{Event: &pgshardv1.VEvent_CopyBegin_{CopyBegin: &pgshardv1.VEvent_CopyBegin{Shard: sh, Schema: rel.schema, Table: rel.table}}}},
				rels:   []*relMeta{nil}, xids: []uint32{0}}
		case *pgshardv1.CopyTablesResponse_Rows_:
			if rel == nil || r.copy.current == nil {
				return status.Error(codes.FailedPrecondition, "copy: rows before a table")
			}
			u = &unit{shard: r.shard}
			for _, row := range m.Rows.GetRows() {
				ev := &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Row_{Row: &pgshardv1.VEvent_Row{Shard: sh, Schema: rel.schema, Table: rel.table,
					Kind: pgshardv1.VEvent_Row_KIND_INSERT, New: tuple(row.GetValues(), nil), Copy: true}}}
				u.events = append(u.events, ev)
				u.rels = append(u.rels, rel)
				u.xids = append(u.xids, 0)
			}
			r.copy.current.Lastpk = append([]byte(nil), m.Rows.GetLastpk()...)
			u.copy = r.copy.state(r.shard)
		case *pgshardv1.CopyTablesResponse_TableDone_:
			name := m.TableDone.GetSchema() + "." + m.TableDone.GetTable()
			r.copy.done = append(r.copy.done, name)
			r.copy.current = nil
			rel = nil
			u = &unit{shard: r.shard, copy: r.copy.state(r.shard),
				events: []*pgshardv1.VEvent{{Event: &pgshardv1.VEvent_CopyCompleted_{CopyCompleted: &pgshardv1.VEvent_CopyCompleted{Shard: sh, Schema: m.TableDone.GetSchema(), Table: m.TableDone.GetTable()}}}},
				rels:   []*relMeta{nil}, xids: []uint32{0}}
		case *pgshardv1.CopyTablesResponse_Done_:
			u = &unit{shard: r.shard, copyDone: true,
				events: []*pgshardv1.VEvent{{Event: &pgshardv1.VEvent_CopyCompleted_{CopyCompleted: &pgshardv1.VEvent_CopyCompleted{Shard: sh}}}},
				rels:   []*relMeta{nil}, xids: []uint32{0}}
			if !r.push(ctx, u) {
				return ctx.Err()
			}
			r.copy = nil
			return nil
		default:
			return status.Errorf(codes.FailedPrecondition, "copy: unhandled message %T", msg.GetResponse())
		}
		if !r.push(ctx, u) {
			return ctx.Err()
		}
		if u.position {
			r.delivered = u.endLSN
		}
	}
}

func relMetaOf(rel *pgshardv1.ChangeEvent_Relation) *relMeta {
	m := &relMeta{schema: rel.GetSchema(), table: rel.GetTable(), identity: rel.GetReplicaIdentity()}
	var sig strings.Builder
	sig.WriteString(m.identity)
	for _, c := range rel.GetColumns() {
		m.columns = append(m.columns, &pgshardv1.VEvent_Relation_Column{Name: c.GetName(), TypeOid: c.GetTypeOid(), TypeModifier: c.GetTypeModifier(), Key: c.GetKey()})
		fmt.Fprintf(&sig, "|%s:%d:%d:%t", c.GetName(), c.GetTypeOid(), c.GetTypeModifier(), c.GetKey())
	}
	m.signature = sig.String()
	return m
}
