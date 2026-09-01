package vstream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/proto"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pooler"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// unit is what a shard reader hands the merger: one whole transaction, a
// standalone event (commit/rollback of a prepared transaction or a
// non-transactional message), a bare position advance, or a terminal error.
type unit struct {
	shard  router.Shard
	events []*pgshardv1.VEvent
	// rels[i] is the relation metadata a consumer must have seen before
	// events[i]; a Row carries one, a Truncate one per truncated table.
	rels     [][]*relMeta
	xids     []uint32
	endLSN   uint64
	commitTS int64
	// position is true when endLSN may become the shard's position once the
	// events were delivered.
	position bool
	// copy is the shard's copy state after this unit; copyDone marks the
	// end of the shard's copy phase.
	copy     *pgshardv1.VCopyState
	copyDone bool
	err      *pgshardv1.VEvent_Error
}

// relMeta is a decoded Relation; rows point at the instance current when
// they were decoded so a later column change never rewrites history.
type relMeta struct {
	schema, table string
	identity      string
	columns       []*pgshardv1.VEvent_Relation_Column
	signature     string
}

func (m *relMeta) event() *pgshardv1.VEvent {
	return &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Relation_{Relation: &pgshardv1.VEvent_Relation{
		Schema: m.schema, Table: m.table, ReplicaIdentity: m.identity, Columns: m.columns}}}
}

func shardRef(sh router.Shard) *pgshardv1.ShardRef {
	return &pgshardv1.ShardRef{ShardSet: sh.Set, ShardId: uint32(sh.ID)}
}

// reader keeps one shard's pooler stream open from a position, reconnecting
// to whatever pooler serves the shard after a promotion, and assembles the
// batches into units pushed to out.
type reader struct {
	shard     router.Shard
	stream    string
	database  string
	twoPhase  bool
	topo      Topology
	out       chan *unit
	ready     chan<- struct{}
	window    time.Duration
	delivered uint64
	maxBytes  int
	maxOpen   int
	// copy is the pending initial copy; nil once streaming.
	copy *copyPhase

	asm assembler
}

var errEpochChanged = errors.New("primary epoch changed")

func (r *reader) run(ctx context.Context) {
	r.asm = assembler{shard: r.shard, relations: map[uint32]*relMeta{}, streamed: map[uint32]*unit{},
		maxBytes: r.maxBytes, maxOpen: r.maxOpen}
	backoff := 200 * time.Millisecond
	var firstFailure time.Time
	for {
		var err error
		if r.copy != nil {
			err = r.copyOnce(ctx)
		} else {
			err = r.once(ctx)
		}
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			firstFailure = time.Time{}
			backoff = 200 * time.Millisecond
			continue
		}
		if ev := fatal(err, r.shard); ev != nil {
			r.push(ctx, &unit{shard: r.shard, err: ev})
			return
		}
		if firstFailure.IsZero() {
			firstFailure = time.Now()
		}
		if time.Since(firstFailure) > r.window {
			r.push(ctx, &unit{shard: r.shard, err: &pgshardv1.VEvent_Error{Code: pgshardv1.VEvent_Error_CODE_SHARD_UNAVAILABLE,
				Message: fmt.Sprintf("shard %s/%d: %v", r.shard.Set, r.shard.ID, err), Shard: shardRef(r.shard)}})
			return
		}
		if !errors.Is(err, errEpochChanged) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 2*time.Second {
				backoff *= 2
			}
		}
	}
}

// positionGoneText recognises, in the message of a pooler too old to send
// the structured reason, the failures that mean the position itself is
// gone: an invalidated or absent slot, or WAL that has been removed.
func positionGoneText(msg string) bool {
	for _, s := range []string{"invalidated", "can no longer get changes", "has already been removed"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	// "does not exist" alone is not enough: a missing publication says it
	// too, and that is a configuration mistake rather than a lost position.
	return strings.Contains(msg, "replication slot") && strings.Contains(msg, "does not exist")
}

// fatal maps a pooler failure that no reconnect can cure to an Error event.
func fatal(err error, sh router.Shard) *pgshardv1.VEvent_Error {
	// The router's own limit, not the pooler's answer: reconnecting would
	// reassemble the same transaction and overrun the same buffer. The
	// last delivered position stands, so the consumer resumes from it.
	var big *errTooLarge
	if errors.As(err, &big) {
		return &pgshardv1.VEvent_Error{Code: pgshardv1.VEvent_Error_CODE_TRANSACTION_TOO_LARGE,
			Message: big.Error(), Shard: shardRef(sh)}
	}
	st, ok := status.FromError(err)
	if !ok || (st.Code() != codes.FailedPrecondition && st.Code() != codes.InvalidArgument) {
		return nil
	}
	code := pgshardv1.VEvent_Error_CODE_INTERNAL
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		switch info.GetReason() {
		case pooler.ReasonPositionTooOld:
			code = pgshardv1.VEvent_Error_CODE_POSITION_TOO_OLD
		case pooler.ReasonReaderActive:
			// Another reader holds the slot: the consumer reconnects, and
			// telling it the position is gone would make it re-copy.
			return nil
		}
	}
	// A pooler from before the structured reason only carries the text.
	// Kept as a fallback for a rolling upgrade, not as the contract.
	if strings.Contains(st.Message(), "active reader") {
		return nil
	}
	// Poolers from before the structured detail only carry the text, and
	// the text has to say the position is gone rather than merely that a
	// StartReplication failed: a consumer told POSITION_TOO_OLD discards
	// its checkpoints and copies everything again, which is a costly answer
	// to a missing publication or a rejected option.
	if code == pgshardv1.VEvent_Error_CODE_INTERNAL && strings.Contains(st.Message(), "start replication") &&
		positionGoneText(st.Message()) {
		code = pgshardv1.VEvent_Error_CODE_POSITION_TOO_OLD
	}
	return &pgshardv1.VEvent_Error{Code: code, Message: fmt.Sprintf("shard %s/%d: %s", sh.Set, sh.ID, st.Message()), Shard: shardRef(sh)}
}

// once opens one pooler stream at the last delivered position and consumes
// it until it fails, the context ends or the shard's primary epoch changes.
func (r *reader) once(ctx context.Context) error {
	epoch := r.topo.Epoch(r.shard)
	client, err := r.topo.Client(r.shard)
	if err != nil {
		return err
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req := &pgshardv1.StreamRequest{Stream: r.stream, Database: r.database, StartLsn: r.delivered}
	if r.twoPhase {
		req.Options = map[string]string{"two_phase": "on"}
	}
	stream, err := client.Stream(sctx, req)
	if err != nil {
		return err
	}
	r.asm.reset()
	for {
		batch, err := stream.Recv()
		if err != nil {
			return err
		}
		if r.topo.Epoch(r.shard) != epoch {
			return errEpochChanged
		}
		for _, ev := range batch.GetEvents() {
			u, err := r.asm.add(ev)
			if err != nil {
				var big *errTooLarge
				if errors.As(err, &big) {
					// Returned as itself: wrapping it in a status turns
					// the router's own limit into a decode failure, and
					// the consumer would be told INTERNAL for something
					// it can act on.
					return err
				}
				return status.Errorf(codes.FailedPrecondition, "decode: %v", err)
			}
			if u == nil {
				continue
			}
			if u.position && u.endLSN <= r.delivered {
				continue
			}
			if !r.push(ctx, u) {
				return ctx.Err()
			}
			if u.position {
				r.delivered = u.endLSN
			}
		}
	}
}

func (r *reader) push(ctx context.Context, u *unit) bool {
	select {
	case r.out <- u:
	case <-ctx.Done():
		return false
	}
	select {
	case r.ready <- struct{}{}:
	default:
	}
	return true
}

// assembler turns one shard's ChangeEvents into units. Plain transactions
// arrive Begin..Commit in one run; streamed (in-progress) transactions
// arrive in segments that may interleave and are kept per xid until their
// commit, prepare or abort.
type assembler struct {
	shard     router.Shard
	relations map[uint32]*relMeta
	cur       *unit
	streamed  map[uint32]*unit
	inStream  uint32
	// buffered is the encoded size of every event held for a transaction
	// that has not committed yet, and maxBytes bounds it. The unit channel
	// bounds transactions that are already assembled; nothing bounded the
	// one being assembled, so a single large transaction, or enough
	// interleaved open ones, could take the router's memory with it -- and
	// the router is not only serving this stream.
	buffered int
	maxBytes int
	// maxOpen bounds interleaved streamed transactions. Bytes alone would
	// let a very large number of small ones through, each with its own
	// map entry and slices.
	maxOpen int
}

// errTooLarge ends a stream whose buffer a transaction did not fit in. The
// last delivered position stands, so a consumer resumes from it.
type errTooLarge struct{ msg string }

func (e *errTooLarge) Error() string { return e.msg }

func (a *assembler) reset() {
	a.cur = nil
	a.streamed = map[uint32]*unit{}
	a.inStream = 0
	a.buffered = 0
}

func (a *assembler) open(ev *pgshardv1.VEvent, ts int64) *unit {
	return &unit{shard: a.shard, events: []*pgshardv1.VEvent{ev}, rels: [][]*relMeta{nil}, xids: []uint32{0}, commitTS: ts, position: true}
}

func (a *assembler) target() *unit {
	if a.inStream != 0 {
		return a.streamed[a.inStream]
	}
	return a.cur
}

func (a *assembler) append(u *unit, ev *pgshardv1.VEvent, xid uint32, rels ...*relMeta) {
	if u == nil {
		return
	}
	u.events = append(u.events, ev)
	u.rels = append(u.rels, rels)
	u.xids = append(u.xids, xid)
	a.buffered += proto.Size(ev)
}

// fits reports whether the transactions being assembled are still within
// the buffer, and says what overran when they are not.
func (a *assembler) fits() error {
	if a.maxBytes > 0 && a.buffered > a.maxBytes {
		return &errTooLarge{fmt.Sprintf("shard %s/%d: %d bytes of uncommitted transactions exceed the %d-byte stream buffer",
			a.shard.Set, a.shard.ID, a.buffered, a.maxBytes)}
	}
	if a.maxOpen > 0 && len(a.streamed) > a.maxOpen {
		return &errTooLarge{fmt.Sprintf("shard %s/%d: %d interleaved in-progress transactions exceed the limit of %d",
			a.shard.Set, a.shard.ID, len(a.streamed), a.maxOpen)}
	}
	return nil
}

// done releases what a finished transaction was holding.
func (a *assembler) done(u *unit) *unit {
	if u != nil {
		for _, ev := range u.events {
			a.buffered -= proto.Size(ev)
		}
		if a.buffered < 0 {
			a.buffered = 0
		}
	}
	return u
}

func (a *assembler) idle() bool { return a.cur == nil && len(a.streamed) == 0 }

func (a *assembler) begin(xid uint32, ts int64, gid string) *pgshardv1.VEvent {
	return &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Begin_{Begin: &pgshardv1.VEvent_Begin{Shard: shardRef(a.shard), Xid: xid, CommitTs: ts, Gid: gid}}}
}

// add consumes one event and returns a finished unit, if any.
func (a *assembler) add(ev *pgshardv1.ChangeEvent) (*unit, error) {
	sh := shardRef(a.shard)
	switch e := ev.GetEvent().(type) {
	case *pgshardv1.ChangeEvent_Begin_:
		a.cur = a.open(a.begin(e.Begin.GetXid(), e.Begin.GetCommitTs(), ""), e.Begin.GetCommitTs())
	case *pgshardv1.ChangeEvent_Commit_:
		u := a.cur
		a.cur = nil
		if u == nil {
			return nil, nil
		}
		a.append(u, &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Commit_{Commit: &pgshardv1.VEvent_Commit{Shard: sh, Lsn: e.Commit.GetCommitLsn(), EndLsn: e.Commit.GetEndLsn()}}}, 0)
		u.endLSN = e.Commit.GetEndLsn()
		return a.done(u), nil
	case *pgshardv1.ChangeEvent_BeginPrepare_:
		a.cur = a.open(a.begin(e.BeginPrepare.GetXid(), e.BeginPrepare.GetPrepareTs(), e.BeginPrepare.GetGid()), e.BeginPrepare.GetPrepareTs())
	case *pgshardv1.ChangeEvent_Prepare_:
		u := a.cur
		a.cur = nil
		if u == nil {
			return nil, nil
		}
		a.append(u, &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Prepare_{Prepare: &pgshardv1.VEvent_Prepare{Shard: sh, Gid: e.Prepare.GetGid(), Lsn: e.Prepare.GetPrepareLsn()}}}, 0)
		u.endLSN = e.Prepare.GetEndLsn()
		return a.done(u), nil
	case *pgshardv1.ChangeEvent_CommitPrepared_:
		u := a.open(&pgshardv1.VEvent{Event: &pgshardv1.VEvent_CommitPrepared_{CommitPrepared: &pgshardv1.VEvent_CommitPrepared{Shard: sh, Gid: e.CommitPrepared.GetGid(), Lsn: e.CommitPrepared.GetCommitLsn()}}}, 0)
		u.endLSN = e.CommitPrepared.GetEndLsn()
		return a.done(u), nil
	case *pgshardv1.ChangeEvent_RollbackPrepared_:
		u := a.open(&pgshardv1.VEvent{Event: &pgshardv1.VEvent_RollbackPrepared_{RollbackPrepared: &pgshardv1.VEvent_RollbackPrepared{Shard: sh, Gid: e.RollbackPrepared.GetGid(), Lsn: e.RollbackPrepared.GetRollbackLsn()}}}, 0)
		u.endLSN = e.RollbackPrepared.GetEndLsn()
		return a.done(u), nil
	case *pgshardv1.ChangeEvent_Relation_:
		a.relations[e.Relation.GetRelationId()] = relMetaOf(e.Relation)
	case *pgshardv1.ChangeEvent_Row_:
		rel, ok := a.relations[e.Row.GetRelationId()]
		if !ok {
			return nil, fmt.Errorf("row for unknown relation %d", e.Row.GetRelationId())
		}
		row := &pgshardv1.VEvent_Row{Shard: sh, Schema: rel.schema, Table: rel.table, Kind: rowKind(e.Row.GetKind()), OldIsKey: e.Row.GetOldIsKey()}
		if e.Row.GetOld() != nil {
			row.Old = tuple(e.Row.GetOld(), nil)
		}
		if e.Row.GetNew() != nil {
			row.New = tuple(e.Row.GetNew(), e.Row.GetUnchangedToast())
		}
		a.append(a.target(), &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Row_{Row: row}}, ev.GetXid(), rel)
	case *pgshardv1.ChangeEvent_Truncate_:
		t := &pgshardv1.VEvent_Truncate{Shard: sh, Cascade: e.Truncate.GetCascade(), RestartIdentity: e.Truncate.GetRestartIdentity()}
		var rels []*relMeta
		for _, id := range e.Truncate.GetRelationIds() {
			if rel, ok := a.relations[id]; ok {
				t.Tables = append(t.Tables, &pgshardv1.VEvent_Truncate_Table{Schema: rel.schema, Table: rel.table})
				rels = append(rels, rel)
			}
		}
		a.append(a.target(), &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Truncate_{Truncate: t}}, ev.GetXid(), rels...)
	case *pgshardv1.ChangeEvent_Message_:
		msg := &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Message_{Message: &pgshardv1.VEvent_Message{Shard: sh, Prefix: e.Message.GetPrefix(), Content: e.Message.GetContent(), Transactional: e.Message.GetTransactional()}}}
		if !e.Message.GetTransactional() {
			return &unit{shard: a.shard, events: []*pgshardv1.VEvent{msg}, rels: [][]*relMeta{nil}, xids: []uint32{0}}, nil
		}
		a.append(a.target(), msg, ev.GetXid())
	case *pgshardv1.ChangeEvent_Keepalive_:
		if a.idle() && e.Keepalive.GetWalEnd() > 0 {
			return &unit{shard: a.shard, endLSN: e.Keepalive.GetWalEnd(), position: true}, nil
		}
	case *pgshardv1.ChangeEvent_StreamStart_:
		xid := e.StreamStart.GetXid()
		a.inStream = xid
		if _, ok := a.streamed[xid]; !ok {
			a.streamed[xid] = a.open(a.begin(xid, 0, ""), 0)
		}
	case *pgshardv1.ChangeEvent_StreamStop_:
		a.inStream = 0
	case *pgshardv1.ChangeEvent_StreamCommit_:
		u := a.streamed[e.StreamCommit.GetXid()]
		delete(a.streamed, e.StreamCommit.GetXid())
		if u == nil {
			return nil, nil
		}
		u.events[0].GetBegin().CommitTs = e.StreamCommit.GetCommitTs()
		u.commitTS = e.StreamCommit.GetCommitTs()
		a.append(u, &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Commit_{Commit: &pgshardv1.VEvent_Commit{Shard: sh, Lsn: e.StreamCommit.GetCommitLsn(), EndLsn: e.StreamCommit.GetEndLsn()}}}, 0)
		u.endLSN = e.StreamCommit.GetEndLsn()
		return a.done(u), nil
	case *pgshardv1.ChangeEvent_StreamPrepare_:
		u := a.streamed[e.StreamPrepare.GetXid()]
		delete(a.streamed, e.StreamPrepare.GetXid())
		if u == nil {
			return nil, nil
		}
		u.events[0].GetBegin().CommitTs = e.StreamPrepare.GetPrepareTs()
		u.events[0].GetBegin().Gid = e.StreamPrepare.GetGid()
		u.commitTS = e.StreamPrepare.GetPrepareTs()
		a.append(u, &pgshardv1.VEvent{Event: &pgshardv1.VEvent_Prepare_{Prepare: &pgshardv1.VEvent_Prepare{Shard: sh, Gid: e.StreamPrepare.GetGid(), Lsn: e.StreamPrepare.GetPrepareLsn()}}}, 0)
		u.endLSN = e.StreamPrepare.GetEndLsn()
		return a.done(u), nil
	case *pgshardv1.ChangeEvent_StreamAbort_:
		xid, sub := e.StreamAbort.GetXid(), e.StreamAbort.GetSubxid()
		if sub == 0 || sub == xid {
			a.done(a.streamed[xid])
			delete(a.streamed, xid)
			break
		}
		if u := a.streamed[xid]; u != nil {
			u.dropXid(sub)
		}
	case *pgshardv1.ChangeEvent_Origin_:
	default:
		return nil, fmt.Errorf("unhandled change event %T", ev.GetEvent())
	}
	// Checked once per consumed event, which is the only place the buffer
	// can have grown.
	return nil, a.fits()
}

func (u *unit) dropXid(xid uint32) {
	n := 0
	for i := range u.events {
		if u.xids[i] == xid {
			continue
		}
		u.events[n], u.rels[n], u.xids[n] = u.events[i], u.rels[i], u.xids[i]
		n++
	}
	u.events, u.rels, u.xids = u.events[:n], u.rels[:n], u.xids[:n]
}

func rowKind(k pgshardv1.ChangeEvent_Row_Kind) pgshardv1.VEvent_Row_Kind {
	switch k {
	case pgshardv1.ChangeEvent_Row_KIND_INSERT:
		return pgshardv1.VEvent_Row_KIND_INSERT
	case pgshardv1.ChangeEvent_Row_KIND_UPDATE:
		return pgshardv1.VEvent_Row_KIND_UPDATE
	case pgshardv1.ChangeEvent_Row_KIND_DELETE:
		return pgshardv1.VEvent_Row_KIND_DELETE
	}
	return pgshardv1.VEvent_Row_KIND_UNSPECIFIED
}

func tuple(vals []*pgshardv1.Value, unchanged []uint32) *pgshardv1.VTuple {
	t := &pgshardv1.VTuple{Columns: make([]*pgshardv1.VColumn, len(vals))}
	for i, v := range vals {
		t.Columns[i] = &pgshardv1.VColumn{Null: v.GetNull(), Value: v.GetData()}
	}
	for _, i := range unchanged {
		if int(i) < len(t.Columns) {
			t.Columns[i].UnchangedToast = true
		}
	}
	return t
}
