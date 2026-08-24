package vstream

import (
	"context"
	"sort"
	"time"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// options are the resolved per-Stream settings.
type options struct {
	twoPhase      bool
	stopOnReshard bool
	heartbeat     time.Duration
	alignSkew     bool
	skew          time.Duration
	alignTimeout  time.Duration
	copy          bool
	copyBatch     uint32
}

func resolveOptions(o *pgshardv1.VStreamOptions) options {
	out := options{twoPhase: o.GetTwoPhase(), stopOnReshard: o.GetStopOnReshard(), alignSkew: o.GetAlignSkew(),
		heartbeat: 5 * time.Second, skew: time.Second, alignTimeout: 10 * time.Second,
		copy: o.GetStartFrom() == pgshardv1.StartFrom_START_FROM_COPY, copyBatch: o.GetCopyBatchRows()}
	if o.GetHeartbeatIntervalMs() > 0 {
		out.heartbeat = time.Duration(o.GetHeartbeatIntervalMs()) * time.Millisecond
	}
	if o.GetAlignSkewMs() > 0 {
		out.skew = time.Duration(o.GetAlignSkewMs()) * time.Millisecond
	}
	if o.GetAlignTimeoutMs() > 0 {
		out.alignTimeout = time.Duration(o.GetAlignTimeoutMs()) * time.Millisecond
	}
	return out
}

// merger serializes the units of every shard onto one send function: each
// unit goes out whole, a VGtid follows every position-bearing unit with
// events, heartbeats carry the vector while idle, and acks are forwarded
// per shard.
type merger struct {
	shards     []router.Shard
	inputs     map[router.Shard]chan *unit
	ready      chan struct{}
	acks       <-chan *pgshardv1.VPosition
	acker      func(router.Shard, uint64)
	send       func(*pgshardv1.VEvent) error
	topo       Topology
	generation uint64
	opts       options
	now        func() time.Time

	position map[router.Shard]uint64
	// copying holds the copy state of every shard still in its copy phase.
	copying    map[router.Shard]*pgshardv1.VCopyState
	sentRel    map[string]string
	lastCommit map[router.Shard]int64
	heldSince  map[router.Shard]time.Time
	heads      map[router.Shard]*unit
	next       int
}

func (m *merger) vector() *pgshardv1.VPosition {
	pos := &pgshardv1.VPosition{ShardMapGeneration: m.generation}
	for _, sh := range m.shards {
		if lsn := m.position[sh]; lsn > 0 {
			pos.Shards = append(pos.Shards, &pgshardv1.VPosition_Shard{Shard: shardRef(sh), Lsn: lsn})
		}
		if st := m.copying[sh]; st != nil {
			pos.CopyState = append(pos.CopyState, st)
		}
	}
	return pos
}

// run drives the fan-in until ctx ends, a shard reports an error, or the
// shard map changes. It returns nil on a clean end (the terminal event was
// sent) and a send error otherwise.
func (m *merger) run(ctx context.Context) error {
	m.sentRel = map[string]string{}
	m.lastCommit = map[router.Shard]int64{}
	m.heldSince = map[router.Shard]time.Time{}
	m.heads = map[router.Shard]*unit{}
	if m.copying == nil {
		m.copying = map[router.Shard]*pgshardv1.VCopyState{}
	}
	if m.now == nil {
		m.now = time.Now
	}
	heartbeat := time.NewTimer(m.opts.heartbeat)
	defer heartbeat.Stop()
	for {
		if m.topo.Generation() != m.generation {
			return m.resharded()
		}
		m.drainAcks()
		m.fill()
		u, wait := m.choose()
		if u != nil {
			if u.err != nil {
				return m.send(&pgshardv1.VEvent{Event: &pgshardv1.VEvent_Error_{Error: u.err}})
			}
			if err := m.emit(u); err != nil {
				return err
			}
			if len(u.events) > 0 {
				heartbeat.Reset(m.opts.heartbeat)
			}
			continue
		}
		if err := m.wait(ctx, wait, heartbeat); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// wait blocks until a shard has data, a held shard's timeout passes, an
// ack arrives (forwarded) or the heartbeat is due (sent).
func (m *merger) wait(ctx context.Context, hold time.Duration, heartbeat *time.Timer) error {
	var holdC <-chan time.Time
	if hold > 0 {
		t := time.NewTimer(hold)
		defer t.Stop()
		holdC = t.C
	}
	select {
	case <-ctx.Done():
	case <-m.ready:
	case <-holdC:
	case pos := <-m.acks:
		m.forwardAck(pos)
	case <-heartbeat.C:
		if err := m.send(&pgshardv1.VEvent{Event: &pgshardv1.VEvent_Heartbeat_{Heartbeat: &pgshardv1.VEvent_Heartbeat{Position: m.vector()}}}); err != nil {
			return err
		}
		heartbeat.Reset(m.opts.heartbeat)
	}
	return nil
}

func (m *merger) resharded() error {
	gen := m.topo.Generation()
	if m.opts.stopOnReshard {
		j := &pgshardv1.VEvent_Journal{ShardMapGeneration: gen}
		for _, sh := range m.shards {
			j.Participants = append(j.Participants, shardRef(sh))
		}
		return m.send(&pgshardv1.VEvent{Event: &pgshardv1.VEvent_Journal_{Journal: j}})
	}
	return m.send(&pgshardv1.VEvent{Event: &pgshardv1.VEvent_Error_{Error: &pgshardv1.VEvent_Error{Code: pgshardv1.VEvent_Error_CODE_RESHARDED,
		Message: "shard map generation changed; restart the stream"}}})
}

func (m *merger) drainAcks() {
	for {
		select {
		case pos := <-m.acks:
			m.forwardAck(pos)
		default:
			return
		}
	}
}

// fill pulls the next unit of every shard without a head.
func (m *merger) fill() {
	for _, sh := range m.shards {
		if m.heads[sh] != nil {
			continue
		}
		select {
		case u := <-m.inputs[sh]:
			m.heads[sh] = u
		default:
		}
	}
}

// choose picks the next unit. Without skew alignment the shards are served
// round robin. With it, a transaction whose commit timestamp runs ahead of
// every other shard's last commit by more than the allowed skew is held
// back, for at most the alignment timeout; among eligible transactions the
// oldest goes first. The returned duration is how long to wait for a held
// shard when nothing is eligible.
func (m *merger) choose() (*unit, time.Duration) {
	if len(m.heads) == 0 {
		return nil, 0
	}
	if !m.opts.alignSkew {
		for i := range m.shards {
			sh := m.shards[(m.next+i)%len(m.shards)]
			if u := m.heads[sh]; u != nil {
				m.next = (m.next + i + 1) % len(m.shards)
				delete(m.heads, sh)
				return u, 0
			}
		}
		return nil, 0
	}
	now := m.now()
	var pick *unit
	var pickShard router.Shard
	var wait time.Duration
	for _, sh := range m.shards {
		u := m.heads[sh]
		if u == nil {
			continue
		}
		if u.err != nil || u.commitTS == 0 {
			delete(m.heads, sh)
			delete(m.heldSince, sh)
			return u, 0
		}
		floor, known := m.slowestOther(sh)
		eligible := !known || u.commitTS <= floor+m.opts.skew.Microseconds()
		if !eligible {
			since, held := m.heldSince[sh]
			if !held {
				since = now
				m.heldSince[sh] = now
			}
			if remaining := m.opts.alignTimeout - now.Sub(since); remaining > 0 {
				if wait == 0 || remaining < wait {
					wait = remaining
				}
				continue
			}
		}
		if pick == nil || u.commitTS < pick.commitTS {
			pick, pickShard = u, sh
		}
	}
	if pick == nil {
		return nil, wait
	}
	delete(m.heads, pickShard)
	delete(m.heldSince, pickShard)
	return pick, 0
}

func (m *merger) slowestOther(self router.Shard) (int64, bool) {
	var floor int64
	known := false
	for _, sh := range m.shards {
		if sh == self {
			continue
		}
		ts, ok := m.lastCommit[sh]
		if !ok {
			continue
		}
		if !known || ts < floor {
			floor, known = ts, true
		}
	}
	return floor, known
}

func (m *merger) emit(u *unit) error {
	for i, ev := range u.events {
		for _, rel := range u.rels[i] {
			if rel == nil {
				continue
			}
			key := rel.schema + "." + rel.table
			if m.sentRel[key] != rel.signature {
				if err := m.send(rel.event()); err != nil {
					return err
				}
				m.sentRel[key] = rel.signature
			}
		}
		if err := m.send(ev); err != nil {
			return err
		}
	}
	if u.commitTS != 0 {
		m.lastCommit[u.shard] = u.commitTS
	}
	if u.position && u.endLSN > m.position[u.shard] {
		m.position[u.shard] = u.endLSN
	}
	if u.copy != nil {
		m.copying[u.shard] = u.copy
	}
	if u.copyDone {
		delete(m.copying, u.shard)
	}
	if !u.position && u.copy == nil && !u.copyDone || len(u.events) == 0 {
		return nil
	}
	if err := m.send(&pgshardv1.VEvent{Event: &pgshardv1.VEvent_Vgtid{Vgtid: &pgshardv1.VEvent_VGtid{Position: m.vector()}}}); err != nil {
		return err
	}
	if u.copyDone && len(m.copying) == 0 {
		return m.send(&pgshardv1.VEvent{Event: &pgshardv1.VEvent_CopyCompleted_{CopyCompleted: &pgshardv1.VEvent_CopyCompleted{}}})
	}
	return nil
}

// forwardAck clamps every shard's acked LSN to what was delivered and hands
// it to the shard's acker.
func (m *merger) forwardAck(pos *pgshardv1.VPosition) {
	for _, p := range pos.GetShards() {
		sh := router.Shard{Set: p.GetShard().GetShardSet(), ID: int32(p.GetShard().GetShardId())}
		if _, ok := m.inputs[sh]; !ok || m.copying[sh] != nil {
			continue
		}
		lsn := p.GetLsn()
		if d := m.position[sh]; lsn > d {
			lsn = d
		}
		if lsn > 0 {
			m.acker(sh, lsn)
		}
	}
}

// positionFrom turns a VPosition into per-shard LSNs.
func positionFrom(pos *pgshardv1.VPosition) map[router.Shard]uint64 {
	out := map[router.Shard]uint64{}
	for _, p := range pos.GetShards() {
		out[router.Shard{Set: p.GetShard().GetShardSet(), ID: int32(p.GetShard().GetShardId())}] = p.GetLsn()
	}
	return out
}

// copyStateFrom turns a VPosition's copy states into per-shard entries.
func copyStateFrom(pos *pgshardv1.VPosition) map[router.Shard]*pgshardv1.VCopyState {
	out := map[router.Shard]*pgshardv1.VCopyState{}
	for _, st := range pos.GetCopyState() {
		out[router.Shard{Set: st.GetShard().GetShardSet(), ID: int32(st.GetShard().GetShardId())}] = st
	}
	return out
}

func sortedShards(in []router.Shard) []router.Shard {
	out := append([]router.Shard(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Set != out[j].Set {
			return out[i].Set < out[j].Set
		}
		return out[i].ID < out[j].ID
	})
	return out
}
