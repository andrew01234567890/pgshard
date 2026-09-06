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
	twoPhase       bool
	stopOnReshard  bool
	heartbeat      time.Duration
	wallClockAlign bool
	skew           time.Duration
	alignTimeout   time.Duration
	copy           bool
	copyBatch      uint32
}

func resolveOptions(o *pgshardv1.VStreamOptions) options {
	out := options{twoPhase: o.GetTwoPhase(), stopOnReshard: o.GetStopOnReshard(), wallClockAlign: o.GetBestEffortWallClockAlignment(),
		heartbeat: 5 * time.Second, skew: time.Second, alignTimeout: 10 * time.Second,
		copy: o.GetStartFrom() == pgshardv1.StartFrom_START_FROM_COPY, copyBatch: o.GetCopyBatchRows()}
	if o.GetHeartbeatIntervalMs() > 0 {
		out.heartbeat = time.Duration(o.GetHeartbeatIntervalMs()) * time.Millisecond
	}
	if o.GetWallClockLeadMs() > 0 {
		out.skew = time.Duration(o.GetWallClockLeadMs()) * time.Millisecond
	}
	if o.GetWallClockHoldMs() > 0 {
		out.alignTimeout = time.Duration(o.GetWallClockHoldMs()) * time.Millisecond
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
	// emitted mirrors position for the unary Ack path, which runs on
	// another goroutine and must not read the map above.
	emitted *emitted
	// copying holds the copy state of every shard still in its copy phase.
	copying    map[router.Shard]*pgshardv1.VCopyState
	sentRel    map[string]string
	lastCommit map[router.Shard]int64
	heldSince  map[router.Shard]time.Time
	// pos is the position message, rebuilt in place rather than allocated
	// again for every transaction; posShard holds its per-shard entries so
	// their shard references are allocated once each.
	pos      *pgshardv1.VPosition
	posShard map[router.Shard]*pgshardv1.VPosition_Shard
	heads    map[router.Shard]*unit
	next     int
}

// vector is where the stream has reached. It is sent after every
// position-bearing transaction, and used to allocate a fresh message plus
// one entry per shard each time -- so a stream of small transactions on a
// wide cluster spent more on saying where it was than on what had changed.
//
// The message is built once and its numbers updated in place. That is
// sound because the only consumer is send, which marshals before it
// returns: the bytes are on the wire before anything can move the position
// again. A consumer that kept the message rather than its contents would
// see it change underneath, which is why this returns to the caller that
// sends it immediately and nothing else calls it.
func (m *merger) vector() *pgshardv1.VPosition {
	if m.pos == nil {
		m.pos = &pgshardv1.VPosition{}
		m.posShard = map[router.Shard]*pgshardv1.VPosition_Shard{}
	}
	m.pos.ShardMapGeneration = m.generation
	m.pos.Shards = m.pos.Shards[:0]
	m.pos.CopyState = m.pos.CopyState[:0]
	for _, sh := range m.shards {
		if lsn := m.position[sh]; lsn > 0 {
			e := m.posShard[sh]
			if e == nil {
				e = &pgshardv1.VPosition_Shard{Shard: shardRef(sh)}
				m.posShard[sh] = e
			}
			e.Lsn = lsn
			m.pos.Shards = append(m.pos.Shards, e)
		}
		if st := m.copying[sh]; st != nil {
			m.pos.CopyState = append(m.pos.CopyState, st)
		}
	}
	return m.pos
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

// choose picks the next unit. Without alignment the shards are served
// round robin. With it, a transaction whose commit timestamp runs ahead of
// every other shard's last commit by more than the allowed lead is held
// back, for at most the hold timeout; among eligible transactions the
// oldest goes first. The returned duration is how long to wait for a held
// shard when nothing is eligible.
//
// The timestamps come from each shard host's own clock and a hold expires
// whether or not the slow shard caught up, so this orders what a consumer
// sees and nothing more. It is not a happens-before relation and must not
// be presented as one.
func (m *merger) choose() (*unit, time.Duration) {
	if len(m.heads) == 0 {
		return nil, 0
	}
	if !m.opts.wallClockAlign {
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
	floors := m.commitFloors()
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
		floor, known := floors.without(sh)
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

// commitFloors summarises the last commit timestamps once per pass. The
// question asked of it -- the slowest shard other than this one -- used to
// be answered by walking every shard again for each candidate, so aligning
// skew cost the square of the shard count on every unit emitted. The two
// smallest timestamps are all that can answer it: excluding one shard
// removes at most the smallest, and then the second smallest stands in.
type commitFloors struct {
	min, next int64
	at        router.Shard
	seen      int
}

func (m *merger) commitFloors() commitFloors {
	var f commitFloors
	for _, sh := range m.shards {
		ts, ok := m.lastCommit[sh]
		if !ok {
			continue
		}
		switch {
		case f.seen == 0 || ts < f.min:
			f.next = f.min
			f.min, f.at = ts, sh
		case f.seen == 1 || ts < f.next:
			f.next = ts
		}
		f.seen++
	}
	return f
}

// without is the slowest commit timestamp among the shards other than
// self, and whether there is one.
func (f commitFloors) without(self router.Shard) (int64, bool) {
	switch {
	case f.seen == 0:
		return 0, false
	case f.at != self:
		return f.min, true
	case f.seen == 1:
		return 0, false
	default:
		return f.next, true
	}
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
		if m.emitted != nil {
			m.emitted.advance(u.shard, u.endLSN)
		}
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
