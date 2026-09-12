package plan

import (
	"strconv"
	"strings"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// ShardGUC is the session setting that targets one shard directly.
const ShardGUC = "pgshard.shard"

// codeInvalidParamVal is invalid_parameter_value, what PostgreSQL answers
// for a SET whose value the setting cannot take.
const codeInvalidParamVal = "22023"

// ParseShardPin reads a pgshard.shard value: a shard id, or empty for RESET.
func ParseShardPin(v string) (*int32, error) {
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}
	id, err := strconv.ParseInt(strings.TrimSpace(v), 10, 32)
	if err != nil || id < 0 {
		e := pgwire.Errorf(codeInvalidParamVal, "%s must be a shard id, not %q", ShardGUC, v)
		e.Hint = "set it to a shard id from pgshard.shard_ranges, or RESET it to route normally"
		return nil, e
	}
	i := int32(id)
	return &i, nil
}

// pinned routes a statement to the shard the session targeted, or refuses
// it because targeting one shard cannot mean anything for that statement.
//
// The rules are drawn where Neki draws them, and for the same reasons:
// reads and writes go to the shard, session and transaction control keep
// taking the ordinary path, and a schema change is refused outright because
// a schema change runs across the cluster rather than on one shard. A
// router-managed function is refused as well: the router cannot both
// forward the table access to a shard and evaluate the function itself.
func (p *Plan) pinned(shard int32, ids []int32) error {
	// Before the Kind switch: the router answers a global nextval itself,
	// so it reads as SessionLocal and would otherwise pass straight
	// through as session control.
	if p.NextVal != "" {
		return notYet("nextval() over a global sequence cannot run while "+ShardGUC+" targets one shard",
			"the router answers the sequence itself and cannot also forward the statement to a shard; RESET "+ShardGUC)
	}
	switch p.Kind {
	case SessionLocal, Refuse:
		// SET, SHOW, BEGIN, PREPARE and friends: unchanged. A pinned
		// session still has to be able to RESET the pin.
		return nil
	case MigrationKind:
		return notYet("a schema change cannot run while "+ShardGUC+" targets one shard: DDL runs across the cluster",
			"RESET "+ShardGUC+", or use another session without it, to run the change cluster-wide")
	}
	// A reference write fans out to every shard by definition, and ADR 16
	// makes that atomicity a property we do not trade away: the copies of a
	// reference table cannot be allowed to diverge, because a join answered
	// locally on any shard would then return a different answer depending
	// on where it ran, and nothing would report it. Pinning one would write
	// exactly one copy.
	if p.Kind == Reference && p.Class.Write {
		return notYet("a write to a reference table cannot run while "+ShardGUC+" targets one shard: it belongs on every shard, in one transaction",
			"RESET "+ShardGUC+" to write it everywhere, or read from the pinned shard instead")
	}
	if !containsShard(ids, shard) {
		e := pgwire.Errorf(codeInvalidParamVal, "%s names shard %d, which is not serving in this shard set", ShardGUC, shard)
		e.Hint = "read the serving shards from pgshard.shard_ranges"
		return e
	}
	// Everything the shard can answer on its own now goes to it, whatever
	// the shard map would have said: that is the point of the setting.
	p.Kind, p.Shards, p.Deferred = Pinned, []int32{shard}, false
	p.merge, p.mergeErr = nil, nil
	return nil
}

func containsShard(ids []int32, id int32) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
