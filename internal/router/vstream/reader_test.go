package vstream

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// TestOldPoolerTextIsReadCarefully: a pooler too old to send the structured
// reason carries only its message, and "start replication" failing is not
// the same as the position being gone. Reading it as the latter costs the
// consumer a full re-snapshot for a missing publication or a rejected
// option.
func TestOldPoolerTextIsReadCarefully(t *testing.T) {
	sh := router.Shard{Set: "default", ID: 1}
	tooOld := []string{
		`start replication: can no longer get changes from replication slot "s" (55000)`,
		`start replication: replication slot "s" does not exist (42704)`,
		`start replication: requested WAL segment has already been removed (58P01)`,
	}
	for _, msg := range tooOld {
		ev := fatal(status.Error(codes.FailedPrecondition, msg), sh)
		if ev == nil || ev.Code != pgshardv1.VEvent_Error_CODE_POSITION_TOO_OLD {
			t.Errorf("%q: want POSITION_TOO_OLD, got %+v", msg, ev)
		}
	}
	internal := []string{
		`start replication: publication "pgshard_all" does not exist for this database (42P01)`,
		`start replication: permission denied for replication (42501)`,
		`start replication: option "proto_version" is not supported (22023)`,
	}
	for _, msg := range internal {
		ev := fatal(status.Error(codes.FailedPrecondition, msg), sh)
		if ev == nil || ev.Code != pgshardv1.VEvent_Error_CODE_INTERNAL {
			t.Errorf("%q: want INTERNAL, got %+v", msg, ev)
		}
	}
}
