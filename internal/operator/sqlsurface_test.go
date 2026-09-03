package operator

import (
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/pgparser"
)

// TestTheSQLSurfaceSaysWhichMajorItIs: an upgrade that moved every group
// to a newer major reports success, and it succeeded -- the data is there
// and the servers are new. What it did not do is give clients that major's
// SQL, because the routers parse with one grammar chosen when they were
// built. Nothing said so, which made a documented limitation look like a
// bug in whichever statement hit it first.
func TestTheSQLSurfaceSaysWhichMajorItIs(t *testing.T) {
	ahead := pgparser.Major + 1
	msg := sqlSurfaceMessage(ahead)
	for _, want := range []string{"the shards run", "routers parse", "only syntax is refused", "server_version"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("the message does not mention %q: %q", want, msg)
		}
	}
	// It has to name both numbers: one of them alone says nothing.
	if !strings.Contains(msg, strconv.Itoa(ahead)) || !strings.Contains(msg, strconv.Itoa(pgparser.Major)) {
		t.Fatalf("the message names only one major: %q", msg)
	}
}

func TestAMatchingMajorSaysSoPlainly(t *testing.T) {
	msg := sqlSurfaceMessage(pgparser.Major)
	if strings.Contains(msg, "refused") {
		t.Fatalf("a matching major must not read as a limitation: %q", msg)
	}
	// An unknown serving major -- a catalog that predates the field -- is
	// not a mismatch either, and must not raise the condition.
	if sqlSurfaceMessage(0) != msg {
		t.Fatalf("an unknown serving major reads differently from a matching one: %q", sqlSurfaceMessage(0))
	}
}

// TestAnOlderServerIsNotTheSameProblem: routers ahead of the shards refuse
// syntax the shards would refuse anyway, which is safe and worth saying
// differently from the case where clients lose access to what they have.
func TestAnOlderServerIsNotTheSameProblem(t *testing.T) {
	behind := sqlSurfaceMessage(pgparser.Major - 1)
	ahead := sqlSurfaceMessage(pgparser.Major + 1)
	if behind == ahead {
		t.Fatal("an older shard major reads the same as a newer one")
	}
	if !strings.Contains(behind, "refused here first") {
		t.Fatalf("the older-shard case must say the refusal is the shards': %q", behind)
	}
}

// TestTheConditionAppearsWhenTheShardsMoveAhead drives it through a real
// reconcile, because a condition nothing sets is a constant.
func TestTheConditionAppearsWhenTheShardsMoveAhead(t *testing.T) {
	r, fp, c := setup(t, "surface")
	bringUp(t, r, fp, c)
	if cond := condition(t, c.Name, pgshardv1alpha1.ConditionSQLSurfaceBehindServers); cond.Status != metav1.ConditionFalse {
		t.Fatalf("a cluster on the router's own major must not report a gap: %+v", cond)
	}

	// The shards move to the next major, as a completed upgrade leaves them.
	fp.mu.Lock()
	for i := range fp.shardSets {
		fp.shardSets[i].PGMajor = pgparser.Major + 1
	}
	fp.mu.Unlock()
	reconcile(t, r, c)

	cond := condition(t, c.Name, pgshardv1alpha1.ConditionSQLSurfaceBehindServers)
	if cond.Status != metav1.ConditionTrue {
		t.Fatalf("the shards are ahead of the grammar and nothing says so: %+v", cond)
	}
	if !strings.Contains(cond.Message, "only syntax is refused") {
		t.Fatalf("the condition does not say what a client loses: %q", cond.Message)
	}
}
