package operator

import (
	"errors"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestChooseCandidatePrefersHighestFlushedReachableStandby(t *testing.T) {
	members := []memberView{
		{Name: "g-0", Listed: false, Reachable: true, InRecovery: false, FlushLSN: 900}, // old primary
		{Name: "g-1", Listed: true, Reachable: true, InRecovery: true, FlushLSN: 100},
		{Name: "g-2", Listed: true, Reachable: true, InRecovery: true, FlushLSN: 200},
		// Listed but not Ready (lagging replay): still eligible, it may hold the only ack.
		{Name: "g-3", Listed: true, Reachable: true, InRecovery: true, FlushLSN: 300},
	}
	got, err := chooseCandidate(members, "g-0", "", 1)
	if err != nil || got != "g-3" {
		t.Fatalf("got %q err %v; want g-3 (highest flushed LSN among reachable standbys)", got, err)
	}
	if got, _ := chooseCandidate(members, "g-0", "g-1", 1); got != "g-3" {
		t.Fatalf("preferred member with a lower LSN must not win, got %q", got)
	}
	members[1].FlushLSN = 300
	if got, _ := chooseCandidate(members, "g-0", "g-1", 1); got != "g-1" {
		t.Fatalf("preferred member at the maximum LSN must win, got %q", got)
	}
	if got, _ := chooseCandidate(members, "g-0", "", 1); got != "g-1" {
		t.Fatalf("ties break by name, got %q", got)
	}
}

func TestChooseCandidateRefusesWhenUnreachableListedStandbysMayHoldAcks(t *testing.T) {
	// ANY 1 over two listed standbys: one unreachable ⇒ 1 reachable + 1 <= 2 ⇒ refuse.
	members := []memberView{
		{Name: "g-0", Reachable: true, InRecovery: false, FlushLSN: 999},
		{Name: "g-1", Listed: true, Reachable: true, InRecovery: true, FlushLSN: 999},
		{Name: "g-2", Listed: true, Reachable: false, InRecovery: true, FlushLSN: 999},
	}
	if got, err := chooseCandidate(members, "g-0", "", 1); !errors.Is(err, errQuorum) || got != "" {
		t.Fatalf("got %q err %v; want errQuorum", got, err)
	}
	// ANY 2 over the same list: the reachable one must hold every ack ⇒ eligible.
	if got, err := chooseCandidate(members, "g-0", "", 2); err != nil || got != "g-1" {
		t.Fatalf("got %q err %v; want g-1 under ANY 2", got, err)
	}
	// Nothing reachable in recovery ⇒ no candidate.
	none := []memberView{
		{Name: "g-0", Reachable: true, InRecovery: true, FlushLSN: 999},
		{Name: "g-1", Listed: true, Reachable: false, InRecovery: true, FlushLSN: 999},
		{Name: "g-2", Listed: true, Reachable: true, InRecovery: false, FlushLSN: 999},
	}
	if got, err := chooseCandidate(none, "g-0", "", 1); !errors.Is(err, errNoCandidate) || got != "" {
		t.Fatalf("got %q err %v; want errNoCandidate", got, err)
	}
	if _, err := chooseCandidate(nil, "g-0", "", 1); !errors.Is(err, errNoCandidate) {
		t.Fatalf("empty member list: %v", err)
	}
}

func TestEpochsAreStrictlyMonotonic(t *testing.T) {
	cases := []struct {
		group     int64
		agent     uint64
		wantNext  int64
		wantPromo int64
	}{
		{0, 0, 1, 1},
		{3, 0, 4, 3},
		{3, 3, 4, 4},
		{3, 7, 8, 8},
	}
	for _, tc := range cases {
		if got := nextEpoch(tc.group, tc.agent); got != tc.wantNext {
			t.Errorf("nextEpoch(%d,%d)=%d want %d", tc.group, tc.agent, got, tc.wantNext)
		}
		if got := promotionEpoch(tc.group, tc.agent); got != tc.wantPromo {
			t.Errorf("promotionEpoch(%d,%d)=%d want %d", tc.group, tc.agent, got, tc.wantPromo)
		}
		if got := nextEpoch(tc.group, tc.agent); got <= tc.group || got <= int64(tc.agent) {
			t.Errorf("nextEpoch(%d,%d)=%d is not above both inputs", tc.group, tc.agent, got)
		}
	}
}

func TestLeaseFenceable(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lease := func(holder string, renewedAgo time.Duration) *coordinationv1.Lease {
		l := &coordinationv1.Lease{}
		l.Spec.HolderIdentity = ptr.To(holder)
		l.Spec.LeaseDurationSeconds = ptr.To(int32(15))
		if renewedAgo >= 0 {
			rt := metav1.NewMicroTime(now.Add(-renewedAgo))
			l.Spec.RenewTime = &rt
		}
		return l
	}
	cases := []struct {
		name  string
		lease *coordinationv1.Lease
		old   string
		want  bool
	}{
		{"empty holder", lease("", time.Second), "g-0", true},
		{"operator fence", lease(FenceHolder, time.Second), "g-0", true},
		{"old primary still renewing", lease("g-0", time.Second), "g-0", true},
		{"other live holder", lease("g-9", time.Second), "g-0", false},
		{"other expired holder", lease("g-9", 20*time.Second), "g-0", true},
		{"other holder never renewed", lease("g-9", -1), "g-0", true},
		{"handover target already holds", lease("g-2", time.Second), "", true},
	}
	for _, tc := range cases {
		allowed := []string{tc.old}
		if tc.name == "handover target already holds" {
			allowed = []string{"", "g-2"}
		}
		if got := leaseFenceable(tc.lease, now, allowed...); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestPrimaryHealthy(t *testing.T) {
	pod := &corev1.Pod{}
	live := AgentStatus{Running: true, Primary: true}
	if primaryHealthy(nil, true, live, nil) {
		t.Error("missing pod cannot be healthy")
	}
	if !primaryHealthy(pod, true, AgentStatus{}, errors.New("rpc")) {
		t.Error("a Ready pod is healthy even when Status fails")
	}
	if !primaryHealthy(pod, false, live, nil) {
		t.Error("a running primary agent keeps the pod healthy while readiness lags")
	}
	if primaryHealthy(pod, false, AgentStatus{Running: true, Primary: false}, nil) {
		t.Error("a designated primary answering as a standby is not healthy")
	}
	if primaryHealthy(pod, false, AgentStatus{}, errors.New("rpc")) {
		t.Error("not Ready and Status failing is unhealthy")
	}
}
