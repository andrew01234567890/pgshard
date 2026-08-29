package operator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func policyPorts(r networkingv1.NetworkPolicyIngressRule) []int32 {
	var out []int32
	for _, p := range r.Ports {
		if p.Protocol == nil || *p.Protocol != corev1.ProtocolTCP {
			continue
		}
		out = append(out, int32(p.Port.IntValue()))
	}
	return out
}

func TestMemberNetworkPolicyAdmitsTheClusterAndItsClients(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "bank", Namespace: "prod"},
	}
	client := networkingv1.NetworkPolicyPeer{
		PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "pgshard-controller"}},
	}
	c.Spec.NetworkPolicy.Clients = []networkingv1.NetworkPolicyPeer{client}

	np := Renderer{}.MemberNetworkPolicy(c)

	if np.Name != "bank-members" || np.Namespace != "prod" || np.Labels[LabelCluster] != "bank" {
		t.Fatalf("meta: %+v", np.ObjectMeta)
	}
	sel, err := metav1.LabelSelectorAsSelector(&np.Spec.PodSelector)
	if err != nil {
		t.Fatal(err)
	}
	member := Group{Cluster: "bank", Kind: "shard", ShardID: 0}.Labels()
	if !sel.Matches(labels.Set(member)) {
		t.Errorf("the policy must select member pods: %v does not match %v", np.Spec.PodSelector, member)
	}
	for name, other := range map[string]map[string]string{
		"router": {LabelCluster: "bank", LabelComponent: routerComponent},
		"admin":  {LabelCluster: "bank", LabelComponent: adminComponent},
	} {
		// A router serves clients on 5432 and probes itself on it; the
		// admin UI listens on a port no rule here opens. Selecting either
		// takes it off the network.
		if sel.Matches(labels.Set(other)) {
			t.Errorf("the policy must not select %s pods: %v", name, np.Spec.PodSelector)
		}
	}
	if len(np.Spec.PolicyTypes) != 1 || np.Spec.PolicyTypes[0] != networkingv1.PolicyTypeIngress {
		t.Errorf("egress must stay unrestricted: %v", np.Spec.PolicyTypes)
	}
	if len(np.Spec.Ingress) != 2 {
		t.Fatalf("want a restricted rule and an open probe rule, got %d", len(np.Spec.Ingress))
	}

	restricted := np.Spec.Ingress[0]
	if got, want := policyPorts(restricted), []int32{postgresPort, agentGRPCPort, poolerGRPCPort}; !equalPorts(got, want) {
		t.Errorf("restricted ports = %v, want %v", got, want)
	}
	if len(restricted.From) != 2 {
		t.Fatalf("restricted peers = %+v", restricted.From)
	}
	if restricted.From[0].PodSelector.MatchLabels[LabelCluster] != "bank" {
		t.Errorf("the cluster's own pods must be admitted: %+v", restricted.From[0])
	}
	if restricted.From[1].PodSelector.MatchLabels["app"] != "pgshard-controller" {
		t.Errorf("a declared client must be admitted: %+v", restricted.From[1])
	}

	open := np.Spec.Ingress[1]
	if len(open.From) != 0 {
		t.Errorf("probes come from the kubelet, which no selector matches: %+v", open.From)
	}
	if got, want := policyPorts(open), []int32{agentHTTPPort, poolerMetricsPort}; !equalPorts(got, want) {
		t.Errorf("open ports = %v, want %v", got, want)
	}
}

func TestMemberNetworkPolicyWithoutClientsStillAdmitsTheCluster(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "solo", Namespace: "default"}}
	np := Renderer{}.MemberNetworkPolicy(c)
	if n := len(np.Spec.Ingress[0].From); n != 1 {
		t.Fatalf("peers = %d, want the cluster itself", n)
	}
}

func equalPorts(got, want []int32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestMemberProbePortsAreTheOpenOnes: a probe the policy blocks takes the
// member NotReady, which is worse than the exposure the policy removes.
func TestMemberProbePortsAreTheOpenOnes(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: "default"}}
	open := map[int32]bool{}
	for _, p := range policyPorts(Renderer{}.MemberNetworkPolicy(c).Spec.Ingress[1]) {
		open[p] = true
	}
	g := Group{Cluster: "probe", Kind: "shard", Replicas: 1}
	pod := Renderer{}.Pod(c, g, 0, RolePrimary, g.MemberName(0), Template(c, g, nil, nil))
	for _, ctr := range pod.Spec.Containers {
		for name, probe := range map[string]*corev1.Probe{"readiness": ctr.ReadinessProbe, "liveness": ctr.LivenessProbe, "startup": ctr.StartupProbe} {
			port, ok := probePort(probe)
			if !ok {
				continue
			}
			if !open[port] {
				t.Errorf("%s %s probe uses port %d, which the policy restricts", ctr.Name, name, port)
			}
		}
	}
}

func probePort(p *corev1.Probe) (int32, bool) {
	switch {
	case p == nil:
		return 0, false
	case p.HTTPGet != nil:
		return int32(p.HTTPGet.Port.IntValue()), true
	case p.TCPSocket != nil:
		return int32(p.TCPSocket.Port.IntValue()), true
	}
	return 0, false
}

// TestPodShapeIsPartOfTheTemplateHash: pods are immutable, so a rendered
// change the template does not describe -- here the pooler's probe moving to
// another port -- reaches existing clusters only if the hash moves with it.
func TestPodShapeIsPartOfTheTemplateHash(t *testing.T) {
	tpl := MemberTemplate{Image: "img"}
	got := tpl.Hash()
	tpl.Shape = "something else"
	if tpl.Hash() != got {
		t.Fatal("Hash must set the shape itself, not take one from the caller")
	}
	var bare MemberTemplate
	bare.Image = "img"
	raw, err := json.Marshal(bare)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got == hex.EncodeToString(sum[:8]) {
		t.Error("the hash does not depend on the pod shape, so a pod rendered differently never rolls")
	}
}
