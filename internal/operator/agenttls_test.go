package operator

import (
	"testing"

	"github.com/andrew01234567890/pgshard/internal/agent"
	"google.golang.org/grpc/credentials"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// TestAgentGRPCTLSOnlyWhenAskedFor: mounting the material and requiring it are
// different acts. agentMounts puts the certificates on every member as soon as
// the cluster has any, so that turning the requirement on later is a restart
// rather than a provisioning step -- and a member whose agent required TLS
// before its callers could present any would be unreachable.
var agentTLSZero = agent.TLSFiles{}

func TestAgentGRPCTLSOnlyWhenAskedFor(t *testing.T) {
	c := &pgshardv1alpha1.PgShardCluster{}
	if got := agentGRPCTLS(c); got != (agentTLSZero) {
		t.Errorf("a cluster with no internalTLS gave its agent %+v", got)
	}

	c.Spec.InternalTLS.SecretRef = &corev1.LocalObjectReference{Name: "internal-tls"}
	if got := agentGRPCTLS(c); got != agentTLSZero {
		t.Errorf("material alone must not make the agent require it; got %+v", got)
	}

	c.Spec.InternalTLS.AgentMTLS = true
	got := agentGRPCTLS(c)
	if got.CertFile == "" || got.KeyFile == "" || got.CAFile == "" {
		t.Fatalf("agentMTLS did not give the agent all three files: %+v", got)
	}
	if got.CertFile != internalTLSMountPath+"/tls.crt" {
		t.Errorf("cert path %q is not where agentMounts mounts the secret", got.CertFile)
	}

	// agentMTLS without material cannot happen (CEL refuses it), but the
	// renderer must not produce half a configuration if it ever did.
	c.Spec.InternalTLS.SecretRef = nil
	if got := agentGRPCTLS(c); got != agentTLSZero {
		t.Errorf("agentMTLS without a secretRef produced %+v", got)
	}
}

// TestTheFleetIsMixedDuringARollout: members restart one at a time, so for the
// length of the roll some agents require mTLS and some still serve plaintext.
// A client with one answer for the whole fleet cannot reach half of it, in
// whichever direction it is wrong.
func TestTheFleetIsMixedDuringARollout(t *testing.T) {
	restarted := map[string]bool{"a:9101": true}
	c := &GRPCAgentClient{
		creds:       stubCreds{},
		RequiresTLS: func(addr string) bool { return restarted[addr] },
	}
	if !c.wantTLS("a:9101") {
		t.Error("a member that has restarted into agentMTLS must be dialled with TLS")
	}
	if c.wantTLS("b:9101") {
		t.Error("a member that has not restarted yet still serves plaintext")
	}

	// The same member, after its restart. The client must not answer from a
	// connection dialled the other way.
	restarted["b:9101"] = true
	if !c.wantTLS("b:9101") {
		t.Error("the predicate is consulted per dial, not cached at construction")
	}

	// No credentials means plaintext whatever the predicate says: there is
	// nothing to present, so believing it would fail every call.
	plain := &GRPCAgentClient{RequiresTLS: func(string) bool { return true }}
	if plain.wantTLS("a:9101") {
		t.Error("a client with no credentials cannot dial TLS")
	}

	// Credentials and no predicate is the finished state: every agent has
	// restarted, so every dial is TLS.
	done := &GRPCAgentClient{creds: stubCreds{}}
	if !done.wantTLS("anything:9101") {
		t.Error("a client with credentials and no predicate must dial TLS")
	}
}

// stubCreds stands in for real transport credentials: wantTLS only asks
// whether any exist.
type stubCreds struct {
	credentials.TransportCredentials
}

// TestAModeIsRecordedFromThePodNotTheSpec: the operator has to dial a member by
// what that member is RUNNING, and during a rollout that is not what the spec
// asks for. Reading it from the pod annotation is what makes the two
// distinguishable; deriving it from the spec would tell every member the same
// thing and be wrong about the ones that have not restarted.
func TestAModeIsRecordedFromThePodNotTheSpec(t *testing.T) {
	var modes AgentTLSModes
	if modes.Requires("10.0.0.1:9101") {
		t.Error("an address nothing has observed must be plaintext")
	}
	modes.Set("10.0.0.1:9101", true)
	modes.Set("10.0.0.2:9101", false)
	if !modes.Requires("10.0.0.1:9101") {
		t.Error("a member observed running agentMTLS must be dialled with TLS")
	}
	if modes.Requires("10.0.0.2:9101") {
		t.Error("a member that has not restarted yet still serves plaintext")
	}
	// The member restarts into the new mode; the next pass records it.
	modes.Set("10.0.0.2:9101", true)
	if !modes.Requires("10.0.0.2:9101") {
		t.Error("the mode must follow the pod across a restart")
	}
	// A nil set is the ordinary case for a cluster that never turned this on.
	var none *AgentTLSModes
	none.Set("10.0.0.1:9101", true)
	if none.Requires("10.0.0.1:9101") {
		t.Error("a nil set must answer plaintext rather than panic")
	}
}
