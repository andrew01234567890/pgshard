package controller

import (
	"testing"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type stubCreds struct {
	credentials.TransportCredentials
}

// The controller is given its own certificate -- which is what an agent's
// caller rule wants ({controller, operator}) -- and it was never used:
// AgentMaterializer was constructed without Creds, so every agent was
// dialled plaintext and a member that had restarted into the mTLS
// requirement refused the handshake, leaving its schema unmaterialised.
//
// The choice stays per member: the row this member published decides, not
// the controller's configuration, because the fleet is mixed for the length
// of a roll.
func TestAnAgentIsDialledTheWayItsOwnRowSays(t *testing.T) {
	with := &AgentMaterializer{Creds: stubCreds{}}
	if got := with.dialCreds(true); got != credentials.TransportCredentials(stubCreds{}) {
		t.Error("a member that requires mTLS was dialled without the controller's certificate")
	}
	if _, plain := with.dialCreds(false).(interface {
		Info() credentials.ProtocolInfo
	}); !plain {
		t.Error("dialCreds returned nothing usable for a plaintext member")
	}
	if with.dialCreds(false) == credentials.TransportCredentials(stubCreds{}) {
		t.Error("a member that has not restarted yet was dialled with TLS; it refuses the handshake")
	}

	// No material to present: plaintext whatever the row says. Believing it
	// would fail every call and say nothing about why.
	none := &AgentMaterializer{}
	if none.dialCreds(true) == credentials.TransportCredentials(stubCreds{}) {
		t.Error("a controller with no certificate claimed to dial TLS")
	}
	if none.dialCreds(true) == nil {
		t.Error("dialCreds must always return usable credentials")
	}
	_ = insecure.NewCredentials()
}
