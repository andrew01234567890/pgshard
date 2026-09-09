package grpccreds

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/pki"
)

// ctxAs is a call arriving from id over a verified chain. The certificate
// is unsigned on purpose: IdentityOfCert reads the URI SAN, and tls has
// already decided the chain is genuine by the time an interceptor runs --
// this is the authority half, not the authenticity half.
func ctxAs(id pki.Identity) context.Context {
	cert := &x509.Certificate{URIs: []*url.URL{id.URI()}}
	state := tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
}

func TestTheInterceptorRefusesAMethodTheCallersRoleMayNotCall(t *testing.T) {
	unary, stream := MethodInterceptors(pki.RoleController, true)
	if unary == nil || stream == nil {
		t.Fatal("authorizing listener got no interceptors")
	}
	ctx := ctxAs(pki.Identity{Namespace: "ns", Cluster: "demo", Role: pki.RoleRouter})
	called := false
	handler := func(context.Context, any) (any, error) { called = true; return "ok", nil }

	if _, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/pgshard.v1.Controller/CreateStream"}, handler); err != nil {
		t.Fatalf("a router must reach CreateStream: %v", err)
	}
	if !called {
		t.Fatal("the handler did not run for an allowed method")
	}

	called = false
	_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/pgshard.v1.Controller/CancelWorkflow"}, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a router calling CancelWorkflow: %v", err)
	}
	if called {
		t.Fatal("the handler ran for a refused method")
	}
}

// A connection with no pgshard identity is refused rather than assumed to
// have been checked elsewhere. The credentials refuse an unidentified peer
// when authorization is on, so reaching the interceptor without one means
// the listener is not the one that checked.
func TestTheInterceptorRefusesACallWithNoIdentity(t *testing.T) {
	unary, _ := MethodInterceptors(pki.RoleController, true)
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	_, err := unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/pgshard.v1.Controller/CreateStream"}, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a call with no identity: %v", err)
	}
}

// A deployment that has not turned identity authorization on is unchanged
// rather than half enforced: no interceptors at all.
func TestNoInterceptorsWithoutIdentityAuthorization(t *testing.T) {
	if u, s := MethodInterceptors(pki.RoleController, false); u != nil || s != nil {
		t.Error("interceptors installed on a listener that does not authorize by identity")
	}
	if u, s := MethodInterceptors("", true); u != nil || s != nil {
		t.Error("interceptors installed for a listener with no role")
	}
}
