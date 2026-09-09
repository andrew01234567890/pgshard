package grpccreds

import (
	"context"
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/pki"
)

// MethodInterceptors narrow an admitted caller to the methods its role may
// call on a listener serving role.
//
// The transport credentials decide WHO may connect; this decides what
// connecting buys them, and the two are different questions. A listener
// with one CA behind every internal workload admits a set of roles, and
// before this every admitted role reached every method the listener serves
// -- so a router's certificate was also a credential for the controller's
// CancelWorkflow.
//
// It is a pair of interceptors rather than another TLS callback because
// the method is not known at handshake time: one connection carries many
// calls, and the question is per call.
//
// Returns nil when the caller is not authorising by identity at all, so a
// deployment that has not turned mTLS on is unchanged rather than half
// enforced.
func MethodInterceptors(role string, authorize bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	if !authorize || role == "" {
		return nil, nil
	}
	check := func(ctx context.Context, fullMethod string) error {
		id, ok := peerIdentity(ctx)
		if !ok {
			// No identity to judge. The credentials refuse an
			// unidentified peer when authorization is on, so reaching
			// here means the listener is not the one that checked --
			// refuse rather than assume it did.
			return status.Error(codes.PermissionDenied, "no pgshard identity on the connection")
		}
		if !pki.AllowedMethod(role, id, fullMethod) {
			return status.Errorf(codes.PermissionDenied, "%s may not call %s", id.Role, fullMethod)
		}
		return nil
	}
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := check(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := check(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
	return unary, stream
}

// peerIdentity reads the pgshard identity off the peer's verified
// certificate. It takes the chain tls has already verified, so it reports
// who the peer is and never whether the peer is genuine.
func peerIdentity(ctx context.Context) (pki.Identity, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return pki.Identity{}, false
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return pki.Identity{}, false
	}
	return identityOf(info.State)
}

func identityOf(state tls.ConnectionState) (pki.Identity, bool) {
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 {
			continue
		}
		if id, err := pki.IdentityOfCert(chain[0]); err == nil {
			return id, true
		}
	}
	return pki.Identity{}, false
}
