// Package agentauth authenticates control-plane calls to member agents with
// a token both sides derive from the cluster's superuser password, which the
// operator provisions and every agent already holds. The password itself
// never travels on the wire.
package agentauth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MetadataKey carries the token on agent RPCs.
const MetadataKey = "pgshard-agent-token"

var derivationKey = []byte("pgshard-agent-auth-v1")

// Token derives the shared token from the superuser password.
func Token(password string) string {
	mac := hmac.New(sha256.New, derivationKey)
	mac.Write([]byte(password))
	return hex.EncodeToString(mac.Sum(nil))
}

// WithToken returns a context whose outgoing gRPC metadata carries the token.
func WithToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataKey, token)
}

// ErrUnauthenticated is returned to callers without a valid token.
var errUnauthenticated = status.Error(codes.Unauthenticated, "agent: missing or invalid "+MetadataKey)

// UnaryServerInterceptor rejects every call that does not present token.
func UnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !authorized(ctx, token) {
			return nil, errUnauthenticated
		}
		return handler(ctx, req)
	}
}

func authorized(ctx context.Context, token string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, got := range md.Get(MetadataKey) {
		if hmac.Equal([]byte(got), []byte(token)) {
			return true
		}
	}
	return false
}
