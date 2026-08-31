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
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MetadataKey carries the token on agent RPCs.
const MetadataKey = "pgshard-agent-token"

var derivationKey = []byte("pgshard-agent-auth-v1")

// Token derives the shared token from the superuser password. An empty
// password is refused: its token would be a well-known constant any caller
// could derive.
func Token(password string) (string, error) {
	if password == "" {
		return "", errors.New("agentauth: refusing to derive a token from an empty password")
	}
	mac := hmac.New(sha256.New, derivationKey)
	mac.Write([]byte(password))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// WithToken returns a context whose outgoing gRPC metadata carries the token.
func WithToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataKey, token)
}

// WithTokens carries several tokens, so one caller reaches agents that
// expect different ones during a rolling update. The server accepts a call
// presenting any token it knows.
func WithTokens(ctx context.Context, tokens ...string) context.Context {
	for _, t := range tokens {
		if t != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, MetadataKey, t)
		}
	}
	return ctx
}

// ErrUnauthenticated is returned to callers without a valid token.
var errUnauthenticated = status.Error(codes.Unauthenticated, "agent: missing or invalid "+MetadataKey)

// UnaryServerInterceptor rejects every call that does not present token.
// An empty expected token rejects everything.
func UnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	return DynamicUnaryServerInterceptor(func() (string, error) { return token, nil })
}

// DynamicUnaryServerInterceptor re-evaluates the expected token on every
// call, so a rotated superuser Secret is picked up without restarting the
// agent. A failed or empty evaluation rejects the call.
func DynamicUnaryServerInterceptor(tokenFn func() (string, error)) grpc.UnaryServerInterceptor {
	return AnyOfUnaryServerInterceptor(func() ([]string, error) {
		token, err := tokenFn()
		if err != nil {
			return nil, err
		}
		return []string{token}, nil
	})
}

// AnyOfUnaryServerInterceptor accepts a call presenting any one of the
// tokens tokensFn returns. Several exist so an agent can be moved onto its
// own token without a flag day: for one release it accepts both that and
// the one derived from the superuser password, and callers send both, so
// old and new agents are reachable by old and new callers throughout a
// rolling update. Removing the derived one is a deliberate later step.
//
// A failed evaluation, no tokens, or only empty ones rejects the call: an
// empty expected token would otherwise match a caller that sent nothing.
func AnyOfUnaryServerInterceptor(tokensFn func() ([]string, error)) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		tokens, err := tokensFn()
		if err != nil || !authorized(ctx, tokens) {
			return nil, errUnauthenticated
		}
		return handler(ctx, req)
	}
}

func authorized(ctx context.Context, tokens []string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, want := range tokens {
		if want == "" {
			continue
		}
		for _, got := range md.Get(MetadataKey) {
			if hmac.Equal([]byte(got), []byte(want)) {
				return true
			}
		}
	}
	return false
}
