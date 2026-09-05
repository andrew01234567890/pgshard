// Package agentauth authenticates control-plane calls to member agents with
// a token the operator generates and mounts into every member.
//
// It used to be derived from the cluster's superuser password instead, which
// made that password a control-plane credential: anything holding it could
// call Promote, Demote, Rewind and Reclone. The derived token was withdrawn
// in PGS-572 once every caller sent the mounted one.
package agentauth

import (
	"context"
	"crypto/hmac"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MetadataKey carries the token on agent RPCs.
const MetadataKey = "pgshard-agent-token"

// WithToken returns a context whose outgoing gRPC metadata carries the token.
func WithToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataKey, token)
}

// WithTokens carries several tokens, for a caller that reaches agents of
// more than one cluster in a single pass. The server accepts a call
// presenting any token it knows, so each agent takes its own and ignores
// the rest.
//
// This is not the withdrawn derived-token fallback: these are distinct
// clusters' own mounted tokens, and neither is derived from a password. A
// restore is the case -- it reconciles prepared transactions on the source
// cluster and polls the primaries of the new one, and every cluster's agent
// token is generated independently.
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

// AnyOfStreamServerInterceptor is the streaming half of
// AnyOfUnaryServerInterceptor, and exists so that adding a streaming
// method to the agent cannot quietly add an unauthenticated one.
//
// The Agent service is unary throughout today, so this gates nothing yet.
// That is the point: a unary-only interceptor leaves a service whose
// methods include Promote, Demote, Rewind and Reclone one `returns
// (stream ...)` away from an open door, and nothing would fail to say so.
func AnyOfStreamServerInterceptor(tokensFn func() ([]string, error)) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		tokens, err := tokensFn()
		if err != nil || !authorized(ss.Context(), tokens) {
			return errUnauthenticated
		}
		return handler(srv, ss)
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
