package agentauth

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func TestServerInterceptorGatesEveryRPC(t *testing.T) {
	// Not a hex string: a 32-character one reads as a real key to the
	// secret scanner, and a test fixture that fails the gate is a fixture
	// nobody can commit.
	const token = "the-agents-control-plane-token"
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor(token)))
	pgshardv1.RegisterAgentServer(srv, &pgshardv1.UnimplementedAgentServer{})
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return ln.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	cl := pgshardv1.NewAgentClient(conn)
	ctx := context.Background()

	if _, err := cl.Promote(ctx, &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no token: %v, want Unauthenticated", err)
	}
	const wrong = "not-the-token"
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Promote(WithToken(ctx, wrong), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: %v, want Unauthenticated", err)
	}
	if _, err := cl.Promote(WithToken(ctx, token), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("valid token must reach the handler: %v, want Unimplemented", err)
	}
	if _, err := cl.Status(ctx, &pgshardv1.StatusRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Status without token: %v, want Unauthenticated", err)
	}
}

func dialInterceptedAgent(t *testing.T, ic grpc.UnaryServerInterceptor) pgshardv1.AgentClient {
	t.Helper()
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(ic))
	pgshardv1.RegisterAgentServer(srv, &pgshardv1.UnimplementedAgentServer{})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return ln.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pgshardv1.NewAgentClient(conn)
}

func TestEmptyExpectedTokenRejectsEverything(t *testing.T) {
	cl := dialInterceptedAgent(t, UnaryServerInterceptor(""))
	if _, err := cl.Promote(WithToken(context.Background(), ""), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("empty expected token must reject even an empty presented token: %v", err)
	}
}

func TestDynamicInterceptorFollowsRotation(t *testing.T) {
	const oldTok, newTok = "token-before-rotation", "token-after-rotation"
	current := oldTok
	var mu sync.Mutex
	cl := dialInterceptedAgent(t, DynamicUnaryServerInterceptor(func() (string, error) {
		mu.Lock()
		defer mu.Unlock()
		return current, nil
	}))
	ctx := context.Background()
	if _, err := cl.Promote(WithToken(ctx, oldTok), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("old token before rotation: %v", err)
	}
	mu.Lock()
	current = newTok
	mu.Unlock()
	if _, err := cl.Promote(WithToken(ctx, newTok), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("rotated token must be accepted without a restart: %v", err)
	}
	if _, err := cl.Promote(WithToken(ctx, oldTok), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("stale token after rotation: %v", err)
	}
}

// TestAnAgentAcceptsOnlyItsOwnToken. The control-plane token used to be
// derived from the superuser password, and for one release an agent
// accepted that as well as the mounted one so a cluster could be rolled a
// member at a time. PGS-572 withdrew the derived half: the password is no
// longer a credential that unlocks Promote, Demote, Rewind and Reclone.
func TestAnAgentAcceptsOnlyItsOwnToken(t *testing.T) {
	const own = "0123456789abcdef"
	md, _ := metadata.FromOutgoingContext(WithToken(context.Background(), own))
	if got := md.Get(MetadataKey); len(got) != 1 || got[0] != own {
		t.Fatalf("a caller sends one token: %v", got)
	}

	for _, tc := range []struct {
		name   string
		expect []string
		accept bool
	}{
		{"the agent's own token", []string{own}, true},
		{"an agent of another cluster", []string{"someone-elses"}, false},
		{"an agent with no token at all", nil, false},
		{"an agent whose token is empty", []string{""}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := metadata.NewIncomingContext(context.Background(), md)
			var reached bool
			handler := func(context.Context, any) (any, error) { reached = true; return nil, nil }
			_, err := AnyOfUnaryServerInterceptor(func() ([]string, error) { return tc.expect, nil })(in, nil, nil, handler)
			if reached != tc.accept {
				t.Fatalf("reached = %v, want %v (err %v)", reached, tc.accept, err)
			}
		})
	}
}

// TestStreamInterceptorGatesTheSameWayAsUnary: the Agent service is unary
// throughout, so a unary-only interceptor authenticates everything today
// and would keep looking correct the moment somebody adds `returns
// (stream ...)` to a service whose methods include Promote, Demote,
// Rewind and Reclone. These exercise the streaming interceptor directly,
// since there is no streaming RPC to call.
func TestStreamInterceptorGatesTheSameWayAsUnary(t *testing.T) {
	tokens := func() ([]string, error) { return []string{"expected"}, nil }
	intercept := AnyOfStreamServerInterceptor(tokens)
	handled := false
	// Returns a distinguishable error so a test cannot mistake "the handler
	// ran" for "the interceptor allowed it and the handler did nothing".
	errHandlerRan := errors.New("handler ran")
	handler := func(any, grpc.ServerStream) error { handled = true; return errHandlerRan }

	t.Run("a stream with no token is refused", func(t *testing.T) {
		handled = false
		err := intercept(nil, fakeStream{ctx: context.Background()}, nil, handler)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("err = %v, want Unauthenticated", err)
		}
		if handled {
			t.Fatal("the handler ran for an unauthenticated stream")
		}
	})

	t.Run("a stream with the wrong token is refused", func(t *testing.T) {
		handled = false
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(MetadataKey, "wrong"))
		if err := intercept(nil, fakeStream{ctx: ctx}, nil, handler); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("err = %v, want Unauthenticated", err)
		}
		if handled {
			t.Fatal("the handler ran for a stream presenting the wrong token")
		}
	})

	t.Run("a stream with the token is served", func(t *testing.T) {
		handled = false
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(MetadataKey, "expected"))
		if err := intercept(nil, fakeStream{ctx: ctx}, nil, handler); !errors.Is(err, errHandlerRan) {
			t.Fatalf("err = %v, want the handler's own error, proving it was reached", err)
		}
		if !handled {
			t.Fatal("the handler did not run for an authenticated stream")
		}
	})

	t.Run("a token source that errors refuses rather than opens", func(t *testing.T) {
		handled = false
		failing := AnyOfStreamServerInterceptor(func() ([]string, error) { return nil, errors.New("no secret") })
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(MetadataKey, "expected"))
		if err := failing(nil, fakeStream{ctx: ctx}, nil, handler); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("err = %v, want Unauthenticated", err)
		}
		if handled {
			t.Fatal("the handler ran although the tokens could not be read")
		}
	})
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f fakeStream) Context() context.Context { return f.ctx }
