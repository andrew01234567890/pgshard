package agentauth

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func TestTokenIsNotThePassword(t *testing.T) {
	tok, err := Token("hunter2")
	if err != nil || tok == "hunter2" || len(tok) != 64 {
		t.Fatalf("token = %q, err = %v", tok, err)
	}
	a, _ := Token("a")
	b, _ := Token("b")
	if a == b {
		t.Fatal("tokens must differ per password")
	}
}

func TestTokenRefusesEmptyPassword(t *testing.T) {
	if tok, err := Token(""); err == nil {
		t.Fatalf("Token(\"\") = %q, want error", tok)
	}
}

func TestServerInterceptorGatesEveryRPC(t *testing.T) {
	token, err := Token("pw")
	if err != nil {
		t.Fatal(err)
	}
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
	wrong, err := Token("wrong")
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
	password := "pw-old"
	var mu sync.Mutex
	cl := dialInterceptedAgent(t, DynamicUnaryServerInterceptor(func() (string, error) {
		mu.Lock()
		defer mu.Unlock()
		return Token(password)
	}))
	oldTok, _ := Token("pw-old")
	newTok, _ := Token("pw-new")
	ctx := context.Background()
	if _, err := cl.Promote(WithToken(ctx, oldTok), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("old token before rotation: %v", err)
	}
	mu.Lock()
	password = "pw-new"
	mu.Unlock()
	if _, err := cl.Promote(WithToken(ctx, newTok), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("rotated token must be accepted without a restart: %v", err)
	}
	if _, err := cl.Promote(WithToken(ctx, oldTok), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("stale token after rotation: %v", err)
	}
}
