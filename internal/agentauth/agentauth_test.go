package agentauth

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

func TestTokenIsNotThePassword(t *testing.T) {
	if tok := Token("hunter2"); tok == "hunter2" || len(tok) != 64 {
		t.Fatalf("token = %q", tok)
	}
	if Token("a") == Token("b") {
		t.Fatal("tokens must differ per password")
	}
}

func TestServerInterceptorGatesEveryRPC(t *testing.T) {
	token := Token("pw")
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
	if _, err := cl.Promote(WithToken(ctx, Token("wrong")), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: %v, want Unauthenticated", err)
	}
	if _, err := cl.Promote(WithToken(ctx, token), &pgshardv1.PromoteRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("valid token must reach the handler: %v, want Unimplemented", err)
	}
	if _, err := cl.Status(ctx, &pgshardv1.StatusRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Status without token: %v, want Unauthenticated", err)
	}
}
