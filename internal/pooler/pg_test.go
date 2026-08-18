package pooler

import (
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

var pgImages = []struct{ name, label string }{
	{"ghcr.io/andrew01234567890/pgshard-postgres:18", "pg18"},
	{"ghcr.io/andrew01234567890/pgshard-postgres:19", "pg19"},
}

func TestPostgres(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon unavailable")
	}
	ran := 0
	for _, img := range pgImages {
		if exec.Command("docker", "image", "inspect", img.name).Run() != nil {
			if out, err := exec.Command("docker", "pull", img.name).CombinedOutput(); err != nil {
				t.Logf("image %s unavailable: %v: %s", img.name, err, out)
				continue
			}
		}
		ran++
		t.Run(img.label, func(t *testing.T) { runPGSuite(t, img.name) })
	}
	if ran == 0 {
		t.Fatal("no PostgreSQL image available")
	}
}

func startPostgres(t *testing.T, image string) (addr, adminDSN string) {
	t.Helper()
	port := freePort(t)
	script := `initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 printf 'host all postgres all trust\nhost all all all scram-sha-256\n' >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*'`
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port), image, "sh", "-ec", script).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	adminDSN = fmt.Sprintf("postgres://postgres@%s/postgres?sslmode=disable", addr)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, adminDSN)
		cancel()
		if err == nil {
			_ = conn.Close(context.Background())
			return addr, adminDSN
		}
		time.Sleep(300 * time.Millisecond)
	}
	logs, _ := exec.Command("docker", "logs", id).CombinedOutput()
	t.Fatalf("postgres did not become ready:\n%s", logs)
	return "", ""
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// deriveKeys recomputes ClientKey/ServerKey the way the router recovers them
// from a SCRAM exchange: from the password plus the salt and iteration count
// of the verifier PostgreSQL stores in pg_authid.
func deriveKeys(ctx context.Context, t *testing.T, admin *pgx.Conn, role, password string) (ck, sk []byte) {
	t.Helper()
	var stored string
	if err := admin.QueryRow(ctx, "select rolpassword from pg_authid where rolname = $1", role).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	v, err := pgwire.ParseSCRAMVerifier(stored)
	if err != nil {
		t.Fatal(err)
	}
	salted, err := pbkdf2.Key(sha256.New, password, v.Salt, v.Iterations, sha256.Size)
	if err != nil {
		t.Fatal(err)
	}
	ck = hmacSHA256(salted, []byte("Client Key"))
	sk = hmacSHA256(salted, []byte("Server Key"))
	if h := sha256.Sum256(ck); string(h[:]) != string(v.StoredKey) {
		t.Fatal("derived ClientKey does not hash to the stored key")
	}
	return ck, sk
}

type pgHarness struct {
	addr   string
	admin  *pgx.Conn
	src    *StaticSource
	srv    *Server
	client pgshardv1.PoolerClient
	ck, sk []byte
}

func (h *pgHarness) identity() *pgshardv1.UserIdentity {
	return &pgshardv1.UserIdentity{Username: "appuser", ScramClientKey: append([]byte(nil), h.ck...), ScramServerKey: append([]byte(nil), h.sk...)}
}

func runPGSuite(t *testing.T, image string) {
	ctx := context.Background()
	addr, adminDSN := startPostgres(t, image)
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(ctx) })
	for _, sql := range []string{
		"create role appuser login password 'app-secret'",
		"create table secret_t (x int)",
		"create table items (id int primary key, name text)",
		"grant select, insert, update, delete on items to appuser",
	} {
		if _, err := admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	ck, sk := deriveKeys(ctx, t, admin, "appuser", "app-secret")

	src := NewStaticSource(View{Generation: 3, Epoch: 1, Role: pgshardv1.HealthStatus_ROLE_PRIMARY, Serving: true})
	dialer := Dialer{Address: addr, Timeout: 5 * time.Second}
	srv := NewServer(Config{Pool: NewPool(PoolConfig{MaxBackends: 4}, dialer), Source: src, Dialer: dialer, Database: "postgres",
		Logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	srv.Register(g)
	go func() { _ = g.Serve(l) }()
	conn, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); g.Stop() })
	h := &pgHarness{addr: addr, admin: admin, src: src, srv: srv, client: pgshardv1.NewPoolerClient(conn), ck: ck, sk: sk}

	t.Run("wrong_keys_rejected", h.testWrongKeys)
	t.Run("current_user", h.testCurrentUser)
	t.Run("permission_denied", h.testPermissionDenied)
	t.Run("prepared_statement", h.testPrepared)
	t.Run("copy_in", h.testCopyIn)
	t.Run("stale_generation", h.testStaleGeneration)
	t.Run("cancel", h.testCancel)
	t.Run("drain_lets_txn_commit", h.testDrain)
}

func (h *pgHarness) exec(t *testing.T, sid, sql string, ident *pgshardv1.UserIdentity) []*pgshardv1.ExecuteResponse {
	t.Helper()
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return roundTrip(t, stream, queryReq(sid, sql, gen(3, 1), ident))
}

func rows(rs []*pgshardv1.ExecuteResponse) []string {
	var out []string
	for _, r := range rs {
		if d := r.GetDataRow(); d != nil {
			var cols []string
			for _, c := range d.Columns {
				if c.Null {
					cols = append(cols, "NULL")
				} else {
					cols = append(cols, string(c.Data))
				}
			}
			out = append(out, strings.Join(cols, "|"))
		}
	}
	return out
}

func (h *pgHarness) testCurrentUser(t *testing.T) {
	rs := h.exec(t, "cu", "select current_user", h.identity())
	if e := firstError(rs); e != nil {
		t.Fatalf("error: %v", e)
	}
	if got := rows(rs); len(got) != 1 || got[0] != "appuser" {
		t.Fatalf("rows = %v", got)
	}
	if rs[len(rs)-1].GetReadyForQuery().TxnStatus != pgshardv1.ReadyForQuery_TXN_STATUS_IDLE {
		t.Fatal("expected idle")
	}
}

func (h *pgHarness) testPermissionDenied(t *testing.T) {
	rs := h.exec(t, "pd", "select * from secret_t", h.identity())
	e := firstError(rs)
	if e == nil || e.Sqlstate != "42501" {
		t.Fatalf("want 42501, got %v", e)
	}
}

func (h *pgHarness) testWrongKeys(t *testing.T) {
	ident := h.identity()
	ident.ScramClientKey[0] ^= 0xff
	rs := h.exec(t, "wk", "select 1", ident)
	e := firstError(rs)
	if e == nil || e.Sqlstate != "53300" || !strings.Contains(e.Message, "28P01") {
		t.Fatalf("want authentication failure surfaced, got %v", e)
	}
}

func (h *pgHarness) testPrepared(t *testing.T) {
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	g := gen(3, 1)
	msgs := []*pgshardv1.ExecuteRequest{
		{SessionId: "ps", Generation: g, User: h.identity(), Message: &pgshardv1.ExecuteRequest_Parse{Parse: &pgshardv1.Parse{Name: "ins", Sql: "insert into items values ($1, $2) returning id"}}},
		{SessionId: "ps", Generation: g, Message: &pgshardv1.ExecuteRequest_Bind{Bind: &pgshardv1.Bind{Statement: "ins", Params: []*pgshardv1.Value{{Data: []byte("1")}, {Data: []byte("one")}}}}},
		{SessionId: "ps", Generation: g, Message: &pgshardv1.ExecuteRequest_Describe{Describe: &pgshardv1.Describe{Kind: pgshardv1.Describe_KIND_PORTAL}}},
		{SessionId: "ps", Generation: g, Message: &pgshardv1.ExecuteRequest_Execute{Execute: &pgshardv1.ExecutePortal{}}},
		{SessionId: "ps", Generation: g, Message: &pgshardv1.ExecuteRequest_Sync{Sync: &pgshardv1.Sync{}}},
	}
	for _, m := range msgs {
		if err := stream.Send(m); err != nil {
			t.Fatal(err)
		}
	}
	rs := collect(t, stream)
	if e := firstError(rs); e != nil {
		t.Fatalf("error: %v", e)
	}
	var kinds []string
	for _, r := range rs {
		kinds = append(kinds, fmt.Sprintf("%T", r.Message))
	}
	want := []string{"*pgshardv1.ExecuteResponse_ParseComplete", "*pgshardv1.ExecuteResponse_BindComplete",
		"*pgshardv1.ExecuteResponse_RowDescription", "*pgshardv1.ExecuteResponse_DataRow",
		"*pgshardv1.ExecuteResponse_CommandComplete", "*pgshardv1.ExecuteResponse_ReadyForQuery"}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("kinds = %v", kinds)
	}
	if got := rows(rs); got[0] != "1" {
		t.Fatalf("rows = %v", got)
	}
	var name string
	if err := h.admin.QueryRow(context.Background(), "select name from items where id = 1").Scan(&name); err != nil || name != "one" {
		t.Fatalf("row not inserted: %v %q", err, name)
	}
}

func (h *pgHarness) testCopyIn(t *testing.T) {
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	g := gen(3, 1)
	if err := stream.Send(queryReq("cp", "copy items (id, name) from stdin", g, h.identity())); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil || resp.GetCopyInResponse() == nil {
		t.Fatalf("want CopyInResponse, got %v %v", resp, err)
	}
	for _, m := range []*pgshardv1.ExecuteRequest{
		{SessionId: "cp", Generation: g, Message: &pgshardv1.ExecuteRequest_CopyData{CopyData: &pgshardv1.CopyData{Data: []byte("10\tten\n")}}},
		{SessionId: "cp", Generation: g, Message: &pgshardv1.ExecuteRequest_CopyData{CopyData: &pgshardv1.CopyData{Data: []byte("11\televen\n")}}},
		{SessionId: "cp", Generation: g, Message: &pgshardv1.ExecuteRequest_CopyDone{CopyDone: &pgshardv1.CopyDone{}}},
	} {
		if err := stream.Send(m); err != nil {
			t.Fatal(err)
		}
	}
	rs := collect(t, stream)
	if e := firstError(rs); e != nil {
		t.Fatalf("copy error: %v", e)
	}
	var tag string
	for _, r := range rs {
		if c := r.GetCommandComplete(); c != nil {
			tag = c.Tag
		}
	}
	if tag != "COPY 2" {
		t.Fatalf("tag = %q", tag)
	}
	var n int
	if err := h.admin.QueryRow(context.Background(), "select count(*) from items where id in (10, 11)").Scan(&n); err != nil || n != 2 {
		t.Fatalf("copied rows = %d %v", n, err)
	}
	rs = roundTrip(t, stream, queryReq("cp", "copy (select id from items where id >= 10 order by id) to stdout", g, nil))
	var out []string
	for _, r := range rs {
		if d := r.GetCopyData(); d != nil {
			out = append(out, strings.TrimSpace(string(d.Data)))
		}
	}
	if strings.Join(out, ",") != "10,11" {
		t.Fatalf("copy out = %v", out)
	}
}

func (h *pgHarness) testStaleGeneration(t *testing.T) {
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs := roundTrip(t, stream, queryReq("sg", "select 1", gen(2, 1), h.identity()))
	e := firstError(rs)
	if e == nil || e.Sqlstate != "55000" || e.Message != "stale routing generation" {
		t.Fatalf("got %v", e)
	}
	rs = roundTrip(t, stream, queryReq("sg", "select 1", gen(3, 0), nil))
	if e := firstError(rs); e == nil || e.Message != "stale primary epoch" {
		t.Fatalf("got %v", e)
	}
	live, _ := h.srv.cfg.Pool.Stats()
	before := live
	rs = roundTrip(t, stream, queryReq("sg", "select 1", gen(3, 1), nil))
	if e := firstError(rs); e != nil {
		t.Fatalf("current generation refused: %v", e)
	}
	if live, _ := h.srv.cfg.Pool.Stats(); live < before {
		t.Fatal("stats regressed")
	}
}

func (h *pgHarness) testCancel(t *testing.T) {
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(queryReq("cn", "select pg_sleep(30)", gen(3, 1), h.identity())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.srv.held() >= 1 })
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	if _, err := h.client.Cancel(context.Background(), &pgshardv1.CancelRequest{SessionId: "cn"}); err != nil {
		t.Fatal(err)
	}
	rs := collect(t, stream)
	e := firstError(rs)
	if e == nil || e.Sqlstate != "57014" {
		t.Fatalf("want 57014 query_canceled, got %v", e)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("cancel took too long")
	}
	rs = roundTrip(t, stream, queryReq("cn", "select 2", gen(3, 1), nil))
	if e := firstError(rs); e != nil {
		t.Fatalf("session unusable after cancel: %v", e)
	}
}

func (h *pgHarness) testDrain(t *testing.T) {
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	g := gen(3, 1)
	roundTrip(t, stream, queryReq("dr", "begin", g, h.identity()))
	rs := roundTrip(t, stream, queryReq("dr", "insert into items values (99, 'drain')", g, nil))
	if e := firstError(rs); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- h.srv.Drain(context.Background()) }()
	waitFor(t, h.srv.draining.Load)
	select {
	case err := <-done:
		t.Fatalf("drain returned with an open transaction: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	rs = roundTrip(t, stream, queryReq("dr", "commit", g, nil))
	if e := firstError(rs); e != nil {
		t.Fatalf("commit refused during drain: %v", e)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("drain did not complete after commit")
	}
	var name string
	if err := h.admin.QueryRow(context.Background(), "select name from items where id = 99").Scan(&name); err != nil || name != "drain" {
		t.Fatalf("committed row missing: %v %q", err, name)
	}
	var n int
	if err := h.admin.QueryRow(context.Background(), "select count(*) from pg_stat_activity where usename = 'appuser'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d appuser backends still connected after drain", n)
	}
}
