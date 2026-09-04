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

	"github.com/andrew01234567890/pgshard/internal/dockertest"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// pgImages lists candidate images per major; the project image is preferred
// and the official image is the fallback (used in CI until the project images
// are published). One image per label is run.
var pgImages = []struct{ name, label string }{
	{"ghcr.io/andrew01234567890/pgshard-postgres:18", "pg18"},
	{"postgres:18", "pg18"},
	{"ghcr.io/andrew01234567890/pgshard-postgres:19", "pg19"},
	{"postgres:19", "pg19"},
	{"postgres:19beta3", "pg19"},
}

func TestPostgres(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		dockertest.Unavailable(t, "docker not on PATH")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker daemon unavailable")
	}
	ran := 0
	seen := map[string]bool{}
	for _, img := range pgImages {
		if seen[img.label] {
			continue
		}
		if exec.Command("docker", "image", "inspect", img.name).Run() != nil {
			if out, err := exec.Command("docker", "pull", img.name).CombinedOutput(); err != nil {
				t.Logf("image %s unavailable: %v: %s", img.name, err, out)
				continue
			}
		}
		ran++
		seen[img.label] = true
		t.Run(img.label, func(t *testing.T) { runPGSuite(t, img.name) })
	}
	if ran == 0 {
		t.Fatal("no PostgreSQL image available")
	}
}

func startPostgres(t *testing.T, image string) (addr, adminDSN string) {
	t.Helper()
	// Admission here rather than in each test: a test that starts a
	// container is exactly the one that needs a slot, and putting it at
	// the one place that starts them means a new test cannot forget.
	dockertest.Parallel(t)
	script := `initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 printf 'host all postgres all trust\nhost replication postgres all trust\nhost all all all scram-sha-256\n' >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*' -c wal_level=logical -c max_prepared_transactions=16`
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", "127.0.0.1::5432", "--user", "postgres", "--entrypoint", "sh", image, "-ec", script).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	addr = dockertest.HostPort(t, id, "5432")
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
		Logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil)),
		Stream: StreamConfig{DSN: adminDSN, Shard: "shard0", Heartbeat: 300 * time.Millisecond}})
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
	t.Run("per_database_routing", h.testPerDatabase)
	t.Run("current_user", h.testCurrentUser)
	t.Run("permission_denied", h.testPermissionDenied)
	t.Run("prepared_statement", h.testPrepared)
	t.Run("row_limited_portal_suspends", h.testPortalSuspended)
	t.Run("flush_answers_without_sync", h.testFlush)
	t.Run("backend_state_does_not_outlive_a_session", h.testNoStateLeak)
	t.Run("copy_in", h.testCopyIn)
	t.Run("stale_generation", h.testStaleGeneration)
	t.Run("cancel", h.testCancel)
	t.Run("copy_tables", h.testCopyTables)
	t.Run("change_stream", h.testStream)
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

func (h *pgHarness) execDB(t *testing.T, sid, database, sql string, ident *pgshardv1.UserIdentity) []*pgshardv1.ExecuteResponse {
	t.Helper()
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	req := queryReq(sid, sql, gen(3, 1), ident)
	req.Database = database
	return roundTrip(t, stream, req)
}

func (h *pgHarness) testPerDatabase(t *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"create database db_a owner appuser",
		"create database db_b owner appuser",
	} {
		if _, err := h.admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, db := range []string{"db_a", "db_b"} {
		dsn := fmt.Sprintf("postgres://postgres@%s/%s?sslmode=disable", h.addr, db)
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, fmt.Sprintf("create table marker (v text); insert into marker values ('%s'); grant select on marker to appuser", db)); err != nil {
			t.Fatal(err)
		}
		_ = c.Close(ctx)
	}
	for i, db := range []string{"db_a", "db_b", "db_a"} {
		sid := fmt.Sprintf("pdb%d", i)
		rs := h.execDB(t, sid, db, "select current_database(), v from marker", h.identity())
		if e := firstError(rs); e != nil {
			t.Fatalf("%s: %v", db, e)
		}
		if got := rows(rs); len(got) != 1 || got[0] != db+"|"+db {
			t.Fatalf("%s: rows = %v", db, got)
		}
	}
	rs := h.execDB(t, "pdbmissing", "db_missing", "select 1", h.identity())
	if e := firstError(rs); e == nil || e.Sqlstate != "3D000" {
		t.Fatalf("missing database must be refused with 3D000: %v", e)
	}
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
	if e == nil || e.Sqlstate != "28P01" {
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
	// Drain returning means the pooler let go of its backends, not that
	// PostgreSQL has finished reaping them: the backend leaves
	// pg_stat_activity when its process exits, a moment later.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := h.admin.QueryRow(context.Background(), "select count(*) from pg_stat_activity where usename = 'appuser'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d appuser backends still connected after drain", n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// testPortalSuspended: a real backend answers a row-limited Execute with
// PortalSuspended instead of CommandComplete. The conversion dropped it,
// so a partial fetch reached the router as a finished result set.
func (h *pgHarness) testPortalSuspended(t *testing.T) {
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	g := gen(3, 1)
	for _, m := range []*pgshardv1.ExecuteRequest{
		{SessionId: "sus", Generation: g, User: h.identity(), Message: &pgshardv1.ExecuteRequest_Parse{Parse: &pgshardv1.Parse{Sql: "select i from generate_series(1, 10) i"}}},
		{SessionId: "sus", Generation: g, Message: &pgshardv1.ExecuteRequest_Bind{Bind: &pgshardv1.Bind{}}},
		{SessionId: "sus", Generation: g, Message: &pgshardv1.ExecuteRequest_Execute{Execute: &pgshardv1.ExecutePortal{MaxRows: 3}}},
		{SessionId: "sus", Generation: g, Message: &pgshardv1.ExecuteRequest_Sync{Sync: &pgshardv1.Sync{}}},
	} {
		if err := stream.Send(m); err != nil {
			t.Fatal(err)
		}
	}
	rs := collect(t, stream)
	if e := firstError(rs); e != nil {
		t.Fatalf("error: %v", e)
	}
	var suspended, completed, dataRows int
	for _, r := range rs {
		switch r.Message.(type) {
		case *pgshardv1.ExecuteResponse_PortalSuspended:
			suspended++
		case *pgshardv1.ExecuteResponse_CommandComplete:
			completed++
		case *pgshardv1.ExecuteResponse_DataRow:
			dataRows++
		}
	}
	if suspended != 1 {
		t.Fatalf("PortalSuspended count = %d, want the row limit to suspend the portal", suspended)
	}
	if dataRows != 3 {
		t.Fatalf("data rows = %d, want the row limit", dataRows)
	}
	if completed != 0 {
		t.Fatalf("CommandComplete count = %d, want none: a suspended portal did not finish", completed)
	}
}

// testFlush: a Flush must produce the answers to everything sent since the
// last one, without a ReadyForQuery and without ending the batch. There is
// no PostgreSQL message that says "that is all", so the pooler counts what
// it forwarded and answers with its own FlushComplete; getting that count
// wrong hangs the session, which is the bug this exists to fix.
func (h *pgHarness) testFlush(t *testing.T) {
	stream, err := h.client.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	g := gen(3, 1)
	send := func(msgs ...*pgshardv1.ExecuteRequest) {
		t.Helper()
		for _, m := range msgs {
			if err := stream.Send(m); err != nil {
				t.Fatal(err)
			}
		}
	}
	req := func(m any) *pgshardv1.ExecuteRequest {
		r := &pgshardv1.ExecuteRequest{SessionId: "fl", Generation: g, User: h.identity()}
		switch v := m.(type) {
		case *pgshardv1.Parse:
			r.Message = &pgshardv1.ExecuteRequest_Parse{Parse: v}
		case *pgshardv1.Bind:
			r.Message = &pgshardv1.ExecuteRequest_Bind{Bind: v}
		case *pgshardv1.ExecutePortal:
			r.Message = &pgshardv1.ExecuteRequest_Execute{Execute: v}
		case *pgshardv1.Describe:
			r.Message = &pgshardv1.ExecuteRequest_Describe{Describe: v}
		case *pgshardv1.Flush:
			r.Message = &pgshardv1.ExecuteRequest_Flush{Flush: v}
		case *pgshardv1.Sync:
			r.Message = &pgshardv1.ExecuteRequest_Sync{Sync: v}
		}
		return r
	}
	readTo := func(want string) []*pgshardv1.ExecuteResponse {
		t.Helper()
		var out []*pgshardv1.ExecuteResponse
		for {
			r, err := stream.Recv()
			if err != nil {
				t.Fatalf("waiting for %s: %v", want, err)
			}
			out = append(out, r)
			if fmt.Sprintf("%T", r.Message) == want {
				return out
			}
		}
	}

	// Describe adds a second message that answers with two responses, one
	// of which is not a terminator; miscounting it would hang here.
	send(req(&pgshardv1.Parse{Sql: "select i from generate_series(1, 5) i"}),
		req(&pgshardv1.Bind{}),
		req(&pgshardv1.Describe{Kind: pgshardv1.Describe_KIND_PORTAL}),
		req(&pgshardv1.ExecutePortal{MaxRows: 2}),
		req(&pgshardv1.Flush{}))
	first := readTo("*pgshardv1.ExecuteResponse_FlushComplete")
	if e := firstError(first); e != nil {
		t.Fatalf("flush: %v", e)
	}
	var rows, suspended, ready int
	for _, r := range first {
		switch r.Message.(type) {
		case *pgshardv1.ExecuteResponse_DataRow:
			rows++
		case *pgshardv1.ExecuteResponse_PortalSuspended:
			suspended++
		case *pgshardv1.ExecuteResponse_ReadyForQuery:
			ready++
		}
	}
	if rows != 2 || suspended != 1 {
		t.Fatalf("first flush: %d rows, %d suspends; want the row limit and one suspend", rows, suspended)
	}
	if ready != 0 {
		t.Fatal("a Flush must not produce ReadyForQuery: that is what makes it a Flush")
	}

	// The portal survived, so the client fetches the rest from it.
	send(req(&pgshardv1.ExecutePortal{MaxRows: 2}), req(&pgshardv1.Flush{}))
	second := readTo("*pgshardv1.ExecuteResponse_FlushComplete")
	rows = 0
	for _, r := range second {
		if _, ok := r.Message.(*pgshardv1.ExecuteResponse_DataRow); ok {
			rows++
		}
	}
	if rows != 2 {
		t.Fatalf("second flush returned %d rows, want the portal to have continued", rows)
	}

	// And Sync still ends the batch it left open.
	send(req(&pgshardv1.Sync{}))
	if e := firstError(readTo("*pgshardv1.ExecuteResponse_ReadyForQuery")); e != nil {
		t.Fatalf("sync after flush: %v", e)
	}
}

// testNoStateLeak: the router pins on syntax, and a statement's effect on
// the backend need not be visible in its syntax. SELECT set_config(...,
// false) sets a GUC and pg_advisory_lock() takes a lock the backend holds,
// and both parse as ordinary reads -- so neither pinned the session, and
// whatever they left was inherited by the next logical session of the same
// role. That is a leak between clients, not untidiness.
// waitIdle waits until every backend is back in the pool. The reset runs
// after the client has its ReadyForQuery, so without this the next session
// starts on a second backend while the first is still being cleaned, and
// an assertion about what the first left behind races the cleaning.
func waitIdle(t *testing.T, h *pgHarness) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if live, idle := h.srv.cfg.Pool.Stats(); live == idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("backends did not return to the pool")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *pgHarness) testNoStateLeak(t *testing.T) {
	ctx := context.Background()
	run := func(session, sql string) []*pgshardv1.ExecuteResponse {
		t.Helper()
		stream, err := h.client.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&pgshardv1.ExecuteRequest{SessionId: session, Generation: gen(3, 1), User: h.identity(),
			Message: &pgshardv1.ExecuteRequest_SimpleQuery{SimpleQuery: &pgshardv1.SimpleQuery{Sql: sql}}}); err != nil {
			t.Fatal(err)
		}
		rs := collect(t, stream)
		if e := firstError(rs); e != nil {
			t.Fatalf("%s: %v", sql, e)
		}
		return rs
	}
	// Neither of these looks like a SET to a parser. Each runs on its own
	// logical session, so the backend goes back to the pool between them --
	// which is the handoff the state must not survive.
	run("leak1", "SELECT set_config('application_name', 'left-behind', false)")
	run("leak2", "SELECT pg_advisory_lock(4242)")

	waitIdle(t, h)
	// Asked of the whole server, not of whichever backend this session
	// happens to land on: a check that only inspects the current backend
	// passes for free whenever the pool hands out a different one.
	if got := rows(run("innocent1", "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'left-behind'")); len(got) != 1 || got[0] != "0" {
		t.Errorf("a pooled backend still carries the application_name a finished session set: %v", got)
	}
	// pg_try_advisory_lock reports false while another backend holds it, so
	// this fails if the lock outlived the session that took it. The reset
	// runs after the client has its ReadyForQuery, so the next session can
	// start -- on a different backend -- while the one that took the lock
	// is still being cleaned; a lock that was actually leaked never clears.
	// Likewise: pg_try_advisory_lock succeeds on the session that already
	// holds the lock, so it reports nothing unless the pool happens to hand
	// out a different backend. pg_locks sees the holder whichever backend
	// asks.
	if got := rows(run("innocent2", "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'")); len(got) != 1 || got[0] != "0" {
		t.Errorf("advisory lock still held by a pooled backend: %v", got)
	}
}
