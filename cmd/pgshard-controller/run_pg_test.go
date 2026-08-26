package main

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/dockertest"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/cli"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

const pgImage = "ghcr.io/andrew01234567890/pgshard-postgres:18"

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func startPostgres(t *testing.T) string {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	if exec.Command("docker", "image", "inspect", pgImage).Run() != nil {
		if out, err := exec.Command("docker", "pull", pgImage).CombinedOutput(); err != nil {
			dockertest.Unavailable(t, "image %s unavailable: %v: %s", pgImage, err, out)
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port),
		"--entrypoint", "sh", pgImage, "-ec",
		`initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 echo "host all all all trust" >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*'`).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			_ = conn.Close(context.Background())
			return dsn
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("postgres did not become ready")
	return ""
}

func TestRunAgainstPostgres(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.databases (name) VALUES ('app')`); err != nil {
		t.Fatal(err)
	}

	rctx, cancel := context.WithCancel(ctx)
	var out, errb syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- runController(rctx, []string{"--catalog-dsn", dsn, "--listen", "127.0.0.1:0", "--insecure-dev", "--election-retry", "200ms"}, &out, &errb)
	}()
	var addr string
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline) && addr == ""; time.Sleep(50 * time.Millisecond) {
		if m := regexp.MustCompile(`listening on (\S+)`).FindStringSubmatch(out.String()); m != nil {
			addr = m[1]
		}
	}
	if addr == "" {
		cancel()
		t.Fatalf("controller did not listen: %s / %s", out.String(), errb.String())
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cc.Close() }()
	client := pgshardv1.NewControllerClient(cc)
	if resp, err := client.ListWorkflows(ctx, &pgshardv1.ListWorkflowsRequest{}); err != nil || len(resp.Workflows) != 0 {
		t.Fatalf("list: %v %v", resp, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key)
		VALUES ('app', 'public', 'orders', 'sharded', 'customer_id')`); err != nil {
		t.Fatal(err)
	}
	var placement string
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline) && placement == ""; time.Sleep(100 * time.Millisecond) {
		_ = conn.QueryRow(ctx, `SELECT effective_placement FROM pgshard.table_status WHERE table_name = 'orders'`).Scan(&placement)
	}
	if placement != "sharded" {
		t.Fatalf("table_status not reconciled by the running controller: %s", errb.String())
	}
	if _, err := conn.Exec(ctx, `UPDATE pgshard.tables SET shard_key = 'region_id' WHERE table_name = 'orders'`); err != nil {
		t.Fatal(err)
	}
	var workflows []*pgshardv1.Workflow
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline) && len(workflows) == 0; time.Sleep(100 * time.Millisecond) {
		resp, err := client.ListWorkflows(ctx, &pgshardv1.ListWorkflowsRequest{Kind: pgshardv1.WorkflowKind_WORKFLOW_KIND_REKEY})
		if err != nil {
			t.Fatal(err)
		}
		workflows = resp.Workflows
	}
	if len(workflows) != 1 || workflows[0].State != pgshardv1.WorkflowState_WORKFLOW_STATE_PENDING {
		t.Fatalf("workflows %v", workflows)
	}
	cancel()
	if code := <-done; code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
}
