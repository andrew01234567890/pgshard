//go:build integration

package router

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/router"
)

const (
	appDatabase = "app"
	appRole     = "app"
	appPassword = "app-secret"
)

func pgImage() string {
	if v := os.Getenv("PGSHARD_PG_IMAGE"); v != "" {
		return v
	}
	return "ghcr.io/andrew01234567890/pgshard-postgres:18"
}

// stack is one catalog, one shard, a pooler and a router.
type stack struct {
	catalogDSN string
	shardAddr  string
	shardDSN   string
	routerAddr string
	routerPort string
	poolerAddr string
	routerBin  string
	peerAddr   string
	healthAddr string
	routerLog  *logBuffer
	poolerLog  *logBuffer

	controllerBin string
	controllerLog *logBuffer
	controller    *exec.Cmd
	controllerRun <-chan struct{}
	// shardDSNs are the superuser DSNs of the shards by id, for the
	// controller's resolver and DDL applier.
	shardDSNs map[int]string
}

// routerProc is one extra router process on the stack.
type routerProc struct {
	cmd        *exec.Cmd
	exited     <-chan struct{}
	addr       string
	peerAddr   string
	healthAddr string
	log        *logBuffer
}

type logBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func requireDocker(tb testing.TB) {
	tb.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		tb.Skip("docker not on PATH")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		tb.Skip("docker daemon unavailable")
	}
	if exec.Command("docker", "image", "inspect", pgImage()).Run() != nil {
		if out, err := exec.Command("docker", "pull", pgImage()).CombinedOutput(); err != nil {
			tb.Skipf("image %s unavailable: %v: %s", pgImage(), err, out)
		}
	}
}

// dockerHostPort reads back the 127.0.0.1 address Docker published for the
// container's PostgreSQL port.
func dockerHostPort(tb testing.TB, container string) string {
	tb.Helper()
	out, err := exec.Command("docker", "port", container, "5432/tcp").Output()
	if err != nil {
		tb.Fatalf("docker port %s: %v", container, err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if addr := strings.TrimSpace(line); strings.HasPrefix(addr, "127.0.0.1:") {
			return addr
		}
	}
	tb.Fatalf("docker port %s: no 127.0.0.1 mapping in %q", container, out)
	return ""
}

// freePort is still how the pgshard binaries under test are given a listen
// address: they take one on the command line and do not report a port they
// chose themselves, so there is nothing to read back. The window is real but
// small, and closing it means teaching each binary to accept :0 and say
// where it landed.
func freePort(tb testing.TB) int {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// containers maps a server's host address to the docker container running
// it. Docker chooses the host port now, so the name cannot be derived from
// the port after the fact; whoever wants the logs asks here.
var containers sync.Map

// containerAt is the container name startPostgres gave the server listening
// on addr, which may be a bare host:port or a DSN containing one.
func containerAt(tb testing.TB, addr string) string {
	tb.Helper()
	if u, err := url.Parse(addr); err == nil && u.Host != "" {
		addr = u.Host
	}
	name, ok := containers.Load(addr)
	if !ok {
		tb.Fatalf("no container recorded for %s; startPostgres records every server it starts", addr)
	}
	return name.(string)
}

func startPostgres(tb testing.TB, name string, opts ...string) (addr, adminDSN string) {
	tb.Helper()
	script := `initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 printf 'host all postgres all trust\nhost all all all scram-sha-256\n' >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*' -c wal_level=logical ` + strings.Join(opts, " ")
	cname := fmt.Sprintf("pgshard-router-e2e-%s-%d", name, time.Now().UnixNano())
	// Docker chooses the host port and is asked which one it took. Choosing
	// it here would mean binding a listener, closing it, and asking Docker
	// to bind the same number a moment later; between those two anything
	// else on the machine can take it, and "ports are not available" says
	// nothing about the code under test.
	out, err := exec.Command("docker", "run", "-d", "--rm", "--name", cname, "-p", "127.0.0.1::5432", "--entrypoint", "sh", pgImage(), "-ec", script).CombinedOutput()
	if err != nil {
		tb.Fatalf("docker run: %v: %s", err, out)
	}
	tb.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", cname).Run() })
	addr = dockerHostPort(tb, cname)
	containers.Store(addr, cname)
	tb.Cleanup(func() { containers.Delete(addr) })
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
	logs, _ := exec.Command("docker", "logs", cname).CombinedOutput()
	tb.Fatalf("%s did not become ready:\n%s", name, logs)
	return "", ""
}

func buildBinaries(tb testing.TB) (pooler, rtr string) {
	tb.Helper()
	return buildBinariesTagged(tb, "")
}

// buildBinariesTagged builds the pooler and a router with the given build
// tags.
func buildBinariesTagged(tb testing.TB, tags string) (pooler, rtr string) {
	tb.Helper()
	dir := tb.TempDir()
	root, err := filepath.Abs("../../..")
	if err != nil {
		tb.Fatal(err)
	}
	for _, c := range []string{"pgshard-pooler", "pgshard-router", "pgshard-controller"} {
		cmd := exec.Command("go", "build", "-tags", tags, "-o", filepath.Join(dir, c), "./cmd/"+c)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			tb.Fatalf("build %s: %v: %s", c, err, out)
		}
	}
	return filepath.Join(dir, "pgshard-pooler"), filepath.Join(dir, "pgshard-router")
}

// controllerBinary is the controller built next to the router.
func controllerBinary(rtr string) string {
	return filepath.Join(filepath.Dir(rtr), "pgshard-controller")
}

// startController (re)starts the stack's controller with the shard DSNs
// known so far; the previous instance, if any, is stopped first.
func (s *stack) startController(tb testing.TB) {
	tb.Helper()
	s.stopController()
	s.controllerLog = &logBuffer{}
	args := []string{"run", "--catalog-dsn", s.catalogDSN, "--listen", "", "--election-retry", "500ms", "--apply-interval", "200ms", "--verify-roles-interval", "1s"}
	for id, dsn := range s.shardDSNs {
		args = append(args, "--shard-dsn", fmt.Sprintf("default/%d=%s", id, dsn))
	}
	s.controller, s.controllerRun = startProcess(tb, s.controllerLog, "reconciling without a gRPC listener", s.controllerBin, args...)
}

func (s *stack) stopController() {
	if s.controller == nil {
		return
	}
	_ = s.controller.Process.Signal(os.Interrupt)
	select {
	case <-s.controllerRun:
	case <-time.After(15 * time.Second):
		_ = s.controller.Process.Kill()
		<-s.controllerRun
	}
	s.controller = nil
}

// killController stops the controller without warning.
func (s *stack) killController() {
	if s.controller == nil {
		return
	}
	_ = s.controller.Process.Kill()
	<-s.controllerRun
	s.controller = nil
}

func startProcess(tb testing.TB, log *logBuffer, ready string, bin string, args ...string) (*exec.Cmd, <-chan struct{}) {
	tb.Helper()
	return startProcessEnv(tb, log, ready, nil, bin, args...)
}

// startProcessEnv is startProcess with extra environment variables.
func startProcessEnv(tb testing.TB, log *logBuffer, ready string, env []string, bin string, args ...string) (*exec.Cmd, <-chan struct{}) {
	tb.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		tb.Fatal(err)
	}
	waited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(waited) }()
	tb.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-waited:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-waited
		}
	})
	deadline := time.Now().Add(60 * time.Second)
	for !strings.Contains(log.String(), ready) {
		if time.Now().After(deadline) {
			tb.Fatalf("%s did not report %q:\n%s", filepath.Base(bin), ready, log.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cmd, waited
}

// startRouter launches another router on the stack with the given instance
// id, forwarding cancels it does not own to peers.
func (s *stack) startRouter(tb testing.TB, instanceID int, peers map[int]string, extra ...string) *routerProc {
	tb.Helper()
	p := &routerProc{log: &logBuffer{}}
	port := freePort(tb)
	p.addr = fmt.Sprintf("127.0.0.1:%d", port)
	p.peerAddr = fmt.Sprintf("127.0.0.1:%d", freePort(tb))
	p.healthAddr = fmt.Sprintf("127.0.0.1:%d", freePort(tb))
	args := []string{"serve", "--insecure-dev", "--listen", "0.0.0.0:" + fmt.Sprint(port), "--catalog-dsn", s.catalogDSN,
		"--instance-id", fmt.Sprint(instanceID), "--peer-cancel-listen", p.peerAddr, "--health-listen", p.healthAddr,
		"--drain-timeout", "5s", "--drain-delay", "1s", "--buffer-window", "10s"}
	for id, addr := range peers {
		args = append(args, "--peer", fmt.Sprintf("%d=%s", id, addr))
	}
	args = append(args, extra...)
	p.cmd, p.exited = startProcess(tb, p.log, "listening on", s.routerBin, args...)
	return p
}

func startStack(tb testing.TB) *stack {
	tb.Helper()
	return startStackWith(tb, nil)
}

// startStackWith is startStack with extra PostgreSQL options for shard 0.
func startStackWith(tb testing.TB, shardOpts []string) *stack {
	tb.Helper()
	return startStackFull(tb, nil, shardOpts)
}

// startStackFull is startStack with extra PostgreSQL options for the
// catalog and for shard 0.
func startStackFull(tb testing.TB, catalogOpts, shardOpts []string) *stack {
	tb.Helper()
	requireDocker(tb)
	poolerBin, routerBin := buildBinaries(tb)
	s := &stack{routerLog: &logBuffer{}, poolerLog: &logBuffer{}}
	_, s.catalogDSN = startPostgres(tb, "catalog", catalogOpts...)
	s.shardAddr, s.shardDSN = startPostgres(tb, "shard0", shardOpts...)
	s.poolerAddr = fmt.Sprintf("127.0.0.1:%d", freePort(tb))
	err := router.DevBootstrap{CatalogDSN: s.catalogDSN, ShardDSN: s.shardDSN, Database: appDatabase, Role: appRole,
		Password: appPassword, PoolerEndpoint: s.poolerAddr, Epoch: 1}.Run(context.Background())
	if err != nil {
		tb.Fatalf("bootstrap: %v", err)
	}
	host, port, _ := net.SplitHostPort(s.shardAddr)
	startProcess(tb, s.poolerLog, "listening on", poolerBin, "run", "--insecure-dev", "--listen", s.poolerAddr,
		"--pg-host", host, "--pg-port", port, "--pg-database", appDatabase, "--stream-dsn", streamDSN(s.shardDSN),
		"--catalog-dsn", s.catalogDSN, "--shard-set", router.DefaultShardSet, "--shard-id", "0", "--drain-timeout", "5s")
	s.routerBin = routerBin
	s.controllerBin = controllerBinary(routerBin)
	s.shardDSNs = map[int]string{0: s.shardDSN}
	s.startController(tb)
	s.routerPort = fmt.Sprint(freePort(tb))
	s.routerAddr = "127.0.0.1:" + s.routerPort
	s.peerAddr = fmt.Sprintf("127.0.0.1:%d", freePort(tb))
	s.healthAddr = fmt.Sprintf("127.0.0.1:%d", freePort(tb))
	startProcess(tb, s.routerLog, "listening on", routerBin, "serve", "--insecure-dev", "--listen", "0.0.0.0:"+s.routerPort,
		"--catalog-dsn", s.catalogDSN, "--drain-timeout", "5s", "--drain-delay", "1s",
		"--instance-id", "1", "--peer-cancel-listen", s.peerAddr, "--health-listen", s.healthAddr)
	return s
}

// streamDSN is the superuser DSN of a shard's app database, used by the
// pooler for change-stream replication connections.
func streamDSN(adminDSN string) string {
	return strings.Replace(adminDSN, "/postgres?", "/"+appDatabase+"?", 1)
}

func (s *stack) connectTo(tb testing.TB, addr string) *pgx.Conn {
	tb.Helper()
	conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", appRole, appPassword, addr, appDatabase))
	if err != nil {
		tb.Fatalf("connect to %s: %v", addr, err)
	}
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func (s *stack) dsn(user, password, db string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, s.routerAddr, db)
}

func (s *stack) connect(tb testing.TB) *pgx.Conn {
	tb.Helper()
	conn, err := pgx.Connect(context.Background(), s.dsn(appRole, appPassword, appDatabase))
	if err != nil {
		tb.Fatalf("connect through router: %v\nrouter log:\n%s\npooler log:\n%s", err, s.routerLog.String(), s.poolerLog.String())
	}
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}
