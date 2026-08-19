//go:build integration

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

var integrationImages = []string{
	"ghcr.io/andrew01234567890/pgshard-postgres:18",
	"ghcr.io/andrew01234567890/pgshard-postgres:19",
}

func TestAgentIntegration(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker unavailable")
	}
	bin := buildAgent(t)
	ran := 0
	for _, img := range integrationImages {
		if exec.Command("docker", "image", "inspect", img).Run() != nil {
			t.Logf("image %s not present; skipping", img)
			continue
		}
		ran++
		t.Run(img[strings.LastIndex(img, ":")+1:], func(t *testing.T) { runAgentSuite(t, img, bin) })
	}
	if ran == 0 {
		t.Fatal("no pgshard-postgres image available")
	}
}

func buildAgent(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "pgshard-agent")
	cmd := exec.Command("go", "build", "-o", bin, "../../cmd/pgshard-agent")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build agent: %v\n%s", err, out)
	}
	return bin
}

type node struct {
	t         *testing.T
	name      string
	container string
	http      string
	grpc      pgshardv1.AgentClient
}

type harness struct {
	t      *testing.T
	image  string
	bin    string
	net    string
	cfgDir string
	suffix string
	nodes  map[string]*node
	// extra is merged into every member config (backup settings and the like).
	extra map[string]any
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func newHarness(t *testing.T, image, bin string) *harness {
	t.Helper()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	h := &harness{t: t, image: image, bin: bin, net: "pgshard-agent-" + suffix, suffix: suffix, nodes: map[string]*node{}}
	h.cfgDir = filepath.Join(t.TempDir(), "cfg")
	if err := os.MkdirAll(h.cfgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.cfgDir, "pw"), []byte("pgshard-test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	docker(t, "network", "create", h.net)
	t.Cleanup(func() {
		for _, n := range h.nodes {
			_ = exec.Command("docker", "rm", "-f", n.container).Run()
		}
		_ = exec.Command("docker", "network", "rm", h.net).Run()
	})
	return h
}

func (h *harness) containerName(member string) string {
	return "pgshard-agent-" + h.suffix + "-" + member
}

func (h *harness) writeConfig(member string, role Role, source string, peers []string) string {
	h.t.Helper()
	cfg := map[string]any{
		"cluster": "it", "shard": "s0", "member": member, "role": string(role),
		"pgdata": "/var/lib/postgresql/data", "passwordFile": "/cfg/pw",
		"primaryConninfo": "host=" + h.containerName(source) + " port=5432 user=postgres",
		"podCIDR":         "0.0.0.0/0", "peerFailsafeURLs": peers,
		"lease":           map[string]any{"enabled": false},
		"shutdownTimeout": "20s",
	}
	for k, v := range h.extra {
		cfg[k] = v
	}
	b, _ := json.Marshal(cfg)
	path := filepath.Join(h.cfgDir, member+".json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		h.t.Fatal(err)
	}
	return "/cfg/" + member + ".json"
}

func (h *harness) start(member string, role Role, source string, peers []string) *node {
	h.t.Helper()
	cfgPath := h.writeConfig(member, role, source, peers)
	name := h.containerName(member)
	docker(h.t, "run", "-d", "--name", name, "--network", h.net,
		"-v", h.bin+":/pgshard-agent:ro", "-v", h.cfgDir+":/cfg:ro",
		"-p", "127.0.0.1::8080", "-p", "127.0.0.1::9090",
		"--entrypoint", "/pgshard-agent", h.image, "run", "--config", cfgPath)
	n := &node{t: h.t, name: member, container: name}
	n.connect()
	h.nodes[member] = n
	return n
}

// connect (re)reads the published ports; docker restart reassigns them.
func (n *node) connect() {
	n.t.Helper()
	n.http = "http://" + docker(n.t, "port", n.container, "8080/tcp")
	conn, err := grpc.NewClient(docker(n.t, "port", n.container, "9090/tcp"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		n.t.Fatal(err)
	}
	n.t.Cleanup(func() { _ = conn.Close() })
	n.grpc = pgshardv1.NewAgentClient(conn)
}

func (n *node) waitHTTP(path string, want int, timeout time.Duration) {
	n.t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		resp, err := http.Get(n.http + path)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == want {
				return
			}
			last = fmt.Sprint(resp.StatusCode)
		} else {
			last = err.Error()
		}
		time.Sleep(300 * time.Millisecond)
	}
	n.t.Fatalf("%s: %s did not return %d within %s (last: %s)\n%s", n.name, path, want, timeout, last, n.logs())
}

func (n *node) httpCode(path string) int {
	n.t.Helper()
	resp, err := http.Get(n.http + path)
	if err != nil {
		return -1
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func (n *node) logs() string {
	out, _ := exec.Command("docker", "logs", "--tail", "60", n.container).CombinedOutput()
	return string(out)
}

func (n *node) psql(sql string) string {
	n.t.Helper()
	out, err := exec.Command("docker", "exec", "-e", "PGPASSWORD=pgshard-test", n.container,
		"psql", "-h", "/tmp", "-U", "postgres", "-At", "-c", sql).CombinedOutput()
	if err != nil {
		n.t.Fatalf("%s psql %q: %v\n%s", n.name, sql, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (n *node) status() *pgshardv1.StatusResponse {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := n.grpc.Status(ctx, &pgshardv1.StatusRequest{})
	if err != nil {
		n.t.Fatalf("%s status: %v", n.name, err)
	}
	return st
}

func (n *node) waitStandbyCaughtUp(primary *node, timeout time.Duration) {
	n.t.Helper()
	target := primary.psql("SELECT pg_current_wal_lsn()")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.status().GetRole() == pgshardv1.StatusResponse_ROLE_STANDBY {
			out, err := exec.Command("docker", "exec", "-e", "PGPASSWORD=pgshard-test", n.container,
				"psql", "-h", "/tmp", "-U", "postgres", "-At", "-c",
				"SELECT pg_last_wal_replay_lsn() >= '"+target+"'::pg_lsn").CombinedOutput()
			if err == nil && strings.TrimSpace(string(out)) == "t" {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	n.t.Fatalf("%s did not catch up to %s within %s\n%s", n.name, target, timeout, n.logs())
}

func runAgentSuite(t *testing.T, image, bin string) {
	h := newHarness(t, image, bin)
	peersOf := func(members ...string) []string {
		var out []string
		for _, m := range members {
			out = append(out, "http://"+h.containerName(m)+":8080/failsafe")
		}
		return out
	}
	p := h.start("s0-0", RolePrimary, "s0-1", peersOf("s0-1"))
	p.waitHTTP("/startz", 200, 90*time.Second)
	p.waitHTTP("/readyz", 200, 60*time.Second)
	if got := p.psql("SHOW wal_level"); got != "logical" {
		t.Fatalf("wal_level=%s", got)
	}
	if got := p.psql("SHOW wal_log_hints"); got != "on" {
		t.Fatalf("wal_log_hints=%s", got)
	}
	p.psql("CREATE TABLE t (id int primary key, note text)")
	p.psql("INSERT INTO t VALUES (1, 'first')")

	s := h.start("s0-1", RoleStandby, "s0-0", peersOf("s0-0"))
	s.waitHTTP("/startz", 200, 120*time.Second)
	s.waitHTTP("/readyz", 200, 60*time.Second)
	s.waitStandbyCaughtUp(p, 30*time.Second)
	if got := s.psql("SELECT note FROM t WHERE id = 1"); got != "first" {
		t.Fatalf("standby did not replicate: %q", got)
	}
	if got := p.psql("SELECT slot_name || ':' || active FROM pg_replication_slots"); got != "pgshard_s0_1:true" {
		t.Fatalf("slot on primary: %q", got)
	}
	if got := p.psql("SELECT application_name FROM pg_stat_replication"); got != "s0-1" {
		t.Fatalf("application_name: %q", got)
	}
	if st := s.status(); st.GetRole() != pgshardv1.StatusResponse_ROLE_STANDBY || !st.GetRunning() || st.GetEpoch() != 0 {
		t.Fatalf("standby status: %v", st)
	}
	if p.httpCode("/livez") != 200 || s.httpCode("/livez") != 200 || s.httpCode("/failsafe") != 200 {
		t.Fatal("livez/failsafe should be 200 with all peers up")
	}

	ctx := context.Background()
	t.Log("promote with stale epoch is refused")
	if err := s.status(); err == nil {
		resp, err := s.grpc.Promote(ctx, &pgshardv1.PromoteRequest{Epoch: 0})
		if err != nil || resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "stale epoch") {
			t.Fatalf("promote epoch 0: err=%v resp=%v", err, resp)
		}
		if s.status().GetRole() != pgshardv1.StatusResponse_ROLE_STANDBY {
			t.Fatal("stale promote changed the role")
		}
	}

	t.Log("promote with epoch 1")
	resp, err := s.grpc.Promote(ctx, &pgshardv1.PromoteRequest{Epoch: 1})
	if err != nil || resp.GetError() != nil {
		t.Fatalf("promote: err=%v resp=%v\n%s", err, resp, s.logs())
	}
	if resp.GetEpoch() != 1 || resp.GetTimeline() != 2 {
		t.Fatalf("promote response: %v", resp)
	}
	st := s.status()
	if st.GetRole() != pgshardv1.StatusResponse_ROLE_PRIMARY || st.GetEpoch() != 1 || st.GetTimeline() != 2 {
		t.Fatalf("status after promote: %v", st)
	}
	s.waitHTTP("/readyz", 200, 30*time.Second)
	if s.psql("SELECT pg_is_in_recovery()") != "f" {
		t.Fatal("new primary still in recovery")
	}
	s.psql("INSERT INTO t VALUES (2, 'after-promote')")

	t.Log("replay of a stale epoch on the new primary is refused")
	if r, err := s.grpc.Promote(ctx, &pgshardv1.PromoteRequest{Epoch: 1}); err != nil || r.GetError() == nil {
		t.Fatalf("epoch 1 replayed: %v %v", r, err)
	}
	if r, err := s.grpc.Reload(ctx, &pgshardv1.ReloadRequest{Epoch: 0}); err != nil || r.GetError() == nil {
		t.Fatalf("reload with stale epoch accepted: %v %v", r, err)
	}
	if r, err := s.grpc.Reload(ctx, &pgshardv1.ReloadRequest{Epoch: 2}); err != nil || r.GetError() == nil {
		t.Fatalf("reload with a future epoch accepted: %v %v", r, err)
	}
	if r, err := s.grpc.Reload(ctx, &pgshardv1.ReloadRequest{Epoch: 1}); err != nil || r.GetError() != nil {
		t.Fatalf("reload with the current epoch refused: %v %v", r, err)
	}

	t.Log("old primary diverges, then demotes via pg_rewind")
	p.psql("INSERT INTO t VALUES (100, 'diverged')")
	p.psql("CHECKPOINT")
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Minute)
	dresp, err := p.grpc.Demote(dctx, &pgshardv1.DemoteRequest{Epoch: 1})
	dcancel()
	if err != nil || dresp.GetError() != nil {
		t.Fatalf("demote: err=%v resp=%v\n%s", err, dresp, p.logs())
	}
	if strings.Contains(p.logs(), "pg_rewind failed") {
		t.Fatalf("demote fell back to reclone; expected rewind\n%s", p.logs())
	}
	p.waitHTTP("/readyz", 200, 90*time.Second)
	p.waitStandbyCaughtUp(s, 60*time.Second)
	if got := p.psql("SELECT string_agg(note, ',' ORDER BY id) FROM t"); got != "first,after-promote" {
		t.Fatalf("rewound standby content: %q", got)
	}
	if st := p.status(); st.GetRole() != pgshardv1.StatusResponse_ROLE_STANDBY || st.GetEpoch() != 1 || st.GetTimeline() != 2 {
		t.Fatalf("old primary status: %v", st)
	}
	if got := s.psql("SELECT slot_name || ':' || active FROM pg_replication_slots"); got != "pgshard_s0_0:true" {
		t.Fatalf("slots on new primary: %q", got)
	}

	t.Log("epoch survives an agent restart")
	docker(t, "restart", p.container)
	p.connect()
	p.waitHTTP("/readyz", 200, 120*time.Second)
	if st := p.status(); st.GetEpoch() != 1 || st.GetRole() != pgshardv1.StatusResponse_ROLE_STANDBY {
		t.Fatalf("status after restart: %v", st)
	}

	t.Log("reclone rebuilds the standby from the primary")
	rctx, rcancel := context.WithTimeout(ctx, 5*time.Minute)
	rresp, err := p.grpc.Reclone(rctx, &pgshardv1.RecloneRequest{Epoch: 1, SourceKind: pgshardv1.RecloneRequest_SOURCE_KIND_PRIMARY})
	rcancel()
	if err != nil || rresp.GetError() != nil || rresp.GetEpoch() != 1 {
		t.Fatalf("reclone: err=%v resp=%v\n%s", err, rresp, p.logs())
	}
	if !strings.Contains(p.logs(), "cloning from primary") {
		t.Fatalf("reclone did not clone\n%s", p.logs())
	}
	p.waitHTTP("/readyz", 200, 90*time.Second)
	s.psql("INSERT INTO t VALUES (3, 'after-reclone')")
	p.waitStandbyCaughtUp(s, 60*time.Second)
	if got := p.psql("SELECT count(*) FROM t"); got != "3" {
		t.Fatalf("recloned standby rows: %s", got)
	}

	t.Log("slot RPCs and backup RPCs without a policy")
	cs, err := s.grpc.CreateSlot(ctx, &pgshardv1.CreateSlotRequest{Epoch: 1, Name: "extra", Kind: pgshardv1.SlotKind_SLOT_KIND_PHYSICAL})
	if err != nil || cs.GetError() != nil {
		t.Fatalf("create slot: %v %v", cs, err)
	}
	ls, err := s.grpc.ListSlots(ctx, &pgshardv1.ListSlotsRequest{})
	if err != nil || len(ls.GetSlots()) != 2 || ls.GetSlots()[0].GetName() != "extra" || ls.GetSlots()[1].GetName() != "pgshard_s0_0" || !ls.GetSlots()[1].GetActive() {
		t.Fatalf("list slots: %v %v", ls, err)
	}
	if ds, err := s.grpc.DropSlot(ctx, &pgshardv1.DropSlotRequest{Epoch: 1, Name: "extra"}); err != nil || ds.GetError() != nil {
		t.Fatalf("drop slot: %v %v", ds, err)
	}
	t.Log("stream slot lifecycle: failover slot, publication, standby sync, synchronized_standby_slots")
	if r, err := s.grpc.CreateStreamSlot(ctx, &pgshardv1.CreateStreamSlotRequest{Epoch: 0, Stream: "orders", Database: "postgres"}); err != nil || r.GetError() == nil {
		t.Fatalf("stale create stream slot accepted: %v %v", r, err)
	}
	if r, err := s.grpc.CreateStreamSlot(ctx, &pgshardv1.CreateStreamSlotRequest{Epoch: 1, Stream: "Bad-Name", Database: "postgres"}); err != nil || r.GetError() == nil {
		t.Fatalf("bad stream name accepted: %v %v", r, err)
	}
	css, err := s.grpc.CreateStreamSlot(ctx, &pgshardv1.CreateStreamSlotRequest{Epoch: 1, Stream: "orders", Database: "postgres", TwoPhase: true})
	if err != nil || css.GetError() != nil || css.GetSlot() != "pgshard_orders_s0" || css.GetLsn() == 0 {
		t.Fatalf("create stream slot: %v %v", css, err)
	}
	if again, err := s.grpc.CreateStreamSlot(ctx, &pgshardv1.CreateStreamSlotRequest{Epoch: 1, Stream: "orders", Database: "postgres", TwoPhase: true}); err != nil || again.GetError() != nil || again.GetSlot() != css.GetSlot() || again.GetLsn() == 0 {
		t.Fatalf("recreate with same two_phase must be idempotent: %v %v", again, err)
	}
	if again, err := s.grpc.CreateStreamSlot(ctx, &pgshardv1.CreateStreamSlotRequest{Epoch: 1, Stream: "orders", Database: "postgres", TwoPhase: false}); err != nil || again.GetError() == nil {
		t.Fatalf("recreate with different two_phase must fail: %v %v", again, err)
	}
	if got := s.psql("SELECT puballtables FROM pg_publication WHERE pubname = 'pgshard_all'"); got != "t" {
		t.Fatalf("publication: %q", got)
	}
	ls, err = s.grpc.ListSlots(ctx, &pgshardv1.ListSlotsRequest{})
	if err != nil || len(ls.GetSlots()) != 2 {
		t.Fatalf("list slots: %v %v", ls, err)
	}
	if sl := ls.GetSlots()[0]; sl.GetName() != "pgshard_orders_s0" || sl.GetKind() != pgshardv1.SlotKind_SLOT_KIND_LOGICAL || sl.GetPlugin() != "pgoutput" ||
		!sl.GetFailover() || !sl.GetTwoPhase() || sl.GetActive() || sl.GetWalStatus() != "reserved" || sl.GetDatabase() != "postgres" || sl.GetSynced() || sl.GetTemporary() {
		t.Fatalf("stream slot: %v", sl)
	}
	sss, err := s.grpc.SetSynchronizedStandbySlots(ctx, &pgshardv1.SetSynchronizedStandbySlotsRequest{Epoch: 1, Slots: []string{"pgshard_s0_0", "pgshard_missing"}})
	if err != nil || sss.GetError() != nil || len(sss.GetApplied()) != 1 || sss.GetApplied()[0] != "pgshard_s0_0" {
		t.Fatalf("set synchronized standby slots: %v %v", sss, err)
	}
	if got := s.psql("SHOW synchronized_standby_slots"); got != "pgshard_s0_0" {
		t.Fatalf("synchronized_standby_slots: %q", got)
	}
	s.psql("INSERT INTO t VALUES (4, 'for-sync')")
	deadline := time.Now().Add(90 * time.Second)
	for {
		pls, err := p.grpc.ListSlots(ctx, &pgshardv1.ListSlotsRequest{})
		if err == nil && len(pls.GetSlots()) == 1 && pls.GetSlots()[0].GetName() == "pgshard_orders_s0" && pls.GetSlots()[0].GetSynced() && pls.GetSlots()[0].GetFailover() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("failover slot not synchronized to the standby: %v %v\n%s", pls, err, p.logs())
		}
		time.Sleep(time.Second)
	}
	if r, err := s.grpc.SetSynchronizedStandbySlots(ctx, &pgshardv1.SetSynchronizedStandbySlotsRequest{Epoch: 1}); err != nil || r.GetError() != nil || len(r.GetApplied()) != 0 {
		t.Fatalf("clear synchronized standby slots: %v %v", r, err)
	}
	if got := s.psql("SHOW synchronized_standby_slots"); got != "" {
		t.Fatalf("synchronized_standby_slots after clear: %q", got)
	}
	if r, err := s.grpc.DropStreamSlot(ctx, &pgshardv1.DropStreamSlotRequest{Epoch: 1, Stream: "orders"}); err != nil || r.GetError() != nil {
		t.Fatalf("drop stream slot: %v %v", r, err)
	}
	if r, err := s.grpc.DropStreamSlot(ctx, &pgshardv1.DropStreamSlotRequest{Epoch: 1, Stream: "orders"}); err != nil || r.GetError() != nil {
		t.Fatalf("drop stream slot twice: %v %v", r, err)
	}
	if got := s.psql("SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'pgshard_orders_s0'"); got != "0" {
		t.Fatalf("stream slot still present: %s", got)
	}

	rp, err := s.grpc.CreateRestorePoint(ctx, &pgshardv1.CreateRestorePointRequest{Epoch: 1, Name: "rp1"})
	if err != nil || rp.GetError() != nil || rp.GetLsn() == 0 {
		t.Fatalf("restore point: %v %v", rp, err)
	}
	if bk, err := s.grpc.Backup(ctx, &pgshardv1.BackupRequest{Epoch: 1}); err != nil || !strings.Contains(bk.GetError().GetMessage(), "no backup policy") {
		t.Fatalf("backup without policy: %v %v", bk, err)
	}
	if ri, err := s.grpc.RestoreInfo(ctx, &pgshardv1.RestoreInfoRequest{}); err != nil || !strings.Contains(ri.GetError().GetMessage(), "no backup policy") {
		t.Fatalf("restore info without policy: %v %v", ri, err)
	}
	if s.status().GetEpoch() != 1 {
		t.Fatalf("epoch after refused backup: %d", s.status().GetEpoch())
	}

	t.Log("restart RPC")
	rs, err := s.grpc.Restart(ctx, &pgshardv1.RestartRequest{Epoch: 1, Mode: pgshardv1.RestartRequest_MODE_FAST})
	if err != nil || rs.GetError() != nil {
		t.Fatalf("restart: %v %v\n%s", rs, err, s.logs())
	}
	s.waitHTTP("/readyz", 200, 60*time.Second)

	t.Log("isolated primary without kube API self-fences")
	docker(t, "stop", "-t", "30", p.container)
	s.waitHTTP("/livez", 500, 30*time.Second)
	deadline = time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if docker(t, "inspect", "-f", "{{.State.Running}}", s.container) == "false" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if docker(t, "inspect", "-f", "{{.State.Running}}", s.container) != "false" {
		t.Fatalf("isolated primary did not exit\n%s", s.logs())
	}
	if !strings.Contains(s.logs(), "self-fencing") {
		t.Fatalf("no self-fence log\n%s", s.logs())
	}
	if code := docker(t, "inspect", "-f", "{{.State.ExitCode}}", p.container); code != "0" {
		t.Fatalf("SIGTERM shutdown of a standby exited %s\n%s", code, p.logs())
	}
}
