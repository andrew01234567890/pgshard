//go:build integration

package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/dockertest"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

const minioImage = "minio/minio:RELEASE.2025-09-07T16-13-09Z"

// startMinIO runs MinIO on the harness network with the pgshard bucket and
// returns the agent's backup settings for stanza against it.
func (h *harness) startMinIO(stanza string) map[string]any {
	t := h.t
	t.Helper()
	minio := "pgshard-minio-" + h.suffix
	docker(t, "run", "-d", "--name", minio, "--network", h.net,
		"-e", "MINIO_ROOT_USER=minioadmin", "-e", "MINIO_ROOT_PASSWORD=minioadmin", minioImage, "server", "/data")
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", minio).Run() })
	for name, content := range map[string]string{"key": "minioadmin\n", "keySecret": "minioadmin\n", "passphrase": "integration-cipher\n"} {
		if err := os.WriteFile(filepath.Join(h.cfgDir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// The bucket must exist before the stanza can be created; a throwaway
	// mc-style call through the MinIO image does it without another image.
	docker(t, "run", "--rm", "--network", h.net, "--entrypoint", "sh", minioImage, "-c",
		"for i in $(seq 1 30); do mc alias set store http://"+minio+":9000 minioadmin minioadmin >/dev/null 2>&1 && mc mb --ignore-existing store/pgshard && exit 0; sleep 1; done; exit 1")
	return map[string]any{
		"stanza": stanza,
		"repo": map[string]any{
			"type": "s3", "bucket": "pgshard", "endpoint": "http://" + minio + ":9000", "region": "us-east-1",
			"path": "/it", "uriStyle": "path", "verifyTLS": false, "credentialsDir": "/cfg",
		},
		"cipherPassFile": "/cfg/passphrase",
		"retentionFull":  2,
	}
}

// restoreFrom starts member as a fresh primary restored from the harness
// backup settings with the given restore spec and waits until it promoted.
func (h *harness) restoreFrom(member string, spec map[string]any) *node {
	t := h.t
	t.Helper()
	bk := map[string]any{}
	for k, v := range h.extra["backup"].(map[string]any) {
		bk[k] = v
	}
	bk["stanza"] = member + "-pg18"
	saved := h.extra
	restoreExtra := map[string]any{"backup": bk, "restore": spec}
	for k, v := range saved {
		if k != "backup" && k != "restore" {
			restoreExtra[k] = v
		}
	}
	h.extra = restoreExtra
	n := h.start(member, RolePrimary, member, nil)
	h.extra = saved
	n.waitHTTP("/startz", 200, 4*time.Minute)
	n.waitHTTP("/readyz", 200, 60*time.Second)
	if got := n.psql("SELECT pg_is_in_recovery()"); got != "f" {
		t.Fatalf("%s still in recovery\n%s", member, n.logs())
	}
	if got := n.psql("SELECT timeline_id FROM pg_control_checkpoint()"); got == "1" {
		t.Fatalf("%s stayed on timeline 1\n%s", member, n.logs())
	}
	return n
}

// TestBackupIntegration drives pgBackRest through the agent against MinIO:
// archive_command wiring, stanza creation, full/diff/incr backups, info,
// verify and the primary-only rule.
func TestBackupIntegration(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	image := integrationImages[0]
	if img := os.Getenv("PGSHARD_POSTGRES_IMAGE"); img != "" {
		image = img
	}
	if exec.Command("docker", "image", "inspect", image).Run() != nil {
		t.Skipf("image %s not present", image)
	}
	bin := buildAgent(t)
	h := newHarness(t, image, bin)
	h.extra = map[string]any{"backup": h.startMinIO("it-s0-pg18")}
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
	if got := p.psql("SHOW archive_mode"); got != "on" {
		t.Fatalf("archive_mode=%s", got)
	}
	if got := p.psql("SHOW archive_command"); !strings.Contains(got, "--stanza=it-s0-pg18 archive-push %p") {
		t.Fatalf("archive_command=%s", got)
	}
	s := h.start("s0-1", RoleStandby, "s0-0", peersOf("s0-0"))
	s.waitHTTP("/startz", 200, 120*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	waitInfo := func(cond func(*pgshardv1.RestoreInfoResponse) bool, what string) *pgshardv1.RestoreInfoResponse {
		t.Helper()
		deadline := time.Now().Add(2 * time.Minute)
		var last *pgshardv1.RestoreInfoResponse
		for time.Now().Before(deadline) {
			last, _ = p.grpc.RestoreInfo(ctx, &pgshardv1.RestoreInfoRequest{})
			if last.GetError() == nil && cond(last) {
				return last
			}
			time.Sleep(time.Second)
		}
		t.Fatalf("timed out waiting for %s; last %v\n%s", what, last, p.logs())
		return nil
	}
	waitInfo(func(r *pgshardv1.RestoreInfoResponse) bool {
		return r.GetStatusCode() != 0 || r.GetStanza() == "it-s0-pg18"
	}, "stanza")

	full, err := p.grpc.Backup(ctx, &pgshardv1.BackupRequest{Type: pgshardv1.BackupRequest_TYPE_FULL})
	if err != nil || full.GetError() != nil {
		t.Fatalf("full backup: %v %v\n%s", err, full.GetError(), p.logs())
	}
	if !strings.HasSuffix(full.GetBackupRef(), "F") || full.GetInfo().GetType() != "full" || full.GetInfo().GetStopLsn() <= full.GetInfo().GetStartLsn() || full.GetInfo().GetArchiveStop() == "" {
		t.Fatalf("full info %v", full)
	}
	p.psql("CREATE TABLE b (id int primary key)")
	p.psql("INSERT INTO b SELECT generate_series(1, 5000)")
	seg := p.psql("SELECT pg_walfile_name(pg_switch_wal())")
	diff, err := p.grpc.Backup(ctx, &pgshardv1.BackupRequest{Type: pgshardv1.BackupRequest_TYPE_DIFF})
	if err != nil || diff.GetError() != nil {
		t.Fatalf("diff backup: %v %v", err, diff.GetError())
	}
	if diff.GetInfo().GetType() != "diff" || diff.GetInfo().GetPrior() != full.GetBackupRef() {
		t.Fatalf("diff info %v", diff.GetInfo())
	}
	p.psql("INSERT INTO b SELECT generate_series(5001, 6000)")
	incr, err := p.grpc.Backup(ctx, &pgshardv1.BackupRequest{Type: pgshardv1.BackupRequest_TYPE_INCR})
	if err != nil || incr.GetError() != nil {
		t.Fatalf("incr backup: %v %v", err, incr.GetError())
	}
	if incr.GetInfo().GetType() != "incr" || incr.GetInfo().GetPrior() != diff.GetBackupRef() {
		t.Fatalf("incr info %v", incr.GetInfo())
	}
	info := waitInfo(func(r *pgshardv1.RestoreInfoResponse) bool {
		return r.GetArchiveMax() >= seg && len(r.GetBackups()) == 3
	}, "archived WAL "+seg)
	if info.GetStatusCode() != 0 || info.GetBackups()[0].GetLabel() != full.GetBackupRef() || info.GetBackups()[2].GetLabel() != incr.GetBackupRef() {
		t.Fatalf("info %v", info)
	}
	if got := p.psql("SELECT last_archived_wal FROM pg_stat_archiver"); got < seg {
		t.Fatalf("pg_stat_archiver.last_archived_wal=%s < %s", got, seg)
	}
	ver, err := p.grpc.Verify(ctx, &pgshardv1.VerifyRequest{})
	if err != nil || ver.GetError() != nil {
		t.Fatalf("verify: %v %v", err, ver.GetError())
	}
	exp, err := p.grpc.Expire(ctx, &pgshardv1.ExpireRequest{})
	if err != nil || exp.GetError() != nil {
		t.Fatalf("expire: %v %v", err, exp.GetError())
	}
	if resp, _ := s.grpc.Backup(ctx, &pgshardv1.BackupRequest{}); resp.GetError() == nil || !strings.Contains(resp.GetError().GetMessage(), "primary only") {
		t.Fatalf("standby backup must be refused: %v", resp)
	}
	if got := s.psql("SHOW restore_command"); !strings.Contains(got, "archive-get") {
		t.Fatalf("standby restore_command=%s", got)
	}
	if out, err := exec.Command("docker", "exec", p.container, "grep", "-c", "repo1-cipher-type=aes-256-cbc", "/etc/pgbackrest/pgbackrest.conf").CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "1" {
		t.Fatalf("pgbackrest.conf cipher: %v %s", err, out)
	}

	// Replica re-clone from the repository: rows written after the last
	// backup arrive through the archive and the stream.
	p.psql("INSERT INTO b SELECT generate_series(6001, 6500)")
	rc, err := s.grpc.Reclone(ctx, &pgshardv1.RecloneRequest{SourceKind: pgshardv1.RecloneRequest_SOURCE_KIND_BACKUP})
	if err != nil || rc.GetError() != nil {
		t.Fatalf("reclone from backup: %v %v\n%s", err, rc.GetError(), s.logs())
	}
	if !strings.Contains(s.logs(), "recloning from the repository") {
		t.Fatalf("standby did not restore from the repository:\n%s", s.logs())
	}
	s.waitHTTP("/readyz", 200, 120*time.Second)
	waitCount(t, s, "b", 6500)
	if got := s.psql("SELECT pg_is_in_recovery()"); got != "t" {
		t.Fatalf("recloned standby in recovery = %s", got)
	}
	if got := s.psql("SELECT status FROM pg_stat_wal_receiver"); got != "streaming" {
		t.Fatalf("recloned standby wal receiver = %q", got)
	}

	// Restore points and a time between two states of the table.
	p.psql("SELECT pg_create_restore_point('rp1')")
	p.psql("INSERT INTO b SELECT generate_series(6501, 7000)")
	tAfter := p.psql("SELECT clock_timestamp()")
	time.Sleep(1100 * time.Millisecond)
	p.psql("DELETE FROM b WHERE id > 5000")
	seg2 := p.psql("SELECT pg_walfile_name(pg_switch_wal())")
	waitInfo(func(r *pgshardv1.RestoreInfoResponse) bool { return r.GetArchiveMax() >= seg2 }, "archived WAL "+seg2)

	restore := func(member string, spec map[string]any) *node {
		t.Helper()
		n := h.restoreFrom(member, spec)
		if got := n.psql("SHOW archive_command"); !strings.Contains(got, "--stanza="+member+"-pg18 archive-push") {
			t.Fatalf("%s archive_command=%s", member, got)
		}
		if got := n.psql("SHOW archive_mode"); got != "on" {
			t.Fatalf("%s archive_mode=%s", member, got)
		}
		if got := n.psql("SHOW restore_command"); !strings.Contains(got, "--stanza="+member+"-pg18 archive-get") {
			t.Fatalf("%s restore_command=%s", member, got)
		}
		return n
	}
	byName := restore("rp", map[string]any{"stanza": "it-s0-pg18", "type": "name", "target": "rp1", "backupId": incr.GetBackupRef()})
	if got := byName.psql("SELECT count(*) FROM b"); got != "6500" {
		t.Fatalf("restore to rp1: %s rows\n%s", got, byName.logs())
	}
	byTime := restore("rt", map[string]any{"stanza": "it-s0-pg18", "type": "time", "target": tAfter})
	if got := byTime.psql("SELECT count(*) FROM b"); got != "7000" {
		t.Fatalf("restore to %s: %s rows\n%s", tAfter, got, byTime.logs())
	}
	if got := p.psql("SELECT count(*) FROM b"); got != "5000" {
		t.Fatalf("source rows changed: %s", got)
	}
	// The source stanza only ever holds the source's WAL: its own history
	// stops at timeline 1.
	if info := waitInfo(func(*pgshardv1.RestoreInfoResponse) bool { return true }, "info"); len(info.GetBackups()) != 3 {
		t.Fatalf("source stanza changed by restores: %v", info)
	}
}

func waitCount(t *testing.T, n *node, table string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var got string
	for time.Now().Before(deadline) {
		got = n.psql("SELECT count(*) FROM " + table)
		if got == fmt.Sprint(want) {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s: %s has %s rows, want %d\n%s", n.name, table, got, want, n.logs())
}
