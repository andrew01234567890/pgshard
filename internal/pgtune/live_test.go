//go:build integration

package pgtune

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

var liveImages = []string{
	"ghcr.io/andrew01234567890/pgshard-postgres:18",
	"ghcr.io/andrew01234567890/pgshard-postgres:19",
}

// TestLiveEveryGUCExists starts each PostgreSQL image with the rendered
// override included from postgresql.conf and proves every emitted setting is
// accepted, sourced from that file, and readable through SHOW.
func TestLiveEveryGUCExists(t *testing.T) {
	if exec.Command("docker", "info").Run() != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ran := 0
	for _, img := range liveImages {
		if exec.Command("docker", "image", "inspect", img).Run() != nil {
			t.Logf("image %s not present; skipping", img)
			continue
		}
		ran++
		for _, p := range []Profile{ProfileOLTP, ProfileAnalytics} {
			t.Run(img[strings.LastIndex(img, ":")+1:]+"/"+string(p), func(t *testing.T) { liveCheck(t, img, p) })
		}
	}
	if ran == 0 {
		t.Fatal("no pgshard-postgres image available")
	}
}

func liveCheck(t *testing.T, img string, p Profile) {
	in := baseInput(2000, 4*GiB, p)
	in.Major = 18
	if strings.HasSuffix(img, ":19") {
		in.Major = 19
	}
	in.Overrides = map[string]string{"statement_timeout": "30s"}
	s, err := Derive(in)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pgshard.override.conf"), []byte(s.Render()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("pgtune-live-%d", time.Now().UnixNano())
	script := `set -e
initdb -D "$PGDATA" --auth=trust --username=postgres >/dev/null
openssl req -new -x509 -days 1 -nodes -subj /CN=pgtune -keyout "$PGDATA/server.key" -out "$PGDATA/server.crt" 2>/dev/null
chmod 600 "$PGDATA/server.key"
echo "include_if_exists = '/override/pgshard.override.conf'" >> "$PGDATA/postgresql.conf"
exec postgres -D "$PGDATA"`
	out, err := exec.Command("docker", "run", "-d", "--name", name, "--user", "postgres", "--shm-size", "2g",
		"-e", "PGDATA=/tmp/pgdata", "-v", dir+":/override:ro", img, "sh", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "-v", name).Run() })

	ready := false
	for i := 0; i < 60 && !ready; i++ {
		ready = exec.Command("docker", "exec", name, "pg_isready", "-U", "postgres", "-q").Run() == nil
		if !ready {
			if state, _ := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", name).Output(); strings.TrimSpace(string(state)) != "true" {
				break
			}
			time.Sleep(time.Second)
		}
	}
	if !ready {
		logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
		t.Fatalf("postgres did not start with the rendered override:\n%s\n%s", s.Render(), logs)
	}
	psql := func(sql string) string {
		out, err := exec.Command("docker", "exec", name, "psql", "-U", "postgres", "-Atq", "-c", sql).CombinedOutput()
		if err != nil {
			t.Fatalf("psql %q: %v\n%s", sql, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	fromFile := map[string]bool{}
	for _, line := range strings.Split(psql("SELECT name FROM pg_settings WHERE source = 'configuration file'"), "\n") {
		fromFile[line] = true
	}
	for _, x := range s {
		if !fromFile[x.Name] {
			t.Errorf("%s: not sourced from the override file (source=%q)", x.Name, psql("SELECT source FROM pg_settings WHERE name='"+x.Name+"'"))
		}
	}
	for _, x := range s {
		if got := psql("SHOW " + x.Name); got == "" {
			t.Errorf("SHOW %s returned nothing", x.Name)
		}
	}
	for name, want := range map[string]string{
		"shared_buffers": "1GB", "wal_level": "logical", "io_method": "worker", "ssl": "on",
		"max_prepared_transactions": "108", "password_encryption": "scram-sha-256", "statement_timeout": "30s",
		"idle_replication_slot_timeout": "1d", "wal_compression": "zstd", "shared_preload_libraries": "pg_stat_statements",
	} {
		if got := psql("SHOW " + name); got != want {
			t.Errorf("SHOW %s = %q, want %q", name, got, want)
		}
	}
	if got := psql("SELECT current_setting('work_mem')"); got != value(s, "work_mem") {
		t.Errorf("work_mem = %s, want %s", got, value(s, "work_mem"))
	}
}
