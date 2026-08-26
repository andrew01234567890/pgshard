package pgwire

import (
	"context"
	"github.com/andrew01234567890/pgshard/internal/dockertest"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const psqlImage = "ghcr.io/andrew01234567890/pgshard-postgres:18"

// TestPsqlConformance drives the real psql from the project's PostgreSQL
// image against the fake server. It is an integration test: it needs the
// docker daemon and the image and is skipped otherwise.
func TestPsqlConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in -short mode")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		dockertest.Unavailable(t, "docker not on PATH; skipping psql conformance test")
	}
	if err := exec.Command("docker", "image", "inspect", psqlImage).Run(); err != nil {
		t.Skipf("image %s not available locally; skipping psql conformance test", psqlImage)
	}
	scram, err := BuildSCRAMVerifier("s3cret", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The container reaches the host through host-gateway, which works with
	// both native Docker and Docker Desktop (where --network host is the VM
	// and only IPv4 listeners are forwarded).
	ts := startServerOn(t, Config{
		ServerVersion: "18.6 (pgshard)",
		Authenticator: SCRAMAuthenticator{Lookup: lookup(map[string]string{"alice": scram.String()})},
	}, "tcp4", "0.0.0.0:0")
	_, port, err := net.SplitHostPort(ts.addr)
	if err != nil {
		t.Fatal(err)
	}
	run := func(t *testing.T, password string, args ...string) (string, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		name := "pgwire-psql-" + strings.ReplaceAll(t.Name(), "/", "-")
		full := append([]string{"run", "--rm", "--name", name, "--add-host", "host.docker.internal:host-gateway",
			"-e", "PGPASSWORD=" + password, "-e", "PGCONNECT_TIMEOUT=10",
			"--entrypoint", "psql", psqlImage, "-h", "host.docker.internal", "-p", port, "-U", "alice", "-d", "db", "-X", "-v", "ON_ERROR_STOP=1"}, args...)
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
		// Docker Desktop publishes new host listeners to host-gateway lazily,
		// so a first attempt can be refused; retry a few times.
		for attempt := 0; ; attempt++ {
			out, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
			if err != nil && attempt < 5 && strings.Contains(string(out), "Connection refused") {
				time.Sleep(time.Second)
				continue
			}
			return string(out), err
		}
	}
	t.Run("select1", func(t *testing.T) {
		out, err := run(t, "s3cret", "-c", "select 1")
		if err != nil {
			t.Fatalf("psql: %v\n%s", err, out)
		}
		want := " ?column? \n----------\n        1\n(1 row)\n\n"
		if out != want {
			t.Fatalf("psql output = %q, want %q", out, want)
		}
	})
	t.Run("tuples-only", func(t *testing.T) {
		out, err := run(t, "s3cret", "-tA", "-c", "select 1")
		if err != nil || out != "1\n" {
			t.Fatalf("psql -tA: %v %q", err, out)
		}
	})
	t.Run("multi-statement refused", func(t *testing.T) {
		out, err := run(t, "s3cret", "-c", "select 1; select 1")
		if err == nil || !strings.Contains(out, "multi-statement simple queries are not supported") {
			t.Fatalf("psql: %v %q", err, out)
		}
	})
	t.Run("wrong password", func(t *testing.T) {
		out, err := run(t, "nope", "-c", "select 1")
		if err == nil || !strings.Contains(out, "password authentication failed") {
			t.Fatalf("psql: %v %q", err, out)
		}
	})
	t.Run("server version", func(t *testing.T) {
		out, err := run(t, "s3cret", "-tA", "-c", `\echo :SERVER_VERSION_NAME`)
		if err != nil || strings.TrimSpace(out) != "18.6 (pgshard)" {
			t.Fatalf("psql: %v %q", err, out)
		}
	})
	waitNoSessions(t, ts.Server)
}
