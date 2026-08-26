//go:build e2e || chaos

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// SystemNamespace is where pgshard components run and where must-gather looks.
const SystemNamespace = "pgshard-system"

// Cluster wraps kubectl access to the cluster under test.
type Cluster struct {
	Kubeconfig string
	Context    string
	Artifacts  string
}

// NewCluster resolves cluster access from the environment.
func NewCluster(t testing.TB) *Cluster {
	t.Helper()
	c := &Cluster{
		Kubeconfig: os.Getenv("KUBECONFIG"),
		Artifacts:  os.Getenv("E2E_ARTIFACTS"),
	}
	if c.Kubeconfig == "" {
		name := os.Getenv("KIND_CLUSTER_NAME")
		if name == "" {
			name = "pgshard-e2e"
		}
		c.Context = "kind-" + name
	}
	if c.Artifacts == "" {
		c.Artifacts = "artifacts"
	}
	if err := os.MkdirAll(c.Artifacts, 0o755); err != nil {
		t.Fatalf("create artifacts dir: %v", err)
	}
	if _, err := c.Kubectl(context.Background(), nil, "version", "--request-timeout=30s"); err != nil {
		t.Fatalf("cluster unreachable: %v", err)
	}
	return c
}

// Kubectl runs kubectl with the cluster's kubeconfig/context and returns stdout.
func (c *Cluster) Kubectl(ctx context.Context, stdin []byte, args ...string) (string, error) {
	full := []string{}
	if c.Kubeconfig != "" {
		full = append(full, "--kubeconfig", c.Kubeconfig)
	}
	if c.Context != "" {
		full = append(full, "--context", c.Context)
	}
	full = append(full, args...)
	cmd := exec.CommandContext(ctx, "kubectl", full...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// PortForward runs kubectl port-forward to a Service on an ephemeral local
// port and returns the base URL once the port is accepting connections. The
// forwarder stops when ctx is cancelled or the returned function is called.
func (c *Cluster) PortForward(ctx context.Context, namespace, service string, port int) (string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	local := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	full := []string{}
	if c.Kubeconfig != "" {
		full = append(full, "--kubeconfig", c.Kubeconfig)
	}
	if c.Context != "" {
		full = append(full, "--context", c.Context)
	}
	full = append(full, "-n", namespace, "port-forward", "--address", "127.0.0.1", "svc/"+service, fmt.Sprintf("%d:%d", local, port))
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, "kubectl", full...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		return "", nil, err
	}
	stop := func() {
		cancel()
		_ = cmd.Wait()
	}
	addr := fmt.Sprintf("127.0.0.1:%d", local)
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return "http://" + addr, stop, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			stop()
			return "", nil, fmt.Errorf("port-forward %s/%s: %w: %s", namespace, service, err, strings.TrimSpace(stderr.String()))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Apply applies a manifest from memory.
func (c *Cluster) Apply(ctx context.Context, manifest string) error {
	_, err := c.Kubectl(ctx, []byte(manifest), "apply", "-f", "-")
	return err
}

// Delete deletes the objects in a manifest, ignoring missing ones.
func (c *Cluster) Delete(ctx context.Context, manifest string) error {
	_, err := c.Kubectl(ctx, []byte(manifest), "delete", "--ignore-not-found", "--wait=true", "-f", "-")
	return err
}

// WaitPodsReady blocks until all pods matching selector in namespace are Ready.
func (c *Cluster) WaitPodsReady(ctx context.Context, namespace, selector string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		out, err := c.Kubectl(ctx, nil, "-n", namespace, "get", "pods", "-l", selector, "-o", "name")
		remaining := time.Until(deadline).Truncate(time.Second)
		if err == nil && strings.TrimSpace(out) != "" && remaining > 0 {
			_, err = c.Kubectl(ctx, nil, "-n", namespace, "wait", "--for=condition=Ready", "pod", "-l", selector,
				"--timeout="+remaining.String())
			if err == nil {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pods %q in %s not ready after %s: %w", selector, namespace, timeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// MustGather dumps cluster state and pgshard-system pod logs into the artifacts dir.
func (c *Cluster) MustGather(ctx context.Context, name string) {
	dir := filepath.Join(c.Artifacts, name)
	_ = os.MkdirAll(dir, 0o755)
	save := func(file string, args ...string) {
		out, err := c.Kubectl(ctx, nil, args...)
		if err != nil {
			out += "\n# error: " + err.Error() + "\n"
		}
		_ = os.WriteFile(filepath.Join(dir, file), []byte(out), 0o644)
	}
	save("all.yaml", "get", "all", "-A", "-o", "yaml")
	save("events.txt", "get", "events", "-A", "--sort-by=.lastTimestamp")
	save("nodes.txt", "describe", "nodes")
	save("describe-"+SystemNamespace+".txt", "-n", SystemNamespace, "describe", "all")
	pods, err := c.Kubectl(ctx, nil, "-n", SystemNamespace, "get", "pods", "-o", "name")
	if err != nil {
		return
	}
	for _, p := range strings.Fields(pods) {
		short := strings.TrimPrefix(p, "pod/")
		save("logs-"+short+".txt", "-n", SystemNamespace, "logs", "--all-containers", "--prefix", p)
		save("logs-"+short+"-previous.txt", "-n", SystemNamespace, "logs", "--all-containers", "--previous", p)
	}
}

// Summary is a compact snapshot of what the cluster looks like right now:
// pgshard pods with their readiness, the cluster's conditions, and the most
// recent events. It is meant to be embedded in a timeout failure so the CI log
// says what the test was waiting on without anyone downloading the artifacts.
func (c *Cluster) Summary(ctx context.Context, namespace string) string {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var b strings.Builder
	section := func(title string, args ...string) {
		out, err := c.Kubectl(ctx, nil, args...)
		if err != nil {
			out = "error: " + err.Error()
		}
		out = strings.TrimSpace(out)
		if out == "" {
			out = "(none)"
		}
		fmt.Fprintf(&b, "\n--- %s ---\n%s\n", title, out)
	}
	section("pods", "-n", namespace, "get", "pods", "-o", "wide")
	section("pgshardclusters", "-n", namespace, "get", "pgshardcluster", "-o",
		`jsonpath={range .items[*]}{.metadata.name}{"\tshards="}{.status.effectiveShards}{"\n"}{range .status.conditions[*]}{"  "}{.type}{"="}{.status}{"\n"}{end}{end}`)
	section("recent events", "-n", namespace, "get", "events", "--sort-by=.lastTimestamp")
	return b.String()
}

// GatherOnFailure registers a cleanup that collects diagnostics when the test fails.
func (c *Cluster) GatherOnFailure(t testing.TB) {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			c.MustGather(ctx, t.Name())
		}
	})
}
