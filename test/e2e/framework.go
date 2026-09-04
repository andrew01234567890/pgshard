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
	"sync"
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

// PortForward runs kubectl port-forward to a Service and returns the base URL
// once the port is accepting connections. The forwarder stops when ctx is
// cancelled or the returned function is called.
//
// kubectl chooses the local port and says which one it took. Choosing it
// here would mean binding a listener, closing it, and asking kubectl to bind
// the same number a moment later -- and between those two the suites running
// in parallel, or anything else on the machine, can take it. The failure that
// produces says nothing about the code under test.
func (c *Cluster) PortForward(ctx context.Context, namespace, service string, port int) (string, func(), error) {
	full := []string{}
	if c.Kubeconfig != "" {
		full = append(full, "--kubeconfig", c.Kubeconfig)
	}
	if c.Context != "" {
		full = append(full, "--context", c.Context)
	}
	full = append(full, "-n", namespace, "port-forward", "--address", "127.0.0.1", "svc/"+service, fmt.Sprintf(":%d", port))
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, "kubectl", full...)
	var stderr bytes.Buffer
	var stdout lockedBuffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	if err := cmd.Start(); err != nil {
		cancel()
		return "", nil, err
	}
	stop := func() {
		cancel()
		_ = cmd.Wait()
	}
	deadline := time.Now().Add(30 * time.Second)
	var addr string
	for {
		if addr == "" {
			addr = forwardedAddr(stdout.String())
		}
		if addr != "" {
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err == nil {
				_ = conn.Close()
				return "http://" + addr, stop, nil
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			stop()
			return "", nil, fmt.Errorf("port-forward %s/%s: no local address after %s: %s%s",
				namespace, service, time.Since(deadline.Add(-30*time.Second)).Round(time.Second),
				strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// forwardedAddr reads the local address out of kubectl's "Forwarding from
// 127.0.0.1:39217 -> 8081", which is how it reports the port it chose.
func forwardedAddr(out string) string {
	for _, line := range strings.Split(out, "\n") {
		_, rest, ok := strings.Cut(line, "Forwarding from ")
		if !ok {
			continue
		}
		addr, _, _ := strings.Cut(rest, " ->")
		if addr = strings.TrimSpace(addr); strings.HasPrefix(addr, "127.0.0.1:") {
			return addr
		}
	}
	return ""
}

// lockedBuffer is written by the kubectl goroutine and read by the wait loop.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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
	backOffSince := map[string]time.Time{}
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
		if stuck := durableImagePullFailure(c.imagePullBackOff(ctx, namespace, selector), backOffSince, time.Now(), imagePullGrace); stuck != "" {
			return fmt.Errorf("pods %q in %s cannot pull an image, which waiting does not fix:\n%s", selector, namespace, stuck)
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

// imagePullGrace is how long a container may sit in ImagePullBackOff before
// the suite calls it, rather than waiting out the whole timeout. It is not
// zero because a registry hiccup backs off once and then succeeds, and a
// suite that fails on the first one trades a slow failure for a flaky one.
const imagePullGrace = 90 * time.Second

// imagePullBackOffTemplate renders one "pod: message" line per container the
// kubelet has given up pulling for, and nothing for a pod that is running,
// still pulling, or too young to have container statuses at all.
const imagePullBackOffTemplate = `{{range .items}}{{$pod := .metadata.name}}` +
	`{{with .status.containerStatuses}}{{range .}}{{with .state}}{{with .waiting}}` +
	`{{if eq .reason "ImagePullBackOff"}}{{$pod}}: {{.message}}{{"\n"}}{{end}}` +
	`{{end}}{{end}}{{end}}{{end}}{{end}}`

// imagePullBackOff lists the containers the kubelet has given up pulling for,
// one "pod/container: message" per line.
func (c *Cluster) imagePullBackOff(ctx context.Context, namespace, selector string) []string {
	out, err := c.Kubectl(ctx, nil, "-n", namespace, "get", "pods", "-l", selector, "-o", "go-template="+imagePullBackOffTemplate)
	if err != nil {
		return nil
	}
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// durableImagePullFailure reports the containers that have been backing off
// for longer than grace, and records when each was first seen. A container
// that recovers drops out of the input and is forgotten.
//
// A pod that cannot pull its image never becomes ready, so waiting out the
// timeout only delays the same failure and reports it as "pods not ready" --
// the image and the 403 behind it end up in a must-gather dump instead of in
// the message. Both backup cells failed this way on every pull request for
// long enough that the cause was looked for in the product.
func durableImagePullFailure(lines []string, since map[string]time.Time, now time.Time, grace time.Duration) string {
	current := make(map[string]bool, len(lines))
	var stuck []string
	for _, l := range lines {
		current[l] = true
		first, ok := since[l]
		if !ok {
			since[l] = now
			continue
		}
		if now.Sub(first) >= grace {
			stuck = append(stuck, l)
		}
	}
	for l := range since {
		if !current[l] {
			delete(since, l)
		}
	}
	return strings.Join(stuck, "\n")
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
	// Pod identity, so a pod that was REPLACED during the run is visible
	// rather than inferred. The logs below are whatever pods exist now, plus
	// a -previous.txt for a container that restarted; a pod whose Deployment
	// replaced it leaves no trace at all, and its log is the one that had the
	// history. A UID that does not match the one the suite saw at the start
	// says that is what happened.
	save("pod-identity.txt", "get", "pods", "-A", "-o",
		`custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,UID:.metadata.uid,RESTARTS:.status.containerStatuses[*].restartCount,START:.status.startTime`)
	save("nodes.txt", "describe", "nodes")
	save("describe-"+SystemNamespace+".txt", "-n", SystemNamespace, "describe", "all")

	// Member pods and their PVCs, explicitly. `get all` does not include
	// PersistentVolumeClaims at all, and a storage test polls precisely the
	// storageClassName on them -- so the bundle omitted the object the
	// failing assertion was about. Phase and conditions are spelled out
	// because a pod that is Running but not Ready is the common shape of
	// these failures and a phase alone does not show it.
	save("pods.txt", "get", "pods", "-A", "-o", "wide")
	save("pod-conditions.txt", "get", "pods", "-A", "-o",
		`custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,PHASE:.status.phase,READY:.status.conditions[?(@.type=="Ready")].status,REASON:.status.conditions[?(@.type=="Ready")].reason`)
	save("pvcs.txt", "get", "pvc", "-A", "-o",
		`custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,STATUS:.status.phase,CLASS:.spec.storageClassName,VOLUME:.spec.volumeName,REQUESTED:.spec.resources.requests.storage`)
	save("pgshard-objects.yaml", "get", "pgshardclusters,pgshardgroups,pgshardbackups,pgshardrestores,pgshardreshards",
		"-A", "-o", "yaml")

	// Logs for every pod the operator manages, in whatever namespace the
	// suite put its cluster in. Only SystemNamespace was collected before,
	// so an operator failure kept its own log and none of the members it
	// was failing to reconcile -- which is the half that says what the
	// members were actually doing.
	c.gatherPodLogs(ctx, save, SystemNamespace, "")
	c.gatherPodLogs(ctx, save, "", LabelCluster)
}

// LabelCluster marks every pod the operator creates for a cluster. It is
// duplicated from internal/operator rather than imported so the e2e module
// does not depend on the operator's internals.
const LabelCluster = "pgshard.io/cluster"

// gatherPodLogs saves current and previous logs for the pods matching
// selector, in one namespace or across all of them when namespace is empty.
func (c *Cluster) gatherPodLogs(ctx context.Context, save func(string, ...string), namespace, selector string) {
	args := []string{"get", "pods", "-o", "custom-columns=NS:.metadata.namespace,NAME:.metadata.name", "--no-headers"}
	if namespace == "" {
		args = append(args, "-A")
	} else {
		args = append([]string{"-n", namespace}, args...)
	}
	if selector != "" {
		args = append(args, "-l", selector)
	}
	out, err := c.Kubectl(ctx, nil, args...)
	if err != nil {
		return
	}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		ns, name := f[0], f[1]
		save("logs-"+ns+"-"+name+".txt", "-n", ns, "logs", "--all-containers", "--prefix", "pod/"+name)
		save("logs-"+ns+"-"+name+"-previous.txt", "-n", ns, "logs", "--all-containers", "--previous", "pod/"+name)
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
