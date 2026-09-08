package dockertest

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// ReadyIdle is how long a container may make no progress at all before its
// wait gives up, and ReadyCap is the longest the wait will run whatever it
// sees.
//
// The bound is on PROGRESS rather than on total time because the failure it
// exists to survive is a starved machine, not a broken container. A fixed
// total is generous when the box is idle and too short when eight suites
// start PostgreSQL at once -- which is when it fires, so it fires on the
// runs least likely to be a real defect. Waiting while the container's log
// is still growing costs nothing on a healthy start and does not give up on
// a slow one.
//
// ReadyCap exists so a container that logs forever without listening still
// ends the test rather than the package's timeout.
const (
	ReadyIdle = 90 * time.Second
	ReadyCap  = 6 * time.Minute
)

// Connector reports whether the server is accepting connections. It is a
// parameter so this package does not depend on a driver: every caller
// already has one.
type Connector func(ctx context.Context) error

// WaitReady blocks until connect succeeds, and fails the test when the
// container stops, stops making progress, or reaches ReadyCap.
func WaitReady(tb testing.TB, id string, connect Connector) {
	tb.Helper()
	if err := waitReady(id, connect, dockerProbes{running: containerRunning, logSize: containerLogSize, log: containerLog}, ReadyIdle, ReadyCap); err != nil {
		tb.Fatal(err)
	}
}

// dockerProbes is what waitReady asks about the container. It is a
// parameter so the decisions below can be tested without a docker daemon:
// every branch here is one this package exists to get right, and none of
// them is reachable from a test that needs a real slow container.
type dockerProbes struct {
	running func(id string) (bool, string)
	logSize func(id string) int
	log     func(id string) string
}

// waitReady is WaitReady's decision, separated so it can be tested.
//
// It gives up EARLY when the container has exited: a container that died in
// initdb answers nothing, and waiting the full bound for it turns a
// one-line configuration error into a timeout that says only that
// PostgreSQL never became ready.
func waitReady(id string, connect Connector, probe dockerProbes, idle, limit time.Duration) error {
	start := time.Now()
	lastProgress, lastSize := start, probe.logSize(id)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := connect(ctx)
		cancel()
		if err == nil {
			return nil
		}
		if running, why := probe.running(id); !running {
			return fmt.Errorf("the container stopped after %s without accepting a connection (%s); last error %w\ncontainer log:\n%s",
				time.Since(start).Round(time.Second), why, err, probe.log(id))
		}
		if size := probe.logSize(id); size != lastSize {
			lastProgress, lastSize = time.Now(), size
		}
		switch {
		case time.Since(lastProgress) > idle:
			return fmt.Errorf("the container logged nothing for %s and never accepted a connection (%s in total); last error %w\ncontainer log:\n%s",
				idle, time.Since(start).Round(time.Second), err, probe.log(id))
		case time.Since(start) > limit:
			return fmt.Errorf("the container did not accept a connection within %s, though it was still logging; last error %w\ncontainer log:\n%s",
				limit, err, probe.log(id))
		}
		time.Sleep(min(100*time.Millisecond, idle/4))
	}
}

// containerRunning reports whether the container is still up, and what
// docker said if it is not.
func containerRunning(id string) (bool, string) {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Status}} exit={{.State.ExitCode}}", id).CombinedOutput()
	if err != nil {
		// A container started with --rm is gone the moment it exits, so
		// inspect failing is itself the answer.
		return false, "it is no longer known to docker"
	}
	status := strings.TrimSpace(string(out))
	return strings.HasPrefix(status, "running"), status
}

func containerLog(id string) string {
	out, _ := exec.Command("docker", "logs", "--tail", "40", id).CombinedOutput()
	if len(out) == 0 {
		return "(empty)"
	}
	return string(out)
}

// containerLogSize is the progress signal: a container that is still
// writing is still starting, however slowly the machine is running it.
func containerLogSize(id string) int {
	out, err := exec.Command("docker", "logs", id).CombinedOutput()
	if err != nil {
		return -1
	}
	return len(out)
}
